package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
	"unicode"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	dispatchStateName = "alfred-dispatch-state"
	dispatchStateKey  = "state.json"

	dispatchPrepared     = "prepared"
	dispatchSubmitted    = "submitted"
	dispatchAcknowledged = "acknowledged"
	dispatchCompleted    = "completed"
	dispatchFailed       = "failed"
	dispatchStalled      = "stalled"

	dispatchStateMaxBytes = 512 * 1024
	dispatchMaxEntries    = 256
	dispatchPayloadMax    = 4096
	dispatchHistoryTTL    = time.Hour
)

type dispatchEntry struct {
	UUID              string                `json:"uuid"`
	Workload          types.NamespacedName  `json:"workload"`
	WorkloadUID       types.UID             `json:"workloadUID"`
	IRName            string                `json:"irName"`
	IRUID             types.UID             `json:"irUID"`
	Component         v1beta1.ComponentType `json:"component"`
	Instance          int32                 `json:"instance"`
	FromNode          string                `json:"fromNode"`
	Targets           []string              `json:"targets,omitempty"`
	Payload           string                `json:"payload"`
	SourceFingerprint string                `json:"sourceFingerprint"`
	CreatedAt         time.Time             `json:"createdAt"`
	LastAttempt       *time.Time            `json:"lastAttempt,omitempty"`
	AcknowledgedAt    *time.Time            `json:"acknowledgedAt,omitempty"`
	CompletedAt       *time.Time            `json:"completedAt,omitempty"`
	Phase             string                `json:"phase"`
	Reason            string                `json:"reason,omitempty"`
}

type dispatchJournal struct {
	Version      string          `json:"version"`
	Entries      []dispatchEntry `json:"entries"`
	BackoffUntil *time.Time      `json:"backoffUntil,omitempty"`
}

func loadDispatchJournal(
	ctx context.Context,
	reader client.Reader,
	namespace string,
) (*corev1.ConfigMap, *dispatchJournal, error) {
	cm := &corev1.ConfigMap{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dispatchStateName}, cm); err != nil {
		return nil, nil, fmt.Errorf("load dispatch state ConfigMap: %w", err)
	}
	raw, ok := cm.Data[dispatchStateKey]
	if !ok {
		return nil, nil, errors.New("dispatch state ConfigMap is missing state data")
	}
	if len(raw) > dispatchStateMaxBytes {
		return nil, nil, errors.New("dispatch state exceeds size limit")
	}
	if err := validateRawDispatchState(raw); err != nil {
		return nil, nil, err
	}

	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var journal *dispatchJournal
	if err := decoder.Decode(&journal); err != nil {
		return nil, nil, errors.New("dispatch state contains malformed or unsupported JSON")
	}
	if journal == nil {
		return nil, nil, errors.New("dispatch state must be a JSON object")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, nil, err
	}
	if err := validateDispatchJournal(journal); err != nil {
		return nil, nil, err
	}
	return cm, journal, nil
}

func saveDispatchJournal(
	ctx context.Context,
	writer client.Client,
	cm *corev1.ConfigMap,
	journal *dispatchJournal,
) error {
	if cm == nil {
		return errors.New("dispatch state ConfigMap is required")
	}
	if cm.Name != dispatchStateName || cm.Namespace == "" {
		return errors.New("dispatch state ConfigMap has invalid identity")
	}
	if cm.ResourceVersion == "" {
		return errors.New("dispatch state ConfigMap has no observed resourceVersion")
	}
	if err := validateDispatchJournal(journal); err != nil {
		return err
	}
	raw, err := json.Marshal(journal)
	if err != nil {
		return errors.New("dispatch state cannot be encoded")
	}
	if len(raw) > dispatchStateMaxBytes {
		return errors.New("dispatch state exceeds size limit")
	}

	updated := cm.DeepCopy()
	if updated.Data == nil {
		updated.Data = make(map[string]string)
	}
	updated.Data[dispatchStateKey] = string(raw)
	if err := writer.Update(ctx, updated); err != nil {
		return fmt.Errorf("update dispatch state ConfigMap: %w", err)
	}
	return nil
}

func (journal *dispatchJournal) prune(now time.Time) {
	if journal == nil {
		return
	}
	cutoff := now.Add(-dispatchHistoryTTL)
	retained := journal.Entries[:0]
	for _, entry := range journal.Entries {
		if !entry.terminal() || !entry.CompletedAt.Before(cutoff) {
			retained = append(retained, entry)
		}
	}
	journal.Entries = retained
}

func (entry dispatchEntry) terminal() bool {
	if entry.Phase != dispatchCompleted && entry.Phase != dispatchFailed {
		return false
	}
	return entry.CompletedAt != nil && !entry.CompletedAt.Before(entry.CreatedAt)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("dispatch state contains trailing JSON data")
	}
	return nil
}

func validateRawDispatchState(raw string) error {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.UseNumber()
	if err := scanUniqueJSONValue(decoder); err != nil {
		if errors.Is(err, errDuplicateJSONMember) {
			return errDuplicateJSONMember
		}
		return errors.New("dispatch state contains malformed JSON")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return err
	}

	var shape struct {
		Entries []struct {
			Instance json.RawMessage `json:"instance"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(raw), &shape); err != nil {
		return errors.New("dispatch state contains malformed JSON")
	}
	for i := range shape.Entries {
		instance := bytes.TrimSpace(shape.Entries[i].Instance)
		if len(instance) == 0 || bytes.Equal(instance, []byte("null")) {
			return fmt.Errorf("dispatch state entry %d is missing instance", i)
		}
	}
	return nil
}

var errDuplicateJSONMember = errors.New("dispatch state contains duplicate object member")

func scanUniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		members := make(map[string]struct{})
		for decoder.More() {
			memberToken, err := decoder.Token()
			if err != nil {
				return err
			}
			member, ok := memberToken.(string)
			if !ok {
				return errors.New("JSON object member is not a string")
			}
			foldedMember := foldJSONMember(member)
			if _, duplicate := members[foldedMember]; duplicate {
				return errDuplicateJSONMember
			}
			members[foldedMember] = struct{}{}
			if err := scanUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("unexpected JSON delimiter")
	}
}

// foldJSONMember matches encoding/json's case-insensitive struct-field lookup.
func foldJSONMember(member string) string {
	folded := make([]rune, 0, len(member))
	for _, r := range member {
		for {
			next := unicode.SimpleFold(r)
			if next <= r {
				break
			}
			r = next
		}
		folded = append(folded, r)
	}
	return string(folded)
}

func validateDispatchJournal(journal *dispatchJournal) error {
	if journal == nil {
		return errors.New("dispatch state is required")
	}
	if journal.Version != "v1" {
		return errors.New("dispatch state has unsupported version")
	}
	if journal.Entries == nil {
		return errors.New("dispatch state entries are required")
	}
	if len(journal.Entries) > dispatchMaxEntries {
		return errors.New("dispatch state exceeds entry limit")
	}
	if journal.BackoffUntil != nil && journal.BackoffUntil.IsZero() {
		return errors.New("dispatch state has invalid backoff time")
	}

	seen := make(map[uuid.UUID]struct{}, len(journal.Entries))
	for i := range journal.Entries {
		parsedUUID, err := validateDispatchEntry(&journal.Entries[i])
		if err != nil {
			return fmt.Errorf("dispatch state entry %d is invalid: %w", i, err)
		}
		if _, ok := seen[parsedUUID]; ok {
			return fmt.Errorf("dispatch state entry %d has duplicate UUID", i)
		}
		seen[parsedUUID] = struct{}{}
	}
	return nil
}

func validateDispatchEntry(entry *dispatchEntry) (uuid.UUID, error) {
	parsedUUID, err := uuid.Parse(entry.UUID)
	if err != nil || parsedUUID.Variant() != uuid.RFC4122 {
		return uuid.Nil, errors.New("UUID is not RFC4122")
	}
	if len(validation.IsDNS1123Label(entry.Workload.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(entry.Workload.Name)) != 0 {
		return uuid.Nil, errors.New("workload identity is invalid")
	}
	if entry.WorkloadUID == "" {
		return uuid.Nil, errors.New("workload UID is required")
	}
	if len(validation.IsDNS1123Subdomain(entry.IRName)) != 0 {
		return uuid.Nil, errors.New("InferenceReplica name is invalid")
	}
	if entry.IRUID == "" {
		return uuid.Nil, errors.New("InferenceReplica UID is required")
	}
	if !validDispatchComponent(entry.Component) {
		return uuid.Nil, errors.New("component is invalid")
	}
	if entry.Instance < 0 {
		return uuid.Nil, errors.New("instance must be nonnegative")
	}
	if entry.FromNode == "" {
		return uuid.Nil, errors.New("source node is required")
	}
	if entry.SourceFingerprint == "" {
		return uuid.Nil, errors.New("source fingerprint is required")
	}
	if len(entry.Payload) > dispatchPayloadMax || !json.Valid([]byte(entry.Payload)) {
		return uuid.Nil, errors.New("payload is invalid")
	}
	if entry.CreatedAt.IsZero() {
		return uuid.Nil, errors.New("creation time is required")
	}
	if entry.LastAttempt != nil && entry.LastAttempt.Before(entry.CreatedAt) {
		return uuid.Nil, errors.New("last attempt predates creation")
	}
	if entry.AcknowledgedAt != nil && entry.AcknowledgedAt.Before(entry.CreatedAt) {
		return uuid.Nil, errors.New("acknowledgement predates creation")
	}

	switch entry.Phase {
	case dispatchCompleted, dispatchFailed:
		if !entry.terminal() {
			return uuid.Nil, errors.New("terminal phase has invalid completion time")
		}
	case dispatchPrepared, dispatchSubmitted, dispatchAcknowledged, dispatchStalled:
		if entry.CompletedAt != nil {
			return uuid.Nil, errors.New("unresolved phase carries completion time")
		}
	default:
		return uuid.Nil, errors.New("phase is invalid")
	}
	return parsedUUID, nil
}

func validDispatchComponent(component v1beta1.ComponentType) bool {
	switch component {
	case v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent:
		return true
	default:
		return false
	}
}
