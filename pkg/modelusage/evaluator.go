package modelusage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/evictionplanner"
)

// Evaluator maintains history and returns advisory planner eligibility. Reader
// must be uncached (e.g. manager.GetAPIReader); Writer patches only history.
type Evaluator struct {
	reader     client.Reader
	writer     client.Writer
	references *ReferenceQuery
	now        func() time.Time
}

// NewEvaluator uses time.Now when now is nil.
func NewEvaluator(reader client.Reader, writer client.Writer, references *ReferenceQuery, now func() time.Time) *Evaluator {
	if now == nil {
		now = time.Now
	}
	return &Evaluator{reader: reader, writer: writer, references: references, now: now}
}

type history struct {
	IdleSince                 *time.Time `json:"idleSince,omitempty"`
	LastObservedDemandEndedAt *time.Time `json:"lastObservedDemandEndedAt,omitempty"`
}

// Reconcile checks current references before changing history. Planner callers
// must pass the inventory UID; an empty expectedUID is for history-only events.
// A protected success consumes a release event: the remaining source's later
// removal supplies its own event. Errors should retry the identical request.
// This result is advisory, not authorization to delete artifacts; the execution
// controller must coordinate with concurrently starting serving demand.
func (e *Evaluator) Reconcile(ctx context.Context, request Request, expectedUID types.UID, inFlight bool) (evictionplanner.Eligibility, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		model := &v1beta1.BaseModel{}
		if err := e.reader.Get(ctx, request.Model, model); err != nil {
			return protected("BaseModel read failed"), err
		}
		if model.UID == "" || model.ResourceVersion == "" {
			return protected("BaseModel identity is incomplete"), fmt.Errorf("BaseModel %s requires UID and resourceVersion", request.Model)
		}
		if expectedUID != "" && model.UID != expectedUID {
			return protected("BaseModel UID changed"), fmt.Errorf("BaseModel %s UID changed from %s to %s", request.Model, expectedUID, model.UID)
		}
		// History-only callers also pin the first observed UID across conflict retries.
		expectedUID = model.UID
		if reason := excluded(model, inFlight); reason != "" {
			return protected(reason), nil
		}
		active, err := e.references.HasReferences(ctx, request.Model)
		if err != nil {
			return protected("current serving references could not be established"), err
		}
		if active {
			return protected("serving demand exists"), nil
		}

		now := e.now().UTC()
		before, valid := parseHistory(model.Annotations[constants.ModelUsageHistoryAnnotation], now, model.CreationTimestamp.Time)
		after := before
		removedAt := request.ReferenceRemovedAt
		// An event from before this CR's lifetime (or in the future) cannot establish
		// its demand end. Replaying an older event never moves persisted time back.
		if !removedAt.IsZero() && !model.CreationTimestamp.IsZero() && !removedAt.Before(model.CreationTimestamp.Time) && !removedAt.After(now) {
			if after.LastObservedDemandEndedAt == nil || removedAt.After(*after.LastObservedDemandEndedAt) {
				end := removedAt.UTC()
				after.LastObservedDemandEndedAt = &end
			}
		}
		if after.LastObservedDemandEndedAt == nil && after.IdleSince == nil {
			after.IdleSince = &now
		}
		if !valid || !sameHistory(before, after) {
			if err := e.patchHistory(ctx, model, after); err != nil {
				if apierrors.IsConflict(err) {
					lastErr = err
					continue
				}
				return protected("usage history patch failed"), err
			}
		}
		// The planner's IdleSince carries the chosen recency reference. A confirmed
		// demand end always takes precedence over the initial-idle fallback.
		idleSince := after.LastObservedDemandEndedAt
		if idleSince == nil {
			idleSince = after.IdleSince
		}
		return evictionplanner.Eligibility{Eligible: true, IdleSince: idleSince}, nil
	}
	return protected("usage history conflicts exhausted retries"), lastErr
}

func protected(reason string) evictionplanner.Eligibility {
	return evictionplanner.Eligibility{Reason: reason}
}

func excluded(model *v1beta1.BaseModel, inFlight bool) string {
	switch {
	case !model.DeletionTimestamp.IsZero():
		return "BaseModel is deleting"
	case model.Spec.Storage != nil && model.Spec.Storage.StorageUri != nil && strings.HasPrefix(*model.Spec.Storage.StorageUri, "vendor://"):
		return "vendor model"
	case strings.EqualFold(model.Labels[constants.ReserveModelArtifact], "true"):
		return "model artifact is reserved"
	case model.Annotations["ome.io/artifact-residency"] == "Evicted":
		return "model artifact is evicted"
	case inFlight:
		return "eviction is in flight"
	}
	return ""
}

// parseHistory validates each field independently. Unknown fields do not cause
// writes, and valid history is left byte-for-byte intact during continued idle.
func parseHistory(raw string, now, created time.Time) (history, bool) {
	if raw == "" {
		return history{}, true
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil || fields == nil {
		return history{}, false
	}
	valid := true
	parse := func(name string) *time.Time {
		value, exists := fields[name]
		if !exists {
			return nil
		}
		var text string
		if err := json.Unmarshal(value, &text); err != nil {
			valid = false
			return nil
		}
		timestamp, err := time.Parse(time.RFC3339, text)
		if err != nil || timestamp.IsZero() || timestamp.After(now) || timestamp.Before(created) {
			valid = false
			return nil
		}
		timestamp = timestamp.UTC()
		return &timestamp
	}
	h := history{IdleSince: parse("idleSince"), LastObservedDemandEndedAt: parse("lastObservedDemandEndedAt")}
	return h, valid
}

func sameHistory(a, b history) bool {
	equal := func(a, b *time.Time) bool { return a == nil && b == nil || a != nil && b != nil && a.Equal(*b) }
	return equal(a.IdleSince, b.IdleSince) && equal(a.LastObservedDemandEndedAt, b.LastObservedDemandEndedAt)
}

func (e *Evaluator) patchHistory(ctx context.Context, model *v1beta1.BaseModel, h history) error {
	encoded, err := json.Marshal(h)
	if err != nil {
		return err
	}
	// UID and resourceVersion fence replacement and concurrent metadata changes.
	// The merge touches only this annotation; other annotations/spec/status stay.
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"uid":             model.UID,
			"resourceVersion": model.ResourceVersion,
			"annotations":     map[string]string{constants.ModelUsageHistoryAnnotation: string(encoded)},
		},
	})
	if err != nil {
		return err
	}
	return e.writer.Patch(ctx, model, client.RawPatch(types.MergePatchType, patch))
}
