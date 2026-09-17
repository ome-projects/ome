package mutate

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

const runtimeSyncTokenPrefix = "cli-runtime-sync-"

type RuntimeSyncPlan struct {
	patch                  []byte
	target                 reportv1alpha1.ActionTarget
	requestID, token, hash string
	rows                   [][2]string
	valid                  bool
}

func (RuntimeSyncPlan) MarshalJSON() ([]byte, error) { return nil, ErrRuntime }
func (RuntimeSyncPlan) MarshalYAML() (any, error)    { return nil, ErrRuntime }
func (RuntimeSyncPlan) String() string               { return "<mutate.RuntimeSyncPlan redacted>" }
func (RuntimeSyncPlan) GoString() string             { return "<mutate.RuntimeSyncPlan redacted>" }
func (p RuntimeSyncPlan) Patch() []byte              { return append([]byte{}, p.patch...) }

// ResponseHasToken confirms that the API returned the annotation requested by
// this exact plan. Identity alone cannot prove that admission kept the token.
func (p RuntimeSyncPlan) ResponseHasToken(annotations map[string]string) bool {
	return p.valid && annotations[constants.RuntimeSyncAnnotationKey] == p.token
}

// NewRuntimeSyncRequestID uses bounded random UUID attempts, never time or user input.
func NewRuntimeSyncRequestID(e effective.RuntimeSyncEvidence, entropy io.Reader) (string, error) {
	if entropy == nil {
		return "", ErrRuntime
	}
	for range 4 {
		var raw [16]byte
		if _, err := io.ReadFull(entropy, raw[:]); err != nil {
			return "", errors.New("generate private runtime sync request identity failed")
		}
		raw[6] = (raw[6] & 15) | 64
		raw[8] = (raw[8] & 63) | 128
		uuid := fmt.Sprintf("%x-%x-%x-%x-%x", raw[:4], raw[4:6], raw[6:8], raw[8:10], raw[10:])
		if e.TokenAvailable(runtimeSyncTokenPrefix + uuid) {
			return uuid, nil
		}
	}
	return "", errors.New("generate unique runtime sync request identity failed")
}

func PrepareRuntimeSync(v *v1beta1.InferenceService, e effective.RuntimeSyncEvidence, work RuntimeSyncReplicaEvidence, uuid string, clock reportv1alpha1.Clock) (RuntimeSyncPlan, error) {
	if err := ValidateTarget(v); err != nil {
		return RuntimeSyncPlan{}, err
	}
	if !e.MatchesInferenceService(v) || !work.valid || work.uid != string(v.UID) || work.rv != v.ResourceVersion || !slices.Equal(e.NativeComponents(), work.components) || !requestUUID.MatchString(uuid) || !e.TokenAvailable(runtimeSyncTokenPrefix+uuid) {
		return RuntimeSyncPlan{}, ErrRuntime
	}
	for _, key := range []string{constants.RolloutPromoteAnnotation, constants.RolloutRollbackAnnotation, constants.RolloutRepinAnnotation} {
		if _, present := v.Annotations[key]; present {
			return RuntimeSyncPlan{}, ErrPending
		}
	}
	if v.Status.Rollout != nil && (v.Status.Rollout.ActiveRun != nil || v.Status.Rollout.LastRun != nil && v.Status.Rollout.LastRun.Outcome == v1beta1.RolloutRunRolledBack) || v.Status.Canary != nil && v.Status.Canary.RolledBackRevisionHash != "" {
		return RuntimeSyncPlan{}, ErrStale
	}
	rows := e.PreviewRows()
	for _, row := range rows {
		for _, value := range row {
			if strings.ContainsAny(value, "\r\n\t") || len(value) > 768 {
				return RuntimeSyncPlan{}, ErrUnsafeValue
			}
		}
	}
	// Exact scalar identities are refused, never silently redacted/truncated.
	for _, row := range rows {
		if row[0] == "Freshness" {
			continue
		}
		if row[1] != "" && !SafeScalar(row[1]) {
			return RuntimeSyncPlan{}, ErrUnsafeValue
		}
	}
	token := runtimeSyncTokenPrefix + uuid
	patch := []patchOperation{{Op: "test", Path: "/metadata/uid", Value: string(v.UID)}, {Op: "test", Path: "/metadata/resourceVersion", Value: v.ResourceVersion}}
	path := "/metadata/annotations/ome.io~1runtime-sync"
	if old, present := v.Annotations[constants.RuntimeSyncAnnotationKey]; present {
		patch = append(patch, patchOperation{Op: "test", Path: path, Value: old})
	}
	if v.Annotations == nil {
		patch = append(patch, patchOperation{Op: "add", Path: "/metadata/annotations", Value: map[string]string{}})
	}
	patch = append(patch, patchOperation{Op: "add", Path: path, Value: token})
	raw, err := json.Marshal(patch)
	if err != nil {
		return RuntimeSyncPlan{}, ErrRuntime
	}
	paused, freeze := constants.RolloutPauseState(v.Annotations)
	pause := "Not recognized"
	if paused {
		pause = "Paused"
	}
	if freeze {
		pause = "Freeze"
	}
	rows = append(rows, [2]string{"Pause state", pause})
	return RuntimeSyncPlan{valid: true, patch: raw, target: reportv1alpha1.ActionTarget{Kind: "InferenceService", Name: v.Name, Namespace: v.Namespace, UID: string(v.UID), ResourceVersion: v.ResourceVersion}, requestID: uuid, token: token, hash: e.TargetHash(), rows: rows}, nil
}

func (p RuntimeSyncPlan) Result(mode reportv1alpha1.DryRunMode, clock reportv1alpha1.Clock) reportv1alpha1.ActionResult {
	if !p.valid {
		return reportv1alpha1.ActionResult{}
	}
	result := reportv1alpha1.NewActionResult("runtime sync", p.target, mode, clock)
	result.RequestID = p.requestID
	result.RevisionHash = p.hash
	result.Message = "Validated locally; no patch sent."
	return result
}
