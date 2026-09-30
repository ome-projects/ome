package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/alfred/placement"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// Version the value, not the journal schema: older binaries can still reconcile
// submitted entries but will fail closed when retrying a new prepared entry.
const semanticFingerprintPrefix = "semantic-v1:"

type dispatchFingerprintVersion uint8

const (
	dispatchFingerprintLegacy dispatchFingerprintVersion = iota
	dispatchFingerprintSemantic
)

// dispatchSemanticSourceFingerprint fences the original request's meaning across retry
// attempts. Fresh policy, observation, simulation and owner CAS remain required
// for each attempt; this hash is neither authority nor a consumer-side fence.
// Row and pods are private normalized copies supplied by dispatchSourceFingerprint.
func dispatchSemanticSourceFingerprint(owner *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, row *v1beta1.OMENativeInstanceStatus, pods []corev1.Pod) (string, error) {
	if err := placement.CheckNewSurge(owner, ir); err != nil {
		return "", err
	}
	service, replica := owner.DeepCopy(), ir.DeepCopy()
	normalizeSourceMetadata(&service.ObjectMeta)
	normalizeSourceMetadata(&replica.ObjectMeta)
	// Generation alone does not describe a spec change: a valid placement
	// pause/release advances it without changing the request's source. Preserve
	// complete specs instead, normalizing only the validated authority revision.
	if replica.Spec.PlacementExecution != nil {
		replica.Spec.PlacementExecution.Revision = 0
	}
	authority, err := protocol.FromDerived(service)
	if err != nil {
		return "", err
	}
	if authority != nil {
		if err := normalizeSourcePlacementAnnotation(service.Annotations, authority); err != nil {
			return "", err
		}
		// ISVC annotations are also present in the public runner templates.
		// Normalize only a copy of this same authority, not arbitrary template
		// metadata or controller-defined revision exclusions.
		for i := range replica.Spec.Runners {
			if err := normalizeSourcePlacementAnnotation(replica.Spec.Runners[i].Template.Annotations, authority); err != nil {
				return "", err
			}
		}
	}
	// Event delivery stamps and previous-failure diagnostics do not change the
	// live source. Pod-derived observations were already cleared by the caller.
	row.Announced, row.LastFailure = nil, nil
	for i := range row.Conditions {
		condition := &row.Conditions[i]
		condition.Reason, condition.Message = "", ""
		condition.ObservedGeneration = 0
		condition.LastTransitionTime = metav1.Time{}
	}
	// Conditions are a map by type, including types unknown to this binary.
	// Retain every type/status, and retain lifecycle ReadySince and Operation.
	sort.Slice(row.Conditions, func(i, j int) bool { return row.Conditions[i].Type < row.Conditions[j].Type })
	raw, err := json.Marshal(struct {
		OwnerMetadata  metav1.ObjectMeta
		OwnerSpec      v1beta1.InferenceServiceSpec
		IRMetadata     metav1.ObjectMeta
		IRSpec         v1beta1.InferenceReplicaSpec
		Revision       string
		UpdateRevision string
		Row            *v1beta1.OMENativeInstanceStatus
		Pods           []corev1.Pod
	}{service.ObjectMeta, service.Spec, replica.ObjectMeta, replica.Spec, replica.Status.CurrentRevision, replica.Status.UpdateRevision, row, pods})
	if err != nil {
		return "", fmt.Errorf("source fingerprint: %w", err)
	}
	sum := sha256.Sum256(raw)
	return semanticFingerprintPrefix + hex.EncodeToString(sum[:]), nil
}

func normalizeSourceMetadata(meta *metav1.ObjectMeta) {
	meta.Generation, meta.ResourceVersion, meta.ManagedFields = 0, "", nil
}

func normalizeSourcePlacementAnnotation(annotations map[string]string, authority *v1beta1.PlacementExecutionPolicy) error {
	raw, exists := annotations[constants.PlacementExecution]
	if !exists {
		return nil
	}
	policy, err := protocol.Decode(raw)
	if err != nil || protocol.Validate(policy) != nil {
		// A template override or an unreadable value is not placement
		// authority. Keep it byte-exact.
		return nil
	}
	policy.Revision = authority.Revision
	if !equality.Semantic.DeepEqual(*policy, *authority) {
		return nil
	}
	// Only the validated revision changes across retries. Clear it in place,
	// inside the versioned envelope or on the bare policy, so every retry of
	// the same request fences on the same value.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return fmt.Errorf("source placement fingerprint: %w", err)
	}
	if inner, ok := fields["policy"]; ok {
		var policyFields map[string]json.RawMessage
		if err := json.Unmarshal(inner, &policyFields); err != nil {
			return fmt.Errorf("source placement fingerprint: %w", err)
		}
		policyFields["revision"] = json.RawMessage("0")
		normalizedPolicy, err := json.Marshal(policyFields)
		if err != nil {
			return fmt.Errorf("source placement fingerprint: %w", err)
		}
		fields["policy"] = normalizedPolicy
	} else {
		fields["revision"] = json.RawMessage("0")
	}
	normalized, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("source placement fingerprint: %w", err)
	}
	annotations[constants.PlacementExecution] = string(normalized)
	return nil
}
