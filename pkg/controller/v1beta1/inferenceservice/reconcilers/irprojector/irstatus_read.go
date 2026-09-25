package irprojector

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// ComponentIR returns the authoritative InferenceReplica for one Component of
// an ISVC. Returns (nil, nil) when the IR does not exist yet. Any other read
// failure is returned as a wrapped error so safety gates can distinguish a
// missing observation from an unreliable read.
func ComponentIR(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (*v1beta1.InferenceReplica, error) {
	// A nil reader degrades to "no authoritative status" rather than
	// panicking a reconcile — same result callers get for a missing IR.
	if reads == nil {
		return nil, nil
	}
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: namespace, Name: InferenceReplicaName(isvcName, c)}
	if err := reads.Get(ctx, key, ir); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get InferenceReplica %s/%s: %w", key.Namespace, key.Name, err)
	}
	return ir, nil
}

// EffectivePartition returns the partition an InferenceReplica spec holds
// Instances at — the same source order the workload engine uses, so every
// reader agrees with the plan. spec.pacing.partition wins when set: it is
// the rollout-control value the ISVC controller projects for a canary step
// or plan-gate hold, and an explicit 0 there releases every Instance even
// over a user partition. Otherwise the user's
// spec.lifecycle.updateStrategy.rollingUpdate.partition applies. nil when
// neither is set.
func EffectivePartition(lifecycle *v1beta1.LifecycleSpec, pacing *v1beta1.InferenceReplicaPacing) *int32 {
	if pacing != nil && pacing.Partition != nil {
		return pacing.Partition
	}
	if lifecycle == nil || lifecycle.UpdateStrategy == nil || lifecycle.UpdateStrategy.RollingUpdate == nil {
		return nil
	}
	return lifecycle.UpdateStrategy.RollingUpdate.Partition
}

// IRPartition returns the effective partition carried on a projected
// InferenceReplica spec (see EffectivePartition), or 0 (the API-defined
// "update every Instance" value) when the IR is nil or no partition is
// set. Shared by ComponentIRPartition and callers that already hold the
// IR object.
func IRPartition(ir *v1beta1.InferenceReplica) int32 {
	if ir == nil {
		return 0
	}
	p := EffectivePartition(ir.Spec.Lifecycle, ir.Spec.Pacing)
	if p == nil {
		return 0
	}
	return *p
}

// ComponentIRStatus returns the authoritative InferenceReplica status for one
// Component of an ISVC, read via the supplied reader. The full-object reader
// owns missing and error semantics so status-only consumers share the same
// behavior as consumers that also need desired spec state.
func ComponentIRStatus(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (*v1beta1.InferenceReplicaStatus, error) {
	ir, err := ComponentIR(ctx, reads, namespace, isvcName, c)
	if err != nil || ir == nil {
		return nil, err
	}
	return &ir.Status, nil
}

// DecodedComponentIR is the decoded-accessor form of ComponentIR for callers
// that inspect per-Instance rows: the object is returned in the dense
// logical shape, decoded with the Decoder carried by reads, together with
// the encoding it was stored in. A malformed or unbounded ColumnarV2 payload
// is an error, never an empty row set. ComponentIR and ComponentIRStatus stay
// raw for callers that read only spec, metadata, or top-level status.
func DecodedComponentIR(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (*v1beta1.InferenceReplica, irstatus.Encoding, error) {
	if reads == nil {
		return nil, "", nil
	}
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: namespace, Name: InferenceReplicaName(isvcName, c)}
	encoding, err := irstatus.GetDecoded(ctx, reads, key, ir)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("get InferenceReplica %s/%s: %w", key.Namespace, key.Name, err)
	}
	return ir, encoding, nil
}

// DecodedComponentIRStatus returns the decoded status of one Component's
// InferenceReplica, or nil when the IR does not exist yet.
func DecodedComponentIRStatus(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (*v1beta1.InferenceReplicaStatus, error) {
	ir, _, err := DecodedComponentIR(ctx, reads, namespace, isvcName, c)
	if err != nil || ir == nil {
		return nil, err
	}
	return &ir.Status, nil
}

// ComponentIRPartition returns the effective partition for one Component
// of an ISVC, read from the projected InferenceReplica spec (the projected
// spec.pacing.partition, else the user's
// spec.lifecycle.updateStrategy.rollingUpdate.partition). The IR spec
// carries the merged ISVC↔runtime lifecycle, so this is the partition the
// workload controller actually stages Instances at — including a partition
// inherited from the ServingRuntime, which the raw ISVC spec never shows.
// Coordination MUST read this value rather than re-deriving it from the
// unmerged ISVC: a raw-spec read reports partition 0 for a runtime-staged
// Component and treats its held Instances as an incomplete rollout forever.
//
// Returns 0 when the IR does not exist yet or no partition is set (the
// API-defined "update every Instance" value). Any other read failure is
// returned as a wrapped error so safety gates can fail closed instead of
// mistaking a flaky read for "no partition".
func ComponentIRPartition(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (int32, error) {
	ir, err := ComponentIR(ctx, reads, namespace, isvcName, c)
	if err != nil {
		return 0, err
	}
	return IRPartition(ir), nil
}
