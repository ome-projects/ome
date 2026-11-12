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

// ComponentIR returns the InferenceReplica an ISVC projects for one
// Component, by its projected name. Returns (nil, nil) when the IR does not
// exist yet. Any other read failure is returned as a wrapped error so safety
// gates can distinguish a missing observation from an unreliable read.
func ComponentIR(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (*v1beta1.InferenceReplica, error) {
	return componentIRAt(ctx, reads, types.NamespacedName{Namespace: namespace, Name: InferenceReplicaName(isvcName, c)})
}

// ComponentIRFor is ComponentIR for whichever replica serves role c on the
// service, projected or referenced. Returns (nil, nil) for a nil service.
func ComponentIRFor(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType) (*v1beta1.InferenceReplica, error) {
	if isvc == nil {
		return nil, nil
	}
	return componentIRAt(ctx, reads, RoleReplicaKey(isvc, c))
}

// componentIRAt reads the InferenceReplica at key. (nil, nil) when it does
// not exist; any other read failure is wrapped with the key.
func componentIRAt(ctx context.Context, reads client.Reader, key types.NamespacedName) (*v1beta1.InferenceReplica, error) {
	// A nil reader degrades to "no authoritative status" rather than
	// panicking a reconcile — same result callers get for a missing IR.
	if reads == nil {
		return nil, nil
	}
	ir := &v1beta1.InferenceReplica{}
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

// IRPartition returns the effective partition carried on an
// InferenceReplica spec (see EffectivePartition), or 0 (the API-defined
// "update every Instance" value) when the IR is nil or no partition is
// set. Shared by ComponentIRPartitionFor and callers that already hold the
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

// ComponentIRStatusFor returns the status of the InferenceReplica serving
// role c on the service, projected or referenced, read via the supplied
// reader. The full-object reader owns the missing and error semantics, so a
// status-only consumer behaves like one that also needs the spec.
func ComponentIRStatusFor(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType) (*v1beta1.InferenceReplicaStatus, error) {
	return statusOf(ComponentIRFor(ctx, reads, isvc, c))
}

// statusOf projects a full-object read onto its status, preserving the
// (nil, nil) missing case and the error case.
func statusOf(ir *v1beta1.InferenceReplica, err error) (*v1beta1.InferenceReplicaStatus, error) {
	if err != nil || ir == nil {
		return nil, err
	}
	return &ir.Status, nil
}

// DecodedComponentIR is the decoded-accessor form of ComponentIR for callers
// that inspect per-Instance rows: the object is returned in the dense
// logical shape, decoded with the Decoder carried by reads, together with
// the encoding it was stored in. A malformed or unbounded ColumnarV2 payload
// is an error, never an empty row set. ComponentIR and ComponentIRStatusFor
// stay raw for callers that read only spec, metadata, or top-level status.
func DecodedComponentIR(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (*v1beta1.InferenceReplica, irstatus.Encoding, error) {
	return decodedComponentIRAt(ctx, reads, types.NamespacedName{Namespace: namespace, Name: InferenceReplicaName(isvcName, c)})
}

// DecodedComponentIRFor is DecodedComponentIR for whichever replica serves
// role c on the service, projected or referenced. Returns (nil, "", nil)
// for a nil service.
func DecodedComponentIRFor(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType) (*v1beta1.InferenceReplica, irstatus.Encoding, error) {
	if isvc == nil {
		return nil, "", nil
	}
	return decodedComponentIRAt(ctx, reads, RoleReplicaKey(isvc, c))
}

// decodedComponentIRAt reads and decodes the InferenceReplica at key.
// (nil, "", nil) when it does not exist; any other read or decode failure
// is wrapped with the key.
func decodedComponentIRAt(ctx context.Context, reads client.Reader, key types.NamespacedName) (*v1beta1.InferenceReplica, irstatus.Encoding, error) {
	if reads == nil {
		return nil, "", nil
	}
	ir := &v1beta1.InferenceReplica{}
	encoding, err := irstatus.GetDecoded(ctx, reads, key, ir)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("get InferenceReplica %s/%s: %w", key.Namespace, key.Name, err)
	}
	return ir, encoding, nil
}

// ComponentIRPartitionFor returns the effective partition of the
// InferenceReplica serving role c on the service, projected or referenced,
// read from the replica spec (the projected spec.pacing.partition, else the
// user's spec.lifecycle.updateStrategy.rollingUpdate.partition). The IR spec
// carries the merged ISVC and runtime lifecycle, so this is the partition the
// workload controller holds Instances at, a partition inherited from the
// ServingRuntime included, which the raw ISVC spec never shows. Coordination
// reads this value rather than re-deriving it from the unmerged ISVC: a
// raw-spec read reports partition 0 for a runtime-staged Component and treats
// its held Instances as an incomplete rollout forever.
//
// Returns 0 when the IR does not exist yet or no partition is set (the
// API-defined "update every Instance" value). Any other read failure is
// returned as a wrapped error so safety gates can fail closed instead of
// mistaking a flaky read for "no partition".
func ComponentIRPartitionFor(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType) (int32, error) {
	return partitionOf(ComponentIRFor(ctx, reads, isvc, c))
}

// partitionOf projects a full-object read onto its effective partition,
// propagating a read error and mapping a missing IR to 0.
func partitionOf(ir *v1beta1.InferenceReplica, err error) (int32, error) {
	if err != nil {
		return 0, err
	}
	return IRPartition(ir), nil
}

// RetryBlockFor reads a replica status's retry record for revision,
// whatever its state; false when the revision carries none.
func RetryBlockFor(status *v1beta1.InferenceReplicaStatus, revision string) (v1beta1.RetryBlock, bool) {
	if status == nil || revision == "" {
		return v1beta1.RetryBlock{}, false
	}
	for i := range status.RetryBlocks {
		if b := &status.RetryBlocks[i]; b.TargetRevision == revision {
			return *b.DeepCopy(), true
		}
	}
	return v1beta1.RetryBlock{}, false
}

// HeldRetryBlock reads a replica status's retry record for revision when
// that record is Held: the attempts at the revision are spent and no fresh
// start opens on it until a new revision or an operator release.
func HeldRetryBlock(status *v1beta1.InferenceReplicaStatus, revision string) (v1beta1.RetryBlock, bool) {
	block, ok := RetryBlockFor(status, revision)
	if !ok || block.State != v1beta1.RetryBlockHeld {
		return v1beta1.RetryBlock{}, false
	}
	return block, true
}

// HeldUpdateReason is the hold text a Held retry record reads as; every
// reader that names the record uses these words.
func HeldUpdateReason(b v1beta1.RetryBlock) string {
	return fmt.Sprintf("update to %s held after %d failed attempt(s): %s", b.TargetRevision, b.AttemptsStarted, b.Reason)
}
