package coordination

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
)

// ReplicaRollTargetHash is the hash of the revision a replica rolls its
// Instances onto: the rollback target it carries while that
// ControllerRevision exists and belongs to the replica's history (one it
// controls, or one its service owns), else the update revision its status
// reports, else its current one. The replica controller creates a peer's
// per-revision Services for this revision before any pod of it exists, and
// the sweep of a Component's per-revision Services keeps this revision for
// the same reason, so the two writers never disagree about it.
func ReplicaRollTargetHash(ctx context.Context, reads client.Reader, ir *v1beta1.InferenceReplica) (string, error) {
	if ir == nil {
		return "", nil
	}
	if name := rollbackTargetName(ir); name != "" {
		cr := &appsv1.ControllerRevision{}
		switch err := reads.Get(ctx, types.NamespacedName{Namespace: ir.Namespace, Name: name}, cr); {
		case err == nil && revision.RetainedBy(cr, ir.UID, irprojector.HistoryScope(ir)):
			return query.RevisionFromName(name).Hash(), nil
		case err == nil || apierrors.IsNotFound(err):
			// A target outside the replica's history, or gone, is read as
			// none: the replica rolls toward its spec target instead.
		default:
			return "", fmt.Errorf("get %s rollback revision %s: %w", ir.Spec.Component, name, err)
		}
	}
	if name := ir.Status.UpdateRevision; name != "" {
		return query.RevisionFromName(name).Hash(), nil
	}
	if name := ir.Status.CurrentRevision; name != "" {
		return query.RevisionFromName(name).Hash(), nil
	}
	return "", nil
}

// rollbackTargetName is the ControllerRevision a replica's pacing points its
// roll at, "" when none is set.
func rollbackTargetName(ir *v1beta1.InferenceReplica) string {
	if ir.Spec.Pacing == nil || ir.Spec.Pacing.RollbackToRevision == nil {
		return ""
	}
	return *ir.Spec.Pacing.RollbackToRevision
}

// retainedRevisionHashes is the revision-hash set the sweep keeps for one
// Component: every hash with a live pod plus target, the hash the
// Component's replica rolls toward. That one is kept with zero pods because
// a peer Component creates its per-revision Services ahead of the pods that
// name them. Returns liveHashes as-is when the replica reports no target.
func retainedRevisionHashes(target string, liveHashes map[string]int32) map[string]int32 {
	if target == "" {
		return liveHashes
	}
	if _, live := liveHashes[target]; live {
		return liveHashes
	}
	retained := make(map[string]int32, len(liveHashes)+1)
	for h, n := range liveHashes {
		retained[h] = n
	}
	retained[target] = 0
	return retained
}

// SweepPerRevisionServices deletes the per-revision Service pairs of one
// Component whose revision has no live pod (liveHashes is the per-revision
// pod-count map) and is not the revision the Component's replica rolls
// toward. Every writer of per-revision Services prunes through this one
// rule: coordination for the Components it owns, the canary executor for
// the members of a canary group. The replica reads through reads, the
// Services are listed and deleted through c. A read error propagates: a
// sweep that guessed an empty target would delete the Services a peer's
// pods are about to name.
func SweepPerRevisionServices(ctx context.Context, c client.Client, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, liveHashes map[string]int32) error {
	if c == nil {
		return nil
	}
	ir, err := irprojector.ComponentIRFor(ctx, reads, isvc, component)
	if err != nil {
		return fmt.Errorf("resolve retained revisions for %s: %w", component, err)
	}
	return SweepPerRevisionServicesOfReplica(ctx, c, reads, isvc, component, ir, liveHashes)
}

// SweepPerRevisionServicesOfReplica is SweepPerRevisionServices for a
// caller that holds the Component's replica as it read it (nil when the
// Component has none), so a pass that read the replica for its own reading
// does not read it again; the ControllerRevision a rollback target names is
// still read through reads.
func SweepPerRevisionServicesOfReplica(ctx context.Context, c client.Client, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, replica *v1beta1.InferenceReplica, liveHashes map[string]int32) error {
	if c == nil {
		return nil
	}
	target, err := ReplicaRollTargetHash(ctx, reads, replica)
	if err != nil {
		return fmt.Errorf("resolve retained revisions for %s: %w", component, err)
	}
	return gcOrphanedPerRevisionServices(ctx, c, isvc, component, retainedRevisionHashes(target, liveHashes))
}
