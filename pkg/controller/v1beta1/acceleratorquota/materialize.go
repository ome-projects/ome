package acceleratorquota

import (
	"context"
	"fmt"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/backend"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

// MaterializeOptions configures rendering the tree into an enforcement backend.
// A nil Backend leaves the controller reporting tree position only, which is
// what a management-mode manager wants: it holds the authored fleet tree and
// enforces nothing locally.
type MaterializeOptions struct {
	Backend backend.Backend
}

// Enabled reports whether this manager materializes anything.
func (o MaterializeOptions) Enabled() bool { return o.Backend != nil }

// partitionByDeletion splits the listed nodes into those still being
// reconciled and those on their way out.
//
// A node under deletion is not part of the tree it is leaving. Building it in
// would re-render the very objects the finalizer is about to reap, in the same
// pass, and the reap would then look like it had failed.
func partitionByDeletion(items []v1beta1.AcceleratorQuota) (live, deleting []v1beta1.AcceleratorQuota) {
	for i := range items {
		if items[i].DeletionTimestamp.IsZero() {
			live = append(live, items[i])
			continue
		}
		deleting = append(deleting, items[i])
	}
	return live, deleting
}

// ensureFinalizer claims the node so its objects can be reaped on deletion.
//
// Without it, deleting the CR would leave its ClusterQueue and LocalQueues
// behind, still admitting workloads against a budget nobody can see any more.
func (r *Reconciler) ensureFinalizer(ctx context.Context, quota *v1beta1.AcceleratorQuota) error {
	if controllerutil.ContainsFinalizer(quota, v1beta1.AcceleratorQuotaFinalizer) {
		return nil
	}
	return r.patchFinalizer(ctx, quota, controllerutil.AddFinalizer)
}

// patchFinalizer writes the node with its finalizer added or removed. A node
// deleted or rewritten mid-pass is left to the next reconcile, which reads the
// newer object.
func (r *Reconciler) patchFinalizer(ctx context.Context, quota *v1beta1.AcceleratorQuota, change func(client.Object, string) bool) error {
	updated := quota.DeepCopy()
	change(updated, v1beta1.AcceleratorQuotaFinalizer)
	if err := r.Patch(ctx, updated, client.MergeFrom(quota)); err != nil && !goneOrRewritten(err) {
		return err
	}
	return nil
}

// finalize reaps a deleted node's objects and releases the finalizer.
//
// Deletion is the one path that removes enforcement objects. A freeze holds
// them; only an explicit delete of the CR takes them away, so an operator who
// wants a tenant's quota gone has exactly one way to say so.
func (r *Reconciler) finalize(ctx context.Context, quota *v1beta1.AcceleratorQuota, keep sets.Set[string]) error {
	if !controllerutil.ContainsFinalizer(quota, v1beta1.AcceleratorQuotaFinalizer) {
		return nil
	}
	if r.Materialize.Enabled() {
		sweeper, ok := r.Materialize.Backend.(interface {
			Sweep(context.Context, sets.Set[string]) (int, error)
		})
		if ok {
			swept, err := sweeper.Sweep(ctx, keep)
			recordSwept(SweepTriggerFinalize, swept)
			if err != nil {
				return fmt.Errorf("reaping objects for %s: %w", quota.Name, err)
			}
		}
	}

	if r.Project.Enabled() {
		if err := r.reapProjections(ctx, quota.Name); err != nil {
			return fmt.Errorf("reaping projections for %s: %w", quota.Name, err)
		}
	}

	// The node's enforcement objects are gone; its accelerator figures describe
	// nothing now, and a gauge left behind would keep reporting a deleted
	// tenant's allowance.
	deleteQuotaSeries(quota.Name)

	return r.patchFinalizer(ctx, quota, controllerutil.RemoveFinalizer)
}

// materialize renders the reachable, unfrozen part of the tree and applies it,
// then sweeps whatever no longer belongs.
//
// Frozen nodes are retained rather than written: their last-good objects stay
// exactly as they are. That is the whole freeze rule — a misauthored parent
// must not take its children's admitted workloads down with it.
func (r *Reconciler) materialize(ctx context.Context, built *tree.Tree, frozen map[string]tree.Violation) (map[string]backend.NodeOutcome, error) {
	plan := backend.Plan{Retain: sets.New[string]()}
	keep := sets.New[string]()

	for _, node := range built.Nodes() {
		keep.Insert(node.Name())
		if _, isFrozen := frozen[node.Name()]; isFrozen {
			plan.Retain.Insert(node.Name())
			continue
		}
		if !node.Reachable() {
			// An unreachable node names a parent that is not in the tree. It has
			// no place to hang, so there is nothing coherent to write; it keeps
			// whatever it had and reports Degraded through the tree checks.
			plan.Retain.Insert(node.Name())
			continue
		}
		plan.Write = append(plan.Write, node)
	}

	result, err := r.Materialize.Backend.Materialize(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("materializing the quota tree: %w", err)
	}
	recordApplied(result.Applied)

	if sweeper, ok := r.Materialize.Backend.(interface {
		Sweep(context.Context, sets.Set[string]) (int, error)
	}); ok {
		swept, err := sweeper.Sweep(ctx, keep)
		recordSwept(SweepTriggerMaterialize, swept)
		if err != nil {
			return result.Nodes, fmt.Errorf("sweeping orphaned objects: %w", err)
		}
	}
	return result.Nodes, nil
}

// setMaterializationStatus stamps the Materialized condition and the freeze
// bookkeeping that makes last-good durable across a restart.
//
// The bookkeeping is on status rather than inferred, because after a restart
// the controller cannot otherwise tell "frozen, holding output from generation
// 4" from "never materialized at all", and those call for opposite actions.
func setMaterializationStatus(status *v1beta1.AcceleratorQuotaStatus, generation int64, outcome backend.NodeOutcome, isFrozen bool, now metav1.Time) {
	condition := metav1.Condition{
		Type:               v1beta1.AcceleratorQuotaMaterialized,
		Status:             metav1.ConditionTrue,
		Reason:             v1beta1.AcceleratorQuotaReasonAdmitted,
		Message:            "enforcement objects match this node's budget",
		LastTransitionTime: now,
		ObservedGeneration: generation,
	}

	switch {
	case isFrozen:
		condition.Status = metav1.ConditionFalse
		condition.Reason = v1beta1.AcceleratorQuotaReasonFrozen
		condition.Message = "output is held at its last-good state; no objects were written or removed"
		if status.Materialization == nil {
			status.Materialization = &v1beta1.AcceleratorQuotaMaterialization{}
		}
		if !status.Materialization.Frozen {
			status.Materialization.Frozen = true
			status.Materialization.FrozenAt = &now
		}
		status.Materialization.Reason = condition.Reason

	case outcome.Reason != "":
		condition.Status = metav1.ConditionFalse
		condition.Reason = outcome.Reason
		condition.Message = outcome.Message

	default:
		if status.Materialization == nil {
			status.Materialization = &v1beta1.AcceleratorQuotaMaterialization{}
		}
		status.Materialization.Frozen = false
		status.Materialization.FrozenAt = nil
		status.Materialization.Reason = ""
		status.Materialization.LastAppliedGeneration = generation
		status.Materialization.LastAppliedTime = &now
	}

	apimeta.SetStatusCondition(&status.Conditions, condition)
}

// equalMaterialization compares the durable freeze bookkeeping, excluding
// LastAppliedTime. Including it would make every pass a write, since a clean
// re-apply restamps the time whether or not anything moved.
func equalMaterialization(a, b *v1beta1.AcceleratorQuotaMaterialization) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Frozen == b.Frozen &&
		a.Reason == b.Reason &&
		a.LastAppliedGeneration == b.LastAppliedGeneration
}
