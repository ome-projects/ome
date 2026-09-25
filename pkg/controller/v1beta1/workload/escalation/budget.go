// Per-Component rollout budget — within-Component MaxSurge /
// MaxUnavailable ceilings sourced from
// plan.UpdateStrategy.RollingUpdate. Composes orthogonally with the
// coordination-group ceilings the ISVC adapter wires into
// input.UpdateGate: both layers are independent capacities, the
// effective gate allows an Instance start only when BOTH layers do.
//
// Budgets here cap WITHIN-Component pod surge / drain, regardless of
// whether the Component participates in any coordination group: the
// per-Component LifecycleSpec.UpdateStrategy.RollingUpdate fields are
// copied by the converter onto workload.RollingUpdate and consumed by
// this file. Group-wide ceilings still apply through the UpdateGate
// callback in workload.Reconcile.
//
// Composition rule (documented in lifecycle_types.go on RollingUpdate.MaxSurge):
//
//	effective surge cap = min(group_max_surge, per_component_max_surge)
//	effective unavail cap = min(group_max_unavail, per_component_max_unavail)
//
// The two layers govern different things:
//   - per-Component RollingUpdate.MaxSurge — cap on extra pods the
//     Component may have alive temporarily during ONE rollout.
//   - CoordinationPacing.MaxSurge — cap on extra pods OMENative may
//     add across the whole coordination group (so two peer Components
//     rolling at once don't both surge to budget=4 each, totaling 8).
//
// Defaults: nil per-Component RollingUpdate.{MaxSurge,MaxUnavailable}
// → "no per-Component cap" (the coordination-group ceiling is the
// only constraint). The 25% / 25% defaults the API documents are
// applied by upstream defaulters (webhook) when the operator wants
// them; an unset value here is treated as "uncapped" so we don't
// silently impose a budget the operator didn't write.
package escalation

import (
	"sort"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils"
)

// BudgetNoLimit is returned by Per* budget helpers when the source
// RollingUpdate field is nil or unparsable — signals "no within-
// Component cap from this field" so the dispatcher's min(group, per-
// Component) composition collapses to the group-level cap.
const BudgetNoLimit = int32(-1)

// PerComponentMaxSurgeBudget resolves
// plan.UpdateStrategy.RollingUpdate.MaxSurge against the Component's
// replica count. Returns BudgetNoLimit when RollingUpdate is nil or
// MaxSurge is unset — the caller treats that as "this layer does not
// cap" and defers to the coordination-group layer (which has its own
// 25% default).
//
// Integer values are returned as-is (clamped to >= 0). Percent strings
// resolve to ceil(replicas * percent / 100) to match upstream
// appsv1.Deployment.Strategy.RollingUpdate.MaxSurge semantics and the
// group-wide MaxSurgeBudget helper. A 0% expression on any replica
// count returns 0 (no surge allowed via this layer); a 25% expression
// on 4 replicas returns 1 (one extra pod allowed).
func PerComponentMaxSurgeBudget(ru *types.RollingUpdate, replicas int32) int32 {
	if ru == nil || ru.MaxSurge == nil {
		return BudgetNoLimit
	}
	return utils.ScaledCountFromIntOrString(ru.MaxSurge, replicas, true)
}

// PerComponentMaxUnavailableBudget is the unavailability dual of
// PerComponentMaxSurgeBudget. nil RollingUpdate or nil MaxUnavailable
// returns BudgetNoLimit; the dispatcher falls through to the
// coordination-group ceiling. Integer values return as-is, percent
// strings resolve via ceil semantics so a "25%" budget on 4 replicas
// returns 1.
func PerComponentMaxUnavailableBudget(ru *types.RollingUpdate, replicas int32) int32 {
	if ru == nil || ru.MaxUnavailable == nil {
		return BudgetNoLimit
	}
	return utils.ScaledCountFromIntOrString(ru.MaxUnavailable, replicas, true)
}

// EffectivePartition returns the partition the update pass holds at.
// The projected Pacing.Partition wins when set — it is the rollout-
// control value the composer (a canary step, a plan-gate hold) owns,
// and an explicit 0 there releases every Instance even when the user's
// strategy carries a partition. Otherwise the user's
// RollingUpdate.Partition applies. nil when neither is set.
func EffectivePartition(pacing *types.WorkloadPacing, ru *types.RollingUpdate) *int32 {
	if pacing != nil && pacing.Partition != nil {
		return pacing.Partition
	}
	if ru == nil {
		return nil
	}
	return ru.Partition
}

// HeldByPartition reports whether the hold candidate at `rank` (its
// position in PartitionHeldIndices' ascending candidate order) is held
// by the effective partition (the StatefulSet-style canary): the first
// `partition` candidates are held. A nil partition or partition <= 0
// holds nothing.
func HeldByPartition(partition *int32, rank int32) bool {
	if partition == nil || *partition <= 0 {
		return false
	}
	return rank < *partition
}

// PartitionHeldIndices returns the set of Instance indices the
// effective partition holds on their current revision this pass.
// Partition counts Instances to hold, not index values — migration and
// lowest-unused surge allocation leave sparse index sets (e.g. {1,2}
// for replicas=2), so a raw index<Partition compare holds the wrong
// count. The hold is keyed to revision membership, not position in the
// index set: gang surge places TARGET-revision replacements at freed
// (often lower) indices, so a positional hold would let a replacement
// steal a held slot mid-roll and un-hold a still-old Instance — ending
// the roll with zero old-revision Instances and the desired staged
// shape (ReachedDesiredShape) permanently unsatisfiable.
//
// Hold candidates are planned Instances observed OFF the target
// revision (unset RunningRevision included — ReachedDesiredShape
// counts those as held too, and the two must agree on membership).
// Excluded from candidacy, so they are never held:
//   - unobserved Instances (nothing running to hold);
//   - Instances already converging to target — Phase=Updating and any
//     preserved Update operation (gang-surge target markers, Failed
//     continuations). An in-flight update must finish: holding it
//     strands its surge pod, which is never promoted to serving and
//     permanently consumes the surge budget, deadlocking the roll on
//     the non-held Instances. The hold falls to the next-lowest old-
//     revision Instance instead, preserving the count;
//   - transient migration surge Instances (Op.Type=Migrate on a
//     non-Migrating phase) — the pair's source carries the steady
//     capacity.
//
// The lowest-indexed `partition` candidates are held.
func PartitionHeldIndices(partition *int32, input types.ReconcileInput, planned []types.InstancePlan, targetRevName string) map[int32]bool {
	held := make(map[int32]bool)
	if partition == nil || *partition <= 0 {
		return held
	}
	tgt := query.RevisionFromName(targetRevName)
	candidates := make([]int32, 0, len(planned))
	for _, inst := range planned {
		s := input.ObservedState.Instance(inst.Index)
		if s == nil {
			continue
		}
		if query.RevisionFromName(s.RunningRevision).Same(tgt) {
			continue
		}
		if s.Phase == types.InstancePhaseUpdating {
			continue
		}
		if s.Operation != nil {
			if s.Operation.Type == types.InstanceOperationUpdate {
				continue
			}
			if s.Operation.Type == types.InstanceOperationMigrate && s.Phase != types.InstancePhaseMigrating {
				continue
			}
		}
		candidates = append(candidates, inst.Index)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })
	for i, idx := range candidates {
		if !HeldByPartition(partition, int32(i)) {
			break
		}
		held[idx] = true
	}
	return held
}

// CurrentSurgeInFlight counts InstanceStatuses participating in an
// in-flight SurgeThenDrain operation — those that already have an
// extra pod alive from a prior wake-up. The dispatcher consults this
// before deciding whether to start ONE MORE surge this wake-up. The
// step-set { "Surge", "SurgeDrain", "SurgeDrainSettle" } mirrors the
// coordination-group
// CheckSurge accounting in
// pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/ratio.go
// — both layers see the same in-flight signal so the budget math is
// consistent.
func CurrentSurgeInFlight(statuses []types.InstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		if s.Operation == nil {
			continue
		}
		// Surge contributes +1 pod alive; SurgeDrain does too (the
		// old pod hasn't been deleted yet). Other ops (Drain for
		// recreate, InPlace for in-place patch) don't add pods.
		switch s.Operation.Step {
		case "Surge", "SurgeDrain", "SurgeDrainSettle":
			n++
		}
	}
	return n
}

// CurrentUnavailableInFlight counts InstanceStatuses currently in an
// unavailable Update (non-surge step) — those whose pod is offline or being
// patched in place. A Failed Instance whose Update operation is preserved for
// retry still consumes the budget; otherwise the dispatcher can start another
// healthy Instance in the same wake-up and take the whole Component offline.
// Used as the prior-pass anchor for the per-Component MaxUnavailable budget
// check, similar to how coordination.CheckUnavailability counts
// (replicas - serving).
//
// We exclude surge lifecycle steps because those don't take pods
// offline (a new pod surges IN before the old one drains OUT). Other
// Updating steps (InPlace, Drain) DO take the pod offline, so they
// count.
func CurrentUnavailableInFlight(statuses []types.InstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		updating := s.Phase == types.InstancePhaseUpdating
		failedUpdate := s.Phase == types.InstancePhaseFailed && s.Operation != nil &&
			s.Operation.Type == types.InstanceOperationUpdate
		if !updating && !failedUpdate {
			continue
		}
		if s.Operation == nil {
			n++
			continue
		}
		switch s.Operation.Step {
		case "Surge", "SurgeDrain", "SurgeDrainSettle":
			// Surge doesn't take a pod offline.
			continue
		default:
			n++
		}
	}
	return n
}

// CurrentRestartingInFlight counts InstanceStatuses whose pod set is
// being rebuilt by a Restart from a prior wake-up. A Restarting
// Instance has its pods deleted and recreated, so it is unavailable
// capacity in exactly the sense the MaxUnavailable budget bounds, and
// it anchors the restart pass's projection the way
// CurrentUnavailableInFlight anchors the update pass's.
//
// Phase only, deliberately: a Failed row still holding its Restart
// operation is counted by neither anchor, so a repair that failed
// releases the budget while its Instance stays down. The RetryBlock
// against the revision is what bounds repeated failures there. The
// update arm differs: it does charge a Failed row's preserved Update.
func CurrentRestartingInFlight(statuses []types.InstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		if s.Phase == types.InstancePhaseRestarting {
			n++
		}
	}
	return n
}

// budgetDenies projects ONE more fresh start against a per-Component
// budget layer and reports the projected total with the verdict. prior
// is the in-flight count carried from earlier wake-ups, counted once;
// inFlight is what this pass has already admitted. An uncapped layer
// never denies.
func BudgetDenies(budget, prior, inFlight int32) (int32, bool) {
	if budget == BudgetNoLimit {
		return 0, false
	}
	projected := prior + inFlight + 1
	return projected, projected > budget
}
