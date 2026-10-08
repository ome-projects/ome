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
	"time"

	corev1 "k8s.io/api/core/v1"

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
		// A parked attempt holds no slot in either phase: the ownership
		// table reads it as settled and claiming nothing, so the next attempt on its
		// Instance is a fresh start; a Failed continuation still claims one.
		updating := s.Phase == types.InstancePhaseUpdating && types.StateOf(&s) != types.StateUpdateParked
		failedUpdate := s.Phase == types.InstancePhaseFailed && types.ClaimOf(&s) == types.OwnerUpdate
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

// ExtraPodSurge is what a Component's extra pods hold against its surge
// budget: every pod the API lists for an Instance beyond the Instance's
// own pod count, whether the pod is Terminating inside its deletion grace
// or a replacement a disposed attempt left alive. The Component never
// carries more than replicas plus maxSurge live pods, so a slot such a
// pod holds is not a slot a fresh start may take.
type ExtraPodSurge struct {
	Slots int32
	// Terminating lists the Terminating pods counted, in name order.
	Terminating []string
	// Live lists the extra pods counted that no deletion has been
	// requested for, in name order.
	Live []string
	// LiveSlotsByInstance is, per Instance, the slots its live extra pods
	// hold. A fresh start on that Instance evicts them before it creates
	// anything, so the slot it takes is the slot they held.
	LiveSlotsByInstance map[int32]int32
	// NextRelease is the earliest deletion deadline among the Terminating
	// pods counted; zero when none is.
	NextRelease time.Time
}

// ExtraPodSurgeInFlight counts, in surge slots, the pods beyond an
// Instance's own pod count that the API still lists: a Terminating pod
// until its DeletionTimestamp (the request time plus the pod's grace),
// after which it stops counting, and a live extra pod for as long as it
// exists. Buckets another anchor charges — a surge step, the index it
// pins, a rolled-not-serving row — are never charged again. A bucket the
// plan does not own is charged for its Terminating pods only: its live
// pods are the scale-down's to retire.
func ExtraPodSurgeInFlight(plan types.ComponentPlan, statuses []types.InstanceStatus, byInstance map[int32][]*corev1.Pod, rolled []int32, now time.Time) ExtraPodSurge {
	charged := make(map[int32]struct{}, len(statuses))
	runningRevision := make(map[int32]query.RevisionID, len(statuses))
	for i := range statuses {
		runningRevision[statuses[i].Index] = query.RevisionFromName(statuses[i].RunningRevision)
		op := statuses[i].Operation
		if op == nil {
			continue
		}
		switch op.Step {
		case types.UpdateStepSurge, types.UpdateStepSurgeDrain, types.UpdateStepSurgeDrainSettle:
			charged[statuses[i].Index] = struct{}{}
			if op.SurgeIndex != nil {
				charged[*op.SurgeIndex] = struct{}{}
			}
		}
	}
	for _, idx := range rolled {
		charged[idx] = struct{}{}
	}
	share := make(map[int32]int32, len(plan.Instances))
	podsPerInstance := int32(1)
	for _, inst := range plan.Instances {
		share[inst.Index] = inst.TotalPods()
		if inst.TotalPods() > podsPerInstance {
			podsPerInstance = inst.TotalPods()
		}
	}
	slotsOf := func(pods int32) int32 { return (pods + podsPerInstance - 1) / podsPerInstance }
	indices := make([]int32, 0, len(byInstance))
	for idx := range byInstance {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })

	out := ExtraPodSurge{LiveSlotsByInstance: map[int32]int32{}}
	for _, idx := range indices {
		if _, skip := charged[idx]; skip {
			continue
		}
		var listed, terminating int32
		var terminatingNames []string
		var live []*corev1.Pod
		var release time.Time
		for _, pod := range byInstance[idx] {
			if pod == nil {
				continue
			}
			if pod.DeletionTimestamp != nil {
				deadline := pod.DeletionTimestamp.Time
				if now.After(deadline) {
					continue
				}
				terminating++
				terminatingNames = append(terminatingNames, pod.Name)
				if release.IsZero() || deadline.Before(release) {
					release = deadline
				}
			} else {
				live = append(live, pod)
			}
			listed++
		}
		extra := listed - share[idx]
		if extra <= 0 {
			continue
		}
		terminatingCounted := min(extra, terminating)
		liveCounted := extra - terminatingCounted
		if _, planned := share[idx]; !planned {
			liveCounted = 0
		}
		if terminatingCounted+liveCounted <= 0 {
			continue
		}
		out.Slots += slotsOf(terminatingCounted + liveCounted)
		if terminatingCounted > 0 {
			sort.Strings(terminatingNames)
			out.Terminating = append(out.Terminating, terminatingNames...)
			if out.NextRelease.IsZero() || release.Before(out.NextRelease) {
				out.NextRelease = release
			}
		}
		if liveCounted > 0 {
			out.LiveSlotsByInstance[idx] = slotsOf(liveCounted)
			out.Live = append(out.Live, liveExtraPodNames(live, runningRevision[idx], liveCounted)...)
		}
	}
	return out
}

// liveExtraPodNames names, in name order, the n live pods of a bucket
// that are the extra ones: those off the row's running revision first,
// since a replacement a disposed attempt left behind carries the revision
// it was built for, then the rest from the highest name down. A bucket
// with no pod on the running revision is read by name order alone.
func liveExtraPodNames(pods []*corev1.Pod, running query.RevisionID, n int32) []string {
	var off, on []string
	for _, pod := range pods {
		rev := query.RevisionFromPod(pod)
		if !running.IsZero() && !rev.IsZero() && !rev.Same(running) {
			off = append(off, pod.Name)
			continue
		}
		on = append(on, pod.Name)
	}
	if len(on) == 0 {
		on, off = off, nil
	}
	sort.Strings(off)
	sort.Sort(sort.Reverse(sort.StringSlice(on)))
	names := append(off, on...)
	if int32(len(names)) > n {
		names = names[:n]
	}
	sort.Strings(names)
	return names
}
