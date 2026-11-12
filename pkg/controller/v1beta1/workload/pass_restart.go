package workload

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// executeRestartPass runs the Restart op for the Decision's selected
// Instances. Instances are isolated from each other: one Instance's
// failed step never skips the Instances after it, and the pass fails as
// a whole (joined error) only after every selection has run. A repair
// that opens a new outage is admitted like an update start; one that
// recovers capacity already lost, or drives an open repair, is not held
// by anything. Fresh crash-loop repairs, budgeted or not, open at most
// input.RepairBatchSize per pass; the rest wait for the next pass.
//
// stop=true means the pass consumed the reconcile: a repair opened, held
// or left for the next pass owns the wake-up, because nothing in the
// cluster changes while a denial stands and no watch event is coming.
// When a Create follows in the action list the pass materializes the
// surge-free indices itself before ending, so a stalled repair does not
// starve a scale-up.
func executeRestartPass(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, restarts []RestartSelection, createFollows bool) (res ctrl.Result, stop bool, err error) {
	admission, err := newRepairAdmission(ctx, input, plan, snapshot)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	outcome := &restartPassOutcome{}
	// Fresh crash-loop repairs opened this pass, against the repair batch.
	opened := int32(0)
	// Revisions whose retry schedule admitted a rebuild this pass: a
	// retry schedule authorizes one attempt at a revision at a time.
	retryAttemptOpened := map[string]struct{}{}
	restartBatch := workloadops.NewRestartBatch(deps, input)
	for _, selection := range restarts {
		if repairWaitsForNextPass(ctx, input, plan, selection, opened, retryAttemptOpened) {
			outcome.batchFull = true
			continue
		}
		if selection.OpensUnavailability {
			allowed, denial := admission.admit(ctx, plan, selection.Instance.Index)
			if !allowed {
				if outcome.firstDenial == nil {
					outcome.firstDenial, outcome.firstDeniedIndex = denial, selection.Instance.Index
				}
				continue
			}
		}
		if selection.OpensRepair {
			opened++
		}
		// The Component-wide force-delete sweep runs after planning and before
		// this pass. Remove only the exact UIDs it successfully deleted from
		// the decision snapshot, so a restart can rebuild those names in this
		// pass without another API read.
		pods := restartPodsAfterSweep(selection.pods, input.Swept)
		done, restartErr := restartBatch.Advance(ctx, deps, input, plan, selection.Instance, target, selection.Reason, pods)
		if restartErr != nil {
			logf.FromContext(ctx).Error(restartErr, "restart pass: instance failed",
				"component", plan.Component, "instance", selection.Instance.Index)
			outcome.errs = append(outcome.errs, fmt.Errorf("workload.Reconcile: restart instance %d: %w", selection.Instance.Index, restartErr))
			continue
		}
		if !done {
			outcome.anyRestarting = true
		}
	}
	// A held repair, or one left for the next pass, consumes the pass
	// exactly as an in-flight one does.
	if outcome.consumesThePass() {
		return outcome.endHeld(ctx, deps, input, plan, target, restarts, createFollows, restartBatch.PolicyBoundaryIn(input.Now()))
	}
	if len(outcome.errs) > 0 {
		return ctrl.Result{}, false, errors.Join(outcome.errs...)
	}
	return ctrl.Result{}, false, nil
}

// restartPassOutcome is what the selections of one restart pass left
// behind: whether a repair is still in flight, the first denial of the pass
// and the Instance it held (kept so a Component that opens nothing still
// reports which layer is holding it), whether a selection waits for the
// next pass, and the errors of the selections that failed.
type restartPassOutcome struct {
	anyRestarting    bool
	batchFull        bool
	firstDenial      *types.RolloutHold
	firstDeniedIndex int32
	errs             []error
}

// consumesThePass says whether the pass owns the wake-up: a repair in
// flight, a held repair, or one left for the next pass.
func (o *restartPassOutcome) consumesThePass() bool {
	return o.anyRestarting || o.firstDenial != nil || o.batchFull
}

// endHeld ends a pass that consumed the reconcile: it requeues on the
// restart cadence while a repair is in flight and on the gate cadence
// otherwise, sooner when a node under a rebuilt set comes of age first
// (boundaryIn: that moment is on a clock and emits no event), materializes
// the surge-free indices first when a Create follows, and reports the first
// denial only when the pass opened nothing (a repair that ran is forward
// progress, and its held peers are ordinary pacing).
func (o *restartPassOutcome) endHeld(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, restarts []RestartSelection, createFollows bool, boundaryIn time.Duration) (ctrl.Result, bool, error) {
	interval := workloadops.RestartRequeueInterval(input)
	if !o.anyRestarting {
		interval = input.Requeue.Gate
	}
	res := types.PassRequeue(interval, boundaryIn)
	if createFollows {
		freshPlan := planExcludingRestartSelections(plan, restarts)
		createResult, createErr := createPass(ctx, deps, input, freshPlan, target, createScopeFresh, nil)
		if createErr != nil {
			o.errs = append(o.errs, fmt.Errorf("workload.Reconcile: create fresh indices during restart: %w", createErr))
			return createResult, false, errors.Join(o.errs...)
		}
		// The create pass may carry an earlier explicit wake-up; the
		// restart's own cadence must survive an all-ready create that
		// asks for none.
		res = types.PassRequeue(interval, createResult.RequeueAfter, boundaryIn)
	}
	if len(o.errs) > 0 {
		return ctrl.Result{}, false, errors.Join(o.errs...)
	}
	if !o.anyRestarting {
		if err := announceRepairHeld(ctx, deps, input, plan, o.firstDeniedIndex, o.firstDenial); err != nil {
			return ctrl.Result{}, false, err
		}
	}
	return res, true, nil
}

// repairWaitsForNextPass says whether a selection waits for the next pass:
// a fresh crash-loop repair past the repair batch (opened against
// input.RepairBatchSize), or a rebuild at a revision whose retry schedule
// already admitted an attempt this pass. A revision admitted here is
// recorded in retryAttemptOpened, so a later selection at it waits.
func repairWaitsForNextPass(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, selection RestartSelection, opened int32, retryAttemptOpened map[string]struct{}) bool {
	if selection.OpensRepair && input.RepairBatchSize != nil && opened >= *input.RepairBatchSize {
		logf.FromContext(ctx).V(1).Info("crash-loop repair waits for the next pass: repair batch full",
			"component", plan.Component, "instance", selection.Instance.Index, "repairBatchSize", *input.RepairBatchSize)
		return true
	}
	rev := selection.RetryScheduleRevision
	if rev == "" {
		return false
	}
	if _, taken := retryAttemptOpened[rev]; taken {
		logf.FromContext(ctx).V(1).Info("crash-loop repair waits for the next pass: the revision's retry ladder admits one attempt at a time",
			"component", plan.Component, "instance", selection.Instance.Index, "revision", rev)
		return true
	}
	retryAttemptOpened[rev] = struct{}{}
	return false
}

// restartPodsAfterSweep returns the decision bucket with Pods successfully
// force-deleted earlier in this pass omitted. It preserves the original slice
// when the sweep removed none, and never mutates the snapshot-owned slice.
func restartPodsAfterSweep(pods []*corev1.Pod, swept *types.SweptPods) []*corev1.Pod {
	for i, pod := range pods {
		if pod == nil || !swept.Swept(pod.UID) {
			continue
		}
		kept := make([]*corev1.Pod, 0, len(pods)-1)
		kept = append(kept, pods[:i]...)
		for _, candidate := range pods[i+1:] {
			if candidate == nil || !swept.Swept(candidate.UID) {
				kept = append(kept, candidate)
			}
		}
		return kept
	}
	return pods
}

// planExcludingRestartSelections is the plan with the restarting
// Instances removed, so a create pass run alongside the restart touches
// only surge-free indices.
func planExcludingRestartSelections(plan types.ComponentPlan, restarts []RestartSelection) types.ComponentPlan {
	excluded := make(map[int32]struct{}, len(restarts))
	for _, restart := range restarts {
		excluded[restart.Instance.Index] = struct{}{}
	}

	filtered := plan
	filtered.Instances = make([]types.InstancePlan, 0, len(plan.Instances))
	for _, instance := range plan.Instances {
		if _, restarting := excluded[instance.Index]; restarting {
			continue
		}
		filtered.Instances = append(filtered.Instances, instance)
	}
	return filtered
}

// repairAdmission paces the restart repairs that OPEN a new
// unavailability: rebuilds of a pod set that still serves. A wedge is
// usually a property of the revision, not of one Instance, so without
// pacing a bad image or a bad argument qualifies every Ready Instance in
// the same pass and the Component takes every serving pod offline at
// once. The per-Component MaxUnavailable budget answers here as it does
// for an update start, against the same numbers: selections it denies
// wait for a later pass, a repair already in flight is anchored in
// prior, so the pace holds across passes and not merely within one, and
// so is every Ready Instance whose routed pods are in rotation only as
// comebacks inside the proven window (ops.InstanceServingUnproven): a
// runner the kubelet restarted that has not held Ready again for the
// window is up only between its crashes, so no serving set is taken
// offline beside it. A pod set that serves nothing never reaches this
// admission (ops.RestartOpensUnavailability): its rebuild removes no
// serving capacity and is not charged, and such an Instance holds no slot
// against the repairs of its peers until its own repair is in flight.
//
// The cross-Component coordination gate is not consulted. Its serving
// count needs every pod of an Instance in rotation, and the parked
// member that makes a crash-loop wedge already takes the Instance out of
// that count, so a consult would charge the repair for the outage it
// ends and, under a group budget of one, hold it for good.
type repairAdmission struct {
	budget   int32
	prior    int32
	inFlight int32
	// downOutsideAttempts names, by index, the planned Instances counted
	// in prior by the window rather than by a phase anchor,
	// each with why. A repair of one of them takes over the slot its own
	// outage holds; a denial names the others.
	downOutsideAttempts map[int32]string
}

// newRepairAdmission reads the budget and the Instances already down: the
// attempts the phase anchors count and, under a capped budget, every Ready
// Instance up only inside the proven window (the Component's
// minReadySeconds or the operator's grace for a pod the kubelet keeps
// restarting, whichever is longer), read off the pass's cached pods. A
// window still open raises no watch event when it elapses, so the pass
// requeues for it.
func newRepairAdmission(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, snapshot *ObservedSnapshot) (*repairAdmission, error) {
	statuses := input.ObservedState.InstanceStatuses
	a := &repairAdmission{
		budget:              escalation.PerComponentMaxUnavailableBudget(plan.UpdateStrategy.RollingUpdate, plan.Replicas),
		prior:               escalation.CurrentUnavailableInFlight(statuses) + escalation.CurrentRestartingInFlight(statuses),
		downOutsideAttempts: map[int32]string{},
	}
	if a.budget == escalation.BudgetNoLimit {
		return a, nil
	}
	pods, err := snapshot.CachedPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("workload.Reconcile: list pods for the repair budget (component=%s): %w", plan.Component, err)
	}
	window := provenWindowSeconds(input, plan)
	for _, inst := range plan.Instances {
		row := input.ObservedState.Instance(inst.Index)
		down, wait, why := workloadops.InstanceServingUnproven(row, pods[inst.Index], window, input.Now())
		if !down {
			continue
		}
		a.prior++
		a.downOutsideAttempts[inst.Index] = why
		if wait > 0 {
			input.PassWake.Observe(wait)
		}
	}
	return a, nil
}

// admit reports whether one more repair may open this pass, charging
// the pass's counter when it may. A repair recreates pods in place, so
// the budget is the unavailability one whatever the Component's update
// strategy is: nothing about a rebuild surges. A repair of an Instance
// counted by the window takes over the slot its own outage holds and is
// charged as any repair.
//
// A denial is returned as the hold that produced it, naming the Instances
// the window counts, so the caller can report what is holding the
// Component back.
func (a *repairAdmission) admit(ctx context.Context, plan types.ComponentPlan, index int32) (bool, *types.RolloutHold) {
	prior := a.prior
	if _, down := a.downOutsideAttempts[index]; down {
		prior--
	}
	if projected, denied := escalation.BudgetDenies(a.budget, prior, a.inFlight); denied {
		logf.FromContext(ctx).V(1).Info("crash-loop repair denied by the unavailability budget",
			"component", plan.Component, "instance", index,
			"budget", a.budget, "projected", projected, "downOutsideAttempts", a.downOutsideAttempts)
		return false, &types.RolloutHold{
			Gate:   types.RolloutHoldGateBudget,
			Reason: fmt.Sprintf("per-Component unavailability budget %d exhausted (would become %d)%s", a.budget, projected, a.downClause(index)),
		}
	}
	a.inFlight++
	return true, nil
}

// downClause names the Instances the window counts, the repaired one left
// out, each with why, in index order; empty when there is none.
func (a *repairAdmission) downClause(index int32) string {
	indices := make([]int32, 0, len(a.downOutsideAttempts))
	for idx := range a.downOutsideAttempts {
		if idx != index {
			indices = append(indices, idx)
		}
	}
	if len(indices) == 0 {
		return ""
	}
	slices.Sort(indices)
	parts := make([]string, 0, len(indices))
	for _, idx := range indices {
		parts = append(parts, fmt.Sprintf("Instance %d not serving (%s)", idx, a.downOutsideAttempts[idx]))
	}
	return "; " + strings.Join(parts, "; ")
}

// announceRepairHeld announces that a Component wedged on a crash loop
// opened no repair this pass. The Instance and the budget both go in the
// message, so the operator knows whether to raise MaxUnavailable or
// intervene. Announced once per episode on the held Instance's own row.
func announceRepairHeld(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, index int32, held *types.RolloutHold) error {
	if deps.Recorder == nil || held == nil {
		return nil
	}
	target := types.EventTarget(input)
	if target == nil {
		return nil
	}
	announced, err := status.Announce(ctx, input, index, types.EventReasonRepairHeld)
	if err != nil || !announced {
		return err
	}
	deps.Recorder.Eventf(target, corev1.EventTypeWarning, string(types.EventReasonRepairHeld),
		"OMENative component=%s instance=%d crash-loop repair held by %s: %s",
		plan.Component, index, held.Gate, held.Reason)
	return nil
}
