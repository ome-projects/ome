package workload

import (
	"context"
	"errors"
	"fmt"

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
func executeRestartPass(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, restarts []RestartSelection, createFollows bool) (res ctrl.Result, stop bool, err error) {
	anyRestarting := false
	var restartErrs []error
	admission := newRepairAdmission(input, plan)
	// First denial of the pass, kept so a Component that opens nothing
	// still reports which layer is holding it.
	var firstDenial *types.RolloutHold
	var firstDeniedIndex int32
	// Fresh crash-loop repairs opened this pass, against the repair batch;
	// batchFull marks a selection left for the next pass.
	opened := int32(0)
	batchFull := false
	// Revisions whose retry ladder admitted a rebuild this pass: the
	// ladder authorizes one attempt at a revision at a time.
	ladderOpened := map[string]struct{}{}
	for _, selection := range restarts {
		if selection.OpensRepair && input.RepairBatchSize != nil && opened >= *input.RepairBatchSize {
			logf.FromContext(ctx).V(1).Info("crash-loop repair waits for the next pass: repair batch full",
				"component", plan.Component, "instance", selection.Instance.Index, "repairBatchSize", *input.RepairBatchSize)
			batchFull = true
			continue
		}
		if rev := selection.LadderRevision; rev != "" {
			if _, taken := ladderOpened[rev]; taken {
				logf.FromContext(ctx).V(1).Info("crash-loop repair waits for the next pass: the revision's retry ladder admits one attempt at a time",
					"component", plan.Component, "instance", selection.Instance.Index, "revision", rev)
				batchFull = true
				continue
			}
			ladderOpened[rev] = struct{}{}
		}
		if selection.OpensUnavailability {
			allowed, denial := admission.admit(ctx, plan, selection.Instance.Index)
			if !allowed {
				if firstDenial == nil {
					firstDenial, firstDeniedIndex = denial, selection.Instance.Index
				}
				continue
			}
		}
		if selection.OpensRepair {
			opened++
		}
		done, restartErr := workloadops.Restart(ctx, deps, input, plan, selection.Instance, target, selection.Reason)
		if restartErr != nil {
			logf.FromContext(ctx).Error(restartErr, "restart pass: instance failed",
				"component", plan.Component, "instance", selection.Instance.Index)
			restartErrs = append(restartErrs, fmt.Errorf("workload.Reconcile: restart instance %d: %w", selection.Instance.Index, restartErr))
			continue
		}
		if !done {
			anyRestarting = true
		}
	}
	// A held repair, or one left for the next pass, consumes the pass
	// exactly as an in-flight one does.
	if anyRestarting || firstDenial != nil || batchFull {
		interval := workloadops.RestartRequeueInterval(input)
		if !anyRestarting {
			interval = input.Requeue.Gate
		}
		res = types.PassRequeue(interval)
		if createFollows {
			freshPlan := planExcludingRestartSelections(plan, restarts)
			createResult, createErr := createPass(ctx, deps, input, freshPlan, target, createScopeFresh)
			if createErr != nil {
				restartErrs = append(restartErrs, fmt.Errorf("workload.Reconcile: create fresh indices during restart: %w", createErr))
				return createResult, false, errors.Join(restartErrs...)
			}
			// The create pass may carry an earlier explicit wake-up; the
			// restart's own cadence must survive an all-ready create that
			// asks for none.
			res = types.PassRequeue(interval, createResult.RequeueAfter)
		}
		if len(restartErrs) > 0 {
			return ctrl.Result{}, false, errors.Join(restartErrs...)
		}
		// Only when the pass opened nothing: a repair that ran is forward
		// progress, and its held peers are ordinary pacing.
		if !anyRestarting {
			if err := announceRepairHeld(ctx, deps, input, plan, firstDeniedIndex, firstDenial); err != nil {
				return ctrl.Result{}, false, err
			}
		}
		return res, true, nil
	}
	if len(restartErrs) > 0 {
		return ctrl.Result{}, false, errors.Join(restartErrs...)
	}
	return ctrl.Result{}, false, nil
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
// wait for a later pass, and a repair already in flight is anchored in
// prior, so the pace holds across passes and not merely within one. A
// pod set that serves nothing never reaches this admission
// (ops.RestartOpensUnavailability): its rebuild removes no serving
// capacity and is not charged.
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
}

func newRepairAdmission(input types.ReconcileInput, plan types.ComponentPlan) *repairAdmission {
	statuses := input.ObservedState.InstanceStatuses
	return &repairAdmission{
		budget: escalation.PerComponentMaxUnavailableBudget(plan.UpdateStrategy.RollingUpdate, plan.Replicas),
		prior:  escalation.CurrentUnavailableInFlight(statuses) + escalation.CurrentRestartingInFlight(statuses),
	}
}

// admit reports whether one more repair may open this pass, charging
// the pass's counter when it may. A repair recreates pods in place, so
// the budget is the unavailability one whatever the Component's update
// strategy is: nothing about a rebuild surges.
//
// A denial is returned as the hold that produced it, so the caller can
// report what is holding the Component back.
func (a *repairAdmission) admit(ctx context.Context, plan types.ComponentPlan, index int32) (bool, *types.RolloutHold) {
	if projected, denied := escalation.BudgetDenies(a.budget, a.prior, a.inFlight); denied {
		logf.FromContext(ctx).V(1).Info("crash-loop repair denied by the unavailability budget",
			"component", plan.Component, "instance", index,
			"budget", a.budget, "projected", projected)
		return false, &types.RolloutHold{
			Gate:   types.RolloutHoldGateBudget,
			Reason: fmt.Sprintf("per-Component unavailability budget %d exhausted (would become %d)", a.budget, projected),
		}
	}
	a.inFlight++
	return true, nil
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
