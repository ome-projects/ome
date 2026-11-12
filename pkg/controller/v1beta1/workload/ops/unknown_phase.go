package ops

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// What a rebuild does about a pod that holds one of its target names
// while its kubelet has stopped reporting it: phase Unknown, or Ready
// withdrawn by the control plane with the pod's own last report still
// saying Ready (evidence.UnreportedTargetPods).
//
// Neither is terminal: the node stopped reporting, and the container may
// still be running. Recycling the name the way a Failed or Succeeded
// occupant is recycled would risk two pods of the same identity, so the
// name is only freed by the force-delete sweep, on the same node-death
// evidence and the same operator-configured policy that frees a
// stuck-Terminating pod. Until it is free the step withholds its create,
// and the wait an operator reads off the row is the hold pass's
// (holds/nodeunknown.go). The Create pass runs the sweep over every
// planned Instance at every scope it builds at, a paused Component
// included, so a serving Instance whose node dies is rebuilt elsewhere by
// the ordinary missing-pod paths once the names are freed, whether or not
// another Instance's roll or repair is in flight; under a pause the
// rebuild follows the pause's own rule for a lost pod.

// targetPodNameSet is the desired names as the evidence reading takes
// them.
func targetPodNameSet(targets []podTarget) map[string]struct{} {
	wanted := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		wanted[t.Name] = struct{}{}
	}
	return wanted
}

// recoverUnknownPhaseTargets routes every target name held by a pod its
// kubelet has stopped reporting through the force-delete sweep and
// reports the outcome to its caller: holding=true means the step must
// not create or promote this pass, because a name it needs is still
// occupied or a delete of one has just been issued.
//
// With no force-delete policy configured, or with the node still alive,
// nothing frees the name and the step simply withholds. The attempt
// deadline is parked meanwhile by the hold the pass recorded from the
// same pods. A pod whose Ready was withdrawn while its node posts Ready
// is the kubelet's own lag, holds nothing and withholds nothing.
//
// requeueAt is the next moment the evidence could turn actionable on its
// own. A node that has stopped reporting emits no event, so a caller
// whose pass has no poll of its own must fold it into its requeue, or
// the sweep fires only when something unrelated wakes the owner.
//
// Every unreported pod is visited whatever happened to the ones before
// it: a gang whose members all sit on one dead node is freed as one set,
// so no repair opens on a gang with a member still held. The errors are
// joined and the pass fails on them after the sweep.
func recoverUnknownPhaseTargets(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, existing []*corev1.Pod, targets []podTarget) (bool, time.Time, error) {
	unreported := evidence.UnreportedTargetPods(existing, targetPodNameSet(targets), input.ForceDelete)
	if len(unreported) == 0 {
		return false, time.Time{}, nil
	}
	holding := false
	var nextEvidenceAt time.Time
	var errs []error
	for _, pod := range unreported {
		holds, at, err := sweepUnknownPhasePod(ctx, deps, input, pod, idx)
		if err != nil {
			errs = append(errs, fmt.Errorf("sweep unknown-phase pod %s: %w", pod.Name, err))
			holding = true
			continue
		}
		if !holds {
			continue
		}
		holding = true
		if !at.IsZero() && (nextEvidenceAt.IsZero() || at.Before(nextEvidenceAt)) {
			nextEvidenceAt = at
		}
	}
	return holding, nextEvidenceAt, errors.Join(errs...)
}

// occupiedTargets names every pod of a set as a target of the sweep. A
// rebuild reuses each pod's stable name, so each one is a name it needs.
func occupiedTargets(pods []*corev1.Pod) []podTarget {
	targets := make([]podTarget, 0, len(pods))
	for _, pod := range pods {
		if pod != nil {
			targets = append(targets, podTarget{Name: pod.Name})
		}
	}
	return targets
}

// sweepUnknownPhasePod picks the arm of the force-delete sweep the pod's
// own state calls for. One that is Terminating as well as unreported is
// the stuck-teardown case, which owns the graceful-shutdown window and the
// finalizer report; neither arm reaches these callers otherwise, so
// without this such a pod would hold its name with the attempt deadline
// parked and nothing left to free it.
//
// Reports whether the name still stands against the step this pass: a
// Terminating pod does until its absence is observed, whichever arm
// acted; a pod the node-death arm deleted does too, for the same reason;
// one it left alone does unless its node proved live (UnreportedPodHeld).
func sweepUnknownPhasePod(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32) (bool, time.Time, error) {
	if pod.DeletionTimestamp == nil {
		kind, at, err := forceDeleteOnNodeDeath(ctx, deps, input, pod, idx)
		if err != nil {
			return false, time.Time{}, err
		}
		return kind.Actionable() || evidence.UnreportedPodHeld(pod, kind), at, nil
	}
	at, err := escalateStuckTerminatingWithDeadline(ctx, deps, input, pod, idx)
	return true, at, err
}

// recoverUnknownPhaseInstances applies recoverUnknownPhaseTargets across
// a whole Create pass: every planned Instance, whatever scope the pass
// builds at. A Ready Instance whose pods sit on a lost node has no
// attempt of its own to free them, so this is the one reader every such
// pod has, and a pass narrowed to the indices it may build for (a roll
// or a repair in flight elsewhere, or an operator pause) must still free
// the names of the others, or one Instance's wait, or the pause, would
// keep every other Instance on the lost node. Returns the Instances the
// pass must leave alone this time round and the earliest policy boundary
// among them.
//
// An Instance's target names are read only when one of its pods has no
// report from its kubelet: the sweep has nothing to find among pods that
// report, and at rest that is every pod of the Component.
func recoverUnknownPhaseInstances(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, byInstance map[int32][]*corev1.Pod) (map[int32]bool, time.Time, error) {
	var held map[int32]bool
	var nextEvidenceAt time.Time
	for _, inst := range plan.Instances {
		pods := byInstance[inst.Index]
		if !evidence.AnyUnreportedPod(pods, input.ForceDelete) {
			continue
		}
		holding, at, err := recoverUnknownPhaseTargets(ctx, deps, input, inst.Index,
			pods, expectedPodNamesForInstance(input, plan, inst))
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("Create: recover unknown-phase pods (instance=%d): %w", inst.Index, err)
		}
		if !at.IsZero() && (nextEvidenceAt.IsZero() || at.Before(nextEvidenceAt)) {
			nextEvidenceAt = at
		}
		if holding {
			if held == nil {
				held = make(map[int32]bool)
			}
			held[inst.Index] = true
		}
	}
	return held, nextEvidenceAt, nil
}
