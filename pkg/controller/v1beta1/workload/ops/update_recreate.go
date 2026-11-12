package ops

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// recreateRevisionCause is the cause a recreate records on its operation
// and names in its event. A row that names a running revision moves from
// it: "revision <from> -> <to>". A row that names none has no revision to
// move from, so the cause says what the rebuild follows instead of an
// empty source: a failed create when the Instance never served, or a set
// that never served when the Instance served once and the set its last
// attempt built did not.
func recreateRevisionCause(input workload.ReconcileInput, idx int32, targetRev string) string {
	s := input.ObservedState.Instance(idx)
	switch {
	case s != nil && s.RunningRevision != "":
		return fmt.Sprintf("revision %s -> %s", s.RunningRevision, targetRev)
	case s != nil && s.ReadySince != nil && !s.ReadySince.IsZero():
		return fmt.Sprintf("rebuild of a set that never served -> %s", targetRev)
	default:
		return fmt.Sprintf("rebuild after a failed create -> %s", targetRev)
	}
}

// runningRevisionNeverServed reports whether the row names a running
// revision the Instance never served: the revision was created after the
// row last entered Ready, so no set of it was ever promoted. A repair that
// rebuilds an Instance at the roll target records that revision before
// the set serves; a recreate that replaces the set leaves no set of the
// revision behind the record. A row that was never Ready, or whose
// revision is gone, keeps what it records.
func runningRevisionNeverServed(ctx context.Context, reads client.Reader, input workload.ReconcileInput, idx int32) (bool, error) {
	row := input.ObservedState.Instance(idx)
	if row == nil || row.RunningRevision == "" || row.ReadySince == nil || row.ReadySince.IsZero() {
		return false, nil
	}
	cr := &appsv1.ControllerRevision{}
	if err := reads.Get(ctx, client.ObjectKey{Namespace: input.Key.Namespace, Name: row.RunningRevision}, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get running revision %s: %w", row.RunningRevision, err)
	}
	return row.ReadySince.Time.Before(cr.CreationTimestamp.Time), nil
}

// recreateRoll is one pass of the recreate roll of an Instance: what the
// pass read at its start, and what its phases learn as they run. The roll
// takes the whole set out of rotation, deletes it, and builds it again
// under the roll target at the next recreation count; the pods keep their
// names. A phase reports stop when the pass ends with it; the next pass
// runs the phases again from the top.
type recreateRoll struct {
	deps   workload.Deps
	input  workload.ReconcileInput
	plan   workload.ComponentPlan
	inst   workload.InstancePlan
	target *appsv1.ControllerRevision
	// pods are the Instance's pods as the pass listed them.
	pods []*corev1.Pod

	// recreationCount is the count the pods the attempt builds carry; the
	// pods it removes carry a lower one (openAttempt).
	recreationCount int64
	// oldPods carry a lower recreation count, newPods the attempt's, and
	// unlabelledPods none at all (openAttempt).
	oldPods, newPods, unlabelledPods []*corev1.Pod
	// rebuilt is the Instance the attempt builds: the roll target's runners
	// at the attempt's recreation count; desired is the pod names that set
	// asks for (planRebuiltSet).
	rebuilt workload.InstancePlan
	desired []podTarget
}

// recreateUpdate runs the recreate roll of one Instance: every pod of the
// set leaves rotation and is deleted, and the set is built again under the
// roll target at the next recreation count. It shares the drain, delete
// and rebuild skeleton with the repair but starts from a template change
// rather than from a lost pod. The phases below run in order, from the
// top on every pass; the row and the pods found decide how far a pass
// gets:
//
//	open the attempt; refuse pods that name no recreation count; drain
//	the old set; delete the old set; wait for the old set to clear on a
//	live read; plan the rebuilt set; free the names of its terminal
//	members; create the missing members; mark the set serving; wait for
//	the promote bar; promote.
func recreateUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, pods []*corev1.Pod) (bool, error) {
	roll := &recreateRoll{deps: deps, input: input, plan: plan, inst: inst, target: target, pods: pods}
	phases := []func(context.Context) (bool, error){
		roll.openAttempt,
		roll.refuseUnlabelledPods,
		roll.drainOldPods,
		roll.deleteOldPods,
		roll.waitForOldPodsToClear,
		roll.planRebuiltSet,
		roll.freeTerminalNames,
		roll.createMissingMembers,
		roll.markPodsServing,
		roll.waitForPromoteBar,
	}
	for _, phase := range phases {
		if stop, err := phase(ctx); stop || err != nil {
			return false, err
		}
	}
	return roll.promote(ctx)
}

// openAttempt records the attempt on the row and sorts the pods the pass
// listed by the recreation count the row then names. The row's reason is
// the cause, which tells a revision roll's recreate from a pod-failure
// repair in status: Type Update against Type Restart, and a revision pair
// against a termination reason. The first pass reports the start. A pass
// whose copy of the row carried the attempt in flight goes on with it
// only while the live row still does (recordAttempt); a Failed row that
// kept its recreate is driven again as a continuation.
func (r *recreateRoll) openAttempt(ctx context.Context) (bool, error) {
	row := r.input.ObservedState.Instance(r.inst.Index)
	// The first pass of a recreate finds no Update operation at the drain
	// step; recording the attempt puts the row there.
	firstPass := row == nil || row.Operation == nil ||
		row.Operation.Type != workload.InstanceOperationUpdate || row.Operation.Step != workload.UpdateStepDrain
	inFlight := !firstPass && row.Phase == workload.InstancePhaseUpdating
	cause := recreateRevisionCause(r.input, r.inst.Index, r.target.Name)
	// The row is written on the first pass and on a retarget; only then does
	// its running revision need reading against the revision's own age.
	runningNeverServed := false
	if firstPass || row == nil || row.TargetRevision != r.target.Name {
		unserved, err := runningRevisionNeverServed(ctx, r.deps.Reader(), r.input, r.inst.Index)
		if err != nil {
			return false, fmt.Errorf("read the running revision (instance=%d): %w", r.inst.Index, err)
		}
		runningNeverServed = unserved
	}
	count, open, err := r.recordAttempt(ctx, inFlight, cause, runningNeverServed)
	if err != nil {
		return false, fmt.Errorf("bump Incarnation (instance=%d): %w", r.inst.Index, err)
	}
	if !open {
		return true, nil
	}
	r.recreationCount = count
	if firstPass {
		workload.RecordNormal(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonRecreateUpdateStarted,
			"OMENative %s recreate (%s, incarnation=%d)",
			workload.InstanceKey(r.input.Key.Component, r.inst.Index), cause, count)
	}
	r.oldPods, r.newPods, r.unlabelledPods = query.PartitionPodsByIncarnation(r.pods, count)
	return false, nil
}

// recordAttempt writes the attempt on the row. A pass that found the
// attempt in flight on its copy of the row goes on with it only while the
// live row still carries it, and reads the row as not open once the pass
// before promoted or ended it, so a copy a write behind the row opens
// nothing on a row it did not read. Every other pass opens the attempt:
// the first pass, and the pass that drives a Failed row's kept recreate
// again.
func (r *recreateRoll) recordAttempt(ctx context.Context, inFlight bool, cause string, runningNeverServed bool) (int64, bool, error) {
	if !inFlight {
		count, err := status.StampRecreating(ctx, r.input, r.inst.Index, r.target.Name, cause, r.plan.UpdateStrategy.Type, r.plan.InstanceReadyTimeout, runningNeverServed)
		return count, true, err
	}
	return status.ResumeRecreating(ctx, r.input, r.inst.Index, r.target.Name, cause, r.plan.UpdateStrategy.Type, r.plan.InstanceReadyTimeout, runningNeverServed)
}

// refuseUnlabelledPods ends the pass while a pod of the Instance names no
// recreation count: a pod whose label was stripped, or a third party's pod
// under the managed-by label, must not be deleted as an old member. Each
// such pod is reported.
func (r *recreateRoll) refuseUnlabelledPods(context.Context) (bool, error) {
	if len(r.unlabelledPods) == 0 {
		return false, nil
	}
	for _, pod := range r.unlabelledPods {
		workload.RecordWarning(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonFoundOrphan,
			"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to recreate-update",
			workload.InstanceKey(r.input.Key.Component, r.inst.Index), pod.Namespace, pod.Name)
	}
	return true, nil
}

// drainOldPods takes the old set out of rotation and ends the pass while
// a routed Service still lists any of it. With no old pod standing the
// phase has nothing to do.
func (r *recreateRoll) drainOldPods(ctx context.Context) (bool, error) {
	if len(r.oldPods) == 0 {
		return false, nil
	}
	for _, pod := range r.oldPods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodNotServing(ctx, r.deps.Client, r.deps.Reader(), pod, podreadiness.WriterUpdateRecreateDrain, updateDrainKey(r.inst.Index, r.inst.Incarnation)); err != nil {
			return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
	}
	// The drain check reads live, so that kube-proxy is not still routing
	// to the pod; one Batcher lists each per-revision Service's
	// EndpointSlices once for the whole set.
	drainer := drain.NewBatcher(r.deps.Reader(), r.input.Key.Namespace)
	for _, pod := range r.oldPods {
		serviceName := drainServiceForPod(r.input, r.plan, pod)
		if serviceName == "" {
			continue
		}
		drained, err := drainer.IsPodDrained(ctx, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
		if !drained {
			return true, nil
		}
	}
	return false, nil
}

// deleteOldPods deletes the drained old set, once every create and delete
// of the Instance has been observed, and ends the pass: the rebuild starts
// on the pass that finds no old pod. Each delete is expected before it is
// issued, and the expectation is taken back when the call fails, since a
// failed call fires no event to count it down.
func (r *recreateRoll) deleteOldPods(ctx context.Context) (bool, error) {
	if len(r.oldPods) == 0 {
		return false, nil
	}
	if !r.expectationsSatisfied() {
		return true, nil
	}
	for _, pod := range r.oldPods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		r.deps.ExpectationsCache().ExpectDeletes(r.input.Key.Namespace, r.input.Key.OwnerName, r.input.Key.Component, r.inst.Index, 1)
		if err := r.deps.Client.Delete(ctx, pod); err != nil {
			r.deps.ExpectationsCache().ObservedDelete(r.input.Key.Namespace, r.input.Key.OwnerName, r.input.Key.Component, r.inst.Index)
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return true, nil
}

// waitForOldPodsToClear ends the pass until a live read finds every pod of
// a lower recreation count gone or terminating. The cache can still hold a
// deleted member, and a create under its stable name would fail with
// AlreadyExists.
func (r *recreateRoll) waitForOldPodsToClear(ctx context.Context) (bool, error) {
	clear, err := query.LiveOldPodsClearedForRecreate(ctx, r.deps.Reader(), r.input.Key.Namespace, r.input.Key.OwnerName, r.plan.Component, r.inst.Index, r.recreationCount)
	if err != nil {
		return false, fmt.Errorf("recreateUpdate: live-check Phase A done (instance=%d): %w", r.inst.Index, err)
	}
	return !clear, nil
}

// planRebuiltSet names what the attempt builds: the roll target's runners,
// at the width the target records, under the attempt's recreation count,
// and the pod names that set asks for.
func (r *recreateRoll) planRebuiltSet(context.Context) (bool, error) {
	r.rebuilt = r.inst
	r.rebuilt.Incarnation = r.recreationCount
	r.rebuilt.Runners = r.plan.RunnersFor(r.inst, r.target.Name)
	r.desired = expectedPodNamesForInstance(r.input, r.plan, r.rebuilt)
	return false, nil
}

// freeTerminalNames deletes a rebuilt pod in a terminal phase (crashed,
// exited, evicted or refused by its node): it still holds its name and
// would never clear the promote bar, so the next pass creates it again
// instead of waiting on it to the deadline. A pass that deleted one ends.
func (r *recreateRoll) freeTerminalNames(ctx context.Context) (bool, error) {
	freed, err := recycleTerminalTargets(ctx, r.deps, r.input, r.inst.Index, r.inst.Index,
		workload.InstanceOperationUpdate, r.newPods, r.desired)
	if err != nil {
		return false, fmt.Errorf("recycle terminal replacement (instance=%d): %w", r.inst.Index, err)
	}
	return freed, nil
}

// createMissingMembers creates the pods of the rebuilt set that do not
// exist yet, once every create and delete of the Instance has been
// observed, and ends the pass; a refusal the create path handled ends it
// without an error. With every member standing the phase has nothing to
// do.
func (r *recreateRoll) createMissingMembers(ctx context.Context) (bool, error) {
	existingByName := query.IndexPodsByName(r.newPods)
	missing := make([]podTarget, 0, len(r.desired))
	for _, t := range r.desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return false, nil
	}
	if !r.expectationsSatisfied() {
		return true, nil
	}
	if _, err := createMissingPods(ctx, r.deps, r.input, r.plan, r.rebuilt, r.inst.Index, missing, desiredTemplate(r.input, r.plan, query.RevisionOf(r.target))); err != nil {
		if createRejectionHandled(err) {
			return true, nil
		}
		return false, err
	}
	return true, nil
}

// markPodsServing ends the pass until every pod of the rebuilt set passes
// its runtime probe, then puts the set into rotation under the lifecycle
// key: the recreate drain key lived on the old pods, which are gone.
func (r *recreateRoll) markPodsServing(ctx context.Context) (bool, error) {
	if !query.AllPodsRuntimeReady(r.newPods) {
		return true, nil
	}
	for _, pod := range r.newPods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, r.deps.Client, r.deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
	}
	return false, nil
}

// waitForPromoteBar ends the pass until the set has been Ready for
// MinReadySeconds. ContainersReady only proves the runtime probe passed;
// a pod joins its Service once the kubelet folds the readiness gate into
// PodReady, and promotion releases the slot that holds the next Instance's
// drain.
func (r *recreateRoll) waitForPromoteBar(context.Context) (bool, error) {
	promotable, wait := query.PodSetPromotable(r.newPods, r.plan.MinReadySeconds, r.input.Now())
	if !promotable {
		r.input.PromoteWindow.Observe(wait)
	}
	return !promotable, nil
}

// promote records the row Ready on the roll target and reports the
// finished recreate.
func (r *recreateRoll) promote(ctx context.Context) (bool, error) {
	if err := status.StampReadyOnRevision(ctx, r.input, r.inst.Index, r.target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", r.inst.Index, err)
	}
	workload.RecordNormal(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonRecreateUpdateCompleted,
		"OMENative %s recreate to revision %s complete",
		workload.InstanceKey(r.input.Key.Component, r.inst.Index), r.target.Name)
	return true, nil
}

// expectationsSatisfied reports whether every create and delete the roll
// issued for the Instance has been observed; no phase creates or deletes
// before then.
func (r *recreateRoll) expectationsSatisfied() bool {
	return r.deps.ExpectationsCache().Satisfied(r.input.Key.Namespace, r.input.Key.OwnerName, r.input.Key.Component, r.inst.Index)
}
