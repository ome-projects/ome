package ops

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Tests for a mid-rollout UpdateStrategy.Type change against an Instance
// that is already inside the SurgeThenDrain state machine.
//
// UpdateStrategy is not part of the ControllerRevision payload, so editing
// it does not retarget the roll: the same target revision keeps rolling
// under a different mechanism. UpdateWithPods re-resolves the mode from the
// live plan on every pass, and the mode it lands on has no knowledge of the
// pods, ordinal slot, or serving-gate holds the previous mode left behind.
//
// Each test runs the identical fixture twice: once with SurgeThenDrain
// retained (the control, which pins today's correct behavior) and once with
// the strategy flipped mid-flight.

// midSurgeFixture is one Instance inside a SurgeThenDrain roll from
// runningImage to targetImage: a source pod at ordinal 0 and a replacement
// at ordinal 1, with the Instance's Operation parked at step.
type midSurgeFixture struct {
	client     client.Client
	isvc       *v1beta1.InferenceService
	targetSpec *corev1.PodSpec
	targetCR   *appsv1.ControllerRevision
	sourcePod  *corev1.Pod
	surgePod   *corev1.Pod
}

// newMidSurgeFixture builds the shared mid-surge state. surgeReady controls
// whether the replacement has reached ContainersReady + PodReady and taken
// over serving; sourceServing controls whether the source is still in
// rotation (false models a source the surge already drained).
func newMidSurgeFixture(t *testing.T, step string, surgeReady, sourceServing bool) *midSurgeFixture {
	t.Helper()
	legacyResetExpectations(t)

	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	runningSpec := legacyTargetSpecImage("test:v1")
	targetSpec := legacyTargetSpecImage("test:v2")

	sourcePod := surgePodAtOrdinal(isvc, 0, 1, 0, true /* ready */, sourceServing)
	surgePod := surgePodAtOrdinal(isvc, 0, 1, 1, surgeReady, surgeReady)
	if surgeReady {
		surgePod.Status.Conditions = append(surgePod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
		})
	}

	c := legacyNewFakeClient(t, isvc, ir, sourcePod, surgePod)
	runningCR := legacyEnsureTargetCR(t, c, isvc, runningSpec)
	targetCR := legacyEnsureTargetCR(t, c, isvc, targetSpec)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, runningSpec)

	// The source carries the running revision's hash and the replacement the
	// target's, exactly as createMissingPods stamps them. reclassifyByRevisionHash
	// and inPlaceUpdate's on-target skip both key on this label.
	legacyStampPodRevisionHash(t, c, sourcePod, runningCR.Name)
	legacyStampPodRevisionHash(t, c, surgePod, targetCR.Name)
	sourcePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(runningCR.Name)
	surgePod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(targetCR.Name)

	seedInFlightUpdateOperation(t, c, isvc, 0, step, targetCR.Name)
	if sourceServing != podreadiness.IsServing(sourcePod) {
		t.Fatalf("fixture: source serving gate is %v, want %v", podreadiness.IsServing(sourcePod), sourceServing)
	}

	return &midSurgeFixture{
		client:     c,
		isvc:       isvc,
		targetSpec: targetSpec,
		targetCR:   targetCR,
		sourcePod:  sourcePod,
		surgePod:   surgePod,
	}
}

// run drives one UpdateWithPods pass under strategy, returning the pass's
// done flag. The ReconcileInput is rebuilt per pass because its ObservedState
// is a snapshot — production re-reads it every reconcile.
// GracePeriodSeconds=0 keeps the surge settle step from adding a wait the
// assertions would have to sleep through.
func (f *midSurgeFixture) run(t *testing.T, strategy workload.UpdateStrategyType) bool {
	t.Helper()
	zeroGrace := int32(0)
	plan := legacyComponentPlan(strategy, &workload.InPlaceUpdateStrategy{GracePeriodSeconds: &zeroGrace})
	input := legacyTestInput(f.isvc, f.client, workload.ComponentEngine)
	done, err := UpdateWithPods(context.Background(), legacyTestDeps(f.client), input, plan,
		plan.Instances[0], f.targetCR, f.targetSpec, f.survivingPods(t))
	if err != nil {
		t.Fatalf("UpdateWithPods (strategy=%s): %v", strategy, err)
	}
	return done
}

// survivingPods re-reads the Instance's pods, dropping any the previous pass
// deleted. The dispatcher hands the update op a freshly listed pod set each
// reconcile; a stale slice would keep feeding it a pod that no longer exists.
func (f *midSurgeFixture) survivingPods(t *testing.T) []*corev1.Pod {
	t.Helper()
	out := make([]*corev1.Pod, 0, 2)
	for _, pod := range []*corev1.Pod{f.sourcePod, f.surgePod} {
		if fresh, found := f.livePod(t, pod); found {
			out = append(out, fresh)
		}
	}
	return out
}

// livePod re-reads pod from the fake client. found=false means it was deleted.
func (f *midSurgeFixture) livePod(t *testing.T, pod *corev1.Pod) (*corev1.Pod, bool) {
	t.Helper()
	fresh := &corev1.Pod{}
	err := f.client.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("re-read pod %s: %v", pod.Name, err)
	}
	return fresh, true
}

// seedInFlightUpdateOperation writes an in-flight Update operation onto the
// InferenceReplica so a ReconcileInput built afterwards observes the state a
// prior reconcile would have left.
func seedInFlightUpdateOperation(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, idx int32, step, targetRev string) {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: legacyIRName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index != idx {
			continue
		}
		ir.Status.InstanceStatuses[i].Phase = v1beta1.OMENativeInstanceUpdating
		ir.Status.InstanceStatuses[i].TargetRevision = targetRev
		ir.Status.InstanceStatuses[i].Operation = &v1beta1.InstanceOperation{
			Type:           v1beta1.InstanceOperationUpdate,
			Step:           step,
			TargetRevision: targetRev,
			StartedAt:      metav1.Now(),
			LastProgressAt: metav1.Now(),
		}
		if err := c.Status().Update(context.Background(), ir); err != nil {
			t.Fatalf("seed in-flight operation: %v", err)
		}
		return
	}
	t.Fatalf("no InstanceStatus for idx=%d", idx)
}

// TestUpdateWithPods_StrategyFlipAtStepSurge_HoldsSourceInRotation pins the
// no-downtime invariant across a mid-roll strategy change.
//
// The Instance has surged a replacement that is not yet Ready, so its source
// is the only pod carrying traffic. SurgeThenDrain holds that source in
// rotation until the replacement reports Ready. Flipping the strategy does
// not change the fact that the replacement is unready, so the source must
// still hold — the surge capacity was already spent, and dropping the source
// now takes the Instance to zero serving pods for the whole time the
// replacement needs to come up.
func TestUpdateWithPods_StrategyFlipAtStepSurge_HoldsSourceInRotation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy workload.UpdateStrategyType
	}{
		{"SurgeThenDrain retained", workload.UpdateStrategySurgeThenDrain},
		{"flipped to InPlaceIfPossible", workload.UpdateStrategyInPlaceIfPossible},
		{"flipped to InPlaceOnly", workload.UpdateStrategyInPlaceOnly},
		{"flipped to RecreatePod", workload.UpdateStrategyRecreatePod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, workload.UpdateStepSurge,
				false /* surgeReady */, true /* sourceServing */)

			if done := f.run(t, tc.strategy); done {
				t.Fatalf("expected done=false: the replacement is not Ready yet")
			}

			source, found := f.livePod(t, f.sourcePod)
			if !found {
				t.Fatalf("source pod was deleted while the replacement was still unready")
			}
			if !podreadiness.IsServing(source) {
				t.Errorf("source left rotation while the replacement was still unready: %+v",
					source.Status.Conditions)
			}
		})
	}
}

// syncFakeKubeletImages advances every pod's runtime container status to the
// images currently in its spec — the fake-kubelet step the in-place path's
// runtime-image proof waits on.
func (f *midSurgeFixture) syncFakeKubeletImages(t *testing.T) {
	t.Helper()
	for _, pod := range []*corev1.Pod{f.sourcePod, f.surgePod} {
		fresh, found := f.livePod(t, pod)
		if !found {
			continue
		}
		statuses := make([]corev1.ContainerStatus, 0, len(fresh.Spec.Containers))
		for _, container := range fresh.Spec.Containers {
			statuses = append(statuses, corev1.ContainerStatus{
				Name:        container.Name,
				Image:       container.Image,
				ContainerID: "containerd://" + container.Image,
			})
		}
		fresh.Status.ContainerStatuses = statuses
		if err := f.client.Status().Update(context.Background(), fresh); err != nil {
			t.Fatalf("advance runtime status for %s: %v", fresh.Name, err)
		}
	}
}

// instancePodCount counts the Instance's surviving pods across both ordinal
// slots.
func (f *midSurgeFixture) instancePodCount(t *testing.T) int {
	t.Helper()
	n := 0
	for _, pod := range []*corev1.Pod{f.sourcePod, f.surgePod} {
		if _, found := f.livePod(t, pod); found {
			n++
		}
	}
	return n
}

// TestUpdateWithPods_StrategyFlipAtStepSurge_ConvergesToOnePod pins the
// steady-state shape a single-pod Instance must reach.
//
// SurgeThenDrain runs two pods only inside its surge window; the promote that
// ends the roll deletes the source and advances ActiveOrdinal onto the
// replacement's slot. A mid-roll flip hands the Instance to a mode that
// reuses one pod name and never learned the other slot exists, so its
// promote leaves both pods alive on the target revision. Neither is an alien
// revision, so the superseded-wreckage sweep does not reclaim them either:
// the Instance settles permanently at double its accelerator footprint.
func TestUpdateWithPods_StrategyFlipAtStepSurge_ConvergesToOnePod(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy workload.UpdateStrategyType
	}{
		{"SurgeThenDrain retained", workload.UpdateStrategySurgeThenDrain},
		{"flipped to InPlaceIfPossible", workload.UpdateStrategyInPlaceIfPossible},
		{"flipped to InPlaceOnly", workload.UpdateStrategyInPlaceOnly},
		{"flipped to RecreatePod", workload.UpdateStrategyRecreatePod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, workload.UpdateStepSurge,
				true /* surgeReady */, true /* sourceServing */)

			const maxPasses = 12
			var done bool
			for pass := 0; pass < maxPasses && !done; pass++ {
				f.syncFakeKubeletImages(t)
				done = f.run(t, tc.strategy)
			}
			if !done {
				t.Fatalf("rollout did not converge in %d passes", maxPasses)
			}

			if got := f.instancePodCount(t); got != 1 {
				t.Errorf("Instance settled with %d pods; a single-pod Instance must converge to 1", got)
			}
		})
	}
}

// TestUpdateWithPods_StrategyFlipAtUncommittedSurge_RollsUnderNewStrategy pins
// the escape hatch: a surge whose replacement has not come up is uncommitted,
// so editing the strategy away from SurgeThenDrain must actually take effect.
// This is the lever for a Component whose surge cannot fit — on a full rack
// there is no room for the extra pod, and switching to a mode that reuses the
// pod in place is the way out. Holding the Instance on the surge machine
// instead would leave the roll wedged until instanceReadyTimeout.
//
// The Instance must converge on the source pod at its original ordinal slot,
// rolled to the target revision, with the abandoned replacement reclaimed.
func TestUpdateWithPods_StrategyFlipAtUncommittedSurge_RollsUnderNewStrategy(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurge,
		false /* surgeReady */, true /* sourceServing */)

	const maxPasses = 12
	var done bool
	for pass := 0; pass < maxPasses && !done; pass++ {
		f.syncFakeKubeletImages(t)
		done = f.run(t, workload.UpdateStrategyInPlaceIfPossible)
	}
	if !done {
		t.Fatalf("rollout did not converge in %d passes after the strategy edit", maxPasses)
	}

	if _, found := f.livePod(t, f.surgePod); found {
		t.Errorf("abandoned replacement survived; the surge budget it holds is never released")
	}
	source, found := f.livePod(t, f.sourcePod)
	if !found {
		t.Fatalf("source pod was reclaimed; the Instance has nothing left serving")
	}
	if got := source.Spec.Containers[0].Image; got != f.targetSpec.Containers[0].Image {
		t.Errorf("source image = %q, want %q: the strategy edit did not roll the Instance",
			got, f.targetSpec.Containers[0].Image)
	}

	status := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if status.Phase != v1beta1.OMENativeInstanceReady || status.RunningRevision != f.targetCR.Name {
		t.Errorf("final status = {phase:%s running:%s}, want {phase:Ready running:%s}",
			status.Phase, status.RunningRevision, f.targetCR.Name)
	}
	if status.ActiveOrdinal != 0 {
		t.Errorf("ActiveOrdinal = %d, want 0: the roll stayed on the source's slot", status.ActiveOrdinal)
	}
}

// TestUpdateWithPods_StrategyFlipAtStepSurgeDrain_FinishesSurgeCycle pins the
// point-of-no-return rule the surge state machine states for itself: past
// Step=Surge the source is already draining, so the cycle must run to
// completion (source deleted, ActiveOrdinal advanced onto the replacement's
// slot) rather than be abandoned partway.
//
// A strategy flip at this point hands the Instance to a mode that has no
// concept of the ordinal slot or of the surge-drain hold on the source. The
// hold is only ever released by deleting the source, so a mode that keeps
// the source alive strands it: alive, occupying an accelerator, and
// permanently out of rotation.
func TestUpdateWithPods_StrategyFlipAtStepSurgeDrain_FinishesSurgeCycle(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy workload.UpdateStrategyType
	}{
		{"SurgeThenDrain retained", workload.UpdateStrategySurgeThenDrain},
		{"flipped to InPlaceIfPossible", workload.UpdateStrategyInPlaceIfPossible},
		{"flipped to InPlaceOnly", workload.UpdateStrategyInPlaceOnly},
		{"flipped to RecreatePod", workload.UpdateStrategyRecreatePod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMidSurgeFixture(t, workload.UpdateStepSurgeDrain,
				true /* surgeReady */, false /* sourceServing */)
			// The surge holds the drained source out of rotation under its own
			// writer key. Only the source's deletion removes that entry.
			if err := podreadiness.MarkPodNotServing(context.Background(), f.client, f.client,
				f.sourcePod, podreadiness.WriterUpdateSurgeDrain, surgeDrainKey(0, 1)); err != nil {
				t.Fatalf("seed surge drain hold: %v", err)
			}

			f.run(t, tc.strategy)

			if _, found := f.livePod(t, f.sourcePod); found {
				t.Errorf("drained source survived the pass: the surge cycle was abandoned past its point of no return")
			}
			// Deletion alone does not prove the surge finished the cycle —
			// recreate deletes the source too, by tearing the Instance down. The
			// Operation still being on a surge step is what separates "the surge
			// ran to completion" from "another mode took the pod away".
			status := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
			if status.Operation != nil && !isSurgeUpdateStep(status.Operation.Step) {
				t.Errorf("Operation.Step = %q: the cycle was handed to another mode instead of finishing",
					status.Operation.Step)
			}
		})
	}
}
