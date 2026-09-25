package ops

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestRecreateUpdate_WaitsForPodReadyAfterEnablingServing: with the old pods
// gone and the recreated pod ContainersReady + serving, promotion still waits
// for kubelet to fold the serving gate into PodReady. Promotion releases the
// Instance's unavailability budget slot, so promoting before the pod is
// routable would let the next Instance drain first.
func TestRecreateUpdate_WaitsForPodReadyAfterEnablingServing(t *testing.T) {
	tests := []struct {
		name      string
		podReady  bool
		wantDone  bool
		wantPhase v1beta1.OMENativeInstancePhase
	}{
		{name: "serving gate set but PodReady not observed", podReady: false, wantDone: false, wantPhase: v1beta1.OMENativeInstanceUpdating},
		{name: "PodReady observed", podReady: true, wantDone: true, wantPhase: v1beta1.OMENativeInstanceReady},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			legacyResetExpectations(t)
			isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
			c := legacyNewFakeClient(t, isvc, ir)
			targetSpec := legacyTargetSpecImage("llama:v2")
			target := legacyEnsureTargetCR(t, c, isvc, targetSpec)

			// Mid-recreate state: Phase A drained and deleted the incarnation-1
			// pod, Phase B created the incarnation-2 pod; only Phase C is left.
			if err := legacyMutateInstance(c, isvc, workload.ComponentEngine)(context.Background(), 0, func(s *workload.InstanceStatus) bool {
				s.Incarnation = 2
				s.Phase = workload.InstancePhaseUpdating
				s.TargetRevision = target.Name
				s.Operation = &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepDrain, TargetRevision: target.Name}
				return true
			}); err != nil {
				t.Fatalf("seed in-flight recreate: %v", err)
			}

			pod := legacyPodAtIncarnation(isvc, 0, 2, true /* ContainersReady */, true /* serving */)
			pod.Spec = *targetSpec.DeepCopy()
			if tc.podReady {
				pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
			}
			if err := c.Create(context.Background(), pod); err != nil {
				t.Fatalf("create replacement pod: %v", err)
			}

			input := legacyTestInput(isvc, c, workload.ComponentEngine)
			plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
			done, err := recreateUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target, []*corev1.Pod{pod})
			if err != nil {
				t.Fatalf("recreateUpdate: %v", err)
			}
			if done != tc.wantDone {
				t.Fatalf("done: got %t, want %t", done, tc.wantDone)
			}
			statuses := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)
			if len(statuses) != 1 {
				t.Fatalf("instance statuses: got %d, want 1", len(statuses))
			}
			if statuses[0].Phase != tc.wantPhase {
				t.Fatalf("phase: got %q, want %q", statuses[0].Phase, tc.wantPhase)
			}
		})
	}
}

// TestRecreateUpdate_PromotionWaitsForMinReadySeconds: with the old pods
// gone and the recreated pod serving and PodReady, promotion (which clears
// the Operation and releases the unavailability budget slot) waits until
// the pod has been Ready for the window.
func TestRecreateUpdate_PromotionWaitsForMinReadySeconds(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	newPod := legacyPodAtIncarnation(isvc, 0, 2, true, true)
	newPod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	podReadyAt(newPod, minReadyWindowStart.Add(-5*time.Second))
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir, newPod)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	// Mid-recreate state: Phase A drained and deleted the incarnation-1 pod,
	// Phase B created the incarnation-2 pod; only Phase C (promotion) is left.
	fresh := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: legacyIRName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, fresh); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	fresh.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:          0,
		Incarnation:    2,
		Phase:          v1beta1.OMENativeInstanceUpdating,
		TargetRevision: tcr.Name,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepDrain, TargetRevision: tcr.Name,
		},
	}
	if err := c.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("seed mid-recreate status: %v", err)
	}

	clk := clocktesting.NewFakeClock(minReadyWindowStart)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clk
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	plan.MinReadySeconds = 20

	done, err := recreateUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, []*corev1.Pod{newPod})
	if err != nil {
		t.Fatalf("recreateUpdate inside window: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while the recreated pod is inside the minReadySeconds window")
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating || s.Operation == nil {
		t.Fatalf("promoted inside the window: phase=%q operation=%+v", s.Phase, s.Operation)
	}

	clk.SetTime(minReadyWindowStart.Add(15 * time.Second))
	done, err = recreateUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, []*corev1.Pod{newPod})
	if err != nil {
		t.Fatalf("recreateUpdate after window: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true once the recreated pod became Available")
	}
	s = legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady || s.RunningRevision != tcr.Name || s.Operation != nil {
		t.Fatalf("promotion after window: phase=%q runningRevision=%q operation=%+v", s.Phase, s.RunningRevision, s.Operation)
	}
}

// TestRecreateDrain_ReadyOldIncarnationPodIsNotThePromoteSignal: Phase A
// still owns a pod of the superseded Incarnation. That pod being PodReady
// and serving is what makes it worth draining, not evidence the roll is
// done — so the pass drains and deletes it and promotes nothing.
func TestRecreateDrain_ReadyOldIncarnationPodIsNotThePromoteSignal(t *testing.T) {
	f := recreateDrainInFlightFixture(t)
	old := legacyPodAtIncarnation(f.isvc, 0, 1, true /* ready */, true /* serving */)
	old.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	if err := f.c.Create(context.Background(), old); err != nil {
		t.Fatalf("seed the old-incarnation pod: %v", err)
	}
	f.pod = old

	got := f.run(t, []*corev1.Pod{old}, nil)
	if got.done {
		t.Errorf("done: got true want false (Phase A is not finished)")
	}
	if got.phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", got.phase)
	}
	if got.step != workload.UpdateStepDrain {
		t.Errorf("Operation.Step: got %q want %q", got.step, workload.UpdateStepDrain)
	}
	if got.podSurvives {
		t.Errorf("the ready old-incarnation pod survived Phase A; readiness does not exempt it from the drain")
	}
}

// TestRecreateDrain_PodObservationsThatDecideNothing: the recreate
// promotes over one bar — the bumped set complete, serving and PodReady
// past its availability window — and re-derives it every wake-up. A pod
// wedged Terminating, one whose node is gone, and one in a terminal phase
// all fail that bar in the same way: the roll waits, and only its
// operation deadline ends it.
func TestRecreateDrain_PodObservationsThatDecideNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		// incarnation the observed pod carries: 1 is a Phase A leftover,
		// 2 is a rebuilt member Phase C is waiting on.
		incarnation int64
		observe     func(t *testing.T, f *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod
	}{
		{
			name:        "an old-incarnation pod wedged Terminating",
			incarnation: 1,
			observe: func(t *testing.T, f *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod {
				return terminatingPod(t, f.c, pod)
			},
		},
		{
			name:        "a rebuilt pod whose node is gone",
			incarnation: 2,
			observe: func(_ *testing.T, _ *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod {
				out := pod.DeepCopy()
				out.Spec.NodeName = "node-that-no-longer-exists"
				return out
			},
		},
		{
			name:        "a rebuilt pod in a terminal Failed phase",
			incarnation: 2,
			observe: func(_ *testing.T, _ *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod {
				out := pod.DeepCopy()
				out.Status.Phase = corev1.PodFailed
				return out
			},
		},
		{
			name:        "a rebuilt pod in a terminal Succeeded phase",
			incarnation: 2,
			observe: func(_ *testing.T, _ *nonSurgeFixture, pod *corev1.Pod) *corev1.Pod {
				out := pod.DeepCopy()
				out.Status.Phase = corev1.PodSucceeded
				return out
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := recreateDrainInFlightFixture(t)
			pod := legacyPodAtIncarnation(f.isvc, 0, tc.incarnation, true /* ContainersReady */, false /* not serving */)
			pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
			if err := f.c.Create(context.Background(), pod); err != nil {
				t.Fatalf("seed the observed pod: %v", err)
			}
			f.pod = pod

			got := f.run(t, []*corev1.Pod{tc.observe(t, f, pod)}, nil)
			if got.done {
				t.Errorf("done: got true want false (none of these clears the promote bar)")
			}
			if got.phase != v1beta1.OMENativeInstanceUpdating {
				t.Errorf("Phase: got %q want Updating", got.phase)
			}
			if got.step != workload.UpdateStepDrain {
				t.Errorf("Operation.Step: got %q want %q", got.step, workload.UpdateStepDrain)
			}
			if got.failed {
				t.Errorf("LastFailure: got one want none; only the operation deadline ends this roll")
			}
			if got.blocks != 0 {
				t.Errorf("RetryBlock writes: got %d want none", got.blocks)
			}
		})
	}
}

// TestRecreateDrain_OperatorConfigChangesDoNotChangeWhatItDecides: same
// claim as the in-place half — the recreate consults none of these knobs
// while it converges, so the row, the pod set and the pass's answer come
// out identical to the unedited pass.
func TestRecreateDrain_OperatorConfigChangesDoNotChangeWhatItDecides(t *testing.T) {
	// A Phase C row: the rebuilt pod is ContainersReady but not yet
	// PodReady, so the pass has real work (the serving flip) and a real
	// wait to re-derive.
	seed := func(t *testing.T) (*nonSurgeFixture, *corev1.Pod) {
		t.Helper()
		f := recreateDrainInFlightFixture(t)
		pod := legacyPodAtIncarnation(f.isvc, 0, 2, true /* ContainersReady */, false /* not serving */)
		pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
		if err := f.c.Create(context.Background(), pod); err != nil {
			t.Fatalf("seed the rebuilt pod: %v", err)
		}
		f.pod = pod
		return f, pod
	}

	baseline, basePod := seed(t)
	want := baseline.run(t, []*corev1.Pod{basePod}, nil)

	for _, tc := range configChangeCases() {
		t.Run(tc.name, func(t *testing.T) {
			f, pod := seed(t)
			got := f.run(t, []*corev1.Pod{pod}, tc.tweak)
			if got != want {
				t.Errorf("the %s edit moved the roll: got %+v want the unedited %+v", tc.name, got, want)
			}
		})
	}
}

// TestRecreateDrain_ConflictOnTheServingFlipRetriesFromAFreshRead: Phase
// C's serving flip pins the resourceVersion of the read it was computed
// from, so a concurrent writer on the same pod rejects the patch. The
// row is not a casualty of that: it keeps its phase, its operation and
// its Incarnation, and the pass simply has to run again.
func TestRecreateDrain_ConflictOnTheServingFlipRetriesFromAFreshRead(t *testing.T) {
	f := recreateDrainInFlightFixture(t)
	pod := legacyPodAtIncarnation(f.isvc, 0, 2, true /* ContainersReady */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	if err := f.c.Create(context.Background(), pod); err != nil {
		t.Fatalf("seed the rebuilt pod: %v", err)
	}

	base, ok := f.c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", f.c)
	}
	conflicting := interceptor.NewClient(base, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return apierrors.NewConflict(corev1.Resource("pods"), obj.GetName(),
					errors.New("the object has been modified; please apply your changes to the latest version"))
			}
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})

	in := f.input()
	plan := legacyComponentPlan(f.strategy, nil)
	deps := workload.Deps{Client: conflicting, Recorder: record.NewFakeRecorder(16)}
	done, err := UpdateWithPods(context.Background(), deps, in, plan, plan.Instances[0], f.targetCR, f.targetSpec,
		[]*corev1.Pod{pod})
	if done {
		t.Errorf("done: got true want false (the serving flip never landed)")
	}
	if err == nil {
		t.Errorf("error: got nil want the conflict surfaced so the pass runs again")
	}

	s := legacyInstanceStatusesOnIR(f.c, f.isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating (a conflict costs a pass, not the attempt)", s.Phase)
	}
	if s.Operation == nil || s.Operation.Step != workload.UpdateStepDrain {
		t.Errorf("Operation: got %+v want the recreate intact", s.Operation)
	}
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2", s.Incarnation)
	}
	if len(*f.blocks) != 0 {
		t.Errorf("RetryBlock writes: got %v want none (a conflict blames no revision)", *f.blocks)
	}
}
