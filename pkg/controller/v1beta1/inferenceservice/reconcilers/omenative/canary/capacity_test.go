package canary

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The count a canary sizes its steps against is the replica's own: its live
// spec.replicas when it carries one, else the Component's declared minimum,
// else its maximum; an undeclared Component is zero.
func TestDesiredReplicas(t *testing.T) {
	n := 4
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: &n, MaxReplicas: 6}
	if got := DesiredReplicas(ptr.To(int32(10)), ext); got != 10 {
		t.Fatalf("the live count wins → 10, got %d", got)
	}
	if got := DesiredReplicas(nil, ext); got != 4 {
		t.Fatalf("no live count → the declared minimum 4, got %d", got)
	}
	ext.MinReplicas = nil
	if got := DesiredReplicas(nil, ext); got != 6 {
		t.Fatalf("no minimum → the declared maximum 6, got %d", got)
	}
	if got := DesiredReplicas(nil, nil); got != 0 {
		t.Fatalf("an undeclared Component → 0, got %d", got)
	}
}

// rearmedFinalStep is an engine canary re-armed onto a fixed image after its
// first target parked on an image that never started: the run toward the
// fixed revision is pinned, the ladder sits at its final step with the first
// step's split still programmed, the capacity wait began longer ago than the
// plan's ready timeout, and the replica has rolled every Instance onto the
// fixed revision and promoted it. lagging is how many of those Instances'
// pods the pod view does not yet report Ready.
func rearmedFinalStep(t *testing.T, name string, lagging int) (*v1beta1.InferenceService, DispatchDeps) {
	t.Helper()
	ns := "default"
	const replicas = 8
	n := replicas
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 20 * time.Second}}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	isvc := canaryISVC(steps, i32(60))
	isvc.Namespace, isvc.Name = ns, name
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n, MaxReplicas: replicas}}
	isvc.Spec.Rollout.Groups[0].Canary.ReadyTimeout = &metav1.Duration{Duration: 90 * time.Second}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "fixed", StableRevision: "old"},
	}
	isvc.Status.Rollout.LastRun = &v1beta1.RolloutRunRecord{
		Outcome:         v1beta1.RolloutRunSuperseded,
		TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "broken", StableRevision: "old"}},
	}
	g := rollout.CanaryGroup(isvc, rollout.Policies{})
	start := time.Unix(1000, 0)
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		TargetID:              activeCanaryTargetID(isvc, g),
		CanaryRevisionHash:    "fixed",
		StableRevisionHash:    "old",
		CurrentStep:           1,
		StepEnteredTime:       &metav1.Time{Time: start},
		CapacityWaitSince:     &metav1.Time{Time: start.Add(10 * time.Second)},
		ObservedTrafficWeight: 50,
	})
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePending)
	setTraffic(isvc, v1beta1.EngineComponent, coordination.BuildTrafficTargets(isvc.Name, v1beta1.EngineComponent,
		canaryWeights("fixed", "old", "", "", 50)))

	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "fixed")
	engineIR.Status.CurrentRevision = engineIR.Status.UpdateRevision
	engineIR.Spec.Replicas = ptr.To(int32(replicas))
	objs := []client.Object{isvc, engineIR,
		canaryControllerRevision(ns, isvc.Name, "engine", "old", 1),
		canaryControllerRevision(ns, isvc.Name, "engine", "broken", 2),
		canaryControllerRevision(ns, isvc.Name, "engine", "fixed", 3),
	}
	for i := 0; i < replicas; i++ {
		row := v1beta1.OMENativeInstanceStatus{
			Index: int32(i), Phase: v1beta1.OMENativeInstanceReady,
			RunningRevision: engineIR.Status.UpdateRevision, PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1,
		}
		// The Instances the first target surged keep its failure on record
		// and serve from the surge ordinal.
		ordinal := "0"
		if i >= replicas/2 {
			ordinal = "1"
			row.LastFailure = &v1beta1.InstanceTermination{PodName: fmt.Sprintf("%s-engine-%d-default-1", isvc.Name, i), Reason: "ErrImagePull"}
		}
		engineIR.Status.InstanceStatuses = append(engineIR.Status.InstanceStatuses, row)
		pod := canaryRunnerPod(ns, isvc.Name, "engine", "fixed", fmt.Sprintf("%s-engine-%d-default-%s", isvc.Name, i, ordinal), v1beta1.RunnerNameDefault, ordinal)
		pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(i)
		if i < lagging {
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
		}
		objs = append(objs, pod)
	}
	c := fakeclient.New(t, fakeclient.WithObjects(objs...))
	deps := DispatchDeps{
		Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: g,
		Requeue: reconcileRequeue, ParkedRequeue: failedRequeue, Now: start.Add(2 * time.Minute),
	}
	return isvc, deps
}

// expectFinalStepCutover asserts one pass moved the final step's traffic
// onto the canary revision without parking, and that the drain window then
// completes the canary.
func expectFinalStepCutover(t *testing.T, isvc *v1beta1.InferenceService, deps DispatchDeps) {
	t.Helper()
	ctx := context.Background()
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch (final step): %v", err)
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.Failed != nil {
		t.Fatalf("a replica serving every Instance on the canary revision must not park, got %+v", cs.Failed)
	}
	if got := phaseOf(isvc); got != v1beta1.RolloutPhasePromoting {
		t.Fatalf("the final step must shift traffic onto the canary revision, got phase %q", got)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "fixed")
	if cs.ObservedTrafficWeight != 100 || cs.CurrentStep != 1 {
		t.Fatalf("100%% must be programmed at the final step, got %+v", cs)
	}
	deps.Now = deps.Now.Add(61 * time.Second)
	out, err := Dispatch(ctx, deps)
	if err != nil {
		t.Fatalf("Dispatch (drain elapsed): %v", err)
	}
	if got := phaseOf(isvc); got != v1beta1.RolloutPhaseStable {
		t.Fatalf("the elapsed drain window must complete the canary, got %q", got)
	}
	// The run layer closes the run only on a pass that reads this
	// completion, and the service's own status write wakes nothing.
	if out.RequeueAfter != reconcileRequeue {
		t.Fatalf("the completing pass must come back at the step cadence so the run can close, got %+v", out)
	}
}

// A pod Ready on the canary revision one workload reconcile before the
// replica publishes its row is not capacity yet. The pass does not wait on a
// pod event that already came: it comes back at the step cadence, and the
// replica's row write re-enqueues the service sooner than that.
func TestDispatch_RowsOnePassBehindReadyPodsWakeAtTheStepCadence(t *testing.T) {
	ns, name := "default", "rows-behind"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace, isvc.Name = ns, name
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	engineIR := ir(ns, name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = name + "-engine-old"
	engineIR.Spec.Replicas = ptr.To(int32(4))
	// The replica's account still has every Instance on the stable revision
	// while two pods already serve the canary revision.
	for i := int32(0); i < 4; i++ {
		engineIR.Status.InstanceStatuses = append(engineIR.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: i, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: name + "-engine-old",
			PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1,
		})
	}
	objs := []client.Object{isvc, engineIR,
		canaryPod(ns, name, "engine", "old", name+"-engine-0"),
		canaryPod(ns, name, "engine", "old", name+"-engine-1"),
		canaryPod(ns, name, "engine", "new", name+"-engine-2"),
		canaryPod(ns, name, "engine", "new", name+"-engine-3"),
		canaryControllerRevision(ns, name, "engine", "old", 1),
		canaryControllerRevision(ns, name, "engine", "new", 2),
	}
	c := fakeclient.New(t, fakeclient.WithObjects(objs...))
	deps := DispatchDeps{
		Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{}),
		Requeue: reconcileRequeue, ParkedRequeue: failedRequeue, Now: time.Unix(1000, 0),
	}
	ctx := context.Background()
	out, err := Dispatch(ctx, deps)
	if err != nil {
		t.Fatalf("Dispatch (rows behind): %v", err)
	}
	if got := phaseOf(isvc); got != v1beta1.RolloutPhasePending {
		t.Fatalf("rows the replica has not published are not capacity, got phase %q", got)
	}
	if out.RequeueAfter != reconcileRequeue {
		t.Fatalf("a gate short on unpublished rows must come back at the step cadence, got %+v", out)
	}

	// The replica publishes the rows one pass later: the step serves.
	syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch (rows published): %v", err)
	}
	if got := phaseOf(isvc); got != v1beta1.RolloutPhasePaused {
		t.Fatalf("the published rows fill the step, got phase %q", got)
	}
}

// A canary at its final step whose replica has rolled every Instance onto
// the canary revision and promoted it is at capacity: the step's gate passes
// and the final traffic write lands, however long the capacity wait ran
// before the fleet finished rolling. A capacity timeout is a verdict on
// capacity that never arrived, never on capacity that is serving.
func TestDispatch_FullyRolledReplicaCutsOverAtTheFinalStep(t *testing.T) {
	isvc, deps := rearmedFinalStep(t, "rearm", 0)
	expectFinalStepCutover(t, isvc, deps)
}

// The replica's account of its Instances is the final step's capacity: with
// every Instance serving on the canary revision, a pod view that has not yet
// folded the last surge pods' readiness gates neither holds the step nor
// spends the capacity budget on capacity that is already serving.
func TestDispatch_FinalStepCountsServingInstancesOverALaggingPodView(t *testing.T) {
	isvc, deps := rearmedFinalStep(t, "rearm-lag", 2)
	expectFinalStepCutover(t, isvc, deps)
}

// finishedUnit is a completed canary whose run has closed, as the controller
// leaves it: the done sentinel, phase Stable, traffic on the canary revision
// and no pre-canary identity.
func finishedUnit(t *testing.T, name string) (*v1beta1.InferenceService, DispatchDeps) {
	t.Helper()
	isvc, deps := rearmedFinalStep(t, name, 0)
	expectFinalStepCutover(t, isvc, deps)
	isvc.Status.Rollout.LastRun = &v1beta1.RolloutRunRecord{
		Outcome:         v1beta1.RolloutRunCompleted,
		TargetRevisions: isvc.Status.Rollout.ActiveRun.TargetRevisions,
	}
	isvc.Status.Rollout.ActiveRun = nil
	deps.Now = deps.Now.Add(time.Minute)
	return isvc, deps
}

// expectFinishedUnitUntouched asserts a pass over a finished unit left it
// finished: the done sentinel, no pre-canary identity, phase Stable, all
// traffic on the canary revision, and nothing more asked of the controller.
func expectFinishedUnitUntouched(t *testing.T, isvc *v1beta1.InferenceService, out Outcome) {
	t.Helper()
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CurrentStep != 2 || cs.StableRevisionHash != "" || cs.RolledBackRevisionHash != "" {
		t.Fatalf("a finished unit keeps the done sentinel and no pre-canary identity, got %+v", cs)
	}
	if got := phaseOf(isvc); got != v1beta1.RolloutPhaseStable {
		t.Fatalf("a finished unit keeps reading Stable, got %q", got)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "fixed")
	if out.RequeueAfter != 0 || out.Requeue {
		t.Fatalf("a finished unit asks for nothing more, got %+v", out)
	}
}

// A negative step written by hand into a finished unit's record is read back
// as the done sentinel on the next pass: nothing re-runs, the stable identity
// stays dropped and the pass asks for nothing more.
func TestDispatch_FinishedUnitReadsANegativeStepBackAsTheSentinel(t *testing.T) {
	isvc, deps := finishedUnit(t, "rearm-neg")
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).CurrentStep = -1
	out, err := Dispatch(context.Background(), deps)
	if err != nil {
		t.Fatalf("Dispatch (negative step written): %v", err)
	}
	expectFinishedUnitUntouched(t, isvc, out)
}

// A stable identity equal to the canary hash written by hand into a finished
// unit's record is dropped again on the next pass, with no second completion:
// the record reads finished and the pass asks for nothing more.
func TestDispatch_FinishedUnitDropsAWrittenStableIdentity(t *testing.T) {
	isvc, deps := finishedUnit(t, "rearm-ident")
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).StableRevisionHash = "fixed"
	before := isvc.Status.Components[v1beta1.EngineComponent].PreviousRolledoutRevision
	out, err := Dispatch(context.Background(), deps)
	if err != nil {
		t.Fatalf("Dispatch (identity written): %v", err)
	}
	expectFinishedUnitUntouched(t, isvc, out)
	if got := isvc.Status.Components[v1beta1.EngineComponent].PreviousRolledoutRevision; got != before {
		t.Fatalf("a written identity must not record a second promotion, got %q after %q", got, before)
	}
}
