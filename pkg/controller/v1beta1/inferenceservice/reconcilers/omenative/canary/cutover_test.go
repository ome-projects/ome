package canary

import (
	"context"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// expectSoleTarget asserts one Component's traffic rests entirely on one
// revision.
func expectSoleTarget(t *testing.T, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) {
	t.Helper()
	want := coordination.PerRevisionServiceName(isvc.Name, comp, hash)
	tr := isvc.Status.Components[comp].Traffic
	if len(tr) != 1 || tr[0].RevisionName != want || tr[0].Percent != 100 {
		t.Fatalf("%s traffic must rest on %s alone, got %+v", comp, hash, tr)
	}
}

// expectReleased asserts the pass that releases the held floor, or any pass
// while the released instance is still rolling: the done sentinel is set, the
// projected partition is 0, the unit stays Promoting rather than Stable, and
// the pass asks to come back, since the roll it waits on moves no annotation
// and no step.
func expectReleased(t *testing.T, isvc *v1beta1.InferenceService, res *Result, steps int) {
	t.Helper()
	cs := isvc.Status.Canary
	if cs == nil || int(cs.CurrentStep) != steps {
		t.Fatalf("the release must set the done sentinel, got %+v", cs)
	}
	if !res.Active || res.Complete || res.Partition != 0 {
		t.Fatalf("the release must stay active at partition 0 without completing, got %+v", res)
	}
	if res.RequeueAfter == 0 && !res.Requeue {
		t.Fatalf("the release must ask to come back for the roll it started, got %+v", res)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting {
		t.Fatalf("a unit with an instance still on the stable revision is not Stable, got %q", phaseOf(isvc))
	}
	if cs.StableRevisionHash == "" {
		t.Fatal("the pre-canary identity is kept until nothing runs it")
	}
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(4)}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 0 {
		t.Fatalf("the done sentinel must project partition 0, got %v", fmtPartition(p))
	}
}

// expectStableAfterRoll asserts the one pass that completes the cutover: the
// unit reads Stable, the pass is the Complete edge and the pre-canary
// identity is dropped.
func expectStableAfterRoll(t *testing.T, isvc *v1beta1.InferenceService, res *Result) {
	t.Helper()
	if !res.Active || !res.Complete || res.Partition != 0 {
		t.Fatalf("every instance on the canary revision must complete the cutover, got %+v", res)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("a unit entirely on the canary revision reads Stable, got %q", phaseOf(isvc))
	}
	if isvc.Status.Canary.StableRevisionHash != "" {
		t.Fatalf("completion drops the pre-canary identity, got %q", isvc.Status.Canary.StableRevisionHash)
	}
}

// The final step keeps the last stable instance serving until the traffic
// shift lands and the drain window elapses: the stable revision carries the
// previous step's weight until the 100% write, so its capacity cannot go
// before the write, and the drain window is the wait between that write and
// the scale-down. The step stages every other instance, shifts onto the
// canary capacity that is Ready, the done sentinel releases the hold, and the
// unit reads Stable only once the released instance has rolled.
func TestReconcile_FinalStepHoldsTheLastStableInstanceThroughTheDrain(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), i32(60)))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in) // step 0 serves its split, manual hold
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in) // promoted to the final step
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)

	// The final step's partition keeps one instance on stable while the
	// stable revision still carries traffic.
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: intPtr(4)}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
		t.Fatalf("final step must hold the last stable instance (partition 1) before traffic moves, got %v", fmtPartition(p))
	}

	// Three canary instances Ready is all the hold allows; that is the
	// step's capacity, and the 100% write moves onto it.
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	res := mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || res.Complete {
		t.Fatalf("100%% traffic must move onto the Ready canary capacity, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")
	if res.Partition != 1 {
		t.Fatalf("the stable instance stays held through the drain window, got partition %d", res.Partition)
	}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
		t.Fatalf("projected partition must keep the stable instance through the drain, got %v", fmtPartition(p))
	}

	// Inside the drain window nothing is released.
	in.Now = in.Now.Add(30 * time.Second)
	if res := mustReconcile(t, isvc, in); res.Complete || res.Partition != 1 {
		t.Fatalf("drain window must keep the hold, got %+v", res)
	}

	// The window elapses: the sentinel releases the last instance to roll.
	// The unit is not Stable yet; that instance still serves the stable
	// revision.
	in.Now = in.Now.Add(31 * time.Second)
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")

	// The roll is in flight: the replacement is Ready while the stable
	// instance drains. Still not Stable.
	in.PerRevisionPods = map[string]int32{"new": 4, "old": 1}
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)

	// The stable instance is gone: the cutover is complete.
	in.PerRevisionPods = map[string]int32{"new": 4}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))

	// A finished canary is inert, and a canary pod lost after completion is
	// the workload's repair, not a return to Promoting.
	in.PerRevisionPods = map[string]int32{"new": 3}
	if res := mustReconcile(t, isvc, in); res.Active || phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("capacity lost after completion must leave the unit Stable and inactive, got phase=%q res=%+v", phaseOf(isvc), res)
	}
}

// The pass that releases the floor can also be the pass that completes: a
// unit whose every instance is already on the canary revision when the drain
// elapses has nothing left to roll.
func TestReconcile_DrainElapsedWithNothingHeldCompletesAtOnce(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), i32(60)))
	in := baseInputs(isvc, map[string]int32{"old": 1})
	in.TargetID = "t1"
	in.DesiredReplicas = 1
	mustReconcile(t, isvc, in) // a single instance stages in place
	in.PerRevisionPods = map[string]int32{"new": 1}
	mustReconcile(t, isvc, in) // step 0 serves, manual hold
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	if res := mustReconcile(t, isvc, in); res.Complete || phaseOf(isvc) != v1beta1.RolloutPhasePromoting {
		t.Fatalf("100%% traffic opens the drain window, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	in.Now = in.Now.Add(61 * time.Second)
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}

// A two-member unit completes when every member has rolled: the primary on
// the canary revision alone does not make the unit Stable while a secondary
// still serves an instance on its stable revision or is short of its
// capacity.
func TestReconcile_CutoverCompletesWhenEverySecondaryHasRolled(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	in.Secondaries = map[v1beta1.ComponentType]MemberRevisions{
		v1beta1.DecoderComponent: {CanaryRevisionHash: "dnew", StableRevisionHash: "dold", ReadyCanaryCapacity: 2, DesiredReplicas: 4},
	}
	in.GroupReadyPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{
		v1beta1.DecoderComponent: {"dnew": 2, "dold": 2},
	}
	mustReconcile(t, isvc, in)
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)

	// The primary rolled entirely; the decoder still holds its last stable
	// instance.
	in.PerRevisionPods = map[string]int32{"new": 4}
	in.Secondaries[v1beta1.DecoderComponent] = MemberRevisions{CanaryRevisionHash: "dnew", StableRevisionHash: "dold", ReadyCanaryCapacity: 3, DesiredReplicas: 4}
	in.GroupReadyPerRevisionPods[v1beta1.DecoderComponent] = map[string]int32{"dnew": 3, "dold": 1}
	mustReconcile(t, isvc, in) // 100% traffic moves; the release reads the next pass's count
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)

	// The decoder's replacement is Ready but its stable instance still serves.
	in.Secondaries[v1beta1.DecoderComponent] = MemberRevisions{CanaryRevisionHash: "dnew", StableRevisionHash: "dold", ReadyCanaryCapacity: 4, DesiredReplicas: 4}
	in.GroupReadyPerRevisionPods[v1beta1.DecoderComponent] = map[string]int32{"dnew": 4, "dold": 1}
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 2)

	in.GroupReadyPerRevisionPods[v1beta1.DecoderComponent] = map[string]int32{"dnew": 4}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
	if got := isvc.Status.Components[v1beta1.DecoderComponent].LatestRolledoutRevision; got == "" {
		t.Fatal("completion records the decoder's promoted revision")
	}
}

// A done sentinel with no pre-canary identity on record has nothing left to
// roll and reads Stable at once, whatever the pod set looks like.
func TestReconcile_DoneWithoutStableIdentityReadsStable(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	isvc.Status.Canary = &v1beta1.CanaryStatus{TargetID: "t1", CanaryRevisionHash: "new", CurrentStep: 2, ObservedTrafficWeight: 100}
	in := baseInputs(isvc, map[string]int32{"new": 3, "old": 1})
	in.TargetID = "t1"
	if res := mustReconcile(t, isvc, in); res.Active || phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("a done sentinel without a stable identity reads Stable, got phase=%q res=%+v", phaseOf(isvc), res)
	}
}

// A single-step plan cuts over the same way: all but one instance stage on
// the canary revision, traffic moves once they are Ready, the undelayed
// cutover releases the last stable instance on the next pass, on that
// pass's own capacity count, and the unit reads Stable once that instance
// has rolled.
func TestReconcile_SingleStepPlanStagesAllButTheLastInstance(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}
	isvc := pinActiveRun(canaryISVC(steps, nil))
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.TargetID = "t1"
	if res := mustReconcile(t, isvc, in); res.Partition != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("staging must hold one stable instance, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	if res := mustReconcile(t, isvc, in); res.Partition != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePromoting {
		t.Fatalf("100%% traffic moves with the stable instance still held, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")
	expectReleased(t, isvc, mustReconcile(t, isvc, in), 1)
	in.PerRevisionPods = map[string]int32{"new": 4}
	expectStableAfterRoll(t, isvc, mustReconcile(t, isvc, in))
}

// A Component with a single instance has nothing to hold back: it stages
// in place as its only slot, and the ladder runs as declared.
func TestReconcile_SingleInstanceComponentStagesInPlace(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"old": 1})
	in.DesiredReplicas = 1
	in.TargetID = "t1"
	if res := mustReconcile(t, isvc, in); res.Partition != 0 {
		t.Fatalf("a single instance rolls at step 0, got %+v", res)
	}
}

// The dispatcher-level cutover: the pass that finds the staged canary
// capacity Ready moves 100% traffic onto it while the projected partition
// still holds the last stable instance; the drain window then elapses, the
// sentinel releases it, and the unit reads Stable once the released instance
// is on the canary revision.
func TestDispatch_CutoverMovesTrafficBeforeReleasingStableCapacity(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), i32(60))
	isvc.Namespace, isvc.Name = ns, "cutover"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	g := rollout.CanaryGroup(isvc)
	start := time.Unix(1000, 0)
	// The final step was just entered: the split served 50/50 at step 0.
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		TargetID:              activeCanaryTargetID(isvc, g),
		CanaryRevisionHash:    "new",
		StableRevisionHash:    "old",
		CurrentStep:           1,
		StepEnteredTime:       &metav1.Time{Time: start},
		ObservedTrafficWeight: 50,
	})
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePending)
	setTraffic(isvc, v1beta1.EngineComponent, coordination.BuildTrafficTargets(isvc.Name, v1beta1.EngineComponent,
		canaryWeights("new", "old", "", "", 50)))

	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-old"
	// The held instance is still on stable; every other instance is Ready
	// on the canary revision.
	objs := []runtime.Object{isvc, engineIR,
		canaryPod(ns, isvc.Name, "engine", "old", isvc.Name+"-engine-0"),
		canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-1"),
		canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-2"),
		canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-3"),
		canaryControllerRevision(ns, isvc.Name, "engine", "old", 1),
		canaryControllerRevision(ns, isvc.Name, "engine", "new", 2),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: g, Now: start.Add(time.Minute)}
	ext := &isvc.Spec.Engine.ComponentExtensionSpec

	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch (cutover): %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePromoting {
		t.Fatalf("staged capacity Ready must shift 100%% traffic, got phase %q", got)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 1 {
		t.Fatalf("the last stable instance stays held while traffic settles, got partition %v", fmtPartition(p))
	}

	deps.Now = deps.Now.Add(61 * time.Second)
	out, err := Dispatch(ctx, deps)
	if err != nil {
		t.Fatalf("Dispatch (drain elapsed): %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePromoting {
		t.Fatalf("drain elapsed releases the held instance but is not Stable while it runs the stable revision, got phase %q", got)
	}
	if p := StepPartition(isvc, v1beta1.EngineComponent, ext); p == nil || *p != 0 {
		t.Fatalf("the release must project partition 0, got partition %v", fmtPartition(p))
	}
	if out.RequeueAfter == 0 && !out.Requeue {
		t.Fatalf("the release must ask to come back for the roll it started, got %+v", out)
	}

	// The held instance rolls onto the canary revision.
	if err := c.Delete(ctx, canaryPod(ns, isvc.Name, "engine", "old", isvc.Name+"-engine-0")); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-0")); err != nil {
		t.Fatal(err)
	}
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch (rolled): %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhaseStable {
		t.Fatalf("every instance on the canary revision completes the canary, got phase %q", got)
	}
	if cs := isvc.Status.Canary; cs == nil || cs.CurrentStep != 2 || cs.StableRevisionHash != "" {
		t.Fatalf("completion keeps the done sentinel and drops the pre-canary identity, got %+v", cs)
	}
}

// A secondary's capacity gate at the final step asks for every instance but
// the one its own hold keeps on stable, as the primary's does.
func TestSecondaryCapacityReady_FinalStepStagesAllButTheHeldInstance(t *testing.T) {
	ns := "default"
	n4 := 4
	pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd"}}
	pd.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	pd.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "rtr", StableRevisionHash: "rtrOld", CurrentStep: 1}
	engineIR := ir(ns, "pd", v1beta1.EngineComponent, "eng")
	engineIR.Status.CurrentRevision = "pd-engine-engOld"
	reads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).
		WithRuntimeObjects(ir(ns, "pd", v1beta1.RouterComponent, "rtr"), engineIR).Build()
	staged := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 3, "rtrOld": 1},
		v1beta1.EngineComponent: {"eng": 3, "engOld": 1},
	}
	if !mustSecondaryReady(t, context.Background(), reads, pd, staged, staged, v1beta1.RouterComponent) {
		t.Fatal("final step: a secondary with every instance but the held one Ready on its canary revision is staged")
	}
	short := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 3, "rtrOld": 1},
		v1beta1.EngineComponent: {"eng": 2, "engOld": 2},
	}
	if mustSecondaryReady(t, context.Background(), reads, pd, short, short, v1beta1.RouterComponent) {
		t.Fatal("final step: a secondary short of its staged count is not ready")
	}
}

func intPtr(v int) *int { return &v }

// fmtPartition renders a projected partition for a failure message.
func fmtPartition(p *int32) string {
	if p == nil {
		return "<nil>"
	}
	return strconv.Itoa(int(*p))
}
