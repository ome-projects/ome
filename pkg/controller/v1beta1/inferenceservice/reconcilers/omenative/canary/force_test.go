package canary

import (
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The stories below drive a four-Instance engine whose canary has a timed
// first step and a final step. One Instance runs version 2 when the operator
// forces the run; three still run version 1.

const forceLimit = 6 * time.Hour

// forcedFixture starts the canary at its first step, serving its split, and
// returns the inputs with the force's time limit configured.
func forcedFixture(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	t.Helper()
	isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
	rec := record.NewFakeRecorder(32)
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.Recorder = rec
	in.ForceMaxDuration = forceLimit
	mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the first step must serve its split before the story starts, got phase %q", phaseOf(isvc))
	}
	drain(rec)
	return isvc, in, rec
}

func requestForce(isvc *v1beta1.InferenceService, runID string) {
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	isvc.Annotations[constants.RolloutForceModeAnnotation] = constants.RolloutForceModeDisruptive
	isvc.Annotations[constants.RolloutForceRunAnnotation] = runID
	isvc.Annotations[constants.RolloutForceReasonAnnotation] = "version 1 fails every request"
}

func drain(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev := <-rec.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func expectForceEvent(t *testing.T, events []string, reason, contains string) {
	t.Helper()
	for _, ev := range events {
		if strings.Contains(ev, reason) && strings.Contains(ev, contains) {
			return
		}
	}
	t.Fatalf("expected a %s event containing %q, got %q", reason, contains, events)
}

func expectForceKeysConsumed(t *testing.T, res *Result) {
	t.Helper()
	for _, key := range forceKeys {
		if !slices.Contains(res.Consume, key) {
			t.Fatalf("the pass must hand %s back for removal, consumed %v", key, res.Consume)
		}
	}
}

func forcedOf(isvc *v1beta1.InferenceService) *v1beta1.CanaryForce {
	return rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).Forced
}

func TestForce_DisruptiveReplacesEveryInstanceAndCompletes(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")

	res := mustReconcile(t, isvc, in)
	f := forcedOf(isvc)
	if !f.Running() || f.Mode != v1beta1.CanaryForceDisruptive || f.RunID != "test" || f.Reason != "version 1 fails every request" {
		t.Fatalf("the force must be recorded as a running disruptive force for the open run, got %+v", f)
	}
	if res.Partition != 0 {
		t.Fatalf("a disruptive force releases every Instance, got partition %d", res.Partition)
	}
	if w := weightOn(isvc, in.Component, "new"); w != 100 {
		t.Fatalf("a disruptive force sends all traffic to version 2, got %d%%", w)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting {
		t.Fatalf("the unit reads Promoting while version 2 comes up, got %q", phaseOf(isvc))
	}
	expectForceEvent(t, drain(rec), EventReasonRolloutForceStarted, "serves nothing until")

	p, ok := EffectivePartition(isvc, rollout.Policies{}, v1beta1.EngineComponent, 4)
	if !ok || *p != 0 {
		t.Fatalf("the projected partition must be 0 while the force runs, got %v %v", p, ok)
	}
	s := UpdateStrategyOverride(isvc, rollout.Policies{}, v1beta1.EngineComponent)
	if s == nil || s.Type != v1beta1.UpdateStrategyRecreatePod || s.RollingUpdate.MaxUnavailable.String() != "100%" {
		t.Fatalf("the replica must roll as RecreatePod with every Instance allowed down, got %+v", s)
	}

	// Boots longer than the step's wait and the ready timeout do not stop it.
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 0}
	in.Now = in.Now.Add(2 * time.Hour)
	mustReconcile(t, isvc, in)
	if !forcedOf(isvc).Running() || phaseOf(isvc) != v1beta1.RolloutPhasePromoting {
		t.Fatalf("no timer may stop a force, got force %+v phase %q", forcedOf(isvc), phaseOf(isvc))
	}

	in.PerRevisionPods = map[string]int32{"new": 4}
	in.Now = in.Now.Add(time.Minute)
	res = mustReconcile(t, isvc, in)
	f = forcedOf(isvc)
	if f.Running() || f.EndReason != v1beta1.CanaryForceCompleted {
		t.Fatalf("the force must end Completed once every Instance serves version 2, got %+v", f)
	}
	if !res.Complete || phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("the run must complete, got result %+v phase %q", res, phaseOf(isvc))
	}
	expectForceKeysConsumed(t, res)
	if UpdateStrategyOverride(isvc, rollout.Policies{}, v1beta1.EngineComponent) != nil {
		t.Fatalf("an ended force leaves the Component's own update strategy")
	}
}

func TestForce_EndsAndHandsTheRequestBack(t *testing.T) {
	cases := []struct {
		name  string
		apply func(isvc *v1beta1.InferenceService, in *ReconcileInputs)
		want  v1beta1.CanaryForceEndReason
	}{
		{"the annotation is removed", func(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
			delete(isvc.Annotations, constants.RolloutForceModeAnnotation)
		}, v1beta1.CanaryForceEndedByRemoval},
		{"the run is removed", func(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
			delete(isvc.Annotations, constants.RolloutForceRunAnnotation)
		}, v1beta1.CanaryForceEndedByRemoval},
		{"the mode changes", func(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
			isvc.Annotations[constants.RolloutForceModeAnnotation] = constants.RolloutForceModeBlueGreen
		}, v1beta1.CanaryForceEndedByChange},
		{"the time limit passes", func(_ *v1beta1.InferenceService, in *ReconcileInputs) {
			in.Now = in.Now.Add(forceLimit)
		}, v1beta1.CanaryForceExpired},
		{"a new run opens", func(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
			isvc.Status.Rollout.ActiveRun.RunID = "next"
		}, v1beta1.CanaryForceEndedByRunClose},
		{"the operator pauses", func(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
			isvc.Annotations[constants.PausedRolloutAnnotation] = "true"
		}, v1beta1.CanaryForceEndedByPause},
		{"the operator rolls back", func(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
			isvc.Annotations[constants.RolloutRollbackAnnotation] = "true"
		}, v1beta1.CanaryForceEndedByRollback},
		{"the configuration is removed", func(_ *v1beta1.InferenceService, in *ReconcileInputs) {
			in.ForceMaxDuration = 0
		}, v1beta1.CanaryForceExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in, rec := forcedFixture(t)
			requestForce(isvc, "test")
			mustReconcile(t, isvc, in)
			drain(rec)

			tc.apply(isvc, &in)
			in.Now = in.Now.Add(time.Second)
			mustReconcile(t, isvc, in)
			f := forcedOf(isvc)
			if f.Running() || f.EndReason != tc.want {
				t.Fatalf("the force must end %s, got %+v", tc.want, f)
			}
			expectForceEvent(t, drain(rec), EventReasonRolloutForceEnded, string(tc.want))
			for _, key := range forceKeys {
				if _, still := isvc.Annotations[key]; still {
					t.Fatalf("an ended force hands %s back for removal", key)
				}
			}
			if UpdateStrategyOverride(isvc, rollout.Policies{}, v1beta1.EngineComponent) != nil {
				t.Fatalf("an ended force leaves the Component's own update strategy")
			}
		})
	}
}

func TestForce_ACrashStopsTheForce(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	drain(rec)

	in.Crash = engineCrash()
	in.Now = in.Now.Add(time.Minute)
	res := mustReconcile(t, isvc, in)
	if f := forcedOf(isvc); f.Running() || f.EndReason != v1beta1.CanaryForceEndedByCrash {
		t.Fatalf("a crash must end the force, got %+v", f)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("a crash stops the canary as any crash does, got phase %q", phaseOf(isvc))
	}
	expectForceKeysConsumed(t, res)
	// Two version 1 pods still serve: the crash sends the traffic back to them.
	if w := weightOn(isvc, in.Component, "old"); w != 100 {
		t.Fatalf("a crash while version 1 pods still serve sends the traffic back to them, got %d%% on version 1", w)
	}
	events := drain(rec)
	expectForceEvent(t, events, EventReasonRolloutForceEnded, "Crash")
	expectForceEvent(t, events, EventReasonCanaryPodCrashed, "back on the stable revision")
}

// crashAfterOldPodsGo starts a force, lets the pass see pods as given, and
// then reports a crashing new pod; it returns the events of the crash pass.
func crashAfterOldPodsGo(t *testing.T, isvc *v1beta1.InferenceService, in ReconcileInputs, rec *record.FakeRecorder, runID string, pods map[string]int32) []string {
	t.Helper()
	requestForce(isvc, runID)
	mustReconcile(t, isvc, in)
	in.PerRevisionPods = pods
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	drain(rec)
	// The force moved every Instance to version 2: the engine's replica
	// keeps none on version 1.
	in.StableInstancesKept = i32(0)
	in.Crash = engineCrash()
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("a crash stops the canary, got phase %q", phaseOf(isvc))
	}
	return drain(rec)
}

func expectNoTrafficBack(t *testing.T, events []string) {
	t.Helper()
	expectForceEvent(t, events, EventReasonCanaryPodCrashed, "stays on revision new")
	for _, ev := range events {
		if strings.Contains(ev, "back on") {
			t.Fatalf("no event may say the traffic went back to version 1, got %q", ev)
		}
	}
}

func TestForce_ACrashAfterEveryOldPodIsGoneKeepsTheTraffic(t *testing.T) {
	cases := []struct {
		name string
		pods map[string]int32
	}{
		{"version 2 pods still serve", map[string]int32{"new": 2}},
		// Nothing serves: routes on version 1, which has no pod, would stay
		// empty after a version 2 pod is Ready again.
		{"no pod is Ready", map[string]int32{"new": 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in, rec := forcedFixture(t)
			events := crashAfterOldPodsGo(t, isvc, in, rec, "test", tc.pods)
			if w := weightOn(isvc, in.Component, "new"); w != 100 {
				t.Fatalf("with no version 1 pod left the traffic stays on version 2, got %d%%", w)
			}
			expectNoTrafficBack(t, events)
		})
	}
}

// One version 1 engine still serves, but every version 1 decoder is gone. A
// version 1 engine cannot serve a request without a version 1 decoder, so
// both members stay on version 2, and the event says so.
func TestForce_ACrashKeepsAPairedUnitTogether(t *testing.T) {
	isvc, in, rec := pdUnitOf(quarterThenAll(), 4, 4, map[string]int32{"new": 1, "old": 3}, 1)
	in.ForceMaxDuration = forceLimit
	mustReconcile(t, isvc, in)
	decoderServes(&in, 2, 0, true)
	decoder := in.Secondaries[v1beta1.DecoderComponent]
	decoder.StableInstancesKept = i32(0)
	in.Secondaries[v1beta1.DecoderComponent] = decoder
	events := crashAfterOldPodsGo(t, isvc, in, rec, "run-1", map[string]int32{"new": 2, "old": 1})
	expectAllTrafficOn(t, isvc, v1beta1.EngineComponent, "new")
	expectAllTrafficOn(t, isvc, v1beta1.DecoderComponent, "decnew")
	expectNoTrafficBack(t, events)
}

func TestForce_ClearsAStopItFinds(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
	in := baseInputs(isvc, map[string]int32{"new": 0, "old": 4})
	in.ForceMaxDuration = forceLimit
	mustReconcile(t, isvc, in) // waits on capacity
	in.Now = in.Now.Add(in.DefaultReadyTimeout)
	mustReconcile(t, isvc, in) // the ready timeout stops it
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("the story needs a stopped canary, got phase %q", phaseOf(isvc))
	}

	requestForce(isvc, "test")
	in.Now = in.Now.Add(time.Minute)
	res := mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if !cs.Forced.Running() || cs.Failed != nil || res.Partition != 0 {
		t.Fatalf("a force must clear the stop and release every Instance, got force %+v failed %+v partition %d", cs.Forced, cs.Failed, res.Partition)
	}
}

func TestForce_RefusedRequestsAreRemoved(t *testing.T) {
	cases := []struct {
		name  string
		value string
		limit time.Duration
		why   string
		mode  string
	}{
		{"it names another run", "other", forceLimit, "the open run is test", constants.RolloutForceModeDisruptive},
		{"the configuration is absent", "test", 0, "forceComplete block", constants.RolloutForceModeDisruptive},
		{"it asks for the blue-green mode", "test", forceLimit, "runs only Disruptive", constants.RolloutForceModeBlueGreen},
		{"it names no run", "", forceLimit, "names no run", constants.RolloutForceModeDisruptive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in, rec := forcedFixture(t)
			in.ForceMaxDuration = tc.limit
			requestForce(isvc, tc.value)
			isvc.Annotations[constants.RolloutForceModeAnnotation] = tc.mode
			res := mustReconcile(t, isvc, in)
			if forcedOf(isvc) != nil {
				t.Fatalf("a refused request records no force, got %+v", forcedOf(isvc))
			}
			expectForceKeysConsumed(t, res)
			expectForceEvent(t, drain(rec), EventReasonRolloutForceRefused, tc.why)
			if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
				t.Fatalf("the canary continues where it stood, got phase %q", phaseOf(isvc))
			}
		})
	}
}

// A force that ends before the last step hands the unit back to that step.
// The force deleted the old pods and the new ones are still booting when the
// operator removes it, so step 0 is short of capacity: the unit reads as a
// step waiting for capacity, not as the final drain, and a timeout there
// stops it as any step 0 timeout does, not as a drain timeout.
func TestForce_AnEarlyEndReturnsToTheStep(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	drain(rec)

	delete(isvc.Annotations, constants.RolloutForceModeAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 0}
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if f := forcedOf(isvc); f.Running() || f.EndReason != v1beta1.CanaryForceEndedByRemoval {
		t.Fatalf("the force must end Removed, got %+v", f)
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if got := rollout.StateOf(cs, phaseOf(isvc), 2); got != rollout.CanaryStateStaging || cs.CurrentStep != 0 {
		t.Fatalf("a force that ended at step 0 waits for step 0's capacity, got state %v phase %q step %d", got, phaseOf(isvc), cs.CurrentStep)
	}
	if cs.CapacityWaitSince == nil {
		t.Fatalf("step 0's capacity wait starts again")
	}

	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
	in.Now = in.Now.Add(time.Minute)
	res := mustReconcile(t, isvc, in)
	if w := weightOn(isvc, in.Component, "new"); w != 50 {
		t.Fatalf("step 0 serves its 50%% split again once its capacity is back, got %d%%", w)
	}
	if res.Partition == 0 || cs.CurrentStep != 0 {
		t.Fatalf("step 0 holds old Instances again, got partition %d step %d", res.Partition, cs.CurrentStep)
	}
}

// A request still on the object after its force ended, because the removal
// did not land, is the same request: it starts no second force.
func TestForce_ALeftoverRequestStartsNoSecondForce(t *testing.T) {
	cases := []struct {
		name string
		end  func(isvc *v1beta1.InferenceService, in *ReconcileInputs)
		want v1beta1.CanaryForceEndReason
	}{
		{"after the time limit", func(_ *v1beta1.InferenceService, in *ReconcileInputs) {
			in.Now = in.Now.Add(forceLimit)
		}, v1beta1.CanaryForceExpired},
		{"after a crash", func(_ *v1beta1.InferenceService, in *ReconcileInputs) {
			in.Crash = engineCrash()
		}, v1beta1.CanaryForceEndedByCrash},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in, rec := forcedFixture(t)
			requestForce(isvc, "test")
			mustReconcile(t, isvc, in)
			tc.end(isvc, &in)
			in.Now = in.Now.Add(time.Second)
			mustReconcile(t, isvc, in)
			if f := forcedOf(isvc); f.Running() || f.EndReason != tc.want {
				t.Fatalf("the force must end %s, got %+v", tc.want, f)
			}
			drain(rec)

			// The removal did not land: the next pass reads the request again.
			requestForce(isvc, "test")
			in.Crash = nil
			in.Now = in.Now.Add(time.Second)
			res := mustReconcile(t, isvc, in)
			if f := forcedOf(isvc); f.Running() || f.EndReason != tc.want {
				t.Fatalf("a leftover request must not start a second force, got %+v", f)
			}
			expectForceKeysConsumed(t, res)
			expectForceEvent(t, drain(rec), EventReasonRolloutForceRefused, "forced at most once")
		})
	}
}

// A force the operator took back by removing the annotation may be asked
// for again on the same run.
func TestForce_ARemovedForceMayBeAskedForAgain(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutForceModeAnnotation)
	in.Now = in.Now.Add(time.Second)
	mustReconcile(t, isvc, in)
	drain(rec)

	requestForce(isvc, "test")
	in.Now = in.Now.Add(time.Second)
	mustReconcile(t, isvc, in)
	if !forcedOf(isvc).Running() {
		t.Fatalf("a new request after a removal starts a force, got %+v", forcedOf(isvc))
	}
	expectForceEvent(t, drain(rec), EventReasonRolloutForceStarted, "run test")
}

// An edited run key ends the force as Changed, not Removed. The edited
// request names another run, so the next pass refuses and removes it.
// Correcting the key afterwards is refused too: only removing the request
// lets the operator ask for the same run again.
func TestForce_ACorrectedKeyStartsNoSecondForce(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	isvc.Annotations[constants.RolloutForceRunAnnotation] = "mistyped"
	in.Now = in.Now.Add(time.Second)
	mustReconcile(t, isvc, in)
	if f := forcedOf(isvc); f.Running() || f.EndReason != v1beta1.CanaryForceEndedByChange {
		t.Fatalf("an edited run key ends the force as Changed, got %+v", f)
	}
	expectForceEvent(t, drain(rec), EventReasonRolloutForceEnded, string(v1beta1.CanaryForceEndedByChange))

	in.Now = in.Now.Add(time.Second)
	expectForceKeysConsumed(t, mustReconcile(t, isvc, in))
	expectForceEvent(t, drain(rec), EventReasonRolloutForceRefused, "names run mistyped")
	for _, key := range forceKeys {
		delete(isvc.Annotations, key)
	}

	requestForce(isvc, "test")
	in.Now = in.Now.Add(time.Second)
	res := mustReconcile(t, isvc, in)
	if forcedOf(isvc).Running() {
		t.Fatalf("a corrected key must not start a second force on the same run")
	}
	expectForceKeysConsumed(t, res)
	expectForceEvent(t, drain(rec), EventReasonRolloutForceRefused, "at most once")
}

// A request made while the rollout is paused is refused and removed, so it
// does not delete every old Instance the moment the pause clears.
func TestForce_APausedRolloutRefusesARequest(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}
	requestForce(isvc, "test")
	res := mustReconcile(t, isvc, in)
	if forcedOf(isvc) != nil {
		t.Fatalf("a paused rollout starts no force, got %+v", forcedOf(isvc))
	}
	expectForceKeysConsumed(t, res)
	expectForceEvent(t, drain(rec), EventReasonRolloutForceRefused, "paused")

	delete(isvc.Annotations, constants.PausedRolloutAnnotation)
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if forcedOf(isvc) != nil {
		t.Fatalf("clearing the pause starts nothing, got %+v", forcedOf(isvc))
	}
}

// The request belongs to the whole service. With two canary groups, the
// group that finishes first leaves the request in place for the group still
// forcing; the last one to finish removes it.
func TestForce_TheRequestStaysUntilEveryGroupIsDone(t *testing.T) {
	isvc := canaryISVC(twoStepTimed(time.Hour), nil)
	isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStepTimed(time.Hour)},
	})
	pinActiveRun(isvc)
	engine := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	engine.ForceMaxDuration = forceLimit
	router := engine
	router.Component = v1beta1.RouterComponent
	mustReconcile(t, isvc, engine)
	mustReconcile(t, isvc, router)

	requestForce(isvc, "test")
	mustReconcile(t, isvc, engine)
	mustReconcile(t, isvc, router)
	routerForce := func() *v1beta1.CanaryForce {
		return rollout.CanaryStatusFor(&isvc.Status, v1beta1.RouterComponent).Forced
	}
	if !forcedOf(isvc).Running() || !routerForce().Running() {
		t.Fatalf("both groups must force, got engine %+v router %+v", forcedOf(isvc), routerForce())
	}

	engine.PerRevisionPods = map[string]int32{"new": 4}
	engine.Now = engine.Now.Add(time.Minute)
	res := mustReconcile(t, isvc, engine)
	if f := forcedOf(isvc); f.Running() || f.EndReason != v1beta1.CanaryForceCompleted {
		t.Fatalf("the engine's force completes, got %+v", f)
	}
	if len(res.Consume) != 0 {
		t.Fatalf("the router still forces, so the engine leaves the request, consumed %v", res.Consume)
	}

	router.Now = engine.Now
	mustReconcile(t, isvc, router)
	if !routerForce().Running() {
		t.Fatalf("the router's force keeps running after the engine's completes, got %+v", routerForce())
	}

	router.PerRevisionPods = map[string]int32{"new": 4}
	router.Now = router.Now.Add(time.Minute)
	res = mustReconcile(t, isvc, router)
	if f := routerForce(); f.Running() || f.EndReason != v1beta1.CanaryForceCompleted {
		t.Fatalf("the router's force completes, got %+v", f)
	}
	expectForceKeysConsumed(t, res)
}

// routerCanaryGroup adds a second canary group, on the router, to isvc.
func routerCanaryGroup(isvc *v1beta1.InferenceService) {
	isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStepTimed(time.Hour)},
	})
}

// engineOnlyRun records that the open run moves the engine to version 2 and
// leaves the router on version 1.
func engineOnlyRun(isvc *v1beta1.InferenceService) {
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		{Component: v1beta1.RouterComponent, Revision: "old", StableRevision: "old"},
	}
}

func TestForce_AGroupTheRunLeavesAloneDoesNotHoldTheRequest(t *testing.T) {
	isvc := canaryISVC(twoStepTimed(time.Hour), nil)
	routerCanaryGroup(isvc)
	pinActiveRun(isvc)
	engineOnlyRun(isvc)
	engine := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	engine.ForceMaxDuration = forceLimit
	router := engine
	router.Component = v1beta1.RouterComponent
	mustReconcile(t, isvc, engine)
	mustReconcile(t, isvc, router)
	if rollout.CanaryStatusFor(&isvc.Status, v1beta1.RouterComponent) != nil {
		t.Fatalf("the run leaves the router alone, so the router starts no canary")
	}

	requestForce(isvc, "test")
	mustReconcile(t, isvc, engine)
	mustReconcile(t, isvc, router)
	engine.PerRevisionPods = map[string]int32{"new": 4}
	engine.Now = engine.Now.Add(time.Minute)
	res := mustReconcile(t, isvc, engine)
	if f := forcedOf(isvc); f.Running() || f.EndReason != v1beta1.CanaryForceCompleted {
		t.Fatalf("the engine's force completes, got %+v", f)
	}
	expectForceKeysConsumed(t, res)
}

// unclaimedForceCase is a service holding a force request, and the refusal
// RefuseUnclaimedForce gives it.
type unclaimedForceCase struct {
	name   string
	build  func() *v1beta1.InferenceService
	refuse string // empty: the request is left to the canary groups
}

func unclaimedForceCases() []unclaimedForceCase {
	return []unclaimedForceCase{
		{
			name: "a service whose rollout has no canary",
			build: func() *v1beta1.InferenceService {
				isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
				isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary = nil
				return isvc
			},
			refuse: "no canary group",
		},
		{
			name: "a run that leaves every canary group alone",
			build: func() *v1beta1.InferenceService {
				isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
				isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
					{Component: v1beta1.EngineComponent, Revision: "old", StableRevision: "old"},
				}
				return isvc
			},
			refuse: "starts no canary",
		},
		{
			// A first rollout has no earlier version, so no canary starts.
			name: "a first rollout with no earlier version",
			build: func() *v1beta1.InferenceService {
				isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
				isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
					{Component: v1beta1.EngineComponent, Revision: "new"},
				}
				return isvc
			},
			refuse: "starts no canary",
		},
		{
			name: "a run that moves a canary group not yet started",
			build: func() *v1beta1.InferenceService {
				isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
				engineOnlyRun(isvc)
				return isvc
			},
		},
		{
			// The run records no earlier version, but the group's canary is
			// already running, with its stable revision read from the replica.
			name: "a canary group with a record the run moves",
			build: func() *v1beta1.InferenceService {
				isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
				rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{CanaryRevisionHash: "new"})
				isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
					{Component: v1beta1.EngineComponent, Revision: "new"},
				}
				return isvc
			},
		},
		{
			// An earlier run left a record; the open run takes the group back
			// to its stable revision, so the record runs no canary for it.
			name: "a canary group with an old record the run leaves alone",
			build: func() *v1beta1.InferenceService {
				isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
				rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{CanaryRevisionHash: "new"})
				isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
					{Component: v1beta1.EngineComponent, Revision: "old", StableRevision: "old"},
				}
				return isvc
			},
			refuse: "starts no canary",
		},
	}
}

func TestRefuseUnclaimedForce(t *testing.T) {
	for _, tc := range unclaimedForceCases() {
		t.Run(tc.name, func(t *testing.T) {
			isvc := tc.build()
			requestForce(isvc, "test")
			rec := record.NewFakeRecorder(8)
			keys := RefuseUnclaimedForce(isvc, rollout.Policies{}, rec)
			if tc.refuse == "" {
				if len(keys) != 0 {
					t.Fatalf("a canary group can still act on the request, but it was removed: %v", keys)
				}
				return
			}
			if !slices.Equal(keys, forceKeys) {
				t.Fatalf("both force annotations must be removed, got %v", keys)
			}
			expectForceEvent(t, drain(rec), EventReasonRolloutForceRefused, tc.refuse)
		})
	}

	if keys := RefuseUnclaimedForce(pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil)), rollout.Policies{}, nil); keys != nil {
		t.Fatalf("a service with no request has nothing to remove, got %v", keys)
	}
}

// A force that ends in the same pass a crash is seen, or earlier in the run,
// leaves a run whose version 1 Instances may all be gone. The crash still
// takes the forced-crash path: the traffic stays on version 2, where pods
// will come back, rather than going to a version 1 with no pods.
func TestForce_ACrashAfterTheForceEndsKeepsTheTraffic(t *testing.T) {
	cases := []struct {
		name      string
		end       func(isvc *v1beta1.InferenceService, in *ReconcileInputs)
		want      v1beta1.CanaryForceEndReason
		laterPass bool
	}{
		{"removed in the crash's pass", func(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
			delete(isvc.Annotations, constants.RolloutForceModeAnnotation)
		}, v1beta1.CanaryForceEndedByRemoval, false},
		{"expired in the crash's pass", func(_ *v1beta1.InferenceService, in *ReconcileInputs) {
			in.Now = in.Now.Add(forceLimit)
		}, v1beta1.CanaryForceExpired, false},
		{"removed a pass before the crash", func(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
			delete(isvc.Annotations, constants.RolloutForceModeAnnotation)
		}, v1beta1.CanaryForceEndedByRemoval, true},
		// Another component's push opens a new run and keeps this unit's
		// record at the target it holds; the force ends RunClosed, and the
		// crash guard still knows the old Instances may be gone.
		{"another component's push opens a new run", func(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
			isvc.Status.Rollout.ActiveRun.RunID = "next"
		}, v1beta1.CanaryForceEndedByRunClose, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in, rec := forcedFixture(t)
			requestForce(isvc, "test")
			mustReconcile(t, isvc, in)
			in.PerRevisionPods = map[string]int32{"new": 0}
			in.Now = in.Now.Add(time.Minute)
			mustReconcile(t, isvc, in)
			drain(rec)

			tc.end(isvc, &in)
			in.StableInstancesKept = i32(0)
			if tc.laterPass {
				// The force ended a pass earlier and handed the step back its
				// own share; the crash below is what this story checks.
				in.Now = in.Now.Add(time.Minute)
				mustReconcile(t, isvc, in)
				drain(rec)
			}
			in.Crash = engineCrash()
			in.Now = in.Now.Add(time.Minute)
			mustReconcile(t, isvc, in)
			if f := forcedOf(isvc); f.Running() || f.EndReason != tc.want {
				t.Fatalf("the force ends %s, got %+v", tc.want, f)
			}
			if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
				t.Fatalf("a crash stops the canary, got phase %q", phaseOf(isvc))
			}
			if w := weightOn(isvc, in.Component, "new"); w != 100 {
				t.Fatalf("with no version 1 pod left the traffic stays on version 2, got %d%%", w)
			}
			expectNoTrafficBack(t, drain(rec))
		})
	}
}

// A resume rebuilds the canary record. The force record stays, so a request
// still on the object after a forced crash is refused instead of replacing
// every Instance again, and a later crash still knows the old ones may be
// gone.
func TestForce_AResumeKeepsTheForceRecord(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	in.Crash = engineCrash()
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if f := forcedOf(isvc); f.EndReason != v1beta1.CanaryForceEndedByCrash {
		t.Fatalf("the crash ends the force, got %+v", f)
	}

	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	resumeCanaryStatus(isvc, in.Component, cs, cs.TargetID, 0, cs.ObservedTrafficWeight, in.Now)
	if f := forcedOf(isvc); f == nil || f.EndReason != v1beta1.CanaryForceEndedByCrash {
		t.Fatalf("the resume keeps the force record, got %+v", f)
	}

	drain(rec)
	in.Crash = nil
	requestForce(isvc, "test")
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if forcedOf(isvc).Running() {
		t.Fatalf("a leftover request after a resume starts no second force, got %+v", forcedOf(isvc))
	}
	expectForceEvent(t, drain(rec), EventReasonRolloutForceRefused, "forced at most once")
}

// When a new run retargets the unit, the record is rebuilt for the new target
// before the canary pass runs. A force still running then ends as RunClosed,
// with its event, instead of vanishing with the old record.
func TestForce_ARetargetEndsARunningForceAsRunClosed(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	drain(rec)

	isvc.Status.Rollout.ActiveRun.RunID = "next"
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	resetCanaryStatus(isvc, in.Component, cs, "t2", "new", in.Now)
	if !forcedOf(isvc).Running() {
		t.Fatalf("the rebuilt record keeps the running force until the pass ends it, got %+v", forcedOf(isvc))
	}
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if f := forcedOf(isvc); f.Running() || f.EndReason != v1beta1.CanaryForceEndedByRunClose {
		t.Fatalf("the force ends RunClosed, got %+v", f)
	}
	expectForceEvent(t, drain(rec), EventReasonRolloutForceEnded, string(v1beta1.CanaryForceEndedByRunClose))
}

// The replicas are written before the canary pass. SettleForces ends a force
// the pass no longer allows first, so the projection never writes partition 0
// and the 100% budget for one more pass.
func TestSettleForces(t *testing.T) {
	cases := []struct {
		name  string
		apply func(isvc *v1beta1.InferenceService, now *time.Time, limit *time.Duration)
		want  v1beta1.CanaryForceEndReason
	}{
		{"the mode key is removed", func(isvc *v1beta1.InferenceService, _ *time.Time, _ *time.Duration) {
			delete(isvc.Annotations, constants.RolloutForceModeAnnotation)
		}, v1beta1.CanaryForceEndedByRemoval},
		{"the time limit passes", func(_ *v1beta1.InferenceService, now *time.Time, _ *time.Duration) {
			*now = now.Add(forceLimit)
		}, v1beta1.CanaryForceExpired},
		{"the configuration is removed", func(_ *v1beta1.InferenceService, _ *time.Time, limit *time.Duration) {
			*limit = 0
		}, v1beta1.CanaryForceExpired},
		{"a new run opens", func(isvc *v1beta1.InferenceService, _ *time.Time, _ *time.Duration) {
			isvc.Status.Rollout.ActiveRun.RunID = "next"
		}, v1beta1.CanaryForceEndedByRunClose},
		{"the operator pauses", func(isvc *v1beta1.InferenceService, _ *time.Time, _ *time.Duration) {
			isvc.Annotations[constants.PausedRolloutAnnotation] = "true"
		}, v1beta1.CanaryForceEndedByPause},
		{"the operator rolls back", func(isvc *v1beta1.InferenceService, _ *time.Time, _ *time.Duration) {
			isvc.Annotations[constants.RolloutRollbackAnnotation] = "true"
		}, v1beta1.CanaryForceEndedByRollback},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in, _ := forcedFixture(t)
			requestForce(isvc, "test")
			mustReconcile(t, isvc, in)
			now, limit := in.Now.Add(time.Minute), forceLimit
			tc.apply(isvc, &now, &limit)

			rec := record.NewFakeRecorder(8)
			consumed := SettleForces(isvc, rollout.Policies{}, rec, now, limit)
			if f := forcedOf(isvc); f.Running() || f.EndReason != tc.want {
				t.Fatalf("the force ends %s before the replicas are written, got %+v", tc.want, f)
			}
			expectForceEvent(t, drain(rec), EventReasonRolloutForceEnded, string(tc.want))
			if p, ok := EffectivePartition(isvc, rollout.Policies{}, v1beta1.EngineComponent, 4); ok && *p == 0 {
				t.Fatalf("an ended force projects no partition 0")
			}
			if UpdateStrategyOverride(isvc, rollout.Policies{}, v1beta1.EngineComponent) != nil {
				t.Fatalf("an ended force projects no 100%% budget")
			}
			for _, key := range consumed {
				if _, still := isvc.Annotations[key]; still {
					t.Fatalf("a removed key %s is gone in memory too, so the pass does not read it again", key)
				}
			}
		})
	}

	isvc, in, _ := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	if consumed := SettleForces(isvc, rollout.Policies{}, nil, in.Now.Add(time.Minute), forceLimit); len(consumed) != 0 || !forcedOf(isvc).Running() {
		t.Fatalf("a force still allowed keeps running, got %+v and removed %v", forcedOf(isvc), consumed)
	}
}

// A force on an earlier run leaves its record on the unit. A later ordinary
// canary still treats a crash as any canary does: while its version 1 pods
// exist but are briefly not Ready the traffic rests on the pods that serve,
// and once they are Ready again it goes back to them.
func TestForce_AnEarlierForceLeavesALaterCrashToThePods(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	cs.Forced = &v1beta1.CanaryForce{Mode: v1beta1.CanaryForceDisruptive, RunID: "earlier",
		StartedAt: metav1.Time{Time: in.Now.Add(-time.Hour)}, EndedAt: &metav1.Time{Time: in.Now.Add(-time.Minute)},
		EndReason: v1beta1.CanaryForceCompleted}
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 0}
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{in.Component: {"new": 2, "old": 2}}
	in.Crash = engineCrash()
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("a crash stops the canary, got phase %q", phaseOf(isvc))
	}
	for _, ev := range drain(rec) {
		if strings.Contains(ev, "no Instance of some member is left") {
			t.Fatalf("version 1 still has pods, so the event may not say none is left, got %q", ev)
		}
	}

	in.Crash = nil
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{in.Component: {"new": 2, "old": 2}}
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if w := weightOn(isvc, in.Component, "new"); w == 100 {
		t.Fatalf("once version 1 pods serve again the traffic leaves the failing version 2, got %d%% on version 2", w)
	}
}

// A force ends when a new run opens. A request the operator already wrote
// for the new run is not the old force's: it stays, and starts a force on
// the new run.
func TestForce_EndingAnOldForceKeepsARequestForTheNewRun(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	drain(rec)

	isvc.Status.Rollout.ActiveRun.RunID = "next"
	requestForce(isvc, "next")
	in.Now = in.Now.Add(time.Minute)
	res := mustReconcile(t, isvc, in)
	if f := forcedOf(isvc); f.RunID != "test" || f.EndReason != v1beta1.CanaryForceEndedByRunClose {
		t.Fatalf("the old force ends RunClosed, got %+v", f)
	}
	if len(res.Consume) != 0 || isvc.Annotations[constants.RolloutForceRunAnnotation] != "next" {
		t.Fatalf("the request for the new run stays, consumed %v, annotations %v", res.Consume, isvc.Annotations)
	}

	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if f := forcedOf(isvc); !f.Running() || f.RunID != "next" {
		t.Fatalf("the request starts a force on the new run, got %+v", f)
	}
}

// A new run whose plan no longer has the unit's canary group leaves nothing
// to end a running force there. SettleForces ends it as RunClosed, with its
// event, before the unit's record is released.
func TestSettleForces_EndsAForceWhoseGroupIsGone(t *testing.T) {
	isvc, in, _ := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary = nil

	rec := record.NewFakeRecorder(8)
	SettleForces(isvc, rollout.Policies{}, rec, in.Now.Add(time.Minute), forceLimit)
	if f := forcedOf(isvc); f.Running() || f.EndReason != v1beta1.CanaryForceEndedByRunClose {
		t.Fatalf("the force ends RunClosed, got %+v", f)
	}
	expectForceEvent(t, drain(rec), EventReasonRolloutForceEnded, "no longer has the canary group")
}

// An ordinary canary whose version 1 pods are briefly gone, as while their
// Instances are rebuilt, still keeps those Instances on version 1, so a crash
// behaves as on any canary: no exception applies, and once the rebuilt
// version 1 pods are Ready the traffic goes back to them.
func TestCrash_RebuiltStablePodsTakeTheTrafficBack(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	in.PerRevisionPods = map[string]int32{"new": 2}
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{in.Component: {"new": 2}}
	in.StableInstancesKept = i32(2)
	in.Crash = engineCrash()
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	expectForceEvent(t, drain(rec), EventReasonCanaryPodCrashed, "back on the stable revision")

	in.Crash = nil
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{in.Component: {"new": 2, "old": 2}}
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if w := weightOn(isvc, in.Component, "new"); w == 100 {
		t.Fatalf("rebuilt version 1 pods that serve take the traffic back, got %d%% on version 2", w)
	}
}

// A force on a prefill/decode unit is the engine's. SettleForces reads each
// member's own record, so the decoder's lookup never reaches the engine's
// force and ends a force that is still allowed.
func TestSettleForces_KeepsAPairedUnitsForce(t *testing.T) {
	isvc, in, rec := pdUnitOf(quarterThenAll(), 4, 4, map[string]int32{"new": 1, "old": 3}, 1)
	in.ForceMaxDuration = forceLimit
	mustReconcile(t, isvc, in)
	requestForce(isvc, "run-1")
	mustReconcile(t, isvc, in)
	if !forcedOf(isvc).Running() {
		t.Fatalf("the force starts on the paired unit, got %+v", forcedOf(isvc))
	}
	drain(rec)
	settled := record.NewFakeRecorder(8)
	if consumed := SettleForces(isvc, rollout.Policies{}, settled, in.Now.Add(time.Minute), forceLimit); len(consumed) != 0 {
		t.Fatalf("an allowed force keeps its request, removed %v", consumed)
	}
	if !forcedOf(isvc).Running() {
		t.Fatalf("the paired unit's force keeps running, got %+v", forcedOf(isvc))
	}
	if events := drain(settled); len(events) != 0 {
		t.Fatalf("nothing ends, so nothing is reported, got %q", events)
	}
}

// The replica's count of Instances headed for the stable revision includes
// the ones serving and the ones being rebuilt there, and leaves out the ones
// a roll is moving to another revision. An observation that is not a
// replica's reads as unknown.
func TestInstancesHeadedFor(t *testing.T) {
	rows := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, RunningRevision: "svc-engine-old"},
		{Index: 1, RunningRevision: "svc-engine-old", TargetRevision: "svc-engine-old"},
		{Index: 2, RunningRevision: "svc-engine-old", TargetRevision: "svc-engine-new"},
		{Index: 3, RunningRevision: "svc-engine-new"},
	}
	r := observedCanaryRevisions{targetHash: "new", rows: rows, fromIR: true}
	if got := r.instancesHeadedFor("old"); got == nil || *got != 2 {
		t.Fatalf("two Instances stay on version 1, got %v", got)
	}
	if got := r.instancesHeadedFor("new"); got == nil || *got != 2 {
		t.Fatalf("two Instances run or move to version 2, got %v", got)
	}
	if got := (observedCanaryRevisions{rows: rows}).instancesHeadedFor("old"); got != nil {
		t.Fatalf("an observation that is not a replica's reads as unknown, got %v", *got)
	}
}

// An ordinary canary that keeps its version 1 Instances crashes as any
// canary does, even when nothing serves on either side for the moment: the traffic
// goes back to version 1, whose Instances are being rebuilt.
func TestCrash_AnOrdinaryCanaryWithNothingServingGoesBackToStable(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 0}
	in.StableInstancesKept = i32(2)
	in.Crash = engineCrash()
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if w := weightOn(isvc, in.Component, "old"); w != 100 {
		t.Fatalf("the traffic goes back to version 1, got %d%%", w)
	}
	expectForceEvent(t, drain(rec), EventReasonCanaryPodCrashed, "back on the stable revision")
}

// An ordinary canary can keep no version 1 Instance, as when its final step
// releases the last one. No force drove it, so a crash there behaves as on
// any canary: the traffic goes back to the stable revision.
func TestCrash_AnUnforcedCanaryKeepingNoStableInstanceGoesBackToStable(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 0}
	in.StableInstancesKept = i32(0)
	in.Crash = engineCrash()
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if w := weightOn(isvc, in.Component, "old"); w != 100 {
		t.Fatalf("the traffic goes back to version 1, as without any force, got %d%%", w)
	}
	expectForceEvent(t, drain(rec), EventReasonCanaryPodCrashed, "back on the stable revision")
}

// A group the open run leaves alone can still carry a record from an earlier
// run that stopped short of its last step. It starts no force of its own, and
// it does not keep the request on the object after the group the run moves
// has finished with it.
func TestForce_AnIdleGroupWithAnOldRecordDoesNotHoldTheRequest(t *testing.T) {
	isvc := canaryISVC(twoStepTimed(time.Hour), nil)
	routerCanaryGroup(isvc)
	pinActiveRun(isvc)
	engineOnlyRun(isvc)
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.RouterComponent, &v1beta1.CanaryStatus{
		TargetID: "earlier", CanaryRevisionHash: "old", StableRevisionHash: "older",
	})
	engine := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	engine.ForceMaxDuration = forceLimit
	router := engine
	router.Component = v1beta1.RouterComponent
	router.CanaryRevisionHash, router.StableRevisionHash = "old", "old"
	mustReconcile(t, isvc, engine)

	requestForce(isvc, "test")
	mustReconcile(t, isvc, engine)
	mustReconcile(t, isvc, router)
	if f := rollout.CanaryStatusFor(&isvc.Status, v1beta1.RouterComponent).Forced; f != nil {
		t.Fatalf("a group the run leaves on its stable revision starts no force, got %+v", f)
	}
	engine.PerRevisionPods = map[string]int32{"new": 4}
	engine.Now = engine.Now.Add(time.Minute)
	res := mustReconcile(t, isvc, engine)
	if f := forcedOf(isvc); f.Running() || f.EndReason != v1beta1.CanaryForceCompleted {
		t.Fatalf("the engine's force completes, got %+v", f)
	}
	expectForceKeysConsumed(t, res)
}

// The recorded weight is the share the step holds. A force sends all traffic
// to version 2 but leaves that record alone, so a force that ends early hands
// the step back with its own split: the version 1 pods the step rebuilds
// take their share again while version 2 is still short of the step's
// capacity.
func TestForce_AnEarlyEndKeepsTheStepsShare(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	drain(rec)

	in.PerRevisionPods = map[string]int32{"new": 1, "old": 0}
	in.Now = in.Now.Add(forceLimit)
	mustReconcile(t, isvc, in)
	if f := forcedOf(isvc); f.Running() || f.EndReason != v1beta1.CanaryForceExpired {
		t.Fatalf("the force must end Expired, got %+v", f)
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if cs.ObservedTrafficWeight != 50 {
		t.Fatalf("the record keeps step 0's 50%% share, got %d", cs.ObservedTrafficWeight)
	}

	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if w := weightOn(isvc, in.Component, "new"); w != 50 {
		t.Fatalf("a rebuilt version 1 pod takes the step's share back, got %d%% on version 2", w)
	}
}

// A force that ends at the final step leaves the step to shift its traffic
// to 100% itself, so the final step's drain and gate are measured from that
// shift, not from when the step was entered.
func TestForce_AnEarlyEndAtTheFinalStepRestartsItsClock(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	in.Now = in.Now.Add(time.Hour)
	mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if cs.CurrentStep != 1 || cs.ObservedTrafficWeight != 50 {
		t.Fatalf("the story needs the final step short of its capacity, got step %d weight %d", cs.CurrentStep, cs.ObservedTrafficWeight)
	}
	requestForce(isvc, "test")
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutForceModeAnnotation)
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	drain(rec)

	in.PerRevisionPods = map[string]int32{"new": 4}
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	if cs.StepEnteredTime == nil || !cs.StepEnteredTime.Time.Equal(in.Now) {
		t.Fatalf("the final step's clock starts when its traffic shifts, got %v want %v", cs.StepEnteredTime, in.Now)
	}
}

// A rollback brings the version 1 Instances back, so the force no longer
// decides where a crash goes: a later canary on the same revision crashes
// as one no force drove.
func TestForce_ARollbackUndoesTheForceForLaterCrashes(t *testing.T) {
	isvc, in, rec := forcedFixture(t)
	requestForce(isvc, "test")
	mustReconcile(t, isvc, in)
	in.Crash = engineCrash()
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	in.Crash = nil
	cs := rollout.CanaryStatusFor(&isvc.Status, in.Component)
	if !forcedThisCanary(cs) {
		t.Fatalf("the story needs a canary the force drove, got %+v", cs.Forced)
	}

	isvc.Annotations[constants.RolloutRollbackAnnotation] = "true"
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	drain(rec)
	if cs.RolledBackRevisionHash == "" {
		t.Fatalf("the story needs the canary rolled back")
	}
	if forcedThisCanary(cs) {
		t.Fatalf("a rollback undoes the force for later crashes, got %+v", cs.Forced)
	}
	if f := cs.Forced; f == nil || f.RunID != "test" || f.EndReason != v1beta1.CanaryForceEndedByCrash {
		t.Fatalf("the record still says which run was forced and how it ended, got %+v", f)
	}
}
