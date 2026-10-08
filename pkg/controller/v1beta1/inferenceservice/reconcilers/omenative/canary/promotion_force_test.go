package canary

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// revisionPercent is the share the primary's traffic targets give hash; a
// revision with no weighted target has none.
func revisionPercent(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) int32 {
	for _, target := range isvc.Status.Components[comp].Traffic {
		if query.RevisionFromName(target.RevisionName).Hash() == hash {
			return target.Percent
		}
	}
	return 0
}

// drainEvents returns every event the recorder holds.
func drainEvents(rec *record.FakeRecorder) []string {
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

func expectEvent(t *testing.T, rec *record.FakeRecorder, reason string, fragments ...string) {
	t.Helper()
	events := drainEvents(rec)
	for _, ev := range events {
		if !strings.Contains(ev, reason) {
			continue
		}
		for _, f := range fragments {
			if !strings.Contains(ev, f) {
				t.Errorf("%s event %q lacks %q", reason, ev, f)
			}
		}
		return
	}
	t.Fatalf("expected a %s event, got %v", reason, events)
}

func expectNoEvent(t *testing.T, rec *record.FakeRecorder, reason string) {
	t.Helper()
	for _, ev := range drainEvents(rec) {
		if strings.Contains(ev, reason) {
			t.Fatalf("unexpected %s event %q", reason, ev)
		}
	}
}

// stagingShort arms a manual two-step ladder and parks it on step 0's
// capacity gate with one of the two staged canary pods serving, so the pass
// that follows reads a running capacity-wait clock.
func stagingShort(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	t.Helper()
	isvc := canaryISVC(twoStep(), nil)
	rec := record.NewFakeRecorder(8)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 3})
	in.Recorder = rec
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending || isvc.Status.Canary.CapacityWaitSince == nil {
		t.Fatalf("the ladder must be waiting on step 0's capacity, got phase=%q canary=%+v", phaseOf(isvc), isvc.Status.Canary)
	}
	in.Now = in.Now.Add(time.Minute)
	return isvc, in, rec
}

// A forced promote at a gated step whose capacity is short advances the
// step in one pass: the force is recorded and handed back like a promote,
// the capacity-wait clock is cleared, the forced advance is announced with
// the Ready canary capacity, and the step's traffic follows the serving
// rule, so the one serving canary pod carries the step's share.
func TestReconcile_ForcedPromoteAdvancesACapacityShortStep(t *testing.T) {
	isvc, in, rec := stagingShort(t)
	annotate(isvc, constants.RolloutPromoteForceAnnotation, "new")

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if !res.Stepped || cs.CurrentStep != 1 {
		t.Fatalf("the force must advance the capacity-short step in one pass, got stepped=%v step=%d", res.Stepped, cs.CurrentStep)
	}
	if cs.PromotedThrough != "new" {
		t.Fatalf("the advance must record the force in PromotedThrough, got %q", cs.PromotedThrough)
	}
	if cs.CapacityWaitSince != nil {
		t.Fatalf("the force must clear the capacity-wait clock, got %v", cs.CapacityWaitSince)
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteForceAnnotation {
		t.Fatalf("the force must be handed back for removal after the flush, got %v", res.Consume)
	}
	expectEvent(t, rec, EventReasonCanaryStepForced, "Warning", "step 0", "new", "1 of 2")
	if got := revisionPercent(isvc, v1beta1.EngineComponent, "new"); got != 50 {
		t.Fatalf("the serving canary pod must carry the step's share, got %d%%", got)
	}
	expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
}

// A plain promote at the same capacity-short step still waits: the promote
// opens a gate the step has reached, never the capacity gate in front of
// it, so the step holds Pending with its clock running and the annotation
// stays for the pass that meets capacity.
func TestReconcile_PromoteStillWaitsForCapacity(t *testing.T) {
	isvc, in, rec := stagingShort(t)
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if res.Stepped || cs.CurrentStep != 0 || phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("a promote must not lift the capacity wait, got stepped=%v step=%d phase=%q", res.Stepped, cs.CurrentStep, phaseOf(isvc))
	}
	if cs.CapacityWaitSince == nil {
		t.Fatal("the capacity-wait clock must keep running under a promote")
	}
	if len(res.Consume) != 0 || isvc.Annotations[constants.RolloutPromoteAnnotation] != "new" {
		t.Fatalf("the promote must stay for the pass that meets capacity, got consume=%v annotations=%v", res.Consume, isvc.Annotations)
	}
	if got := revisionPercent(isvc, v1beta1.EngineComponent, "new"); got != 0 {
		t.Fatalf("no traffic may reach the canary while its step is staging, got %d%%", got)
	}
	expectNoEvent(t, rec, EventReasonCanaryStepForced)
}

// A force whose value is not the canary revision is inert: the step keeps
// waiting with its clock running and the annotation is left alone.
func TestReconcile_StaleForceIsInert(t *testing.T) {
	isvc, in, rec := stagingShort(t)
	annotate(isvc, constants.RolloutPromoteForceAnnotation, "someoldhash")

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if res.Stepped || cs.CurrentStep != 0 || cs.CapacityWaitSince == nil {
		t.Fatalf("a stale force must not act, got stepped=%v step=%d clock=%v", res.Stepped, cs.CurrentStep, cs.CapacityWaitSince)
	}
	if len(res.Consume) != 0 || isvc.Annotations[constants.RolloutPromoteForceAnnotation] != "someoldhash" {
		t.Fatalf("a stale force is left alone, got consume=%v annotations=%v", res.Consume, isvc.Annotations)
	}
	expectNoEvent(t, rec, EventReasonCanaryStepForced)
}

// A force whose value is already recorded in PromotedThrough is inert: the
// advance it applied persisted, so the lingering annotation opens nothing
// and lifts nothing; it is handed back for removal again, and the next
// step's capacity wait runs its clock.
func TestReconcile_RecordedForceIsInert(t *testing.T) {
	isvc := canaryISVC(threeStep(), nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new",
		StableRevisionHash: "old",
		CurrentStep:        1,
		PromotedThrough:    "new",
		StepEnteredTime:    &metav1.Time{Time: time.Unix(1000, 0)},
	}
	annotate(isvc, constants.RolloutPromoteForceAnnotation, "new")
	rec := record.NewFakeRecorder(8)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 3})
	in.Recorder = rec

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if res.Stepped || cs.CurrentStep != 1 {
		t.Fatalf("a recorded force must not advance again, got stepped=%v step=%d", res.Stepped, cs.CurrentStep)
	}
	if cs.CapacityWaitSince == nil || phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("a recorded force lifts no wait, got clock=%v phase=%q", cs.CapacityWaitSince, phaseOf(isvc))
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteForceAnnotation {
		t.Fatalf("the lingering force must be handed back for removal, got %v", res.Consume)
	}
	expectNoEvent(t, rec, EventReasonCanaryStepForced)
}

// expectForceIgnored asserts a pass consumed the force without acting on it:
// the step did not move, the force is handed back for removal, and the
// event says why it did nothing.
func expectForceIgnored(t *testing.T, res *Result, cs *v1beta1.CanaryStatus, rec *record.FakeRecorder, step int32, why string) {
	t.Helper()
	if res.Stepped || cs.CurrentStep != step {
		t.Fatalf("the force must not advance, got stepped=%v step=%d want %d", res.Stepped, cs.CurrentStep, step)
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteForceAnnotation {
		t.Fatalf("the force must be consumed, got %v", res.Consume)
	}
	events := drainEvents(rec)
	for _, ev := range events {
		if strings.Contains(ev, EventReasonCanaryStepForced) {
			t.Fatalf("no forced advance may be announced, got %q", ev)
		}
		if strings.Contains(ev, EventReasonCanaryForceIgnored) && strings.Contains(ev, "Warning") && strings.Contains(ev, why) {
			return
		}
	}
	t.Fatalf("expected a %s event saying %q, got %v", EventReasonCanaryForceIgnored, why, events)
}

// A force never overrides a rollout pause or a freeze: the ladder stays
// where the pause found it, clocks untouched, and the force is consumed
// with the reason so it cannot open the first gate the resumed ladder meets.
func TestReconcile_PausedRolloutConsumesTheForce(t *testing.T) {
	for _, tc := range []struct{ depth, why string }{
		{"true", "the rollout is paused"},
		{constants.PausedRolloutFreezeValue, "the rollout is frozen"},
	} {
		t.Run(tc.depth, func(t *testing.T) {
			isvc, in, rec := stagingShort(t)
			clock := isvc.Status.Canary.CapacityWaitSince.Time
			annotate(isvc, constants.PausedRolloutAnnotation, tc.depth)
			annotate(isvc, constants.RolloutPromoteForceAnnotation, "new")

			res, err := Reconcile(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			cs := isvc.Status.Canary
			expectForceIgnored(t, res, cs, rec, 0, tc.why)
			if phaseOf(isvc) != v1beta1.RolloutPhasePending || cs.CapacityWaitSince == nil || !cs.CapacityWaitSince.Time.Equal(clock) {
				t.Fatalf("a paused ladder keeps its phase and clocks, got phase=%q clock=%v", phaseOf(isvc), cs.CapacityWaitSince)
			}
			if got := revisionPercent(isvc, v1beta1.EngineComponent, "new"); got != 0 {
				t.Fatalf("a paused ladder writes no traffic, got %d%% on the canary", got)
			}
		})
	}
}

// A force never overrides a Failed park: the canary stays parked and the
// force is consumed with the reason.
func TestReconcile_FailedCanaryConsumesTheForce(t *testing.T) {
	isvc, in := fixtureFailed(t)
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec
	annotate(isvc, constants.RolloutPromoteForceAnnotation, "new")

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	expectForceIgnored(t, res, cs, rec, 0, "the canary is parked Failed")
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed || cs.Failed == nil {
		t.Fatalf("the park must stand, got phase=%q failed=%+v", phaseOf(isvc), cs.Failed)
	}
}

// A force never overrides a rollback, draining or complete: the unit keeps
// its stable revision and the force is consumed with the reason.
func TestReconcile_RolledBackCanaryConsumesTheForce(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture func(*testing.T) (*v1beta1.InferenceService, ReconcileInputs)
		phase   v1beta1.RolloutPhase
		why     string
	}{
		{"rolling back", fixtureRollingBack, v1beta1.RolloutPhaseRollingBack, "the canary is rolling back"},
		{"rolled back", fixtureRolledBack, v1beta1.RolloutPhaseRolledBack, "the canary is held rolled back"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in := tc.fixture(t)
			rec := record.NewFakeRecorder(8)
			in.Recorder = rec
			annotate(isvc, constants.RolloutPromoteForceAnnotation, "new")

			res, err := Reconcile(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			cs := isvc.Status.Canary
			expectForceIgnored(t, res, cs, rec, 0, tc.why)
			if !res.RolledBack || phaseOf(isvc) != tc.phase || cs.RolledBackRevisionHash != "new" {
				t.Fatalf("the rollback must stand, got rolledBack=%v phase=%q rejected=%q", res.RolledBack, phaseOf(isvc), cs.RolledBackRevisionHash)
			}
		})
	}
}

// A force at the final step with no serving canary pod opens the gate but
// programs no canary traffic: the step enters its drain with the stable
// revision carrying everything, the wait for the release keeps no clock and
// never parks, the force stays live until the completion edge, and the
// release follows once the capacity is back.
func TestReconcile_FinalStepForceWithNoServingCanaryProgramsNoCanaryTraffic(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{}},
	}
	isvc := canaryISVC(steps, nil)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:    "new",
		StableRevisionHash:    "old",
		CurrentStep:           1,
		ObservedTrafficWeight: 50,
		StepEnteredTime:       &metav1.Time{Time: time.Unix(1000, 0)},
	}
	rec := record.NewFakeRecorder(8)
	in := baseInputs(isvc, map[string]int32{"new": 0, "old": 4})
	in.Recorder = rec
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if phaseOf(isvc) != v1beta1.RolloutPhasePending || cs.CapacityWaitSince == nil {
		t.Fatalf("the final step must wait on its capacity first, got phase=%q canary=%+v", phaseOf(isvc), cs)
	}

	in.Now = in.Now.Add(time.Minute)
	annotate(isvc, constants.RolloutPromoteForceAnnotation, "new")
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Complete || cs.CurrentStep != 1 || phaseOf(isvc) != v1beta1.RolloutPhasePromoting {
		t.Fatalf("the force must open the final gate into the drain, got complete=%v step=%d phase=%q", res.Complete, cs.CurrentStep, phaseOf(isvc))
	}
	if cs.CapacityWaitSince != nil {
		t.Fatalf("the forced step keeps no capacity-wait clock, got %v", cs.CapacityWaitSince)
	}
	if got := revisionPercent(isvc, v1beta1.EngineComponent, "new"); got != 0 {
		t.Fatalf("no canary pod serves, so the canary carries no traffic, got %d%%", got)
	}
	if got := revisionPercent(isvc, v1beta1.EngineComponent, "old"); got != 100 {
		t.Fatalf("the stable revision carries everything, got %d%%", got)
	}
	if len(res.Consume) != 0 {
		t.Fatalf("the final-step force stays live until completion, got consume=%v", res.Consume)
	}
	expectEvent(t, rec, EventReasonCanaryStepForced, "Warning", "step 1", "new", "0 of 3")

	// Past the ready timeout the drain's wait does not park while the force
	// is live, and the force is announced only once.
	in.Now = in.Now.Add(cellReadyTimeout + time.Minute)
	res, err = Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || cs.Failed != nil || cs.CapacityWaitSince != nil {
		t.Fatalf("a forced wait never parks, got phase=%q failed=%+v clock=%v", phaseOf(isvc), cs.Failed, cs.CapacityWaitSince)
	}
	if len(res.Consume) != 0 {
		t.Fatalf("the final-step force stays live until completion, got consume=%v", res.Consume)
	}
	expectNoEvent(t, rec, EventReasonCanaryStepForced)

	// Capacity back: the release follows and records the force.
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	res, err = Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if int(cs.CurrentStep) != len(steps) || cs.PromotedThrough != "new" {
		t.Fatalf("the release must mark the done sentinel and record the force, got %+v", cs)
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteForceAnnotation {
		t.Fatalf("the completion edge hands the force back for removal, got %v", res.Consume)
	}
	if got := revisionPercent(isvc, v1beta1.EngineComponent, "new"); got != 100 {
		t.Fatalf("the cutover names the canary alone, got %d%%", got)
	}
}

// A force at an ungated step short of its capacity lets the step proceed:
// the wait is lifted, the step's traffic follows the serving rule, and the
// step advances in one pass with the force recorded like a promote.
func TestReconcile_UngatedStepForceAdvancesWithoutCapacity(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{}},
	}
	isvc := canaryISVC(steps, nil)
	rec := record.NewFakeRecorder(8)
	in := baseInputs(isvc, map[string]int32{"new": 1, "old": 3})
	in.Recorder = rec
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if phaseOf(isvc) != v1beta1.RolloutPhasePending || cs.CapacityWaitSince == nil {
		t.Fatalf("the ungated step must wait on its capacity first, got phase=%q canary=%+v", phaseOf(isvc), cs)
	}

	in.Now = in.Now.Add(time.Minute)
	annotate(isvc, constants.RolloutPromoteForceAnnotation, "new")
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Stepped || cs.CurrentStep != 1 {
		t.Fatalf("the force must let the ungated step proceed in one pass, got stepped=%v step=%d", res.Stepped, cs.CurrentStep)
	}
	if cs.PromotedThrough != "new" || cs.CapacityWaitSince != nil {
		t.Fatalf("the advance records the force and keeps no clock, got record=%q clock=%v", cs.PromotedThrough, cs.CapacityWaitSince)
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteForceAnnotation {
		t.Fatalf("the force must be handed back for removal after the flush, got %v", res.Consume)
	}
	expectEvent(t, rec, EventReasonCanaryStepForced, "Warning", "step 0", "new", "1 of 2")
	if got := revisionPercent(isvc, v1beta1.EngineComponent, "new"); got != 50 {
		t.Fatalf("the serving canary pod must carry the step's share, got %d%%", got)
	}
}

// A force at an ungated final step short of its capacity keeps the drain
// open: no clock, no CapacityTimeout park past the ready timeout, nothing
// consumed while the force is live, and the release follows the capacity.
func TestReconcile_ForcedCapacityShortDrainDoesNotPark(t *testing.T) {
	isvc, in := fixtureDrainingShort(t)
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.CapacityWaitSince == nil {
		t.Fatal("the dip must have started the drain's capacity-wait clock")
	}

	annotate(isvc, constants.RolloutPromoteForceAnnotation, "new")
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || cs.CurrentStep != 1 || cs.CapacityWaitSince != nil {
		t.Fatalf("the force must keep the drain open with no clock, got phase=%q step=%d clock=%v", phaseOf(isvc), cs.CurrentStep, cs.CapacityWaitSince)
	}
	if len(res.Consume) != 0 {
		t.Fatalf("the force stays live through the drain, got consume=%v", res.Consume)
	}
	expectEvent(t, rec, EventReasonCanaryStepForced, "Warning", "step 1", "new", "2 of 3")

	in.Now = in.Now.Add(cellReadyTimeout + time.Minute)
	res, err = Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePromoting || cs.Failed != nil || cs.CapacityWaitSince != nil {
		t.Fatalf("a forced drain never parks past the ready timeout, got phase=%q failed=%+v clock=%v", phaseOf(isvc), cs.Failed, cs.CapacityWaitSince)
	}
	if len(res.Consume) != 0 {
		t.Fatalf("the force stays live through the drain, got consume=%v", res.Consume)
	}
	expectNoEvent(t, rec, EventReasonCanaryStepForced)

	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	res, err = Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if cs.CurrentStep != 2 || cs.PromotedThrough != "new" {
		t.Fatalf("the release must follow the capacity and record the force, got %+v", cs)
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteForceAnnotation {
		t.Fatalf("the completion edge hands the force back for removal, got %v", res.Consume)
	}
}

// A stale force beside a live promote advances exactly one step and records
// the live value: the promote is the applied verb, so a copy of it that
// outlives the flush is inert and handed back, while the stale force is
// consumed with the advance and cannot pin the record.
func TestReconcile_StaleForceBesideALivePromoteRecordsTheLiveValue(t *testing.T) {
	isvc := canaryISVC(threeStep(), nil)
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	annotate(isvc, constants.RolloutPromoteForceAnnotation, "stale")
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if !res.Stepped || cs.CurrentStep != 1 {
		t.Fatalf("the live promote must advance one step, got stepped=%v step=%d", res.Stepped, cs.CurrentStep)
	}
	if cs.PromotedThrough != "new" {
		t.Fatalf("the record must hold the live value, got %q", cs.PromotedThrough)
	}
	if len(res.Consume) != 2 || res.Consume[0] != constants.RolloutPromoteAnnotation || res.Consume[1] != constants.RolloutPromoteForceAnnotation {
		t.Fatalf("both verbs are handed back with the advance, got %v", res.Consume)
	}

	// The removal was lost: both copies are visible again on the next pass.
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	annotate(isvc, constants.RolloutPromoteForceAnnotation, "stale")
	res, err = Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Stepped || cs.CurrentStep != 1 {
		t.Fatalf("a lingering applied promote must not advance again, got stepped=%v step=%d", res.Stepped, cs.CurrentStep)
	}
	if cs.PromotedThrough != "new" {
		t.Fatalf("the record stands while the applied value lingers, got %q", cs.PromotedThrough)
	}
	if len(res.Consume) != 1 || res.Consume[0] != constants.RolloutPromoteAnnotation {
		t.Fatalf("only the applied promote is handed back again, got %v", res.Consume)
	}

	// The applied copy is gone; the stale force alone cannot keep the record.
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if cs.PromotedThrough != "" || cs.CurrentStep != 1 {
		t.Fatalf("the record clears once no key carries the applied value, got record=%q step=%d", cs.PromotedThrough, cs.CurrentStep)
	}
}
