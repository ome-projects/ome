package canary

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The stable side of a serving split on the reference shape: twelve
// prefill and eight decode Instances under a four-step timed canary at 10,
// 30, 60 and 100 percent. The first step stages two engines and one decoder
// and keeps ten engines and seven decoders on the stable revision; the
// second stages four and three and keeps eight and five.

func fourStepTimed(d time.Duration) []v1beta1.RolloutGroupStep {
	pause := &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: d}}
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("10%"), Traffic: 10, Pause: pause},
		{Capacity: intstr.FromString("30%"), Traffic: 30, Pause: pause},
		{Capacity: intstr.FromString("60%"), Traffic: 60, Pause: pause},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// twelveEightServing walks a twelve-engine, eight-decoder unit to its first
// split serving at 10 percent with every stable Instance Ready, recording
// events.
func twelveEightServing(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	t.Helper()
	isvc, in := pdUnit(fourStepTimed(20*time.Second), nil, map[string]int32{"new": 2, "old": 10})
	in.DesiredReplicas = 12
	decoderServes(&in, 1, 7, true)
	m := in.Secondaries[v1beta1.DecoderComponent]
	m.DesiredReplicas = 8
	in.Secondaries[v1beta1.DecoderComponent] = m
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec
	mustReconcile(t, isvc, in)
	if got := rollout.StateOf(rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent), phaseOf(isvc), 4); got != rollout.CanaryStateServing {
		t.Fatalf("the first step must serve, state %s", got)
	}
	drainEvents(rec)
	return isvc, in, rec
}

func TestReconcile_StableSideShortIsToldOnTwelvePrefillAndEightDecode(t *testing.T) {
	isvc, in, rec := twelveEightServing(t)
	// One stable engine pod crash-loops and another is deleted: eight of the
	// ten the step keeps are Ready, so the two canary Instances are a fifth
	// of the Ready engine capacity under a 10 percent split.
	enginePods(&in, map[string]int32{"new": 2, "old": 8}, nil)
	mustReconcile(t, isvc, in)
	expectEvent(t, rec, EventReasonCanaryStableSideShort,
		"engine canary step 0: stable side short",
		"8 of 10 engine Instances Ready on stable revision old, canary share 20 percent above the split's 10 percent",
		"the canary revision carries more than its step until the stable side is whole again")
	if got := isvc.Status.Components[v1beta1.EngineComponent].Traffic; len(got) != 2 || got[0].Percent != 10 {
		t.Fatalf("the split keeps the step's weight, got %+v", got)
	}
}

func TestReconcile_StableSideShortIsToldOnceWhileTheDipStands(t *testing.T) {
	isvc, in, rec := twelveEightServing(t)
	enginePods(&in, map[string]int32{"new": 2, "old": 8}, nil)
	mustReconcile(t, isvc, in)
	expectEvent(t, rec, EventReasonCanaryStableSideShort, "8 of 10 engine Instances")
	// The dip stands: the next passes say nothing more.
	for i := 0; i < 3; i++ {
		in.Now = in.Now.Add(5 * time.Second)
		mustReconcile(t, isvc, in)
		expectNoEvent(t, rec, EventReasonCanaryStableSideShort)
	}
	// The deleted pod is rebuilt and the other serves again: the record
	// clears quietly, and a later dip is told again with its own numbers.
	enginePods(&in, map[string]int32{"new": 2, "old": 10}, nil)
	mustReconcile(t, isvc, in)
	expectNoEvent(t, rec, EventReasonCanaryStableSideShort)
	enginePods(&in, map[string]int32{"new": 2, "old": 9}, nil)
	mustReconcile(t, isvc, in)
	expectEvent(t, rec, EventReasonCanaryStableSideShort, "9 of 10 engine Instances Ready on stable revision old, canary share 18 percent above the split's 10 percent")
}

func TestReconcile_WholeStableSideTellsNothing(t *testing.T) {
	isvc, in, rec := twelveEightServing(t)
	for i := 0; i < 3; i++ {
		in.Now = in.Now.Add(5 * time.Second)
		mustReconcile(t, isvc, in)
	}
	expectNoEvent(t, rec, EventReasonCanaryStableSideShort)
}

func TestReconcile_StableSideShortNamesEachShortMember(t *testing.T) {
	isvc, in, rec := twelveEightServing(t)
	// Both members dip at once: the message lists the engine, then the
	// decoder, each in its own numbers.
	enginePods(&in, map[string]int32{"new": 2, "old": 8}, nil)
	decoderServes(&in, 1, 6, true)
	m := in.Secondaries[v1beta1.DecoderComponent]
	m.DesiredReplicas = 8
	in.Secondaries[v1beta1.DecoderComponent] = m
	in.GroupTotalPerRevisionPods[v1beta1.EngineComponent] = in.PerRevisionPods
	mustReconcile(t, isvc, in)
	expectEvent(t, rec, EventReasonCanaryStableSideShort,
		"8 of 10 engine Instances Ready on stable revision old, canary share 20 percent above the split's 10 percent; "+
			"6 of 7 decoder Instances Ready on stable revision decold, canary share 14 percent above the split's 10 percent")
}

func TestReconcile_StableSideShortWithTheCanaryDownTellsNothing(t *testing.T) {
	isvc, in, rec := twelveEightServing(t)
	// Both canary engine pods are down beside the two stable ones: the
	// canary revision carries nothing, so the dip distorts no split and the
	// capacity gate's own hold speaks for the step.
	enginePods(&in, map[string]int32{"new": 0, "old": 8}, nil)
	mustReconcile(t, isvc, in)
	expectNoEvent(t, rec, EventReasonCanaryStableSideShort)
}

func TestReconcile_StableSideShortIsToldAgainWhenTheNextStepServes(t *testing.T) {
	isvc, in, rec := twelveEightServing(t)
	enginePods(&in, map[string]int32{"new": 2, "old": 8}, nil)
	mustReconcile(t, isvc, in)
	expectEvent(t, rec, EventReasonCanaryStableSideShort, "8 of 10 engine Instances")
	// The pause elapses with the dip standing: the record advances to the
	// second step, whose staging retires stable Instances by design, so the
	// staging reads no stable side and the record clears without a word.
	in.Now = in.Now.Add(20 * time.Second)
	mustReconcile(t, isvc, in)
	in.Now = in.Now.Add(5 * time.Second)
	mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if got := rollout.StateOf(cs, phaseOf(isvc), 4); got != rollout.CanaryStateStaging || cs.StableSideShortSince != nil {
		t.Fatalf("a staging keeps no short stable side, state %s, stableSideShortSince %v", got, cs.StableSideShortSince)
	}
	expectNoEvent(t, rec, EventReasonCanaryStableSideShort)
	// The second step's capacity is met on both members with the dip still
	// there: six of the eight engines the step keeps are Ready, so the 30
	// percent split serves with the four canary engines at 40 percent of
	// the Ready engine capacity, and the step says so once in its numbers.
	enginePods(&in, map[string]int32{"new": 4, "old": 6}, nil)
	in.ReadyCanaryInstances = nil
	decoderServes(&in, 3, 5, true)
	m := in.Secondaries[v1beta1.DecoderComponent]
	m.DesiredReplicas = 8
	in.Secondaries[v1beta1.DecoderComponent] = m
	in.GroupTotalPerRevisionPods[v1beta1.EngineComponent] = in.PerRevisionPods
	mustReconcile(t, isvc, in)
	expectEvent(t, rec, EventReasonCanaryStableSideShort,
		"engine canary step 1: stable side short",
		"6 of 8 engine Instances Ready on stable revision old, canary share 40 percent above the split's 30 percent")
	if got := isvc.Status.Components[v1beta1.EngineComponent].Traffic; len(got) != 2 || got[0].Percent != 30 {
		t.Fatalf("the split keeps the step's weight, got %+v", got)
	}
	in.Now = in.Now.Add(5 * time.Second)
	mustReconcile(t, isvc, in)
	expectNoEvent(t, rec, EventReasonCanaryStableSideShort)
}

func TestReconcile_AStableSideWithNothingReadyIsGoneNotShort(t *testing.T) {
	isvc, in, rec := twelveEightServing(t)
	// Every stable engine pod is lost: the split itself moves onto the
	// canary revision, which is that loss's own reading, and the record
	// carries no short stable side.
	enginePods(&in, map[string]int32{"new": 2, "old": 0}, nil)
	mustReconcile(t, isvc, in)
	expectNoEvent(t, rec, EventReasonCanaryStableSideShort)
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs.StableSideShortSince != nil {
		t.Fatalf("a stable side with nothing Ready is not short, got %v", cs.StableSideShortSince)
	}
}

func TestReconcile_StableSideShortIsNotCarriedPastTheSplit(t *testing.T) {
	isvc, in, rec := twelveEightServing(t)
	enginePods(&in, map[string]int32{"new": 2, "old": 8}, nil)
	mustReconcile(t, isvc, in)
	expectEvent(t, rec, EventReasonCanaryStableSideShort, "8 of 10 engine Instances")
	// A rollback request ends the split: the pass that records the revert
	// clears the dip from the record.
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	mustReconcile(t, isvc, in)
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs.StableSideShortSince != nil {
		t.Fatalf("a reverting unit keeps no short stable side, got %v", cs.StableSideShortSince)
	}
}

func TestStableSideShort(t *testing.T) {
	step := fourStepTimed(time.Minute)[0]
	base := func(canary, stable int32) ReconcileInputs {
		in := baseInputs(canaryISVC(fourStepTimed(time.Minute), nil), map[string]int32{"new": canary, "old": stable})
		in.DesiredReplicas = 12
		return in
	}
	cases := []struct {
		name  string
		in    ReconcileInputs
		short int
	}{
		{"whole", base(2, 10), 0},
		{"one stable Instance short", base(2, 9), 1},
		{"no stable Instance Ready", base(2, 0), 0},
		{"no pod view", func() ReconcileInputs { in := base(2, 9); in.PerRevisionPods = nil; return in }(), 0},
		{"the canary is the stable revision", func() ReconcileInputs { in := base(2, 9); in.CanaryRevisionHash = "old"; return in }(), 0},
		{"a Ready pod on a third revision", func() ReconcileInputs {
			in := base(2, 9)
			in.PerRevisionPods["older"] = 1
			return in
		}(), 0},
		{"the replica's rows count, not the pods", func() ReconcileInputs {
			in := base(2, 9)
			ten := int32(10)
			in.ReadyStableInstances = &ten
			return in
		}(), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stableSideShort(tc.in, step, 10); len(got) != tc.short {
				t.Fatalf("short members %+v, want %d", got, tc.short)
			}
		})
	}
}
