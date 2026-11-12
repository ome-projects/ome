package coordination

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// mkRatioPairFixture is a prefill/decode pair (engine beside decoder) in one
// rollingUpdate group under maintainRatio at the given tolerance, with the
// ratio reference an earlier pass recorded and each side's replica counters
// as the replica publishes them. The replica specs are supplied to the fake
// client by the caller, since the spec is what the reference follows.
func mkRatioPairFixture(tol int32, recorded map[v1beta1.ComponentType]int32, lifecycle map[v1beta1.ComponentType]v1beta1.LifecycleStatus) *v1beta1.InferenceService {
	components := map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	for c, l := range lifecycle {
		l := l
		components[c] = v1beta1.ComponentStatusSpec{Lifecycle: &l}
	}
	return pinActiveRun(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:          "0",
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:        v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{Original: recorded},
				}},
			},
			Components: components,
		},
	})
}

// TestCheckRatio_ResizedDecodeSideIsFinishedAtItsNewCount is the reference
// shape: a prefill/decode service recorded at 15 prefill and 15 decode,
// both SurgeThenDrain at maxSurge 10 percent under maintainRatio tolerance
// 20, resized to 30 prefill and 10 decode and pushed to a new revision. All
// 10 decoders are on the new revision and the prefill side has 18 of 30.
// Against the recorded 15:15 the lockstep read the decoders at 10 of 15,
// never finished, and the prefill side at 18 of 15, so every prefill surge
// was held with "surge would outpace peer new-revision rollout past
// tolerance" for good. The decode side is finished at its
// current count of 10, so it paces nobody; and once the ISVC pass has
// rewritten the reference to 30:10, the band admits the surge too.
//
// The ratio consult is asked directly; on a pair whose engine and decoder
// both step, the batch step takes its place in the gate stack.
func TestCheckRatio_ResizedDecodeSideIsFinishedAtItsNewCount(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	isvc := mkRatioPairFixture(20, map[v1beta1.ComponentType]int32{eng: 15, dec: 15}, map[v1beta1.ComponentType]v1beta1.LifecycleStatus{
		eng: {Replicas: 30, ServingReplicas: 30, UpdatedReplicas: 18},
		dec: {Replicas: 10, ServingReplicas: 10, UpdatedReplicas: 10},
	})
	client := fakeClientForISVCWithDesiredAndInstances(isvc, map[v1beta1.ComponentType]*int32{
		eng: int32Ptr(30), dec: int32Ptr(10),
	}, nil)

	// The pass between the resize and the reference rewrite: the lockstep
	// reads each side against its current desired count.
	gate := ResolveGateContext(context.Background(), client, isvc, eng)
	if _, reason := gate.CheckRatio(0, 0, +1); strings.Contains(reason, "outpace") {
		t.Fatalf("10 of 10 decoders are finished at the decode side's current count, so the lockstep must not hold the prefill surge: %s", reason)
	}
	if allowed, reason := gate.checkRatioLockstep(""); !allowed {
		t.Fatalf("the lockstep alone must not hold the prefill side either: %s", reason)
	}

	// The ISVC pass rewrites the reference to the counts the spec asks for.
	g := ratioGroup(eng, dec)
	obs, err := buildGroupObservation(context.Background(), client, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if state.Original[eng] != 30 || state.Original[dec] != 10 {
		t.Fatalf("the reference must follow the resize to 30:10; got %v", state.Original)
	}
	isvc.Status.RolloutCoordination.Groups[0].ObservedRatio.Original = state.Original

	gate = ResolveGateContext(context.Background(), client, isvc, eng)
	if allowed, reason := gate.CheckRatio(0, 0, +1); !allowed {
		t.Fatalf("a prefill surge at 18 of 30 beside 10 of 10 decoders, reference 30:10 at tolerance 20, must be admitted: %s", reason)
	}
}

// TestCheckRatio_ScaledDownPairDrainsAgainstTheNewCounts is the band's half
// of the same wedge: 8 prefill and 4 decode, both RecreatePod one at a time
// under maintainRatio tolerance 20, scaled to 7 and 3 at the first new pod.
// With the prefill side's first replacement still starting the fleet serves
// 6:3. Against the recorded 8:4 the decode side's first drain projects 6:2,
// three to one against two to one, 50 points off and past twice the
// tolerance, so it is refused for good. Against 7:3 the same drain is the
// one step the tiebreaker admits (three to one against 7:3's 2.33, within
// twice the tolerance), and the prefill side's next drain once its
// replacement serves projects 6:3, inside the band; so both sides finish.
//
// The ratio consult is asked directly; on a pair whose engine and decoder
// both step, the batch step takes its place in the gate stack.
func TestCheckRatio_ScaledDownPairDrainsAgainstTheNewCounts(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	firstNewPod := map[v1beta1.ComponentType]v1beta1.LifecycleStatus{
		eng: {Replicas: 7, ServingReplicas: 6, UpdatedReplicas: 0},
		dec: {Replicas: 3, ServingReplicas: 3, UpdatedReplicas: 0},
	}
	desired := map[v1beta1.ComponentType]*int32{eng: int32Ptr(7), dec: int32Ptr(3)}
	isvc := mkRatioPairFixture(20, map[v1beta1.ComponentType]int32{eng: 8, dec: 4}, firstNewPod)
	client := fakeClientForISVCWithDesiredAndInstances(isvc, desired, nil)

	if allowed, reason := ResolveGateContext(context.Background(), client, isvc, dec).CheckRatio(0, 0, -1); allowed || !strings.Contains(reason, "skew") {
		t.Fatalf("against the stale 8:4 the decode drain reads 6:2, 50 points off, and is refused: allowed=%v reason=%q", allowed, reason)
	}

	g := ratioGroup(eng, dec)
	obs, err := buildGroupObservation(context.Background(), client, isvc, g, map[v1beta1.ComponentType]map[string]int32{})
	if err != nil {
		t.Fatalf("buildGroupObservation: %v", err)
	}
	state := buildRatioState(isvc, g, obs, map[v1beta1.ComponentType]map[string]int32{})
	if state.Original[eng] != 7 || state.Original[dec] != 3 {
		t.Fatalf("the reference must follow the scale to 7:3; got %v", state.Original)
	}
	isvc.Status.RolloutCoordination.Groups[0].ObservedRatio.Original = state.Original

	if allowed, reason := ResolveGateContext(context.Background(), client, isvc, dec).CheckRatio(0, 0, -1); !allowed {
		t.Fatalf("against 7:3 the decode side's first drain (6:3 to 6:2) must be admitted: %s", reason)
	}

	// The prefill replacement serves; its next drain projects 6:3 against 7:3.
	isvc.Status.Components[eng] = v1beta1.ComponentStatusSpec{Lifecycle: &v1beta1.LifecycleStatus{
		Replicas: 7, ServingReplicas: 7, UpdatedReplicas: 1,
	}}
	client = fakeClientForISVCWithDesiredAndInstances(isvc, desired, nil)
	if allowed, reason := ResolveGateContext(context.Background(), client, isvc, eng).CheckRatio(0, 0, -1); !allowed {
		t.Fatalf("the prefill side's next drain at 7:3 serving must be admitted: %s", reason)
	}
}

// TestEvaluateSurge_PeerIsFinishedAtItsCurrentDesiredCount pins the
// lockstep's reading of a resized side in the pass before the reference is
// rewritten: progress is measured over each side's current desired count,
// a side whose new-revision Instances reach that count is finished and
// paces nobody, and a side resized above its start count is measured over
// the count it rolls toward. A caller that supplies no desired counts is
// measured over the reference it supplies.
func TestEvaluateSurge_PeerIsFinishedAtItsCurrentDesiredCount(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: int32Ptr(20),
	}
	resized := RatioState{
		Original: map[v1beta1.ComponentType]int32{eng: 15, dec: 15},
		Current:  map[v1beta1.ComponentType]int32{eng: 30, dec: 10},
		Desired:  map[v1beta1.ComponentType]int32{eng: 30, dec: 10},
		Serving:  map[v1beta1.ComponentType]int32{eng: 30, dec: 10},
		NewPods:  map[v1beta1.ComponentType]int32{eng: 18, dec: 10},
	}
	if held, reason := LockstepHold(pacing, resized, eng); held {
		t.Fatalf("10 of 10 decoders are finished at the decode side's current count; the prefill side must not be held: %s", reason)
	}

	onReferenceOnly := resized
	onReferenceOnly.Desired = nil
	if held, _ := LockstepHold(pacing, onReferenceOnly, eng); !held {
		t.Fatalf("with no desired counts the lockstep reads the reference, where 10 of 15 decoders still trail 18 of 15 prefill")
	}

	rewritten := resized
	rewritten.Original = map[v1beta1.ComponentType]int32{eng: 30, dec: 10}
	if d := EvaluateSurge(pacing, rewritten, eng, 1); d.AllowedSurgeDelta != 1 || d.Reason != "within tolerance" {
		t.Fatalf("with the reference at 30:10 a prefill surge projects 31:10, inside the band: %+v", d)
	}

	grown := RatioState{
		Original: map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
		Current:  map[v1beta1.ComponentType]int32{eng: 8, dec: 2},
		Desired:  map[v1beta1.ComponentType]int32{eng: 8, dec: 2},
		Serving:  map[v1beta1.ComponentType]int32{eng: 8, dec: 2},
		NewPods:  map[v1beta1.ComponentType]int32{eng: 4, dec: 1},
	}
	if held, reason := LockstepHold(pacing, grown, eng); held {
		t.Fatalf("4 of 8 prefill is level with 1 of 2 decode once the prefill side is measured over its new count: %s", reason)
	}
}
