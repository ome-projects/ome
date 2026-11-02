package placement

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// plannedComponentSource plans a per-component Split source over both members
// and records the plan's candidates, settled at their desired shares, in its
// status.
func plannedComponentSource(t *testing.T, source *v1beta1.InferenceService) *v1beta1.InferenceService {
	t.Helper()
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{ID: "plan-a"}, Candidates: settledCandidates(proposal.Assignments)}
	return source
}

func TestComponentSharesCondition(t *testing.T) {
	threeToOne := map[string]int32{"member-a": 3, "member-b": 1}
	even := map[string]int32{"member-a": 1, "member-b": 1}
	for _, tt := range []struct {
		name                string
		source              func() *v1beta1.InferenceService
		wantClear           bool
		wantReason, wantMsg string
	}{
		{name: "decoders with their own weights", source: func() *v1beta1.InferenceService { return componentSplitSource(4, 12, threeToOne, even) },
			wantReason: "OwnWeights", wantMsg: "decoder is split by its own weights"},
		{name: "decoders following the engine shares", source: func() *v1beta1.InferenceService { return componentSplitSource(4, 12, threeToOne, threeToOne) },
			wantReason: "FollowEngineShares", wantMsg: "decoder follows the engine shares"},
		{name: "a weight on a cluster without an engine is named", source: func() *v1beta1.InferenceService { return componentSplitSource(1, 3, threeToOne, even) },
			wantReason: "OwnWeights", wantMsg: "unused because the cluster holds no engine: engine weight on member-b, decoder weight on member-b"},
		{name: "a divided router is reported", source: func() *v1beta1.InferenceService { return dividedRouterSource(4, even) },
			wantReason: "OwnWeights", wantMsg: "decoder follows the engine shares; router is split by its own weights"},
		{name: "a source without weights clears it", source: func() *v1beta1.InferenceService { return ratioSplitSource(4, ptr.To(12)) }, wantClear: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := componentSharesCondition(plannedComponentSource(t, tt.source()))
			if got.clear != tt.wantClear {
				t.Fatalf("clear = %t, want %t", got.clear, tt.wantClear)
			}
			if tt.wantClear {
				return
			}
			if got.cond.Status != corev1.ConditionTrue || got.cond.Reason != tt.wantReason || !strings.Contains(got.cond.Message, tt.wantMsg) {
				t.Errorf("condition = %s/%s %q, want True/%s containing %q", got.cond.Status, got.cond.Reason, got.cond.Message, tt.wantReason, tt.wantMsg)
			}
		})
	}
}

func TestComponentFloorsShowAHeldFloor(t *testing.T) {
	source := plannedComponentSource(t, componentSplitSource(4, 12, map[string]int32{"member-a": 3, "member-b": 1}, map[string]int32{"member-a": 2, "member-b": 1}))
	member := func(engine, decoder int) *v1beta1.InferenceService {
		copied := source.DeepCopy()
		copied.Spec.Placement = nil
		copied.Spec.Engine.MinReplicas = ptr.To(engine)
		copied.Spec.Decoder.MinReplicas = ptr.To(decoder)
		return copied
	}
	// member-b still holds the 6 decoders of the previous even split.
	got := componentFloorsFor(source, map[string]plannedMemberObservation{"member-a": {Member: member(3, 8)}, "member-b": {Member: member(1, 6)}})
	want := map[string][]v1beta1.CandidateComponentFloor{
		"member-a": {{Component: v1beta1.EngineComponent, Current: 3, Desired: 3}, {Component: v1beta1.DecoderComponent, Current: 8, Desired: 8}},
		"member-b": {{Component: v1beta1.EngineComponent, Current: 1, Desired: 1}, {Component: v1beta1.DecoderComponent, Current: 6, Desired: 4}},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("component floors (-want +got):\n%s", diff)
	}
}

func TestComponentFloorsNeedASecondaryComponent(t *testing.T) {
	source := ratioSplitSource(4, nil)
	source.Spec.Decoder = nil
	source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 1, "member-b": 1})
	if got := componentFloorsFor(plannedComponentSource(t, source), nil); got != nil {
		t.Errorf("an engine-only source reports component floors %v", got)
	}
}
