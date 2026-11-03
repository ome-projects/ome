package placement

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

// heldMemberFixture observes member-a while a Split plan of 8 engines grows
// member-b from 2 engines to 4. member-a keeps its 4 engines, but decoder
// weights of 1 and 7 give it one of the 6 decoders that the 6 running engines
// carry. OME holds member-a's copy at a minimum of copyDecoders decoders until
// member-b's replacements serve; member-a runs running of them, and its IR asks
// for requested.
func heldMemberFixture(t *testing.T, copyDecoders, requested, running int32) plannedObservationFixture {
	t.Helper()
	f := observationFixture(t)
	f.source.Spec.Placement.Split.Replicas = ptr.To[int32](8)
	f.source.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(8)}}
	weigh := func(allocation *v1beta1.CandidateAllocationStatus, decoderWeight int64) {
		allocation.Matched, allocation.Weight = true, ptr.To[int64](1)
		allocation.ComponentMatches = []v1beta1.ComponentAffinityMatch{{Component: v1beta1.DecoderComponent, Weight: ptr.To(decoderWeight)}}
	}
	held := f.source.Status.Placement.Candidates[0].Allocation
	held.OriginalReplicas, held.CurrentReplicas, held.DesiredReplicas = 4, 4, 4
	weigh(held, 1)
	growing := &v1beta1.CandidateAllocationStatus{ClusterUID: "member-b-uid", OriginalReplicas: 2, CurrentReplicas: 2, DesiredReplicas: 4}
	weigh(growing, 7)
	f.source.Status.Placement.Candidates = append(f.source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: "member-b", Allocation: growing})
	f.member.Spec.Engine.MinReplicas, f.member.Spec.Engine.MaxReplicas = ptr.To(4), 4
	f.member.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(int(copyDecoders)), MaxReplicas: int(copyDecoders)}}
	f.resources.ir.Spec.Replicas, f.resources.ir.Spec.PlacementReplicaLimit = ptr.To[int32](4), ptr.To[int32](4)
	decoder := f.decoderTwin(f.resources.ir.Spec.PlacementExecution, running)
	ir := decoder[0].(*v1beta1.InferenceReplica)
	ir.Spec.Replicas, ir.Spec.PlacementReplicaLimit = ptr.To(requested), ptr.To(requested)
	f.extraObjects = decoder
	f.components = append(f.components, v1beta1.DecoderComponent)
	return f
}

// ratioRiseFixture observes member-a while a Split plan of 4 engines moves its
// 2 engines to member-b and raises the decoder minimum from 4 to 6. The
// decoders follow the engines, so while both members still run 2 engines each
// one's next share is 3 decoders. member-a's copy still sets the old ratio's
// minimum of 2 decoders, and it runs requested of them; desired is its engine
// share in the new plan.
func ratioRiseFixture(t *testing.T, desired, requested int32) plannedObservationFixture {
	t.Helper()
	f := observationFixture(t)
	f.source.Spec.Placement.Split.Replicas = ptr.To[int32](4)
	f.source.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(6)}}
	member := f.source.Status.Placement.Candidates[0].Allocation
	member.OriginalReplicas, member.CurrentReplicas, member.DesiredReplicas = 2, 2, desired
	f.source.Status.Placement.Candidates = append(f.source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: "member-b", Allocation: &v1beta1.CandidateAllocationStatus{
		ClusterUID: "member-b-uid", Matched: true, OriginalReplicas: 2, CurrentReplicas: 2, DesiredReplicas: 4 - desired, Weight: ptr.To[int64](1),
	}})
	f.member.Spec.Engine.MinReplicas, f.member.Spec.Engine.MaxReplicas = ptr.To(2), 2
	f.member.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2), MaxReplicas: 2}}
	f.resources.ir.Spec.Replicas, f.resources.ir.Spec.PlacementReplicaLimit = ptr.To[int32](2), ptr.To[int32](2)
	decoder := f.decoderTwin(f.resources.ir.Spec.PlacementExecution, 2)
	ir := decoder[0].(*v1beta1.InferenceReplica)
	ir.Spec.Replicas, ir.Spec.PlacementReplicaLimit = ptr.To(requested), ptr.To(requested)
	f.extraObjects = decoder
	f.components = append(f.components, v1beta1.DecoderComponent)
	return f
}

// observeFixture runs one planned observation of the fixture's member.
func observeFixture(t *testing.T, f plannedObservationFixture) plannedMemberObservation {
	t.Helper()
	objects := append([]client.Object{f.member, f.resources.ir}, f.extraObjects...)
	for i := range f.resources.pods {
		objects = append(objects, &f.resources.pods[i])
	}
	scheme := testScheme(t)
	worker := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(f.intercept).Build()
	clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
	r, _ := newPlacer(scheme, clusters, f.source, plannedTestRegistration())
	r.InstanceStatusDecoder = irstatus.NewDecoder(f.decodeBound)
	observation, err := r.observePlannedMember(context.Background(), f.source, f.source.Status.Placement.Candidates[0], f.components)
	if err != nil {
		t.Fatal(err)
	}
	return observation
}

// A member keeps decoders above its next share until their replacements on
// other clusters serve, and its copy's minimum keeps them running. The move's
// surge allowance counts them in engine units at the copy's own ratio, so a
// member that runs its copy's minimum costs its engine target, however small
// its next share; a decoder above the copy's minimum still costs surge, even
// after the member's target drops. Measured against the next share, every kept
// decoder stays excess, so a finished move waits for it.
func TestKeptDecodersCountAgainstTheMemberCopyMinimum(t *testing.T) {
	type units struct{ Occupied, Reserved, AllowanceOccupied, AllowanceReserved int32 }
	for _, tt := range []struct {
		name                                 string
		copyDecoders, requested, run, target int32
		surging, noDecoderMinimum            bool
		want                                 units
	}{
		{name: "decoders at the copy's minimum cost the engine target", copyDecoders: 3, requested: 3, run: 3,
			want: units{Occupied: 12, AllowanceOccupied: 4}},
		{name: "a decoder above the copy's minimum costs surge", copyDecoders: 3, requested: 4, run: 4,
			want: units{Occupied: 16, AllowanceOccupied: 6}},
		{name: "a decoder's rollout surge costs surge at the copy's minimum", copyDecoders: 3, requested: 3, run: 3, surging: true,
			want: units{Occupied: 12, Reserved: 4, AllowanceOccupied: 4, AllowanceReserved: 2}},
		{name: "decoders above a lowered copy minimum cost surge until they leave", copyDecoders: 1, requested: 1, run: 3,
			want: units{Occupied: 12, AllowanceOccupied: 12}},
		{name: "without a decoder minimum each decoder costs one unit", copyDecoders: 3, requested: 4, run: 4, noDecoderMinimum: true,
			want: units{Occupied: 4, AllowanceOccupied: 4}},
		{name: "decoders above the copy's minimum still cost surge after the member's target drops", copyDecoders: 3, requested: 6, run: 6, target: 2,
			want: units{Occupied: 12, AllowanceOccupied: 8}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := heldMemberFixture(t, tt.copyDecoders, tt.requested, tt.run)
			if tt.noDecoderMinimum {
				f.source.Spec.Decoder.MinReplicas = ptr.To(0)
			}
			if tt.target > 0 {
				members := f.source.Status.Placement.Candidates
				members[0].Allocation.CurrentReplicas, members[0].Allocation.DesiredReplicas = tt.target, tt.target
				members[1].Allocation.DesiredReplicas = 8 - tt.target
			}
			if tt.surging {
				decoder := f.extraObjects[0].(*v1beta1.InferenceReplica)
				decoder.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
					Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepSurge, TargetRevision: "service-engine-b",
				}
			}
			observation := observeFixture(t, f)
			if !observation.MemberState.Known {
				t.Fatalf("observation is unknown: %+v", observation.MemberState)
			}
			state, reserved := observation.allowanceState()
			got := units{observation.MemberState.Occupied, observation.RolloutReserved, state.Occupied, reserved}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("engine units (-want +got):\n%s", diff)
			}
		})
	}
}

// A member the plan removes keeps the minimums on its copy: OME stops writing
// the copy once the member no longer matches. When a higher decoder ratio
// raises that member's next share, its acknowledgement compares its decoders
// with the copy's minimum, so the move is not held for growth OME never asks
// the member for. A member that stays must still reach its raised share.
func TestRemovedMemberAcknowledgesAtItsCopyMinimum(t *testing.T) {
	type result struct{ Applied, PauseAcknowledged bool }
	for _, tt := range []struct {
		name               string
		desired, requested int32
		want               result
	}{
		{name: "a removed member at its copy's minimum acknowledges", desired: 0, requested: 2, want: result{Applied: true, PauseAcknowledged: true}},
		{name: "a removed member below its copy's minimum cannot acknowledge", desired: 0, requested: 1},
		{name: "a staying member waits for its raised share", desired: 2, requested: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			observation := observeFixture(t, ratioRiseFixture(t, tt.desired, tt.requested))
			if !observation.MemberState.Known {
				t.Fatalf("observation is unknown: %+v", observation.MemberState)
			}
			got := result{observation.MemberState.Applied, observation.PauseAcknowledged}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("acknowledgement (-want +got):\n%s", diff)
			}
		})
	}
}
