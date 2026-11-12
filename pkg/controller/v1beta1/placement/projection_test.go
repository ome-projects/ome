package placement

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The WorkloadClusters have no metadata.uid, and the source's status holds a
// plan for another source. ProjectSplitReplicas plans from the spec alone. Its
// copy has an empty status and differs from the source only in the replica
// bounds, and neither input changes.
func TestProjectSplitReplicasUsesOfflineSnapshot(t *testing.T) {
	source := ratioSplitSource(60, ptr.To(40))
	source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
	source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{SourceUID: "different-source"}}
	before := source.DeepCopy()
	clusters := registeredClusters("member-a", "member-b")

	projected, err := ProjectSplitReplicas(source, clusters, "member-a")
	require.NoError(t, err)
	want := before.DeepCopy()
	want.Status = v1beta1.InferenceServiceStatus{}
	want.Spec.Engine.MinReplicas, want.Spec.Engine.MaxReplicas = ptr.To(40), 40
	want.Spec.Decoder.MinReplicas, want.Spec.Decoder.MaxReplicas = ptr.To(27), 27
	require.Equal(t, want, projected)
	require.Equal(t, before, source)
	require.Equal(t, registeredClusters("member-a", "member-b"), clusters)
}

// The source's cluster affinity selects only member-a, so member-b gets no
// replicas. ProjectSplitReplicas returns nil and no error for member-b, and
// gives member-a all 6 engine replicas.
func TestProjectSplitReplicasIsNilForUnselectedCluster(t *testing.T) {
	source := ratioSplitSource(6, nil)
	source.Spec.Placement.ClusterAffinity = testAffinity("metadata.name=member-a")
	clusters := registeredClusters("member-a", "member-b")

	projected, err := ProjectSplitReplicas(source, clusters, "member-b")
	require.NoError(t, err)
	require.Nil(t, projected)

	projected, err = ProjectSplitReplicas(source, clusters, "member-a")
	require.NoError(t, err)
	require.Equal(t, 6, *projected.Spec.Engine.MinReplicas)
}

// Each input below gets an error and no copy. Components are checked in name
// order, so a source with several invalid bounds always gets the same error.
// Only replicas that match no WorkloadCluster give an *UnassignedReplicasError,
// the one error the Split plan webhook admits.
func TestProjectSplitReplicasErrors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.InferenceService)
		member  string
		wantErr string
		// unassigned is the Replicas of the *UnassignedReplicasError, or 0 when
		// the error is another kind.
		unassigned int32
	}{
		{name: "mode other than Split", member: "member-a", edit: func(s *v1beta1.InferenceService) { s.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity },
			wantErr: "projection supports only explicit ClusterAffinity placement with mode Split"},
		{name: "no engine", member: "member-a", edit: func(s *v1beta1.InferenceService) { s.Spec.Engine = nil },
			wantErr: "projection requires a declared engine component"},
		{name: "negative engine minimum", member: "member-a", edit: func(s *v1beta1.InferenceService) { s.Spec.Engine.MinReplicas = ptr.To(-1) },
			wantErr: "invalid source engine replica bounds: minReplicas must be >= 0"},
		{name: "engine minimum past the int32 range", member: "member-a", edit: func(s *v1beta1.InferenceService) { s.Spec.Engine.MinReplicas = ptr.To(int(int64(math.MaxInt32) + 1)) },
			wantErr: "engine.minReplicas must fit a nonnegative int32 replica count"},
		{name: "negative router minimum", member: "member-a", edit: func(s *v1beta1.InferenceService) {
			s.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(-1)}}
		}, wantErr: "invalid source router replica bounds: minReplicas must be >= 0"},
		{name: "the decoder is checked before the engine", member: "member-a", edit: func(s *v1beta1.InferenceService) {
			s.Spec.Engine.MinReplicas, s.Spec.Decoder.MaxReplicas = ptr.To(-1), -1
		}, wantErr: "invalid source decoder replica bounds: maxReplicas must be >= 1"},
		{name: "member without a WorkloadCluster", member: "member-c",
			wantErr: `member "member-c" has no WorkloadCluster registration in the inputs`},
		{name: "terms that match no WorkloadCluster", member: "member-a", edit: func(s *v1beta1.InferenceService) {
			s.Spec.Placement.ClusterAffinity = testAffinity("metadata.name=member-c")
		}, wantErr: "6 replicas are unassigned: supply the complete matching WorkloadCluster inventory", unassigned: 6},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := ratioSplitSource(6, nil)
			source.Spec.Placement.ClusterAffinity = testAffinity("metadata.name=member-a")
			if tt.edit != nil {
				tt.edit(source)
			}
			projected, err := ProjectSplitReplicas(source, registeredClusters("member-a", "member-b"), tt.member)
			require.EqualError(t, err, tt.wantErr)
			require.Nil(t, projected)
			var unassigned *UnassignedReplicasError
			if tt.unassigned == 0 {
				require.NotErrorAs(t, err, &unassigned)
				return
			}
			require.ErrorAs(t, err, &unassigned)
			require.Equal(t, tt.unassigned, unassigned.Replicas)
		})
	}
}

// The source divides its routers across member-a and member-b but asks for
// only 1 router, so the controller's planning code refuses the plan.
// ProjectSplitReplicas returns that error for both clusters, and it is not an
// *UnassignedReplicasError.
func TestProjectSplitReplicasReturnsPlanningErrors(t *testing.T) {
	source := ratioSplitSource(6, nil)
	source.Spec.Placement.ClusterAffinity = weightedTermsOn(map[string]v1beta1.ClusterAffinityWeights{"member-a": {Default: ratio(1), Router: ratio(1)}, "member-b": {Default: ratio(1), Router: ratio(1)}})
	source.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}}
	for _, member := range []string{"member-a", "member-b"} {
		projected, err := ProjectSplitReplicas(source, registeredClusters("member-a", "member-b"), member)
		require.ErrorContains(t, err, "router minimum 1 cannot give each of the 2", member)
		require.Nil(t, projected, member)
		var unassigned *UnassignedReplicasError
		require.NotErrorAs(t, err, &unassigned, member)
	}
}
