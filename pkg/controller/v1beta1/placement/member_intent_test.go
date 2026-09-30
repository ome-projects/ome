package placement

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func TestDeriveISVCPlacementIntentStaysOnSource(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{
		v1beta1.PlacementModeSingle, v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit,
	} {
		t.Run(string(mode), func(t *testing.T) {
			src := srcISVCMode(mode, "accelerator=test")
			src.Spec.Placement.ClusterSelector = "metadata.name=cluster-a"
			src.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To(int32(4)), MaxReplicasPerCluster: 3}
			before := src.DeepCopy()
			derived := DeriveISVC(src, "control-plane", "serving")
			require.Nil(t, derived.Spec.Placement)
			require.False(t, IsPlacementEligible(derived), "a member copy cannot become a placement source")
			require.Equal(t, before, src)
		})
	}
}

func TestReconcileSplitMemberKeepsConcreteReplicaPolicy(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	worker := emptyWorker(scheme)
	source := srcISVCSplit("accelerator=test", 4)
	source.Spec.Placement.Split.MaxReplicasPerCluster = 6
	source.Spec.Engine.MinReplicas = ptr.To(8)
	source.Spec.Engine.MaxReplicas = 10
	source.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
		MinReplicas: ptr.To(8), MaxReplicas: 10,
	}}
	source.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
		MinReplicas: ptr.To(1), MaxReplicas: 2,
	}}
	before := source.DeepCopy()

	// An owned member written with placement intent must converge to a local
	// workload while retaining its identity and member-managed metadata.
	member := source.DeepCopy()
	member.UID = "member-uid"
	member.Labels = map[string]string{PlacementOriginLabel: string(source.UID), "member-label": "keep"}
	require.NoError(t, worker.Create(ctx, member))
	clusters := splitTestClusters{fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"cluster-a": workloadcluster.NewNeverCachingClient(worker),
	}}}
	source.Status.Placement = &v1beta1.PlacementStatus{
		Plan:       &v1beta1.PlacementPlanStatus{ID: "plan-a", Revision: 1, SourceUID: source.UID, ObservedGeneration: source.Generation},
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "cluster-a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "cluster-a-uid", CurrentReplicas: 4, DesiredReplicas: 4}}},
	}
	r, cp := newPlacer(scheme, clusters, source, readyWC("cluster-a", map[string]string{"accelerator": "test"}))
	for range 2 {
		err := r.placePlannedOn(ctx, source, source.Status.Placement.Candidates[0])
		require.NoError(t, err)
		got := &v1beta1.InferenceService{}
		require.NoError(t, worker.Get(ctx, req().NamespacedName, got))
		require.Nil(t, got.Spec.Placement)
		require.False(t, IsPlacementEligible(got))
		require.EqualValues(t, "member-uid", got.UID)
		require.Equal(t, "keep", got.Labels["member-label"])
		require.Equal(t, 4, *got.Spec.Engine.MinReplicas)
		require.Equal(t, 6, got.Spec.Engine.MaxReplicas)
		require.Equal(t, 4, *got.Spec.Decoder.MinReplicas)
		require.Equal(t, 6, got.Spec.Decoder.MaxReplicas)
		require.Equal(t, before.Spec.Router, got.Spec.Router)
	}
	liveSource := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(ctx, req().NamespacedName, liveSource))
	require.Equal(t, before.Spec, liveSource.Spec)
}
