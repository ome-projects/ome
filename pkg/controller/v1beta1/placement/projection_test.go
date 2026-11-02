package placement

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestProjectSplitReplicasUsesOfflineSnapshot(t *testing.T) {
	source := ratioSplitSource(60, ptr.To(40))
	source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
	source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{SourceUID: "different-source"}}
	before := source.DeepCopy()
	clusters := []v1beta1.WorkloadCluster{{ObjectMeta: metav1.ObjectMeta{Name: "member-a"}}, {ObjectMeta: metav1.ObjectMeta{Name: "member-b"}}}

	projected, err := ProjectSplitReplicas(source, clusters, "member-a")
	require.NoError(t, err)
	require.Equal(t, 40, *projected.Spec.Engine.MinReplicas)
	require.Equal(t, 27, *projected.Spec.Decoder.MinReplicas)
	require.Empty(t, projected.Status)
	require.Equal(t, before, source)
}
