package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestValidatePlacement(t *testing.T) {
	spec := func(p *v1beta1.PlacementSpec) *v1beta1.InferenceServiceSpec {
		return &v1beta1.InferenceServiceSpec{Placement: p}
	}

	t.Run("nil placement is ok", func(t *testing.T) {
		require.NoError(t, ValidatePlacement(spec(nil)))
	})

	t.Run("valid selectors + split", func(t *testing.T) {
		require.NoError(t, ValidatePlacement(spec(&v1beta1.PlacementSpec{
			Mode:            v1beta1.PlacementModeSplit,
			Requirements:    "accelerator in (gpu-a100, gpu-h100)",
			ClusterSelector: "provider=cloud-a,metadata.name in (cluster-a,cluster-b)",
			Split:           &v1beta1.SplitSpec{Replicas: ptr.To(int32(4)), MaxReplicasPerCluster: 3, MinReplicasPerCluster: 1},
		})))
	})

	t.Run("malformed requirements selector", func(t *testing.T) {
		err := ValidatePlacement(spec(&v1beta1.PlacementSpec{Requirements: "!!!"}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spec.placement.requirements")
	})

	t.Run("malformed clusterSelector", func(t *testing.T) {
		err := ValidatePlacement(spec(&v1beta1.PlacementSpec{ClusterSelector: "="}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spec.placement.clusterSelector")
	})

	t.Run("replicas must be >= 1", func(t *testing.T) {
		err := ValidatePlacement(spec(&v1beta1.PlacementSpec{Split: &v1beta1.SplitSpec{Replicas: ptr.To(int32(0))}}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "replicas")
	})

	t.Run("min per cluster must not exceed max", func(t *testing.T) {
		err := ValidatePlacement(spec(&v1beta1.PlacementSpec{
			Split: &v1beta1.SplitSpec{MaxReplicasPerCluster: 2, MinReplicasPerCluster: 5},
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "minReplicasPerCluster")
	})

	t.Run("negative caps rejected", func(t *testing.T) {
		require.Error(t, ValidatePlacement(spec(&v1beta1.PlacementSpec{Split: &v1beta1.SplitSpec{MaxReplicasPerCluster: -1}})))
		require.Error(t, ValidatePlacement(spec(&v1beta1.PlacementSpec{Split: &v1beta1.SplitSpec{MinReplicasPerCluster: -1}})))
	})
}
