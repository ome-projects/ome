package pod

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestMutator_ReportOnce_WritesEachBlockOnce(t *testing.T) {
	mutator := &Mutator{}

	assert.True(t, mutator.reportOnce(RDMAConfigKeyName, "nothing"), "the first report of a block is written")
	assert.False(t, mutator.reportOnce(RDMAConfigKeyName, "nothing"), "the second report of the same block is not")
	assert.True(t, mutator.reportOnce(RuntimeProfilesConfigKeyName, "nothing"), "another block is reported on its own")
}

func TestMutate_EmptyConfigMapAdmitsTheWholeChainWithoutTables(t *testing.T) {
	mutator := &Mutator{Client: fakeclient.New(t)}
	pod := makePod(map[string]string{
		constants.RDMAAutoInjectAnnotationKey:              "true",
		constants.RuntimeShmProfileAnnotationKey:           "default",
		constants.RuntimeProbeProfileAnnotationKey:         "sglang-http",
		constants.RuntimeObservabilityProfileAnnotationKey: "prometheus",
		constants.EnableMetricAggregation:                  "true",
		constants.SetPrometheusAnnotation:                  "true",
	}, defaultContainer(), v1.Container{Name: "queue-proxy"})

	require.NoError(t, mutator.mutate(pod, &v1.ConfigMap{Data: map[string]string{}}))

	main := pod.Spec.Containers[0]
	assert.Empty(t, main.Env, "no rdma block, no env")
	assert.Nil(t, main.ReadinessProbe, "no runtimeProfiles block, no probes")
	assert.Empty(t, pod.Spec.Containers[1].Env, "no metrics ports, nothing on queue-proxy")
	assert.NotContains(t, pod.Annotations, constants.PrometheusPortAnnotationKey)
	assert.Len(t, pod.Spec.Volumes, 1, "the shm profile needs no table and still mounts /dev/shm")
	assert.Equal(t, DshmVolumeName, pod.Spec.Volumes[0].Name)
}
