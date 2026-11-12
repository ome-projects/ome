package pod

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestMutate_RDMAProfileTableComesFromTheConfigMap(t *testing.T) {
	configMap := &v1.ConfigMap{Data: map[string]string{"rdma": `{
  "defaultProfile": "fabric-a",
  "profiles": {
    "fabric-a": {"env": {"NCCL_IB_HCA": "=mlx5_42", "NCCL_SOCKET_IFNAME": "net1"}}
  }
}`}}
	pod := makePod(map[string]string{constants.RDMAAutoInjectAnnotationKey: "true"}, defaultContainer())

	require.NoError(t, (&Mutator{Client: fakeclient.New(t)}).mutate(pod, configMap))

	assert.Equal(t, []v1.EnvVar{
		{Name: "NCCL_IB_HCA", Value: "=mlx5_42"},
		{Name: "NCCL_SOCKET_IFNAME", Value: "net1"},
	}, pod.Spec.Containers[0].Env, "the ConfigMap's profile decides the env and nothing else is injected")
	assert.Empty(t, pod.Spec.Volumes, "a profile without volumes adds none")
	assert.Nil(t, pod.Spec.Containers[0].SecurityContext, "a profile without a security context sets none")
}

func TestInjectRDMA_AbsentTableInjectsNothing(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RDMAAutoInjectAnnotationKey: "true",
		constants.RDMAProfileAnnotationKey:    "fabric-a",
	}, defaultContainer())
	before := pod.DeepCopy()

	require.NoError(t, injectRDMA(pod, nil))

	assert.Equal(t, before, pod, "without the rdma block the pod is admitted as written")
}

func TestInjectRDMA_NoProfileNamedAnywhereIsRefused(t *testing.T) {
	table := &RDMAConfig{Profiles: map[string]RDMAProfile{"fabric-a": {}}}
	pod := makePod(map[string]string{constants.RDMAAutoInjectAnnotationKey: "true"}, defaultContainer())

	err := injectRDMA(pod, table)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no RDMA profile")
}

func TestNewRDMAConfig_MalformedBlockIsRefused(t *testing.T) {
	_, err := newRDMAConfig(&v1.ConfigMap{Data: map[string]string{RDMAConfigKeyName: `{"profiles": [}`}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), RDMAConfigKeyName)
}

// A chart install ships the rdma block with no profile and no default: every
// cluster's fabric differs, so the cluster's values file carries the table.
func TestShippedRDMATable_NamesNoProfile(t *testing.T) {
	table, err := newRDMAConfig(shippedConfigMap(t))

	require.NoError(t, err)
	require.NotNil(t, table, "the shipped ConfigMap carries the rdma key")
	assert.Empty(t, table.Profiles)
	assert.Empty(t, table.DefaultProfile)
}

func TestInjectRDMA_TableWithoutProfilesInjectsNothing(t *testing.T) {
	pod := makePod(map[string]string{
		constants.RDMAAutoInjectAnnotationKey: "true",
		constants.RDMAProfileAnnotationKey:    "roce",
	}, defaultContainer())
	before := pod.DeepCopy()

	require.NoError(t, injectRDMA(pod, &RDMAConfig{}))

	assert.Equal(t, before, pod, "with no profile configured the pod is admitted as written, even when it names one")
}

func TestMutate_ShippedConfigReportsTheEmptyRDMATableOnce(t *testing.T) {
	mutator := &Mutator{Client: fakeclient.New(t)}
	pod := makePod(map[string]string{constants.RDMAAutoInjectAnnotationKey: "true"}, defaultContainer())

	require.NoError(t, mutator.mutate(pod, shippedConfigMap(t)))

	assert.Empty(t, pod.Spec.Containers[0].Env, "a default install injects no RDMA env")
	assert.Empty(t, pod.Spec.Volumes, "a default install injects no RDMA volumes")
	_, reported := mutator.reported.Load(RDMAConfigKeyName)
	assert.True(t, reported, "the table without profiles is reported once per process")
}
