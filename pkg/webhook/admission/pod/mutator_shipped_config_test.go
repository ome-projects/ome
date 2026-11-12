package pod

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateMutatedPods = flag.Bool("update", false, "rewrite testdata/mutated_pods.golden.json from this run")

// mutationFixtures is one pod per path the annotation-gated injectors take:
// the RDMA profiles and their merge rules, the probe and observability
// profiles with and without a port, metrics aggregation and scraping in
// every combination, and the whole chain on one pod.
func mutationFixtures() []struct {
	name string
	pod  *v1.Pod
} {
	container := func(name string) v1.Container { return v1.Container{Name: name} }
	queueProxy := v1.Container{Name: "queue-proxy", Ports: []v1.ContainerPort{{Name: "http-usermetric", ContainerPort: 9091, Protocol: "TCP"}}}
	rdma := func(extra map[string]string) map[string]string {
		annos := map[string]string{constants.RDMAAutoInjectAnnotationKey: "true"}
		for k, v := range extra {
			annos[k] = v
		}
		return annos
	}
	withEnv := container(DefaultContainerName)
	withEnv.Env = []v1.EnvVar{{Name: "NCCL_DEBUG", Value: "WARN"}}
	withEnv.VolumeMounts = []v1.VolumeMount{{Name: "dshm", MountPath: "/dev/shm"}}
	withEnv.SecurityContext = &v1.SecurityContext{Capabilities: &v1.Capabilities{Add: []v1.Capability{"SYS_PTRACE"}}}
	withReadiness := container(DefaultContainerName)
	withReadiness.ReadinessProbe = &v1.Probe{ProbeHandler: v1.ProbeHandler{HTTPGet: &v1.HTTPGetAction{Path: "/custom", Port: intstr.FromInt32(1234)}}}
	networksPod := makePod(rdma(map[string]string{constants.RDMAProfileAnnotationKey: "multus-attachments", MultusNetworksAnnotation: `[{"name":"rdma0-macvlan","namespace":"multus-system"},{"name":"other","namespace":"net"}]`}), container(DefaultContainerName))
	return []struct {
		name string
		pod  *v1.Pod
	}{
		{"rdma default profile", makePod(rdma(nil), container(DefaultContainerName))},
		{"rdma default profile merges with the container's own values", makePod(rdma(nil), withEnv)},
		{"rdma default profile on a named container", makePod(rdma(map[string]string{constants.RDMAContainerNameAnnotationKey: "engine"}), container("engine"), container("sidecar"))},
		{"rdma default profile skips a pod without the container", makePod(rdma(nil), container("other"))},
		{"rdma named env-only profile", makePod(rdma(map[string]string{constants.RDMAProfileAnnotationKey: "collective-env"}), container(DefaultContainerName))},
		{"rdma named networks-only profile merges the existing annotation", networksPod},
		{"probe profile with the default port", makePod(map[string]string{constants.RuntimeProbeProfileAnnotationKey: "sglang-http"}, container(DefaultContainerName))},
		{"probe profile with a named port keeps an existing probe", makePod(map[string]string{constants.RuntimeProbeProfileAnnotationKey: "vllm-http", constants.RuntimeProbePortAnnotationKey: "8080"}, withReadiness)},
		{"observability profile with the default port", makePod(map[string]string{constants.RuntimeObservabilityProfileAnnotationKey: "prometheus"}, container(DefaultContainerName))},
		{"observability profile with a named port keeps an existing annotation", makePod(map[string]string{constants.RuntimeObservabilityProfileAnnotationKey: "prometheus", constants.RuntimeObservabilityPortAnnotationKey: "9090", constants.PrometheusPathAnnotationKey: "/own"}, container(DefaultContainerName))},
		{"metrics aggregation and scraping on", makePod(map[string]string{constants.EnableMetricAggregation: "true", constants.SetPrometheusAnnotation: "true"}, container(DefaultContainerName), queueProxy)},
		{"metrics aggregation on with the container's own port and path", makePod(map[string]string{constants.EnableMetricAggregation: "true", constants.SetPrometheusAnnotation: "true", constants.ContainerPrometheusPortKey: "9500", constants.ContainerPrometheusPathKey: "/stats"}, container(DefaultContainerName), queueProxy)},
		{"metrics scraping on without aggregation", makePod(map[string]string{constants.EnableMetricAggregation: "false", constants.SetPrometheusAnnotation: "true"}, container(DefaultContainerName), queueProxy)},
		{"metrics aggregation on without a queue-proxy", makePod(map[string]string{constants.EnableMetricAggregation: "true", constants.SetPrometheusAnnotation: "true"}, container(DefaultContainerName))},
		{"metrics switches left to the ConfigMap", makePod(nil, container(DefaultContainerName), queueProxy)},
		{"the whole chain on one pod", makePod(map[string]string{
			constants.RDMAAutoInjectAnnotationKey:              "true",
			constants.RuntimeShmProfileAnnotationKey:           "default",
			constants.RuntimeProbeProfileAnnotationKey:         "sglang-http",
			constants.RuntimeObservabilityProfileAnnotationKey: "prometheus",
			constants.EnableMetricAggregation:                  "true",
			constants.SetPrometheusAnnotation:                  "true",
		}, container(DefaultContainerName), queueProxy)},
	}
}

// TestMutate_ShippedConfigGivesTheGoldenPods runs every fixture through the
// webhook with the ConfigMap the kustomize profile ships, its empty rdma
// block filled with the fixture table the way a cluster's values file fills it,
// and compares the admitted pods with testdata/mutated_pods.golden.json, so
// the shipped tables keep producing the pods they produce today. Run with
// -update to rewrite the golden from this run.
func TestMutate_ShippedConfigGivesTheGoldenPods(t *testing.T) {
	mutator := Mutator{Client: fakeclient.New(t)}
	configMap := configMapWithRDMAFixture(t, shippedConfigMap(t))
	type entry struct {
		Name string  `json:"name"`
		Pod  *v1.Pod `json:"pod"`
	}
	var got []entry
	for _, fixture := range mutationFixtures() {
		require.NoError(t, mutator.mutate(fixture.pod, configMap), fixture.name)
		got = append(got, entry{Name: fixture.name, Pod: fixture.pod})
	}
	rendered, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)
	rendered = append(rendered, '\n')

	golden := filepath.Join("testdata", "mutated_pods.golden.json")
	if *updateMutatedPods {
		require.NoError(t, os.MkdirAll(filepath.Dir(golden), 0o755))
		require.NoError(t, os.WriteFile(golden, rendered, 0o644))
		return
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(rendered), "the shipped ConfigMap must give the golden pods; run with -update after a deliberate change")
}
