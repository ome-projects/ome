package pod

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestMutate_ProbeTimingsAndDefaultPortComeFromTheConfigMap(t *testing.T) {
	configMap := &v1.ConfigMap{Data: map[string]string{"runtimeProfiles": `{
  "defaultPort": 7000,
  "probes": {
    "readiness": {"initialDelaySeconds": 1, "periodSeconds": 2, "timeoutSeconds": 3, "failureThreshold": 4, "successThreshold": 1},
    "liveness": {"periodSeconds": 5, "timeoutSeconds": 6, "failureThreshold": 7},
    "startup": {"initialDelaySeconds": 8, "periodSeconds": 9, "timeoutSeconds": 10, "failureThreshold": 11}
  }
}`}}
	pod := makePod(map[string]string{
		constants.RuntimeProbeProfileAnnotationKey:         "sglang-http",
		constants.RuntimeObservabilityProfileAnnotationKey: "prometheus",
	}, defaultContainer())

	require.NoError(t, (&Mutator{Client: fakeclient.New(t)}).mutate(pod, configMap))

	c := pod.Spec.Containers[0]
	assert.Equal(t, intstr.FromInt32(7000), c.ReadinessProbe.HTTPGet.Port, "the probe port is the ConfigMap's default port")
	assert.Equal(t, int32(2), c.ReadinessProbe.PeriodSeconds)
	assert.Equal(t, int32(7), c.LivenessProbe.FailureThreshold)
	assert.Equal(t, int32(11), c.StartupProbe.FailureThreshold)
	assert.Equal(t, "7000", pod.Annotations[constants.PrometheusPortAnnotationKey], "the scrape port is the ConfigMap's default port")
}

func TestInjectProbes_AbsentBlockInjectsNothingButStillRefusesABadPort(t *testing.T) {
	pod := makePod(map[string]string{constants.RuntimeProbeProfileAnnotationKey: "sglang-http"}, defaultContainer())
	require.NoError(t, injectProbes(pod, nil))
	assert.Nil(t, pod.Spec.Containers[0].ReadinessProbe, "no timings, no probes")

	withPort := makePod(map[string]string{
		constants.RuntimeProbeProfileAnnotationKey: "sglang-http",
		constants.RuntimeProbePortAnnotationKey:    "8080",
	}, defaultContainer())
	require.NoError(t, injectProbes(withPort, nil))
	assert.Nil(t, withPort.Spec.Containers[0].ReadinessProbe, "a port alone does not give the timings")

	badPort := makePod(map[string]string{
		constants.RuntimeProbeProfileAnnotationKey: "sglang-http",
		constants.RuntimeProbePortAnnotationKey:    "not-a-number",
	}, defaultContainer())
	assert.Error(t, injectProbes(badPort, nil), "the pod's own annotation is checked whatever the ConfigMap holds")
}

func TestInjectObservability_AbsentBlockServesOnlyPodsThatNameAPort(t *testing.T) {
	unnamed := makePod(map[string]string{constants.RuntimeObservabilityProfileAnnotationKey: "prometheus"}, defaultContainer())
	require.NoError(t, injectObservability(unnamed, nil))
	assert.NotContains(t, unnamed.Annotations, constants.PrometheusScrapeAnnotationKey, "no default port, no scrape annotations")

	named := makePod(map[string]string{
		constants.RuntimeObservabilityProfileAnnotationKey: "prometheus",
		constants.RuntimeObservabilityPortAnnotationKey:    "9090",
	}, defaultContainer())
	require.NoError(t, injectObservability(named, nil))
	assert.Equal(t, "9090", named.Annotations[constants.PrometheusPortAnnotationKey])
	assert.Equal(t, "/metrics", named.Annotations[constants.PrometheusPathAnnotationKey])
}

func TestInjectProbes_BlockWithoutADefaultPortInjectsNothingForAnUnnamedPort(t *testing.T) {
	profiles := &RuntimeProfilesConfig{Probes: ProbeTimings{Readiness: ProbeTiming{PeriodSeconds: 5}}}
	pod := makePod(map[string]string{constants.RuntimeProbeProfileAnnotationKey: "sglang-http"}, defaultContainer())

	require.NoError(t, injectProbes(pod, profiles))

	assert.Nil(t, pod.Spec.Containers[0].ReadinessProbe)
}
