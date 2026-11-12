package pod

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestMutate_MetricsPortsAndPathComeFromTheConfigMap(t *testing.T) {
	block := func(aggregation string) *v1.ConfigMap {
		return &v1.ConfigMap{Data: map[string]string{"metricsAggregator": `{
  "enableMetricAggregation": "` + aggregation + `",
  "enablePrometheusScraping": "true",
  "containerPort": 7001,
  "path": "/stats",
  "aggregatePort": 7002,
  "podPrometheusPort": 7003
}`}}
	}
	envOf := func(c v1.Container) map[string]string {
		env := map[string]string{}
		for _, e := range c.Env {
			env[e.Name] = e.Value
		}
		return env
	}

	t.Run("aggregation on", func(t *testing.T) {
		pod := makePod(nil, defaultContainer(), v1.Container{Name: "queue-proxy"})
		require.NoError(t, (&Mutator{Client: fakeclient.New(t)}).mutate(pod, block("true")))

		queueProxy := pod.Spec.Containers[1]
		env := envOf(queueProxy)
		assert.Equal(t, "7001", env[constants.ContainerPrometheusMetricsPortEnvVarKey], "the container's scrape port is the ConfigMap's")
		assert.Equal(t, "/stats", env[constants.ContainerPrometheusMetricsPathEnvVarKey])
		assert.Equal(t, "7002", env[constants.QueueProxyAggregatePrometheusMetricsPortEnvVarKey])
		require.Len(t, queueProxy.Ports, 1)
		assert.Equal(t, int32(7002), queueProxy.Ports[0].ContainerPort)
		assert.Equal(t, "7002", pod.Annotations[constants.PrometheusPortAnnotationKey])
		assert.Equal(t, "/stats", pod.Annotations[constants.PrometheusPathAnnotationKey])
	})

	t.Run("aggregation off", func(t *testing.T) {
		pod := makePod(nil, defaultContainer(), v1.Container{Name: "queue-proxy"})
		require.NoError(t, (&Mutator{Client: fakeclient.New(t)}).mutate(pod, block("false")))

		assert.Empty(t, pod.Spec.Containers[1].Env, "nothing is wired into queue-proxy without aggregation")
		assert.Equal(t, "7003", pod.Annotations[constants.PrometheusPortAnnotationKey], "the pod's scrape port is the ConfigMap's pod port")
		assert.Equal(t, "/stats", pod.Annotations[constants.PrometheusPathAnnotationKey])
	})
}

func TestInjectMetricsAggregator_BlockWithoutPortsInjectsNothing(t *testing.T) {
	ma, err := newMetricsAggregator(&v1.ConfigMap{Data: map[string]string{MetricsAggregatorConfigMapKeyName: `{
  "enableMetricAggregation": "true",
  "enablePrometheusScraping": "true"
}`}})
	require.NoError(t, err)
	pod := makePod(nil, defaultContainer(), v1.Container{Name: "queue-proxy"})

	require.NoError(t, ma.InjectMetricsAggregator(pod))

	assert.Empty(t, pod.Spec.Containers[1].Env, "no ports, nothing wired into queue-proxy")
	assert.Empty(t, pod.Spec.Containers[1].Ports)
	assert.NotContains(t, pod.Annotations, constants.PrometheusPortAnnotationKey, "no ports, no scrape annotation")
	assert.Equal(t, "true", pod.Annotations[constants.EnableMetricAggregation], "the switches the pod was given are still written")
}
