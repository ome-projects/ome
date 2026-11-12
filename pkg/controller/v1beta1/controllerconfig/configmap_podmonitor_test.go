package controllerconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	testingutils "sigs.k8s.io/ome/pkg/utils/testing"
)

// shippedScrapeValues is the podMonitor scrape block both shipped
// configurations carry: the kustomize ConfigMap and the chart values.
var shippedScrapeValues = PodMonitorConfig{Path: "/metrics", Interval: "10s", Port: "metrics", FallbackPort: "http"}

// The test: the podMonitor block's scrape values parse beside its labels and
// relabelings, and a block without them parses to empty values, so the
// PodMonitor pass degrades instead of inventing them.
func TestInferenceServicesConfigFromConfigMapParsesPodMonitorScrapeValues(t *testing.T) {
	full := `{"path":"/metrics","interval":"10s","port":"metrics","fallbackPort":"http","labels":{"tier":"cluster"},"relabelings":[{"sourceLabels":["__meta_kubernetes_namespace"],"targetLabel":"k8s_namespace_name"}]}`
	cfg, err := inferenceServicesConfigFromConfigMap(&corev1.ConfigMap{Data: map[string]string{PodMonitorConfigName: full}})
	require.NoError(t, err)
	require.Equal(t, shippedScrapeValues.Path, cfg.PodMonitor.Path)
	require.Equal(t, shippedScrapeValues.Interval, cfg.PodMonitor.Interval)
	require.Equal(t, shippedScrapeValues.Port, cfg.PodMonitor.Port)
	require.Equal(t, shippedScrapeValues.FallbackPort, cfg.PodMonitor.FallbackPort)
	require.Equal(t, map[string]string{"tier": "cluster"}, cfg.PodMonitor.Labels)
	require.Len(t, cfg.PodMonitor.Relabelings, 1)

	cfg, err = inferenceServicesConfigFromConfigMap(&corev1.ConfigMap{Data: map[string]string{PodMonitorConfigName: `{"labels":{"tier":"cluster"}}`}})
	require.NoError(t, err)
	require.Equal(t, PodMonitorConfig{Labels: map[string]string{"tier": "cluster"}}, cfg.PodMonitor)
}

// The test: a scrape path that is not an HTTP path or an interval that is
// not a Prometheus duration is refused when the configuration loads, with
// the offending value in the error.
func TestInferenceServicesConfigFromConfigMapRejectsBadPodMonitorScrapeValues(t *testing.T) {
	for _, c := range []struct{ name, data, want string }{
		{"path without a slash", `{"path":"metrics","interval":"10s"}`, `path "metrics" must start with /`},
		{"interval that is not a duration", `{"path":"/metrics","interval":"ten seconds"}`, `interval "ten seconds"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := inferenceServicesConfigFromConfigMap(&corev1.ConfigMap{Data: map[string]string{PodMonitorConfigName: c.data}})
			require.ErrorContains(t, err, c.want)
		})
	}
}

// The test: the shipped configurations supply the scrape values the code
// has no default for, and both say the same thing: the kustomize ConfigMap
// under config/configmap and the chart's values.yaml.
func TestShippedConfigurationsCarryThePodMonitorScrapeValues(t *testing.T) {
	shipped, err := testingutils.ShippedInferenceServiceConfigMap()
	require.NoError(t, err)
	cfg, err := inferenceServicesConfigFromConfigMap(shipped)
	require.NoError(t, err)
	require.Equal(t, shippedScrapeValues, cfg.PodMonitor, "config/configmap/inferenceservice.yaml")

	var chart PodMonitorConfig
	require.NoError(t, testingutils.ShippedChartValue("ome.podMonitor", &chart))
	require.Equal(t, shippedScrapeValues, chart, "charts/ome-resources/values.yaml ome.podMonitor")
}
