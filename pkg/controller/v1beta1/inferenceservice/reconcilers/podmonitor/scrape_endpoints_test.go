package podmonitor

import (
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// scrapeValues is a complete podMonitor block: the values the chart ships.
func scrapeValues() controllerconfig.PodMonitorConfig {
	return controllerconfig.PodMonitorConfig{Path: "/metrics", Interval: "10s", Port: "metrics", FallbackPort: "http"}
}

// podWithPorts is a one-container pod whose ports carry the given names in
// order; an empty name is an unnamed port.
func podWithPorts(names ...string) *corev1.PodSpec {
	ports := make([]corev1.ContainerPort, 0, len(names))
	for i, name := range names {
		ports = append(ports, corev1.ContainerPort{Name: name, ContainerPort: int32(8000 + i)})
	}
	return &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine", Ports: ports}}}
}

// The test: the configured port is scraped wherever it stands among the
// first container's ports; a container without it is scraped on its first
// port when that port is named, else on the configured fallback; a block
// without a fallback names no port for such a container.
func TestScrapePortName(t *testing.T) {
	noFallback := scrapeValues()
	noFallback.FallbackPort = ""
	cases := []struct {
		name string
		cfg  controllerconfig.PodMonitorConfig
		pod  *corev1.PodSpec
		want string
	}{
		{"configured port after a named port", scrapeValues(), podWithPorts("http", "metrics"), "metrics"},
		{"configured port after an unnamed port", scrapeValues(), podWithPorts("", "metrics"), "metrics"},
		{"first port named, configured port absent", scrapeValues(), podWithPorts("grpc", "http"), "grpc"},
		{"first port unnamed, configured port absent", scrapeValues(), podWithPorts("", "grpc"), "http"},
		{"no ports", scrapeValues(), podWithPorts(), "http"},
		{"no containers", scrapeValues(), &corev1.PodSpec{}, "http"},
		{"nil pod", scrapeValues(), nil, "http"},
		{"no fallback and no named port", noFallback, podWithPorts(""), ""},
		{"no configured port, first named port", controllerconfig.PodMonitorConfig{FallbackPort: "http"}, podWithPorts("grpc", "metrics"), "grpc"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, ScrapePortName(c.cfg, c.pod))
		})
	}
}

// The test: OME's own endpoint comes first, at the configured path and
// interval on the configured port, and the annotation-declared endpoints
// follow at the same interval.
func TestScrapeEndpoints_ConfiguredValuesComeFirst(t *testing.T) {
	annotations := map[string]string{constants.ExtraPodMetricsEndpointsAnnotationKey: "http:/engine_metrics"}
	got := ScrapeEndpoints(scrapeValues(), podWithPorts("http", "metrics"), annotations)
	require.Equal(t, []monitoringv1.PodMetricsEndpoint{
		{Port: ptr.To("metrics"), Path: "/metrics", Interval: "10s"},
		{Port: ptr.To("http"), Path: "/engine_metrics", Interval: "10s"},
	}, got)
}

// The test: a block that lacks the path, the interval or every port name
// yields no endpoint of OME's own; the annotation-declared endpoints are
// still written, with the interval the block has.
func TestScrapeEndpoints_MissingValuesWriteNoOwnEndpoint(t *testing.T) {
	annotations := map[string]string{constants.ExtraPodMetricsEndpointsAnnotationKey: "http:/engine_metrics"}
	withoutPath, withoutInterval, withoutPorts := scrapeValues(), scrapeValues(), scrapeValues()
	withoutPath.Path = ""
	withoutInterval.Interval = ""
	withoutPorts.Port, withoutPorts.FallbackPort = "", ""
	cases := []struct {
		name string
		cfg  controllerconfig.PodMonitorConfig
		want []monitoringv1.PodMetricsEndpoint
	}{
		{"empty block", controllerconfig.PodMonitorConfig{}, []monitoringv1.PodMetricsEndpoint{{Port: ptr.To("http"), Path: "/engine_metrics"}}},
		{"no path", withoutPath, []monitoringv1.PodMetricsEndpoint{{Port: ptr.To("http"), Path: "/engine_metrics", Interval: "10s"}}},
		{"no interval", withoutInterval, []monitoringv1.PodMetricsEndpoint{{Port: ptr.To("http"), Path: "/engine_metrics"}}},
		{"no port names and no named port", withoutPorts, []monitoringv1.PodMetricsEndpoint{{Port: ptr.To("http"), Path: "/engine_metrics", Interval: "10s"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, ScrapeEndpoints(c.cfg, podWithPorts(""), annotations))
		})
	}
	require.Empty(t, ScrapeEndpoints(controllerconfig.PodMonitorConfig{}, podWithPorts("http"), nil), "no values and no annotation: no endpoint at all")
}
