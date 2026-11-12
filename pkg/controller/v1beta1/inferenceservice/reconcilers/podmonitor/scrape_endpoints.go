package podmonitor

import (
	"slices"
	"sync"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// missingScrapeValuesOnce guards the one log line that says the podMonitor
// block lacks a value OME's own endpoint needs; after it every Component's
// PodMonitor carries only annotation-declared endpoints, which the operator
// has been told once.
var missingScrapeValuesOnce sync.Once

// ScrapeEndpoints is the endpoint list of a Component's PodMonitor: OME's
// own endpoint first, cfg.Path on the port ScrapePortName picks every
// cfg.Interval, then the endpoints the Component's extra-endpoints
// annotation declares at the same interval. OME's endpoint needs the path,
// the interval and a port name; a block that lacks one of them yields the
// annotation endpoints alone.
func ScrapeEndpoints(cfg controllerconfig.PodMonitorConfig, podSpec *corev1.PodSpec, annotations map[string]string) []monitoringv1.PodMetricsEndpoint {
	var endpoints []monitoringv1.PodMetricsEndpoint
	if own, ok := ownScrapeEndpoint(cfg, podSpec); ok {
		endpoints = append(endpoints, own)
	}
	return append(endpoints, ParseExtraEndpoints(annotations, monitoringv1.Duration(cfg.Interval))...)
}

// ownScrapeEndpoint is the endpoint OME adds from the configured values, or
// false, logged once per manager, when the block lacks one of them.
func ownScrapeEndpoint(cfg controllerconfig.PodMonitorConfig, podSpec *corev1.PodSpec) (monitoringv1.PodMetricsEndpoint, bool) {
	portName := ScrapePortName(cfg, podSpec)
	if cfg.Path == "" || cfg.Interval == "" || portName == "" {
		missingScrapeValuesOnce.Do(func() {
			log.Info("the podMonitor block of inferenceservice-config lacks a scrape endpoint value; PodMonitors carry only annotation-declared endpoints",
				"path", cfg.Path, "interval", cfg.Interval, "port", cfg.Port, "fallbackPort", cfg.FallbackPort)
		})
		return monitoringv1.PodMetricsEndpoint{}, false
	}
	return monitoringv1.PodMetricsEndpoint{Port: &portName, Path: cfg.Path, Interval: monitoringv1.Duration(cfg.Interval)}, true
}

// ScrapePortName is the container port OME scrapes on a Component's first
// container: cfg.Port when the container declares it, wherever it stands;
// else the first port when it is named; else cfg.FallbackPort, which is
// empty when the block names none.
func ScrapePortName(cfg controllerconfig.PodMonitorConfig, podSpec *corev1.PodSpec) string {
	if podSpec == nil || len(podSpec.Containers) == 0 {
		return cfg.FallbackPort
	}
	ports := podSpec.Containers[0].Ports
	if cfg.Port != "" && slices.ContainsFunc(ports, func(port corev1.ContainerPort) bool { return port.Name == cfg.Port }) {
		return cfg.Port
	}
	if len(ports) > 0 && ports[0].Name != "" {
		return ports[0].Name
	}
	return cfg.FallbackPort
}
