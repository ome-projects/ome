package pod

import (
	"encoding/json"
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils"
)

const MetricsAggregatorConfigMapKeyName = "metricsAggregator"

// MetricsAggregator is the metricsAggregator block of the
// inferenceservice-config ConfigMap. The two switches are the pod defaults
// for ome.io/enable-metric-aggregation and ome.io/enable-prometheus-scraping.
// The rest says where metrics are served: ContainerPort and Path on the
// serving container, unless the pod's prometheus.ome.io annotations name
// others; AggregatePort on queue-proxy for the aggregate; PodPrometheusPort
// as the pod's scrape port when aggregation is off.
type MetricsAggregator struct {
	EnableMetricAggregation  string `json:"enableMetricAggregation"`
	EnablePrometheusScraping string `json:"enablePrometheusScraping"`
	ContainerPort            int32  `json:"containerPort"`
	Path                     string `json:"path"`
	AggregatePort            int32  `json:"aggregatePort"`
	PodPrometheusPort        int32  `json:"podPrometheusPort"`
}

func newMetricsAggregator(configMap *v1.ConfigMap) (*MetricsAggregator, error) {
	ma := &MetricsAggregator{}

	if maConfigVal, ok := configMap.Data[MetricsAggregatorConfigMapKeyName]; ok {
		err := json.Unmarshal([]byte(maConfigVal), &ma)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall %v json string due to %w ", MetricsAggregatorConfigMapKeyName, err)
		}
	}

	return ma, nil
}

// portsConfigured reports whether the block names every port and the path;
// without them the injector wires nothing.
func (ma *MetricsAggregator) portsConfigured() bool {
	return ma.ContainerPort > 0 && ma.Path != "" && ma.AggregatePort > 0 && ma.PodPrometheusPort > 0
}

// setMetricAggregationEnvVarsAndPorts tells queue-proxy where to scrape
// the serving container's metrics and which port to expose the aggregated
// metrics on. The scrape port and path come from the pod's prometheus.ome.io
// annotations, else from the ConfigMap.
func (ma *MetricsAggregator) setMetricAggregationEnvVarsAndPorts(pod *v1.Pod) {
	proxy := findContainer(pod.Spec.Containers, "queue-proxy")
	if proxy == nil {
		return
	}
	omeContainerPromPort := strconv.Itoa(int(ma.ContainerPort))
	if port, ok := pod.ObjectMeta.Annotations[constants.ContainerPrometheusPortKey]; ok {
		omeContainerPromPort = port
	}
	omeContainerPromPath := ma.Path
	if path, ok := pod.ObjectMeta.Annotations[constants.ContainerPrometheusPathKey]; ok {
		omeContainerPromPath = path
	}
	// Update-if-exists like the port injection below: the webhook is
	// registered with reinvocationPolicy=IfNeeded, so this may run more
	// than once on the same pod and must not stack duplicates.
	proxy.Env = utils.MergeEnvs(proxy.Env, []v1.EnvVar{
		{Name: constants.ContainerPrometheusMetricsPortEnvVarKey, Value: omeContainerPromPort},
		{Name: constants.ContainerPrometheusMetricsPathEnvVarKey, Value: omeContainerPromPath},
		{Name: constants.QueueProxyAggregatePrometheusMetricsPortEnvVarKey, Value: strconv.Itoa(int(ma.AggregatePort))},
	})
	proxy.Ports = utils.AppendPortIfNotExists(proxy.Ports, v1.ContainerPort{
		Name:          constants.AggregateMetricsPortName,
		ContainerPort: ma.AggregatePort,
		Protocol:      "TCP",
	})
}

// annotationOrDefault reads the pod annotation, writing value in first
// when the key is absent, so the pod carries the answer it was given.
func annotationOrDefault(pod *v1.Pod, key, value string) string {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	setIfAbsent(pod.Annotations, key, value)
	return pod.Annotations[key]
}

// InjectMetricsAggregator wires queue-proxy aggregate metrics + the
// pod-level prometheus.io scrape annotations. Defaults come from the
// inferenceservice-config ConfigMap; per-pod annotations override. A block
// without the ports leaves both alone.
func (ma *MetricsAggregator) InjectMetricsAggregator(pod *v1.Pod) error {
	aggregate := annotationOrDefault(pod, constants.EnableMetricAggregation, ma.EnableMetricAggregation) == "true"
	scrape := annotationOrDefault(pod, constants.SetPrometheusAnnotation, ma.EnablePrometheusScraping) == "true"
	if !ma.portsConfigured() {
		return nil
	}
	if aggregate {
		ma.setMetricAggregationEnvVarsAndPorts(pod)
	}
	if scrape {
		// Aggregation on ⇒ point prometheus at the aggregator port;
		// off ⇒ the pod's own prometheus port.
		podPromPort := ma.PodPrometheusPort
		if aggregate {
			podPromPort = ma.AggregatePort
		}
		pod.Annotations[constants.PrometheusPortAnnotationKey] = strconv.Itoa(int(podPromPort))
		pod.Annotations[constants.PrometheusPathAnnotationKey] = ma.Path
	}
	return nil
}
