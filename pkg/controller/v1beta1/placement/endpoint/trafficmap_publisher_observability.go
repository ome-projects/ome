package endpoint

import (
	"github.com/prometheus/client_golang/prometheus"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var trafficMapPublicationFallback = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "ome_trafficmap_publication_fallback",
	Help: "Whether a TrafficMap publisher is currently serving a retained positive publication plan.",
}, []string{"namespace", "trafficmap", "publisher"})

func init() {
	ctrlmetrics.Registry.MustRegister(trafficMapPublicationFallback)
}

func recordTrafficMapPublicationFallbackMetric(trafficMap *v1beta1.TrafficMap, publisher string) {
	if trafficMap == nil || trafficMap.Name == "" || publisher == "" {
		return
	}
	value := float64(0)
	if condition := apimeta.FindStatusCondition(
		trafficMap.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	); condition != nil && condition.Status == metav1.ConditionTrue {
		value = 1
	}
	trafficMapPublicationFallback.WithLabelValues(
		trafficMap.Namespace, trafficMap.Name, publisher,
	).Set(value)
}

func deleteTrafficMapPublicationFallbackMetric(namespace, trafficMap, _ string) {
	if trafficMap == "" {
		return
	}
	trafficMapPublicationFallback.DeletePartialMatch(prometheus.Labels{
		"namespace":  namespace,
		"trafficmap": trafficMap,
	})
}
