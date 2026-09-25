package routing

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// One series per capacity-enabled TrafficMap keeps cardinality proportional to
// live routing objects. Per-home details remain in
// spec.entries[].capacity.fallbackReason, where they do not create metric label
// values from transport or decoder errors.
var capacityFallbackHomes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "ome_trafficmap_capacity_fallback_homes",
	Help: "Number of homes currently using control-plane allocation because configured endpoint capacity did not produce a usable ceiling.",
}, []string{"namespace", "trafficmap"})

func init() {
	ctrlmetrics.Registry.MustRegister(capacityFallbackHomes)
}

func capacityFallbackCount(spec v1beta1.TrafficMapSpec) int {
	count := 0
	for _, entry := range spec.Entries {
		if entry.Capacity != nil && entry.Capacity.FallbackReason != "" {
			count++
		}
	}
	return count
}

func capacityFallbackCondition(
	spec v1beta1.TrafficMapSpec,
	enabled bool,
	observedGeneration int64,
) metav1.Condition {
	condition := metav1.Condition{
		Type:               v1beta1.TrafficMapCapacityFallback,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: observedGeneration,
	}
	switch {
	case !enabled:
		condition.Reason = v1beta1.TrafficMapReasonCapacityPollingDisabled
		condition.Message = "endpoint capacity polling is disabled"
	case len(spec.Entries) == 0:
		condition.Reason = v1beta1.TrafficMapReasonNoCapacityTargets
		condition.Message = "no addressable homes require endpoint capacity"
	default:
		fallbacks := capacityFallbackCount(spec)
		if fallbacks == 0 {
			condition.Reason = v1beta1.TrafficMapReasonEndpointCapacityAvailable
			condition.Message = fmt.Sprintf("endpoint capacity is available for all %d home(s)", len(spec.Entries))
			break
		}
		condition.Status = metav1.ConditionTrue
		condition.Reason = v1beta1.TrafficMapReasonEndpointCapacityFallback
		condition.Message = fmt.Sprintf(
			"%d of %d home(s) are using control-plane capacity; inspect spec.entries[].capacity.fallbackReason",
			fallbacks, len(spec.Entries),
		)
	}
	return condition
}

func recordCapacityFallbackMetric(namespace, trafficMap string, enabled bool, spec v1beta1.TrafficMapSpec) {
	if trafficMap == "" {
		return
	}
	if !enabled {
		deleteCapacityFallbackMetric(namespace, trafficMap)
		return
	}
	capacityFallbackHomes.WithLabelValues(namespace, trafficMap).Set(float64(capacityFallbackCount(spec)))
}

func deleteCapacityFallbackMetric(namespace, trafficMap string) {
	if trafficMap == "" {
		return
	}
	capacityFallbackHomes.DeleteLabelValues(namespace, trafficMap)
}
