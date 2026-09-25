package routing

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestCapacityFallbackCondition(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
		spec    v1beta1.TrafficMapSpec
		status  metav1.ConditionStatus
		reason  string
	}{
		{
			name:   "disabled",
			status: metav1.ConditionFalse,
			reason: v1beta1.TrafficMapReasonCapacityPollingDisabled,
		},
		{
			name:    "enabled without targets",
			enabled: true,
			status:  metav1.ConditionFalse,
			reason:  v1beta1.TrafficMapReasonNoCapacityTargets,
		},
		{
			name:    "all reports usable",
			enabled: true,
			spec: v1beta1.TrafficMapSpec{Entries: []v1beta1.TrafficMapEntry{
				{Cluster: "a", Capacity: &v1beta1.TrafficMapCapacity{}},
				{Cluster: "b", Capacity: &v1beta1.TrafficMapCapacity{}},
			}},
			status: metav1.ConditionFalse,
			reason: v1beta1.TrafficMapReasonEndpointCapacityAvailable,
		},
		{
			name:    "one report fell open",
			enabled: true,
			spec: v1beta1.TrafficMapSpec{Entries: []v1beta1.TrafficMapEntry{
				{Cluster: "a", Capacity: &v1beta1.TrafficMapCapacity{FallbackReason: "unreachable"}},
				{Cluster: "b", Capacity: &v1beta1.TrafficMapCapacity{}},
			}},
			status: metav1.ConditionTrue,
			reason: v1beta1.TrafficMapReasonEndpointCapacityFallback,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := capacityFallbackCondition(tt.spec, tt.enabled, 9)
			assert.Equal(t, v1beta1.TrafficMapCapacityFallback, condition.Type)
			assert.Equal(t, tt.status, condition.Status)
			assert.Equal(t, tt.reason, condition.Reason)
			assert.Equal(t, int64(9), condition.ObservedGeneration)
		})
	}
}

func TestCapacityFallbackMetricTracksCurrentStateAndCleansUp(t *testing.T) {
	capacityFallbackHomes.Reset()
	t.Cleanup(capacityFallbackHomes.Reset)

	spec := v1beta1.TrafficMapSpec{Entries: []v1beta1.TrafficMapEntry{
		{Cluster: "a", Capacity: &v1beta1.TrafficMapCapacity{FallbackReason: "unreachable"}},
		{Cluster: "b", Capacity: &v1beta1.TrafficMapCapacity{}},
	}}
	recordCapacityFallbackMetric("ns", "map-a", true, spec)
	registered, err := testutil.GatherAndCount(
		ctrlmetrics.Registry, "ome_trafficmap_capacity_fallback_homes",
	)
	require.NoError(t, err)
	assert.Equal(t, 1, registered, "the controller-runtime metrics endpoint must expose the gauge")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		capacityFallbackHomes.WithLabelValues("ns", "map-a"),
	))

	spec.Entries[0].Capacity.FallbackReason = ""
	recordCapacityFallbackMetric("ns", "map-a", true, spec)
	assert.Equal(t, float64(0), testutil.ToFloat64(
		capacityFallbackHomes.WithLabelValues("ns", "map-a"),
	))

	recordCapacityFallbackMetric("ns", "map-b", true, spec)
	deleteCapacityFallbackMetric("ns", "map-a")
	assert.Equal(t, 1, testutil.CollectAndCount(capacityFallbackHomes),
		"deleting one TrafficMap must preserve other live series")

	recordCapacityFallbackMetric("ns", "map-b", false, spec)
	assert.Equal(t, 0, testutil.CollectAndCount(capacityFallbackHomes),
		"disabling endpoint capacity must remove its series")
}
