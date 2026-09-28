package endpoint

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestTrafficMapPublicationFallbackMetricTracksConditionAndDeletion(t *testing.T) {
	trafficMapPublicationFallback.Reset()
	t.Cleanup(trafficMapPublicationFallback.Reset)

	trafficMap := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: "team-a", Name: "model-a",
	}}
	apimeta.SetStatusCondition(&trafficMap.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapPublicationFallback, Status: metav1.ConditionTrue,
		Reason: v1beta1.TrafficMapReasonLastPositiveRetained,
	})
	recordTrafficMapPublicationFallbackMetric(trafficMap, "publisher-a")
	registered, err := testutil.GatherAndCount(
		ctrlmetrics.Registry, "ome_trafficmap_publication_fallback",
	)
	require.NoError(t, err)
	assert.Equal(t, 1, registered)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		trafficMapPublicationFallback.WithLabelValues("team-a", "model-a", "publisher-a"),
	))

	apimeta.SetStatusCondition(&trafficMap.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapPublicationFallback, Status: metav1.ConditionUnknown,
		Reason: v1beta1.TrafficMapReasonPublicationTransitioning,
	})
	recordTrafficMapPublicationFallbackMetric(trafficMap, "publisher-a")
	assert.Equal(t, float64(0), testutil.ToFloat64(
		trafficMapPublicationFallback.WithLabelValues("team-a", "model-a", "publisher-a"),
	))

	recordTrafficMapPublicationFallbackMetric(trafficMap, "publisher-b")
	other := trafficMap.DeepCopy()
	other.Name = "model-b"
	recordTrafficMapPublicationFallbackMetric(other, "publisher-a")
	deleteTrafficMapPublicationFallbackMetric("team-a", "model-a", "publisher-a")
	assert.Equal(t, 1, testutil.CollectAndCount(trafficMapPublicationFallback),
		"deleting a TrafficMap removes every publisher series without affecting another map")
}
