package endpoint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The stateless branch of the publisher pass: it may return no claims, and it
// waits while another publisher holds claims.

func TestTrafficMapPublisher_StatelessPublisherReturnsClaims_ReportsInvalidPlan(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: false, claims: []string{"unexpected"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidPlan, condition.Reason)
	assert.Equal(t, trafficMapPublisherFailureMessage(trafficMapPublisherReasonInvalidPlan), condition.Message)
}

func TestTrafficMapPublisher_StatelessWhileAnotherPublisherHoldsClaims_IsRejected(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	other := publisherClaimedTrafficMap("other", "different-publisher", []string{"their-target"})
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: false}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap, other)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonClaimRejected, condition.Reason)
}
