package endpoint

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// Deletion of a TrafficMap: the publisher unpublishes from the live claim record,
// repeats while the record grows, and then removes its finalizer.

func TestTrafficMapPublisher_DeletingMapInactiveNoOwner_UnpublishesThenFinalizes(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	trafficMap.Status.SourceUID = ""
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"a", "b"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    1,
			ObservedISVCGeneration:  1,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "a", Weight: 1}, {Target: "b"}},
		},
	}
	fail := true
	var gotKey types.NamespacedName
	var gotJournal v1beta1.TrafficMapPublisherStatus
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		unpublish: func(_ context.Context, key types.NamespacedName, journal v1beta1.TrafficMapPublisherStatus) error {
			gotKey = key
			gotJournal = journal
			assert.Nil(t, journal.LastPositive, "finalization must clear fallback state before cleanup")
			if fail {
				return errors.New("temporary cleanup failure")
			}
			return nil
		},
	}
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap, legacyOwner)
	reconciler.Active = false

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.Error(t, err)
	assert.Equal(t, client.ObjectKeyFromObject(trafficMap), gotKey)
	assert.Equal(t, []string{"a", "b"}, gotJournal.ClaimedTargets)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Equal(t, []string{"a", "b"}, current.Status.Publisher.ClaimedTargets)
	assert.Nil(t, current.Status.Publisher.LastPositive)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonPublicationFinalizing, fallback.Reason,
		"failed cleanup must remain in its durable finalizing state for retry")

	fail = false
	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	got := &v1beta1.TrafficMap{}
	err = kubeClient.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), got)
	assert.True(t, apierrors.IsNotFound(err), "removing the last finalizer completes direct deletion")
}

func TestTrafficMapPublisher_DeletingMapSourceUIDMissingOrWrong_StillUnpublishes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sourceUID types.UID
	}{
		{name: "missing"},
		{name: "mismatched", sourceUID: "other-owner-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, trafficMap := publisherTestObjects()
			now := metav1.Now()
			trafficMap.DeletionTimestamp = &now
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.SourceUID = tc.sourceUID
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
			}
			publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

			require.NoError(t, err)
			assert.Equal(t, []string{"unpublish:target"}, publisher.events)
			err = kubeClient.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), &v1beta1.TrafficMap{})
			assert.True(t, apierrors.IsNotFound(err), "journal cleanup must remain owner-independent")
		})
	}
}

func TestTrafficMapPublisher_DeletingMapRecordNamesAnotherPublisher_KeepsFinalizer(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "old-publisher", ClaimedTargets: []string{"target"},
	}
	publisher := &recordingTrafficMapPublisher{name: "new-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, "old-publisher", current.Status.Publisher.PublisherName)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonPublisherChanged, condition.Reason)
}

func TestTrafficMapPublisher_DeletingMapWithStaleCache_UnpublishesLiveClaims(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"live-claim"},
	}
	baseClient := newTrafficMapPublisherClient(t, trafficMap)
	stale := trafficMap.DeepCopy()
	stale.Status.Publisher = nil
	cacheClient := fakeclient.Wrap(t, baseClient, fakeclient.StaleRead(stale))
	var gotClaims []string
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		unpublish: func(_ context.Context, _ types.NamespacedName, journal v1beta1.TrafficMapPublisherStatus) error {
			gotClaims = append([]string(nil), journal.ClaimedTargets...)
			return nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: cacheClient, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"live-claim"}, gotClaims)
}

func TestTrafficMapPublisher_ClaimsGrowDuringUnpublish_RepeatsBeforeFinalizing(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	now := metav1.Now()
	trafficMap.DeletionTimestamp = &now
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"a"},
	}

	var kubeClient client.Client
	var unpublished [][]string
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		unpublish: func(_ context.Context, _ types.NamespacedName, journal v1beta1.TrafficMapPublisherStatus) error {
			unpublished = append(unpublished, append([]string(nil), journal.ClaimedTargets...))
			journal.ClaimedTargets[0] = "backend-mutated"
			if len(unpublished) == 1 {
				live := getPublisherTrafficMap(t, kubeClient, trafficMap)
				live.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
					PublisherName: "example-publisher", ClaimedTargets: []string{"a", "b"},
				}
				require.NoError(t, kubeClient.Status().Update(context.Background(), live))
			}
			return nil
		},
	}
	reconciler, clientForTest := newTrafficMapPublisherReconciler(t, publisher, trafficMap)
	kubeClient = clientForTest

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"a", "b"}, current.Status.Publisher.ClaimedTargets)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"a"}, {"a", "b"}}, unpublished)
	got := &v1beta1.TrafficMap{}
	err = kubeClient.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), got)
	assert.True(t, apierrors.IsNotFound(err))
}

func TestClearPublisherJournal_PublishedMap_ClearsEveryPublisherOwnedField(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	desiredStatus := v1beta1.TrafficMapStatus{
		Published:                    true,
		ObservedTrafficMapGeneration: trafficMap.Generation,
		GatewayRef: &v1beta1.TrafficMapGatewayRef{
			Group: "networking.example.com", Kind: "GlobalRoute", Name: "model",
		},
		Publisher: &v1beta1.TrafficMapPublisherStatus{
			PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
		},
		Conditions: []metav1.Condition{{
			Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue, Reason: "Published",
		}},
	}
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, trafficMap)
	live := getPublisherTrafficMap(t, kubeClient, trafficMap)
	live.Status = desiredStatus
	require.NoError(t, reconciler.applyPublisherStatus(context.Background(), live))
	live = getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, live.Status.Publisher)

	require.NoError(t, reconciler.clearPublisherJournal(
		context.Background(), client.ObjectKeyFromObject(trafficMap), trafficMap.UID,
		*live.Status.Publisher.DeepCopy(),
	))
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Published)
	assert.Zero(t, current.Status.ObservedTrafficMapGeneration)
	assert.Nil(t, current.Status.GatewayRef)
	assert.Nil(t, current.Status.Publisher)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, trafficMapPublisherReasonUnpublished, published.Reason)
}
