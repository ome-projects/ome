package endpoint

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The stateful branch of the publisher pass: the finalizer, the claim record
// written before any effect, preflight, drain and apply, and the retries after a
// failed step.

func TestTrafficMapPublisher_FirstPass_AddsFinalizerBeforeAnyEffect(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.Empty(t, publisher.events, "no external effect may precede the persisted finalizer")

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Nil(t, current.Status.Publisher)
}

func TestTrafficMapPublisher_NewClaims_RecordsThemBeforeDrainAndApply(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName:  "example-publisher",
		ClaimedTargets: []string{"retired"},
	}
	trafficMap.Status.Conditions = []metav1.Condition{{
		Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue,
		Reason: "Routable", ObservedGeneration: trafficMap.Generation,
	}}
	owner.Spec.Routing = &v1beta1.RoutingSpec{Publisher: &v1beta1.RoutingPublisherSpec{
		Options: map[string]string{"inline": "value"},
	}}

	global := map[string]string{"global": "value"}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		resolve: func(gotGlobal, gotInline map[string]string) (map[string]string, error) {
			assert.Equal(t, global, gotGlobal)
			assert.Equal(t, owner.Spec.Routing.Publisher.Options, gotInline)
			gotGlobal["mutated"] = "resolver"
			gotInline["mutated"] = "resolver"
			return map[string]string{"effective": "blue"}, nil
		},
		plan: func(_ *v1beta1.InferenceService, _ *v1beta1.TrafficMap, options map[string]string) (TrafficMapPublishPlan, error) {
			assert.Equal(t, map[string]string{"effective": "blue"}, options)
			options["effective"] = "plan-mutated"
			return testTrafficMapPublishPlan{claims: []string{"target-z", "target-a"}}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	reconciler.GlobalOptions = global
	reconciler.RequeueAfter = time.Minute
	assertPreclaimed := func() {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		require.NotNil(t, current.Status.Publisher)
		assert.Equal(t, []string{"retired", "target-a", "target-z"}, current.Status.Publisher.ClaimedTargets)
	}
	publisher.drain = func(_ context.Context, key types.NamespacedName, claims []string) error {
		assertPreclaimed()
		assert.Equal(t, client.ObjectKeyFromObject(trafficMap), key)
		assert.Equal(t, []string{"retired"}, claims)
		return nil
	}
	publisher.apply = func(_ context.Context, plan TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		assertPreclaimed()
		assert.Equal(t, []string{"target-z", "target-a"}, plan.Claims())
		return TrafficMapPublishResult{GatewayRef: &v1beta1.TrafficMapGatewayRef{
			Group: "networking.example.com", Kind: "GlobalRoute", Namespace: "routes", Name: "model",
		}}, nil
	}

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, result.RequeueAfter)
	assert.Equal(t, []string{"drain:retired", "apply"}, publisher.events)
	assert.Equal(t, map[string]string{"global": "value"}, global,
		"the resolver must receive a copy of global options")
	assert.Equal(t, map[string]string{"inline": "value"}, owner.Spec.Routing.Publisher.Options,
		"the resolver must receive a copy of inline options")

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, owner.UID, current.Status.SourceUID)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"retired", "target-a", "target-z"}, current.Status.Publisher.ClaimedTargets,
		"drained targets stay claimed until whole-map finalization")
	wantDigest, err := publisherOptionsDigest("example-publisher", map[string]string{"effective": "blue"})
	require.NoError(t, err)
	assert.Equal(t, wantDigest, current.Status.Publisher.ObservedOptionsDigest)
	assert.True(t, current.Status.Published)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.GatewayRef)
	assert.Equal(t, "GlobalRoute", current.Status.GatewayRef.Kind)
	assert.Equal(t, "model", current.Status.GatewayRef.Name)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionTrue, published.Status)
	assert.Equal(t, trafficMapPublisherReasonPublished, published.Reason)
	assert.Equal(t, "TrafficMap is published by publisher \"example-publisher\"", published.Message)
}

func TestTrafficMapPublisher_PreflightFails_ClaimsRecordedNoDrainOrApply(t *testing.T) {
	tests := []struct {
		name      string
		preflight error
		wantError bool
	}{
		{
			name:      "terminal rejection",
			preflight: &terminalPublisherError{err: errors.New("hostname collision")},
		},
		{
			name:      "retryable read failure",
			preflight: errors.New("live route list failed"),
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"retired"},
			}
			base := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target-a"},
			}
			publisher := &preflightingTrafficMapPublisher{recordingTrafficMapPublisher: base}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			publisher.preflight = func(_ context.Context, plan TrafficMapPublishPlan) error {
				assert.Equal(t, []string{"target-a"}, plan.Claims())
				current := getPublisherTrafficMap(t, kubeClient, trafficMap)
				require.NotNil(t, current.Status.Publisher)
				assert.Equal(t, []string{"retired", "target-a"}, current.Status.Publisher.ClaimedTargets,
					"the durable claim must precede preflight")
				return tt.preflight
			}

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			if tt.wantError {
				require.ErrorContains(t, err, tt.preflight.Error())
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, []string{"preflight"}, publisher.events,
				"a failed preflight must precede and prevent Drain and Apply")

			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, trafficMapPublisherReasonClaimRejected, condition.Reason)
		})
	}
}

func TestTrafficMapPublisher_ApplyReportsRemoval_ClearsPublishedStateKeepsClaims(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
	trafficMap.Status.GatewayRef = &v1beta1.TrafficMapGatewayRef{
		Group: "networking.example.com", Kind: "GlobalRoute", Namespace: "routes", Name: "old",
	}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration: 2, ObservedISVCGeneration: 3,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target", Weight: 37}},
		},
	}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			return TrafficMapPublishResult{
				GatewayRef: &v1beta1.TrafficMapGatewayRef{
					Group: "networking.example.com", Kind: "GlobalRoute", Namespace: "routes", Name: "ignored",
				},
				Withdrawn: true,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Published)
	assert.Nil(t, current.Status.GatewayRef)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets,
		"successful withdrawal retains the durable claim until finalization")
	assert.Nil(t, current.Status.Publisher.LastPositive)
	assert.NotEmpty(t, current.Status.Publisher.ObservedOptionsDigest)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, trafficMapPublisherReasonWithdrawn, published.Reason)
	assert.Equal(t, trafficMap.Generation, published.ObservedGeneration)
}

func TestTrafficMapPublisher_InvalidOrConflictingClaims_ReportsAndDoesNothing(t *testing.T) {
	tests := []struct {
		name       string
		claims     []string
		journal    *v1beta1.TrafficMapPublisherStatus
		other      *v1beta1.TrafficMap
		wantReason string
	}{
		{
			name: "duplicate plan claims", claims: []string{"same", "same"},
			wantReason: trafficMapPublisherReasonInvalidPlan,
		},
		{
			name: "oversized target", claims: []string{strings.Repeat("x", v1beta1.MaxTrafficMapPublisherTargetLength+1)},
			wantReason: trafficMapPublisherReasonInvalidPlan,
		},
		{
			name: "publisher mismatch", claims: []string{"new"},
			journal:    &v1beta1.TrafficMapPublisherStatus{PublisherName: "old-publisher", ClaimedTargets: []string{"old"}},
			wantReason: trafficMapPublisherReasonPublisherChanged,
		},
		{
			name: "publisher mismatch with snapshot but no claims", claims: []string{"new"},
			journal: &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "old-publisher",
				LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
					TrafficMapGeneration: 1, ObservedISVCGeneration: 1,
					PlanCompatibilityDigest: testPublisherDigest("a"),
					Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "old", Weight: 1}},
				},
			},
			wantReason: trafficMapPublisherReasonPublisherChanged,
		},
		{
			name: "claim collision", claims: []string{"shared"},
			other:      publisherClaimedTrafficMap("other", "example-publisher", []string{"shared"}),
			wantReason: trafficMapPublisherReasonClaimRejected,
		},
		{
			name:       "global publisher switch with no desired claim",
			other:      publisherClaimedTrafficMap("other", "different-publisher", []string{"theirs"}),
			wantReason: trafficMapPublisherReasonClaimRejected,
		},
		{
			name: "claim union over bound", claims: []string{"new"},
			journal: &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: numberedClaims(v1beta1.MaxTrafficMapPublisherTargets),
			},
			wantReason: trafficMapPublisherReasonClaimRejected,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.Publisher = tt.journal
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: tt.claims,
			}
			objects := []client.Object{owner, trafficMap}
			if tt.other != nil {
				objects = append(objects, tt.other)
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, objects...)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err, "invalid desired state is reported in status without a retry hot-loop")
			assert.Empty(t, publisher.events, "rejected plans must have no external effect")

			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			if tt.journal == nil {
				assert.Nil(t, current.Status.Publisher)
			} else {
				assert.Equal(t, tt.journal, current.Status.Publisher)
			}
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, tt.wantReason, condition.Reason)
			assert.Equal(t, trafficMapPublisherFailureMessage(tt.wantReason), condition.Message)
		})
	}
}

func TestTrafficMapPublisher_SecondMapClaimsATakenTarget_RejectedFromLiveState(t *testing.T) {
	ownerA, trafficMapA := publisherTestObjects()
	trafficMapA.Finalizers = []string{TrafficMapPublisherFinalizer}

	ownerB := ownerA.DeepCopy()
	ownerB.Name = "model-b"
	ownerB.UID = types.UID("owner-b-uid")
	trafficMapB := trafficMapA.DeepCopy()
	trafficMapB.Name = ownerB.Name
	trafficMapB.UID = types.UID("map-b-uid")
	trafficMapB.Spec.Service = ownerB.Name
	trafficMapB.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
		Name: ownerB.Name, UID: ownerB.UID, Controller: trafficMapA.OwnerReferences[0].Controller,
	}}
	trafficMapB.Status.SourceUID = ownerB.UID

	liveClient := newTrafficMapPublisherClient(t, ownerA, trafficMapA, ownerB, trafficMapB)
	// The cache never observes the publisher's status writes: both maps read
	// as seeded, whatever the live store holds.
	trafficMapGets := fakeclient.CountingVerb(fakeclient.Get).OnKind(&v1beta1.TrafficMap{})
	cacheClient := fakeclient.Wrap(t, liveClient, trafficMapGets,
		fakeclient.StaleRead(trafficMapA.DeepCopy(), trafficMapB.DeepCopy()))
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"shared-target"},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: cacheClient, APIReader: liveClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMapA)
	require.NoError(t, err)
	require.Equal(t, []string{"apply"}, publisher.events)
	claimed := getPublisherTrafficMap(t, liveClient, trafficMapA)
	require.NotNil(t, claimed.Status.Publisher)
	assert.Equal(t, []string{"shared-target"}, claimed.Status.Publisher.ClaimedTargets)

	publisher.events = nil
	_, err = reconcileTrafficMapPublisher(t, reconciler, trafficMapB)
	require.NoError(t, err, "the conflicting claim is a reported terminal rejection")
	assert.Empty(t, publisher.events, "the second map must not mutate the shared target")

	rejected := getPublisherTrafficMap(t, liveClient, trafficMapB)
	assert.Nil(t, rejected.Status.Publisher, "a rejected map must not acquire the live claim")
	published := apimeta.FindStatusCondition(rejected.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, trafficMapPublisherReasonClaimRejected, published.Reason)
	assert.Equal(t, 2, trafficMapGets.Count(),
		"both reconcile inputs came from the frozen informer view")
}

func TestTrafficMapPublisher_ClaimsRecordedButFinalizerMissing_RestoresItFirst(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	owner.Generation++
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
	}
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Empty(t, publisher.events)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets)
}

func TestTrafficMapPublisher_RecordWithNoClaimsAndNoFinalizer_RestoresFinalizer(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	owner.Generation++
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher",
	}
	trafficMap.Status.Published = true
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Empty(t, publisher.events)
}

func TestTrafficMapPublisher_ApplyFailsOnce_KeepsClaimsAndSucceedsOnRetry(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	attempts := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			attempts++
			if attempts == 1 {
				return TrafficMapPublishResult{}, errors.New("temporary backend failure")
			}
			return TrafficMapPublishResult{}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.Error(t, err)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets)
	assert.Empty(t, current.Status.Publisher.ObservedOptionsDigest)
	assert.False(t, current.Status.Published)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonApplyFailed, condition.Reason)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Published)
	assert.NotEmpty(t, current.Status.Publisher.ObservedOptionsDigest)
	assert.Equal(t, 2, attempts)
}

func TestTrafficMapPublisher_DrainFailsOnce_KeepsClaimsAndSucceedsOnRetry(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"retired"},
	}
	drainAttempts := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"current"},
		drain: func(_ context.Context, key types.NamespacedName, _ []string) error {
			assert.Equal(t, client.ObjectKeyFromObject(trafficMap), key)
			drainAttempts++
			if drainAttempts == 1 {
				return errors.New("temporary drain failure")
			}
			return nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.Error(t, err)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"current", "retired"}, current.Status.Publisher.ClaimedTargets)
	assert.Equal(t, []string{"drain:retired"}, publisher.events)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonDrainFailed, condition.Reason)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:retired", "drain:retired", "apply"}, publisher.events)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Published)
}

func TestTrafficMapPublisher_SuccessStatusWriteFails_ReappliesOnRetry(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	wrapped := fakeclient.Wrap(t, baseClient,
		fakeclient.FailingVerb(fakeclient.StatusApply, 2, errors.New("injected status write failure")))
	applyCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			applyCalls++
			return TrafficMapPublishResult{}, nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: wrapped, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "injected status write failure")
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{"target"}, current.Status.Publisher.ClaimedTargets)
	assert.False(t, current.Status.Published)

	_, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.Equal(t, 2, applyCalls, "retry must idempotently reapply after a lost success-status write")
	current = getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.True(t, current.Status.Published)
}
