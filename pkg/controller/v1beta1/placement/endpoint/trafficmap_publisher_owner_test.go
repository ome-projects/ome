package endpoint

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
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

// The owner steps of the publisher pass: an inactive installation stops it, the
// owner is read and checked, a stateful publisher waits for the endpoint
// finalizer, and the options are resolved.

func TestTrafficMapPublisher_PolicyAndModeVariants_AppliesOnlyForClusterAffinity(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy v1beta1.PlacementPolicy
		mode   v1beta1.PlacementMode
		want   bool
	}{
		{name: "single", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeSingle, want: true},
		{name: "all", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeAll, want: true},
		{name: "split", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeSplit, want: true},
		{name: "capacity", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeSplitByCapacity, want: true},
		{name: "legacy", policy: v1beta1.PlacementPolicy("Legacy"), mode: v1beta1.PlacementModeAll},
		{name: "omitted policy", mode: v1beta1.PlacementModeSingle},
		{name: "local"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			owner.Spec.Placement = nil
			if tt.mode != "" {
				owner.Spec.Placement = &v1beta1.PlacementSpec{Policy: tt.policy, Mode: tt.mode}
			}
			trafficMap.Spec.Mode = tt.mode
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target-a"},
			}
			r, c := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			if _, err := reconcileTrafficMapPublisher(t, r, trafficMap); err != nil {
				t.Fatal(err)
			}
			var wantEvents []string
			if tt.want {
				wantEvents = []string{"apply"}
			}
			if diff := cmp.Diff(wantEvents, publisher.events); diff != "" {
				t.Fatalf("publication effects (-want +got):\n%s", diff)
			}
			current := getPublisherTrafficMap(t, c, trafficMap)
			if diff := cmp.Diff(tt.want, current.Status.Published); diff != "" {
				t.Fatalf("publication status (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(owner.UID, current.Status.SourceUID); diff != "" {
				t.Fatalf("source identity (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTrafficMapPublisher_SourceUIDMissingOrMismatched_ReportsInvalidOwnerNoPlan(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sourceUID types.UID
	}{
		{name: "missing"},
		{name: "mismatched", sourceUID: "other-owner-uid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.SourceUID = tc.sourceUID
			planCalls := 0
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target-a"},
				plan: func(
					*v1beta1.InferenceService,
					*v1beta1.TrafficMap,
					map[string]string,
				) (TrafficMapPublishPlan, error) {
					planCalls++
					return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

			require.NoError(t, err)
			assert.Zero(t, planCalls)
			assert.Empty(t, publisher.events, "source provenance must gate every external mutation")
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, tc.sourceUID, current.Status.SourceUID,
				"publisher status ownership must not create or overwrite routing provenance")
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
		})
	}
}

func TestTrafficMapPublisher_CacheShowsSourceUIDLiveDoesNot_ReportsInvalidOwner(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.SourceUID = ""
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	stale := trafficMap.DeepCopy()
	stale.Status.SourceUID = owner.UID
	cacheClient := fakeclient.Wrap(t, baseClient, fakeclient.StaleRead(stale))
	planCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		plan: func(
			*v1beta1.InferenceService,
			*v1beta1.TrafficMap,
			map[string]string,
		) (TrafficMapPublishPlan, error) {
			planCalls++
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: cacheClient, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

	require.NoError(t, err)
	assert.Zero(t, planCalls)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.Empty(t, current.Status.SourceUID)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
}

func TestTrafficMapPublisher_FirstOwnerReadFails_ReturnsErrorWithoutPlanning(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	reader := fakeclient.Wrap(t, baseClient,
		fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, apierrors.NewInternalError(errors.New("owner read failed"))).
			OnKind(&v1beta1.InferenceService{}))
	planCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		plan: func(
			*v1beta1.InferenceService,
			*v1beta1.TrafficMap,
			map[string]string,
		) (TrafficMapPublishPlan, error) {
			planCalls++
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: baseClient, APIReader: reader, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

	require.ErrorContains(t, err, "owner read failed")
	assert.Zero(t, planCalls)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
}

func TestTrafficMapPublisher_LegacyEndpointFinalizerPresent_WaitsThenPublishes(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	planCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		plan: func(
			*v1beta1.InferenceService,
			*v1beta1.TrafficMap,
			map[string]string,
		) (TrafficMapPublishPlan, error) {
			planCalls++
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(
		t, publisher, owner, trafficMap, legacyOwner,
	)
	reconciler.RequeueAfter = time.Minute

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, result.RequeueAfter)
	assert.Zero(t, planCalls)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.NotContains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonLegacyActive, condition.Reason)

	require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKeyFromObject(legacyOwner), legacyOwner))
	legacyOwner.Finalizers = nil
	require.NoError(t, kubeClient.Update(context.Background(), legacyOwner))
	result, err = reconcileTrafficMapPublisher(t, reconciler, current)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.Equal(t, 1, planCalls)
	assert.Empty(t, publisher.events, "publication waits for the TrafficMap finalizer write")
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
}

func TestTrafficMapPublisher_AnotherPublishersRecordAndLegacyActive_StillWaits(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{PublisherName: "other-publisher"}
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(
		t, publisher, owner, trafficMap, legacyOwner,
	)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonLegacyActive, condition.Reason)
}

func TestTrafficMapPublisher_OwnRecordAlreadyEstablished_AppliesDespiteLegacy(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
	}
	legacyOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "legacy", Namespace: "team-b", UID: "legacy-uid",
		Finalizers: []string{EndpointFinalizer},
	}}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler, _ := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap, legacyOwner)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"apply"}, publisher.events)
}

func TestTrafficMapPublisher_LegacyISVCListFails_ReturnsErrorNoEffects(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	reader := fakeclient.Wrap(t, baseClient,
		fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall, errors.New("injected InferenceService list failure")).
			OnKind(&v1beta1.InferenceServiceList{}))
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: baseClient, APIReader: reader, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "injected InferenceService list failure")
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonLegacyActive, condition.Reason)
}

func TestTrafficMapPublisher_OptionsResolverRejects_ReportsInvalidOptionsNoEffects(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		resolve: func(map[string]string, map[string]string) (map[string]string, error) {
			return nil, errors.New("protected option is not allowed")
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.NotContains(t, current.Finalizers, TrafficMapPublisherFinalizer)
	assert.Nil(t, current.Status.Publisher)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, trafficMapPublisherReasonInvalidOptions, condition.Reason)
	assert.NotContains(t, condition.Message, "protected option")
}

func TestTrafficMapPublisher_InactiveOrOwnerOptsOut_PlansNothingKeepsFinalizer(t *testing.T) {
	falseValue := false
	tests := []struct {
		name            string
		active          bool
		ownerOptedOut   bool
		ownerDeleting   bool
		ownerIneligible bool
	}{
		{name: "installation disabled"},
		{name: "owner opted out", active: true, ownerOptedOut: true},
		{name: "owner deleting", active: true, ownerDeleting: true},
		{name: "owner placement ineligible", active: true, ownerIneligible: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			if tt.ownerOptedOut {
				owner.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &falseValue}
			}
			if tt.ownerDeleting {
				now := metav1.Now()
				owner.DeletionTimestamp = &now
				owner.Finalizers = []string{"example.com/hold"}
			}
			if tt.ownerIneligible {
				owner.Spec.Placement = nil
			}
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
			}
			if tt.ownerOptedOut {
				trafficMap.Status.Publisher.LastPositive = &v1beta1.TrafficMapPublisherLastPositive{
					TrafficMapGeneration:    1,
					ObservedISVCGeneration:  owner.Generation,
					PlanCompatibilityDigest: testPublisherDigest("a"),
					Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target", Weight: 1}},
				}
			}
			planCalls := 0
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target"},
				plan: func(
					*v1beta1.InferenceService,
					*v1beta1.TrafficMap,
					map[string]string,
				) (TrafficMapPublishPlan, error) {
					planCalls++
					return testTrafficMapPublishPlan{claims: []string{"target"}}, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			reconciler.Active = tt.active

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err)
			assert.Zero(t, planCalls)
			assert.Empty(t, publisher.events, "deletion is requested before publisher effects")
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.True(t, current.DeletionTimestamp.IsZero(), "the routing controller owns map deletion")
			assert.Contains(t, current.Finalizers, TrafficMapPublisherFinalizer)
			if tt.ownerOptedOut {
				assert.Nil(t, current.Status.Publisher.LastPositive,
					"the owning publisher clears its snapshot when the source opts out")
			}
		})
	}
}

func TestTrafficMapPublisher_OwnerOptedOutAndAnotherPublishersRecord_LeavesIt(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	falseValue := false
	owner.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &falseValue}
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	foreign := &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "other-publisher",
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "foreign-target", Weight: 1}},
		},
	}
	trafficMap.Status.Publisher = copyTrafficMapPublisherJournal(foreign)
	planCalls := 0
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		plan: func(
			*v1beta1.InferenceService,
			*v1beta1.TrafficMap,
			map[string]string,
		) (TrafficMapPublishPlan, error) {
			planCalls++
			return testTrafficMapPublishPlan{}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Zero(t, planCalls)
	assert.Empty(t, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, foreign, current.Status.Publisher,
		"an inactive newly selected publisher must not clear or adopt another publisher's snapshot")
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, trafficMapPublisherReasonPublisherChanged, published.Reason)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
}
