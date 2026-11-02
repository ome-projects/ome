package endpoint

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The retained plan of a stateful publisher: a positive plan is captured after
// apply, replayed when the placement is lost, kept while eligibility is unknown,
// and cleared when the current plan applies.

func TestTrafficMapPublisher_PositivePlanApplied_RecordsEveryClaimAsLastPositive(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName:  "example-publisher",
		ClaimedTargets: []string{"retired"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("b"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "retired", Weight: 100}},
		},
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-b", "target-a"},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets: []v1beta1.TrafficMapPublisherTarget{
					{Target: "target-b", Weight: 40},
					{Target: "target-a", Weight: 60},
				},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.False(t, current.Status.Published, "transition must be durable before Apply")
		fallback := apimeta.FindStatusCondition(
			current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
		)
		require.NotNil(t, fallback)
		assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
		assert.Equal(t, v1beta1.TrafficMapReasonPublicationTransitioning, fallback.Reason)
		require.NotNil(t, current.Status.Publisher)
		assert.Nil(t, current.Status.Publisher.LastPositive,
			"claim-set changes must clear the previous snapshot before external mutation")
		assert.Equal(t, []string{"retired", "target-a", "target-b"}, current.Status.Publisher.ClaimedTargets)
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:retired", "apply"}, publisher.events)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	require.NotNil(t, current.Status.Publisher.LastPositive)
	assert.Equal(t, trafficMap.Generation, current.Status.Publisher.LastPositive.TrafficMapGeneration)
	assert.Equal(t, owner.Generation, current.Status.Publisher.LastPositive.ObservedISVCGeneration)
	assert.Equal(t, testPublisherDigest("a"), current.Status.Publisher.LastPositive.PlanCompatibilityDigest)
	assert.Equal(t, []v1beta1.TrafficMapPublisherTarget{
		{Target: "retired", Weight: 0},
		{Target: "target-a", Weight: 60},
		{Target: "target-b", Weight: 40},
	}, current.Status.Publisher.LastPositive.Targets,
		"every retained claim must be represented, including retired targets at zero")
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonCurrentPlan, fallback.Reason)
}

func TestTrafficMapPublisher_PlacementLostWithLastPositive_ReplaysIt(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a", "target-b"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "target-a", Weight: 75},
			{Target: "target-b", Weight: 25},
		},
	}
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		ObservedOptionsDigest: testPublisherDigest("f"),
		LastPositive:          copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(got v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			assert.Equal(t, *lastPositive, got)
			return testTrafficMapPublishPlan{claims: claims}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(_ context.Context, plan TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		assert.Equal(t, claims, plan.Claims())
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.False(t, current.Status.Published)
		assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive,
			"fallback transition must retain the complete retry journal")
		fallback := apimeta.FindStatusCondition(
			current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
		)
		require.NotNil(t, fallback)
		assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"replay", "apply"}, publisher.events)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Published)
	assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.NotEqual(t, testPublisherDigest("f"), current.Status.Publisher.ObservedOptionsDigest,
		"the full current options digest advances after a successful replay")
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionFalse, published.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonPublicationFallback, published.Reason)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionTrue, fallback.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonLastPositiveRetained, fallback.Reason)
	assert.Equal(t, trafficMap.Generation, fallback.ObservedGeneration)
}

func TestTrafficMapPublisher_AllHomesUnreadyAndPublisherOptedIn_ReplaysLastPositive(t *testing.T) {
	owner, trafficMap := allHomesUnreadyPublisherObjects()
	currentClaims := []string{"target-a", "target-b", "target-c"}
	durableClaims := []string{"target-a", "target-b", "target-c", "target-retired"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation - 1,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "target-a", Weight: 75},
			{Target: "target-b", Weight: 25},
			{Target: "target-c", Weight: 0},
			{Target: "target-retired", Weight: 0},
		},
	}
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: durableClaims,
		ObservedOptionsDigest: testPublisherDigest("f"),
		LastPositive:          copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: currentClaims,
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets: []v1beta1.TrafficMapPublisherTarget{
					{Target: "target-a", Weight: 0},
					{Target: "target-b", Weight: 0},
					{Target: "target-c", Weight: 0},
				},
				PlanCompatibilityDigest:      testPublisherDigest("a"),
				NoPositivePlanPolicy:         TrafficMapNoPositivePlanPolicyRetainLastPositive,
				AllowAllHomesUnreadyFallback: true,
			}, nil
		},
		replay: func(got v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			assert.Equal(t, *lastPositive, got)
			return testTrafficMapPublishPlan{claims: durableClaims}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"replay", "apply"}, publisher.events)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, durableClaims, current.Status.Publisher.ClaimedTargets)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionTrue, fallback.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonLastPositiveRetained, fallback.Reason)
}

func TestTrafficMapPublisher_AllHomesUnreadyWithoutOptIn_AppliesZeroesNoReplay(t *testing.T) {
	owner, trafficMap := allHomesUnreadyPublisherObjects()
	durableClaims := []string{"target-a", "target-b", "target-c", "target-retired"}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: durableClaims,
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets: []v1beta1.TrafficMapPublisherTarget{
				{Target: "target-a", Weight: 75},
				{Target: "target-b", Weight: 25},
				{Target: "target-c", Weight: 0},
				{Target: "target-retired", Weight: 0},
			},
		},
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		claims: []string{"target-a", "target-b", "target-c"},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets: []v1beta1.TrafficMapPublisherTarget{
					{Target: "target-a", Weight: 0},
					{Target: "target-b", Weight: 0},
					{Target: "target-c", Weight: 0},
				},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			t.Fatal("generic fallback must not replay AllHomesUnready")
			return nil, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:target-retired", "apply"}, publisher.events)
	assert.Nil(t, getPublisherTrafficMap(t, kubeClient, trafficMap).Status.Publisher.LastPositive)
}

func TestTrafficMapPublisher_LastPositiveDigestMismatch_ClearsItBeforeApply(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a"}
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("b"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
		},
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			t.Fatal("invalid snapshot must not be replayed")
			return nil, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		require.NotNil(t, current.Status.Publisher)
		assert.Nil(t, current.Status.Publisher.LastPositive,
			"incompatible snapshot must be durably invalidated before Apply")
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:target-a", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Nil(t, current.Status.Publisher.LastPositive)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonApplyCurrent, fallback.Reason)
}

func TestTrafficMapPublisher_ClaimsShrinkToASubset_ClearsLastPositiveNoReplay(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a", "target-b"}
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets: []v1beta1.TrafficMapPublisherTarget{
				{Target: "target-a", Weight: 50},
				{Target: "target-b", Weight: 50},
			},
		},
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 0}},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			t.Fatal("a nonempty strict claim subset must not replay the old plan")
			return nil, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.Nil(t, current.Status.Publisher.LastPositive)
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:target-b", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Nil(t, current.Status.Publisher.LastPositive)
}

func TestTrafficMapPublisher_ApplyCurrentPolicyEmptyOrZeroPlan_ClearsLastPositive(t *testing.T) {
	tests := []struct {
		name       string
		claims     []string
		targets    []v1beta1.TrafficMapPublisherTarget
		wantEvents []string
	}{
		{
			name:       "empty",
			wantEvents: []string{"drain:target-a", "apply"},
		},
		{
			name:   "all zero",
			claims: []string{"target-a", "target-b"},
			targets: []v1beta1.TrafficMapPublisherTarget{
				{Target: "target-a", Weight: 0},
				{Target: "target-b", Weight: 0},
			},
			wantEvents: []string{"apply"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			durableClaims := []string{"target-a"}
			if len(tt.claims) != 0 {
				durableClaims = slices.Clone(tt.claims)
			}
			trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation - 1
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: durableClaims,
				LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
					TrafficMapGeneration:    trafficMap.Generation - 1,
					ObservedISVCGeneration:  owner.Generation,
					PlanCompatibilityDigest: testPublisherDigest("a"),
					Targets: []v1beta1.TrafficMapPublisherTarget{
						{Target: "target-a", Weight: 100},
					},
				},
			}
			if len(durableClaims) == 2 {
				trafficMap.Status.Publisher.LastPositive.Targets = append(
					trafficMap.Status.Publisher.LastPositive.Targets,
					v1beta1.TrafficMapPublisherTarget{Target: "target-b", Weight: 0},
				)
			}
			base := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: slices.Clone(tt.claims),
			}
			publisher := &fallbackTrafficMapPublisher{
				recordingTrafficMapPublisher: base,
				capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
					return TrafficMapPublishPlanCapture{
						Targets:                 slices.Clone(tt.targets),
						PlanCompatibilityDigest: testPublisherDigest("a"),
						NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyApplyCurrent,
					}, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			publisher.apply = func(_ context.Context, plan TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
				assert.Equal(t, tt.claims, plan.Claims())
				current := getPublisherTrafficMap(t, kubeClient, trafficMap)
				assert.Nil(t, current.Status.Publisher.LastPositive,
					"ApplyCurrent must durably clear retained state before applying no-positive plans")
				return TrafficMapPublishResult{}, nil
			}

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEvents, publisher.events)
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.True(t, current.Status.Published)
			assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
			assert.Nil(t, current.Status.Publisher.LastPositive)
			fallback := apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			)
			require.NotNil(t, fallback)
			assert.Equal(t, metav1.ConditionFalse, fallback.Status)
			assert.Equal(t, trafficMapPublisherReasonApplyCurrent, fallback.Reason)
		})
	}
}

func TestTrafficMapPublisher_ReportedZeroWhileStatusLags_AppliesCurrentTable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(*v1beta1.InferenceService, *v1beta1.TrafficMap)
		wantApply bool
	}{
		{name: "replica status lags", wantApply: true, mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
			owner.Status.ObservedGeneration--
		}},
		{name: "routing condition lags", wantApply: true, mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
			apimeta.FindStatusCondition(trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable).ObservedGeneration--
		}},
		{name: "ceiling mode still waits", mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
			trafficMap.Spec.CapacityMode = v1beta1.RoutingCapacityModeCeiling
			owner.Status.ObservedGeneration--
		}},
		{name: "a table without a report waits like ceiling mode", mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
			for i := range trafficMap.Spec.Entries {
				trafficMap.Spec.Entries[i].Capacity.ReportedWeight = nil
			}
			owner.Status.ObservedGeneration--
		}},
		{name: "stale source generation still blocks", mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
			owner.Generation++
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, trafficMap := allHomesUnreadyPublisherObjects()
			trafficMap.Spec.CapacityMode = v1beta1.RoutingCapacityModeReported
			withZeroReports(trafficMap)
			apimeta.FindStatusCondition(trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable).Reason = v1beta1.TrafficMapReasonNoRoutableCapacity
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
				LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
					TrafficMapGeneration: trafficMap.Generation - 1, ObservedISVCGeneration: owner.Generation,
					PlanCompatibilityDigest: testPublisherDigest("a"),
					Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
				},
			}
			tc.mutate(owner, trafficMap)
			publisher := &fallbackTrafficMapPublisher{
				recordingTrafficMapPublisher: &recordingTrafficMapPublisher{
					name: "example-publisher", stateful: true, claims: []string{"target-a"},
				},
				capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
					return TrafficMapPublishPlanCapture{
						Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 0}},
						PlanCompatibilityDigest: testPublisherDigest("a"),
						NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
					}, nil
				},
				replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
					t.Fatal("reported zero cannot replay an earlier positive weight")
					return nil, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err)
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			if tc.wantApply {
				assert.Equal(t, []string{"apply"}, publisher.events)
				assert.Nil(t, current.Status.Publisher.LastPositive)
			} else {
				assert.Empty(t, publisher.events)
				assert.NotNil(t, current.Status.Publisher.LastPositive)
			}
		})
	}
}

func TestTrafficMapPublisher_ReportedModeWithoutReportsAndLoss_ReplaysLastPositive(t *testing.T) {
	// A Reported table without a report has fallen back to replica counts, so a
	// lost placement keeps the last positive plan exactly as in Ceiling mode.
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	trafficMap.Spec.CapacityMode = v1beta1.RoutingCapacityModeReported
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: &recordingTrafficMapPublisher{name: "example-publisher", stateful: true},
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			assert.Equal(t, *lastPositive, snapshot)
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"replay", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
}

func TestTrafficMapPublisher_NoAddressableHome_ReplaysLastPositive(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	owner.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "cluster-a", ReadyReplicas: 2,
		}},
	}
	apimeta.FindStatusCondition(
		trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
	).Reason = v1beta1.TrafficMapReasonNoAddressableHome
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			assert.Equal(t, *lastPositive, snapshot)
			return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"replay", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.False(t, current.Status.Published)
	assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
	assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
}

func TestTrafficMapPublisher_PlacementUnknown_KeepsLastPositiveAndReplaysOnceKnown(t *testing.T) {
	tests := []struct {
		name           string
		routableReason string
		makeUnknown    func(*v1beta1.InferenceService)
		makeEligible   func(*v1beta1.InferenceService)
	}{
		{
			name:           "single-cluster loss",
			routableReason: v1beta1.TrafficMapReasonNotPlaced,
			makeUnknown: func(owner *v1beta1.InferenceService) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
			},
			makeEligible: func(owner *v1beta1.InferenceService) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonLost
			},
		},
		{
			name:           "placed cluster temporarily unobservable",
			routableReason: v1beta1.TrafficMapReasonNoAddressableHome,
			makeUnknown: func(owner *v1beta1.InferenceService) {
				owner.Status.Placement = &v1beta1.PlacementStatus{
					Phase: v1beta1.PlacementPhasePlaced,
					Candidates: []v1beta1.CandidatePlacement{{
						Cluster: "cluster-a", ReadyReplicas: 0,
					}},
				}
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
			},
			makeEligible: func(owner *v1beta1.InferenceService) {
				owner.Status.Placement.Candidates[0].ReadyReplicas = 2
				owner.Status.Conditions[0].Status = corev1.ConditionFalse
				owner.Status.Conditions[0].Reason = "PlacementNotReady"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := eligiblePlacementLossPublisherObjects()
			tt.makeUnknown(owner)
			apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			).Reason = tt.routableReason
			lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
				TrafficMapGeneration:    trafficMap.Generation - 1,
				ObservedISVCGeneration:  owner.Generation,
				PlanCompatibilityDigest: testPublisherDigest("a"),
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
			}
			trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
				LastPositive: copyTrafficMapLastPositive(lastPositive),
			}
			apimeta.SetStatusCondition(&trafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapPublicationFallback, Status: metav1.ConditionTrue,
				Reason:             v1beta1.TrafficMapReasonLastPositiveRetained,
				ObservedGeneration: trafficMap.Generation - 1,
			})
			kubeClient := newTrafficMapPublisherClient(t, owner, trafficMap)
			var replayed []v1beta1.TrafficMapPublisherLastPositive
			newPublisher := func() *fallbackTrafficMapPublisher {
				base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
				return &fallbackTrafficMapPublisher{
					recordingTrafficMapPublisher: base,
					capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
						return TrafficMapPublishPlanCapture{
							PlanCompatibilityDigest: testPublisherDigest("a"),
							NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
						}, nil
					},
					replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
						replayed = append(replayed, *copyTrafficMapLastPositive(&snapshot))
						return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
					},
				}
			}
			firstPublisher := newPublisher()
			first := &TrafficMapPublisherReconciler{
				Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(),
				Publisher: firstPublisher, Active: true,
			}

			_, err := reconcileTrafficMapPublisher(t, first, trafficMap)
			require.NoError(t, err)
			assert.Empty(t, firstPublisher.events,
				"PlacementUnknown must neither replay nor clear the retained data plane")
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, []string{"target-a"}, current.Status.Publisher.ClaimedTargets)
			assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
			assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
			fallback := apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			)
			require.NotNil(t, fallback)
			assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
			assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, fallback.Reason)
			published := apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublished,
			)
			require.NotNil(t, published)
			assert.False(t, current.Status.Published)
			assert.Equal(t, metav1.ConditionFalse, published.Status)
			assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, published.Reason)

			liveOwner := &v1beta1.InferenceService{}
			require.NoError(t, kubeClient.Get(
				context.Background(), client.ObjectKeyFromObject(owner), liveOwner,
			))
			tt.makeEligible(liveOwner)
			require.NoError(t, kubeClient.Update(context.Background(), liveOwner))
			liveTrafficMap := getPublisherTrafficMap(t, kubeClient, trafficMap)
			apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionFalse,
				Reason: tt.routableReason, ObservedGeneration: liveTrafficMap.Generation,
			})
			apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
				Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: liveTrafficMap.Generation,
			})
			require.NoError(t, kubeClient.Status().Update(context.Background(), liveTrafficMap))
			current = getPublisherTrafficMap(t, kubeClient, trafficMap)

			restartedPublisher := newPublisher()
			restarted := &TrafficMapPublisherReconciler{
				Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(),
				Publisher: restartedPublisher, Active: true,
			}
			_, err = reconcileTrafficMapPublisher(t, restarted, current)
			require.NoError(t, err)
			assert.Equal(t, []string{"replay", "apply"}, restartedPublisher.events)
			require.Len(t, replayed, 1)
			assert.Equal(t, *lastPositive, replayed[0])
			current = getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
			assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			).Status)
		})
	}
}

func TestTrafficMapPublisher_RoutableReasonLagsTheSource_WaitsThenReplays(t *testing.T) {
	tests := []struct {
		name            string
		laggingReason   string
		caughtUpReason  string
		configureSource func(*v1beta1.InferenceService)
		catchUpSource   func(*v1beta1.InferenceService)
	}{
		{
			name:           "NotPlaced lags NoAddressableHome source",
			laggingReason:  v1beta1.TrafficMapReasonNotPlaced,
			caughtUpReason: v1beta1.TrafficMapReasonNoAddressableHome,
			configureSource: func(owner *v1beta1.InferenceService) {
				owner.Status.Placement = &v1beta1.PlacementStatus{
					Phase: v1beta1.PlacementPhasePlaced,
					Candidates: []v1beta1.CandidatePlacement{{
						Cluster: "cluster-a", ReadyReplicas: 2,
					}},
				}
				owner.Status.Conditions[0].Status = corev1.ConditionFalse
				owner.Status.Conditions[0].Reason = "PlacementNotReady"
			},
		},
		{
			name:           "NoAddressableHome lags PlacementLost source",
			laggingReason:  v1beta1.TrafficMapReasonNoAddressableHome,
			caughtUpReason: v1beta1.TrafficMapReasonNotPlaced,
			configureSource: func(*v1beta1.InferenceService) {
				// eligiblePlacementLossPublisherObjects already has the exact
				// Pending/last-winner/PlacementLost source shape.
			},
		},
		{
			name:           "NoAddressableHome lags pending PlacementUnknown source",
			laggingReason:  v1beta1.TrafficMapReasonNoAddressableHome,
			caughtUpReason: v1beta1.TrafficMapReasonNotPlaced,
			configureSource: func(owner *v1beta1.InferenceService) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonUnknown
			},
			catchUpSource: func(owner *v1beta1.InferenceService) {
				owner.Status.Conditions[0].Reason = placementcontroller.PlacementReadyReasonLost
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := eligiblePlacementLossPublisherObjects()
			tt.configureSource(owner)
			apimeta.FindStatusCondition(
				trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
			).Reason = tt.laggingReason
			lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
				TrafficMapGeneration:    trafficMap.Generation - 1,
				ObservedISVCGeneration:  owner.Generation,
				PlanCompatibilityDigest: testPublisherDigest("a"),
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
			}
			trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
			trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
				LastPositive: copyTrafficMapLastPositive(lastPositive),
			}
			kubeClient := newTrafficMapPublisherClient(t, owner, trafficMap)
			var replayed []v1beta1.TrafficMapPublisherLastPositive
			newPublisher := func() *fallbackTrafficMapPublisher {
				base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
				return &fallbackTrafficMapPublisher{
					recordingTrafficMapPublisher: base,
					capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
						return TrafficMapPublishPlanCapture{
							PlanCompatibilityDigest: testPublisherDigest("a"),
							NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
						}, nil
					},
					replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
						replayed = append(replayed, *copyTrafficMapLastPositive(&snapshot))
						return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
					},
				}
			}
			firstPublisher := newPublisher()
			first := &TrafficMapPublisherReconciler{
				Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(),
				Publisher: firstPublisher, Active: true,
			}

			_, err := reconcileTrafficMapPublisher(t, first, trafficMap)
			require.NoError(t, err)
			assert.Empty(t, firstPublisher.events)
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, []string{"target-a"}, current.Status.Publisher.ClaimedTargets)
			assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
			assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
			fallback := apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			)
			require.NotNil(t, fallback)
			assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
			assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, fallback.Reason)

			if tt.catchUpSource != nil {
				liveOwner := &v1beta1.InferenceService{}
				require.NoError(t, kubeClient.Get(
					context.Background(), client.ObjectKeyFromObject(owner), liveOwner,
				))
				tt.catchUpSource(liveOwner)
				require.NoError(t, kubeClient.Update(context.Background(), liveOwner))
			}
			liveTrafficMap := getPublisherTrafficMap(t, kubeClient, trafficMap)
			apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionFalse,
				Reason: tt.caughtUpReason, ObservedGeneration: liveTrafficMap.Generation,
			})
			apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
				Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
				Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: liveTrafficMap.Generation,
			})
			require.NoError(t, kubeClient.Status().Update(context.Background(), liveTrafficMap))
			current = getPublisherTrafficMap(t, kubeClient, trafficMap)

			restartedPublisher := newPublisher()
			restarted := &TrafficMapPublisherReconciler{
				Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(),
				Publisher: restartedPublisher, Active: true,
			}
			_, err = reconcileTrafficMapPublisher(t, restarted, current)
			require.NoError(t, err)
			assert.Equal(t, []string{"replay", "apply"}, restartedPublisher.events)
			require.Len(t, replayed, 1)
			assert.Equal(t, *lastPositive, replayed[0])
			current = getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
			assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
				current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
			).Status)
		})
	}
}

func TestTrafficMapPublisher_NotPlacedLagsAReadySource_WaitsThenAppliesPositivePlan(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	endpoint, err := apis.ParseURL("https://model.example.com")
	require.NoError(t, err)
	owner.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "cluster-a", Phase: v1beta1.CandidatePhaseAdmitted,
			ReadyReplicas: 1, Endpoint: endpoint.DeepCopy(),
		}},
	}
	owner.Status.Conditions[0].Status = corev1.ConditionTrue
	owner.Status.Conditions[0].Reason = "PlacementReady"
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	kubeClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	positive := false
	newPublisher := func() *fallbackTrafficMapPublisher {
		base := &recordingTrafficMapPublisher{
			name: "example-publisher", stateful: true,
			plan: func(
				*v1beta1.InferenceService,
				*v1beta1.TrafficMap,
				map[string]string,
			) (TrafficMapPublishPlan, error) {
				if positive {
					return testTrafficMapPublishPlan{claims: []string{"target-a"}}, nil
				}
				return testTrafficMapPublishPlan{}, nil
			},
		}
		return &fallbackTrafficMapPublisher{
			recordingTrafficMapPublisher: base,
			capture: func(plan TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
				capture := TrafficMapPublishPlanCapture{
					PlanCompatibilityDigest: testPublisherDigest("a"),
					NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
				}
				if len(plan.Claims()) != 0 {
					capture.Targets = []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 65}}
				}
				return capture, nil
			},
			replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
				t.Fatal("positive routing recovery must apply the current plan, not Replay")
				return nil, nil
			},
		}
	}
	firstPublisher := newPublisher()
	first := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: firstPublisher, Active: true,
	}

	_, err = reconcileTrafficMapPublisher(t, first, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, firstPublisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, fallback.Reason)

	positive = true
	liveTrafficMap := getPublisherTrafficMap(t, kubeClient, trafficMap)
	liveTrafficMap.Spec.Entries = []v1beta1.TrafficMapEntry{{
		Cluster: "cluster-a", Endpoint: endpoint.DeepCopy(), Weight: 100, Healthy: true,
	}}
	require.NoError(t, kubeClient.Update(context.Background(), liveTrafficMap))
	liveTrafficMap = getPublisherTrafficMap(t, kubeClient, trafficMap)
	apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue,
		Reason: v1beta1.TrafficMapReasonRoutable, ObservedGeneration: liveTrafficMap.Generation,
	})
	apimeta.SetStatusCondition(&liveTrafficMap.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
		Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: liveTrafficMap.Generation,
	})
	require.NoError(t, kubeClient.Status().Update(context.Background(), liveTrafficMap))
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)

	restartedPublisher := newPublisher()
	restarted := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: restartedPublisher, Active: true,
	}
	_, err = reconcileTrafficMapPublisher(t, restarted, current)
	require.NoError(t, err)
	assert.Equal(t, []string{"apply"}, restartedPublisher.events)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Published)
	assert.Equal(t, current.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.Publisher.LastPositive)
	assert.Equal(t, []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 65}},
		current.Status.Publisher.LastPositive.Targets)
	assert.Equal(t, metav1.ConditionFalse, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
}

func TestTrafficMapPublisher_PositivePlanAfterFallback_AppliesItAndRecordsIt(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	oldSnapshot := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = oldSnapshot.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
		LastPositive: copyTrafficMapLastPositive(oldSnapshot),
	}
	trafficMap.Status.Conditions = []metav1.Condition{
		{
			Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionFalse,
			Reason: v1beta1.TrafficMapReasonPublicationFallback, ObservedGeneration: trafficMap.Generation - 1,
		},
		{
			Type: v1beta1.TrafficMapPublicationFallback, Status: metav1.ConditionTrue,
			Reason: v1beta1.TrafficMapReasonLastPositiveRetained, ObservedGeneration: trafficMap.Generation - 1,
		},
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 65}},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
		replay: func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
			t.Fatal("a positive recovery plan must not use Replay")
			return nil, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.Equal(t, oldSnapshot, current.Status.Publisher.LastPositive,
			"compatible prior snapshot remains the retry journal until positive Apply succeeds")
		assert.Equal(t, metav1.ConditionUnknown, apimeta.FindStatusCondition(
			current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
		).Status)
		return TrafficMapPublishResult{}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.True(t, current.Status.Published)
	assert.Equal(t, trafficMap.Generation, current.Status.ObservedTrafficMapGeneration)
	require.NotNil(t, current.Status.Publisher.LastPositive)
	assert.Equal(t, trafficMap.Generation, current.Status.Publisher.LastPositive.TrafficMapGeneration)
	assert.Equal(t, []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 65}},
		current.Status.Publisher.LastPositive.Targets)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonCurrentPlan, fallback.Reason)
}

func TestTrafficMapPublisher_PositiveApplyFails_KeepsTheEarlierLastPositive(t *testing.T) {
	owner, trafficMap := allHomesUnreadyPublisherObjects()
	owner.Status.Conditions[0].Status = corev1.ConditionTrue
	owner.Status.Conditions[0].Reason = "PlacementReady"
	for i := range owner.Status.Placement.Candidates {
		owner.Status.Placement.Candidates[i].ReadyReplicas = 1
	}
	for i := range trafficMap.Spec.Entries {
		trafficMap.Spec.Entries[i].Weight = 1
		trafficMap.Spec.Entries[i].Healthy = true
		trafficMap.Spec.Entries[i].Capacity.Ready = 1
	}
	routable := apimeta.FindStatusCondition(
		trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
	)
	routable.Status = metav1.ConditionTrue
	routable.Reason = v1beta1.TrafficMapReasonRoutable
	currentClaims := []string{"target-a", "target-b", "target-c"}
	durableClaims := []string{"target-a", "target-b", "target-c", "target-retired"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation - 1,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "target-a", Weight: 75},
			{Target: "target-b", Weight: 25},
			{Target: "target-c", Weight: 0},
			{Target: "target-retired", Weight: 0},
		},
	}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: durableClaims,
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: currentClaims,
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets: []v1beta1.TrafficMapPublisherTarget{
					{Target: "target-a", Weight: 50},
					{Target: "target-b", Weight: 30},
					{Target: "target-c", Weight: 20},
				},
				PlanCompatibilityDigest:      testPublisherDigest("a"),
				NoPositivePlanPolicy:         TrafficMapNoPositivePlanPolicyRetainLastPositive,
				AllowAllHomesUnreadyFallback: true,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive,
			"the retained plan must survive until positive recovery is fully applied")
		return TrafficMapPublishResult{}, errors.New("partial positive recovery")
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "partial positive recovery")
	assert.Equal(t, []string{"drain:target-retired", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
}

func TestTrafficMapPublisher_SourceStatusLagsItsSpec_KeepsLastPositiveNoEffects(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	owner.Status.ObservedGeneration--
	claims := []string{"target-a"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Empty(t, publisher.events)
	assert.Equal(t, reconciler.RequeueAfter, result.RequeueAfter)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.Equal(t, lastPositive.TrafficMapGeneration, current.Status.ObservedTrafficMapGeneration)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonEligibilityUnknown, fallback.Reason)
}

func TestTrafficMapPublisher_ReplayApplyFails_KeepsLastPositiveAndReplaysWhole(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a", "target-b"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets: []v1beta1.TrafficMapPublisherTarget{
			{Target: "target-a", Weight: 80},
			{Target: "target-b", Weight: 20},
		},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	kubeClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	var replayed []v1beta1.TrafficMapPublisherLastPositive
	applyCalls := 0
	newPublisher := func() *fallbackTrafficMapPublisher {
		base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
		publisher := &fallbackTrafficMapPublisher{
			recordingTrafficMapPublisher: base,
			capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
				return TrafficMapPublishPlanCapture{
					PlanCompatibilityDigest: testPublisherDigest("a"),
					NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
				}, nil
			},
			replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
				replayed = append(replayed, *copyTrafficMapLastPositive(&snapshot))
				return testTrafficMapPublishPlan{claims: claims}, nil
			},
		}
		publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			applyCalls++
			if applyCalls == 1 {
				return TrafficMapPublishResult{}, errors.New("partial backend replay")
			}
			return TrafficMapPublishResult{}, nil
		}
		return publisher
	}
	firstPublisher := newPublisher()
	first := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: firstPublisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, first, trafficMap)
	require.ErrorContains(t, err, "partial backend replay")
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, claims, current.Status.Publisher.ClaimedTargets)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive,
		"a failed partial replay must retain the complete retry journal")
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionUnknown, fallback.Status)
	restoreEligiblePublisherRoutingConditions(t, kubeClient, current)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)

	secondPublisher := newPublisher()
	restarted := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: secondPublisher, Active: true,
	}
	_, err = reconcileTrafficMapPublisher(t, restarted, current)
	require.NoError(t, err)
	require.Len(t, replayed, 2)
	assert.Equal(t, *lastPositive, replayed[0])
	assert.Equal(t, *lastPositive, replayed[1])
	assert.Equal(t, 2, applyCalls)
	current = getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
}

func TestTrafficMapPublisher_SuccessStatusWriteLostAfterReplay_ReplaysAgain(t *testing.T) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	claims := []string{"target-a"}
	lastPositive := &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMap.Generation - 1,
		ObservedISVCGeneration:  owner.Generation,
		PlanCompatibilityDigest: testPublisherDigest("a"),
		Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
	}
	trafficMap.Status.ObservedTrafficMapGeneration = lastPositive.TrafficMapGeneration
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims,
		LastPositive: copyTrafficMapLastPositive(lastPositive),
	}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	failingClient := fakeclient.Wrap(t, baseClient,
		fakeclient.FailingVerb(fakeclient.StatusApply, 2, errors.New("lost fallback success status")))
	replayCalls := 0
	applyCalls := 0
	newPublisher := func() *fallbackTrafficMapPublisher {
		base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
		publisher := &fallbackTrafficMapPublisher{
			recordingTrafficMapPublisher: base,
			capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
				return TrafficMapPublishPlanCapture{
					PlanCompatibilityDigest: testPublisherDigest("a"),
					NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
				}, nil
			},
			replay: func(snapshot v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error) {
				replayCalls++
				assert.Equal(t, *lastPositive, snapshot)
				return testTrafficMapPublishPlan{claims: claims}, nil
			},
		}
		publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			applyCalls++
			return TrafficMapPublishResult{}, nil
		}
		return publisher
	}
	first := &TrafficMapPublisherReconciler{
		Client: failingClient, APIReader: baseClient, Log: logr.Discard(), Publisher: newPublisher(), Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, first, trafficMap)
	require.ErrorContains(t, err, "lost fallback success status")
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.Equal(t, lastPositive, current.Status.Publisher.LastPositive)
	assert.False(t, current.Status.Published)
	assert.Equal(t, metav1.ConditionUnknown, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
	restoreEligiblePublisherRoutingConditions(t, baseClient, current)
	current = getPublisherTrafficMap(t, baseClient, trafficMap)

	restarted := &TrafficMapPublisherReconciler{
		Client: baseClient, APIReader: baseClient, Log: logr.Discard(), Publisher: newPublisher(), Active: true,
	}
	_, err = reconcileTrafficMapPublisher(t, restarted, current)
	require.NoError(t, err)
	assert.Equal(t, 2, replayCalls, "restart must reconstruct and replay the full snapshot")
	assert.Equal(t, 2, applyCalls, "a lost success status must produce an idempotent full reapply")
	current = getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.Equal(t, metav1.ConditionTrue, apimeta.FindStatusCondition(
		current.Status.Conditions, v1beta1.TrafficMapPublicationFallback,
	).Status)
}

func TestTrafficMapPublisher_PositivePlanAppliedAsRemoval_FailsAndRecordsNothing(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	base := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		apply: func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
			return TrafficMapPublishResult{Withdrawn: true}, nil
		},
	}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyApplyCurrent,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.ErrorContains(t, err, "does not match captured plan withdrawal")
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	assert.Nil(t, current.Status.Publisher.LastPositive,
		"a positive plan that was not applied as captured must never be journaled")
	assert.False(t, current.Status.Published)
}

func TestTrafficMapPublisher_PlanIsARemoval_ClearsLastPositiveBeforeEffects(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target-a"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration:    trafficMap.Generation - 1,
			ObservedISVCGeneration:  owner.Generation,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target-a", Weight: 100}},
		},
	}
	base := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	publisher := &fallbackTrafficMapPublisher{
		recordingTrafficMapPublisher: base,
		capture: func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error) {
			return TrafficMapPublishPlanCapture{
				PlanCompatibilityDigest: testPublisherDigest("a"),
				NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
				Withdrawn:               true,
			}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		assert.Nil(t, current.Status.Publisher.LastPositive)
		assert.False(t, current.Status.Published)
		return TrafficMapPublishResult{Withdrawn: true}, nil
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"drain:target-a", "apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Nil(t, current.Status.Publisher.LastPositive)
	assert.False(t, current.Status.Published)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.Equal(t, metav1.ConditionFalse, fallback.Status)
	assert.Equal(t, trafficMapPublisherReasonWithdrawn, fallback.Reason)
}
