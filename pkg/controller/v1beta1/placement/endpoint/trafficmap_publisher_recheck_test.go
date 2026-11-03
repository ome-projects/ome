package endpoint

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The two ways a pass ends on state that changed under it: the fresh-state check
// before any effect fails, or another write changed the claim record.

func TestTrafficMapPublisher_OwnerOrSourceChangesAfterPlanning_AppliesNothing(t *testing.T) {
	changes := []struct {
		name             string
		mutateTrafficMap func(*v1beta1.TrafficMap)
		mutateOwner      func(*v1beta1.InferenceService)
	}{
		{
			name: "source UID removed",
			mutateTrafficMap: func(trafficMap *v1beta1.TrafficMap) {
				trafficMap.Status.SourceUID = ""
			},
		},
		{
			name: "source UID changed",
			mutateTrafficMap: func(trafficMap *v1beta1.TrafficMap) {
				trafficMap.Status.SourceUID = "replacement-owner-uid"
			},
		},
		{
			name: "controller reference removed",
			mutateTrafficMap: func(trafficMap *v1beta1.TrafficMap) {
				trafficMap.OwnerReferences = nil
			},
		},
		{
			name: "owner began deleting",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				now := metav1.Now()
				owner.DeletionTimestamp = &now
				owner.Finalizers = []string{"example.com/hold"}
			},
		},
		{
			name: "owner became ineligible",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Spec.Placement = nil
			},
		},
		{
			name: "owner generation changed",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Generation++
			},
		},
		{
			name: "owner labels changed",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Labels = map[string]string{"example.com/publish": "disabled"}
			},
		},
		{
			name: "owner annotations changed",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Annotations = map[string]string{"example.com/publication": "changed"}
			},
		},
		{
			name: "owner finalizers changed",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				owner.Finalizers = []string{"example.com/publication"}
			},
		},
		{
			name: "owner opted out",
			mutateOwner: func(owner *v1beta1.InferenceService) {
				disabled := false
				owner.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
			},
		},
	}
	for _, change := range changes {
		for _, stateful := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stateful=%t", change.name, stateful), func(t *testing.T) {
				owner, trafficMap := publisherTestObjects()
				if stateful {
					trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
				}
				baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
				reader := fakeclient.Wrap(t, baseClient, changedOnLaterReads(change.mutateTrafficMap, change.mutateOwner, nil))
				planCalls := 0
				var claims []string
				if stateful {
					claims = []string{"target-a"}
				}
				publisher := &recordingTrafficMapPublisher{
					name: "example-publisher", stateful: stateful, claims: claims,
					plan: func(
						*v1beta1.InferenceService,
						*v1beta1.TrafficMap,
						map[string]string,
					) (TrafficMapPublishPlan, error) {
						planCalls++
						return testTrafficMapPublishPlan{claims: claims}, nil
					},
				}
				reconciler := &TrafficMapPublisherReconciler{
					Client: baseClient, APIReader: reader, Log: logr.Discard(), Publisher: publisher, Active: true,
				}

				_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

				require.NoError(t, err)
				assert.Equal(t, 1, planCalls, "planning is pure and follows the initial authoritative provenance check")
				assert.Empty(t, publisher.events, "the last live provenance check must precede external effects")
				current := getPublisherTrafficMap(t, baseClient, trafficMap)
				assert.Equal(t, owner.UID, current.Status.SourceUID)
				condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
				require.NotNil(t, condition)
				assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
			})
		}
	}
}

func TestTrafficMapPublisher_OwnerReadFailsAfterPlanning_ReturnsErrorNoEffects(t *testing.T) {
	for _, stateful := range []bool{false, true} {
		t.Run(fmt.Sprintf("stateful=%t", stateful), func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			if stateful {
				trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
			}
			baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
			reader := fakeclient.Wrap(t, baseClient,
				changedOnLaterReads(nil, nil, apierrors.NewInternalError(errors.New("owner read failed"))))
			planCalls := 0
			var claims []string
			if stateful {
				claims = []string{"target-a"}
			}
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: stateful, claims: claims,
				plan: func(
					*v1beta1.InferenceService,
					*v1beta1.TrafficMap,
					map[string]string,
				) (TrafficMapPublishPlan, error) {
					planCalls++
					return testTrafficMapPublishPlan{claims: claims}, nil
				},
			}
			reconciler := &TrafficMapPublisherReconciler{
				Client: baseClient, APIReader: reader, Log: logr.Discard(), Publisher: publisher, Active: true,
			}

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)

			require.ErrorContains(t, err, "owner read failed")
			assert.Equal(t, 1, planCalls)
			assert.Empty(t, publisher.events)
			current := getPublisherTrafficMap(t, baseClient, trafficMap)
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
		})
	}
}

func TestTrafficMapPublisher_LastPositiveChangedDuringApply_DoesNotConfirmIt(t *testing.T) {
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
			return testTrafficMapPublishPlan{claims: claims}, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		live := getPublisherTrafficMap(t, kubeClient, trafficMap)
		live.Status.Publisher.LastPositive.Targets[0].Weight = 99
		require.NoError(t, kubeClient.Status().Update(context.Background(), live))
		return TrafficMapPublishResult{}, nil
	}

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Equal(t, int64(99), current.Status.Publisher.LastPositive.Targets[0].Weight)
	fallback := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback)
	require.NotNil(t, fallback)
	assert.NotEqual(t, metav1.ConditionTrue, fallback.Status,
		"status must not confirm a snapshot other than the one actually replayed")
}

func TestTrafficMapPublisher_RecordLostDuringApply_RequeuesWithoutRewritingIt(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	publisher.apply = func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error) {
		live := getPublisherTrafficMap(t, kubeClient, trafficMap)
		live.Status.Publisher = nil
		require.NoError(t, kubeClient.Status().Update(context.Background(), live))
		return TrafficMapPublishResult{GatewayRef: &v1beta1.TrafficMapGatewayRef{
			Kind: "HTTPRoute", Name: "model",
		}}, nil
	}

	result, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	assert.Nil(t, current.Status.Publisher)
	assert.False(t, current.Status.Published)
	assert.Nil(t, current.Status.GatewayRef)
}

func TestEndClaimRecordWrite_RecordChangedOrWriteFailed_RequeuesOrReturnsError(t *testing.T) {
	pass := &publisherPass{}
	next, err := pass.endClaimRecordWrite(fmt.Errorf("%w: claims moved", errTrafficMapPublisherJournalChanged))
	if next || err != nil || !pass.result.Requeue {
		t.Fatalf("a changed claim record requeues without an error: next=%v err=%v result=%+v", next, err, pass.result)
	}
	pass = &publisherPass{}
	writeFailed := errors.New("patch status")
	next, err = pass.endClaimRecordWrite(writeFailed)
	if next || !errors.Is(err, writeFailed) || pass.result != (ctrl.Result{}) {
		t.Fatalf("any other write failure returns its error: next=%v err=%v result=%+v", next, err, pass.result)
	}
}
