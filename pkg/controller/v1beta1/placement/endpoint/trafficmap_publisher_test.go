package endpoint

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The frame of the publisher pass: setup validation, the two watch predicates,
// the step runner, the status apply, and the digest, enqueue and claim helpers.

func TestTrafficMapPublisher_StatusWriteConflictsOnce_RetriesAndPublishes(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	baseClient := newTrafficMapPublisherClient(t, owner, trafficMap)
	conflicted := false
	concurrentRoutingWrite := func() {
		conflicted = true
		live := getPublisherTrafficMap(t, baseClient, trafficMap)
		apimeta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
			Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue,
			Reason: "ConcurrentRoutingWrite", ObservedGeneration: live.Generation,
		})
		require.NoError(t, baseClient.Status().Update(context.Background(), live))
	}
	wrapped := fakeclient.Wrap(t, baseClient, statusWriteConflictsOnce(concurrentRoutingWrite))
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target"},
	}
	reconciler := &TrafficMapPublisherReconciler{
		Client: wrapped, APIReader: baseClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	current := getPublisherTrafficMap(t, baseClient, trafficMap)
	assert.True(t, conflicted)
	published := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, published)
	assert.Equal(t, metav1.ConditionTrue, published.Status)
}

func TestTrafficMapPublisherStatusApply_Rendered_CarriesOnlyPublisherOwnedFields(t *testing.T) {
	_, trafficMap := publisherTestObjects()
	trafficMap.ResourceVersion = "17"
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: []string{"target"},
		LastPositive: &v1beta1.TrafficMapPublisherLastPositive{
			TrafficMapGeneration: 2, ObservedISVCGeneration: 3,
			PlanCompatibilityDigest: testPublisherDigest("a"),
			Targets:                 []v1beta1.TrafficMapPublisherTarget{{Target: "target", Weight: 37}},
		},
	}
	trafficMap.Status.GatewayRef = &v1beta1.TrafficMapGatewayRef{Kind: "HTTPRoute", Name: "model"}
	trafficMap.Status.Conditions = []metav1.Condition{
		{Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue, Reason: "Routable"},
		{Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue, Reason: "Published"},
		{Type: v1beta1.TrafficMapPublicationFallback, Status: metav1.ConditionFalse, Reason: "CurrentPlanApplied"},
	}

	content := publisherApplyContent(t, trafficMapPublisherStatusApply(trafficMap))
	metadata := content["metadata"].(map[string]any)
	assert.Equal(t, "17", metadata["resourceVersion"])
	status := content["status"].(map[string]any)
	_, found := status["sourceUID"]
	assert.False(t, found, "publisher SSA must not own routing source provenance")
	publisher := status["publisher"].(map[string]any)
	lastPositive := publisher["lastPositive"].(map[string]any)
	assert.Equal(t, int64(2), lastPositive["trafficMapGeneration"])
	targets := lastPositive["targets"].([]any)
	require.Len(t, targets, 1)
	assert.Equal(t, int64(37), targets[0].(map[string]any)["weight"])
	conditions := status["conditions"].([]any)
	require.Len(t, conditions, 2)
	assert.Equal(t, v1beta1.TrafficMapPublished, conditions[0].(map[string]any)["type"])
	assert.Equal(t, v1beta1.TrafficMapPublicationFallback, conditions[1].(map[string]any)["type"])

	trafficMap.Status.Publisher = nil
	trafficMap.Status.GatewayRef = nil
	trafficMap.Status.Published = false
	trafficMap.Status.ObservedTrafficMapGeneration = 0
	content = publisherApplyContent(t, trafficMapPublisherStatusApply(trafficMap))
	status = content["status"].(map[string]any)
	_, found = status["publisher"]
	assert.False(t, found, "owned pointer fields are removed by SSA omission")
	_, found = status["gatewayRef"]
	assert.False(t, found, "owned pointer fields are removed by SSA omission")
	assert.Equal(t, false, status["published"])
	assert.Equal(t, int64(0), status["observedTrafficMapGeneration"])
}

func TestTrafficMapPublisherHelpers_DigestEnqueueAndClaimChecks_MatchTheirContracts(t *testing.T) {
	first, err := publisherOptionsDigest("publisher-a", map[string]string{"b": "2", "a": "1"})
	require.NoError(t, err)
	second, err := publisherOptionsDigest("publisher-a", map[string]string{"a": "1", "b": "2"})
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, first)
	otherPublisher, err := publisherOptionsDigest("publisher-b", map[string]string{"a": "1", "b": "2"})
	require.NoError(t, err)
	assert.NotEqual(t, first, otherPublisher)

	options := trafficMapPublisherControllerOptions()
	assert.Equal(t, 1, options.MaxConcurrentReconciles)
	require.NotNil(t, options.NeedLeaderElection)
	assert.True(t, *options.NeedLeaderElection)
	requests := enqueueOwnedTrafficMap(context.Background(), &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"},
	})
	require.Len(t, requests, 1)
	assert.Equal(t, types.NamespacedName{Namespace: "team-a", Name: "model"}, requests[0].NamespacedName)
	assert.True(t, publisherClaimsReplayCompatible(nil, []string{"a", "b"}),
		"the factual empty plan is the eligible-loss exception")
	assert.True(t, publisherClaimsReplayCompatible([]string{"a", "b"}, []string{"a", "b"}))
	assert.False(t, publisherClaimsReplayCompatible([]string{"a"}, []string{"a", "b"}),
		"a nonempty strict subset is a claim-set change")
}

func TestValidateSetup_NoLeaderElectionOrNoResync_ReturnsError(t *testing.T) {
	publisher := &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}
	kubeClient := newTrafficMapPublisherClient(t)
	reconciler := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Publisher: publisher, Active: true,
	}

	err := reconciler.validateSetup()
	require.ErrorContains(t, err, "requires leader election")

	reconciler.LeaderElectionEnabled = true
	err = reconciler.validateSetup()
	require.ErrorContains(t, err, "requires a positive resync period")

	reconciler.Active = false
	reconciler.LeaderElectionEnabled = false
	err = reconciler.validateSetup()
	require.ErrorContains(t, err, "requires leader election")

	reconciler.LeaderElectionEnabled = true
	require.NoError(t, reconciler.validateSetup())
}

func TestTrafficMapPublisherOwnerChange_AnyRelevantFieldChanges_PassesUnchangedDrops(t *testing.T) {
	oldOwner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "model", Namespace: "team-a", Generation: 1,
	}}
	unchanged := oldOwner.DeepCopy()
	assert.False(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: unchanged,
	}))

	newGeneration := oldOwner.DeepCopy()
	newGeneration.Generation++
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newGeneration,
	}))

	newAnnotation := oldOwner.DeepCopy()
	newAnnotation.Annotations = map[string]string{"example.com/key": "value"}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newAnnotation,
	}))

	newLabel := oldOwner.DeepCopy()
	newLabel.Labels = map[string]string{"example.com/key": "value"}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newLabel,
	}))

	newFinalizer := oldOwner.DeepCopy()
	newFinalizer.Finalizers = []string{"example.com/finalizer"}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newFinalizer,
	}))

	newObservedGeneration := oldOwner.DeepCopy()
	newObservedGeneration.Status.ObservedGeneration = 1
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newObservedGeneration,
	}))

	newPlacement := oldOwner.DeepCopy()
	newPlacement.Status.Placement = &v1beta1.PlacementStatus{Phase: v1beta1.PlacementPhasePending}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newPlacement,
	}))

	newReady := oldOwner.DeepCopy()
	newReady.Status.Conditions = duckv1.Conditions{{
		Type: apis.ConditionReady, Status: corev1.ConditionUnknown, Reason: "PlacementLost",
	}}
	assert.True(t, trafficMapPublisherOwnerChange.Update(event.UpdateEvent{
		ObjectOld: oldOwner, ObjectNew: newReady,
	}))
}

func TestTrafficMapPublicationChange_PublisherOwnedStatusOnly_DroppedOthersPass(t *testing.T) {
	_, old := publisherTestObjects()
	current := old.DeepCopy()
	current.Status.Published = true
	current.Status.ObservedTrafficMapGeneration = current.Generation
	current.Status.Conditions = []metav1.Condition{{Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue}}
	require.False(t, trafficMapPublicationChange.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: current}))
	for _, kind := range []string{v1beta1.TrafficMapRoutable, v1beta1.TrafficMapCapacityFallback, v1beta1.TrafficMapOverrideActive} {
		current = old.DeepCopy()
		current.Status.Conditions = []metav1.Condition{{Type: kind, Status: metav1.ConditionFalse}}
		require.True(t, trafficMapPublicationChange.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: current}), kind)
	}
	require.True(t, trafficMapPublicationChange.Create(event.CreateEvent{Object: old}))
	require.True(t, trafficMapPublicationChange.Delete(event.DeleteEvent{Object: old}))
}

func TestRunPublisherSteps_StopsAtTheFirstStepThatEndsThePass(t *testing.T) {
	var ran []string
	step := func(name string, next bool, err error) publisherStep {
		return func(context.Context) (bool, error) {
			ran = append(ran, name)
			return next, err
		}
	}
	run := func(steps ...publisherStep) (done bool, order string, err error) {
		ran = nil
		done, err = runPublisherSteps(context.Background(), steps)
		return done, strings.Join(ran, ","), err
	}

	done, order, err := run(step("first", true, nil), step("second", true, nil))
	if err != nil || !done || order != "first,second" {
		t.Fatalf("every step went on: done=%v err=%v ran=%q, want done=true err=nil ran=first,second", done, err, order)
	}
	done, order, err = run(step("first", true, nil), step("ends", false, nil), step("never", true, nil))
	if err != nil || done || order != "first,ends" {
		t.Fatalf("a step ended the pass: done=%v err=%v ran=%q, want done=false err=nil ran=first,ends", done, err, order)
	}
	writeFailed := errors.New("write the status")
	done, order, err = run(step("fails", true, writeFailed), step("never", true, nil))
	if !errors.Is(err, writeFailed) || done || order != "fails" {
		t.Fatalf("a step failed: done=%v err=%v ran=%q, want done=false the step's error ran=fails", done, err, order)
	}

	pass := &publisherPass{}
	requeue := ctrl.Result{Requeue: true}
	done, order, err = run(step("first", true, nil), func(context.Context) (bool, error) {
		ran = append(ran, "ends")
		return pass.end(requeue, nil)
	}, step("never", true, nil))
	if err != nil || done || order != "first,ends" || pass.result != requeue {
		t.Fatalf("a step ended the pass with a result: done=%v err=%v ran=%q result=%+v, want done=false err=nil ran=first,ends result=%+v", done, err, order, pass.result, requeue)
	}
}
