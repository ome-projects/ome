package endpoint

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A TrafficMap records the owner generation it was derived from, and the
// routing controller rewrites the map after every owner edit. A map behind
// its owner is one whose rewrite has not landed yet: the publisher leaves its
// status alone and publishes when the rewrite arrives. A map ahead of its
// owner, or one whose name or owner UID does not match, is not this owner's
// map and is rejected.

// recordingLogSink keeps the publisher's info and error lines apart.
type recordingLogSink struct {
	infos  []string
	errors []string
}

func (s *recordingLogSink) Init(logr.RuntimeInfo)               {}
func (s *recordingLogSink) Enabled(int) bool                    { return true }
func (s *recordingLogSink) Info(_ int, msg string, _ ...any)    { s.infos = append(s.infos, msg) }
func (s *recordingLogSink) Error(_ error, msg string, _ ...any) { s.errors = append(s.errors, msg) }
func (s *recordingLogSink) WithValues(...any) logr.LogSink      { return s }
func (s *recordingLogSink) WithName(string) logr.LogSink        { return s }

// publishedTrafficMapObjects is an owner and its map after a publication:
// the finalizer is on, status.published is true and Published is True.
func publishedTrafficMapObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Published = true
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{PublisherName: "example-publisher"}
	trafficMap.Status.Conditions = []metav1.Condition{{
		Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue,
		Reason: trafficMapPublisherReasonPublished, ObservedGeneration: trafficMap.Generation,
	}}
	return owner, trafficMap
}

// ownerCheckFixture runs publisher passes over one map with a stateful
// publisher that counts what it is asked to do, and keeps the log lines.
type ownerCheckFixture struct {
	kubeClient   client.WithWatch
	publisher    *recordingTrafficMapPublisher
	sink         *recordingLogSink
	reconciler   *TrafficMapPublisherReconciler
	trafficMap   *v1beta1.TrafficMap
	resolveCalls int
	planCalls    int
}

// newOwnerCheckFixture stores the owner and the map. With laterReads set,
// the uncached reads after the first ones go through that option.
func newOwnerCheckFixture(
	t *testing.T,
	owner *v1beta1.InferenceService,
	trafficMap *v1beta1.TrafficMap,
	laterReads fakeclient.Option,
) *ownerCheckFixture {
	t.Helper()
	fixture := &ownerCheckFixture{
		kubeClient: newTrafficMapPublisherClient(t, owner, trafficMap),
		sink:       &recordingLogSink{},
		trafficMap: trafficMap,
	}
	fixture.publisher = &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true,
		resolve: func(map[string]string, map[string]string) (map[string]string, error) {
			fixture.resolveCalls++
			return nil, nil
		},
		plan: func(*v1beta1.InferenceService, *v1beta1.TrafficMap, map[string]string) (TrafficMapPublishPlan, error) {
			fixture.planCalls++
			return testTrafficMapPublishPlan{}, nil
		},
	}
	var reader client.Reader = fixture.kubeClient
	if laterReads != nil {
		reader = fakeclient.Wrap(t, fixture.kubeClient, laterReads)
	}
	fixture.reconciler = &TrafficMapPublisherReconciler{
		Client: fixture.kubeClient, APIReader: reader, Log: logr.New(fixture.sink),
		Publisher: fixture.publisher, Active: true, RequeueAfter: time.Minute,
	}
	return fixture
}

func (f *ownerCheckFixture) pass(t *testing.T) ctrl.Result {
	t.Helper()
	result, err := reconcileTrafficMapPublisher(t, f.reconciler, f.trafficMap)
	require.NoError(t, err)
	return result
}

func (f *ownerCheckFixture) current(t *testing.T) *v1beta1.TrafficMap {
	t.Helper()
	return getPublisherTrafficMap(t, f.kubeClient, f.trafficMap)
}

// publishedCondition returns the map's Published condition.
func publishedCondition(t *testing.T, trafficMap *v1beta1.TrafficMap) *metav1.Condition {
	t.Helper()
	condition := apimeta.FindStatusCondition(trafficMap.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	return condition
}

// assertPublicationKept checks that a pass left the earlier publication
// standing: no plan, no effect, no status change, no error line, one info line.
func assertPublicationKept(t *testing.T, fixture *ownerCheckFixture, result ctrl.Result) {
	t.Helper()
	assert.Equal(t, ctrl.Result{RequeueAfter: time.Minute}, result, "the pass waits for the rewrite or the resync")
	assert.Empty(t, fixture.publisher.events)
	current := fixture.current(t)
	assert.True(t, current.Status.Published)
	condition := publishedCondition(t, current)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Equal(t, trafficMapPublisherReasonPublished, condition.Reason)
	assert.Nil(t, apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublicationFallback))
	assert.Empty(t, fixture.sink.errors, "a map behind its owner is not an error")
	assert.Len(t, fixture.sink.infos, 1, "one info line says the map waits for its rewrite")
}

func TestTrafficMapPublisher_MapBehindOwner_WaitsForTheRewrite(t *testing.T) {
	owner, trafficMap := publishedTrafficMapObjects()
	owner.Generation++
	fixture := newOwnerCheckFixture(t, owner, trafficMap, nil)

	result := fixture.pass(t)

	assert.Zero(t, fixture.resolveCalls)
	assert.Zero(t, fixture.planCalls)
	assertPublicationKept(t, fixture, result)
}

func TestTrafficMapPublisher_OwnerMovesAheadWhilePlanning_WaitsForTheRewrite(t *testing.T) {
	owner, trafficMap := publishedTrafficMapObjects()
	ownerMovesAhead := changedOnLaterReads(nil, func(owner *v1beta1.InferenceService) { owner.Generation++ }, nil)
	fixture := newOwnerCheckFixture(t, owner, trafficMap, ownerMovesAhead)

	result := fixture.pass(t)

	assert.Equal(t, 1, fixture.planCalls, "the plan was made before the owner moved")
	assertPublicationKept(t, fixture, result)
}

func TestTrafficMapPublisher_MapAheadOfOwnerOrAnotherOwner_IsRejected(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1beta1.InferenceService, *v1beta1.TrafficMap)
	}{
		{
			name: "map ahead of its owner",
			mutate: func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.ObservedISVCGeneration = owner.Generation + 1
			},
		},
		{
			name: "map name differs from the owner name",
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Name = "different"
			},
		},
		{
			name: "service name differs from the owner name",
			mutate: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
				trafficMap.Spec.Service = "different"
			},
		},
		{
			name: "owner UID differs from the controller reference",
			mutate: func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) {
				owner.UID = "replacement-owner-uid"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publishedTrafficMapObjects()
			tt.mutate(owner, trafficMap)
			fixture := newOwnerCheckFixture(t, owner, trafficMap, nil)

			result := fixture.pass(t)

			assert.Equal(t, ctrl.Result{RequeueAfter: time.Minute}, result)
			assert.Zero(t, fixture.resolveCalls, "ownership is checked before the options")
			assert.Zero(t, fixture.planCalls, "ownership is checked before the plan")
			assert.Empty(t, fixture.publisher.events)
			current := fixture.current(t)
			assert.False(t, current.Status.Published)
			condition := publishedCondition(t, current)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, trafficMapPublisherReasonInvalidOwner, condition.Reason)
			assert.Len(t, fixture.sink.errors, 1, "a map that is not this owner's is an error")
			assert.Empty(t, fixture.sink.infos)
		})
	}
}

func TestTrafficMapPublisher_RewriteArrives_MapPublishes(t *testing.T) {
	owner, trafficMap := publishedTrafficMapObjects()
	owner.Generation++
	fixture := newOwnerCheckFixture(t, owner, trafficMap, nil)
	fixture.pass(t)
	assert.Empty(t, fixture.publisher.events)

	// The routing controller rewrites the map from the owner's new generation.
	rewritten := fixture.current(t)
	rewritten.Generation++
	rewritten.Spec.ObservedISVCGeneration = owner.Generation
	require.NoError(t, fixture.kubeClient.Update(context.Background(), rewritten))

	fixture.pass(t)

	assert.Equal(t, 1, fixture.planCalls, "the rewritten map is planned once")
	assert.Equal(t, []string{"apply"}, fixture.publisher.events)
	current := fixture.current(t)
	assert.True(t, current.Status.Published)
	condition := publishedCondition(t, current)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Equal(t, trafficMapPublisherReasonPublished, condition.Reason)
	assert.Equal(t, rewritten.Generation, condition.ObservedGeneration)
	assert.Equal(t, rewritten.Generation, current.Status.ObservedTrafficMapGeneration)
}
