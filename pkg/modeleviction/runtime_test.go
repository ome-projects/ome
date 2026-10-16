package modeleviction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/evictionplanner"
	"sigs.k8s.io/ome/pkg/modelusage"
)

func TestHistoryHandlesDeletionAndLastClaimRelease(t *testing.T) {
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Namespace: "customer", Name: "model", UID: "model", CreationTimestamp: metav1.NewTime(roundTime.Add(-time.Hour))}}
	c, q := fakeSources(t, model)
	evaluator := modelusage.NewEvaluator(c, c, q, func() time.Time { return roundTime })
	worker := newWorker(nil, evaluator, DefaultOptions(), func(context.Context) bool { return true })
	defer worker.queue.ShutDown()
	worker.clock = clocktesting.NewFakeClock(roundTime)
	worker.active.Store(true)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "customer", Name: "endpoint"}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}}}
	// Delete events can arrive as tombstones after a watch reconnect.
	worker.deleted(toolscache.DeletedFinalStateUnknown{Obj: isvc})
	require.Equal(t, 1, worker.queue.Len())
	require.True(t, worker.processHistory(context.Background()))
	stored := &v1beta1.BaseModel{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "customer", Name: "model"}, stored))
	require.Contains(t, stored.Annotations[constants.ModelUsageHistoryAnnotation], "lastObservedDemandEndedAt")

	old := stored.DeepCopy()
	old.Annotations[constants.ModelArtifactUseClaimsAnnotation] = `{"endpoint":true}`
	worker.removed(old, stored)
	require.Equal(t, 1, worker.queue.Len())
	request, shutdown := worker.queue.Get()
	require.False(t, shutdown)
	worker.queue.Done(request)
	require.Equal(t, types.NamespacedName{Namespace: "customer", Name: "model"}, request.Model)
	require.Equal(t, roundTime, request.ReferenceRemovedAt)
	worker.removed(stored, stored)
	require.Zero(t, worker.queue.Len(), "ordinary model updates must not cause a history feedback loop")
	worker.active.Store(false)
	worker.deleted(isvc)
	require.Zero(t, worker.queue.Len(), "standby worker must not accumulate events")
}

func TestHistoryRetryRetainsOriginalEventTime(t *testing.T) {
	requests := make(chan modelusage.Request, 2)
	calls := 0
	evaluator := evaluatorFunc(func(_ context.Context, r modelusage.Request, _ types.UID, _ bool) (evictionplanner.Eligibility, error) {
		requests <- r
		calls++
		if calls == 1 {
			return evictionplanner.Eligibility{}, errors.New("temporary API failure")
		}
		return evictionplanner.Eligibility{}, apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "basemodels"}, r.Model.Name)
	})
	worker := newWorker(nil, evaluator, DefaultOptions(), func(context.Context) bool { return true })
	defer worker.queue.ShutDown()
	event := modelusage.Request{Model: types.NamespacedName{Namespace: "customer", Name: "gone"}, ReferenceRemovedAt: roundTime}
	worker.queue.Add(event)
	require.True(t, worker.processHistory(context.Background()))
	require.Eventually(t, func() bool { return worker.queue.Len() == 1 }, 3*time.Second, 10*time.Millisecond)
	require.True(t, worker.processHistory(context.Background()))
	require.Equal(t, event, <-requests)
	require.Equal(t, event, <-requests)
	require.Zero(t, worker.queue.NumRequeues(event), "NotFound finishes the event instead of retrying forever")
}

func TestWorkerWaitsForSyncAndSchedulesAfterCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := DefaultOptions()
	clock := clocktesting.NewFakeClock(roundTime)
	synced := make(chan struct{})
	started := make(chan struct{}, 2)
	finishRound := make(chan struct{}, 2)
	evaluator := evaluatorFunc(func(context.Context, modelusage.Request, types.UID, bool) (evictionplanner.Eligibility, error) {
		return evictionplanner.Eligibility{}, nil
	})
	worker := newWorker(func(ctx context.Context) (*Report, error) {
		started <- struct{}{}
		select {
		case <-finishRound:
			return &Report{DryRun: true}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, evaluator, options, func(ctx context.Context) bool {
		select {
		case <-synced:
			return true
		case <-ctx.Done():
			return false
		}
	})
	worker.clock = clock
	stopped := make(chan error, 1)
	go func() { stopped <- worker.Start(ctx) }()
	require.Empty(t, started, "no planning before reference cache synchronization")
	close(synced)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first round did not start")
	}
	clock.Step(2 * options.Interval)
	require.Empty(t, started, "a slow round cannot overlap another round")
	finishRound <- struct{}{}
	require.Eventually(t, clock.HasWaiters, time.Second, time.Millisecond)
	clock.Step(options.Interval - time.Second)
	require.Empty(t, started)
	clock.Step(time.Second)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("second round did not start")
	}
	cancel()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
	require.True(t, worker.NeedLeaderElection())
}
