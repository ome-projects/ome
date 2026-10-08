package placement

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// A result that re-enqueues the request carries the normal queue priority
// unless the pass set one; a result that ends the pass carries none.
func TestNormalizeRetryPriority(t *testing.T) {
	for _, tc := range []struct {
		name string
		res  ctrl.Result
		err  error
		want *int
	}{
		{name: "done", res: ctrl.Result{}, want: nil},
		{name: "retry after a delay", res: ctrl.Result{RequeueAfter: time.Minute}, want: ptr.To(retryPriority)},
		{name: "immediate requeue", res: ctrl.Result{Requeue: true}, want: ptr.To(retryPriority)}, //nolint:staticcheck // the conflict path still uses Requeue
		{name: "error", res: ctrl.Result{}, err: errors.New("member unreachable"), want: ptr.To(retryPriority)},
		{name: "priority chosen by the pass", res: ctrl.Result{RequeueAfter: time.Minute, Priority: ptr.To(5)}, want: ptr.To(5)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeRetryPriority(tc.res, tc.err).Priority
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("priority = %v, want %v", deref(got), deref(tc.want))
			}
		})
	}
}

func deref(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

// A source delivered by the informer's initial list enters the priority queue
// at the low priority. If its retry keeps that priority, continuously present
// normal-priority work starves it for good; with the normalized priority it is
// served within a bounded number of turns.
func TestRetryPriority_InitialListSourceIsServedBehindLiveWork(t *testing.T) {
	q := priorityqueue.New[reconcile.Request]("placement-retry-priority")
	defer q.ShutDown()
	request := func(name string) reconcile.Request {
		return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "team", Name: name}}
	}
	waitForLength := func(n int) {
		deadline := time.Now().Add(5 * time.Second)
		for q.Len() < n {
			if time.Now().After(deadline) {
				t.Fatalf("queue length %d, want at least %d", q.Len(), n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	h := &handler.EnqueueRequestForObject{}
	cold := map[string]bool{"cold-a": true, "cold-b": true}
	for name := range cold {
		h.Create(context.Background(), event.CreateEvent{
			Object:          &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team"}},
			IsInInitialList: true,
		}, q)
	}
	waitForLength(2)
	// The first pass of each cold source fails transiently; the retry keeps the
	// priority the request entered with, as the controller does when the
	// result names none.
	for range 2 {
		item, priority, _ := q.GetWithPriority()
		if priority != handler.LowPriority {
			t.Fatalf("initial-list item entered at priority %d, want %d", priority, handler.LowPriority)
		}
		q.Done(item)
		q.AddWithOpts(priorityqueue.AddOpts{After: 10 * time.Millisecond, Priority: ptr.To(priority)}, item)
	}
	waitForLength(2)
	for _, name := range []string{"busy-a", "busy-b", "busy-c"} {
		q.Add(request(name))
	}
	waitForLength(5)
	coldRuns := 0
	for range 100 {
		item, priority, _ := q.GetWithPriority()
		if cold[item.Name] {
			coldRuns++
		}
		q.Done(item)
		q.AddWithOpts(priorityqueue.AddOpts{Priority: ptr.To(priority)}, item)
		waitForLength(5)
	}
	if coldRuns != 0 {
		t.Fatalf("a preserved low priority let the cold sources run %d times behind live work; the failure mode this test pins is zero", coldRuns)
	}
	// With the controller's normalized retry result, the cold sources rejoin
	// the normal-priority rotation and are served within a few turns.
	retry := normalizeRetryPriority(ctrl.Result{RequeueAfter: time.Minute}, nil)
	q.AddWithOpts(priorityqueue.AddOpts{Priority: retry.Priority}, request("cold-a"), request("cold-b"))
	served := map[string]bool{}
	for range 5 {
		item, _, _ := q.GetWithPriority()
		if cold[item.Name] {
			served[item.Name] = true
		}
		q.Done(item)
	}
	if len(served) != 2 {
		t.Fatalf("cold sources served within five turns: %v, want both", served)
	}
}
