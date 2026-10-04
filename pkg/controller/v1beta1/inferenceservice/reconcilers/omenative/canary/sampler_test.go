package canary

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
)

func passEval(*int32) evalFunc {
	return func(context.Context, SampleRequest) analysis.Result {
		return analysis.Result{Outcome: analysis.Pass}
	}
}

// TestSampler_GetKicksThenCaches: first Get misses and kicks a background query;
// completion emits an event for the right ISVC; the next Get serves the cached
// result; the query ran exactly once.
func TestSampler_GetKicksThenCaches(t *testing.T) {
	events := make(chan event.GenericEvent, 1)
	var calls int32
	s := NewSampler(func(context.Context, SampleRequest) analysis.Result {
		atomic.AddInt32(&calls, 1)
		return analysis.Result{Outcome: analysis.Fail}
	}, events, 4, time.Minute)

	req := SampleRequest{Key: SampleKey{Namespace: "ns", ISVCName: "svc"}}
	if _, _, ok := s.Get(req, time.Time{}); ok {
		t.Fatal("first Get should miss")
	}
	select {
	case ev := <-events:
		if ev.Object.GetNamespace() != "ns" || ev.Object.GetName() != "svc" {
			t.Fatalf("event for wrong object: %s/%s", ev.Object.GetNamespace(), ev.Object.GetName())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected a completion event")
	}
	res, at, ok := s.Get(req, time.Time{})
	if !ok || res.Outcome != analysis.Fail || at.IsZero() {
		t.Fatalf("second Get should serve cached Fail, got ok=%v outcome=%v", ok, res.Outcome)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("eval should run once, ran %d", got)
	}
}

// TestSampler_DedupsInflight: many Gets for the same key while a query is in
// flight kick only ONE background query.
func TestSampler_DedupsInflight(t *testing.T) {
	events := make(chan event.GenericEvent, 4)
	release := make(chan struct{})
	var calls int32
	s := NewSampler(func(context.Context, SampleRequest) analysis.Result {
		atomic.AddInt32(&calls, 1)
		<-release
		return analysis.Result{Outcome: analysis.Pass}
	}, events, 4, time.Minute)

	req := SampleRequest{Key: SampleKey{ISVCName: "svc"}}
	for i := 0; i < 5; i++ {
		if _, _, ok := s.Get(req, time.Time{}); ok {
			t.Fatal("Gets during an in-flight query should miss")
		}
	}
	close(release)
	<-events
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("dedup failed: %d evals, want 1", got)
	}
}

// TestSampler_StarvationMetrics: when MaxConcurrency is saturated, a query that
// queues behind the semaphore bumps the starvation counter and is recorded as
// inflight once it runs — the signal operators use to see Interval being silently
// violated. The semaphore semantics (one slot at a time) are unchanged.
func TestSampler_StarvationMetrics(t *testing.T) {
	events := make(chan event.GenericEvent, 4)
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	s := NewSampler(func(context.Context, SampleRequest) analysis.Result {
		started <- struct{}{}
		<-release
		return analysis.Result{Outcome: analysis.Pass}
	}, events, 1, time.Minute) // cap of 1: the second key must queue

	starvedBefore := testutil.ToFloat64(canarySamplerStarvedTotal)

	// First key takes the only slot.
	s.Get(SampleRequest{Key: SampleKey{ISVCName: "a"}}, time.Time{})
	<-started
	if got := testutil.ToFloat64(canarySamplerInflight); got != 1 {
		t.Fatalf("inflight should be 1 while first query runs, got %v", got)
	}

	// Second key finds the slot busy → it starves and queues.
	s.Get(SampleRequest{Key: SampleKey{ISVCName: "b"}}, time.Time{})
	deadline := time.After(2 * time.Second)
	for {
		if testutil.ToFloat64(canarySamplerStarvedTotal)-starvedBefore >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("second query should have starved on the saturated semaphore")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if got := testutil.ToFloat64(canarySamplerQueueDepth); got != 1 {
		t.Fatalf("queue depth should be 1 while the second query waits, got %v", got)
	}

	close(release)
	<-started // second query now runs
	<-events
	<-events

	// Drain to terminal: both done, no slots held, nothing queued.
	deadline = time.After(2 * time.Second)
	for {
		if testutil.ToFloat64(canarySamplerInflight) == 0 && testutil.ToFloat64(canarySamplerQueueDepth) == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("inflight/queue should drain to 0, got inflight=%v queue=%v",
				testutil.ToFloat64(canarySamplerInflight), testutil.ToFloat64(canarySamplerQueueDepth))
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestSampler_FreshnessSince: a result is served only to a caller whose `since`
// predates it; a caller that already consumed it (since == producedAt) misses.
func TestSampler_FreshnessSince(t *testing.T) {
	events := make(chan event.GenericEvent, 4)
	s := NewSampler(passEval(nil), events, 4, time.Minute)
	t0 := time.Unix(1000, 0)
	s.now = func() time.Time { return t0 }

	req := SampleRequest{Key: SampleKey{ISVCName: "svc"}}
	if _, _, ok := s.Get(req, time.Time{}); ok {
		t.Fatal("first Get should miss")
	}
	<-events // entry now cached at t0

	if _, at, ok := s.Get(req, t0.Add(-time.Second)); !ok || !at.Equal(t0) {
		t.Fatalf("Get(since<t0) should hit at t0, got ok=%v at=%v", ok, at)
	}
	if _, _, ok := s.Get(req, t0); ok {
		t.Fatal("Get(since==producedAt) should miss — that sample was already consumed")
	}
}

// TestSampler_ProducedAtSurvivesStatus: the caller keeps producedAt in a
// metav1.Time status field, which stores whole seconds, and passes that stored
// value back as `since`. A sample consumed under a wall clock with sub-second
// detail must then read as consumed, not as newer than its own record.
func TestSampler_ProducedAtSurvivesStatus(t *testing.T) {
	events := make(chan event.GenericEvent, 4)
	s := NewSampler(passEval(nil), events, 4, time.Minute)
	s.now = func() time.Time { return time.Unix(1000, int64(350*time.Millisecond)) }

	req := SampleRequest{Key: SampleKey{ISVCName: "svc"}}
	if _, _, ok := s.Get(req, time.Time{}); ok {
		t.Fatal("first Get should miss")
	}
	<-events
	_, producedAt, ok := s.Get(req, time.Time{})
	if !ok {
		t.Fatal("the landed sample should be served")
	}
	stored := metav1.NewTime(producedAt).Rfc3339Copy().Time
	if _, _, ok := s.Get(req, stored); ok {
		t.Fatalf("Get(since = producedAt as status stores it, %v) served the consumed sample produced at %v again", stored, producedAt)
	}
}

// TestSampler_PlanEditIsolation: keys differing only in PlanHash are fully
// isolated — a Get under the edited plan neither dedups against the old plan's
// in-flight query nor consumes its (late) result, so a stale completion can only
// ever be read under the plan that produced it.
func TestSampler_PlanEditIsolation(t *testing.T) {
	events := make(chan event.GenericEvent, 4)
	releaseOld := make(chan struct{})
	var calls int32
	s := NewSampler(func(_ context.Context, req SampleRequest) analysis.Result {
		atomic.AddInt32(&calls, 1)
		if req.Key.PlanHash == "old" {
			<-releaseOld // the old plan's query is slow; it lands after the edit
			return analysis.Result{Outcome: analysis.Pass}
		}
		return analysis.Result{Outcome: analysis.Fail}
	}, events, 4, time.Minute)

	oldKey := SampleKey{ISVCName: "svc", Revision: "rev", Step: 1, PlanHash: "old"}
	newKey := oldKey
	newKey.PlanHash = "new"

	if _, _, ok := s.Get(SampleRequest{Key: oldKey}, time.Time{}); ok {
		t.Fatal("first Get should miss")
	}
	// The plan is edited while the old query is in flight: the new key must kick
	// its OWN query, not dedup against the old one.
	if _, _, ok := s.Get(SampleRequest{Key: newKey}, time.Time{}); ok {
		t.Fatal("edited-plan Get should miss, not consume the old plan's state")
	}
	<-events // the new plan's query (not blocked) lands first
	res, _, ok := s.Get(SampleRequest{Key: newKey}, time.Time{})
	if !ok || res.Outcome != analysis.Fail {
		t.Fatalf("edited plan should see its own result (Fail), got ok=%v outcome=%v", ok, res.Outcome)
	}

	close(releaseOld)
	<-events // the old plan's late completion lands under the old key only
	if res, _, ok := s.Get(SampleRequest{Key: newKey}, time.Time{}); !ok || res.Outcome != analysis.Fail {
		t.Fatalf("stale old-plan Pass must not surface under the edited plan, got ok=%v outcome=%v", ok, res.Outcome)
	}
	if res, _, ok := s.Get(SampleRequest{Key: oldKey}, time.Time{}); !ok || res.Outcome != analysis.Pass {
		t.Fatalf("old plan's result stays under its own key, got ok=%v outcome=%v", ok, res.Outcome)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected exactly one query per plan (2 total), got %d", got)
	}
}

// TestSampler_EvictsStale: a cached entry older than the TTL is evicted on access,
// so a subsequent Get re-queries.
func TestSampler_EvictsStale(t *testing.T) {
	events := make(chan event.GenericEvent, 4)
	var calls int32
	s := NewSampler(func(context.Context, SampleRequest) analysis.Result {
		atomic.AddInt32(&calls, 1)
		return analysis.Result{Outcome: analysis.Pass}
	}, events, 4, time.Minute)
	t0 := time.Unix(1000, 0)
	s.now = func() time.Time { return t0 }

	req := SampleRequest{Key: SampleKey{ISVCName: "svc"}}
	s.Get(req, time.Time{})
	<-events // entry at t0

	s.now = func() time.Time { return t0.Add(2 * time.Minute) } // past the 1m TTL
	if _, _, ok := s.Get(req, time.Time{}); ok {
		t.Fatal("stale entry must be evicted, so Get misses")
	}
	<-events // the miss kicked a fresh query
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected 2 evals (initial + after evict), got %d", got)
	}
}
