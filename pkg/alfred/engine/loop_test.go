package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

type stubSource struct{ snap *snapshot.ClusterSnapshot }

func (s *stubSource) Latest() *snapshot.ClusterSnapshot { return s.snap }

type stubPolicy struct {
	calls int64
	out   []policy.Candidate
}

func (p *stubPolicy) Name() string { return "stub" }
func (p *stubPolicy) Evaluate(*snapshot.ClusterSnapshot, *config.Config) []policy.Candidate {
	atomic.AddInt64(&p.calls, 1)
	return p.out
}

func newTestLoop(t *testing.T, snap *snapshot.ClusterSnapshot, p policy.Policy) (*DecisionLoop, *Reporter, chan struct{}) {
	t.Helper()
	reporter, m, _, _ := newTestReporter(t, recommendationsCM(nil))
	early := make(chan struct{}, 1)
	loop := &DecisionLoop{
		Snapshots: &stubSource{snap: snap},
		Store:     config.NewStore(),
		Policies:  []policy.Policy{p},
		Arbiter:   &Arbiter{Ledger: NewLedger()},
		Reporter:  reporter,
		Metrics:   m,
		Log:       logr.Discard(),
		EarlyTick: early,
		Now:       func() time.Time { return testNow },
	}
	return loop, reporter, early
}

func TestDecisionLoopIsLeaderOnly(t *testing.T) {
	loop, _, _ := newTestLoop(t, nil, &stubPolicy{})
	if !loop.NeedLeaderElection() {
		t.Fatal("exactly one replica may decide and act")
	}
}

func TestRunOncePipeline(t *testing.T) {
	snap := scenario().Build()
	executable := cand("prod/a", "node1")
	advisory := cand("prod/b", "node3")
	advisory.Executable = false
	advisory.AdvisoryReason = policy.AdvisoryNoSurgeHeadroom
	p := &stubPolicy{out: []policy.Candidate{executable, advisory}}
	loop, reporter, _ := newTestLoop(t, snap, p)

	loop.RunOnce(context.Background())

	if got := atomic.LoadInt64(&p.calls); got != 1 {
		t.Fatalf("policy evaluated %d times, want 1", got)
	}
	m := reporter.Metrics
	if got := promtestutil.ToFloat64(m.RecommendationsAccepted.WithLabelValues("defragmentation", "prod/a", "engine")); got != 1 {
		t.Fatalf("admitted candidate not reported: %v", got)
	}
	if got := promtestutil.ToFloat64(m.RecommendationsProduced.WithLabelValues("defragmentation", "prod/b", "engine", policy.ReasonFragmentation, "false")); got != 1 {
		t.Fatalf("advisory bypass not reported: %v", got)
	}
	if got := promtestutil.ToFloat64(m.CircuitBreakerState); got != 0 {
		t.Fatalf("breaker gauge = %v, want 0", got)
	}
	if got := promtestutil.CollectAndCount(m.DecisionLoopDuration); got != 1 {
		t.Fatalf("decision duration histogram families = %d", got)
	}
}

func TestRunOnceReportsStructuredOMENativeExecutorState(t *testing.T) {
	snap := scenario().Build()
	snap.OMENativeExecutor.Available = true
	loop, reporter, _ := newTestLoop(t, snap, &stubPolicy{})

	loop.RunOnce(context.Background())

	if reporter.omenativeDegraded {
		t.Fatal("available structured state must not report OMENative degraded")
	}
}

func TestRunOnceWithoutSnapshotSkips(t *testing.T) {
	p := &stubPolicy{}
	loop, _, _ := newTestLoop(t, nil, p)
	loop.RunOnce(context.Background())
	if atomic.LoadInt64(&p.calls) != 0 {
		t.Fatal("no snapshot means no evaluation")
	}
}

func TestBreakerGaugeFollowsLedger(t *testing.T) {
	snap := scenario().Build()
	loop, reporter, _ := newTestLoop(t, snap, &stubPolicy{})
	for i := 0; i < 4; i++ {
		loop.Arbiter.Ledger.RecordOutcome(false, testNow.Add(-time.Minute))
	}
	loop.RunOnce(context.Background())
	if got := promtestutil.ToFloat64(reporter.Metrics.CircuitBreakerState); got != 1 {
		t.Fatalf("breaker gauge = %v, want 1 while open", got)
	}
}

// TestStartEarlyTickAdvances: with a 5-minute interval, only the early-tick
// signal can trigger the second pass inside the test timeout.
func TestStartEarlyTickAdvances(t *testing.T) {
	p := &stubPolicy{}
	loop, _, early := newTestLoop(t, scenario().Build(), p)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Start(ctx) }()

	waitFor := func(passes int64) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for atomic.LoadInt64(&p.calls) < passes {
			select {
			case <-deadline:
				t.Fatalf("loop stuck at %d passes, want %d", atomic.LoadInt64(&p.calls), passes)
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
	waitFor(1) // immediate first pass
	early <- struct{}{}
	waitFor(2) // advanced by the early tick, not the 5m timer

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error on shutdown: %v", err)
	}
}

func TestEarlyTickerObserve(t *testing.T) {
	node := func(ready corev1.ConditionStatus, extra ...corev1.NodeCondition) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node1"},
			Status: corev1.NodeStatus{Conditions: append([]corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: ready},
			}, extra...)},
		}
	}
	ticker := &EarlyTicker{
		Store: config.NewStore(), // default earlyTickOn: [NodeConditionChange]
		Log:   logr.Discard(),
		C:     make(chan struct{}, 1),
	}

	// A status flip signals.
	ticker.observe(node(corev1.ConditionTrue), node(corev1.ConditionFalse))
	select {
	case <-ticker.C:
	default:
		t.Fatal("a condition flip must advance the tick")
	}

	// A heartbeat-only update does not.
	ticker.observe(node(corev1.ConditionTrue), node(corev1.ConditionTrue))
	select {
	case <-ticker.C:
		t.Fatal("heartbeat updates must not advance the tick")
	default:
	}

	// A new condition appearing signals; a full channel never blocks.
	bad := corev1.NodeCondition{Type: "GpuUnhealthy", Status: corev1.ConditionTrue}
	ticker.observe(node(corev1.ConditionTrue), node(corev1.ConditionTrue, bad))
	ticker.observe(node(corev1.ConditionTrue), node(corev1.ConditionTrue, bad, corev1.NodeCondition{Type: "X", Status: corev1.ConditionTrue}))
	if len(ticker.C) != 1 {
		t.Fatalf("signals must collapse into the buffered slot, got %d", len(ticker.C))
	}
	<-ticker.C

	// Disabled trigger stays silent.
	if _, err := ticker.Store.Update([]byte("schemaVersion: 1\nearlyTickOn: []")); err != nil {
		t.Fatal(err)
	}
	ticker.observe(node(corev1.ConditionTrue), node(corev1.ConditionFalse))
	select {
	case <-ticker.C:
		t.Fatal("a disabled trigger must not signal")
	default:
	}
}
