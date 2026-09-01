package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

type stubSource struct {
	snap      *snapshot.ClusterSnapshot
	refresh   func(context.Context) error
	onLatest  func()
	refreshed atomic.Int64
}

type latestOnlySource struct{ snap *snapshot.ClusterSnapshot }

func (s *latestOnlySource) Latest() *snapshot.ClusterSnapshot { return s.snap }

func (s *stubSource) Latest() *snapshot.ClusterSnapshot {
	if s.onLatest != nil {
		s.onLatest()
	}
	return s.snap
}

func (s *stubSource) Refresh(ctx context.Context) error {
	s.refreshed.Add(1)
	if s.refresh == nil {
		return nil
	}
	return s.refresh(ctx)
}

type stubPolicy struct {
	calls      int64
	out        []policy.Candidate
	onEvaluate func()
}

func (p *stubPolicy) Name() string { return "stub" }
func (p *stubPolicy) Evaluate(*snapshot.ClusterSnapshot, *config.Config) []policy.Candidate {
	atomic.AddInt64(&p.calls, 1)
	if p.onEvaluate != nil {
		p.onEvaluate()
	}
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

func TestFreshDecisionOrdersRefreshLatestEvaluate(t *testing.T) {
	steps := make(chan string, 3)
	source := &stubSource{
		snap: scenario().Build(),
		refresh: func(context.Context) error {
			steps <- "refresh"
			return nil
		},
		onLatest: func() { steps <- "latest" },
	}
	p := &stubPolicy{onEvaluate: func() { steps <- "evaluate" }}
	loop, _, _ := newTestLoop(t, source.snap, p)
	loop.Snapshots = source

	if err := loop.runFreshDecision(context.Background()); err != nil {
		t.Fatalf("runFreshDecision() error = %v", err)
	}

	for i, want := range []string{"refresh", "latest", "evaluate"} {
		select {
		case got := <-steps:
			if got != want {
				t.Fatalf("step %d = %q, want %q", i, got, want)
			}
		default:
			t.Fatalf("missing step %d (%s)", i, want)
		}
	}
}

func TestFreshDecisionRefreshFailureSkipsEvaluationAndReporting(t *testing.T) {
	wantErr := errors.New("snapshot refresh failed")
	source := &stubSource{
		snap:    scenario().Build(),
		refresh: func(context.Context) error { return wantErr },
	}
	p := &stubPolicy{}
	loop, reporter, _ := newTestLoop(t, source.snap, p)
	loop.Snapshots = source

	err := loop.runFreshDecision(context.Background())

	if !errors.Is(err, wantErr) {
		t.Fatalf("runFreshDecision() error = %v, want %v", err, wantErr)
	}
	if got := atomic.LoadInt64(&p.calls); got != 0 {
		t.Fatalf("policy evaluations = %d, want 0", got)
	}
	if reporter.omenativeSeeded {
		t.Fatal("refresh failure reached reporter")
	}
}

func TestFreshDecisionLatestOnlySourceFailsClosed(t *testing.T) {
	p := &stubPolicy{}
	loop, reporter, _ := newTestLoop(t, scenario().Build(), p)
	loop.Snapshots = &latestOnlySource{snap: scenario().Build()}

	if err := loop.runFreshDecision(context.Background()); err == nil {
		t.Fatal("runFreshDecision() error = nil, want unsupported refresh error")
	}
	if got := atomic.LoadInt64(&p.calls); got != 0 {
		t.Fatalf("policy evaluations = %d, want 0", got)
	}
	if reporter.omenativeSeeded {
		t.Fatal("missing refresh support reached reporter")
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

// TestStartEarlyTickRunsSupplementalPass verifies that, with a five-minute
// interval, the early signal can request a second pass inside the test timeout.
func TestStartEarlyTickRunsSupplementalPass(t *testing.T) {
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
	waitFor(2) // requested by the early signal, not the 5m timer

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error on shutdown: %v", err)
	}
}

func TestStartEarlyTickDoesNotResetRegularCadence(t *testing.T) {
	p := &stubPolicy{}
	loop, _, early := newTestLoop(t, scenario().Build(), p)
	source := loop.Snapshots.(*stubSource)
	fakeClock := clocktesting.NewFakeClock(testNow)
	loop.timerClock = fakeClock
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Start(ctx) }()

	waitForPolicyCalls(t, p, 1)
	waitForFakeTimer(t, fakeClock)
	fakeClock.Step(2 * time.Minute)
	early <- struct{}{}
	waitForPolicyCalls(t, p, 2)
	if got := source.refreshed.Load(); got != 1 {
		t.Fatalf("early refreshes = %d, want 1", got)
	}

	// The original five-minute deadline remains at t=5m. Resetting it in the
	// early branch would postpone this pass until t=7m.
	fakeClock.Step(3 * time.Minute)
	waitForPolicyCalls(t, p, 3)

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error on shutdown: %v", err)
	}
}

func TestStartRegularTickReloadsInterval(t *testing.T) {
	p := &stubPolicy{}
	loop, _, _ := newTestLoop(t, scenario().Build(), p)
	fakeClock := clocktesting.NewFakeClock(testNow)
	loop.timerClock = fakeClock
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Start(ctx) }()

	waitForPolicyCalls(t, p, 1)
	waitForFakeTimer(t, fakeClock)
	if _, err := loop.Store.Update([]byte("schemaVersion: 1\ndecisionLoopInterval: 1m")); err != nil {
		t.Fatal(err)
	}
	fakeClock.Step(5 * time.Minute)
	waitForPolicyCalls(t, p, 2)
	waitForFakeTimer(t, fakeClock)
	fakeClock.Step(time.Minute)
	waitForPolicyCalls(t, p, 3)

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error on shutdown: %v", err)
	}
}

func TestStartCoincidentRegularAndEarlyTickRefreshesOnce(t *testing.T) {
	p := &stubPolicy{}
	loop, _, early := newTestLoop(t, scenario().Build(), p)
	fakeClock := clocktesting.NewFakeClock(testNow)
	loop.timerClock = fakeClock
	wantErr := errors.New("coincident refresh failed")
	firstRefreshEntered := make(chan struct{})
	releaseFirstRefresh := make(chan struct{})
	secondRefresh := make(chan struct{})
	source := loop.Snapshots.(*stubSource)
	source.refresh = func(ctx context.Context) error {
		switch source.refreshed.Load() {
		case 1:
			close(firstRefreshEntered)
			select {
			case <-releaseFirstRefresh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case 2:
			close(secondRefresh)
			return wantErr
		default:
			return nil
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- loop.Start(ctx) }()

	waitForPolicyCalls(t, p, 1)
	waitForFakeTimer(t, fakeClock)
	early <- struct{}{}
	select {
	case <-firstRefreshEntered:
	case <-time.After(time.Second):
		t.Fatal("first early refresh did not start")
	}

	// Hold the first early pass so both the regular deadline and another
	// coalesced early signal are pending when the loop returns to select.
	early <- struct{}{}
	fakeClock.Step(5 * time.Minute)
	close(releaseFirstRefresh)
	waitForPolicyCalls(t, p, 2)
	select {
	case <-secondRefresh:
	case <-time.After(time.Second):
		t.Fatal("coincident tick did not refresh")
	}
	if got := atomic.LoadInt64(&p.calls); got != 2 {
		t.Fatalf("refresh failure allowed coincident decision: calls = %d, want 2", got)
	}

	// The failed coincident pass still consumed the regular deadline and reset
	// the normal cadence. It must not immediately run a stale regular decision.
	waitForFakeTimer(t, fakeClock)
	fakeClock.Step(5 * time.Minute)
	waitForPolicyCalls(t, p, 3)

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
		t.Fatal("a condition flip must request a supplemental pass")
	}

	// A heartbeat-only update does not.
	ticker.observe(node(corev1.ConditionTrue), node(corev1.ConditionTrue))
	select {
	case <-ticker.C:
		t.Fatal("heartbeat updates must not request a supplemental pass")
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

func waitForPolicyCalls(t *testing.T, p *stubPolicy, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt64(&p.calls) < want {
		if time.Now().After(deadline) {
			t.Fatalf("policy calls = %d, want at least %d", atomic.LoadInt64(&p.calls), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForFakeTimer(t *testing.T, fakeClock *clocktesting.FakeClock) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !fakeClock.HasWaiters() {
		if time.Now().After(deadline) {
			t.Fatal("decision loop did not arm its regular timer")
		}
		time.Sleep(time.Millisecond)
	}
}
