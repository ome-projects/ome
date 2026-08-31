package engine

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// SnapshotSource yields the freshest observation-loop snapshot; nil before
// the first pass. observer.Loop implements it.
type SnapshotSource interface {
	Latest() *snapshot.ClusterSnapshot
}

// DecisionLoop is the leader-only decision Runnable: policies → arbiter →
// reporter (→ dispatcher, in the execute path) on the configured cadence.
// One pass at a time by construction — a single goroutine runs RunOnce, so
// an overrunning pass delays the next tick and never overlaps it: every
// safety bound assumes admissions are totally ordered.
type DecisionLoop struct {
	Snapshots SnapshotSource
	Store     *config.Store
	Policies  []policy.Policy
	Arbiter   *Arbiter
	Reporter  *Reporter
	Metrics   *metrics.Metrics
	Log       logr.Logger

	// EarlyTick advances the next tick when a subscribed event (a node
	// condition change) arrives; it never interrupts a running pass. The
	// channel must be buffered (capacity 1): a signal landing mid-pass
	// waits there and fires the next select immediately.
	EarlyTick <-chan struct{}

	// Now overrides the clock in tests.
	Now func() time.Time
}

var _ manager.Runnable = &DecisionLoop{}
var _ manager.LeaderElectionRunnable = &DecisionLoop{}

// NeedLeaderElection returns true: exactly one replica decides and acts.
func (l *DecisionLoop) NeedLeaderElection() bool { return true }

// Start runs an immediate first pass, then ticks at decisionLoopInterval,
// re-reading the interval every pass so a config reload takes effect without
// a restart.
func (l *DecisionLoop) Start(ctx context.Context) error {
	l.RunOnce(ctx)
	for {
		timer := time.NewTimer(l.Store.Get().DecisionLoopInterval.Duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		case <-l.EarlyTick:
			timer.Stop()
		}
		l.RunOnce(ctx)
	}
}

// RunOnce executes one decision pass against the latest snapshot. Exported
// for tests; Start is its only production caller.
func (l *DecisionLoop) RunOnce(ctx context.Context) {
	started := l.now()
	snap := l.Snapshots.Latest()
	if snap == nil {
		l.Log.V(1).Info("no snapshot yet; skipping decision pass")
		return
	}
	cfg := l.Store.Get()

	l.Reporter.ReportOMENativeState(snap.OMENativeExecutor.Available)

	var candidates []policy.Candidate
	for _, p := range l.Policies {
		candidates = append(candidates, p.Evaluate(snap, cfg)...)
	}
	decisions := l.Arbiter.Admit(snap, candidates, cfg, l.now())

	if l.Arbiter.Ledger != nil && l.Arbiter.Ledger.BreakerOpen(l.now()) {
		l.Metrics.CircuitBreakerState.Set(1)
	} else {
		l.Metrics.CircuitBreakerState.Set(0)
	}

	// The Dispatcher joins in the execute path (with the VAP guard); until
	// then every admitted candidate is withheld and the Reporter says so.
	l.Reporter.ReportCycle(ctx, candidates, decisions, cfg, l.now())

	l.Metrics.DecisionLoopDuration.Observe(l.now().Sub(started).Seconds())
}

func (l *DecisionLoop) now() time.Time {
	if l.Now == nil {
		return time.Now()
	}
	return l.Now()
}
