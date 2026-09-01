package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/utils/clock"
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

// snapshotRefresher is kept internal so Latest-only SnapshotSource
// implementations remain source-compatible. Early decisions fail closed when
// the configured source cannot also coordinate a refresh.
type snapshotRefresher interface {
	Refresh(context.Context) error
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

	// EarlyTick requests a supplemental pass when a subscribed event (a node
	// condition change) arrives; it never interrupts a running pass or resets
	// the regular timer. The channel must be buffered (capacity 1): a signal
	// landing mid-pass waits there and fires the next select immediately.
	EarlyTick <-chan struct{}

	// timerClock drives the regular decision timer. Nil uses the real clock.
	timerClock clock.Clock

	// Now overrides the clock in tests.
	Now func() time.Time
}

var _ manager.Runnable = &DecisionLoop{}
var _ manager.LeaderElectionRunnable = &DecisionLoop{}

// NeedLeaderElection returns true: exactly one replica decides and acts.
func (l *DecisionLoop) NeedLeaderElection() bool { return true }

// Start runs an immediate first pass, then regular passes at
// decisionLoopInterval. Early signals add fresh, serialized passes without
// moving the current regular deadline. The interval is reloaded only when a
// regular deadline is consumed, including a deadline coincident with an early
// signal.
func (l *DecisionLoop) Start(ctx context.Context) error {
	l.RunOnce(ctx)
	timer := l.decisionClock().NewTimer(l.Store.Get().DecisionLoopInterval.Duration)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
			// If an early signal is already pending at the regular deadline,
			// fold both into one fresh pass. A failed refresh skips that pass,
			// but the consumed regular deadline still resets normal cadence.
			if l.takeEarlyTick() {
				l.runFreshDecisionLogged(ctx)
			} else {
				l.RunOnce(ctx)
			}
			timer.Reset(l.Store.Get().DecisionLoopInterval.Duration)
		case <-l.EarlyTick:
			// The timer may have become ready at the same instant as the early
			// signal. Drain it if so and count this as the regular pass; otherwise
			// leave the still-running timer completely untouched.
			regularDue := false
			select {
			case <-timer.C():
				regularDue = true
			default:
			}
			l.runFreshDecisionLogged(ctx)
			if regularDue {
				timer.Reset(l.Store.Get().DecisionLoopInterval.Duration)
			}
		}
	}
}

func (l *DecisionLoop) takeEarlyTick() bool {
	select {
	case <-l.EarlyTick:
		return true
	default:
		return false
	}
}

func (l *DecisionLoop) runFreshDecisionLogged(ctx context.Context) {
	if err := l.runFreshDecision(ctx); err != nil {
		l.Log.Error(err, "snapshot refresh failed; skipping decision pass")
	}
}

func (l *DecisionLoop) runFreshDecision(ctx context.Context) error {
	refresher, ok := l.Snapshots.(snapshotRefresher)
	if !ok {
		return fmt.Errorf("snapshot source does not support refresh")
	}
	if err := refresher.Refresh(ctx); err != nil {
		return fmt.Errorf("refresh snapshot: %w", err)
	}
	l.RunOnce(ctx)
	return nil
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

func (l *DecisionLoop) decisionClock() clock.Clock {
	if l.timerClock == nil {
		return clock.RealClock{}
	}
	return l.timerClock
}
