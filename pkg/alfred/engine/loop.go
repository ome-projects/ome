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

// MigrationDispatcher reconciles requests and gates new submissions. It also
// returns outstanding request observations that no policy currently emits.
type MigrationDispatcher interface {
	Execute(context.Context, *snapshot.ClusterSnapshot, []policy.Candidate, *config.Config, *Arbiter) ([]policy.Candidate, []Decision)
}

// DecisionLoop is the leader-only decision Runnable: policies → optional
// dispatch and arbitration → reporter on the configured cadence.
// One pass at a time by construction — a single goroutine runs RunOnce, so
// an overrunning pass delays the next tick and never overlaps it: every
// safety bound assumes admissions are totally ordered.
type DecisionLoop struct {
	Snapshots SnapshotSource
	Store     *config.Store
	Policies  []policy.Policy
	// Predictions is optional and can only enrich non-executable advice.
	Predictions *PredictionStage
	Dispatcher  MigrationDispatcher
	Arbiter     *Arbiter
	Reporter    *Reporter
	Metrics     *metrics.Metrics
	Log         logr.Logger

	// EarlyTick requests a fresh supplemental pass without postponing the
	// regular deadline or interrupting a running pass. The channel has capacity
	// one so event storms coalesce.
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
// moving the current regular deadline. The interval is reloaded when a
// regular deadline is consumed, including a deadline coincident with an early
// signal, and when the active configuration changes: the pending deadline is
// then recomputed under the new interval from the point the current one was
// armed, running at once if that instant has already passed. A leader elected
// before the ConfigMap is applied thus follows the configured cadence as soon
// as it lands instead of waiting out the built-in default.
func (l *DecisionLoop) Start(ctx context.Context) error {
	l.RunOnce(ctx)
	// Subscribe before reading the interval so an update landing between the
	// two still wakes the loop.
	changed := l.Store.Changed()
	clk := l.decisionClock()
	interval := l.Store.Get().DecisionLoopInterval.Duration
	armedAt := clk.Now()
	timer := clk.NewTimer(interval)
	defer timer.Stop()
	// resetCadence arms the next regular deadline one freshly loaded interval
	// after the consumed one.
	resetCadence := func() {
		interval = l.Store.Get().DecisionLoopInterval.Duration
		armedAt = clk.Now()
		timer.Reset(interval)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
			l.runRegularPass(ctx)
			resetCadence()
		case <-l.EarlyTick:
			// The timer may become ready either just before the early signal or
			// while its refresh is in flight. Drain it in either case and count the
			// fresh attempt as the regular pass; otherwise leave the still-running
			// timer completely untouched.
			regularDue := false
			select {
			case <-timer.C():
				regularDue = true
			default:
			}
			l.runFreshDecisionLogged(ctx)
			if !regularDue {
				select {
				case <-timer.C():
					regularDue = true
				default:
				}
			}
			if regularDue {
				resetCadence()
			}
		case <-changed:
			changed = l.Store.Changed()
			next := l.Store.Get().DecisionLoopInterval.Duration
			if next == interval {
				continue
			}
			// Move the pending deadline to where the new interval places it
			// from the point the current one was armed. A deadline that has
			// already expired under the old interval is superseded by the
			// recomputed one.
			interval = next
			select {
			case <-timer.C():
			default:
			}
			if wait := armedAt.Add(interval).Sub(clk.Now()); wait > 0 {
				timer.Reset(wait)
			} else {
				l.runRegularPass(ctx)
				resetCadence()
			}
		}
	}
}

// runRegularPass runs the pass owed at a regular deadline. An early signal
// already pending is folded into one fresh pass; a failed refresh skips that
// pass, but the consumed deadline still resets normal cadence.
func (l *DecisionLoop) runRegularPass(ctx context.Context) {
	if l.takeEarlyTick() {
		l.runFreshDecisionLogged(ctx)
	} else {
		l.RunOnce(ctx)
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
	if l.Dispatcher == nil || cfg.Mode != config.ModeExecute {
		if l.Predictions != nil {
			candidates = l.Predictions.Annotate(ctx, snap, cfg, candidates)
		} else {
			for i, candidate := range candidates {
				candidates[i] = gateSchedulingCandidate(snap, cfg, candidate)
			}
		}
	}
	var decisions []Decision
	if l.Dispatcher != nil {
		// Execute also reconciles outstanding requests in recommend-only
		// mode, without submitting annotations in that mode.
		candidates, decisions = l.Dispatcher.Execute(ctx, snap, candidates, cfg, l.Arbiter)
	} else {
		decisions = l.Arbiter.Admit(snap, candidates, cfg, l.now())
	}

	if l.Arbiter.Ledger != nil && l.Arbiter.Ledger.BreakerOpen(l.now()) {
		l.Metrics.CircuitBreakerState.Set(1)
	} else {
		l.Metrics.CircuitBreakerState.Set(0)
	}

	l.Reporter.ReportCycle(ctx, candidates, decisions, cfg, l.now(), snap.Timestamp)

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
