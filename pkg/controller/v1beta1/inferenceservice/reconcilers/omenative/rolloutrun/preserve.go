package rolloutrun

import (
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// PreserveNewerRun guards the run state across re-based status writes: when
// a conflict retry copies an in-memory status onto a fresher live object, a
// writer working from a stale cache would otherwise roll the recorded plan
// back to a predecessor — and a rolled-back plan record is unrecoverable, because
// the repin verb that produced it was one-shot and already consumed. Every
// run boundary advances the state's monotonic clock (OpenedAt, PinnedAt,
// LastRun.ClosedAt), so "newer" is well-defined: when the live side is
// strictly newer, its ActiveRun/LastRun pair is kept, along with an active
// pre-step hold (the clamp that stops a step after a plan reread from raising traffic —
// losing that bit would program the exposure the hold exists to block).
// Ties keep dst: equal clocks mean the same boundary, and dst carries the
// current pass's engine progress.
func PreserveNewerRun(dst *v1beta1.InferenceServiceStatus, liveRollout *v1beta1.RolloutStatus, livePreStepHold bool) {
	if liveRollout == nil {
		return
	}
	var dstRollout *v1beta1.RolloutStatus
	if dst != nil {
		dstRollout = dst.Rollout
	}
	if !runClock(liveRollout).After(runClock(dstRollout)) {
		return
	}
	if dst.Rollout == nil {
		dst.Rollout = &v1beta1.RolloutStatus{}
	}
	dst.Rollout.ActiveRun = liveRollout.ActiveRun
	dst.Rollout.LastRun = liveRollout.LastRun
	if livePreStepHold && dst.Canary != nil {
		dst.Canary.PreStepHold = true
	}
}

// runClock is the run state's monotonic timestamp: the latest boundary it
// records. Zero when no run state exists.
func runClock(rs *v1beta1.RolloutStatus) time.Time {
	var t time.Time
	if rs == nil {
		return t
	}
	if rs.LastRun != nil && rs.LastRun.ClosedAt != nil && rs.LastRun.ClosedAt.Time.After(t) {
		t = rs.LastRun.ClosedAt.Time
	}
	if rs.ActiveRun != nil {
		if rs.ActiveRun.OpenedAt.Time.After(t) {
			t = rs.ActiveRun.OpenedAt.Time
		}
		if rs.ActiveRun.PinnedAt.Time.After(t) {
			t = rs.ActiveRun.PinnedAt.Time
		}
	}
	return t
}
