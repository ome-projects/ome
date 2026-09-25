package types

import "time"

// PromoteWindow is the pass-scoped sink for the minReadySeconds availability
// window a promote is still waiting out: the earliest instant at which some
// Instance's pod set becomes promotable on its own, with no watch event to
// announce it. It exists because the op state machines report progress as
// (done, error) and have no result of their own, so the remainder is
// deposited here and folded into the pass's requeue by the dispatcher — a
// waiting path then wakes when the window elapses instead of polling it.
// All methods are nil-safe.
type PromoteWindow struct {
	pending time.Duration
}

// Observe records how much longer a pod set must stay Ready before it can be
// promoted, keeping the earliest such instant seen this pass: the first
// Instance to cross its window is the first thing the next pass can commit.
func (w *PromoteWindow) Observe(remaining time.Duration) {
	if w == nil || remaining <= 0 {
		return
	}
	if w.pending == 0 || remaining < w.pending {
		w.pending = remaining
	}
}

// Pending returns the wake-up the pass owes a promote that is only waiting
// out its window, or zero when nothing is waiting.
func (w *PromoteWindow) Pending() time.Duration {
	if w == nil {
		return 0
	}
	return w.pending
}
