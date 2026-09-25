package types

import "time"

// PassWake is the pass-scoped sink for a wake-up an op pass owes but has
// no result of its own to carry: a row held on operator configuration
// that no watch event will announce — nothing in the cluster changes
// when a ConfigMap key finally appears. The op deposits the wake-up here
// and the dispatcher merges it into the pass result, so the hold is
// re-consulted instead of waiting for an unrelated event or the resync.
// All methods are nil-safe.
type PassWake struct {
	pending time.Duration
	bare    bool
}

// Observe records that the pass must come back, after the given interval
// when the operator configured one — keeping the earliest seen this pass
// — or on the controller's rate-limited backoff when it did not.
func (w *PassWake) Observe(after time.Duration) {
	if w == nil {
		return
	}
	if after <= 0 {
		w.bare = true
		return
	}
	if w.pending == 0 || after < w.pending {
		w.pending = after
	}
}

// Pending returns the earliest configured interval deposited this pass,
// or zero when none was. It competes with the pass's other explicit
// waits under the earliest-wins rule.
func (w *PassWake) Pending() time.Duration {
	if w == nil {
		return 0
	}
	return w.pending
}

// Bare reports that a pass deposited an unconfigured wake-up, which asks
// for the rate-limited backoff. It applies only when the result carries
// no explicit wait at all: an explicit wait already beats the backoff.
func (w *PassWake) Bare() bool {
	return w != nil && w.bare
}
