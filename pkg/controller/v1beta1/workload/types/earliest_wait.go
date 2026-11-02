package types

import "time"

// EarliestPositive folds candidate into a running earliest wait: the
// smallest positive duration wins, and a candidate of zero or less asks
// for nothing, so current stands. Every place that merges several waits
// into one requeue uses this rule.
func EarliestPositive(current, candidate time.Duration) time.Duration {
	if candidate <= 0 {
		return current
	}
	if current <= 0 || candidate < current {
		return candidate
	}
	return current
}

// earliestWait is the memory a pass-scoped sink embeds: the earliest
// positive wait deposited this pass, zero when none was. The sinks keep
// their own nil-safe methods and fold each deposit through observe.
type earliestWait struct {
	pending time.Duration
}

func (w *earliestWait) observe(wait time.Duration) {
	w.pending = EarliestPositive(w.pending, wait)
}
