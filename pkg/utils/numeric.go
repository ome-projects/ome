package utils

// DerefInt32 returns *p for a non-nil pointer and 0 for nil. Handy for
// event messages and counters where an "<nil>" would be confusing.
func DerefInt32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

// ClampInt32 returns v held inside [lo, hi]; the upper bound is applied last,
// so hi wins when lo > hi.
func ClampInt32(v, lo, hi int32) int32 {
	if v < lo {
		v = lo
	}
	if v > hi {
		v = hi
	}
	return v
}
