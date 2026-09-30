package placement

import (
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// DispatcherMode selects how many of the matched candidates to race at once.
// Config-driven; an empty/unrecognized mode degrades to AllAtOnce, so absent
// config preserves the historical fleet-wide fan-out.
type DispatcherMode string

const (
	// DispatcherModeAllAtOnce clones onto every matched candidate at once; the
	// fastest to admit wins. The historical default.
	DispatcherModeAllAtOnce DispatcherMode = "AllAtOnce"

	// DispatcherModeIncremental clones onto candidates in sorted batches (step per
	// round, bounded by a round timeout), widening only if none admits — trades
	// latency for a smaller blast radius on large fleets.
	DispatcherModeIncremental DispatcherMode = "Incremental"
)

// Dispatcher turns the matched candidate set for one source ISVC into the subset
// to nominate on this pass. The placement controller persists nominations. It
// returns the nominated subset (a prefix of candidates, in sorted order) and an
// advisory hold: a non-zero requeue delay asking the caller to wait before
// widening (an incremental round still in progress), zero to use the normal poll.
// now is injected so the round clock is testable without sleeping.
type Dispatcher interface {
	Nominate(key types.UID, candidates []string, now time.Time) (nominated []string, hold time.Duration)
}

// dispatcherFor returns the Dispatcher for a configured mode. An empty or
// unrecognized mode falls back to all-at-once (graceful degradation: absent or
// bad config preserves the historical fleet-wide fan-out instead of failing or
// silently switching strategy). stepSize and roundTimeout are only consulted by
// the incremental dispatcher; they are supplied by the caller from config with
// their own absent-config fallbacks, so this constructor adds no magic literals.
func dispatcherFor(mode DispatcherMode, stepSize int, roundTimeout time.Duration) Dispatcher {
	if mode == DispatcherModeIncremental {
		return newIncrementalDispatcher(stepSize, roundTimeout)
	}
	return allAtOnceDispatcher{}
}

// allAtOnceDispatcher nominates every matched candidate in a single round.
type allAtOnceDispatcher struct{}

func (allAtOnceDispatcher) Nominate(_ types.UID, candidates []string, _ time.Time) ([]string, time.Duration) {
	return candidates, 0
}

// incrementalDispatcher widens nominations in sorted batches with a configured
// dwell per batch. Its clock is in-memory; the controller retains accepted
// nominations across restarts through the persisted placement plan.
type incrementalDispatcher struct {
	// stepSize is how many additional candidates are nominated per round. It is
	// config-supplied (resolved by the caller); this type does not invent a
	// default. A non-positive step would make no progress, so Nominate coerces it
	// to advance by at least one candidate per round rather than wedging.
	stepSize int
	// roundTimeout bounds how long one batch is given to produce a winner before
	// the next batch is added. Config-supplied; a non-positive timeout means every
	// pass may advance (no enforced dwell) rather than blocking forever.
	roundTimeout time.Duration

	mu sync.Mutex
	// rounds tracks, per source-ISVC UID, the nominated set so far and when the
	// current (newest) batch was nominated. The clock starts when a batch is
	// added and gates when the next batch may be.
	rounds map[types.UID]*incrementalRound
}

// incrementalRound is the in-memory per-ISVC bookkeeping for the incremental
// walk: the clusters nominated so far and when the latest batch was admitted to
// the set.
type incrementalRound struct {
	nominated  []string
	roundStart time.Time
}

func newIncrementalDispatcher(stepSize int, roundTimeout time.Duration) *incrementalDispatcher {
	return &incrementalDispatcher{
		stepSize:     stepSize,
		roundTimeout: roundTimeout,
		rounds:       make(map[types.UID]*incrementalRound),
	}
}

// Nominate widens the in-memory nominated set one batch per round: the first
// pass nominates the leading batch and starts the round clock; within
// roundTimeout it returns the same set plus the remaining hold; once the round
// elapses it appends the next batch. candidates is assumed sorted, so the
// nominated set is always a prefix of it (deterministic, never un-nominated);
// candidates that left the fleet are dropped from the retained set.
func (d *incrementalDispatcher) Nominate(key types.UID, candidates []string, now time.Time) ([]string, time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(candidates) == 0 {
		// Nothing to nominate; clear any stale round so a later re-match starts
		// fresh rather than inheriting an empty/elapsed window.
		delete(d.rounds, key)
		return nil, 0
	}

	r, found := d.rounds[key]
	if found {
		// Reconcile the retained set against the current candidates: keep only the
		// clusters that are still candidates (a departed cluster must not stay
		// nominated), preserving the established order.
		r.nominated = intersectOrdered(r.nominated, candidates)
	}
	if !found || len(r.nominated) == 0 {
		// First batch (or the retained set emptied because every prior nominee
		// left): nominate the leading batch and start the round clock.
		first := slices.Clone(candidates[:d.batchEnd(0, len(candidates))])
		d.rounds[key] = &incrementalRound{nominated: first, roundStart: now}
		// If the leading batch already covers every candidate there is nothing left
		// to widen to, so impose no hold (consistent with the full-set case below).
		if len(first) >= len(candidates) {
			return slices.Clone(first), 0
		}
		return slices.Clone(first), d.roundTimeout
	}

	// Already nominated everything? Nothing left to widen to; surface the full set
	// with no hold so the caller falls back to its normal poll cadence.
	if len(r.nominated) >= len(candidates) {
		return slices.Clone(r.nominated), 0
	}

	// A batch is in flight. Hold until the round elapses, then add the next batch.
	if remaining := d.roundTimeout - now.Sub(r.roundStart); remaining > 0 {
		return slices.Clone(r.nominated), remaining
	}

	end := d.batchEnd(len(r.nominated), len(candidates))
	r.nominated = slices.Clone(candidates[:end])
	r.roundStart = now
	// If the set is now full there is nothing more to widen to; return no hold.
	if len(r.nominated) >= len(candidates) {
		return slices.Clone(r.nominated), 0
	}
	return slices.Clone(r.nominated), d.roundTimeout
}

// forget drops any in-memory round state for an ISVC. Called when the source is
// deleted (its placement is being torn down) so the map does not retain an entry
// for a gone ISVC. A re-created ISVC gets a fresh UID and so a fresh walk.
func (d *incrementalDispatcher) forget(key types.UID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.rounds, key)
}

// batchEnd returns the exclusive upper bound of the nominated prefix after
// growing from have to have+step, clamped to total. step is coerced to at least
// one so a misconfigured non-positive step still makes forward progress (one
// candidate per round) instead of stalling the walk.
func (d *incrementalDispatcher) batchEnd(have, total int) int {
	step := d.stepSize
	if step < 1 {
		step = 1
	}
	end := have + step
	if end > total {
		end = total
	}
	return end
}

// intersectOrdered returns the elements of keep that are still present in the
// allowed set, preserving keep's order. Used to drop departed clusters from the
// retained nominated set without reordering the survivors.
func intersectOrdered(keep, allowed []string) []string {
	if len(keep) == 0 {
		return keep
	}
	allow := make(map[string]struct{}, len(allowed))
	for _, a := range allowed {
		allow[a] = struct{}{}
	}
	out := keep[:0:0]
	for _, k := range keep {
		if _, ok := allow[k]; ok {
			out = append(out, k)
		}
	}
	return out
}

// forgetRoundState is the optional capability a stateful dispatcher exposes so
// the reconciler can drop per-ISVC bookkeeping on teardown. The all-at-once
// dispatcher is stateless and does not implement it; the incremental one does.
type forgetRoundState interface {
	forget(key types.UID)
}
