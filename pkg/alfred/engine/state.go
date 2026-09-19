// Package engine is Alfred's decision core: the Arbiter that admits or
// rejects the merged candidate stream under the global safety bounds, and the
// Ledger that carries the little cross-pass state arbitration needs.
// Everything here is pure and
// clock-injected: methods take `now`, hold no client, and re-derive whatever
// they can from the snapshot so leader failover degrades safety margins,
// never correctness.
package engine

import (
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// Breaker parameters (OEP-fixed): trip when more than half of the recent
// outcomes failed, pause everything for an hour.
const (
	breakerWindow   = 10
	breakerPause    = 60 * time.Minute
	breakerMinTrips = 4 // outcomes required before the breaker may trip
)

// ledgerRetention bounds how long a dispatch record matters: the hourly cap
// looks back one hour, the node cooldown far less, and the dispatcher's
// abandon timeout for a stalled migration is also one hour.
const ledgerRetention = time.Hour

// DispatchRecord is one actuation the engine performed: who moved, from
// where, toward which claimed target, and under which policy class.
type DispatchRecord struct {
	Workload types.NamespacedName
	FromNode string
	// Target is the claimed placement (first feasible hint at admission);
	// empty for an eviction expected to reuse its source.
	Target string
	// GPUs is the replacement footprint the migration will occupy on
	// Target while in flight.
	GPUs int64
	// Health marks a node-health evacuation (class-aware rules).
	Health bool
	At     time.Time

	// absorbed is set once the snapshot reflects the migration's outcome;
	// the record then stops claiming capacity but still counts toward the
	// hourly ledger.
	absorbed bool
}

// Ledger is the Arbiter's cross-pass memory. In-flight state is
// authoritative in the snapshot (request annotations + non-terminal
// MigrationHistory, rebuilt every pass); the ledger adds only what the
// cluster cannot carry: dispatch claims not yet visible in the snapshot, the
// rolling hourly window, the outcome ring, and the breaker clock. On leader
// failover a fresh ledger under-counts toward safety (caps re-derive from
// the snapshot via max) and never resumes stale claims.
type Ledger struct {
	// dispatches holds every actuation of the last hour (ledgerRetention),
	// appended by RecordDispatch after the Dispatcher writes a request and
	// reconciled by AbsorbSnapshot each pass. One list feeds three reads
	// with different lifetimes: the rolling-hour cap counts every record,
	// absorbed or not (DispatchesWithinHour); the per-node cooldown reads
	// target landings and defrag-class sources inside its 10m window
	// (NodeCooling); and the capacity arithmetic counts only records whose
	// migration is still in flight — AbsorbSnapshot marks a record
	// absorbed once its workload shows a newer terminal migration or has
	// left the cluster, releasing the claim (ActiveClaims).
	dispatches []DispatchRecord

	// outcomes is the circuit breaker's evidence: one bool per terminal
	// migration outcome (true = completed), newest last, capped at
	// breakerWindow. RecordOutcome trips the breaker when at least
	// breakerMinTrips outcomes are held and more than half are failures,
	// then clears the slice so the post-pause window starts fresh instead
	// of re-tripping on stale failures. Lost on failover by design: a new
	// leader starts with an empty window, and the caps still bound churn
	// while it refills.
	outcomes []bool

	// breakerTil is the instant execution resumes after a breaker trip
	// (now + breakerPause at trip time); the zero value means the breaker
	// has never tripped. BreakerOpen is simply now < breakerTil — nothing
	// resets it early, and RecordOutcome may push it further out.
	breakerTil time.Time
}

// NewLedger returns an empty ledger.
func NewLedger() *Ledger { return &Ledger{} }

// RecordDispatch appends one performed actuation.
func (l *Ledger) RecordDispatch(rec DispatchRecord) {
	l.dispatches = append(l.dispatches, rec)
}

// RecordOutcome appends one terminal migration outcome and trips the breaker
// when the recent window is majority-failure. The minimum-sample floor keeps
// one unlucky first move from freezing execution for an hour.
func (l *Ledger) RecordOutcome(success bool, now time.Time) {
	l.outcomes = append(l.outcomes, success)
	if len(l.outcomes) > breakerWindow {
		l.outcomes = l.outcomes[len(l.outcomes)-breakerWindow:]
	}
	failures := 0
	for _, ok := range l.outcomes {
		if !ok {
			failures++
		}
	}
	if len(l.outcomes) >= breakerMinTrips && failures*2 > len(l.outcomes) {
		l.breakerTil = now.Add(breakerPause)
		// A fresh window after the pause: stale failures must not
		// re-trip the breaker off the first post-resume outcome.
		l.outcomes = nil
	}
}

// BreakerOpen reports whether the circuit breaker is holding execution.
func (l *Ledger) BreakerOpen(now time.Time) bool { return now.Before(l.breakerTil) }

// BreakerOpenUntil exposes the hold deadline for reporting.
func (l *Ledger) BreakerOpenUntil() time.Time { return l.breakerTil }

// AbsorbSnapshot reconciles the ledger against a fresh snapshot: records
// older than the retention window drop out entirely, and a record stops
// claiming capacity once its migration can no longer occupy the target —
// the workload shows a terminal migration newer than the dispatch (the
// replacement pod, if any, is now real occupancy), or the workload is gone
// from the cluster altogether. Absorbed records still count toward the
// hourly ledger.
func (l *Ledger) AbsorbSnapshot(snap *snapshot.ClusterSnapshot, now time.Time) {
	kept := l.dispatches[:0]
	for _, rec := range l.dispatches {
		if now.Sub(rec.At) > ledgerRetention {
			continue
		}
		if !rec.absorbed {
			if w, ok := snap.Workloads[rec.Workload]; !ok {
				rec.absorbed = true
			} else if w.LastMigration != nil && w.LastMigration.After(rec.At) {
				rec.absorbed = true
			}
		}
		kept = append(kept, rec)
	}
	l.dispatches = kept
}

// DispatchesWithinHour counts this ledger's actuations in the rolling hour.
func (l *Ledger) DispatchesWithinHour(now time.Time) int {
	count := 0
	for _, rec := range l.dispatches {
		if now.Sub(rec.At) <= time.Hour {
			count++
		}
	}
	return count
}

// NodeCooling reports the target-scoped per-node cooldown: a node an action
// landed on as a target — or left as the source of a routine (defrag-class)
// move — within the window. Health-evacuation sources are exempt: the point
// of a drain is outflow, bounded by the in-flight cap instead.
func (l *Ledger) NodeCooling(node string, window time.Duration, now time.Time) bool {
	if node == "" {
		// Eviction records carry an empty Target; an empty query must
		// not match them.
		return false
	}
	for _, rec := range l.dispatches {
		if now.Sub(rec.At) >= window {
			continue
		}
		if rec.Target == node {
			return true
		}
		if rec.FromNode == node && !rec.Health {
			return true
		}
	}
	return false
}

// ActiveClaims sums the replacement GPUs of unabsorbed dispatches per target
// node — capacity a still-running migration will occupy, which must count as
// allocated so two admissions can never both "fit" into the same free block.
func (l *Ledger) ActiveClaims() map[string]int64 {
	claims := map[string]int64{}
	for _, rec := range l.dispatches {
		if rec.absorbed || rec.Target == "" {
			continue
		}
		claims[rec.Target] += rec.GPUs
	}
	return claims
}
