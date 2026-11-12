package audit

import (
	"slices"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ReasonAutoRecover is the Reason written on a relocation record: the row
// the deadline decision's relocation branch records. Search for this
// constant to find every site that reads or writes auto-recovery records.
//
// AutoRecover entries are records, not migration work orders: they are
// written terminal (Phase=Completed, Outcome=relocate-recreate) and the
// migration-request detector skips them. Their three jobs: budget
// accounting (CountAutoRecoverAttempts), node-exclusion memory
// (RecentAutoRecoverExclusions, which the render NotIn overlay reads), and
// the audit trail.
const ReasonAutoRecover = "AutoRecover"

// OutcomeRelocateRecreate is the Outcome written on a relocation record:
// the instance was decided Failed and its rebuild is steered off the
// recorded FromNode through the exclusion overlay. This is the mover path,
// distinct from the copier (surge-migration) machinery.
const OutcomeRelocateRecreate = "relocate-recreate"

// HasInFlightMigrationForInstance reports whether the Ledger holds an
// in-flight (Started) row for this (component, instance). Used to
// suppress fresh auto-recovery requests while a migration of the same
// Instance is still being driven by the migration detector.
func HasInFlightMigrationForInstance(ledger *Ledger, component string, idx int32) bool {
	if ledger == nil {
		return false
	}
	return slices.ContainsFunc(ledger.Entries, func(e Entry) bool {
		return e.InFlight() && e.Component == component && e.SourceInstance == idx
	})
}

// CountAutoRecoverAttempts returns the number of entries for this
// (component, instance) whose Reason is AutoRecover, whatever their
// phase. Gates the relocation budget (AutoMigrateMaxAttempts).
//
// Every such entry counts, open or terminal, because each one is a
// relocation attempt that took a slot of the budget. Counting by reason
// alone keeps an Instance whose pod fails for a cause no node fixes from
// earning the budget over again.
//
// Bound: the shared owner Ledger keeps at most maxTerminalEntries
// terminal entries (audit.go), so an owner with very many migrations can
// evict old AutoRecover entries, and with them this count and the
// node-exclusion memory. That is the accepted bound on how long the
// memory lasts.
func CountAutoRecoverAttempts(ledger *Ledger, component string, idx int32) int32 {
	if ledger == nil {
		return 0
	}
	count := int32(0)
	for _, e := range ledger.Entries {
		if isAutoRecoverFor(e, component, idx) {
			count++
		}
	}
	return count
}

// RecentAutoRecoverExclusions returns the node exclusions of the most
// recent (append-order) AutoRecover entries for this (component,
// instance), at most limit distinct nodes, each with the revision it was
// recorded for. This is the node-exclusion memory the render overlay
// consumes; bounding to the relocation budget keeps the NotIn term from
// growing without bound. An entry with no revision excludes nothing: it
// predates revision scoping and stays only for the budget and the audit
// trail. Neither does a released one (ReleaseAutoRecoverExclusions).
func RecentAutoRecoverExclusions(ledger *Ledger, component string, idx int32, limit int32) []types.NodeExclusion {
	if ledger == nil || limit <= 0 {
		return nil
	}
	return newestDistinctNodes(heldExclusions(ledger, component, idx), limit)
}

// heldExclusions are the node exclusions the AutoRecover entries of one
// (component, instance) still hold, in append order.
func heldExclusions(ledger *Ledger, component string, idx int32) []types.NodeExclusion {
	var held []types.NodeExclusion
	for _, e := range ledger.Entries {
		if isAutoRecoverFor(e, component, idx) && e.holdsExclusion() {
			held = append(held, types.NodeExclusion{Node: e.FromNode, Revision: e.Revision})
		}
	}
	return held
}

// newestDistinctNodes keeps each node at its most recent exclusion, at most
// limit nodes counted newest first, and returns them in append order; nil
// when there are none. Distinct nodes are counted before the window is
// cut, so a node recorded twice does not shrink the window below limit
// nodes.
func newestDistinctNodes(exclusions []types.NodeExclusion, limit int32) []types.NodeExclusion {
	seen := make(map[string]struct{}, len(exclusions))
	var kept []types.NodeExclusion
	for i := len(exclusions) - 1; i >= 0 && int32(len(kept)) < limit; i-- {
		if _, dup := seen[exclusions[i].Node]; dup {
			continue
		}
		seen[exclusions[i].Node] = struct{}{}
		kept = append(kept, exclusions[i])
	}
	if len(kept) == 0 {
		return nil
	}
	slices.Reverse(kept)
	return kept
}

// ReleaseAutoRecoverExclusions lets go of every exclusion this
// (component, instance) holds for revision — the set a rebuild at that
// revision rendered — and returns the distinct nodes released, in
// append order; nil when nothing was held. The entries stay: a released
// record still counts on the relocation budget, so a node that keeps
// failing a revision that serves elsewhere is retried a bounded number
// of times, not forever.
func ReleaseAutoRecoverExclusions(ledger *Ledger, component string, idx int32, revision string) []string {
	if ledger == nil || revision == "" {
		return nil
	}
	seen := map[string]struct{}{}
	var released []string
	for i := range ledger.Entries {
		e := &ledger.Entries[i]
		if !isAutoRecoverFor(*e, component, idx) || !e.holdsExclusion() || e.Revision != revision {
			continue
		}
		e.Released = true
		if _, dup := seen[e.FromNode]; !dup {
			seen[e.FromNode] = struct{}{}
			released = append(released, e.FromNode)
		}
	}
	return released
}

// NodesExcludedForRevision narrows exclusions to the nodes recorded for
// revision — the set a rebuild at that revision renders. No revision
// names no node.
func NodesExcludedForRevision(exclusions []types.NodeExclusion, revision string) []string {
	if revision == "" {
		return nil
	}
	var nodes []string
	for _, exclusion := range exclusions {
		if exclusion.Revision == revision {
			nodes = append(nodes, exclusion.Node)
		}
	}
	return nodes
}

// NewestAutoRecoverEntry returns a copy of the most recent
// (append-order) AutoRecover entry for this (component, instance), or
// nil when none exists. The deadline decision's replay guard anchors
// on it: an entry completed AFTER the current attempt began is that
// attempt's own record, persisted on a pass that didn't finish.
func NewestAutoRecoverEntry(ledger *Ledger, component string, idx int32) *Entry {
	if ledger == nil {
		return nil
	}
	for i := len(ledger.Entries) - 1; i >= 0; i-- {
		if isAutoRecoverFor(ledger.Entries[i], component, idx) {
			e := ledger.Entries[i]
			return &e
		}
	}
	return nil
}

// RemoveAutoRecoverEntries drops every AutoRecover entry for this
// (component, instance) and reports whether anything was removed — the
// success-prune mirror of the RetryBlock prune: an instance that
// reached Ready has proven its placement, so its exclusion memory and
// budget accounting reset.
func RemoveAutoRecoverEntries(ledger *Ledger, component string, idx int32) bool {
	if ledger == nil {
		return false
	}
	before := len(ledger.Entries)
	ledger.Entries = slices.DeleteFunc(ledger.Entries, func(e Entry) bool {
		return isAutoRecoverFor(e, component, idx)
	})
	return len(ledger.Entries) < before
}

// isAutoRecoverFor reports whether e is an AutoRecover record for the
// (component, instance) pair.
func isAutoRecoverFor(e Entry, component string, idx int32) bool {
	return e.Reason == ReasonAutoRecover && e.Component == component && e.SourceInstance == idx
}

// holdsExclusion reports whether the record still excludes a node: it
// names the node and the revision, and was not released.
func (e Entry) holdsExclusion() bool {
	return e.FromNode != "" && e.Revision != "" && !e.Released
}
