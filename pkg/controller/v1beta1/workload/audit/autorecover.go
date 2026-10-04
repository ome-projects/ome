package audit

import "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"

// ReasonAutoRecover is the Reason string stamped on relocation
// directives — the ledger entries the deadline disposition's relocation
// branch records. Search for this constant to find every site that
// reads or writes auto-recovery records.
//
// AutoRecover entries are RECORDS, not migration work orders: they are
// written terminal (Phase=Completed, Outcome=relocate-recreate) and the
// migration-request detector skips them. Their three jobs: budget
// accounting (CountAutoRecoverAttempts), node-exclusion memory
// (RecentAutoRecoverExclusions → the render NotIn overlay), and audit
// evidence.
const ReasonAutoRecover = "AutoRecover"

// OutcomeRelocateRecreate is the Outcome stamped on a relocation
// directive: the instance was disposed Failed and its rebuild is
// steered off the recorded FromNode via the exclusion overlay — the
// mover path, distinct from the copier (surge-migration) machinery.
const OutcomeRelocateRecreate = "relocate-recreate"

// HasInFlightMigrationForInstance reports whether the ledger has a
// Started entry for this (component, instance) without a terminal
// counterpart. Used to suppress fresh auto-recovery requests while an
// existing one is still being driven by the migration detector's
// ledger source.
func HasInFlightMigrationForInstance(ledger *Ledger, component string, idx int32) bool {
	if ledger == nil {
		return false
	}
	terminal := map[string]struct{}{}
	for _, e := range ledger.Entries {
		if e.Phase == PhaseCompleted || e.Phase == PhaseFailed {
			terminal[e.RequestUUID] = struct{}{}
		}
	}
	for _, e := range ledger.Entries {
		if e.Phase != PhaseStarted {
			continue
		}
		if e.Component != component {
			continue
		}
		if e.SourceInstance != idx {
			continue
		}
		if _, done := terminal[e.RequestUUID]; done {
			continue
		}
		return true
	}
	return false
}

// CountAutoRecoverAttempts returns the number of ledger entries (any
// phase) for this (component, instance) whose Reason == AutoRecover.
// Gates the operator-config relocation budget.
//
// Counts ALL entries (legacy Started + terminal directives) — a prior
// attempt counts toward the cap regardless of its phase, so a
// misconfigured pod doesn't repeatedly chew through the relocation
// budget.
//
// Bound: the shared owner ledger ring-buffers terminal entries
// (maxTerminalEntries in audit.go), so an extremely churny owner can
// evict old AutoRecover directives — and with them this budget count
// and the node-exclusion memory. Accepted bound on how long the memory
// persists.
func CountAutoRecoverAttempts(ledger *Ledger, component string, idx int32) int32 {
	if ledger == nil {
		return 0
	}
	count := int32(0)
	for _, e := range ledger.Entries {
		if !isAutoRecoverFor(e, component, idx) {
			continue
		}
		count++
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
	var matched []types.NodeExclusion
	for _, e := range ledger.Entries {
		if !isAutoRecoverFor(e, component, idx) || e.FromNode == "" || e.Revision == "" || e.Released {
			continue
		}
		matched = append(matched, types.NodeExclusion{Node: e.FromNode, Revision: e.Revision})
	}
	// Dedup BEFORE windowing: duplicate FromNodes (e.g. a directive
	// re-recorded for the same node) must not shrink the window below
	// limit DISTINCT nodes. Walk newest-first keeping each node at its
	// most recent occurrence, cap at limit, then restore append order.
	seen := make(map[string]struct{}, len(matched))
	var distinct []types.NodeExclusion // newest-first
	for i := len(matched) - 1; i >= 0 && int32(len(distinct)) < limit; i-- {
		if _, dup := seen[matched[i].Node]; dup {
			continue
		}
		seen[matched[i].Node] = struct{}{}
		distinct = append(distinct, matched[i])
	}
	if len(distinct) == 0 {
		return nil
	}
	out := make([]types.NodeExclusion, 0, len(distinct))
	for i := len(distinct) - 1; i >= 0; i-- {
		out = append(out, distinct[i])
	}
	return out
}

// ReleaseAutoRecoverExclusions lets go of every exclusion this
// (component, instance) holds for revision — the set a rebuild at that
// revision rendered — and returns the distinct nodes released, in
// append order; nil when nothing was held. The entries stay: a released
// directive still counts on the relocation budget, so a node that keeps
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
		if !isAutoRecoverFor(*e, component, idx) || e.Released || e.FromNode == "" || e.Revision != revision {
			continue
		}
		e.Released = true
		if _, dup := seen[e.FromNode]; dup {
			continue
		}
		seen[e.FromNode] = struct{}{}
		released = append(released, e.FromNode)
	}
	return released
}

// NodesExcludedForRevision narrows exclusions to the nodes recorded for
// revision — the set a rebuild at that revision renders.
func NodesExcludedForRevision(exclusions []types.NodeExclusion, revision string) []string {
	var nodes []string
	for _, exclusion := range exclusions {
		if revision != "" && exclusion.Revision == revision {
			nodes = append(nodes, exclusion.Node)
		}
	}
	return nodes
}

// NewestAutoRecoverEntry returns a copy of the most recent
// (append-order) AutoRecover entry for this (component, instance), or
// nil when none exists. The deadline disposition's replay guard anchors
// on it: an entry completed AFTER the current attempt began is that
// attempt's own directive, persisted on a pass that didn't finish.
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
	kept := ledger.Entries[:0]
	removed := false
	for _, e := range ledger.Entries {
		if isAutoRecoverFor(e, component, idx) {
			removed = true
			continue
		}
		kept = append(kept, e)
	}
	ledger.Entries = kept
	return removed
}

// isAutoRecoverFor reports whether e is an AutoRecover record for the
// (component, instance) pair.
func isAutoRecoverFor(e Entry, component string, idx int32) bool {
	return e.Reason == ReasonAutoRecover && e.Component == component && e.SourceInstance == idx
}
