package audit

import (
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func autoRecoverEntry(uuid, node string, completedOffset time.Duration) Entry {
	return Entry{
		RequestUUID:    uuid,
		Component:      "engine",
		SourceInstance: 0,
		Phase:          PhaseCompleted,
		Reason:         ReasonAutoRecover,
		Outcome:        OutcomeRelocateRecreate,
		FromNode:       node,
		Revision:       "own-engine-rev1",
		StartedAt:      fixedNow.Add(completedOffset).Format(time.RFC3339),
		CompletedAt:    fixedNow.Add(completedOffset).Format(time.RFC3339),
	}
}

// excludedNodes flattens exclusions to their node names, in order.
func excludedNodes(exclusions []types.NodeExclusion) []string {
	var nodes []string
	for _, exclusion := range exclusions {
		nodes = append(nodes, exclusion.Node)
	}
	return nodes
}

// TestRecentAutoRecoverExclusions_DedupBeforeWindow pins the windowing
// order: duplicate FromNodes are collapsed BEFORE the last-limit slice,
// so a repeated node cannot shrink the effective exclusion memory below
// limit DISTINCT nodes. With entries [A, B, B] and limit 2, the older
// distinct node A must survive (slicing first would yield [B, B] → [B]).
func TestRecentAutoRecoverExclusions_DedupBeforeWindow(t *testing.T) {
	ledger := &Ledger{Entries: []Entry{
		autoRecoverEntry("u1", "node-a", -3*time.Minute),
		autoRecoverEntry("u2", "node-b", -2*time.Minute),
		autoRecoverEntry("u3", "node-b", -time.Minute),
	}}

	got := excludedNodes(RecentAutoRecoverExclusions(ledger, "engine", 0, 2))
	if len(got) != 2 || got[0] != "node-a" || got[1] != "node-b" {
		t.Errorf("nodes: got %v want [node-a node-b] (dedup before window)", got)
	}

	// limit 1 keeps only the most recent distinct node.
	got = excludedNodes(RecentAutoRecoverExclusions(ledger, "engine", 0, 1))
	if len(got) != 1 || got[0] != "node-b" {
		t.Errorf("nodes (limit 1): got %v want [node-b]", got)
	}
}

func TestRecentAutoRecoverExclusions_EmptyCases(t *testing.T) {
	if got := RecentAutoRecoverExclusions(nil, "engine", 0, 3); got != nil {
		t.Errorf("nil ledger: got %v want nil", got)
	}
	ledger := &Ledger{Entries: []Entry{autoRecoverEntry("u1", "node-a", 0)}}
	if got := RecentAutoRecoverExclusions(ledger, "engine", 0, 0); got != nil {
		t.Errorf("limit 0: got %v want nil", got)
	}
	if got := RecentAutoRecoverExclusions(ledger, "decoder", 0, 3); got != nil {
		t.Errorf("other component: got %v want nil", got)
	}
}

// TestRecentAutoRecoverExclusions_CarryTheirRevision pins the scope of
// the memory: each exclusion carries the revision its directive was
// recorded for, the newest directive for a node decides its revision,
// a directive with no revision excludes nothing yet still counts on the
// budget, and NodesExcludedForRevision narrows the set to one revision.
func TestRecentAutoRecoverExclusions_CarryTheirRevision(t *testing.T) {
	older := autoRecoverEntry("u1", "node-a", -3*time.Minute)
	newer := autoRecoverEntry("u2", "node-a", -2*time.Minute)
	newer.Revision = "own-engine-rev2"
	other := autoRecoverEntry("u3", "node-b", -time.Minute)
	legacy := autoRecoverEntry("u4", "node-c", 0)
	legacy.Revision = ""
	ledger := &Ledger{Entries: []Entry{older, newer, other, legacy}}

	got := RecentAutoRecoverExclusions(ledger, "engine", 0, 5)
	want := []types.NodeExclusion{{Node: "node-a", Revision: "own-engine-rev2"}, {Node: "node-b", Revision: "own-engine-rev1"}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("exclusions: got %v want %v (newest directive per node, no revision-less entry)", got, want)
	}
	if nodes := NodesExcludedForRevision(got, "own-engine-rev1"); len(nodes) != 1 || nodes[0] != "node-b" {
		t.Errorf("nodes for rev1: got %v want [node-b]", nodes)
	}
	if nodes := NodesExcludedForRevision(got, ""); nodes != nil {
		t.Errorf("nodes for no revision: got %v want none", nodes)
	}
	if count := CountAutoRecoverAttempts(ledger, "engine", 0); count != 4 {
		t.Errorf("budget: got %d want 4 (a revision-less directive still counts)", count)
	}
}

// TestNewestAutoRecoverEntry pins the append-order recency contract the
// disposition replay guard relies on.
func TestNewestAutoRecoverEntry(t *testing.T) {
	if got := NewestAutoRecoverEntry(nil, "engine", 0); got != nil {
		t.Errorf("nil ledger: got %+v want nil", got)
	}
	ledger := &Ledger{Entries: []Entry{
		autoRecoverEntry("u1", "node-a", -2*time.Minute),
		autoRecoverEntry("u2", "node-b", -time.Minute),
		// Non-AutoRecover entry after the newest directive must not win.
		{RequestUUID: "u3", Component: "engine", SourceInstance: 0, Phase: PhaseStarted},
	}}
	got := NewestAutoRecoverEntry(ledger, "engine", 0)
	if got == nil || got.RequestUUID != "u2" || got.FromNode != "node-b" {
		t.Fatalf("newest: got %+v want u2/node-b", got)
	}
	if got := NewestAutoRecoverEntry(ledger, "engine", 1); got != nil {
		t.Errorf("other instance: got %+v want nil", got)
	}
}

// TestReleaseAutoRecoverExclusions pins the release: only this
// Instance's unreleased directives for the revision are let go, each
// freed node is reported once, the entries stay for the budget, and a
// second release finds nothing.
func TestReleaseAutoRecoverExclusions(t *testing.T) {
	sameNodeTwice := autoRecoverEntry("u2", "node-a", -2*time.Minute)
	otherRevision := autoRecoverEntry("u3", "node-b", -time.Minute)
	otherRevision.Revision = "own-engine-rev2"
	otherInstance := autoRecoverEntry("u4", "node-c", 0)
	otherInstance.SourceInstance = 1
	ledger := &Ledger{Entries: []Entry{autoRecoverEntry("u1", "node-a", -3*time.Minute), sameNodeTwice, otherRevision, otherInstance}}

	released := ReleaseAutoRecoverExclusions(ledger, "engine", 0, "own-engine-rev1")
	if len(released) != 1 || released[0] != "node-a" {
		t.Fatalf("released: got %v want [node-a] (reported once, other revisions and instances untouched)", released)
	}
	got := excludedNodes(RecentAutoRecoverExclusions(ledger, "engine", 0, 5))
	if len(got) != 1 || got[0] != "node-b" {
		t.Errorf("exclusions after release: got %v want [node-b] (the other revision's directive stands)", got)
	}
	if other := excludedNodes(RecentAutoRecoverExclusions(ledger, "engine", 1, 5)); len(other) != 1 || other[0] != "node-c" {
		t.Errorf("other instance: got %v want [node-c]", other)
	}
	if count := CountAutoRecoverAttempts(ledger, "engine", 0); count != 3 {
		t.Errorf("budget: got %d want 3 (released directives still count)", count)
	}
	if again := ReleaseAutoRecoverExclusions(ledger, "engine", 0, "own-engine-rev1"); again != nil {
		t.Errorf("second release: got %v want nil", again)
	}
	if ReleaseAutoRecoverExclusions(ledger, "engine", 0, "") != nil || ReleaseAutoRecoverExclusions(nil, "engine", 0, "own-engine-rev1") != nil {
		t.Errorf("no revision or no ledger must release nothing")
	}
}
