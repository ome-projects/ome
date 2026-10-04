package audit

import (
	"testing"
)

// forceDeleteEntry builds a terminal ForceDelete record shaped exactly
// as the Delete op's escalation writes it.
func forceDeleteEntry(podUID, component string, idx int32, node, startedAt string) Entry {
	return Entry{
		RequestUUID:    podUID,
		Component:      component,
		SourceInstance: idx,
		Phase:          PhaseCompleted,
		Reason:         ReasonForceDelete,
		Outcome:        OutcomeForceDeleteUnreachable,
		FromNode:       node,
		StartedAt:      startedAt,
		CompletedAt:    startedAt,
	}
}

// ForceDelete rows are terminal audit records — inert for every
// AutoRecover reader: they never count toward the relocation budget,
// never feed the node-exclusion overlay, never anchor the disposition
// replay guard, and never read as an in-flight migration.
func TestForceDeleteEntries_InertForAutoRecoverReaders(t *testing.T) {
	ledger := &Ledger{Entries: []Entry{
		forceDeleteEntry("uid-1", "engine", 0, "node-a", "2026-07-23T00:00:00Z"),
		forceDeleteEntry("uid-2", "engine", 0, "node-b", "2026-07-23T00:01:00Z"),
		{RequestUUID: "uid-3", Component: "engine", SourceInstance: 0,
			Phase: PhaseCompleted, Reason: ReasonForceDelete,
			Outcome: OutcomeForceDeleteFinalizerReport, FromNode: "node-c",
			StartedAt: "2026-07-23T00:02:00Z", CompletedAt: "2026-07-23T00:02:00Z"},
	}}

	if n := CountAutoRecoverAttempts(ledger, "engine", 0); n != 0 {
		t.Errorf("CountAutoRecoverAttempts: got %d want 0 (force-deletes must not burn relocation budget)", n)
	}
	if exclusions := RecentAutoRecoverExclusions(ledger, "engine", 0, 5); len(exclusions) != 0 {
		t.Errorf("RecentAutoRecoverExclusions: got %v want none (force-deletes must not steer placement)", exclusions)
	}
	if e := NewestAutoRecoverEntry(ledger, "engine", 0); e != nil {
		t.Errorf("NewestAutoRecoverEntry: got %+v want nil", e)
	}
	if RemoveAutoRecoverEntries(ledger, "engine", 0) {
		t.Errorf("RemoveAutoRecoverEntries: force-delete records must not be pruned as directives")
	}
	if HasInFlightMigrationForInstance(ledger, "engine", 0) {
		t.Errorf("HasInFlightMigrationForInstance: terminal force-delete rows are not in-flight migrations")
	}
}

// ForceDelete rows key on the pod UID in the same RequestUUID keyspace
// as migrations; a migration request reusing a force-deleted pod's UID
// must not be swallowed as already-terminal.
func TestHasCompletedOrFailedRequest_IgnoresForceDeleteRows(t *testing.T) {
	ledger := &Ledger{Entries: []Entry{
		forceDeleteEntry("uid-x", "engine", 0, "node-a", "2026-07-23T00:00:00Z"),
	}}
	if ledger.HasCompletedOrFailedRequest("uid-x") {
		t.Errorf("ForceDelete row must not satisfy HasCompletedOrFailedRequest")
	}
	// A genuine terminal migration row for the same UUID (Reason is the
	// requester's free-form string, often empty) still matches.
	ledger.Entries = append(ledger.Entries, Entry{
		RequestUUID: "uid-x", Component: "engine", Phase: PhaseCompleted,
		StartedAt: "2026-07-23T00:01:00Z", CompletedAt: "2026-07-23T00:01:00Z",
	})
	if !ledger.HasCompletedOrFailedRequest("uid-x") {
		t.Errorf("terminal migration row must still satisfy HasCompletedOrFailedRequest")
	}
}

// Capacity note: ValidateCapacity now reads status.migrations records,
// which force-delete ledger rows can never enter (the accept pass never
// creates one and the upgrade import filters Reason=ForceDelete), so
// the old ledger-level ValidateCapacity force-delete exclusion test is
// structurally obsolete. The HasCompletedOrFailedRequest exclusion
// above still matters — the ledger remains the dedup history.
