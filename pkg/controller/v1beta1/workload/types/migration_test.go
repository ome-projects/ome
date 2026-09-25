package types

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMigrationPhaseTerminal(t *testing.T) {
	for _, p := range []MigrationPhase{MigrationPhaseCompleted, MigrationPhaseFailed, MigrationPhaseRelocated} {
		if !p.Terminal() {
			t.Errorf("%s ends the record", p)
		}
	}
	for _, p := range []MigrationPhase{
		MigrationPhaseAccepted, MigrationPhaseSurgePending,
		MigrationPhaseSurgeReady, MigrationPhaseDraining, "",
	} {
		if p.Terminal() {
			t.Errorf("%s is still in flight", p)
		}
	}
}

// The phase chain advances forward only, so a stale pass can never regress
// a record it observed early.
func TestMigrationPhaseAtOrPastOrdersTheChain(t *testing.T) {
	chain := []MigrationPhase{
		MigrationPhaseAccepted, MigrationPhaseSurgePending,
		MigrationPhaseSurgeReady, MigrationPhaseDraining, MigrationPhaseCompleted,
	}
	for i, at := range chain {
		for j, target := range chain {
			want := i >= j
			if got := MigrationPhaseAtOrPast(at, target); got != want {
				t.Errorf("%s at-or-past %s: got %v want %v", at, target, got, want)
			}
		}
	}
	if !MigrationPhaseAtOrPast(MigrationPhaseFailed, MigrationPhaseCompleted) {
		t.Error("every terminal phase ranks with the others")
	}
}

func TestFindMigrationRecordAliasesTheSlice(t *testing.T) {
	records := []MigrationRecord{{RequestUUID: "a"}, {RequestUUID: "b"}}
	found := FindMigrationRecord(records, "b")
	if found == nil {
		t.Fatal("an existing request must be found")
	}
	found.Phase = MigrationPhaseDraining
	if records[1].Phase != MigrationPhaseDraining {
		t.Error("the caller advances the record in place, so the result must alias it")
	}
	if FindMigrationRecord(records, "zz") != nil {
		t.Error("an unknown request finds nothing")
	}
}

// Auto records are born terminal, so the dispatcher never picks one; among
// the Manual records in flight it drives the oldest.
func TestNextManualMigrationPicksTheOldestInFlight(t *testing.T) {
	at := func(min int) metav1.Time {
		return metav1.NewTime(time.Date(2026, 1, 1, 0, min, 0, 0, time.UTC))
	}
	records := []MigrationRecord{
		{RequestUUID: "auto", Trigger: MigrationTriggerAuto, StartedAt: at(0)},
		{RequestUUID: "done", Trigger: MigrationTriggerManual, Phase: MigrationPhaseCompleted, StartedAt: at(1)},
		{RequestUUID: "new", Trigger: MigrationTriggerManual, Phase: MigrationPhaseAccepted, StartedAt: at(5)},
		{RequestUUID: "old", Trigger: MigrationTriggerManual, Phase: MigrationPhaseDraining, StartedAt: at(2)},
	}
	picked := NextManualMigration(records)
	if picked == nil || picked.RequestUUID != "old" {
		t.Fatalf("oldest in-flight Manual record wins: got %+v", picked)
	}
	if NextManualMigration(nil) != nil {
		t.Error("no records, no work")
	}
	if got := NextManualMigration(records[:2]); got != nil {
		t.Errorf("an Auto record and a terminal one are no work: got %+v", got)
	}
}

// A record has taken its surge index once SurgeInstance names one; index
// 0 is a valid surge index and a negative sentinel is not an allocation.
func TestMigrationRecord_SurgeAllocatedReadsTheIndex(t *testing.T) {
	zero, negative := int32(0), int32(-1)
	if (MigrationRecord{}).SurgeAllocated() {
		t.Error("nil SurgeInstance is unallocated")
	}
	if !(MigrationRecord{SurgeInstance: &zero}).SurgeAllocated() {
		t.Error("index 0 is an allocation")
	}
	if (MigrationRecord{SurgeInstance: &negative}).SurgeAllocated() {
		t.Error("a negative index is not an allocation")
	}
}
