package types

import "testing"

// The observation answers "the row at this index" once, for every
// reader: a hit aliases the observed row, a miss is nil rather than a
// zero row, and nothing about the slice order is assumed.
func TestWorkloadObservedState_InstanceAliasesTheObservedRow(t *testing.T) {
	observed := WorkloadObservedState{InstanceStatuses: []InstanceStatus{
		{Index: 3, Phase: InstancePhaseReady},
		{Index: 0, Phase: InstancePhaseCreating},
	}}
	row := observed.Instance(0)
	if row == nil || row.Phase != InstancePhaseCreating {
		t.Fatalf("Instance(0) = %+v, want the Creating row", row)
	}
	if row != &observed.InstanceStatuses[1] {
		t.Fatal("the lookup must alias the observation, not copy it")
	}
	if got := observed.Instance(7); got != nil {
		t.Fatalf("Instance(7) = %+v, want nil for an index the observation lacks", got)
	}
	var empty WorkloadObservedState
	if got := empty.Instance(0); got != nil {
		t.Fatalf("an empty observation has no rows: got %+v", got)
	}
}
