package types

import "testing"

// TestOperationRetiredReplacementTerminating_ReadsTheTokenAlone: the
// predicate is true of an operation carrying the retired replacement's
// token and of nothing else, a missing operation included.
func TestOperationRetiredReplacementTerminating_ReadsTheTokenAlone(t *testing.T) {
	cases := map[string]struct {
		op   *InstanceOperation
		want bool
	}{
		"no operation":        {op: nil, want: false},
		"no token":            {op: &InstanceOperation{Type: InstanceOperationUpdate, Step: UpdateStepSurge}, want: false},
		"another token":       {op: &InstanceOperation{Type: InstanceOperationUpdate, Step: UpdateStepSurge, Waiting: WaitingReasonPaused}, want: false},
		"the retirement's":    {op: &InstanceOperation{Type: InstanceOperationUpdate, Step: UpdateStepSurge, Waiting: WaitingReasonRetiredReplacementTerminating}, want: true},
		"on the cleanup step": {op: &InstanceOperation{Type: InstanceOperationUpdate, Step: UpdateStepGangSurgeTargetCleanup, Waiting: WaitingReasonRetiredReplacementTerminating}, want: true},
	}
	for name, tc := range cases {
		if got := OperationRetiredReplacementTerminating(tc.op); got != tc.want {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
	if OperationExternallyHeld(&InstanceOperation{Type: InstanceOperationUpdate, Waiting: WaitingReasonRetiredReplacementTerminating}) {
		t.Errorf("the retirement's token must not read as an external hold: the deadline clock does not stop for it")
	}
}
