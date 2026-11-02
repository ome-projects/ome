package types

import (
	"testing"
)

func TestAdapterPublished(t *testing.T) {
	pods, available := AdapterPublished(&InstanceStatus{PodCount: 3, AvailablePodCount: 2})
	if pods != 3 || available != 2 {
		t.Fatalf("published counters: got (%d, %d) want (3, 2)", pods, available)
	}
	if pods, available = AdapterPublished(nil); pods != 0 || available != 0 {
		t.Fatalf("unobserved row: got (%d, %d) want (0, 0)", pods, available)
	}
}

// TestStepConstantValues checks each step constant against the string the
// status rows carry: the owner table, the stored rows and the replay goldens
// read the bytes, so a constant may not drift from the step it names.
func TestStepConstantValues(t *testing.T) {
	for name, tc := range map[string]struct{ got, want string }{
		"UpdateStepInPlace":                {UpdateStepInPlace, "InPlace"},
		"UpdateStepDrain":                  {UpdateStepDrain, "Drain"},
		"UpdateStepParked":                 {UpdateStepParked, "Parked"},
		"RestartStepDrain":                 {RestartStepDrain, "Drain"},
		"DeleteStepDrain":                  {DeleteStepDrain, "Drain"},
		"MigrateStepCreateSurge":           {MigrateStepCreateSurge, "CreateSurge"},
		"UpdateStepSurge":                  {UpdateStepSurge, "Surge"},
		"UpdateStepGangSurgeTarget":        {UpdateStepGangSurgeTarget, "GangSurgeTarget"},
		"UpdateStepGangSurgeTargetCleanup": {UpdateStepGangSurgeTargetCleanup, "GangSurgeTargetCleanup"},
		"UpdateStepSurgeDrain":             {UpdateStepSurgeDrain, "SurgeDrain"},
		"UpdateStepSurgeDrainSettle":       {UpdateStepSurgeDrainSettle, "SurgeDrainSettle"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", name, tc.got, tc.want)
		}
	}
}
