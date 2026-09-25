package types

import "testing"

// The condition identifiers are matched byte-for-byte by operator
// dashboards, so their wire values are part of the contract.

func TestConditionIdentifiersKeepTheirWireValues(t *testing.T) {
	for _, tc := range []struct {
		got, want string
	}{
		{ConditionGangSchedulingUnavailable.String(), "GangSchedulingUnavailable"},
		{ConditionMigrationPolicyUnconfigured.String(), "MigrationPolicyUnconfigured"},
		{ReasonPodGroupCRDNotInstalled.String(), "PodGroupCRDNotInstalled"},
		{ReasonGangSchedulingAvailable.String(), "GangSchedulingAvailable"},
		{ReasonMigrationCapacityUnconfigured.String(), "MigrationCapacityUnconfigured"},
		{ReasonMigrationCapacityConfigured.String(), "MigrationCapacityConfigured"},
	} {
		if tc.got != tc.want {
			t.Errorf("wire value: got %q want %q", tc.got, tc.want)
		}
	}
}
