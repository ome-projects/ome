package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestConditionReasonBytes(t *testing.T) {
	for _, tt := range []struct{ got, want string }{
		{reasonAwaitingMemberConvergence, "AwaitingMemberConvergence"},
		{reasonClusterPolicyUnresolved, "HomePolicyUnresolved"},
		{reasonMemberApplicationFailed, "MemberApplicationFailed"},
		{reasonPlanNotPersisted, "PlanNotPersisted"},
		{reasonPositiveMinimumRequired, "PositiveFloorRequired"},
		{reasonAllocationObservationIncomplete, "AllocationObservationIncomplete"},
		{reasonAllocationShortfall, "AllocationShortfall"},
		{reasonAssignedMinimumsAdmitted, "AssignedFloorsAdmitted"},
		{reasonAwaitingChosenCluster, "AwaitingSingleWinner"},
		{reasonAwaitingCurrentPlan, "AwaitingCurrentPlan"},
		{reasonAwaitingMemberApplication, "AwaitingMemberApplication"},
		{reasonAwaitingReplacementAdmission, "AwaitingReplacementAdmission"},
	} {
		if tt.got != tt.want {
			t.Errorf("reason = %q, want %q", tt.got, tt.want)
		}
	}
	want := map[string]string{
		"AwaitingMemberApplication":       "Waiting for observations of the accepted placement plan",
		"AwaitingReplacementAdmission":    "The bounded replacement race has no admitted winner",
		"AwaitingSingleWinner":            "The admission race has no committed winner",
		"AwaitingCurrentPlan":             "Current source intent has no accepted allocation",
		"AllocationObservationIncomplete": "Candidate allocations do not account for the accepted floor",
		"AllocationShortfall":             "One or more assigned floors are not admitted or requested replicas remain unassigned",
		"AssignedFloorsAdmitted":          "Every assigned floor is admitted and no requested replicas remain unassigned",
	}
	if diff := cmp.Diff(want, reasonMessages); diff != "" {
		t.Errorf("reason messages (-want +got):\n%s", diff)
	}
}
