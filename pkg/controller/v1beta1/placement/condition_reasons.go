package placement

// The reasons the placement conditions and plan steps carry. Operators and
// tests read the strings, so each keeps its bytes.
const (
	reasonAwaitingMemberConvergence = "AwaitingMemberConvergence"
	reasonClusterPolicyUnresolved   = "HomePolicyUnresolved"
	reasonMemberApplicationFailed   = "MemberApplicationFailed"
	reasonPlanNotPersisted          = "PlanNotPersisted"
	reasonPositiveMinimumRequired   = "PositiveFloorRequired"

	reasonAllocationObservationIncomplete = "AllocationObservationIncomplete"
	reasonAllocationShortfall             = "AllocationShortfall"
	reasonAssignedMinimumsAdmitted        = "AssignedFloorsAdmitted"
	reasonAwaitingChosenCluster           = "AwaitingSingleWinner"
	reasonAwaitingCurrentPlan             = "AwaitingCurrentPlan"
	reasonAwaitingMemberApplication       = "AwaitingMemberApplication"
	reasonAwaitingReplacementAdmission    = "AwaitingReplacementAdmission"
)

// reasonMessages is the message a condition carries for each reason, keyed by
// the constants above. Four names say what the string does not:
// reasonClusterPolicyUnresolved carries "HomePolicyUnresolved",
// reasonPositiveMinimumRequired carries "PositiveFloorRequired",
// reasonAssignedMinimumsAdmitted carries "AssignedFloorsAdmitted" and
// reasonAwaitingChosenCluster carries "AwaitingSingleWinner".
var reasonMessages = map[string]string{
	reasonAwaitingMemberApplication:       "Waiting for observations of the accepted placement plan",
	reasonAwaitingReplacementAdmission:    "The bounded replacement race has no admitted winner",
	reasonAwaitingChosenCluster:           "The admission race has no committed winner",
	reasonAwaitingCurrentPlan:             "Current source intent has no accepted allocation",
	reasonAllocationObservationIncomplete: "Candidate allocations do not account for the accepted floor",
	reasonAllocationShortfall:             "One or more assigned floors are not admitted or requested replicas remain unassigned",
	reasonAssignedMinimumsAdmitted:        "Every assigned floor is admitted and no requested replicas remain unassigned",
}
