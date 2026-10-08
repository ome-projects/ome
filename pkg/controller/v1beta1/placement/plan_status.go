package placement

import (
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

func placementInputCondition(source *v1beta1.InferenceService) policyCondition {
	condition := policyCondition{condType: apis.ConditionType(v1beta1.PlacementInputValid), cond: apis.Condition{
		Status: corev1.ConditionTrue, Reason: "ValidPlacementIntent", Message: "Placement intent is valid",
	}}
	if err := validation.ValidatePlacementIntent(source); err != nil {
		condition.cond.Status = corev1.ConditionFalse
		condition.cond.Reason = "InvalidPlacementIntent"
		condition.cond.Message = err.Error()
	}
	return condition
}

func placementSatisfactionConditions(source *v1beta1.InferenceService) []policyCondition {
	status := source.Status.Placement
	if status == nil || status.Plan == nil {
		return nil
	}
	condition := policyCondition{condType: apis.ConditionType(v1beta1.PlacementSatisfied), cond: apis.Condition{
		Status: corev1.ConditionUnknown, Reason: "AwaitingMemberApplication", Message: "Waiting for observations of the accepted placement plan",
	}}
	accepted := status.Plan
	if accepted.SingleMove != nil && accepted.SingleMove.Selected == "" {
		condition.cond.Reason = "AwaitingReplacementAdmission"
		condition.cond.Message = "The bounded replacement race has no admitted winner"
		return []policyCondition{condition}
	}
	if accepted.Mode == v1beta1.PlacementModeSingle && accepted.Winner == "" {
		condition.cond.Reason = "AwaitingSingleWinner"
		condition.cond.Message = "The admission race has no committed winner"
		return []policyCondition{condition}
	}
	if source.UID == "" || accepted.ID == "" || accepted.Revision <= 0 || accepted.SourceUID != source.UID || accepted.ObservedGeneration != source.Generation {
		condition.cond.Reason = "AwaitingCurrentPlan"
		condition.cond.Message = "Current source intent has no accepted allocation"
		return []policyCondition{condition}
	}
	known, shortfall := true, accepted.UnassignedReplicas > 0
	var assigned int64
	for _, candidate := range status.Candidates {
		if candidate.Allocation == nil {
			known = false
			continue
		}
		if accepted.Mode == v1beta1.PlacementModeAll && (candidate.Allocation.InventoryPending || candidate.Allocation.HomeInputsPending || (candidate.Allocation.Matched && (candidate.Allocation.DesiredHome == nil || candidate.Allocation.DesiredHome.InputDigest != accepted.InputDigest))) {
			known = false
		}
		floor := candidate.Allocation.DesiredReplicas
		assigned += int64(floor)
		if floor == 0 && candidate.Allocation.DesiredHome == nil {
			continue
		}
		if candidate.Allocation.ClusterUID == "" || !candidate.ObservationKnown || candidate.AppliedPlanID != accepted.ID {
			known = false
			continue
		}
		shortfall = shortfall || candidate.AdmittedReplicas < floor
	}
	if assigned != accepted.AssignedReplicas || accepted.RequestedReplicas != assigned+int64(accepted.UnassignedReplicas) {
		condition.cond.Reason = "AllocationObservationIncomplete"
		condition.cond.Message = "Candidate allocations do not account for the accepted floor"
	} else if shortfall {
		condition.cond.Status = corev1.ConditionFalse
		condition.cond.Reason = "AllocationShortfall"
		condition.cond.Message = "One or more assigned floors are not admitted or requested replicas remain unassigned"
	} else if known {
		condition.cond.Status = corev1.ConditionTrue
		condition.cond.Reason = "AssignedFloorsAdmitted"
		condition.cond.Message = "Every assigned floor is admitted and no requested replicas remain unassigned"
	}
	return []policyCondition{condition}
}

// mergeCandidateObservations keeps allocation authority when refreshing member
// observations. An omitted planned home remains present with unknown observation.
func mergeCandidateObservations(stored, observed []v1beta1.CandidatePlacement) []v1beta1.CandidatePlacement {
	planned := map[string]v1beta1.CandidatePlacement{}
	for _, candidate := range stored {
		if candidate.Allocation != nil {
			planned[candidate.Cluster] = candidate
		}
	}
	if len(planned) == 0 {
		return observed
	}
	out := make([]v1beta1.CandidatePlacement, 0, len(observed)+len(planned))
	for _, observation := range observed {
		candidate := *observation.DeepCopy()
		if previous, ok := planned[candidate.Cluster]; ok {
			candidate.Allocation = previous.Allocation.DeepCopy()
			// An observation that does not identify the stored allocation neither
			// acknowledges it nor retracts the acknowledgement already recorded.
			if observation.Allocation == nil || observation.Allocation.ClusterUID != previous.Allocation.ClusterUID {
				candidate.ObservationKnown = false
				candidate.AppliedPlanID = previous.AppliedPlanID
			}
			delete(planned, candidate.Cluster)
		}
		out = append(out, candidate)
	}
	for _, previous := range stored {
		if _, missing := planned[previous.Cluster]; missing {
			candidate := *previous.DeepCopy()
			candidate.ObservationKnown = false
			out = append(out, candidate)
		}
	}
	return out
}
