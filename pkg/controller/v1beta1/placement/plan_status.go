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
	accepted := status.Plan
	if accepted.SingleMove != nil && accepted.SingleMove.Selected == "" {
		return []policyCondition{satisfactionCondition(corev1.ConditionUnknown, reasonAwaitingReplacementAdmission)}
	}
	if accepted.Mode == v1beta1.PlacementModeSingle && accepted.Winner == "" {
		return []policyCondition{satisfactionCondition(corev1.ConditionUnknown, reasonAwaitingChosenCluster)}
	}
	if source.UID == "" || accepted.ID == "" || accepted.Revision <= 0 || accepted.SourceUID != source.UID || accepted.ObservedGeneration != source.Generation {
		return []policyCondition{satisfactionCondition(corev1.ConditionUnknown, reasonAwaitingCurrentPlan)}
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
		desiredReplicas := candidate.Allocation.DesiredReplicas
		assigned += int64(desiredReplicas)
		if desiredReplicas == 0 && candidate.Allocation.DesiredHome == nil {
			continue
		}
		if candidate.Allocation.ClusterUID == "" || !candidate.ObservationKnown || candidate.AppliedPlanID != accepted.ID {
			known = false
			continue
		}
		shortfall = shortfall || candidate.AdmittedReplicas < desiredReplicas
	}
	switch {
	case assigned != accepted.AssignedReplicas || accepted.RequestedReplicas != assigned+int64(accepted.UnassignedReplicas):
		return []policyCondition{satisfactionCondition(corev1.ConditionUnknown, reasonAllocationObservationIncomplete)}
	case shortfall:
		return []policyCondition{satisfactionCondition(corev1.ConditionFalse, reasonAllocationShortfall)}
	case known:
		return []policyCondition{satisfactionCondition(corev1.ConditionTrue, reasonAssignedMinimumsAdmitted)}
	}
	return []policyCondition{satisfactionCondition(corev1.ConditionUnknown, reasonAwaitingMemberApplication)}
}

// satisfactionCondition is the PlacementSatisfied condition for one reason,
// carrying the message the reason has in reasonMessages.
func satisfactionCondition(status corev1.ConditionStatus, reason string) policyCondition {
	return policyCondition{condType: apis.ConditionType(v1beta1.PlacementSatisfied), cond: apis.Condition{
		Status: status, Reason: reason, Message: reasonMessages[reason],
	}}
}

// mergeCandidateObservations keeps allocation authority when refreshing member
// observations. An omitted planned member remains present with unknown observation.
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
