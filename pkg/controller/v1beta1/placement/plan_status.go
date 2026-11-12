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

// placementSatisfactionConditions is the PlacementSatisfied condition of a
// source with an accepted plan: Unknown while the plan cannot be decided yet or
// its members are not all known to have applied it, False while an assigned
// minimum is short or replicas are unassigned, True when every assigned
// minimum is admitted. A source without a plan carries no condition.
func placementSatisfactionConditions(source *v1beta1.InferenceService) []policyCondition {
	status := source.Status.Placement
	if status == nil || status.Plan == nil {
		return nil
	}
	if reason := planAwaitingReason(source, status.Plan); reason != "" {
		return []policyCondition{satisfactionCondition(corev1.ConditionUnknown, reason)}
	}
	admission := readCandidateAdmission(status.Plan, status.Candidates)
	return []policyCondition{admission.condition(status.Plan)}
}

// planAwaitingReason is why the accepted plan cannot be decided yet: a bounded
// replacement with no chosen cluster, a Single plan with no chosen cluster, or
// a plan that is not the current source's. Empty when the plan can be decided.
func planAwaitingReason(source *v1beta1.InferenceService, accepted *v1beta1.PlacementPlanStatus) string {
	switch {
	case accepted.SingleMove != nil && accepted.SingleMove.Selected == "":
		return reasonAwaitingReplacementAdmission
	case accepted.Mode == v1beta1.PlacementModeSingle && accepted.Winner == "":
		return reasonAwaitingChosenCluster
	case !acceptedPlanCurrent(source, accepted):
		return reasonAwaitingCurrentPlan
	}
	return ""
}

// acceptedPlanCurrent reports whether the plan was accepted for the source as
// it is now: an identified plan with a revision, recorded for the source's UID
// and its current generation.
func acceptedPlanCurrent(source *v1beta1.InferenceService, accepted *v1beta1.PlacementPlanStatus) bool {
	return source.UID != "" && accepted.ID != "" && accepted.Revision > 0 &&
		accepted.SourceUID == source.UID && accepted.ObservedGeneration == source.Generation
}

// planAccountsFor reports whether the plan's totals add up over the replica
// units its candidates are assigned: the assigned total it records, and the
// requested total as assigned plus unassigned.
func planAccountsFor(accepted *v1beta1.PlacementPlanStatus, assigned int64) bool {
	return assigned == accepted.AssignedReplicas && accepted.RequestedReplicas == assigned+int64(accepted.UnassignedReplicas)
}

// candidateAdmission is what the stored candidates say about the accepted
// plan: the replica units assigned to them, whether every assigned member is
// known to have applied the plan, and whether an assigned minimum is short.
type candidateAdmission struct {
	assigned  int64
	known     bool
	shortfall bool
}

// readCandidateAdmission reads the candidates against the accepted plan;
// unassigned replicas count as a shortfall from the start.
func readCandidateAdmission(accepted *v1beta1.PlacementPlanStatus, candidates []v1beta1.CandidatePlacement) candidateAdmission {
	admission := candidateAdmission{known: true, shortfall: accepted.UnassignedReplicas > 0}
	for _, candidate := range candidates {
		admission.add(accepted, candidate)
	}
	return admission
}

// add reads one candidate. A candidate without an allocation, or whose
// observation does not identify the accepted plan, leaves the plan unknown; a
// member assigned nothing needs no admission unless it holds a desired
// placement.
func (a *candidateAdmission) add(accepted *v1beta1.PlacementPlanStatus, candidate v1beta1.CandidatePlacement) {
	allocation := candidate.Allocation
	if allocation == nil {
		a.known = false
		return
	}
	if accepted.Mode == v1beta1.PlacementModeAll && allInputsUnsettled(allocation, accepted.InputDigest) {
		a.known = false
	}
	a.assigned += int64(allocation.DesiredReplicas)
	if allocation.DesiredReplicas == 0 && allocation.DesiredHome == nil {
		return
	}
	if allocation.ClusterUID == "" || !candidate.ObservationKnown || candidate.AppliedPlanID != accepted.ID {
		a.known = false
		return
	}
	a.shortfall = a.shortfall || candidate.AdmittedReplicas < allocation.DesiredReplicas
}

// allInputsUnsettled reports whether an All-mode allocation is still being
// worked out: its inventory or placement inputs are pending, or it matched but
// its desired placement was derived from other inputs than the plan's.
func allInputsUnsettled(allocation *v1beta1.CandidateAllocationStatus, inputDigest string) bool {
	return allocation.InventoryPending || allocation.HomeInputsPending ||
		(allocation.Matched && (allocation.DesiredHome == nil || allocation.DesiredHome.InputDigest != inputDigest))
}

// condition is the PlacementSatisfied condition the admission reading gives.
func (a candidateAdmission) condition(accepted *v1beta1.PlacementPlanStatus) policyCondition {
	switch {
	case !planAccountsFor(accepted, a.assigned):
		return satisfactionCondition(corev1.ConditionUnknown, reasonAllocationObservationIncomplete)
	case a.shortfall:
		return satisfactionCondition(corev1.ConditionFalse, reasonAllocationShortfall)
	case a.known:
		return satisfactionCondition(corev1.ConditionTrue, reasonAssignedMinimumsAdmitted)
	}
	return satisfactionCondition(corev1.ConditionUnknown, reasonAwaitingMemberApplication)
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
