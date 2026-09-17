package v1alpha1

import "slices"

// StatusAutoscale summarizes controller-reported autoscaling evidence already
// mirrored onto the parent InferenceService. It is not a live scaler probe.
type StatusAutoscale struct {
	Summary    AutoscaleSummary           `json:"summary"`
	Evidence   EvidenceLevel              `json:"evidence"`
	Components []StatusAutoscaleComponent `json:"components"`
	Issues     []AutoscaleIssue           `json:"issues"`
}

// StatusAutoscaleComponent keeps only the facts useful in the overview. The
// dedicated autoscale status command owns targets, conditions, and timestamps.
type StatusAutoscaleComponent struct {
	Type              RuntimeComponentType     `json:"type"`
	State             AutoscaleComponentState  `json:"state"`
	Class             AutoscaleClass           `json:"class"`
	ManagedBy         AutoscaleManagedBy       `json:"managedBy"`
	TargetEvidence    AutoscaleTargetState     `json:"targetEvidence"`
	ReplicaEvidence   AutoscaleReplicaState    `json:"replicaEvidence"`
	ConditionEvidence AutoscaleConditionsState `json:"conditionEvidence"`
	CurrentReplicas   *int32                   `json:"currentReplicas,omitempty"`
	DesiredReplicas   *int32                   `json:"desiredReplicas,omitempty"`
}

func statusAutoscaleCanonical(value StatusAutoscale) (StatusAutoscale, []StatusIssueCode) {
	evidenceInvalid := value.Evidence != EvidenceReported && value.Evidence != EvidenceUnavailable ||
		value.Evidence == EvidenceUnavailable && (value.Summary.State == AutoscaleStateReported ||
			value.Summary.State == AutoscaleStatePartial || len(value.Components) > 0)
	result := StatusAutoscale{
		Summary: AutoscaleSummary{State: clusterEnum(value.Summary.State, AutoscaleStateUnavailable,
			AutoscaleStateReported, AutoscaleStatePartial, AutoscaleStateUnavailable, AutoscaleStateInvalid)},
		Evidence:   clusterEnum(value.Evidence, EvidenceUnavailable, EvidenceReported),
		Components: []StatusAutoscaleComponent{}, Issues: []AutoscaleIssue{},
	}
	if len(value.Components) > 3 || len(value.Issues) > 48 {
		result.Summary.State = AutoscaleStateUnavailable
		result.Evidence = EvidenceUnavailable
		if evidenceInvalid {
			result.Summary.State = AutoscaleStateInvalid
			return result, []StatusIssueCode{"CollectionLimitExceeded", "AutoscaleUnavailable", "UnsupportedData"}
		}
		return result, []StatusIssueCode{"CollectionLimitExceeded", "AutoscaleUnavailable"}
	}
	issues := []StatusIssueCode{}
	for _, row := range value.Components {
		row.Type = canonicalRolloutComponentType(row.Type)
		if row.Type == "" {
			issues = append(issues, "UnsupportedComponent")
			result.Summary.State = AutoscaleStateInvalid
			continue
		}
		originalClass, originalManager, originalConditions := row.Class, row.ManagedBy, row.ConditionEvidence
		row.State = clusterEnum(row.State, AutoscaleComponentInvalid, AutoscaleComponentReported,
			AutoscaleComponentPartial, AutoscaleComponentNotReported, AutoscaleComponentInvalid)
		row.Class = clusterEnum(row.Class, AutoscaleClassUnknown, AutoscaleClassHPA,
			AutoscaleClassKEDA, AutoscaleClassExternal, AutoscaleClassNone, AutoscaleClassUnknown)
		row.ManagedBy = clusterEnum(row.ManagedBy, AutoscaleManagedByUnknown,
			AutoscaleManagedByOME, AutoscaleManagedByExternal, AutoscaleManagedByNone, AutoscaleManagedByUnknown)
		row.TargetEvidence = clusterEnum(row.TargetEvidence, AutoscaleTargetInvalid,
			AutoscaleTargetReported, AutoscaleTargetNotReported, AutoscaleTargetUnavailable, AutoscaleTargetInvalid)
		row.ReplicaEvidence = clusterEnum(row.ReplicaEvidence, AutoscaleReplicasInvalid,
			AutoscaleReplicasReported, AutoscaleReplicasAmbiguous, AutoscaleReplicasNotReported,
			AutoscaleReplicasUnavailable, AutoscaleReplicasInvalid)
		row.ConditionEvidence = clusterEnum(row.ConditionEvidence, AutoscaleConditionsUnavailable,
			AutoscaleConditionsReported, AutoscaleConditionsNotReported,
			AutoscaleConditionsUnavailable, AutoscaleConditionsInvalid)
		if row.ReplicaEvidence == AutoscaleReplicasReported || row.ReplicaEvidence == AutoscaleReplicasAmbiguous {
			if row.CurrentReplicas == nil || row.DesiredReplicas == nil || *row.CurrentReplicas < 0 || *row.DesiredReplicas < 0 {
				row.ReplicaEvidence = AutoscaleReplicasInvalid
				row.CurrentReplicas, row.DesiredReplicas = nil, nil
				issues = append(issues, "AutoscaleUnavailable")
			} else {
				row.CurrentReplicas, row.DesiredReplicas = copyInt32(row.CurrentReplicas), copyInt32(row.DesiredReplicas)
			}
		} else {
			row.CurrentReplicas, row.DesiredReplicas = nil, nil
		}
		if row.Class != originalClass || row.ManagedBy != originalManager || row.ConditionEvidence != originalConditions ||
			row.TargetEvidence == AutoscaleTargetInvalid || row.ReplicaEvidence == AutoscaleReplicasInvalid || row.ConditionEvidence == AutoscaleConditionsInvalid ||
			row.State == AutoscaleComponentReported && (row.Class == AutoscaleClassUnknown || row.ManagedBy == AutoscaleManagedByUnknown) {
			row.State = AutoscaleComponentInvalid
		}
		if row.State == AutoscaleComponentReported && row.ManagedBy == AutoscaleManagedByOME &&
			(row.ConditionEvidence == AutoscaleConditionsNotReported || row.ConditionEvidence == AutoscaleConditionsUnavailable) {
			row.State = AutoscaleComponentPartial
		}
		result.Components = append(result.Components, row)
	}
	slices.SortFunc(result.Components, func(a, b StatusAutoscaleComponent) int {
		return componentRank(a.Type) - componentRank(b.Type)
	})
	components := result.Components
	result.Components = []StatusAutoscaleComponent{}
	for i, row := range components {
		if i > 0 && components[i-1].Type == row.Type || i+1 < len(components) && components[i+1].Type == row.Type {
			issues = append(issues, "UnsupportedComponent")
			result.Summary.State = AutoscaleStateInvalid
			continue
		}
		result.Components = append(result.Components, row)
	}
	for _, issue := range value.Issues {
		if !statusAutoscaleIssueKnown(issue.Code) || issue.Component != "" && canonicalRolloutComponentType(issue.Component) == "" {
			issues = append(issues, "UnsupportedData")
			result.Summary.State = AutoscaleStateInvalid
			continue
		}
		result.Issues = append(result.Issues, issue)
	}
	slices.SortFunc(result.Issues, func(a, b AutoscaleIssue) int {
		if rank := autoscaleComponentRank(a.Component) - autoscaleComponentRank(b.Component); rank != 0 {
			return rank
		}
		if a.Component < b.Component {
			return -1
		}
		if a.Component > b.Component {
			return 1
		}
		if a.Code < b.Code {
			return -1
		}
		if a.Code > b.Code {
			return 1
		}
		return 0
	})
	result.Issues = slices.Compact(result.Issues)
	for _, row := range result.Components {
		switch row.State {
		case AutoscaleComponentInvalid:
			result.Summary.State = AutoscaleStateInvalid
		case AutoscaleComponentPartial, AutoscaleComponentNotReported:
			if result.Summary.State == AutoscaleStateReported {
				result.Summary.State = AutoscaleStatePartial
			}
		}
	}
	if len(result.Components) == 0 && result.Summary.State == AutoscaleStateReported {
		result.Summary.State = AutoscaleStateUnavailable
		result.Evidence = EvidenceUnavailable
		issues = append(issues, "AutoscaleUnavailable")
	}
	if evidenceInvalid {
		result.Summary.State = AutoscaleStateInvalid
		issues = append(issues, "UnsupportedData")
	}
	return result, issues
}

func statusAutoscaleIssueKnown(code AutoscaleIssueCode) bool {
	switch code {
	case AutoscaleIssueUnknownComponentStatus, AutoscaleIssueAutoscalerNotReported,
		AutoscaleIssueScaleTargetNotReported, AutoscaleIssueClassInvalid,
		AutoscaleIssueManagedByInvalid, AutoscaleIssueOwnershipMismatch,
		AutoscaleIssueSpecSourceInvalid, AutoscaleIssueUnexpectedScalerEvidence,
		AutoscaleIssueReplicaEvidenceAmbiguous, AutoscaleIssueReplicaEvidenceInvalid,
		AutoscaleIssueScaleTargetInvalid, AutoscaleIssueConditionInvalid,
		AutoscaleIssueConditionConflict:
		return true
	default:
		return false
	}
}
