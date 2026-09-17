package v1alpha1

import "slices"

// WaitRolloutObservation records only message-free rollout evidence. The
// canonical summary's reported state remains qualified by its epoch/evidence.
type WaitRolloutObservation struct {
	Validity   string                `json:"validity"`
	Summary    RolloutSummary        `json:"summary"`
	Issues     []RolloutIssue        `json:"issues"`
	Inspection WaitRolloutInspection `json:"inspection"`
	Warnings   []WaitRolloutWarning  `json:"warnings"`
}

type WaitRolloutWarning string

type WaitRolloutInspection struct {
	State        string `json:"state"`
	Conditions   int    `json:"conditions"`
	Components   int    `json:"components"`
	Groups       int    `json:"groups"`
	PinnedGroups int    `json:"pinnedGroups"`
	Targets      int    `json:"targets"`
}

func (r WaitRequested) IsRollout() bool {
	return r == WaitRequestedRolloutStable || r == WaitRequestedRolloutFailed || r == WaitRequestedRolloutRolledBack
}

func (o WaitRolloutObservation) Canonical() WaitRolloutObservation {
	if o.Validity != "Valid" && o.Validity != "Unavailable" {
		o.Validity = "Invalid"
	}
	if o.Validity == "Unavailable" && o.Inspection.State == "NotInspected" && o.Summary.CoordinationReady == "" {
		o.Summary.CoordinationReady = RolloutConditionUnobserved
	}
	o.Summary = (RolloutStatusContent{Summary: o.Summary}).Canonical().Summary
	if len(o.Issues) > 64 || len(o.Warnings) > 9 {
		o.Validity = "Invalid"
		o.Inspection.State = "LimitExceeded"
		o.Issues = nil
		o.Warnings = nil
	}
	issues := make([]RolloutIssue, 0, len(o.Issues))
	for _, issue := range o.Issues {
		issue.Code = canonicalRolloutIssueCode(issue.Code)
		issue.Component = canonicalRolloutComponentType(issue.Component)
		if issue.Group != nil {
			group := *issue.Group
			if group < 0 || group > 2 {
				o.Validity = "Invalid"
				issue.Group = nil
			} else {
				issue.Group = &group
			}
		}
		issues = append(issues, issue)
	}
	o.Issues = (RolloutStatusContent{Issues: issues}).Canonical().Issues
	warnings := []WaitRolloutWarning{}
	for _, w := range o.Warnings {
		switch w {
		case "InvalidConditionRecord", "ConflictingReadyConditions", "FutureConditionTimestamp", "OversizedConditionRecord", "InvalidPinnedRun", "InvalidRolloutRecord", "StaleReportedCondition", "MissingRolloutEvidence", "InvalidRolloutTimestamp":
			if !slices.Contains(warnings, w) {
				warnings = append(warnings, w)
			}
		}
	}
	slices.Sort(warnings)
	o.Warnings = warnings
	switch o.Inspection.State {
	case "Complete", "NotInspected", "Invalid", "LimitExceeded":
	default:
		o.Inspection.State = "Invalid"
	}
	o.Inspection.Conditions = max(0, min(64, o.Inspection.Conditions))
	o.Inspection.Components = max(0, min(3, o.Inspection.Components))
	o.Inspection.Groups = max(0, min(3, o.Inspection.Groups))
	o.Inspection.PinnedGroups = max(0, min(3, o.Inspection.PinnedGroups))
	o.Inspection.Targets = max(0, min(3, o.Inspection.Targets))
	return o
}
