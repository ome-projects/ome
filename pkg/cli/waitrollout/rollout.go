// Package waitrollout asserts only qualified canonical reported rollout states.
package waitrollout

import (
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	report "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

// Evaluate consumes one already-read private ISVC, never acquiring other data.
func Evaluate(v *v1beta1.InferenceService, requested report.WaitRequested, now time.Time) (waitengine.Decision, report.WaitRolloutObservation, error) {
	if !requested.IsRollout() {
		return waitengine.Decision{}, report.WaitRolloutObservation{}, &waitengine.Error{Reason: waitengine.ReasonInvalidOptions}
	}
	o, inspectErr := inspect(v, now)
	if inspectErr != nil {
		return waitengine.Decision{}, o, inspectErr
	}
	projected, err := rolloutprojection.Project(v, report.ClockFunc(func() time.Time { return now }))
	if err != nil {
		return waitengine.Decision{}, report.WaitRolloutObservation{}, &waitengine.Error{Reason: waitengine.ReasonInvalidIdentity}
	}
	o.Summary = projected.Content.Summary
	o.Issues = projected.Content.Issues
	for _, issue := range o.Issues {
		switch issue.Code {
		case report.RolloutIssueEpochUnverifiable, report.RolloutIssueAnalysisInconclusive:
		case report.RolloutIssueGroupStatusMissing, report.RolloutIssueComponentStatusMissing, report.RolloutIssueCanaryStatusMissing:
			if o.Validity == "Valid" {
				o.Validity = "Unavailable"
			}
			o.Warnings = appendUnique(o.Warnings, "MissingRolloutEvidence")
		default:
			o.Validity = "Invalid"
			o.Warnings = appendUnique(o.Warnings, "InvalidRolloutRecord")
		}
	}
	wanted := map[report.WaitRequested]report.RolloutState{report.WaitRequestedRolloutStable: report.RolloutStateSucceeded, report.WaitRequestedRolloutFailed: report.RolloutStateFailed, report.WaitRequestedRolloutRolledBack: report.RolloutStateRolledBack}[requested]
	d := waitengine.Decision{Reason: waitengine.ReasonRolloutNotMatched, Matched: o.Validity == "Valid" && o.Summary.ReportedState == wanted}
	if o.Validity == "Invalid" {
		d.Reason = waitengine.ReasonInvalidRollout
	} else if o.Validity == "Unavailable" {
		d.Reason = waitengine.ReasonRolloutNotRecorded
	}
	if d.Matched {
		d.Reason = waitengine.ReasonRolloutMatched
	}
	return d, o.Canonical(), nil
}

func appendUnique(values []report.WaitRolloutWarning, value report.WaitRolloutWarning) []report.WaitRolloutWarning {
	for _, v := range values {
		if v == value {
			return values
		}
	}
	return append(values, value)
}
