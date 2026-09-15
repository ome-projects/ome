// Package waitpredicate evaluates reported conditions, not rollout convergence.
package waitpredicate

import (
	"slices"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"knative.dev/pkg/apis"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

type Warning string

const (
	WarningOversizedRecord  Warning = "OversizedConditionRecord"
	WarningInvalidRecord    Warning = "InvalidConditionRecord"
	WarningFutureTimestamp  Warning = "FutureConditionTimestamp"
	WarningConflictingReady Warning = "ConflictingReadyConditions"
	WarningDuplicateReady   Warning = "DuplicateReadyConditions"
)

type Inspection struct {
	State     string    `json:"state"`
	Total     int       `json:"total"`
	Inspected int       `json:"inspected"`
	Warnings  []Warning `json:"warnings"`
}
type Observation struct {
	Status              string     `json:"status"`
	Validity            string     `json:"validity"`
	GenerationFreshness string     `json:"generationFreshness"`
	Inspection          Inspection `json:"inspection"`
}

// EvaluateReady scans the entire bounded condition set before matching. OME's
// global observedGeneration is not a parent-spec acknowledgement: native status
// does not stamp it and raw/LWS status copies child values. It is never a gate.
// Knative Reason, Message and LastTransitionTime are optional.
func EvaluateReady(v *ome.InferenceService, requested corev1.ConditionStatus, now time.Time) (waitengine.Decision, Observation, error) {
	o := Observation{Status: "NotRecorded", Validity: "Unavailable", GenerationFreshness: "Unverifiable", Inspection: Inspection{State: "Complete", Warnings: []Warning{}}}
	if v == nil || v.UID == "" || v.ResourceVersion == "" || len(utilvalidation.IsDNS1123Subdomain(v.Name)) > 0 || len(utilvalidation.IsDNS1123Label(v.Namespace)) > 0 {
		return waitengine.Decision{}, o, &waitengine.Error{Reason: waitengine.ReasonInvalidIdentity}
	}
	if !validStatus(requested) {
		return waitengine.Decision{}, o, &waitengine.Error{Reason: waitengine.ReasonInvalidOptions}
	}
	conditions := v.Status.Conditions
	o.Inspection.Total = len(conditions)
	if len(conditions) > 64 {
		o.Inspection.State = "LimitExceeded"
		return waitengine.Decision{}, o, &waitengine.Error{Reason: waitengine.ReasonInspectionLimit}
	}
	var first *apis.Condition
	invalidReady := false
	for i := range conditions {
		c := &conditions[i]
		o.Inspection.Inspected++
		valid := inspect(c, now, &o.Inspection)
		if c.Type != apis.ConditionReady {
			continue
		}
		if !valid {
			invalidReady = true
		}
		if first == nil {
			first = c
			continue
		}
		if first.Status != c.Status || first.Severity != c.Severity {
			invalidReady = true
			addWarning(&o.Inspection, WarningConflictingReady)
		} else {
			addWarning(&o.Inspection, WarningDuplicateReady)
		}
	}
	slices.Sort(o.Inspection.Warnings)
	if first == nil {
		return waitengine.Decision{Reason: waitengine.ReasonNotRecorded}, o, nil
	}
	if validStatus(first.Status) {
		o.Status = string(first.Status)
	}
	if invalidReady {
		o.Validity = "Invalid"
		return waitengine.Decision{Reason: waitengine.ReasonInvalidCondition}, o, nil
	}
	o.Validity = "Valid"
	if first.Status == requested {
		return waitengine.Decision{Matched: true, Reason: waitengine.ReasonMatched}, o, nil
	}
	return waitengine.Decision{Reason: waitengine.ReasonNotMatched}, o, nil
}

func validStatus(status corev1.ConditionStatus) bool {
	return status == corev1.ConditionTrue || status == corev1.ConditionFalse || status == corev1.ConditionUnknown
}
func inspect(c *apis.Condition, now time.Time, inspection *Inspection) bool {
	valid := true
	if len(c.Type) > 253 || len(c.Reason) > 1024 || len(c.Message) > 4096 {
		valid = false
		addWarning(inspection, WarningOversizedRecord)
	}
	if c.Type == "" || !validStatus(c.Status) || !utf8.ValidString(string(c.Type)) || !utf8.ValidString(c.Reason) || !utf8.ValidString(c.Message) ||
		(c.Severity != apis.ConditionSeverityError && c.Severity != apis.ConditionSeverityWarning && c.Severity != apis.ConditionSeverityInfo) {
		valid = false
		addWarning(inspection, WarningInvalidRecord)
	}
	if !c.LastTransitionTime.Inner.IsZero() && c.LastTransitionTime.Inner.Time.After(now) {
		valid = false
		addWarning(inspection, WarningFutureTimestamp)
	}
	return valid
}
func addWarning(inspection *Inspection, warning Warning) {
	if !slices.Contains(inspection.Warnings, warning) {
		inspection.Warnings = append(inspection.Warnings, warning)
	}
	inspection.State = "Partial"
}
