package status

import (
	"errors"
	"slices"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"knative.dev/pkg/apis"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitpredicate"
	"sigs.k8s.io/ome/pkg/constants"
)

var errStatusSource = errors.New("status source is invalid")

// projectStatus consumes an already identity-bound snapshot without any reads.
func projectStatus(snapshot *report, clock r.Clock) (r.StatusReport, error) {
	if snapshot == nil || snapshot.ISVC == nil {
		return r.StatusReport{}, errStatusSource
	}
	if clock == nil {
		clock = r.SystemClock{}
	}
	now := clock.Now().UTC()
	v := snapshot.ISVC
	_, ready, err := waitpredicate.EvaluateReady(v, corev1.ConditionTrue, now)
	if err != nil {
		var failure *waitengine.Error
		if !errors.As(err, &failure) || failure.Reason != waitengine.ReasonInspectionLimit {
			return r.StatusReport{}, errStatusSource
		}
		ready.Validity = "Invalid"
	}
	c := r.StatusContent{Generation: v.Generation, ObservedGeneration: v.Status.ObservedGeneration, GenerationFreshness: "Unverifiable", Pods: snapshot.PodObservation, Events: snapshot.EventObservation}
	c.Ready = r.StatusReady{Status: r.StatusReadyState(ready.Status), Validity: r.StatusValidity(ready.Validity), Inspection: r.StatusInspection{State: r.StatusInspectionState(ready.Inspection.State), Total: ready.Inspection.Total, Inspected: ready.Inspection.Inspected, Warnings: []r.StatusIssueCode{}}}
	for _, code := range ready.Inspection.Warnings {
		c.Ready.Inspection.Warnings = append(c.Ready.Inspection.Warnings, r.StatusIssueCode(code))
	}
	if slices.Contains(ready.Inspection.Warnings, waitpredicate.WarningConflictingReady) {
		c.Ready.Status = "NotRecorded"
	}
	if ready.Validity == "Valid" {
		found := false
		for _, condition := range v.Status.Conditions {
			if condition.Type == apis.ConditionReady && (!found || condition.Reason < c.Ready.Reason || condition.Reason == c.Ready.Reason && condition.Message < c.Ready.Message) {
				c.Ready.Reason, c.Ready.Message = condition.Reason, condition.Message
				found = true
			}
		}
	}
	if v.Spec.Runtime != nil {
		c.Runtime = v.Spec.Runtime.Name
	}
	if v.Spec.Model != nil {
		c.Model = v.Spec.Model.Name
	}
	accepted, issues := acceptedStatusPods(snapshot)
	if len(snapshot.PodIssues) <= maxStatusPods {
		c.Issues = append(c.Issues, snapshot.PodIssues...)
	} else {
		c.Issues = append(c.Issues, "CollectionLimitExceeded")
	}
	c.Issues = append(c.Issues, issues...)
	c.Components = projectStatusComponents(snapshot, accepted, now)
	c.RecentEvents, issues = projectStatusEvents(snapshot, accepted, now)
	c.Issues = append(c.Issues, issues...)
	if statusRolloutWithinBounds(v) {
		rollout, projectionErr := rolloutprojection.Project(v, r.ClockFunc(func() time.Time { return now }))
		if projectionErr != nil {
			return r.StatusReport{}, errStatusSource
		}
		c.Rollout.Summary = rollout.Content.Summary
		for _, issue := range rollout.Content.Issues {
			c.Rollout.Issues = append(c.Rollout.Issues, issue.Code)
		}
		for _, warning := range rollout.Warnings {
			c.Rollout.Warnings = append(c.Rollout.Warnings, warning.Code)
		}
	} else {
		c.Issues = append(c.Issues, "CollectionLimitExceeded", "RolloutUnavailable")
	}
	result := r.NewStatusReport(r.Metadata{Name: v.Name, Namespace: v.Namespace}, c, r.ClockFunc(func() time.Time { return now }))
	result.Sources = []r.SourceReference{{Kind: "InferenceService", Name: v.Name, Namespace: v.Namespace, Generation: v.Generation, Evidence: r.EvidenceObserved}}
	return result.Canonical(), nil
}

// Only exact labelled, namespace-bound, unambiguous Pod identities may
// contribute aggregates or Warning Event ownership. No workload inference.
func acceptedStatusPods(snapshot *report) ([]corev1.Pod, []r.StatusIssueCode) {
	issues := []r.StatusIssueCode{}
	result := []corev1.Pod{}
	if len(snapshot.Pods) > 3 {
		return result, []r.StatusIssueCode{"CollectionLimitExceeded"}
	}
	total := 0
	for _, pods := range snapshot.Pods {
		total += len(pods)
		if total > maxStatusPods {
			return result, []r.StatusIssueCode{"CollectionLimitExceeded"}
		}
	}
	if snapshot.PodObservation.State == "Unavailable" {
		return result, issues
	}
	names, uids := map[string]int{}, map[string]int{}
	for _, pods := range snapshot.Pods {
		for _, p := range pods {
			if !statusPrivateIdentity(p.Name, string(p.UID)) {
				continue
			}
			names[p.Name]++
			uids[string(p.UID)]++
		}
	}
	for component, pods := range snapshot.Pods {
		if component != ome.EngineComponent && component != ome.DecoderComponent && component != ome.RouterComponent {
			issues = append(issues, "UnsupportedComponent")
			continue
		}
		for _, p := range pods {
			if p.Namespace != snapshot.ISVC.Namespace || p.Labels[constants.InferenceServiceLabel] != snapshot.ISVC.Name || p.Labels[constants.OMEComponentLabel] != string(component) || !statusPrivateIdentity(p.Name, string(p.UID)) || names[p.Name] != 1 || uids[string(p.UID)] != 1 {
				issues = append(issues, "PodIdentityRejected")
				continue
			}
			if !statusPodShape(p) {
				issues = append(issues, "PodMalformed")
				continue
			}
			result = append(result, p)
		}
	}
	slices.SortFunc(result, func(a, b corev1.Pod) int { return stringsCompare(a.Name, b.Name) })
	return result, issues
}
func stringsCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func statusPrivateIdentity(name, uid string) bool {
	if len(name) > 253 || len(validation.IsDNS1123Subdomain(name)) != 0 || uid == "" || len(uid) > 128 {
		return false
	}
	for _, value := range []byte(uid) {
		if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '-' || value == '_' || value == '.' {
			continue
		}
		return false
	}
	return true
}
func statusPodShape(p corev1.Pod) bool {
	if len(p.Status.Conditions) > 64 || len(p.Status.ContainerStatuses) > 64 || len(p.Status.InitContainerStatuses) > 64 || len(p.Status.EphemeralContainerStatuses) > 64 {
		return false
	}
	var ready corev1.ConditionStatus
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			if c.Status != corev1.ConditionTrue && c.Status != corev1.ConditionFalse && c.Status != corev1.ConditionUnknown || ready != "" && ready != c.Status {
				return false
			}
			ready = c.Status
		}
	}
	for _, statuses := range [][]corev1.ContainerStatus{p.Status.ContainerStatuses, p.Status.InitContainerStatuses, p.Status.EphemeralContainerStatuses} {
		for _, c := range statuses {
			if c.RestartCount < 0 {
				return false
			}
		}
	}
	return true
}
func projectStatusComponents(snapshot *report, pods []corev1.Pod, now time.Time) []r.StatusComponent {
	v := snapshot.ISVC
	rows := []r.StatusComponent{}
	for _, component := range componentOrder {
		declared := component == ome.EngineComponent && v.Spec.Engine != nil || component == ome.DecoderComponent && v.Spec.Decoder != nil || component == ome.RouterComponent && v.Spec.Router != nil
		seen := false
		for _, p := range pods {
			if p.Labels[constants.OMEComponentLabel] == string(component) {
				seen = true
				break
			}
		}
		if !declared && !seen {
			continue
		}
		row := r.StatusComponent{Type: r.RuntimeComponentType(component), Ready: "NotRecorded", Validity: "Unavailable", Evidence: r.EvidenceUnavailable}
		if len(v.Status.Conditions) <= 64 {
			bound := *v
			bound.Status = v.Status
			bound.Status.Conditions = []apis.Condition{}
			kind := apis.ConditionType("EngineReady")
			if component == ome.DecoderComponent {
				kind = "DecoderReady"
			}
			if component == ome.RouterComponent {
				kind = "RouterReady"
			}
			for _, condition := range v.Status.Conditions {
				if condition.Type == kind {
					condition.Type = apis.ConditionReady
					bound.Status.Conditions = append(bound.Status.Conditions, condition)
				}
			}
			_, ready, err := waitpredicate.EvaluateReady(&bound, corev1.ConditionTrue, now)
			if err == nil {
				row.Ready = r.StatusReadyState(ready.Status)
				row.Validity = r.StatusValidity(ready.Validity)
				if slices.Contains(ready.Inspection.Warnings, waitpredicate.WarningConflictingReady) {
					row.Ready = "NotRecorded"
				}
			}
		} else {
			row.Validity = "Invalid"
		}
		if snapshot.PodObservation.State != "Unavailable" {
			row.Evidence = r.EvidenceObserved
		}
		for _, p := range pods {
			if p.Labels[constants.OMEComponentLabel] != string(component) {
				continue
			}
			row.Pods.Total++
			switch p.Status.Phase {
			case corev1.PodRunning:
				row.Pods.Running++
			case corev1.PodPending:
				row.Pods.Pending++
			case corev1.PodFailed:
				row.Pods.Failed++
			case corev1.PodSucceeded:
				row.Pods.Succeeded++
			default:
				row.Pods.Unknown++
			}
			if p.DeletionTimestamp != nil {
				row.Pods.Terminating++
			}
			for _, condition := range p.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					row.Pods.Ready++
					break
				}
			}
			for _, statuses := range [][]corev1.ContainerStatus{p.Status.ContainerStatuses, p.Status.InitContainerStatuses, p.Status.EphemeralContainerStatuses} {
				for _, container := range statuses {
					row.Pods.Restarts += int64(container.RestartCount)
				}
			}
		}
		rows = append(rows, row)
	}
	return rows
}
func projectStatusEvents(snapshot *report, pods []corev1.Pod, now time.Time) ([]r.StatusEvent, []r.StatusIssueCode) {
	rows := []r.StatusEvent{}
	issues := []r.StatusIssueCode{}
	if len(snapshot.Events) > maxStatusEvents {
		return rows, []r.StatusIssueCode{"CollectionLimitExceeded"}
	}
	if snapshot.EventObservation.State == "Unavailable" {
		return rows, issues
	}
	for _, event := range snapshot.Events {
		ref := event.InvolvedObject
		accepted := ref.Kind == "InferenceService" && ref.Name == snapshot.ISVC.Name && ref.UID == snapshot.ISVC.UID
		if ref.Kind == "Pod" {
			for _, p := range pods {
				if ref.Name == p.Name && ref.UID == p.UID {
					accepted = true
					break
				}
			}
		}
		if !accepted || event.Namespace != snapshot.ISVC.Namespace || ref.Namespace != "" && ref.Namespace != snapshot.ISVC.Namespace || event.Type != corev1.EventTypeWarning {
			issues = append(issues, "EventIdentityRejected")
			continue
		}
		if event.Count < 0 || len(event.Reason) > 1024 || len(event.Message) > 4096 || !utf8.ValidString(event.Reason) || !utf8.ValidString(event.Message) {
			issues = append(issues, "EventMalformed")
			continue
		}
		stamp, count, valid := statusEventTime(event, now)
		if !valid {
			issues = append(issues, "EventMalformed")
			continue
		}
		row := r.StatusEvent{Kind: ref.Kind, Name: ref.Name, Reason: event.Reason, Message: event.Message, Count: count}
		if !stamp.IsZero() {
			row.LastSeen = &stamp
		}
		rows = append(rows, row)
	}
	return rows, issues
}
func statusEventTime(event corev1.Event, now time.Time) (time.Time, int32, bool) {
	stamps := []time.Time{event.FirstTimestamp.Time, event.LastTimestamp.Time, event.EventTime.Time}
	count := event.Count
	if event.Series != nil {
		if event.Series.Count < 0 || event.Series.LastObservedTime.IsZero() || !event.EventTime.IsZero() && event.Series.LastObservedTime.Before(&event.EventTime) {
			return time.Time{}, 0, false
		}
		stamps = append(stamps, event.Series.LastObservedTime.Time)
		count = event.Series.Count
	}
	var latest time.Time
	for _, stamp := range stamps {
		if stamp.IsZero() {
			continue
		}
		if stamp.After(now) {
			return time.Time{}, 0, false
		}
		if stamp.After(latest) {
			latest = stamp
		}
	}
	if !event.FirstTimestamp.IsZero() && !event.LastTimestamp.IsZero() && event.LastTimestamp.Before(&event.FirstTimestamp) {
		return time.Time{}, 0, false
	}
	return latest, count, true
}

// Admission windows apply after decoding. The existing canonical rollout
// projector remains the only strategy interpreter; oversized input is omitted.
func statusRolloutWithinBounds(v *ome.InferenceService) bool {
	if len(v.Status.Conditions) > 64 || len(v.Status.Components) > 3 {
		return false
	}
	if v.Spec.Rollout != nil && !statusGroupsWithinBounds(v.Spec.Rollout.Groups) {
		return false
	}
	for _, component := range v.Status.Components {
		if len(component.Traffic) > 3 {
			return false
		}
	}
	if v.Status.Canary != nil && len(v.Status.Canary.MetricResults) > 10 {
		return false
	}
	if v.Status.RolloutCoordination != nil {
		if len(v.Status.RolloutCoordination.Groups) > 3 {
			return false
		}
		for _, group := range v.Status.RolloutCoordination.Groups {
			if len(group.Components) > 3 || len(group.Order) > 3 {
				return false
			}
			if ratio := group.ObservedRatio; ratio != nil && (len(ratio.Original) > 3 || len(ratio.Current) > 3 || len(ratio.NewPods) > 3) {
				return false
			}
		}
	}
	if rollout := v.Status.Rollout; rollout != nil {
		if len(rollout.Groups) > 3 {
			return false
		}
		if run := rollout.ActiveRun; run != nil {
			if len(run.Plan.Groups) > 3 || len(run.TargetRevisions) > 3 {
				return false
			}
			for _, group := range run.Plan.Groups {
				if !statusGroupsWithinBounds([]ome.RolloutGroup{group.Group}) {
					return false
				}
			}
		}
		if run := rollout.LastRun; run != nil && (len(run.Groups) > 3 || len(run.TargetRevisions) > 3) {
			return false
		}
	}
	return true
}
func statusGroupsWithinBounds(groups []ome.RolloutGroup) bool {
	if len(groups) > 3 {
		return false
	}
	for _, group := range groups {
		if len(group.Components) > 3 || len(group.Order) > 3 {
			return false
		}
		if group.Canary != nil {
			if source := group.Canary.Prometheus; source != nil {
				if len(source.Headers) > 16 || len(source.ServerAddress) > 1024 {
					return false
				}
				for key, value := range source.Headers {
					if len(key) > 256 || len(value) > 1024 {
						return false
					}
				}
			}
			if len(group.Canary.Steps) > 20 {
				return false
			}
			for _, step := range group.Canary.Steps {
				if step.Analysis != nil && len(step.Analysis.Metrics) > 10 {
					return false
				}
				if step.Analysis != nil {
					for _, metric := range step.Analysis.Metrics {
						if len(metric.Name) > 253 || len(metric.Query) > 4096 || len(metric.Threshold) > 256 {
							return false
						}
					}
				}
			}
		}
	}
	return true
}
