package v1alpha1

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const (
	InstanceStatusReportKind = "InstanceStatusReport"
	instanceStatusTextWidth  = 256
)

type InstanceStatusState string

const (
	InstanceStatusStateReported     InstanceStatusState = "Reported"
	InstanceStatusStatePartial      InstanceStatusState = "Partial"
	InstanceStatusStateUnavailable  InstanceStatusState = "Unavailable"
	InstanceStatusStateNotProjected InstanceStatusState = "NotProjected"
	InstanceStatusStateNotOMENative InstanceStatusState = "NotOMENative"
	InstanceStatusStateMissing      InstanceStatusState = "Missing"
)

type InstanceStatusIssueCode string

const (
	InstanceStatusIssueCollectionUnavailable     InstanceStatusIssueCode = "CollectionUnavailable"
	InstanceStatusIssueCollectionTruncated       InstanceStatusIssueCode = "CollectionTruncated"
	InstanceStatusIssueIdentityRejected          InstanceStatusIssueCode = "IdentityRejected"
	InstanceStatusIssueAuthoritativeInvalid      InstanceStatusIssueCode = "AuthoritativeInvalid"
	InstanceStatusIssueInstanceMissing           InstanceStatusIssueCode = "InstanceMissing"
	InstanceStatusIssueNotOMENative              InstanceStatusIssueCode = "NotOMENative"
	InstanceStatusIssueConditionsTruncated       InstanceStatusIssueCode = "ConditionsTruncated"
	InstanceStatusIssueOperationDetailsTruncated InstanceStatusIssueCode = "OperationDetailsTruncated"
	InstanceStatusIssueOperationInvalid          InstanceStatusIssueCode = "OperationInvalid"
	InstanceStatusIssueLastFailureInvalid        InstanceStatusIssueCode = "LastFailureInvalid"
	InstanceStatusIssuePodsUnavailable           InstanceStatusIssueCode = "PodsUnavailable"
	InstanceStatusIssuePodsTruncated             InstanceStatusIssueCode = "PodsTruncated"
	InstanceStatusIssuePodIdentityRejected       InstanceStatusIssueCode = "PodIdentityRejected"
	InstanceStatusIssuePodDetailsTruncated       InstanceStatusIssueCode = "PodDetailsTruncated"
	InstanceStatusIssueEventsUnavailable         InstanceStatusIssueCode = "EventsUnavailable"
	InstanceStatusIssueEventsTruncated           InstanceStatusIssueCode = "EventsTruncated"
	InstanceStatusIssueEventIdentityRejected     InstanceStatusIssueCode = "EventIdentityRejected"
)

type InstanceStatusSummary struct {
	State     InstanceStatusState   `json:"state"`
	Component RuntimeComponentType  `json:"component"`
	Index     int32                 `json:"index"`
	Evidence  InstanceEvidenceState `json:"evidence"`
	Truncated bool                  `json:"truncated"`
}

type InstanceStatusCondition struct {
	Type               string     `json:"type"`
	Status             string     `json:"status"`
	Reason             string     `json:"reason,omitempty"`
	LastTransitionTime *time.Time `json:"lastTransitionTime,omitempty"`
}

type InstanceStatusOperation struct {
	ID              string     `json:"id"`
	Type            string     `json:"type"`
	Step            string     `json:"step"`
	StartedAt       *time.Time `json:"startedAt,omitempty"`
	LastProgressAt  *time.Time `json:"lastProgressAt,omitempty"`
	Deadline        *time.Time `json:"deadline,omitempty"`
	RetryCount      int32      `json:"retryCount"`
	TargetRevision  string     `json:"targetRevision,omitempty"`
	Reason          string     `json:"reason,omitempty"`
	SurgeIndex      *int32     `json:"surgeIndex,omitempty"`
	FromNode        string     `json:"fromNode,omitempty"`
	TargetNodeHints []string   `json:"targetNodeHints"`
	RequestUUID     string     `json:"requestUUID,omitempty"`
}

type InstanceStatusFailure struct {
	PodName       string     `json:"podName"`
	ContainerName string     `json:"containerName,omitempty"`
	Reason        string     `json:"reason,omitempty"`
	ExitCode      *int32     `json:"exitCode,omitempty"`
	Time          *time.Time `json:"time,omitempty"`
}

type InstanceStatusInstance struct {
	InferenceReplica string                    `json:"inferenceReplica"`
	Index            int32                     `json:"index"`
	Incarnation      int64                     `json:"incarnation"`
	Phase            InstancePhase             `json:"phase"`
	RunningRevision  string                    `json:"runningRevision,omitempty"`
	TargetRevision   string                    `json:"targetRevision,omitempty"`
	Pods             InstancePodCounts         `json:"pods"`
	Admitted         bool                      `json:"admitted"`
	Conditions       []InstanceStatusCondition `json:"conditions"`
	Operation        *InstanceStatusOperation  `json:"operation,omitempty"`
	LastFailure      *InstanceStatusFailure    `json:"lastFailure,omitempty"`
}

type InstanceStatusPod struct {
	Name         string `json:"name"`
	Runner       string `json:"runner,omitempty"`
	Revision     string `json:"revision,omitempty"`
	Incarnation  int64  `json:"incarnation"`
	Phase        string `json:"phase"`
	Ready        bool   `json:"ready"`
	ServingReady bool   `json:"servingReady"`
	Node         string `json:"node,omitempty"`
	RestartCount int32  `json:"restartCount"`
	Deleting     bool   `json:"deleting"`
}

type InstanceStatusEvent struct {
	TargetKind string     `json:"targetKind"`
	TargetName string     `json:"targetName"`
	Reason     string     `json:"reason"`
	Count      int32      `json:"count"`
	FirstSeen  *time.Time `json:"firstSeen,omitempty"`
	LastSeen   *time.Time `json:"lastSeen,omitempty"`
}

type InstanceStatusIssue struct {
	Code              InstanceStatusIssueCode `json:"code"`
	UnavailableReason UnavailableReason       `json:"unavailableReason,omitempty"`
}

type InstanceStatusWarning struct {
	Code WarningCode `json:"code"`
}

type InstanceStatusContent struct {
	Summary  InstanceStatusSummary   `json:"summary"`
	Instance *InstanceStatusInstance `json:"instance,omitempty"`
	Pods     []InstanceStatusPod     `json:"pods"`
	Events   []InstanceStatusEvent   `json:"events"`
	Issues   []InstanceStatusIssue   `json:"issues"`
}

type InstanceStatusReport struct {
	APIVersion  string                  `json:"apiVersion"`
	Kind        string                  `json:"kind"`
	Metadata    Metadata                `json:"metadata"`
	CollectedAt time.Time               `json:"collectedAt"`
	Sources     []SourceReference       `json:"sources"`
	Content     InstanceStatusContent   `json:"content"`
	Warnings    []InstanceStatusWarning `json:"warnings"`
}

func NewInstanceStatusReport(metadata Metadata, content InstanceStatusContent, clock Clock) InstanceStatusReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (InstanceStatusReport{
		APIVersion: APIVersion, Kind: InstanceStatusReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []SourceReference{}, Content: content,
		Warnings: []InstanceStatusWarning{},
	}).Canonical()
}

func (r InstanceStatusReport) Canonical() InstanceStatusReport {
	out := r
	out.APIVersion = APIVersion
	out.Kind = InstanceStatusReportKind
	out.CollectedAt = r.CollectedAt.UTC()
	out.Metadata.Name = safeInstanceStatusText(r.Metadata.Name, 253)
	out.Metadata.Namespace = safeInstanceStatusText(r.Metadata.Namespace, 63)
	out.Sources = append([]SourceReference{}, r.Sources...)
	for i := range out.Sources {
		source := &out.Sources[i]
		source.Kind = safeInstanceStatusText(source.Kind, 64)
		source.Namespace = safeInstanceStatusText(source.Namespace, 63)
		source.Name = safeInstanceStatusText(source.Name, 253)
		source.UID = safeInstanceStatusText(source.UID, 128)
		source.ResourceVersion = ""
		if source.CollectedAt.IsZero() {
			source.CollectedAt = out.CollectedAt
		} else {
			source.CollectedAt = source.CollectedAt.UTC()
		}
	}
	sort.SliceStable(out.Sources, func(i, j int) bool { return sourceLess(out.Sources[i], out.Sources[j]) })
	out.Content = canonicalInstanceStatusContent(r.Content)
	out.Warnings = append([]InstanceStatusWarning{}, r.Warnings...)
	sort.Slice(out.Warnings, func(i, j int) bool { return out.Warnings[i].Code < out.Warnings[j].Code })
	out.Warnings = slices.Compact(out.Warnings)
	return out
}

func canonicalInstanceStatusContent(in InstanceStatusContent) InstanceStatusContent {
	out := in
	out.Pods = append([]InstanceStatusPod{}, in.Pods...)
	for i := range out.Pods {
		pod := &out.Pods[i]
		pod.Name = safeInstanceStatusText(pod.Name, 253)
		pod.Runner = safeInstanceStatusText(pod.Runner, 63)
		pod.Revision = safeInstanceStatusText(pod.Revision, 253)
		pod.Phase = safeInstanceStatusText(pod.Phase, 32)
		pod.Node = safeInstanceStatusText(pod.Node, 253)
	}
	sort.SliceStable(out.Pods, func(i, j int) bool {
		return cmp.Or(cmp.Compare(out.Pods[i].Name, out.Pods[j].Name), cmp.Compare(out.Pods[i].Runner, out.Pods[j].Runner)) < 0
	})
	out.Events = append([]InstanceStatusEvent{}, in.Events...)
	for i := range out.Events {
		event := &out.Events[i]
		event.TargetKind = safeInstanceStatusText(event.TargetKind, 64)
		event.TargetName = safeInstanceStatusText(event.TargetName, 253)
		event.Reason = safeInstanceStatusText(event.Reason, 128)
		event.FirstSeen = copyInstanceStatusTime(event.FirstSeen)
		event.LastSeen = copyInstanceStatusTime(event.LastSeen)
	}
	sort.SliceStable(out.Events, func(i, j int) bool {
		a, b := out.Events[i], out.Events[j]
		return cmp.Or(cmp.Compare(a.TargetKind, b.TargetKind), cmp.Compare(a.TargetName, b.TargetName), cmp.Compare(a.Reason, b.Reason), cmp.Compare(a.Count, b.Count)) < 0
	})
	out.Issues = append([]InstanceStatusIssue{}, in.Issues...)
	sort.Slice(out.Issues, func(i, j int) bool {
		return cmp.Or(cmp.Compare(out.Issues[i].Code, out.Issues[j].Code), cmp.Compare(out.Issues[i].UnavailableReason, out.Issues[j].UnavailableReason)) < 0
	})
	out.Issues = slices.Compact(out.Issues)
	if in.Instance == nil {
		out.Instance = nil
		return out
	}
	instance := *in.Instance
	instance.InferenceReplica = safeInstanceStatusText(instance.InferenceReplica, 253)
	instance.RunningRevision = safeInstanceStatusText(instance.RunningRevision, 253)
	instance.TargetRevision = safeInstanceStatusText(instance.TargetRevision, 253)
	instance.Conditions = append([]InstanceStatusCondition{}, in.Instance.Conditions...)
	for i := range instance.Conditions {
		condition := &instance.Conditions[i]
		condition.Type = safeInstanceStatusText(condition.Type, 128)
		condition.Status = safeInstanceStatusText(condition.Status, 16)
		condition.Reason = safeInstanceStatusText(condition.Reason, 128)
		condition.LastTransitionTime = copyInstanceStatusTime(condition.LastTransitionTime)
	}
	sort.SliceStable(instance.Conditions, func(i, j int) bool {
		a, b := instance.Conditions[i], instance.Conditions[j]
		return cmp.Or(cmp.Compare(a.Type, b.Type), cmp.Compare(a.Status, b.Status), cmp.Compare(a.Reason, b.Reason)) < 0
	})
	if in.Instance.Operation != nil {
		operation := *in.Instance.Operation
		operation.ID = safeInstanceStatusText(operation.ID, 128)
		operation.Type = safeInstanceStatusText(operation.Type, 32)
		operation.Step = safeInstanceStatusText(operation.Step, 128)
		operation.TargetRevision = safeInstanceStatusText(operation.TargetRevision, 253)
		operation.Reason = safeInstanceStatusText(operation.Reason, instanceStatusTextWidth)
		operation.FromNode = safeInstanceStatusText(operation.FromNode, 253)
		operation.RequestUUID = safeInstanceStatusText(operation.RequestUUID, 128)
		operation.StartedAt = copyInstanceStatusTime(operation.StartedAt)
		operation.LastProgressAt = copyInstanceStatusTime(operation.LastProgressAt)
		operation.Deadline = copyInstanceStatusTime(operation.Deadline)
		operation.SurgeIndex = copyInstanceStatusInt32(operation.SurgeIndex)
		operation.TargetNodeHints = append([]string{}, in.Instance.Operation.TargetNodeHints...)
		for i := range operation.TargetNodeHints {
			operation.TargetNodeHints[i] = safeInstanceStatusText(operation.TargetNodeHints[i], 253)
		}
		sort.Strings(operation.TargetNodeHints)
		operation.TargetNodeHints = slices.Compact(operation.TargetNodeHints)
		instance.Operation = &operation
	}
	if in.Instance.LastFailure != nil {
		failure := *in.Instance.LastFailure
		failure.PodName = safeInstanceStatusText(failure.PodName, 253)
		failure.ContainerName = safeInstanceStatusText(failure.ContainerName, 63)
		failure.Reason = safeInstanceStatusText(failure.Reason, 128)
		failure.ExitCode = copyInstanceStatusInt32(failure.ExitCode)
		failure.Time = copyInstanceStatusTime(failure.Time)
		instance.LastFailure = &failure
	}
	out.Instance = &instance
	return out
}

func (r InstanceStatusReport) Table() report.Table {
	c := r.Canonical()
	table := report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: [][]string{}}
	add := func(field, value string) {
		table.Rows = append(table.Rows, []string{printers.BoundedCell(field, 12), printers.BoundedCell(value, 64)})
	}
	add("state", fmt.Sprintf("%s %s[%d] evidence=%s", c.Content.Summary.State, c.Content.Summary.Component, c.Content.Summary.Index, c.Content.Summary.Evidence))
	if instance := c.Content.Instance; instance != nil {
		add("instance", fmt.Sprintf("%s inc=%d phase=%s admitted=%t", instance.InferenceReplica, instance.Incarnation, instance.Phase, instance.Admitted))
		add("revisions", fmt.Sprintf("running=%s target=%s", dash(instance.RunningRevision), dash(instance.TargetRevision)))
		add("persisted", fmt.Sprintf("pods=%d serving=%d available=%d", instance.Pods.Total, instance.Pods.Serving, instance.Pods.Available))
		for _, condition := range instance.Conditions {
			add("condition", fmt.Sprintf("%s=%s reason=%s", condition.Type, condition.Status, dash(condition.Reason)))
		}
		if operation := instance.Operation; operation != nil {
			add("operation", fmt.Sprintf("%s id=%s step=%s retry=%d", operation.Type, operation.ID, operation.Step, operation.RetryCount))
			add("op target", fmt.Sprintf("revision=%s reason=%s", dash(operation.TargetRevision), dash(operation.Reason)))
			add("op timing", fmt.Sprintf("start=%s progress=%s deadline=%s", statusTime(operation.StartedAt), statusTime(operation.LastProgressAt), statusTime(operation.Deadline)))
			add("op nodes", fmt.Sprintf("from=%s surge=%s hints=%s", dash(operation.FromNode), statusInt32(operation.SurgeIndex), dash(strings.Join(operation.TargetNodeHints, ","))))
		}
		if failure := instance.LastFailure; failure != nil {
			add("failure", fmt.Sprintf("pod=%s container=%s reason=%s exit=%s", failure.PodName, dash(failure.ContainerName), dash(failure.Reason), statusInt32(failure.ExitCode)))
			add("fail time", statusTime(failure.Time))
		}
	}
	for _, pod := range c.Content.Pods {
		add("pod", fmt.Sprintf("%s %s/%s ready=%t serving=%t restarts=%d node=%s deleting=%t", pod.Name, pod.Runner, pod.Phase, pod.Ready, pod.ServingReady, pod.RestartCount, dash(pod.Node), pod.Deleting))
	}
	for _, event := range c.Content.Events {
		add("event", fmt.Sprintf("%s/%s reason=%s count=%d", event.TargetKind, event.TargetName, event.Reason, event.Count))
	}
	for _, issue := range c.Content.Issues {
		value := string(issue.Code)
		if issue.UnavailableReason != "" {
			value += " reason=" + string(issue.UnavailableReason)
		}
		add("issue", value)
	}
	return table
}

func safeInstanceStatusText(value string, width int) string {
	lower := strings.ToLower(value)
	for _, marker := range []string{"bearer ", "token", "password", "secret", "authorization", "credential", "api_key", "apikey", "private key"} {
		if strings.Contains(lower, marker) {
			return "[REDACTED]"
		}
	}
	return printers.BoundedCell(value, width)
}

func copyInstanceStatusTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func copyInstanceStatusInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func dash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func statusTime(value *time.Time) string {
	if value == nil {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func statusInt32(value *int32) string {
	if value == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *value)
}
