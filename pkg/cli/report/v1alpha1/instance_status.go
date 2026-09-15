package v1alpha1

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/safetext"
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
	InstanceStatusIssueDuplicateComponent        InstanceStatusIssueCode = "DuplicateComponent"
	InstanceStatusIssueParentGenerationMissing   InstanceStatusIssueCode = "ParentGenerationMissing"
	InstanceStatusIssueParentGenerationInvalid   InstanceStatusIssueCode = "ParentGenerationInvalid"
	InstanceStatusIssueParentGenerationStale     InstanceStatusIssueCode = "ParentGenerationStale"
	InstanceStatusIssueParentGenerationAhead     InstanceStatusIssueCode = "ParentGenerationAhead"
	InstanceStatusIssueStatusUnobserved          InstanceStatusIssueCode = "StatusUnobserved"
	InstanceStatusIssueObservedGenerationInvalid InstanceStatusIssueCode = "ObservedGenerationInvalid"
	InstanceStatusIssueStaleGeneration           InstanceStatusIssueCode = "StaleGeneration"
	InstanceStatusIssueRevisionInvalid           InstanceStatusIssueCode = "RevisionInvalid"
	InstanceStatusIssueAggregateInvalid          InstanceStatusIssueCode = "AggregateInvalid"
	InstanceStatusIssueStatusesNotReported       InstanceStatusIssueCode = "StatusesNotReported"
	InstanceStatusIssueStatusRowsTruncated       InstanceStatusIssueCode = "StatusRowsTruncated"
	InstanceStatusIssueOutputTruncated           InstanceStatusIssueCode = "OutputTruncated"
	InstanceStatusIssueSparseIndices             InstanceStatusIssueCode = "SparseIndices"
	InstanceStatusIssueDuplicateIndex            InstanceStatusIssueCode = "DuplicateIndex"
	InstanceStatusIssueIndexInvalid              InstanceStatusIssueCode = "IndexInvalid"
	InstanceStatusIssuePhaseInvalid              InstanceStatusIssueCode = "PhaseInvalid"
	InstanceStatusIssueInstanceRevisionInvalid   InstanceStatusIssueCode = "InstanceRevisionInvalid"
	InstanceStatusIssuePodCountsInvalid          InstanceStatusIssueCode = "PodCountsInvalid"
	InstanceStatusIssueIncarnationInvalid        InstanceStatusIssueCode = "IncarnationInvalid"
	InstanceStatusIssueInstanceMissing           InstanceStatusIssueCode = "InstanceMissing"
	InstanceStatusIssueNotOMENative              InstanceStatusIssueCode = "NotOMENative"
	InstanceStatusIssueConditionsTruncated       InstanceStatusIssueCode = "ConditionsTruncated"
	InstanceStatusIssueConditionGenerationStale  InstanceStatusIssueCode = "ConditionGenerationStale"
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
	InstanceStatusIssueMigrationsTruncated       InstanceStatusIssueCode = "MigrationsTruncated"
	InstanceStatusIssueMigrationInvalid          InstanceStatusIssueCode = "MigrationInvalid"
	InstanceStatusIssueEncodingUnsupported       InstanceStatusIssueCode = "EncodingUnsupported"
)

type InstanceStatusSummary struct {
	State     InstanceStatusState   `json:"state"`
	Component RuntimeComponentType  `json:"component"`
	Index     int32                 `json:"index"`
	Evidence  InstanceEvidenceState `json:"evidence"`
	Truncated bool                  `json:"truncated"`
}

type InstanceStatusDeployment struct {
	Mode              DeploymentMode       `json:"mode,omitempty"`
	Source            DeploymentModeSource `json:"source,omitempty"`
	Origin            string               `json:"origin,omitempty"`
	Evidence          EvidenceLevel        `json:"evidence"`
	UnavailableReason UnavailableReason    `json:"unavailableReason,omitempty"`
}

type InstanceStatusEncoding struct {
	Name              string            `json:"name,omitempty"`
	Evidence          EvidenceLevel     `json:"evidence"`
	UnavailableReason UnavailableReason `json:"unavailableReason,omitempty"`
}

type InstanceStatusCondition struct {
	Type               string                `json:"type"`
	Status             string                `json:"status"`
	ObservedGeneration int64                 `json:"observedGeneration"`
	Evidence           InstanceEvidenceState `json:"evidence"`
	Reason             string                `json:"reason,omitempty"`
	LastTransitionTime *time.Time            `json:"lastTransitionTime,omitempty"`
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

type InstanceStatusMigration struct {
	RequestUUID     string     `json:"requestUUID"`
	Role            string     `json:"role"`
	Trigger         string     `json:"trigger"`
	SourceInstance  int32      `json:"sourceInstance"`
	SurgeInstance   *int32     `json:"surgeInstance,omitempty"`
	Phase           string     `json:"phase"`
	AllocatedAt     *time.Time `json:"allocatedAt,omitempty"`
	FromNode        string     `json:"fromNode,omitempty"`
	TargetNodeHints []string   `json:"targetNodeHints"`
	Attempt         int32      `json:"attempt"`
	Reason          string     `json:"reason,omitempty"`
	Message         string     `json:"message,omitempty"`
	StartedAt       *time.Time `json:"startedAt,omitempty"`
	Deadline        *time.Time `json:"deadline,omitempty"`
	CompletedAt     *time.Time `json:"completedAt,omitempty"`
	Succeeded       *bool      `json:"succeeded,omitempty"`
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
	ReadySince       *time.Time                `json:"readySince,omitempty"`
	ActiveOrdinal    *int32                    `json:"activeOrdinal,omitempty"`
	Conditions       []InstanceStatusCondition `json:"conditions"`
	Migrations       []InstanceStatusMigration `json:"migrations"`
	Operation        *InstanceStatusOperation  `json:"operation,omitempty"`
	LastFailure      *InstanceStatusFailure    `json:"lastFailure,omitempty"`
}

type InstanceStatusPod struct {
	Name         string `json:"name"`
	Runner       string `json:"runner,omitempty"`
	Revision     string `json:"revision,omitempty"`
	Incarnation  int64  `json:"incarnation"`
	Phase        string `json:"phase"`
	Ready        string `json:"ready"`
	ServingReady string `json:"servingReady"`
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
	Summary    InstanceStatusSummary    `json:"summary"`
	Deployment InstanceStatusDeployment `json:"deployment"`
	Encoding   InstanceStatusEncoding   `json:"encoding"`
	Instance   *InstanceStatusInstance  `json:"instance,omitempty"`
	Pods       []InstanceStatusPod      `json:"pods"`
	Events     []InstanceStatusEvent    `json:"events"`
	Issues     []InstanceStatusIssue    `json:"issues"`
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
	if out.Deployment.Evidence == "" {
		out.Deployment.Evidence = EvidenceUnavailable
	}
	if out.Encoding.Evidence == "" {
		out.Encoding.Evidence = EvidenceUnavailable
	}
	out.Encoding.Name = safeInstanceStatusText(out.Encoding.Name, 32)
	out.Deployment.Origin = safeInstanceStatusText(out.Deployment.Origin, 32)
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
		a, b := out.Pods[i], out.Pods[j]
		return cmp.Or(
			cmp.Compare(a.Name, b.Name), cmp.Compare(a.Runner, b.Runner),
			cmp.Compare(a.Revision, b.Revision), cmp.Compare(a.Incarnation, b.Incarnation),
			cmp.Compare(a.Phase, b.Phase), cmp.Compare(a.Ready, b.Ready),
			cmp.Compare(a.ServingReady, b.ServingReady), cmp.Compare(a.Node, b.Node),
			cmp.Compare(a.RestartCount, b.RestartCount), compareInstanceStatusBool(a.Deleting, b.Deleting),
		) < 0
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
		return cmp.Or(
			cmp.Compare(a.TargetKind, b.TargetKind), cmp.Compare(a.TargetName, b.TargetName),
			cmp.Compare(a.Reason, b.Reason), cmp.Compare(a.Count, b.Count),
			compareInstanceStatusTime(a.FirstSeen, b.FirstSeen), compareInstanceStatusTime(a.LastSeen, b.LastSeen),
		) < 0
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
	instance.ReadySince = copyInstanceStatusTime(instance.ReadySince)
	instance.ActiveOrdinal = copyInstanceStatusInt32(instance.ActiveOrdinal)
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
		return cmp.Or(
			cmp.Compare(a.Type, b.Type), cmp.Compare(a.Status, b.Status), cmp.Compare(a.ObservedGeneration, b.ObservedGeneration), cmp.Compare(a.Evidence, b.Evidence),
			cmp.Compare(a.Reason, b.Reason), compareInstanceStatusTime(a.LastTransitionTime, b.LastTransitionTime),
		) < 0
	})
	instance.Migrations = append([]InstanceStatusMigration{}, in.Instance.Migrations...)
	for i := range instance.Migrations {
		migration := &instance.Migrations[i]
		migration.RequestUUID = safeInstanceStatusText(migration.RequestUUID, 128)
		migration.Role = safeInstanceStatusText(migration.Role, 16)
		migration.Trigger = safeInstanceStatusText(migration.Trigger, 16)
		migration.Phase = safeInstanceStatusText(migration.Phase, 32)
		migration.FromNode = safeInstanceStatusText(migration.FromNode, 253)
		migration.Reason = safeInstanceStatusText(migration.Reason, 128)
		migration.Message = safeInstanceStatusText(migration.Message, instanceStatusTextWidth)
		migration.SurgeInstance = copyInstanceStatusInt32(migration.SurgeInstance)
		migration.AllocatedAt = copyInstanceStatusTime(migration.AllocatedAt)
		migration.StartedAt = copyInstanceStatusTime(migration.StartedAt)
		migration.Deadline = copyInstanceStatusTime(migration.Deadline)
		migration.CompletedAt = copyInstanceStatusTime(migration.CompletedAt)
		migration.Succeeded = copyInstanceStatusBool(migration.Succeeded)
		migration.TargetNodeHints = append([]string{}, migration.TargetNodeHints...)
		for j := range migration.TargetNodeHints {
			migration.TargetNodeHints[j] = safeInstanceStatusText(migration.TargetNodeHints[j], 253)
		}
		sort.Strings(migration.TargetNodeHints)
		migration.TargetNodeHints = slices.Compact(migration.TargetNodeHints)
	}
	sort.SliceStable(instance.Migrations, func(i, j int) bool {
		a, b := instance.Migrations[i], instance.Migrations[j]
		return cmp.Or(
			cmp.Compare(a.RequestUUID, b.RequestUUID), cmp.Compare(a.Role, b.Role),
			cmp.Compare(a.Trigger, b.Trigger), cmp.Compare(a.SourceInstance, b.SourceInstance),
			compareInstanceStatusInt32(a.SurgeInstance, b.SurgeInstance), cmp.Compare(a.Phase, b.Phase),
			compareInstanceStatusTime(a.AllocatedAt, b.AllocatedAt), cmp.Compare(a.FromNode, b.FromNode),
			cmp.Compare(strings.Join(a.TargetNodeHints, "\x00"), strings.Join(b.TargetNodeHints, "\x00")),
			cmp.Compare(a.Attempt, b.Attempt), cmp.Compare(a.Reason, b.Reason), cmp.Compare(a.Message, b.Message),
			compareInstanceStatusTime(a.StartedAt, b.StartedAt), compareInstanceStatusTime(a.Deadline, b.Deadline),
			compareInstanceStatusTime(a.CompletedAt, b.CompletedAt), compareInstanceStatusBoolPointer(a.Succeeded, b.Succeeded),
		) < 0
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
	add("deployment", fmt.Sprintf("mode=%s source=%s origin=%s evidence=%s", instanceStatusDash(string(c.Content.Deployment.Mode)), instanceStatusDash(string(c.Content.Deployment.Source)), instanceStatusDash(c.Content.Deployment.Origin), c.Content.Deployment.Evidence))
	add("encoding", fmt.Sprintf("name=%s evidence=%s reason=%s", instanceStatusDash(c.Content.Encoding.Name), c.Content.Encoding.Evidence, instanceStatusDash(string(c.Content.Encoding.UnavailableReason))))
	if instance := c.Content.Instance; instance != nil {
		add("instance", fmt.Sprintf("%s inc=%d phase=%s admitted=%t", instance.InferenceReplica, instance.Incarnation, instance.Phase, instance.Admitted))
		add("revisions", fmt.Sprintf("running=%s target=%s", instanceStatusDash(instance.RunningRevision), instanceStatusDash(instance.TargetRevision)))
		add("persisted", fmt.Sprintf("pods=%d serving=%d available=%d", instance.Pods.Total, instance.Pods.Serving, instance.Pods.Available))
		add("lifecycle", fmt.Sprintf("activeOrdinal=%s readySince=%s", statusInt32(instance.ActiveOrdinal), statusTime(instance.ReadySince)))
		for _, condition := range instance.Conditions {
			add("condition", fmt.Sprintf("%s=%s gen=%d evidence=%s reason=%s", condition.Type, condition.Status, condition.ObservedGeneration, condition.Evidence, instanceStatusDash(condition.Reason)))
		}
		if operation := instance.Operation; operation != nil {
			add("operation", fmt.Sprintf("%s id=%s step=%s retry=%d", operation.Type, operation.ID, operation.Step, operation.RetryCount))
			add("op target", fmt.Sprintf("revision=%s reason=%s", instanceStatusDash(operation.TargetRevision), instanceStatusDash(operation.Reason)))
			add("op timing", fmt.Sprintf("start=%s progress=%s deadline=%s", statusTime(operation.StartedAt), statusTime(operation.LastProgressAt), statusTime(operation.Deadline)))
			add("op nodes", fmt.Sprintf("from=%s surge=%s hints=%s", instanceStatusDash(operation.FromNode), statusInt32(operation.SurgeIndex), instanceStatusDash(strings.Join(operation.TargetNodeHints, ","))))
		}
		if failure := instance.LastFailure; failure != nil {
			add("failure", fmt.Sprintf("pod=%s container=%s reason=%s exit=%s", failure.PodName, instanceStatusDash(failure.ContainerName), instanceStatusDash(failure.Reason), statusInt32(failure.ExitCode)))
			add("fail time", statusTime(failure.Time))
		}
		for _, migration := range instance.Migrations {
			add("migration", fmt.Sprintf("%s role=%s phase=%s source=%d surge=%s", migration.RequestUUID, migration.Role, migration.Phase, migration.SourceInstance, statusInt32(migration.SurgeInstance)))
		}
	}
	for _, pod := range c.Content.Pods {
		add("pod", fmt.Sprintf("%s %s/%s ready=%s serving=%s restarts=%d node=%s deleting=%t", pod.Name, pod.Runner, pod.Phase, pod.Ready, pod.ServingReady, pod.RestartCount, instanceStatusDash(pod.Node), pod.Deleting))
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

// WideTable renders the same canonical report without compact-table identity
// elision. Values remain bounded by the typed report's canonical limits.
func (r InstanceStatusReport) WideTable() report.Table {
	c := r.Canonical()
	table := report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: [][]string{}}
	add := func(field, value string) {
		table.Rows = append(table.Rows, []string{printers.BoundedCell(field, 32), printers.BoundedCell(value, 256)})
	}
	add("apiVersion", c.APIVersion)
	add("kind", c.Kind)
	add("metadata namespace", instanceStatusDash(c.Metadata.Namespace))
	add("metadata name", instanceStatusDash(c.Metadata.Name))
	add("collected at", statusTime(&c.CollectedAt))
	for index, source := range c.Sources {
		prefix := fmt.Sprintf("source[%d] ", index)
		add(prefix+"kind", source.Kind)
		add(prefix+"namespace", instanceStatusDash(source.Namespace))
		add(prefix+"name", source.Name)
		add(prefix+"uid", instanceStatusDash(source.UID))
		add(prefix+"generation", strconv.FormatInt(source.Generation, 10))
		add(prefix+"evidence", string(source.Evidence))
		add(prefix+"collected at", statusTime(&source.CollectedAt))
		add(prefix+"unavailable", instanceStatusDash(string(source.UnavailableReason)))
	}
	add("state", string(c.Content.Summary.State))
	add("component", string(c.Content.Summary.Component))
	add("index", strconv.FormatInt(int64(c.Content.Summary.Index), 10))
	add("evidence", string(c.Content.Summary.Evidence))
	add("truncated", strconv.FormatBool(c.Content.Summary.Truncated))
	add("deployment mode", instanceStatusDash(string(c.Content.Deployment.Mode)))
	add("deployment source", instanceStatusDash(string(c.Content.Deployment.Source)))
	add("deployment origin", instanceStatusDash(c.Content.Deployment.Origin))
	add("deployment evidence", string(c.Content.Deployment.Evidence))
	add("deployment unavailable", instanceStatusDash(string(c.Content.Deployment.UnavailableReason)))
	add("encoding name", instanceStatusDash(c.Content.Encoding.Name))
	add("encoding evidence", string(c.Content.Encoding.Evidence))
	add("encoding unavailable", instanceStatusDash(string(c.Content.Encoding.UnavailableReason)))
	if instance := c.Content.Instance; instance != nil {
		add("inference replica", instance.InferenceReplica)
		add("instance index", strconv.FormatInt(int64(instance.Index), 10))
		add("incarnation", strconv.FormatInt(instance.Incarnation, 10))
		add("phase", string(instance.Phase))
		add("running revision", instanceStatusDash(instance.RunningRevision))
		add("target revision", instanceStatusDash(instance.TargetRevision))
		add("admitted", strconv.FormatBool(instance.Admitted))
		add("persisted pods", strconv.FormatInt(int64(instance.Pods.Total), 10))
		add("persisted serving", strconv.FormatInt(int64(instance.Pods.Serving), 10))
		add("persisted available", strconv.FormatInt(int64(instance.Pods.Available), 10))
		add("ready since", statusTime(instance.ReadySince))
		add("active ordinal", statusInt32(instance.ActiveOrdinal))
		for index, condition := range instance.Conditions {
			prefix := fmt.Sprintf("condition[%d] ", index)
			add(prefix+"type", condition.Type)
			add(prefix+"status", condition.Status)
			add(prefix+"generation", strconv.FormatInt(condition.ObservedGeneration, 10))
			add(prefix+"evidence", string(condition.Evidence))
			add(prefix+"reason", instanceStatusDash(condition.Reason))
			add(prefix+"transition", statusTime(condition.LastTransitionTime))
		}
		if operation := instance.Operation; operation != nil {
			add("operation id", operation.ID)
			add("operation type", operation.Type)
			add("operation step", operation.Step)
			add("operation revision", instanceStatusDash(operation.TargetRevision))
			add("operation reason", instanceStatusDash(operation.Reason))
			add("operation retries", strconv.FormatInt(int64(operation.RetryCount), 10))
			add("operation surge index", statusInt32(operation.SurgeIndex))
			add("operation from node", instanceStatusDash(operation.FromNode))
			if len(operation.TargetNodeHints) == 0 {
				add("operation target node", "-")
			}
			for _, node := range operation.TargetNodeHints {
				add("operation target node", node)
			}
			add("operation request UUID", instanceStatusDash(operation.RequestUUID))
			add("operation started", statusTime(operation.StartedAt))
			add("operation progress", statusTime(operation.LastProgressAt))
			add("operation deadline", statusTime(operation.Deadline))
		}
		if failure := instance.LastFailure; failure != nil {
			add("failure pod", failure.PodName)
			add("failure container", instanceStatusDash(failure.ContainerName))
			add("failure reason", instanceStatusDash(failure.Reason))
			add("failure exit code", statusInt32(failure.ExitCode))
			add("failure time", statusTime(failure.Time))
		}
		for index, migration := range instance.Migrations {
			prefix := fmt.Sprintf("migration[%d] ", index)
			add(prefix+"request UUID", migration.RequestUUID)
			add(prefix+"role", migration.Role)
			add(prefix+"trigger", migration.Trigger)
			add(prefix+"source index", strconv.FormatInt(int64(migration.SourceInstance), 10))
			add(prefix+"surge index", statusInt32(migration.SurgeInstance))
			add(prefix+"phase", migration.Phase)
			add(prefix+"attempt", strconv.FormatInt(int64(migration.Attempt), 10))
			add(prefix+"from node", instanceStatusDash(migration.FromNode))
			if len(migration.TargetNodeHints) == 0 {
				add(prefix+"target node", "-")
			}
			for _, node := range migration.TargetNodeHints {
				add(prefix+"target node", node)
			}
			add(prefix+"reason", instanceStatusDash(migration.Reason))
			add(prefix+"message", instanceStatusDash(migration.Message))
			add(prefix+"started", statusTime(migration.StartedAt))
			add(prefix+"allocated", statusTime(migration.AllocatedAt))
			add(prefix+"deadline", statusTime(migration.Deadline))
			add(prefix+"completed", statusTime(migration.CompletedAt))
			add(prefix+"succeeded", statusBool(migration.Succeeded))
		}
	}
	for index, pod := range c.Content.Pods {
		prefix := fmt.Sprintf("pod[%d] ", index)
		add(prefix+"name", pod.Name)
		add(prefix+"runner", instanceStatusDash(pod.Runner))
		add(prefix+"revision", instanceStatusDash(pod.Revision))
		add(prefix+"incarnation", strconv.FormatInt(pod.Incarnation, 10))
		add(prefix+"phase", pod.Phase)
		add(prefix+"ready", pod.Ready)
		add(prefix+"serving ready", pod.ServingReady)
		add(prefix+"node", instanceStatusDash(pod.Node))
		add(prefix+"restarts", strconv.FormatInt(int64(pod.RestartCount), 10))
		add(prefix+"deleting", strconv.FormatBool(pod.Deleting))
	}
	for index, event := range c.Content.Events {
		prefix := fmt.Sprintf("event[%d] ", index)
		add(prefix+"target kind", event.TargetKind)
		add(prefix+"target name", event.TargetName)
		add(prefix+"reason", event.Reason)
		add(prefix+"count", strconv.FormatInt(int64(event.Count), 10))
		add(prefix+"first seen", statusTime(event.FirstSeen))
		add(prefix+"last seen", statusTime(event.LastSeen))
	}
	for index, issue := range c.Content.Issues {
		prefix := fmt.Sprintf("issue[%d] ", index)
		add(prefix+"code", string(issue.Code))
		add(prefix+"unavailable", instanceStatusDash(string(issue.UnavailableReason)))
	}
	for index, warning := range c.Warnings {
		add(fmt.Sprintf("warning[%d] code", index), string(warning.Code))
	}
	return table
}

func safeInstanceStatusText(value string, width int) string {
	return safetext.Sanitize(value, width)
}

func compareInstanceStatusBool(left, right bool) int {
	if left == right {
		return 0
	}
	if !left {
		return -1
	}
	return 1
}

func compareInstanceStatusTime(left, right *time.Time) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	return left.Compare(*right)
}

func compareInstanceStatusInt32(left, right *int32) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	return cmp.Compare(*left, *right)
}

func compareInstanceStatusBoolPointer(left, right *bool) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	return compareInstanceStatusBool(*left, *right)
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

func copyInstanceStatusBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func instanceStatusDash(value string) string {
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

func statusBool(value *bool) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatBool(*value)
}
