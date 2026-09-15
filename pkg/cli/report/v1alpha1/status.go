package v1alpha1

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/safetext"
)

type StatusReadyState string
type StatusValidity string
type StatusInspectionState string
type StatusCollectionState string
type StatusSourceReason string
type StatusIssueCode string

type StatusReady struct {
	Status     StatusReadyState `json:"status"`
	Validity   StatusValidity   `json:"validity"`
	Reason     string           `json:"reason,omitempty"`
	Message    string           `json:"message,omitempty"`
	Inspection StatusInspection `json:"inspection"`
}
type StatusInspection struct {
	State     StatusInspectionState `json:"state"`
	Total     int                   `json:"total"`
	Inspected int                   `json:"inspected"`
	Warnings  []StatusIssueCode     `json:"warnings"`
}
type StatusCollection struct {
	State          StatusCollectionState `json:"state"`
	Reason         StatusSourceReason    `json:"reason,omitempty"`
	Observed       int                   `json:"observed"`
	Truncated      bool                  `json:"truncated"`
	SkippedTargets int                   `json:"skippedTargets"`
}
type StatusPodCounts struct {
	Total       int   `json:"total"`
	Ready       int   `json:"ready"`
	Restarts    int64 `json:"restarts"`
	Running     int   `json:"running"`
	Pending     int   `json:"pending"`
	Failed      int   `json:"failed"`
	Succeeded   int   `json:"succeeded"`
	Unknown     int   `json:"unknown"`
	Terminating int   `json:"terminating"`
}
type StatusComponent struct {
	Type     RuntimeComponentType `json:"type"`
	Ready    StatusReadyState     `json:"ready"`
	Validity StatusValidity       `json:"validity"`
	Pods     StatusPodCounts      `json:"pods"`
	Evidence EvidenceLevel        `json:"evidence"`
}
type StatusEvent struct {
	Kind     string     `json:"kind"`
	Name     string     `json:"name"`
	Reason   string     `json:"reason,omitempty"`
	Message  string     `json:"message,omitempty"`
	Count    int32      `json:"count"`
	LastSeen *time.Time `json:"lastSeen,omitempty"`
}
type StatusRollout struct {
	Summary  RolloutSummary     `json:"summary"`
	Issues   []RolloutIssueCode `json:"issues"`
	Warnings []WarningCode      `json:"warnings"`
}
type StatusContent struct {
	Ready               StatusReady       `json:"ready"`
	Runtime             string            `json:"runtime,omitempty"`
	Model               string            `json:"model,omitempty"`
	Generation          int64             `json:"generation"`
	ObservedGeneration  int64             `json:"observedGeneration"`
	GenerationFreshness string            `json:"generationFreshness"`
	Components          []StatusComponent `json:"components"`
	Pods                StatusCollection  `json:"pods"`
	Events              StatusCollection  `json:"events"`
	RecentEvents        []StatusEvent     `json:"recentEvents"`
	Rollout             StatusRollout     `json:"rollout"`
	Issues              []StatusIssueCode `json:"issues"`
}
type StatusReport struct{ Envelope[StatusContent] }

func NewStatusReport(metadata Metadata, content StatusContent, clock Clock) StatusReport {
	return (StatusReport{Envelope: NewEnvelope("StatusReport", metadata, content, clock)}).Canonical()
}

// Canonical owns all public values; raw objects and errors never enter this schema.
func (c StatusContent) Canonical() StatusContent {
	c.Ready.Status = statusReadyState(c.Ready.Status)
	c.Ready.Validity = clusterEnum(c.Ready.Validity, StatusValidity("Invalid"), "Valid", "Invalid", "Unavailable")
	if c.Ready.Status == "NotRecorded" && c.Ready.Validity == "Valid" {
		c.Ready.Validity = "Unavailable"
	}
	c.Ready.Reason, c.Ready.Message = statusText(c.Ready.Reason, 256), statusText(c.Ready.Message, 1024)
	c.Ready.Inspection.State = clusterEnum(c.Ready.Inspection.State, StatusInspectionState("LimitExceeded"), "Complete", "Partial", "LimitExceeded")
	if c.Ready.Inspection.Total < 0 || c.Ready.Inspection.Total > 64 || c.Ready.Inspection.Inspected < 0 || c.Ready.Inspection.Inspected > min(c.Ready.Inspection.Total, 64) {
		c.Ready.Inspection.Total = min(max(c.Ready.Inspection.Total, 0), 65)
		c.Ready.Inspection.Inspected = min(max(c.Ready.Inspection.Inspected, 0), min(c.Ready.Inspection.Total, 64))
		c.Ready.Inspection.State = "LimitExceeded"
	}
	c.Ready.Inspection.Warnings = statusIssues(c.Ready.Inspection.Warnings)
	c.Runtime, c.Model = statusName(c.Runtime, false), statusName(c.Model, false)
	c.GenerationFreshness = "Unverifiable"
	issues := statusIssues(c.Issues)
	if c.Generation < 0 || c.ObservedGeneration < 0 {
		c.Generation = max(c.Generation, 0)
		c.ObservedGeneration = max(c.ObservedGeneration, 0)
		issues = append(issues, "InvalidGeneration")
	}
	c.Pods, c.Events = statusCollection(c.Pods, 1000), statusCollection(c.Events, 100)
	components := []StatusComponent{}
	if len(c.Components) > 3 {
		issues = append(issues, "CollectionLimitExceeded")
	} else {
		for _, row := range c.Components {
			row.Type = canonicalRolloutComponentType(row.Type)
			if row.Type == "" {
				issues = append(issues, "UnsupportedComponent")
				continue
			}
			row.Ready = statusReadyState(row.Ready)
			row.Validity = clusterEnum(row.Validity, StatusValidity("Unavailable"), "Valid", "Invalid", "Unavailable")
			row.Evidence = clusterEnum(row.Evidence, EvidenceUnavailable, EvidenceObserved, EvidenceReported)
			if !statusCountsValid(row.Pods) {
				row.Pods = StatusPodCounts{}
				row.Evidence = EvidenceUnavailable
				issues = append(issues, "PodMalformed")
			}
			components = append(components, row)
		}
	}
	slices.SortFunc(components, func(a, b StatusComponent) int {
		return cmp.Or(cmp.Compare(componentRank(a.Type), componentRank(b.Type)), cmp.Compare(a.Ready, b.Ready), cmp.Compare(a.Validity, b.Validity), cmp.Compare(a.Pods.Total, b.Pods.Total))
	})
	c.Components = []StatusComponent{}
	for i, row := range components {
		if i > 0 && components[i-1].Type == row.Type || i+1 < len(components) && components[i+1].Type == row.Type {
			issues = append(issues, "UnsupportedComponent")
			continue
		}
		c.Components = append(c.Components, row)
	}
	rawEvents := c.RecentEvents
	c.RecentEvents = []StatusEvent{}
	if len(rawEvents) > 100 {
		issues = append(issues, "CollectionLimitExceeded")
	} else {
		for _, event := range rawEvents {
			if event.Kind != "Pod" && event.Kind != "InferenceService" || event.Count < 0 || len(event.Reason) > 1024 || len(event.Message) > 4096 || statusName(event.Name, false) == "" {
				issues = append(issues, "EventMalformed")
				continue
			}
			event.Name = statusName(event.Name, false)
			event.Reason = statusText(event.Reason, 256)
			event.Message = statusText(event.Message, 1024)
			if event.LastSeen != nil {
				stamp := event.LastSeen.UTC()
				event.LastSeen = &stamp
			}
			c.RecentEvents = append(c.RecentEvents, event)
		}
	}
	slices.SortFunc(c.RecentEvents, func(a, b StatusEvent) int {
		var aTime, bTime time.Time
		if a.LastSeen != nil {
			aTime = *a.LastSeen
		}
		if b.LastSeen != nil {
			bTime = *b.LastSeen
		}
		return cmp.Or(bTime.Compare(aTime), cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Name, b.Name), cmp.Compare(a.Reason, b.Reason), cmp.Compare(a.Message, b.Message), cmp.Compare(a.Count, b.Count))
	})
	c.Rollout.Summary = (RolloutStatusContent{Summary: c.Rollout.Summary}).Canonical().Summary
	rolloutIssues := []RolloutIssueCode{}
	if len(c.Rollout.Issues) <= 32 {
		for _, code := range c.Rollout.Issues {
			rolloutIssues = append(rolloutIssues, canonicalRolloutIssueCode(code))
		}
	} else {
		issues = append(issues, "CollectionLimitExceeded")
	}
	slices.Sort(rolloutIssues)
	c.Rollout.Issues = slices.Compact(rolloutIssues)
	rolloutWarnings := []WarningCode{}
	if len(c.Rollout.Warnings) <= 8 {
		for _, code := range c.Rollout.Warnings {
			rolloutWarnings = append(rolloutWarnings, canonicalRolloutWarningCode(code))
		}
	} else {
		issues = append(issues, "CollectionLimitExceeded")
	}
	slices.Sort(rolloutWarnings)
	c.Rollout.Warnings = slices.Compact(rolloutWarnings)
	c.Issues = statusIssues(issues)
	return c
}

func statusReadyState(value StatusReadyState) StatusReadyState {
	return clusterEnum(value, StatusReadyState("NotRecorded"), "True", "False", "Unknown", "NotRecorded")
}
func statusCollection(c StatusCollection, limit int) StatusCollection {
	c.State = clusterEnum(c.State, StatusCollectionState("Unavailable"), "Reported", "Partial", "Unavailable")
	c.Reason = clusterEnum(c.Reason, StatusSourceReason("Unreadable"), "", "Forbidden", "Unauthorized", "NotFound", "Timeout", "Unavailable", "Unreadable", "MalformedPayload", "UnsupportedAPI")
	if c.Observed < 0 || c.Observed > limit || c.SkippedTargets < 0 || c.SkippedTargets > 1000 {
		c.Observed = 0
		c.SkippedTargets = 0
		c.State = "Partial"
		c.Reason = "MalformedPayload"
		c.Truncated = true
	}
	if c.Truncated && c.State == "Reported" {
		c.State = "Partial"
	}
	return c
}
func statusCountsValid(c StatusPodCounts) bool {
	// At most 1000 Pods, each with 64 regular/init/ephemeral counters.
	const maxRestarts = int64(1000) * 192 * 2147483647
	if c.Total < 0 || c.Total > 1000 || c.Ready < 0 || c.Ready > c.Total || c.Restarts < 0 || c.Restarts > maxRestarts || c.Terminating < 0 || c.Terminating > c.Total {
		return false
	}
	for _, count := range []int{c.Running, c.Pending, c.Failed, c.Succeeded, c.Unknown} {
		if count < 0 || count > c.Total {
			return false
		}
	}
	return c.Running+c.Pending+c.Failed+c.Succeeded+c.Unknown == c.Total
}
func statusText(value string, width int) string {
	if strings.Contains(value, "://") {
		return "[OMITTED]"
	}
	return safetext.Sanitize(value, width)
}
func statusName(value string, namespace bool) string {
	clean := safetext.Sanitize(value, 253)
	if clean == "[REDACTED]" || clean == "[OMITTED]" {
		return clean
	}
	if namespace {
		if len(validation.IsDNS1123Label(value)) == 0 {
			return clean
		}
	} else if len(validation.IsDNS1123Subdomain(value)) == 0 {
		return clean
	}
	return ""
}
func statusIssues(values []StatusIssueCode) []StatusIssueCode {
	result := []StatusIssueCode{}
	if len(values) > 64 {
		return []StatusIssueCode{"CollectionLimitExceeded"}
	}
	for _, code := range values {
		result = append(result, clusterEnum(code, StatusIssueCode("UnsupportedData"), "UnsupportedData", "UnsupportedComponent", "PodIdentityRejected", "PodMalformed", "EventIdentityRejected", "EventMalformed", "CollectionLimitExceeded", "RolloutUnavailable", "InvalidGeneration", "OversizedConditionRecord", "InvalidConditionRecord", "FutureConditionTimestamp", "ConflictingReadyConditions", "DuplicateReadyConditions"))
	}
	slices.Sort(result)
	return slices.Compact(result)
}
func componentRank(value RuntimeComponentType) int {
	switch value {
	case RuntimeComponentEngine:
		return 0
	case RuntimeComponentDecoder:
		return 1
	default:
		return 2
	}
}

func (r StatusReport) Canonical() StatusReport {
	// The generic envelope copies before sorting. Reject untrusted collection
	// sizes first, and never forward generic free-form warning strings.
	rawSources := r.Sources
	r.Sources = []SourceReference{}
	r.Warnings = []Warning{}
	r.Envelope = r.Envelope.Canonical()
	r.Kind = "StatusReport"
	r.Metadata = Metadata{Name: statusName(r.Metadata.Name, false), Namespace: statusName(r.Metadata.Namespace, true)}
	sources := []SourceReference{}
	if len(rawSources) <= 1 {
		for _, source := range rawSources {
			if source.Kind == "InferenceService" && statusName(source.Name, false) == r.Metadata.Name && statusName(source.Namespace, true) == r.Metadata.Namespace {
				sources = append(sources, SourceReference{Kind: "InferenceService", Name: r.Metadata.Name, Namespace: r.Metadata.Namespace, Generation: max(source.Generation, 0), Evidence: EvidenceObserved, CollectedAt: r.CollectedAt})
			}
		}
	}
	r.Sources = sources
	r.Warnings = []Warning{}
	return r
}
func (c StatusContent) Table() report.Table    { return c.table(false) }
func (r StatusReport) Table() report.Table     { return r.table(false) }
func (r StatusReport) WideTable() report.Table { return r.table(true) }
func (r StatusReport) table(wide bool) report.Table {
	r = r.Canonical()
	t := r.Content.table(wide)
	t.Rows = append([][]string{{"Name", printers.BoundedCell(r.Metadata.Name, 57)}, {"Namespace", printers.BoundedCell(r.Metadata.Namespace, 57)}}, t.Rows...)
	if wide {
		t.Rows = append(t.Rows, []string{"Collected at", r.CollectedAt.Format(time.RFC3339Nano)})
		for _, source := range r.Sources {
			t.Rows = append(t.Rows, []string{"Source generation", fmt.Sprint(source.Generation)}, []string{"Source evidence", string(source.Evidence)})
		}
	}
	return t
}
func (c StatusContent) table(wide bool) report.Table {
	c = c.Canonical()
	t := report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: [][]string{}}
	add := func(field, value string) {
		t.Rows = append(t.Rows, []string{printers.BoundedCell(field, 20), printers.BoundedCell(value, 57)})
	}
	add("Ready", string(c.Ready.Status)+" / "+string(c.Ready.Validity))
	add("Ready reason", c.Ready.Reason)
	if wide {
		add("Ready message", c.Ready.Message)
		add("Condition inspection", fmt.Sprintf("%s %d/%d", c.Ready.Inspection.State, c.Ready.Inspection.Inspected, c.Ready.Inspection.Total))
		for _, code := range c.Ready.Inspection.Warnings {
			add("Condition warning", string(code))
		}
	}
	add("Runtime", c.Runtime)
	add("Model", c.Model)
	add("Generation", fmt.Sprintf("%d observed=%d; advisory Unverifiable", c.Generation, c.ObservedGeneration))
	add("Pod observation", fmt.Sprintf("%s count=%d truncated=%t %s", c.Pods.State, c.Pods.Observed, c.Pods.Truncated, c.Pods.Reason))
	add("Event observation", fmt.Sprintf("%s count=%d truncated=%t %s", c.Events.State, c.Events.Observed, c.Events.Truncated, c.Events.Reason))
	if wide {
		add("Pod targets skipped", fmt.Sprint(c.Pods.SkippedTargets))
		add("Event targets skip", fmt.Sprint(c.Events.SkippedTargets))
	}
	for _, component := range c.Components {
		add(string(component.Type), fmt.Sprintf("%s / %s; Ready pods=%d/%d restarts=%d", component.Ready, component.Validity, component.Pods.Ready, component.Pods.Total, component.Pods.Restarts))
		if wide {
			add(string(component.Type)+" Pod total", fmt.Sprint(component.Pods.Total))
			add(string(component.Type)+" Pod Ready", fmt.Sprint(component.Pods.Ready))
			add(string(component.Type)+" restarts", fmt.Sprint(component.Pods.Restarts))
			add(string(component.Type)+" phases", fmt.Sprintf("R=%d P=%d F=%d S=%d U=%d deleting=%d", component.Pods.Running, component.Pods.Pending, component.Pods.Failed, component.Pods.Succeeded, component.Pods.Unknown, component.Pods.Terminating))
			add(string(component.Type)+" evidence", string(component.Evidence))
		}
	}
	add("Rollout", string(c.Rollout.Summary.State)+" reported="+string(c.Rollout.Summary.ReportedState))
	add("Rollout evidence", string(c.Rollout.Summary.Evidence)+" / "+string(c.Rollout.Summary.Epoch))
	if wide {
		add("Coordination Ready", string(c.Rollout.Summary.CoordinationReady))
		for _, code := range c.Rollout.Issues {
			add("Rollout issue", string(code))
		}
		for _, code := range c.Rollout.Warnings {
			add("Rollout warning", string(code))
		}
	}
	for _, event := range c.RecentEvents {
		add("Warning "+event.Kind, event.Name+" "+event.Reason)
		if wide {
			add("Event message", event.Message)
			add("Event count", fmt.Sprint(event.Count))
			if event.LastSeen != nil {
				add("Event last seen", event.LastSeen.Format(time.RFC3339Nano))
			}
		}
	}
	for _, code := range c.Issues {
		add("Issue", string(code))
	}
	add("Full safe values", "Use -o json or -o yaml")
	add("Rollout detail", "kubectl ome rollout status NAME")
	return t
}
