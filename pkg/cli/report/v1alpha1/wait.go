package v1alpha1

import (
	"fmt"
	"slices"
	"strconv"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/safetext"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitpredicate"
)

const WaitReportKind = "WaitReport"

type WaitRequested string

const (
	WaitRequestedTrue              WaitRequested = "Ready=True"
	WaitRequestedFalse             WaitRequested = "Ready=False"
	WaitRequestedUnknown           WaitRequested = "Ready=Unknown"
	WaitRequestedRolloutStable     WaitRequested = "Rollout=Stable"
	WaitRequestedRolloutFailed     WaitRequested = "Rollout=Failed"
	WaitRequestedRolloutRolledBack WaitRequested = "Rollout=RolledBack"
	WaitRequestedMigrationTerminal WaitRequested = "Migration=Terminal"
	WaitRequestedReadyReplicas     WaitRequested = "Replicas=Ready"
)

type WaitCounts struct {
	Gets         int `json:"gets"`
	Watches      int `json:"watches"`
	Polls        int `json:"polls"`
	Events       int `json:"events"`
	Observations int `json:"observations"`
}
type WaitContent struct {
	Requested           WaitRequested                 `json:"requested"`
	Outcome             waitengine.Outcome            `json:"outcome"`
	Reason              waitengine.Reason             `json:"reason"`
	Observed            waitpredicate.Observation     `json:"observed"`
	Rollout             *WaitRolloutObservation       `json:"rollout,omitempty"`
	Migration           *WaitMigrationObservation     `json:"migration,omitempty"`
	ReadyReplicas       *WaitReadyReplicasObservation `json:"readyReplicas,omitempty"`
	Evidence            EvidenceLevel                 `json:"evidence"`
	ElapsedMilliseconds int64                         `json:"elapsedMilliseconds"`
	Counts              WaitCounts                    `json:"counts"`
	Method              waitengine.Method             `json:"sourceMethod"`
	Fallback            bool                          `json:"pollingFallback"`
}
type WaitReport Envelope[WaitContent]

func NewWaitReport(metadata Metadata, content WaitContent, clock Clock) WaitReport {
	return WaitReport(NewEnvelope(WaitReportKind, metadata, content, clock)).Canonical()
}
func (r WaitReport) Canonical() WaitReport {
	r = WaitReport(Envelope[WaitContent](r).Canonical())
	r.Kind = WaitReportKind
	r.Metadata.Name = safetext.Sanitize(r.Metadata.Name, 253)
	r.Metadata.Namespace = safetext.Sanitize(r.Metadata.Namespace, 63)
	// This report intentionally has no generic source identities/warning text.
	// Its single source and whole-inspection diagnostics are concrete content.
	r.Sources = []SourceReference{}
	r.Warnings = []Warning{}
	return r
}
func (c WaitContent) Canonical() WaitContent {
	if c.Requested != WaitRequestedTrue && c.Requested != WaitRequestedFalse && c.Requested != WaitRequestedUnknown && !c.Requested.IsRollout() && !c.Requested.IsMigration() && !c.Requested.IsReadyReplicas() {
		c.Requested = "Unknown"
	}
	switch c.Outcome {
	case waitengine.OutcomeMatched, waitengine.OutcomeTimedOut, waitengine.OutcomeNotFound, waitengine.OutcomeDeleted, waitengine.OutcomeReplaced:
	default:
		c.Outcome = "Unknown"
	}
	switch c.Reason {
	case waitengine.ReasonMatched, waitengine.ReasonNotRecorded, waitengine.ReasonNotMatched, waitengine.ReasonInvalidCondition:
		if c.Requested.IsMigration() || c.Requested.IsReadyReplicas() {
			c.Reason = "PredicateUnmet"
		}
	case waitengine.ReasonRolloutMatched, waitengine.ReasonRolloutNotMatched, waitengine.ReasonRolloutNotRecorded, waitengine.ReasonInvalidRollout:
		if !c.Requested.IsRollout() {
			c.Reason = "PredicateUnmet"
		}
	case waitengine.ReasonMigrationMatched, waitengine.ReasonMigrationNotRecorded, waitengine.ReasonMigrationInProgress, waitengine.ReasonInvalidMigration:
		if !c.Requested.IsMigration() {
			c.Reason = "PredicateUnmet"
		}
	case waitengine.ReasonReplicaReadyMatched, waitengine.ReasonReplicaReadyNotMatched, waitengine.ReasonReplicaReadyNotRecorded, waitengine.ReasonInvalidReplicaReady:
		if !c.Requested.IsReadyReplicas() {
			c.Reason = "PredicateUnmet"
		}
	default:
		c.Reason = "PredicateUnmet"
	}
	switch c.Method {
	case waitengine.MethodInitialGET, waitengine.MethodRefreshGET, waitengine.MethodWatch, waitengine.MethodPoll:
	default:
		c.Method = "Unknown"
	}
	switch c.Observed.Status {
	case "True", "False", "Unknown", "NotRecorded":
	default:
		c.Observed.Status = "NotRecorded"
	}
	switch c.Observed.Validity {
	case "Valid", "Invalid", "Unavailable":
	default:
		c.Observed.Validity = "Invalid"
	}
	c.Observed.GenerationFreshness = "Unverifiable"
	inspection := &c.Observed.Inspection
	switch inspection.State {
	case "Complete", "Partial", "LimitExceeded", "NotInspected":
	default:
		inspection.State = "Invalid"
	}
	inspection.Total = max(0, min(inspection.Total, 64))
	inspection.Inspected = max(0, min(inspection.Inspected, 64))
	warnings := make([]waitpredicate.Warning, 0, len(inspection.Warnings))
	for _, warning := range inspection.Warnings {
		switch warning {
		case waitpredicate.WarningOversizedRecord, waitpredicate.WarningInvalidRecord, waitpredicate.WarningFutureTimestamp, waitpredicate.WarningConflictingReady, waitpredicate.WarningDuplicateReady:
			if !slices.Contains(warnings, warning) {
				warnings = append(warnings, warning)
			}
		}
	}
	slices.Sort(warnings)
	inspection.Warnings = warnings
	c.Evidence = EvidenceReported
	if c.Observed.Validity == "Unavailable" {
		c.Evidence = EvidenceUnavailable
	}
	c.ElapsedMilliseconds = max(int64(0), c.ElapsedMilliseconds)
	c.Counts.Gets = max(0, min(c.Counts.Gets, 17282))
	c.Counts.Watches = max(0, min(c.Counts.Watches, 2))
	c.Counts.Polls = max(0, min(c.Counts.Polls, 17280))
	c.Counts.Events = max(0, min(c.Counts.Events, 4096))
	c.Counts.Observations = max(0, min(c.Counts.Observations, 21378))
	if c.Requested.IsRollout() {
		if c.Rollout == nil {
			c.Rollout = &WaitRolloutObservation{Validity: "Unavailable", Inspection: WaitRolloutInspection{State: "NotInspected"}}
		}
		rollout := c.Rollout.Canonical()
		c.Rollout = &rollout
		c.Observed = waitpredicate.Observation{Status: "NotRecorded", Validity: "Unavailable", GenerationFreshness: "Unverifiable", Inspection: waitpredicate.Inspection{State: "NotInspected", Warnings: []waitpredicate.Warning{}}}
		c.Evidence = EvidenceReported
		if rollout.Validity == "Unavailable" {
			c.Evidence = EvidenceUnavailable
		}
	} else {
		c.Rollout = nil
	}
	if c.Requested.IsMigration() {
		if c.Migration == nil {
			c.Migration = &WaitMigrationObservation{Phase: MigrationPhaseUnknown, Outcome: MigrationOutcomeUnknown, Validity: "Unavailable"}
		}
		migration := c.Migration.Canonical()
		if c.Outcome == waitengine.OutcomeMatched &&
			(migration.Validity != "Valid" ||
				migration.Phase != MigrationPhaseCompleted && migration.Phase != MigrationPhaseFailed && migration.Phase != MigrationPhaseRelocated) {
			// A terminal assertion cannot be matched by malformed or active
			// evidence, even if a caller constructs this report directly.
			migration.Validity = "Invalid"
			c.Outcome = "Unknown"
			c.Reason = waitengine.ReasonInvalidMigration
		}
		c.Migration = &migration
		c.Observed = waitpredicate.Observation{Status: "NotRecorded", Validity: "Unavailable", GenerationFreshness: "Unverifiable", Inspection: waitpredicate.Inspection{State: "NotInspected", Warnings: []waitpredicate.Warning{}}}
		c.Evidence = EvidenceUnavailable
		if migration.Validity == "Valid" {
			c.Evidence = EvidenceReported
		}
	} else {
		c.Migration = nil
	}
	if c.Requested.IsReadyReplicas() {
		if c.ReadyReplicas == nil {
			c.ReadyReplicas = &WaitReadyReplicasObservation{Validity: "Unavailable"}
		}
		ready := c.ReadyReplicas.Canonical()
		if c.Outcome == waitengine.OutcomeMatched &&
			(ready.Validity != "Valid" || ready.Observed == nil || *ready.Observed != ready.Requested || c.Reason != waitengine.ReasonReplicaReadyMatched) {
			ready.Validity = "Invalid"
			ready.Observed = nil
			c.Outcome = "Unknown"
			c.Reason = waitengine.ReasonInvalidReplicaReady
		}
		c.ReadyReplicas = &ready
		c.Observed = waitpredicate.Observation{Status: "NotRecorded", Validity: "Unavailable", GenerationFreshness: "Unverifiable", Inspection: waitpredicate.Inspection{State: "NotInspected", Warnings: []waitpredicate.Warning{}}}
		c.Evidence = EvidenceUnavailable
		if ready.Validity == "Valid" {
			c.Evidence = EvidenceReported
		}
	} else {
		c.ReadyReplicas = nil
	}
	return c
}
func (r WaitReport) Table() report.Table {
	r = r.Canonical()
	return r.table(false)
}
func (r WaitReport) WideTable() report.Table {
	r = r.Canonical()
	return r.table(true)
}
func (c WaitContent) Table() report.Table { return WaitReport{Content: c.Canonical()}.table(false) }
func (r WaitReport) table(wide bool) report.Table {
	c := r.Content
	if c.ReadyReplicas != nil {
		return r.readyReplicasTable(wide)
	}
	if c.Migration != nil {
		migration := c.Migration
		attribution := "Exact request ID in live IR status"
		if migration.Validity == "Unavailable" {
			attribution = "No verified matching IR record"
		} else if migration.Validity != "Valid" {
			attribution = "Unverifiable; incomplete IR evidence"
		} else if c.Outcome == waitengine.OutcomeTimedOut {
			attribution = "Last observed exact IR record; terminal unverified"
		}
		rows := [][]string{
			{"Service", r.Metadata.Namespace + "/" + r.Metadata.Name},
			{"Requested", string(c.Requested)}, {"Request ID", migration.RequestID},
			{"Outcome", string(c.Outcome)}, {"Migration phase", string(migration.Phase)},
			{"Migration outcome", string(migration.Outcome)}, {"Component", string(migration.Component)},
			{"Migration validity", migration.Validity}, {"Reason", string(c.Reason)},
			{"Evidence", string(c.Evidence)}, {"Attribution", attribution},
			{"Source method", string(c.Method)}, {"Polling fallback", strconv.FormatBool(c.Fallback)},
			{"Elapsed milliseconds", strconv.FormatInt(c.ElapsedMilliseconds, 10)},
			{"GET / WATCH / polls", fmt.Sprintf("%d / %d / %d", c.Counts.Gets, c.Counts.Watches, c.Counts.Polls)},
		}
		if wide {
			rows = append(rows, []string{"Inspected IR sources", strconv.Itoa(migration.InspectedSources)},
				[]string{"Inspected records", strconv.Itoa(migration.InspectedRecords)},
				[]string{"Cancellation", "Cooperative requests; plugins may ignore it"})
		}
		for i := range rows {
			rows[i][0] = printers.BoundedCell(rows[i][0], 22)
			rows[i][1] = printers.BoundedCell(rows[i][1], 54)
		}
		return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
	}
	rows := [][]string{
		{"Service", r.Metadata.Namespace + "/" + r.Metadata.Name},
		{"Requested", string(c.Requested)},
		{"Outcome", string(c.Outcome)},
		{"Observed Ready", c.Observed.Status},
		{"Condition validity", c.Observed.Validity},
		{"Reason", string(c.Reason)},
		{"Evidence", string(c.Evidence)},
		{"Generation freshness", c.Observed.GenerationFreshness},
		{"Freshness caveat", "Reported condition; not current-spec convergence"},
		{"Condition inspection", fmt.Sprintf("%s (%d/%d)", c.Observed.Inspection.State, c.Observed.Inspection.Inspected, c.Observed.Inspection.Total)},
		{"Source method", string(c.Method)},
		{"Polling fallback", strconv.FormatBool(c.Fallback)},
		{"Elapsed milliseconds", strconv.FormatInt(c.ElapsedMilliseconds, 10)},
		{"GET / WATCH / polls", fmt.Sprintf("%d / %d / %d", c.Counts.Gets, c.Counts.Watches, c.Counts.Polls)},
		{"Events / observations", fmt.Sprintf("%d / %d", c.Counts.Events, c.Counts.Observations)},
	}
	if c.Rollout != nil {
		o := c.Rollout
		rows = append([][]string{
			{"Service", r.Metadata.Namespace + "/" + r.Metadata.Name}, {"Requested", string(c.Requested)}, {"Outcome", string(c.Outcome)},
			{"Reported rollout", string(o.Summary.ReportedState)}, {"Rollout state", string(o.Summary.State)}, {"Rollout validity", o.Validity},
			{"Reason", string(c.Reason)}, {"Evidence", string(c.Evidence)}, {"Rollout epoch", string(o.Summary.Epoch)}, {"Coordination Ready", string(o.Summary.CoordinationReady)},
			{"Freshness caveat", "Reported rollout; not current-spec convergence"}, {"Attribution caveat", "Same-object state; not action attribution"},
			{"Rollout inspection", o.Inspection.State}, {"Expansion", "JSON/YAML retain complete safe report values"},
		}, rows[10:]...)
		for _, issue := range o.Issues {
			value := string(issue.Code)
			if issue.Group != nil {
				value += fmt.Sprintf(" group=%d", *issue.Group)
			}
			if issue.Component != "" {
				value += " component=" + string(issue.Component)
			}
			rows = append(rows, []string{"Rollout issue", value})
		}
		for _, warning := range o.Warnings {
			rows = append(rows, []string{"Inspection warning", string(warning)})
		}
		if wide {
			rows = append(rows, []string{"Inspected conditions", strconv.Itoa(o.Inspection.Conditions)}, []string{"Inspected components", strconv.Itoa(o.Inspection.Components)}, []string{"Inspected live groups", strconv.Itoa(o.Inspection.Groups)}, []string{"Pinned groups", strconv.Itoa(o.Inspection.PinnedGroups)}, []string{"Inspected targets", strconv.Itoa(o.Inspection.Targets)})
		}
	}
	for _, warning := range c.Observed.Inspection.Warnings {
		rows = append(rows, []string{"Inspection warning", string(warning)})
	}
	if wide {
		rows = append(rows, []string{"Cancellation", "Cooperative requests; external plugins may ignore it"})
	}
	for i := range rows {
		rows[i][0] = printers.BoundedCell(rows[i][0], 22)
		rows[i][1] = printers.BoundedCell(rows[i][1], 54)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}
