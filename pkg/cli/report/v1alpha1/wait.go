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
	WaitRequestedTrue    WaitRequested = "Ready=True"
	WaitRequestedFalse   WaitRequested = "Ready=False"
	WaitRequestedUnknown WaitRequested = "Ready=Unknown"
)

type WaitCounts struct {
	Gets         int `json:"gets"`
	Watches      int `json:"watches"`
	Polls        int `json:"polls"`
	Events       int `json:"events"`
	Observations int `json:"observations"`
}
type WaitContent struct {
	Requested           WaitRequested             `json:"requested"`
	Outcome             waitengine.Outcome        `json:"outcome"`
	Reason              waitengine.Reason         `json:"reason"`
	Observed            waitpredicate.Observation `json:"observed"`
	Evidence            EvidenceLevel             `json:"evidence"`
	ElapsedMilliseconds int64                     `json:"elapsedMilliseconds"`
	Counts              WaitCounts                `json:"counts"`
	Method              waitengine.Method         `json:"sourceMethod"`
	Fallback            bool                      `json:"pollingFallback"`
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
	if c.Requested != WaitRequestedTrue && c.Requested != WaitRequestedFalse && c.Requested != WaitRequestedUnknown {
		c.Requested = "Unknown"
	}
	switch c.Outcome {
	case waitengine.OutcomeMatched, waitengine.OutcomeTimedOut, waitengine.OutcomeNotFound, waitengine.OutcomeDeleted, waitengine.OutcomeReplaced:
	default:
		c.Outcome = "Unknown"
	}
	switch c.Reason {
	case waitengine.ReasonMatched, waitengine.ReasonNotRecorded, waitengine.ReasonNotMatched, waitengine.ReasonInvalidCondition:
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
