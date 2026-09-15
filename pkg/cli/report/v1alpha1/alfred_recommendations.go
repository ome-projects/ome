package v1alpha1

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// AlfredRecommendationsReport is a CLI report, never a resource for apply.
type AlfredRecommendationsReport = Envelope[AlfredRecommendationsContent]

// AlfredRecommendation contains only allowlisted diagnostic values. The
// persisted wire has no executable flag. Dispatch is reported history, not
// authorization to act or independent evidence of workload convergence.
type AlfredRecommendation struct {
	Workload       string `json:"workload"`
	Component      string `json:"component"`
	Instance       int32  `json:"instance"`
	Policy         string `json:"policy"`
	Reason         string `json:"reason"`
	Outcome        string `json:"outcome"`
	Classification string `json:"classification"`
	Executability  string `json:"executability"`
	AdvisoryReason string `json:"advisoryReason,omitempty"`
	RejectReason   string `json:"rejectReason,omitempty"`
	DispatchStatus string `json:"dispatchStatus,omitempty"`
	DispatchReason string `json:"dispatchReason,omitempty"`
}

// AlfredRecommendationsContent describes one bounded cycle, not a complete
// history. Omitted node records and scheduling payloads have no inferred state.
type AlfredRecommendationsContent struct {
	SourceNamespace        string                 `json:"sourceNamespace"`
	ConfigName             string                 `json:"configName"`
	RecordName             string                 `json:"recordName,omitempty"`
	State                  string                 `json:"state"`
	ConfigState            string                 `json:"configState"`
	ConfigAuthority        string                 `json:"configAuthority"`
	RecordState            string                 `json:"recordState"`
	Authority              string                 `json:"authority"`
	Freshness              string                 `json:"freshness"`
	ConfigKey              string                 `json:"configKey"`
	RecordKey              string                 `json:"recordKey"`
	ConfigMode             string                 `json:"configMode,omitempty"`
	RecordMode             string                 `json:"recordMode,omitempty"`
	Timestamp              *time.Time             `json:"timestamp,omitempty"`
	FreshnessWindowSeconds float64                `json:"freshnessWindowSeconds"`
	Rows                   int                    `json:"rows"`
	Scanned                int                    `json:"scanned"`
	Invalid                int                    `json:"invalid"`
	Duplicates             int                    `json:"duplicates"`
	Omitted                int                    `json:"omitted"`
	Truncated              bool                   `json:"truncated"`
	Observations           string                 `json:"observations"`
	Issues                 []string               `json:"issues"`
	Recommendations        []AlfredRecommendation `json:"recommendations"`
}

// Canonical copies and totally orders safe values without mutating callers.
func (c AlfredRecommendationsContent) Canonical() AlfredRecommendationsContent {
	c.Recommendations = append([]AlfredRecommendation{}, c.Recommendations...)
	slices.SortFunc(c.Recommendations, func(a, b AlfredRecommendation) int {
		return cmp.Or(cmp.Compare(a.Workload, b.Workload), cmp.Compare(a.Component, b.Component), cmp.Compare(a.Instance, b.Instance),
			cmp.Compare(a.Policy, b.Policy), cmp.Compare(a.Reason, b.Reason), cmp.Compare(a.Outcome, b.Outcome),
			cmp.Compare(a.Classification, b.Classification), cmp.Compare(a.Executability, b.Executability),
			cmp.Compare(a.AdvisoryReason, b.AdvisoryReason), cmp.Compare(a.RejectReason, b.RejectReason),
			cmp.Compare(a.DispatchStatus, b.DispatchStatus), cmp.Compare(a.DispatchReason, b.DispatchReason))
	})
	c.Issues = append([]string{}, c.Issues...)
	slices.Sort(c.Issues)
	c.Issues = slices.Compact(c.Issues)
	if c.Timestamp != nil {
		timestamp := c.Timestamp.UTC()
		c.Timestamp = &timestamp
	}
	return c
}

// Table keeps every physical line within 80 columns, including long names.
func (c AlfredRecommendationsContent) Table() report.Table { return c.table(false) }

// WideTable expands identities, source details, and allowlisted reason codes.
func (c AlfredRecommendationsContent) WideTable() report.Table { return c.table(true) }

func (c AlfredRecommendationsContent) table(wide bool) report.Table {
	c = c.Canonical()
	t := report.Table{Headers: []string{"SUBJECT", "EVIDENCE", "DETAIL"}}
	add := func(a, b, d string) {
		if !wide {
			a = printers.BoundedCell(a, 24)
			b = printers.BoundedCell(b, 19)
			d = printers.BoundedCell(d, 29)
		}
		t.Rows = append(t.Rows, []string{a, b, d})
	}
	add("Alfred recommendations", c.State, c.Freshness)
	add("Source namespace", c.SourceNamespace, "Config and cycle sources")
	add("Config source", c.ConfigName, c.ConfigKey)
	if c.RecordName != "" {
		add("Record source", c.RecordKey, c.RecordName)
	}
	add("ConfigMap config", c.ConfigState, c.ConfigMode)
	add("Latest cycle", c.RecordState, c.RecordMode)
	add("Rows", fmt.Sprintf("%d shown/%d scanned", c.Rows, c.Scanned), fmt.Sprintf("%d invalid; %d omitted", c.Invalid, c.Omitted))
	for _, row := range c.Recommendations {
		subject := fmt.Sprintf("%s/%s#%d", row.Workload, row.Component, row.Instance)
		add(subject, row.Outcome, row.Policy+"/"+row.Reason)
		if wide {
			for _, code := range []string{row.AdvisoryReason, row.RejectReason, row.DispatchReason} {
				if code != "" {
					add(subject, "Reason code", code)
				}
			}
		}
	}
	for _, issue := range c.Issues {
		add("Issue", issue, "")
	}
	add("Advisory evidence", "Not authorization", "Convergence unverified")
	add("Executability", "Unverifiable", "Not persisted in cycle")
	add("Scope", "Latest cycle only", "Node/scheduling data omitted")
	if wide {
		add("Config authority", c.ConfigAuthority, "Alfred may retain last-known-good config")
		add("Cycle authority", c.Authority, "Does not prove policy loop health")
		add("Freshness window", fmt.Sprintf("%.0fs", c.FreshnessWindowSeconds), "2 x declared decision loop interval")
		if c.Timestamp != nil {
			add("Cycle timestamp", c.Timestamp.Format(time.RFC3339Nano), "")
		}
	}
	return t
}
