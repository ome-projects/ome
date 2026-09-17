package v1alpha1

import (
	"strconv"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

// WaitReadyReplicasObservation reports one exact IR status counter. A nil
// Observed value means no verified count; it is not an observed zero.
type WaitReadyReplicasObservation struct {
	Component RuntimeComponentType `json:"component"`
	Requested int32                `json:"requested"`
	Observed  *int32               `json:"observed,omitempty"`
	Validity  string               `json:"validity"`
}

func (r WaitRequested) IsReadyReplicas() bool { return r == WaitRequestedReadyReplicas }

func (o WaitReadyReplicasObservation) Canonical() WaitReadyReplicasObservation {
	switch o.Component {
	case RuntimeComponentEngine, RuntimeComponentDecoder, RuntimeComponentRouter:
	default:
		o.Component = ""
		o.Validity = "Invalid"
	}
	if o.Requested < 0 {
		o.Requested = 0
		o.Validity = "Invalid"
	}
	if o.Validity != "Valid" && o.Validity != "Unavailable" {
		o.Validity = "Invalid"
	}
	if o.Validity == "Valid" && (o.Observed == nil || *o.Observed < 0) {
		o.Validity = "Invalid"
	}
	if o.Validity != "Valid" {
		o.Observed = nil
	} else {
		value := *o.Observed
		o.Observed = &value
	}
	return o
}

func (r WaitReport) readyReplicasTable(wide bool) report.Table {
	c := r.Content
	o := c.ReadyReplicas
	observed := "<unavailable>"
	if o.Observed != nil {
		observed = strconv.FormatInt(int64(*o.Observed), 10)
	}
	interpretation := "No verified exact count"
	if c.Outcome == waitengine.OutcomeMatched && o.Validity == "Valid" {
		interpretation = "Exact at observed IR snapshot"
	} else if c.Outcome == waitengine.OutcomeTimedOut && o.Validity == "Valid" {
		interpretation = "Last observation; currentness unverified"
	}
	rows := [][]string{
		{"Service", r.Metadata.Namespace + "/" + r.Metadata.Name},
		{"Requested", string(c.Requested)}, {"Component", string(o.Component)},
		{"Target ready", strconv.FormatInt(int64(o.Requested), 10)},
		{"Outcome", string(c.Outcome)}, {"Observed ready", observed},
		{"Count validity", o.Validity}, {"Reason", string(c.Reason)},
		{"Evidence", string(c.Evidence)}, {"Interpretation", interpretation},
		{"Source method", string(c.Method)}, {"Polling fallback", strconv.FormatBool(c.Fallback)},
		{"Elapsed milliseconds", strconv.FormatInt(c.ElapsedMilliseconds, 10)},
		{"GET / WATCH / polls", strconv.Itoa(c.Counts.Gets) + " / " + strconv.Itoa(c.Counts.Watches) + " / " + strconv.Itoa(c.Counts.Polls)},
	}
	if wide {
		rows = append(rows,
			[]string{"Count authority", "IR status; not /scale or an action receipt"},
			[]string{"Readiness caveat", "Ready count; not serving or availability"},
			[]string{"Cancellation", "Cooperative requests; plugins may ignore it"})
	}
	for i := range rows {
		rows[i][0] = printers.BoundedCell(rows[i][0], 22)
		rows[i][1] = printers.BoundedCell(rows[i][1], 54)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}
