package v1alpha1

import (
	"fmt"
	"strconv"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitscale"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// WaitScaleObservation is the bounded projection of an exact IR spec/status read.
// It describes current logical Instance count, not Ready convergence or
// attribution to a preceding scale action.
type WaitScaleObservation waitscale.Observation

func (r WaitRequested) IsScale() bool { return r == WaitRequestedScaleCurrent }

func (o WaitScaleObservation) Canonical() WaitScaleObservation {
	switch o.Component {
	case ome.EngineComponent, ome.DecoderComponent, ome.RouterComponent:
	default:
		if o.Component != "" || o.Validity == "Valid" {
			o.Validity = "Invalid"
		}
		o.Component = ""
	}
	if o.Requested < 0 || o.Requested == 0 && o.Validity == "Valid" {
		o.Requested, o.Validity = 0, "Invalid"
	}
	if o.Encoding != "" && o.Encoding != string(irstatus.EncodingDenseV1) && o.Encoding != string(irstatus.EncodingColumnarV2) {
		o.Encoding, o.Validity = "", "Invalid"
	}
	switch o.Freshness {
	case "Current", "Stale", "Unavailable":
	default:
		o.Freshness, o.Validity = "Unavailable", "Invalid"
	}
	switch o.Validity {
	case "Valid", "Unavailable", "Invalid":
	default:
		o.Validity = "Invalid"
	}
	if o.Validity == "Valid" && (o.Freshness != "Current" || o.Encoding == "" ||
		o.SpecReplicas == nil || *o.SpecReplicas < 0 ||
		o.CurrentReplicas == nil || *o.CurrentReplicas < 0 ||
		o.ReadyReplicas == nil || *o.ReadyReplicas < 0 || *o.ReadyReplicas > *o.CurrentReplicas) {
		o.Validity = "Invalid"
	}
	if o.Validity != "Valid" {
		o.SpecReplicas, o.CurrentReplicas, o.ReadyReplicas = nil, nil, nil
		o.Encoding = ""
	} else {
		spec, current, ready := *o.SpecReplicas, *o.CurrentReplicas, *o.ReadyReplicas
		o.SpecReplicas, o.CurrentReplicas, o.ReadyReplicas = &spec, &current, &ready
	}
	return o
}

func (r WaitReport) scaleTable(wide bool) report.Table {
	c := r.Content
	o := c.Scale
	count := func(v *int32) string {
		if v == nil {
			return "<unavailable>"
		}
		return strconv.FormatInt(int64(*v), 10)
	}
	interpretation := "No verified exact scale count"
	if c.Outcome == waitengine.OutcomeMatched && o.Validity == "Valid" {
		interpretation = "Exact desired and current count at IR snapshot"
	} else if c.Outcome == waitengine.OutcomeTimedOut && o.Validity == "Valid" {
		interpretation = "Last observation; currentness unverified"
	}
	rows := [][]string{
		{"Service", r.Metadata.Namespace + "/" + r.Metadata.Name},
		{"Requested", string(c.Requested)}, {"Component", string(o.Component)},
		{"Target replicas", strconv.FormatInt(int64(o.Requested), 10)},
		{"Outcome", string(c.Outcome)}, {"Spec replicas", count(o.SpecReplicas)},
		{"Current replicas", count(o.CurrentReplicas)}, {"Ready replicas", count(o.ReadyReplicas)},
		{"Status encoding", o.Encoding}, {"Validity", o.Validity}, {"Freshness", o.Freshness},
		{"Reason", string(c.Reason)}, {"Evidence", string(c.Evidence)},
		{"Interpretation", interpretation},
		{"Count caveat", "Current = logical Instances any phase; Ready separate"},
		{"Attribution", "Exact IR snapshot; not action attribution"},
		{"Source method", string(c.Method)},
		{"GET / WATCH / polls", fmt.Sprintf("%d / %d / %d", c.Counts.Gets, c.Counts.Watches, c.Counts.Polls)},
		{"Elapsed milliseconds", strconv.FormatInt(c.ElapsedMilliseconds, 10)},
	}
	if wide {
		rows = append(rows,
			[]string{"Read scope", "Parent GET / exact IR GET; no namespace LIST"},
			[]string{"Cancellation", "Cooperative requests; plugins may ignore it"})
	}
	for i := range rows {
		rows[i][0] = printers.BoundedCell(rows[i][0], 22)
		rows[i][1] = printers.BoundedCell(rows[i][1], 54)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}
