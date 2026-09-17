package v1alpha1

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitruntime"
)

// WaitRuntimeSyncObservation is an allowlisted copy of same-object reported
// token state. It cannot establish live-runtime or serving convergence.
type WaitRuntimeSyncObservation waitruntime.Observation

func (r WaitRequested) IsRuntimeSync() bool { return r == WaitRequestedRuntimeSyncAcknowledged }

func (o WaitRuntimeSyncObservation) Canonical() WaitRuntimeSyncObservation {
	if o.RequestID != "" {
		id, err := uuid.Parse(o.RequestID)
		if len(o.RequestID) != 36 || err != nil || id.String() != o.RequestID || id.Version() != 4 || id.Variant() != uuid.RFC4122 {
			o.RequestID = ""
			o.Validity = "Invalid"
		}
	} else if o.Validity == "Valid" {
		o.Validity = "Invalid"
	}
	switch o.TokenState {
	case waitruntime.TokenAcknowledged, waitruntime.TokenPending, waitruntime.TokenStatusOnly,
		waitruntime.TokenSuperseded, waitruntime.TokenAbsent, waitruntime.TokenInvalid, waitruntime.TokenUnavailable:
	default:
		o.TokenState, o.Validity = waitruntime.TokenInvalid, "Invalid"
	}
	switch o.DriftState {
	case waitruntime.DriftClear, waitruntime.DriftReportedTrue, waitruntime.DriftReportedFalse,
		waitruntime.DriftReportedUnknown, waitruntime.DriftInvalid, waitruntime.DriftUnavailable:
	default:
		o.DriftState, o.Validity = waitruntime.DriftInvalid, "Invalid"
	}
	switch o.PinState {
	case waitruntime.PinManaged, waitruntime.PinNotApplicable, waitruntime.PinUnavailable, waitruntime.PinInvalid:
	default:
		o.PinState, o.Validity = waitruntime.PinInvalid, "Invalid"
	}
	if o.PlacementState != waitruntime.PlacementDirect && o.PlacementState != waitruntime.PlacementUnsupported {
		o.PlacementState, o.Validity = waitruntime.PlacementUnsupported, "Invalid"
	}
	if o.InspectedConditions < 0 || o.InspectedConditions > 64 {
		o.Validity = "Invalid"
	}
	o.InspectedConditions = max(0, min(o.InspectedConditions, 64))
	if o.Validity != "Valid" && o.Validity != "Unavailable" {
		o.Validity = "Invalid"
	}
	if o.Validity == "Valid" && (o.PinState != waitruntime.PinManaged || o.PlacementState != waitruntime.PlacementDirect ||
		(o.TokenState != waitruntime.TokenAcknowledged && o.TokenState != waitruntime.TokenPending && o.TokenState != waitruntime.TokenSuperseded) ||
		o.DriftState == waitruntime.DriftInvalid || o.DriftState == waitruntime.DriftUnavailable) {
		o.Validity = "Invalid"
	}
	o.GenerationFreshness = "Unverifiable"
	return o
}

func (r WaitReport) runtimeSyncTable(wide bool) report.Table {
	c := r.Content
	o := c.RuntimeSync
	interpretation := "No verified exact acknowledgment"
	if c.Outcome == waitengine.OutcomeMatched && o.Validity == "Valid" {
		interpretation = "Token acknowledged in bound parent snapshot"
	} else if c.Outcome == waitengine.OutcomeTimedOut && o.Validity == "Valid" {
		interpretation = "Last observation; currentness unverified"
	}
	rows := [][]string{
		{"Service", r.Metadata.Namespace + "/" + r.Metadata.Name},
		{"Requested", string(c.Requested)}, {"Request ID", o.RequestID},
		{"Outcome", string(c.Outcome)}, {"Token state", string(o.TokenState)},
		{"Drift state", string(o.DriftState)}, {"Managed pin", string(o.PinState)},
		{"Placement", string(o.PlacementState)}, {"Validity", o.Validity},
		{"Reason", string(c.Reason)}, {"Evidence", string(c.Evidence)},
		{"Interpretation", interpretation},
		{"Sync caveat", "Reported token; not live-runtime convergence"},
		{"Attribution", "Same-object state; not action attribution"},
		{"Source method", string(c.Method)}, {"Polling fallback", strconv.FormatBool(c.Fallback)},
		{"Elapsed milliseconds", strconv.FormatInt(c.ElapsedMilliseconds, 10)},
		{"GET / WATCH / polls", fmt.Sprintf("%d / %d / %d", c.Counts.Gets, c.Counts.Watches, c.Counts.Polls)},
	}
	if wide {
		rows = append(rows,
			[]string{"Generation freshness", o.GenerationFreshness},
			[]string{"Inspected conditions", strconv.Itoa(o.InspectedConditions)},
			[]string{"Read scope", "Bound parent GET; no runtime or IR reads"},
			[]string{"Cancellation", "Cooperative requests; plugins may ignore it"})
	}
	for i := range rows {
		rows[i][0] = printers.BoundedCell(rows[i][0], 22)
		rows[i][1] = printers.BoundedCell(rows[i][1], 54)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}
