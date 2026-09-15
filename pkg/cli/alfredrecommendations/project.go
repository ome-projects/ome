package alfredrecommendations

import (
	"encoding/json"
	"io"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
	reportv1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// cycleWire mirrors only necessary fields of engine.cycleRecord. It never
// imports engines or decodes UUIDs, node hints, scheduling, or free text.
type cycleWire struct {
	Timestamp       string          `json:"timestamp"`
	Mode            string          `json:"mode"`
	Recommendations json.RawMessage `json:"recommendations"`
}

type recommendationWire struct {
	Workload       string `json:"workload"`
	Component      string `json:"component"`
	Instance       *int32 `json:"instance"`
	Policy         string `json:"policy"`
	Reason         string `json:"reason"`
	Outcome        string `json:"outcome"`
	AdvisoryReason string `json:"advisoryReason"`
	RejectReason   string `json:"rejectReason"`
	DispatchStatus string `json:"dispatchStatus"`
	DispatchReason string `json:"dispatchReason"`
	// Retained solely to distinguish reporter candidate keys; never emitted.
	FromNode string `json:"fromNode"`
}

// Project is pure: it applies bounded validation before sorting and output
// caps, and never emits untrusted error text or unrecognized values.
func Project(snapshot Snapshot, clock reportv1.Clock) reportv1.AlfredRecommendationsReport {
	c := reportv1.AlfredRecommendationsContent{
		SourceNamespace: snapshot.Namespace, ConfigName: snapshot.ConfigName, RecordName: snapshot.Config.RecordName,
		State: "Unavailable", ConfigState: snapshot.Config.State, RecordState: snapshot.RecordState,
		ConfigAuthority: "SelectedConfigMap", Authority: "AlfredReportedCycle", Freshness: "Unavailable",
		ConfigKey: snapshot.ConfigKey, RecordKey: RecordKey, ConfigMode: snapshot.Config.Mode,
		FreshnessWindowSeconds: 2 * snapshot.Config.Interval.Seconds(),
		Observations:           "LatestCycleOnly;NodeAndSchedulingDetailsOmitted",
	}
	r := reportv1.NewEnvelope("AlfredRecommendationsReport", reportv1.Metadata{Namespace: snapshot.Namespace, Name: snapshot.ConfigName}, c, clock)
	r.Sources = append(r.Sources, source(snapshot.Namespace, snapshot.ConfigName, snapshot.Config.State))
	if snapshot.Config.State != "Available" {
		return finish(r)
	}
	if snapshot.RecordState == "Disabled" {
		r.Content.State = "Disabled"
		return finish(r)
	}
	r.Sources = append(r.Sources, source(snapshot.Namespace, snapshot.Config.RecordName, snapshot.RecordState))
	if snapshot.RecordState != "Available" {
		return finish(r)
	}
	if len(snapshot.Record) > MaxRecordBytes {
		r.Content.RecordState = "Oversized"
		return finish(r)
	}
	var wire cycleWire
	if !validJSON(snapshot.Record) || json.Unmarshal([]byte(snapshot.Record), &wire) != nil {
		r.Content.RecordState = "Malformed"
		return finish(r)
	}
	var rows []recommendationWire
	if len(wire.Recommendations) == 0 || json.Unmarshal(wire.Recommendations, &rows) != nil {
		r.Content.RecordState = "Malformed"
		return finish(r)
	}
	stamp, err := time.Parse(time.RFC3339Nano, wire.Timestamp)
	if err != nil || stamp.IsZero() || (wire.Mode != "execute" && wire.Mode != "recommend-only") {
		r.Content.RecordState = "Malformed"
		return finish(r)
	}
	r.Content.RecordMode = wire.Mode
	r.Content.Timestamp = &stamp
	r.Content.Freshness = "Recent"
	if stamp.After(r.CollectedAt) {
		r.Content.Freshness = "Future"
	} else if stamp.Add(snapshot.Config.Interval).Add(snapshot.Config.Interval).Before(r.CollectedAt) {
		r.Content.Freshness = "Stale"
	}
	if wire.Mode != snapshot.Config.Mode {
		r.Content.Issues = append(r.Content.Issues, "ConfigRecordModeMismatch")
	}
	if len(rows) > MaxScannedRows {
		r.Content.RecordState = "ScanLimitExceeded"
		r.Content.Truncated = true
		r.Content.Omitted = len(rows)
		return finish(r)
	}
	r.Content.Scanned = len(rows)
	// Use a typed tuple: concatenation would permit ambiguous group identities.
	type key struct {
		workload, component, policy, reason, fromNode string
		instance                                      int32
	}
	keys := make([]key, len(rows))
	counts := make(map[key]int, len(keys))
	for i, row := range rows {
		index := int32(-1)
		if row.Instance != nil {
			index = *row.Instance
		}
		keys[i] = key{row.Workload, row.Component, row.Policy, row.Reason, row.FromNode, index}
		counts[keys[i]]++
	}
	for i, row := range rows {
		value, ok := projectRow(row)
		if counts[keys[i]] > 1 {
			ok = false
			r.Content.Duplicates++
		}
		if !ok {
			r.Content.Invalid++
			continue
		}
		r.Content.Recommendations = append(r.Content.Recommendations, value)
	}
	r.Content = r.Content.Canonical()
	if len(r.Content.Recommendations) > MaxRows {
		r.Content.Recommendations = r.Content.Recommendations[:MaxRows]
		r.Content.Truncated = true
	}
	r.Content.Rows = len(r.Content.Recommendations)
	r.Content.Omitted = r.Content.Scanned - r.Content.Rows
	r.Content.State = "Reported"
	if r.Content.Scanned == 0 {
		r.Content.State = "Empty"
	}
	if r.Content.Invalid > 0 {
		r.Content.Issues = append(r.Content.Issues, "InvalidRows")
	}
	if r.Content.Duplicates > 0 {
		r.Content.Issues = append(r.Content.Issues, "DuplicateCandidates")
	}
	if r.Content.Invalid > 0 || r.Content.Truncated {
		r.Content.State = "Partial"
	}
	return finish(r)
}

func source(ns, name, state string) reportv1.SourceReference {
	s := reportv1.SourceReference{Kind: "ConfigMap", Namespace: ns, Name: name, Evidence: reportv1.EvidenceObserved}
	if state != "Available" {
		s.Evidence = reportv1.EvidenceUnavailable
		s.UnavailableReason = reportv1.UnavailableUnreadable
		switch state {
		case "NotFound", "KeyAbsent":
			s.UnavailableReason = reportv1.UnavailableNotFound
		case "Forbidden":
			s.UnavailableReason = reportv1.UnavailableForbidden
		case "Malformed", "UnsupportedSchema", "IdentityMismatch", "Oversized":
			s.UnavailableReason = reportv1.UnavailableMalformedPayload
		}
	}
	return s
}

func finish(r reportv1.AlfredRecommendationsReport) reportv1.AlfredRecommendationsReport {
	c := &r.Content
	if c.State == "Unavailable" {
		r.Warnings = append(r.Warnings, reportv1.Warning{Code: reportv1.WarningSourceUnavailable})
	}
	if c.State == "Partial" {
		r.Warnings = append(r.Warnings, reportv1.Warning{Code: reportv1.WarningPartialData})
	}
	if c.Freshness == "Stale" || c.Freshness == "Future" {
		r.Warnings = append(r.Warnings, reportv1.Warning{Code: reportv1.WarningStaleEvidence})
	}
	if c.Truncated || c.RecordState == "Oversized" || c.ConfigState == "Oversized" {
		c.Truncated = true
		r.Warnings = append(r.Warnings, reportv1.Warning{Code: reportv1.WarningTruncated})
	}
	return r.Canonical()
}

func projectRow(w recommendationWire) (reportv1.AlfredRecommendation, bool) {
	r := reportv1.AlfredRecommendation{}
	parts := strings.Split(w.Workload, "/")
	if len(parts) != 2 || len(validation.IsDNS1123Label(parts[0])) != 0 || len(validation.IsDNS1123Subdomain(parts[1])) != 0 || w.Instance == nil || *w.Instance < 0 {
		return r, false
	}
	if w.FromNode != "" && len(validation.IsDNS1123Subdomain(w.FromNode)) != 0 {
		return r, false
	}
	if !oneOf(w.Component, "engine", "decoder", "router") || !oneOf(w.Policy, "defragmentation", "nodehealth", "migration-dispatch") || !oneOf(w.Reason, "Fragmentation", "NodeUnhealthy", "NodeMaintenance", "RemediationSignal") {
		return r, false
	}
	if !oneOf(w.AdvisoryReason, "", "NoSurgeHeadroom", "VolumePinned", "LWSMigrationUnsupported", "RawDeploymentMigrationUnsupported", "OMENativeUnavailable", "OMENativeObservationInvalid", "OMENativeStateIneligible", "NonExecutableObservedFragmentation", "MigrationSurfaceDisabled", "ModelUnresolved") || !validReject(w.RejectReason) || !validDispatchReason(w.DispatchReason) {
		return r, false
	}
	class := "Unverifiable"
	switch w.Outcome {
	case "advisory":
		class = "Advisory"
	case "withheld":
		class = "Withheld"
	case "rejected":
		class = "Rejected"
	case "admitted": // Reserved reporter constant; no execution inference.
	case "submitted", "acknowledged", "completed", "failed", "stalled":
		if w.DispatchStatus != w.Outcome {
			return r, false
		}
		class = "ReportedDispatch"
	default:
		return r, false
	}
	if w.DispatchStatus != "" && (w.DispatchStatus != w.Outcome || !oneOf(w.DispatchStatus, "withheld", "submitted", "acknowledged", "completed", "failed", "stalled")) {
		return r, false
	}
	if (w.RejectReason != "" && w.Outcome != "rejected") || (w.DispatchReason != "" && w.DispatchStatus == "") {
		return r, false
	}
	r = reportv1.AlfredRecommendation{Workload: w.Workload, Component: w.Component, Instance: *w.Instance,
		Policy: w.Policy, Reason: w.Reason, Outcome: w.Outcome, Classification: class, Executability: "Unverifiable",
		AdvisoryReason: w.AdvisoryReason, RejectReason: w.RejectReason, DispatchStatus: w.DispatchStatus, DispatchReason: w.DispatchReason}
	return r, true
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func validReject(value string) bool {
	return oneOf(value, "", "CircuitBreakerOpen", "InstanceGone", "NotMovable", "MalformedRequestPending", "MigrationStateInvalid", "InstanceTerminating", "WorkloadBusy", "AutoscalerActive", "Cooldown", "PlacementCooldown", "NodeCooldown", "TargetUnderEvacuation", "TargetUnavailable", "TargetNodeBusy", "NoCapacity", "InFlightCap", "HourlyCap")
}

func validDispatchReason(value string) bool {
	return validReject(value) || oneOf(value,
		"ExecutionDisabled", "JournalUnavailable", "MultipleUnresolvedRequests", "UnresolvedRequest",
		"JournalPayloadInvalid", "ObservationStale", "FailureBackoff", "InvalidRequest", "SerialDispatchLimit",
		"InvalidDispatchOptions", "InvalidCooldown", "ConfigurationChanged", "SimulationUnavailable",
		"DispatchDeadline", "GuardUnavailable", "ObservationUnavailable", "PolicyNoLongerEligible",
		"NotAdmitted", "SnapshotUnavailable", "SourceUnsupported", "SourceChanged", "SimulationNotFeasible",
		"SimulationStale", "SchedulingStateChanged", "SafetyStateChanged", "PredictedTargetUnsafe", "TargetCooldown",
		"OwnerUnavailable", "ReplicaUnavailable", "ReplicaIdentityChanged", "MigrationStatusInvalid",
		"TerminalStatusObserved", "UUIDStatusObserved", "ConsumerDeadlineExceeded", "AcknowledgedStatusMissing",
		"RequestPayloadChanged", "RequestAnnotationObserved", "AcknowledgementTimeout", "OwnerChanged",
		"SubmissionPrepared", "RequestSubmitted", "SubmissionUncertain", "SubmissionJournalUncertain")
}

// Reject duplicate JSON members (including in ignored payloads), excessive
// nesting, multiple documents, and non-object records before typed decoding.
func validJSON(raw string) bool {
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	if !jsonContainer(d, '{', 0) {
		return false
	}
	_, err = d.Token()
	return err == io.EOF
}

func jsonContainer(d *json.Decoder, opening json.Delim, depth int) bool {
	if depth > 32 {
		return false
	}
	seen := map[string]bool{}
	for d.More() {
		if opening == '{' {
			token, err := d.Token()
			if err != nil {
				return false
			}
			key, ok := token.(string)
			if !ok || seen[key] {
				return false
			}
			seen[key] = true
		}
		token, err := d.Token()
		if err != nil {
			return false
		}
		if delimiter, ok := token.(json.Delim); ok {
			if !jsonContainer(d, delimiter, depth+1) {
				return false
			}
		}
	}
	_, err := d.Token()
	return err == nil
}
