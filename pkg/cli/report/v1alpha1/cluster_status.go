package v1alpha1

import (
	"cmp"
	"slices"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// ClusterObservationState describes the read, not remote connectivity.
type ClusterObservationState string

const (
	ClusterComplete    ClusterObservationState = "Complete"
	ClusterEmpty       ClusterObservationState = "Empty"
	ClusterPartial     ClusterObservationState = "Partial"
	ClusterUnavailable ClusterObservationState = "Unavailable"
)

// ClusterConnectionKind describes a declaration only.
type ClusterConnectionKind string

const (
	ClusterKubeConfigSecret ClusterConnectionKind = "KubeConfigSecret"
	ClusterProfile          ClusterConnectionKind = "ClusterProfile"
	ClusterInvalidSource    ClusterConnectionKind = "Invalid"
)

// ClusterConditionState distinguishes absent, malformed, and oversized groups.
type ClusterConditionState string

const (
	ClusterConditionReported  ClusterConditionState = "Reported"
	ClusterConditionMissing   ClusterConditionState = "Missing"
	ClusterConditionMalformed ClusterConditionState = "Malformed"
	ClusterConditionTruncated ClusterConditionState = "Truncated"
)

// ClusterReady is the closed controller-reported Ready value set.
type ClusterReady string

const (
	ClusterReadyTrue    ClusterReady = "True"
	ClusterReadyFalse   ClusterReady = "False"
	ClusterReadyUnknown ClusterReady = "Unknown"
)

// ClusterReason is a message-free allowlist. Arbitrary reason text is omitted.
type ClusterReason string

const (
	ClusterReasonConnected        ClusterReason = "Connected"
	ClusterReasonDisconnected     ClusterReason = "Disconnected"
	ClusterReasonConnectionFailed ClusterReason = "ConnectionFailed"
	ClusterReasonUnknown          ClusterReason = "Other"
	ClusterReasonMissing          ClusterReason = "Missing"
)

// ClusterConnectionState is controller evidence, never a successful probe.
type ClusterConnectionState string

const (
	ClusterConnectionUnknown          ClusterConnectionState = "Unknown"
	ClusterConnectionReportedReady    ClusterConnectionState = "ReportedReady"
	ClusterConnectionReportedNotReady ClusterConnectionState = "ReportedNotReady"
)

// ClusterProfileResolution explicitly records that no profile was resolved.
type ClusterProfileResolution string

const ClusterProfileNotAttempted ClusterProfileResolution = "NotAttempted"

// ClusterCondition is bounded public evidence, never a raw API condition.
type ClusterCondition struct {
	Type               string                `json:"type"`
	Status             ClusterReady          `json:"status"`
	ObservedGeneration int64                 `json:"observedGeneration"`
	Freshness          StatusFreshness       `json:"freshness"`
	Reason             ClusterReason         `json:"reason"`
	TransitionTime     *time.Time            `json:"transitionTime,omitempty"`
	TransitionState    ClusterConditionState `json:"transitionState"`
}

// ClusterStatusRow carries one independently evaluated object's evidence.
// Secret coordinates, messages, UID, and resource versions have no wire fields.
type ClusterStatusRow struct {
	Name                string                   `json:"name"`
	Generation          int64                    `json:"generation"`
	SourceKind          ClusterConnectionKind    `json:"declaredConnectionKind"`
	SourceState         ClusterConditionState    `json:"sourceState"`
	DeclaredProfile     string                   `json:"declaredProfile,omitempty"`
	ProfileResolution   ClusterProfileResolution `json:"profileResolution"`
	Evidence            EvidenceLevel            `json:"evidence"`
	ReportedReady       ClusterReady             `json:"reportedReady"`
	ConditionState      ClusterConditionState    `json:"conditionState"`
	Freshness           StatusFreshness          `json:"freshness"`
	ConnectionState     ClusterConnectionState   `json:"connectionState"`
	TotalConditions     int                      `json:"totalConditions"`
	ScannedConditions   int                      `json:"scannedConditions"`
	RetainedConditions  int                      `json:"retainedConditions"`
	ConditionsTruncated bool                     `json:"conditionsTruncated"`
	Conditions          []ClusterCondition       `json:"conditions"`
}

// ClusterStatusReport is the CLI-owned observational alpha output contract.
type ClusterStatusReport struct {
	APIVersion           string                  `json:"apiVersion"`
	Kind                 string                  `json:"kind"`
	CollectedAt          time.Time               `json:"collectedAt"`
	Observation          ClusterObservationState `json:"observation"`
	UnavailableReason    UnavailableReason       `json:"unavailableReason,omitempty"`
	ObservedPages        int                     `json:"observedPages"`
	ReturnedSources      int                     `json:"returnedSources"`
	AdmittedSources      int                     `json:"admittedSources"`
	SourceLimit          int                     `json:"sourceLimit"`
	PageLimit            int                     `json:"pageLimit"`
	PageSize             int64                   `json:"pageSize"`
	ConditionScanLimit   int                     `json:"conditionScanLimit"`
	ConditionOutputLimit int                     `json:"conditionOutputLimit"`
	SourcesTruncated     bool                    `json:"sourcesTruncated"`
	Clusters             []ClusterStatusRow      `json:"clusters"`
}

// Canonical owns every mutable slice and time pointer and imposes a total
// order on all public fields, including caller-supplied enum values and ties.
func (r ClusterStatusReport) Canonical() ClusterStatusReport {
	r.APIVersion, r.Kind = APIVersion, "ClusterStatusReport"
	r.CollectedAt = r.CollectedAt.UTC()
	r.Observation = clusterEnum(r.Observation, ClusterUnavailable, ClusterComplete, ClusterEmpty, ClusterPartial)
	r.UnavailableReason = clusterEnum(r.UnavailableReason, UnavailableUnreadable, "", UnavailableNotFound, UnavailableForbidden, UnavailableUnsupportedAPI, UnavailableMalformedPayload)
	r.Clusters = append([]ClusterStatusRow{}, r.Clusters...)
	for i := range r.Clusters {
		row := &r.Clusters[i]
		row.Name = clusterPublicName(row.Name)
		row.DeclaredProfile = clusterPublicName(row.DeclaredProfile)
		row.ProfileResolution = "NotAttempted"
		row.SourceKind = clusterEnum(row.SourceKind, ClusterInvalidSource, ClusterKubeConfigSecret, ClusterProfile)
		row.SourceState = clusterEnum(row.SourceState, ClusterConditionMalformed, ClusterConditionReported, ClusterConditionTruncated)
		row.Evidence = clusterEnum(row.Evidence, EvidenceUnavailable, EvidenceDeclared, EvidenceReported)
		row.ReportedReady = clusterEnum(row.ReportedReady, ClusterReadyUnknown, ClusterReadyTrue, ClusterReadyFalse)
		row.ConditionState = clusterEnum(row.ConditionState, ClusterConditionMalformed, ClusterConditionReported, ClusterConditionMissing, ClusterConditionTruncated)
		row.Freshness = clusterEnum(row.Freshness, StatusFreshnessInvalid, StatusFreshnessCurrent, StatusFreshnessStale, StatusFreshnessUnobserved)
		row.ConnectionState = clusterEnum(row.ConnectionState, "Unknown", "ReportedReady", "ReportedNotReady")
		row.Conditions = append([]ClusterCondition{}, row.Conditions...)
		for j := range row.Conditions {
			condition := &row.Conditions[j]
			condition.Type = clusterEnum(condition.Type, "Other", "Ready")
			condition.Status = clusterEnum(condition.Status, ClusterReadyUnknown, ClusterReadyTrue, ClusterReadyFalse)
			condition.Freshness = clusterEnum(condition.Freshness, StatusFreshnessInvalid, StatusFreshnessCurrent, StatusFreshnessStale, StatusFreshnessUnobserved)
			condition.Reason = clusterEnum(condition.Reason, ClusterReasonUnknown, ClusterReasonConnected, ClusterReasonDisconnected, ClusterReasonConnectionFailed, ClusterReasonMissing)
			condition.TransitionState = clusterEnum(condition.TransitionState, ClusterConditionMalformed, ClusterConditionReported, ClusterConditionMissing)
			if condition.TransitionTime != nil {
				value := condition.TransitionTime.UTC()
				condition.TransitionTime = &value
			}
		}
		slices.SortFunc(row.Conditions, func(a, b ClusterCondition) int {
			if a.Type != b.Type {
				if a.Type == "Ready" {
					return -1
				}
				return 1
			}
			return clusterConditionCompare(a, b)
		})
	}
	slices.SortFunc(r.Clusters, func(a, b ClusterStatusRow) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return clusterRowCompare(a, b)
	})
	return r
}

func clusterEnum[T ~string](value, fallback T, allowed ...T) T {
	if slices.Contains(allowed, value) {
		return value
	}
	return fallback
}

func clusterPublicName(name string) string {
	if name == "" {
		return ""
	}
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return ""
	}
	return name
}

func clusterConditionCompare(a, b ClusterCondition) int {
	return cmp.Or(
		cmp.Compare(a.Type, b.Type),
		cmp.Compare(a.Status, b.Status),
		cmp.Compare(a.ObservedGeneration, b.ObservedGeneration),
		cmp.Compare(a.Freshness, b.Freshness),
		cmp.Compare(a.Reason, b.Reason),
		clusterTimeCompare(a.TransitionTime, b.TransitionTime),
		cmp.Compare(a.TransitionState, b.TransitionState),
	)
}

func clusterTimeCompare(a, b *time.Time) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}
	if a.Before(*b) {
		return -1
	}
	if a.After(*b) {
		return 1
	}
	return 0
}

func clusterRowCompare(a, b ClusterStatusRow) int {
	return cmp.Or(
		cmp.Compare(a.Generation, b.Generation),
		cmp.Compare(a.SourceKind, b.SourceKind),
		cmp.Compare(a.SourceState, b.SourceState),
		cmp.Compare(a.DeclaredProfile, b.DeclaredProfile),
		cmp.Compare(a.ProfileResolution, b.ProfileResolution),
		cmp.Compare(a.Evidence, b.Evidence),
		cmp.Compare(a.ReportedReady, b.ReportedReady),
		cmp.Compare(a.ConditionState, b.ConditionState),
		cmp.Compare(a.Freshness, b.Freshness),
		cmp.Compare(a.ConnectionState, b.ConnectionState),
		cmp.Compare(a.TotalConditions, b.TotalConditions),
		cmp.Compare(a.ScannedConditions, b.ScannedConditions),
		cmp.Compare(a.RetainedConditions, b.RetainedConditions),
		clusterBoolCompare(a.ConditionsTruncated, b.ConditionsTruncated),
		slices.CompareFunc(a.Conditions, b.Conditions, clusterConditionCompare),
	)
}

func clusterBoolCompare(a, b bool) int {
	if a == b {
		return 0
	}
	if a {
		return 1
	}
	return -1
}

// Table is the compact physical 80-column view of the same canonical report.
func (r ClusterStatusReport) Table() report.Table {
	r = r.Canonical()
	t := report.Table{Headers: []string{"CLUSTER", "DECLARED", "READY", "FRESHNESS", "CONDITIONS"}, Rows: [][]string{}}
	add := func(values ...string) {
		widths := []int{20, 16, 7, 10, 10}
		for i := range values {
			values[i] = printers.BoundedCell(printers.OrDash(values[i]), widths[i])
		}
		t.Rows = append(t.Rows, values)
	}
	// Non-DNS context labels cannot collide with an observed resource name.
	add("@ observation", string(r.Observation), "", "", "")
	add("@ availability", string(r.UnavailableReason), "", "", "")
	add("@ sources", "returned="+strconv.Itoa(r.ReturnedSources), "kept="+strconv.Itoa(r.AdmittedSources), "max="+strconv.Itoa(r.SourceLimit), "cut="+strconv.FormatBool(r.SourcesTruncated))
	add("@ pages", "read="+strconv.Itoa(r.ObservedPages), "max="+strconv.Itoa(r.PageLimit), "size="+strconv.FormatInt(r.PageSize, 10), "")
	detailTruncated := false
	for _, row := range r.Clusters {
		source, conditions := string(row.SourceKind), string(row.ConditionState)
		if row.SourceState == ClusterConditionTruncated {
			source = printers.BoundedCell(source, 15) + "*"
			detailTruncated = true
		}
		if row.ConditionsTruncated {
			conditions += "*"
			detailTruncated = true
		}
		add(row.Name, source, string(row.ReportedReady), string(row.Freshness), conditions)
	}
	if len(r.Clusters) == 0 {
		add("-", "-", "Unknown", "Unobserved", string(r.Observation))
	}
	if detailTruncated {
		add("@ * = truncated", "", "", "", "")
	}
	return t
}

// WideTable exposes full safe details in narrow key/value rows, not a wider
// table. Long valid names wrap without losing public evidence.
func (r ClusterStatusReport) WideTable() report.Table {
	r = r.Canonical()
	t := report.Table{Headers: []string{"FIELD", "EVIDENCE"}, Rows: [][]string{}}
	add := func(key, value string) {
		if value == "" {
			value = "-"
		}
		for len(value) > 46 {
			t.Rows = append(t.Rows, []string{key, value[:46]})
			key = ""
			value = value[46:]
		}
		t.Rows = append(t.Rows, []string{key, value})
	}
	add("Observation", string(r.Observation))
	add("Unavailable reason", string(r.UnavailableReason))
	add("Source counts returned/admitted", strconv.Itoa(r.ReturnedSources)+"/"+strconv.Itoa(r.AdmittedSources))
	add("Pages/limit/size", strconv.Itoa(r.ObservedPages)+"/"+strconv.Itoa(r.PageLimit)+"/"+strconv.FormatInt(r.PageSize, 10))
	add("Source limit/truncated", strconv.Itoa(r.SourceLimit)+"/"+strconv.FormatBool(r.SourcesTruncated))
	add("Condition scan/output limits", strconv.Itoa(r.ConditionScanLimit)+"/"+strconv.Itoa(r.ConditionOutputLimit))
	add("Collected at", r.CollectedAt.Format(time.RFC3339Nano))
	for _, row := range r.Clusters {
		add("Cluster", row.Name)
		add("Generation", strconv.FormatInt(row.Generation, 10))
		add("Declared connection", string(row.SourceKind))
		add("Source state", string(row.SourceState))
		add("Declared profile", row.DeclaredProfile)
		add("Profile resolution", string(row.ProfileResolution))
		add("Evidence", string(row.Evidence))
		add("Reported Ready", string(row.ReportedReady))
		add("Ready condition state", string(row.ConditionState))
		add("Ready freshness", string(row.Freshness))
		add("Connection state", string(row.ConnectionState))
		add("Conditions total/scanned/kept", strconv.Itoa(row.TotalConditions)+"/"+strconv.Itoa(row.ScannedConditions)+"/"+strconv.Itoa(row.RetainedConditions))
		add("Conditions truncated", strconv.FormatBool(row.ConditionsTruncated))
		for _, c := range row.Conditions {
			add("Condition type/status", c.Type+"/"+string(c.Status))
			add("Observed generation/freshness", strconv.FormatInt(c.ObservedGeneration, 10)+"/"+string(c.Freshness))
			add("Reason classification", string(c.Reason))
			add("Transition evidence", string(c.TransitionState))
			if c.TransitionTime != nil {
				add("Transition time", c.TransitionTime.Format(time.RFC3339Nano))
			}
		}
	}
	return t
}
