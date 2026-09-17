package v1alpha1

import "fmt"

// StatusPlacement is a bounded overview of parent-reported placement. The
// dedicated placement command retains per-home and routing detail.
type StatusPlacement struct {
	State            PlacementValue       `json:"state"`
	Mode             PlacementValue       `json:"mode"`
	ModeEvidence     PlacementValue       `json:"modeEvidence"`
	Phase            PlacementValue       `json:"phase"`
	ReportedCluster  string               `json:"reportedCluster,omitempty"`
	Candidates       PlacementPreview     `json:"candidates"`
	EndpointState    PlacementValue       `json:"endpointState"`
	AdmittedReplicas StatusPlacementCount `json:"admittedReplicas"`
	ReadyReplicas    StatusPlacementCount `json:"readyReplicas"`
	Evidence         EvidenceLevel        `json:"evidence"`
	Freshness        PlacementValue       `json:"freshness"`
}

// StatusPlacementCount represents an aggregate only when every reported home
// supplies a usable value. Zero in the API is not treated as observed zero.
type StatusPlacementCount struct {
	Value *int64         `json:"value,omitempty"`
	State PlacementValue `json:"state"`
}

func statusPlacementCanonical(value StatusPlacement) (StatusPlacement, []StatusIssueCode) {
	issues := []StatusIssueCode{}
	rawCluster := value.ReportedCluster
	value.State = clusterEnum(value.State, PlacementValue("Unavailable"), "NotConfigured", "NotReported", "Reported", "Partial", "Unavailable")
	value.Mode = clusterEnum(value.Mode, PlacementValue("Unknown"), "Single", "All", "Split", "Unknown", "NotApplicable")
	value.ModeEvidence = clusterEnum(value.ModeEvidence, PlacementValue("Unavailable"), "Declared", "Defaulted", "Unavailable", "NotApplicable")
	value.Phase = clusterEnum(value.Phase, PlacementValue("Unknown"), "Pending", "Racing", "Placed", "Failed", "NotRecorded", "Unknown")
	value.ReportedCluster = statusName(value.ReportedCluster, false)
	value.EndpointState = clusterEnum(value.EndpointState, PlacementValue("Unknown"), "Present", "NotRecorded", "InvalidEndpoint", "Unknown")
	value.Evidence = clusterEnum(value.Evidence, EvidenceUnavailable, EvidenceReported, EvidenceUnavailable)
	value.Freshness = clusterEnum(value.Freshness, PlacementValue("Unverifiable"), "Unverifiable", "NotApplicable")
	value.Candidates.State = clusterEnum(value.Candidates.State, PlacementValue("Unavailable"), "NotRecorded", "Validated", "MalformedPayload", "BudgetExceeded", "ConflictingDuplicates", "Unavailable")
	if value.Candidates.Total < 0 || value.Candidates.Kept < 0 || value.Candidates.Kept > value.Candidates.Total || value.Candidates.Kept > 64 {
		value.Candidates = PlacementPreview{State: "MalformedPayload"}
		issues = append(issues, "PlacementUnavailable")
	} else if value.Candidates.Total > 256 {
		value.Candidates.Total = 257
		value.Candidates.State = "BudgetExceeded"
		value.Candidates.Truncated = true
		issues = append(issues, "PlacementUnavailable")
	}
	value.AdmittedReplicas = statusPlacementCountCanonical(value.AdmittedReplicas)
	value.ReadyReplicas = statusPlacementCountCanonical(value.ReadyReplicas)
	aggregateRejected := false
	if value.Mode == "Split" && (value.Candidates.State != "Validated" || value.Candidates.Truncated || value.Candidates.Total == 0 || value.Candidates.Kept != value.Candidates.Total) {
		if value.AdmittedReplicas.State == "Reported" {
			value.AdmittedReplicas = StatusPlacementCount{State: "Unavailable"}
			aggregateRejected = true
		}
		if value.ReadyReplicas.State == "Reported" {
			value.ReadyReplicas = StatusPlacementCount{State: "Unavailable"}
			aggregateRejected = true
		}
	}
	if value.State == "NotConfigured" || value.State == "NotReported" || value.State == "Unavailable" {
		value.Evidence = EvidenceUnavailable
		value.Freshness = "NotApplicable"
		value.ReportedCluster = ""
		value.EndpointState = "NotRecorded"
		value.Candidates = PlacementPreview{State: "NotRecorded"}
		value.AdmittedReplicas = StatusPlacementCount{State: "NotApplicable"}
		value.ReadyReplicas = StatusPlacementCount{State: "NotApplicable"}
		if value.State == "NotReported" && value.Mode == "Split" {
			value.AdmittedReplicas.State, value.ReadyReplicas.State = "Unknown", "Unknown"
		}
		if value.State == "NotConfigured" {
			value.Mode, value.ModeEvidence = "NotApplicable", "NotApplicable"
		}
		value.Phase = "NotRecorded"
	} else if value.Evidence != EvidenceReported {
		value.State = "Unavailable"
		issues = append(issues, "PlacementUnavailable")
	} else {
		value.Freshness = "Unverifiable"
		if value.State == "Reported" && (aggregateRejected || value.Mode == "Unknown" || value.Mode == "NotApplicable" ||
			value.Phase == "Unknown" || value.Phase == "NotRecorded" ||
			value.Candidates.State != "Validated" && value.Candidates.State != "NotRecorded" || value.Candidates.Truncated ||
			value.EndpointState == "Unknown" || value.EndpointState == "InvalidEndpoint" ||
			rawCluster != "" && value.ReportedCluster == "" ||
			value.Mode == "Split" && (value.AdmittedReplicas.State == "Unavailable" || value.ReadyReplicas.State == "Unavailable" ||
				value.Phase == "Placed" && (value.AdmittedReplicas.State != "Reported" || value.ReadyReplicas.State != "Reported"))) {
			value.State = "Partial"
		}
	}
	if value.State == "Unavailable" {
		value.Phase = "NotRecorded"
		value.ReportedCluster = ""
		value.Candidates = PlacementPreview{State: "Unavailable"}
		value.EndpointState = "NotRecorded"
		value.Freshness = "NotApplicable"
		value.AdmittedReplicas = StatusPlacementCount{State: "Unavailable"}
		value.ReadyReplicas = StatusPlacementCount{State: "Unavailable"}
	}
	if value.Mode != "Split" {
		value.AdmittedReplicas = StatusPlacementCount{State: "NotApplicable"}
		value.ReadyReplicas = StatusPlacementCount{State: "NotApplicable"}
	}
	return value, issues
}

func statusPlacementCountCanonical(value StatusPlacementCount) StatusPlacementCount {
	value.State = clusterEnum(value.State, PlacementValue("Unavailable"), "Reported", "Unknown", "Unavailable", "NotApplicable")
	if value.State != "Reported" || value.Value == nil || *value.Value <= 0 || *value.Value > 64*2147483647 {
		if value.State == "Reported" {
			value.State = "Unavailable"
		}
		value.Value = nil
		return value
	}
	copy := *value.Value
	value.Value = &copy
	return value
}

func statusPlacementCountCell(value StatusPlacementCount) string {
	if value.State == "Reported" && value.Value != nil {
		return fmt.Sprint(*value.Value)
	}
	return string(value.State)
}
