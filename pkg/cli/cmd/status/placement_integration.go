package status

import (
	"slices"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/placementcollection"
	"sigs.k8s.io/ome/pkg/cli/placementprojection"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

// projectStatusPlacement reuses the dedicated placement view's parent-only
// projection. It does not list WorkloadClusters or contact serving clusters.
func projectStatusPlacement(v *ome.InferenceService, clock r.Clock) r.StatusPlacement {
	if !statusHasPlacementSignal(v) {
		return r.StatusPlacement{State: "NotConfigured", Mode: "NotApplicable", ModeEvidence: "NotApplicable", Phase: "NotRecorded", Candidates: r.PlacementPreview{State: "NotRecorded"}, EndpointState: "NotRecorded", AdmittedReplicas: r.StatusPlacementCount{State: "NotApplicable"}, ReadyReplicas: r.StatusPlacementCount{State: "NotApplicable"}, Evidence: r.EvidenceUnavailable, Freshness: "NotApplicable"}
	}
	result, err := placementprojection.ProjectStatus(placementcollection.Result{InferenceService: v}, clock)
	if err != nil {
		return r.StatusPlacement{State: "Unavailable", Mode: "Unknown", ModeEvidence: "Unavailable", Phase: "NotRecorded", Evidence: r.EvidenceUnavailable, Freshness: "NotApplicable"}
	}
	p := result.Content
	value := r.StatusPlacement{
		State: "NotReported", Mode: p.Inputs.Mode, ModeEvidence: p.Inputs.ModeEvidence,
		Phase: "NotRecorded", Candidates: r.PlacementPreview{State: "NotRecorded"},
		EndpointState: "NotRecorded", AdmittedReplicas: r.StatusPlacementCount{State: "NotApplicable"},
		ReadyReplicas: r.StatusPlacementCount{State: "NotApplicable"},
		Evidence:      r.EvidenceUnavailable, Freshness: "NotApplicable",
	}
	if v.Status.Placement == nil {
		if value.Mode == "Split" {
			value.AdmittedReplicas.State, value.ReadyReplicas.State = "Unknown", "Unknown"
		}
		return value
	}
	value.State, value.Phase, value.Evidence, value.Freshness = "Reported", p.Placement.Phase, r.EvidenceReported, p.Placement.Source.Freshness
	value.ReportedCluster, value.Candidates, value.EndpointState = p.Placement.ReportedCluster, p.Placement.HomePreview, p.Placement.Address.State
	if value.Mode == "Split" {
		value.AdmittedReplicas = statusPlacementSum(p.Placement, true)
		value.ReadyReplicas = statusPlacementSum(p.Placement, false)
	}
	if len(p.Issues) > 0 || value.Mode == "Unknown" || value.Phase == "Unknown" || value.Phase == "NotRecorded" || value.EndpointState == "InvalidEndpoint" ||
		value.Candidates.State != "Validated" && value.Candidates.State != "NotRecorded" || value.Candidates.Truncated ||
		value.Mode == "Split" && value.Phase == "Placed" &&
			(value.AdmittedReplicas.State != "Reported" || value.ReadyReplicas.State != "Reported") {
		value.State = "Partial"
	}
	return value
}

func statusPlacementSum(p r.PlacementReported, admitted bool) r.StatusPlacementCount {
	preview := p.HomePreview
	if preview.State != "Validated" || preview.Truncated || preview.Total == 0 || preview.Kept != preview.Total || len(p.Homes) != preview.Total {
		if preview.State == "Validated" && preview.Total == 0 || preview.State == "NotRecorded" {
			return r.StatusPlacementCount{State: "Unknown"}
		}
		return r.StatusPlacementCount{State: "Unavailable"}
	}
	var sum int64
	for _, home := range p.Homes {
		count := home.ReadyReplicas
		if admitted {
			count = home.AdmittedReplicas
		}
		if count.State != "Reported" || count.Value == nil {
			return r.StatusPlacementCount{State: "Unknown"}
		}
		sum += int64(*count.Value)
	}
	return r.StatusPlacementCount{Value: &sum, State: "Reported"}
}

func statusHasPlacementSignal(v *ome.InferenceService) bool {
	if v == nil {
		return false
	}
	if v.Spec.Placement != nil || v.Status.Placement != nil || slices.Contains(v.Finalizers, "ome.io/placement") {
		return true
	}
	for _, key := range []string{"ome.io/accelerator-requirements", "ome.io/cluster-selector", constants.PlacementOrigin, constants.PlacementOriginUID, constants.PlacementControlPlane} {
		if _, ok := v.Annotations[key]; ok {
			return true
		}
		if _, ok := v.Labels[key]; ok {
			return true
		}
	}
	return false
}
