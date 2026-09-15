// Package placementprojection presents immutable observations, not placement
// eligibility, admission, quota, routing, or serving-health predictions.
package placementprojection

import (
	"errors"
	"sort"

	"k8s.io/apimachinery/pkg/labels"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	c "sigs.k8s.io/ome/pkg/cli/placementcollection"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func ProjectStatus(snapshot c.Result, clock v.Clock) (v.PlacementStatusReport, error) {
	content, _, err := projectStatus(snapshot.InferenceService)
	if err != nil {
		return v.PlacementStatusReport{}, err
	}
	r := v.NewEnvelope(v.KindPlacementStatus, metadata(snapshot), content, clock)
	r.Sources = []v.SourceReference{primarySource(snapshot)}
	return r.Canonical(), nil
}

func ProjectExplain(snapshot c.Result, clock v.Clock) (v.PlacementExplainReport, error) {
	status, selector, err := projectStatus(snapshot.InferenceService)
	if err != nil {
		return v.PlacementExplainReport{}, err
	}
	content := v.PlacementExplainContent{Status: status, Fleet: snapshot.Fleet, Clusters: []v.PlacementCluster{}, UnobservedInputs: []v.PlacementValue{"RemoteConnection", "Admission", "Quota", "Capacity", "PolicyDistribution", "RolloutPreflight", "StickyPlacement"}}
	if len(snapshot.WorkloadClusters) > 64 {
		content.Fleet.State = "Unavailable"
		content.Fleet.Reason = "BudgetExceeded"
		content.Fleet.Complete = false
	} else {
		seen := map[string]*ome.WorkloadCluster{}
		invalid := false
		for i := range snapshot.WorkloadClusters {
			w := &snapshot.WorkloadClusters[i]
			if !publicName(w.Name) || w.Namespace != "" || w.Generation < 0 {
				invalid = true
				break
			}
			if prior, ok := seen[w.Name]; ok {
				if !equalCluster(prior, w) {
					invalid = true
					break
				}
				continue
			}
			seen[w.Name] = w
		}
		if invalid {
			content.Fleet.State = "Unavailable"
			content.Fleet.Reason = "MalformedPayload"
			content.Fleet.Complete = false
		} else {
			for _, w := range seen {
				row := v.PlacementCluster{Name: w.Name, ComputedSelectorCompatible: "Unknown", ReportedHome: "Unknown", ConnectionSource: connectionSource(w)}
				conditions, inspection := projectConditions(w.Status.Conditions, w.Generation, now(clock))
				row.ConditionPreview = inspection
				if inspection.State != "Validated" {
					addIssue(&content.Issues, "WorkloadClusterConditions", inspection.State)
				}
				row.ReportedReady = conditionValue(conditions, "Ready")
				if status.Inputs.RequirementsState == "NoRequirements" {
					row.ComputedSelectorCompatible = "NotApplicable"
				} else if selector != nil && validLabels(w.Labels) {
					row.ComputedSelectorCompatible = truth(selector.Matches(labels.Set(w.Labels)))
				}
				if status.Placement.HomePreview.State == "Validated" {
					row.ReportedHome = "False"
					for _, h := range snapshot.InferenceService.Status.Placement.Candidates {
						if h.Cluster == w.Name {
							row.ReportedHome = "True"
						}
					}
				}
				content.Clusters = append(content.Clusters, row)
			}
		}
	}
	r := v.NewEnvelope(v.KindPlacementExplain, metadata(snapshot), content, clock)
	r.Sources = []v.SourceReference{primarySource(snapshot), optionalSource("WorkloadCluster", "", "bounded-current-context-fleet", content.Fleet)}
	return r.Canonical(), nil
}

func ProjectEndpoint(snapshot c.Result, clock v.Clock) (v.PlacementEndpointReport, error) {
	status, _, err := projectStatus(snapshot.InferenceService)
	if err != nil {
		return v.PlacementEndpointReport{}, err
	}
	content := projectRouting(snapshot, status, now(clock))
	r := v.NewEnvelope(v.KindPlacementEndpoint, metadata(snapshot), content, clock)
	r.Sources = []v.SourceReference{primarySource(snapshot), optionalSource("TrafficMap", snapshot.InferenceService.Namespace, snapshot.InferenceService.Name, content.Routing.Acquisition)}
	return r.Canonical(), nil
}

func metadata(s c.Result) v.Metadata {
	return v.Metadata{Namespace: s.InferenceService.Namespace, Name: s.InferenceService.Name}
}
func primarySource(s c.Result) v.SourceReference {
	return v.SourceReference{Kind: "InferenceService", Namespace: s.InferenceService.Namespace, Name: s.InferenceService.Name, Evidence: v.EvidenceReported}
}

func optionalSource(kind, namespace, name string, acquisition v.PlacementAcquisition) v.SourceReference {
	out := v.SourceReference{Kind: kind, Namespace: namespace, Name: name, Evidence: v.EvidenceObserved}
	if acquisition.State == "Unavailable" || acquisition.State == "" {
		out.Evidence = v.EvidenceUnavailable
		switch acquisition.Reason {
		case "Forbidden":
			out.UnavailableReason = v.UnavailableForbidden
		case "NotFound":
			out.UnavailableReason = v.UnavailableNotFound
		case "UnsupportedAPI":
			out.UnavailableReason = v.UnavailableUnsupportedAPI
		case "OwnershipUnbound", "MalformedPayload":
			out.UnavailableReason = v.UnavailableMalformedPayload
		default:
			out.UnavailableReason = v.UnavailableUnreadable
		}
	}
	return out
}

func addIssue(issues *[]v.PlacementIssue, group, code v.PlacementValue) {
	for i := range *issues {
		if (*issues)[i].Group == group && (*issues)[i].Code == code {
			(*issues)[i].Count++
			return
		}
	}
	*issues = append(*issues, v.PlacementIssue{Group: group, Code: code, Count: 1})
}

func projectStatus(parent *ome.InferenceService) (v.PlacementStatusContent, labels.Selector, error) {
	if parent == nil || !publicName(parent.Name) || !publicNamespace(parent.Namespace) || parent.UID == "" || parent.Generation < 0 {
		return v.PlacementStatusContent{}, nil, errors.New("placement requires a valid identity-bound InferenceService")
	}
	inputs, selector := projectInputs(parent)
	content := v.PlacementStatusContent{Inputs: inputs, Placement: v.PlacementReported{Phase: "NotRecorded", Source: reported(), Address: address(nil), ServiceAddress: address(parent.Status.URL), Homes: []v.PlacementHome{}, HomePreview: v.PlacementPreview{State: "NotRecorded"}, ProvenancePreview: v.PlacementPreview{State: "NotRecorded"}}, Issues: []v.PlacementIssue{}}
	p := parent.Status.Placement
	if p == nil {
		return content, selector, nil
	}
	content.Placement.Phase = placementPhase(p.Phase)
	if p.Cluster != "" {
		if publicName(p.Cluster) {
			content.Placement.ReportedCluster = p.Cluster
		} else {
			addIssue(&content.Issues, "Placement", "InvalidCluster")
		}
	}
	content.Placement.Address = address(p.Endpoint)
	content.Placement.HomePreview.Total = len(p.Candidates)
	content.Placement.ProvenancePreview = v.PlacementPreview{State: "Unavailable", Total: len(p.Candidates)}
	if len(p.Candidates) > 256 {
		content.Placement.HomePreview.State = "BudgetExceeded"
		return content, selector, nil
	}
	seen := map[string]*ome.CandidatePlacement{}
	projected := map[string]v.PlacementHome{}
	content.Placement.ProvenancePreview.State = "Validated"
	for i := range p.Candidates {
		h := &p.Candidates[i]
		if !validHome(h) {
			content.Placement.HomePreview.State = "MalformedPayload"
			content.Placement.ProvenancePreview.State = "Unavailable"
			return content, selector, nil
		}
		if prior, ok := seen[h.Cluster]; ok {
			if !boundedProvenance(prior) || !boundedProvenance(h) || !equalHome(prior, h) {
				content.Placement.HomePreview.State = "ConflictingDuplicates"
				content.Placement.ProvenancePreview.State = "Unavailable"
				return content, selector, nil
			}
			continue
		}
		seen[h.Cluster] = h
		prov := provenance(h)
		if prov.State == "MalformedPayload" || prov.State == "BudgetExceeded" {
			addIssue(&content.Issues, "CandidateProvenance", prov.State)
			content.Placement.ProvenancePreview.State = "Unavailable"
		}
		projected[h.Cluster] = v.PlacementHome{Cluster: h.Cluster, Phase: candidatePhase(h.Phase), Address: address(h.Endpoint), Source: reported(), AdmittedReplicas: count(h.AdmittedReplicas, inputs.Mode == "Split"), ReadyReplicas: count(h.ReadyReplicas, inputs.Mode == "Split"), Provenance: prov}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	content.Placement.HomePreview = v.PlacementPreview{State: "Validated", Total: len(names), Kept: len(names), Truncated: len(names) > 64}
	content.Placement.ProvenancePreview.Total = len(names)
	content.Placement.ProvenancePreview.Kept = len(names)
	content.Placement.ProvenancePreview.Truncated = len(names) > 64
	if len(names) > 64 {
		names = names[:64]
		content.Placement.HomePreview.Kept = 64
		content.Placement.ProvenancePreview.Kept = 64
	}
	for _, name := range names {
		content.Placement.Homes = append(content.Placement.Homes, projected[name])
	}
	return content, selector, nil
}
