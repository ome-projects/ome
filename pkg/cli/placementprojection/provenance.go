package placementprojection

import (
	"reflect"
	"sort"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func equalHome(a, b *ome.CandidatePlacement) bool { return reflect.DeepEqual(a, b) }
func equalCluster(a, b *ome.WorkloadCluster) bool { return reflect.DeepEqual(a, b) }

func validHome(h *ome.CandidatePlacement) bool {
	if !publicName(h.Cluster) || !safeText(string(h.Phase), 64, false) || h.AdmittedReplicas < 0 || h.ReadyReplicas < 0 || (h.Endpoint != nil && address(h.Endpoint).State != "Present") {
		return false
	}
	return true
}

func provenanceBudget(h *ome.CandidatePlacement) bool {
	return (h.Autoscaling != nil && (len(h.Autoscaling.Policies) > 64 || len(h.Autoscaling.Components) > 3)) || (h.Rollout != nil && len(h.Rollout.ActiveGroups) > 64)
}

func boundedProvenance(h *ome.CandidatePlacement) bool {
	if a := h.Autoscaling; a != nil {
		if len(a.Policies) > 64 || len(a.Components) > 3 {
			return false
		}
		for _, p := range a.Policies {
			if !safeText(p.Name, 253, false) || !safeText(p.PortableDigest, 256, false) {
				return false
			}
		}
		for key, p := range a.Components {
			if !safeText(string(key), 64, false) || !safeText(p.ResolvedDigest, 256, false) {
				return false
			}
		}
	}
	if r := h.Rollout; r != nil {
		if len(r.ActiveGroups) > 64 || !safeText(r.ActiveRunID, 256, false) {
			return false
		}
		for _, g := range r.ActiveGroups {
			if !safeText(string(g.Source), 64, false) || !safeText(g.PolicyName, 253, false) || !safeText(g.PortableDigest, 256, false) {
				return false
			}
		}
		if r.LastRun != nil && (!safeText(string(r.LastRun.Outcome), 64, false) || !safeText(r.LastRun.Digest, 256, false)) {
			return false
		}
	}
	return true
}

func provenance(h *ome.CandidatePlacement) v.PlacementProvenance {
	out := v.PlacementProvenance{State: "NotRecorded", Source: reported(), Policies: []v.PlacementPolicy{}, Components: []v.PlacementComponent{}, ActiveGroups: []v.PlacementRolloutGroup{}, AutoscalingReady: "Unknown", LastOutcome: "NotRecorded", LastDigestState: "NotRecorded", PolicyPreview: v.PlacementPreview{State: "NotRecorded"}, GroupPreview: v.PlacementPreview{State: "NotRecorded"}}
	if h.Autoscaling == nil && h.Rollout == nil {
		return out
	}
	if provenanceBudget(h) {
		out.State = "BudgetExceeded"
		out.PolicyPreview.State = "Unavailable"
		out.GroupPreview.State = "Unavailable"
		return out
	}
	if !boundedProvenance(h) {
		out.State = "MalformedPayload"
		out.PolicyPreview.State = "Unavailable"
		out.GroupPreview.State = "Unavailable"
		return out
	}
	out.State = "Reported"
	invalid := func() v.PlacementProvenance {
		return v.PlacementProvenance{State: "MalformedPayload", Source: reported(), Policies: []v.PlacementPolicy{}, Components: []v.PlacementComponent{}, ActiveGroups: []v.PlacementRolloutGroup{}, AutoscalingReady: "Unknown", LastOutcome: "Unknown", LastDigestState: "Unavailable", PolicyPreview: v.PlacementPreview{State: "Unavailable"}, GroupPreview: v.PlacementPreview{State: "Unavailable"}}
	}
	if a := h.Autoscaling; a != nil {
		seen := map[string]string{}
		for _, p := range a.Policies {
			if !publicName(p.Name) || !safeDigest(p.PortableDigest, "pv1:") {
				return invalid()
			}
			if prior, ok := seen[p.Name]; ok && prior != p.PortableDigest {
				return invalid()
			}
			seen[p.Name] = p.PortableDigest
		}
		names := make([]string, 0, len(seen))
		for name := range seen {
			names = append(names, name)
		}
		sort.Strings(names)
		out.PolicyPreview = v.PlacementPreview{State: "Validated", Total: len(names), Kept: len(names), Truncated: len(names) > 16}
		if len(names) > 16 {
			names = names[:16]
			out.PolicyPreview.Kept = 16
		}
		for _, name := range names {
			out.Policies = append(out.Policies, v.PlacementPolicy{Name: name, PortableDigest: seen[name], DigestState: digestState(seen[name])})
		}
		for component, p := range a.Components {
			if (component != ome.EngineComponent && component != ome.DecoderComponent && component != ome.RouterComponent) || !safeDigest(p.ResolvedDigest, "rv1:") {
				return invalid()
			}
			out.Components = append(out.Components, v.PlacementComponent{Component: v.PlacementValue(component), ResolvedDigest: p.ResolvedDigest, DigestState: digestState(p.ResolvedDigest), Ready: optionalTruth(p.Ready)})
		}
		out.AutoscalingReady = optionalTruth(a.Ready)
	}
	if r := h.Rollout; r != nil {
		out.ActiveRunPresent = r.ActiveRunID != ""
		for _, g := range r.ActiveGroups {
			if (g.Source != ome.RolloutPlanSourceInline && g.Source != ome.RolloutPlanSourcePolicy) || !safeDigest(g.PortableDigest, "rp1:") || (g.Source == ome.RolloutPlanSourcePolicy && !publicName(g.PolicyName)) || (g.Source == ome.RolloutPlanSourceInline && g.PolicyName != "") {
				return invalid()
			}
		}
		out.GroupPreview = v.PlacementPreview{State: "Validated", Total: len(r.ActiveGroups), Kept: len(r.ActiveGroups), Truncated: len(r.ActiveGroups) > 16}
		keep := len(r.ActiveGroups)
		if keep > 16 {
			keep = 16
			out.GroupPreview.Kept = 16
		}
		for i := 0; i < keep; i++ {
			g := r.ActiveGroups[i]
			out.ActiveGroups = append(out.ActiveGroups, v.PlacementRolloutGroup{Ordinal: i, Source: v.PlacementValue(g.Source), PolicyName: g.PolicyName, PortableDigest: g.PortableDigest, DigestState: digestState(g.PortableDigest)})
		}
		if r.LastRun != nil {
			if !safeDigest(r.LastRun.Digest, "rp1:") {
				return invalid()
			}
			switch r.LastRun.Outcome {
			case ome.RolloutRunCompleted, ome.RolloutRunRolledBack, ome.RolloutRunSuperseded:
				out.LastOutcome = v.PlacementValue(r.LastRun.Outcome)
			default:
				out.LastOutcome = "Unknown"
			}
			out.LastDigest = r.LastRun.Digest
			out.LastDigestState = digestState(r.LastRun.Digest)
		}
	}
	return out
}

func digestState(value string) v.PlacementValue {
	if value == "" {
		return "NotRecorded"
	}
	return "Reported"
}
