package placementprojection

import (
	"reflect"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	validation "k8s.io/apimachinery/pkg/util/validation"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	c "sigs.k8s.io/ome/pkg/cli/placementcollection"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/safetext"
)

func boundMap(parent *ome.InferenceService, tm *ome.TrafficMap) bool {
	if tm == nil || tm.Name != parent.Name || tm.Namespace != parent.Namespace || tm.Spec.Service != parent.Name || tm.Generation < 0 || tm.DeletionTimestamp != nil || parent.DeletionTimestamp != nil || len(tm.OwnerReferences) > 64 {
		return false
	}
	controllers := 0
	matched := false
	for _, owner := range tm.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		controllers++
		if len(owner.APIVersion) > 256 {
			return false
		}
		version, err := schema.ParseGroupVersion(owner.APIVersion)
		if err == nil && version.Group == "ome.io" && version.Version != "" && owner.Kind == "InferenceService" && owner.Name == parent.Name && owner.UID == parent.UID {
			matched = true
		}
	}
	return controllers == 1 && matched
}

func projectRouting(snapshot c.Result, status v.PlacementStatusContent, clock time.Time) v.PlacementEndpointContent {
	out := v.PlacementEndpointContent{Status: status, Entries: []v.PlacementRoute{}, Conditions: []v.PlacementCondition{}, ConditionPreview: v.PlacementPreview{State: "NotRecorded"}, Issues: []v.PlacementIssue{}, Routing: v.PlacementRouting{Acquisition: snapshot.TrafficMapAcquisition, Mode: "NotRecorded", Source: v.PlacementEvidence{Evidence: v.EvidenceUnavailable, Freshness: "Unavailable", Reason: snapshot.TrafficMapAcquisition.Reason}, Routable: conditionValue(nil, "Routable"), Programmed: conditionValue(nil, "Programmed"), Acknowledgement: "NoAcknowledgement", AcknowledgementSource: reported(), EntryPreview: v.PlacementPreview{State: "NotRecorded"}, ProbePreview: v.PlacementPreview{State: "NotRecorded"}}}
	tm := snapshot.TrafficMap
	if tm == nil {
		if out.Routing.Acquisition.State == "" {
			out.Routing.Acquisition.State = "Unavailable"
			out.Routing.Acquisition.Reason = "NotRecorded"
		}
		return out
	}
	if !boundMap(snapshot.InferenceService, tm) {
		out.Routing.Acquisition.State = "Unavailable"
		out.Routing.Acquisition.Reason = "OwnershipUnbound"
		out.Routing.Acquisition.Admitted = 0
		out.Routing.Acquisition.Complete = false
		out.Routing.Source.Reason = "OwnershipUnbound"
		return out
	}
	out.Routing.Mode = modeValue(tm.Spec.Mode)
	out.Routing.Source = freshness(tm.Spec.ObservedISVCGeneration, snapshot.InferenceService.Generation)
	out.Routing.Source.Evidence = v.EvidenceObserved
	out.Conditions, out.ConditionPreview = projectConditions(tm.Status.Conditions, tm.Generation, clock)
	if out.ConditionPreview.State != "Validated" {
		addIssue(&out.Issues, "TrafficMapConditions", out.ConditionPreview.State)
	}
	out.Routing.Routable = conditionValue(out.Conditions, "Routable")
	out.Routing.Programmed = conditionValue(out.Conditions, "Programmed")
	projectAcknowledgement(&out.Routing, tm)
	if ref := tm.Status.GatewayRef; ref != nil {
		ns := ref.Namespace
		if ns == "" {
			ns = tm.Namespace
		}
		if len(ref.Group) <= 253 && safetext.Sanitize(ref.Group, 253) == ref.Group && (ref.Group == "" || len(validation.IsDNS1123Subdomain(ref.Group)) == 0) && len(ref.Kind) <= 64 && safetext.Sanitize(ref.Kind, 64) == ref.Kind && len(validation.IsQualifiedName(ref.Kind)) == 0 && publicName(ref.Name) && publicNamespace(ns) {
			out.Routing.Gateway = &v.PlacementGateway{Group: ref.Group, Kind: ref.Kind, Namespace: ns, Name: ref.Name, State: "ReportedReferenceNotResolved"}
		} else {
			addIssue(&out.Issues, "Gateway", "MalformedReference")
		}
	}
	out.Routing.EntryPreview.Total = len(tm.Spec.Entries)
	out.Routing.ProbePreview = v.PlacementPreview{State: "Unavailable", Total: len(tm.Spec.Entries)}
	if len(tm.Spec.Entries) > 256 {
		out.Routing.EntryPreview.State = "BudgetExceeded"
		return out
	}
	seen := map[string]*ome.TrafficMapEntry{}
	for i := range tm.Spec.Entries {
		e := &tm.Spec.Entries[i]
		if !validEntry(e) {
			out.Routing.EntryPreview.State = "MalformedPayload"
			return out
		}
		if prior, ok := seen[e.Cluster]; ok {
			if !reflect.DeepEqual(prior, e) {
				out.Routing.EntryPreview.State = "ConflictingDuplicates"
				return out
			}
			continue
		}
		seen[e.Cluster] = e
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	out.Routing.EntryPreview = v.PlacementPreview{State: "Validated", Total: len(names), Kept: len(names), Truncated: len(names) > 64}
	out.Routing.ProbePreview = v.PlacementPreview{State: "Validated", Total: len(names), Kept: len(names), Truncated: len(names) > 64}
	projected := map[string]v.PlacementRoute{}
	for _, name := range names {
		e := seen[name]
		row := v.PlacementRoute{Cluster: name, Address: address(e.Endpoint), Weight: e.Weight, Healthy: e.Healthy, Source: out.Routing.Source, Probe: v.PlacementProbe{Result: "NotRecorded", ConsecutiveFailures: v.PlacementCount{State: "Unknown"}, Source: reported()}}
		if cap := e.Capacity; cap != nil {
			row.Capacity = &v.PlacementCapacity{Allocated: count(cap.Allocated, true), Ready: count(cap.Ready, true), Source: "UnknownOrDefaultedControlPlane", FactorState: "UnknownOrDefaultedIdentity", Reported: pointerCount(cap.Reported)}
			if cap.Source != "" {
				row.Capacity.Source = v.PlacementValue(cap.Source)
			}
			if cap.Factor != nil {
				factor := cap.Factor.DeepCopy()
				row.Capacity.Factor = factor.String()
				row.Capacity.FactorState = "Reported"
			}
			if cap.Reported != nil {
				row.Capacity.Reported.State = "Reported"
			}
		}
		if probe := e.Probe; probe != nil {
			if probe.LastProbeTime != nil && probe.LastProbeTime.Time.After(clock) {
				row.Probe = v.PlacementProbe{Result: "Invalid", ConsecutiveFailures: v.PlacementCount{State: "Unknown"}, Source: v.PlacementEvidence{Evidence: v.EvidenceUnavailable, Freshness: "Invalid", Reason: "FutureObservationTime"}}
				addIssue(&out.Issues, "RecordedProbes", "FutureObservationTime")
				out.Routing.ProbePreview.State = "Unavailable"
				projected[name] = row
				continue
			}
			row.Probe.Result = "Unknown"
			if probe.Result != "" {
				row.Probe.Result = v.PlacementValue(probe.Result)
			}
			row.Probe.ConsecutiveFailures = count(probe.ConsecutiveFailures, true)
			if probe.LastProbeTime != nil && !probe.LastProbeTime.IsZero() {
				value := probe.LastProbeTime.Time.UTC()
				row.Probe.LastAttemptTime = &value
			}
		}
		projected[name] = row
	}
	if len(names) > 64 {
		names = names[:64]
		out.Routing.EntryPreview.Kept = 64
		out.Routing.ProbePreview.Kept = 64
	}
	for _, name := range names {
		out.Entries = append(out.Entries, projected[name])
	}
	return out
}

func validEntry(e *ome.TrafficMapEntry) bool {
	if !publicName(e.Cluster) || e.Weight < 0 || (e.Endpoint != nil && address(e.Endpoint).State != "Present") {
		return false
	}
	if cap := e.Capacity; cap != nil {
		if cap.Allocated < 0 || cap.Ready < 0 || (cap.Reported != nil && *cap.Reported < 0) || (cap.Source != "" && cap.Source != ome.CapacitySourceControlPlane && cap.Source != ome.CapacitySourceEndpoint) {
			return false
		}
		if cap.Factor != nil {
			factor := cap.Factor.DeepCopy()
			if len(factor.String()) > 256 || factor.Sign() <= 0 {
				return false
			}
		}
	}
	if probe := e.Probe; probe != nil {
		if (probe.Result != "" && probe.Result != ome.ProbeResultPassing && probe.Result != ome.ProbeResultFailing && probe.Result != ome.ProbeResultUnknown) || probe.ConsecutiveFailures < 0 || len(probe.Message) > 4096 {
			return false
		}
	}
	return true
}

func projectAcknowledgement(out *v.PlacementRouting, tm *ome.TrafficMap) {
	condition := out.Programmed
	observed := tm.Status.ObservedTrafficMapGeneration
	out.AcknowledgementSource = freshness(observed, tm.Generation)
	if condition.Reason == "NotRecorded" && observed == 0 && !tm.Status.Programmed {
		out.Acknowledgement = "NoAcknowledgement"
		out.AcknowledgementSource.Reason = "NoAcknowledgement"
		return
	}
	if observed < 0 || observed > tm.Generation || condition.Source.Freshness == "Invalid" || (tm.Status.Programmed && condition.Status == "False") {
		out.Acknowledgement = "Invalid"
		out.AcknowledgementSource.Freshness = "Invalid"
		out.AcknowledgementSource.Reason = "ConflictingAcknowledgement"
		return
	}
	if condition.Reason != "NotRecorded" {
		var suppliedGeneration int64
		for _, raw := range tm.Status.Conditions {
			if raw.Type == "Programmed" {
				suppliedGeneration = raw.ObservedGeneration
			}
		}
		if observed != 0 && suppliedGeneration != 0 && observed != suppliedGeneration {
			out.Acknowledgement = "Invalid"
			out.AcknowledgementSource.Freshness = "Invalid"
			out.AcknowledgementSource.Reason = "ConflictingAcknowledgement"
			return
		}
		out.AcknowledgementSource = condition.Source
		if observed == 0 {
			out.AcknowledgementSource.Freshness = "Unverifiable"
			out.AcknowledgementSource.Reason = "NoObservationGeneration"
		}
		switch condition.Status {
		case "True":
			out.Acknowledgement = "ReportedTrue"
		case "False":
			out.Acknowledgement = "ReportedFalse"
		default:
			out.Acknowledgement = "Unknown"
		}
		return
	}
	if tm.Status.Programmed {
		out.Acknowledgement = "ReportedTrue"
	} else {
		out.Acknowledgement = "Unknown"
	}
}
