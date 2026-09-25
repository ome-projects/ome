package placementprojection

import (
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

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

type routingDrainEvidence struct {
	refs          []string
	canonicalRefs []string
	preview       v.PlacementPreview
}

type routingProbeEvidence struct {
	policyDigest      string
	policyDigestState v.PlacementValue
	gated             bool
	gatedState        v.PlacementValue
	issue             v.PlacementValue
}

type routingCapacityFallbackEvidence struct {
	value v.PlacementValue
	issue v.PlacementValue
}

type projectedTrafficMapEntry struct {
	source           *ome.TrafficMapEntry
	drain            routingDrainEvidence
	probe            routingProbeEvidence
	capacityFallback routingCapacityFallbackEvidence
}

func projectRouting(snapshot c.Result, status v.PlacementStatusContent, clock time.Time) v.PlacementEndpointContent {
	out := v.PlacementEndpointContent{Status: status, Entries: []v.PlacementRoute{}, Conditions: []v.PlacementCondition{}, ConditionPreview: v.PlacementPreview{State: "NotRecorded"}, Issues: []v.PlacementIssue{}, Routing: v.PlacementRouting{Acquisition: snapshot.TrafficMapAcquisition, Mode: "NotRecorded", Source: v.PlacementEvidence{Evidence: v.EvidenceUnavailable, Freshness: "Unavailable", Reason: snapshot.TrafficMapAcquisition.Reason}, Routable: conditionValue(nil, "Routable"), OverrideActive: conditionValue(nil, "OverrideActive"), CapacityFallback: conditionValue(nil, "CapacityFallback"), Published: conditionValue(nil, "Published"), Acknowledgement: "NoAcknowledgement", AcknowledgementSource: reported(), EntryPreview: v.PlacementPreview{State: "NotRecorded"}, ProbePreview: v.PlacementPreview{State: "NotRecorded"}}}
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
	out.Conditions, out.ConditionPreview = projectConditions(
		tm.Status.Conditions,
		tm.Generation,
		clock,
		trafficMapRoutableRule,
		trafficMapPublishedRule,
		trafficMapOverrideActiveRule,
		trafficMapCapacityFallbackRule,
	)
	if out.ConditionPreview.State != "Validated" {
		addIssue(&out.Issues, "TrafficMapConditions", out.ConditionPreview.State)
	}
	out.Routing.Routable = conditionValue(out.Conditions, "Routable")
	out.Routing.Published = conditionValue(out.Conditions, "Published")
	out.Routing.OverrideActive = conditionValue(out.Conditions, "OverrideActive")
	out.Routing.CapacityFallback = conditionValue(out.Conditions, "CapacityFallback")
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
	seen := map[string]*projectedTrafficMapEntry{}
	for i := range tm.Spec.Entries {
		e := &tm.Spec.Entries[i]
		if !validEntry(e) {
			out.Routing.EntryPreview.State = "MalformedPayload"
			return out
		}
		candidate := preflightTrafficMapEntry(e, clock)
		if prior, ok := seen[e.Cluster]; ok {
			if !equalTrafficMapEntry(prior.source, e) {
				out.Routing.EntryPreview.State = "ConflictingDuplicates"
				return out
			}
			prior.drain = mergeRoutingDrainEvidence(prior.drain, candidate.drain)
			prior.probe = mergeRoutingProbeEvidence(prior.probe, candidate.probe)
			prior.capacityFallback = mergeRoutingCapacityFallbackEvidence(prior.capacityFallback, candidate.capacityFallback)
			continue
		}
		seen[e.Cluster] = &candidate
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
		entry := seen[name]
		e := entry.source
		row := v.PlacementRoute{Cluster: name, Address: address(e.Endpoint), Weight: e.Weight, Healthy: e.Healthy, DrainRefs: []string{}, DrainPreview: v.PlacementPreview{State: "NotRecorded"}, Source: out.Routing.Source, Probe: v.PlacementProbe{Result: "NotRecorded", PolicyDigestState: "NotRecorded", GatedState: "NotRecorded", ConsecutiveFailures: v.PlacementCount{State: "Unknown"}, Source: reported()}}
		row.DrainRefs = append([]string{}, entry.drain.refs...)
		row.DrainPreview = entry.drain.preview
		if row.DrainPreview.State != "Validated" {
			addIssue(&out.Issues, "DrainRefs", row.DrainPreview.State)
		}
		if cap := e.Capacity; cap != nil {
			row.Capacity = &v.PlacementCapacity{Allocated: count(cap.Allocated, true), Ready: count(cap.Ready, true), Source: "UnknownOrDefaultedControlPlane", FactorState: "UnknownOrDefaultedIdentity", Reported: pointerCount(cap.Reported), FallbackReason: entry.capacityFallback.value}
			if entry.capacityFallback.issue != "" {
				addIssue(&out.Issues, "CapacityFallbackReasons", entry.capacityFallback.issue)
			}
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
			if entry.probe.issue != "" {
				row.Probe = unavailableProbe(entry.probe.issue)
				addIssue(&out.Issues, "RecordedProbes", entry.probe.issue)
				out.Routing.ProbePreview.State = "Unavailable"
				projected[name] = row
				continue
			}
			row.Probe.Result = "Unknown"
			row.Probe.PolicyDigest = entry.probe.policyDigest
			row.Probe.PolicyDigestState = entry.probe.policyDigestState
			row.Probe.Gated = entry.probe.gated
			row.Probe.GatedState = entry.probe.gatedState
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

func preflightTrafficMapEntry(source *ome.TrafficMapEntry, clock time.Time) projectedTrafficMapEntry {
	out := projectedTrafficMapEntry{
		source: source,
		drain:  projectRoutingDrainEvidence(source.DrainRefs),
		probe:  projectRoutingProbeEvidence(source.Probe, clock),
	}
	if source.Capacity != nil {
		out.capacityFallback = projectRoutingCapacityFallbackEvidence(source.Capacity.FallbackReason)
	}
	return out
}

func projectRoutingProbeEvidence(probe *ome.TrafficMapProbe, clock time.Time) routingProbeEvidence {
	out := routingProbeEvidence{policyDigestState: "NotRecorded", gatedState: "NotRecorded"}
	if probe == nil {
		return out
	}
	if probe.LastProbeTime != nil && probe.LastProbeTime.Time.After(clock) {
		return routingProbeEvidence{policyDigestState: "Unavailable", gatedState: "Unavailable", issue: "FutureObservationTime"}
	}
	digest, state, ok := projectProbePolicyDigest(probe.PolicyDigest)
	if !ok {
		return routingProbeEvidence{policyDigestState: "Unavailable", gatedState: "Unavailable", issue: "MalformedPayload"}
	}
	return routingProbeEvidence{policyDigest: digest, policyDigestState: state, gated: probe.Gated, gatedState: "Reported"}
}

func projectRoutingCapacityFallbackEvidence(raw string) routingCapacityFallbackEvidence {
	value, ok := projectCapacityFallbackReason(raw)
	out := routingCapacityFallbackEvidence{value: value}
	if !ok {
		out.issue = "MalformedPayload"
	}
	return out
}

func mergeRoutingDrainEvidence(left, right routingDrainEvidence) routingDrainEvidence {
	total := max(left.preview.Total, right.preview.Total)
	truncated := left.preview.Truncated || right.preview.Truncated
	if left.preview.State == "ConflictingDuplicates" || right.preview.State == "ConflictingDuplicates" {
		return routingDrainEvidence{refs: []string{}, canonicalRefs: []string{}, preview: v.PlacementPreview{State: "ConflictingDuplicates", Total: total, Truncated: truncated}}
	}
	if left.preview.State == right.preview.State {
		switch left.preview.State {
		case "MalformedPayload", "BudgetExceeded":
			return routingDrainEvidence{refs: []string{}, canonicalRefs: []string{}, preview: v.PlacementPreview{State: left.preview.State, Total: total, Truncated: truncated}}
		case "Validated":
			if left.preview.Total == right.preview.Total && left.preview.Truncated == right.preview.Truncated && slices.Equal(left.canonicalRefs, right.canonicalRefs) {
				return routingDrainEvidence{refs: append([]string{}, left.refs...), canonicalRefs: append([]string{}, left.canonicalRefs...), preview: v.PlacementPreview{State: "Validated", Total: total, Kept: len(left.refs), Truncated: truncated}}
			}
		}
	}
	return routingDrainEvidence{refs: []string{}, canonicalRefs: []string{}, preview: v.PlacementPreview{State: "ConflictingDuplicates", Total: total, Truncated: truncated}}
}

func mergeRoutingProbeEvidence(left, right routingProbeEvidence) routingProbeEvidence {
	if left.issue == "ConflictingDuplicates" || right.issue == "ConflictingDuplicates" {
		return routingProbeEvidence{policyDigestState: "Unavailable", gatedState: "Unavailable", issue: "ConflictingDuplicates"}
	}
	if left == right {
		return left
	}
	return routingProbeEvidence{policyDigestState: "Unavailable", gatedState: "Unavailable", issue: "ConflictingDuplicates"}
}

func mergeRoutingCapacityFallbackEvidence(left, right routingCapacityFallbackEvidence) routingCapacityFallbackEvidence {
	if left.issue == "ConflictingDuplicates" || right.issue == "ConflictingDuplicates" {
		return routingCapacityFallbackEvidence{value: "ConflictingDuplicates", issue: "ConflictingDuplicates"}
	}
	if left == right {
		return left
	}
	return routingCapacityFallbackEvidence{value: "ConflictingDuplicates", issue: "ConflictingDuplicates"}
}

func projectDrainRefs(raw []string) ([]string, v.PlacementPreview) {
	evidence := projectRoutingDrainEvidence(raw)
	return evidence.refs, evidence.preview
}

func projectRoutingDrainEvidence(raw []string) routingDrainEvidence {
	preview := v.PlacementPreview{State: "Validated", Total: len(raw)}
	if len(raw) > 64 {
		preview.State = "BudgetExceeded"
		preview.Truncated = true
		return routingDrainEvidence{refs: []string{}, canonicalRefs: []string{}, preview: preview}
	}
	seen := make(map[string]struct{}, len(raw))
	refs := make([]string, 0, len(raw))
	for _, ref := range raw {
		if !validDrainRef(ref) {
			preview.State = "MalformedPayload"
			return routingDrainEvidence{refs: []string{}, canonicalRefs: []string{}, preview: preview}
		}
		if _, ok := seen[ref]; ok {
			preview.State = "MalformedPayload"
			return routingDrainEvidence{refs: []string{}, canonicalRefs: []string{}, preview: preview}
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	preview.Kept = len(refs)
	retained := refs
	if len(refs) > 16 {
		retained = refs[:16]
		preview.Kept = 16
		preview.Truncated = true
	}
	return routingDrainEvidence{refs: retained, canonicalRefs: refs, preview: preview}
}

func validDrainRef(ref string) bool {
	if ref == "" || len(ref) > 128 || !utf8.ValidString(ref) || strings.TrimSpace(ref) != ref {
		return false
	}
	if hasDrainRefUserinfo(ref) {
		return false
	}
	for _, char := range ref {
		if unicode.IsControl(char) {
			return false
		}
	}
	return safetext.Sanitize(ref, 128) == ref
}

func hasDrainRefUserinfo(ref string) bool {
	parsed, err := url.Parse(ref)
	if err == nil {
		if parsed.User != nil {
			return true
		}
	}
	return hasCredentialShapedFallbackAuthority(ref)
}

func hasCredentialShapedFallbackAuthority(ref string) bool {
	candidate := ref
	for attempts := 0; attempts <= len(ref); attempts++ {
		if hasCredentialShapedAuthorityLayer(candidate) {
			return true
		}
		decoded, err := url.PathUnescape(candidate)
		if err != nil {
			return true
		}
		if decoded == candidate {
			return false
		}
		candidate = decoded
	}
	return false
}

func hasCredentialShapedAuthorityLayer(ref string) bool {
	remainder := ref
	explicitAuthority := false
	if strings.HasPrefix(remainder, "//") {
		remainder = remainder[2:]
		explicitAuthority = true
	} else if scheme := strings.Index(remainder, "://"); scheme > 0 && validDrainRefScheme(remainder[:scheme]) {
		remainder = remainder[scheme+3:]
		explicitAuthority = true
	}
	authority := remainder
	hasSuffix := false
	if boundary := strings.IndexAny(remainder, "/?#"); boundary >= 0 {
		authority = remainder[:boundary]
		hasSuffix = true
	}
	at := strings.LastIndexByte(authority, '@')
	if at <= 0 || at == len(authority)-1 {
		return false
	}
	if explicitAuthority {
		return true
	}
	host := authority[at+1:]
	if !hasSuffix && !strings.ContainsAny(host, ".:[]") && !strings.EqualFold(host, "localhost") {
		return false
	}
	userinfo := authority[:at]
	decodedUserinfo, err := url.PathUnescape(userinfo)
	if err != nil {
		return true
	}
	return strings.ContainsRune(decodedUserinfo, ':')
}

func validDrainRefScheme(value string) bool {
	if value == "" || value[0] < 'A' || value[0] > 'Z' && value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, char := range value[1:] {
		if char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '+' || char == '-' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func projectProbePolicyDigest(raw string) (string, v.PlacementValue, bool) {
	if raw == "" {
		return "", "NotRecorded", true
	}
	const prefix = "sha256:"
	if len(raw) != len(prefix)+64 || !strings.HasPrefix(raw, prefix) {
		return "", "Unavailable", false
	}
	for _, char := range raw[len(prefix):] {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return "", "Unavailable", false
		}
	}
	return raw, "Reported", true
}

func unavailableProbe(reason v.PlacementValue) v.PlacementProbe {
	return v.PlacementProbe{Result: "Invalid", PolicyDigestState: "Unavailable", GatedState: "Unavailable", ConsecutiveFailures: v.PlacementCount{State: "Unknown"}, Source: v.PlacementEvidence{Evidence: v.EvidenceUnavailable, Freshness: "Invalid", Reason: reason}}
}

func projectCapacityFallbackReason(raw string) (v.PlacementValue, bool) {
	if len(raw) > 4096 || !utf8.ValidString(raw) {
		return "MalformedPayload", false
	}
	for _, char := range raw {
		if unicode.IsControl(char) {
			return "MalformedPayload", false
		}
	}
	switch {
	case raw == "":
		return "NotRecorded", true
	case raw == "no capacity report yet":
		return "NoReport", true
	case strings.HasPrefix(raw, "capacity target is invalid:"):
		return "InvalidTarget", true
	case raw == "capacity clock is not configured":
		return "ClockUnavailable", true
	case raw == "capacity observer executor is not configured", strings.HasPrefix(raw, "capacity observation could not be submitted:"):
		return "ObserverUnavailable", true
	case strings.HasPrefix(raw, "capacity endpoint unreachable:"):
		return "Unreachable", true
	case strings.HasPrefix(raw, "capacity endpoint returned HTTP "):
		return "HTTPError", true
	case strings.HasPrefix(raw, "reading capacity response:"):
		return "ResponseReadError", true
	case strings.HasPrefix(raw, "capacity request did not complete:"):
		return "RequestIncomplete", true
	case strings.HasPrefix(raw, "capacity report is negative (") || raw == "capacity report timestamp is in the future":
		return "InvalidReport", true
	case strings.HasPrefix(raw, "capacity report is stale ("):
		return "StaleReport", true
	case strings.HasPrefix(raw, "capacity report awaiting quorum (") || strings.HasPrefix(raw, "capacity report awaiting quorum after stale samples expired ("):
		return "AwaitingQuorum", true
	default:
		return "OtherReportedReason", true
	}
}

func equalTrafficMapEntry(left, right *ome.TrafficMapEntry) bool {
	return reflect.DeepEqual(boundedTrafficMapEntryBase(left), boundedTrafficMapEntryBase(right))
}

func boundedTrafficMapEntryBase(source *ome.TrafficMapEntry) ome.TrafficMapEntry {
	out := *source
	out.DrainRefs = nil
	if source.Probe != nil {
		probe := *source.Probe
		probe.PolicyDigest = ""
		probe.Gated = false
		out.Probe = &probe
	}
	if source.Capacity != nil {
		capacity := *source.Capacity
		capacity.FallbackReason = ""
		out.Capacity = &capacity
	}
	return out
}

func validEntry(e *ome.TrafficMapEntry) bool {
	if !publicName(e.Cluster) || e.Weight < 0 || e.Weight > 1_000_000 || (e.Endpoint != nil && address(e.Endpoint).State != "Present") {
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
	condition := out.Published
	observed := tm.Status.ObservedTrafficMapGeneration
	out.AcknowledgementSource = freshness(observed, tm.Generation)
	if condition.Reason == "NotRecorded" && observed == 0 && !tm.Status.Published {
		out.Acknowledgement = "NoAcknowledgement"
		out.AcknowledgementSource.Reason = "NoAcknowledgement"
		return
	}
	conditionRecorded := condition.Reason != "NotRecorded"
	failure := publisherFailureReason(condition.Reason) && condition.Status == "False"
	if observed < 0 || observed > tm.Generation || condition.Source.Freshness == "Invalid" ||
		(conditionRecorded && tm.Status.Published && condition.Status != "True") ||
		(conditionRecorded && !validPublisherConditionShape(condition)) {
		out.Acknowledgement = "Invalid"
		out.AcknowledgementSource.Freshness = "Invalid"
		out.AcknowledgementSource.Reason = "ConflictingAcknowledgement"
		return
	}
	if condition.Reason != "NotRecorded" {
		var suppliedGeneration int64
		for _, raw := range tm.Status.Conditions {
			if raw.Type == "Published" {
				suppliedGeneration = raw.ObservedGeneration
			}
		}
		if observed != 0 && suppliedGeneration != 0 && observed != suppliedGeneration &&
			(!failure || observed > suppliedGeneration) {
			out.Acknowledgement = "Invalid"
			out.AcknowledgementSource.Freshness = "Invalid"
			out.AcknowledgementSource.Reason = "ConflictingAcknowledgement"
			return
		}
		out.AcknowledgementSource = condition.Source
		if observed == 0 && !failure {
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
	if tm.Status.Published {
		out.Acknowledgement = "ReportedTrue"
	} else {
		out.Acknowledgement = "Unknown"
	}
}

func publisherFailureReason(reason v.PlacementValue) bool {
	switch reason {
	case "InvalidOwner", "InvalidOptions", "InvalidPlan", "LegacyLifecycleActive", "ClaimRejected", "DrainFailed", "ApplyFailed", "UnpublishFailed", "PublisherChanged":
		return true
	default:
		return false
	}
}

func validPublisherConditionShape(condition v.PlacementCondition) bool {
	switch condition.Reason {
	case "Published":
		return condition.Status == "True"
	case "Withdrawn", "Unpublished":
		return condition.Status == "False"
	default:
		return publisherFailureReason(condition.Reason) && condition.Status == "False"
	}
}
