package placementprojection

import (
	"reflect"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metavalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func now(clock v.Clock) time.Time {
	if clock == nil {
		return time.Now().UTC()
	}
	return clock.Now().UTC()
}

func freshness(observed, current int64) v.PlacementEvidence {
	source := v.PlacementEvidence{Evidence: v.EvidenceReported, Freshness: "Current"}
	switch {
	case observed < 0 || current < 0 || observed > current:
		source.Freshness = "Invalid"
		source.Reason = "InvalidGeneration"
	case observed == 0 || current == 0:
		source.Freshness = "Unverifiable"
		source.Reason = "NoObservationGeneration"
	case observed < current:
		source.Freshness = "Stale"
		source.Reason = "StaleGeneration"
	}
	return source
}

func conditionValue(conditions []v.PlacementCondition, kind v.PlacementValue) v.PlacementCondition {
	for _, condition := range conditions {
		if condition.Type == kind {
			return condition
		}
	}
	return v.PlacementCondition{Type: kind, Status: "Unknown", Reason: "NotRecorded", Source: reported()}
}

type conditionRule struct {
	Type    v.PlacementValue
	Reasons map[string]struct{}
}

var (
	workloadClusterReadyRule = conditionRule{
		Type: "Ready",
		Reasons: conditionReasons(
			"ProbeFailedRetrying",
			"ProbeSucceeded",
			"Connected",
			"ConnectionFailed",
			"ProbeFailed",
			"Ready",
			"NotReady",
			"UnsupportedClusterSource",
			"UnsupportedProfileSource",
			"ClusterProfileUnsupported",
		),
	}
	trafficMapRoutableRule = conditionRule{
		Type: "Routable",
		Reasons: conditionReasons(
			"Routable",
			"NotPlaced",
			"NoAddressableHome",
			"AllHomesUnready",
			"NoRoutableCapacity",
			"AllHomesProbeFailed",
			"TrafficDrain",
		),
	}
	trafficMapPublishedRule = conditionRule{
		Type: "Published",
		Reasons: conditionReasons(
			"Published",
			"Withdrawn",
			"InvalidOwner",
			"InvalidOptions",
			"InvalidPlan",
			"LegacyLifecycleActive",
			"ClaimRejected",
			"DrainFailed",
			"ApplyFailed",
			"UnpublishFailed",
			"PublisherChanged",
			"Unpublished",
		),
	}
	trafficMapOverrideActiveRule = conditionRule{
		Type:    "OverrideActive",
		Reasons: conditionReasons("OverridesApplied", "OverridesPending", "NoOverrides"),
	}
	trafficMapCapacityFallbackRule = conditionRule{
		Type: "CapacityFallback",
		Reasons: conditionReasons(
			"CapacityPollingDisabled",
			"NoCapacityTargets",
			"EndpointCapacityAvailable",
			"EndpointCapacityUnavailable",
		),
	}
)

func conditionReasons(values ...string) map[string]struct{} {
	reasons := make(map[string]struct{}, len(values))
	for _, value := range values {
		reasons[value] = struct{}{}
	}
	return reasons
}

func projectConditions(raw []metav1.Condition, generation int64, clock time.Time, rules ...conditionRule) ([]v.PlacementCondition, v.PlacementPreview) {
	preview := v.PlacementPreview{State: "Validated", Total: len(raw)}
	out := []v.PlacementCondition{}
	if len(raw) > 64 {
		for _, rule := range rules {
			out = append(out, invalidCondition(rule.Type, "BudgetExceeded"))
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
		return out, v.PlacementPreview{State: "BudgetExceeded", Total: len(raw)}
	}
	seen := map[string]*metav1.Condition{}
	invalid := map[string]v.PlacementValue{}
	for i := range raw {
		x := &raw[i]
		typeWithinBudget := len(x.Type) <= 253
		if typeWithinBudget {
			if prior, ok := seen[x.Type]; ok && !reflect.DeepEqual(prior, x) {
				recordConditionIssue(invalid, x.Type, "ConflictingDuplicates")
			}
			seen[x.Type] = x
		}
		if !typeWithinBudget || len(x.Reason) > 1024 || len(x.Message) > 4096 {
			preview.State = "MalformedPayload"
			if typeWithinBudget {
				recordConditionIssue(invalid, x.Type, "MalformedCondition")
			}
			continue
		}
		if len(metavalidation.ValidateCondition(*x, field.NewPath("condition"))) != 0 || x.LastTransitionTime.Time.After(clock) {
			recordConditionIssue(invalid, x.Type, "MalformedCondition")
		}
	}
	for _, rule := range rules {
		kind := string(rule.Type)
		if issue, ok := invalid[kind]; ok {
			out = append(out, invalidCondition(rule.Type, issue))
			continue
		}
		if x, ok := seen[kind]; ok {
			out = append(out, v.PlacementCondition{Type: rule.Type, Status: v.PlacementValue(x.Status), Reason: conditionReason(x.Reason, rule.Reasons), Source: freshness(x.ObservedGeneration, generation)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	preview.Kept = len(out)
	if len(invalid) > 0 {
		preview.State = "MalformedPayload"
	}
	return out, preview
}

func recordConditionIssue(invalid map[string]v.PlacementValue, conditionType string, issue v.PlacementValue) {
	if invalid[conditionType] == "ConflictingDuplicates" && issue != "ConflictingDuplicates" {
		return
	}
	invalid[conditionType] = issue
}

func invalidCondition(kind, reason v.PlacementValue) v.PlacementCondition {
	return v.PlacementCondition{Type: kind, Status: "Unknown", Reason: reason, Source: v.PlacementEvidence{Evidence: v.EvidenceUnavailable, Freshness: "Invalid", Reason: reason}}
}
func conditionReason(reason string, reasons map[string]struct{}) v.PlacementValue {
	if _, ok := reasons[reason]; ok {
		return v.PlacementValue(reason)
	}
	return "OtherReportedReason"
}
