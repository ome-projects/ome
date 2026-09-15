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

func projectConditions(raw []metav1.Condition, generation int64, clock time.Time) ([]v.PlacementCondition, v.PlacementPreview) {
	preview := v.PlacementPreview{State: "Validated", Total: len(raw)}
	out := []v.PlacementCondition{}
	if len(raw) > 64 {
		return []v.PlacementCondition{invalidCondition("Ready", "BudgetExceeded"), invalidCondition("Routable", "BudgetExceeded"), invalidCondition("Programmed", "BudgetExceeded")}, v.PlacementPreview{State: "BudgetExceeded", Total: len(raw)}
	}
	seen := map[string]*metav1.Condition{}
	invalid := map[string]v.PlacementValue{}
	for i := range raw {
		x := &raw[i]
		if len(x.Type) > 253 || len(x.Reason) > 1024 || len(x.Message) > 4096 {
			preview.State = "MalformedPayload"
			if len(x.Type) <= 253 {
				invalid[x.Type] = "MalformedCondition"
			}
			continue
		}
		if len(metavalidation.ValidateCondition(*x, field.NewPath("condition"))) != 0 || x.LastTransitionTime.Time.After(clock) {
			invalid[x.Type] = "MalformedCondition"
		}
		if prior, ok := seen[x.Type]; ok && !reflect.DeepEqual(prior, x) {
			invalid[x.Type] = "ConflictingDuplicates"
		}
		seen[x.Type] = x
	}
	for _, kind := range []string{"Ready", "Routable", "Programmed"} {
		if issue, ok := invalid[kind]; ok {
			out = append(out, invalidCondition(v.PlacementValue(kind), issue))
			continue
		}
		if x, ok := seen[kind]; ok {
			out = append(out, v.PlacementCondition{Type: v.PlacementValue(kind), Status: v.PlacementValue(x.Status), Reason: conditionReason(x.Reason), Source: freshness(x.ObservedGeneration, generation)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	preview.Kept = len(out)
	if len(invalid) > 0 {
		preview.State = "MalformedPayload"
	}
	return out, preview
}

func invalidCondition(kind, reason v.PlacementValue) v.PlacementCondition {
	return v.PlacementCondition{Type: kind, Status: "Unknown", Reason: reason, Source: v.PlacementEvidence{Evidence: v.EvidenceUnavailable, Freshness: "Invalid", Reason: reason}}
}
func conditionReason(reason string) v.PlacementValue {
	switch reason {
	case "Routable", "NotPlaced", "NoAddressableHome", "AllHomesUnready", "ProbeFailedRetrying", "ProbeSucceeded", "Connected", "ConnectionFailed", "ProbeFailed", "Programmed", "NotProgrammed", "Ready", "NotReady", "UnsupportedClusterSource", "UnsupportedProfileSource", "ClusterProfileUnsupported":
		return v.PlacementValue(reason)
	default:
		return "OtherReportedReason"
	}
}
