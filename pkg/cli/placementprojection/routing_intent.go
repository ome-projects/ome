package placementprojection

import (
	"time"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omevalidation "sigs.k8s.io/ome/pkg/validation"
)

const (
	routingFactorScanLimit          = 256
	routingProbeStatusScanLimit     = 1000
	routingCapacityOptionScanLimit  = 256
	routingPublisherOptionScanLimit = 64
)

// projectRoutingIntent summarizes only persisted declaration evidence. It
// intentionally does not resolve operator defaults and never copies raw paths,
// free-form format names, map keys, map values, or validation errors.
func projectRoutingIntent(parent *ome.InferenceService) v.PlacementRoutingIntent {
	out := v.PlacementRoutingIntent{
		State:      "Absent",
		Enablement: "Inherited",
		CapacityFactors: v.PlacementRoutingCapacityFactors{
			Source: "Inherited",
		},
		Probe: v.PlacementRoutingProbeIntent{
			State: "Inherited", Method: "NotApplicable",
			AllFailedPolicy: "NotApplicable",
		},
		Capacity: v.PlacementRoutingCapacityIntent{
			State: "Inherited", Method: "NotApplicable",
		},
		Publisher: v.PlacementRoutingPublisherIntent{State: "Inherited"},
	}
	if parent == nil {
		return out
	}

	routing := parent.Spec.Routing
	var legacyFactorsPresent bool
	var legacyFactorCount int
	if parent.Spec.Placement != nil {
		// The legacy field remains observable during its API compatibility window.
		legacyFactorsPresent = parent.Spec.Placement.CapacityFactors != nil //nolint:staticcheck
		legacyFactorCount = len(parent.Spec.Placement.CapacityFactors)      //nolint:staticcheck
	}
	routingFactorsPresent := routing != nil && routing.CapacityFactors != nil

	switch {
	case routingFactorsPresent && legacyFactorsPresent:
		out.CapacityFactors.Source = "Conflict"
		out.CapacityFactors.Count = len(routing.CapacityFactors) + legacyFactorCount
	case routingFactorsPresent:
		out.CapacityFactors.Source = "Routing"
		out.CapacityFactors.Count = len(routing.CapacityFactors)
	case legacyFactorsPresent:
		out.CapacityFactors.Source = "LegacyPlacement"
		out.CapacityFactors.Count = legacyFactorCount
	}

	if routing == nil && !legacyFactorsPresent {
		return out
	}
	out.State = "Declared"
	if routing != nil {
		out.Enablement = routingEnablement(routing.Enabled)
		out.Probe = routingProbeIntent(routing.Probe)
		out.Capacity = routingCapacityIntent(routing.Capacity)
		out.Publisher = routingPublisherIntent(routing.Publisher)
	}

	if routingIntentExceedsBudget(routing, legacyFactorCount) {
		out.State = "BudgetExceeded"
		return out
	}
	if omevalidation.ValidateRouting(&parent.Spec) != nil {
		out.State = "Invalid"
	}
	return out
}

func routingEnablement(value *bool) v.PlacementValue {
	if value == nil {
		return "Inherited"
	}
	if *value {
		return "OptIn" // codespell:ignore optin
	}
	return "OptOut"
}

func routingProbeIntent(probe *ome.RoutingProbeSpec) v.PlacementRoutingProbeIntent {
	out := v.PlacementRoutingProbeIntent{
		State: "Inherited", Method: "NotApplicable",
		AllFailedPolicy: "NotApplicable",
	}
	if probe == nil {
		return out
	}
	if probe.Disabled {
		out.State = "Disabled"
		return out
	}
	out.State = "Configured"
	out.PathPresent = probe.Path != ""
	out.Method = routingMethod(probe.Method)
	out.AcceptStatusCount = len(probe.AcceptStatuses)
	out.GateStatusCount = len(probe.GateStatuses)
	out.Period = routingDeclaredDuration(probe.Period.Duration)
	out.Timeout = routingDeclaredDuration(probe.Timeout.Duration)
	out.FailureThreshold = probe.FailureThreshold
	out.SuccessThreshold = probe.SuccessThreshold
	switch probe.AllFailedPolicy {
	case ome.RoutingAllFailedPolicyPreserveTraffic:
		out.AllFailedPolicy = "PreserveTraffic"
	case ome.RoutingAllFailedPolicyDrain:
		out.AllFailedPolicy = "Drain"
	default:
		out.AllFailedPolicy = "Unknown"
	}
	return out
}

func routingCapacityIntent(capacity *ome.RoutingCapacitySpec) v.PlacementRoutingCapacityIntent {
	out := v.PlacementRoutingCapacityIntent{State: "Inherited", Method: "NotApplicable"}
	if capacity == nil {
		return out
	}
	if capacity.Disabled {
		out.State = "Disabled"
		return out
	}
	out.State = "Configured"
	out.PathPresent = capacity.Path != ""
	out.Method = routingMethod(capacity.Method)
	out.FormatPresent = capacity.Format != ""
	out.OptionCount = len(capacity.Options)
	out.Period = routingDeclaredDuration(capacity.Period.Duration)
	out.Timeout = routingDeclaredDuration(capacity.Timeout.Duration)
	out.Samples = capacity.Samples
	out.Quorum = capacity.Quorum
	out.MaxAge = routingDeclaredDuration(capacity.MaxAge.Duration)
	return out
}

func routingDeclaredDuration(value time.Duration) string {
	if value == 0 {
		return ""
	}
	return value.String()
}

func routingPublisherIntent(publisher *ome.RoutingPublisherSpec) v.PlacementRoutingPublisherIntent {
	if publisher == nil {
		return v.PlacementRoutingPublisherIntent{State: "Inherited"}
	}
	return v.PlacementRoutingPublisherIntent{
		State: "Configured", OptionCount: len(publisher.Options),
	}
}

func routingMethod(method string) v.PlacementValue {
	switch method {
	case "GET", "HEAD", "POST":
		return v.PlacementValue(method)
	default:
		return "Unknown"
	}
}

func routingIntentExceedsBudget(routing *ome.RoutingSpec, legacyFactorCount int) bool {
	if legacyFactorCount > routingFactorScanLimit || routing == nil {
		return legacyFactorCount > routingFactorScanLimit
	}
	if len(routing.CapacityFactors) > routingFactorScanLimit {
		return true
	}
	if routing.Probe != nil &&
		len(routing.Probe.AcceptStatuses)+len(routing.Probe.GateStatuses) > routingProbeStatusScanLimit {
		return true
	}
	if routing.Capacity != nil && len(routing.Capacity.Options) > routingCapacityOptionScanLimit {
		return true
	}
	return routing.Publisher != nil &&
		len(routing.Publisher.Options) > routingPublisherOptionScanLimit
}
