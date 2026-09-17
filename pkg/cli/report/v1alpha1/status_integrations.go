package v1alpha1

import "slices"

// StatusSummaryState describes a compact, independently acquired feature view.
type StatusSummaryState string

const (
	StatusSummaryReported      StatusSummaryState = "Reported"
	StatusSummaryPartial       StatusSummaryState = "Partial"
	StatusSummaryNotConfigured StatusSummaryState = "NotConfigured"
	StatusSummaryInvalid       StatusSummaryState = "Invalid"
	StatusSummaryUnavailable   StatusSummaryState = "Unavailable"
)

// StatusSummaryReason is fixed vocabulary; raw API errors never enter status.
type StatusSummaryReason string

const (
	StatusReasonAutoSelectionNotProbed StatusSummaryReason = "AutoSelectionNotProbed"
	StatusReasonVirtualDeployment      StatusSummaryReason = "VirtualDeployment"
	StatusReasonReadFailed             StatusSummaryReason = "ReadFailed"
	StatusReasonProjectionInvalid      StatusSummaryReason = "ProjectionInvalid"
)

// StatusTraffic copies only the controller-reported traffic summary. Policy
// freshness is specific to the BackendPolicyReady condition, not a data-plane
// realization claim.
type StatusTraffic struct {
	State           TrafficState           `json:"state"`
	Translator      TrafficTranslator      `json:"translator,omitempty"`
	Algorithm       TrafficAlgorithm       `json:"algorithm,omitempty"`
	PolicyReady     TrafficConditionStatus `json:"policyReady,omitempty"`
	PolicyFreshness TrafficFreshness       `json:"policyFreshness,omitempty"`
	Evidence        EvidenceLevel          `json:"evidence"`
}

// StatusRuntimeSummary keeps declared runtime (StatusContent.Runtime) separate
// from the active, observed pin configuration.
type StatusRuntimeSummary struct {
	State        StatusSummaryState  `json:"state"`
	Reason       StatusSummaryReason `json:"reason,omitempty"`
	ActiveName   string              `json:"activeName,omitempty"`
	ActiveKind   RuntimeKind         `json:"activeKind,omitempty"`
	ActiveOrigin ConfigurationOrigin `json:"activeOrigin,omitempty"`
	ActiveState  ConfigurationState  `json:"activeState"`
	PinState     RuntimePinState     `json:"pinState,omitempty"`
	Freshness    StatusFreshness     `json:"statusFreshness,omitempty"`
	Evidence     EvidenceLevel       `json:"evidence"`
}

// StatusAcceleratorComponent contains declared intent and separately reported
// selection/class verification; request payloads and reason text stay out.
type StatusAcceleratorComponent struct {
	Type          RuntimeComponentType      `json:"type"`
	Intent        AcceleratorIntentState    `json:"intent"`
	DeclaredClass string                    `json:"declaredClass,omitempty"`
	Selection     AcceleratorSelectionState `json:"selection"`
	SelectedClass string                    `json:"selectedClass,omitempty"`
	Class         AcceleratorClassState     `json:"class"`
}

type StatusAccelerator struct {
	State      StatusSummaryState           `json:"state"`
	Reason     StatusSummaryReason          `json:"reason,omitempty"`
	Freshness  StatusFreshness              `json:"statusFreshness,omitempty"`
	Evidence   EvidenceLevel                `json:"evidence"`
	Components []StatusAcceleratorComponent `json:"components"`
}

func statusSummaryReason(value StatusSummaryReason) StatusSummaryReason {
	return clusterEnum(value, StatusSummaryReason(""), "", StatusReasonAutoSelectionNotProbed,
		StatusReasonVirtualDeployment, StatusReasonReadFailed, StatusReasonProjectionInvalid)
}

func statusTrafficCanonical(value StatusTraffic) StatusTraffic {
	value.State = clusterEnum(value.State, TrafficStateUnavailable, TrafficStateReported,
		TrafficStatePending, TrafficStateDegraded, TrafficStatePartial,
		TrafficStateUnavailable, TrafficStateInvalid)
	value.Translator = clusterEnum(value.Translator, TrafficTranslatorUnavailable,
		TrafficTranslatorEnvoyGateway, TrafficTranslatorIstio, TrafficTranslatorUnavailable)
	value.Algorithm = clusterEnum(value.Algorithm, TrafficAlgorithmUnknown,
		TrafficAlgorithmDefault, TrafficAlgorithmRoundRobin, TrafficAlgorithmLeastRequest,
		TrafficAlgorithmRandom, TrafficAlgorithmConsistentHash, TrafficAlgorithmUnknown)
	value.PolicyReady = clusterEnum(value.PolicyReady, TrafficConditionUnknown,
		TrafficConditionTrue, TrafficConditionFalse, TrafficConditionUnknown)
	value.PolicyFreshness = clusterEnum(value.PolicyFreshness, TrafficFreshnessUnavailable,
		TrafficFreshnessCurrent, TrafficFreshnessStale, TrafficFreshnessUnverifiable,
		TrafficFreshnessUnavailable)
	value.Evidence = clusterEnum(value.Evidence, EvidenceUnavailable, EvidenceReported, EvidenceUnavailable)
	if value.State == TrafficStateUnavailable {
		value.Evidence = EvidenceUnavailable
		value.Translator = ""
		value.Algorithm = ""
		value.PolicyReady = ""
		value.PolicyFreshness = ""
	} else if value.Evidence == EvidenceUnavailable && value.State != TrafficStateInvalid {
		value.State = TrafficStateInvalid
		value.Translator = TrafficTranslatorUnavailable
		value.Algorithm = TrafficAlgorithmUnknown
		value.PolicyReady = TrafficConditionUnknown
		value.PolicyFreshness = TrafficFreshnessUnavailable
	}
	return value
}

func statusRuntimeCanonical(value StatusRuntimeSummary) StatusRuntimeSummary {
	value.State = clusterEnum(value.State, StatusSummaryUnavailable, StatusSummaryReported,
		StatusSummaryPartial, StatusSummaryNotConfigured, StatusSummaryInvalid, StatusSummaryUnavailable)
	value.Reason = statusSummaryReason(value.Reason)
	value.ActiveName = statusName(value.ActiveName, false)
	value.ActiveKind = clusterEnum(value.ActiveKind, RuntimeKind(""), "",
		RuntimeKindServingRuntime, RuntimeKindClusterServingRuntime)
	value.ActiveOrigin = clusterEnum(value.ActiveOrigin, ConfigurationOrigin(""), "",
		ConfigurationOriginLiveRuntime, ConfigurationOriginControllerRevision)
	value.ActiveState = clusterEnum(value.ActiveState, ConfigurationStateUnavailable,
		ConfigurationStateAvailable, ConfigurationStateUnavailable)
	value.PinState = clusterEnum(value.PinState, RuntimePinState(""), "",
		RuntimePinStateNotApplicable, RuntimePinStateAwaitingPin, RuntimePinStateResolved,
		RuntimePinStateDesiredReportedMismatch, RuntimePinStateRevisionMissing,
		RuntimePinStateRevisionInvalid, RuntimePinStateRevisionDisabled,
		RuntimePinStateUnavailable, RuntimePinStateInvalidIntent)
	value.Freshness = clusterEnum(value.Freshness, StatusFreshness(""), "",
		StatusFreshnessCurrent, StatusFreshnessStale, StatusFreshnessUnobserved,
		StatusFreshnessInvalid)
	value.Evidence = clusterEnum(value.Evidence, EvidenceUnavailable, EvidenceComputed, EvidenceUnavailable)
	if value.Evidence == EvidenceUnavailable && (value.State == StatusSummaryReported || value.State == StatusSummaryPartial) {
		value.State = StatusSummaryInvalid
		value.ActiveName, value.ActiveKind, value.ActiveOrigin, value.PinState, value.Freshness = "", "", "", "", ""
		value.ActiveState = ConfigurationStateUnavailable
	}
	if value.State == StatusSummaryUnavailable || value.State == StatusSummaryNotConfigured {
		value.ActiveName, value.ActiveKind, value.ActiveOrigin, value.PinState, value.Freshness = "", "", "", "", ""
		value.ActiveState, value.Evidence = ConfigurationStateUnavailable, EvidenceUnavailable
	} else if value.ActiveState == ConfigurationStateAvailable && (value.ActiveName == "" || value.ActiveKind == "" || value.ActiveOrigin == "") {
		value.State, value.ActiveState, value.Evidence = StatusSummaryInvalid, ConfigurationStateUnavailable, EvidenceUnavailable
		value.ActiveName, value.ActiveKind, value.ActiveOrigin = "", "", ""
	}
	return value
}

func statusAcceleratorCanonical(value StatusAccelerator) StatusAccelerator {
	value.State = clusterEnum(value.State, StatusSummaryUnavailable, StatusSummaryReported,
		StatusSummaryPartial, StatusSummaryNotConfigured, StatusSummaryInvalid, StatusSummaryUnavailable)
	value.Reason = statusSummaryReason(value.Reason)
	value.Freshness = clusterEnum(value.Freshness, StatusFreshness(""), "",
		StatusFreshnessCurrent, StatusFreshnessStale, StatusFreshnessUnobserved,
		StatusFreshnessInvalid)
	value.Evidence = clusterEnum(value.Evidence, EvidenceUnavailable, EvidenceComputed, EvidenceUnavailable)
	if value.Evidence == EvidenceUnavailable && (value.State == StatusSummaryReported || value.State == StatusSummaryPartial) {
		value.State = StatusSummaryInvalid
		value.Freshness = ""
		value.Components = []StatusAcceleratorComponent{}
	}
	result := []StatusAcceleratorComponent{}
	if len(value.Components) > 2 {
		value.State, value.Evidence = StatusSummaryInvalid, EvidenceUnavailable
	} else {
		for _, row := range value.Components {
			if row.Type != RuntimeComponentEngine && row.Type != RuntimeComponentDecoder {
				value.State, value.Evidence = StatusSummaryInvalid, EvidenceUnavailable
				continue
			}
			row.Intent = clusterEnum(row.Intent, AcceleratorIntentInvalid,
				AcceleratorIntentClass, AcceleratorIntentPolicy,
				AcceleratorIntentNotConfigured, AcceleratorIntentInvalid)
			row.Selection = clusterEnum(row.Selection, AcceleratorSelectionInvalid,
				AcceleratorSelectionReported, AcceleratorSelectionNotReported,
				AcceleratorSelectionUnavailable, AcceleratorSelectionInvalid,
				AcceleratorSelectionNotConfigured)
			row.Class = clusterEnum(row.Class, AcceleratorClassInvalid,
				AcceleratorClassObserved, AcceleratorClassNotRequested,
				AcceleratorClassNotFound, AcceleratorClassForbidden,
				AcceleratorClassUnsupportedAPI, AcceleratorClassUnreadable,
				AcceleratorClassInvalid)
			row.DeclaredClass = statusName(row.DeclaredClass, false)
			row.SelectedClass = statusName(row.SelectedClass, false)
			if row.Intent != AcceleratorIntentClass {
				row.DeclaredClass = ""
			}
			if row.Selection != AcceleratorSelectionReported {
				row.SelectedClass = ""
			}
			if row.Selection == AcceleratorSelectionReported && row.SelectedClass == "" {
				row.Selection, row.Class = AcceleratorSelectionInvalid, AcceleratorClassInvalid
				value.State, value.Evidence = StatusSummaryInvalid, EvidenceUnavailable
			}
			result = append(result, row)
		}
	}
	slices.SortFunc(result, func(a, b StatusAcceleratorComponent) int { return componentRank(a.Type) - componentRank(b.Type) })
	if len(result) == 2 && result[0].Type == result[1].Type {
		result = []StatusAcceleratorComponent{}
		value.State, value.Evidence = StatusSummaryInvalid, EvidenceUnavailable
	}
	if value.State == StatusSummaryUnavailable || value.State == StatusSummaryNotConfigured {
		value.Freshness, value.Evidence = "", EvidenceUnavailable
		result = []StatusAcceleratorComponent{}
		if value.State == StatusSummaryNotConfigured {
			value.Reason = ""
		}
	}
	value.Components = result
	return value
}
