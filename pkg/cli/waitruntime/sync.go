// Package waitruntime evaluates one reported runtime-sync request on an
// already-read InferenceService. It does not resolve live runtime revisions.
package waitruntime

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	knapis "knative.dev/pkg/apis"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// The action stores this prefix plus its public request ID in the annotation.
const tokenPrefix = "cli-runtime-sync-"

const (
	ReasonObserved             waitengine.Reason = "RuntimeSyncObserved"
	ReasonNotAcknowledged      waitengine.Reason = "RuntimeSyncNotAcknowledged"
	ReasonNotRecorded          waitengine.Reason = "RuntimeSyncNotRecorded"
	ReasonUnsupportedPlacement waitengine.Reason = "UnsupportedPlacement"
)

type TokenState string

const (
	TokenAcknowledged TokenState = "Acknowledged"
	TokenPending      TokenState = "Pending"
	TokenStatusOnly   TokenState = "StatusOnly"
	TokenSuperseded   TokenState = "Superseded"
	TokenAbsent       TokenState = "Absent"
	TokenInvalid      TokenState = "Invalid"
	TokenUnavailable  TokenState = "Unavailable"
)

type DriftState string

const (
	DriftClear           DriftState = "Clear"
	DriftReportedTrue    DriftState = "ReportedTrue"
	DriftReportedFalse   DriftState = "ReportedFalse"
	DriftReportedUnknown DriftState = "ReportedUnknown"
	DriftInvalid         DriftState = "Invalid"
	DriftUnavailable     DriftState = "Unavailable"
)

type PinState string

const (
	PinManaged       PinState = "Managed"
	PinNotApplicable PinState = "NotApplicable"
	PinUnavailable   PinState = "Unavailable"
	PinInvalid       PinState = "Invalid"
)

type PlacementState string

const (
	PlacementDirect      PlacementState = "Direct"
	PlacementUnsupported PlacementState = "UnsupportedPlacement"
)

type Observation struct {
	RequestID           string         `json:"requestID"`
	TokenState          TokenState     `json:"tokenState"`
	DriftState          DriftState     `json:"driftState"`
	PinState            PinState       `json:"pinState"`
	PlacementState      PlacementState `json:"placementState"`
	Validity            string         `json:"validity"`
	InspectedConditions int            `json:"inspectedConditions"`
	GenerationFreshness string         `json:"generationFreshness"`
}

// Evaluate observes request consumption and reported drift clearance on one
// parent snapshot. It does not claim live runtime or child convergence.
func Evaluate(v *ome.InferenceService, requestID string) (waitengine.Decision, Observation, error) {
	if len(requestID) != 36 {
		return waitengine.Decision{}, Observation{}, &waitengine.Error{Reason: waitengine.ReasonInvalidOptions}
	}
	id, err := uuid.Parse(requestID)
	if err != nil || id.String() != requestID || id.Version() != 4 || id.Variant() != uuid.RFC4122 {
		return waitengine.Decision{}, Observation{}, &waitengine.Error{Reason: waitengine.ReasonInvalidOptions}
	}
	if v == nil || v.Kind != "" && v.Kind != "InferenceService" || v.APIVersion != "" && v.APIVersion != "ome.io/v1beta1" ||
		len(utilvalidation.IsDNS1123Subdomain(v.Name)) != 0 || len(utilvalidation.IsDNS1123Label(v.Namespace)) != 0 ||
		!safeIdentity(string(v.UID)) || !safeIdentity(v.ResourceVersion) || v.Generation <= 0 {
		return waitengine.Decision{}, Observation{}, &waitengine.Error{Reason: waitengine.ReasonInvalidIdentity}
	}
	if len(v.Status.Conditions) > 64 || len(v.Finalizers) > 64 {
		return waitengine.Decision{}, Observation{}, &waitengine.Error{Reason: waitengine.ReasonInspectionLimit}
	}
	o := Observation{RequestID: requestID, TokenState: TokenUnavailable, DriftState: DriftUnavailable, PinState: PinNotApplicable, PlacementState: PlacementDirect, Validity: "Unavailable", GenerationFreshness: "Unverifiable"}
	if placementOwned(v) {
		o.PlacementState = PlacementUnsupported
		return waitengine.Decision{Reason: ReasonUnsupportedPlacement}, o, nil
	}
	expected := tokenPrefix + requestID
	o.TokenState, o.DriftState, o.Validity = TokenPending, DriftClear, "Valid"
	if v.DeletionTimestamp != nil {
		// The wait engine normally terminates before invoking the predicate.
		// Keep the pure evaluator safe when called directly as well.
		o.Validity = "Unavailable"
	}
	o.PinState = classifyPin(v)
	switch o.PinState {
	case PinNotApplicable, PinUnavailable:
		o.Validity = "Unavailable"
	case PinInvalid:
		o.Validity = "Invalid"
	}
	annotation := v.Annotations[constants.RuntimeSyncAnnotationKey]
	switch {
	case len(annotation) > 256 || len(v.Status.LastRuntimeSyncToken) > 256:
		o.TokenState, o.Validity = TokenInvalid, "Invalid"
	case annotation == expected && v.Status.LastRuntimeSyncToken == expected:
		o.TokenState = TokenAcknowledged
	case annotation == expected:
		o.TokenState = TokenPending
	case annotation == "" && v.Status.LastRuntimeSyncToken == expected:
		o.TokenState = TokenStatusOnly
	case annotation != "":
		o.TokenState = TokenSuperseded
	default:
		o.TokenState = TokenAbsent
	}
	if o.Validity == "Valid" && (o.TokenState == TokenAbsent || o.TokenState == TokenStatusOnly) {
		o.Validity = "Unavailable"
	}
	foundDrift := false
	for _, condition := range v.Status.Conditions {
		o.InspectedConditions++
		if len(condition.Type) == 0 || len(condition.Type) > 253 || !utf8.ValidString(string(condition.Type)) || strings.ContainsFunc(string(condition.Type), unicode.IsControl) {
			o.DriftState, o.Validity = DriftInvalid, "Invalid"
			continue
		}
		if string(condition.Type) == constants.RuntimeDriftedConditionType {
			if foundDrift || !validDrift(&condition) {
				o.DriftState, o.Validity = DriftInvalid, "Invalid"
			} else if o.DriftState != DriftInvalid {
				switch condition.Status {
				case corev1.ConditionTrue:
					o.DriftState = DriftReportedTrue
				case corev1.ConditionFalse:
					o.DriftState = DriftReportedFalse
				case corev1.ConditionUnknown:
					o.DriftState = DriftReportedUnknown
				}
			}
			foundDrift = true
		}
	}
	if o.Validity == "Valid" && o.TokenState == TokenAcknowledged && o.DriftState == DriftClear && o.PinState == PinManaged {
		return waitengine.Decision{Matched: true, Reason: ReasonObserved}, o, nil
	}
	if o.Validity == "Invalid" {
		return waitengine.Decision{Reason: waitengine.ReasonInvalidCondition}, o, nil
	}
	return waitengine.Decision{Reason: ReasonNotAcknowledged}, o, nil
}

func placementOwned(v *ome.InferenceService) bool {
	if v.Spec.Placement != nil || v.Status.Placement != nil {
		return true
	}
	for _, finalizer := range v.Finalizers {
		if finalizer == "ome.io/placement" {
			return true
		}
	}
	for _, key := range []string{"ome.io/accelerator-requirements", "ome.io/cluster-selector", constants.PlacementOrigin, constants.PlacementOriginUID, constants.PlacementControlPlane} {
		if _, present := v.Annotations[key]; present {
			return true
		}
		if _, present := v.Labels[key]; present {
			return true
		}
	}
	return false
}

func classifyPin(v *ome.InferenceService) PinState {
	ref := v.Spec.Runtime
	if ref == nil || ref.AutoSync == nil || *ref.AutoSync || ref.Revision != nil && *ref.Revision != "" {
		return PinNotApplicable
	}
	if len(utilvalidation.IsDNS1123Subdomain(ref.Name)) != 0 ||
		ref.Kind != nil && *ref.Kind != runtimeselector.KindServingRuntime && *ref.Kind != runtimeselector.KindClusterServingRuntime ||
		ref.APIGroup != nil && *ref.APIGroup != "ome.io" {
		return PinInvalid
	}
	if v.Status.PinnedRevisionName == "" {
		return PinUnavailable
	}
	if len(utilvalidation.IsDNS1123Subdomain(v.Status.PinnedRevisionName)) != 0 {
		return PinInvalid
	}
	return PinManaged
}

func safeIdentity(value string) bool {
	if len(value) == 0 || len(value) > 256 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

func validDrift(c *knapis.Condition) bool {
	if len(c.Reason) > 1024 || len(c.Message) > 4096 {
		return false
	}
	if !utf8.ValidString(c.Reason) || !utf8.ValidString(c.Message) {
		return false
	}
	if c.Status != corev1.ConditionTrue && c.Status != corev1.ConditionFalse && c.Status != corev1.ConditionUnknown {
		return false
	}
	return c.Severity == "" || c.Severity == knapis.ConditionSeverityInfo || c.Severity == knapis.ConditionSeverityWarning || c.Severity == knapis.ConditionSeverityError
}
