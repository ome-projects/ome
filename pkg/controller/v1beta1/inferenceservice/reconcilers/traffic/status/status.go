// Package status builds the TrafficStatus the InferenceService
// controller writes back to the API server after invoking the traffic
// translator.
//
// Build is a pure function so the writer can be unit-tested without a
// live API server or controller-runtime client. The reconciler is
// responsible for the actual Update / Patch call; this package just
// produces the value to assign.
//
// This package intentionally consumes only primitive inputs (no
// dependency on the parent traffic package) so the reconciler can
// compose intent resolution -> translator invocation -> status build
// without an import cycle.
package status

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// NoopTranslatorName is the name of the translator that emits nothing.
// Build short-circuits on it to surface NoTranslatorAvailable; the noop
// translator reads its Name from this constant.
const NoopTranslatorName = "noop"

// GatewayAcceptance is the post-apply observation the reconciler
// extracts from the gateway controller's status writeback. This package
// owns the type; the traffic package names it AcceptanceState, so the
// translators' answer reaches Build unconverted.
type GatewayAcceptance int

const (
	// GatewayAcceptancePending means no acceptance signal — first
	// reconcile after create, or the gateway controller has not run yet.
	// The condition surfaces as Unknown/Pending.
	GatewayAcceptancePending GatewayAcceptance = iota
	// GatewayAcceptanceAccepted means the gateway controller has
	// accepted the policy. Condition becomes True/AcceptedByGateway.
	GatewayAcceptanceAccepted
	// GatewayAcceptanceRejected means the gateway controller has
	// rejected the policy. Condition becomes False/GatewayRejected;
	// the reason/message from the observation are passed through.
	GatewayAcceptanceRejected
)

// algorithmDefault is the value Build stamps into
// TrafficStatus.Algorithm when the operator did not pick an
// algorithm. Surfaced verbatim so operators reading the status can
// distinguish "OME defaulted" from any concrete algorithm name.
const algorithmDefault = "Default"

// BuildArgs is the input to Build. The reconciler fills this in from
// the translator's outputs and the InferenceService it is reconciling.
type BuildArgs struct {
	// TranslatorName is the active translator's Name(). The noop
	// translator name short-circuits to NoTranslatorAvailable;
	// every other name proceeds to the success / error branch.
	TranslatorName string

	// HasIntent reports whether the operator declared any
	// traffic-management intent on this InferenceService. When false
	// Build returns nil so older clients keep seeing no traffic
	// status.
	HasIntent bool

	// Algorithm is the operator-declared load-balancing algorithm
	// (e.g. "RoundRobin"), or "" when unset. Build stamps "Default"
	// into TrafficStatus.Algorithm in the empty case so operators
	// can distinguish "OME defaulted" from any concrete algorithm.
	// Caller resolves this from the intent's typed core so Build does
	// not need to import the intent types.
	Algorithm string

	// EmittedPolicy is the resource the translator produced for this
	// reconcile, or nil when the translator returned no resource
	// (noop, or a real translator that deferred).
	EmittedPolicy client.Object

	// TargetedRoutes lists the OME-managed HTTPRoute names the
	// emitted policy targets. Empty when no policy was emitted.
	TargetedRoutes []string

	// Passthroughs lists ome.io/btp.* or ome.io/dr.* paths the
	// translator stitched into the emitted resource. Surfaced in the
	// condition message so operators can audit what was used without
	// reading the policy resource directly. Empty when none.
	Passthroughs []string

	// TranslateErr is the error from Translator.Translate. When
	// non-nil it supersedes every other branch and produces a
	// BackendPolicyReady=False condition with reason
	// TranslationFailed (or ConflictingPolicy if ConflictDetected).
	TranslateErr error

	// ConflictDetected is set when applyPolicy refused to overwrite
	// a pre-existing backend policy that isn't owned by this ISVC
	// (hand-authored, or owned by a different controller). When true,
	// the BackendPolicyReady condition is False with reason
	// ConflictingPolicy and message ConflictMessage. Supersedes
	// TranslateErr's TranslationFailed branch.
	ConflictDetected bool
	ConflictMessage  string

	// GatewayAcceptance is the post-apply observation extracted from
	// the gateway controller's status writeback on the emitted policy
	// resource. Defaults to Pending (zero value), keeping the
	// condition at Unknown/Pending.
	GatewayAcceptance GatewayAcceptance

	// GatewayAcceptanceReason / GatewayAcceptanceMessage are passed
	// through from the gateway controller's condition so operators
	// can see why the policy was rejected (or what triggered the
	// acceptance). Both empty when GatewayAcceptance=Pending.
	GatewayAcceptanceReason  string
	GatewayAcceptanceMessage string

	// UnsupportedAnnotations lists ome.io/* traffic annotations the
	// operator declared that the active translator does not honor
	// (wrong pass-through prefix, or a per-key annotation outside
	// SupportedAnnotations). Sorted for deterministic messages.
	//
	// When this or UnsupportedFields is non-empty, Build adds a second
	// condition of type BackendPolicyUnsupportedFields=True with both
	// lists in the message. The condition is omitted when both slices
	// are empty (positive-polarity: absence = nothing dropped) and on
	// noop / TranslationFailed branches where the primary condition
	// already explains the situation.
	UnsupportedAnnotations []string

	// UnsupportedFields lists typed spec.traffic capability tokens the
	// operator declared that the active translator does not implement
	// (e.g. endpointOverride on a DestinationRule backend). Sorted for
	// deterministic messages. Feeds the same
	// BackendPolicyUnsupportedFields condition as
	// UnsupportedAnnotations.
	UnsupportedFields []string

	// ObservedGeneration is the InferenceService.metadata.generation
	// observed when this reconcile started. Stamped into the
	// condition so operators can correlate status with spec.
	ObservedGeneration int64

	// Now is the timestamp used for the condition's
	// LastTransitionTime. Injected explicitly so Build stays
	// deterministic for tests.
	Now metav1.Time
}

// Build produces the TrafficStatus for the InferenceService, or nil
// when there is no traffic intent to surface. The returned struct is
// safe for direct assignment to InferenceServiceStatus.Traffic.
//
// Build is deterministic: same args -> identical output. The
// reconciler relies on that property to avoid spurious status
// patches when nothing has changed.
func Build(args BuildArgs) *v1beta1.TrafficStatus {
	if !args.HasIntent {
		return nil
	}

	out := &v1beta1.TrafficStatus{
		Algorithm: algorithmFor(args.Algorithm),
	}

	if args.EmittedPolicy != nil {
		gvk := args.EmittedPolicy.GetObjectKind().GroupVersionKind()
		out.BackendPolicyResource = &v1beta1.BackendPolicyRef{
			APIVersion: gvk.GroupVersion().String(),
			Kind:       gvk.Kind,
			Name:       args.EmittedPolicy.GetName(),
		}
		// Defensive copy so a caller mutating TargetedRoutes after
		// the call doesn't smuggle changes into the written status.
		if len(args.TargetedRoutes) > 0 {
			out.TargetedHTTPRoutes = append(make([]string, 0, len(args.TargetedRoutes)), args.TargetedRoutes...)
		}
	}

	out.Conditions = []metav1.Condition{buildReadyCondition(args)}
	if cond, ok := buildUnsupportedFieldsCondition(args); ok {
		out.Conditions = append(out.Conditions, cond)
	}
	return out
}

// buildUnsupportedFieldsCondition returns the
// BackendPolicyUnsupportedFields condition and true when the active
// translator dropped operator-declared typed spec.traffic fields or
// annotations. Returns ok=false when there is nothing to report, or
// when another condition already explains the situation (noop /
// TranslationFailed) and listing the dropped keys would be redundant
// noise.
func buildUnsupportedFieldsCondition(args BuildArgs) (metav1.Condition, bool) {
	if len(args.UnsupportedAnnotations) == 0 && len(args.UnsupportedFields) == 0 {
		return metav1.Condition{}, false
	}
	if args.TranslateErr != nil {
		return metav1.Condition{}, false
	}
	if args.TranslatorName == NoopTranslatorName {
		return metav1.Condition{}, false
	}
	var parts []string
	if len(args.UnsupportedFields) > 0 {
		parts = append(parts, fmt.Sprintf(
			"%d typed spec.traffic field(s) %v",
			len(args.UnsupportedFields), args.UnsupportedFields,
		))
	}
	if len(args.UnsupportedAnnotations) > 0 {
		parts = append(parts, fmt.Sprintf(
			"%d operator-declared annotation(s) %v",
			len(args.UnsupportedAnnotations), args.UnsupportedAnnotations,
		))
	}
	return metav1.Condition{
		Type:               v1beta1.TrafficConditionBackendPolicyUnsupportedFields,
		Status:             metav1.ConditionTrue,
		Reason:             v1beta1.TrafficReasonUnsupportedField,
		LastTransitionTime: args.Now,
		ObservedGeneration: args.ObservedGeneration,
		Message: fmt.Sprintf(
			"translator %q does not honor %s",
			args.TranslatorName, strings.Join(parts, "; "),
		),
	}, true
}

func buildReadyCondition(args BuildArgs) metav1.Condition {
	cond := metav1.Condition{
		Type:               v1beta1.TrafficConditionBackendPolicyReady,
		LastTransitionTime: args.Now,
		ObservedGeneration: args.ObservedGeneration,
	}

	switch {
	case args.ConflictDetected:
		cond.Status = metav1.ConditionFalse
		cond.Reason = v1beta1.TrafficReasonConflictingPolicy
		cond.Message = args.ConflictMessage

	case args.TranslateErr != nil:
		cond.Status = metav1.ConditionFalse
		cond.Reason = v1beta1.TrafficReasonTranslationFailed
		cond.Message = args.TranslateErr.Error()

	case args.TranslatorName == NoopTranslatorName:
		cond.Status = metav1.ConditionFalse
		cond.Reason = v1beta1.TrafficReasonNoTranslatorAvailable
		cond.Message = "no Gateway-implementation backend policy CRD installed; traffic intent ignored"

	case args.EmittedPolicy == nil:
		// Translator ran without error but produced no resource. Pending
		// is the honest answer; a translator that wants to refuse on
		// principle returns an error instead.
		cond.Status = metav1.ConditionUnknown
		cond.Reason = v1beta1.TrafficReasonPending
		cond.Message = "translator did not emit a backend policy resource"

	default:
		// Policy was applied. The gateway controller's acceptance
		// signal (if any) drives the final condition.
		switch args.GatewayAcceptance {
		case GatewayAcceptanceAccepted:
			cond.Status = metav1.ConditionTrue
			cond.Reason = v1beta1.TrafficReasonAcceptedByGateway
			cond.Message = acceptedMessage(args)
		case GatewayAcceptanceRejected:
			cond.Status = metav1.ConditionFalse
			cond.Reason = v1beta1.TrafficReasonGatewayRejected
			cond.Message = rejectedMessage(args)
		default: // GatewayAcceptancePending
			cond.Status = metav1.ConditionUnknown
			cond.Reason = v1beta1.TrafficReasonPending
			cond.Message = passthroughMessage(args.Passthroughs)
		}
	}

	return cond
}

func acceptedMessage(args BuildArgs) string {
	if args.GatewayAcceptanceMessage != "" {
		return args.GatewayAcceptanceMessage
	}
	if len(args.Passthroughs) == 0 {
		return "backend policy accepted by gateway controller"
	}
	return fmt.Sprintf(
		"backend policy accepted by gateway controller with %d pass-through field(s) (%v)",
		len(args.Passthroughs), args.Passthroughs,
	)
}

func rejectedMessage(args BuildArgs) string {
	if args.GatewayAcceptanceMessage != "" {
		return args.GatewayAcceptanceMessage
	}
	if args.GatewayAcceptanceReason != "" {
		return fmt.Sprintf("backend policy rejected by gateway controller: %s", args.GatewayAcceptanceReason)
	}
	return "backend policy rejected by gateway controller"
}

func passthroughMessage(paths []string) string {
	if len(paths) == 0 {
		return "backend policy emitted; awaiting gateway acceptance"
	}
	return fmt.Sprintf(
		"backend policy emitted with %d pass-through field(s) (%v); awaiting gateway acceptance",
		len(paths), paths,
	)
}

func algorithmFor(s string) string {
	if s == "" {
		return algorithmDefault
	}
	return s
}
