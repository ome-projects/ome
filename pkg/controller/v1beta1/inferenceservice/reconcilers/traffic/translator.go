// Translator is the per-Gateway-implementation abstraction that turns a
// resolved traffic-management intent into the right backend policy
// resource for whichever Gateway implementation is installed in the
// cluster (Envoy Gateway BackendTrafficPolicy, Istio DestinationRule,
// kgateway TrafficPolicy, etc.). Translator implementations live under
// pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators/.
//
// The interface lives alongside ResolvedIntent — both are consumed by
// the Reconciler and translator implementations, and the interface
// names ResolvedIntent so splitting them would create an import cycle.
package traffic

import (
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/status"
)

// AcceptanceState reports whether the Gateway implementation has
// accepted the emitted backend policy resource. The reconciler reads
// status.conditions on the post-apply object via Translator.
// ObserveAcceptance and passes the answer to status.Build, which owns
// the type and picks the TrafficStatus condition from it.
type AcceptanceState = status.GatewayAcceptance

const (
	// AcceptancePending means the gateway controller has not (yet)
	// written an acceptance signal. Typical on first reconcile after
	// the policy was created, or for translators whose target
	// implementation does not surface acceptance state.
	AcceptancePending = status.GatewayAcceptancePending
	// AcceptanceAccepted means the gateway controller has accepted
	// the policy.
	AcceptanceAccepted = status.GatewayAcceptanceAccepted
	// AcceptanceRejected means the gateway controller has rejected
	// the policy. The reason / message live on AcceptanceObservation.
	AcceptanceRejected = status.GatewayAcceptanceRejected
)

// AcceptanceObservation is the result of Translator.ObserveAcceptance.
// State drives the TrafficStatus condition; Reason / Message are
// passed through to the operator-visible condition so they can debug
// without inspecting the policy resource directly.
type AcceptanceObservation struct {
	State   AcceptanceState
	Reason  string
	Message string
}

// Translator emits the backend policy resource for a single
// InferenceService. Exactly one translator is active per controller
// process (chosen by the factory at startup); the active translator's
// Translate method is invoked per ISVC during reconciliation when
// the ISVC declares any traffic-management intent.
type Translator interface {
	// Name returns a stable identifier for log / metric labels
	// (e.g. "envoy-gateway", "istio", "noop"). Must be unique
	// across all translators registered with the factory.
	Name() string

	// SupportedAnnotations returns the set of ome.io/* annotation
	// keys the translator can honor. The reconciler uses this set to
	// surface UnsupportedField conditions for annotations declared
	// on the ISVC that this translator does not implement (e.g. an
	// Envoy-only field set on an Istio cluster). Returning an empty
	// set is valid — it means the translator implements no
	// annotation extensions (legitimate for a stub).
	//
	// The result is the support matrix of which policy fields each
	// gateway backend honors, expressed as runtime data.
	SupportedAnnotations() sets.Set[string]

	// SupportedPassthroughPrefixes returns the ome.io/* annotation
	// prefixes whose contents this translator stitches verbatim into
	// the emitted resource. Annotations under any other pass-through
	// prefix are surfaced as UnsupportedField (e.g. ome.io/dr.* on a
	// cluster where the Envoy Gateway translator is active).
	//
	// Returning nil / an empty slice is valid (e.g. noop). Each
	// prefix MUST end with a "." to match the constants in
	// pkg/constants/traffic_annotations.go.
	SupportedPassthroughPrefixes() []string

	// SupportedTrafficFields returns the set of typed spec.traffic
	// capability tokens (pkg/constants TrafficCapability*) this
	// translator emits. The reconciler surfaces required-but-
	// undeclared capabilities as an UnsupportedField condition naming
	// the field and the active translator; the admission webhook
	// receives the same set so intent that must not degrade silently
	// (reserved values, partially-applied hash keys) is rejected up
	// front. Returning an empty set is valid — it means the translator
	// emits no typed traffic field (legitimate for a stub).
	SupportedTrafficFields() sets.Set[string]

	// Translate produces the backend policy resource for the
	// InferenceService given the resolved intent and the list of
	// OME-managed HTTPRoute names the policy must target (top-level,
	// router, decoder, engine; per-revision HTTPRoutes for canaries
	// are not separate routes — the HTTPRoute builder represents
	// canaries as weighted backendRefs on the existing per-Component
	// routes).
	//
	// Returns:
	//   - the emitted resource (typed as client.Object so the
	//     translator package can be implementation-agnostic). The
	//     resource MUST have its OwnerReferences set to the ISVC so
	//     deletion of the ISVC garbage-collects the policy. The
	//     resource's name MUST be deterministic — typically the ISVC
	//     name — so subsequent reconciles update rather than create
	//     duplicates.
	//   - the list of pass-through field paths that were stitched
	//     into the resource (for status condition message + metrics).
	//     Empty when the translator stitched nothing.
	//   - an error when translation fails. Returning (nil, nil, nil)
	//     means "no resource needed for this intent" (e.g. the noop
	//     translator returns this so the reconciler skips emission).
	//
	// Translate must be deterministic: same inputs -> byte-identical
	// output. The reconciler uses this property to avoid noisy
	// API server updates when nothing has changed.
	Translate(
		isvc *v1beta1.InferenceService,
		targetHTTPRoutes []string,
		intent *ResolvedIntent,
	) (client.Object, []string, error)

	// Watches returns a zero-value instance of the resource type the
	// translator emits, used by the reconciler's SetupWithManager
	// to register an Owns() watch. Returns nil when the translator
	// does not emit any resource (e.g. the noop translator).
	Watches() client.Object

	// ObserveAcceptance inspects the post-apply policy resource (as
	// fetched from the API server) and reports whether the Gateway
	// implementation has accepted it. Returning AcceptancePending
	// means "no acceptance signal yet" — the reconciler keeps the
	// BackendPolicyReady condition at Pending.
	//
	// obj is the resource the translator previously emitted, fetched
	// fresh from the cluster so its status subresource reflects any
	// updates the gateway controller has written since.
	ObserveAcceptance(obj client.Object) AcceptanceObservation
}
