// Package translators holds Translator implementations that turn
// resolved traffic-management intent into backend policy resources for
// specific Gateway implementations.
//
// Each translator turns a resolved traffic-management intent into the
// backend policy resource its target Gateway implementation expects.
// Exactly one translator is selected per controller process by the
// factory at startup based on installed CRDs.
//
// Translators in this package:
//
//   - noop: returned when no supported Gateway-implementation policy CRD
//     is installed. Emits nothing and surfaces NoTranslatorAvailable so
//     the reconciler can record the condition without blocking the ISVC
//     lifecycle.
//   - envoygateway: emits gateway.envoyproxy.io/v1alpha1 BackendTrafficPolicy.
package translators

import (
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/status"
)

// NoopName is the stable identifier the factory uses for log / metric
// labels. The status writer matches on it to surface the
// NoTranslatorAvailable condition, so it reads the writer's constant.
const NoopName = status.NoopTranslatorName

// Noop is the translator the factory falls back to when no supported
// Gateway-implementation backend policy CRD is installed.
//
// Translate returns (nil, nil, nil) on every call — the reconciler
// interprets that as "no resource to emit" and skips the apply. The
// reconciler is also expected to surface a NoTranslatorAvailable
// condition on the ISVC so operators can see why their traffic-
// management intent did not take effect; surfacing that condition is
// the reconciler's job, not the translator's, so this implementation
// stays a pure no-op.
type Noop struct{}

// NewNoop returns a ready-to-use Noop translator. There is no
// per-cluster configuration to bind, so this exists only to match the
// constructor shape of real translators.
func NewNoop() *Noop {
	return &Noop{}
}

// Name returns the stable identifier "noop".
func (n *Noop) Name() string {
	return NoopName
}

// SupportedAnnotations returns the empty set. The Noop translator
// honors no annotations. The reconciler does NOT surface
// UnsupportedField for noop — the NoTranslatorAvailable condition on
// BackendPolicyReady already explains the situation; listing every
// dropped key would be noise.
func (n *Noop) SupportedAnnotations() sets.Set[string] {
	return sets.New[string]()
}

// SupportedPassthroughPrefixes returns nil. The Noop translator emits
// nothing so nothing can be stitched through.
func (n *Noop) SupportedPassthroughPrefixes() []string {
	return nil
}

// SupportedTrafficFields returns the empty set: the Noop translator
// emits no typed spec.traffic field. The reconciler does not surface
// UnsupportedField for noop — the NoTranslatorAvailable condition
// already explains that no traffic intent takes effect.
func (n *Noop) SupportedTrafficFields() sets.Set[string] {
	return sets.New[string]()
}

// Translate is a no-op. Returning (nil, nil, nil) signals "no resource
// needed for this intent" per the Translator contract.
func (n *Noop) Translate(
	_ *v1beta1.InferenceService,
	_ []string,
	_ *traffic.ResolvedIntent,
) (client.Object, []string, error) {
	return nil, nil, nil
}

// Watches returns nil because the Noop translator does not emit any
// resource the reconciler would need to watch.
func (n *Noop) Watches() client.Object {
	return nil
}

// ObserveAcceptance always returns AcceptancePending because the Noop
// translator emits no resource and the reconciler never calls this
// method when no policy was emitted. Defined to satisfy the
// Translator interface.
func (n *Noop) ObserveAcceptance(_ client.Object) traffic.AcceptanceObservation {
	return traffic.AcceptanceObservation{State: traffic.AcceptancePending}
}

// Compile-time check that Noop satisfies the Translator interface.
var _ traffic.Translator = (*Noop)(nil)
