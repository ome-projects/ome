// Package factory selects the active traffic translator at controller
// startup.
//
// Exactly one Translator is active per controller process. The
// selection rules (highest priority wins):
//
//  1. Envoy Gateway — gateway.envoyproxy.io/v1alpha1.BackendTrafficPolicy
//     CRD is installed in the cluster.
//  2. Istio — networking.istio.io/v1.DestinationRule CRD is installed.
//  3. Noop — fallback when no supported policy CRD is available.
//
// Order is meaningful: Envoy Gateway BackendTrafficPolicy is the most
// expressive supported back end, so we prefer it whenever it's present.
// Operators on clusters that ship both Envoy Gateway and Istio (rare,
// but legal) get Envoy Gateway behavior. The factory does not honor
// runtime overrides — translator selection is a process-level decision,
// not a per-ISVC one, to keep the produced policy resource type stable
// across reconciles.
//
// Both the Envoy Gateway translator (BackendTrafficPolicy) and the
// Istio translator (DestinationRule) build their resources as
// unstructured.Unstructured to avoid vendoring the gateway.envoyproxy.io
// and Istio Go modules.
package factory

import (
	"fmt"

	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators/envoygateway"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators/istio"
	"sigs.k8s.io/ome/pkg/utils"
)

// crdProbe abstracts utils.IsCrdAvailable for test injection. The
// signature mirrors IsCrdAvailable exactly so the production wiring is
// a one-liner.
type crdProbe func(cfg *rest.Config, groupVersion, kind string) (bool, error)

// New returns the Translator selected for the cluster reachable via
// cfg. It probes the well-known backend-policy CRDs in priority order
// and returns the highest-priority match. Falls back to the Noop
// translator when no probe succeeds.
//
// An error is returned only when a CRD probe itself fails (e.g.
// network error talking to the API server). A clean "CRD not present"
// answer is not an error; it advances to the next probe.
func New(cfg *rest.Config) (traffic.Translator, error) {
	return newWithProbe(cfg, utils.IsCrdAvailable)
}

func newWithProbe(cfg *rest.Config, probe crdProbe) (traffic.Translator, error) {
	// 1. Envoy Gateway BackendTrafficPolicy (preferred).
	found, err := probe(cfg, envoygateway.PolicyGVK.GroupVersion().String(), envoygateway.PolicyGVK.Kind)
	if err != nil {
		return nil, fmt.Errorf("probe %s: %w", envoygateway.PolicyGVK.String(), err)
	}
	if found {
		return envoygateway.New(), nil
	}

	// 2. Istio DestinationRule.
	found, err = probe(cfg, istio.PolicyGVK.GroupVersion().String(), istio.PolicyGVK.Kind)
	if err != nil {
		return nil, fmt.Errorf("probe %s: %w", istio.PolicyGVK.String(), err)
	}
	if found {
		return istio.New(), nil
	}

	// 3. Noop fallback.
	return translators.NewNoop(), nil
}
