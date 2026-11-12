package factory

import (
	"errors"
	"testing"

	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators/envoygateway"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/translators/istio"
)

// fakeProbe returns the next value from results on each call, recording
// the (groupVersion, kind) it was queried with. Useful for asserting
// probe ordering as well as outcome.
type fakeProbe struct {
	calls   []probeCall
	results []probeResult
}

type probeCall struct {
	groupVersion string
	kind         string
}

type probeResult struct {
	found bool
	err   error
}

func (f *fakeProbe) probe(_ *rest.Config, groupVersion, kind string) (bool, error) {
	f.calls = append(f.calls, probeCall{groupVersion: groupVersion, kind: kind})
	if len(f.results) == 0 {
		return false, nil
	}
	r := f.results[0]
	f.results = f.results[1:]
	return r.found, r.err
}

func TestNew_NoBackendPolicyCRD_ReturnsNoop(t *testing.T) {
	// No backend-policy CRDs installed — both probes return (false, nil).
	f := &fakeProbe{results: []probeResult{
		{found: false},
		{found: false},
	}}
	got, err := newWithProbe(&rest.Config{}, "cluster.local", f.probe)
	if err != nil {
		t.Fatalf("newWithProbe err = %v, want nil", err)
	}
	if got.Name() != translators.NoopName {
		t.Fatalf("Name() = %q, want %q", got.Name(), translators.NoopName)
	}
	// Both probes must have been attempted in priority order.
	if len(f.calls) != 2 {
		t.Fatalf("expected 2 probes, got %d: %#v", len(f.calls), f.calls)
	}
	if f.calls[0].kind != "BackendTrafficPolicy" {
		t.Fatalf("expected first probe BackendTrafficPolicy, got %q", f.calls[0].kind)
	}
	if f.calls[1].kind != "DestinationRule" {
		t.Fatalf("expected second probe DestinationRule, got %q", f.calls[1].kind)
	}
}

func TestNew_EnvoyGatewayDetected_SelectsEnvoyTranslator(t *testing.T) {
	// First probe hits — the factory must stop and not also probe Istio,
	// and it must return the Envoy Gateway translator (not Noop).
	f := &fakeProbe{results: []probeResult{
		{found: true},
	}}
	got, err := newWithProbe(&rest.Config{}, "cluster.local", f.probe)
	if err != nil {
		t.Fatalf("newWithProbe err = %v, want nil", err)
	}
	if got.Name() != envoygateway.Name {
		t.Fatalf("Name() = %q, want %q", got.Name(), envoygateway.Name)
	}
	if len(f.calls) != 1 {
		t.Fatalf("expected exactly 1 probe (short-circuit), got %d: %#v", len(f.calls), f.calls)
	}
}

func TestNew_IstioDetected_SelectsIstioTranslator(t *testing.T) {
	// Envoy Gateway absent, Istio present.
	f := &fakeProbe{results: []probeResult{
		{found: false},
		{found: true},
	}}
	got, err := newWithProbe(&rest.Config{}, "cluster.local", f.probe)
	if err != nil {
		t.Fatalf("newWithProbe err = %v, want nil", err)
	}
	if got.Name() != istio.Name {
		t.Fatalf("Name() = %q, want %q", got.Name(), istio.Name)
	}
	if len(f.calls) != 2 {
		t.Fatalf("expected 2 probes, got %d: %#v", len(f.calls), f.calls)
	}
}

func TestNew_ProbeError_PropagatedWithGVKContext(t *testing.T) {
	sentinel := errors.New("simulated discovery failure")
	f := &fakeProbe{results: []probeResult{
		{err: sentinel},
	}}
	_, err := newWithProbe(&rest.Config{}, "cluster.local", f.probe)
	if err == nil {
		t.Fatalf("newWithProbe err = nil, want error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("newWithProbe err = %v, want wraps %v", err, sentinel)
	}
}

func TestNew_PrefersEnvoyOverIstioWhenBothPresent(t *testing.T) {
	// Both CRDs installed (rare but legal). Envoy Gateway must win
	// because of priority order, so Istio must NOT be probed.
	f := &fakeProbe{results: []probeResult{
		{found: true},
		// Second result is intentionally not consumed — if the factory
		// keeps probing, the test will fail on call count below.
		{found: true},
	}}
	got, err := newWithProbe(&rest.Config{}, "cluster.local", f.probe)
	if err != nil {
		t.Fatalf("newWithProbe err = %v, want nil", err)
	}
	if got.Name() != envoygateway.Name {
		t.Fatalf("Name() = %q, want %q (Envoy must win over Istio)", got.Name(), envoygateway.Name)
	}
	if len(f.calls) != 1 {
		t.Fatalf("expected exactly 1 probe (Envoy short-circuits Istio), got %d", len(f.calls))
	}
}
