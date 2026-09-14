package endpoint

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	// GlobalHostAnnotation pins an explicit global host for one InferenceService,
	// overriding Config.GlobalHostTemplate. When neither this nor a usable
	// template yields a host, the service is not published.
	GlobalHostAnnotation = "ome.io/global-host"
)

// Config holds the operator-supplied, config-driven inputs the endpoint
// publisher needs. Every behavioral value lives here (manager flags / chart /
// GitOps), never as a literal in code: an absent value degrades gracefully
// (publishing is skipped) rather than silently injecting a magic host, gateway,
// or port.
//
// +kubebuilder:object:generate=false
type Config struct {
	// GlobalHostTemplate renders the global host for an InferenceService that does
	// not set GlobalHostAnnotation. A text/template evaluated against
	// HostTemplateData (e.g. "{{.Name}}.{{.Namespace}}.global.example"). Empty
	// disables template-derived hosts: such services publish only if they carry an
	// explicit GlobalHostAnnotation.
	GlobalHostTemplate string

	// GlobalGateway is the Gateway the published HTTPRoute attaches to, in
	// "namespace/name" form (the global-traffic gateway on the control-plane
	// cluster). Empty disables the Gateway API backend entirely (Reconcile
	// no-ops, IsEnabled()==false) — the publisher never invents a gateway.
	GlobalGateway string

	// RouteNamespace is the namespace the published HTTPRoute and its backing
	// resources are created in. Empty falls back to the
	// InferenceService's own namespace (so the route is co-located with — and
	// garbage-collected alongside — its source object by default).
	RouteNamespace string

	// BackendPort is the port on the winning cluster's ingress the global host
	// routes to. Required: a non-positive port would yield a port-0 backend
	// Service and HTTPRoute backendRef, so it disables the backend
	// (IsEnabled()==false) instead — supply it via config.
	BackendPort int32

	// GatewayBackend configures hostname rewriting, backend TLS, and the optional
	// direct-address fallback for each workload cluster Gateway.
	GatewayBackend GatewayBackendConfig

	// Labels are stamped onto every resource the publisher creates, so an
	// operator can select/observe published backends. Optional.
	Labels map[string]string
}

// GatewayBackendConfig configures how the global Gateway connects to each
// workload cluster Gateway while retaining an ExternalName Service as the
// baseline backend representation.
//
// +kubebuilder:object:generate=false
type GatewayBackendConfig struct {
	// RewriteHostname replaces the request Host header with the selected home's
	// ingress hostname before forwarding.
	RewriteHostname bool

	// TLS configures HTTPS origination and certificate validation independently
	// of how the backing Service obtains addresses.
	TLS GatewayBackendTLSConfig

	// EndpointSlices enables the direct-address fallback for environments where
	// the ExternalName resolves to an address the global Gateway cannot reach.
	EndpointSlices GatewayBackendEndpointSliceConfig
}

// GatewayBackendTLSConfig configures a BackendTLSPolicy for every home.
//
// +kubebuilder:object:generate=false
type GatewayBackendTLSConfig struct {
	Enabled bool

	// WellKnownCACertificates selects the trust roots used to authenticate each
	// backend Gateway. It is written verbatim to BackendTLSPolicy.
	WellKnownCACertificates string
}

// GatewayBackendEndpointSliceConfig configures direct Gateway address
// publication for every home.
//
// +kubebuilder:object:generate=false
type GatewayBackendEndpointSliceConfig struct {
	Enabled bool

	// AddressRefreshInterval controls how often published gateway addresses are
	// re-read. Required when Enabled; a zero value schedules no periodic refresh.
	AddressRefreshInterval time.Duration
}

// HostTemplateData is the field set GlobalHostTemplate is evaluated against.
type HostTemplateData struct {
	Name      string
	Namespace string
}

// IsEnabled reports whether the Gateway API backend is configured enough to run.
// Without a global gateway there is nothing to attach a route to, and without a
// backend port the published Service and backendRef would be invalid, so the
// backend stays off rather than guessing either.
func (c Config) IsEnabled() bool {
	return strings.TrimSpace(c.GlobalGateway) != "" && c.BackendPort > 0
}

// GlobalHostFor returns the global host to program for isvc: the explicit
// GlobalHostAnnotation if set, otherwise GlobalHostTemplate rendered against the
// ISVC's name/namespace. Returns "" (no error) when neither yields a host; the
// caller treats that as "not publishable" and skips it.
func (c Config) GlobalHostFor(isvc *v1beta1.InferenceService) (string, error) {
	if h := strings.TrimSpace(isvc.Annotations[GlobalHostAnnotation]); h != "" {
		return h, nil
	}
	tmplText := strings.TrimSpace(c.GlobalHostTemplate)
	if tmplText == "" {
		return "", nil
	}
	tmpl, err := template.New("globalHost").Parse(tmplText)
	if err != nil {
		return "", fmt.Errorf("parse globalHostTemplate %q: %w", tmplText, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, HostTemplateData{Name: isvc.Name, Namespace: isvc.Namespace}); err != nil {
		return "", fmt.Errorf("render globalHostTemplate %q: %w", tmplText, err)
	}
	return strings.TrimSpace(buf.String()), nil
}
