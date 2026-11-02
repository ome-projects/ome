package interfaces

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ComponentNotReadyError is a builder's answer when the Component a route
// would send traffic to cannot back it yet: nothing is built, and the strategy
// that asked reports the condition.
type ComponentNotReadyError struct {
	Component v1beta1.ComponentType
}

func (e *ComponentNotReadyError) Error() string {
	return string(e.Component) + " component not ready"
}

// ClusterLocalEndpointClass is the conventional class tagged on the in-cluster
// (<svc>.<ns>.svc.cluster.local) endpoint that status.address projects. It is a
// status taxonomy label (like a condition-type string), not a config-driven
// behavioral value.
const ClusterLocalEndpointClass = "cluster-local"

// Endpoint is one reachable endpoint for an InferenceService route: the tuple
// (class, host, path, scheme) the status writer publishes, derived from the same
// logic that builds the HTTPRoute hosts + paths so status and routing cannot
// drift. Host + Path together form the reachable URL
// "<Scheme>://<Host><Path>"; in the shared-host scheme Path is the load-bearing
// "/<namespace>/<service>/" prefix, and "/" in the per-ISVC-subdomain scheme.
type Endpoint struct {
	// Class is the operator-declared endpoint class copied verbatim from the
	// gateway config (IngressGatewaySpec.Class / IngressConfig.OmeIngressGatewayClass):
	// "internal", "external", "cluster-local", or any config-defined value. Empty
	// when the gateway declares no class.
	Class string
	// Host is the route hostname for this gateway.
	Host string
	// Path is the route's PathPrefix match for this ISVC/component.
	Path string
	// Scheme is the configured urlScheme ("http"/"https").
	Scheme string
	// Primary is true for the config-designated primary gateway (the one
	// status.url projects). Exactly one endpoint per route is primary.
	Primary bool
}

// HTTPRouteBuilder builds Gateway API HTTPRoute resources
type HTTPRouteBuilder interface {
	// BuildHTTPRoute returns the route for componentType, or a
	// *ComponentNotReadyError when its Component cannot back a route yet.
	BuildHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService, componentType string) (client.Object, error)
	// Endpoints returns every reachable endpoint for the route named by
	// serviceName ("<isvc>" for the top-level route, "<isvc>-engine" etc. for a
	// component), one per gateway the route attaches to. It is derived from the
	// same gateway/host/path logic as BuildHTTPRoute so status never drifts from
	// the actual routing.
	Endpoints(isvc *v1beta1.InferenceService, serviceName string) ([]Endpoint, error)
}

// IngressBuilder builds Kubernetes Ingress resources
type IngressBuilder interface {
	// BuildIngress returns the Ingress, or a *ComponentNotReadyError when the
	// Component its rules send traffic to cannot back them yet.
	BuildIngress(ctx context.Context, isvc *v1beta1.InferenceService) (client.Object, error)
}
