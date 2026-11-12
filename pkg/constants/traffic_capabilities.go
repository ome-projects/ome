// Typed spec.traffic capability tokens.
//
// Each token names one typed spec.traffic behavior a traffic
// translator can emit. Translators declare the subset they implement
// via Translator.SupportedTrafficFields; the reconciler surfaces
// required-but-undeclared tokens as a BackendPolicyUnsupportedFields
// condition, and the admission webhook receives the active
// translator's set to reject intent that must not degrade silently.
//
// Token strings double as the operator-facing field identifier in
// condition messages and admission errors, so they read as field
// paths on the InferenceService spec.
package constants

const (
	// TrafficCapabilityAlgorithm covers spec.traffic.algorithm
	// (load-balancing algorithm selection).
	TrafficCapabilityAlgorithm = "spec.traffic.algorithm"

	// TrafficCapabilityHashHeader covers consistent hashing on a
	// single request header.
	TrafficCapabilityHashHeader = "spec.traffic.consistentHash.type=Header"

	// TrafficCapabilityHashMultipleHeaders covers concatenating two or
	// more headers into one consistent-hash input. Declared separately
	// from single-header hashing because a translator without it would
	// hash on a subset of the declared headers, silently changing
	// session affinity.
	TrafficCapabilityHashMultipleHeaders = "spec.traffic.consistentHash.headers(multiple)"

	// TrafficCapabilityHashCookie covers consistent hashing on a named
	// cookie.
	TrafficCapabilityHashCookie = "spec.traffic.consistentHash.type=Cookie"

	// TrafficCapabilityHashSourceIP covers consistent hashing on the
	// client source IP.
	TrafficCapabilityHashSourceIP = "spec.traffic.consistentHash.type=SourceIP"

	// TrafficCapabilityEndpointOverrideHeader covers routing to the
	// exact endpoint named by a request header.
	TrafficCapabilityEndpointOverrideHeader = "spec.traffic.endpointOverride.type=Header"

	// TrafficCapabilityEndpointOverrideMetadata covers routing to the
	// exact endpoint named by request metadata. Reserved: admission
	// rejects the value until a translator declares this capability.
	TrafficCapabilityEndpointOverrideMetadata = "spec.traffic.endpointOverride.type=Metadata"
)

// Route-provider capability tokens.
//
// Each token names one kind of per-revision traffic intent the route
// provider can express. The route provider is whatever renders an
// InferenceService's external route: Gateway API HTTPRoutes, a
// Kubernetes Ingress, or nothing at all when ingress creation is
// disabled and requests reach the external Service directly (or
// through whatever the operator puts in front of it). Operators
// declare the set per provider in the inferenceservice-config
// ConfigMap (ingress.routeProviderCapabilities); the admission
// webhook and the rollout run layer read the same declaration.
const (
	// RouteCapabilityWeightedRevisionRouting covers splitting a
	// Component's external route across its revisions by percentage —
	// what a canary step's traffic value asks of the provider.
	RouteCapabilityWeightedRevisionRouting = "weightedRevisionRouting"
)

// KnownRouteCapabilities lists every route-provider capability token,
// so a declaration can be checked for typos at config load.
func KnownRouteCapabilities() []string {
	return []string{RouteCapabilityWeightedRevisionRouting}
}
