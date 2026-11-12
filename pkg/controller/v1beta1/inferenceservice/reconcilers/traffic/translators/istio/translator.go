// Package istio translates traffic intent into an Istio DestinationRule
// (networking.istio.io/v1).
//
// The translator emits an unstructured.Unstructured object so OME does
// not vendor the Istio API. Field paths are those of the Istio
// DestinationRule API; control-plane schema errors surface as
// GatewayRejected via the post-apply observation.
//
// Hosting model: one DR per InferenceService targeting the top-level
// Service hostname (isvc.Name in the ISVC namespace). Per-Component
// Services (engine, decoder, router) are not covered — operators needing
// per-component behavior can hand-author additional DRs.
//
// Pass-through (ome.io/dr.<path>=<value>) is applied last so an operator
// can override any structured field.
package istio

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
)

// Name is the stable identifier the factory uses for log/metric labels
// and that the status writer matches on.
const Name = "istio"

const (
	groupIstio     = "networking.istio.io"
	versionV1      = "v1"
	kindDR         = "DestinationRule"
	istioReadyType = "Reconciled" // Istio control plane uses Reconciled / NotReconciled on .status
)

// PolicyGVK is the DestinationRule kind the translator emits; the
// factory probes for its CRD before selecting this translator.
var PolicyGVK = schema.GroupVersionKind{
	Group:   groupIstio,
	Version: versionV1,
	Kind:    kindDR,
}

// algorithmToIstioSimple maps LoadBalancingType values to Istio's
// loadBalancer.simple enum (Istio uses UPPER_SNAKE_CASE).
// ConsistentHash is handled separately (it sets consistentHash, not
// simple).
var algorithmToIstioSimple = map[v1beta1.LoadBalancingType]string{
	v1beta1.LoadBalancingTypeRoundRobin:   "ROUND_ROBIN",
	v1beta1.LoadBalancingTypeLeastRequest: "LEAST_REQUEST",
	v1beta1.LoadBalancingTypeRandom:       "RANDOM",
}

// Translator emits a DestinationRule per InferenceService.
type Translator struct {
	// clusterDomain is the cluster's DNS domain from ingress.clusterDomain;
	// the DestinationRule host is the Service's in-cluster hostname under it.
	clusterDomain string
}

// New returns a ready-to-use Translator. clusterDomain is the cluster's DNS
// domain from the operator configuration; empty renders the bare Service
// name as the host, which Istio expands itself.
func New(clusterDomain string) *Translator { return &Translator{clusterDomain: clusterDomain} }

// Name returns the stable identifier "istio".
func (t *Translator) Name() string { return Name }

// Watches returns a zero-value unstructured DestinationRule with the
// GVK set so controller-runtime's Owns() can register the watch
// without importing the Istio types.
func (t *Translator) Watches() client.Object {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(PolicyGVK)
	return u
}

// SupportedAnnotations returns the ome.io/* annotation keys the Istio
// DR can honor. This is a STRICT SUBSET of the Envoy Gateway translator's
// matrix because the DestinationRule API doesn't cover retries (those
// live on VirtualService), per-endpoint circuit-breaker limits, or
// max-connection-duration. Unsupported keys surface as UnsupportedField
// on the InferenceService when this translator is active.
func (t *Translator) SupportedAnnotations() sets.Set[string] {
	return sets.New(
		constants.CircuitBreakerMaxConnectionsAnnotation,
		constants.CircuitBreakerMaxParallelRequestsAnnotation,
		constants.CircuitBreakerMaxPendingRequestsAnnotation,
		constants.TimeoutIdleAnnotation,
		constants.TimeoutTCPConnectAnnotation,
	)
}

// SupportedPassthroughPrefixes declares that this translator owns the
// ome.io/dr.* pass-through namespace.
func (t *Translator) SupportedPassthroughPrefixes() []string {
	return []string{constants.PassthroughIstioPrefix}
}

// SupportedTrafficFields returns the typed spec.traffic capability
// tokens the DestinationRule emission covers. Multi-header hashing is
// not declared — Istio's httpHeaderName is a single string — and
// endpoint override has no DestinationRule analogue.
func (t *Translator) SupportedTrafficFields() sets.Set[string] {
	return sets.New(
		constants.TrafficCapabilityAlgorithm,
		constants.TrafficCapabilityHashHeader,
		constants.TrafficCapabilityHashCookie,
		constants.TrafficCapabilityHashSourceIP,
	)
}

// host is the DestinationRule host: the Service's fully qualified in-cluster
// name under the configured cluster domain. Without a domain it is the bare
// Service name: Istio takes a dotted host as fully qualified and ignores a
// rule whose host matches no Service, but expands a bare name with the
// rule's namespace and its own cluster domain.
func (t *Translator) host(isvc *v1beta1.InferenceService) string {
	if t.clusterDomain == "" {
		return isvc.Name
	}
	return constants.ServiceHostname(isvc.Name, isvc.Namespace, t.clusterDomain)
}

// Translate produces the DestinationRule for the InferenceService.
// Deterministic — same inputs produce byte-identical output.
func (t *Translator) Translate(
	isvc *v1beta1.InferenceService,
	_ []string,
	intent *traffic.ResolvedIntent,
) (client.Object, []string, error) {
	dr := &unstructured.Unstructured{}
	dr.SetGroupVersionKind(PolicyGVK)
	dr.SetName(isvc.Name)
	dr.SetNamespace(isvc.Namespace)

	// Istio DR targets a Service hostname, not HTTPRoutes. We target
	// the top-level Service; per-Component coverage would require
	// multiple DRs (deferred).
	if err := unstructured.SetNestedField(dr.Object, t.host(isvc), "spec", "host"); err != nil {
		return nil, nil, fmt.Errorf("set host: %w", err)
	}

	fields, err := intentFields(intent)
	if err != nil {
		return nil, nil, err
	}
	if err := writeFields(dr, fields); err != nil {
		return nil, nil, err
	}

	passthroughs, err := applyPassthroughs(dr, intent.PassthroughIstio)
	if err != nil {
		return nil, nil, err
	}
	return dr, passthroughs, nil
}

// ObserveAcceptance inspects the DestinationRule status. Istio writes
// a `Reconciled` condition on status.conditions when the resource is
// accepted; absent/False maps to Pending/Rejected.
func (t *Translator) ObserveAcceptance(obj client.Object) traffic.AcceptanceObservation {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok || u == nil {
		return traffic.AcceptanceObservation{State: traffic.AcceptancePending}
	}
	conds, found, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if err != nil || !found {
		return traffic.AcceptanceObservation{State: traffic.AcceptancePending}
	}
	for _, raw := range conds {
		c, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if cType, _ := c["type"].(string); cType != istioReadyType {
			continue
		}
		statusStr, _ := c["status"].(string)
		reason, _ := c["reason"].(string)
		message, _ := c["message"].(string)
		switch statusStr {
		case "True":
			return traffic.AcceptanceObservation{
				State:   traffic.AcceptanceAccepted,
				Reason:  reason,
				Message: message,
			}
		case "False":
			return traffic.AcceptanceObservation{
				State:   traffic.AcceptanceRejected,
				Reason:  reason,
				Message: message,
			}
		}
	}
	return traffic.AcceptanceObservation{State: traffic.AcceptancePending}
}

// policyField is one field of spec.trafficPolicy: its path under it, the value
// written there, and the name an error reports.
type policyField struct {
	name  string
	path  []string
	value interface{}
}

// policyFieldAt is the field at a dotted path under trafficPolicy; an error
// names the path.
func policyFieldAt(dotted string, value interface{}) policyField {
	return policyField{name: dotted, path: strings.Split(dotted, "."), value: value}
}

// consistentHashField is a field under loadBalancer.consistentHash; an error
// names it from consistentHash down.
func consistentHashField(dotted string, value interface{}) policyField {
	field := policyFieldAt("loadBalancer.consistentHash."+dotted, value)
	field.name = "consistentHash." + dotted
	return field
}

// writeFields sets each field on the DestinationRule, in order.
func writeFields(dr *unstructured.Unstructured, fields []policyField) error {
	for _, f := range fields {
		path := append([]string{"spec", "trafficPolicy"}, f.path...)
		if err := unstructured.SetNestedField(dr.Object, f.value, path...); err != nil {
			return fmt.Errorf("set %s: %w", f.name, err)
		}
	}
	return nil
}

// intentFields are the trafficPolicy fields the intent asks for, in the order
// they are written: load balancing, the circuit breaker, then timeouts. Retry
// intent has no DestinationRule field (retries live on the VirtualService);
// the reconciler reports the dropped retry-* annotations as UnsupportedField.
func intentFields(intent *traffic.ResolvedIntent) ([]policyField, error) {
	var fields []policyField
	if intent.Traffic != nil {
		loadBalancer, err := loadBalancerFields(intent.Traffic)
		if err != nil {
			return nil, err
		}
		fields = append(fields, loadBalancer...)
	}
	if intent.CircuitBreaker != nil {
		fields = append(fields, circuitBreakerFields(intent.CircuitBreaker)...)
	}
	if intent.Timeout != nil {
		fields = append(fields, timeoutFields(intent.Timeout)...)
	}
	return fields, nil
}

// loadBalancerFields are the loadBalancer fields of a traffic spec: the simple
// algorithm, then the consistent hash. EndpointOverride has no DestinationRule
// analogue; SupportedTrafficFields does not declare it, so the reconciler
// reports it as an unsupported typed field.
func loadBalancerFields(spec *v1beta1.TrafficSpec) ([]policyField, error) {
	var fields []policyField
	if spec.Algorithm != nil && *spec.Algorithm != v1beta1.LoadBalancingTypeConsistentHash {
		simple, ok := algorithmToIstioSimple[*spec.Algorithm]
		if !ok {
			return nil, fmt.Errorf("unsupported algorithm %q for Istio DR", *spec.Algorithm)
		}
		fields = append(fields, policyFieldAt("loadBalancer.simple", simple))
	}
	if spec.ConsistentHash != nil {
		hash, err := consistentHashFields(spec.ConsistentHash)
		if err != nil {
			return nil, err
		}
		fields = append(fields, hash...)
	}
	return fields, nil
}

// consistentHashFields are the consistentHash fields of a hashing spec. Istio's
// httpHeaderName is a single string, so only the first header is emitted;
// admission refuses multi-header specs while this translator is active, and
// the reconciler reports one that slips through as an unsupported typed field.
func consistentHashFields(ch *v1beta1.ConsistentHashSpec) ([]policyField, error) {
	switch ch.Type {
	case v1beta1.HashTypeHeader:
		if len(ch.Headers) == 0 {
			return nil, errors.New("consistentHash.type=Header requires at least one header")
		}
		return []policyField{consistentHashField("httpHeaderName", ch.Headers[0].Name)}, nil
	case v1beta1.HashTypeCookie:
		if ch.Cookie == nil {
			return nil, errors.New("consistentHash.type=Cookie requires cookie")
		}
		fields := []policyField{consistentHashField("httpCookie.name", ch.Cookie.Name)}
		if ch.Cookie.TTLSeconds != nil && *ch.Cookie.TTLSeconds > 0 {
			fields = append(fields, consistentHashField("httpCookie.ttl", fmt.Sprintf("%ds", *ch.Cookie.TTLSeconds)))
		}
		return fields, nil
	case v1beta1.HashTypeSourceIP:
		return []policyField{consistentHashField("useSourceIp", true)}, nil
	}
	return nil, nil
}

// circuitBreakerFields are the connectionPool fields of a circuit breaker.
// Istio's closest analogue of a cap on in-flight requests is http2MaxRequests.
// MaxParallelRetries and PerEndpointMaxConnections have no DestinationRule
// analogue; the reconciler reports them as UnsupportedField.
func circuitBreakerFields(cb *traffic.CircuitBreakerIntent) []policyField {
	var fields []policyField
	if cb.MaxConnections != nil {
		fields = append(fields, policyFieldAt("connectionPool.tcp.maxConnections", int64(*cb.MaxConnections)))
	}
	if cb.MaxParallelRequests != nil {
		fields = append(fields, policyFieldAt("connectionPool.http.http2MaxRequests", int64(*cb.MaxParallelRequests)))
	}
	if cb.MaxPendingRequests != nil {
		fields = append(fields, policyFieldAt("connectionPool.http.http1MaxPendingRequests", int64(*cb.MaxPendingRequests)))
	}
	return fields
}

// timeoutFields are the connectionPool timeouts. MaxConnectionDuration has no
// DestinationRule analogue.
func timeoutFields(to *traffic.TimeoutIntent) []policyField {
	var fields []policyField
	if to.Idle != nil {
		fields = append(fields, policyFieldAt("connectionPool.http.idleTimeout", to.Idle.String()))
	}
	if to.TCPConnect != nil {
		fields = append(fields, policyFieldAt("connectionPool.tcp.connectTimeout", to.TCPConnect.String()))
	}
	return fields
}

// applyPassthroughs writes each ome.io/dr.<path>=<value> annotation
// verbatim into spec.trafficPolicy.<path>. Pass-throughs win over
// structured fields at the same path. Returns sorted list of paths
// applied so status output is deterministic.
func applyPassthroughs(dr *unstructured.Unstructured, paths map[string]string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	applied := make([]string, 0, len(paths))
	for k := range paths {
		applied = append(applied, k)
	}
	sort.Strings(applied)

	fields := make([]policyField, 0, len(applied))
	for _, k := range applied {
		fields = append(fields, policyField{name: fmt.Sprintf("passthrough %q", k), path: strings.Split(k, "."), value: parseScalar(paths[k])})
	}
	if err := writeFields(dr, fields); err != nil {
		return nil, err
	}
	return applied, nil
}

// parseScalar best-effort-coerces a string annotation value into a
// type unstructured.SetNestedField accepts.
func parseScalar(v string) interface{} {
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return v
}

// Compile-time check that Translator satisfies the Translator interface.
var _ traffic.Translator = (*Translator)(nil)
