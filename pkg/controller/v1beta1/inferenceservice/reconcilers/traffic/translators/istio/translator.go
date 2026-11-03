// Package istio translates traffic intent into an Istio DestinationRule
// (networking.istio.io/v1).
//
// The translator emits an unstructured.Unstructured object so OME does
// not vendor the Istio API. Field paths are pinned by the Istio
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
type Translator struct{}

// New returns a ready-to-use Translator.
func New() *Translator { return &Translator{} }

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
	host := fmt.Sprintf("%s.%s.svc.cluster.local", isvc.Name, isvc.Namespace)
	if err := unstructured.SetNestedField(dr.Object, host, "spec", "host"); err != nil {
		return nil, nil, fmt.Errorf("set host: %w", err)
	}

	if intent.Traffic != nil {
		if err := applyLoadBalancer(dr, intent.Traffic); err != nil {
			return nil, nil, err
		}
	}
	if intent.CircuitBreaker != nil {
		if err := applyCircuitBreaker(dr, intent.CircuitBreaker); err != nil {
			return nil, nil, err
		}
	}
	if intent.Timeout != nil {
		if err := applyTimeout(dr, intent.Timeout); err != nil {
			return nil, nil, err
		}
	}
	// retry intent is intentionally ignored — Istio's DR doesn't model
	// retries (those live on VirtualService). The reconciler surfaces
	// the dropped retry-* annotations as UnsupportedField.

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

func applyLoadBalancer(dr *unstructured.Unstructured, spec *v1beta1.TrafficSpec) error {
	if spec.Algorithm != nil && *spec.Algorithm != v1beta1.LoadBalancingTypeConsistentHash {
		simple, ok := algorithmToIstioSimple[*spec.Algorithm]
		if !ok {
			return fmt.Errorf("unsupported algorithm %q for Istio DR", *spec.Algorithm)
		}
		if err := unstructured.SetNestedField(
			dr.Object, simple,
			"spec", "trafficPolicy", "loadBalancer", "simple",
		); err != nil {
			return fmt.Errorf("set loadBalancer.simple: %w", err)
		}
	}
	if spec.ConsistentHash != nil {
		if err := applyConsistentHash(dr, spec.ConsistentHash); err != nil {
			return err
		}
	}
	// EndpointOverride has no DestinationRule analogue. The intent is
	// dropped here; SupportedTrafficFields does not declare it, so the
	// reconciler surfaces it as an unsupported typed field.
	return nil
}

func applyConsistentHash(dr *unstructured.Unstructured, ch *v1beta1.ConsistentHashSpec) error {
	switch ch.Type {
	case v1beta1.HashTypeHeader:
		if len(ch.Headers) == 0 {
			return errors.New("consistentHash.type=Header requires at least one header")
		}
		// Istio's httpHeaderName is a single string, so only the first
		// header is emitted. Admission rejects multi-header specs when
		// this translator is active (SupportedTrafficFields omits the
		// multi-header capability); the reconciler surfaces any that
		// slip through as an unsupported typed field.
		if err := unstructured.SetNestedField(
			dr.Object, ch.Headers[0].Name,
			"spec", "trafficPolicy", "loadBalancer", "consistentHash", "httpHeaderName",
		); err != nil {
			return fmt.Errorf("set consistentHash.httpHeaderName: %w", err)
		}
	case v1beta1.HashTypeCookie:
		if ch.Cookie == nil {
			return errors.New("consistentHash.type=Cookie requires cookie")
		}
		if err := unstructured.SetNestedField(
			dr.Object, ch.Cookie.Name,
			"spec", "trafficPolicy", "loadBalancer", "consistentHash", "httpCookie", "name",
		); err != nil {
			return fmt.Errorf("set consistentHash.httpCookie.name: %w", err)
		}
		if ch.Cookie.TTLSeconds != nil && *ch.Cookie.TTLSeconds > 0 {
			if err := unstructured.SetNestedField(
				dr.Object, fmt.Sprintf("%ds", *ch.Cookie.TTLSeconds),
				"spec", "trafficPolicy", "loadBalancer", "consistentHash", "httpCookie", "ttl",
			); err != nil {
				return fmt.Errorf("set consistentHash.httpCookie.ttl: %w", err)
			}
		}
	case v1beta1.HashTypeSourceIP:
		if err := unstructured.SetNestedField(
			dr.Object, true,
			"spec", "trafficPolicy", "loadBalancer", "consistentHash", "useSourceIp",
		); err != nil {
			return fmt.Errorf("set consistentHash.useSourceIp: %w", err)
		}
	}
	return nil
}

func applyCircuitBreaker(dr *unstructured.Unstructured, cb *traffic.CircuitBreakerIntent) error {
	if cb.MaxConnections != nil {
		if err := unstructured.SetNestedField(
			dr.Object, int64(*cb.MaxConnections),
			"spec", "trafficPolicy", "connectionPool", "tcp", "maxConnections",
		); err != nil {
			return fmt.Errorf("set connectionPool.tcp.maxConnections: %w", err)
		}
	}
	if cb.MaxParallelRequests != nil {
		// Istio's closest analogue is http2MaxRequests (TCP-level
		// concurrent request cap). Operators want a "max in-flight
		// requests" cap; this is it.
		if err := unstructured.SetNestedField(
			dr.Object, int64(*cb.MaxParallelRequests),
			"spec", "trafficPolicy", "connectionPool", "http", "http2MaxRequests",
		); err != nil {
			return fmt.Errorf("set connectionPool.http.http2MaxRequests: %w", err)
		}
	}
	if cb.MaxPendingRequests != nil {
		if err := unstructured.SetNestedField(
			dr.Object, int64(*cb.MaxPendingRequests),
			"spec", "trafficPolicy", "connectionPool", "http", "http1MaxPendingRequests",
		); err != nil {
			return fmt.Errorf("set connectionPool.http.http1MaxPendingRequests: %w", err)
		}
	}
	// MaxParallelRetries and PerEndpointMaxConnections have no direct
	// Istio DR analogue; the reconciler surfaces them as
	// UnsupportedField.
	return nil
}

func applyTimeout(dr *unstructured.Unstructured, to *traffic.TimeoutIntent) error {
	if to.Idle != nil {
		if err := unstructured.SetNestedField(
			dr.Object, to.Idle.String(),
			"spec", "trafficPolicy", "connectionPool", "http", "idleTimeout",
		); err != nil {
			return fmt.Errorf("set connectionPool.http.idleTimeout: %w", err)
		}
	}
	if to.TCPConnect != nil {
		if err := unstructured.SetNestedField(
			dr.Object, to.TCPConnect.String(),
			"spec", "trafficPolicy", "connectionPool", "tcp", "connectTimeout",
		); err != nil {
			return fmt.Errorf("set connectionPool.tcp.connectTimeout: %w", err)
		}
	}
	// MaxConnectionDuration has no Istio DR analogue.
	return nil
}

// applyPassthroughs writes each ome.io/dr.<path>=<value> annotation
// verbatim into spec.trafficPolicy.<path>. Pass-throughs win over
// structured fields at the same path. Returns sorted list of paths
// applied so status output is deterministic.
func applyPassthroughs(dr *unstructured.Unstructured, paths map[string]string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	sortedKeys := make([]string, 0, len(paths))
	for k := range paths {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	applied := make([]string, 0, len(sortedKeys))
	for _, k := range sortedKeys {
		segments := append([]string{"spec", "trafficPolicy"}, strings.Split(k, ".")...)
		if err := unstructured.SetNestedField(dr.Object, parseScalar(paths[k]), segments...); err != nil {
			return nil, fmt.Errorf("set passthrough %q: %w", k, err)
		}
		applied = append(applied, k)
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
