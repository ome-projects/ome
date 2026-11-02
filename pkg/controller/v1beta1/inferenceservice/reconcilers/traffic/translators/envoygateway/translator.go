// Package envoygateway translates traffic intent into an Envoy Gateway
// BackendTrafficPolicy (gateway.envoyproxy.io/v1alpha1).
//
// The translator emits an unstructured.Unstructured object so OME does
// not vendor the Envoy Gateway Go module — a dep that would force
// large transitive upgrades (notably KEDA). The string paths the
// translator writes into map to the BackendTrafficPolicy CRD schema;
// gateway-controller schema errors surface as GatewayRejected on the
// InferenceService status.
//
// Pass-through (`ome.io/btp.<path>=<value>`) is applied last so an
// operator can override any structured field — this is the documented
// escape hatch.
package envoygateway

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

// Name is the stable identifier the factory uses for log / metric
// labels and that the status writer matches on.
const Name = "envoy-gateway"

// Envoy Gateway BackendTrafficPolicy coordinates. These are pinned by
// the Envoy Gateway public API; any change (e.g. a v1beta1 graduation)
// is a deliberate upgrade.
const (
	groupEnvoyGateway = "gateway.envoyproxy.io"
	versionV1Alpha1   = "v1alpha1"
	kindBTP           = "BackendTrafficPolicy"

	groupGatewayAPI = "gateway.networking.k8s.io"
	kindHTTPRoute   = "HTTPRoute"
)

// PolicyGVK is the BackendTrafficPolicy kind the translator emits; the
// factory probes for its CRD before selecting this translator.
var PolicyGVK = schema.GroupVersionKind{
	Group:   groupEnvoyGateway,
	Version: versionV1Alpha1,
	Kind:    kindBTP,
}

// Translator emits a BackendTrafficPolicy per InferenceService.
type Translator struct{}

// New returns a ready-to-use Translator. There is no per-cluster
// configuration to bind so this exists solely to match the constructor
// shape of other translators (consistency for the factory).
func New() *Translator {
	return &Translator{}
}

// Name returns the stable identifier "envoy-gateway".
func (t *Translator) Name() string {
	return Name
}

// Watches returns a zero-value unstructured BackendTrafficPolicy with
// the GVK set so controller-runtime's Owns() can register the watch
// without us importing the Envoy Gateway types.
func (t *Translator) Watches() client.Object {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(PolicyGVK)
	return u
}

// gatewayAcceptedConditionType is the well-known Gateway API
// condition type the Envoy Gateway control plane writes to the BTP's
// status.conditions list when it accepts (or rejects) a policy.
// Pinned by Gateway API GEP-713; same value used across BackendTLSPolicy,
// BackendTrafficPolicy, ClientTrafficPolicy, etc.
const gatewayAcceptedConditionType = "Accepted"

// ObserveAcceptance inspects the post-apply BackendTrafficPolicy and
// reports whether Envoy Gateway has accepted it. The control plane
// writes a Gateway-API-shaped condition (type=Accepted) to
// status.conditions; we map True->Accepted, False->Rejected, anything
// else (or absent) -> Pending.
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
		if cType, _ := c["type"].(string); cType != gatewayAcceptedConditionType {
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
		// status=Unknown or anything else falls through to Pending.
	}
	return traffic.AcceptanceObservation{State: traffic.AcceptancePending}
}

// SupportedPassthroughPrefixes declares that the Envoy Gateway
// translator owns the ome.io/btp.* pass-through namespace. The
// reconciler uses this to surface UnsupportedField when an operator
// sets an ome.io/dr.* (Istio) pass-through on an EG cluster.
func (t *Translator) SupportedPassthroughPrefixes() []string {
	return []string{constants.PassthroughEnvoyGatewayPrefix}
}

// SupportedAnnotations returns every ome.io/* annotation key that
// flows through a translator: the Envoy Gateway BTP honors them all.
// Pass-through (ome.io/btp.*) is not listed because pass-through is
// per-prefix, not per-key.
func (t *Translator) SupportedAnnotations() sets.Set[string] {
	return traffic.BackendAnnotationKeys()
}

// SupportedTrafficFields returns the typed spec.traffic capability
// tokens the BackendTrafficPolicy emission covers: every algorithm,
// all hash sources including multi-header concatenation, and
// header-based endpoint override. The reserved Metadata endpoint
// override is not declared because nothing is emitted for it.
func (t *Translator) SupportedTrafficFields() sets.Set[string] {
	return sets.New(
		constants.TrafficCapabilityAlgorithm,
		constants.TrafficCapabilityHashHeader,
		constants.TrafficCapabilityHashMultipleHeaders,
		constants.TrafficCapabilityHashCookie,
		constants.TrafficCapabilityHashSourceIP,
		constants.TrafficCapabilityEndpointOverrideHeader,
	)
}

// Translate produces the BackendTrafficPolicy resource for the
// InferenceService. Deterministic — same inputs produce byte-identical
// output, so the reconciler's coarse Update doesn't churn the API
// server when nothing has changed.
func (t *Translator) Translate(
	isvc *v1beta1.InferenceService,
	targetHTTPRoutes []string,
	intent *traffic.ResolvedIntent,
) (client.Object, []string, error) {
	btp := &unstructured.Unstructured{}
	btp.SetGroupVersionKind(PolicyGVK)
	btp.SetName(isvc.Name)
	btp.SetNamespace(isvc.Namespace)

	if err := setTargetRefs(btp, targetHTTPRoutes); err != nil {
		return nil, nil, err
	}

	if intent.Traffic != nil {
		if err := applyLoadBalancer(btp, intent.Traffic); err != nil {
			return nil, nil, err
		}
	}
	if intent.CircuitBreaker != nil {
		if err := applyCircuitBreaker(btp, intent.CircuitBreaker); err != nil {
			return nil, nil, err
		}
	}
	if intent.Retry != nil {
		if err := applyRetry(btp, intent.Retry); err != nil {
			return nil, nil, err
		}
	}
	if intent.Timeout != nil {
		if err := applyTimeout(btp, intent.Timeout); err != nil {
			return nil, nil, err
		}
	}

	// Pass-through stitches are applied LAST so operators can override
	// any structured field via ome.io/btp.<path>=<value>. The returned
	// slice is sorted so the status condition message and metrics are
	// deterministic.
	passthroughs, err := applyPassthroughs(btp, intent.PassthroughEnvoyGateway)
	if err != nil {
		return nil, nil, err
	}
	return btp, passthroughs, nil
}

// setTargetRefs writes spec.targetRefs as a list of HTTPRoute
// references. One entry per OME-managed HTTPRoute.
func setTargetRefs(btp *unstructured.Unstructured, routes []string) error {
	refs := make([]interface{}, 0, len(routes))
	for _, name := range routes {
		refs = append(refs, map[string]interface{}{
			"group": groupGatewayAPI,
			"kind":  kindHTTPRoute,
			"name":  name,
		})
	}
	return unstructured.SetNestedSlice(btp.Object, refs, "spec", "targetRefs")
}

func applyLoadBalancer(btp *unstructured.Unstructured, spec *v1beta1.TrafficSpec) error {
	if spec.Algorithm != nil {
		// EG BackendTrafficPolicy CRD has CEL validation
		//   self.type == 'ConsistentHash' ? has(self.consistentHash) : !has(self.consistentHash)
		// Admission catches the missing consistentHash block (the
		// MissingConsistentHashSpec rule), but if admission is
		// bypassed the translator must still fail safe rather than
		// emit a malformed BTP that the gateway controller will
		// reject silently. Same for the inverse — a consistentHash
		// block without algorithm=ConsistentHash would violate the
		// CEL rule from the other side.
		if *spec.Algorithm == v1beta1.LoadBalancingTypeConsistentHash && spec.ConsistentHash == nil {
			return fmt.Errorf("spec.traffic.algorithm=ConsistentHash requires spec.traffic.consistentHash to be set")
		}
		if *spec.Algorithm != v1beta1.LoadBalancingTypeConsistentHash && spec.ConsistentHash != nil {
			return fmt.Errorf("spec.traffic.consistentHash must not be set when algorithm=%q", *spec.Algorithm)
		}
		if err := unstructured.SetNestedField(
			btp.Object, string(*spec.Algorithm),
			"spec", "loadBalancer", "type",
		); err != nil {
			return fmt.Errorf("set loadBalancer.type: %w", err)
		}
	}
	if spec.ConsistentHash != nil {
		if err := applyConsistentHash(btp, spec.ConsistentHash); err != nil {
			return err
		}
	}
	if spec.EndpointOverride != nil {
		if err := applyEndpointOverride(btp, spec.EndpointOverride); err != nil {
			return err
		}
	}
	return nil
}

func applyConsistentHash(btp *unstructured.Unstructured, ch *v1beta1.ConsistentHashSpec) error {
	// Map the OME enum to the EG-side type field. EG v1.7+ added the
	// plural "Headers" type + headers[] list for multi-header hashing
	// (the singular "Header"/header is marked Deprecated in EG v1.7).
	// The OME API exposes a single "Header" enum value; the translator
	// picks the EG-side type based on header count so that older EG
	// clusters keep working with one header.
	egType := string(ch.Type)
	if ch.Type == v1beta1.HashTypeHeader && len(ch.Headers) > 1 {
		egType = "Headers"
	}
	if err := unstructured.SetNestedField(
		btp.Object, egType,
		"spec", "loadBalancer", "consistentHash", "type",
	); err != nil {
		return fmt.Errorf("set consistentHash.type: %w", err)
	}
	switch ch.Type {
	case v1beta1.HashTypeHeader:
		if len(ch.Headers) == 0 {
			return errors.New("consistentHash.type=Header requires at least one header")
		}
		if len(ch.Headers) == 1 {
			// Singular form — works on EG <v1.7 too.
			if err := unstructured.SetNestedField(
				btp.Object, ch.Headers[0].Name,
				"spec", "loadBalancer", "consistentHash", "header", "name",
			); err != nil {
				return fmt.Errorf("set consistentHash.header.name: %w", err)
			}
		} else {
			// Plural form — requires EG v1.7+ which exposes
			// spec.loadBalancer.consistentHash.headers[] alongside the
			// new "Headers" type enum value. CEL validation in the EG
			// CRD enforces type/field correspondence; the type
			// override above keeps us aligned with that.
			items := make([]interface{}, 0, len(ch.Headers))
			for _, h := range ch.Headers {
				items = append(items, map[string]interface{}{"name": h.Name})
			}
			if err := unstructured.SetNestedSlice(
				btp.Object, items,
				"spec", "loadBalancer", "consistentHash", "headers",
			); err != nil {
				return fmt.Errorf("set consistentHash.headers: %w", err)
			}
		}
	case v1beta1.HashTypeCookie:
		if ch.Cookie == nil {
			return errors.New("consistentHash.type=Cookie requires cookie")
		}
		if err := unstructured.SetNestedField(
			btp.Object, ch.Cookie.Name,
			"spec", "loadBalancer", "consistentHash", "cookie", "name",
		); err != nil {
			return fmt.Errorf("set consistentHash.cookie.name: %w", err)
		}
		if ch.Cookie.TTLSeconds != nil && *ch.Cookie.TTLSeconds > 0 {
			if err := unstructured.SetNestedField(
				btp.Object, fmt.Sprintf("%ds", *ch.Cookie.TTLSeconds),
				"spec", "loadBalancer", "consistentHash", "cookie", "ttl",
			); err != nil {
				return fmt.Errorf("set consistentHash.cookie.ttl: %w", err)
			}
		}
	case v1beta1.HashTypeSourceIP:
		// EG infers source-IP hashing from type=SourceIP alone; no
		// additional fields required.
	}
	return nil
}

func applyEndpointOverride(btp *unstructured.Unstructured, eo *v1beta1.EndpointOverrideSpec) error {
	switch eo.Type {
	case v1beta1.EndpointOverrideTypeHeader:
		if len(eo.Headers) == 0 {
			return errors.New("endpointOverride.type=Header requires at least one header")
		}
		extract := make([]interface{}, 0, len(eo.Headers))
		for _, h := range eo.Headers {
			extract = append(extract, map[string]interface{}{
				"header": h.Name,
			})
		}
		if err := unstructured.SetNestedSlice(
			btp.Object, extract,
			"spec", "loadBalancer", "endpointOverride", "extractFrom",
		); err != nil {
			return fmt.Errorf("set endpointOverride.extractFrom: %w", err)
		}
	case v1beta1.EndpointOverrideTypeMetadata:
		// Reserved value with no BackendTrafficPolicy emission. It is
		// absent from SupportedTrafficFields, so admission rejects it
		// and the reconciler surfaces it as an unsupported typed field
		// if one slips through.
	}
	return nil
}

func applyCircuitBreaker(btp *unstructured.Unstructured, cb *traffic.CircuitBreakerIntent) error {
	setInt := func(value *int32, path ...string) error {
		if value == nil {
			return nil
		}
		full := append([]string{"spec", "circuitBreaker"}, path...)
		if err := unstructured.SetNestedField(btp.Object, int64(*value), full...); err != nil {
			return fmt.Errorf("set %s: %w", strings.Join(path, "."), err)
		}
		return nil
	}
	if err := setInt(cb.MaxConnections, "maxConnections"); err != nil {
		return err
	}
	if err := setInt(cb.MaxParallelRequests, "maxParallelRequests"); err != nil {
		return err
	}
	if err := setInt(cb.MaxPendingRequests, "maxPendingRequests"); err != nil {
		return err
	}
	if err := setInt(cb.MaxParallelRetries, "maxParallelRetries"); err != nil {
		return err
	}
	if err := setInt(cb.PerEndpointMaxConnections, "perEndpoint", "maxConnections"); err != nil {
		return err
	}
	return nil
}

func applyRetry(btp *unstructured.Unstructured, r *traffic.RetryIntent) error {
	if r.Attempts != nil {
		if err := unstructured.SetNestedField(
			btp.Object, int64(*r.Attempts),
			"spec", "retry", "numRetries",
		); err != nil {
			return fmt.Errorf("set retry.numRetries: %w", err)
		}
	}
	if r.PerTryTimeout != nil {
		if err := unstructured.SetNestedField(
			btp.Object, r.PerTryTimeout.String(),
			"spec", "retry", "perRetry", "timeout",
		); err != nil {
			return fmt.Errorf("set retry.perRetry.timeout: %w", err)
		}
	}
	if len(r.RetryOn) > 0 {
		triggers, codes := splitRetryOnTokens(r.RetryOn)
		if len(triggers) > 0 {
			items := make([]interface{}, 0, len(triggers))
			for _, t := range triggers {
				items = append(items, t)
			}
			if err := unstructured.SetNestedSlice(
				btp.Object, items,
				"spec", "retry", "retryOn", "triggers",
			); err != nil {
				return fmt.Errorf("set retry.retryOn.triggers: %w", err)
			}
		}
		if len(codes) > 0 {
			items := make([]interface{}, 0, len(codes))
			for _, c := range codes {
				items = append(items, int64(c))
			}
			if err := unstructured.SetNestedSlice(
				btp.Object, items,
				"spec", "retry", "retryOn", "httpStatusCodes",
			); err != nil {
				return fmt.Errorf("set retry.retryOn.httpStatusCodes: %w", err)
			}
		}
	}
	return nil
}

func applyTimeout(btp *unstructured.Unstructured, to *traffic.TimeoutIntent) error {
	if to.Idle != nil {
		if err := unstructured.SetNestedField(
			btp.Object, to.Idle.String(),
			"spec", "timeout", "http", "connectionIdleTimeout",
		); err != nil {
			return fmt.Errorf("set timeout.http.connectionIdleTimeout: %w", err)
		}
	}
	if to.MaxConnectionDuration != nil {
		if err := unstructured.SetNestedField(
			btp.Object, to.MaxConnectionDuration.String(),
			"spec", "timeout", "http", "maxConnectionDuration",
		); err != nil {
			return fmt.Errorf("set timeout.http.maxConnectionDuration: %w", err)
		}
	}
	if to.TCPConnect != nil {
		if err := unstructured.SetNestedField(
			btp.Object, to.TCPConnect.String(),
			"spec", "timeout", "tcp", "connectTimeout",
		); err != nil {
			return fmt.Errorf("set timeout.tcp.connectTimeout: %w", err)
		}
	}
	return nil
}

// applyPassthroughs writes each ome.io/btp.<path>=<value> annotation
// verbatim into spec.<path>. Pass-throughs win over structured fields
// at the same path. Returns the sorted list of paths applied so the
// status writer can surface them deterministically.
func applyPassthroughs(btp *unstructured.Unstructured, paths map[string]string) ([]string, error) {
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
		segments := append([]string{"spec"}, strings.Split(k, ".")...)
		if err := unstructured.SetNestedField(btp.Object, parseScalar(paths[k]), segments...); err != nil {
			return nil, fmt.Errorf("set passthrough %q: %w", k, err)
		}
		applied = append(applied, k)
	}
	return applied, nil
}

// parseScalar best-effort-coerces a string annotation value into a
// type unstructured.SetNestedField accepts. Falls back to the original
// string when nothing parses. The gateway controller catches type
// mismatches at admission and we surface GatewayRejected — OME does
// not pre-validate pass-through types since the schema is owned by
// the gateway implementation.
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

// splitRetryOnTokens splits the operator-supplied retry-on list into
// EG's two buckets: numeric tokens become httpStatusCodes; everything
// else stays a string trigger. Empty tokens are skipped (Resolve
// already drops them, this is defense-in-depth).
func splitRetryOnTokens(tokens []string) (triggers []string, codes []int32) {
	for _, t := range tokens {
		if t == "" {
			continue
		}
		if n, err := strconv.ParseInt(t, 10, 32); err == nil {
			codes = append(codes, int32(n))
			continue
		}
		triggers = append(triggers, t)
	}
	return triggers, codes
}

// Compile-time check that Translator satisfies the Translator interface.
var _ traffic.Translator = (*Translator)(nil)
