// Package traffic translates an InferenceService's traffic policy into
// gateway-specific routing resources.
//
// The translator reads the operator's intent (TrafficSpec typed core +
// ome.io/* annotation extension) from an InferenceService, resolves it
// into a ResolvedIntent struct, and hands that to a per-Gateway-
// implementation translator. The translator emits one backend policy
// resource (e.g. Envoy Gateway BackendTrafficPolicy) targeting every
// OME-managed HTTPRoute for the ISVC.
//
// This file defines the resolved-intent data model and the resolve
// function. The Translator interface lives in translator.go. Translator
// implementations live under translators/.
package traffic

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// circuitBreakerKeys, retryKeys and timeoutKeys are the ome.io/* annotation
// keys that feed each resolved group. Together they are
// BackendAnnotationKeys, the keys that flow through a translator.
var (
	circuitBreakerKeys = []string{
		constants.CircuitBreakerMaxConnectionsAnnotation,
		constants.CircuitBreakerMaxParallelRequestsAnnotation,
		constants.CircuitBreakerMaxPendingRequestsAnnotation,
		constants.CircuitBreakerMaxParallelRetriesAnnotation,
		constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation,
	}
	retryKeys = []string{
		constants.RetryAttemptsAnnotation,
		constants.RetryOnAnnotation,
		constants.RetryPerTryTimeoutAnnotation,
	}
	timeoutKeys = []string{
		constants.TimeoutIdleAnnotation,
		constants.TimeoutMaxConnectionDurationAnnotation,
		constants.TimeoutTCPConnectAnnotation,
	}
)

// ResolvedIntent is the per-ISVC traffic-management intent after
// merging the typed core (spec.traffic.*) and the ome.io/* annotation
// extension. Translators receive a ResolvedIntent and decide how to
// represent it in their implementation's policy CRD.
//
// An intent is "empty" (HasIntent() == false) when the operator has
// not declared any traffic-management knobs on the InferenceService.
// In that case the reconciler skips translation entirely — no backend
// policy resource is emitted.
type ResolvedIntent struct {
	// Traffic is the typed core from spec.traffic: the live pointer on the
	// ISVC, which translators treat as read-only. Nil when unset.
	Traffic *v1beta1.TrafficSpec

	// CircuitBreaker holds the resolved circuit-breaker knobs from
	// the ome.io/circuit-breaker-* annotation extension. Nil when
	// none are set.
	CircuitBreaker *CircuitBreakerIntent

	// Retry holds the resolved retry knobs from ome.io/retry-*.
	// Nil when none are set.
	Retry *RetryIntent

	// Timeout holds the resolved sub-second / connection timeouts
	// from ome.io/timeout-*. The request-level timeout stays on
	// the per-Component ComponentExtensionSpec.TimeoutSeconds and is
	// applied by the HTTPRoute builder, not by the translator. Nil
	// when no annotations are set.
	Timeout *TimeoutIntent

	// PassthroughEnvoyGateway is the set of raw
	// ome.io/btp.<path>=<value> annotations, with the prefix
	// stripped. The Envoy Gateway translator stitches these
	// verbatim into the emitted BackendTrafficPolicy. Other
	// translators surface them as UnsupportedField.
	//
	// Map key is the dotted path (e.g. "loadBalancer.slowStart.window")
	// and value is the raw annotation value (e.g. "30s"). Always
	// non-nil but possibly empty.
	PassthroughEnvoyGateway map[string]string

	// PassthroughIstio is the equivalent for ome.io/dr.<path>.
	// Always non-nil but possibly empty.
	PassthroughIstio map[string]string
}

// CircuitBreakerIntent holds resolved circuit-breaker values. Each
// field is a pointer so the translator can distinguish "operator
// asked for this value" from "operator did not set this knob".
type CircuitBreakerIntent struct {
	MaxConnections            *int32
	MaxParallelRequests       *int32
	MaxPendingRequests        *int32
	MaxParallelRetries        *int32
	PerEndpointMaxConnections *int32
}

// RetryIntent holds resolved retry-related values. RetryOn is the
// parsed (and validated, by the webhook) list of retry-condition
// tokens.
type RetryIntent struct {
	Attempts      *int32
	RetryOn       []string
	PerTryTimeout *time.Duration
}

// TimeoutIntent holds resolved non-request-level timeout values.
type TimeoutIntent struct {
	Idle                  *time.Duration
	MaxConnectionDuration *time.Duration
	TCPConnect            *time.Duration
}

// HasIntent reports whether the operator has declared any
// traffic-management intent on the ISVC: a typed core that names
// something a translator renders, a resolved annotation group, or a
// pass-through. Translators are not invoked when this is false; the
// reconciler emits nothing.
func (r *ResolvedIntent) HasIntent() bool {
	if r == nil {
		return false
	}
	return statesTraffic(r.Traffic) ||
		r.CircuitBreaker != nil || r.Retry != nil || r.Timeout != nil ||
		len(r.PassthroughEnvoyGateway) > 0 || len(r.PassthroughIstio) > 0
}

// statesTraffic reports whether the typed core asks for anything a
// translator renders: an algorithm, a consistent hash or an endpoint
// override.
func statesTraffic(t *v1beta1.TrafficSpec) bool {
	return t != nil && (t.Algorithm != nil || t.ConsistentHash != nil || t.EndpointOverride != nil)
}

// Resolve produces a ResolvedIntent from an InferenceService spec.
// It merges the typed core with the ome.io/* annotation extension and
// extracts pass-through namespaces into their dedicated maps.
//
// Values that fail to parse are silently skipped — the validating
// webhook (pkg/validation/traffic_annotations.go) rejects malformed
// values at admission time, so a Resolve call here on a valid ISVC
// will not lose data.
func Resolve(isvc *v1beta1.InferenceService) *ResolvedIntent {
	return &ResolvedIntent{
		Traffic:                 isvc.Spec.Traffic,
		CircuitBreaker:          resolveCircuitBreaker(isvc.Annotations),
		Retry:                   resolveRetry(isvc.Annotations),
		Timeout:                 resolveTimeout(isvc.Annotations),
		PassthroughEnvoyGateway: passthroughs(isvc.Annotations, constants.PassthroughEnvoyGatewayPrefix),
		PassthroughIstio:        passthroughs(isvc.Annotations, constants.PassthroughIstioPrefix),
	}
}

// passthroughs are the annotations under prefix, keyed by the dotted path
// that follows it. Never nil.
func passthroughs(annotations map[string]string, prefix string) map[string]string {
	out := map[string]string{}
	for key, value := range annotations {
		if path, ok := strings.CutPrefix(key, prefix); ok {
			out[path] = value
		}
	}
	return out
}

func resolveCircuitBreaker(annotations map[string]string) *CircuitBreakerIntent {
	if !anyKeySet(annotations, circuitBreakerKeys) {
		return nil
	}
	return &CircuitBreakerIntent{
		MaxConnections:            parseInt32(annotations[constants.CircuitBreakerMaxConnectionsAnnotation]),
		MaxParallelRequests:       parseInt32(annotations[constants.CircuitBreakerMaxParallelRequestsAnnotation]),
		MaxPendingRequests:        parseInt32(annotations[constants.CircuitBreakerMaxPendingRequestsAnnotation]),
		MaxParallelRetries:        parseInt32(annotations[constants.CircuitBreakerMaxParallelRetriesAnnotation]),
		PerEndpointMaxConnections: parseInt32(annotations[constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation]),
	}
}

func resolveRetry(annotations map[string]string) *RetryIntent {
	if !anyKeySet(annotations, retryKeys) {
		return nil
	}
	return &RetryIntent{
		Attempts:      parseInt32(annotations[constants.RetryAttemptsAnnotation]),
		RetryOn:       retryConditions(annotations[constants.RetryOnAnnotation]),
		PerTryTimeout: parseDuration(annotations[constants.RetryPerTryTimeoutAnnotation]),
	}
}

// retryConditions splits the comma-separated retry-condition tokens,
// trimmed, with blanks dropped; nil when none remain.
func retryConditions(raw string) []string {
	var out []string
	for _, tok := range strings.Split(raw, ",") {
		if tok = strings.TrimSpace(tok); tok != "" {
			out = append(out, tok)
		}
	}
	return out
}

func resolveTimeout(annotations map[string]string) *TimeoutIntent {
	if !anyKeySet(annotations, timeoutKeys) {
		return nil
	}
	return &TimeoutIntent{
		Idle:                  parseDuration(annotations[constants.TimeoutIdleAnnotation]),
		MaxConnectionDuration: parseDuration(annotations[constants.TimeoutMaxConnectionDurationAnnotation]),
		TCPConnect:            parseDuration(annotations[constants.TimeoutTCPConnectAnnotation]),
	}
}

// anyKeySet reports whether the annotations carry any of keys, with any
// value.
func anyKeySet(annotations map[string]string, keys []string) bool {
	return slices.ContainsFunc(keys, func(k string) bool {
		_, ok := annotations[k]
		return ok
	})
}

func parseInt32(value string) *int32 {
	if value == "" {
		return nil
	}
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return nil
	}
	out := int32(n)
	return &out
}

func parseDuration(value string) *time.Duration {
	if value == "" {
		return nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return nil
	}
	return &d
}
