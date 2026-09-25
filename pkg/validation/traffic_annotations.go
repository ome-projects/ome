// Traffic annotation validation.
//
// Parses, type-coerces, and cross-checks the ome.io/* traffic
// annotations defined in pkg/constants/traffic_annotations.go.
// Wired into the InferenceService validating webhook after the
// typed-core check (ValidateTrafficSpec).
package validation

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/trafficdrain"
)

// trafficAnnotationKind classifies an annotation key's expected value
// type so the webhook can coerce + reject malformed values uniformly.
type trafficAnnotationKind int

const (
	kindUnknown trafficAnnotationKind = iota
	kindInt
	kindPositiveInt // int >= 1
	kindDuration
	kindBool
	kindRetryOnList
	kindPromoteTarget // canary revision hash
	kindTrafficDrain  // versioned JSON map of manual route-arm holds
)

// trafficAnnotationKinds is the authoritative classifier for every
// documented ome.io/* traffic annotation key. Keys not in this map and not
// matching a pass-through prefix are unrecognised (subject to the
// Did-you-mean check).
//
// Must stay in lockstep with pkg/constants/traffic_annotations.go.
var trafficAnnotationKinds = map[string]trafficAnnotationKind{
	// Circuit breaker — int counts.
	constants.CircuitBreakerMaxConnectionsAnnotation:            kindInt,
	constants.CircuitBreakerMaxParallelRequestsAnnotation:       kindInt,
	constants.CircuitBreakerMaxPendingRequestsAnnotation:        kindInt,
	constants.CircuitBreakerMaxParallelRetriesAnnotation:        kindInt,
	constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation: kindInt,

	// Retries.
	constants.RetryAttemptsAnnotation:      kindInt,
	constants.RetryOnAnnotation:            kindRetryOnList,
	constants.RetryPerTryTimeoutAnnotation: kindDuration,

	// Sub-second / connection timeouts.
	constants.TimeoutIdleAnnotation:                  kindDuration,
	constants.TimeoutMaxConnectionDurationAnnotation: kindDuration,
	constants.TimeoutTCPConnectAnnotation:            kindDuration,

	// Operational.
	constants.TrafficDrainAnnotation:           kindTrafficDrain,
	constants.ManagedByConflictAckedAnnotation: kindBool,
	constants.RolloutReadyTimeoutAnnotation:    kindDuration,
	constants.RevisionHistoryLimitAnnotation:   kindPositiveInt,
	constants.RolloutPromoteAnnotation:         kindPromoteTarget,
	constants.RolloutRollbackAnnotation:        kindBool,
}

// validRetryOnTokens enumerates the documented Envoy retry-on
// tokens. Tokens are case-sensitive (Envoy is case-sensitive).
var validRetryOnTokens = map[string]struct{}{
	"5xx":                    {},
	"reset":                  {},
	"gateway-error":          {},
	"connect-failure":        {},
	"retriable-status-codes": {},
}

// ValidateTrafficAnnotations is the webhook entry point. It walks
// every annotation on the object, validates the type-coerced value of
// known keys, surfaces Did-you-mean for near-miss unknown keys in the
// ome.io/ namespace, and enforces cross-annotation rules.
//
// knownPassthroughPrefixes is the set of ome.io/* pass-through prefixes
// the active Gateway-implementation translator supports. When
// non-empty, pass-through keys outside the set are rejected with
// UnsupportedPassthrough (fast-fail at admission instead of relying on
// the controller's BackendPolicyUnsupportedFields condition). When
// empty (legacy / test default), all documented pass-through prefixes
// are accepted.
//
// Returns admission warnings (for non-fatal hints like
// "perEndpointMaxConnections > 1 with ConsistentHash usually defeats
// stickiness") alongside the first hard error.
func ValidateTrafficAnnotations(annotations map[string]string, traffic *v1beta1.TrafficSpec, knownPassthroughPrefixes ...string) (warnings []string, err error) {
	// Per-key parse + type check.
	for key, value := range annotations {
		kind, known := trafficAnnotationKinds[key]
		if known {
			if err := validateAnnotationValue(key, value, kind); err != nil {
				return warnings, err
			}
			continue
		}

		// Pass-through namespaces. Either accept all documented prefixes
		// (back-compat when knownPassthroughPrefixes is empty) or only
		// those the active translator can honor.
		if strings.HasPrefix(key, constants.PassthroughEnvoyGatewayPrefix) ||
			strings.HasPrefix(key, constants.PassthroughIstioPrefix) {
			if len(knownPassthroughPrefixes) > 0 && !hasMatchingPrefix(key, knownPassthroughPrefixes) {
				return warnings, fmt.Errorf(
					"annotation %q uses a pass-through prefix the active Gateway-implementation translator does not honor; supported prefixes: %v (UnsupportedPassthrough)",
					key, knownPassthroughPrefixes,
				)
			}
			continue
		}

		// Did-you-mean for near-miss ome.io/* keys. Only fires for
		// keys in the ome.io namespace; foreign-namespace annotations
		// are ignored entirely.
		if strings.HasPrefix(key, constants.OMEAPIGroupName+"/") {
			if suggestion, ok := closestTrafficAnnotation(key); ok {
				return warnings, fmt.Errorf("unknown traffic annotation %q. Did you mean %q? (UnknownTrafficAnnotation)", key, suggestion)
			}
		}
	}

	// Cross-rules — only check if dependencies are present.
	if w, err := crossRuleRetry(annotations); err != nil {
		return warnings, err
	} else if w != "" {
		warnings = append(warnings, w)
	}
	if w, err := crossRuleStickyEndpointCap(annotations, traffic); err != nil {
		return warnings, err
	} else if w != "" {
		warnings = append(warnings, w)
	}
	if w, err := crossRuleRolloutTimeoutSanity(annotations); err != nil {
		return warnings, err
	} else if w != "" {
		warnings = append(warnings, w)
	}

	return warnings, nil
}

func validateAnnotationValue(key, value string, kind trafficAnnotationKind) error {
	switch kind {
	case kindInt:
		if _, err := strconv.Atoi(value); err != nil {
			return fmt.Errorf("annotation %q value %q is not a valid integer (InvalidIntValue): %w", key, value, err)
		}
	case kindPositiveInt:
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("annotation %q value %q is not a valid integer (InvalidIntValue): %w", key, value, err)
		}
		if n < 1 {
			return fmt.Errorf("annotation %q value %q must be a positive integer (>= 1) (InvalidPositiveIntValue)", key, value)
		}
	case kindDuration:
		if _, err := time.ParseDuration(value); err != nil {
			return fmt.Errorf("annotation %q value %q is not a valid duration (InvalidDuration): %w", key, value, err)
		}
	case kindBool:
		// Only "true" is meaningful; "false" is identical to absence.
		// We accept either spelling so operators can be explicit.
		if value != "true" && value != "false" {
			return fmt.Errorf("annotation %q value %q must be \"true\" or \"false\" (InvalidBoolValue)", key, value)
		}
	case kindRetryOnList:
		if value == "" {
			return fmt.Errorf("annotation %q must be a non-empty comma-separated list (InvalidRetryOn)", key)
		}
		for _, raw := range strings.Split(value, ",") {
			tok := strings.TrimSpace(raw)
			if tok == "" {
				return fmt.Errorf("annotation %q has an empty token (InvalidRetryOn)", key)
			}
			if _, ok := validRetryOnTokens[tok]; !ok {
				return fmt.Errorf("annotation %q token %q is not a supported retry condition (InvalidRetryOn)", key, tok)
			}
		}
	case kindPromoteTarget:
		// The canary manual-promote verb only ever matches the exact canary
		// revision hash (promotion.shouldAdvanceManual), so admission accepts
		// nothing else — any other value would sit on the object doing nothing.
		if !isRevisionHashToken(value) {
			return fmt.Errorf("annotation %q value %q must be the canary revision hash to promote, copied from status.canary.canaryRevisionHash (lowercase alphanumeric, at least 6 chars); \"full\" is not a supported promotion target (InvalidRolloutPromoteTarget)", key, value)
		}
	case kindTrafficDrain:
		if _, err := trafficdrain.Parse(value); err != nil {
			return fmt.Errorf("annotation %q is not a valid traffic-drain value (InvalidTrafficDrain): %w", key, err)
		}
	}
	return nil
}

// isRevisionHashToken reports whether s looks like a ControllerRevision hash
// suffix — lowercase alphanumeric, at least 6 chars — the value the canary's
// manual-promote verb expects (status.canary.canaryRevisionHash).
func isRevisionHashToken(s string) bool {
	if len(s) < 6 {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

// crossRuleRetry enforces "retry-attempts > 0 requires retry-on".
func crossRuleRetry(annotations map[string]string) (string, error) {
	attemptsStr, hasAttempts := annotations[constants.RetryAttemptsAnnotation]
	if !hasAttempts {
		return "", nil
	}
	n, _ := strconv.Atoi(attemptsStr) // already validated as int above
	if n <= 0 {
		return "", nil
	}
	if _, hasOn := annotations[constants.RetryOnAnnotation]; !hasOn {
		return "", fmt.Errorf("annotation %q set to %d requires %q to specify retry conditions (MissingRetryOn)",
			constants.RetryAttemptsAnnotation, n, constants.RetryOnAnnotation)
	}
	return "", nil
}

// crossRuleStickyEndpointCap warns when
// circuit-breaker-per-endpoint-max-connections > 1 with
// ConsistentHash — this combination usually defeats the purpose of
// sticky routing.
func crossRuleStickyEndpointCap(annotations map[string]string, traffic *v1beta1.TrafficSpec) (string, error) {
	if traffic == nil || traffic.Algorithm == nil || *traffic.Algorithm != v1beta1.LoadBalancingTypeConsistentHash {
		return "", nil
	}
	capStr, ok := annotations[constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation]
	if !ok {
		return "", nil
	}
	n, _ := strconv.Atoi(capStr) // validated above
	if n > 1 {
		return fmt.Sprintf("annotation %q=%d combined with traffic.algorithm=ConsistentHash usually defeats sticky routing; set to 1 to keep sessions on a single pod",
			constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation, n), nil
	}
	return "", nil
}

// crossRuleRolloutTimeoutSanity warns when rollout-ready-timeout is
// set to less than 1 minute. Most LLM-runtime warmups need at least
// minutes; a sub-minute timeout almost guarantees Failed rollouts.
func crossRuleRolloutTimeoutSanity(annotations map[string]string) (string, error) {
	v, ok := annotations[constants.RolloutReadyTimeoutAnnotation]
	if !ok {
		return "", nil
	}
	d, err := time.ParseDuration(v) // validated above
	if err != nil {
		return "", nil
	}
	if d > 0 && d < time.Minute {
		return fmt.Sprintf("annotation %q=%s is less than 1 minute; most LLM runtimes need longer to reach Ready (RolloutTimeoutTooShort)",
			constants.RolloutReadyTimeoutAnnotation, v), nil
	}
	return "", nil
}

// closestTrafficAnnotation returns the documented traffic
// annotation key closest to the given input under Damerau–Levenshtein
// distance, capped at 2. Returns "", false when no candidate is
// within distance 2.
func closestTrafficAnnotation(input string) (string, bool) {
	const maxDistance = 2
	best := ""
	bestDistance := maxDistance + 1
	for key := range trafficAnnotationKinds {
		d := levenshtein(input, key)
		if d < bestDistance {
			best = key
			bestDistance = d
		}
	}
	if bestDistance <= maxDistance {
		return best, true
	}
	return "", false
}

// levenshtein computes the edit distance between two strings.
// Standard dynamic-programming algorithm with a two-row buffer.
func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min3(
				prev[j]+1,      // deletion
				curr[j-1]+1,    // insertion
				prev[j-1]+cost, // substitution
			)
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

func min3(a, b, c int) int {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	return m
}

// hasMatchingPrefix reports whether key starts with any prefix in the
// list. Used to gate ome.io/btp.* and ome.io/dr.* pass-throughs against
// the active translator's SupportedPassthroughPrefixes.
func hasMatchingPrefix(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}
