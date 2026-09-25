package validation

import (
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestValidateTrafficAnnotations_TypedValues(t *testing.T) {
	tests := []struct {
		name         string
		annotations  map[string]string
		wantOK       bool
		wantContains string
	}{
		// Happy paths.
		{
			name:        "empty annotations",
			annotations: nil,
			wantOK:      true,
		},
		{
			name: "valid circuit breaker int",
			annotations: map[string]string{
				constants.CircuitBreakerMaxConnectionsAnnotation: "1024",
			},
			wantOK: true,
		},
		{
			name: "valid duration",
			annotations: map[string]string{
				constants.TimeoutIdleAnnotation: "60s",
			},
			wantOK: true,
		},
		{
			name: "valid retry-on tokens",
			annotations: map[string]string{
				constants.RetryAttemptsAnnotation: "0",
				constants.RetryOnAnnotation:       "5xx,reset,gateway-error",
			},
			wantOK: true,
		},
		{
			name: "valid promote target revision hash",
			annotations: map[string]string{
				constants.RolloutPromoteAnnotation: "e5d6f79d",
			},
			wantOK: true,
		},
		{
			name: "valid rollback bool",
			annotations: map[string]string{
				constants.RolloutRollbackAnnotation: "true",
			},
			wantOK: true,
		},
		{
			name: "valid revision history limit",
			annotations: map[string]string{
				constants.RevisionHistoryLimitAnnotation: "1",
			},
			wantOK: true,
		},
		{
			name: "valid traffic drain map",
			annotations: map[string]string{
				constants.TrafficDrainAnnotation: `{"drain-1":{"cluster":"cluster-a","reason":"planned maintenance"}}`,
			},
			wantOK: true,
		},

		// Type-coercion failures.
		{
			name: "int annotation not an int",
			annotations: map[string]string{
				constants.CircuitBreakerMaxConnectionsAnnotation: "notanumber",
			},
			wantContains: "InvalidIntValue",
		},
		{
			name: "duration annotation not a duration",
			annotations: map[string]string{
				constants.TimeoutIdleAnnotation: "10",
			},
			wantContains: "InvalidDuration",
		},
		{
			name: "bool annotation not true/false",
			annotations: map[string]string{
				constants.RolloutRollbackAnnotation: "yes",
			},
			wantContains: "InvalidBoolValue",
		},
		{
			name: "retry-on with unknown token",
			annotations: map[string]string{
				constants.RetryAttemptsAnnotation: "1",
				constants.RetryOnAnnotation:       "5xx,not-a-real-condition",
			},
			wantContains: "InvalidRetryOn",
		},
		{
			name: "retry-on empty token in list",
			annotations: map[string]string{
				constants.RetryAttemptsAnnotation: "1",
				constants.RetryOnAnnotation:       "5xx,,reset",
			},
			wantContains: "InvalidRetryOn",
		},
		{
			name: "revision history limit not an int",
			annotations: map[string]string{
				constants.RevisionHistoryLimitAnnotation: "many",
			},
			wantContains: "InvalidIntValue",
		},
		{
			name: "revision history limit zero",
			annotations: map[string]string{
				constants.RevisionHistoryLimitAnnotation: "0",
			},
			wantContains: "InvalidPositiveIntValue",
		},
		{
			name: "revision history limit negative",
			annotations: map[string]string{
				constants.RevisionHistoryLimitAnnotation: "-3",
			},
			wantContains: "InvalidPositiveIntValue",
		},
		{
			name: "promote target negative",
			annotations: map[string]string{
				constants.RolloutPromoteAnnotation: "-1",
			},
			wantContains: "InvalidRolloutPromoteTarget",
		},
		{
			name: "promote target garbage",
			annotations: map[string]string{
				constants.RolloutPromoteAnnotation: "next",
			},
			wantContains: "InvalidRolloutPromoteTarget",
		},
		{
			// The executor only ever matches the exact canary revision hash, so a
			// "full" promotion could never take effect — reject it at admission
			// instead of accepting a command that silently does nothing.
			name: "promote target full is rejected",
			annotations: map[string]string{
				constants.RolloutPromoteAnnotation: "full",
			},
			wantContains: "InvalidRolloutPromoteTarget",
		},
		{
			name: "traffic drain malformed JSON",
			annotations: map[string]string{
				constants.TrafficDrainAnnotation: `{"drain-1":`,
			},
			wantContains: "InvalidTrafficDrain",
		},
		{
			name: "traffic drain missing reason",
			annotations: map[string]string{
				constants.TrafficDrainAnnotation: `{"drain-1":{"cluster":"cluster-a"}}`,
			},
			wantContains: "InvalidTrafficDrain",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateTrafficAnnotations(tc.annotations, nil)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("expected ok, got error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantContains)
			}
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantContains, err)
			}
		})
	}
}

func TestValidateTrafficAnnotations_DidYouMean(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		expectError bool
		expectHint  string
	}{
		{
			name:        "exact known key has no suggestion",
			key:         constants.CircuitBreakerMaxConnectionsAnnotation,
			expectError: false,
		},
		{
			name:        "single-char typo gets suggestion",
			key:         constants.OMEAPIGroupName + "/circut-breaker-max-connections", // missing i
			expectError: true,
			expectHint:  constants.CircuitBreakerMaxConnectionsAnnotation,
		},
		{
			name:        "transposition gets suggestion",
			key:         constants.OMEAPIGroupName + "/circuit-braeker-max-connections", // transposed
			expectError: true,
			expectHint:  constants.CircuitBreakerMaxConnectionsAnnotation,
		},
		{
			name:        "far-away key gets no suggestion (ignored)",
			key:         constants.OMEAPIGroupName + "/this-is-not-related-at-all",
			expectError: false, // > Levenshtein 2 from every known key → silently ignored
		},
		{
			name:        "non-ome.io annotation is ignored entirely",
			key:         "foo.example.com/random",
			expectError: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ann := map[string]string{tc.key: "1"}
			// For "exact known key", give a valid value for the type.
			if tc.key == constants.CircuitBreakerMaxConnectionsAnnotation {
				ann[tc.key] = "1024"
			}
			_, err := ValidateTrafficAnnotations(ann, nil)
			if !tc.expectError {
				if err != nil {
					t.Fatalf("expected ok, got error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error with hint %q, got nil", tc.expectHint)
			}
			if !strings.Contains(err.Error(), "UnknownTrafficAnnotation") {
				t.Fatalf("expected UnknownTrafficAnnotation in error, got: %v", err)
			}
			if !strings.Contains(err.Error(), tc.expectHint) {
				t.Fatalf("expected Did-you-mean hint %q in error, got: %v", tc.expectHint, err)
			}
		})
	}
}

func TestValidateTrafficAnnotations_PassthroughIgnored(t *testing.T) {
	// Pass-through namespaces accept any key; the translator validates
	// the field path at emit time. Webhook must not reject these when
	// no knownPassthroughPrefixes filter is set (back-compat default).
	tests := []struct {
		name string
		key  string
	}{
		{
			name: "envoy gateway passthrough",
			key:  constants.PassthroughEnvoyGatewayPrefix + "loadBalancer.slowStart.window",
		},
		{
			name: "istio passthrough",
			key:  constants.PassthroughIstioPrefix + "trafficPolicy.connectionPool.tcp.tcpKeepalive.time",
		},
		{
			name: "passthrough with deep path",
			key:  constants.PassthroughEnvoyGatewayPrefix + "deeply.nested.field.with.lots.of.dots",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateTrafficAnnotations(map[string]string{tc.key: "any-value"}, nil)
			if err != nil {
				t.Fatalf("pass-through %q must be accepted by webhook, got error: %v", tc.key, err)
			}
		})
	}
}

func TestValidateTrafficAnnotations_PassthroughGatedByKnownPrefixes(t *testing.T) {
	// When the validator is wired with the active translator's
	// SupportedPassthroughPrefixes, pass-through annotations outside
	// the set must be rejected at admission (UnsupportedPassthrough).
	tests := []struct {
		name         string
		key          string
		known        []string
		wantErrMatch string // empty = expect success
	}{
		{
			name:  "btp.* accepted when EG prefix known",
			key:   constants.PassthroughEnvoyGatewayPrefix + "loadBalancer.slowStart.window",
			known: []string{constants.PassthroughEnvoyGatewayPrefix},
		},
		{
			name:         "btp.* rejected when only Istio prefix known",
			key:          constants.PassthroughEnvoyGatewayPrefix + "loadBalancer.slowStart.window",
			known:        []string{constants.PassthroughIstioPrefix},
			wantErrMatch: "UnsupportedPassthrough",
		},
		{
			name:  "dr.* accepted when Istio prefix known",
			key:   constants.PassthroughIstioPrefix + "trafficPolicy.connectionPool.tcp.tcpKeepalive.time",
			known: []string{constants.PassthroughIstioPrefix},
		},
		{
			name:         "dr.* rejected when only EG prefix known",
			key:          constants.PassthroughIstioPrefix + "trafficPolicy.connectionPool.tcp.tcpKeepalive.time",
			known:        []string{constants.PassthroughEnvoyGatewayPrefix},
			wantErrMatch: "UnsupportedPassthrough",
		},
		{
			name:         "any pass-through rejected when prefix list is non-empty but doesn't match",
			key:          constants.PassthroughEnvoyGatewayPrefix + "loadBalancer.x",
			known:        []string{"ome.io/future-translator."},
			wantErrMatch: "UnsupportedPassthrough",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateTrafficAnnotations(map[string]string{tc.key: "v"}, nil, tc.known...)
			if tc.wantErrMatch == "" {
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error matching %q, got nil", tc.wantErrMatch)
			}
			if !strings.Contains(err.Error(), tc.wantErrMatch) {
				t.Fatalf("err = %v, want substring %q", err, tc.wantErrMatch)
			}
		})
	}
}

func TestValidateTrafficAnnotations_CrossRules(t *testing.T) {
	consistentHash := v1beta1.LoadBalancingTypeConsistentHash
	tests := []struct {
		name         string
		annotations  map[string]string
		traffic      *v1beta1.TrafficSpec
		wantOK       bool
		wantContains string
		wantWarning  string
	}{
		{
			name: "retry-attempts > 0 requires retry-on",
			annotations: map[string]string{
				constants.RetryAttemptsAnnotation: "3",
			},
			wantContains: "MissingRetryOn",
		},
		{
			name: "retry-attempts=0 doesn't require retry-on",
			annotations: map[string]string{
				constants.RetryAttemptsAnnotation: "0",
			},
			wantOK: true,
		},
		{
			name: "retry-attempts + retry-on is ok",
			annotations: map[string]string{
				constants.RetryAttemptsAnnotation: "3",
				constants.RetryOnAnnotation:       "5xx",
			},
			wantOK: true,
		},
		{
			name: "sticky + per-endpoint cap > 1 warns",
			annotations: map[string]string{
				constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation: "10",
			},
			traffic: &v1beta1.TrafficSpec{
				Algorithm: &consistentHash,
				ConsistentHash: &v1beta1.ConsistentHashSpec{
					Type:    v1beta1.HashTypeHeader,
					Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
				},
			},
			wantOK:      true,
			wantWarning: "defeats sticky routing",
		},
		{
			name: "sticky + per-endpoint cap = 1 no warning",
			annotations: map[string]string{
				constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation: "1",
			},
			traffic: &v1beta1.TrafficSpec{
				Algorithm: &consistentHash,
				ConsistentHash: &v1beta1.ConsistentHashSpec{
					Type:    v1beta1.HashTypeHeader,
					Headers: []v1beta1.HashHeader{{Name: "X-Session-ID"}},
				},
			},
			wantOK: true,
		},
		{
			name: "rollout timeout < 1 minute warns",
			annotations: map[string]string{
				constants.RolloutReadyTimeoutAnnotation: "30s",
			},
			wantOK:      true,
			wantWarning: "RolloutTimeoutTooShort",
		},
		{
			name: "rollout timeout > 1 minute no warning",
			annotations: map[string]string{
				constants.RolloutReadyTimeoutAnnotation: "10m",
			},
			wantOK: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			warnings, err := ValidateTrafficAnnotations(tc.annotations, tc.traffic)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("expected ok, got error: %v", err)
				}
				if tc.wantWarning != "" {
					if len(warnings) == 0 {
						t.Fatalf("expected warning containing %q, got none", tc.wantWarning)
					}
					found := false
					for _, w := range warnings {
						if strings.Contains(w, tc.wantWarning) {
							found = true
							break
						}
					}
					if !found {
						t.Fatalf("expected warning containing %q in %v", tc.wantWarning, warnings)
					}
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantContains)
			}
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantContains, err)
			}
		})
	}
}

func TestLevenshtein(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"abc", "abc", 0},
		{"abc", "", 3},
		{"", "abc", 3},
		{"abc", "abx", 1},  // substitution
		{"abc", "ab", 1},   // deletion
		{"abc", "abcd", 1}, // insertion
		{"kitten", "sitting", 3},
	}
	for _, tc := range tests {
		t.Run(tc.a+"_"+tc.b, func(t *testing.T) {
			got := levenshtein(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("levenshtein(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
