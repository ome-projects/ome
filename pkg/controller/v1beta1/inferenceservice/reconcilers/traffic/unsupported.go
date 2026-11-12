package traffic

import (
	"slices"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/validation"
)

// BackendAnnotationKeys is the canonical set of ome.io/* keys that
// flow through a translator: the circuit-breaker, retry and timeout
// groups Resolve reads. The Envoy Gateway translator honors all of
// them; a translator MAY support a subset (e.g. an implementation that
// doesn't model per-endpoint circuit-breaker limits), and
// operator-declared keys in this set that the active translator does
// NOT support are surfaced as UnsupportedField on the InferenceService.
func BackendAnnotationKeys() sets.Set[string] {
	return sets.New(slices.Concat(circuitBreakerKeys, retryKeys, timeoutKeys)...)
}

// allPassthroughPrefixes is the canonical set of ome.io/* pass-through
// prefixes. A pass-through annotation under a prefix NOT in the active
// translator's SupportedPassthroughPrefixes is surfaced as
// UnsupportedField.
func allPassthroughPrefixes() []string {
	return []string{
		constants.PassthroughEnvoyGatewayPrefix,
		constants.PassthroughIstioPrefix,
	}
}

// ComputeUnsupportedAnnotations returns the sorted list of ome.io/*
// traffic annotations declared on the InferenceService that the
// active translator does not honor. Two sources:
//
//  1. A per-key annotation in BackendAnnotationKeys() that the
//     translator's SupportedAnnotations does not include.
//  2. A pass-through annotation under a prefix the translator does
//     not own (e.g. ome.io/dr.* on an Envoy-Gateway cluster).
//
// Operational annotations (rollout-promote, rollout-rollback,
// managed-by-conflict-acked, etc.) are NOT translator-handled and
// are excluded from the check.
//
// Returned slice is sorted for deterministic status messages.
func ComputeUnsupportedAnnotations(annotations map[string]string, translator Translator) []string {
	if len(annotations) == 0 {
		return nil
	}
	support := annotationSupport{
		backend:  BackendAnnotationKeys(),
		honored:  translator.SupportedAnnotations(),
		prefixes: translator.SupportedPassthroughPrefixes(),
	}
	var unsupported []string
	for key := range annotations {
		if support.drops(key) {
			unsupported = append(unsupported, key)
		}
	}
	sort.Strings(unsupported)
	return unsupported
}

// annotationSupport is what the active translator honors: of the backend
// keys, the honored ones, and the pass-through prefixes it owns.
type annotationSupport struct {
	backend  sets.Set[string]
	honored  sets.Set[string]
	prefixes []string
}

// drops reports whether the translator ignores key: a backend key it does
// not honor, or a pass-through under a prefix it does not own. Any other
// annotation is not the translator's to honor.
func (s annotationSupport) drops(key string) bool {
	if s.backend.Has(key) {
		return !s.honored.Has(key)
	}
	return underPrefix(key, allPassthroughPrefixes()) && !underPrefix(key, s.prefixes)
}

// ComputeUnsupportedTrafficFields returns the sorted list of typed
// spec.traffic capability tokens the InferenceService requires that
// the active translator does not declare in SupportedTrafficFields.
// These are fields Translate drops (or partially applies); the status
// writer surfaces them in the BackendPolicyUnsupportedFields condition
// alongside dropped annotations. The required set comes from the same
// validation helper the admission webhook uses, so the two views of
// "what this spec needs" cannot drift.
func ComputeUnsupportedTrafficFields(spec *v1beta1.TrafficSpec, translator Translator) []string {
	required := validation.RequiredTrafficCapabilities(spec)
	if len(required) == 0 {
		return nil
	}
	supported := translator.SupportedTrafficFields()
	var unsupported []string
	for _, capability := range required {
		if !supported.Has(capability) {
			unsupported = append(unsupported, capability)
		}
	}
	sort.Strings(unsupported)
	return unsupported
}

// underPrefix reports whether key starts with one of prefixes.
func underPrefix(key string, prefixes []string) bool {
	return slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(key, p) })
}
