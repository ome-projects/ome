// Traffic provider-capability validation.
//
// Maps the typed spec.traffic core onto the capability tokens the
// active Gateway-implementation translator declares
// (Translator.SupportedTrafficFields) and enforces the admission-time
// half of the capability contract. The reconciler-side half —
// surfacing dropped fields as a status condition — lives in the
// traffic reconciler package and reuses RequiredTrafficCapabilities so
// the two checks cannot drift.
package validation

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// RequiredTrafficCapabilities returns the capability tokens
// (pkg/constants TrafficCapability*) the given typed spec.traffic
// block requires from a translator. Empty when t is nil or declares
// nothing. Output order follows field order in TrafficSpec so it is
// deterministic.
func RequiredTrafficCapabilities(t *v1beta1.TrafficSpec) []string {
	if t == nil {
		return nil
	}
	var out []string
	if t.Algorithm != nil {
		out = append(out, constants.TrafficCapabilityAlgorithm)
	}
	if c := t.ConsistentHash; c != nil {
		if rule, ok := hashTypeRules[c.Type]; ok {
			out = append(out, rule.capability)
			if rule.reads == hashKeyHeaders && len(c.Headers) > 1 {
				out = append(out, constants.TrafficCapabilityHashMultipleHeaders)
			}
		}
	}
	if e := t.EndpointOverride; e != nil {
		if rule, ok := endpointOverrideRules[e.Type]; ok {
			out = append(out, rule.capability)
		}
	}
	return out
}

// ValidateTrafficCapabilities enforces the admission-time capability
// gate for typed spec.traffic fields. supportedCapabilities is the
// active translator's declared capability set; an empty set means the
// provider is unknown to the webhook (test wiring, or the noop
// translator) and the provider-specific gate is disabled — the
// controller surfaces dropped fields through the
// BackendPolicyUnsupportedFields condition instead.
//
// Two rules:
//
//  1. endpointOverride.type=Metadata is rejected unless a translator
//     declares the capability, regardless of provider knowledge: the
//     value is reserved, no translator emits anything for it, so
//     admitting it would report intent that can never take effect.
//  2. Multi-header consistent hashing is rejected when the capability
//     set is known and lacks the token: a translator without it hashes
//     on a subset of the declared headers, silently changing session
//     affinity — a partial application worse than an outright drop, so
//     it fails fast at admission instead of degrading behind a
//     condition.
//
// Fields a translator drops wholesale (e.g. endpointOverride on a
// DestinationRule backend) stay admissible and surface through the
// condition path, matching the API's documented contract.
func ValidateTrafficCapabilities(t *v1beta1.TrafficSpec, supportedCapabilities []string) error {
	if t == nil {
		return nil
	}
	supported := make(map[string]struct{}, len(supportedCapabilities))
	for _, c := range supportedCapabilities {
		supported[c] = struct{}{}
	}

	if t.EndpointOverride != nil && t.EndpointOverride.Type == v1beta1.EndpointOverrideTypeMetadata {
		if _, ok := supported[constants.TrafficCapabilityEndpointOverrideMetadata]; !ok {
			return fmt.Errorf(
				"spec.traffic.endpointOverride.type=Metadata is reserved and not implemented by the active translator (ReservedEndpointOverrideType)")
		}
	}

	if len(supportedCapabilities) == 0 {
		return nil
	}
	if t.ConsistentHash != nil && t.ConsistentHash.Type == v1beta1.HashTypeHeader && len(t.ConsistentHash.Headers) > 1 {
		if _, ok := supported[constants.TrafficCapabilityHashMultipleHeaders]; !ok {
			return fmt.Errorf(
				"spec.traffic.consistentHash declares %d headers but the active translator hashes on a single header only; declare exactly one header (UnsupportedMultiHeaderHash)",
				len(t.ConsistentHash.Headers))
		}
	}
	return nil
}
