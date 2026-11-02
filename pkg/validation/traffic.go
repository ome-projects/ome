// Traffic-management validation.
//
// Webhook-level validation for the TrafficSpec typed core (LB
// algorithm, hash key, endpoint override). The hash-type and
// override-type tables below also feed the capability list in
// traffic_capabilities.go. Annotation parsing and rollout validation
// live in sibling files.
package validation

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// hashKeySource is a field of spec.traffic.consistentHash that can carry
// the hash key.
type hashKeySource string

const (
	hashKeyHeaders hashKeySource = "headers"
	hashKeyCookie  hashKeySource = "cookie"
)

func (k hashKeySource) isSet(c *v1beta1.ConsistentHashSpec) bool {
	if k == hashKeyCookie {
		return c.Cookie != nil
	}
	return len(c.Headers) > 0
}

// hashTypeRule is one row of the consistent-hash table: the key field a
// hash type reads (none for SourceIP), the key fields it refuses in the
// order they are reported, and the translator capability it needs.
type hashTypeRule struct {
	reads      hashKeySource
	refuses    []hashKeySource
	capability string
}

var hashTypeRules = map[v1beta1.HashType]hashTypeRule{
	v1beta1.HashTypeHeader:   {reads: hashKeyHeaders, refuses: []hashKeySource{hashKeyCookie}, capability: constants.TrafficCapabilityHashHeader},
	v1beta1.HashTypeCookie:   {reads: hashKeyCookie, refuses: []hashKeySource{hashKeyHeaders}, capability: constants.TrafficCapabilityHashCookie},
	v1beta1.HashTypeSourceIP: {refuses: []hashKeySource{hashKeyCookie, hashKeyHeaders}, capability: constants.TrafficCapabilityHashSourceIP},
}

// endpointOverrideRule is one row of the endpoint-override table: whether
// the override type reads the headers list, and the translator
// capability it needs. Metadata is reserved and reads no field.
type endpointOverrideRule struct {
	readsHeaders bool
	capability   string
}

var endpointOverrideRules = map[v1beta1.EndpointOverrideType]endpointOverrideRule{
	v1beta1.EndpointOverrideTypeHeader:   {readsHeaders: true, capability: constants.TrafficCapabilityEndpointOverrideHeader},
	v1beta1.EndpointOverrideTypeMetadata: {capability: constants.TrafficCapabilityEndpointOverrideMetadata},
}

// ValidateTrafficSpec runs the typed-core validation rules for
// InferenceService.spec.traffic. Returns nil when traffic is unset or
// internally consistent.
func ValidateTrafficSpec(t *v1beta1.TrafficSpec) error {
	if t == nil {
		return nil
	}

	if err := validateConsistentHashCoupling(t); err != nil {
		return err
	}
	if err := validateConsistentHashShape(t.ConsistentHash); err != nil {
		return err
	}
	if err := validateEndpointOverride(t.EndpointOverride); err != nil {
		return err
	}

	return nil
}

// validateConsistentHashCoupling enforces that ConsistentHash sub-block
// is present iff Algorithm == ConsistentHash.
func validateConsistentHashCoupling(t *v1beta1.TrafficSpec) error {
	isConsistentHash := t.Algorithm != nil && *t.Algorithm == v1beta1.LoadBalancingTypeConsistentHash
	switch {
	case isConsistentHash && t.ConsistentHash == nil:
		return fmt.Errorf("spec.traffic.consistentHash is required when spec.traffic.algorithm=ConsistentHash (MissingConsistentHashSpec)")
	case !isConsistentHash && t.ConsistentHash != nil:
		return fmt.Errorf("spec.traffic.consistentHash is only valid when spec.traffic.algorithm=ConsistentHash (UnexpectedConsistentHashSpec)")
	}
	return nil
}

// validateConsistentHashShape enforces the exactly-one-source rule for
// the ConsistentHash sub-block: the key field the type reads is set,
// the other key fields stay unset, and every listed header has a name.
func validateConsistentHashShape(c *v1beta1.ConsistentHashSpec) error {
	if c == nil {
		return nil
	}
	rule, ok := hashTypeRules[c.Type]
	if !ok {
		// CRD enum tag should reject this before reaching the webhook;
		// keep a defensive check in case CRD validation is bypassed.
		return fmt.Errorf("spec.traffic.consistentHash.type=%q is not a valid HashType", string(c.Type))
	}
	if err := missingHashKey(c, rule.reads); err != nil {
		return err
	}
	for _, key := range rule.refuses {
		if key.isSet(c) {
			return fmt.Errorf("spec.traffic.consistentHash.%s must not be set when type=%s (MultipleHashKeys)", key, c.Type)
		}
	}
	for i, h := range c.Headers {
		if h.Name == "" {
			return fmt.Errorf("spec.traffic.consistentHash.headers[%d].name is empty (MissingHashKey)", i)
		}
	}
	return nil
}

// missingHashKey refuses a spec whose key field is absent: an empty
// header list, no cookie, or a cookie without a name.
func missingHashKey(c *v1beta1.ConsistentHashSpec, key hashKeySource) error {
	switch key {
	case hashKeyHeaders:
		if len(c.Headers) == 0 {
			return fmt.Errorf("spec.traffic.consistentHash.headers must be non-empty when type=%s (MissingHashKey)", c.Type)
		}
	case hashKeyCookie:
		if c.Cookie == nil {
			return fmt.Errorf("spec.traffic.consistentHash.cookie is required when type=%s (MissingHashKey)", c.Type)
		}
		if c.Cookie.Name == "" {
			return fmt.Errorf("spec.traffic.consistentHash.cookie.name is empty (MissingHashKey)")
		}
	}
	return nil
}

// validateEndpointOverride enforces the headers requirement of the
// override types that read the headers list.
func validateEndpointOverride(e *v1beta1.EndpointOverrideSpec) error {
	if e == nil {
		return nil
	}
	rule, ok := endpointOverrideRules[e.Type]
	if !ok {
		return fmt.Errorf("spec.traffic.endpointOverride.type=%q is not a valid EndpointOverrideType", string(e.Type))
	}
	if !rule.readsHeaders {
		return nil
	}
	if len(e.Headers) == 0 {
		return fmt.Errorf("spec.traffic.endpointOverride.headers must be non-empty when type=%s (MissingEndpointOverrideKey)", e.Type)
	}
	for i, h := range e.Headers {
		if h.Name == "" {
			return fmt.Errorf("spec.traffic.endpointOverride.headers[%d].name is empty (MissingEndpointOverrideKey)", i)
		}
	}
	return nil
}
