package render

import (
	"encoding/json"
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/digest"
)

const (
	portableDigestPrefix = "pv1:"
	resolvedDigestPrefix = "rv1:"
	digestHexLength      = 12
)

// PortableDigest hashes the policy spec in a canonical form: API defaults
// applied first, then deterministic JSON. Defaulting comes first because the
// apiserver stores a defaulted object (enforcement: Default filled by its
// schema default) while a raw file in a repository omits the field; hashing
// raw bytes would make byte-identical files mismatch their own stored
// objects. Every consumer of the digest (member controllers, the
// control-plane preflight, CI tooling) links this function, so the values
// are comparable with no trust chain.
func PortableDigest(spec *v1beta1.AutoscalerPolicySpec) (string, error) {
	canonical := canonicalizeSpec(spec)
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("encode policy spec for digest: %w", err)
	}
	return portableDigestPrefix + digest.Short(encoded, digestHexLength), nil
}

// canonicalizeSpec returns a defaulted deep copy of the spec. Keep this in
// lockstep with the CRD schema defaults: a default added to the API without
// a matching line here splits the digest between raw files and stored
// objects.
func canonicalizeSpec(spec *v1beta1.AutoscalerPolicySpec) *v1beta1.AutoscalerPolicySpec {
	canonical := spec.DeepCopy()
	if canonical.Enforcement == "" {
		canonical.Enforcement = v1beta1.PolicyEnforcementDefault
	}
	return canonical
}

// resolvedDigestFor hashes one component's rendered output together with the
// inputs that vary per cluster: the effective bounds and the bound provider
// endpoint. It differs between member clusters by design; it answers "did my
// policy edit land on this cluster?", while cross-cluster equality is the
// portable digest's job.
func resolvedDigestFor(rendered *v1beta1.ComponentAutoscaler, rctx Context, serverAddresses []string) (string, error) {
	payload := struct {
		Autoscaler      *v1beta1.ComponentAutoscaler `json:"autoscaler"`
		MinReplicas     int32                        `json:"minReplicas"`
		MaxReplicas     int32                        `json:"maxReplicas"`
		ServerAddresses []string                     `json:"serverAddresses,omitempty"`
	}{rendered, rctx.MinReplicas, rctx.MaxReplicas, serverAddresses}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode rendered autoscaler for digest: %w", err)
	}
	return resolvedDigestPrefix + digest.Short(encoded, digestHexLength), nil
}
