// Package rolloutpolicy is the pure, K8s-client-free core of the RolloutPolicy
// feature: portable digests and ref→group composition. The run opener, the
// placement inflater, and the RolloutPolicy status controller all link this
// package, so no two of them can disagree about a digest or a composed group.
package rolloutpolicy

import (
	"encoding/json"
	"fmt"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/digest"
)

const (
	portableDigestPrefix = "rp1:"
	digestHexLength      = 12
)

// PortableDigest hashes a policy spec in canonical form (deterministic JSON of
// a deep copy). Equal across clusters iff the specs match — the fleet-drift
// comparator. The RolloutPolicy schema declares no field defaults today; if
// one is ever added, it must be applied here before hashing (the apiserver
// stores defaulted objects while raw GitOps files omit the field, and hashing
// raw shapes would make byte-identical files digest-mismatch their own stored
// objects — the lesson the AutoscalerPolicy digest already codifies).
func PortableDigest(spec *v1beta1.RolloutPolicySpec) (string, error) {
	encoded, err := json.Marshal(spec.DeepCopy())
	if err != nil {
		return "", fmt.Errorf("encode rollout policy spec for digest: %w", err)
	}
	return portableDigestPrefix + ShortHash(encoded), nil
}

// ProgressionDigest hashes one progression body by embedding it in a synthetic
// policy spec, so an inline progression and a policy carrying the identical
// body produce the SAME digest. That equality is the preview contract: during
// ref-first migration, shadowedPolicyRef.wouldPinDigest matching the inline
// group's observed digest proves the cutover is a no-op.
func ProgressionDigest(g *v1beta1.RolloutGroup) (string, error) {
	if g == nil || !hasInlineProgression(g) {
		return "", nil
	}
	synthetic := &v1beta1.RolloutPolicySpec{
		Canary:        g.Canary,
		BlueGreen:     g.BlueGreen,
		RollingUpdate: g.RollingUpdate,
	}
	return PortableDigest(synthetic)
}

// CombinedDigest folds per-group digests into the single value the repin
// annotation CAS-compares against. Order-sensitive by design: group order is
// plan content.
func CombinedDigest(digests []string) string {
	return portableDigestPrefix + ShortHash([]byte(strings.Join(digests, "|")))
}

// ShortHash is the digest core, exported so run identity and combined-digest
// callers hash with the exact same truncation the portable digest uses.
func ShortHash(data []byte) string {
	return digest.Short(data, digestHexLength)
}
