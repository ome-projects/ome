package rollout

import (
	"sort"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

// canaryTargetIDPrefix versions the fingerprint a canary record carries for
// the targets it was written for.
const canaryTargetIDPrefix = "ct1:"

// CanaryUnit is the smallest set of Components that advance through a canary
// ladder together, named by its entrypoint Component. The router is its own
// unit; every role that pairs (engine, decoder and encoder) joins one unit
// named by the engine, because they speak a pairing protocol to each other
// and rolling them apart can leave a new-protocol prefill with no pairable
// peer. A unit is what one canary run owns.
func CanaryUnit(c v1beta1.ComponentType) v1beta1.ComponentType {
	if role, ok := v1beta1.RoleOf(c); ok && role.Pairs {
		return v1beta1.EngineComponent
	}
	return c
}

// CanaryUnitsOf returns the units a group touches, in a stable order.
func CanaryUnitsOf(g *v1beta1.RolloutGroup) []v1beta1.ComponentType {
	if g == nil {
		return nil
	}
	seen := map[v1beta1.ComponentType]bool{}
	var out []v1beta1.ComponentType
	for _, c := range g.Components {
		u := CanaryUnit(c)
		if !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	return out
}

// CanaryTargetID fingerprints the revisions a canary group's members are
// held to, one "component=revision" part per distinct member in a fixed
// order. The record a run starts carries the fingerprint of the run's
// targets; the same fingerprint over the members' observed targets says
// whether those targets still stand. A member with no revision leaves the fingerprint
// empty, so a caller never reads a partial one as a change.
func CanaryTargetID(members []v1beta1.ComponentType, revisionOf func(v1beta1.ComponentType) string) string {
	parts := make([]string, 0, len(members))
	seen := make(map[v1beta1.ComponentType]struct{}, len(members))
	for _, comp := range members {
		if _, duplicate := seen[comp]; duplicate {
			continue
		}
		seen[comp] = struct{}{}
		rev := revisionOf(comp)
		if rev == "" {
			return ""
		}
		parts = append(parts, string(comp)+"="+rev)
	}
	if len(parts) == 0 {
		return ""
	}
	sort.Strings(parts)
	return canaryTargetIDPrefix + rolloutpolicy.ShortHash([]byte(strings.Join(parts, ";")))
}
