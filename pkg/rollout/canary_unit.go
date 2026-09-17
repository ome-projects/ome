package rollout

import "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"

// CanaryUnit is the smallest set of Components that advance through a canary
// ladder together, named by its entrypoint Component. The router is its own
// unit; engine and decoder are one unit, because a P/D pair speaks a pairing
// protocol to each other and rolling them apart can leave a new-protocol
// prefill with no pairable peer. A unit is what one canary run owns.
func CanaryUnit(c v1beta1.ComponentType) v1beta1.ComponentType {
	if c == v1beta1.DecoderComponent {
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
