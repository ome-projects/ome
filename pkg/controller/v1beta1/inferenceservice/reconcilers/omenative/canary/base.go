package canary

import (
	"k8s.io/apimachinery/pkg/api/equality"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// unitState is what the executor projects for one Component: the canary
// record and the phase and traffic derived from it.
type unitState struct {
	canary  *v1beta1.CanaryStatus
	phase   v1beta1.RolloutPhase
	traffic []v1beta1.ComponentTrafficTarget
}

func unitStateOf(cs v1beta1.ComponentStatusSpec) unitState {
	return unitState{canary: cs.Canary.DeepCopy(), phase: cs.RolloutPhase, traffic: copyTraffic(cs.Traffic)}
}

func copyTraffic(in []v1beta1.ComponentTrafficTarget) []v1beta1.ComponentTrafficTarget {
	if in == nil {
		return nil
	}
	out := make([]v1beta1.ComponentTrafficTarget, 0, len(in))
	for i := range in {
		out = append(out, *in[i].DeepCopy())
	}
	return out
}

func (u unitState) equal(o unitState) bool {
	return u.phase == o.phase &&
		equality.Semantic.DeepEqual(u.canary, o.canary) &&
		equality.Semantic.DeepEqual(u.traffic, o.traffic)
}

// Base is the executor-owned state a reconcile pass read before it decided
// anything. The status flush re-bases the pass's in-memory status onto the
// live object, so a pass that read a stale copy would otherwise write its
// decisions over a newer record; comparing the live record with the base is
// what tells the two apart. A nil Base guards nothing.
type Base struct {
	// owned is every Component a canary group governs. The executor writes
	// each one's traffic, and the primary's record and phase, so a secondary
	// is guarded like the primary although it carries no record of its own.
	owned map[v1beta1.ComponentType]struct{}
	units map[v1beta1.ComponentType]unitState
	alias *v1beta1.CanaryStatus
	stale bool
}

// NewBase snapshots the executor-owned state of an InferenceService as read.
func NewBase(isvc *v1beta1.InferenceService) *Base {
	b := &Base{owned: rollout.CanaryOwnedComponents(isvc)}
	if isvc == nil {
		b.snapshot(nil)
		return b
	}
	b.snapshot(&isvc.Status)
	return b
}

func (b *Base) snapshot(s *v1beta1.InferenceServiceStatus) {
	b.units = map[v1beta1.ComponentType]unitState{}
	b.alias = nil
	if s == nil {
		return
	}
	b.alias = s.Canary.DeepCopy()
	for c, cs := range s.Components {
		b.units[c] = unitStateOf(cs)
	}
}

// ownedUnits lists the Components whose projection the executor owns: every
// member of a canary group, and any Component that carries a canary record on
// either side.
func (b *Base) ownedUnits(o *Base) map[v1beta1.ComponentType]struct{} {
	units := map[v1beta1.ComponentType]struct{}{}
	for c := range b.owned {
		units[c] = struct{}{}
	}
	for _, side := range []*Base{b, o} {
		for c, u := range side.units {
			if u.canary != nil {
				units[c] = struct{}{}
			}
		}
	}
	return units
}

func (b *Base) equal(o *Base) bool {
	if !equality.Semantic.DeepEqual(b.alias, o.alias) {
		return false
	}
	for c := range b.ownedUnits(o) {
		if !b.units[c].equal(o.units[c]) {
			return false
		}
	}
	return true
}

// PreserveFresh keeps the live executor state in dst when it no longer
// matches the base: the pass decided from a stale copy of the record, and a
// decision made from a stale record is not written; the next pass makes it
// again from the current one. It reports whether it kept the live state.
func (b *Base) PreserveFresh(dst, live *v1beta1.InferenceServiceStatus) bool {
	if b == nil || dst == nil || live == nil {
		return false
	}
	current := &Base{}
	current.snapshot(live)
	if b.equal(current) {
		return false
	}
	if dst.Components == nil {
		dst.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	for c := range b.ownedUnits(current) {
		entry := dst.Components[c]
		u := current.units[c]
		entry.Canary = u.canary.DeepCopy()
		entry.RolloutPhase = u.phase
		entry.Traffic = copyTraffic(u.traffic)
		dst.Components[c] = entry
	}
	dst.Canary = current.alias.DeepCopy()
	b.stale = true
	return true
}

// Advance records the state a successful write persisted, the base of every
// later decision in the same pass.
func (b *Base) Advance(written *v1beta1.InferenceServiceStatus) {
	if b == nil {
		return
	}
	b.snapshot(written)
}

// Stale reports whether a flush in this pass kept the live state over the
// pass's decisions; the pass requeues to decide again from that state.
func (b *Base) Stale() bool {
	return b != nil && b.stale
}
