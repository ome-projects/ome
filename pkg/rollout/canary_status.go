package rollout

import "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"

// hasPerUnitCanary reports whether any unit has recorded its run on its own
// Component, which is what distinguishes a status the per-unit writer has
// touched from one persisted before per-unit state existed.
func hasPerUnitCanary(s *v1beta1.InferenceServiceStatus) bool {
	for _, cs := range s.Components {
		if cs.Canary != nil {
			return true
		}
	}
	return false
}

// CanaryStatusFor returns the canary run state for the unit the Component
// belongs to, or nil. It reads the unit entrypoint's ComponentStatusSpec and
// falls back to the legacy InferenceServiceStatus.Canary alias, without which a
// status persisted before per-unit state existed would be invisible and the
// reconcile would restart that run from step 0.
//
// The fallback holds only while NO unit has per-unit state, because an absent
// entry has two very different meanings. Before any unit has written, it means
// "this run predates per-unit state" and the alias is the only copy. After, it
// means "this unit has no run yet" — and handing it the alias would let a unit
// inherit another unit's canary and rollback target and drive its own ladder
// against them.
func CanaryStatusFor(s *v1beta1.InferenceServiceStatus, unit v1beta1.ComponentType) *v1beta1.CanaryStatus {
	if s == nil {
		return nil
	}
	if cs, ok := s.Components[CanaryUnit(unit)]; ok && cs.Canary != nil {
		return cs.Canary
	}
	if hasPerUnitCanary(s) {
		return nil
	}
	return s.Canary
}

// SetCanaryStatusFor writes the run state onto the unit's entrypoint
// Component. Passing nil clears it. The legacy alias is derived from per-unit
// state by SyncLegacyCanaryAlias rather than written here.
func SetCanaryStatusFor(s *v1beta1.InferenceServiceStatus, unit v1beta1.ComponentType, cs *v1beta1.CanaryStatus) {
	if s == nil {
		return
	}
	u := CanaryUnit(unit)
	if s.Components == nil {
		s.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	c := s.Components[u]
	c.Canary = cs
	s.Components[u] = c
}

// SyncLegacyCanaryAlias republishes the entrypoint unit's run on the legacy
// InferenceServiceStatus.Canary field that existing readers still consume. The
// router is the entrypoint when it has a run, else the engine unit.
//
// The alias is DERIVED from per-unit state, recomputed once per reconcile,
// rather than written alongside it. Status round-trips through the apiserver,
// so a copy published once is a separate object from the run the executor goes
// on to mutate: it would silently freeze at the step it was published on while
// the run advanced underneath it. Deriving it keeps a single source of truth.
//
// While NO unit has per-unit state the alias is left untouched — it may hold a
// run persisted before per-unit state existed, and that copy is the only one.
func SyncLegacyCanaryAlias(s *v1beta1.InferenceServiceStatus) {
	if s == nil || !hasPerUnitCanary(s) {
		return
	}
	if cs, ok := s.Components[v1beta1.RouterComponent]; ok && cs.Canary != nil {
		s.Canary = cs.Canary
		return
	}
	if cs, ok := s.Components[v1beta1.EngineComponent]; ok && cs.Canary != nil {
		s.Canary = cs.Canary
	}
}
