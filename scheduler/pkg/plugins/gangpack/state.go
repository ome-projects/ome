package gangpack

import "k8s.io/kube-scheduler/framework"

// pinStateKey identifies the per-scheduling-cycle pin that PreFilter records and
// Filter enforces. It is internal CycleState plumbing — it never leaves the
// process — so the name is a neutral local identifier, not a label convention.
const pinStateKey framework.StateKey = "gangpack.pin"

// pinState is the domain a gang is committed to for this scheduling cycle, plus
// the topology label key needed to test a candidate node against it.
type pinState struct {
	domain      string
	topologyKey string
	gang        gangInfo
	commitment  uint64
	// soleFit marks a domain no other could stand in for: the only one that fit
	// the gang, or the one its bound members impose. A veto there is not re-planned.
	soleFit bool
}

// Clone satisfies framework.StateData. The struct is value-only, so a shallow
// copy is a full copy.
func (s *pinState) Clone() framework.StateData {
	c := *s
	return &c
}

// writePin records the pinned domain for this cycle (called by PreFilter).
func writePin(state framework.CycleState, domain string, gang gangInfo, commitment ...uint64) {
	var id uint64
	if len(commitment) > 0 {
		id = commitment[0]
	}
	state.Write(pinStateKey, &pinState{domain: domain, topologyKey: gang.topologyKey, gang: gang, commitment: id})
}

// markSoleFit flags the cycle's pin as the only domain that fit the gang, so a
// veto there parks the member instead of activating a re-plan.
func markSoleFit(state framework.CycleState) {
	if pin := readPin(state); pin != nil {
		pin.soleFit = true
	}
}

// readPin returns the pin recorded for this cycle, or nil when none was recorded
// (the pod is not a pinned gang member — Filter then imposes no domain constraint).
func readPin(state framework.CycleState) *pinState {
	if state == nil {
		return nil
	}
	v, err := state.Read(pinStateKey)
	if err != nil {
		return nil
	}
	s, ok := v.(*pinState)
	if !ok {
		return nil
	}
	return s
}
