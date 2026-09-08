package gangpack

import (
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// pinStateKey identifies the per-scheduling-cycle pin that PreFilter normally
// records and Filter enforces. A hard-spread gang records it in Reserve instead,
// after the regular filters select a node. It is internal CycleState plumbing —
// it never leaves the process — so the name is a neutral local identifier, not a
// label convention.
const pinStateKey framework.StateKey = "gangpack.pin"

// pinState is the domain a gang is committed to for this scheduling cycle, plus
// the topology label key needed to test a candidate node against it.
type pinState struct {
	domain      string
	topologyKey string
	gang        gangInfo
	commitment  uint64
}

// Clone satisfies framework.StateData. The struct is value-only, so a shallow
// copy is a full copy.
func (s *pinState) Clone() framework.StateData {
	c := *s
	return &c
}

// writePin records the pinned domain for this cycle (called by PreFilter or,
// for a deferred hard-spread plan, Reserve).
func writePin(state framework.CycleState, domain string, gang gangInfo, commitment ...uint64) {
	var id uint64
	if len(commitment) > 0 {
		id = commitment[0]
	}
	state.Write(pinStateKey, &pinState{domain: domain, topologyKey: gang.topologyKey, gang: gang, commitment: id})
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

// deferredPinState is an unplaced gang whose current member has a hard topology
// spread constraint. PreFilter cannot know which nodes the framework's later
// filters will reject, so it exposes every domain that can hold the whole gang.
// Reserve commits the gang to the domain of the node selected from that filtered
// set. The maps are immutable after this state is written.
type deferredPinState struct {
	gang          gangInfo
	need          int
	free          topology.FreeByDomain
	nodesByDomain map[string][]string
}

const deferredPinStateKey framework.StateKey = "gangpack.deferred-pin"

func (s *deferredPinState) Clone() framework.StateData {
	if s == nil {
		return (*deferredPinState)(nil)
	}
	c := &deferredPinState{
		gang:          s.gang,
		need:          s.need,
		free:          make(topology.FreeByDomain, len(s.free)),
		nodesByDomain: make(map[string][]string, len(s.nodesByDomain)),
	}
	for domain, free := range s.free {
		c.free[domain] = free
	}
	for domain, nodes := range s.nodesByDomain {
		c.nodesByDomain[domain] = append([]string(nil), nodes...)
	}
	return c
}

func writeDeferredPin(state framework.CycleState, plan *deferredPinState) {
	state.Write(deferredPinStateKey, plan)
}

func readDeferredPin(state framework.CycleState) *deferredPinState {
	if state == nil {
		return nil
	}
	v, err := state.Read(deferredPinStateKey)
	if err != nil {
		return nil
	}
	plan, ok := v.(*deferredPinState)
	if !ok {
		return nil
	}
	return plan
}
