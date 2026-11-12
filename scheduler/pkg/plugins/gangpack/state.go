package gangpack

import "k8s.io/kube-scheduler/framework"

// assignmentStateKey identifies the per-scheduling-cycle domain assignment that
// PreFilter records and Filter enforces. It is internal CycleState plumbing — it
// never leaves the process — so the name is a neutral local identifier, not a
// label convention.
const assignmentStateKey framework.StateKey = "gangpack.assignment"

// assignmentState is the domain a gang is committed to for this scheduling
// cycle, plus the topology label key needed to test a candidate node against it.
type assignmentState struct {
	domain      string
	topologyKey string
	gang        gangInfo
	commitment  uint64
	// soleFit marks a domain no other could stand in for: the only one that fit
	// the gang when it was planned, or the one its bound members impose. A member
	// following the assignment inherits it. A veto there is not re-planned.
	soleFit bool
}

// Clone satisfies framework.StateData. The struct is value-only, so a shallow
// copy is a full copy.
func (s *assignmentState) Clone() framework.StateData {
	c := *s
	return &c
}

// writeAssignment records the gang's domain and the id of the commitment that
// holds it for this cycle (called by PreFilter).
func writeAssignment(state framework.CycleState, domain string, gang gangInfo, commitment uint64) {
	state.Write(assignmentStateKey, &assignmentState{domain: domain, topologyKey: gang.topologyKey, gang: gang, commitment: commitment})
}

// markSoleFit flags the cycle's assignment as the only domain that fit the gang,
// so a veto there holds the member instead of activating a re-plan.
func markSoleFit(state framework.CycleState) {
	if assignment := readAssignment(state); assignment != nil {
		assignment.soleFit = true
	}
}

// readAssignment returns the assignment recorded for this cycle, or nil when
// none was recorded (the pod is not an assigned gang member — Filter then
// imposes no domain constraint).
func readAssignment(state framework.CycleState) *assignmentState {
	return cycleStateValue[assignmentState](state, assignmentStateKey)
}

// cycleStateValue reads the value this plugin stored under key for the cycle,
// or nil when there is no state, nothing under the key, or a value of another
// type.
func cycleStateValue[T any](state framework.CycleState, key framework.StateKey) *T {
	if state == nil {
		return nil
	}
	v, err := state.Read(key)
	if err != nil {
		return nil
	}
	s, ok := any(v).(*T)
	if !ok {
		return nil
	}
	return s
}
