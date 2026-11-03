package types

import (
	"fmt"
	"strconv"
	"strings"
)

// RolloutHoldGate names the layer that most recently denied a
// per-Instance Update for a Component. Values mirror
// v1beta1.RolloutHoldGate byte-for-byte; the adapter converts by simple
// string cast, workload code never imports the CRD type.
type RolloutHoldGate string

const (
	RolloutHoldGateRatio      RolloutHoldGate = "Ratio"
	RolloutHoldGateSequential RolloutHoldGate = "Sequential"
	RolloutHoldGateBudget     RolloutHoldGate = "Budget"
	RolloutHoldGateRetryBlock RolloutHoldGate = "RetryBlock"
	RolloutHoldGateHeld       RolloutHoldGate = "Held"
	RolloutHoldGatePairing    RolloutHoldGate = "Pairing"
)

// DrainHolds collects the drain-time holds the update ops observe in one
// pass. A held drain is forward progress withheld rather than an admission
// denial, so executeUpdatePass records the first one even when another
// Instance progressed.
type DrainHolds struct {
	first *RolloutHold
}

// Observe keeps the first hold seen this pass.
func (h *DrainHolds) Observe(hold RolloutHold) {
	if h == nil || h.first != nil {
		return
	}
	kept := hold
	h.first = &kept
}

// First returns the hold kept this pass, or nil.
func (h *DrainHolds) First() *RolloutHold {
	if h == nil {
		return nil
	}
	return h.first
}

// RolloutHold is the workload-side mirror of the most recent per-Instance
// Update denial observed for a Component this pass — the transient fact
// executeUpdatePass discovers via RecordRolloutHold, before the adapter's
// status writer decides whether it changes the persisted Since. No
// timestamp: persistence (including the churn-safe Since anchor) is the
// v1beta1 status writer's concern, not the dispatcher's.
type RolloutHold struct {
	// Gate names the layer that produced Reason.
	Gate RolloutHoldGate
	// Reason is the operator-facing denial string from the gate that
	// produced it.
	Reason string
	// Target is the ControllerRevision name the held Update was aimed at.
	Target string
}

// holdGateTokens are the rollout hold gates as the waits a parked attempt
// names on Operation.Waiting. The hold authorities never write one.
var holdGateTokens = map[string]struct{}{
	string(RolloutHoldGateRatio):      {},
	string(RolloutHoldGateSequential): {},
	string(RolloutHoldGateBudget):     {},
	string(RolloutHoldGateRetryBlock): {},
	string(RolloutHoldGateHeld):       {},
	string(RolloutHoldGatePairing):    {},
}

// ParkedWaitingReason reports whether token names a rollout hold gate.
func ParkedWaitingReason(token string) bool {
	_, ok := holdGateTokens[token]
	return ok
}

// OperationParked reports whether op is an Update attempt parked after
// its disposition: its deadline or a wedged pod ended it while its pod
// set was still alive, so the row keeps it on the parked step with the
// wait the next attempt stands behind. No pass drives a parked attempt;
// the update trigger re-opens the row as a fresh start once that wait
// ends, and the row's phase is whatever its pod set earns.
func OperationParked(op *InstanceOperation) bool {
	return op != nil && op.Type == InstanceOperationUpdate && op.Step == UpdateStepParked
}

// InstancesNotServingClause names, for a hold's reason, the Instances
// already on revision that do not serve it: "; Instance 3 on revision
// <rev> not serving", "; Instances 0, 2 on revision <rev> not serving".
// Empty when there are none. The update pass and the status writer both
// word the hold they stand behind through it, so an operator reads one
// account whichever of them reports the hold.
func InstancesNotServingClause(indices []int32, revision string) string {
	if len(indices) == 0 {
		return ""
	}
	noun, list := InstanceNoun(indices)
	return fmt.Sprintf("; %s %s on revision %s not serving", noun, list, revision)
}

// InstanceNoun is the noun and the comma-joined list a clause names a
// non-empty set of Instance indices with: "Instance" and "3", or
// "Instances" and "0, 2".
func InstanceNoun(indices []int32) (noun, list string) {
	names := make([]string, 0, len(indices))
	for _, idx := range indices {
		names = append(names, strconv.Itoa(int(idx)))
	}
	noun = "Instance"
	if len(indices) > 1 {
		noun = "Instances"
	}
	return noun, strings.Join(names, ", ")
}
