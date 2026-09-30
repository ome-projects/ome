package types

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
