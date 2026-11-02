package coordination

// Event reasons emitted on the InferenceService for coordination state
// machine transitions. Operators see these in `kubectl describe isvc`
// — they are the primary surface for understanding what the
// coordination layer is doing.
const (
	// EventReasonGroupSurging fires when a group enters Surging.
	EventReasonGroupSurging = "CoordinationGroupSurging"

	// EventReasonGroupShifting fires when a group enters Shifting and
	// the Status.Components.<c>.Traffic[] writer has updated the
	// per-Component traffic weights.
	EventReasonGroupShifting = "CoordinationGroupShifting"

	// EventReasonGroupCompleted fires when a group returns to Idle
	// after a rollout cycle.
	EventReasonGroupCompleted = "CoordinationGroupCompleted"

	// EventReasonGroupFailed fires when a group enters Failed (Ready
	// timeout, persistent reconcile error, etc.).
	EventReasonGroupFailed = "CoordinationGroupFailed"

	// EventReasonRatioSkewRejected fires on the pass RatioBalanced pacing
	// starts holding a Component's next start to avoid drifting the
	// cross-Component ratio; the message names the move held by the
	// Component's strategy (a surge start, a recreate or an in-place
	// restart). The group status carries the refusal (SurgeHeldByRatio)
	// while it holds, and the passes it holds across record nothing more.
	EventReasonRatioSkewRejected = "RatioSkewRejected"

	// EventReasonRatioGateBypassed fires when the OMENative ops
	// dispatcher admits an in-place start (InPlaceIfPossible /
	// InPlaceOnly) past the RatioBalanced serving-capacity band without
	// running it. The band's "-1" projection treats every drain as a
	// permanent capacity loss, but an in-place drain is paired 1:1 with
	// a same-pod return (mark-not-ready -> patch image -> mark-ready)
	// so the net capacity change is ~0 and the projection would
	// over-block the smaller Component of a ratio pair indefinitely.
	// The new-revision lockstep half of the gate still runs for the
	// start, and CheckUnavailabilityGate still governs concurrent
	// disruption, so RatioBalanced safety is not lost -- operators just
	// see this event explaining why the band did not fire.
	EventReasonRatioGateBypassed = "RatioGateBypassed"

	// EventReasonRatioStepOutsideTolerance fires when the RatioBalanced
	// serving band admits a drain that leaves the serving ratio outside the
	// tolerance, because no single step of that side can stay inside it at
	// the group's shape (8 prefill beside 2 decode under tolerance 20: one
	// decode pod out reads 8:1, 100 points off). The band admits one such
	// step at a time from a fleet serving its reference in band, instead of
	// holding the roll for good; the message carries the counts and the
	// tolerance, so the operator can widen the tolerance or resize the
	// group to a shape one step keeps inside it.
	EventReasonRatioStepOutsideTolerance = "RatioStepOutsideTolerance"

	// EventReasonMixedPairingObserved fires on the pass a RollingUpdate
	// group first sees distinct (vN, vM) revision pairs in flight; the
	// group status carries the picture (MixedPairing) while it holds, and
	// the passes it holds across record nothing more. Informational: it
	// helps operators audit back-compat assertions.
	EventReasonMixedPairingObserved = "MixedPairingObserved"

	// EventReasonSequentialAwaitingNext fires when a Sequential group
	// enters the soak window between Components, waiting for the
	// operator to bump the next Component's spec.
	EventReasonSequentialAwaitingNext = "SequentialAwaitingNext"

	// EventReasonSequentialNextSpecBumpDeferred fires when an
	// operator bumps Component i+1's spec while Component i is still
	// mid-rollout. The bump is observed but not acted on yet.
	EventReasonSequentialNextSpecBumpDeferred = "SequentialNextSpecBumpDeferred"

	// EventReasonConsiderCoordinationGroup is an informational hint
	// emitted on multi-Component ISVCs without any declared group,
	// suggesting that operators consider declaring a coordination
	// group.
	EventReasonConsiderCoordinationGroup = "ConsiderCoordinationGroup"
)
