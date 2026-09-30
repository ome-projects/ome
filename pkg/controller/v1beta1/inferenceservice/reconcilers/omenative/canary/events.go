package canary

// Event reasons recorded on the InferenceService during a canary rollout, so the
// step-by-step progression is visible via `kubectl describe isvc` / events.
const (
	// EventReasonCanaryStepAdvanced is recorded when the canary advances to the
	// next step.
	EventReasonCanaryStepAdvanced = "CanaryStepAdvanced"
	// EventReasonCanaryCompleted is recorded when the canary reaches Stable.
	EventReasonCanaryCompleted = "CanaryCompleted"
	// EventReasonRolloutResumed is recorded when ome.io/rollout-resume clears a
	// terminal phase and the ladder re-enters at step 0.
	EventReasonRolloutResumed = "RolloutResumed"
	// EventReasonRolloutResumeRejected is recorded when an ome.io/rollout-resume
	// cannot be honored: a stale hash, no pinned run, or a value that
	// addresses no canary of the InferenceService.
	EventReasonRolloutResumeRejected = "RolloutResumeRejected"
	// EventReasonCanaryRollbackIgnored is recorded when a rollback request
	// finds no canary in flight and is removed rather than left to fire on
	// the next canary's first pass.
	EventReasonCanaryRollbackIgnored = "CanaryRollbackIgnored"
	// EventReasonCanaryRunLost is recorded while a live canary has no pinned
	// rollout run and holds in place until the run layer reopens or adopts it.
	EventReasonCanaryRunLost = "CanaryRunLost"
	// EventReasonCanaryStableRevisionMissing is recorded when a rollback finds
	// no retained ControllerRevision for the stable revision and parks Failed
	// instead of reporting a revert that cannot happen.
	EventReasonCanaryStableRevisionMissing = "CanaryStableRevisionMissing"
)
