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
	// EventReasonCanaryRollbackIgnored is recorded when a rollback request is
	// removed without a revert: it finds no canary in flight, or a canary
	// already parked because its stable revision is not retained.
	EventReasonCanaryRollbackIgnored = "CanaryRollbackIgnored"
	// EventReasonCanaryRunLost is recorded while a live canary has no pinned
	// rollout run and holds in place until the run layer reopens or adopts it.
	EventReasonCanaryRunLost = "CanaryRunLost"
	// EventReasonCanaryStableRevisionMissing is recorded when a rollback finds
	// no retained ControllerRevision for the stable revision and parks Failed
	// instead of reporting a revert that cannot happen.
	EventReasonCanaryStableRevisionMissing = "CanaryStableRevisionMissing"
	// EventReasonCanaryStableRevisionEmpty is recorded when a rollback's
	// target, a member's stable revision, runs no live Instance while the
	// ladder holds a floor for it: a rollback the controller decided parks
	// instead, and one the operator requested proceeds. The event names the
	// revision and the Component.
	EventReasonCanaryStableRevisionEmpty = "CanaryStableRevisionEmpty"
	// EventReasonCanaryPodCrashed is recorded when a canary pod keeps dying
	// after its Instance was serving, a crash loop rather than a single
	// restart, and the ladder parks at its step, naming the pod.
	EventReasonCanaryPodCrashed = "CanaryPodCrashed"
	// EventReasonCanaryStepForced is recorded when ome.io/rollout-promote-force
	// opens a step's gate without its capacity: it names the step, the canary
	// revision and the Ready canary capacity at that moment.
	EventReasonCanaryStepForced = "CanaryStepForced"
	// EventReasonCanaryForceIgnored is recorded when a forced promote is
	// removed without effect: the rollout is paused or frozen, or the canary
	// is parked Failed, rolling back or held rolled back.
	EventReasonCanaryForceIgnored = "CanaryForceIgnored"
)
