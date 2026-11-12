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
	// terminal phase: the steps continue at the step a stop interrupted, or
	// start over after a rollback. The message names the step and how far
	// each member's roll had come.
	EventReasonRolloutResumed = "RolloutResumed"
	// EventReasonRolloutResumeRejected is recorded when an ome.io/rollout-resume
	// cannot be honored: a stale hash, no pinned run, a stopped canary's revision no
	// member of the unit targets any more, or a value that addresses no
	// canary of the InferenceService.
	EventReasonRolloutResumeRejected = "RolloutResumeRejected"
	// EventReasonCanaryRollbackIgnored is recorded when a rollback request is
	// removed without a revert: it finds no canary in flight (none armed, or
	// a cutover that has landed), or a canary already parked because its
	// stable revision is not retained.
	EventReasonCanaryRollbackIgnored = "CanaryRollbackIgnored"
	// EventReasonCanaryRunLost is recorded while a live canary has no pinned
	// rollout run and holds in place until the run layer reopens or adopts it.
	EventReasonCanaryRunLost = "CanaryRunLost"
	// EventReasonCanaryTargetReverted is recorded when a canary's record ends
	// because the spec returned to the revision the unit started from: with
	// no run open, a push reverted before any Instance moved leaves nothing
	// to roll; under a run that records every member at its stable revision,
	// the Instances still off it roll back with no canary step and the
	// traffic follows them. Either way the unit reads Stable with no record.
	EventReasonCanaryTargetReverted = "CanaryTargetReverted"
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
	// EventReasonCanaryRollHeld is recorded on the pass a unit stops Failed,
	// whatever the reason: the roll is held where it stands, so no further
	// Instance of any member moves to the stopped revision and the traffic
	// split stays as the stop left it, until a rollback, a resume or a new
	// revision. The message names how far each member's roll had come.
	EventReasonCanaryRollHeld = "CanaryRollHeld"
	// EventReasonCanaryAnalysisRolledBack is recorded on the pass whose metric
	// sample decides a rollback: the failing samples reached the step's limit,
	// a sample could not be read under onInconclusive Rollback, or no sample
	// could be read for the ready timeout under RollbackOnStall. The message
	// names the step, the cause and every failing or unread check of the
	// deciding sample with its member, value and threshold, so the reason is
	// on the object's events and not only in status.
	EventReasonCanaryAnalysisRolledBack = "CanaryAnalysisRolledBack"
	// EventReasonCanaryCutoverTimeout is recorded when a unit past its last
	// step waits longer than the ready timeout for the Instances it released
	// to serve their new revision, and stops Failed with reason
	// CapacityTimeout: it names, per member, the Instances that do not serve
	// it yet and says that the traffic stays where the cutover put it.
	EventReasonCanaryCutoverTimeout = "CanaryCutoverTimeout"
	// EventReasonCanaryForceIgnored is recorded when a forced promote is
	// removed without effect: the rollout is paused or frozen, or the canary
	// is parked Failed, rolling back or held rolled back.
	EventReasonCanaryForceIgnored = "CanaryForceIgnored"
	// EventReasonCanaryTrafficOffStable is recorded when a member's share of
	// the traffic leaves its stable revision because no pod of it serves:
	// the share goes to the revision that serves until a stable pod serves
	// again. The message names each member, its stable revision and the
	// revision that carries the share.
	EventReasonCanaryTrafficOffStable = "CanaryTrafficOffStable"
	// EventReasonCanaryTrafficBackOnStable is recorded when a stable pod
	// serves again and the share that left the stable revision for want of
	// one is back on it.
	EventReasonCanaryTrafficBackOnStable = "CanaryTrafficBackOnStable"
	// EventReasonCanaryStableSideShort is recorded on the pass that first
	// reads a member's stable side short under the split a step serves:
	// fewer Ready Instances on its stable revision than the step keeps
	// there, at least one, so the canary revision carries a larger share of
	// the Ready capacity than the split's weight. The message gives each
	// short member's counts and shares. It is not recorded again while the
	// dip stands under the same step's split.
	EventReasonCanaryStableSideShort = "CanaryStableSideShort"
)
