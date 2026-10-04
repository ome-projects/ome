package types

// workloadCausedWaitingReasons is the set of failure reasons —
// kubelet container state.Waiting reasons plus the apiserver rejection
// reasons the create/patch classifier stamps — that are
// DETERMINISTICALLY scoped to the workload revision: the failure travels
// with the pod template, so relocating the pod to another node (or
// simply retrying) reproduces it identically. These
// are Kubernetes API semantics, identical on every cluster; they are
// declared as package constants, not config (a knob here could only be
// set wrongly: removing an entry re-enables retrying a fault that
// cannot self-recover, adding an ambiguous entry pins workloads to
// dead hardware).
//
// This is the evidence set that blames the revision outright — for the
// single-attempt disposition (workload root) and the gang-surge abandon
// (workload/ops) alike. It does not decide whether a failed attempt
// counts on the revision's retry ladder (every attempt does, see
// FailureCause); it decides that no node would do better, so with no
// ladder configured such a failure Holds the revision at once.
//
// Contract table (reason → what it means → why relocation cannot help):
//
//	ImagePullBackOff           kubelet exhausted its pull retries for
//	                           the image reference. The registry serves
//	                           the same reference to every node — a new
//	                           node re-pulls the same missing/broken
//	                           image and parks in the same state.
//	ErrImagePull               the pull itself failed (manifest absent,
//	                           tag deleted, access denied for the ref).
//	                           The reference is part of the revision;
//	                           every node resolves it the same way.
//	InvalidImageName           the image reference fails validation
//	                           before any pull is attempted. No node
//	                           can parse an unparsable reference.
//	CreateContainerConfigError the container's config (missing
//	                           ConfigMap/Secret key, invalid env
//	                           projection) was rejected at container
//	                           create. The config travels with the
//	                           revision, not the node.
//	InvalidPodSpec             the apiserver rejected the pod object
//	                           itself (422 Invalid) — a field the pod
//	                           template sets is unacceptable. Not a
//	                           kubelet reason: it is stamped by the
//	                           create/patch rejection classifier (see
//	                           RejectionReasonInvalidPodSpec). No node
//	                           ever sees the pod, so placement is
//	                           irrelevant; only a corrected revision is
//	                           admissible.
//
// EXCLUDED — ambiguous scope (could be the revision OR the
// device/node): CrashLoopBackOff, RunContainerError,
// CreateContainerError, and the readiness limbo a pod reaches when it
// runs every container yet never reports ContainersReady
// (ReasonContainersNotReady). A repeated process exit or a runtime start
// rejection can equally be a broken binary (revision fault) or a dead
// GPU / broken driver / node-local runtime damage (placement fault).
// Holding the revision on the first such failure would wedge a sound
// revision on dead hardware — the block never lifts and nothing
// relocates the pod — so these reasons route to relocation first,
// bounded by the operator's autoMigrate.maxAttempts. An attempt that
// ends without a relocation directive is still a failed attempt at the
// revision and counts on its retry ladder like any other, so a revision
// that crashes on every start ends Held at updateRetry.maxAttempts
// rather than retried forever; a wave the relocation directive claimed
// is not counted a second time. An instance that reaches Ready prunes
// its AutoRecover records and its block. A revision held for a failure
// that was the node's after all is released by the operator through the
// release annotation; a revision that fails on several nodes in a row
// was not the node's fault.
var workloadCausedWaitingReasons = map[string]struct{}{
	"ImagePullBackOff":            {},
	"ErrImagePull":                {},
	"InvalidImageName":            {},
	"CreateContainerConfigError":  {},
	RejectionReasonInvalidPodSpec: {},
}

// IsWorkloadCausedReason reports whether reason — a kubelet waiting
// reason, or the Reason an escalator recorded on InstanceTermination —
// is in the workload-caused set.
func IsWorkloadCausedReason(reason string) bool {
	_, ok := workloadCausedWaitingReasons[reason]
	return ok
}

// RepairWaitsOnWorkload reports whether the row is a Restart parked at
// Failed on a workload-caused failure. The kubelet retries such a cause
// in place and a fresh pod set would wedge on it identically, so the
// retry ladder owes the park no re-arm: the row stays Failed with its
// reason until the configuration or image is fixed, and resumes on its
// own once the kubelet starts the pod.
func RepairWaitsOnWorkload(s *InstanceStatus) bool {
	return s != nil && s.Phase == InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == InstanceOperationRestart &&
		s.LastFailure != nil && IsWorkloadCausedReason(s.LastFailure.Reason)
}

// environmentCausedReasons are the failure reasons under which the
// cluster refused to run the attempt at all: the scheduler found no
// placement, the gang's deterministic PodGroup name belongs to another
// controller, or the apiserver could not reach its admission webhook.
// None says anything about the pod template, and no corrected revision
// changes any of them, so such a wave never reaches a revision's retry
// ladder.
var environmentCausedReasons = map[string]struct{}{
	WaitingReasonUnschedulable:          {},
	PodGroupOwnershipConflictReason:     {},
	RejectionReasonAdmissionUnavailable: {},
}

// IsEnvironmentCausedReason reports whether reason is one the
// environment, not the attempt, is responsible for.
func IsEnvironmentCausedReason(reason string) bool {
	_, ok := environmentCausedReasons[reason]
	return ok
}

// FailureCause is what a failed attempt's evidence says about who is at
// fault, and so how the wave reaches the target revision's retry ladder
// (ApplyUpdateFailureToRetryBlock).
type FailureCause int

const (
	// CauseUnattributed: the failure could equally be the revision or the
	// node it ran on — a crash loop, a runtime start rejection, readiness
	// never reached, a bare elapsed deadline. It is a failed attempt at
	// the revision and counts on the ladder like any other; with no
	// ladder configured it is left unrecorded.
	CauseUnattributed FailureCause = iota
	// CauseWorkload: the failure travels with the pod template
	// (IsWorkloadCausedReason), so no node would do better. It counts on
	// the ladder, and with no ladder configured it Holds at once.
	CauseWorkload
	// CauseEnvironment: the cluster refused to run the attempt
	// (IsEnvironmentCausedReason). It says nothing about the revision and
	// is never counted.
	CauseEnvironment
)

// FailureCauseOf classifies a recorded failure reason — a kubelet waiting
// reason, or the Reason an escalator stamped on LastFailure.
func FailureCauseOf(reason string) FailureCause {
	switch {
	case IsWorkloadCausedReason(reason):
		return CauseWorkload
	case IsEnvironmentCausedReason(reason):
		return CauseEnvironment
	}
	return CauseUnattributed
}
