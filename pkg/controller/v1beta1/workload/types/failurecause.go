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
// This is the one evidence set that charges a revision's RetryBlock
// ladder toward Held — for the single-attempt disposition (workload
// root) and the gang-surge abandon (workload/ops) alike. Every other
// failure reason paces the next attempt without blaming the revision.
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
// CreateContainerError. A repeated process exit or a runtime start
// rejection can equally be a broken binary (revision fault) or a dead
// GPU / broken driver / node-local runtime damage (placement fault).
// For those, a wrong suppression (holding the revision) is an
// UNBOUNDED loop on dead hardware — the revision is fine, the block
// never lifts, and nothing relocates the pod — while a wrong migration
// is RELOCATION-bounded by the operator's autoMigrate.maxAttempts.
// Operation-specific recovery may retry only when the persisted
// relocation evidence authorizes it; otherwise it leaves the Instance
// Failed for operator action. An instance that reaches Ready prunes its
// AutoRecover records and resets the budget. Ambiguous reasons therefore
// route to bounded relocation, never to revision blame.
var workloadCausedWaitingReasons = map[string]struct{}{
	"ImagePullBackOff":            {},
	"ErrImagePull":                {},
	"InvalidImageName":            {},
	"CreateContainerConfigError":  {},
	RejectionReasonInvalidPodSpec: {},
}

// IsWorkloadCausedReason reports whether reason — a kubelet waiting
// reason, or the Reason an escalator recorded on InstanceTermination —
// is in the workload-caused set. Only such evidence may charge a
// revision's retry ladder.
func IsWorkloadCausedReason(reason string) bool {
	_, ok := workloadCausedWaitingReasons[reason]
	return ok
}
