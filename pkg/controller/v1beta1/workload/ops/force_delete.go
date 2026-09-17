package ops

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// forceDeleteEvidence classifies one Terminating pod against the
// stuck-teardown force-delete predicate. Exactly three values are
// actionable; everything else leaves the pod alone.
type forceDeleteEvidence int

const (
	// evidenceNotConfigured: no ForceDeletePolicy — the escalation does
	// not exist.
	evidenceNotConfigured forceDeleteEvidence = iota
	// evidenceWithinGrace: the pod is inside its own graceful-deletion
	// window (plus the configured slack).
	evidenceWithinGrace
	// evidenceForeignFinalizers: the pod is pinned by finalizers.
	// Report-only; force-delete could not remove the object anyway.
	evidenceForeignFinalizers
	// evidenceUnscheduled: the pod never landed on a node — there is no
	// node whose death could be proven.
	evidenceUnscheduled
	// evidenceNodeReadError: the live Node read failed (non-NotFound).
	// Fail-safe: not actionable this pass.
	evidenceNodeReadError
	// evidenceNodeHealthy: the node has no current unreachable evidence.
	evidenceNodeHealthy
	// evidenceNodeNotDeadLongEnough: unreachable taint / NotReady
	// condition present but younger than the threshold (or of unprovable
	// age).
	evidenceNodeNotDeadLongEnough
	// evidenceNodeGone: the Node object is NotFound. Actionable.
	evidenceNodeGone
	// evidenceNodeUnreachableTaint: node.kubernetes.io/unreachable with
	// TimeAdded at least the threshold old. Actionable.
	evidenceNodeUnreachableTaint
	// evidenceNodeNotReady: NodeReady False/Unknown with
	// LastTransitionTime at least the threshold old. Actionable.
	evidenceNodeNotReady
)

// actionable reports whether the evidence justifies a force-delete.
func (e forceDeleteEvidence) actionable() bool {
	switch e {
	case evidenceNodeGone, evidenceNodeUnreachableTaint, evidenceNodeNotReady:
		return true
	}
	return false
}

// String names the evidence branch for events and logs.
func (e forceDeleteEvidence) String() string {
	switch e {
	case evidenceNotConfigured:
		return "not-configured"
	case evidenceWithinGrace:
		return "within-grace"
	case evidenceForeignFinalizers:
		return "foreign-finalizers"
	case evidenceUnscheduled:
		return "unscheduled"
	case evidenceNodeReadError:
		return "node-read-error"
	case evidenceNodeHealthy:
		return "node-healthy"
	case evidenceNodeNotDeadLongEnough:
		return "node-not-dead-long-enough"
	case evidenceNodeGone:
		return "node-gone"
	case evidenceNodeUnreachableTaint:
		return "node-unreachable-taint"
	case evidenceNodeNotReady:
		return "node-not-ready"
	}
	return "unknown"
}

// evidenceResult is the classification plus the supporting detail the
// escalation reports in its event and audit entry.
type evidenceResult struct {
	kind     forceDeleteEvidence
	nodeName string
	// requeueAt is the next time-based evidence boundary or live recheck.
	// Zero means progress depends on a resource event.
	requeueAt time.Time
	// overdue is how far past the pod's own DeletionTimestamp now is.
	overdue time.Duration
	// nodeReadErr carries the non-NotFound Node read failure when kind
	// is evidenceNodeReadError, for the caller to log.
	nodeReadErr error
}

// stuckTerminatingEvidence is the PURE classification behind the
// force-delete escalation. Safety contract, branch by branch:
//
//	NotConfigured        — nil policy: the escalation does not exist; no
//	                       node is ever read. Acting would be acting on
//	                       an in-code default, which this feature bans.
//	WithinGrace          — now <= DeletionTimestamp+OverdueSlack. The
//	                       pod's DeletionTimestamp ALREADY includes its
//	                       own terminationGracePeriodSeconds (the k8s
//	                       contract: deletionTimestamp = request time +
//	                       grace), so a 10-minute-drain pod gets its 10
//	                       minutes and a 30s pod gets 30s — that is why
//	                       the check is per-pod. Acting earlier would
//	                       race a healthy graceful shutdown.
//	ForeignFinalizers    — force-delete cannot remove a finalizer-pinned
//	                       object, and stripping another controller's
//	                       finalizer is off-limits. Report-only.
//	Unscheduled          — no NodeName: there is no node whose death
//	                       could prove the container isn't running.
//	NodeReadError        — the live read failed; without evidence the
//	                       fail-safe is to do nothing this pass.
//	NodeHealthy          — Ready=True, evaluated FIRST and vetoing every
//	                       node-death branch below, including a lingering
//	                       unreachable taint: the kubelet writes
//	                       NodeStatus itself, so a dead node cannot post
//	                       Ready=True — a current Ready=True always
//	                       outranks stale taint state, and the veto can
//	                       only shrink the fire surface, never grow it.
//	                       A Ready node means the kubelet is merely slow;
//	                       force-deleting risks a double-running
//	                       container. Never actionable.
//	NodeNotDeadLongEnough— taint/NotReady present but younger than the
//	                       threshold (or age unprovable): a blip, not a
//	                       death. Wait.
//	NodeGone             — Node object deleted: no kubelet exists to run
//	                       the container or acknowledge the termination.
//	                       Safe to remove the API object.
//	NodeUnreachableTaint — the node-lifecycle controller has marked the
//	                       node unreachable for >= threshold (age from
//	                       the taint's own TimeAdded — no observation
//	                       state to persist, restart-safe).
//	NodeNotReady         — NodeReady False/Unknown for >= threshold (age
//	                       from the condition's own LastTransitionTime).
//
// Reads the Node through the LIVE reader (never the informer cache) so
// a stale cache can neither hold a recovered node "NotReady" nor miss a
// fresh recovery.
func stuckTerminatingEvidence(ctx context.Context, reader client.Reader, pod *corev1.Pod, policy *workload.ForceDeletePolicy, now time.Time) evidenceResult {
	if policy == nil {
		return evidenceResult{kind: evidenceNotConfigured}
	}
	if pod.DeletionTimestamp == nil {
		// Not Terminating — nothing to escalate. Defensive: callers only
		// pass Terminating pods.
		return evidenceResult{kind: evidenceWithinGrace}
	}
	overdueAt := pod.DeletionTimestamp.Add(policy.OverdueSlack)
	if !now.After(overdueAt) {
		// The policy is strictly "past" the deadline. One nanosecond is the
		// smallest representable wake after equality, not a polling cadence.
		return evidenceResult{kind: evidenceWithinGrace, requeueAt: overdueAt.Add(time.Nanosecond)}
	}
	overdue := now.Sub(pod.DeletionTimestamp.Time)
	if len(pod.Finalizers) > 0 {
		return evidenceResult{kind: evidenceForeignFinalizers, nodeName: pod.Spec.NodeName, overdue: overdue}
	}
	if pod.Spec.NodeName == "" {
		return evidenceResult{kind: evidenceUnscheduled, overdue: overdue}
	}

	node := &corev1.Node{}
	if err := reader.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return evidenceResult{kind: evidenceNodeGone, nodeName: pod.Spec.NodeName, overdue: overdue}
		}
		return evidenceResult{kind: evidenceNodeReadError, nodeName: pod.Spec.NodeName, overdue: overdue, nodeReadErr: err}
	}

	// Ready=True vetoes everything below, including a lingering
	// unreachable taint: the kubelet writes NodeStatus itself, so a dead
	// node cannot post Ready=True — a current Ready=True always outranks
	// stale taint state. The veto can only shrink the fire surface,
	// never grow it.
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
			return evidenceResult{
				kind:      evidenceNodeHealthy,
				nodeName:  node.Name,
				requeueAt: now.Add(policy.NodeUnreachableThreshold),
				overdue:   overdue,
			}
		}
	}

	// Evidence present but younger than the threshold — remembered so
	// the fall-through distinguishes "dying, wait" from "healthy".
	young := false
	var nextEvidenceAt time.Time
	for _, taint := range node.Spec.Taints {
		if taint.Key != corev1.TaintNodeUnreachable {
			continue
		}
		// Age comes from the taint's own TimeAdded; a nil TimeAdded
		// cannot prove the threshold elapsed.
		if taint.TimeAdded != nil {
			actionableAt := taint.TimeAdded.Add(policy.NodeUnreachableThreshold)
			if !now.Before(actionableAt) {
				return evidenceResult{kind: evidenceNodeUnreachableTaint, nodeName: node.Name, overdue: overdue}
			}
			if nextEvidenceAt.IsZero() || actionableAt.Before(nextEvidenceAt) {
				nextEvidenceAt = actionableAt
			}
		}
		young = true
	}
	for _, cond := range node.Status.Conditions {
		if cond.Type != corev1.NodeReady {
			continue
		}
		if cond.Status == corev1.ConditionFalse || cond.Status == corev1.ConditionUnknown {
			actionableAt := cond.LastTransitionTime.Add(policy.NodeUnreachableThreshold)
			if !now.Before(actionableAt) {
				return evidenceResult{kind: evidenceNodeNotReady, nodeName: node.Name, overdue: overdue}
			}
			if nextEvidenceAt.IsZero() || actionableAt.Before(nextEvidenceAt) {
				nextEvidenceAt = actionableAt
			}
			young = true
		}
	}
	if young {
		res := evidenceResult{kind: evidenceNodeNotDeadLongEnough, nodeName: node.Name, overdue: overdue}
		if !nextEvidenceAt.IsZero() {
			res.requeueAt = nextEvidenceAt
		}
		return res
	}
	return evidenceResult{
		kind:      evidenceNodeHealthy,
		nodeName:  node.Name,
		requeueAt: now.Add(policy.NodeUnreachableThreshold),
		overdue:   overdue,
	}
}

// escalateStuckTerminating evaluates one Terminating pod against the
// force-delete predicate and, when the evidence is actionable,
// force-deletes it (grace 0, UID-preconditioned), then events and
// ledgers the action. Foreign-finalizer pods get a once-per-pod-UID
// Warning instead. All other classifications are silent no-ops.
//
// Callers with periodic polling may treat errors as advisory. A caller
// without polling must return the error so controller-runtime backoff
// supplies another observation opportunity.
func escalateStuckTerminating(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32) error {
	_, err := escalateStuckTerminatingWithDeadline(ctx, deps, input, pod, idx)
	return err
}

// escalateStuckTerminatingPods applies the escalation to every pod in
// pods that is already Terminating. A teardown phase that waits for the
// pods it deleted to disappear waits forever when the node running them
// is dead, because only a live kubelet clears the pod object; this gives
// such a phase the same escalation the scale-down path has. Pods whose
// evidence is not actionable are left untouched.
//
// Call it BEFORE any expectations gate: the unobserved delete is exactly
// what those expectations are blocked on, so an escalation behind the
// gate would be waiting on the wedge it exists to clear.
//
// For callers on a periodic requeue, the next pass re-evaluates every
// pod, so no policy-boundary deadline has to be threaded back.
func escalateStuckTerminatingPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pods []*corev1.Pod, idx int32) error {
	if input.ForceDelete == nil {
		return nil
	}
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			continue
		}
		if err := escalateStuckTerminating(ctx, deps, input, pod, idx); err != nil {
			return err
		}
	}
	return nil
}

// escalateStuckTerminatingWithDeadline also reports the next exact policy
// boundary. Callers without a periodic poll use it to preserve time-driven
// force-delete progress without inventing a default cadence.
func escalateStuckTerminatingWithDeadline(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32) (time.Time, error) {
	if input.ForceDelete == nil {
		return time.Time{}, nil
	}
	res := stuckTerminatingEvidence(ctx, deps.Reader(), pod, input.ForceDelete, input.Now())
	if res.kind == evidenceNodeReadError {
		return time.Time{}, fmt.Errorf("force-delete evidence for pod %s: node %s read: %w", pod.Name, res.nodeName, res.nodeReadErr)
	}
	if res.kind == evidenceForeignFinalizers {
		return time.Time{}, reportFinalizerBlockedPod(ctx, deps, input, pod, idx, res)
	}
	if !res.kind.actionable() {
		return res.requeueAt, nil
	}

	// UID precondition: OMENative reuses stable pod names, so a
	// same-name successor must be un-hittable.
	uid := pod.UID
	if err := deps.Client.Delete(ctx, pod,
		client.GracePeriodSeconds(0),
		client.Preconditions{UID: &uid},
	); err != nil {
		// NotFound: the wedge object is already gone. Conflict: the UID
		// precondition missed — a successor exists; never touch it.
		// Both are success.
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("force-delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	// Event + ledger AFTER the successful delete: a crash in this window
	// loses only the audit record, never repeats the action — the object
	// is gone (grace 0, no finalizers, UID-preconditioned), so the next
	// pass no longer sees the pod.
	recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonPodForceDeleted,
		"OMENative %s: force-deleted stuck-Terminating pod %s on node %s (evidence=%s, %s past the pod's own deletion deadline)",
		instanceKey(input.Key.Component, idx), pod.Name, pod.Spec.NodeName, res.kind, res.overdue.Round(time.Second))
	if err := recordForceDeleteLedgerEntry(ctx, deps, input, pod, idx, audit.OutcomeForceDeleteUnreachable); err != nil {
		return time.Time{}, fmt.Errorf("record force-delete ledger entry (pod=%s): %w", pod.Name, err)
	}
	return time.Time{}, nil
}

// reportFinalizerBlockedPod emits the once-per-pod-UID Warning for an
// overdue Terminating pod pinned by foreign finalizers, using a
// terminal ledger row keyed by the pod UID as the dedup marker.
//
// Dedup argument: the pod UID keys the row (a given UID has exactly
// one DeletionTimestamp, ever), the row is persisted in the audit
// ConfigMap so it survives controller restarts, and UpsertEntry
// replaces rather than appends on the same key — so a wedged pod
// produces at most one Warning for its lifetime. Repeats are possible
// only while the ledger write itself keeps failing (abnormal and
// bounded to one attempt per reconciliation pass) or after ring-buffer
// eviction of the marker (requires 200 newer terminal entries while
// the same pod stays wedged).
func reportFinalizerBlockedPod(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32, res evidenceResult) error {
	owner := ledgerOwnerObject(input)
	ledger, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), owner)
	if err != nil {
		return fmt.Errorf("load audit ledger (pod=%s): %w", pod.Name, err)
	}
	// Match on the report outcome specifically: an unreachable-action row
	// for the same UID means the force-delete already fired and the pod
	// somehow persisted (e.g. a finalizer added between evidence read and
	// delete) — a genuinely new condition that must still warn.
	for _, e := range ledger.Entries {
		if e.RequestUUID == string(pod.UID) && e.Reason == audit.ReasonForceDelete &&
			e.Outcome == audit.OutcomeForceDeleteFinalizerReport {
			return nil
		}
	}
	recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonPodDeleteBlockedByFinalizer,
		"OMENative %s: pod %s is %s past its own deletion deadline but pinned by finalizers %v; OME never strips another controller's finalizer — the finalizer owner must resolve it",
		instanceKey(input.Key.Component, idx), pod.Name, res.overdue.Round(time.Second), pod.Finalizers)
	return recordForceDeleteLedgerEntry(ctx, deps, input, pod, idx, audit.OutcomeForceDeleteFinalizerReport)
}

// recordForceDeleteLedgerEntry persists the terminal ForceDelete audit
// row for pod. RequestUUID carries the pod UID so the row is naturally
// idempotent per pod object (UpsertEntry replaces on the same key).
func recordForceDeleteLedgerEntry(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32, outcome string) error {
	owner := ledgerOwnerObject(input)
	ledger, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), owner)
	if err != nil {
		return fmt.Errorf("load audit ledger: %w", err)
	}
	now := input.Now().UTC().Format(time.RFC3339)
	ledger.UpsertEntry(audit.Entry{
		RequestUUID:    string(pod.UID),
		Component:      string(input.Key.Component),
		SourceInstance: idx,
		Phase:          audit.PhaseCompleted,
		Reason:         audit.ReasonForceDelete,
		Outcome:        outcome,
		FromNode:       pod.Spec.NodeName,
		StartedAt:      now,
		CompletedAt:    now,
	})
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, owner, ledgerOwnerGVK(input), ledger); err != nil {
		return fmt.Errorf("persist audit ledger: %w", err)
	}
	return nil
}
