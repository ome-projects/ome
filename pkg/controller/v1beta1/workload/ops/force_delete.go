package ops

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

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
	res := evidence.StuckTerminating(ctx, deps.Reader(), pod, input.ForceDelete, input.Now())
	if res.Kind == evidence.NodeReadError {
		return time.Time{}, fmt.Errorf("force-delete evidence for pod %s: node %s read: %w", pod.Name, res.NodeName, res.NodeReadErr)
	}
	if res.Kind == evidence.ForeignFinalizers {
		return time.Time{}, reportFinalizerBlockedPod(ctx, deps, input, pod, idx, res)
	}
	if !res.Kind.Actionable() {
		return res.RequeueAt, nil
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
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonPodForceDeleted,
		"OMENative %s: force-deleted stuck-Terminating pod %s on node %s (evidence=%s, %s past the pod's own deletion deadline)",
		workload.InstanceKey(input.Key.Component, idx), pod.Name, pod.Spec.NodeName, res.Kind, res.Overdue.Round(time.Second))
	if err := recordForceDeleteLedgerEntry(ctx, deps, input, pod, idx, audit.OutcomeForceDeleteUnreachable); err != nil {
		return time.Time{}, fmt.Errorf("record force-delete ledger entry (pod=%s): %w", pod.Name, err)
	}
	return time.Time{}, nil
}

// forceDeleteOnNodeDeath removes a pod that is NOT on its way out yet
// but whose node has stopped reporting it, once the configured policy's
// node-death evidence proves no kubelet is left to run its containers.
// Reports whether the pod object is gone.
//
// The overdue-slack branch of the Terminating sweep does not apply: a
// pod nobody has asked to delete has no graceful-shutdown window to
// respect, so the node evidence alone decides. Everything else is the
// same escalation — grace zero, UID-preconditioned so a same-name
// successor is un-hittable, event and audit row after the delete lands.
//
// A pod already carrying a DeletionTimestamp belongs to the
// stuck-Terminating sweep, which owns the graceful-shutdown window, the
// finalizer report and its dedup; callers route it there instead.
//
// Also reports the next exact policy boundary, the same contract
// escalateStuckTerminatingWithDeadline has: the evidence turns
// actionable on a clock, and a node that has stopped reporting emits no
// event to wake anyone on, so a caller without a poll must carry the
// boundary into its requeue.
func forceDeleteOnNodeDeath(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32) (bool, time.Time, error) {
	if input.ForceDelete == nil || pod == nil || pod.DeletionTimestamp != nil {
		return false, time.Time{}, nil
	}
	res := evidence.NodeDeath(ctx, deps.Reader(), pod, input.ForceDelete, input.Now(), 0)
	if res.Kind == evidence.NodeReadError {
		return false, time.Time{}, fmt.Errorf("node-death evidence for pod %s: node %s read: %w", pod.Name, res.NodeName, res.NodeReadErr)
	}
	if !res.Kind.Actionable() {
		return false, res.RequeueAt, nil
	}

	uid := pod.UID
	if err := deps.Client.Delete(ctx, pod,
		client.GracePeriodSeconds(0),
		client.Preconditions{UID: &uid},
	); err != nil {
		// NotFound: the object is already gone. Conflict: the UID
		// precondition missed — a successor holds the name; never touch it.
		if apierrors.IsNotFound(err) {
			return true, time.Time{}, nil
		}
		if apierrors.IsConflict(err) {
			return false, time.Time{}, nil
		}
		return false, time.Time{}, fmt.Errorf("force-delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonPodForceDeleted,
		"OMENative %s: force-deleted pod %s held in phase Unknown on node %s (evidence=%s)",
		workload.InstanceKey(input.Key.Component, idx), pod.Name, pod.Spec.NodeName, res.Kind)
	if err := recordForceDeleteLedgerEntry(ctx, deps, input, pod, idx, audit.OutcomeForceDeleteUnreachable); err != nil {
		return true, time.Time{}, fmt.Errorf("record force-delete ledger entry (pod=%s): %w", pod.Name, err)
	}
	return true, time.Time{}, nil
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
func reportFinalizerBlockedPod(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32, res evidence.TerminatingResult) error {
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
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonPodDeleteBlockedByFinalizer,
		"OMENative %s: pod %s is %s past its own deletion deadline but pinned by finalizers %v; OME never strips another controller's finalizer — the finalizer owner must resolve it",
		workload.InstanceKey(input.Key.Component, idx), pod.Name, res.Overdue.Round(time.Second), pod.Finalizers)
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
