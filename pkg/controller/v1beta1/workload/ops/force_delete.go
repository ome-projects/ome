package ops

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// escalateStuckTerminating evaluates one Terminating pod against the
// force-delete predicate and, when the node reading is actionable,
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
// node reading is not actionable are left untouched.
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

// SweepStuckTerminatingPods applies the escalation to every Terminating
// pod of the Component, bucketed by Instance index as the pass observed
// them, whether or not an operation still waits on the pod. A drained pod
// that a finished or abandoned operation left behind has no verb pass to
// re-read it, and a node that has stopped reporting raises no event to
// wake one, so the Component pass is the one reader every such pod has.
// Pods are visited in index and name order so the writes land in the same
// order on every pass.
//
// Reports the earliest exact policy boundary among the pods left in place
// (zero when none has one) so a pass with no sooner wake-up can come back
// at it, and the joined errors of the pods whose node could not be
// read or whose delete failed: one pod's error does not shield another's
// from the sweep.
func SweepStuckTerminatingPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, byInstance map[int32][]*corev1.Pod) (time.Time, error) {
	if input.ForceDelete == nil {
		return time.Time{}, nil
	}
	indices := make([]int32, 0, len(byInstance))
	for idx := range byInstance {
		indices = append(indices, idx)
	}
	slices.Sort(indices)
	var (
		earliest time.Time
		errs     []error
	)
	for _, idx := range indices {
		pods := slices.Clone(byInstance[idx])
		slices.SortFunc(pods, func(a, b *corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
		for _, pod := range pods {
			if pod == nil || pod.DeletionTimestamp == nil {
				continue
			}
			at, err := escalateStuckTerminatingWithDeadline(ctx, deps, input, pod, idx)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			earliest = earlierTime(earliest, at)
		}
	}
	return earliest, errors.Join(errs...)
}

// escalateStuckTerminatingWithDeadline also reports the next exact policy
// boundary. Callers without a periodic poll use it to preserve time-driven
// force-delete progress without inventing a default cadence.
func escalateStuckTerminatingWithDeadline(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32) (time.Time, error) {
	if input.ForceDelete == nil || input.Swept.Swept(pod.UID) {
		return time.Time{}, nil
	}
	res := evidence.StuckTerminating(ctx, deps.NodeReaders(), pod, input.ForceDelete, input.Now())
	if res.Kind == evidence.NodeReadError {
		return time.Time{}, fmt.Errorf("force-delete node reading for pod %s: node %s read: %w", pod.Name, res.NodeName, res.NodeReadErr)
	}
	if res.NodeReadErr != nil {
		logf.FromContext(ctx).V(1).Info("force-delete: the node of a pod inside its grace period could not be read, so the pod waits for its deletion deadline plus the slack",
			"pod", pod.Name, "node", res.NodeName, "error", res.NodeReadErr.Error())
	}
	if res.Kind == evidence.ForeignFinalizers {
		return time.Time{}, reportFinalizerBlockedPod(ctx, deps, input, pod, idx, res)
	}
	if !res.Kind.Actionable() {
		return res.RequeueAt, nil
	}

	text := fmt.Sprintf("force-deleted %s (evidence=%s, %s)", forceDeletedPodText(pod, res.GraceSkipped), res.Kind, deletionDeadlineOffset(res.Overdue))
	_, err := forceDeletePod(ctx, deps, input, pod, idx, text)
	return time.Time{}, err
}

// forceDeletePod removes a Terminating pod no kubelet will stop or remove
// (removePodNow) and records the removal the way every force-delete is
// recorded: the swept set of the pass, a Warning carrying text, and the
// audit row. A pod already gone, or whose name a successor holds, needs
// nothing, and removed is false. A crash between the delete and the record
// loses only the record, never repeats the action, since the object is
// gone and the next pass no longer sees the pod.
func forceDeletePod(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32, text string) (removed bool, err error) {
	uid := pod.UID
	removed, err = removePodNow(ctx, deps, pod)
	if err != nil || !removed {
		return false, err
	}
	input.Swept.Record(uid)
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonPodForceDeleted,
		"OMENative %s: %s", workload.InstanceKey(input.Key.Component, idx), text)
	if err := recordForceDelete(ctx, deps, input, pod, idx); err != nil {
		return true, err
	}
	return true, nil
}

// forceDeletedPodText names a force-deleted Terminating pod and its node
// for the event. A pod that went before its deletion deadline plus the
// slack still had time left, so its text says why OME did not wait.
func forceDeletedPodText(pod *corev1.Pod, graceSkipped bool) string {
	if graceSkipped {
		return fmt.Sprintf("Terminating pod %s on node %s without waiting for its deletion deadline plus overdueSlack, because skipGracePeriodOnUnreachableNode is set",
			pod.Name, pod.Spec.NodeName)
	}
	return fmt.Sprintf("stuck-Terminating pod %s on node %s", pod.Name, pod.Spec.NodeName)
}

// deletionDeadlineOffset says how far a force-deleted pod was from its
// own deletion deadline. Under SkipGracePeriodOnUnreachableNode a pod on
// an unreachable node can go before that deadline.
func deletionDeadlineOffset(overdue time.Duration) string {
	if overdue < 0 {
		return fmt.Sprintf("%s before the pod's own deletion deadline", (-overdue).Round(time.Second))
	}
	return fmt.Sprintf("%s past the pod's own deletion deadline", overdue.Round(time.Second))
}

// forceDeleteOnNodeDeath removes a pod that is NOT on its way out yet
// but whose node has stopped reporting it, once NodeDeath lets it go: the
// node is gone, or has been unreachable or NotReady for at least the
// policy's threshold. That rests on how long the node has been unreachable
// or NotReady, not on proof that no kubelet is left to run the pod.
// Reports the class it read: an actionable class means the pod object is
// gone or a same-name successor already holds the name.
//
// The overdue-slack branch of the Terminating sweep does not apply: a
// pod nobody has asked to delete has no graceful-shutdown window to
// respect, so the node reading alone decides. Everything else is the
// same escalation — grace zero, UID-preconditioned so a same-name
// successor is un-hittable, event and audit row after the delete lands.
//
// A pod already carrying a DeletionTimestamp belongs to the
// stuck-Terminating sweep, which owns the graceful-shutdown window, the
// finalizer report and its dedup; callers route it there instead, and
// this arm reports NotConfigured for it as it does with no policy.
//
// Also reports the next exact policy boundary, the same contract
// escalateStuckTerminatingWithDeadline has: the reading turns actionable
// on a clock, and a node that has stopped reporting emits no event to
// wake anyone on, so a caller without a poll must carry the boundary
// into its requeue.
func forceDeleteOnNodeDeath(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32) (evidence.TerminatingClass, time.Time, error) {
	res, err := readNodeDeath(ctx, deps, input, pod)
	if err != nil || !res.Kind.Actionable() {
		return res.Kind, res.RequeueAt, err
	}
	removed, err := removePodNow(ctx, deps, pod)
	if err != nil || !removed {
		return res.Kind, time.Time{}, err
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonPodForceDeleted,
		"OMENative %s: force-deleted pod %s on node %s, which its kubelet stopped reporting (evidence=%s)",
		workload.InstanceKey(input.Key.Component, idx), pod.Name, pod.Spec.NodeName, res.Kind)
	return res.Kind, time.Time{}, recordForceDelete(ctx, deps, input, pod, idx)
}

// forceDeleteNeverRanPodsOnNodeDeath removes every pod in pods that never
// ran a container (PodNeverRanAContainer) once the node under it has read
// unreachable or NotReady for the configured policy's threshold, with the
// grace-zero delete of forceDeleteOnNodeDeath. Such a pod served nothing
// and holds no state, so removing it costs only the create that puts the
// name on a live node, which the caller issues on its next pass; a pod
// that ran keeps its name, since its container may still run on the
// node. The set is visited on every pass of the open repair, so the node
// is read through the informer cache and live only once the cache shows
// the threshold passed (NodeDeathScreened); with no policy no Node is
// read. The rule measures an age, so a pod whose Node object is missing
// is not read here. Reports whether a pod was removed and the earliest
// moment a node still short of the threshold comes of age; one pod's
// error does not stop the others.
func forceDeleteNeverRanPodsOnNodeDeath(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, pods []*corev1.Pod) (bool, time.Time, error) {
	if input.ForceDelete == nil {
		return false, time.Time{}, nil
	}
	removed := false
	var nextAt time.Time
	var errs []error
	for _, pod := range pods {
		if !evidence.PodNeverRanAContainer(pod) {
			continue
		}
		res := evidence.NodeDeathScreened(ctx, deps.NodeReaders(), pod, input.ForceDelete, input.Now())
		if res.Kind == evidence.NodeReadError {
			errs = append(errs, nodeReadFailure(pod, res.NodeName, res.NodeReadErr))
			continue
		}
		if !res.Kind.OlderThanThreshold() {
			nextAt = earlierTime(nextAt, res.RequeueAt)
			continue
		}
		gone, err := removePodNow(ctx, deps, pod)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !gone {
			continue
		}
		removed = true
		workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonPodForceDeleted,
			"OMENative %s: force-deleted pod %s, which never ran a container, because %s (reading=%s); the repair rebuilds it on another node",
			workload.InstanceKey(input.Key.Component, idx), pod.Name, nodeDeathWords(res, input.ForceDelete.NodeUnreachableThreshold), res.Kind)
		if err := recordForceDelete(ctx, deps, input, pod, idx); err != nil {
			errs = append(errs, err)
		}
	}
	return removed, nextAt, errors.Join(errs...)
}

// nodeDeathWords says, for an event, what the cluster reports about the
// node under a removed pod.
func nodeDeathWords(res evidence.TerminatingResult, threshold time.Duration) string {
	if res.Kind == evidence.NodeUnreachableTaint {
		return fmt.Sprintf("node %s has carried the unreachable taint for at least %s", res.NodeName, threshold)
	}
	return fmt.Sprintf("node %s has read NotReady for at least %s", res.NodeName, threshold)
}

// readNodeDeath reads the node under pod with the configured policy
// through the live reader: does the cluster prove no kubelet is left to
// run the pod's containers. With no policy, no pod, or a pod already on
// its way out it reads NotConfigured, and a failed Node read is an error,
// not a reading.
func readNodeDeath(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod) (evidence.TerminatingResult, error) {
	if input.ForceDelete == nil || pod == nil || pod.DeletionTimestamp != nil {
		return evidence.TerminatingResult{Kind: evidence.NotConfigured}, nil
	}
	res := evidence.NodeDeath(ctx, deps.Reader(), pod, input.ForceDelete, input.Now(), 0)
	if res.Kind == evidence.NodeReadError {
		return res, nodeReadFailure(pod, res.NodeName, res.NodeReadErr)
	}
	return res, nil
}

// nodeReadFailure is the pass's error for the Node under pod that could
// not be read.
func nodeReadFailure(pod *corev1.Pod, nodeName string, err error) error {
	return fmt.Errorf("node-death reading for pod %s: node %s read: %w", pod.Name, nodeName, err)
}

// removePodNow is the delete every force-delete issues: grace zero under
// the pod's UID, so a same-name successor is never hit (OMENative reuses
// stable pod names). Reports whether this call removed the object: a
// NotFound or a UID mismatch means the object was already gone or a
// successor holds the name, the same outcome with nothing to report.
func removePodNow(ctx context.Context, deps workload.Deps, pod *corev1.Pod) (bool, error) {
	uid := pod.UID
	if err := deps.Client.Delete(ctx, pod,
		client.GracePeriodSeconds(0),
		client.Preconditions{UID: &uid},
	); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return false, nil
		}
		return false, fmt.Errorf("force-delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return true, nil
}

// recordForceDelete writes the audit row every force-delete ends with: a
// pod removed because its node stopped reporting it.
func recordForceDelete(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32) error {
	if err := recordForceDeleteLedgerEntry(ctx, deps, input, pod, idx, audit.OutcomeForceDeleteUnreachable); err != nil {
		return fmt.Errorf("record force-delete ledger entry (pod=%s): %w", pod.Name, err)
	}
	return nil
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
	owner, _ := input.AuditOwner()
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
//
// The ledger is one object per owner, written by every Component's pass,
// so a write that loses the race is reloaded and retried rather than
// failing the pass that already removed the pod.
func recordForceDeleteLedgerEntry(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, pod *corev1.Pod, idx int32, outcome string) error {
	owner, ownerGVK := input.AuditOwner()
	now := input.Now().UTC().Format(time.RFC3339)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ledger, err := audit.LoadLedgerForOwner(ctx, deps.Reader(), owner)
		if err != nil {
			return fmt.Errorf("load audit ledger: %w", err)
		}
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
		if err := audit.PersistLedgerForOwner(ctx, deps.Client, owner, ownerGVK, ledger); err != nil {
			return fmt.Errorf("persist audit ledger: %w", err)
		}
		return nil
	})
}
