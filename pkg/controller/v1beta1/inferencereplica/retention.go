package inferencereplica

import (
	"context"
	"iter"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
)

// sweepRevisions trims accumulated ControllerRevisions for this IR down
// to the effective non-live retention limit, never deleting a revision
// that is currently live (CurrentRevision / UpdateRevision or any
// InstanceStatus.RunningRevision / TargetRevision). The limit resolves
// per IR: Spec.RevisionHistoryLimit (projected from the parent ISVC's
// ome.io/revision-history-limit annotation) wins over the operator-level
// lifecycle.revisionHistoryLimit config; with neither set, the sweep is
// skipped entirely (nothing is pruned — never a baked-in fallback).
//
// Best-effort and non-fatal: a retention failure is logged + evented but
// NEVER fails the reconcile. Retention runs as a post-commit step and
// the error is suppressed entirely (matching the IR reconciler's
// headless-Service and deferred-status-write suppression pattern)
// because the next reconcile re-runs the sweep — a stale extra CR is
// harmless, a failed reconcile is not.
//
// MUST be called AFTER the status write so the live-name union reads the
// just-committed CurrentRevision / UpdateRevision (the deferred status
// writer mirrors those onto ir.Status before this runs).
func (r *Reconciler) sweepRevisions(ctx context.Context, ir *v1beta1.InferenceReplica) {
	limit, configured := r.resolveRevisionRetentionLimit(ir)
	if !configured {
		return
	}

	// liveNames union protects in-flight migrations: an Instance whose
	// RunningRevision points at an older CR must keep that CR, or the
	// migration resume path can't find its source payload and bails.
	liveNames := revision.LiveRevisionNames(
		ir.Status.CurrentRevision,
		ir.Status.UpdateRevision,
		rowRevisions(ir.Status.InstanceStatuses),
	)

	if err := revision.RetainControllerRevisions(
		ctx, r.Client, r.APIReader, irRevisionKey(ir), limit, liveNames...,
	); err != nil {
		r.Log.V(1).Error(err, "ControllerRevision retention sweep failed; non-live revisions may accumulate until the next reconcile",
			"ir", client.ObjectKeyFromObject(ir))
		if r.Recorder != nil {
			r.Recorder.Eventf(ir, corev1.EventTypeWarning, "RevisionRetentionFailed",
				"InferenceReplica %s/%s ControllerRevision retention sweep failed: %v",
				ir.Namespace, ir.Name, err)
		}
	}
}

// rowRevisions yields, per stored row, the two revision names the sweep
// protects: the one the Instance runs and the one it moves toward. The
// sweep reads nothing else of a row, so no row is copied for it.
func rowRevisions(rows []v1beta1.OMENativeInstanceStatus) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for i := range rows {
			if !yield(rows[i].RunningRevision, rows[i].TargetRevision) {
				return
			}
		}
	}
}

// resolveRevisionRetentionLimit returns the effective non-live retention
// cap for this IR and whether one is configured at all. Resolution
// order: Spec.RevisionHistoryLimit (per-ISVC annotation projection),
// then lifecycle.revisionHistoryLimit from the inferenceservice-config
// ConfigMap (through the shared short-TTL ConfigCache). Absent, invalid,
// or unloadable config resolves to unconfigured — the sweep prunes
// nothing (a stale extra CR is harmless; deleting on a fabricated
// default is not) — with one V(1) log naming the cause.
func (r *Reconciler) resolveRevisionRetentionLimit(ir *v1beta1.InferenceReplica) (int, bool) {
	if ir.Spec.RevisionHistoryLimit != nil && *ir.Spec.RevisionHistoryLimit > 0 {
		return int(*ir.Spec.RevisionHistoryLimit), true
	}
	if r.Clientset == nil {
		r.Log.V(1).Info("revision retention unresolved: no clientset wired; skipping the sweep (nothing pruned)",
			"ir", client.ObjectKeyFromObject(ir))
		return 0, false
	}
	cfg, err := controllerconfig.NewLifecycleConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		r.Log.V(1).Info("revision retention unresolved: lifecycle config load failed; skipping the sweep (nothing pruned)",
			"ir", client.ObjectKeyFromObject(ir), "error", err.Error())
		return 0, false
	}
	limit, err := cfg.ToRevisionHistoryLimit()
	if err != nil {
		r.Log.V(1).Info("revision retention invalid; skipping the sweep (nothing pruned)",
			"ir", client.ObjectKeyFromObject(ir), "error", err.Error())
		return 0, false
	}
	if limit == nil {
		r.Log.V(1).Info("revision retention unconfigured (no lifecycle.revisionHistoryLimit in inferenceservice-config and no per-ISVC annotation); skipping the sweep (nothing pruned)",
			"ir", client.ObjectKeyFromObject(ir))
		return 0, false
	}
	return int(*limit), true
}
