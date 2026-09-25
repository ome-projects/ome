// teardown.go owns the finalizer-driven IR teardown path. An IR with a
// DeletionTimestamp reconciles as "desired instances = zero": the
// scale-down batch pipeline tears every Instance down (drain → graceful
// delete → stuck-Terminating force-delete escalation → audit) and the
// teardown finalizer lifts only when owned component Pods are gone and
// owned PodGroups are authoritatively absent. Teardown therefore carries
// the same guarantees as scale-down instead of un-instrumented background GC.
//
// Pods keep their IR owner references throughout: if the controller
// dies mid-teardown, background GC still collects everything the moment
// the finalizer is removed or force-purged. GC racing the scale-down pass
// is benign — fewer live pods is progress by the completion definition.
package inferencereplica

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TeardownFinalizer gates IR deletion on the reconciled teardown path:
// added on every reconcile while DeletionTimestamp is nil, removed once
// no owned component Pod or pending owned PodGroup remains (or the configured
// lifecycle.teardown deadline passes). An already-Terminating IR without it is
// left entirely to background GC.
const TeardownFinalizer = "ome.io/ir-teardown"

// Teardown event reasons, emitted on the IR itself (the parent ISVC is
// typically already deleted by the time teardown runs).
const (
	// ReasonTeardownBlocked warns that pods survive with no teardown
	// deadline configured (strict hold). Emitted every teardown pass
	// with a stable message shape — the EventRecorder aggregates
	// identical (reason, message) pairs into one Event with a bumped
	// count, so the steady state is one Event, not spam.
	ReasonTeardownBlocked = "TeardownBlocked"
	// ReasonTeardownDeadlineExceeded warns that the configured
	// lifecycle.teardown.deadline elapsed with pods surviving; the
	// finalizer is released and background GC owns the remainder.
	ReasonTeardownDeadlineExceeded = "TeardownDeadlineExceeded"
)

// teardownEscapeHint documents the operator's universal unwedge in
// every teardown Warning: stripping the finalizer by hand always works.
const teardownEscapeHint = "manual escape: kubectl patch inferencereplica %s -n %s --type=merge -p '{\"metadata\":{\"finalizers\":null}}'"

// reconcileTeardown drives one Terminating IR toward gone:
//
//  1. Finalizer absent → no-op; background GC owns cleanup.
//  2. Dispatch workload.Reconcile in Teardown mode: every observed
//     Instance runs the scale-down batch pipeline, including the
//     force-delete escalation when lifecycle.forceDelete is configured
//     — the designed unwedger for pods stuck on dead nodes.
//  3. Completion = no live owned component Pods (a Pod whose status entry was
//     lost must still block) and no owned PodGroups in the authoritative inventory.
//     Then delete the IR-owned headless Service and remove the finalizer.
//  4. Survivors past the configured lifecycle.teardown.deadline →
//     Warning + release the finalizer to background GC. No deadline
//     configured → strict hold with a per-pass aggregated Warning.
//
// The parent ISVC is typically ALREADY deleted here (background GC
// deletes the ISVC first): the input builder tolerates parent=nil (the
// audit ledger re-parents to the IR, events fall back to the IR) and
// the normal path's deferred status aggregation is deliberately not
// run — see the comment at the dispatch site.
func (r *Reconciler) reconcileTeardown(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(ir, TeardownFinalizer) {
		r.deleteScaleDownSeries(ir)
		log.V(1).Info("Terminating without teardown finalizer; background GC owns cleanup")
		return ctrl.Result{}, nil
	}

	deadline, deadlineInvalidReason := r.resolveTeardownDeadline(log)
	clockInput := workloadtypes.ReconcileInput{Clock: r.Clock}
	var deadlineAt time.Time
	if deadline != nil {
		deadlineAt = ir.DeletionTimestamp.Add(*deadline)
	}
	if deadline != nil && clockInput.Now().After(deadlineAt) {
		pods, lerr := query.LiveListPodsForComponent(ctx, r.APIReader, ir.Namespace, ir.Spec.ParentRef.Name,
			v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))
		summary := fmt.Sprintf("%d owned pod(s) observed", len(podsControlledBy(pods, ir.UID)))
		if lerr != nil {
			summary = "pod observation failed: " + lerr.Error()
		}
		r.warnTeardown(ir, ReasonTeardownDeadlineExceeded, fmt.Sprintf(
			"teardown deadline %s exceeded (%s; selector: parent=%q, component=%q); releasing finalizer %s to background GC; "+teardownEscapeHint,
			deadline, summary, ir.Spec.ParentRef.Name, ir.Spec.Component, TeardownFinalizer, ir.Name, ir.Namespace))
		parent := r.resolveParentFrom(ctx, r.APIReader, ir)
		r.closeDanglingLedgerEntries(ctx, log, ir, parent)
		r.deleteScaleDownSeries(ir)
		if ferr := r.removeTeardownFinalizer(ctx, ir); ferr != nil {
			return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: remove finalizer past deadline: %w", ferr)
		}
		return ctrl.Result{}, nil
	}

	// Best-effort parent resolve: NotFound → nil, everything below
	// tolerates it. Live reader, not the cache: a stale cached NotFound
	// on a live parent would aim the dangling-ledger close at the empty
	// IR-owned ledger and leave the parent-owned entry dangling. Resolve
	// lifecycle config as usual — the force-delete escalation must keep
	// working during teardown.
	parent := r.resolveParentFrom(ctx, r.APIReader, ir)
	forceDeletePolicy := r.resolveForceDeletePolicy(log)

	// Desired-spec content is irrelevant under Teardown (the planned
	// index set is treated as empty); the input/plan only need to be
	// well-formed so Delete can read Key/Component/InstanceReadyTimeout.
	// The same-target update retry policy is nil: the Update pass never
	// runs in teardown, and neither does the escalation pass (zero
	// escalation windows / zero relocation budget — wedge escalation
	// belongs to the Delete pipeline's lifecycle.forceDelete). Only the
	// readiness backstop is carried: the Drain operation stamps its
	// deadline from it, so teardown resolves it from the same two levels
	// the reconcile path does. The rest of the zero lifecycleSettings is
	// safe for the same reason as the retry policy: the surge settle
	// window and gang bounds belong to passes teardown does not run, the
	// migration caps gate admission of work a deleting IR accepts none
	// of, the retry-block history cap only prunes on a RetryBlock write
	// teardown never makes, and the requeue cadence is not the Delete
	// pipeline's — it paces itself with ScaleDownRequeueInterval plus the
	// force-delete and teardown deadlines, set below. Coordination group
	// defaults are likewise irrelevant: teardown never consults the
	// update gate.
	settings := lifecycleSettings{InstanceReadyTimeout: r.resolveConfiguredInstanceReadyTimeout(log)}
	input := r.buildReconcileInput(ctx, ir, parent, nil, forceDeletePolicy, settings, 0, coordination.GroupDefaults{})
	input.Teardown = true
	input.ScaleDownPodBatchSize = r.ScaleDownPodBatchSize
	input.ScaleDownRequeueInterval = r.ScaleDownRequeueInterval

	plan, perr := workload.BuildPlan(v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), input.DesiredSpec, input.ObservedState)
	if perr != nil {
		return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: build plan (ir=%s/%s): %w", ir.Namespace, ir.Name, perr)
	}
	plan.InstanceReadyTimeout = workload.ResolveInstanceReadyTimeout(
		input.DesiredSpec.Lifecycle.InstanceReadyTimeout, settings.InstanceReadyTimeout)
	pods, lerr := query.LiveListPodsForComponent(ctx, r.APIReader, ir.Namespace, ir.Spec.ParentRef.Name,
		v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))
	if lerr != nil {
		return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: live-list component pods: %w", lerr)
	}
	ownedPods := podsControlledBy(pods, ir.UID)
	if invalid := invalidInstanceIndexPodCount(ownedPods); invalid > 0 {
		return ctrl.Result{}, fmt.Errorf(
			"InferenceReplica teardown: authoritative snapshot contains %d UID-owned component pod(s) without a valid %s label; refusing teardown effects",
			invalid, query.LabelInstanceIdx)
	}
	input.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{
		OwnerUID:   ir.UID,
		Pods:       ownedPods,
		ByInstance: query.BucketPodsByInstanceIdx(ownedPods),
	}
	var podGroupInventory *workloadgang.PodGroupInventory
	if input.DesiredSpec.GangSchedulingAvailable {
		podGroupInventory, lerr = workloadgang.ObservePodGroups(ctx, r.APIReader, input.OwnerObject)
		if lerr != nil {
			return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: observe PodGroups: %w", lerr)
		}
		input.FinalizeInstanceResources = workloadgang.BuildFinalizeInstanceResources(
			r.Client, r.APIReader, podGroupInventory, input.OwnerObject, input.Key.OwnerName, plan.Component)
	}

	// The normal path's deferred aggregateAndWriteStatus is NOT
	// installed here: its counter/condition writes die with the IR. The
	// workload escalation pass does not run under Teardown either — the
	// stuck-pod / InstanceReadyTimeout backstops assume a converging
	// rollout, and firing them against Phase=Deleting bookkeeping would
	// fight the scale-down pipeline that already owns wedge escalation (via
	// lifecycle.forceDelete). Instance statuses complete through the atomic
	// mutation batch after their Pods and per-Instance resources are gone.
	deps := workloadtypes.Deps{
		Client:       r.Client,
		APIReader:    r.APIReader,
		Recorder:     r.Recorder,
		Expectations: r.Expectations,
		Clock:        r.Clock,
	}
	result, err := workload.Reconcile(ctx, deps, input, plan, nil)
	if err != nil {
		return ctrl.Result{}, err
	}
	// A committed status batch is a reconciliation boundary. Resource
	// completion is evaluated from the next authoritative snapshot so delete
	// admission and status removal remain durable before the finalizer lifts.
	if result.Requeue {
		return result, nil
	}
	if len(input.ObservedState.InstanceStatuses) == 0 {
		if _, derr := deleteTeardownOrphanPodGroups(ctx, r.Client, podGroupInventory, ownedPods, input.ScaleDownPodBatchSize); derr != nil {
			return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: delete orphan PodGroups: %w", derr)
		}
	}

	if len(ownedPods) == 0 && teardownPodGroupsFinalized(podGroupInventory) {
		if derr := r.deleteHeadlessService(ctx, ir); derr != nil {
			return ctrl.Result{}, derr
		}
		r.closeDanglingLedgerEntries(ctx, log, ir, parent)
		r.deleteScaleDownSeries(ir)
		if ferr := r.removeTeardownFinalizer(ctx, ir); ferr != nil {
			return ctrl.Result{}, fmt.Errorf("InferenceReplica teardown: remove finalizer: %w", ferr)
		}
		log.Info("Teardown complete; finalizer removed")
		return ctrl.Result{}, nil
	}

	if deadline == nil {
		// Strict hold. The message is stable per (survivor set, config
		// state) — sorted names, no timestamps — so the recorder
		// aggregates repeats into one Event with a count. Two variants:
		// deadline genuinely unconfigured vs configured-but-invalid; an
		// operator who wrote a bad duration must not be told nothing is
		// configured.
		detail := "no lifecycle.teardown.deadline configured"
		if deadlineInvalidReason != "" {
			detail = "lifecycle.teardown.deadline configured but invalid: " + deadlineInvalidReason
		}
		r.warnTeardown(ir, ReasonTeardownBlocked, fmt.Sprintf(
			"teardown blocked: %d owned pod(s) and %d owned PodGroup(s) remain (selector: parent=%q, component=%q); finalizer %s holds until cleanup completes (%s); "+teardownEscapeHint,
			len(ownedPods), pendingTeardownPodGroups(podGroupInventory), ir.Spec.ParentRef.Name, ir.Spec.Component, TeardownFinalizer, detail, ir.Name, ir.Namespace))
	}
	if result.IsZero() {
		// Pods survive with no Delete in flight (e.g. a statusless
		// orphan no Instance owns): keep polling at the Delete cadence
		// so external cleanup or config changes are picked up.
		result = scaleDownRequeueResult(r.ScaleDownRequeueInterval)
	}
	if deadline != nil {
		now := clockInput.Now()
		if now.After(deadlineAt) {
			return ctrl.Result{Requeue: true}, nil
		}
		// Deadline release is strictly after the configured duration. One
		// nanosecond schedules the smallest representable wake past equality.
		remaining := deadlineAt.Sub(now) + time.Nanosecond
		result = foldRequeueAfter(result, remaining)
	}
	return result, nil
}

// closeDanglingLedgerEntries marks every in-flight (Phase=Started)
// migration ledger entry for this component terminal
// (Phase=Failed, Outcome=owner-torn-down) before the finalizer lifts.
// A migration whose instances are deleted mid-flight by teardown can
// never complete, and when the ledger owner is the parent ISVC and the
// parent outlives the IR (operator deletes just the IR; the projector
// recreates a fresh one), a Started entry left behind would resume as
// a phantom migration on the fresh IR and keep consuming the in-flight
// capacity cap. Closed rows are records, not work orders.
//
// The ledger ConfigMap is shared across every component of the parent
// ISVC, and persist is a whole-blob overwrite — so the close runs under
// retry.RetryOnConflict with each attempt re-loading the ledger through
// the live reader and re-applying only this component's close. A peer
// component's concurrent write (e.g. a fresh Started entry from its
// Migrate machinery while the parent lives on) conflicts our Update
// instead of being clobbered, and the retry rebases on top of it.
//
// Best-effort by design (never-worse rule): ledger load/persist
// failures — including exhausted conflict retries — log at V(1) and
// return; audit bookkeeping must never block finalizer removal. Owner
// resolution mirrors reconcileRelocationDirectives' rule: parent ISVC
// when resolvable, else the IR (buildReconcileInput sets LedgerOwner
// only when the parent resolves, with no IR fallback).
func (r *Reconciler) closeDanglingLedgerEntries(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService) {
	owner := client.Object(ir)
	gvk := irGVK
	if parent != nil {
		owner = parent
		gvk = isvcGVK
	}
	component := string(ir.Spec.Component)
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		ledger, err := audit.LoadLedgerForOwner(ctx, r.APIReader, owner)
		if err != nil {
			return err
		}
		closed := false
		for i := range ledger.Entries {
			e := &ledger.Entries[i]
			if e.Phase != audit.PhaseStarted || e.Component != component {
				continue
			}
			*e = audit.NewTerminalEntry(*e, audit.PhaseFailed, audit.OutcomeOwnerTornDown)
			closed = true
		}
		if !closed {
			return nil
		}
		return audit.PersistLedgerForOwner(ctx, r.Client, owner, gvk, ledger)
	})
	if err != nil {
		log.V(1).Info("teardown: dangling-ledger close failed; proceeding with finalizer removal", "error", err.Error())
	}
}

// deleteHeadlessService removes the IR-owned per-Component headless
// Service — the one object the ensure path (buildHeadlessServiceSpec +
// service.ReconcileHeadlessService) maintains. Delete-by-name is the
// live check; NotFound is success. Per-revision routed Services are
// ISVC-controller-owned and not touched here.
func (r *Reconciler) deleteHeadlessService(ctx context.Context, ir *v1beta1.InferenceReplica) error {
	spec := buildHeadlessServiceSpec(ir)
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Namespace: spec.Namespace}}
	if err := r.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("InferenceReplica teardown: delete headless service %s/%s: %w", spec.Namespace, spec.Name, err)
	}
	return nil
}

// removeTeardownFinalizer lifts the teardown finalizer against a fresh
// read under RetryOnConflict. NotFound at any step means the IR is
// already gone — success.
func (r *Reconciler) removeTeardownFinalizer(ctx context.Context, ir *v1beta1.InferenceReplica) error {
	key := client.ObjectKeyFromObject(ir)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.APIReader.Get(ctx, key, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !controllerutil.ContainsFinalizer(fresh, TeardownFinalizer) {
			return nil
		}
		controllerutil.RemoveFinalizer(fresh, TeardownFinalizer)
		if err := r.Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		return nil
	})
}

// warnTeardown emits a teardown Warning on the IR itself — the parent
// ISVC is typically already deleted — mirrored to the controller log so
// log-aggregated views see it without the events stream.
func (r *Reconciler) warnTeardown(ir *v1beta1.InferenceReplica, reason, message string) {
	r.Log.Info("IR teardown warning",
		"inferencereplica", client.ObjectKeyFromObject(ir),
		"reason", reason,
		"message", message)
	if r.Recorder == nil {
		return
	}
	r.Recorder.Event(ir, corev1.EventTypeWarning, reason, message)
}

func deleteTeardownOrphanPodGroups(
	ctx context.Context,
	c client.Client,
	inventory *workloadgang.PodGroupInventory,
	pods []*corev1.Pod,
	budget *int32,
) (int, error) {
	if inventory == nil || !inventory.Available() {
		return 0, nil
	}
	referenced := make(map[string]struct{})
	podsByInstance := query.BucketPodsByInstanceIdx(pods)
	for _, pod := range pods {
		if pod != nil && pod.Labels[query.LabelPodGroup] != "" {
			referenced[pod.Labels[query.LabelPodGroup]] = struct{}{}
		}
	}
	pending := int32(0)
	for _, entry := range inventory.OwnedEntries() {
		if entry.PodGroup.DeletionTimestamp != nil || inventory.DeleteAccepted(entry.Name) {
			pending++
		}
	}
	deleted := 0
	for _, entry := range inventory.OwnedEntries() {
		if entry.PodGroup.DeletionTimestamp != nil || inventory.DeleteAccepted(entry.Name) {
			continue
		}
		if _, hasPods := referenced[entry.Name]; hasPods {
			continue
		}
		// The PodGroup name is the direct reference. Its owned index is
		// conservative recovery evidence when a Pod lost that reference.
		if entry.IndexKnown && len(podsByInstance[entry.Index]) > 0 {
			continue
		}
		if budget != nil && pending+int32(deleted) >= *budget {
			break
		}
		if err := inventory.DeleteOwnedName(ctx, c, entry.Name); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func teardownPodGroupsFinalized(inventory *workloadgang.PodGroupInventory) bool {
	return pendingTeardownPodGroups(inventory) == 0
}

func pendingTeardownPodGroups(inventory *workloadgang.PodGroupInventory) int {
	if inventory == nil || !inventory.Available() {
		return 0
	}
	return len(inventory.OwnedEntries())
}
