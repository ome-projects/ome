package irprojector

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
)

// AggregateIRStatus reads the live InferenceReplica for each
// IR-managed Component on the given ISVC and writes IR.Status into
// ISVC.Status.Components[<component>].Lifecycle via Status().Update.
//
// The IR controller computes the per-Component aggregate counters and
// the projector just relays them onto the parent ISVC, so the downstream
// status contract (kubectl, dashboards, status field shapes) is stable.
//
// Called from the ISVC controller's reconcile loop AFTER the
// per-Component dispatcher (irprojector.EnsureInferenceReplica for the
// IR-managed OMENative Components) finishes. The dispatch path writes
// IR.Spec; the IR controller reconciles + writes IR.Status; THIS function
// reads IR.Status back onto the parent ISVC.
//
// componentModes carries the resolved DeploymentModeType for each
// declared Component on the ISVC (engine/decoder/router). The
// caller already computes this via isvcutils.DetermineDeploymentModes
// for its dispatch decisions — passing the map through avoids
// recomputing per-Component modes here and keeps the IR-managed
// predicate (IsIRManagedComponent) reading the same resolved mode the
// dispatch site read. Components missing from the map are treated as
// non-OMENative and skipped.
//
// Writes directly via Status().Update rather than mutating the
// passed-in ISVC for the outer updateStatus to flush, because the
// outer flush's retry.RetryOnConflict path re-Gets latest + merges
// preserved.Lifecycle from latest — clobbering what we just wrote
// would defeat the purpose.
//
// Mutates the in-memory ISVC after the write so subsequent code in
// the same reconcile pass observes the post-write state.
//
// Sequence per Component:
//
//  1. Skip Components whose resolved deployment mode is not
//     OMENative — the RawDeployment / MultiNode path already wrote
//     ISVC.Status.Components[c].Lifecycle in its own pass.
//
//  2. Fetch the IR by name. NotFound is non-fatal — the IR may not
//     yet exist on the first reconcile, OR the IR was deleted between
//     the projector's CreateOrUpdate and our read. In either case,
//     leaving the ISVC status entry empty is the right behavior (the
//     next reconcile re-creates / re-reads).
//
//  3. Status().Update under retry.RetryOnConflict to merge the
//     IR-derived OMENative subtree onto the live ISVC. Re-read on
//     conflict so concurrent writers (a different Component's
//     reconcile) don't lose to us.
//
// Errors are wrapped with the offending IR namespace/name for grep-
// ability in operator logs. A real read/write error (apiserver
// outage, permission denied) is returned to the caller so the
// reconcile retries; transient NotFound on the IR Get is swallowed.
func AggregateIRStatus(ctx context.Context, c client.Client, reads client.Reader, isvc *v1beta1.InferenceService, componentModes map[v1beta1.ComponentType]constants.DeploymentModeType) error {
	if reads == nil {
		reads = c
	}
	if c == nil {
		return fmt.Errorf("AggregateIRStatus: nil client")
	}
	if isvc == nil {
		return fmt.Errorf("AggregateIRStatus: nil ISVC")
	}

	for _, comp := range allDeclaredComponents(isvc) {
		mode := componentModes[comp]
		// Non-OMENative Components are reconciled by their own dispatch
		// path (RawDeployment / MultiNode), which already wrote their
		// status subtree; skip them here.
		if !IsIRManagedComponent(mode) {
			continue
		}
		if err := aggregateOneComponent(ctx, c, reads, isvc, comp); err != nil {
			return err
		}
	}
	return nil
}

// aggregateOneComponent fetches one IR, computes the desired
// LifecycleStatus, and writes it to the live ISVC via
// Status().Update under retry.RetryOnConflict.
//
// On apierrors.IsNotFound for the IR Get: returns nil so the outer
// loop keeps going. The next reconcile picks up the IR's fresh
// status once the cache observes it.
//
// On apierrors.IsNotFound for the ISVC Update: returns nil so a
// race with ISVC deletion drops cleanly.
func aggregateOneComponent(ctx context.Context, c client.Client, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) error {
	name := InferenceReplicaName(isvc.Name, component)
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: name}
	ir := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, ir); err != nil {
		if apierrors.IsNotFound(err) {
			// First-reconcile race: the projector's CreateOrUpdate
			// landed but the cache hasn't observed it yet, OR the
			// IR was deleted between writes. Leave the status entry
			// empty and let the next reconcile pick it up. Log at
			// V(1) so a *persistent* NotFound (projector failing to
			// create) is visible in operator logs — otherwise this
			// state is invisible from the ISVC status surface.
			log.FromContext(ctx).V(1).Info("InferenceReplica not found; leaving ISVC status entry empty until next reconcile",
				"isvc", client.ObjectKeyFromObject(isvc),
				"inferencereplica", key,
				"component", component)
			return nil
		}
		return fmt.Errorf("AggregateIRStatus: get IR %s/%s for component=%s: %w",
			isvc.Namespace, name, component, err)
	}

	desired := IRStatusToComponentStatus(ir)
	// topCond is the standard top-level Component-ready condition
	// (EngineReady / DecoderReady / RouterReady) derived from the
	// OMENative counters. Computed once
	// outside the retry closure: the derivation is a pure function of
	// `desired`, which is recomputed only when the IR changes (a
	// separate reconcile would re-enter AggregateIRStatus). On a peer
	// status-write conflict we re-Get the ISVC and re-stamp, but the
	// condition itself is stable for this pass.
	// The committed IR carries the merged Instance lifecycle policy. Pod-level
	// disruption policy on the parent ISVC is independent of this readiness floor.
	// ir.Spec.Replicas supplies the desired count so surge does not inflate it.
	topCond := status.TopLevelComponentReadyFromLifecycle(component, desired, ir.Spec.Lifecycle, ir.Spec.Replicas)
	isvcKey := client.ObjectKeyFromObject(isvc)
	// Stamping topCond recomputes the aggregate Ready condition, so for an
	// OMENative ISVC this write is usually the one that first flips Ready
	// True — the reconciler's own status flush then sees it already set.
	// Capture the transition here so end-to-end deployment latency is not
	// systematically dropped for the components that use this path.
	var (
		readyFlipped bool
		prevReady    *knapis.Condition
	)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := reads.Get(ctx, isvcKey, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("re-read ISVC: %w", err)
		}
		if fresh.Status.Components == nil {
			fresh.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
		}
		// Snapshot the live status before mutation so a no-op rollup
		// (IR.Status unchanged since the last pass) skips the
		// Status().Update below. Each ISVC status write re-triggers the
		// ISVC reconciler, so at steady state the unconditional write
		// amplified idle reconciles. SetCondition (knative conditionSet)
		// preserves LastTransitionTime when the condition value is
		// unchanged, so an unchanged status DeepEquals its prior snapshot.
		before := fresh.Status.DeepCopy()
		cs := fresh.Status.Components[component]
		cs.Lifecycle = desired.DeepCopy()
		fresh.Status.Components[component] = cs
		// Ingress and direct-engine checks ran this pass but are not flushed yet.
		// Carry only aggregate-Ready prerequisites; an IR-managed engine supplies
		// its own condition below. Missing caller conditions leave live values intact.
		fresh.Status.SetCondition(v1beta1.IngressReady, isvc.Status.GetCondition(v1beta1.IngressReady))
		if component != v1beta1.EngineComponent {
			fresh.Status.SetCondition(v1beta1.EngineReady, isvc.Status.GetCondition(v1beta1.EngineReady))
		}
		// Emit the top-level component-ready condition in the same
		// Status().Update round-trip as the subtree write. Without this,
		// the OMENative counters land but no EngineReady / DecoderReady /
		// RouterReady ever flips True, so the aggregate Ready rollup and
		// `kubectl wait --for=condition=Ready` see Status=Unknown
		// indefinitely.
		if topCond != nil {
			fresh.Status.SetCondition(topCond.Type, topCond)
		}
		// Skip the write when the recomputed status matches what's already
		// live: a no-op rollup must perform ZERO writes so it triggers
		// nothing downstream. The in-memory mirror after the retry loop
		// still runs so callers in this pass observe the fresh subtree.
		if equality.Semantic.DeepEqual(*before, fresh.Status) {
			return nil
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("update ISVC status: %w", err)
		}
		prevReady = before.GetCondition(knapis.ConditionReady)
		readyFlipped = status.IsReadyTrue(fresh.Status) &&
			(prevReady == nil || prevReady.Status != v1.ConditionTrue)
		return nil
	}); err != nil {
		return fmt.Errorf("AggregateIRStatus: persist component=%s status: %w", component, err)
	}
	if readyFlipped {
		status.ObserveTimeToReady(isvc, prevReady)
	}

	// Mirror onto the caller's in-memory ISVC so downstream code in
	// the same reconcile pass observes the post-write state. Without
	// the mirror, the outer reconciler's status-equality short-circuit
	// (inferenceServiceStatusEqual) may incorrectly skip the next
	// updateStatus call because its baseline doesn't reflect what we
	// just committed.
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[component]
	cs.Lifecycle = desired.DeepCopy()
	isvc.Status.Components[component] = cs
	// Mirror the just-committed top-level condition onto the cached
	// ISVC so the outer reconciler's updateStatus flush — which copies
	// p.ISVC.Status onto a fresh re-read of the cluster — doesn't clobber
	// it on the next pass.
	if topCond != nil {
		isvc.Status.SetCondition(topCond.Type, topCond)
	}
	return nil
}

// IRStatusToComponentStatus projects the IR.Status fields onto a
// fresh LifecycleStatus. 1:1 field mapping — IR.Status and the
// per-Component LifecycleStatus were intentionally shaped identically.
//
// Returns a non-nil pointer even when every field is zero; the
// presence of the pointer is itself a signal that the IR-managed
// path is driving the Component, and downstream consumers (the
// controller's preserveLifecycleStatus helper) treat the nil/non-nil
// boundary as load-bearing.
func IRStatusToComponentStatus(ir *v1beta1.InferenceReplica) *v1beta1.LifecycleStatus {
	out := &v1beta1.LifecycleStatus{
		ObservedGeneration:   ir.Status.ObservedGeneration,
		Replicas:             ir.Status.Replicas,
		ReadyReplicas:        ir.Status.ReadyReplicas,
		ServingReplicas:      ir.Status.ServingReplicas,
		AvailableReplicas:    ir.Status.AvailableReplicas,
		UpdatedReplicas:      ir.Status.UpdatedReplicas,
		UpdatedReadyReplicas: ir.Status.UpdatedReadyReplicas,
		CurrentRevision:      ir.Status.CurrentRevision,
		UpdateRevision:       ir.Status.UpdateRevision,
		LabelSelector:        ir.Status.LabelSelector,
	}
	if ir.Status.CollisionCount != nil {
		cc := *ir.Status.CollisionCount
		out.CollisionCount = &cc
	}
	out.RolloutHold = ir.Status.RolloutHold.DeepCopy()
	// Per-Instance detail is intentionally NOT projected onto the ISVC
	// summary — the authoritative source is the IR's own status, read
	// directly by in-cluster consumers via ComponentIRStatus.
	// Conditions follow the same deep-copy contract.
	if len(ir.Status.Conditions) > 0 {
		out.Conditions = make([]metav1.Condition, len(ir.Status.Conditions))
		copy(out.Conditions, ir.Status.Conditions)
	}
	return out
}

// allDeclaredComponents returns the ComponentTypes that have a
// non-nil spec on the ISVC. Used by AggregateIRStatus to decide
// which IRs to look up. Components whose specs are nil are not
// reconciled by anything — including IR-managed paths — so we skip
// them.
func allDeclaredComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	if isvc.Spec.Engine != nil {
		out = append(out, v1beta1.EngineComponent)
	}
	if isvc.Spec.Decoder != nil {
		out = append(out, v1beta1.DecoderComponent)
	}
	if isvc.Spec.Router != nil {
		out = append(out, v1beta1.RouterComponent)
	}
	return out
}
