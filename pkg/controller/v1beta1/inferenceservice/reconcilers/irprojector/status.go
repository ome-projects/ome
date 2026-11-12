package irprojector

import (
	"context"
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// aggregateOneComponent copies one role's replica status onto its service:
// it reads the replica as the cache holds it, writes the lifecycle block and
// the role's ready condition onto the live service when they changed, counts
// a time to ready when that write first made the service Ready, and leaves
// the same block and condition on the pass's own copy of the service.
//
// A replica the cache does not hold is not an error: the next pass reads it
// once the cache has it. A service gone before or during the write is not an
// error either: the deletion takes the status with it.
func aggregateOneComponent(ctx context.Context, c client.Client, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) error {
	key := RoleReplicaKey(isvc, component)
	ir := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, ir); err != nil {
		if apierrors.IsNotFound(err) {
			// Logged at V(1): a replica that never appears is visible in the
			// operator's logs, where the service status cannot show it.
			log.FromContext(ctx).V(1).Info("InferenceReplica not found; leaving ISVC status entry empty until next reconcile",
				"isvc", client.ObjectKeyFromObject(isvc),
				"inferencereplica", key,
				"component", component)
			return nil
		}
		return fmt.Errorf("AggregateIRStatus: get IR %s/%s for component=%s: %w",
			isvc.Namespace, key.Name, component, err)
	}
	write := roleStatusWrite{
		component: component,
		replica:   ir,
		// The pass's copy seeds the carried generation for the paths that
		// never read the live service; the write settles it on the live read.
		lifecycle: IRStatusToComponentStatus(ir, isvc.Status.Components[component].Lifecycle),
	}
	// The role's ready condition follows from the lifecycle block alone, so
	// it is derived once and written on every attempt. The replica's desired
	// count sizes the serving minimum, so a surge does not raise it.
	write.ready = status.TopLevelComponentReadyFromLifecycle(component, write.lifecycle, ir.Spec.Lifecycle, ir.Spec.Replicas)
	flip, err := write.persist(ctx, c, reads, client.ObjectKeyFromObject(isvc))
	if err != nil {
		return fmt.Errorf("AggregateIRStatus: persist component=%s status: %w", component, err)
	}
	if flip.happened {
		status.ObserveTimeToReady(isvc, flip.before)
	}
	write.mirrorOnto(isvc)
	return nil
}

// roleStatusWrite is one role's status on its way onto the service: the
// replica it was read from, the lifecycle block and the ready condition to
// write.
type roleStatusWrite struct {
	component v1beta1.ComponentType
	replica   *v1beta1.InferenceReplica
	lifecycle *v1beta1.LifecycleStatus
	ready     *knapis.Condition
}

// readinessFlip says whether a write made the service Ready, and what the
// aggregate Ready condition on the live service was before that write.
type readinessFlip struct {
	happened bool
	before   *knapis.Condition
}

// persist writes the block and the condition onto the live service under a
// conflict retry: every attempt re-reads the service live, settles the
// carried generation on that read, and writes only when the status changed.
// A service gone before or during the write ends the attempt with nothing
// written.
func (w *roleStatusWrite) persist(ctx context.Context, c client.Client, reads client.Reader, key client.ObjectKey) (readinessFlip, error) {
	var flip readinessFlip
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceService{}
		if err := reads.Get(ctx, key, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("re-read ISVC: %w", err)
		}
		before := fresh.Status.DeepCopy()
		w.applyTo(fresh, fresh.Generation)
		// An unchanged status costs zero writes, so an idle pass triggers
		// nothing downstream. The condition set keeps LastTransitionTime for
		// an unchanged condition, so such a status is deep-equal to its
		// snapshot.
		if equality.Semantic.DeepEqual(*before, fresh.Status) {
			return nil
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("update ISVC status: %w", err)
		}
		flip = readinessFlipOf(before, fresh.Status)
		return nil
	})
	return flip, err
}

// applyTo writes the block and the condition onto a service. The carried
// generation is settled against that service's generation and the block it
// holds already, so a retry never writes a generation from an older read.
func (w *roleStatusWrite) applyTo(isvc *v1beta1.InferenceService, generation int64) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[w.component]
	w.lifecycle.ObservedGeneration = lifecycleObservedGeneration(w.replica, cs.Lifecycle, generation)
	cs.Lifecycle = w.lifecycle.DeepCopy()
	isvc.Status.Components[w.component] = cs
	// Without the role's condition no EngineReady, DecoderReady or
	// RouterReady ever turns True and the aggregate Ready stays Unknown.
	if w.ready != nil {
		isvc.Status.SetCondition(w.ready.Type, w.ready)
	}
}

// mirrorOnto leaves the block and the condition on the pass's own copy of
// the service, so the reconciler's final status flush, which compares its
// copy with the cluster, does not undo the write on the next pass.
func (w *roleStatusWrite) mirrorOnto(isvc *v1beta1.InferenceService) {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	cs := isvc.Status.Components[w.component]
	cs.Lifecycle = w.lifecycle.DeepCopy()
	isvc.Status.Components[w.component] = cs
	if w.ready != nil {
		isvc.Status.SetCondition(w.ready.Type, w.ready)
	}
}

// readinessFlipOf reads whether a committed write took the service from not
// Ready to Ready. The baseline is the live status the write replaced, never
// the pass's copy: the reconciler's own flush writes readiness too, and a
// stale baseline would count one path's transition for the other.
func readinessFlipOf(before *v1beta1.InferenceServiceStatus, after v1beta1.InferenceServiceStatus) readinessFlip {
	prev := before.GetCondition(knapis.ConditionReady)
	return readinessFlip{
		happened: status.IsReadyTrue(after) && (prev == nil || prev.Status != v1.ConditionTrue),
		before:   prev,
	}
}

// IRStatusToComponentStatus projects the IR.Status fields onto a
// fresh LifecycleStatus. 1:1 field mapping — IR.Status and the
// per-Component LifecycleStatus were intentionally shaped identically —
// except ObservedGeneration, which reports the parent ISVC generation
// (see lifecycleObservedGeneration). live is the Lifecycle block already
// on the ISVC for this Component, nil when there is none.
//
// Returns a non-nil pointer even when every field is zero; the
// presence of the pointer is itself a signal that the IR-managed
// path is driving the Component, and downstream consumers (the
// controller's preserveLifecycleStatus helper) treat the nil/non-nil
// boundary as load-bearing.
func IRStatusToComponentStatus(ir *v1beta1.InferenceReplica, live *v1beta1.LifecycleStatus) *v1beta1.LifecycleStatus {
	out := &v1beta1.LifecycleStatus{
		ObservedGeneration:   lifecycleObservedGeneration(ir, live, 0),
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
	// Per-Instance detail is not projected onto the ISVC summary. The
	// replica's own status is the source, and in-cluster consumers read its
	// rows through DecodedComponentIRFor, which decodes a compacted status.
	// Conditions follow the same deep-copy contract.
	if len(ir.Status.Conditions) > 0 {
		out.Conditions = make([]metav1.Condition, len(ir.Status.Conditions))
		copy(out.Conditions, ir.Status.Conditions)
	}
	return out
}

// lifecycleObservedGeneration decides the ISVC generation the Lifecycle
// block reports. The projector stamps the parent ISVC generation on the
// IR with every projection, so once the IR has reconciled its current
// spec the stamp is the generation this block reflects. Until then the
// value already live on the ISVC is carried (zero when there is none).
// A replica no projector stamps (one the service fronts by reference)
// reports isvcGeneration, the live generation of the service, once it has
// reconciled its own spec; a caller without the live service passes 0 and
// gets the carried value. The IR's own generation is never reported: it
// also moves on IR-only spec changes (migration mailbox annotations,
// pause/unpause, canary steps, autoscaler replica edits) that leave the
// ISVC generation untouched, which would strand consumers comparing this
// field against ISVC.metadata.generation.
func lifecycleObservedGeneration(ir *v1beta1.InferenceReplica, live *v1beta1.LifecycleStatus, isvcGeneration int64) int64 {
	var carried int64
	if live != nil {
		carried = live.ObservedGeneration
	}
	if ir.Status.ObservedGeneration != ir.Generation {
		return carried
	}
	raw, ok := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	if !ok {
		if isvcGeneration > 0 {
			return isvcGeneration
		}
		return carried
	}
	stamp, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return carried
	}
	return stamp
}

// allDeclaredComponents returns the ComponentTypes that have a
// non-nil spec on the ISVC, or the roles it serves through referenced
// replicas. Used by AggregateIRStatus to decide which IRs to look up.
// Components whose specs are nil are not reconciled by anything —
// including IR-managed paths — so we skip them.
func allDeclaredComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	if ReferencesReplicas(isvc) {
		return ReferencedRoles(isvc)
	}
	var out []v1beta1.ComponentType
	for _, view := range v1beta1.ServiceRoles(&isvc.Spec).Declared() {
		out = append(out, view.Type)
	}
	return out
}
