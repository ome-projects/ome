package routing

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
	"sigs.k8s.io/ome/pkg/trafficdrain"
	"sigs.k8s.io/ome/pkg/validation"
)

const trafficMapRoutableStatusFieldOwner = "ome-trafficmap-routing"

// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=trafficmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=trafficmaps/status,verbs=get;update;patch

// Reconciler watches InferenceServices on the control-plane cluster and
// generates the capacity-aware TrafficMap a gateway consumes: one namespaced
// TrafficMap per ISVC, named after it and owner-ref'd to it so it is
// garbage-collected with the ISVC. It recomputes the routing table whenever
// placement or per-home capacity changes.
//
// While routing remains selected, the map exists even when nothing is
// routable: an empty table plus a Routable=False reason. When routing becomes
// ineligible, a TrafficMap-backed publisher may hold the map through its
// finalizer until external state is withdrawn. The routing controller writes
// spec, sourceUID, and routing conditions; the publisher owns its disjoint status fields.
type Reconciler struct {
	client.Client
	// APIReader confirms cache misses before state is forgotten. A transient
	// informer miss must not discard probe hysteresis for a live service.
	APIReader client.Reader
	Log       logr.Logger
	// Config gates generation. When disabled the controller reaps what it owns.
	Config Config
	// Prober supplies the optional end-to-end reachability verdict ANDed into
	// each home's health gate. Nil, or configured off, means the gate stays
	// readyReplicas > 0 and no entry carries probe provenance.
	Prober *Prober
	// Capacity supplies the optional home-reported ceiling on each home's
	// planned allocation. Nil, or configured off, means the plan stands and
	// every entry records CapacitySourceControlPlane.
	Capacity *CapacityPoller
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	isvc, found, err := r.getInferenceService(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !found {
		r.Prober.Forget(req.NamespacedName)
		r.Capacity.Forget(req.NamespacedName)
		deleteCapacityFallbackMetric(req.Namespace, req.Name)
		return ctrl.Result{}, r.reapSourceAbsentTrafficMap(ctx, req.NamespacedName)
	}
	if !isvc.DeletionTimestamp.IsZero() {
		r.Prober.Forget(req.NamespacedName)
		r.Capacity.Forget(req.NamespacedName)
		deleteCapacityFallbackMetric(req.Namespace, req.Name)
		return ctrl.Result{}, r.reap(ctx, isvc)
	}

	// A TrafficMap-backed publisher uses its finalizer as the teardown handshake:
	// retain the map until external state is withdrawn and the source is released.
	if !routingEnabled(r.Config, isvc) || !placementcontroller.IsPlacementEligible(isvc) {
		r.Prober.Forget(req.NamespacedName)
		r.Capacity.Forget(req.NamespacedName)
		deleteCapacityFallbackMetric(req.Namespace, req.Name)
		return ctrl.Result{}, r.reap(ctx, isvc)
	}

	if err := validation.ValidateRouting(&isvc.Spec); err != nil {
		return ctrl.Result{}, fmt.Errorf("validate routing policy: %w", err)
	}

	overrides, err := trafficdrain.FromAnnotations(isvc.Annotations)
	if err != nil {
		// Preserve the last valid map if malformed intent reaches storage;
		// treating it as absent could restore traffic during a drain.
		return ctrl.Result{}, fmt.Errorf("parse %s: %w", constants.TrafficDrainAnnotation, err)
	}

	probePolicy, err := ResolveProbePolicy(r.Config, &isvc.Spec)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve probe policy: %w", err)
	}
	capacityPolicy, err := ResolveCapacityPolicy(r.Config, &isvc.Spec)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve capacity policy: %w", err)
	}
	probeTargets := targetsForInferenceService(isvc, probePolicy.PolicyDigest)
	capacityTargets := targetsForInferenceService(isvc, "")
	persisted, err := r.ownedTrafficMap(ctx, isvc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if probePolicy.IsEnabled() && r.Prober == nil {
		return ctrl.Result{}, fmt.Errorf("endpoint probing is enabled but no prober is configured")
	}
	if capacityPolicy.IsEnabled() && r.Capacity == nil {
		return ctrl.Result{}, fmt.Errorf("capacity polling is enabled but no poller is configured")
	}
	if r.Prober != nil {
		_, err = r.Prober.Reconcile(ctx, ProbeReconcileRequest{
			OwnerUID:  isvc.UID,
			Map:       req.NamespacedName,
			Policy:    probePolicy,
			Targets:   probeTargets,
			Persisted: persisted,
		})
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile endpoint probes: %w", err)
		}
	}
	if r.Capacity != nil {
		_, err = r.Capacity.Reconcile(ctx, CapacityReconcileRequest{
			OwnerUID: isvc.UID,
			Map:      req.NamespacedName,
			Policy:   capacityPolicy,
			Targets:  capacityTargets,
		})
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile endpoint capacity: %w", err)
		}
	}

	// Unroutable is a state of the map, not a reason to remove it: build the
	// table (possibly empty) and let the condition carry the explanation.
	spec, status, reason := buildSpec(isvc, probePolicy, capacityPolicy, r.Prober, r.Capacity, overrides...)
	if err := r.apply(ctx, isvc, spec); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.setRoutingStatus(ctx, isvc, status, reason, capacityPolicy.IsEnabled(), overrides...); err != nil {
		return ctrl.Result{}, err
	}
	recordCapacityFallbackMetric(isvc.Namespace, isvc.Name, capacityPolicy.IsEnabled(), spec)
	var probeRequeueAfter time.Duration
	if r.Prober != nil && probePolicy.IsEnabled() && len(probeTargets) > 0 {
		probeRequeueAfter = r.Prober.nextDelay(
			isvc.UID, req.NamespacedName, probeTargets, r.Prober.Clock.Now(),
		)
	}
	var capacityRequeueAfter time.Duration
	if r.Capacity != nil && capacityPolicy.IsEnabled() && len(capacityTargets) > 0 {
		capacityRequeueAfter = r.Capacity.nextDelay(
			isvc.UID, req.NamespacedName, capacityTargets, capacityPolicy, r.Capacity.Clock.Now(),
		)
	}
	return observationRequeueResult(
		probePolicy.IsEnabled(), len(probeTargets), probeRequeueAfter,
		capacityPolicy.IsEnabled(), len(capacityTargets), capacityRequeueAfter,
	), nil
}

func observationRequeueResult(
	probeEnabled bool,
	probeTargets int,
	probeAfter time.Duration,
	capacityEnabled bool,
	capacityTargets int,
	capacityAfter time.Duration,
) ctrl.Result {
	var requeueAfter time.Duration
	for _, schedule := range []struct {
		enabled bool
		targets int
		after   time.Duration
	}{
		{enabled: probeEnabled, targets: probeTargets, after: probeAfter},
		{enabled: capacityEnabled, targets: capacityTargets, after: capacityAfter},
	} {
		if !schedule.enabled || schedule.targets == 0 {
			continue
		}
		if schedule.after <= 0 {
			return ctrl.Result{Requeue: true}
		}
		if requeueAfter == 0 || schedule.after < requeueAfter {
			requeueAfter = schedule.after
		}
	}
	return ctrl.Result{RequeueAfter: requeueAfter}
}

func (r *Reconciler) getInferenceService(
	ctx context.Context,
	key types.NamespacedName,
) (*v1beta1.InferenceService, bool, error) {
	isvc := &v1beta1.InferenceService{}
	if err := r.Get(ctx, key, isvc); err == nil {
		return isvc, true, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, false, err
	}
	if r.APIReader == nil {
		return nil, false, fmt.Errorf("confirm InferenceService cache miss: API reader is not configured")
	}
	live := &v1beta1.InferenceService{}
	if err := r.APIReader.Get(ctx, key, live); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("confirm InferenceService cache miss: %w", err)
	}
	return live, true, nil
}

func (r *Reconciler) ownedTrafficMap(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
) (*v1beta1.TrafficMap, error) {
	if r.APIReader == nil {
		return nil, fmt.Errorf("read TrafficMap provenance: API reader is not configured")
	}
	tm := &v1beta1.TrafficMap{}
	key := types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}
	if err := r.APIReader.Get(ctx, key, tm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !placementcontroller.TrafficMapHasExactController(tm, isvc) {
		return nil, fmt.Errorf("TrafficMap %s exists but is not controlled by InferenceService UID %q", key, isvc.UID)
	}
	if err := validateTrafficMapSourceUID(tm, isvc); err != nil {
		return nil, err
	}
	return tm, nil
}

// apply creates or updates the ISVC's TrafficMap to carry the desired spec,
// owner-ref'd to the ISVC. It touches only spec; the independently managed
// status subresource is left untouched.
//
// It retries on conflict rather than surfacing the rejection: owning the
// TrafficMap re-enqueues this reconciler on its own spec writes, and the
// Routable status write bumps the same object's resourceVersion, so a cached
// read can lag a concurrent pass. CreateOrUpdate re-reads on each attempt, so a
// retry converges on the live version instead of returning a 409 that would log
// as a spurious reconcile error.
func (r *Reconciler) apply(ctx context.Context, isvc *v1beta1.InferenceService, spec v1beta1.TrafficMapSpec) error {
	var op controllerutil.OperationResult
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{Name: isvc.Name, Namespace: isvc.Namespace}}
		var err error
		op, err = controllerutil.CreateOrUpdate(ctx, r.Client, tm, func() error {
			if (tm.UID != "" || tm.ResourceVersion != "") &&
				!placementcontroller.TrafficMapHasExactController(tm, isvc) {
				return fmt.Errorf("TrafficMap %s exists but is not controlled by InferenceService UID %q",
					client.ObjectKeyFromObject(tm), isvc.UID)
			}
			if err := validateTrafficMapSourceUID(tm, isvc); err != nil {
				return err
			}
			if err := controllerutil.SetControllerReference(isvc, tm, r.Scheme()); err != nil {
				return err
			}
			tm.Spec = spec
			return nil
		})
		return err
	}); err != nil {
		return err
	}
	if op != controllerutil.OperationResultNone {
		r.Log.Info("trafficmap generated", "isvc", client.ObjectKeyFromObject(isvc).String(),
			"op", op, "mode", spec.Mode, "homes", len(spec.Entries))
	}
	return nil
}

// setRoutingStatus writes the routing controller's conditions onto the ISVC's
// TrafficMap. Routable explains an empty or degraded table; CapacityFallback
// reports when configured endpoint capacity has fallen open to the plan.
//
// It re-reads and retries on conflict rather than patching blind. The routing
// conditions are list-map entries with their own server-side-apply field
// manager, so they coexist with the publisher's independently owned Published
// condition.
func (r *Reconciler) setRoutingStatus(ctx context.Context, isvc *v1beta1.InferenceService,
	status metav1.ConditionStatus, reason string, capacityEnabled bool, overrides ...trafficdrain.Override) error {
	key := types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		tm := &v1beta1.TrafficMap{}
		if err := r.APIReader.Get(ctx, key, tm); err != nil {
			// Gone, or not ours to annotate: nothing to report on.
			return client.IgnoreNotFound(err)
		}
		if !placementcontroller.TrafficMapHasExactController(tm, isvc) {
			return nil
		}
		if err := validateTrafficMapSourceUID(tm, isvc); err != nil {
			return err
		}
		tm.Status.SourceUID = isvc.UID
		cond := metav1.Condition{
			Type:               v1beta1.TrafficMapRoutable,
			Status:             status,
			Reason:             reason,
			Message:            routableMessage(reason, status, len(tm.Spec.Entries)),
			ObservedGeneration: tm.Generation,
		}
		apimeta.SetStatusCondition(&tm.Status.Conditions, cond)
		apimeta.SetStatusCondition(&tm.Status.Conditions,
			capacityFallbackCondition(tm.Spec, capacityEnabled, tm.Generation))
		apimeta.SetStatusCondition(&tm.Status.Conditions,
			overrideActiveCondition(tm.Spec.Entries, overrides, tm.Generation))
		return r.Status().Apply(ctx, trafficMapRoutingStatusApply(tm),
			client.FieldOwner(trafficMapRoutableStatusFieldOwner), client.ForceOwnership)
	})
}

func trafficMapRoutingStatusApply(trafficMap *v1beta1.TrafficMap) runtime.ApplyConfiguration {
	status := map[string]any{}
	if trafficMap.Status.SourceUID != "" {
		status["sourceUID"] = string(trafficMap.Status.SourceUID)
	}
	conditions := make([]any, 0, 3)
	for _, conditionType := range []string{
		v1beta1.TrafficMapRoutable,
		v1beta1.TrafficMapCapacityFallback,
		v1beta1.TrafficMapOverrideActive,
	} {
		if condition := apimeta.FindStatusCondition(trafficMap.Status.Conditions, conditionType); condition != nil {
			conditions = append(conditions, trafficMapRoutingConditionApply(*condition))
		}
	}
	if len(conditions) != 0 {
		status["conditions"] = conditions
	}
	apply := &unstructured.Unstructured{Object: map[string]any{"status": status}}
	apply.SetAPIVersion(v1beta1.SchemeGroupVersion.String())
	apply.SetKind("TrafficMap")
	apply.SetNamespace(trafficMap.Namespace)
	apply.SetName(trafficMap.Name)
	apply.SetResourceVersion(trafficMap.ResourceVersion)
	return client.ApplyConfigurationFromUnstructured(apply)
}

func trafficMapRoutingConditionApply(condition metav1.Condition) map[string]any {
	apply := map[string]any{
		"type":               condition.Type,
		"status":             string(condition.Status),
		"observedGeneration": condition.ObservedGeneration,
		"reason":             condition.Reason,
		"message":            condition.Message,
	}
	if !condition.LastTransitionTime.IsZero() {
		apply["lastTransitionTime"] = condition.LastTransitionTime.UTC().Format(time.RFC3339)
	}
	return apply
}

func overrideActiveCondition(entries []v1beta1.TrafficMapEntry, overrides []trafficdrain.Override,
	generation int64) metav1.Condition {
	applied := make(map[string]struct{}, len(overrides))
	arms := 0
	for i := range entries {
		if len(entries[i].DrainRefs) == 0 {
			continue
		}
		arms++
		for _, ref := range entries[i].DrainRefs {
			applied[ref] = struct{}{}
		}
	}

	cond := metav1.Condition{
		Type:               v1beta1.TrafficMapOverrideActive,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.TrafficMapReasonNoOverrides,
		Message:            "no traffic-drain annotation override is configured",
		ObservedGeneration: generation,
	}
	if len(overrides) > 0 && len(applied) == 0 {
		cond.Reason = v1beta1.TrafficMapReasonOverridesPending
		cond.Message = fmt.Sprintf("%d traffic-drain annotation override(s) pending; no matching routable arm", len(overrides))
		return cond
	}
	if len(applied) > 0 {
		cond.Status = metav1.ConditionTrue
		cond.Reason = v1beta1.TrafficMapReasonOverridesApplied
		cond.Message = fmt.Sprintf("%d of %d traffic-drain annotation override(s) active across %d route arm(s); inspect spec.entries[].drainRefs",
			len(applied), len(overrides), arms)
	}
	return cond
}

// routableMessage renders the operator-facing explanation for a Routable reason.
// The entry count is included because "empty" and "degraded" are different
// problems and the reason alone does not distinguish how many homes exist.
func routableMessage(reason string, status metav1.ConditionStatus, entries int) string {
	switch reason {
	case v1beta1.TrafficMapReasonNotPlaced:
		return "InferenceService is not Placed; no homes to route to yet"
	case v1beta1.TrafficMapReasonNoAddressableHome:
		return "placed, but no admitted home reports an addressable endpoint"
	case v1beta1.TrafficMapReasonAllHomesUnready:
		return fmt.Sprintf("%d home(s) addressable but none has ready replicas; all route weights are zero", entries)
	case v1beta1.TrafficMapReasonTrafficDrain:
		return fmt.Sprintf("manual traffic-drain overrides set all %d route arm(s) to zero", entries)
	case v1beta1.TrafficMapReasonNoRoutableCapacity:
		return fmt.Sprintf("%d home(s) addressable but none has positive routable capacity; all route weights are zero", entries)
	case v1beta1.TrafficMapReasonAllHomesProbeFailed:
		if status == metav1.ConditionTrue {
			return fmt.Sprintf("%d home(s) have conclusive probe failures; preserving traffic among homes with ready capacity", entries)
		}
		return fmt.Sprintf("%d home(s) have conclusive probe failures; all route weights are zero", entries)
	default:
		return fmt.Sprintf("%d home(s) serving", entries)
	}
}

// reap deletes a TrafficMap with an exact source controller or matching durable
// source provenance. It never overrides a conflicting controller. The direct
// TrafficMap watch keeps ownerless cleanup live when publisher status changes.
func (r *Reconciler) reap(ctx context.Context, isvc *v1beta1.InferenceService) error {
	if r.APIReader == nil {
		return fmt.Errorf("reap TrafficMap: API reader is not configured")
	}
	if isvc == nil {
		return fmt.Errorf("reap TrafficMap: InferenceService is nil")
	}
	tm := &v1beta1.TrafficMap{}
	key := types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}
	if isvc.UID == "" {
		return fmt.Errorf("reap TrafficMap %s: InferenceService UID is empty", key)
	}
	if err := r.APIReader.Get(ctx, key, tm); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !tm.DeletionTimestamp.IsZero() {
		return nil
	}
	exactController := placementcontroller.TrafficMapHasExactController(tm, isvc)
	if !exactController {
		// Durable routing provenance survives orphan propagation, but it never
		// overrides another controller's ownership.
		if placementcontroller.TrafficMapHasController(tm) || tm.Status.SourceUID != isvc.UID {
			return nil
		}
	}
	if err := validateTrafficMapSourceUID(tm, isvc); err != nil {
		return err
	}
	// Give the inactive publisher a chance to restore its finalizer before the
	// deletion request can erase a durable cleanup journal.
	if trafficMapNeedsPublisherFinalizerRepair(tm) {
		return nil
	}
	return r.deleteTrafficMap(ctx, key, tm)
}

// reapSourceAbsentTrafficMap removes a generated map whose source is
// authoritatively absent and whose controller reference has already been
// orphaned. Durable source provenance distinguishes it from an unattributed
// legacy or hand-authored object at the same key.
func (r *Reconciler) reapSourceAbsentTrafficMap(
	ctx context.Context,
	key types.NamespacedName,
) error {
	if r.APIReader == nil {
		return fmt.Errorf("reap source-absent TrafficMap: API reader is not configured")
	}
	tm := &v1beta1.TrafficMap{}
	if err := r.APIReader.Get(ctx, key, tm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("read source-absent TrafficMap %s: %w", key, err)
	}
	if !tm.DeletionTimestamp.IsZero() ||
		placementcontroller.TrafficMapHasController(tm) ||
		tm.Status.SourceUID == "" ||
		tm.Spec.Service != key.Name {
		return nil
	}
	// A stateful publisher repairs this finalizer before routing requests
	// deletion, preserving the durable claim journal until cleanup completes.
	if trafficMapNeedsPublisherFinalizerRepair(tm) {
		return nil
	}
	return r.deleteTrafficMap(ctx, key, tm)
}

func (r *Reconciler) deleteTrafficMap(
	ctx context.Context,
	key types.NamespacedName,
	tm *v1beta1.TrafficMap,
) error {
	if tm.UID == "" {
		return fmt.Errorf("reap TrafficMap %s: UID is empty", key)
	}
	if tm.ResourceVersion == "" {
		return fmt.Errorf("reap TrafficMap %s: resourceVersion is empty", key)
	}
	uid := tm.UID
	resourceVersion := tm.ResourceVersion
	return client.IgnoreNotFound(r.Delete(ctx, tm, client.Preconditions{
		UID:             &uid,
		ResourceVersion: &resourceVersion,
	}))
}

func trafficMapNeedsPublisherFinalizerRepair(trafficMap *v1beta1.TrafficMap) bool {
	return trafficMapHasPublisherClaims(trafficMap) &&
		!controllerutil.ContainsFinalizer(trafficMap, placementcontroller.TrafficMapPublisherFinalizer)
}

func trafficMapHasPublisherClaims(trafficMap *v1beta1.TrafficMap) bool {
	return trafficMap != nil && trafficMap.Status.Publisher != nil &&
		len(trafficMap.Status.Publisher.ClaimedTargets) != 0
}

func validateTrafficMapSourceUID(
	tm *v1beta1.TrafficMap,
	isvc *v1beta1.InferenceService,
) error {
	if tm == nil || isvc == nil || tm.Status.SourceUID == "" {
		return nil
	}
	if tm.Status.SourceUID != isvc.UID {
		return fmt.Errorf("TrafficMap %s source UID %q does not match InferenceService UID %q",
			client.ObjectKeyFromObject(tm), tm.Status.SourceUID, isvc.UID)
	}
	return nil
}

// buildSpec projects an ISVC's placement into the desired TrafficMap spec, plus
// the Routable verdict that explains it. An unroutable ISVC yields an empty
// table rather than no table, so a consumer can tell "known and unroutable"
// from "unknown".
//
// Probe and capacity observations are optional overlays on an otherwise pure
// projection of the ISVC. Nil observers with disabled policies yield the plan
// alone, which the watch predicate uses to detect source changes.
func buildSpec(
	isvc *v1beta1.InferenceService,
	probePolicy ResolvedProbePolicy,
	capacityPolicy ResolvedCapacityPolicy,
	probe *Prober,
	capacity *CapacityPoller,
	overrides ...trafficdrain.Override,
) (v1beta1.TrafficMapSpec, metav1.ConditionStatus, string) {
	empty := v1beta1.TrafficMapSpec{
		Service:                isvc.Name,
		Mode:                   placementMode(isvc),
		ObservedISVCGeneration: isvc.Generation,
	}
	pl := isvc.Status.Placement
	if pl == nil || pl.Phase != v1beta1.PlacementPhasePlaced {
		return empty, metav1.ConditionFalse, v1beta1.TrafficMapReasonNotPlaced
	}
	cands := servingCandidates(isvc)
	if len(cands) == 0 {
		return empty, metav1.ConditionFalse, v1beta1.TrafficMapReasonNoAddressableHome
	}

	// Index-aligned: homes feed the weight computation, entries carry the result
	// plus provenance, both in the same cluster-sorted order.
	homes := make([]Home, len(cands))
	targets := make([]Target, len(cands))
	capacityFallbackReasons := make([]string, len(cands))
	for i, c := range cands {
		targets[i] = targetForCandidate(isvc, c, probePolicy.PolicyDigest)
		reported, fallbackReason := capacity.observation(targets[i], capacityPolicy)
		capacityFallbackReasons[i] = fallbackReason
		// A nil factor is the identity 1.0; a per-cluster override scales the home's
		// share for heterogeneous hardware. A nil reachability verdict does not
		// gate — probing off, or no verdict yet.
		homes[i] = Home{
			Cluster:   c.Cluster,
			Allocated: c.AdmittedReplicas,
			Ready:     c.ReadyReplicas,
			Factor:    capacityFactor(isvc, c.Cluster),
			Reachable: probe.Reachable(targets[i]),
			Reported:  reported,
		}
	}
	computedWeights := weights(homes, probeAllFailedPolicy(probePolicy))

	entries := make([]v1beta1.TrafficMapEntry, len(cands))
	for i, c := range cands {
		entries[i] = v1beta1.TrafficMapEntry{
			Cluster:  c.Cluster,
			Endpoint: c.Endpoint.DeepCopy(),
			Weight:   computedWeights[i],
			// Healthy reports readiness plus probe evidence. Capacity remains
			// independent: a healthy home can have weight zero when its admitted or
			// reported capacity is zero, while PreserveTraffic may ignore only a
			// fleet-wide probe failure.
			Healthy: c.ReadyReplicas > 0 && (homes[i].Reachable == nil || *homes[i].Reachable),
			Capacity: &v1beta1.TrafficMapCapacity{
				Allocated:      homes[i].EffectiveAllocated(),
				Ready:          c.ReadyReplicas,
				Factor:         homes[i].Factor,
				Source:         allocationSource(homes[i]),
				Reported:       homes[i].Reported,
				FallbackReason: capacityFallbackReasons[i],
			},
			Probe: probe.Provenance(targets[i]),
		}
	}
	manualDrainCausedAllZero := applyTrafficDrains(entries, overrides)
	hasPositiveWeight := slices.ContainsFunc(entries, func(e v1beta1.TrafficMapEntry) bool { return e.Weight > 0 })
	status := metav1.ConditionFalse
	reason := v1beta1.TrafficMapReasonNoRoutableCapacity
	if hasPositiveWeight {
		status = metav1.ConditionTrue
		reason = v1beta1.TrafficMapReasonRoutable
		if allProbesFailed(homes) {
			reason = v1beta1.TrafficMapReasonAllHomesProbeFailed
		}
	} else if manualDrainCausedAllZero {
		reason = v1beta1.TrafficMapReasonTrafficDrain
	} else if !slices.ContainsFunc(homes, func(h Home) bool { return h.Ready > 0 }) {
		reason = v1beta1.TrafficMapReasonAllHomesUnready
	} else if !slices.ContainsFunc(homes, func(h Home) bool { return h.RoutableReplicas() > 0 }) {
		reason = v1beta1.TrafficMapReasonNoRoutableCapacity
	} else if allProbesFailed(homes) {
		reason = v1beta1.TrafficMapReasonAllHomesProbeFailed
	}
	return v1beta1.TrafficMapSpec{
		Service:                isvc.Name,
		Mode:                   placementMode(isvc),
		Entries:                entries,
		ObservedISVCGeneration: isvc.Generation,
	}, status, reason
}

func probeAllFailedPolicy(policy ResolvedProbePolicy) AllFailedPolicy {
	if !policy.IsEnabled() {
		return AllFailedPolicyPreserveTraffic
	}
	return policy.Probe.AllFailedPolicy
}

func targetForCandidate(
	isvc *v1beta1.InferenceService,
	candidate *v1beta1.CandidatePlacement,
	policyDigest string,
) Target {
	return Target{
		OwnerUID:     isvc.UID,
		Map:          types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace},
		Cluster:      candidate.Cluster,
		URL:          candidate.Endpoint.String(),
		PolicyDigest: policyDigest,
	}
}

func targetsForInferenceService(isvc *v1beta1.InferenceService, policyDigest string) []Target {
	if isvc.Status.Placement == nil || isvc.Status.Placement.Phase != v1beta1.PlacementPhasePlaced {
		return nil
	}
	candidates := servingCandidates(isvc)
	targets := make([]Target, 0, len(candidates))
	for _, candidate := range candidates {
		target := targetForCandidate(isvc, candidate, policyDigest)
		endpoint, err := CanonicalEndpoint(target.URL)
		if err != nil {
			continue
		}
		target.URL = endpoint
		targets = append(targets, target)
	}
	return targets
}

// applyTrafficDrains applies manual holds after automatic capacity and health.
func applyTrafficDrains(entries []v1beta1.TrafficMapEntry, overrides []trafficdrain.Override) bool {
	hadPositive := slices.ContainsFunc(entries, func(e v1beta1.TrafficMapEntry) bool { return e.Weight > 0 })
	for i := range entries {
		for j := range overrides {
			if overrides[j].Cluster == entries[i].Cluster {
				entries[i].DrainRefs = append(entries[i].DrainRefs, overrides[j].ID)
			}
		}
		if len(entries[i].DrainRefs) > 0 {
			sort.Strings(entries[i].DrainRefs)
			entries[i].Weight = 0
		}
	}
	return hadPositive && !slices.ContainsFunc(entries, func(e v1beta1.TrafficMapEntry) bool { return e.Weight > 0 })
}

// allocationSource names which input produced the entry's allocation. Endpoint
// only when a report actually lowered the plan: a report that matched or
// exceeded the plan left Allocated untouched, so claiming Endpoint there would
// misattribute a control-plane number to the home.
func allocationSource(h Home) v1beta1.CapacitySource {
	if h.Reported != nil && h.EffectiveAllocated() != h.Allocated {
		return v1beta1.CapacitySourceEndpoint
	}
	return v1beta1.CapacitySourceControlPlane
}

// servingCandidates returns the admitted candidates that report an addressable
// endpoint — the serving homes — sorted by cluster for a deterministic table.
func servingCandidates(isvc *v1beta1.InferenceService) []*v1beta1.CandidatePlacement {
	pl := isvc.Status.Placement
	if pl == nil {
		return nil
	}
	var cs []*v1beta1.CandidatePlacement
	for i := range pl.Candidates {
		c := &pl.Candidates[i]
		if c.Phase == v1beta1.CandidatePhaseAdmitted && c.Endpoint != nil && c.Endpoint.Host != "" {
			cs = append(cs, c)
		}
	}
	// Annotation-based Single placement can carry only its top-level winner and
	// endpoint. Treat it as one serving candidate.
	if len(cs) == 0 && placementMode(isvc) == v1beta1.PlacementModeSingle &&
		pl.Cluster != "" && pl.Endpoint != nil && pl.Endpoint.Host != "" {
		cs = append(cs, &v1beta1.CandidatePlacement{
			Cluster:          pl.Cluster,
			Phase:            v1beta1.CandidatePhaseAdmitted,
			Endpoint:         pl.Endpoint.DeepCopy(),
			AdmittedReplicas: 1,
			ReadyReplicas:    1,
		})
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].Cluster < cs[j].Cluster })
	return cs
}

// capacityFactor returns the per-replica relative serving capacity the ISVC
// declares for a workload cluster, or nil when none is set. spec.routing is
// canonical; the placement field is a compatibility alias used only when the
// routing map is absent. Nil is the identity factor 1.0 — the weight computation
// treats a nil or non-positive factor as 1.
func capacityFactor(isvc *v1beta1.InferenceService, cluster string) *resource.Quantity {
	var factors map[string]resource.Quantity
	if isvc.Spec.Routing != nil && isvc.Spec.Routing.CapacityFactors != nil {
		factors = isvc.Spec.Routing.CapacityFactors
	} else if isvc.Spec.Placement != nil {
		//nolint:staticcheck // compatibility read for deprecated spec.placement.capacityFactors
		factors = isvc.Spec.Placement.CapacityFactors
	}
	if factors == nil {
		return nil
	}
	if q, ok := factors[cluster]; ok {
		qc := q.DeepCopy()
		return &qc
	}
	return nil
}

// placementMode returns the ISVC's declared placement mode, defaulting legacy
// annotation-based placement to Single.
func placementMode(isvc *v1beta1.InferenceService) v1beta1.PlacementMode {
	if isvc.Spec.Placement == nil {
		return v1beta1.PlacementModeSingle
	}
	return isvc.Spec.Placement.Mode
}

// SetupWithManager wires the controller: reconcile ISVCs, reacting only to
// events that can change the generated table, and watch TrafficMaps by their
// same namespace/name source key. The direct watch continues to work after
// orphan propagation removes a map's owner reference.
//
// Probe and capacity observations run inside reconciliation and use their
// earliest returned delay as RequeueAfter.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		return fmt.Errorf("TrafficMap routing API reader is not configured")
	}
	b := ctrl.NewControllerManagedBy(mgr).
		Named(RoutingControllerName).
		For(&v1beta1.InferenceService{}, builder.WithPredicates(routingTableChange)).
		Watches(&v1beta1.TrafficMap{}, handler.EnqueueRequestsFromMapFunc(enqueueTrafficMapSource))
	if r.Config.Observer.MaxConcurrentReconciles > 0 {
		b = b.WithOptions(controller.Options{
			MaxConcurrentReconciles: r.Config.Observer.MaxConcurrentReconciles,
		})
	}

	return b.Complete(r)
}

func enqueueTrafficMapSource(_ context.Context, object client.Object) []ctrl.Request {
	if _, ok := object.(*v1beta1.TrafficMap); !ok {
		return nil
	}
	return []ctrl.Request{{NamespacedName: client.ObjectKeyFromObject(object)}}
}

// routingTableChange admits ISVC events that can change the generated TrafficMap:
// any create/delete, a deletion-timestamp transition, and updates where the
// projected spec, or the Routable status/reason we would report, differs. Routine ISVC churn that does
// not alter the table is dropped.
var routingTableChange = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldISVC, ok1 := e.ObjectOld.(*v1beta1.InferenceService)
		newISVC, ok2 := e.ObjectNew.(*v1beta1.InferenceService)
		if !ok1 || !ok2 {
			return true // unexpected type: fail safe
		}
		if oldISVC.DeletionTimestamp.IsZero() != newISVC.DeletionTimestamp.IsZero() {
			return true
		}
		if placementcontroller.IsPlacementEligible(oldISVC) != placementcontroller.IsPlacementEligible(newISVC) {
			return true
		}
		if routingOptedOut(oldISVC) != routingOptedOut(newISVC) {
			return true
		}
		if !equality.Semantic.DeepEqual(routingProbeSpec(oldISVC), routingProbeSpec(newISVC)) {
			return true
		}
		if !equality.Semantic.DeepEqual(routingCapacitySpec(oldISVC), routingCapacitySpec(newISVC)) {
			return true
		}
		if !equality.Semantic.DeepEqual(routingPublisherSpec(oldISVC), routingPublisherSpec(newISVC)) {
			return true
		}
		oldDrain, oldHasDrain := oldISVC.Annotations[constants.TrafficDrainAnnotation]
		newDrain, newHasDrain := newISVC.Annotations[constants.TrafficDrainAnnotation]
		if oldHasDrain != newHasDrain || oldDrain != newDrain {
			return true
		}
		// The condition is part of what we write, so a change in status or reason
		// has to wake us even when the table itself is byte-identical — an ISVC
		// going NotPlaced -> NoAddressableHome keeps an empty table but must
		// re-explain itself.
		oldSpec, oldStatus, oldReason := buildSpec(
			oldISVC, ResolvedProbePolicy{}, ResolvedCapacityPolicy{}, nil, nil,
		)
		newSpec, newStatus, newReason := buildSpec(
			newISVC, ResolvedProbePolicy{}, ResolvedCapacityPolicy{}, nil, nil,
		)
		if oldStatus != newStatus || oldReason != newReason {
			return true
		}
		return !equality.Semantic.DeepEqual(oldSpec, newSpec)
	},
}

func routingEnabled(config Config, isvc *v1beta1.InferenceService) bool {
	return config.IsEnabled() && !routingOptedOut(isvc)
}

func routingOptedOut(isvc *v1beta1.InferenceService) bool {
	return isvc != nil && isvc.Spec.Routing != nil &&
		isvc.Spec.Routing.Enabled != nil && !*isvc.Spec.Routing.Enabled
}

func routingProbeSpec(isvc *v1beta1.InferenceService) *v1beta1.RoutingProbeSpec {
	if isvc == nil || isvc.Spec.Routing == nil {
		return nil
	}
	return isvc.Spec.Routing.Probe
}

func routingCapacitySpec(isvc *v1beta1.InferenceService) *v1beta1.RoutingCapacitySpec {
	if isvc == nil || isvc.Spec.Routing == nil {
		return nil
	}
	return isvc.Spec.Routing.Capacity
}

func routingPublisherSpec(isvc *v1beta1.InferenceService) *v1beta1.RoutingPublisherSpec {
	if isvc == nil || isvc.Spec.Routing == nil {
		return nil
	}
	return isvc.Spec.Routing.Publisher
}
