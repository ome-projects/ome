package endpoint

import (
	"context"
	"net"
	"slices"
	"sort"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Reconciler watches InferenceServices on the control-plane cluster and, when an
// ISVC is Placed (status.placement reports a winner and an addressable
// endpoint), programs the configured Publisher so the global host resolves to
// the winner's ingress. It repoints the backend on re-placement and tears it
// down when the ISVC is no longer placed or is deleted. When the Publisher's
// backend is not configured, Reconcile publishes nothing and releases whatever
// it already owns.
type Reconciler struct {
	client.Client
	Log logr.Logger
	// Publisher is the global-traffic backend. Required.
	Publisher EndpointPublisher
	// Config supplies the config-driven host/gateway/port/namespace inputs.
	Config Config
}

// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices/finalizers,verbs=update
// +kubebuilder:rbac:groups=ome.io,resources=trafficmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=backendtlspolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch;create;update;patch;delete

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	isvc := &v1beta1.InferenceService{}
	if err := r.Get(ctx, req.NamespacedName, isvc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Backend not configured: tear down anything already published — it carries no
	// OwnerReferences, so dropping the finalizer without unpublishing leaks it —
	// and release the object, deleted or not.
	if !r.Config.IsEnabled() {
		if controllerutil.ContainsFinalizer(isvc, EndpointFinalizer) {
			if err := r.Publisher.Unpublish(ctx, isvc); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(isvc, EndpointFinalizer)
			if err := r.Update(ctx, isvc); err != nil {
				return requeueOnConflict(err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !isvc.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, isvc)
	}

	target, ok, err := r.resolveTarget(isvc, r.trafficMapWeights(ctx, isvc))
	if err != nil {
		// A bad global-host template is the operator's config error; surface it
		// and retry on the next change/poll rather than hot-looping.
		r.Log.Error(err, "endpoint: resolve target failed", "isvc", req.String())
		return ctrl.Result{}, nil
	}

	if !ok {
		// Not placed (or no usable host yet): ensure any previously-published
		// backend is removed, then drop the finalizer so an ISVC that never
		// publishes is not held.
		return r.reconcileUnpublish(ctx, isvc)
	}

	if err := r.ensureFinalizer(ctx, isvc); err != nil {
		return requeueOnConflict(err)
	}
	if err := r.Publisher.Publish(ctx, isvc, target); err != nil {
		return ctrl.Result{}, err
	}
	r.Log.Info("endpoint published", "isvc", req.String(), "backend", r.Publisher.Name(),
		"globalHost", target.GlobalHost, "homes", len(target.Homes))
	if r.Config.GatewayBackend.EndpointSlices.Enabled && r.Config.GatewayBackend.EndpointSlices.AddressRefreshInterval > 0 {
		return ctrl.Result{RequeueAfter: r.Config.GatewayBackend.EndpointSlices.AddressRefreshInterval}, nil
	}
	return ctrl.Result{}, nil
}

// resolveTarget builds the publication Target from status.placement. ok is false
// when the ISVC is not in a publishable state: not Placed, no winner cluster, no
// addressable endpoint host yet, or no global host resolvable from config. In
// every not-ok case the caller tears down any stale backend rather than leaving
// the global host pointed at a cluster that no longer wins.
func (r *Reconciler) resolveTarget(isvc *v1beta1.InferenceService, weights map[string]int32) (Target, bool, error) {
	host, err := r.Config.GlobalHostFor(isvc)
	if err != nil {
		return Target{}, false, err
	}
	if host == "" {
		return Target{}, false, nil
	}
	pl := isvc.Status.Placement
	if pl == nil || pl.Phase != v1beta1.PlacementPhasePlaced {
		return Target{}, false, nil
	}
	homes := homesFromPlacement(pl)
	if len(homes) == 0 {
		// Placed but no home is addressable yet — nothing concrete to point at.
		return Target{}, false, nil
	}
	applyTrafficMapWeights(homes, weights)
	return Target{GlobalHost: host, Homes: homes}, true, nil
}

// trafficMapWeights returns the per-cluster apply-verbatim weights the routing
// controller published for the ISVC in its TrafficMap, keyed by cluster, or nil
// when no TrafficMap exists (routing disabled, or not yet generated). The
// TrafficMap is an optional input: a missing map — or a read that fails — leaves
// the reactive ready-replica weights in place, so the publisher works unchanged
// when routing is off.
func (r *Reconciler) trafficMapWeights(ctx context.Context, isvc *v1beta1.InferenceService) map[string]int32 {
	tm := &v1beta1.TrafficMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, tm); err != nil {
		if !apierrors.IsNotFound(err) {
			r.Log.V(1).Info("endpoint: TrafficMap read failed, using reactive weights",
				"isvc", client.ObjectKeyFromObject(isvc).String(), "err", err.Error())
		}
		return nil
	}
	weights := make(map[string]int32, len(tm.Spec.Entries))
	for _, e := range tm.Spec.Entries {
		weights[e.Cluster] = e.Weight
	}
	return weights
}

// applyTrafficMapWeights overlays the TrafficMap's capacity-aware, health-gated
// weights onto the serving homes, replacing the reactive ready-replica weight
// homesFromPlacement seeds. A home absent from the map — or a nil map, when no
// TrafficMap exists — keeps its reactive weight, so publication degrades to the
// live ready-replica split when routing is off or the map lags placement.
func applyTrafficMapWeights(homes []Home, weights map[string]int32) {
	if weights == nil {
		return
	}
	for i := range homes {
		if w, ok := weights[homes[i].Cluster]; ok {
			homes[i].Weight = w
		}
	}
}

// homesFromPlacement extracts the serving homes — admitted candidates that
// report an addressable endpoint — from a placement status, sorted by cluster
// for a deterministic route. This is the uniform source for both Single (one
// winner candidate) and All/Split (one per serving cluster). It falls back to
// the top-level cluster/endpoint for a status that predates per-candidate
// endpoints, so a legacy Single placement still publishes.
func homesFromPlacement(pl *v1beta1.PlacementStatus) []Home {
	var homes []Home
	for i := range pl.Candidates {
		c := &pl.Candidates[i]
		if c.Phase == v1beta1.CandidatePhaseAdmitted && c.Endpoint != nil && c.Endpoint.Host != "" {
			homes = append(homes, Home{Cluster: c.Cluster, BackendHost: hostOnly(c.Endpoint.Host), Weight: c.ReadyReplicas})
		}
	}
	if len(homes) == 0 && pl.Cluster != "" && pl.Endpoint != nil && pl.Endpoint.Host != "" {
		homes = append(homes, Home{Cluster: pl.Cluster, BackendHost: hostOnly(pl.Endpoint.Host)})
	}
	sort.Slice(homes, func(i, j int) bool { return homes[i].Cluster < homes[j].Cluster })
	return homes
}

// hostOnly strips a port from an endpoint host. A URL's Host is "host:port"
// when the endpoint carries an explicit port (e.g.
// "svc.ns.svc.cluster.local:8000"), but the backend ExternalName Service takes a
// BARE hostname — the port is supplied separately from Config.BackendPort — so a
// host:port here makes the Service spec.externalName fail RFC-1123 validation.
func hostOnly(h string) string {
	if host, _, err := net.SplitHostPort(h); err == nil {
		return host
	}
	return h
}

func (r *Reconciler) reconcileUnpublish(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(isvc, EndpointFinalizer) {
		// Never published; nothing to clean and no finalizer to drop.
		return ctrl.Result{}, nil
	}
	if err := r.Publisher.Unpublish(ctx, isvc); err != nil {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(isvc, EndpointFinalizer)
	if err := r.Update(ctx, isvc); err != nil {
		return requeueOnConflict(err)
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) reconcileDelete(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(isvc, EndpointFinalizer) {
		return ctrl.Result{}, nil
	}
	if err := r.Publisher.Unpublish(ctx, isvc); err != nil {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(isvc, EndpointFinalizer)
	if err := r.Update(ctx, isvc); err != nil {
		return requeueOnConflict(err)
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) ensureFinalizer(ctx context.Context, isvc *v1beta1.InferenceService) error {
	if controllerutil.AddFinalizer(isvc, EndpointFinalizer) {
		return r.Update(ctx, isvc)
	}
	return nil
}

// requeueOnConflict turns an optimistic-lock conflict on the ISVC into a clean
// requeue instead of a returned error. The endpoint publisher and the placement
// controller both update the same source ISVC — the placer writes status, this
// writes the finalizer — so the finalizer Update can lose a resourceVersion race
// during the place transition. That is benign and self-corrects on the requeue
// (the next pass re-reads the fresh object), so it must NOT surface as an
// error-level "Reconciler error": that is misleading log noise and trips the
// suite's write-conflict gate. Any other error is returned unchanged.
func requeueOnConflict(err error) (ctrl.Result, error) {
	if apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{}, err
}

// SetupWithManager wires the controller: reconcile ISVCs, but only react to
// events that can change a publication decision — placement-status changes,
// deletions, and the global-host annotation — plus TrafficMap spec changes, so a
// weight-only update (which does not touch ISVC status) still re-publishes.
// Routine ISVC spec churn does not re-publish on every reconcile.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Named explicitly so it doesn't collide with the placement controller
	// (both do For(&InferenceService{})).
	return ctrl.NewControllerManagedBy(mgr).
		Named(PlacementEndpointControllerName).
		For(&v1beta1.InferenceService{}, builder.WithPredicates(placementPublishChange)).
		// A TrafficMap is named after and owner-ref'd to its ISVC, so map a spec
		// change back to that ISVC. Generation-gated: a weight change bumps the
		// spec generation, while any future status write does not, so the publisher
		// re-runs on new weights but not on its own status echo.
		Watches(&v1beta1.TrafficMap{},
			handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(),
				&v1beta1.InferenceService{}, handler.OnlyControllerOwner()),
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}

// placementPublishChange admits ISVC events that can change what the publisher
// programs: any create/delete, and updates that change status.placement (winner,
// phase, or endpoint) or the global-host annotation. Other spec/status churn is
// dropped so the publisher is not re-driven needlessly.
var placementPublishChange = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldISVC, ok1 := e.ObjectOld.(*v1beta1.InferenceService)
		newISVC, ok2 := e.ObjectNew.(*v1beta1.InferenceService)
		if !ok1 || !ok2 {
			return true // unexpected type: fail safe
		}
		if !placementEqual(oldISVC.Status.Placement, newISVC.Status.Placement) {
			return true
		}
		if oldISVC.Annotations[GlobalHostAnnotation] != newISVC.Annotations[GlobalHostAnnotation] {
			return true
		}
		// React to deletion entering (finalizer teardown) even if placement is
		// otherwise unchanged.
		return oldISVC.DeletionTimestamp.IsZero() != newISVC.DeletionTimestamp.IsZero()
	},
}

// placementEqual reports whether two PlacementStatus values agree on the fields
// the publisher consumes: phase, winner cluster, endpoint host, and the
// candidate-derived serving homes (including their weights).
func placementEqual(a, b *v1beta1.PlacementStatus) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Phase != b.Phase || a.Cluster != b.Cluster {
		return false
	}
	return endpointHost(a) == endpointHost(b) && slices.Equal(homesFromPlacement(a), homesFromPlacement(b))
}

func endpointHost(pl *v1beta1.PlacementStatus) string {
	if pl == nil || pl.Endpoint == nil {
		return ""
	}
	return pl.Endpoint.Host
}
