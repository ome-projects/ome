package endpoint

import (
	"context"
	"errors"
	"fmt"
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
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
)

// Reconciler watches InferenceServices and their TrafficMaps on the control
// plane and realizes the Gateway API publication lifecycle.
type Reconciler struct {
	client.Client
	// APIReader provides the authoritative TrafficMap view used to serialize
	// legacy endpoint effects behind TrafficMap publisher cleanup.
	APIReader client.Reader
	Log       logr.Logger
	// Publisher is the Gateway API traffic backend. Required.
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
	handoffPending, err := r.legacyTrafficMapHandoffPending(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if handoffPending {
		return ctrl.Result{}, nil
	}

	// Backend not configured: tear down anything already published — it carries no
	// OwnerReferences, so dropping the finalizer without unpublishing leaks it —
	// and release the object, deleted or not.
	if !r.Config.IsEnabled() {
		if controllerutil.ContainsFinalizer(isvc, EndpointFinalizer) {
			handoffPending, err := r.legacyTrafficMapHandoffPending(ctx, client.ObjectKeyFromObject(isvc))
			if err != nil {
				return ctrl.Result{}, err
			}
			if handoffPending {
				return ctrl.Result{}, nil
			}
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

	target, ok, err := r.resolveTarget(isvc, nil)
	if err != nil {
		// An invalid global-host template is an operator configuration error;
		// surface it and retry on the next change rather than hot-looping.
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
	handoffPending, err = r.legacyTrafficMapHandoffPending(ctx, client.ObjectKeyFromObject(isvc))
	if err != nil {
		return ctrl.Result{}, err
	}
	if handoffPending {
		return ctrl.Result{}, nil
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

func routingOptedOut(isvc *v1beta1.InferenceService) bool {
	return isvc.Spec.Routing != nil && isvc.Spec.Routing.Enabled != nil && !*isvc.Spec.Routing.Enabled
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
	weightsAuthoritative := applyTrafficMapWeights(homes, weights)
	return Target{
		Service:              isvc.Name,
		GlobalHost:           host,
		Homes:                homes,
		WeightsAuthoritative: weightsAuthoritative,
	}, true, nil
}

// legacyTrafficMapHandoffPending reads the authoritative handoff state for the
// legacy publisher. A publisher finalizer or durable publisher-owned status
// blocks all legacy effects. TrafficMap weights belong exclusively to the
// TrafficMap publisher path; the legacy path always uses placement weights.
func (r *Reconciler) legacyTrafficMapHandoffPending(
	ctx context.Context,
	key types.NamespacedName,
) (bool, error) {
	if r.APIReader == nil {
		return false, errors.New("endpoint publisher API reader is not configured")
	}
	tm := &v1beta1.TrafficMap{}
	if err := r.APIReader.Get(ctx, key, tm); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read TrafficMap handoff state for %s: %w", key, err)
	}
	return trafficMapBlocksLegacyPublisher(tm), nil
}

func trafficMapBlocksLegacyPublisher(trafficMap *v1beta1.TrafficMap) bool {
	return trafficMap != nil &&
		(controllerutil.ContainsFinalizer(trafficMap, TrafficMapPublisherFinalizer) ||
			publisherStatusNeedsFinalizer(trafficMap.Status, true))
}

// applyTrafficMapWeights overlays the TrafficMap's capacity-aware, health-gated
// weights onto the serving homes, replacing the reactive ready-replica weight
// homesFromPlacement seeds. A home absent from the map — or a nil map, when no
// TrafficMap exists — keeps its reactive weight, so publication degrades to the
// live ready-replica split when routing is off or the map lags placement. The
// return value is true only when every current home had an explicit map entry;
// only that complete table may authorize an all-zero route.
func applyTrafficMapWeights(homes []Home, weights map[string]int32) bool {
	if weights == nil {
		return false
	}
	complete := true
	for i := range homes {
		if w, ok := weights[homes[i].Cluster]; ok {
			homes[i].Weight = w
		} else {
			complete = false
		}
	}
	return complete
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
			homes = append(homes, Home{
				Cluster: c.Cluster, Endpoint: c.Endpoint.String(),
				BackendHost: hostOnly(c.Endpoint.Host), Weight: c.ReadyReplicas,
			})
		}
	}
	if len(homes) == 0 && pl.Cluster != "" && pl.Endpoint != nil && pl.Endpoint.Host != "" {
		homes = append(homes, Home{
			Cluster: pl.Cluster, Endpoint: pl.Endpoint.String(), BackendHost: hostOnly(pl.Endpoint.Host),
		})
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
	handoffPending, err := r.legacyTrafficMapHandoffPending(ctx, client.ObjectKeyFromObject(isvc))
	if err != nil {
		return ctrl.Result{}, err
	}
	if handoffPending {
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
	handoffPending, err := r.legacyTrafficMapHandoffPending(ctx, client.ObjectKeyFromObject(isvc))
	if err != nil {
		return ctrl.Result{}, err
	}
	if handoffPending {
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
// deletions, and the global-host annotation — plus TrafficMap publication-input
// changes. Routine ISVC and TrafficMap provenance churn does not re-publish.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		return fmt.Errorf("endpoint publisher API reader is not configured")
	}
	// Named explicitly so it doesn't collide with the placement controller
	// (both do For(&InferenceService{})).
	return ctrl.NewControllerManagedBy(mgr).
		Named(PlacementEndpointControllerName).
		For(&v1beta1.InferenceService{}, builder.WithPredicates(placementPublishChange)).
		// A TrafficMap is named after its ISVC. Map by that stable key instead
		// of its owner reference, which orphan deletion propagation may remove.
		Watches(&v1beta1.TrafficMap{},
			handler.EnqueueRequestsFromMapFunc(enqueueTrafficMapISVC),
			builder.WithPredicates(trafficMapPublishChange)).
		Complete(r)
}

func enqueueTrafficMapISVC(_ context.Context, object client.Object) []ctrl.Request {
	if _, ok := object.(*v1beta1.TrafficMap); !ok {
		return nil
	}
	return []ctrl.Request{{NamespacedName: client.ObjectKeyFromObject(object)}}
}

// trafficMapPublishChange admits TrafficMap events that can change the
// publisher's desired target. Entry order and routing provenance do not affect
// publication; service, cluster, endpoint, and weight do.
var trafficMapPublishChange = predicate.Funcs{
	CreateFunc: func(event.CreateEvent) bool { return true },
	DeleteFunc: func(event.DeleteEvent) bool { return true },
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldTrafficMap, ok1 := e.ObjectOld.(*v1beta1.TrafficMap)
		newTrafficMap, ok2 := e.ObjectNew.(*v1beta1.TrafficMap)
		if !ok1 || !ok2 || oldTrafficMap == nil || newTrafficMap == nil {
			return true // unexpected type: fail safe
		}
		if trafficMapBlocksLegacyPublisher(oldTrafficMap) != trafficMapBlocksLegacyPublisher(newTrafficMap) ||
			oldTrafficMap.DeletionTimestamp.IsZero() != newTrafficMap.DeletionTimestamp.IsZero() {
			return true
		}
		return !trafficMapPublicationEqual(oldTrafficMap, newTrafficMap)
	},
}

type trafficMapPublicationEntry struct {
	cluster  string
	endpoint string
	weight   int32
}

func trafficMapPublicationEqual(a, b *v1beta1.TrafficMap) bool {
	if a.Spec.Service != b.Spec.Service || len(a.Spec.Entries) != len(b.Spec.Entries) {
		return false
	}
	return slices.Equal(trafficMapPublicationEntries(a), trafficMapPublicationEntries(b))
}

func trafficMapPublicationEntries(tm *v1beta1.TrafficMap) []trafficMapPublicationEntry {
	entries := make([]trafficMapPublicationEntry, 0, len(tm.Spec.Entries))
	for i := range tm.Spec.Entries {
		entry := &tm.Spec.Entries[i]
		endpoint := ""
		if entry.Endpoint != nil {
			endpoint = entry.Endpoint.String()
		}
		entries = append(entries, trafficMapPublicationEntry{
			cluster:  entry.Cluster,
			endpoint: endpoint,
			weight:   entry.Weight,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].cluster != entries[j].cluster {
			return entries[i].cluster < entries[j].cluster
		}
		if entries[i].endpoint != entries[j].endpoint {
			return entries[i].endpoint < entries[j].endpoint
		}
		return entries[i].weight < entries[j].weight
	})
	return entries
}

// placementPublishChange admits ISVC events that can change what the publisher
// programs: any create/delete, and updates that change placement eligibility,
// status.placement (winner, phase, or endpoint), or the global-host annotation.
// Other spec/status churn is dropped so the publisher is not re-driven
// needlessly.
var placementPublishChange = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldISVC, ok1 := e.ObjectOld.(*v1beta1.InferenceService)
		newISVC, ok2 := e.ObjectNew.(*v1beta1.InferenceService)
		if !ok1 || !ok2 {
			return true // unexpected type: fail safe
		}
		if placementcontroller.IsPlacementEligible(oldISVC) != placementcontroller.IsPlacementEligible(newISVC) {
			return true
		}
		if routingOptedOut(oldISVC) != routingOptedOut(newISVC) {
			return true
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
