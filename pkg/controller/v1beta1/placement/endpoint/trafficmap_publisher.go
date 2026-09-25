package endpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
)

const (
	// TrafficMapPublisherControllerName identifies the controller that realizes
	// TrafficMaps through an explicitly selected publisher.
	TrafficMapPublisherControllerName = "trafficmap-publisher"

	// TrafficMapPublisherFinalizer preserves the TrafficMap and its claim journal
	// until the selected publisher has removed every external side effect.
	TrafficMapPublisherFinalizer = placementcontroller.TrafficMapPublisherFinalizer
)

const (
	trafficMapPublisherReasonPublished        = "Published"
	trafficMapPublisherReasonWithdrawn        = "Withdrawn"
	trafficMapPublisherReasonInvalidOwner     = "InvalidOwner"
	trafficMapPublisherReasonInvalidOptions   = "InvalidOptions"
	trafficMapPublisherReasonInvalidPlan      = "InvalidPlan"
	trafficMapPublisherReasonLegacyActive     = "LegacyLifecycleActive"
	trafficMapPublisherReasonClaimRejected    = "ClaimRejected"
	trafficMapPublisherReasonDrainFailed      = "DrainFailed"
	trafficMapPublisherReasonApplyFailed      = "ApplyFailed"
	trafficMapPublisherReasonUnpublishFailed  = "UnpublishFailed"
	trafficMapPublisherReasonPublisherChanged = "PublisherChanged"
	trafficMapPublisherReasonUnpublished      = "Unpublished"
	trafficMapPublisherOptionsDigestVersion   = "v1"
	trafficMapPublisherStatusFieldOwner       = "ome-trafficmap-publisher"
)

// TrafficMapPublishPlan is an immutable, publisher-specific publication plan.
// Claims returns the complete canonical target set the plan may mutate. The
// controller persists that set before passing the plan back to Apply.
type TrafficMapPublishPlan interface {
	Claims() []string
}

// TrafficMapPublishResult reports the outcome of a successfully applied plan.
type TrafficMapPublishResult struct {
	GatewayRef *v1beta1.TrafficMapGatewayRef
	// Withdrawn reports that Apply successfully removed or withheld publication.
	// The controller records the generation and options digest while keeping the
	// durable claim journal, but reports Published=False with no GatewayRef.
	Withdrawn bool
}

// TrafficMapPublisher realizes TrafficMaps through one shared publisher
// instance. Plan and ResolveOptions must be pure: they validate and construct
// immutable per-reconcile values but perform no external mutation.
//
// Drain keeps retired claims owned at zero while Apply realizes the current
// plan. The source key lets a publisher verify target ownership without reading
// the owner. Unpublish removes every external override named by a deletion
// journal; it must need neither the TrafficMap nor its owner to remain readable.
type TrafficMapPublisher interface {
	Name() string
	// Stateful reports whether publication creates effects that require a claim
	// journal, finalizer cleanup, and leader-serialized reconciliation. A false
	// result is reserved for consumers with no external cleanup obligation.
	Stateful() bool
	ResolveOptions(global, inline map[string]string) (map[string]string, error)
	Plan(
		owner *v1beta1.InferenceService,
		trafficMap *v1beta1.TrafficMap,
		effectiveOptions map[string]string,
	) (TrafficMapPublishPlan, error)
	Drain(ctx context.Context, key types.NamespacedName, claims []string) error
	Apply(ctx context.Context, plan TrafficMapPublishPlan) (TrafficMapPublishResult, error)
	Unpublish(
		ctx context.Context,
		key types.NamespacedName,
		journal v1beta1.TrafficMapPublisherStatus,
	) error
}

// trafficMapPublisherPreflighter is an optional in-package hook for publishers
// that must validate shared external state after their durable claims are
// persisted but before any drain or apply mutation.
type trafficMapPublisherPreflighter interface {
	Preflight(context.Context, TrafficMapPublishPlan) error
}

// TrafficMapPublisherReconciler owns the Kubernetes lifecycle around one
// explicitly selected publisher. Stateful publishers are serialized because
// the uncached claim scan and resource-version-guarded status write form their
// collision boundary.
type TrafficMapPublisherReconciler struct {
	client.Client
	APIReader     client.Reader
	Log           logr.Logger
	Publisher     TrafficMapPublisher
	GlobalOptions map[string]string
	// RequeueAfter is the per-object resync period. Active stateful publishers
	// require a positive value so a map blocked by another map's claim is retried
	// after that claim is released.
	RequeueAfter time.Duration
	// Active is the installation-level gate. The routing controller owns map
	// deletion; an inactive publisher waits for that deletion and only finalizes.
	Active                bool
	LeaderElectionEnabled bool
}

// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=trafficmaps,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=trafficmaps/finalizers,verbs=update
// +kubebuilder:rbac:groups=ome.io,resources=trafficmaps/status,verbs=get;update;patch

func (r *TrafficMapPublisherReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Publisher == nil {
		return ctrl.Result{}, fmt.Errorf("TrafficMap publisher is not configured")
	}
	if r.APIReader == nil {
		return ctrl.Result{}, fmt.Errorf("TrafficMap publisher API reader is not configured")
	}
	if err := validatePublisherName(r.Publisher.Name()); err != nil {
		return ctrl.Result{}, err
	}

	trafficMap := &v1beta1.TrafficMap{}
	if err := r.Get(ctx, req.NamespacedName, trafficMap); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !trafficMap.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, trafficMap)
	}
	// Claims are a durable record of external state. Repair a missing finalizer
	// before any gate or validation can stop reconciliation and leave that state
	// unprotected from deletion.
	if publisherStatusNeedsFinalizer(trafficMap.Status, r.Publisher.Stateful()) &&
		!controllerutil.ContainsFinalizer(trafficMap, TrafficMapPublisherFinalizer) {
		added, err := r.ensureFinalizer(ctx, trafficMap)
		if err != nil {
			return ctrl.Result{}, err
		}
		if added {
			return ctrl.Result{Requeue: true}, nil
		}
	}
	if !r.Active {
		return r.successResult(), nil
	}

	owner, err := r.owner(ctx, trafficMap)
	if err != nil {
		if errors.Is(err, errTrafficMapPublisherOwnerRead) {
			return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonInvalidOwner, err)
		}
		return r.reject(ctx, trafficMap, trafficMapPublisherReasonInvalidOwner, err)
	}
	if !publisherOwnerActive(owner) {
		return r.successResult(), nil
	}
	if r.Publisher.Stateful() && (trafficMap.Status.Publisher == nil ||
		trafficMap.Status.Publisher.PublisherName != r.Publisher.Name()) {
		legacyOwners, err := r.legacyEndpointOwners(ctx)
		if err != nil {
			return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonLegacyActive,
				fmt.Errorf("check legacy endpoint publication state: %w", err))
		}
		if legacyOwners != 0 {
			return r.reject(ctx, trafficMap, trafficMapPublisherReasonLegacyActive, fmt.Errorf(
				"%d InferenceService(s) retain the legacy endpoint publisher finalizer; complete their cleanup before publishing TrafficMaps",
				legacyOwners,
			))
		}
	}
	inlineOptions := routingPublisherOptions(owner)
	effectiveOptions, err := r.Publisher.ResolveOptions(
		clonePublisherOptions(r.GlobalOptions),
		clonePublisherOptions(inlineOptions),
	)
	if err != nil {
		return r.reject(ctx, trafficMap, trafficMapPublisherReasonInvalidOptions, err)
	}
	effectiveOptions = clonePublisherOptions(effectiveOptions)

	plan, err := r.Publisher.Plan(owner.DeepCopy(), trafficMap.DeepCopy(), clonePublisherOptions(effectiveOptions))
	if err != nil {
		return r.reject(ctx, trafficMap, trafficMapPublisherReasonInvalidPlan, err)
	}
	if plan == nil {
		return r.reject(ctx, trafficMap, trafficMapPublisherReasonInvalidPlan,
			fmt.Errorf("TrafficMap publisher %q returned a nil plan", r.Publisher.Name()))
	}
	desiredClaims, err := normalizePublisherClaims(plan.Claims())
	if err != nil {
		return r.reject(ctx, trafficMap, trafficMapPublisherReasonInvalidPlan, err)
	}

	if !r.Publisher.Stateful() {
		if len(desiredClaims) != 0 {
			return r.reject(ctx, trafficMap, trafficMapPublisherReasonInvalidPlan,
				fmt.Errorf("stateless TrafficMap publisher %q planned %d target claims",
					r.Publisher.Name(), len(desiredClaims)))
		}
		if err := r.preflightStateless(ctx, trafficMap, owner); err != nil {
			if errors.Is(err, errTrafficMapPublisherPlanStale) {
				return ctrl.Result{Requeue: true}, nil
			}
			if errors.Is(err, errTrafficMapPublisherOwnerRead) {
				return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonInvalidOwner, err)
			}
			if errors.Is(err, errTrafficMapPublisherSourceChanged) {
				return r.reject(ctx, trafficMap, trafficMapPublisherReasonInvalidOwner, err)
			}
			if errors.Is(err, errTrafficMapPublisherChanged) {
				return r.reject(ctx, trafficMap, trafficMapPublisherReasonPublisherChanged, err)
			}
			var terminal *terminalPublisherError
			if errors.As(err, &terminal) {
				return r.reject(ctx, trafficMap, trafficMapPublisherReasonClaimRejected, terminal.err)
			}
			return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonClaimRejected, err)
		}
		result, err := r.Publisher.Apply(ctx, plan)
		if err != nil {
			return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonApplyFailed, err)
		}
		if err := r.markPublished(ctx, client.ObjectKeyFromObject(trafficMap), trafficMap.UID,
			trafficMap.Generation, nil, effectiveOptions, result); err != nil {
			if errors.Is(err, errTrafficMapPublisherJournalChanged) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
		return r.successResult(), nil
	}

	added, err := r.ensureFinalizer(ctx, trafficMap)
	if err != nil {
		return ctrl.Result{}, err
	}
	if added {
		return ctrl.Result{Requeue: true}, nil
	}

	previousClaims, err := r.preclaim(ctx, trafficMap, owner, desiredClaims)
	if err != nil {
		if errors.Is(err, errTrafficMapPublisherPlanStale) {
			return ctrl.Result{Requeue: true}, nil
		}
		if errors.Is(err, errTrafficMapPublisherOwnerRead) {
			return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonInvalidOwner, err)
		}
		if errors.Is(err, errTrafficMapPublisherSourceChanged) {
			return r.reject(ctx, trafficMap, trafficMapPublisherReasonInvalidOwner, err)
		}
		if errors.Is(err, errTrafficMapPublisherChanged) {
			return r.reject(ctx, trafficMap, trafficMapPublisherReasonPublisherChanged, err)
		}
		var terminal *terminalPublisherError
		if errors.As(err, &terminal) {
			return r.reject(ctx, trafficMap, trafficMapPublisherReasonClaimRejected, terminal.err)
		}
		return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonClaimRejected, err)
	}
	if preflighter, ok := r.Publisher.(trafficMapPublisherPreflighter); ok {
		if err := preflighter.Preflight(ctx, plan); err != nil {
			var terminal *terminalPublisherError
			if errors.As(err, &terminal) {
				return r.reject(ctx, trafficMap, trafficMapPublisherReasonClaimRejected, terminal.err)
			}
			return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonClaimRejected, err)
		}
	}
	expectedClaims := publisherClaimUnion(previousClaims, desiredClaims)
	staleClaims := publisherClaimDifference(previousClaims, desiredClaims)
	if len(staleClaims) != 0 {
		if err := r.Publisher.Drain(ctx, client.ObjectKeyFromObject(trafficMap), slices.Clone(staleClaims)); err != nil {
			return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonDrainFailed, err)
		}
	}
	result, err := r.Publisher.Apply(ctx, plan)
	if err != nil {
		return r.retryFailure(ctx, trafficMap, trafficMapPublisherReasonApplyFailed, err)
	}
	if err := r.markPublished(ctx, client.ObjectKeyFromObject(trafficMap), trafficMap.UID,
		trafficMap.Generation, expectedClaims, effectiveOptions, result); err != nil {
		if errors.Is(err, errTrafficMapPublisherJournalChanged) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return r.successResult(), nil
}

func (r *TrafficMapPublisherReconciler) owner(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
) (*v1beta1.InferenceService, error) {
	key := client.ObjectKeyFromObject(trafficMap)
	live := &v1beta1.TrafficMap{}
	if err := r.APIReader.Get(ctx, key, live); err != nil {
		return nil, fmt.Errorf("%w: read TrafficMap %s provenance: %w",
			errTrafficMapPublisherOwnerRead, key, err)
	}
	if live.UID != trafficMap.UID {
		return nil, fmt.Errorf("%w: TrafficMap %s was replaced while validating source provenance",
			errTrafficMapPublisherOwnerInvalid, key)
	}
	return r.readTrafficMapOwner(ctx, live)
}

func (r *TrafficMapPublisherReconciler) readTrafficMapOwner(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
) (*v1beta1.InferenceService, error) {
	ref := metav1.GetControllerOf(trafficMap)
	if ref == nil || ref.APIVersion != v1beta1.SchemeGroupVersion.String() ||
		ref.Kind != "InferenceService" || ref.Name == "" || ref.UID == "" {
		return nil, fmt.Errorf("%w: TrafficMap %s has no valid InferenceService controller reference",
			errTrafficMapPublisherOwnerInvalid, client.ObjectKeyFromObject(trafficMap))
	}
	if trafficMap.Name != ref.Name {
		return nil, fmt.Errorf("%w: TrafficMap name %q does not match owner name %q",
			errTrafficMapPublisherOwnerInvalid, trafficMap.Name, ref.Name)
	}
	if trafficMap.Spec.Service != ref.Name {
		return nil, fmt.Errorf("%w: TrafficMap service %q does not match owner name %q",
			errTrafficMapPublisherOwnerInvalid, trafficMap.Spec.Service, ref.Name)
	}
	owner := &v1beta1.InferenceService{}
	ownerKey := types.NamespacedName{Namespace: trafficMap.Namespace, Name: ref.Name}
	if err := r.APIReader.Get(ctx, ownerKey, owner); err != nil {
		return nil, fmt.Errorf("%w: read TrafficMap owner %s: %w",
			errTrafficMapPublisherOwnerRead, ownerKey, err)
	}
	if owner.UID != ref.UID {
		return nil, fmt.Errorf("%w: TrafficMap %s owner UID %q does not match InferenceService UID %q",
			errTrafficMapPublisherOwnerInvalid, client.ObjectKeyFromObject(trafficMap), ref.UID, owner.UID)
	}
	if err := validatePublisherSourceUID(trafficMap, owner.UID); err != nil {
		return nil, err
	}
	if trafficMap.Spec.ObservedISVCGeneration != owner.Generation {
		return nil, fmt.Errorf(
			"%w: TrafficMap observed InferenceService generation %d does not match owner generation %d",
			errTrafficMapPublisherOwnerInvalid, trafficMap.Spec.ObservedISVCGeneration, owner.Generation,
		)
	}
	return owner, nil
}

func publisherOwnerActive(owner *v1beta1.InferenceService) bool {
	return owner != nil && owner.DeletionTimestamp.IsZero() &&
		placementcontroller.IsPlacementEligible(owner) && !routingOptedOut(owner)
}

func (r *TrafficMapPublisherReconciler) validateLivePublisherOwner(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	expected *v1beta1.InferenceService,
) error {
	owner, err := r.readTrafficMapOwner(ctx, trafficMap)
	if err != nil {
		if errors.Is(err, errTrafficMapPublisherOwnerRead) {
			return err
		}
		return fmt.Errorf("%w: %v", errTrafficMapPublisherSourceChanged, err)
	}
	if owner.UID != expected.UID || owner.Generation != expected.Generation {
		return fmt.Errorf(
			"%w: InferenceService %s identity changed from UID %q generation %d to UID %q generation %d",
			errTrafficMapPublisherSourceChanged, client.ObjectKeyFromObject(owner),
			expected.UID, expected.Generation, owner.UID, owner.Generation,
		)
	}
	if !equality.Semantic.DeepEqual(owner.Labels, expected.Labels) ||
		!equality.Semantic.DeepEqual(owner.Annotations, expected.Annotations) ||
		!equality.Semantic.DeepEqual(owner.Finalizers, expected.Finalizers) {
		return fmt.Errorf("%w: InferenceService %s publication metadata changed while planning",
			errTrafficMapPublisherSourceChanged, client.ObjectKeyFromObject(owner))
	}
	if !publisherOwnerActive(owner) {
		return fmt.Errorf("%w: InferenceService %s is deleting, opted out, or not placement eligible",
			errTrafficMapPublisherSourceChanged, client.ObjectKeyFromObject(owner))
	}
	return nil
}

func (r *TrafficMapPublisherReconciler) ensureFinalizer(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
) (bool, error) {
	if controllerutil.ContainsFinalizer(trafficMap, TrafficMapPublisherFinalizer) {
		return false, nil
	}
	key := client.ObjectKeyFromObject(trafficMap)
	added := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		live := &v1beta1.TrafficMap{}
		if err := r.APIReader.Get(ctx, key, live); err != nil {
			return err
		}
		if live.UID != trafficMap.UID {
			return fmt.Errorf("TrafficMap %s was replaced while adding its publisher finalizer", key)
		}
		if !live.DeletionTimestamp.IsZero() {
			return fmt.Errorf("TrafficMap %s entered deletion before its publisher finalizer was added", key)
		}
		if controllerutil.ContainsFinalizer(live, TrafficMapPublisherFinalizer) {
			return nil
		}
		base := live.DeepCopy()
		controllerutil.AddFinalizer(live, TrafficMapPublisherFinalizer)
		if err := r.Patch(ctx, live, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		added = true
		return nil
	})
	return added, err
}

func (r *TrafficMapPublisherReconciler) preclaim(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	owner *v1beta1.InferenceService,
	desiredClaims []string,
) ([]string, error) {
	key := client.ObjectKeyFromObject(trafficMap)
	var previousClaims []string
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		live := &v1beta1.TrafficMap{}
		if err := r.APIReader.Get(ctx, key, live); err != nil {
			return fmt.Errorf("%w: read TrafficMap %s before claiming targets: %w",
				errTrafficMapPublisherOwnerRead, key, err)
		}
		if live.UID != trafficMap.UID {
			return fmt.Errorf("%w: TrafficMap %s was replaced while claiming publisher targets",
				errTrafficMapPublisherSourceChanged, key)
		}
		if live.Generation != trafficMap.Generation || !live.DeletionTimestamp.IsZero() {
			return errTrafficMapPublisherPlanStale
		}
		if err := r.validateLivePublisherOwner(ctx, live, owner); err != nil {
			return err
		}
		if !controllerutil.ContainsFinalizer(live, TrafficMapPublisherFinalizer) {
			return errTrafficMapPublisherPlanStale
		}

		existing, digest, err := publisherJournalForName(live.Status.Publisher, r.Publisher.Name())
		if err != nil {
			return &terminalPublisherError{err: err}
		}
		union := publisherClaimUnion(existing, desiredClaims)
		if len(union) > v1beta1.MaxTrafficMapPublisherTargets {
			return &terminalPublisherError{err: fmt.Errorf(
				"publisher claim union has %d targets, maximum is %d",
				len(union), v1beta1.MaxTrafficMapPublisherTargets)}
		}
		if err := r.rejectClaimCollisions(ctx, live, union); err != nil {
			return err
		}
		previousClaims = slices.Clone(existing)
		return r.patchPublisherStatus(ctx, live, func(status *v1beta1.TrafficMapStatus) error {
			status.Publisher = &v1beta1.TrafficMapPublisherStatus{
				PublisherName:         r.Publisher.Name(),
				ClaimedTargets:        slices.Clone(union),
				ObservedOptionsDigest: digest,
			}
			return nil
		})
	})
	if errors.Is(err, errTrafficMapPublisherPlanStale) {
		return nil, errTrafficMapPublisherPlanStale
	}
	return previousClaims, err
}

func (r *TrafficMapPublisherReconciler) preflightStateless(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	owner *v1beta1.InferenceService,
) error {
	key := client.ObjectKeyFromObject(trafficMap)
	live := &v1beta1.TrafficMap{}
	if err := r.APIReader.Get(ctx, key, live); err != nil {
		return fmt.Errorf("%w: read TrafficMap %s before stateless apply: %w",
			errTrafficMapPublisherOwnerRead, key, err)
	}
	if live.UID != trafficMap.UID {
		return fmt.Errorf("%w: TrafficMap %s was replaced while validating publisher state",
			errTrafficMapPublisherSourceChanged, key)
	}
	if live.Generation != trafficMap.Generation || !live.DeletionTimestamp.IsZero() {
		return errTrafficMapPublisherPlanStale
	}
	if err := r.validateLivePublisherOwner(ctx, live, owner); err != nil {
		return err
	}
	existingClaims, _, err := publisherJournalForName(live.Status.Publisher, r.Publisher.Name())
	if err != nil {
		return &terminalPublisherError{err: err}
	}
	if len(existingClaims) != 0 {
		return &terminalPublisherError{err: fmt.Errorf(
			"stateless TrafficMap publisher %q cannot adopt %d existing target claims",
			r.Publisher.Name(), len(existingClaims),
		)}
	}
	return r.rejectClaimCollisions(ctx, live, nil)
}

func (r *TrafficMapPublisherReconciler) rejectClaimCollisions(
	ctx context.Context,
	self *v1beta1.TrafficMap,
	claims []string,
) error {
	wanted := make(map[string]struct{}, len(claims))
	for _, claim := range claims {
		wanted[claim] = struct{}{}
	}
	maps := &v1beta1.TrafficMapList{}
	if err := r.APIReader.List(ctx, maps); err != nil {
		return fmt.Errorf("list TrafficMaps for publisher claim ownership: %w", err)
	}
	for i := range maps.Items {
		other := &maps.Items[i]
		if other.UID == self.UID || other.Status.Publisher == nil {
			continue
		}
		otherClaims, err := normalizePublisherClaims(other.Status.Publisher.ClaimedTargets)
		if err != nil {
			return &terminalPublisherError{err: fmt.Errorf(
				"TrafficMap %s has an invalid publisher journal: %w",
				client.ObjectKeyFromObject(other), err)}
		}
		if len(otherClaims) != 0 && other.Status.Publisher.PublisherName != r.Publisher.Name() {
			return &terminalPublisherError{err: fmt.Errorf(
				"TrafficMap %s retains claims for publisher %q while publisher %q is selected",
				client.ObjectKeyFromObject(other), other.Status.Publisher.PublisherName, r.Publisher.Name())}
		}
		for _, claim := range otherClaims {
			if _, collision := wanted[claim]; collision {
				return &terminalPublisherError{err: fmt.Errorf(
					"publisher target %q is already claimed by TrafficMap %s",
					claim, client.ObjectKeyFromObject(other))}
			}
		}
	}
	return nil
}

// legacyEndpointOwners fences first adoption by a stateful TrafficMap publisher
// from side effects owned by the InferenceService lifecycle. That lifecycle
// adds its finalizer before publication and removes it only after cleanup.
func (r *TrafficMapPublisherReconciler) legacyEndpointOwners(ctx context.Context) (int, error) {
	services := &v1beta1.InferenceServiceList{}
	if err := r.APIReader.List(ctx, services); err != nil {
		return 0, err
	}
	count := 0
	for i := range services.Items {
		if controllerutil.ContainsFinalizer(&services.Items[i], EndpointFinalizer) {
			count++
		}
	}
	return count, nil
}

func (r *TrafficMapPublisherReconciler) finalize(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
) (ctrl.Result, error) {
	key := client.ObjectKeyFromObject(trafficMap)
	live := &v1beta1.TrafficMap{}
	if err := r.APIReader.Get(ctx, key, live); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if live.UID != trafficMap.UID {
		return ctrl.Result{}, fmt.Errorf("TrafficMap %s was replaced while finalizing publisher state", key)
	}
	if !controllerutil.ContainsFinalizer(live, TrafficMapPublisherFinalizer) {
		return ctrl.Result{}, nil
	}
	journal := v1beta1.TrafficMapPublisherStatus{PublisherName: r.Publisher.Name()}
	if live.Status.Publisher != nil {
		journal = *live.Status.Publisher.DeepCopy()
	}
	claims, digest, err := publisherJournalForName(&journal, r.Publisher.Name())
	if err != nil {
		return r.reject(ctx, live, trafficMapPublisherReasonPublisherChanged, err)
	}
	journal.PublisherName = r.Publisher.Name()
	journal.ClaimedTargets = claims
	journal.ObservedOptionsDigest = digest
	if err := r.Publisher.Unpublish(ctx, key, *journal.DeepCopy()); err != nil {
		return r.retryFailure(ctx, live, trafficMapPublisherReasonUnpublishFailed, err)
	}
	if err := r.clearPublisherJournal(ctx, key, live.UID, journal); err != nil {
		if errors.Is(err, errTrafficMapPublisherJournalChanged) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	if err := r.removeFinalizer(ctx, key, live.UID); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TrafficMapPublisherReconciler) clearPublisherJournal(
	ctx context.Context,
	key types.NamespacedName,
	uid types.UID,
	expected v1beta1.TrafficMapPublisherStatus,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		live := &v1beta1.TrafficMap{}
		if err := r.APIReader.Get(ctx, key, live); err != nil {
			return client.IgnoreNotFound(err)
		}
		if live.UID != uid {
			return fmt.Errorf("TrafficMap %s was replaced while clearing its publisher journal", key)
		}
		current := v1beta1.TrafficMapPublisherStatus{PublisherName: r.Publisher.Name()}
		if live.Status.Publisher != nil {
			current = *live.Status.Publisher.DeepCopy()
		}
		claims, digest, err := publisherJournalForName(&current, r.Publisher.Name())
		if err != nil {
			return fmt.Errorf("%w: %v", errTrafficMapPublisherJournalChanged, err)
		}
		current.PublisherName = r.Publisher.Name()
		current.ClaimedTargets = claims
		current.ObservedOptionsDigest = digest
		if !equality.Semantic.DeepEqual(current, expected) {
			return fmt.Errorf("%w: journal changed after unpublish", errTrafficMapPublisherJournalChanged)
		}
		if err := r.removePublisherStatusPointers(ctx, live); err != nil {
			return err
		}
		cleared := &v1beta1.TrafficMap{}
		if err := r.APIReader.Get(ctx, key, cleared); err != nil {
			return client.IgnoreNotFound(err)
		}
		if cleared.UID != uid {
			return fmt.Errorf("TrafficMap %s was replaced while clearing publisher status", key)
		}
		if cleared.Status.Publisher != nil || cleared.Status.GatewayRef != nil {
			return fmt.Errorf("%w: publisher status changed during cleanup", errTrafficMapPublisherJournalChanged)
		}
		return r.patchPublisherStatus(ctx, cleared, func(status *v1beta1.TrafficMapStatus) error {
			status.Publisher = nil
			status.Published = false
			status.GatewayRef = nil
			status.ObservedTrafficMapGeneration = 0
			condition := metav1.Condition{
				Type:               v1beta1.TrafficMapPublished,
				Status:             metav1.ConditionFalse,
				Reason:             trafficMapPublisherReasonUnpublished,
				Message:            "TrafficMap publisher state is removed",
				ObservedGeneration: cleared.Generation,
			}
			apimeta.SetStatusCondition(&status.Conditions, condition)
			return nil
		})
	})
}

// removePublisherStatusPointers uses a narrow merge patch because an object
// restored with an existing journal may not yet have server-side-apply field
// ownership. The resourceVersion precondition makes removal conditional on the
// exact journal that Unpublish just consumed without touching sibling fields.
func (r *TrafficMapPublisherReconciler) removePublisherStatusPointers(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
) error {
	if trafficMap.Status.Publisher == nil && trafficMap.Status.GatewayRef == nil {
		return nil
	}
	desired := trafficMap.DeepCopy()
	desired.Status.Publisher = nil
	desired.Status.GatewayRef = nil
	return r.Status().Patch(ctx, desired,
		client.MergeFromWithOptions(trafficMap, client.MergeFromWithOptimisticLock{}))
}

func (r *TrafficMapPublisherReconciler) removeFinalizer(
	ctx context.Context,
	key types.NamespacedName,
	uid types.UID,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		live := &v1beta1.TrafficMap{}
		if err := r.APIReader.Get(ctx, key, live); err != nil {
			return client.IgnoreNotFound(err)
		}
		if live.UID != uid {
			return fmt.Errorf("TrafficMap %s was replaced while removing its publisher finalizer", key)
		}
		if live.Status.Publisher != nil || live.Status.GatewayRef != nil || live.Status.Published ||
			live.Status.ObservedTrafficMapGeneration != 0 {
			return fmt.Errorf("TrafficMap %s still has publisher-owned status", key)
		}
		if !controllerutil.ContainsFinalizer(live, TrafficMapPublisherFinalizer) {
			return nil
		}
		base := live.DeepCopy()
		controllerutil.RemoveFinalizer(live, TrafficMapPublisherFinalizer)
		return r.Patch(ctx, live, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	})
}

func (r *TrafficMapPublisherReconciler) markPublished(
	ctx context.Context,
	key types.NamespacedName,
	uid types.UID,
	generation int64,
	expectedClaims []string,
	effectiveOptions map[string]string,
	result TrafficMapPublishResult,
) error {
	digest, err := publisherOptionsDigest(r.Publisher.Name(), effectiveOptions)
	if err != nil {
		return err
	}
	return r.patchStatusByKey(ctx, key, uid, func(status *v1beta1.TrafficMapStatus) error {
		liveClaims, _, journalErr := publisherJournalForName(status.Publisher, r.Publisher.Name())
		if journalErr != nil {
			return fmt.Errorf("%w: %v", errTrafficMapPublisherJournalChanged, journalErr)
		}
		if r.Publisher.Stateful() {
			if status.Publisher == nil {
				return fmt.Errorf("%w: publisher journal is missing", errTrafficMapPublisherJournalChanged)
			}
			if !slices.Equal(liveClaims, expectedClaims) {
				return fmt.Errorf(
					"%w: claimed targets changed from %v to %v",
					errTrafficMapPublisherJournalChanged, expectedClaims, liveClaims,
				)
			}
		} else if len(liveClaims) != 0 {
			return fmt.Errorf(
				"%w: stateless publisher found %d claimed targets",
				errTrafficMapPublisherJournalChanged, len(liveClaims),
			)
		}
		status.Published = !result.Withdrawn
		status.ObservedTrafficMapGeneration = generation
		if result.Withdrawn {
			status.GatewayRef = nil
		} else {
			status.GatewayRef = copyTrafficMapGatewayRef(result.GatewayRef)
		}
		if status.Publisher == nil || status.Publisher.PublisherName != r.Publisher.Name() {
			status.Publisher = &v1beta1.TrafficMapPublisherStatus{PublisherName: r.Publisher.Name()}
		}
		status.Publisher.ObservedOptionsDigest = digest
		condition := metav1.Condition{
			Type:               v1beta1.TrafficMapPublished,
			Status:             metav1.ConditionTrue,
			Reason:             trafficMapPublisherReasonPublished,
			Message:            fmt.Sprintf("TrafficMap is published by publisher %q", r.Publisher.Name()),
			ObservedGeneration: generation,
		}
		if result.Withdrawn {
			condition.Status = metav1.ConditionFalse
			condition.Reason = trafficMapPublisherReasonWithdrawn
			condition.Message = fmt.Sprintf("TrafficMap publication is withdrawn by publisher %q", r.Publisher.Name())
		}
		apimeta.SetStatusCondition(&status.Conditions, condition)
		return nil
	})
}

// reject records a terminal configuration or ownership error and waits for a
// watched object change or the configured resync instead of hot-looping.
func (r *TrafficMapPublisherReconciler) reject(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	reason string,
	cause error,
) (ctrl.Result, error) {
	if err := r.markFailed(ctx, trafficMap, reason, cause); err != nil {
		return ctrl.Result{}, err
	}
	return r.successResult(), nil
}

// retryFailure records a transient external failure and returns it so the
// controller workqueue retries with rate limiting.
func (r *TrafficMapPublisherReconciler) retryFailure(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	reason string,
	cause error,
) (ctrl.Result, error) {
	statusErr := r.markFailed(ctx, trafficMap, reason, cause)
	return ctrl.Result{}, errors.Join(cause, statusErr)
}

func (r *TrafficMapPublisherReconciler) markFailed(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	reason string,
	cause error,
) error {
	r.Log.Error(cause, "TrafficMap publisher reconciliation failed",
		"trafficMap", client.ObjectKeyFromObject(trafficMap), "reason", reason)
	return r.patchStatusByKey(ctx, client.ObjectKeyFromObject(trafficMap), trafficMap.UID,
		func(status *v1beta1.TrafficMapStatus) error {
			status.Published = false
			condition := metav1.Condition{
				Type:               v1beta1.TrafficMapPublished,
				Status:             metav1.ConditionFalse,
				Reason:             reason,
				Message:            trafficMapPublisherFailureMessage(reason),
				ObservedGeneration: trafficMap.Generation,
			}
			apimeta.SetStatusCondition(&status.Conditions, condition)
			return nil
		})
}

func (r *TrafficMapPublisherReconciler) patchStatusByKey(
	ctx context.Context,
	key types.NamespacedName,
	uid types.UID,
	mutate func(*v1beta1.TrafficMapStatus) error,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		live := &v1beta1.TrafficMap{}
		if err := r.APIReader.Get(ctx, key, live); err != nil {
			return client.IgnoreNotFound(err)
		}
		if live.UID != uid {
			return fmt.Errorf("TrafficMap %s was replaced while updating publisher status", key)
		}
		return r.patchPublisherStatus(ctx, live, mutate)
	})
}

func (r *TrafficMapPublisherReconciler) patchPublisherStatus(
	ctx context.Context,
	live *v1beta1.TrafficMap,
	mutate func(*v1beta1.TrafficMapStatus) error,
) error {
	desired := live.DeepCopy()
	if err := mutate(&desired.Status); err != nil {
		return err
	}
	// A restored object may carry publisher fields without this controller's
	// server-side-apply ownership. Explicitly patch zero-value transitions before
	// applying the sparse status projection so withdrawal cannot leave stale
	// publication state or a stale GatewayRef behind.
	if (live.Status.Published && !desired.Status.Published) ||
		(live.Status.GatewayRef != nil && desired.Status.GatewayRef == nil) {
		narrowed := live.DeepCopy()
		narrowed.Status.Published = desired.Status.Published
		narrowed.Status.GatewayRef = copyTrafficMapGatewayRef(desired.Status.GatewayRef)
		if err := r.Status().Patch(ctx, narrowed,
			client.MergeFromWithOptions(live, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		desired.ResourceVersion = narrowed.ResourceVersion
	}
	// Apply even when the typed values are unchanged so this controller remains
	// the declared owner of its sparse status projection after a restart.
	return r.applyPublisherStatus(ctx, desired)
}

func (r *TrafficMapPublisherReconciler) applyPublisherStatus(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
) error {
	return r.Status().Apply(ctx, trafficMapPublisherStatusApply(trafficMap),
		client.FieldOwner(trafficMapPublisherStatusFieldOwner), client.ForceOwnership)
}

func trafficMapPublisherStatusApply(trafficMap *v1beta1.TrafficMap) runtime.ApplyConfiguration {
	status := map[string]any{
		"published":                    trafficMap.Status.Published,
		"observedTrafficMapGeneration": trafficMap.Status.ObservedTrafficMapGeneration,
	}
	if trafficMap.Status.Publisher != nil {
		publisher := map[string]any{
			"publisherName": trafficMap.Status.Publisher.PublisherName,
		}
		if len(trafficMap.Status.Publisher.ClaimedTargets) != 0 {
			claims := make([]any, len(trafficMap.Status.Publisher.ClaimedTargets))
			for i, claim := range trafficMap.Status.Publisher.ClaimedTargets {
				claims[i] = claim
			}
			publisher["claimedTargets"] = claims
		}
		if trafficMap.Status.Publisher.ObservedOptionsDigest != "" {
			publisher["observedOptionsDigest"] = trafficMap.Status.Publisher.ObservedOptionsDigest
		}
		status["publisher"] = publisher
	}
	if trafficMap.Status.GatewayRef != nil {
		gatewayRef := map[string]any{
			"kind": trafficMap.Status.GatewayRef.Kind,
			"name": trafficMap.Status.GatewayRef.Name,
		}
		if trafficMap.Status.GatewayRef.Group != "" {
			gatewayRef["group"] = trafficMap.Status.GatewayRef.Group
		}
		if trafficMap.Status.GatewayRef.Namespace != "" {
			gatewayRef["namespace"] = trafficMap.Status.GatewayRef.Namespace
		}
		status["gatewayRef"] = gatewayRef
	}
	conditions := make([]any, 0, 1)
	if published := apimeta.FindStatusCondition(
		trafficMap.Status.Conditions, v1beta1.TrafficMapPublished,
	); published != nil {
		conditions = append(conditions, trafficMapConditionApply(*published))
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

func trafficMapConditionApply(condition metav1.Condition) map[string]any {
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

func (r *TrafficMapPublisherReconciler) successResult() ctrl.Result {
	if r.RequeueAfter > 0 {
		return ctrl.Result{RequeueAfter: r.RequeueAfter}
	}
	return ctrl.Result{}
}

// SetupWithManager registers a single-worker TrafficMap reconciler and maps
// source InferenceService changes to the same namespace/name. The one-worker
// invariant serializes the uncached claim scan, claim write, and external call.
func (r *TrafficMapPublisherReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.validateSetup(); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(TrafficMapPublisherControllerName).
		For(&v1beta1.TrafficMap{}).
		Watches(&v1beta1.InferenceService{},
			handler.EnqueueRequestsFromMapFunc(enqueueOwnedTrafficMap),
			builder.WithPredicates(trafficMapPublisherOwnerChange)).
		WithOptions(trafficMapPublisherControllerOptions()).
		Complete(r)
}

func (r *TrafficMapPublisherReconciler) validateSetup() error {
	if r.Publisher == nil {
		return fmt.Errorf("TrafficMap publisher is not configured")
	}
	if r.APIReader == nil {
		return fmt.Errorf("TrafficMap publisher API reader is not configured")
	}
	if err := validatePublisherName(r.Publisher.Name()); err != nil {
		return err
	}
	if r.Publisher.Stateful() && !r.LeaderElectionEnabled {
		return fmt.Errorf("stateful TrafficMap publisher %q requires leader election", r.Publisher.Name())
	}
	if r.Active && r.Publisher.Stateful() && r.RequeueAfter <= 0 {
		return fmt.Errorf("stateful TrafficMap publisher %q requires a positive resync period", r.Publisher.Name())
	}
	return nil
}

func trafficMapPublisherControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: 1,
		NeedLeaderElection:      ptr.To(true),
	}
}

func enqueueOwnedTrafficMap(_ context.Context, object client.Object) []ctrlreconcile.Request {
	if object == nil {
		return nil
	}
	return []ctrlreconcile.Request{{NamespacedName: client.ObjectKeyFromObject(object)}}
}

var trafficMapPublisherOwnerChange = predicate.Funcs{
	CreateFunc: func(event.CreateEvent) bool { return true },
	DeleteFunc: func(event.DeleteEvent) bool { return true },
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldOwner, ok1 := e.ObjectOld.(*v1beta1.InferenceService)
		newOwner, ok2 := e.ObjectNew.(*v1beta1.InferenceService)
		if !ok1 || !ok2 || oldOwner == nil || newOwner == nil {
			return true
		}
		return oldOwner.Generation != newOwner.Generation ||
			oldOwner.DeletionTimestamp.IsZero() != newOwner.DeletionTimestamp.IsZero() ||
			!slices.Equal(oldOwner.Finalizers, newOwner.Finalizers) ||
			!equality.Semantic.DeepEqual(oldOwner.Labels, newOwner.Labels) ||
			!equality.Semantic.DeepEqual(oldOwner.Annotations, newOwner.Annotations)
	},
}

var (
	errTrafficMapPublisherPlanStale      = errors.New("TrafficMap changed while claiming publisher targets")
	errTrafficMapPublisherJournalChanged = errors.New("TrafficMap publisher journal changed during reconciliation")
	errTrafficMapPublisherChanged        = errors.New("TrafficMap publisher changed while claims remain")
	errTrafficMapPublisherSourceChanged  = errors.New("TrafficMap source provenance changed during reconciliation")
	errTrafficMapPublisherOwnerInvalid   = errors.New("TrafficMap owner binding is invalid")
	errTrafficMapPublisherOwnerRead      = errors.New("TrafficMap owner state read failed")
)

func validatePublisherSourceUID(trafficMap *v1beta1.TrafficMap, ownerUID types.UID) error {
	if trafficMap.Status.SourceUID == "" {
		return fmt.Errorf("%w: TrafficMap %s source UID is not established",
			errTrafficMapPublisherSourceChanged, client.ObjectKeyFromObject(trafficMap))
	}
	if trafficMap.Status.SourceUID != ownerUID {
		return fmt.Errorf("%w: TrafficMap %s source UID %q does not match InferenceService UID %q",
			errTrafficMapPublisherSourceChanged, client.ObjectKeyFromObject(trafficMap),
			trafficMap.Status.SourceUID, ownerUID)
	}
	return nil
}

type terminalPublisherError struct {
	err error
}

func (e *terminalPublisherError) Error() string { return e.err.Error() }
func (e *terminalPublisherError) Unwrap() error { return e.err }

func validatePublisherName(name string) error {
	if name == "" {
		return fmt.Errorf("TrafficMap publisher name must not be empty")
	}
	if len(name) > v1beta1.MaxTrafficMapPublisherNameLength {
		return fmt.Errorf("TrafficMap publisher name has %d bytes, maximum is %d",
			len(name), v1beta1.MaxTrafficMapPublisherNameLength)
	}
	return nil
}

func normalizePublisherClaims(claims []string) ([]string, error) {
	if len(claims) > v1beta1.MaxTrafficMapPublisherTargets {
		return nil, fmt.Errorf("publisher plan has %d targets, maximum is %d",
			len(claims), v1beta1.MaxTrafficMapPublisherTargets)
	}
	normalized := slices.Clone(claims)
	slices.Sort(normalized)
	for i, claim := range normalized {
		if claim == "" {
			return nil, fmt.Errorf("publisher target must not be empty")
		}
		if len(claim) > v1beta1.MaxTrafficMapPublisherTargetLength {
			return nil, fmt.Errorf("publisher target %q has %d bytes, maximum is %d",
				claim, len(claim), v1beta1.MaxTrafficMapPublisherTargetLength)
		}
		if i > 0 && claim == normalized[i-1] {
			return nil, fmt.Errorf("publisher plan contains duplicate target %q", claim)
		}
	}
	return normalized, nil
}

func publisherJournalForName(
	journal *v1beta1.TrafficMapPublisherStatus,
	name string,
) ([]string, string, error) {
	if journal == nil {
		return nil, "", nil
	}
	claims, err := normalizePublisherClaims(journal.ClaimedTargets)
	if err != nil {
		return nil, "", fmt.Errorf("invalid publisher journal: %w", err)
	}
	if len(claims) != 0 && journal.PublisherName != name {
		return nil, "", fmt.Errorf("%w: from %q to %q",
			errTrafficMapPublisherChanged, journal.PublisherName, name)
	}
	digest := journal.ObservedOptionsDigest
	if journal.PublisherName != name {
		digest = ""
	}
	return claims, digest, nil
}

func publisherJournalHasClaims(journal *v1beta1.TrafficMapPublisherStatus) bool {
	return journal != nil && len(journal.ClaimedTargets) != 0
}

func publisherStatusNeedsFinalizer(status v1beta1.TrafficMapStatus, stateful bool) bool {
	if publisherJournalHasClaims(status.Publisher) {
		return true
	}
	return stateful && (status.Publisher != nil || status.Published || status.GatewayRef != nil ||
		status.ObservedTrafficMapGeneration != 0)
}

func trafficMapPublisherFailureMessage(reason string) string {
	switch reason {
	case trafficMapPublisherReasonInvalidOwner:
		return "TrafficMap owner identity is invalid or stale"
	case trafficMapPublisherReasonInvalidOptions:
		return "Publisher options are invalid"
	case trafficMapPublisherReasonInvalidPlan:
		return "Publisher plan is invalid"
	case trafficMapPublisherReasonLegacyActive:
		return "Legacy endpoint publisher cleanup is incomplete"
	case trafficMapPublisherReasonClaimRejected:
		return "Publisher claims could not be acquired"
	case trafficMapPublisherReasonDrainFailed:
		return "Publisher could not drain stale targets"
	case trafficMapPublisherReasonApplyFailed:
		return "Publisher could not apply the TrafficMap"
	case trafficMapPublisherReasonUnpublishFailed:
		return "Publisher could not remove TrafficMap state"
	case trafficMapPublisherReasonPublisherChanged:
		return "Publisher journal does not match the selected publisher"
	default:
		return "TrafficMap publisher reconciliation failed"
	}
}

func publisherClaimUnion(left, right []string) []string {
	union := append(make([]string, 0, len(left)+len(right)), left...)
	union = append(union, right...)
	slices.Sort(union)
	return slices.Compact(union)
}

func publisherClaimDifference(left, right []string) []string {
	rightSet := make(map[string]struct{}, len(right))
	for _, claim := range right {
		rightSet[claim] = struct{}{}
	}
	difference := make([]string, 0, len(left))
	for _, claim := range left {
		if _, found := rightSet[claim]; !found {
			difference = append(difference, claim)
		}
	}
	slices.Sort(difference)
	return difference
}

func routingPublisherOptions(owner *v1beta1.InferenceService) map[string]string {
	if owner == nil || owner.Spec.Routing == nil || owner.Spec.Routing.Publisher == nil {
		return nil
	}
	return owner.Spec.Routing.Publisher.Options
}

func copyTrafficMapGatewayRef(ref *v1beta1.TrafficMapGatewayRef) *v1beta1.TrafficMapGatewayRef {
	if ref == nil {
		return nil
	}
	copy := *ref
	return &copy
}

func clonePublisherOptions(options map[string]string) map[string]string {
	if len(options) == 0 {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(options))
	for key, value := range options {
		cloned[key] = value
	}
	return cloned
}

func publisherOptionsDigest(publisherName string, options map[string]string) (string, error) {
	payload := struct {
		Version   string            `json:"version"`
		Publisher string            `json:"publisher"`
		Options   map[string]string `json:"options"`
	}{
		Version:   trafficMapPublisherOptionsDigestVersion,
		Publisher: publisherName,
		Options:   clonePublisherOptions(options),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode effective publisher options: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
