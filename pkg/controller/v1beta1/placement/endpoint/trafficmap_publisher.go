package endpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
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
	"sigs.k8s.io/ome/pkg/constants"
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
	"sigs.k8s.io/ome/pkg/validation"
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
	trafficMapPublisherReasonPublished          = "Published"
	trafficMapPublisherReasonWithdrawn          = "Withdrawn"
	trafficMapPublisherReasonInvalidOwner       = "InvalidOwner"
	trafficMapPublisherReasonInvalidOptions     = "InvalidOptions"
	trafficMapPublisherReasonInvalidPlan        = "InvalidPlan"
	trafficMapPublisherReasonLegacyActive       = "LegacyLifecycleActive"
	trafficMapPublisherReasonClaimRejected      = "ClaimRejected"
	trafficMapPublisherReasonDrainFailed        = "DrainFailed"
	trafficMapPublisherReasonApplyFailed        = "ApplyFailed"
	trafficMapPublisherReasonUnpublishFailed    = "UnpublishFailed"
	trafficMapPublisherReasonPublisherChanged   = "PublisherChanged"
	trafficMapPublisherReasonUnpublished        = "Unpublished"
	trafficMapPublisherReasonCurrentPlan        = "CurrentPlanApplied"
	trafficMapPublisherReasonApplyCurrent       = "ApplyCurrent"
	trafficMapPublisherReasonUnsupported        = "Unsupported"
	trafficMapPublisherReasonEligibilityUnknown = "EligibilityUnknown"
	trafficMapPublisherOptionsDigestVersion     = "v1"
	trafficMapPublisherStatusFieldOwner         = "ome-trafficmap-publisher"
)

// TrafficMapNoPositivePlanPolicy is the publisher-resolved action for a valid
// current plan that contains no positive target weight.
type TrafficMapNoPositivePlanPolicy string

const (
	TrafficMapNoPositivePlanPolicyApplyCurrent       TrafficMapNoPositivePlanPolicy = "ApplyCurrent"
	TrafficMapNoPositivePlanPolicyRetainLastPositive TrafficMapNoPositivePlanPolicy = "RetainLastPositive"
)

// TrafficMapPublishPlan is an immutable, publisher-specific publication plan.
// Claims returns the complete canonical target set the plan may mutate. The
// controller persists that set before passing the plan back to Apply.
type TrafficMapPublishPlan interface {
	Claims() []string
}

// TrafficMapPublishPlanCapture is the publisher-neutral, post-transform view of
// an opaque publication plan. Withdrawn is explicit because an empty withdrawal
// and an empty, authoritative plan have different fallback semantics.
type TrafficMapPublishPlanCapture struct {
	Targets                 []v1beta1.TrafficMapPublisherTarget
	PlanCompatibilityDigest string
	NoPositivePlanPolicy    TrafficMapNoPositivePlanPolicy
	// AllowAllHomesUnreadyFallback opts this publisher plan into replaying a
	// compatible retained positive plan for a definitive AllHomesUnready
	// routing verdict. This permits an earlier source generation and a current
	// claim subset whose omitted retained targets all have zero weight. The
	// shared default remains fail closed because the verdict can be a real outage.
	AllowAllHomesUnreadyFallback bool
	Withdrawn                    bool
}

// TrafficMapPublisherFallback is an optional capability for publishers whose
// complete replay state is a bounded canonical target-to-weight set. Both
// methods must be pure; the shared controller owns validation, persistence,
// claim arbitration, ordering, and the call to Apply.
type TrafficMapPublisherFallback interface {
	Capture(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error)
	Replay(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error)
}

type trafficMapPublicationPreparation struct {
	previousClaims []string
	expectedClaims []string
	useFallback    bool
	lastPositive   *v1beta1.TrafficMapPublisherLastPositive
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

// Reconcile runs one pass of the publisher over the TrafficMap req names.
// publisherPass.run lists the steps of a pass in order.
func (r *TrafficMapPublisherReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pass := &publisherPass{reconciler: r, key: req.NamespacedName}
	return pass.run(ctx)
}

// publisherPass is one reconcile pass over one TrafficMap: what the pass
// read, what it planned, and the result a step ended it with.
type publisherPass struct {
	reconciler *TrafficMapPublisherReconciler
	key        types.NamespacedName
	trafficMap *v1beta1.TrafficMap
	owner      *v1beta1.InferenceService
	// effectiveOptions are the publisher's options once the installation's
	// and the service's inline ones are resolved; the status records their
	// digest.
	effectiveOptions map[string]string
	plan             TrafficMapPublishPlan
	desiredClaims    []string
	// capture is the publisher-neutral view of the plan; nil for a publisher
	// without retained-plan fallback.
	capture *TrafficMapPublishPlanCapture
	// preparation is what preclaim recorded: the claims before and after it,
	// and whether a retained positive plan replaces the current one. Nil for
	// a stateless publisher.
	preparation *trafficMapPublicationPreparation
	// applyPlan is the plan Apply receives: the current plan, or the retained
	// positive plan the publisher replayed.
	applyPlan TrafficMapPublishPlan
	applied   TrafficMapPublishResult
	// result is what the pass returns when a step ends it.
	result ctrl.Result
}

// publisherStep is one step of a pass. It reports whether the pass goes on;
// a step that ends the pass has recorded the result through end.
type publisherStep func(context.Context) (bool, error)

// run takes the pass through its steps in order and stops at the first one
// that ends it. The list is the whole order of a pass; publish picks the
// branch the publisher's kind needs.
func (p *publisherPass) run(ctx context.Context) (ctrl.Result, error) {
	_, err := runPublisherSteps(ctx, []publisherStep{
		p.checkConfiguration,
		p.readTrafficMap,
		p.finalizeDeleting,
		p.repairFinalizer,
		p.stopWhenInactive,
		p.readOwner,
		p.stopForInactiveOwner,
		p.waitForEndpointFinalizerCleanup,
		p.resolveOptions,
		p.planPublication,
		p.capturePlan,
		p.publish,
	})
	return p.result, err
}

// runPublisherSteps runs the steps in order and reports whether every one
// let the pass go on. It stops at the first step that ends the pass or
// fails and returns that step's error.
func runPublisherSteps(ctx context.Context, steps []publisherStep) (bool, error) {
	for _, step := range steps {
		if next, err := step(ctx); !next || err != nil {
			return false, err
		}
	}
	return true, nil
}

// end records the result a step ends the pass with and reports that the
// pass does not go on.
func (p *publisherPass) end(result ctrl.Result, err error) (bool, error) {
	p.result = result
	return false, err
}

// endClaimRecordWrite ends the pass after a status write failed: a claim
// record another pass changed requeues; any other failure returns its error.
func (p *publisherPass) endClaimRecordWrite(err error) (bool, error) {
	if errors.Is(err, errTrafficMapPublisherJournalChanged) {
		return p.end(ctrl.Result{Requeue: true}, nil)
	}
	return p.end(ctrl.Result{}, err)
}

// checkConfiguration ends the pass when the reconciler is not wired: no
// publisher, no uncached reader, an invalid publisher name, or a stateless
// publisher that offers retained-plan fallback.
func (p *publisherPass) checkConfiguration(context.Context) (bool, error) {
	publisher := p.reconciler.Publisher
	if publisher == nil {
		return p.end(ctrl.Result{}, fmt.Errorf("TrafficMap publisher is not configured"))
	}
	if p.reconciler.APIReader == nil {
		return p.end(ctrl.Result{}, fmt.Errorf("TrafficMap publisher API reader is not configured"))
	}
	if err := validatePublisherName(publisher.Name()); err != nil {
		return p.end(ctrl.Result{}, err)
	}
	if _, capable := publisher.(TrafficMapPublisherFallback); capable && !publisher.Stateful() {
		return p.end(ctrl.Result{}, fmt.Errorf(
			"TrafficMap publisher %q exposes retained-plan fallback but is stateless", publisher.Name(),
		))
	}
	return true, nil
}

// readTrafficMap reads the map from the cache and records its fallback
// metric. A map that is gone ends the pass once its metric is removed.
func (p *publisherPass) readTrafficMap(ctx context.Context) (bool, error) {
	trafficMap := &v1beta1.TrafficMap{}
	if err := p.reconciler.Get(ctx, p.key, trafficMap); err != nil {
		if client.IgnoreNotFound(err) == nil {
			deleteTrafficMapPublicationFallbackMetric(p.key.Namespace, p.key.Name, p.reconciler.Publisher.Name())
		}
		return p.end(ctrl.Result{}, client.IgnoreNotFound(err))
	}
	recordTrafficMapPublicationFallbackMetric(trafficMap, p.reconciler.Publisher.Name())
	p.trafficMap = trafficMap
	return true, nil
}

// finalizeDeleting hands a deleting map to finalization.
func (p *publisherPass) finalizeDeleting(ctx context.Context) (bool, error) {
	if p.trafficMap.DeletionTimestamp.IsZero() {
		return true, nil
	}
	return p.end(p.reconciler.finalize(ctx, p.trafficMap))
}

// repairFinalizer adds the finalizer a map with durable claims must carry,
// before any gate or validation can stop the pass and leave those claims
// unprotected from deletion.
func (p *publisherPass) repairFinalizer(ctx context.Context) (bool, error) {
	if !publisherStatusNeedsFinalizer(p.trafficMap.Status, p.reconciler.Publisher.Stateful()) ||
		controllerutil.ContainsFinalizer(p.trafficMap, TrafficMapPublisherFinalizer) {
		return true, nil
	}
	return p.ensureFinalizer(ctx)
}

// ensureFinalizer adds the publisher finalizer when the map lacks it and
// requeues the pass once it is added.
func (p *publisherPass) ensureFinalizer(ctx context.Context) (bool, error) {
	added, err := p.reconciler.ensureFinalizer(ctx, p.trafficMap)
	if err != nil {
		return p.end(ctrl.Result{}, err)
	}
	if added {
		return p.end(ctrl.Result{Requeue: true}, nil)
	}
	return true, nil
}

// stopWhenInactive ends the pass of an installation whose publisher is
// switched off; the routing controller owns the map's deletion.
func (p *publisherPass) stopWhenInactive(context.Context) (bool, error) {
	if p.reconciler.Active {
		return true, nil
	}
	return p.end(p.reconciler.successResult(), nil)
}

// readOwner reads the owning InferenceService through the uncached reader
// and checks the map's provenance against it. A failed read retries, a map
// behind its owner waits for its rewrite, and an invalid binding is rejected.
func (p *publisherPass) readOwner(ctx context.Context) (bool, error) {
	owner, err := p.reconciler.owner(ctx, p.trafficMap)
	if err != nil {
		switch {
		case errors.Is(err, errTrafficMapPublisherOwnerRead):
			return p.end(p.reconciler.retryFailure(ctx, p.trafficMap, trafficMapPublisherReasonInvalidOwner, err))
		case errors.Is(err, errTrafficMapPublisherOwnerAhead):
			return p.waitForRewrite(err)
		}
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidOwner, err))
	}
	p.owner = owner
	return true, nil
}

// waitForRewrite ends the pass without a status write. The map was derived
// from an older owner generation; the routing controller's rewrite is a spec
// change this controller watches, so the next pass follows it.
func (p *publisherPass) waitForRewrite(cause error) (bool, error) {
	p.reconciler.Log.Info("TrafficMap waits for the rewrite that follows its owner's newer generation",
		"trafficMap", client.ObjectKeyFromObject(p.trafficMap), "detail", cause.Error())
	return p.end(p.reconciler.successResult(), nil)
}

// stopForInactiveOwner marks the map unpublished and ends the pass when its
// owner is deleting, opted out or not placement eligible. A claim record
// another publisher wrote is rejected instead.
func (p *publisherPass) stopForInactiveOwner(ctx context.Context) (bool, error) {
	if publisherOwnerActive(p.owner) {
		return true, nil
	}
	if _, err := normalizedPublisherJournal(p.trafficMap.Status.Publisher, p.reconciler.Publisher.Name()); err != nil {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonPublisherChanged, err))
	}
	if err := p.reconciler.markInactive(ctx, p.trafficMap); err != nil {
		return p.endClaimRecordWrite(err)
	}
	return p.end(p.reconciler.successResult(), nil)
}

// waitForEndpointFinalizerCleanup keeps a stateful publisher from taking its
// first claims on a map while any InferenceService still carries the endpoint
// publisher's finalizer. A map whose claim record this publisher wrote goes on.
func (p *publisherPass) waitForEndpointFinalizerCleanup(ctx context.Context) (bool, error) {
	if !p.reconciler.Publisher.Stateful() || p.claimRecordIsOurs() {
		return true, nil
	}
	legacyOwners, err := p.reconciler.legacyEndpointOwners(ctx)
	if err != nil {
		return p.end(p.reconciler.retryFailure(ctx, p.trafficMap, trafficMapPublisherReasonLegacyActive,
			fmt.Errorf("check legacy endpoint publication state: %w", err)))
	}
	if legacyOwners != 0 {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonLegacyActive, fmt.Errorf(
			"%d InferenceService(s) retain the legacy endpoint publisher finalizer; complete their cleanup before publishing TrafficMaps",
			legacyOwners,
		)))
	}
	return true, nil
}

// claimRecordIsOurs reports a map whose claim record (status.publisher) this
// publisher wrote.
func (p *publisherPass) claimRecordIsOurs() bool {
	record := p.trafficMap.Status.Publisher
	return record != nil && record.PublisherName == p.reconciler.Publisher.Name()
}

// resolveOptions merges the installation's options with the service's inline
// ones through the publisher.
func (p *publisherPass) resolveOptions(ctx context.Context) (bool, error) {
	serviceOptions, err := routingPublisherOptions(p.owner)
	if err != nil {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidOptions, err))
	}
	effectiveOptions, err := p.reconciler.Publisher.ResolveOptions(
		clonePublisherOptions(p.reconciler.GlobalOptions),
		serviceOptions,
	)
	if err != nil {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidOptions, err))
	}
	p.effectiveOptions = clonePublisherOptions(effectiveOptions)
	return true, nil
}

// planPublication asks the publisher for its plan from copies of the owner
// and the map, and normalizes the claims the plan may touch. The current
// plan is what Apply receives unless a retained plan replaces it.
func (p *publisherPass) planPublication(ctx context.Context) (bool, error) {
	publisher := p.reconciler.Publisher
	plan, err := publisher.Plan(p.owner.DeepCopy(), p.trafficMap.DeepCopy(), clonePublisherOptions(p.effectiveOptions))
	if err != nil {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidPlan, err))
	}
	if plan == nil {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidPlan,
			fmt.Errorf("TrafficMap publisher %q returned a nil plan", publisher.Name())))
	}
	desiredClaims, err := normalizePublisherClaims(plan.Claims())
	if err != nil {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidPlan, err))
	}
	p.plan, p.applyPlan, p.desiredClaims = plan, plan, desiredClaims
	return true, nil
}

// capturePlan takes the publisher-neutral view of the plan from a publisher
// with retained-plan fallback and checks it against the claims.
func (p *publisherPass) capturePlan(ctx context.Context) (bool, error) {
	fallback, ok := p.reconciler.Publisher.(TrafficMapPublisherFallback)
	if !ok {
		return true, nil
	}
	captured, err := fallback.Capture(p.plan)
	if err != nil {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidPlan,
			fmt.Errorf("capture TrafficMap publication plan: %w", err)))
	}
	if err := validateTrafficMapPlanCapture(&captured, p.desiredClaims); err != nil {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidPlan, err))
	}
	p.capture = &captured
	return true, nil
}

// publish runs the branch the publisher's kind needs. A stateless publisher
// checks fresh state and applies. A stateful one claims its targets first,
// replays a retained positive plan when preclaim chose one, drains the
// claims it no longer needs, applies, and records the outcome.
func (p *publisherPass) publish(ctx context.Context) (bool, error) {
	if !p.reconciler.Publisher.Stateful() {
		return runPublisherSteps(ctx, []publisherStep{
			p.rejectStatelessClaims,
			p.preflightStateless,
			p.markTransitioning,
			p.apply,
			p.recordStatelessPublication,
		})
	}
	return runPublisherSteps(ctx, []publisherStep{
		p.ensureFinalizer,
		p.preclaim,
		p.replayRetainedPlan,
		p.preflight,
		p.drainStaleClaims,
		p.apply,
		p.rejectWithdrawnReplay,
		p.checkCapturedWithdrawal,
		p.recordPublication,
	})
}

// rejectStatelessClaims rejects a plan from a stateless publisher that names
// target claims; such a publisher keeps no claim record to claim them in.
func (p *publisherPass) rejectStatelessClaims(ctx context.Context) (bool, error) {
	if len(p.desiredClaims) == 0 {
		return true, nil
	}
	return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidPlan,
		fmt.Errorf("stateless TrafficMap publisher %q planned %d target claims",
			p.reconciler.Publisher.Name(), len(p.desiredClaims))))
}

// preflightStateless rechecks the map, its owner and the claim record from
// fresh state before a stateless apply.
func (p *publisherPass) preflightStateless(ctx context.Context) (bool, error) {
	if err := p.reconciler.preflightStateless(ctx, p.trafficMap, p.owner); err != nil {
		return p.endPreEffectFailure(ctx, err)
	}
	return true, nil
}

// endPreEffectFailure ends the pass after the fresh-state check before any
// external effect failed: a changed map requeues, an owner that moved ahead
// of the map waits for the rewrite, a failed owner read retries, a changed
// source or publisher is rejected, a terminal claim problem is rejected, and
// any other failure retries.
func (p *publisherPass) endPreEffectFailure(ctx context.Context, err error) (bool, error) {
	switch {
	case errors.Is(err, errTrafficMapPublisherPlanStale):
		return p.end(ctrl.Result{Requeue: true}, nil)
	case errors.Is(err, errTrafficMapPublisherOwnerAhead):
		return p.waitForRewrite(err)
	case errors.Is(err, errTrafficMapPublisherOwnerRead):
		return p.end(p.reconciler.retryFailure(ctx, p.trafficMap, trafficMapPublisherReasonInvalidOwner, err))
	case errors.Is(err, errTrafficMapPublisherSourceChanged):
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonInvalidOwner, err))
	case errors.Is(err, errTrafficMapPublisherChanged):
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonPublisherChanged, err))
	}
	var terminal *terminalPublisherError
	if errors.As(err, &terminal) {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonClaimRejected, terminal.err))
	}
	return p.end(p.reconciler.retryFailure(ctx, p.trafficMap, trafficMapPublisherReasonClaimRejected, err))
}

// markTransitioning writes the transitioning condition before a stateless
// apply.
func (p *publisherPass) markTransitioning(ctx context.Context) (bool, error) {
	if err := p.reconciler.markTransitioning(ctx, p.trafficMap, nil, false); err != nil {
		return p.end(ctrl.Result{}, err)
	}
	return true, nil
}

// apply realizes the plan through the publisher.
func (p *publisherPass) apply(ctx context.Context) (bool, error) {
	result, err := p.reconciler.Publisher.Apply(ctx, p.applyPlan)
	if err != nil {
		return p.end(p.reconciler.retryFailure(ctx, p.trafficMap, trafficMapPublisherReasonApplyFailed, err))
	}
	p.applied = result
	return true, nil
}

// recordStatelessPublication writes the applied generation and options
// digest, with no claims, and ends the pass.
func (p *publisherPass) recordStatelessPublication(ctx context.Context) (bool, error) {
	if err := p.reconciler.markPublicationApplied(ctx, client.ObjectKeyFromObject(p.trafficMap), p.trafficMap.UID,
		p.trafficMap.Generation, nil, p.effectiveOptions, p.applied, nil, nil, false); err != nil {
		return p.endClaimRecordWrite(err)
	}
	return p.end(p.reconciler.successResult(), nil)
}

// preclaim writes the claims this pass may touch into the map's claim record
// from fresh state, before any external effect, and decides whether a
// retained positive plan is replayed. An unknown fallback eligibility ends the
// pass without effects.
func (p *publisherPass) preclaim(ctx context.Context) (bool, error) {
	preparation, err := p.reconciler.preclaim(ctx, p.trafficMap, p.owner, p.desiredClaims, p.capture)
	if err != nil {
		if errors.Is(err, errTrafficMapPublisherEligibilityUnknown) {
			return p.end(p.reconciler.successResult(), nil)
		}
		return p.endPreEffectFailure(ctx, err)
	}
	p.preparation = preparation
	return true, nil
}

// replayRetainedPlan swaps in the retained positive plan when preclaim chose
// to replay one. A replay that fails, returns no plan or names other claims
// than the record holds clears the retained plan, and the current plan applies.
func (p *publisherPass) replayRetainedPlan(ctx context.Context) (bool, error) {
	if !p.preparation.useFallback {
		return true, nil
	}
	replayed, err := p.replayedPlan()
	if err == nil {
		p.applyPlan = replayed
		return true, nil
	}
	if err := p.reconciler.invalidateLastPositive(ctx, p.trafficMap, p.preparation.expectedClaims); err != nil {
		return p.endClaimRecordWrite(err)
	}
	p.preparation.useFallback = false
	p.preparation.lastPositive = nil
	return true, nil
}

// replayedPlan asks the publisher to rebuild the retained positive plan and
// checks that its claims match the recorded ones exactly.
func (p *publisherPass) replayedPlan() (TrafficMapPublishPlan, error) {
	fallback := p.reconciler.Publisher.(TrafficMapPublisherFallback)
	replayed, err := fallback.Replay(*copyTrafficMapLastPositive(p.preparation.lastPositive))
	if err != nil {
		return nil, err
	}
	if replayed == nil {
		return nil, fmt.Errorf("publisher returned a nil replay plan")
	}
	replayedClaims, err := normalizePublisherClaims(replayed.Claims())
	if err != nil {
		return nil, err
	}
	if !slices.Equal(replayedClaims, p.preparation.expectedClaims) {
		return nil, fmt.Errorf("replay claims %v do not exactly match durable claims %v",
			replayedClaims, p.preparation.expectedClaims)
	}
	return replayed, nil
}

// preflight lets a publisher that validates shared external state do so once
// its claims are durable and before any drain or apply.
func (p *publisherPass) preflight(ctx context.Context) (bool, error) {
	preflighter, ok := p.reconciler.Publisher.(trafficMapPublisherPreflighter)
	if !ok {
		return true, nil
	}
	err := preflighter.Preflight(ctx, p.applyPlan)
	if err == nil {
		return true, nil
	}
	var terminal *terminalPublisherError
	if errors.As(err, &terminal) {
		return p.end(p.reconciler.reject(ctx, p.trafficMap, trafficMapPublisherReasonClaimRejected, terminal.err))
	}
	return p.end(p.reconciler.retryFailure(ctx, p.trafficMap, trafficMapPublisherReasonClaimRejected, err))
}

// drainStaleClaims keeps at zero the claims the record holds and the current
// plan no longer names. A replayed plan keeps every claim.
func (p *publisherPass) drainStaleClaims(ctx context.Context) (bool, error) {
	if p.preparation.useFallback {
		return true, nil
	}
	staleClaims := publisherClaimDifference(p.preparation.previousClaims, p.desiredClaims)
	if len(staleClaims) == 0 {
		return true, nil
	}
	if err := p.reconciler.Publisher.Drain(ctx, client.ObjectKeyFromObject(p.trafficMap), slices.Clone(staleClaims)); err != nil {
		return p.end(p.reconciler.retryFailure(ctx, p.trafficMap, trafficMapPublisherReasonDrainFailed, err))
	}
	return true, nil
}

// rejectWithdrawnReplay retries when the publisher withdrew publication while
// replaying a retained positive plan, which must publish.
func (p *publisherPass) rejectWithdrawnReplay(ctx context.Context) (bool, error) {
	if !p.preparation.useFallback || !p.applied.Withdrawn {
		return true, nil
	}
	return p.end(p.reconciler.retryFailure(ctx, p.trafficMap, trafficMapPublisherReasonApplyFailed,
		fmt.Errorf("publisher reported withdrawal while replaying a retained positive plan")))
}

// checkCapturedWithdrawal retries when Apply's withdrawal differs from the
// captured plan's, after clearing the retained plan the capture would have
// confirmed.
func (p *publisherPass) checkCapturedWithdrawal(ctx context.Context) (bool, error) {
	if p.preparation.useFallback || p.capture == nil || p.applied.Withdrawn == p.capture.Withdrawn {
		return true, nil
	}
	if err := p.reconciler.invalidateLastPositive(ctx, p.trafficMap, p.preparation.expectedClaims); err != nil {
		return p.endClaimRecordWrite(err)
	}
	return p.end(p.reconciler.retryFailure(ctx, p.trafficMap, trafficMapPublisherReasonApplyFailed, fmt.Errorf(
		"publisher Apply withdrawal result %t does not match captured plan withdrawal %t",
		p.applied.Withdrawn, p.capture.Withdrawn,
	)))
}

// recordPublication writes the applied generation, claims and options digest
// and, for a positive current plan applied as captured, the retained positive
// plan, then ends the pass.
func (p *publisherPass) recordPublication(ctx context.Context) (bool, error) {
	var committed *v1beta1.TrafficMapPublisherLastPositive
	if p.commitsCapturedPlan() {
		committed = trafficMapLastPositiveFromCapture(
			p.trafficMap.Generation, p.owner.Generation, p.preparation.expectedClaims, p.capture,
		)
	}
	if err := p.reconciler.markPublicationApplied(ctx, client.ObjectKeyFromObject(p.trafficMap), p.trafficMap.UID,
		p.trafficMap.Generation, p.preparation.expectedClaims, p.effectiveOptions, p.applied,
		p.preparation.lastPositive, committed, p.preparation.useFallback); err != nil {
		return p.endClaimRecordWrite(err)
	}
	return p.end(p.reconciler.successResult(), nil)
}

// commitsCapturedPlan reports a current plan with positive weight that was
// applied as captured; its capture becomes the retained positive plan.
func (p *publisherPass) commitsCapturedPlan() bool {
	return !p.preparation.useFallback && p.capture != nil &&
		trafficMapPlanCapturePositive(p.capture) && !p.capture.Withdrawn
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
	if err := checkObservedOwnerGeneration(trafficMap, owner); err != nil {
		return nil, err
	}
	return owner, nil
}

// checkObservedOwnerGeneration compares the owner generation the map was
// derived from with the live owner's. A map behind its owner waits for the
// routing controller's rewrite; a map ahead of its owner was derived from an
// object this owner never was and is rejected.
func checkObservedOwnerGeneration(trafficMap *v1beta1.TrafficMap, owner *v1beta1.InferenceService) error {
	observed := trafficMap.Spec.ObservedISVCGeneration
	switch {
	case observed < owner.Generation:
		return fmt.Errorf("%w: TrafficMap observed InferenceService generation %d, owner generation is %d",
			errTrafficMapPublisherOwnerAhead, observed, owner.Generation)
	case observed > owner.Generation:
		return fmt.Errorf("%w: TrafficMap observed InferenceService generation %d is ahead of owner generation %d",
			errTrafficMapPublisherOwnerInvalid, observed, owner.Generation)
	}
	return nil
}

func publisherOwnerActive(owner *v1beta1.InferenceService) bool {
	return owner != nil && owner.DeletionTimestamp.IsZero() &&
		placementcontroller.IsPlacementEligible(owner) && validation.ValidatePlacementIntent(owner) == nil && !placementcontroller.RoutingOptedOut(owner)
}

func (r *TrafficMapPublisherReconciler) validateLivePublisherOwner(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	expected *v1beta1.InferenceService,
) (*v1beta1.InferenceService, error) {
	owner, err := r.readTrafficMapOwner(ctx, trafficMap)
	if err != nil {
		if errors.Is(err, errTrafficMapPublisherOwnerRead) || errors.Is(err, errTrafficMapPublisherOwnerAhead) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", errTrafficMapPublisherSourceChanged, err)
	}
	if owner.UID != expected.UID || owner.Generation != expected.Generation {
		return nil, fmt.Errorf(
			"%w: InferenceService %s identity changed from UID %q generation %d to UID %q generation %d",
			errTrafficMapPublisherSourceChanged, client.ObjectKeyFromObject(owner),
			expected.UID, expected.Generation, owner.UID, owner.Generation,
		)
	}
	if !equality.Semantic.DeepEqual(owner.Labels, expected.Labels) ||
		!equality.Semantic.DeepEqual(owner.Annotations, expected.Annotations) ||
		!equality.Semantic.DeepEqual(owner.Finalizers, expected.Finalizers) ||
		owner.Status.ObservedGeneration != expected.Status.ObservedGeneration ||
		!equality.Semantic.DeepEqual(owner.Status.Placement, expected.Status.Placement) ||
		!equality.Semantic.DeepEqual(
			owner.Status.GetCondition(apis.ConditionReady),
			expected.Status.GetCondition(apis.ConditionReady),
		) {
		return nil, fmt.Errorf("%w: InferenceService %s publication inputs changed while planning",
			errTrafficMapPublisherSourceChanged, client.ObjectKeyFromObject(owner))
	}
	if !publisherOwnerActive(owner) {
		return nil, fmt.Errorf("%w: InferenceService %s is deleting, opted out, or not placement eligible",
			errTrafficMapPublisherSourceChanged, client.ObjectKeyFromObject(owner))
	}
	return owner, nil
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

// preclaim writes the claims this pass may touch into the map's claim record
// from fresh state, before any external effect, and decides whether a retained
// positive plan is replayed. A write that conflicts is retried on a fresh read;
// a map that moved under the pass is stale. claimAttempt.run lists the steps.
func (r *TrafficMapPublisherReconciler) preclaim(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	owner *v1beta1.InferenceService,
	desiredClaims []string,
	capture *TrafficMapPublishPlanCapture,
) (*trafficMapPublicationPreparation, error) {
	var preparation *trafficMapPublicationPreparation
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		attempt := &claimAttempt{reconciler: r, trafficMap: trafficMap, owner: owner, desiredClaims: desiredClaims, capture: capture}
		var err error
		preparation, err = attempt.run(ctx)
		return err
	})
	if errors.Is(err, errTrafficMapPublisherPlanStale) {
		return nil, errTrafficMapPublisherPlanStale
	}
	return preparation, err
}

// claimAttempt is one attempt of preclaim: the map, owner and plan the pass
// read, the live map and owner this attempt reads, and the claim record it
// found on the live map.
type claimAttempt struct {
	reconciler    *TrafficMapPublisherReconciler
	trafficMap    *v1beta1.TrafficMap
	owner         *v1beta1.InferenceService
	desiredClaims []string
	// capture is the publisher-neutral view of the plan; nil for a publisher
	// without retained-plan fallback.
	capture *TrafficMapPublishPlanCapture
	// live is the map as the apiserver holds it now, liveOwner its owner.
	live      *v1beta1.TrafficMap
	liveOwner *v1beta1.InferenceService
	// journal is the live map's claim record for this publisher; existing are
	// the claims it holds.
	journal  *v1beta1.TrafficMapPublisherStatus
	existing []string
}

// run reads fresh state, replays the retained plan when its rules allow it,
// and otherwise claims the current plan.
func (a *claimAttempt) run(ctx context.Context) (*trafficMapPublicationPreparation, error) {
	if err := a.readFreshState(ctx); err != nil {
		return nil, err
	}
	if preparation, decided, err := a.replayRetainedPlan(ctx); decided {
		return preparation, err
	}
	return a.claimCurrentPlan(ctx)
}

// readFreshState reads the map live and checks that the pass still speaks of
// the same map, owner and claim record: a replaced map or a changed owner is
// a source change; a moved generation, a deletion or a missing finalizer is
// stale; a record another publisher owns is terminal.
func (a *claimAttempt) readFreshState(ctx context.Context) error {
	key := client.ObjectKeyFromObject(a.trafficMap)
	live := &v1beta1.TrafficMap{}
	if err := a.reconciler.APIReader.Get(ctx, key, live); err != nil {
		return fmt.Errorf("%w: read TrafficMap %s before claiming targets: %w",
			errTrafficMapPublisherOwnerRead, key, err)
	}
	if live.UID != a.trafficMap.UID {
		return fmt.Errorf("%w: TrafficMap %s was replaced while claiming publisher targets",
			errTrafficMapPublisherSourceChanged, key)
	}
	if live.Generation != a.trafficMap.Generation || !live.DeletionTimestamp.IsZero() {
		return errTrafficMapPublisherPlanStale
	}
	liveOwner, err := a.reconciler.validateLivePublisherOwner(ctx, live, a.owner)
	if err != nil {
		return err
	}
	if !controllerutil.ContainsFinalizer(live, TrafficMapPublisherFinalizer) {
		return errTrafficMapPublisherPlanStale
	}
	journal, err := normalizedPublisherJournal(live.Status.Publisher, a.reconciler.Publisher.Name())
	if err != nil {
		return &terminalPublisherError{err: err}
	}
	a.live, a.liveOwner, a.journal, a.existing = live, liveOwner, journal, journal.ClaimedTargets
	return nil
}

// replayRetainedPlan decides whether the retained positive plan replaces a
// current plan that has no positive weight: the publisher asks to retain it,
// the placement is known lost, the claims are compatible and the retained
// plan is still valid. It reports whether it decided the attempt: with the
// replay prepared and recorded, or with an unknown eligibility written.
func (a *claimAttempt) replayRetainedPlan(ctx context.Context) (*trafficMapPublicationPreparation, bool, error) {
	if !a.asksToRetain() {
		return nil, false, nil
	}
	eligible, known := trafficMapFallbackEligibilityWithAllHomesUnready(a.live, a.liveOwner, a.capture.AllowAllHomesUnreadyFallback)
	if !known {
		return nil, true, a.writeEligibilityUnknown(ctx)
	}
	if !eligible || !a.retainedPlanReplays() {
		return nil, false, nil
	}
	if err := a.reconciler.rejectClaimCollisions(ctx, a.live, a.existing); err != nil {
		return nil, true, err
	}
	preparation := &trafficMapPublicationPreparation{
		previousClaims: slices.Clone(a.existing),
		expectedClaims: slices.Clone(a.existing),
		useFallback:    true,
		lastPositive:   copyTrafficMapLastPositive(a.journal.LastPositive),
	}
	return preparation, true, a.recordReplay(ctx)
}

// asksToRetain reports a current plan with no positive weight whose publisher
// asks for the last positive one instead.
func (a *claimAttempt) asksToRetain() bool {
	return a.capture != nil && !a.capture.Withdrawn &&
		!trafficMapPlanCapturePositive(a.capture) &&
		a.capture.NoPositivePlanPolicy == TrafficMapNoPositivePlanPolicyRetainLastPositive
}

// noMemberServes reports that the routing table found no serving member and
// the publisher opted into replaying through that state.
func (a *claimAttempt) noMemberServes() bool {
	return a.capture.AllowAllHomesUnreadyFallback &&
		trafficMapRoutableReason(a.live) == v1beta1.TrafficMapReasonAllHomesUnready
}

// retainedPlanReplays reports that the recorded claims may serve the current
// plan through the retained plan: the current claims are none or equal to the
// recorded ones and the plan is valid for this source generation; while no
// member serves, a subset whose omitted targets carry no weight, valid from
// an earlier source generation too.
func (a *claimAttempt) retainedPlanReplays() bool {
	compatible := publisherClaimsReplayCompatible(a.desiredClaims, a.existing)
	validate := validateTrafficMapLastPositive
	if a.noMemberServes() {
		compatible = publisherClaimsReplayCompatibleForAllHomesUnready(a.desiredClaims, a.existing, a.journal.LastPositive)
		validate = validateTrafficMapLastPositiveFromEarlierGeneration
	}
	return compatible && validate(
		a.journal.LastPositive, a.existing, a.liveOwner.Generation,
		a.capture.PlanCompatibilityDigest, a.live.Generation,
	) == nil
}

// writeEligibilityUnknown writes the Unknown publication conditions and ends
// the attempt without an effect: a source status that lags its spec cannot
// tell a lost placement from one still being written.
func (a *claimAttempt) writeEligibilityUnknown(ctx context.Context) error {
	err := a.reconciler.patchPublisherStatus(ctx, a.live, func(status *v1beta1.TrafficMapStatus) error {
		setTrafficMapPublicationConditions(
			status, a.live.Generation, metav1.ConditionUnknown,
			trafficMapPublisherReasonEligibilityUnknown,
			"TrafficMap fallback eligibility cannot be determined from fresh status",
		)
		return nil
	})
	if err != nil {
		return err
	}
	return errTrafficMapPublisherEligibilityUnknown
}

// recordReplay writes the claim record as it is with the transitioning
// conditions: the retained plan's claims are the ones this pass touches.
func (a *claimAttempt) recordReplay(ctx context.Context) error {
	return a.reconciler.patchPublisherStatus(ctx, a.live, func(status *v1beta1.TrafficMapStatus) error {
		status.Publisher = copyTrafficMapPublisherJournal(a.journal)
		setTrafficMapPublicationConditions(
			status, a.live.Generation, metav1.ConditionUnknown,
			v1beta1.TrafficMapReasonPublicationTransitioning,
			"TrafficMap publisher is transitioning to a retained positive plan",
		)
		return nil
	})
}

// claimCurrentPlan records the union of the recorded and the planned claims,
// at most the publisher target limit and none claimed by another map, keeps
// the retained plan only while a positive plan still matches it, and writes
// the transitioning conditions.
func (a *claimAttempt) claimCurrentPlan(ctx context.Context) (*trafficMapPublicationPreparation, error) {
	union := publisherClaimUnion(a.existing, a.desiredClaims)
	if len(union) > v1beta1.MaxTrafficMapPublisherTargets {
		return nil, &terminalPublisherError{err: fmt.Errorf(
			"publisher claim union has %d targets, maximum is %d",
			len(union), v1beta1.MaxTrafficMapPublisherTargets)}
	}
	if err := a.reconciler.rejectClaimCollisions(ctx, a.live, union); err != nil {
		return nil, err
	}
	var lastPositive *v1beta1.TrafficMapPublisherLastPositive
	if a.keepsRetainedPlan(union) {
		lastPositive = a.journal.LastPositive
	}
	preparation := &trafficMapPublicationPreparation{
		previousClaims: slices.Clone(a.existing),
		expectedClaims: slices.Clone(union),
		lastPositive:   copyTrafficMapLastPositive(lastPositive),
	}
	err := a.reconciler.patchPublisherStatus(ctx, a.live, func(status *v1beta1.TrafficMapStatus) error {
		status.Publisher = &v1beta1.TrafficMapPublisherStatus{
			PublisherName:         a.reconciler.Publisher.Name(),
			ClaimedTargets:        slices.Clone(union),
			ObservedOptionsDigest: a.journal.ObservedOptionsDigest,
			LastPositive:          copyTrafficMapLastPositive(lastPositive),
		}
		setTrafficMapPublicationConditions(
			status, a.live.Generation, metav1.ConditionUnknown,
			v1beta1.TrafficMapReasonPublicationTransitioning,
			"TrafficMap publisher is applying the current plan",
		)
		return nil
	})
	return preparation, err
}

// keepsRetainedPlan reports that the retained plan stays beside a positive
// current plan: the claims are unchanged and the plan is valid for this source
// generation, or, with the publisher's opt-in, the current claims are a
// compatible subset and the plan is valid from an earlier source generation.
func (a *claimAttempt) keepsRetainedPlan(union []string) bool {
	if a.capture == nil || a.capture.Withdrawn || !trafficMapPlanCapturePositive(a.capture) {
		return false
	}
	keep := slices.Equal(a.existing, a.desiredClaims) && validateTrafficMapLastPositive(
		a.journal.LastPositive, union, a.liveOwner.Generation,
		a.capture.PlanCompatibilityDigest, a.live.Generation,
	) == nil
	if !keep && a.capture.AllowAllHomesUnreadyFallback {
		keep = publisherClaimsReplayCompatibleForAllHomesUnready(
			a.desiredClaims, a.existing, a.journal.LastPositive,
		) && validateTrafficMapLastPositiveFromEarlierGeneration(
			a.journal.LastPositive, a.existing, a.liveOwner.Generation,
			a.capture.PlanCompatibilityDigest, a.live.Generation,
		) == nil
	}
	return keep
}

func (r *TrafficMapPublisherReconciler) markTransitioning(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	expectedClaims []string,
	keepLastPositive bool,
) error {
	return r.patchStatusByKey(ctx, client.ObjectKeyFromObject(trafficMap), trafficMap.UID,
		func(status *v1beta1.TrafficMapStatus) error {
			claims, _, err := publisherJournalForName(status.Publisher, r.Publisher.Name())
			if err != nil {
				return fmt.Errorf("%w: %v", errTrafficMapPublisherJournalChanged, err)
			}
			if !slices.Equal(claims, expectedClaims) {
				return fmt.Errorf("%w: claimed targets changed from %v to %v",
					errTrafficMapPublisherJournalChanged, expectedClaims, claims)
			}
			if status.Publisher != nil && !keepLastPositive {
				status.Publisher.LastPositive = nil
			}
			setTrafficMapPublicationConditions(
				status, trafficMap.Generation, metav1.ConditionUnknown,
				v1beta1.TrafficMapReasonPublicationTransitioning,
				"TrafficMap publisher is applying the current plan",
			)
			return nil
		})
}

func (r *TrafficMapPublisherReconciler) invalidateLastPositive(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
	expectedClaims []string,
) error {
	return r.patchStatusByKey(ctx, client.ObjectKeyFromObject(trafficMap), trafficMap.UID,
		func(status *v1beta1.TrafficMapStatus) error {
			claims, _, err := publisherJournalForName(status.Publisher, r.Publisher.Name())
			if err != nil || status.Publisher == nil || !slices.Equal(claims, expectedClaims) {
				return fmt.Errorf("%w: retained plan journal changed", errTrafficMapPublisherJournalChanged)
			}
			status.Publisher.LastPositive = nil
			setTrafficMapPublicationConditions(
				status, trafficMap.Generation, metav1.ConditionUnknown,
				v1beta1.TrafficMapReasonPublicationTransitioning,
				"TrafficMap retained plan is invalid; applying the current plan",
			)
			return nil
		})
}

func (r *TrafficMapPublisherReconciler) markInactive(
	ctx context.Context,
	trafficMap *v1beta1.TrafficMap,
) error {
	return r.patchStatusByKey(ctx, client.ObjectKeyFromObject(trafficMap), trafficMap.UID,
		func(status *v1beta1.TrafficMapStatus) error {
			if _, err := normalizedPublisherJournal(status.Publisher, r.Publisher.Name()); err != nil {
				return fmt.Errorf("%w: %v", errTrafficMapPublisherJournalChanged, err)
			}
			status.Published = false
			if status.Publisher != nil {
				status.Publisher.LastPositive = nil
			}
			apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1beta1.TrafficMapPublished,
				Status:             metav1.ConditionFalse,
				Reason:             trafficMapPublisherReasonUnpublished,
				Message:            "TrafficMap source is deleting or has opted out of publication",
				ObservedGeneration: trafficMap.Generation,
			})
			apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1beta1.TrafficMapPublicationFallback,
				Status:             metav1.ConditionFalse,
				Reason:             trafficMapPublisherReasonUnpublished,
				Message:            "TrafficMap publication fallback is inactive",
				ObservedGeneration: trafficMap.Generation,
			})
			return nil
		})
}

func setTrafficMapPublicationConditions(
	status *v1beta1.TrafficMapStatus,
	generation int64,
	fallbackStatus metav1.ConditionStatus,
	reason string,
	message string,
) {
	status.Published = false
	apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               v1beta1.TrafficMapPublished,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
	apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               v1beta1.TrafficMapPublicationFallback,
		Status:             fallbackStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

func normalizedPublisherJournal(
	journal *v1beta1.TrafficMapPublisherStatus,
	name string,
) (*v1beta1.TrafficMapPublisherStatus, error) {
	claims, digest, err := publisherJournalForName(journal, name)
	if err != nil {
		return nil, err
	}
	normalized := &v1beta1.TrafficMapPublisherStatus{
		PublisherName:         name,
		ClaimedTargets:        claims,
		ObservedOptionsDigest: digest,
	}
	if journal != nil && journal.PublisherName == name {
		normalized.LastPositive = copyTrafficMapLastPositive(journal.LastPositive)
	}
	return normalized, nil
}

func copyTrafficMapPublisherJournal(
	journal *v1beta1.TrafficMapPublisherStatus,
) *v1beta1.TrafficMapPublisherStatus {
	if journal == nil {
		return nil
	}
	return &v1beta1.TrafficMapPublisherStatus{
		PublisherName:         journal.PublisherName,
		ClaimedTargets:        slices.Clone(journal.ClaimedTargets),
		ObservedOptionsDigest: journal.ObservedOptionsDigest,
		LastPositive:          copyTrafficMapLastPositive(journal.LastPositive),
	}
}

func copyTrafficMapLastPositive(
	lastPositive *v1beta1.TrafficMapPublisherLastPositive,
) *v1beta1.TrafficMapPublisherLastPositive {
	if lastPositive == nil {
		return nil
	}
	return &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    lastPositive.TrafficMapGeneration,
		ObservedISVCGeneration:  lastPositive.ObservedISVCGeneration,
		PlanCompatibilityDigest: lastPositive.PlanCompatibilityDigest,
		Targets:                 slices.Clone(lastPositive.Targets),
	}
}

func validateTrafficMapPlanCapture(
	capture *TrafficMapPublishPlanCapture,
	desiredClaims []string,
) error {
	if capture == nil {
		return fmt.Errorf("TrafficMap publisher returned a nil plan capture")
	}
	if capture.NoPositivePlanPolicy != TrafficMapNoPositivePlanPolicyApplyCurrent &&
		capture.NoPositivePlanPolicy != TrafficMapNoPositivePlanPolicyRetainLastPositive {
		return fmt.Errorf("publisher returned unknown no-positive plan policy %q",
			capture.NoPositivePlanPolicy)
	}
	if capture.AllowAllHomesUnreadyFallback &&
		capture.NoPositivePlanPolicy != TrafficMapNoPositivePlanPolicyRetainLastPositive {
		return fmt.Errorf("AllHomesUnready fallback requires no-positive plan policy %q",
			TrafficMapNoPositivePlanPolicyRetainLastPositive)
	}
	if capture.AllowAllHomesUnreadyFallback && capture.Withdrawn {
		return fmt.Errorf("withdrawn publisher plan cannot enable AllHomesUnready fallback")
	}
	if err := validatePublisherDigest(capture.PlanCompatibilityDigest); err != nil {
		return fmt.Errorf("invalid plan compatibility digest: %w", err)
	}
	targets, claims, _, err := normalizeTrafficMapPublisherTargets(capture.Targets, false)
	if err != nil {
		return fmt.Errorf("invalid captured publisher targets: %w", err)
	}
	if !slices.Equal(claims, desiredClaims) {
		return fmt.Errorf("captured target set %v does not exactly match plan claims %v", claims, desiredClaims)
	}
	if capture.Withdrawn && trafficMapTargetsPositive(targets) {
		return fmt.Errorf("withdrawn publisher plan contains a positive target weight")
	}
	capture.Targets = targets
	return nil
}

func validateTrafficMapLastPositive(
	lastPositive *v1beta1.TrafficMapPublisherLastPositive,
	claims []string,
	ownerGeneration int64,
	compatibilityDigest string,
	currentTrafficMapGeneration int64,
) error {
	return validateTrafficMapLastPositiveGeneration(
		lastPositive, claims, ownerGeneration, compatibilityDigest,
		currentTrafficMapGeneration, false,
	)
}

func validateTrafficMapLastPositiveFromEarlierGeneration(
	lastPositive *v1beta1.TrafficMapPublisherLastPositive,
	claims []string,
	ownerGeneration int64,
	compatibilityDigest string,
	currentTrafficMapGeneration int64,
) error {
	return validateTrafficMapLastPositiveGeneration(
		lastPositive, claims, ownerGeneration, compatibilityDigest,
		currentTrafficMapGeneration, true,
	)
}

func validateTrafficMapLastPositiveGeneration(
	lastPositive *v1beta1.TrafficMapPublisherLastPositive,
	claims []string,
	ownerGeneration int64,
	compatibilityDigest string,
	currentTrafficMapGeneration int64,
	allowPreviousOwnerGeneration bool,
) error {
	if lastPositive == nil {
		return fmt.Errorf("retained positive plan is absent")
	}
	if lastPositive.TrafficMapGeneration < 1 ||
		lastPositive.TrafficMapGeneration > currentTrafficMapGeneration {
		return fmt.Errorf("retained TrafficMap generation %d is outside [1,%d]",
			lastPositive.TrafficMapGeneration, currentTrafficMapGeneration)
	}
	if lastPositive.ObservedISVCGeneration < 1 ||
		lastPositive.ObservedISVCGeneration > ownerGeneration ||
		(!allowPreviousOwnerGeneration && lastPositive.ObservedISVCGeneration != ownerGeneration) {
		return fmt.Errorf("retained source generation %d does not match current generation %d",
			lastPositive.ObservedISVCGeneration, ownerGeneration)
	}
	if err := validatePublisherDigest(lastPositive.PlanCompatibilityDigest); err != nil {
		return fmt.Errorf("invalid retained plan compatibility digest: %w", err)
	}
	if lastPositive.PlanCompatibilityDigest != compatibilityDigest {
		return fmt.Errorf("retained plan compatibility digest does not match current options")
	}
	_, targetClaims, positive, err := normalizeTrafficMapPublisherTargets(lastPositive.Targets, true)
	if err != nil {
		return fmt.Errorf("invalid retained publisher targets: %w", err)
	}
	if !positive {
		return fmt.Errorf("retained publisher plan has no positive target")
	}
	if !slices.Equal(targetClaims, claims) {
		return fmt.Errorf("retained target set %v does not exactly match durable claims %v",
			targetClaims, claims)
	}
	return nil
}

func normalizeTrafficMapPublisherTargets(
	targets []v1beta1.TrafficMapPublisherTarget,
	requireNonEmpty bool,
) ([]v1beta1.TrafficMapPublisherTarget, []string, bool, error) {
	if requireNonEmpty && len(targets) == 0 {
		return nil, nil, false, fmt.Errorf("publisher target list must not be empty")
	}
	if len(targets) > v1beta1.MaxTrafficMapPublisherTargets {
		return nil, nil, false, fmt.Errorf("publisher plan has %d targets, maximum is %d",
			len(targets), v1beta1.MaxTrafficMapPublisherTargets)
	}
	normalized := slices.Clone(targets)
	slices.SortFunc(normalized, func(left, right v1beta1.TrafficMapPublisherTarget) int {
		return strings.Compare(left.Target, right.Target)
	})
	claims := make([]string, len(normalized))
	positive := false
	for i := range normalized {
		target := normalized[i]
		if target.Target == "" {
			return nil, nil, false, fmt.Errorf("publisher target must not be empty")
		}
		if len(target.Target) > v1beta1.MaxTrafficMapPublisherTargetLength {
			return nil, nil, false, fmt.Errorf("publisher target %q has %d bytes, maximum is %d",
				target.Target, len(target.Target), v1beta1.MaxTrafficMapPublisherTargetLength)
		}
		if target.Weight < 0 {
			return nil, nil, false, fmt.Errorf("publisher target %q has negative weight %d",
				target.Target, target.Weight)
		}
		if i > 0 && target.Target == normalized[i-1].Target {
			return nil, nil, false, fmt.Errorf("publisher plan contains duplicate target %q", target.Target)
		}
		claims[i] = target.Target
		positive = positive || target.Weight > 0
	}
	return normalized, claims, positive, nil
}

func validatePublisherDigest(digest string) error {
	if len(digest) != len("sha256:")+sha256.Size*2 || digest[:len("sha256:")] != "sha256:" {
		return fmt.Errorf("must match sha256:<64 lowercase hex characters>")
	}
	hexDigest := digest[len("sha256:"):]
	if hexDigest != strings.ToLower(hexDigest) {
		return fmt.Errorf("must use lowercase hexadecimal")
	}
	if _, err := hex.DecodeString(hexDigest); err != nil {
		return fmt.Errorf("must use hexadecimal: %w", err)
	}
	return nil
}

func trafficMapPlanCapturePositive(capture *TrafficMapPublishPlanCapture) bool {
	return capture != nil && trafficMapTargetsPositive(capture.Targets)
}

func trafficMapTargetsPositive(targets []v1beta1.TrafficMapPublisherTarget) bool {
	return slices.ContainsFunc(targets, func(target v1beta1.TrafficMapPublisherTarget) bool {
		return target.Weight > 0
	})
}

func trafficMapLastPositiveFromCapture(
	trafficMapGeneration int64,
	ownerGeneration int64,
	claims []string,
	capture *TrafficMapPublishPlanCapture,
) *v1beta1.TrafficMapPublisherLastPositive {
	weights := make(map[string]int64, len(capture.Targets))
	for _, target := range capture.Targets {
		weights[target.Target] = target.Weight
	}
	targets := make([]v1beta1.TrafficMapPublisherTarget, 0, len(claims))
	for _, claim := range claims {
		targets = append(targets, v1beta1.TrafficMapPublisherTarget{
			Target: claim,
			Weight: weights[claim],
		})
	}
	return &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    trafficMapGeneration,
		ObservedISVCGeneration:  ownerGeneration,
		PlanCompatibilityDigest: capture.PlanCompatibilityDigest,
		Targets:                 targets,
	}
}

func trafficMapFallbackEligibility(
	trafficMap *v1beta1.TrafficMap,
	owner *v1beta1.InferenceService,
) (eligible bool, known bool) {
	return trafficMapFallbackEligibilityWithAllHomesUnready(trafficMap, owner, false)
}

func trafficMapFallbackEligibilityWithAllHomesUnready(
	trafficMap *v1beta1.TrafficMap,
	owner *v1beta1.InferenceService,
	allowAllHomesUnready bool,
) (eligible bool, known bool) {
	if trafficMap == nil || owner == nil {
		return false, false
	}
	if trafficMap.Spec.CapacityMode == v1beta1.RoutingCapacityModeReported && trafficMapCarriesReport(trafficMap) {
		// A current report decides its cluster's weight, so replica or condition lag
		// cannot delay the table and an earlier positive plan cannot replace it.
		// Source identity and generation are checked before publication. A table
		// without a report has fallen back to replica counts, like a Ceiling table.
		return false, true
	}
	if _, found := owner.Annotations[constants.TrafficDrainAnnotation]; found {
		return false, true
	}
	if owner.Status.ObservedGeneration != owner.Generation {
		return false, false
	}
	routable := apimeta.FindStatusCondition(trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable)
	override := apimeta.FindStatusCondition(trafficMap.Status.Conditions, v1beta1.TrafficMapOverrideActive)
	if routable == nil || override == nil ||
		routable.ObservedGeneration != trafficMap.Generation ||
		override.ObservedGeneration != trafficMap.Generation {
		return false, false
	}
	if routable.Status != metav1.ConditionFalse {
		return false, false
	}
	if trafficMapPlacementMode(trafficMap.Spec.Mode) != trafficMapOwnerPlacementMode(owner) {
		return false, false
	}
	if override.Status != metav1.ConditionFalse || override.Reason != v1beta1.TrafficMapReasonNoOverrides {
		return false, false
	}
	placement := owner.Status.Placement
	if placement == nil {
		return false, true
	}
	ready := owner.Status.GetCondition(apis.ConditionReady)
	placementUnknown := ready != nil && ready.Status == corev1.ConditionUnknown &&
		ready.Reason == placementcontroller.PlacementReadyReasonUnknown
	switch routable.Reason {
	case v1beta1.TrafficMapReasonNotPlaced:
		if len(trafficMap.Spec.Entries) != 0 {
			return false, false
		}
		if placement.Phase == v1beta1.PlacementPhasePlaced {
			// Both eligible routing states have an empty plan, so a status-only
			// source transition (including address recovery) can reach us before
			// Routable changes reason or the routing spec becomes positive.
			return false, false
		}
		if trafficMapPlacementMode(trafficMap.Spec.Mode) != v1beta1.PlacementModeSingle ||
			placement.Phase == v1beta1.PlacementPhaseAdmitting ||
			placement.Phase == v1beta1.PlacementPhaseRacing || //nolint:staticcheck // Treat legacy status as definitive during rolling upgrades.
			placement.Phase == v1beta1.PlacementPhaseFailed {
			return false, true
		}
		if placement.Phase != v1beta1.PlacementPhasePending {
			return false, false
		}
		if placement.Cluster == "" {
			return false, true
		}
		if placement.Endpoint != nil || len(placement.Candidates) != 0 {
			return false, false
		}
		if placementUnknown {
			return false, false
		}
		if ready == nil {
			return false, false
		}
		return trafficMapSinglePlacementLost(trafficMap.Spec.Mode, placement, ready), true
	case v1beta1.TrafficMapReasonNoAddressableHome:
		if len(trafficMap.Spec.Entries) != 0 {
			return false, false
		}
		if placement.Phase == v1beta1.PlacementPhasePending {
			// The source has reached the other eligible empty-plan state while
			// the routing condition still names the previous one.
			return false, false
		}
		if placement.Phase == v1beta1.PlacementPhaseAdmitting ||
			placement.Phase == v1beta1.PlacementPhaseRacing || //nolint:staticcheck // Treat legacy status as definitive during rolling upgrades.
			placement.Phase == v1beta1.PlacementPhaseFailed {
			return false, true
		}
		if placement.Phase != v1beta1.PlacementPhasePlaced {
			return false, false
		}
		if placementUnknown {
			return false, false
		}
		if ready == nil {
			return false, false
		}
		if placement.Endpoint != nil && placement.Endpoint.Host != "" {
			return false, false
		}
		hasReadyWithoutAddress := false
		for _, candidate := range placement.Candidates {
			if candidate.Endpoint != nil && candidate.Endpoint.Host != "" {
				return false, false
			}
			hasReadyWithoutAddress = hasReadyWithoutAddress || candidate.ReadyReplicas > 0
		}
		return hasReadyWithoutAddress, true
	case v1beta1.TrafficMapReasonAllHomesUnready:
		if placement.Phase != v1beta1.PlacementPhaseAdmitting &&
			placement.Phase != v1beta1.PlacementPhaseFailed && placementUnknown {
			return false, false
		}
		if !allowAllHomesUnready {
			return false, true
		}
		if placement.Phase == v1beta1.PlacementPhaseAdmitting ||
			placement.Phase == v1beta1.PlacementPhaseFailed {
			return false, true
		}
		if placement.Phase != v1beta1.PlacementPhasePlaced || ready == nil ||
			ready.Status != corev1.ConditionFalse || len(trafficMap.Spec.Entries) == 0 {
			return false, false
		}
		for _, entry := range trafficMap.Spec.Entries {
			if entry.Endpoint == nil || entry.Endpoint.Host == "" || entry.Weight != 0 || entry.Healthy {
				return false, false
			}
			if len(entry.DrainRefs) != 0 || entry.Probe != nil && entry.Probe.Gated {
				return false, true
			}
			if entry.Capacity == nil {
				return false, false
			}
			if entry.Capacity.Allocated <= 0 {
				return false, true
			}
			if entry.Capacity.Ready != 0 {
				return false, false
			}
		}
		return true, true
	case v1beta1.TrafficMapReasonNoRoutableCapacity,
		v1beta1.TrafficMapReasonAllHomesProbeFailed,
		v1beta1.TrafficMapReasonTrafficDrain:
		return false, true
	default:
		return false, false
	}
}

// trafficMapCarriesReport reports whether any entry's weight comes from an
// accepted endpoint report.
func trafficMapCarriesReport(trafficMap *v1beta1.TrafficMap) bool {
	return slices.ContainsFunc(trafficMap.Spec.Entries, func(entry v1beta1.TrafficMapEntry) bool {
		return entry.Capacity != nil && entry.Capacity.ReportedWeight != nil
	})
}

func trafficMapSinglePlacementLost(
	mode v1beta1.PlacementMode,
	placement *v1beta1.PlacementStatus,
	ready *apis.Condition,
) bool {
	return placement != nil && ready != nil &&
		trafficMapPlacementMode(mode) == v1beta1.PlacementModeSingle &&
		placement.Phase == v1beta1.PlacementPhasePending &&
		placement.Cluster != "" && placement.Endpoint == nil && len(placement.Candidates) == 0 &&
		ready.Status == corev1.ConditionUnknown &&
		ready.Reason == placementcontroller.PlacementReadyReasonLost
}

func trafficMapPlacementMode(mode v1beta1.PlacementMode) v1beta1.PlacementMode {
	if mode == "" {
		return v1beta1.PlacementModeSingle
	}
	return mode
}

func trafficMapOwnerPlacementMode(owner *v1beta1.InferenceService) v1beta1.PlacementMode {
	if owner == nil || owner.Spec.Placement == nil {
		return v1beta1.PlacementModeSingle
	}
	return trafficMapPlacementMode(owner.Spec.Placement.Mode)
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
	if _, err := r.validateLivePublisherOwner(ctx, live, owner); err != nil {
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
		deleteTrafficMapPublicationFallbackMetric(key.Namespace, key.Name, r.Publisher.Name())
		return ctrl.Result{}, nil
	}
	if _, err := normalizedPublisherJournal(live.Status.Publisher, r.Publisher.Name()); err != nil {
		return r.reject(ctx, live, trafficMapPublisherReasonPublisherChanged, err)
	}
	if err := r.patchPublisherStatus(ctx, live, func(status *v1beta1.TrafficMapStatus) error {
		if status.Publisher != nil {
			status.Publisher.LastPositive = nil
		}
		setTrafficMapPublicationConditions(
			status, live.Generation, metav1.ConditionFalse,
			v1beta1.TrafficMapReasonPublicationFinalizing,
			"TrafficMap publisher is removing external state",
		)
		return nil
	}); err != nil {
		return ctrl.Result{}, err
	}
	live = &v1beta1.TrafficMap{}
	if err := r.APIReader.Get(ctx, key, live); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if live.UID != trafficMap.UID {
		return ctrl.Result{}, fmt.Errorf("TrafficMap %s was replaced while finalizing publisher state", key)
	}
	journal := v1beta1.TrafficMapPublisherStatus{PublisherName: r.Publisher.Name()}
	if live.Status.Publisher != nil {
		journal = *copyTrafficMapPublisherJournal(live.Status.Publisher)
	}
	claims, digest, err := publisherJournalForName(&journal, r.Publisher.Name())
	if err != nil {
		return r.reject(ctx, live, trafficMapPublisherReasonPublisherChanged, err)
	}
	journal.PublisherName = r.Publisher.Name()
	journal.ClaimedTargets = claims
	journal.ObservedOptionsDigest = digest
	if err := r.Publisher.Unpublish(ctx, key, *journal.DeepCopy()); err != nil {
		r.Log.Error(err, "TrafficMap publisher finalization failed", "trafficMap", key)
		return ctrl.Result{}, err
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
	deleteTrafficMapPublicationFallbackMetric(key.Namespace, key.Name, r.Publisher.Name())
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
			apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1beta1.TrafficMapPublicationFallback,
				Status:             metav1.ConditionFalse,
				Reason:             v1beta1.TrafficMapReasonPublicationFinalizing,
				Message:            "TrafficMap publication fallback is finalized",
				ObservedGeneration: cleared.Generation,
			})
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

func (r *TrafficMapPublisherReconciler) markPublicationApplied(
	ctx context.Context,
	key types.NamespacedName,
	uid types.UID,
	generation int64,
	expectedClaims []string,
	effectiveOptions map[string]string,
	result TrafficMapPublishResult,
	expectedLastPositive *v1beta1.TrafficMapPublisherLastPositive,
	committedLastPositive *v1beta1.TrafficMapPublisherLastPositive,
	usingFallback bool,
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
			if !equality.Semantic.DeepEqual(status.Publisher.LastPositive, expectedLastPositive) {
				return fmt.Errorf("%w: retained positive plan changed after external apply",
					errTrafficMapPublisherJournalChanged)
			}
		} else if len(liveClaims) != 0 {
			return fmt.Errorf(
				"%w: stateless publisher found %d claimed targets",
				errTrafficMapPublisherJournalChanged, len(liveClaims),
			)
		}
		if status.Publisher == nil || status.Publisher.PublisherName != r.Publisher.Name() {
			status.Publisher = &v1beta1.TrafficMapPublisherStatus{PublisherName: r.Publisher.Name()}
		}
		status.Publisher.ObservedOptionsDigest = digest
		status.GatewayRef = copyTrafficMapGatewayRef(result.GatewayRef)
		if result.Withdrawn {
			status.GatewayRef = nil
		}
		if usingFallback {
			if expectedLastPositive == nil {
				return fmt.Errorf("%w: retained positive plan is missing", errTrafficMapPublisherJournalChanged)
			}
			status.Published = false
			status.ObservedTrafficMapGeneration = expectedLastPositive.TrafficMapGeneration
			apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1beta1.TrafficMapPublished,
				Status:             metav1.ConditionFalse,
				Reason:             v1beta1.TrafficMapReasonPublicationFallback,
				Message:            fmt.Sprintf("TrafficMap publisher %q retained the last positive plan", r.Publisher.Name()),
				ObservedGeneration: generation,
			})
			apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
				Type:               v1beta1.TrafficMapPublicationFallback,
				Status:             metav1.ConditionTrue,
				Reason:             v1beta1.TrafficMapReasonLastPositiveRetained,
				Message:            "The last fully applied positive publication plan is retained",
				ObservedGeneration: generation,
			})
			return nil
		}

		status.Published = !result.Withdrawn
		status.ObservedTrafficMapGeneration = generation
		status.Publisher.LastPositive = copyTrafficMapLastPositive(committedLastPositive)
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
		fallbackReason := trafficMapPublisherReasonCurrentPlan
		fallbackMessage := "The current TrafficMap publication plan is applied"
		if result.Withdrawn {
			fallbackReason = trafficMapPublisherReasonWithdrawn
			fallbackMessage = "TrafficMap publication is withdrawn"
		} else if _, capable := r.Publisher.(TrafficMapPublisherFallback); !capable {
			fallbackReason = trafficMapPublisherReasonUnsupported
			fallbackMessage = "The selected publisher does not retain publication plans"
		} else if committedLastPositive == nil {
			fallbackReason = trafficMapPublisherReasonApplyCurrent
			fallbackMessage = "The current no-positive TrafficMap plan is applied"
		}
		apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               v1beta1.TrafficMapPublicationFallback,
			Status:             metav1.ConditionFalse,
			Reason:             fallbackReason,
			Message:            fallbackMessage,
			ObservedGeneration: generation,
		})
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
			setTrafficMapPublicationConditions(
				status, trafficMap.Generation, metav1.ConditionUnknown,
				reason, trafficMapPublisherFailureMessage(reason),
			)
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
	clearsLastPositive := live.Status.Publisher != nil && live.Status.Publisher.LastPositive != nil &&
		(desired.Status.Publisher == nil || desired.Status.Publisher.LastPositive == nil)
	if (live.Status.Published && !desired.Status.Published) ||
		(live.Status.GatewayRef != nil && desired.Status.GatewayRef == nil) || clearsLastPositive {
		narrowed := live.DeepCopy()
		narrowed.Status.Published = desired.Status.Published
		// The boolean and its conditions form one acknowledgement. Readers must
		// never observe a cleared boolean with a successful Published condition.
		for _, conditionType := range []string{v1beta1.TrafficMapPublished, v1beta1.TrafficMapPublicationFallback} {
			if condition := apimeta.FindStatusCondition(desired.Status.Conditions, conditionType); condition != nil {
				apimeta.SetStatusCondition(&narrowed.Status.Conditions, *condition)
			}
		}
		narrowed.Status.GatewayRef = copyTrafficMapGatewayRef(desired.Status.GatewayRef)
		if clearsLastPositive && narrowed.Status.Publisher != nil {
			narrowed.Status.Publisher.LastPositive = nil
		}
		if err := r.Status().Patch(ctx, narrowed,
			client.MergeFromWithOptions(live, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		desired.ResourceVersion = narrowed.ResourceVersion
	}
	// Apply even when the typed values are unchanged so this controller remains
	// the declared owner of its sparse status projection after a restart.
	if err := r.applyPublisherStatus(ctx, desired); err != nil {
		return err
	}
	recordTrafficMapPublicationFallbackMetric(desired, r.Publisher.Name())
	return nil
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
		if trafficMap.Status.Publisher.LastPositive != nil {
			lastPositive := trafficMap.Status.Publisher.LastPositive
			targets := make([]any, len(lastPositive.Targets))
			for i, target := range lastPositive.Targets {
				targets[i] = map[string]any{
					"target": target.Target,
					"weight": target.Weight,
				}
			}
			publisher["lastPositive"] = map[string]any{
				"trafficMapGeneration":    lastPositive.TrafficMapGeneration,
				"observedISVCGeneration":  lastPositive.ObservedISVCGeneration,
				"planCompatibilityDigest": lastPositive.PlanCompatibilityDigest,
				"targets":                 targets,
			}
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
	conditions := make([]any, 0, 2)
	for _, conditionType := range []string{
		v1beta1.TrafficMapPublished,
		v1beta1.TrafficMapPublicationFallback,
	} {
		if condition := apimeta.FindStatusCondition(trafficMap.Status.Conditions, conditionType); condition != nil {
			conditions = append(conditions, trafficMapConditionApply(*condition))
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
		For(&v1beta1.TrafficMap{}, builder.WithPredicates(trafficMapPublicationChange)).
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
	if _, capable := r.Publisher.(TrafficMapPublisherFallback); capable && !r.Publisher.Stateful() {
		return fmt.Errorf("TrafficMap publisher %q exposes retained-plan fallback but is stateless", r.Publisher.Name())
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

var trafficMapPublicationChange = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldMap, oldOK := e.ObjectOld.(*v1beta1.TrafficMap)
		newMap, newOK := e.ObjectNew.(*v1beta1.TrafficMap)
		if !oldOK || !newOK || placementcontroller.TrafficMapInputsChanged(oldMap, newMap) {
			return true
		}
		// Routing verdicts govern fallback eligibility. Publisher-owned status
		// is an output; the configured resync repairs external drift.
		for _, conditionType := range []string{v1beta1.TrafficMapRoutable, v1beta1.TrafficMapCapacityFallback, v1beta1.TrafficMapOverrideActive} {
			if !equality.Semantic.DeepEqual(apimeta.FindStatusCondition(oldMap.Status.Conditions, conditionType), apimeta.FindStatusCondition(newMap.Status.Conditions, conditionType)) {
				return true
			}
		}
		return false
	},
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
			!equality.Semantic.DeepEqual(oldOwner.Annotations, newOwner.Annotations) ||
			oldOwner.Status.ObservedGeneration != newOwner.Status.ObservedGeneration ||
			!equality.Semantic.DeepEqual(oldOwner.Status.Placement, newOwner.Status.Placement) ||
			!equality.Semantic.DeepEqual(
				oldOwner.Status.GetCondition(apis.ConditionReady),
				newOwner.Status.GetCondition(apis.ConditionReady),
			)
	},
}

var (
	errTrafficMapPublisherPlanStale          = errors.New("TrafficMap changed while claiming publisher targets")
	errTrafficMapPublisherJournalChanged     = errors.New("TrafficMap publisher journal changed during reconciliation")
	errTrafficMapPublisherChanged            = errors.New("TrafficMap publisher changed while claims remain")
	errTrafficMapPublisherSourceChanged      = errors.New("TrafficMap source provenance changed during reconciliation")
	errTrafficMapPublisherOwnerInvalid       = errors.New("TrafficMap owner binding is invalid")
	errTrafficMapPublisherOwnerAhead         = errors.New("TrafficMap owner generation is ahead of the map")
	errTrafficMapPublisherOwnerRead          = errors.New("TrafficMap owner state read failed")
	errTrafficMapPublisherEligibilityUnknown = errors.New("TrafficMap fallback eligibility is unknown")
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
	if journal.PublisherName != name && (len(claims) != 0 || journal.LastPositive != nil) {
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
	if publisherJournalHasClaims(status.Publisher) ||
		(status.Publisher != nil && status.Publisher.LastPositive != nil) {
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

func publisherClaimsReplayCompatible(current, durable []string) bool {
	return len(current) == 0 || slices.Equal(current, durable)
}

func publisherClaimsReplayCompatibleForAllHomesUnready(
	current, durable []string,
	lastPositive *v1beta1.TrafficMapPublisherLastPositive,
) bool {
	if len(current) == 0 || lastPositive == nil {
		return false
	}
	currentSet := make(map[string]struct{}, len(current))
	durableSet := make(map[string]struct{}, len(durable))
	for _, claim := range current {
		currentSet[claim] = struct{}{}
	}
	for _, claim := range durable {
		durableSet[claim] = struct{}{}
	}
	for _, claim := range current {
		if _, found := durableSet[claim]; !found {
			return false
		}
	}
	for _, target := range lastPositive.Targets {
		if _, found := currentSet[target.Target]; !found && target.Weight > 0 {
			return false
		}
	}
	return true
}

func trafficMapRoutableReason(trafficMap *v1beta1.TrafficMap) string {
	if trafficMap == nil {
		return ""
	}
	condition := apimeta.FindStatusCondition(
		trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
	)
	if condition == nil {
		return ""
	}
	return condition.Reason
}

// routingPublisherOptions returns a copy of the service's publisher options:
// spec.routing.publisher.options together with the options its
// ome.io/routing-publisher.<option> annotations set. The annotations are
// checked here, under the limits of the spec field, because the control plane
// runs no InferenceService admission webhook.
func routingPublisherOptions(owner *v1beta1.InferenceService) (map[string]string, error) {
	if owner == nil {
		return map[string]string{}, nil
	}
	if err := validation.ValidateRoutingPublisherOptionAnnotations(owner.Annotations, &owner.Spec); err != nil {
		return nil, err
	}
	var specOptions map[string]string
	if owner.Spec.Routing != nil && owner.Spec.Routing.Publisher != nil {
		specOptions = owner.Spec.Routing.Publisher.Options
	}
	options := clonePublisherOptions(specOptions)
	for key, value := range owner.Annotations {
		if option, found := strings.CutPrefix(key, constants.RoutingPublisherOptionAnnotationPrefix); found {
			options[option] = value
		}
	}
	return options, nil
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
