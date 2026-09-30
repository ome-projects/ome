package placement

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/validation"
)

const (
	// DefaultPlacementRequeue is the fallback status-refresh poll cadence used
	// when Reconciler.Requeue is unset. The operative value is supplied by the
	// manager flag / chart; this is the graceful-degradation default.
	DefaultPlacementRequeue = 30 * time.Second

	// DefaultPlaceTimeout is the fallback per-cluster operation deadline used
	// when Reconciler.PlaceTimeout is unset. The operative value is supplied by
	// the manager flag / chart; this is the graceful-degradation default that
	// keeps one wedged remote from stalling progress on its healthy peers.
	DefaultPlaceTimeout = 10 * time.Second

	// DefaultWinnerLostGrace is the fallback grace window used when
	// Reconciler.WinnerLostGracePeriod is unset. The operative value is supplied
	// by the manager flag / chart; this is the graceful-degradation default that
	// lets a transient winner-derived gap heal before re-racing.
	DefaultWinnerLostGrace = 1 * time.Minute

	placementReadyReasonPending = "PlacementPending"
	// PlacementReadyReasonLost identifies an authoritative loss of every current
	// home without treating an unreadable member as absent.
	PlacementReadyReasonLost = "PlacementLost"
	// PlacementReadyReasonUnknown identifies an incomplete placement observation
	// that cannot prove either serving readiness or authoritative placement loss.
	PlacementReadyReasonUnknown    = "PlacementUnknown"
	placementReadyReasonAdmitting  = "PlacementAdmitting"
	placementReadyReasonReady      = "PlacementReady"
	placementReadyReasonNotReady   = "PlacementNotReady"
	placementReadyReasonFailed     = "PlacementFailed"
	placementReadyMessagePending   = "Waiting for an eligible workload cluster"
	placementReadyMessageLost      = "All recorded placement homes are confirmed absent"
	placementReadyMessageAdmitting = "Waiting for a workload cluster to admit the InferenceService"
	placementReadyMessageReady     = "At least one admitted placement has a ready ingress and ready replicas"
	placementReadyMessageNotReady  = "Placement is admitted but no candidate has a ready ingress, endpoint, and replicas"
	placementReadyMessageFailed    = "The selected placement failed terminally"
	placementReadyMessageUnknown   = "Serving readiness could not be fully observed for this placement"
)

var sourcePlacementConditionSet = apis.NewLivingConditionSet()

// placementResult is the status the reconciler writes for one pass.
type placementResult struct {
	conditions       []policyCondition
	winner           string
	phase            v1beta1.PlacementPhase
	candidates       []v1beta1.CandidatePlacement
	url              *apis.URL // published endpoint; mirrored to BOTH status.placement.endpoint and status.url
	ready            bool      // at least one observed admitted home has ready replicas and a ready ingress
	readinessUnknown bool      // one or more candidates lacked a current serving-readiness observation
	placementLost    bool      // every relevant standing home was conclusively absent
}

// ClusterClients is the subset of *workloadcluster.Manager the placer needs.
// *workloadcluster.Manager satisfies it; tests inject a fake.
type ClusterClients interface {
	ClientFor(name string) (workloadcluster.SelectivelyCachingClient, bool)
	Connected() []string
}

// Reconciler is the control-plane fan-out controller. It clones the
// derived ISVC onto every matched candidate cluster, lets each cluster's Kueue
// gate the pods, declares the first candidate (by sorted name) whose Kueue admits
// the pods the winner, deletes the losers, and re-places if the winner is later
// lost. It runs ONLY on the control-plane cluster, where the local ISVC->pods
// reconciler is disabled. Status refresh is event-driven via the remote
// watch funnel, with a poll fallback (the Requeue cadence).
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Log      logr.Logger
	Clusters ClusterClients
	// APIReader is the live (uncached) reader used to re-read the control-plane
	// ISVC inside the conflict-retry loop. This controller and the endpoint
	// publisher both write this object, so a 409 here is genuine external
	// contention: the apiserver holds a newer ResourceVersion than the informer
	// has observed, and re-reading the cache would resubmit the same stale base
	// for the whole backoff. Required: SetupWithManager defaults it to
	// mgr.GetAPIReader() and rejects a reconciler still missing it.
	APIReader client.Reader
	// MemberOperatorNamespace locates member configuration and runtime pins.
	// Missing configuration holds resolution of dependencies that need it.
	MemberOperatorNamespace string
	// Capacity locates verified hardware inputs for capacity allocations.
	Capacity        *CapacityConfig
	CapacityClock   clock.PassiveClock
	capacityMu      sync.Mutex
	capacityPending map[types.NamespacedName]capacityPending
	// InstanceStatusDecoder decodes the per-Instance representation of the
	// member-cluster InferenceReplica statuses this controller inspects, under
	// the operator-configured ColumnarV2 row bound. The zero value carries no
	// bound: DenseV1 decodes unchanged and any ColumnarV2 object fails closed.
	InstanceStatusDecoder irstatus.Decoder
	// Requeue is the status-refresh poll cadence; defaults to DefaultPlacementRequeue.
	Requeue time.Duration
	// LocalQueue is the Kueue LocalQueue a derived workload's pods join when the
	// source ISVC carries no per-ISVC queue annotation. It names a resource the
	// operator provisioned, so it is config-driven (manager flag / chart) with no
	// in-code default; empty leaves the queue label off the derived.
	LocalQueue string
	// ControlPlaneID is this control plane's identity, stamped onto every derived
	// ISVC (PlacementControlPlaneLabel) so the GC sweep only reaps deriveds THIS
	// control plane created. Config-driven (manager flag / chart); empty leaves
	// the stamp off and keeps single-control-plane behavior.
	ControlPlaneID string
	// MaxConcurrentReconciles caps parallel placement reconciles (distinct ISVCs
	// only — controller-runtime serializes per object key, so independent ISVCs
	// reconcile in parallel safely). Each reconcile fans out across remote
	// clusters, so the single-worker default serializes the whole fleet behind
	// one slow remote; raising this lets independent ISVCs make progress
	// concurrently. Sourced from a flag/chart value; no in-code default. Zero
	// (unset) preserves controller-runtime's single-worker default.
	MaxConcurrentReconciles int
	// PlaceTimeout bounds a single per-cluster observation, apply, or delete so one
	// stuck/slow remote cannot block progress on the healthy candidates. The
	// operative value is supplied by the manager flag / chart; zero (unset) falls
	// back to DefaultPlaceTimeout (graceful degradation, no magic literal in the
	// hot path).
	PlaceTimeout time.Duration
	// WinnerLostGracePeriod is how long the controller waits after the sticky
	// winner is confirmed absent or loses admission before giving up and re-racing.
	// This covers derived-workload recreation and WorkloadCluster removal without
	// treating an unreadable member as evidence of loss. The controller re-races
	// once the window elapses. Config-driven
	// (manager flag / chart); zero (unset) falls back to DefaultWinnerLostGrace.
	WinnerLostGracePeriod time.Duration

	// DispatcherMode selects the fan-out BREADTH policy: AllAtOnce clones onto
	// every matched candidate at once (the historical behavior); Incremental
	// probes the candidates in batches. Config-driven (manager flag / chart);
	// empty/unrecognized degrades gracefully to all-at-once via dispatcherFor, so
	// absent config preserves the existing fleet-wide fan-out.
	DispatcherMode DispatcherMode
	// DispatcherStepSize is how many additional candidates the Incremental
	// dispatcher nominates per round. Only consulted in Incremental mode.
	// Config-driven (manager flag / chart); a non-positive value makes the walk
	// advance by one candidate per round (graceful degradation, no magic literal).
	DispatcherStepSize int
	// DispatcherRoundTimeout bounds how long one Incremental round is given for a
	// nominated cluster to win before the next batch is added. Only consulted in
	// Incremental mode. Config-driven (manager flag / chart); a non-positive value
	// lets each pass advance with no enforced dwell.
	DispatcherRoundTimeout time.Duration

	// dispatcher is the resolved breadth policy (built once from DispatcherMode on
	// first use). Its nominations feed the persisted race plan. dispatcherOnce
	// guards lazy construction so the Reconciler stays usable when assembled as a
	// bare struct literal (the cmd wiring and the tests both do that) without a
	// constructor.
	dispatcher     Dispatcher
	dispatcherOnce sync.Once

	// winnerLostSince tracks, per source-ISVC UID, the first time this controller
	// observed the sticky winner as absent or as having lost admission.
	// It is the in-memory backing for WinnerLostGracePeriod: the PlacementStatus
	// API carries no such timestamp and this package may not extend it, so the
	// grace clock lives here. A control-plane restart loses the marker and the
	// grace window simply restarts — strictly conservative (it only ever delays a
	// re-race), never destructive. Entries are cleared the moment the derived is
	// seen again or the winner is re-raced.
	winnerLostSince sync.Map // map[types.UID]time.Time

	// converge is the resolved status-convergence configuration assembled at
	// SetupWithManager from ConvergeOptions (the remote watch-funnel channel plus
	// the batch/safety timings). Nil until Setup runs — the helper methods fall
	// back to the Requeue struct field + package defaults so a Reconciler built
	// directly (unit tests) keeps working without Setup.
	converge *convergeConfig

	// policy is the AutoscalerPolicy preflight/skew state: the config-driven
	// preflight tunables plus the in-memory per-(source,home) bookkeeping the
	// multi-cluster policy detectors need (the API carries no such state).
	// SetupWithManager resolves it from the autoscalerPolicy config block; a
	// Reconciler built directly (unit tests) gets the package defaults lazily
	// via policyState. Inert for sources without a policy ref.
	policy     *policyPreflight
	policyOnce sync.Once

	// Recorder emits placement preflight warnings (e.g. a placed plan whose
	// manual gate cannot be advanced from the control plane) as Kubernetes
	// events on the source ISVC. SetupWithManager defaults it from the
	// manager; nil (a bare-struct Reconciler) skips event emission.
	Recorder record.EventRecorder

	// rollout is the RolloutPolicy preflight state: the staged condition, the
	// per-source resolved policies the derive-time inflation consumes, and the
	// per-(source,home) lifted run provenance. Built lazily via rolloutState;
	// inert for sources without rollout policy refs.
	rollout     *rolloutPreflight
	rolloutOnce sync.Once
}

// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices/finalizers,verbs=update
// +kubebuilder:rbac:groups=ome.io,resources=workloadclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=rolloutpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=trafficmaps,verbs=get

// Reconcile maps an apiserver optimistic-lock conflict to a clean requeue. The
// control plane runs several controllers that write the same source
// InferenceService — this placer writes status and its finalizer, the endpoint
// publisher writes its own finalizer — so a write can lose a resourceVersion
// race. That is benign and self-corrects on the requeue, so it must not surface
// as an error-level "Reconciler error" (misleading log noise). All other results
// pass through unchanged.
func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcile(ctx, request)
	if apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return res, err
}

func (r *Reconciler) reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	isvc := &v1beta1.InferenceService{}
	if err := r.Get(ctx, request.NamespacedName, isvc); err != nil {
		if apierrors.IsNotFound(err) {
			r.forgetCapacity(request.NamespacedName)
			// Source already gone. The winner-lost grace marker is keyed by UID
			// (unknown here) and is cleared in reconcileDelete once the finalizer
			// runs; the worst case if that never ran is one stale map entry that
			// never grows (a re-created ISVC gets a fresh UID), so nothing to do.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !isvc.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, isvc)
	}
	// The primary watch and queued status events can include local services.
	// Only placement sources may acquire placement state or remote allocations.
	if !IsPlacementEligible(isvc) {
		r.forgetCapacity(request.NamespacedName)
		return ctrl.Result{}, nil
	}

	// The committed winner is placement authority; informer lag cannot reopen
	// its race or discard a terminal winner between status writes.
	if r.APIReader == nil {
		return ctrl.Result{}, fmt.Errorf("placement requires a direct source reader")
	}
	current := &v1beta1.InferenceService{}
	if err := r.APIReader.Get(ctx, request.NamespacedName, current); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !current.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, current)
	}
	if !IsPlacementEligible(current) {
		r.forgetCapacity(request.NamespacedName)
		return ctrl.Result{}, nil
	}
	isvc = current
	if !isvc.Spec.Placement.UsesClusterAffinity() {
		r.forgetCapacity(request.NamespacedName)
		return r.reconcileLegacy(ctx, isvc)
	}
	ctx = unverifiedBackend(ctx, isvc)
	if placementMode(isvc) != v1beta1.PlacementModeSplitByCapacity {
		r.forgetCapacity(request.NamespacedName)
	}
	clusters := &v1beta1.WorkloadClusterList{}
	if err := r.List(ctx, clusters); err != nil {
		return ctrl.Result{}, err
	}
	// Member observation is independent from placement eligibility. Status homes
	// are read before any readiness or policy gate so those gates can stop
	// actuation without freezing the source's view of serving health.
	observations := r.observeStandingHomes(ctx, isvc, clusters.Items)
	if err := validation.ValidatePlacementIntent(isvc); err != nil {
		return r.writeObservedPlacement(ctx, isvc, observations)
	}
	if isvc.Status.Placement != nil && isvc.Status.Placement.Plan != nil && isvc.Status.Placement.Plan.Mode != "" && placementMode(isvc) != isvc.Status.Placement.Plan.Mode {
		observations.projectAll = true
		return r.writeSplitHold(ctx, isvc, observations, "PlacementModeChangeBlocked", fmt.Errorf("standing full-policy homes require their accepted execution mode"))
	}
	// Validate before a full-object write can normalize or prune invalid intent.
	if controllerutil.AddFinalizer(isvc, PlacementFinalizer) {
		if err := r.Update(ctx, isvc); err != nil {
			return ctrl.Result{}, err
		}
	}

	candidates, reason, err := MatchCandidates(isvc, clusters.Items)
	split := placementMode(isvc) == v1beta1.PlacementModeSplit || placementMode(isvc) == v1beta1.PlacementModeSplitByCapacity
	if split || placementMode(isvc) == v1beta1.PlacementModeAll {
		// Outgoing homes still serve during a move. Unreadable peers cannot
		// prevent discovery of health on the remaining matched members.
		observations.projectAll = true
		for _, cluster := range clusters.Items {
			if split && observations.matches[cluster.Name] && !slices.Contains(observations.standing, cluster.Name) {
				observations.standing = append(observations.standing, cluster.Name)
				observations.refresh(ctx, r, isvc, cluster.Name)
			}
		}
	}
	retainedSingle := placementMode(isvc) == v1beta1.PlacementModeSingle && winnerCluster(isvc) != ""
	if err != nil || (len(candidates) == 0 && !split && placementMode(isvc) != v1beta1.PlacementModeAll && !retainedSingle) {
		// Surface WHY there are no candidates (malformed selector / no
		// requirements declared / no Ready clusters / no match) so the empty
		// set is diagnosable instead of an indistinguishable Pending.
		r.Log.Info("no placement candidates", "isvc", isvc.Namespace+"/"+isvc.Name,
			"reason", reason, "error", err)
		if isvc.Status.Placement != nil && len(observations.standing) > 0 {
			// WorkloadCluster readiness is sampled independently from the derived
			// workload. Keep reconciling standing status while the current fleet
			// view blocks new placement.
			return r.writeObservedPlacement(ctx, isvc, observations)
		}
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}

	// AutoscalerPolicy preflight: for a source that references policies, gate
	// the candidate set on member capability, policy presence, and digest
	// equality against the control-plane anchor, and hold Split entirely while
	// its per-cluster ceiling is unbounded. Nil (candidates untouched) for the
	// common no-ref ISVC.
	pf := r.preflightPolicies(ctx, isvc, candidates, clusters.Items)
	if pf != nil && pf.holdAsIs {
		return r.writeObservedPlacement(ctx, isvc, observations)
	}
	if pf != nil && !pf.hold {
		candidates = pf.eligible
	}

	// RolloutPolicy preflight: resolve ref-only rollout groups against the
	// control-plane policy copies (the bodies fan-out inflates into the
	// derived spec) and gate candidates on the rollout capability label. Runs
	// on the autoscaler-narrowed candidate set; skipped while the autoscaler
	// preflight already holds (nothing fans out either way). Nil for the
	// common rollout-less ISVC.
	var rf *policyPreflightOutcome
	if pf == nil || !pf.hold {
		rf = r.preflightRolloutPolicies(ctx, isvc, candidates, clusters.Items)
		if rf != nil && rf.holdAsIs {
			return r.writeObservedPlacement(ctx, isvc, observations)
		}
		if rf != nil && !rf.hold {
			candidates = rf.eligible
		}
	}

	var res ctrl.Result
	if (pf != nil && pf.hold) || (rf != nil && rf.hold) {
		res, err = r.writeObservedPlacement(ctx, isvc, observations)
	} else {
		ctx, err = r.preflightBackends(ctx, isvc, clusters.Items, candidates)
		if err != nil {
			r.forgetCapacity(request.NamespacedName)
			observations.projectAll = true
			for _, name := range observations.standing {
				if _, exists := observations.get(name); !exists {
					observations.refresh(ctx, r, isvc, name)
				}
			}
			return r.writeObservedPlacement(ctx, isvc, observations)
		}
		backendEligible := verifiedBackendCandidates(ctx, candidates)
		// Branch on placement mode. Each mode owns its own reconcile: Single keeps one
		// winner, All keeps every home that admits, and Split apportions the requested
		// replicas across its candidate set.
		switch mode := placementMode(isvc); mode {
		case v1beta1.PlacementModeSingle:
			res, err = r.reconcileSinglePlanned(ctx, isvc, clusters.Items, backendEligible, observations)
		case v1beta1.PlacementModeAll:
			res, err = r.reconcileAllPlanned(ctx, isvc, clusters.Items, backendEligible, observations)
		case v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity:
			res, err = r.reconcileSplit(ctx, isvc, clusters.Items, backendEligible, observations)
		default:
			r.Log.Info("placement mode not supported by this build; holding Pending",
				"mode", mode, "isvc", isvc.Namespace+"/"+isvc.Name)
			res, err = r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
		}
	}
	transient := (pf != nil && pf.transient) || (rf != nil && rf.transient)
	if err == nil && transient && res.RequeueAfter > r.requeue() {
		// A preflight verdict this pass rests on a transient member or anchor
		// read error: re-verify at the poll cadence rather than waiting out
		// the long steady-state backstop.
		res.RequeueAfter = r.requeue()
	}
	return res, err
}

// placementMode resolves the default only for Legacy placement.
func placementMode(isvc *v1beta1.InferenceService) v1beta1.PlacementMode {
	return isvc.Spec.Placement.EffectiveMode()
}

// splitDesiredReplicas is the Split desired replica count: spec.placement.split.
// replicas when set, else the engine component's minReplicas (the guaranteed
// floor). Zero when neither is declared.
func splitDesiredReplicas(isvc *v1beta1.InferenceService) int32 {
	if p := isvc.Spec.Placement; p != nil && p.Split != nil && p.Split.Replicas != nil {
		return *p.Split.Replicas
	}
	if isvc.Spec.Engine != nil && isvc.Spec.Engine.MinReplicas != nil {
		floor := *isvc.Spec.Engine.MinReplicas
		if floor > 0 && floor <= math.MaxInt32 {
			return int32(floor)
		}
	}
	return 0
}

// placementScaleComponents are the replica-scaled components whose
// admitted/ready counts define a home's replica count: Engine and, for a PD
// service, the Decoder — a replica is the coordinated pair. Router is excluded
// (a shared front-end, not per-replica). Falls back to the first declared
// component when neither Engine nor Decoder is present.
func placementScaleComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	var cs []v1beta1.ComponentType
	if isvc.Spec.Engine != nil {
		cs = append(cs, v1beta1.EngineComponent)
	}
	if isvc.Spec.Decoder != nil {
		cs = append(cs, v1beta1.DecoderComponent)
	}
	if len(cs) == 0 {
		if dc := declaredComponents(isvc); len(dc) > 0 {
			cs = append(cs, dc[0])
		}
	}
	return cs
}

// placementAdmittedReplicas is a home's admitted replica count: the MIN
// admitted instances across the scaled components, since a PD replica is
// admitted only when BOTH its engine and decoder instances are (an engine-only
// home is just the engine count). Zero when no scaled component is declared.
func placementAdmittedReplicas(comps []v1beta1.ComponentType, statuses map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus) int32 {
	if len(comps) == 0 {
		return 0
	}
	mn := int32(math.MaxInt32)
	for _, c := range comps {
		if n := admittedReplicaCount(statuses[c]); n < mn {
			mn = n
		}
	}
	return mn
}

// placementReadyReplicas is a home's ready replica count (the endpoint weight):
// the MIN ReadyReplicas across the scaled components, for the same pairing
// reason.
func placementReadyReplicas(comps []v1beta1.ComponentType, statuses map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus) int32 {
	if len(comps) == 0 {
		return 0
	}
	mn := int32(math.MaxInt32)
	for _, c := range comps {
		var r int32
		if st := statuses[c]; st != nil {
			r = st.ReadyReplicas
		}
		if r < mn {
			mn = r
		}
	}
	return mn
}

func (r *Reconciler) deleteDerivedOnBounded(ctx context.Context, cluster string, isvc *v1beta1.InferenceService) error {
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	defer cancel()
	return r.deleteDerivedOn(cctx, cluster, isvc)
}

// deleteLosers best-effort deletes THIS ISVC's derived copy on each cluster in
// clusters except keep. deleteDerivedOn only ever deletes an object that carries
// this source ISVC's origin label, so a same-named ISVC a user created directly
// on a workload cluster is never touched (cross-tenant data-loss guard).
//
// Per-cluster failures are tolerated: one slow/unreachable cluster
// must not block loser cleanup on the healthy ones, and must NOT block recording
// the winner. Failures are logged; the next poll retries, and the GC sweep is a
// backstop. Callers that must confirm full teardown (reconcileDelete) verify the
// derived is actually gone separately rather than relying on the return here.
func (r *Reconciler) deleteLosers(ctx context.Context, isvc *v1beta1.InferenceService, clusters []string, keep string) {
	for _, c := range clusters {
		if c == keep {
			continue
		}
		if err := r.deleteDerivedOnBounded(ctx, c, isvc); err != nil {
			r.Log.Error(err, "loser cleanup failed on cluster (will retry next poll)", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
		}
	}
}

// getDerived fetches the derived ISVC on a cluster. Returns ok=false if the
// cluster is not connected or the object does not exist.
func (r *Reconciler) getDerived(ctx context.Context, cluster string, isvc *v1beta1.InferenceService) (*v1beta1.InferenceService, bool, error) {
	cl, ok := r.Clusters.ClientFor(cluster)
	if !ok {
		return nil, false, nil
	}
	derived := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}, derived); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	r.observeDerivedPolicyStatus(isvc, cluster, derived)
	r.observeDerivedRolloutStatus(isvc, cluster, derived)
	return derived, true, nil
}

// placeOn creates-or-updates the derived ISVC on the target cluster. The control
// plane owns the derived Spec and the metadata keys IT stamps (origin markers,
// Kueue queue/serving gating); it does NOT own the rest of the object's labels
// and annotations. The worker-cluster reconciler adds its own metadata (status
// bookkeeping annotations, generated labels) to the derived object, so this
// merges the control-plane-owned keys into the existing maps rather than
// replacing them wholesale — preserving worker-side reconciler state across the
// poll-driven re-apply.
func (r *Reconciler) placeOn(ctx context.Context, cluster string, src *v1beta1.InferenceService, existingOnly bool) error {
	cl, target, err := r.backendClient(ctx, src, cluster)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, backendTargetKey{}, target)
	d, err := r.derivedFor(src)
	if err != nil {
		return err
	}
	return r.applyDerived(ctx, cluster, cl, src, d, existingOnly)
}

// derivedFor builds the derived ISVC for src, inflating ref-only rollout
// groups from the policies the rollout preflight resolved this pass. A ref
// with no staged resolution fails the per-cluster apply outright — a derived
// spec is placed fully inflated or not at all.
func (r *Reconciler) derivedFor(src *v1beta1.InferenceService) (*v1beta1.InferenceService, error) {
	d := DeriveISVC(src, r.ControlPlaneID, r.LocalQueue)
	if err := inflateRolloutGroups(d, r.rolloutState().plansFor(src.UID)); err != nil {
		return nil, fmt.Errorf("derive %s/%s: %w", src.Namespace, src.Name, err)
	}
	return d, nil
}

// applyDerived create-or-updates the derived ISVC `desired` on the target
// cluster.
func (r *Reconciler) applyDerived(ctx context.Context, cluster string, cl client.Client, src, desired *v1beta1.InferenceService, existingOnly bool) error {
	policy, err := protocol.FromDerived(desired)
	if err != nil {
		return err
	}
	target := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, cl, target, func() error {
		// A sticky refresh cannot recreate a winner before its absence grace.
		if existingOnly && target.ResourceVersion == "" {
			return errMemberAbsent
		}
		// A same-named local service or another source's copy cannot be adopted.
		if target.ResourceVersion != "" && !isOurDerived(target, src) {
			return fmt.Errorf("refusing to overwrite non-derived InferenceService %s/%s on candidate cluster: not a placement derived of control plane %q",
				target.Namespace, target.Name, r.ControlPlaneID)
		}
		backend, err := r.resolveMemberBackend(ctx, cl, src, desired)
		if err != nil {
			return err
		}
		current, err := protocol.FromDerived(target)
		if err != nil {
			return err
		}
		if err := protocol.Authorize(current, policy); err != nil {
			return err
		}
		if policy != nil {
			if target.ResourceVersion == "" {
				if err := requireEmptyMemberInventory(ctx, cl, src); err != nil {
					return err
				}
			}
			if err := r.checkPlanCurrent(ctx, src); err != nil {
				return err
			}
		}
		if err := backend.Check(ctx); err != nil {
			return err
		}
		if err := r.checkSourceSnapshot(ctx, src); err != nil {
			return err
		}
		if err := r.checkBackendTransport(ctx, cluster); err != nil {
			return err
		}
		// Before the wholesale re-stamp below overwrites it, the live remote
		// spec is the evidence for the FieldPruned detector: a member apiserver
		// that pruned a stamped autoscalerPolicyRef reverts it here every pass.
		r.observePolicyRefStamp(src, cluster, target, desired)
		target.Labels = mergeOwnedKeys(target.Labels, desired.Labels)
		target.Annotations = mergeOwnedKeys(target.Annotations, desired.Annotations)
		target.Spec = desired.Spec
		return nil
	})
	return err
}

// mergeOwnedKeys overlays the control-plane-owned keys (from desired) onto the
// existing map without dropping keys the worker-cluster reconciler added. nil
// existing maps are initialized only when there is something to set.
func mergeOwnedKeys(existing, owned map[string]string) map[string]string {
	if len(owned) == 0 {
		return existing
	}
	if existing == nil {
		existing = make(map[string]string, len(owned))
	}
	for k, v := range owned {
		existing[k] = v
	}
	return existing
}

// deleteDerivedOn best-effort deletes the derived ISVC on a cluster, but ONLY
// if the object actually present there is a copy this control plane derived from
// THIS source ISVC (it carries our origin label set to the source UID). It never
// deletes by identity alone: a same-named ISVC a user created directly on the
// workload cluster, or a copy derived from a different source UID, is left
// untouched. The UID precondition on Delete closes the read-then-delete race so
// a concurrently-recreated object isn't deleted out from under its owner.
func (r *Reconciler) deleteDerivedOn(ctx context.Context, cluster string, isvc *v1beta1.InferenceService) error {
	cl, ok := r.Clusters.ClientFor(cluster)
	if !ok {
		return nil
	}
	derived := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}, derived); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !isOurDerived(derived, isvc) {
		// Same-named object that is NOT ours: do not delete (cross-tenant guard).
		return nil
	}
	uid := derived.UID
	opts := []client.DeleteOption{}
	if uid != "" {
		opts = append(opts, client.Preconditions{UID: &uid})
	}
	if err := cl.Delete(ctx, derived, opts...); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// isOurDerived reports whether derived is a placement copy this control plane
// created from src (its origin label/annotation equals src's UID). The empty UID
// case (e.g. a fake/source without a server-assigned UID) requires the marker to
// be present and non-empty, never matches a blank.
func isOurDerived(derived, src *v1beta1.InferenceService) bool {
	want := string(src.UID)
	if want == "" {
		return false
	}
	return derived.Labels[PlacementOriginLabel] == want ||
		derived.Annotations[PlacementOriginUIDAnnotation] == want
}

func (r *Reconciler) reconcileDelete(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(isvc, PlacementFinalizer) {
		return ctrl.Result{}, nil
	}
	// The endpoint publisher owns this finalizer and removes it after its route is
	// authoritatively absent. Leave both the source and derived workloads intact
	// until then; the endpoint controller can continue reconciling independently.
	if controllerutil.ContainsFinalizer(isvc, EndpointFinalizer) {
		return ctrl.Result{RequeueAfter: r.requeue()}, nil
	}
	trafficMapRemaining, err := r.ownedTrafficMapRemaining(ctx, isvc)
	if err != nil {
		return ctrl.Result{}, err
	}
	if trafficMapRemaining {
		return ctrl.Result{RequeueAfter: r.requeue()}, nil
	}
	// Clean up our derived copy on every currently-connected cluster. The delete
	// is origin-guarded, so it only removes copies derived from THIS source. A
	// cluster that is disconnected at delete time can't be cleaned now; it is
	// reaped later by the GC sweep (the source UID is gone once the finalizer is
	// removed), so we don't block teardown on an unreachable cluster.
	//
	// We do, however, hold the finalizer if a CONNECTED cluster errored on
	// cleanup: dropping the finalizer then would orphan a still-reachable derived
	// (and the GC only catches it on its slower cadence). Requeue and retry.
	connected := r.Clusters.Connected()
	r.deleteLosers(ctx, isvc, connected, "")
	if remaining, err := r.derivedRemainingOnConnected(ctx, isvc, connected); err != nil {
		return ctrl.Result{}, err
	} else if remaining {
		// A connected cluster still holds our derived (transient delete error
		// tolerated by deleteLosers). Keep the finalizer; retry on the next poll.
		return ctrl.Result{RequeueAfter: r.requeue()}, nil
	}
	controllerutil.RemoveFinalizer(isvc, PlacementFinalizer)
	if err := r.Update(ctx, isvc); err != nil {
		return ctrl.Result{}, err
	}
	// Teardown complete: drop any winner-lost grace marker so the in-memory map
	// does not retain an entry for a now-deleted source ISVC.
	r.clearGrace(isvc.UID)
	r.forgetCapacity(client.ObjectKeyFromObject(isvc))
	// Likewise drop the AutoscalerPolicy preflight/skew bookkeeping for this
	// source (staged condition, home observations, prune counters).
	r.policyState().forget(isvc.UID)
	// Likewise the RolloutPolicy preflight bookkeeping (staged condition,
	// resolved plans, lifted run provenance).
	r.rolloutState().forget(isvc.UID)
	// Likewise drop any incremental-dispatcher round state for this source so its
	// in-memory map does not retain an entry for a now-deleted ISVC.
	if f, ok := r.nominate().(forgetRoundState); ok {
		f.forget(isvc.UID)
	}
	// Likewise the placement metrics, so a deleted ISVC stops reporting a phase
	// and a winning cluster.
	DeleteForISVC(isvc.Namespace, isvc.Name)
	return ctrl.Result{}, nil
}

// ownedTrafficMapRemaining reports whether a TrafficMap attributable to the
// source, or ambiguous durable publication state, still exists. It is the
// teardown barrier that lets the publisher drain routing state before placement
// removes serving workloads.
func (r *Reconciler) ownedTrafficMapRemaining(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
) (bool, error) {
	if r.APIReader == nil {
		return false, fmt.Errorf("check TrafficMap teardown barrier: API reader is not configured")
	}
	tm := &v1beta1.TrafficMap{}
	key := types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}
	if err := r.APIReader.Get(ctx, key, tm); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("check TrafficMap teardown barrier for %s: %w", key, err)
	}
	if isvc.UID == "" {
		return false, fmt.Errorf("check TrafficMap teardown barrier for %s: InferenceService UID is empty", key)
	}
	return trafficMapBlocksSourceTeardown(tm, isvc), nil
}

// derivedRemainingOnConnected reports whether any connected cluster still holds
// our derived copy of isvc. Used during teardown to decide whether the finalizer
// can be safely removed. A transient GET error is reported as an error (caller
// requeues) rather than silently treated as "gone".
func (r *Reconciler) derivedRemainingOnConnected(ctx context.Context, isvc *v1beta1.InferenceService, connected []string) (bool, error) {
	for _, c := range connected {
		derived, exists, err := r.getDerived(ctx, c, isvc)
		if err != nil {
			return false, err
		}
		if exists && isOurDerived(derived, isvc) {
			return true, nil
		}
	}
	return false, nil
}

// writePlacement sets status.placement (+ the mirrored status.url) for one pass,
// retrying on conflict since the local object may change during the
// cross-cluster fan-out/race. Returns the long safety requeue: steady-state
// status convergence is event-driven (the remote watch funnel re-reconciles
// this source when a derived's status changes), so this is only the backstop
// re-read for a missed event, not the former fixed status poll.
func (r *Reconciler) writePlacement(ctx context.Context, isvc *v1beta1.InferenceService, res placementResult) (ctrl.Result, error) {
	// For a policy-referencing source, attach the lifted per-home autoscaling
	// and rollout state to the candidates and collect the policy conditions to
	// write in the same status update. Both preflights stage the shared
	// PlacementPolicyPreflight type; the merge keeps the operative verdict.
	// Nil (result untouched) for the common no-ref ISVC.
	policyConds := mergePolicyConditions(
		r.policyStatusForWrite(isvc, &res),
		r.rolloutStatusForWrite(isvc, &res))
	policyConds = mergePolicyConditions(policyConds, res.conditions)
	if backend, _ := ctx.Value(backendContextKey{}).(*backendPreflight); backend != nil && backend.sourceUID == isvc.UID {
		policyConds = mergePolicyConditions(policyConds, []policyCondition{backend.condition})
	}
	if placementMode(isvc) == v1beta1.PlacementModeSplitByCapacity && !slices.ContainsFunc(policyConds, func(c policyCondition) bool { return c.condType == apis.ConditionType(v1beta1.PlacementCapacityFresh) }) {
		policyConds = append(policyConds, capacityFreshCondition("Unknown", "CapacityNotObserved", "Capacity inputs have not been verified in this reconcile"))
	}
	key := client.ObjectKeyFromObject(isvc)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := &v1beta1.InferenceService{}
		if err := r.APIReader.Get(ctx, key, cur); err != nil {
			return err
		}
		if !plan.SameSnapshot(isvc, cur) {
			return plan.ErrStaleSnapshot
		}
		if cur.Status.Placement != nil && cur.Status.Placement.Plan != nil && cur.Status.Placement.Plan.Mode == v1beta1.PlacementModeSingle && res.winner != cur.Status.Placement.Plan.Winner {
			return plan.ErrStaleSnapshot
		}
		readyBefore := cur.Status.GetCondition(apis.ConditionReady)
		if cur.Status.Placement == nil {
			cur.Status.Placement = &v1beta1.PlacementStatus{}
		}
		cur.Status.Placement.Cluster = res.winner
		cur.Status.Placement.Phase = res.phase
		cur.Status.Placement.Candidates = mergeCandidateObservations(cur.Status.Placement.Candidates, res.candidates)
		cur.Status.Placement.Endpoint = res.url
		cur.Status.URL = res.url
		applyPolicyConditions(&cur.Status, policyConds)
		if placementMode(isvc) != v1beta1.PlacementModeSplitByCapacity {
			cur.Status.Conditions = slices.DeleteFunc(cur.Status.Conditions, func(c apis.Condition) bool { return c.Type == apis.ConditionType(v1beta1.PlacementCapacityFresh) })
		}
		applyPolicyConditions(&cur.Status, []policyCondition{placementInputCondition(isvc)})
		applyPolicyConditions(&cur.Status, placementSatisfactionConditions(cur))
		setSourcePlacementReady(&cur.Status, res, readyBefore)
		// res was computed from the reconciled snapshot, not the live object read
		// for conflict-safe status persistence. A concurrent spec update must not
		// be reported as observed until its own reconcile computes placement.
		cur.Status.ObservedGeneration = isvc.Generation
		return r.Status().Update(ctx, cur)
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		if errors.Is(err, plan.ErrStaleSnapshot) {
			return ctrl.Result{RequeueAfter: r.requeue()}, nil
		}
		return ctrl.Result{}, err
	}
	// Publish only after the status write landed, so the gauges never advertise
	// a placement the API server rejected.
	recordPlacement(isvc, res)
	return ctrl.Result{RequeueAfter: r.safetyRequeue()}, nil
}

// setSourcePlacementReady reports whether the control-plane InferenceService is
// usable. Placement admission and serving readiness are separate: a Placed
// service is Ready only after an admitted home has both ready replicas and an
// addressable endpoint.
func setSourcePlacementReady(status *v1beta1.InferenceServiceStatus, res placementResult, previous *apis.Condition) {
	manager := sourcePlacementConditionSet.Manage(status)
	switch res.phase {
	case v1beta1.PlacementPhasePending:
		if res.placementLost {
			manager.MarkUnknown(apis.ConditionReady, PlacementReadyReasonLost, placementReadyMessageLost)
		} else if res.readinessUnknown {
			manager.MarkUnknown(apis.ConditionReady, PlacementReadyReasonUnknown, placementReadyMessageUnknown)
		} else {
			manager.MarkUnknown(apis.ConditionReady, placementReadyReasonPending, placementReadyMessagePending)
		}
	case v1beta1.PlacementPhaseAdmitting:
		manager.MarkUnknown(apis.ConditionReady, placementReadyReasonAdmitting, placementReadyMessageAdmitting)
	case v1beta1.PlacementPhasePlaced:
		if res.ready {
			manager.MarkTrueWithReason(apis.ConditionReady, placementReadyReasonReady, placementReadyMessageReady)
		} else if res.readinessUnknown {
			manager.MarkUnknown(apis.ConditionReady, PlacementReadyReasonUnknown, placementReadyMessageUnknown)
		} else {
			manager.MarkFalse(apis.ConditionReady, placementReadyReasonNotReady, placementReadyMessageNotReady)
		}
	case v1beta1.PlacementPhaseFailed:
		manager.MarkFalse(apis.ConditionReady, placementReadyReasonFailed, placementReadyMessageFailed)
	default:
		manager.MarkUnknown(apis.ConditionReady, PlacementReadyReasonUnknown, placementReadyMessageUnknown)
	}

	// Other condition writers can temporarily recompute Ready while adding their
	// own conditions. Preserve its transition timestamp when the final placement
	// verdict is unchanged across the status write.
	current := status.GetCondition(apis.ConditionReady)
	if !sameConditionState(previous, current) {
		return
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == apis.ConditionReady {
			status.Conditions[i].LastTransitionTime = previous.LastTransitionTime
			return
		}
	}
}

func candidateHasServingData(candidate v1beta1.CandidatePlacement) bool {
	return candidate.Phase == v1beta1.CandidatePhaseAdmitted &&
		candidate.ReadyReplicas > 0 && candidate.Endpoint != nil && candidate.Endpoint.Host != ""
}

func normalizeCandidatePhase(candidate v1beta1.CandidatePlacement) v1beta1.CandidatePlacement {
	if candidate.Phase == v1beta1.CandidatePhasePlaced { //nolint:staticcheck // Normalize legacy status during upgrades.
		candidate.Phase = v1beta1.CandidatePhaseAdmitting
	}
	return candidate
}

func placementCandidateServing(derived *v1beta1.InferenceService, candidate v1beta1.CandidatePlacement) bool {
	return derived != nil && derived.Status.IsConditionReady(v1beta1.IngressReady) &&
		candidateHasServingData(candidate)
}

func sameConditionState(a, b *apis.Condition) bool {
	return a != nil && b != nil &&
		a.Type == b.Type && a.Status == b.Status && a.Severity == b.Severity &&
		a.Reason == b.Reason && a.Message == b.Message
}

// endpointFor returns the winner's externally-addressable URL from its derived
// ISVC status, or nil until the worker reports one.
func endpointFor(derived *v1beta1.InferenceService) *apis.URL {
	if derived == nil {
		return nil
	}
	return derived.Status.URL
}

func winnerCluster(isvc *v1beta1.InferenceService) string {
	if isvc.Status.Placement == nil {
		return ""
	}
	if isvc.Status.Placement.Plan != nil && isvc.Status.Placement.Plan.Mode == v1beta1.PlacementModeSingle {
		return isvc.Status.Placement.Plan.Winner
	}
	return isvc.Status.Placement.Cluster
}

func (r *Reconciler) requeue() time.Duration {
	if r.Requeue > 0 {
		return r.Requeue
	}
	return DefaultPlacementRequeue
}

func (r *Reconciler) placeTimeout() time.Duration {
	if r.PlaceTimeout > 0 {
		return r.PlaceTimeout
	}
	return DefaultPlaceTimeout
}

func (r *Reconciler) winnerLostGrace() time.Duration {
	if r.WinnerLostGracePeriod > 0 {
		return r.WinnerLostGracePeriod
	}
	return DefaultWinnerLostGrace
}

// nominate returns the resolved fan-out breadth policy, building it once from the
// configured DispatcherMode (and, for Incremental, the step size / round timeout).
// An empty/unrecognized mode degrades to all-at-once. Lazily constructed so the
// Reconciler works as a bare struct literal without a dedicated constructor.
func (r *Reconciler) nominate() Dispatcher {
	r.dispatcherOnce.Do(func() {
		r.dispatcher = dispatcherFor(r.DispatcherMode, r.DispatcherStepSize, r.DispatcherRoundTimeout)
	})
	return r.dispatcher
}

// graceRemaining records (on first observation) and reports how long is left in
// the winner-lost grace window for this source ISVC. The clock starts when the
// winner is first confirmed absent or has lost admission; subsequent observations
// reuse that start. A non-positive return means the window has
// elapsed (caller should re-race). now is injected so tests can drive the clock
// without sleeping.
func (r *Reconciler) graceRemaining(uid types.UID, now time.Time) time.Duration {
	v, _ := r.winnerLostSince.LoadOrStore(uid, now)
	since := v.(time.Time)
	return r.winnerLostGrace() - now.Sub(since)
}

// clearGrace drops any winner-lost grace marker for this source ISVC. Called
// whenever the derived is observed present again, or once the winner is re-raced,
// so the next disappearance starts a fresh window rather than inheriting a stale
// start time.
func (r *Reconciler) clearGrace(uid types.UID) {
	r.winnerLostSince.Delete(uid)
}

// SetupWithManager wires the placer: reconcile ISVCs, re-enqueue the
// placement-eligible ISVCs whose candidate set is affected when a WorkloadCluster
// changes in a placement-relevant way (new/removed capacity, readiness flip, or
// label change), and — when a status watch-funnel channel is supplied via
// WithStatusEvents — re-enqueue a source ISVC when its derived's status changes
// on a workload cluster. The predicate suppresses the per-heartbeat status
// writes (the WorkloadCluster reconciler re-stamps Ready every health interval)
// so a routine heartbeat does NOT fan out to every ISVC.
//
// ConvergeOptions keep the funnel channel and the status timing knobs (batch
// debounce, long safety requeue) injectable so tests can wire a fake channel and
// shrink the windows; an unset option degrades to the package default.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts ...ConvergeOption) error {
	cfg := resolveConvergeConfig(opts...)
	r.converge = &cfg
	if r.policy == nil {
		r.policy = newPolicyPreflight(loadPolicyPreflightConfig(mgr, r.Log))
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor(PlacementControllerName)
	}

	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if err := r.validateWiring(); err != nil {
		return err
	}

	// Register the field index that marks ISVCs declaring placement requirements,
	// so a cluster change lists only those (not every cached ISVC) before the
	// per-ISVC candidate check in isvcsForClusterChange.
	if err := registerPlacementEligibleIndex(context.Background(), mgr.GetFieldIndexer()); err != nil {
		return err
	}
	b := ctrl.NewControllerManagedBy(mgr).
		Named(PlacementControllerName).
		For(&v1beta1.InferenceService{}, builder.WithPredicates(placementRelevantSourceChange)).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Watches(&v1beta1.WorkloadCluster{}, handler.EnqueueRequestsFromMapFunc(r.isvcsForClusterChange),
			builder.WithPredicates(placementRelevantClusterChange))
	if r.Capacity != nil {
		b = b.Watches(&v1beta1.AcceleratorQuota{}, handler.EnqueueRequestsFromMapFunc(r.isvcsForCapacityChange), builder.WithPredicates(capacityRelevantChange))
	}
	// Member event streams share one debounce handler, so a burst of status
	// changes for one source produces one reconcile.
	eventHandler := r.statusEventHandler(cfg.batchPeriod)
	for _, events := range cfg.statusEvents {
		b = b.WatchesRawSource(source.Channel(events, eventHandler))
	}
	return b.Complete(r)
}

// validateWiring rejects a mis-wired reconciler at setup. The
// authoritative (live) reader is a correctness dependency — see
// workload/types AuthoritativeReader.
func (r *Reconciler) validateWiring() error {
	if r.APIReader == nil {
		return fmt.Errorf("placement: APIReader (AuthoritativeReader) must be wired")
	}
	return nil
}

// statusEventHandler enqueues the funnel-resolved local key with the configured
// batch debounce. The event Object's namespace/name is the source ISVC key (the
// funnel resolved it from the remote derived), so no remote/label work happens
// here on the hot path.
func (r *Reconciler) statusEventHandler(batchPeriod time.Duration) handler.EventHandler {
	return handler.Funcs{
		GenericFunc: func(_ context.Context, e event.GenericEvent, q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			if e.Object == nil {
				return
			}
			req := ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: e.Object.GetNamespace(),
				Name:      e.Object.GetName(),
			}}
			if batchPeriod > 0 {
				q.AddAfter(req, batchPeriod)
			} else {
				q.Add(req)
			}
		},
	}
}

// placementRelevantClusterChange admits WorkloadCluster events that can change
// placement decisions: create/delete (capacity appears/disappears) and updates
// that flip the Ready condition status or change labels. It drops the routine
// status heartbeat (the health-probe re-stamp of an unchanged Ready condition),
// which would otherwise re-enqueue every control-plane ISVC on each probe.
var placementRelevantClusterChange = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldWC, ok1 := e.ObjectOld.(*v1beta1.WorkloadCluster)
		newWC, ok2 := e.ObjectNew.(*v1beta1.WorkloadCluster)
		if !ok1 || !ok2 {
			return true // unexpected type: fail safe by re-enqueuing
		}
		if clusterReady(oldWC) != clusterReady(newWC) {
			return true
		}
		return !maps.Equal(oldWC.Labels, newWC.Labels)
	},
}

// clusterReady reports whether the WorkloadCluster's Ready condition is True.
func clusterReady(wc *v1beta1.WorkloadCluster) bool {
	return apimeta.IsStatusConditionTrue(wc.Status.Conditions, v1beta1.WorkloadClusterReady)
}

// placementEligibleIndexField is the controller-runtime cache field-index name
// keyed on whether an ISVC declares ANY placement requirement (and is therefore
// eligible for the cross-cluster fan-out). Without it, a WorkloadCluster change
// lists EVERY cached InferenceService — including single-cluster ISVCs that carry
// no placement annotations and can never be a candidate — and re-enqueues them
// all; with it, MatchingFields narrows the list to the fan-out-eligible ISVCs in
// O(eligible), and isvcsForClusterChange then filters to those for which THIS
// cluster is (or was) a candidate.
//
// The field name is an internal index identifier (a code constant), not a
// behavioral/user-facing value — no config surface is required.
const placementEligibleIndexField = "ome.io/placement-eligible"

// placementEligibleIndexValue is the indexed value for an ISVC that declares a
// placement requirement. A constant truthy token; the index only ever maps the
// eligible ISVCs (the extractor returns nil for the rest, leaving them unindexed).
const placementEligibleIndexValue = "true"

// placementEligibleIndexExtractor is the cache IndexerFunc for
// placementEligibleIndexField. It returns the truthy token for an ISVC that
// declares an accelerator-requirements or cluster-selector annotation (the same
// signal MatchCandidates uses to decide fan-out eligibility), and nil otherwise
// so non-placement ISVCs stay out of the index.
func placementEligibleIndexExtractor(obj client.Object) []string {
	isvc, ok := obj.(*v1beta1.InferenceService)
	if !ok {
		return nil
	}
	if !IsPlacementEligible(isvc) {
		return nil
	}
	return []string{placementEligibleIndexValue}
}

// declaresPlacementRequirement preserves Legacy's no-selector/local boundary.
// Explicit new-policy fields remain eligible so invalid opt-in is observable.
func declaresPlacementRequirement(isvc *v1beta1.InferenceService) bool {
	if p := isvc.Spec.Placement; p != nil && (p.Policy != "" && p.Policy != v1beta1.PlacementPolicyLegacy || p.ClusterAffinity != nil || p.MaxSurge != nil || p.ReplacementTimeout != nil || p.Mode == v1beta1.PlacementModeSplitByCapacity) {
		return true
	}
	requirements, selector := placementInputs(isvc)
	return requirements != "" || selector != ""
}

// IsPlacementEligible reports whether an InferenceService participates in the
// multi-cluster placement flow. Consumers of status.placement use this same
// predicate so ordinary, single-cluster InferenceServices do not accidentally
// acquire multi-cluster artifacts.
func IsPlacementEligible(isvc *v1beta1.InferenceService) bool {
	if isvc == nil || isvc.Labels[PlacementOriginLabel] != "" || isvc.Annotations[PlacementOriginUIDAnnotation] != "" {
		return false
	}
	return declaresPlacementRequirement(isvc)
}

// registerPlacementEligibleIndex installs placementEligibleIndexField on the
// supplied indexer (mgr.GetFieldIndexer()). Call once during manager setup,
// before Start, so isvcsForClusterChange resolves fan-out-eligible ISVCs through
// the index instead of scanning every cached InferenceService.
func registerPlacementEligibleIndex(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &v1beta1.InferenceService{}, placementEligibleIndexField, placementEligibleIndexExtractor)
}

// isvcsForClusterChange maps a WorkloadCluster change to the reconcile requests
// for the placement-eligible ISVCs whose candidate set is affected by that
// cluster. It narrows to fan-out-eligible ISVCs via the field index, then keeps
// only those for which the changed cluster is (or just stopped being) a
// candidate:
//
//   - selector MATCHES the changed cluster's labels or metadata.name: the cluster
//     may now be a candidate (entering the set on a label add / readiness flip),
//     or remains one (a label change elsewhere on the cluster) — re-evaluate.
//   - status already REFERENCES the changed cluster (winner or candidate): the
//     change may be the cluster LEAVING the set (a label removed so the selector
//     no longer matches, or readiness flipped away). The event carries only the
//     post-change object, so a selector check alone would miss the departure; the
//     status reference catches it so the placement re-races off the lost cluster.
//
// An ISVC that is neither a current/possible candidate nor placed on the cluster
// is not enqueued — the change cannot affect its placement.
func (r *Reconciler) isvcsForClusterChange(ctx context.Context, obj client.Object) []ctrl.Request {
	wc, ok := obj.(*v1beta1.WorkloadCluster)
	if !ok {
		return nil
	}
	clusterName := wc.GetName()
	clusterSelectorSet := labels.Set(wc.Labels)

	list := &v1beta1.InferenceServiceList{}
	if err := r.List(ctx, list, client.MatchingFields{placementEligibleIndexField: placementEligibleIndexValue}); err != nil {
		r.Log.Error(err, "fan-out cluster event: list placement-eligible ISVCs failed", "cluster", clusterName)
		return nil
	}
	reqs := make([]ctrl.Request, 0, len(list.Items))
	for i := range list.Items {
		isvc := &list.Items[i]
		if !clusterAffectsISVC(isvc, clusterName, clusterSelectorSet) {
			continue
		}
		reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}})
	}
	return reqs
}

// clusterAffectsISVC reports whether a change to the named cluster can affect
// this ISVC's placement: either the ISVC's selector matches the cluster's
// selector set (it is/becomes a candidate), or the ISVC's status already
// references the cluster (it is/was placed there and must re-evaluate if the
// cluster is leaving the candidate set). A malformed selector is treated as
// "affects" so reconcile can surface the malformed-selector status.
func clusterAffectsISVC(isvc *v1beta1.InferenceService, clusterName string, clusterSelectorSet labels.Set) bool {
	if isvcStatusReferencesCluster(isvc, clusterName) {
		return true
	}
	selector, err := placementSelector(isvc)
	if err != nil {
		return true
	}
	_, matches := selector.Match(&v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Labels: clusterSelectorSet}})
	return matches
}

// isvcStatusReferencesCluster reports whether the ISVC's placement status names
// the cluster as its winner or among its fan-out candidates — i.e. the ISVC is
// currently placed on (or racing on) that cluster.
func isvcStatusReferencesCluster(isvc *v1beta1.InferenceService, clusterName string) bool {
	p := isvc.Status.Placement
	if p == nil {
		return false
	}
	if p.Cluster == clusterName {
		return true
	}
	for i := range p.Candidates {
		if p.Candidates[i].Cluster == clusterName {
			return true
		}
	}
	return false
}
