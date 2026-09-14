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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

const (
	// DefaultPlacementRequeue is the fallback status-refresh poll cadence used
	// when Reconciler.Requeue is unset. The operative value is supplied by the
	// manager flag / chart; this is the graceful-degradation default.
	DefaultPlacementRequeue = 30 * time.Second

	// DefaultPlaceTimeout is the fallback per-cluster fan-out apply deadline used
	// when Reconciler.PlaceTimeout is unset. The operative value is supplied by
	// the manager flag / chart; this is the graceful-degradation default that
	// keeps one wedged remote from stalling the apply to its healthy peers.
	DefaultPlaceTimeout = 10 * time.Second

	// DefaultWinnerLostGrace is the fallback grace window used when
	// Reconciler.WinnerLostGracePeriod is unset. The operative value is supplied
	// by the manager flag / chart; this is the graceful-degradation default that
	// lets a transient winner-derived gap heal before re-racing.
	DefaultWinnerLostGrace = 1 * time.Minute
)

// placementResult is the status the reconciler writes for one pass.
type placementResult struct {
	winner     string
	phase      v1beta1.PlacementPhase
	candidates []v1beta1.CandidatePlacement
	url        *apis.URL // published endpoint; mirrored to BOTH status.placement.endpoint and status.url
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
	// PlaceTimeout bounds a single per-cluster placeOn during fan-out so one
	// stuck/slow remote cannot block the apply to the healthy candidates. The
	// operative value is supplied by the manager flag / chart; zero (unset) falls
	// back to DefaultPlaceTimeout (graceful degradation, no magic literal in the
	// hot path).
	PlaceTimeout time.Duration
	// WinnerLostGracePeriod is how long the controller waits, after the sticky
	// winner's derived first appears ABSENT on a connected winner cluster, before
	// giving up on it and re-racing. It rides out a transient gap (the worker
	// recreating the derived, a brief apiserver hiccup) so a momentary blip does
	// not tear down a healthy placement and re-fan-out the whole candidate set.
	// A genuinely-deleted derived re-races once the window elapses. Config-driven
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
	// first use). It is the nomination step fanOut applies to. dispatcherOnce
	// guards lazy construction so the Reconciler stays usable when assembled as a
	// bare struct literal (the cmd wiring and the tests both do that) without a
	// constructor.
	dispatcher     Dispatcher
	dispatcherOnce sync.Once

	// winnerLostSince tracks, per source-ISVC UID, the first time this controller
	// observed the sticky winner's derived ABSENT on a connected winner cluster.
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

	if controllerutil.AddFinalizer(isvc, PlacementFinalizer) {
		if err := r.Update(ctx, isvc); err != nil {
			return ctrl.Result{}, err
		}
	}

	clusters := &v1beta1.WorkloadClusterList{}
	if err := r.List(ctx, clusters); err != nil {
		return ctrl.Result{}, err
	}
	candidates, reason, err := MatchCandidates(isvc, clusters.Items)
	if err != nil || len(candidates) == 0 {
		// Surface WHY there are no candidates (malformed selector / no
		// requirements declared / no Ready clusters / no match) so the empty
		// set is diagnosable instead of an indistinguishable Pending.
		r.Log.Info("no placement candidates", "isvc", isvc.Namespace+"/"+isvc.Name,
			"reason", reason, "error", err)
		if err == nil && reason == MatchReasonNoReadyClusters && isvc.Status.Placement != nil &&
			placementTargetExists(isvc.Status.Placement, clusters.Items) {
			// Fleet readiness is sampled and can disappear for one health interval.
			// Preserve the last-known placement (including its endpoint and homes)
			// rather than publishing Pending and derouting still-serving traffic.
			// Retention keys on the target still EXISTING, not on it being ready,
			// so a decommissioned fleet falls through to Pending instead of
			// pinning the InferenceService to a cluster that is gone.
			return ctrl.Result{RequeueAfter: r.requeue()}, nil
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
		// The standing winner failed preflight, or a Placed source's anchor
		// read failed transiently: keep the last-written placement untouched
		// (no Pending write, no re-race) and re-check at the poll cadence,
		// mirroring the winner-disconnected handling.
		return ctrl.Result{RequeueAfter: r.requeue()}, nil
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
			return ctrl.Result{RequeueAfter: r.requeue()}, nil
		}
		if rf != nil && !rf.hold {
			candidates = rf.eligible
		}
	}

	var res ctrl.Result
	if (pf != nil && pf.hold) || (rf != nil && rf.hold) {
		res, err = r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	} else {
		// Branch on placement mode. Each mode owns its own reconcile: Single keeps one
		// winner (sticky race + loser sweep); All keeps every home that admits. Split
		// is not yet implemented — hold Pending rather than silently run it as Single,
		// so an operator never mistakes an un-split placement for a split one.
		switch mode := placementMode(isvc); mode {
		case v1beta1.PlacementModeSingle:
			res, err = r.reconcileSingle(ctx, isvc, candidates)
		case v1beta1.PlacementModeAll:
			res, err = r.reconcileAll(ctx, isvc, candidates)
		case v1beta1.PlacementModeSplit:
			res, err = r.reconcileSplit(ctx, isvc, candidates)
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

// reconcileSingle implements PlacementModeSingle: fan out to the candidates, race
// on Kueue admission, keep the first fully-admitted cluster (lexical tie-break),
// sweep the losers, and hold that winner sticky across polls — re-racing only if
// it is lost (derived deleted, or admission lost past the grace window). This is
// the default mode.
func (r *Reconciler) reconcileSingle(ctx context.Context, isvc *v1beta1.InferenceService, candidates []string) (ctrl.Result, error) {
	// Sticky winner: keep the current winner while its derived ISVC still
	// exists and it remains a candidate. Otherwise fall through to re-race.
	if winner := winnerCluster(isvc); winner != "" && slices.Contains(candidates, winner) {
		state, derived, err := r.getWinnerDerived(ctx, winner, isvc)
		if err != nil {
			// A transient remote GET error must NOT abort the reconcile or be
			// misread as "derived gone" (which would prematurely re-race and
			// sweep losers). Hold the existing placement and retry next poll.
			r.Log.Error(err, "sticky winner: remote get failed; holding placement", "cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name)
			return ctrl.Result{RequeueAfter: r.requeue()}, nil
		}
		switch state {
		case winnerDisconnected:
			// The winner cluster is not reachable this pass. This is NOT evidence
			// the derived was deleted — re-racing now would tear down a healthy
			// placement on a transport flap. Hold the placement and retry; the
			// grace clock is deliberately NOT armed (it tracks absence on a
			// CONNECTED winner, not a disconnected transport).
			r.Log.Info("sticky winner: cluster disconnected; holding placement", "cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name)
			return ctrl.Result{RequeueAfter: r.requeue()}, nil

		case winnerDerivedPresent:
			cl, ok := r.Clusters.ClientFor(winner)
			if !ok {
				// Winner disconnected between the derived GET and now; hold
				// rather than judge its state on partial data (consistent with
				// the winnerDisconnected handling above).
				r.Log.Info("sticky winner: cluster client unavailable; holding placement", "cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name)
				return ctrl.Result{RequeueAfter: r.requeue()}, nil
			}
			// Read the authoritative per-component IR status from the winner
			// cluster (source of truth; the derived ISVC no longer mirrors it).
			statuses, err := componentIRStatuses(ctx, cl, derived)
			if err != nil {
				// A transient IR read error must not be misread as terminal
				// failure; hold and retry next poll.
				r.Log.Error(err, "sticky winner: reading IR statuses failed; holding placement", "cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name)
				return ctrl.Result{RequeueAfter: r.requeue()}, nil
			}
			if IsTerminallyFailed(derived, statuses) {
				// The placement itself failed; surface it and stop. Do NOT
				// re-place into a hot fail-loop — needs human/spec action.
				u := endpointFor(derived)
				if u == nil {
					u = isvc.Status.URL
				}
				return r.writePlacement(ctx, isvc, placementResult{
					winner: winner, phase: v1beta1.PlacementPhaseFailed,
					candidates: []v1beta1.CandidatePlacement{{Cluster: winner, Phase: v1beta1.CandidatePhaseAdmitted}},
					url:        u,
				})
			}
			// The winner must still hold its Kueue admission. If it lost it (e.g.
			// preemption evicted every instance of a component), the placement is no
			// longer live even though the derived object exists. Ride out a transient
			// dip (rollout, brief re-gate) with the same grace window a vanished
			// derived uses, then re-race off the healthy candidates. Without this a
			// preempted winner is reported Placed forever while serving nothing.
			if !AllComponentsAdmitted(derived, statuses) {
				if remaining := r.graceRemaining(isvc.UID, time.Now()); remaining > 0 {
					r.Log.Info("sticky winner: lost admission; within grace window, holding placement",
						"cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name, "graceRemaining", remaining)
					return ctrl.Result{RequeueAfter: min(remaining, r.requeue())}, nil
				}
				r.clearGrace(isvc.UID)
				r.Log.Info("sticky winner: lost admission past grace window; re-racing",
					"cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name)
				break // exit the switch; fall through to the re-race path below
			}
			// Admitted and present: healthy. Drop any pending grace marker, re-apply
			// on the winner, and sweep any stray derived on the other candidates
			// (origin-guarded, so a same-named user ISVC is safe).
			r.clearGrace(isvc.UID)
			if err := r.placeOnBounded(ctx, winner, cl, isvc); err != nil {
				return ctrl.Result{}, err
			}
			r.deleteLosers(ctx, isvc, candidates, winner)
			return r.writePlacement(ctx, isvc, placedResult(winner, derived, isvc))

		case winnerDerivedAbsent:
			// The winner is connected but its derived is gone. Distinguish a
			// transient gap (worker recreating it, brief apiserver blip) from a
			// genuine deletion by holding the placement until the grace window
			// elapses; only then re-race. Without this, a momentary disappearance
			// would re-fan-out the whole candidate set and churn the placement.
			if remaining := r.graceRemaining(isvc.UID, time.Now()); remaining > 0 {
				r.Log.Info("sticky winner: derived absent; within grace window, holding placement",
					"cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name, "graceRemaining", remaining)
				// Requeue when the grace window is due to expire (or at the poll
				// cadence, whichever is sooner) so re-placement is not deferred a
				// full poll past the deadline.
				return ctrl.Result{RequeueAfter: min(remaining, r.requeue())}, nil
			}
			// Grace elapsed: the winner is genuinely lost. Clear the marker and
			// fall through to re-race.
			r.clearGrace(isvc.UID)
			r.Log.Info("sticky winner: derived absent past grace window; re-racing", "cluster", winner, "isvc", isvc.Namespace+"/"+isvc.Name)
		}
	}

	// Nominate the subset of candidates to clone onto this pass. AllAtOnce
	// nominates the whole set (unchanged fleet-wide fan-out); Incremental probes
	// in batches and may ask us to hold between rounds. fanOut then acts on the
	// nominated set rather than directly on all candidates.
	nominated, hold := r.nominate().Nominate(isvc.UID, candidates, time.Now())
	if len(nominated) == 0 {
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}

	// Fan out to the connected nominated candidates.
	placed, err := r.fanOut(ctx, isvc, nominated)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(placed) == 0 {
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}

	// Race: the first placed candidate whose derived ISVC reports an admitted
	// instance wins.
	winner, err := r.findWinner(ctx, isvc, placed)
	if err != nil {
		return ctrl.Result{}, err
	}
	if winner == "" {
		cands := make([]v1beta1.CandidatePlacement, 0, len(placed))
		for _, c := range placed {
			cands = append(cands, v1beta1.CandidatePlacement{Cluster: c, Phase: v1beta1.CandidatePhasePlaced})
		}
		res, err := r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhaseRacing, candidates: cands})
		if err == nil {
			// Racing is an active wait for Kueue admission, so re-poll at the fast
			// cadence to observe the winner promptly. writePlacement otherwise
			// returns the long steady-state backstop, which only suffices when the
			// status funnel event-drives re-reconciles; without the funnel that
			// would leave the race unresolved until the backstop fires.
			res.RequeueAfter = r.requeue()
			// An incremental round in flight re-evaluates when it is due to elapse
			// (or the poll cadence, whichever is sooner) so the next batch is
			// nominated without waiting a full poll past the round deadline.
			if hold > 0 {
				res.RequeueAfter = min(hold, res.RequeueAfter)
			}
		}
		return res, err
	}

	// Winner: sweep the losers (every candidate cluster except the winner;
	// each delete is origin-guarded so only our derived copies are removed).
	r.deleteLosers(ctx, isvc, candidates, winner)
	wd, _, err := r.getDerived(ctx, winner, isvc)
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.writePlacement(ctx, isvc, placedResult(winner, wd, isvc))
}

// placementMode returns the ISVC's placement cardinality, defaulting to Single
// when spec.placement is unset or its mode is empty (the legacy/annotation path).
func placementMode(isvc *v1beta1.InferenceService) v1beta1.PlacementMode {
	if p := isvc.Spec.Placement; p != nil && p.Mode != "" {
		return p.Mode
	}
	return v1beta1.PlacementModeSingle
}

// reconcileAll implements PlacementModeAll: run the ISVC on EVERY candidate that
// admits and keep them all. There is no single winner and no loser sweep — a
// candidate that has not admitted yet is retained and keeps trying (capacity may
// free up), and a home that later loses admission simply drops out of the served
// set while the others continue. All is best-effort: the placement is Placed
// once at least one home is admitted, and stays Racing only while every home is
// still gated; it never fails just because some candidate cannot admit.
//
// There is deliberately no sticky-winner / re-race path here: those protect the
// single-home invariant, which All does not have. Each home is independent.
// placementTargetExists reports whether the placement still names at least one
// WorkloadCluster that is present in the fleet, in any readiness state.
func placementTargetExists(pl *v1beta1.PlacementStatus, clusters []v1beta1.WorkloadCluster) bool {
	present := make(map[string]struct{}, len(clusters))
	for i := range clusters {
		present[clusters[i].Name] = struct{}{}
	}
	if pl.Cluster != "" {
		if _, ok := present[pl.Cluster]; ok {
			return true
		}
	}
	for _, c := range pl.Candidates {
		if _, ok := present[c.Cluster]; ok {
			return true
		}
	}
	return false
}

func (r *Reconciler) reconcileAll(ctx context.Context, isvc *v1beta1.InferenceService, candidates []string) (ctrl.Result, error) {
	// A candidate that cannot be observed this pass keeps its last-published
	// state. The WorkloadCluster API can remain Ready while a newly elected
	// process is still rebuilding its local remote-client registry.
	prev := make(map[string]v1beta1.CandidatePlacement, len(candidates))
	if isvc.Status.Placement != nil {
		for _, c := range isvc.Status.Placement.Candidates {
			prev[c.Cluster] = c
		}
	}

	placed, err := r.fanOut(ctx, isvc, candidates)
	if err != nil {
		return ctrl.Result{}, err
	}
	placedNow := make(map[string]bool, len(placed))
	for _, c := range placed {
		placedNow[c] = true
	}

	cands := make([]v1beta1.CandidatePlacement, 0, len(candidates))
	admitted := 0
	carryForward := func(c string) {
		if p, ok := prev[c]; ok {
			cands = append(cands, p)
			if p.Phase == v1beta1.CandidatePhaseAdmitted {
				admitted++
			}
		}
	}
	unobserved := false
	for _, c := range candidates {
		if !placedNow[c] {
			unobserved = true
			if _, had := prev[c]; had {
				r.Log.Info("all: candidate unavailable; keeping last-known home state", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			}
			carryForward(c)
			continue
		}
		derived, ok, err := r.getDerived(ctx, c, isvc)
		if err != nil {
			r.Log.Error(err, "all: reading derived failed; keeping last-known home state", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			unobserved = true
			carryForward(c)
			continue
		}
		if !ok {
			unobserved = true
			carryForward(c)
			continue
		}
		cl, ok := r.Clusters.ClientFor(c)
		if !ok {
			unobserved = true
			carryForward(c)
			continue
		}
		statuses, err := componentIRStatuses(ctx, cl, derived)
		if err != nil {
			r.Log.Error(err, "all: reading IR statuses failed; keeping last-known home state", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			unobserved = true
			carryForward(c)
			continue
		}
		if AllComponentsAdmitted(derived, statuses) {
			admitted++
			cands = append(cands, v1beta1.CandidatePlacement{
				Cluster: c, Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: endpointFor(derived),
			})
			continue
		}
		cands = append(cands, v1beta1.CandidatePlacement{Cluster: c, Phase: v1beta1.CandidatePhasePlaced})
	}
	if len(placed) == 0 && len(cands) == 0 {
		res, err := r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
		if err == nil {
			res.RequeueAfter = r.requeue()
		}
		return res, err
	}

	phase := v1beta1.PlacementPhaseRacing
	if admitted > 0 {
		phase = v1beta1.PlacementPhasePlaced
	}
	// No top-level winner cluster/URL in All: the per-home endpoints in
	// candidates[] are the source of truth (Cluster/Endpoint stay empty).
	res, err := r.writePlacement(ctx, isvc, placementResult{phase: phase, candidates: cands})
	// Re-poll at the normal cadence while ANY home is still gated. All admits
	// homes independently and at different times, so a home that admits AFTER the
	// first must be observed without waiting for the long steady-state backstop —
	// even once the placement is Placed on the earlier home(s). Once every home is
	// admitted, writePlacement's backstop requeue stands.
	if err == nil && (admitted < len(cands) || unobserved) {
		res.RequeueAfter = r.requeue()
	}
	return res, err
}

// reconcileSplit implements PlacementModeSplit: distribute the desired replica
// count (spec.placement.split.replicas, else the engine floor minReplicas)
// across candidate clusters. It requests a target on each, lets that cluster's
// Kueue admit as many whole-gang replicas as fit, keeps every home that admits
// >=1, and sums admitted until the floor is met. Packed by default — fill the
// fewest clusters in preference order, spilling the remainder to the next;
// Balanced (spec.placement.split.spread) apportions an even share across all
// candidates. The endpoint weight follows each home's ready replicas.
//
// Distribution is a single ordered pass with no over-admission trim and no
// deficit re-apportionment beyond that pass — it converges to >= floor
// admitted, Packed, and accepts transient overshoot. Like All, there is no
// sticky winner or re-race: each home is independent.
func (r *Reconciler) reconcileSplit(ctx context.Context, isvc *v1beta1.InferenceService, candidates []string) (ctrl.Result, error) {
	desired := splitDesiredReplicas(isvc)
	if desired <= 0 {
		// No floor declared — nothing to distribute.
		return r.writePlacement(ctx, isvc, placementResult{phase: v1beta1.PlacementPhasePending})
	}
	// Split mode implies spec.placement is set; the split sub-block may be nil
	// (defaults: distribute the floor, Packed, uncapped, no anti-sliver floor).
	var maxPer, minPer int32
	spread := false
	if sp := isvc.Spec.Placement.Split; sp != nil {
		maxPer, minPer, spread = sp.MaxReplicasPerCluster, sp.MinReplicasPerCluster, sp.Spread
	}
	scaleComps := splitScaleComponents(isvc)

	// Phase 1 — observe each candidate's current derived: admitted + ready replica
	// counts and the home endpoint. Observing BEFORE (re)apportioning is what lets
	// the loop both fill a deficit and trim over-admission in the same pass. A
	// disconnected candidate is not observable this pass; a sub-minPer sliver is
	// counted as zero (dropped below).
	type homeObs struct {
		admitted, ready int32
		endpoint        *apis.URL
		present, sliver bool
		// unreadable marks a home whose state could not be read this pass, as
		// distinct from one observed to hold nothing. Apportionment credits it
		// with its last published count so a transient read error cannot look
		// like freed capacity and provision a duplicate elsewhere, and the
		// apply phase leaves it alone.
		unreadable bool
	}
	// Last published per-home counts, used to keep an unreadable home's share
	// stable across the pass.
	lastAdmitted := make(map[string]int32, len(candidates))
	lastCand := make(map[string]v1beta1.CandidatePlacement, len(candidates))
	if isvc.Status.Placement != nil {
		for _, c := range isvc.Status.Placement.Candidates {
			lastAdmitted[c.Cluster] = c.AdmittedReplicas
			lastCand[c.Cluster] = c
		}
	}
	obs := make(map[string]*homeObs, len(candidates))
	admitted := make(map[string]int32, len(candidates))
	markUnreadable := func(c string, o *homeObs) {
		o.unreadable = true
		o.present = false
		admitted[c] = lastAdmitted[c]
	}
	for _, c := range candidates {
		o := &homeObs{}
		obs[c] = o
		cl, ok := r.Clusters.ClientFor(c)
		if !ok {
			markUnreadable(c, o)
			continue // disconnected; retry next poll
		}
		derived, ok, err := r.getDerived(ctx, c, isvc)
		if err != nil {
			r.Log.Error(err, "split: reading derived failed; keeping last-known home state", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			markUnreadable(c, o)
			continue
		}
		if !ok {
			continue // no derived yet
		}
		o.present = true
		statuses, err := componentIRStatuses(ctx, cl, derived)
		if err != nil {
			r.Log.Error(err, "split: reading IR statuses failed; keeping last-known home state", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			markUnreadable(c, o)
			continue
		}
		o.admitted = splitAdmittedReplicas(scaleComps, statuses)
		o.ready = splitReadyReplicas(scaleComps, statuses)
		o.endpoint = endpointFor(derived)
		admitted[c] = o.admitted
		if minPer > 0 && o.admitted > 0 && o.admitted < minPer {
			// Sub-floor sliver: do not count it toward the floor; it is swept below.
			o.sliver = true
			admitted[c] = 0
		}
	}

	// Phase 2 — apportion the desired count into a per-cluster target request.
	targets := splitApportion(candidates, admitted, desired, maxPer, spread)

	// Phase 3 — apply: request the target on kept clusters, sweep the rest, and
	// record per-home status from the observed counts.
	var admittedTotal int32
	cands := make([]v1beta1.CandidatePlacement, 0, len(candidates))
	for _, c := range candidates {
		o := obs[c]
		if o.unreadable {
			if p, had := lastCand[c]; had {
				cands = append(cands, p)
				if p.Phase == v1beta1.CandidatePhaseAdmitted {
					counted := p.AdmittedReplicas
					if t := targets[c]; counted > t {
						counted = t
					}
					admittedTotal += counted
				}
			}
			continue
		}
		if o.sliver || targets[c] <= 0 {
			// Sliver, or not needed (Packed floor met / trimmed away): sweep any derived.
			if o.present {
				if err := r.deleteDerivedOn(ctx, c, isvc); err != nil {
					r.Log.Error(err, "split: sweep cluster failed", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
				}
			}
			continue
		}
		cl, ok := r.Clusters.ClientFor(c)
		if !ok {
			continue // disconnected; retry next poll
		}
		if err := r.placeOnReplicasBounded(ctx, c, cl, isvc, targets[c], maxPer); err != nil {
			r.Log.Error(err, "split: place failed on cluster", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			continue // per-cluster tolerated, like fanOut
		}
		if o.admitted == 0 {
			// Gated so far: keep trying; record a placed (not-yet-admitted) candidate.
			cands = append(cands, v1beta1.CandidatePlacement{Cluster: c, Phase: v1beta1.CandidatePhasePlaced})
			continue
		}
		// Count admitted toward the floor, capped by the target — a trimmed home is
		// on its way down to target, so do not credit the excess it still shows.
		counted := o.admitted
		if counted > targets[c] {
			counted = targets[c]
		}
		admittedTotal += counted
		cands = append(cands, v1beta1.CandidatePlacement{
			Cluster:          c,
			Phase:            v1beta1.CandidatePhaseAdmitted,
			Endpoint:         o.endpoint,
			AdmittedReplicas: o.admitted,
			ReadyReplicas:    o.ready,
		})
	}

	phase := v1beta1.PlacementPhaseRacing
	if admittedTotal > 0 {
		phase = v1beta1.PlacementPhasePlaced
	}
	res, err := r.writePlacement(ctx, isvc, placementResult{phase: phase, candidates: cands})
	if err == nil && admittedTotal < desired {
		// Floor not yet met (homes still gated / capacity filling in): re-poll at
		// the normal cadence rather than the long steady-state backstop.
		res.RequeueAfter = r.requeue()
	}
	return res, err
}

// splitApportion computes each candidate's target replica REQUEST for Split.
// admitted[c] is the replicas currently admitted on c (0 if none). A target of 0
// means "do not use this cluster" (sweep). Preference order is the candidates
// slice order. Two Packed regimes plus Balanced:
//
//   - TRIM (sum admitted >= desired): pin each cluster to min(admitted,
//     remaining) in preference order — keep the preferred clusters full and shed
//     the excess from the least-preferred, so requests sum to exactly desired and
//     no gated remainder can push the admitted total any higher. This is what
//     converges transient over-admission back down.
//   - FILL (sum admitted < desired): over-request the remaining deficit on each
//     cluster in preference order (Kueue admits what fits); already-admitted
//     replicas close the deficit. Over-admission from gated remainders admitting
//     later is cleaned up by TRIM on the next pass.
//   - Balanced (spread): request an even ceil(desired/n) share on every candidate.
func splitApportion(candidates []string, admitted map[string]int32, desired, maxPer int32, spread bool) map[string]int32 {
	targets := make(map[string]int32, len(candidates))
	capTo := func(v int32) int32 {
		if maxPer > 0 && v > maxPer {
			return maxPer
		}
		return v
	}
	if spread {
		share := capTo(ceilDiv(desired, int32(len(candidates))))
		for _, c := range candidates {
			targets[c] = share
		}
		return targets
	}
	var total int32
	for _, c := range candidates {
		total += admitted[c]
	}
	remaining := desired
	if total >= desired {
		for _, c := range candidates { // TRIM
			t := capTo(min32(admitted[c], remaining))
			targets[c] = t
			remaining -= t
		}
		return targets
	}
	for _, c := range candidates { // FILL
		if remaining <= 0 {
			targets[c] = 0
			continue
		}
		targets[c] = capTo(remaining)
		remaining -= admitted[c]
	}
	return targets
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// splitDesiredReplicas is the Split desired replica count: spec.placement.split.
// replicas when set, else the engine component's minReplicas (the guaranteed
// floor). Zero when neither is declared.
func splitDesiredReplicas(isvc *v1beta1.InferenceService) int32 {
	if p := isvc.Spec.Placement; p != nil && p.Split != nil && p.Split.Replicas != nil {
		return *p.Split.Replicas
	}
	if isvc.Spec.Engine != nil && isvc.Spec.Engine.MinReplicas != nil {
		return int32(*isvc.Spec.Engine.MinReplicas)
	}
	return 0
}

// splitScaleComponents are the replica-scaled components whose admitted/ready
// counts define a Split home's replica count: Engine and, for a PD service, the
// Decoder — a replica is the coordinated pair. Router is excluded (a shared
// front-end, not per-replica). Falls back to the first declared component when
// neither Engine nor Decoder is present (unusual for Split).
func splitScaleComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
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

// splitAdmittedReplicas is a home's admitted replica count: the MIN admitted
// instances across the scaled components, since a PD replica is admitted only
// when BOTH its engine and decoder instances are (an engine-only home is just
// the engine count). Zero when no scaled component is declared.
func splitAdmittedReplicas(comps []v1beta1.ComponentType, statuses map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus) int32 {
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

// splitReadyReplicas is a home's ready replica count (the endpoint weight): the
// MIN ReadyReplicas across the scaled components, for the same pairing reason.
func splitReadyReplicas(comps []v1beta1.ComponentType, statuses map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus) int32 {
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

// ceilDiv is integer ceiling division; b <= 0 returns a (no split).
func ceilDiv(a, b int32) int32 {
	if b <= 0 {
		return a
	}
	return (a + b - 1) / b
}

// fanOut ensures the derived ISVC exists on each connected candidate. A
// per-cluster failure does not abort the others — one bad cluster must not deny
// the race to the healthy ones. Returns the clusters actually placed on; returns
// an error only when connected candidates existed but ALL failed.
//
// Fault isolation (scale hardening): the connected-cluster set is snapshotted
// ONCE before the loop so membership does not shift mid-fan-out, and each placeOn
// runs under a per-cluster deadline (placeTimeout) so a single wedged/slow remote
// — one whose apiserver hangs rather than returning an error — cannot stall the
// apply to its healthy peers. A cluster that is not connected at snapshot time is
// silently skipped and retried next poll; a connected cluster whose apply errors
// or deadlines out is recorded as a per-cluster error (logged, tolerated) and
// likewise retried. Neither is allowed to block the race for the healthy ones.
func (r *Reconciler) fanOut(ctx context.Context, isvc *v1beta1.InferenceService, candidates []string) ([]string, error) {
	// Snapshot connectivity up front: a stable view for this whole fan-out, so a
	// cluster connecting/disconnecting mid-loop can't make membership inconsistent
	// between the skip check here and the per-cluster apply below.
	connected := connectedSet(r.Clusters)

	var placed []string
	var errs []error
	for _, c := range candidates {
		if !connected[c] {
			continue // not connected this pass; the next poll re-attempts it
		}
		cl, ok := r.Clusters.ClientFor(c)
		if !ok {
			continue // disconnected between snapshot and lookup; skip
		}
		if err := r.placeOnBounded(ctx, c, cl, isvc); err != nil {
			r.Log.Error(err, "fan out failed on cluster", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			errs = append(errs, fmt.Errorf("%q: %w", c, err))
			continue
		}
		placed = append(placed, c)
	}
	if len(placed) == 0 && len(errs) > 0 {
		return nil, fmt.Errorf("fan out %s/%s failed on all connected candidates: %w", isvc.Namespace, isvc.Name, errors.Join(errs...))
	}
	return placed, nil
}

// placeOnBounded runs placeOn under a per-cluster deadline so one stuck remote
// cannot block the fan-out to the healthy candidates. A deadline overrun surfaces
// as a (wrapped) context.DeadlineExceeded error the caller records per-cluster
// and retries next poll.
func (r *Reconciler) placeOnBounded(ctx context.Context, cluster string, cl client.Client, isvc *v1beta1.InferenceService) error {
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	defer cancel()
	return r.placeOn(cctx, cluster, cl, isvc)
}

// placeOnReplicasBounded runs placeOnReplicas under the per-cluster deadline
// (Split's per-cluster apportioned apply).
func (r *Reconciler) placeOnReplicasBounded(ctx context.Context, cluster string, cl client.Client, isvc *v1beta1.InferenceService, replicas, maxPer int32) error {
	cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
	defer cancel()
	return r.placeOnReplicas(cctx, cluster, cl, isvc, replicas, maxPer)
}

// connectedSet snapshots the currently-connected cluster names as a set for O(1)
// membership checks during a single fan-out pass.
func connectedSet(clusters ClusterClients) map[string]bool {
	names := clusters.Connected()
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return set
}

// findWinner returns the winning cluster: the first placed candidate, in sorted
// candidate order, whose derived ISVC reports EVERY declared component admitted,
// or "" if none is fully admitted yet. Requiring all components (not just one
// instance) prevents a PD service from being declared placed while only the
// engine has cleared Kueue and the decoder is still gated. When several clusters
// fully admit near-simultaneously the lowest-named candidate wins — a
// deterministic tie-break, not literally "first in wall-clock time". A candidate
// whose derived or IR status cannot be read this pass is skipped, so one
// unreachable cluster cannot deny the race to its healthy peers.
func (r *Reconciler) findWinner(ctx context.Context, isvc *v1beta1.InferenceService, placed []string) (string, error) {
	for _, c := range placed {
		derived, ok, err := r.getDerived(ctx, c, isvc)
		if err != nil {
			r.Log.Error(err, "race: reading derived failed; skipping candidate", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			continue
		}
		if !ok {
			continue
		}
		cl, ok := r.Clusters.ClientFor(c)
		if !ok {
			continue
		}
		// Read the authoritative per-component IR status from candidate cluster
		// c (source of truth; the derived ISVC no longer mirrors it).
		statuses, err := componentIRStatuses(ctx, cl, derived)
		if err != nil {
			r.Log.Error(err, "race: reading IR statuses failed; skipping candidate", "cluster", c, "isvc", isvc.Namespace+"/"+isvc.Name)
			continue
		}
		if AllComponentsAdmitted(derived, statuses) {
			return c, nil
		}
	}
	return "", nil
}

// deleteLosers best-effort deletes THIS ISVC's derived copy on each cluster in
// clusters except keep. deleteDerivedOn only ever deletes an object that carries
// this source ISVC's origin label, so a same-named ISVC a user created directly
// on a workload cluster is never touched (cross-tenant data-loss guard).
//
// Per-cluster failures are tolerated (like fanOut): one slow/unreachable cluster
// must not block loser cleanup on the healthy ones, and must NOT block recording
// the winner. Failures are logged; the next poll retries, and the GC sweep is a
// backstop. Callers that must confirm full teardown (reconcileDelete) verify the
// derived is actually gone separately rather than relying on the return here.
func (r *Reconciler) deleteLosers(ctx context.Context, isvc *v1beta1.InferenceService, clusters []string, keep string) {
	for _, c := range clusters {
		if c == keep {
			continue
		}
		if err := r.deleteDerivedOn(ctx, c, isvc); err != nil {
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

// winnerDerivedState is the outcome of inspecting the sticky winner's derived
// ISVC, with the disconnected case kept DISTINCT from the genuinely-absent case
// (which getDerived collapses into ok=false). The winner-lost grace logic must
// treat a disconnected winner as transient (hold, no grace clock) and only run
// the grace clock when the derived is absent on a CONNECTED winner.
type winnerDerivedState int

const (
	// winnerDisconnected: the winner cluster has no live client this pass
	// (transport flap / not yet (re)connected). Transient — not evidence the
	// derived was deleted.
	winnerDisconnected winnerDerivedState = iota
	// winnerDerivedPresent: the winner is connected and its derived exists.
	winnerDerivedPresent
	// winnerDerivedAbsent: the winner is connected but its derived is NotFound.
	// This is the only state that arms the winner-lost grace clock.
	winnerDerivedAbsent
)

// getWinnerDerived inspects the sticky winner's derived ISVC, distinguishing a
// disconnected winner from a connected-but-absent derived so the caller can
// apply the grace window only to a genuine disappearance. A transient GET error
// is returned as an error (caller holds placement and requeues) rather than
// misread as absence.
func (r *Reconciler) getWinnerDerived(ctx context.Context, cluster string, isvc *v1beta1.InferenceService) (winnerDerivedState, *v1beta1.InferenceService, error) {
	cl, ok := r.Clusters.ClientFor(cluster)
	if !ok {
		return winnerDisconnected, nil, nil
	}
	derived := &v1beta1.InferenceService{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}, derived); err != nil {
		if apierrors.IsNotFound(err) {
			return winnerDerivedAbsent, nil, nil
		}
		return winnerDisconnected, nil, err
	}
	r.observeDerivedPolicyStatus(isvc, cluster, derived)
	r.observeDerivedRolloutStatus(isvc, cluster, derived)
	return winnerDerivedPresent, derived, nil
}

// placeOn creates-or-updates the derived ISVC on the target cluster. The control
// plane owns the derived Spec and the metadata keys IT stamps (origin markers,
// Kueue queue/serving gating); it does NOT own the rest of the object's labels
// and annotations. The worker-cluster reconciler adds its own metadata (status
// bookkeeping annotations, generated labels) to the derived object, so this
// merges the control-plane-owned keys into the existing maps rather than
// replacing them wholesale — preserving worker-side reconciler state across the
// poll-driven re-apply.
func (r *Reconciler) placeOn(ctx context.Context, cluster string, cl client.Client, src *v1beta1.InferenceService) error {
	d, err := r.derivedFor(src)
	if err != nil {
		return err
	}
	return r.applyDerived(ctx, cluster, cl, src, d)
}

// placeOnReplicas is placeOn with a Split per-cluster apportioned replica band
// pinned onto the derived's scalable components before the apply: replicas is
// this home's share (the floor), maxPer the per-cluster ceiling (0 = uncapped).
func (r *Reconciler) placeOnReplicas(ctx context.Context, cluster string, cl client.Client, src *v1beta1.InferenceService, replicas, maxPer int32) error {
	d, err := r.derivedFor(src)
	if err != nil {
		return err
	}
	setDerivedReplicas(d, replicas, maxPer)
	return r.applyDerived(ctx, cluster, cl, src, d)
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
func (r *Reconciler) applyDerived(ctx context.Context, cluster string, cl client.Client, src, desired *v1beta1.InferenceService) error {
	target := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, cl, target, func() error {
		// Origin-guard the apply, mirroring the origin guard on the delete path
		// (deleteDerivedOn): a target that already exists (CreateOrUpdate's Get
		// populated it, so ResourceVersion is set) and is NOT a derived of THIS
		// source must not be overwritten. Without this, a same-named ISVC a user
		// created directly on the workload cluster — or another control plane's
		// object — would have its Spec replaced and origin markers stamped onto
		// it (cross-tenant data loss). Returning an error makes CreateOrUpdate a
		// no-op write and surfaces per-cluster like any other fan-out failure. A
		// first-time create (empty ResourceVersion) and a re-apply of our own
		// derived (isOurDerived true) both proceed.
		if target.ResourceVersion != "" && !isOurDerived(target, src) {
			return fmt.Errorf("refusing to overwrite non-derived InferenceService %s/%s on candidate cluster: not a placement derived of control plane %q",
				target.Namespace, target.Name, r.ControlPlaneID)
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
	key := client.ObjectKeyFromObject(isvc)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := &v1beta1.InferenceService{}
		if err := r.APIReader.Get(ctx, key, cur); err != nil {
			return err
		}
		if cur.Status.Placement == nil {
			cur.Status.Placement = &v1beta1.PlacementStatus{}
		}
		cur.Status.Placement.Cluster = res.winner
		cur.Status.Placement.Phase = res.phase
		cur.Status.Placement.Candidates = res.candidates
		cur.Status.Placement.Endpoint = res.url
		cur.Status.URL = res.url
		applyPolicyConditions(&cur.Status, policyConds)
		return r.Status().Update(ctx, cur)
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	// Publish only after the status write landed, so the gauges never advertise
	// a placement the API server rejected.
	recordPlacement(isvc, res)
	return ctrl.Result{RequeueAfter: r.safetyRequeue()}, nil
}

// endpointFor returns the winner's externally-addressable URL from its derived
// ISVC status, or nil until the worker reports one.
func endpointFor(derived *v1beta1.InferenceService) *apis.URL {
	if derived == nil {
		return nil
	}
	return derived.Status.URL
}

// placedResult builds the Placed status for winner. The endpoint is sticky: if
// the winner's derived has no URL this pass (transient worker-status gap), the
// last-published URL (from the in-memory isvc) is kept rather than cleared, so
// an external LB watching status.url does not see a spurious deroute.
func placedResult(winner string, derived, isvc *v1beta1.InferenceService) placementResult {
	u := endpointFor(derived)
	if u == nil {
		u = isvc.Status.URL // keep last-known endpoint
	}
	return placementResult{
		winner: winner, phase: v1beta1.PlacementPhasePlaced,
		candidates: []v1beta1.CandidatePlacement{{Cluster: winner, Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: u}},
		url:        u,
	}
}

func winnerCluster(isvc *v1beta1.InferenceService) string {
	if isvc.Status.Placement == nil {
		return ""
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

// graceRemaining records (on first observation) and reports how long is LEFT in
// the winner-lost grace window for this source ISVC. The clock starts the first
// time the winner's derived is seen absent on a connected winner; subsequent
// observations reuse that start. A non-positive return means the window has
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
		For(&v1beta1.InferenceService{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Watches(&v1beta1.WorkloadCluster{}, handler.EnqueueRequestsFromMapFunc(r.isvcsForClusterChange),
			builder.WithPredicates(placementRelevantClusterChange))
	// Consume the remote watch-funnel channel: each event already carries the
	// LOCAL source key (resolved funnel-side), so the handler just enqueues that
	// key, debounced by the batch period so a burst of derived-status updates for
	// one ISVC folds into a single reconcile.
	if cfg.statusEvents != nil {
		b = b.WatchesRawSource(source.Channel(cfg.statusEvents, r.statusEventHandler(cfg.batchPeriod)))
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
	if !declaresPlacementRequirement(isvc) {
		return nil
	}
	return []string{placementEligibleIndexValue}
}

// declaresPlacementRequirement reports whether the ISVC is eligible for
// cross-cluster fan-out — it declares a requirement or cluster selector via
// spec.placement or the legacy annotations. Uses placementInputs so it stays in
// lockstep with requirementSelector (the source of truth for what counts as a
// requirement); a divergence would index ISVCs the matcher ignores, or vice versa.
func declaresPlacementRequirement(isvc *v1beta1.InferenceService) bool {
	requirements, clusterSelector := placementInputs(isvc)
	return requirements != "" || clusterSelector != ""
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
//   - selector MATCHES the changed cluster's labels: the cluster may now be a
//     candidate (entering the set on a label add / readiness flip), or remains one
//     (a label change elsewhere on the cluster) — re-evaluate.
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
	clusterLabels := labels.Set(wc.GetLabels())

	list := &v1beta1.InferenceServiceList{}
	if err := r.List(ctx, list, client.MatchingFields{placementEligibleIndexField: placementEligibleIndexValue}); err != nil {
		r.Log.Error(err, "fan-out cluster event: list placement-eligible ISVCs failed", "cluster", clusterName)
		return nil
	}
	reqs := make([]ctrl.Request, 0, len(list.Items))
	for i := range list.Items {
		isvc := &list.Items[i]
		if !clusterAffectsISVC(isvc, clusterName, clusterLabels) {
			continue
		}
		reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}})
	}
	return reqs
}

// clusterAffectsISVC reports whether a change to the named cluster (with the
// given post-change labels) can affect this ISVC's placement: either the ISVC's
// requirement selector matches the cluster's labels (it is/becomes a candidate),
// or the ISVC's status already references the cluster (it is/was placed there and
// must re-evaluate if the cluster is leaving the candidate set). A malformed
// selector is treated as "affects" (fail safe: re-enqueue so the reconcile can
// surface the malformed-selector status).
func clusterAffectsISVC(isvc *v1beta1.InferenceService, clusterName string, clusterLabels labels.Set) bool {
	if isvcStatusReferencesCluster(isvc, clusterName) {
		return true
	}
	sel, hasReq, err := requirementSelector(isvc)
	if err != nil {
		return true // malformed selector: fail safe by re-enqueuing
	}
	if !hasReq {
		return false // not fanned out fleet-wide; the cluster cannot be a candidate
	}
	return sel.Matches(clusterLabels)
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
