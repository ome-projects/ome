package canary

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// ReconcileInputs is the subset of state the canary executor needs. It mirrors
// coordination.ReconcileInputs. DesiredReplicas and ReadyCanaryInstances are
// Instance counts; PerRevisionPods supplies ready serving Pod counts and the
// capacity fallback when the exact Instance count is unavailable.
type ReconcileInputs struct {
	Client client.Client
	// Reader is the live API reader for one-off reads of types the manager does
	// not watch (the analysis auth Secret): reading those through the cached
	// Client would spin up a cluster-wide informer for the type.
	Reader client.Reader
	// Recorder publishes the operator-verb outcomes an operator needs to tell
	// an applied verb from an inapplicable one. nil (tests) silences them.
	Recorder record.EventRecorder
	ISVC     *v1beta1.InferenceService
	// Group is the canary group being dispatched — the unit's own ladder. Nil
	// resolves it from Component.
	Group              *v1beta1.RolloutGroup
	Component          v1beta1.ComponentType
	CanaryRevisionHash string
	// StableRevisionHash is the stable revision for the canary's component:
	// the persisted status identity when one exists, otherwise the observed
	// non-canary revision (which seeds the persisted identity at canary start).
	StableRevisionHash string
	// CanaryPairingProtocol / StablePairingProtocol are the P/D pairing
	// tokens the two revisions were minted under, published on the traffic
	// targets so routing consumers pair engine/decoder targets by equal
	// values. Empty pairs with anything.
	CanaryPairingProtocol string
	StablePairingProtocol string
	// ReadyCanaryInstances is the count of complete PodReady target Instances,
	// resolved from the IR's runner topology. nil uses PerRevisionPods.
	ReadyCanaryInstances *int32
	DesiredReplicas      int32
	PerRevisionPods      map[string]int32
	// GroupTotalPerRevisionPods counts every pod of every configured member
	// of the canary group per revision hash, ready or not, keyed by
	// Component (primary included). A revert is complete when the rejected
	// pods are gone, not merely unready, so the revert-complete check reads
	// it. nil (single-Component callers) checks the primary's ready pods alone.
	GroupTotalPerRevisionPods map[v1beta1.ComponentType]map[string]int32
	// GroupStableRevisionHashes is each member's persisted stable revision,
	// keyed by Component; the primary's is the identity the executor resolves
	// itself. A member with none recorded is not waited on.
	GroupStableRevisionHashes map[v1beta1.ComponentType]string
	// SecondaryCapacityReady is true when every NON-primary component's canary
	// Instances have reached that component's step newCount. The primary uses its
	// exact complete-PodReady target Instance count. Single-component canaries
	// pass true.
	SecondaryCapacityReady bool
	// Secondaries carries each NON-primary member's revision pair and pairing
	// protocols, keyed by Component. Every member publishes the ladder's
	// weights on its own revisions wherever the primary's traffic is written;
	// a member absent here has its traffic left alone. nil (single-Component
	// callers) writes the primary's alone.
	Secondaries map[v1beta1.ComponentType]MemberRevisions
	Now         time.Time
	// Sampler reads analysis metric results without blocking the reconcile: a miss
	// kicks a bounded background query and an event re-reconciles when it lands.
	// The controller wires *Sampler; tests inject a fake. Only used by analysis steps.
	Sampler stepSampler
	// Prometheus is the canary-level metrics source (GroupCanary.Prometheus) shared
	// by all analysis steps. Its ServerAddress overrides BundledPrometheusAddress;
	// nil falls back to it.
	Prometheus *v1beta1.AnalysisPrometheus
	// BundledPrometheusAddress is the operator-configured default source
	// (controllerconfig canaryAnalysis), used when Prometheus is nil or sets no
	// ServerAddress. Empty means no default source — samples read inconclusive.
	BundledPrometheusAddress string
	// QueryTimeout bounds one background sampling pass (controllerconfig canaryAnalysis).
	QueryTimeout time.Duration
	// RunActive is whether a rollout run is pinned for this ISVC. Without one
	// the executor is maintenance-only: it services the terminal holds (done
	// sentinel, rolled-back, failed) but never initializes a new canary or
	// re-arms toward a new target — opening a run is the run layer's job, and
	// starting a canary outside a pinned run would execute an unpinned plan.
	RunActive bool
	// TargetID identifies the canary group's pinned Component target set. Unlike
	// the primary revision hash, it changes when only a secondary Component
	// changes, but not when an unrelated rollout group changes.
	TargetID string
	// DefaultReadyTimeout is the operator-configured capacity-gate bound used
	// when neither the ready-timeout annotation nor the plan's readyTimeout
	// sets one. Zero means no configured default: the gate never escalates to
	// Failed on capacity wait alone.
	DefaultReadyTimeout time.Duration
	// Requeue is the operator-configured cadence at which an in-progress step
	// is re-checked (capacity coming up, a timed pause, a drain window);
	// ParkedRequeue is the heartbeat of a Failed park. Zero means
	// unconfigured: the executor asks for the controller's rate-limited
	// requeue instead of a fixed cadence.
	Requeue       time.Duration
	ParkedRequeue time.Duration
}

// MemberRevisions is one secondary's revision pair: the target its capacity
// gate verified and its persisted stable revision, with the pairing protocol
// each was minted under. A member that was not bumped names the same revision
// twice; a member with no stable revision leaves it empty.
type MemberRevisions struct {
	CanaryRevisionHash    string
	StableRevisionHash    string
	CanaryPairingProtocol string
	StablePairingProtocol string
}

// The ready Pod count bounds the exact ready Instance count and remains the
// fallback for direct callers that do not provide topology-aware observation.
func readyCapacityCount(readyPods int32, readyInstances *int32) int32 {
	if readyInstances != nil && *readyInstances < readyPods {
		return *readyInstances
	}
	return readyPods
}

// unitRetargeted reports whether ANY Component in this canary unit has a
// target distinct from its stable revision in the active run.
//
// It is per-UNIT, not per-Component, because a unit's revision identity is its
// primary's: a [router, engine] group whose router is unchanged but whose
// engine retargeted still has work to do, and suppressing it there would skip
// a real rollout (see TestDispatch_SecondaryOnlyTargetStartsCanary).
//
// Conservative on missing information: with no active run, no group, or no
// recorded targets there is nothing to prove the unit idle, so it reports
// true and the caller's other guards decide.
func unitRetargeted(in ReconcileInputs) bool {
	return groupRetargeted(in.ISVC, in.Group, in.Component)
}

// groupRetargeted is unitRetargeted over an explicit group; fallback stands in
// for the group's members when the group is unknown.
func groupRetargeted(isvc *v1beta1.InferenceService, group *v1beta1.RolloutGroup, fallback v1beta1.ComponentType) bool {
	if isvc == nil || isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return true
	}
	targets := isvc.Status.Rollout.ActiveRun.TargetRevisions
	if len(targets) == 0 {
		return true
	}
	members := map[v1beta1.ComponentType]bool{}
	if group != nil {
		for _, c := range group.Components {
			members[c] = true
		}
	}
	if len(members) == 0 {
		members[fallback] = true
	}
	seen := false
	for i := range targets {
		t := &targets[i]
		if !members[t.Component] {
			continue
		}
		seen = true
		if t.Revision != t.StableRevision {
			return true
		}
	}
	// Every member recorded revision == stableRevision: this unit is idle.
	// If the run recorded none of them, fall back to arming.
	return !seen
}

func readyCanaryCapacity(in ReconcileInputs) int32 {
	return readyCapacityCount(in.PerRevisionPods[in.CanaryRevisionHash], in.ReadyCanaryInstances)
}

// stepSampler is the analysis-sampling seam the executor calls. Production wires
// the async *Sampler; tests inject a fake returning canned results so the step
// logic stays hermetic. Get is non-blocking: see Sampler.Get.
type stepSampler interface {
	Get(req SampleRequest, since time.Time) (analysis.Result, time.Time, bool)
}

// Result reports the executor's decision for one reconcile. Active=false means
// no canary is in progress. Partition is the StatefulSet-style partition the
// controller projects onto the canary component's spec.pacing.partition
// (instances < Partition are held on the stable revision). RequeueAfter > 0
// asks the controller to re-reconcile (capacity / pause / drain pending).
type Result struct {
	Active   bool
	Complete bool
	// Stepped is true on the reconcile that advanced to the next (intermediate)
	// step — the edge the step counter + step Event are recorded on.
	Stepped bool
	// RolledBack is true while the canary is rolling back / held rolled-back; the
	// controller reads it (and status.canary.RolledBackRevisionHash) to point the
	// IR at the stable ControllerRevision.
	RolledBack   bool
	Partition    int32
	RequeueAfter time.Duration
	// Requeue asks for the controller's rate-limited requeue when no cadence
	// is configured for the wait the executor is in.
	Requeue bool
	// Consume lists the operator annotations this pass applied. The controller
	// removes them after the status write that carries their effect has
	// landed; each applied verb leaves a record in status (PromotedThrough)
	// that keeps the still-visible annotation inert until then.
	Consume []string
}

// wake sets the result's requeue from a configured cadence, falling back to
// the controller's rate-limited requeue when none is configured.
func (r *Result) wake(d time.Duration) *Result {
	if d > 0 {
		r.RequeueAfter = d
	} else {
		r.Requeue = true
	}
	return r
}

// Reconcile advances the canary toward the declared plan, mutating isvc.Status
// in-memory. No-op (Active=false) when spec.rollout.canary is unset. Per step it
// performs capacity → traffic → pause, advancing on promotion; the final step
// (TrafficWeight 100) drains and scales the stable revision down.
func Reconcile(ctx context.Context, in ReconcileInputs) (*Result, error) {
	// Operator annotations are applied in status first and removed by the
	// controller after the flush; the pass only records which ones it took,
	// and deletes them in-memory so nothing later in the pass re-reads them.
	var consumed []string
	take := func(key string) {
		delete(in.ISVC.Annotations, key)
		consumed = append(consumed, key)
	}
	res, err := reconcile(ctx, in, take)
	if res != nil {
		res.Consume = consumed
	}
	return res, err
}

func reconcile(ctx context.Context, in ReconcileInputs, take func(string)) (*Result, error) {
	// The legacy single-run field is a projection of per-unit state, so it is
	// re-derived on every exit path rather than written alongside each mutation
	// — a copy published once would freeze while the run advanced past it.
	defer rollout.SyncLegacyCanaryAlias(&in.ISVC.Status)

	g := in.Group
	if g == nil {
		g = rollout.CanaryGroupFor(in.ISVC, in.Component)
	}
	if g == nil || g.Canary == nil || len(g.Canary.Steps) == 0 {
		return &Result{Active: false}, nil
	}
	plan := g.Canary

	cs := rollout.CanaryStatusFor(&in.ISVC.Status, in.Component)
	// Status is an unvalidated subresource: clamp a negative step (an external
	// write) before it can index plan.Steps.
	if cs != nil && cs.CurrentStep < 0 {
		cs.CurrentStep = 0
	}

	// Global pause (ome.io/rollout-paused): the operator held the rollout.
	// Observe only — no step advance, no traffic or phase write, no annotation
	// consumption, no rollback arm/clear, and no state-machine (re)initialization.
	// Step timers are left untouched, so clearing the pause resumes the current
	// step with its clocks intact. Both pause depths hold the canary equally;
	// the depth only changes whether Instance repair keeps running underneath.
	if paused, _ := constants.RolloutPauseState(in.ISVC.Annotations); paused {
		return pausedResult(in, cs, plan), nil
	}
	targetChanged := cs != nil && in.RunActive && in.TargetID != "" && cs.TargetID != "" && cs.TargetID != in.TargetID
	if cs != nil && cs.TargetID == "" && in.TargetID != "" {
		// Adopt status written before canary state was bound to a rollout run.
		cs.TargetID = in.TargetID
	}
	// When the primary changes, stable and canary identities must remain
	// distinct. A secondary-only run intentionally uses the same primary hash
	// for both identities and is distinguished by TargetID. The controller supplies
	// the IR's current revision when it can repair an invalid persisted pair.
	if cs != nil && cs.CanaryRevisionHash != "" && cs.StableRevisionHash == cs.CanaryRevisionHash &&
		in.StableRevisionHash != "" && in.StableRevisionHash != cs.CanaryRevisionHash {
		cs.StableRevisionHash = in.StableRevisionHash
	}

	// Resume (ome.io/rollout-resume): the operator cleared a terminal hold.
	// It runs ahead of the holds themselves — those re-arm only toward a NEW
	// target, which is exactly the escape this verb exists to provide without
	// one — and returns rather than falling through, so the step machine only
	// sees the re-armed status once the observation it reads has caught up.
	if resumed, err := handleResume(ctx, in, cs, take); err != nil {
		return nil, err
	} else if resumed {
		return (&Result{Active: true}).wake(in.Requeue), nil
	}

	// Verbs and holds, in the order the canary grid gives them precedence.
	// A pass that ends inside one of them returns here; otherwise cs is the
	// (possibly freshly armed) status the step machine runs against.
	var res *Result
	var err error
	if cs, res, err = applyHolds(ctx, in, cs, plan, targetChanged, take); err != nil || res != nil {
		return res, err
	}

	// Backfill a missing stable identity from the observed stable revision (a
	// canary re-armed after a completed one, or a status recorded before the
	// identity was persisted). Once set it is never re-inferred: the live pod
	// set stops naming the pre-canary stable as revisions retarget and drain.
	if cs.StableRevisionHash == "" && in.StableRevisionHash != "" {
		cs.StableRevisionHash = in.StableRevisionHash
	}

	// Finish any promotion whose advance already persisted: hand the applied
	// promote annotation back for removal and, once it is gone, clear the
	// durable record so manual promotion re-arms for later steps.
	syncPromotedThrough(in, cs, take)
	step := plan.Steps[cs.CurrentStep]

	newCount := resolveStepNewCount(step, in.DesiredReplicas)
	partition := partitionForNewCount(in.DesiredReplicas, newCount)

	// Capacity gate: don't shift traffic until the canary pods are Ready — the
	// primary's own newCount AND every secondary component's canary capacity (PD,
	// so a Ready router can't advance the step while the engine/decoder canary
	// pods behind it are still coming up). The wait is timed from when it
	// began, never from the step's soak anchor, so a long bake cannot spend the
	// budget and a dip cannot restart the soak; past the ready timeout the
	// canary parks Failed with the stable revision still serving.
	readyCanary := readyCanaryCapacity(in)
	state := rollout.StateOf(cs, in.ISVC.Status.Components[in.Component].RolloutPhase, len(plan.Steps))
	if (readyCanary < newCount || !in.SecondaryCapacityReady) && state != rollout.CanaryStateDraining {
		// A drain is exempt: the final traffic write has landed, and capacity
		// lost after cutover is the workload engine's repair, not a canary
		// state.
		if cs.CapacityWaitSince == nil {
			cs.CapacityWaitSince = &metav1.Time{Time: in.Now}
		}
		if capacityGateExpired(cs, resolveReadyTimeout(in, plan), in.Now) {
			parkFailed(in.ISVC, in.Component, cs, v1beta1.CanaryFailureCapacityTimeout, in.Now)
			return (&Result{Active: true, Partition: partition}).wake(in.ParkedRequeue), nil
		}
		if state == rollout.CanaryStateServing || state == rollout.CanaryStatePreHold {
			// A split that was serving keeps its phase and its programmed
			// weight through a dip; the gate only keeps the step from moving.
			return (&Result{Active: true, Partition: partition}).wake(in.Requeue), nil
		}
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePending)
		return (&Result{Active: true, Partition: partition}).wake(in.Requeue), nil
	}
	cs.CapacityWaitSince = nil

	// Pre-step hold (repin clamp): the pinned plan was replaced mid-run and
	// the clamped step would raise exposure, so the traffic raise waits for
	// an explicit promote (value = canary hash). Capacity above already
	// converged toward the clamped step — capacity ahead of traffic is the
	// supported warm-up pattern; exposure is the traffic write below.
	if cs.PreStepHold {
		if in.ISVC.Annotations[constants.RolloutPromoteAnnotation] == cs.CanaryRevisionHash {
			// The release is recorded like any applied promote: the record
			// keeps the annotation inert until the controller removes it after
			// the flush, so a lost flush cannot leave the hold released and
			// the verb gone.
			cs.PreStepHold = false
			cs.PromotedThrough = cs.CanaryRevisionHash
			take(constants.RolloutPromoteAnnotation)
		} else {
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePaused)
			return (&Result{Active: true, Partition: partition}).wake(in.Requeue), nil
		}
	}

	// Capacity satisfied: program this step's external traffic weight on
	// every member of the unit.
	weightBefore := cs.ObservedTrafficWeight
	applyGroupTraffic(in, step.Traffic)

	// Final step (TrafficWeight 100): drain the stable revision, then complete.
	if int(cs.CurrentStep) == len(plan.Steps)-1 {
		// Anchor the drain window and the final gate to the moment 100%
		// traffic actually shifts, not to step entry: on slow capacity the
		// final step is entered well before traffic moves, so measuring from
		// step entry could consume the whole window before cutover. The pass
		// whose traffic write moved the weight to 100 is that moment.
		if weightBefore != 100 {
			cs.StepEnteredTime = &metav1.Time{Time: in.Now}
		}
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePromoting)
		// The final 100% step honors the same gate as intermediate steps: analysis
		// validates at full traffic (a breach within the drain window still rolls
		// back; an inconclusive stall parks Failed), a bare Pause holds completion
		// for an explicit promote, and a timed Pause holds for its duration —
		// anchored, like the drain window, to the moment 100% traffic shifts. An
		// ungated step proceeds straight to the drain.
		if stepGated(step) {
			switch evaluateStep(ctx, in, cs, step) {
			case decRollback:
				cs.RolledBackRevisionHash = cs.CanaryRevisionHash
				return reconcileRollback(in, cs), nil
			case decFailed:
				parkFailed(in.ISVC, in.Component, cs, v1beta1.CanaryFailureAnalysisStalled, in.Now)
				return (&Result{Active: true, Partition: 0}).wake(in.ParkedRequeue), nil
			case decHold:
				// A held gate reads Paused on every step: the phase names the
				// wait, and Promoting is the drain that follows the gate.
				setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePaused)
				return (&Result{Active: true, Partition: 0}).wake(stepWake(in, step)), nil
			case decAdvance:
				// gate passed at 100% — fall through to the drain window + completion.
			}
		}
		if drainElapsed(cs, plan, in.Now) {
			// A promote that opens the final gate stays live through the drain
			// window (the gate re-evaluates every pass until completion). At the
			// completion edge it is recorded and handed back for removal after
			// the flush; the done sentinel's sync clears the record once the
			// annotation is observed gone.
			if v, ok := in.ISVC.Annotations[constants.RolloutPromoteAnnotation]; ok {
				cs.PromotedThrough = v
				take(constants.RolloutPromoteAnnotation)
			}
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
			// Mark done (sentinel), don't clear: keeps EffectivePartition at
			// partition 0 so the old revision drains instead of being re-held by
			// a step-0 partition. ObservedTrafficWeight stays 100 (all on canary).
			cs.CurrentStep = int32(len(plan.Steps))
			cs.ObservedTrafficWeight = 100
			// The canary revision is the stable revision now: drop the pre-canary
			// identity so it cannot leak into a later rollout's rollback target.
			cs.StableRevisionHash = ""
			return &Result{Active: true, Complete: true, Partition: 0}, nil
		}
		return (&Result{Active: true, Partition: 0}).wake(in.Requeue), nil
	}

	// Intermediate step: serving a split. Anchor the pause to the moment the split
	// FIRST serves — re-stamp StepEnteredTime on entering the active phase (the gate
	// just passed; the prior phase was Pending). On slow capacity the step is entered
	// well before traffic moves, so measuring the pause from step entry would consume
	// the soak before the split is even up (model-load can far exceed the pause in
	// prod). Mirrors the final step's Promoting anchor above. Re-stamp once: skip when
	// already Canarying/Paused.
	if cur := in.ISVC.Status.Components[in.Component].RolloutPhase; cur != v1beta1.RolloutPhaseCanarying && cur != v1beta1.RolloutPhasePaused {
		cs.StepEnteredTime = &metav1.Time{Time: in.Now}
	}
	setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseCanarying)
	if stepGated(step) {
		switch evaluateStep(ctx, in, cs, step) {
		case decRollback:
			cs.RolledBackRevisionHash = cs.CanaryRevisionHash
			return reconcileRollback(in, cs), nil
		case decFailed:
			parkFailed(in.ISVC, in.Component, cs, v1beta1.CanaryFailureAnalysisStalled, in.Now)
			return (&Result{Active: true, Partition: partition}).wake(in.ParkedRequeue), nil
		case decHold:
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePaused)
			return (&Result{Active: true, Partition: partition}).wake(stepWake(in, step)), nil
		case decAdvance:
			// fall through to advance.
		}
	}
	advanceStep(in, take)
	return (&Result{Active: true, Stepped: true, Partition: partition}).wake(in.Requeue), nil
}

// pausedResult reports a globally-paused canary's state without mutating it.
// The rollback signal echoes persisted status so the controller neither arms
// nor clears the IR's RollbackToRevision while paused, and the partition
// echoes the current step so the staged split stays where it is.
func pausedResult(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary) *Result {
	// Not started, or already done: inactive. A pause must not initialize the
	// state machine (initialization stamps the step timers) — a canary toward a
	// target that appeared while paused starts once the pause clears.
	if cs == nil || int(cs.CurrentStep) >= len(plan.Steps) {
		return &Result{Active: false}
	}
	if cs.RolledBackRevisionHash != "" {
		return (&Result{Active: true, RolledBack: true}).wake(in.Requeue)
	}
	step := plan.Steps[cs.CurrentStep]
	partition := partitionForNewCount(in.DesiredReplicas, resolveStepNewCount(step, in.DesiredReplicas))
	return (&Result{Active: true, Partition: partition}).wake(in.Requeue)
}

// resetCanaryStatus re-arms the state machine at step 0 toward a new target,
// dropping ALL prior-canary state (analysis failure budget, sampling
// timestamps, metric results, rollback hold, observed traffic) — carrying any
// of it over would let a stale failure budget roll back the new revision.
// StableRevisionHash is the one field PRESERVED: a mid-canary retarget does
// not change which revision was stable when the rollout began, and dropping
// it would make a later rollback target the partially-rolled intermediate.
//
// Re-projecting the RolloutPhase is part of the re-arm. The phase lives on
// the Component status, not in cs: left behind, a terminal phase would keep
// a hold keyed on it from ever re-arming, and a serving phase would make the
// re-armed step read as a split that dipped rather than one staging from
// scratch. Pending is the honest phase at step 0, and the pass's own gate
// overwrites it as soon as it decides.
func resetCanaryStatus(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, cs *v1beta1.CanaryStatus, targetID, hash string, now time.Time) {
	*cs = v1beta1.CanaryStatus{
		TargetID:           targetID,
		CanaryRevisionHash: hash,
		StableRevisionHash: cs.StableRevisionHash,
		StepEnteredTime:    &metav1.Time{Time: now},
	}
	setPhase(isvc, c, v1beta1.RolloutPhasePending)
}

// stableHashFor resolves the stable revision hash for rollback decisions: the
// persisted status identity when present (it survives retargets and the
// stable pods' drain), otherwise inferred from the live pod set — the
// fallback for canary statuses recorded before the identity was persisted.
func stableHashFor(cs *v1beta1.CanaryStatus, pods map[string]int32, rejectedHash string) string {
	if cs != nil && cs.StableRevisionHash != "" {
		return cs.StableRevisionHash
	}
	return otherRevision(pods, rejectedHash)
}

// reconcileRollback drives the component back to the stable revision and holds
// there, rejecting cs.RolledBackRevisionHash. Traffic goes 100% to stable; the
// controller reads cs.RolledBackRevisionHash and makes the IR roll every Instance
// back to the stable ControllerRevision, so the rejected-revision pods drain.
// While they drain → RollingBack; once gone → RolledBack (held until a different
// target appears).
func reconcileRollback(in ReconcileInputs, cs *v1beta1.CanaryStatus) *Result {
	stableHash := stableHashFor(cs, in.PerRevisionPods, cs.RolledBackRevisionHash)
	// The rejected entry is written at 0% (dropped), so only the stable
	// target's protocol matters. It is known only when the resolved stable
	// hash matches the one the dispatcher resolved a protocol for; the
	// live-pod fallback degrades to "" (pairs with anything).
	stableProtocol := ""
	if stableHash == in.StableRevisionHash {
		stableProtocol = in.StablePairingProtocol
	}
	applyTraffic(in.ISVC, in.Component, cs.RolledBackRevisionHash, stableHash, "", stableProtocol, 0)
	applyGroupStableTraffic(in)
	// RolledBack is the revert-COMPLETE phase, so it must wait out every
	// non-stable revision — not just the rejected one. A retargeted canary
	// (A -> partial B -> retarget C -> rollback) leaves stragglers on the
	// intermediate revision B; declaring RolledBack while they serve would
	// end the run under them, and the plan gate then holds their reverts
	// with no run to open (the rejected target is excluded from run-open
	// divergence by design). With no stable identity to compare against,
	// degrade to the rejected-revision check rather than never completing.
	rollingBack := cs.RolledBackRevisionHash != stableHash && in.PerRevisionPods[cs.RolledBackRevisionHash] > 0
	if stableHash != "" {
		for h, n := range in.PerRevisionPods {
			if h != "" && h != stableHash && n > 0 {
				rollingBack = true
				break
			}
		}
	}
	// The unit reverts as a whole, and a revert is complete only when the
	// rejected pods are gone, ready or not. The run closes on the primary's
	// phase, and a member's revert is an update the plan gate refuses once
	// no run is pinned, so a primary declared RolledBack ahead of its
	// members would strand them on the rejected revision.
	if !rollingBack {
		rollingBack = groupRollingBack(in, stableHash)
	}
	if rollingBack {
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseRollingBack)
		return (&Result{Active: true, RolledBack: true}).wake(in.Requeue)
	}
	setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseRolledBack)
	return &Result{Active: true, RolledBack: true}
}

// groupRollingBack reports whether any member of the canary unit, the primary
// included, still has a pod off its stable revision. primaryStable is the
// identity the primary's own check resolved; the other members use their
// persisted one. A member with no stable identity is not waited on: it is
// sent no revert signal, so nothing is in flight for it.
func groupRollingBack(in ReconcileInputs, primaryStable string) bool {
	for member, pods := range in.GroupTotalPerRevisionPods {
		stable := in.GroupStableRevisionHashes[member]
		if member == in.Component {
			stable = primaryStable
		}
		if stable == "" {
			continue
		}
		for h, n := range pods {
			if h != "" && h != stable && n > 0 {
				return true
			}
		}
	}
	return false
}

// drainElapsed reports whether the ScaleDownDelaySeconds drain window has passed
// since the final step was entered. A nil/zero delay completes immediately.
func drainElapsed(cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, now time.Time) bool {
	if plan.ScaleDownDelaySeconds == nil || *plan.ScaleDownDelaySeconds <= 0 {
		return true
	}
	if cs.StepEnteredTime == nil {
		return false
	}
	deadline := cs.StepEnteredTime.Time.Add(time.Duration(*plan.ScaleDownDelaySeconds) * time.Second)
	return !now.Before(deadline)
}

// resolveReadyTimeout is how long a step's capacity gate may stay unsatisfied
// before the canary is marked Failed: the ready-timeout annotation wins, then
// the plan's readyTimeout, then the operator-configured default. Zero (nothing
// configured anywhere) disables the escalation — the value is deliberately
// config-driven with no in-code literal.
func resolveReadyTimeout(in ReconcileInputs, plan *v1beta1.GroupCanary) time.Duration {
	if v, ok := in.ISVC.Annotations[constants.RolloutReadyTimeoutAnnotation]; ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	if plan != nil && plan.ReadyTimeout != nil && plan.ReadyTimeout.Duration > 0 {
		return plan.ReadyTimeout.Duration
	}
	return in.DefaultReadyTimeout
}

// effectiveCanaryPlan is the effective canary body for callers that hold only
// ReconcileInputs (the plan indexing itself stays in Reconcile).
func effectiveCanaryPlan(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *v1beta1.GroupCanary {
	g := rollout.CanaryGroupFor(isvc, component)
	if g == nil {
		return nil
	}
	return g.Canary
}

// capacityGateExpired reports whether the current capacity wait has lasted
// past the timeout, measured from CapacityWaitSince. A nil status, an
// unrecorded wait or a non-positive timeout is never expired, so a transient
// nil cannot wrongly fail a rollout.
func capacityGateExpired(cs *v1beta1.CanaryStatus, timeout time.Duration, now time.Time) bool {
	if cs == nil || cs.CapacityWaitSince == nil || timeout <= 0 {
		return false
	}
	return !now.Before(cs.CapacityWaitSince.Time.Add(timeout))
}
