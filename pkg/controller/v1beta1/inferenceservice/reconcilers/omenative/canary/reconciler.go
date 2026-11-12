package canary

import (
	"context"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
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
// Instance counts read from the primary's InferenceReplica; PerRevisionPods
// supplies ready Pod counts per revision for the rollback, re-arm and cutover
// checks, and stands in for capacity only when no replica was observed.
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
	// Policies is the set of RolloutPolicy objects the run layer observed
	// for this pass; the effective view resolves the spec's references
	// through it outside a pinned run.
	Policies rollout.Policies
	// Group is the canary group being dispatched — the unit's own ladder. Nil
	// resolves it from Component through the effective view.
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
	// ReadyCanaryInstances is the replica's account of the canary revision's
	// capacity: the Instances whose declared pod set runs the canary revision
	// in rotation and, under the Component's minReadySeconds, has held Ready
	// for at least that long. A multi-pod gang counts once and only whole.
	// nil (no replica observed) falls back to PerRevisionPods.
	ReadyCanaryInstances *int32
	// ReadyStableInstances is the replica's account of the stable revision's
	// capacity, counted the way ReadyCanaryInstances counts the canary's.
	// The step keeps a fixed number of Instances on the stable revision; a
	// count below it is the stable side short. nil (no replica observed)
	// falls back to PerRevisionPods.
	ReadyStableInstances *int32
	// CapacityWindowWait is how long until the soonest canary pod still
	// inside the Component's minReadySeconds window has held Ready for it,
	// zero when none is waiting. The crossing raises no watch event, so a
	// capacity wait short on the window alone wakes for it when it comes
	// before the step cadence.
	CapacityWindowWait time.Duration
	// DesiredReplicas is the Instance count the primary's replica is scaled
	// to (DesiredReplicas): every step's new-Instance count and partition
	// are resolved against it.
	DesiredReplicas int32
	PerRevisionPods map[string]int32
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
	// GroupReadyPerRevisionPods counts each member's ready serving pods per
	// revision hash, keyed by Component. A rollback moves a member's traffic
	// onto its stable revision only when that revision has serving capacity;
	// a member absent here is written on the primary's view alone.
	GroupReadyPerRevisionPods map[v1beta1.ComponentType]map[string]int32
	// Crash is a canary pod in a crash loop after its Instance was serving, on
	// any member of the unit; nil when none is. A single restart is a capacity
	// dip, not a crash. The dispatcher reads it from the pods and the Instance
	// rows; a live ladder parks on it.
	Crash *CanaryCrash
	// CanaryRestartedAt is the newest moment a canary pod of the unit died or
	// came back, on any member; zero when none has. The dispatcher reads it
	// from the pods and the Instance rows beside Crash. A timed soak measures
	// from the later of the split first serving and this instant: a single
	// restart keeps the step, and the step moves only once a full soak has
	// passed since it.
	CanaryRestartedAt time.Time
	// InstancesOffTarget names, per retargeted member, the Instances the
	// member's replica does not count as serving on its target revision,
	// each with the phase its row reports, in index order. Only the stop at
	// the cutover reads it, so the names are built on that pass alone and
	// cost nothing on any other. nil when no replica was observed.
	InstancesOffTarget func() map[v1beta1.ComponentType][]string
	// SecondaryCapacityReady is true when every NON-primary component's
	// serving canary Instances have reached that component's step staged
	// count, counted the way ReadyCanaryInstances counts the primary's.
	// Single-component canaries pass true.
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
// each was minted under, and the Ready capacity the gate counted on the
// target. A member that was not bumped names the same revision twice; a
// member with no stable revision leaves it empty.
type MemberRevisions struct {
	CanaryRevisionHash    string
	StableRevisionHash    string
	CanaryPairingProtocol string
	StablePairingProtocol string
	// ReadyCanaryCapacity is the member's serving Instances on its target
	// revision as the capacity gate counts them, the member's replica's own
	// account; above zero, a target Instance is in rotation and has held
	// Ready for the member's minReadySeconds.
	ReadyCanaryCapacity int32
	// DesiredReplicas is the Instance count the member's replica is scaled
	// to, the capacity its target must reach before the cutover is complete.
	DesiredReplicas int32
	// ReadyStableInstances is the member's serving Instances on its stable
	// revision, counted as ReadyCanaryCapacity counts the target's; nil (no
	// replica observed) falls back to the member's ready pods.
	ReadyStableInstances *int32
}

// retargeted reports whether the member rolls toward a revision other than
// its stable one; a member at its stable revision takes no part in the steps.
func (m MemberRevisions) retargeted() bool {
	return m.CanaryRevisionHash != "" && m.CanaryRevisionHash != m.StableRevisionHash
}

// canaryCapacity is the canary capacity the gates compare: the replica's
// serving Instance count on the canary revision when a replica was observed,
// else the ready pod count that stands in for it.
func canaryCapacity(servingInstances *int32, readyPods int32) int32 {
	if servingInstances != nil {
		return *servingInstances
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

// readyCanaryCapacity is the primary's canary capacity.
func readyCanaryCapacity(in ReconcileInputs) int32 {
	return canaryCapacity(in.ReadyCanaryInstances, in.PerRevisionPods[in.CanaryRevisionHash])
}

// readyStableCapacity is the stable capacity the stable-side reading
// compares: the replica's serving Instance count on the stable revision when
// a replica was observed, else the ready pod count that stands in for it.
func readyStableCapacity(in ReconcileInputs) int32 {
	return canaryCapacity(in.ReadyStableInstances, in.PerRevisionPods[in.StableRevisionHash])
}

// capacityWake is the wait of a pass whose capacity gate holds: the step
// cadence, or the moment the soonest canary pod clears its availability
// window when that comes first, since the crossing raises no watch event.
func capacityWake(in ReconcileInputs) time.Duration {
	if in.CapacityWindowWait > 0 && (in.Requeue <= 0 || in.CapacityWindowWait < in.Requeue) {
		return in.CapacityWindowWait
	}
	return in.Requeue
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
// asks the controller to re-reconcile (capacity / pause / drain pending, or
// the one Complete pass, which comes back so the run layer can close the run
// on the completion it wrote).
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
	// landed; each applied verb leaves a record in status (PromotedThrough,
	// the rejected revision of a rollback) that keeps the still-visible
	// annotation inert until then.
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
// (TrafficWeight 100) shifts traffic off the stable revision, drains it,
// releases its last instance to roll on a later pass that has counted the
// canary capacity itself, and reads Stable once every instance serves the
// canary revision.
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
		g = rollout.CanaryGroupFor(in.ISVC, in.Policies, in.Component)
	}
	if g == nil || g.Canary == nil || len(g.Canary.Steps) == 0 {
		return &Result{Active: false}, nil
	}
	plan := g.Canary

	cs := rollout.CanaryStatusFor(&in.ISVC.Status, in.Component)
	// Status is an unvalidated subresource: a negative step is an external
	// write, read back from the record before it can index plan.Steps.
	if cs != nil && cs.CurrentStep < 0 {
		cs.CurrentStep = rollout.StepOf(cs, in.ISVC.Status.Components[in.Component].RolloutPhase, plan.Steps)
	}

	// Global pause (ome.io/rollout-paused): the operator held the rollout.
	// Observe only — no step advance, no traffic or phase write, no rollback
	// arm/clear, and no state-machine (re)initialization. Step timers are
	// left untouched, so clearing the pause resumes the current step with its
	// clocks intact; the one clock a pause stops is the capacity wait, the
	// ready timeout's, because time the operator holds the rollout is not
	// time the pods were given to come up. Both pause depths hold the canary
	// equally; the depth only changes whether Instance repair keeps running
	// underneath. The one verb the pause consumes is a forced promote: it
	// never overrides the pause, and left in place it would open the first
	// gate the resumed ladder meets.
	if paused, freeze := constants.RolloutPauseState(in.ISVC.Annotations); paused {
		if cs != nil && int(cs.CurrentStep) < len(plan.Steps) {
			why := "the rollout is paused"
			if freeze {
				why = "the rollout is frozen"
			}
			ignoreForce(in, cs, take, why)
		}
		if cs != nil {
			stopCapacityWait(cs, rollout.StateOf(cs, in.ISVC.Status.Components[in.Component].RolloutPhase, len(plan.Steps)))
		}
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
	if resumed, err := handleResume(ctx, in, cs, plan, take); err != nil {
		return nil, err
	} else if resumed {
		return (&Result{Active: true}).wake(in.Requeue), nil
	}

	// The stable side is read against the split the pass leaves, on every
	// path from here: a hold, a stop or a move of the split clears the
	// record, a step that serves short sets it.
	defer func() { observeStableSide(in, cs, plan) }()

	// Verbs and holds, in the order the canary grid gives them precedence.
	// A pass that ends inside one of them returns here; otherwise cs is the
	// (possibly freshly armed) status the step machine runs against.
	var res *Result
	var err error
	if cs, res, err = applyHolds(ctx, in, g, cs, plan, targetChanged, take); err != nil || res != nil {
		return res, err
	}

	// Backfill a missing stable identity from the observed stable revision (a
	// canary re-armed after a completed one, or a status recorded before the
	// identity was persisted). Once set it is never re-inferred: the live pod
	// set stops naming the pre-canary stable as revisions retarget and drain.
	if cs.StableRevisionHash == "" && in.StableRevisionHash != "" {
		cs.StableRevisionHash = in.StableRevisionHash
	}

	// A canary pod that served and keeps dying is the canary's verdict on the
	// revision, read before any gate so no count can argue with it: the
	// ladder parks where it stands and the broken revision serves nothing.
	if in.Crash != nil {
		return parkOnCrash(in, cs), nil
	}

	// Finish any promotion whose advance already persisted: hand the applied
	// promote annotation back for removal and, once it is gone, clear the
	// durable record so manual promotion re-arms for later steps.
	syncPromotedThrough(in, cs, take)
	step := plan.Steps[cs.CurrentStep]

	// A forced promote applies at every step of an armed ladder: it lifts the
	// step's capacity wait and opens the step's gate if it has one.
	forced := forceRequested(in.ISVC, cs)

	// The step's partition keeps the stable revision's last instance (the
	// held floor) for as long as the canary is in flight, so the capacity a
	// step stages, and its gate waits for, is what the remaining instances can
	// reach.
	partition := stepPartition(step, in.DesiredReplicas)
	newCount := in.DesiredReplicas - partition

	// Capacity gate: don't shift traffic until the canary Instances serve, as
	// their replicas account for them — the primary's own newCount AND every
	// secondary component's canary capacity (PD, so a serving router can't
	// advance the step while the engine/decoder canary Instances behind it are
	// still coming up). The wait is timed from when it
	// began, never from the step's soak anchor, so a long bake cannot spend the
	// budget and a dip cannot restart the soak (a canary pod's restart does,
	// through the soak's own reading of the pods); past the ready timeout the
	// canary parks Failed with the stable revision still serving, and the
	// roll is held where it stands (EffectivePartition holds every Instance).
	readyCanary := readyCanaryCapacity(in)
	// A target with Ready pods is the member's latest ready revision whether
	// or not the step's capacity is met yet.
	publishReadyRevisions(in)
	state := rollout.StateOf(cs, in.ISVC.Status.Components[in.Component].RolloutPhase, len(plan.Steps))
	capacityShort := readyCanary < newCount || !in.SecondaryCapacityReady
	// A forced step's wait keeps no clock: the operator lifted it, so it is
	// never timed and never parks, while the partition still asks for the
	// staged count and capacity converges behind the gate. A clock that was
	// running is the force taking effect on this pass.
	lifted := forced && cs.CapacityWaitSince != nil
	if forced {
		cs.CapacityWaitSince = nil
	}
	if capacityShort && !forced && cs.CapacityWaitSince == nil {
		cs.CapacityWaitSince = &metav1.Time{Time: in.Now}
	}
	if capacityShort && !forced && state != rollout.CanaryStateDraining {
		// The held split names the canary revision only while a pod of it
		// serves; its share rests on the stable revision otherwise and
		// returns on the pass that sees a canary pod serve again.
		holdGroupTraffic(in)
	}
	if capacityShort && !forced && capacityGateExpired(cs, resolveReadyTimeout(in, plan), in.Now) {
		// A drain's split names the canary revision alone, so its park hands
		// the traffic back to the held stable instance, as a crash park does.
		if state == rollout.CanaryStateDraining {
			returnTrafficToStable(in)
		}
		return stopOnCapacityTimeout(in, cs), nil
	}
	if capacityShort && !forced && state != rollout.CanaryStateDraining {
		// A drain is exempt from this hold: capacity lost after the final
		// traffic write is the workload engine's repair, and the drain's own
		// release waits on it below, inside the same timeout.
		if state == rollout.CanaryStateServing || state == rollout.CanaryStatePreHold {
			// A split that was serving keeps its phase and its recorded
			// weight through a dip; the gate only keeps the step from moving,
			// and the metric gate is still read for the one decision that
			// needs no canary capacity, the rollback.
			if gateRollsBackWhileShort(ctx, in, cs, state, step) {
				return rollBackOnGateFailure(in, cs), nil
			}
			return (&Result{Active: true, Partition: partition}).wake(capacityWake(in)), nil
		}
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePending)
		return (&Result{Active: true, Partition: partition}).wake(capacityWake(in)), nil
	}
	if !capacityShort {
		cs.CapacityWaitSince = nil
	}

	// Pre-step hold (repin clamp): the pinned plan was replaced mid-run and
	// the clamped step would raise exposure, so the traffic raise waits for
	// an explicit promote (value = canary hash). Capacity above already
	// converged toward the clamped step — capacity ahead of traffic is the
	// supported warm-up pattern; exposure is the traffic write below.
	if cs.PreStepHold {
		released := false
		for _, key := range promoteKeys {
			if in.ISVC.Annotations[key] != cs.CanaryRevisionHash {
				continue
			}
			// The release is recorded like any applied promote: the record
			// keeps the annotation inert until the controller removes it after
			// the flush, so a lost flush cannot leave the hold released and
			// the verb gone. A force is spent on the release like a promote.
			cs.PreStepHold = false
			cs.PromotedThrough = cs.CanaryRevisionHash
			take(key)
			released = true
		}
		if !released {
			// The held split follows the pods like any held split: its
			// share is back on the canary revision as soon as one serves.
			holdGroupTraffic(in)
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePaused)
			return (&Result{Active: true, Partition: partition}).wake(in.Requeue), nil
		}
		// A verb spent on the release opens no gate on this pass.
		forced = false
	}

	// Capacity satisfied: program this step's external traffic weight on
	// every member of the unit.
	weightBefore := cs.ObservedTrafficWeight
	applyGroupTraffic(in, step.Traffic)

	// Final step (TrafficWeight 100): shift traffic off the stable revision,
	// hold its last instance through the drain window, then release it: the
	// done sentinel drops the floor, the held instance rolls, and the unit
	// reads Stable once nothing serves the stable revision.
	if int(cs.CurrentStep) == len(plan.Steps)-1 {
		// Anchor the drain window and the final gate to the moment 100%
		// traffic actually shifts, not to step entry: on slow capacity the
		// final step is entered well before traffic moves, so measuring from
		// step entry could consume the whole window before cutover. The pass
		// whose traffic write moved the weight to 100 is that moment.
		shifted := weightBefore != 100
		if shifted {
			cs.StepEnteredTime = &metav1.Time{Time: in.Now}
		}
		// The final step re-reads a live promote on every pass until
		// completion, so a force is announced once: on the pass that opens
		// the step into its drain or lifts a running capacity wait.
		opening := in.ISVC.Status.Components[in.Component].RolloutPhase != v1beta1.RolloutPhasePromoting
		// A metric gate that passed at 100 percent stays open for the rest
		// of the drain: Promoting on this step means the gate has passed. It
		// is still sampled on its interval, and only a breach at the limit,
		// an unread sample under the Rollback policy or a stall closes it; a
		// pass with nothing to decide holds nothing, and the release follows
		// the window.
		gateOpen := stepIsAnalysis(step) && !shifted && !opening
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
				return rollBackOnGateFailure(in, cs), nil
			case decFailed:
				// A stall is a stop like the capacity gate's: the drain's split
				// names the canary revision alone, so the stable Instance the
				// drain kept takes the traffic back, as it does under a crash.
				returnTrafficToStable(in)
				parkFailed(in, cs, v1beta1.CanaryFailureAnalysisStalled)
				return (&Result{Active: true, Partition: in.DesiredReplicas}).wake(in.ParkedRequeue), nil
			case decHold:
				// A held gate reads Paused on every step: the phase names the
				// wait, and Promoting is the drain that follows the gate. A
				// gate that already passed is not held again by a pass with
				// nothing to decide on.
				if !gateOpen {
					setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePaused)
					return (&Result{Active: true, Partition: partition}).wake(stepWake(in, step)), nil
				}
			case decAdvance:
				// gate passed at 100% — fall through to the drain window + completion.
			}
		}
		if forced && (opening || lifted) {
			announceForce(in, cs, readyCanary, newCount)
		}
		// The release cannot be undone, so it is decided on a count taken
		// after the 100% write landed: the pass that moved traffic ends here,
		// and a drain short of canary capacity holds it until it is back.
		if shifted || capacityShort {
			return (&Result{Active: true, Partition: partition}).wake(in.Requeue), nil
		}
		if drainElapsed(cs, plan, in.Now) {
			// A promote that opens the final gate stays live through the drain
			// window (the gate re-evaluates every pass until completion). At the
			// completion edge it is recorded and handed back for removal after
			// the flush; the done sentinel's sync clears the record once the
			// annotation is observed gone.
			recordPromotes(in, cs, take)
			// Mark done (sentinel), don't clear: EffectivePartition reads the
			// sentinel as partition 0, which releases the held stable instance
			// now that traffic has left it and drained, instead of re-holding
			// it under a step-0 partition. The release is projected on the next
			// pass, so the roll it starts is finished by finishCutover from
			// here on; the cutover names the target alone, whatever serves.
			cs.CurrentStep = int32(len(plan.Steps))
			applyCutoverTraffic(in)
			return finishCutover(in, cs, plan, false), nil
		}
		return (&Result{Active: true, Partition: partition}).wake(in.Requeue), nil
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
			return rollBackOnGateFailure(in, cs), nil
		case decFailed:
			// The stable side still carries its share of a split, so the
			// stop leaves the step's split as it stands.
			parkFailed(in, cs, v1beta1.CanaryFailureAnalysisStalled)
			return (&Result{Active: true, Partition: in.DesiredReplicas}).wake(in.ParkedRequeue), nil
		case decHold:
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePaused)
			return (&Result{Active: true, Partition: partition}).wake(stepWake(in, step)), nil
		case decAdvance:
			// fall through to advance.
		}
	}
	if forced {
		announceForce(in, cs, readyCanary, newCount)
	}
	advanceStep(in, take)
	return (&Result{Active: true, Stepped: true, Partition: partition}).wake(in.Requeue), nil
}

// parkOnCrash parks a live ladder at its current step because a canary pod
// keeps dying after it was serving. A capacity dip, a single restart among
// them, keeps the step's split while the workload repairs it; a crash loop
// returns the split to the stable revision, because the capacity is there
// and cannot be trusted with traffic. The park
// is the capacity gate's: the step's canary capacity did not hold, and the
// pod that broke it is named in the event. Stable keeps serving until the
// operator rolls back, resumes or pushes a new revision.
func parkOnCrash(in ReconcileInputs, cs *v1beta1.CanaryStatus) *Result {
	returnTrafficToStable(in)
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryPodCrashed,
		"%s canary pod %s keeps dying after it was serving (%s); the ladder is parked at step %d and the step's traffic is back on the stable revision",
		in.Crash.Component, in.Crash.PodName, in.Crash.Detail, cs.CurrentStep)
	parkFailed(in, cs, v1beta1.CanaryFailureCapacityTimeout)
	return (&Result{Active: true, Partition: in.DesiredReplicas}).wake(in.ParkedRequeue)
}

// gateRollsBackWhileShort reads the metric gate of a serving step whose
// canary capacity is short and reports whether it decided a rollback. The
// step cannot advance until the capacity is back, so a passing sample and a
// stall move nothing; a breach at the failure limit, or an unreadable gate
// under a rollback policy, is the gate's reading of the pods that still
// serve, and the revert it decides needs no canary capacity.
func gateRollsBackWhileShort(ctx context.Context, in ReconcileInputs, cs *v1beta1.CanaryStatus, state rollout.CanaryState, step v1beta1.RolloutGroupStep) bool {
	return state == rollout.CanaryStateServing && stepIsAnalysis(step) && evaluateStep(ctx, in, cs, step) == decRollback
}

// pausedResult reports a globally-paused canary's state without mutating it.
// The rollback signal echoes persisted status so the controller neither arms
// nor clears the IR's RollbackToRevision while paused, and the partition
// echoes the current step so the staged split stays where it is.
func pausedResult(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary) *Result {
	// Not started: inactive. A pause must not initialize the state machine
	// (initialization stamps the step timers) — a canary toward a target
	// that appeared while paused starts once the pause clears.
	if cs == nil {
		return &Result{Active: false}
	}
	// A stop holds every Instance where it stands, paused or not; a stop
	// that recorded a rejection echoes its revert signal as any revert does.
	if rollout.StateOf(cs, in.ISVC.Status.Components[in.Component].RolloutPhase, len(plan.Steps)) == rollout.CanaryStateFailed {
		return (&Result{Active: true, RolledBack: cs.RolledBackRevisionHash != "", Partition: in.DesiredReplicas}).wake(in.Requeue)
	}
	// A revert is echoed wherever it began, a rollback at the sentinel
	// included: the record keeps the sentinel beside the rejection.
	if cs.RolledBackRevisionHash != "" {
		return (&Result{Active: true, RolledBack: true}).wake(in.Requeue)
	}
	// Already done: inactive.
	if int(cs.CurrentStep) >= len(plan.Steps) {
		return &Result{Active: false}
	}
	partition := stepPartition(plan.Steps[cs.CurrentStep], in.DesiredReplicas)
	return (&Result{Active: true, Partition: partition}).wake(in.Requeue)
}

// stopCapacityWait clears the capacity wait of a paused unit in a state
// where the wait is live, at a step or at the cutover after the release:
// the wait is the ready timeout's clock, and no time the operator holds the
// rollout counts toward it. The first pass after the pause that finds the
// capacity still short, or the released Instances still rolling, clocks the
// wait again, so the pods have the whole timeout from then. A unit that is
// Failed or rolling back keeps its record as it is.
func stopCapacityWait(cs *v1beta1.CanaryStatus, state rollout.CanaryState) {
	switch state {
	case rollout.CanaryStateStaging, rollout.CanaryStateServing, rollout.CanaryStatePreHold, rollout.CanaryStateDraining, rollout.CanaryStateDone:
		cs.CapacityWaitSince = nil
	}
}

// finishCutover runs a unit whose done sentinel is set. The sentinel released
// the held floor, so the last stable instance is rolling; until every member
// serves on its target revision alone the unit stays Promoting at partition 0
// and comes back at the step cadence, because the roll it waits on moves no
// annotation and no step. That wait is clocked like a step's capacity wait
// once the release is projected (rolling): past the ready timeout the unit
// stops Failed with the Instances that do not serve the new revision named,
// and the traffic stays where the cutover put it, because the stable
// revision has no held Instance left to carry it. The release pass itself
// clocks nothing, since its projection lands on the next pass. Once the
// cutover has landed the promoted revisions are recorded, the pre-canary
// identity is dropped (the canary revision is the stable revision now, so it
// cannot leak into a later rollout's rollback target), and the unit reads
// Stable; that transition is the one Complete pass. A unit with no
// pre-canary identity on record has nothing left to roll and reads Stable at
// once, which also keeps a canary pod lost after completion a workload
// repair, not a canary state.
func finishCutover(in ReconcileInputs, cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, rolling bool) *Result {
	// A unit that reads Stable at the sentinel has completed, and the
	// completion dropped its pre-canary identity; one written onto the
	// record since is dropped again, with no second completion edge.
	if in.ISVC.Status.Components[in.Component].RolloutPhase == v1beta1.RolloutPhaseStable {
		cs.StableRevisionHash = ""
	}
	if cs.StableRevisionHash == "" {
		cs.CapacityWaitSince = nil
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
		return &Result{Active: false}
	}
	if !cutoverComplete(in) {
		if rolling && cs.CapacityWaitSince == nil {
			cs.CapacityWaitSince = &metav1.Time{Time: in.Now}
		}
		if timeout := resolveReadyTimeout(in, plan); rolling && capacityGateExpired(cs, timeout, in.Now) {
			reportCutoverTimeout(in, cs, timeout)
			return stopOnCapacityTimeout(in, cs)
		}
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePromoting)
		return (&Result{Active: true, Partition: 0}).wake(capacityWake(in))
	}
	// Every member's target owns its traffic now; the pre-canary stable is
	// the revision each superseded. The run layer closes the run only on a
	// pass that reads this completion, and a status write of the service
	// re-enqueues nothing, so the completing pass asks to come back once at
	// the step cadence rather than wait for an unrelated event.
	cs.CapacityWaitSince = nil
	recordPromotedRevisions(in, cs.StableRevisionHash)
	cs.StableRevisionHash = ""
	setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
	return (&Result{Active: true, Complete: true, Partition: 0}).wake(in.Requeue)
}

// stopOnCapacityTimeout stops a unit whose canary capacity did not come up
// inside the ready timeout, at a step or at the cutover: the Failed record
// with reason CapacityTimeout, every Instance held where it stands, and the
// stopped cadence.
func stopOnCapacityTimeout(in ReconcileInputs, cs *v1beta1.CanaryStatus) *Result {
	parkFailed(in, cs, v1beta1.CanaryFailureCapacityTimeout)
	return (&Result{Active: true, Partition: in.DesiredReplicas}).wake(in.ParkedRequeue)
}

// reportCutoverTimeout records the stop of a unit past its last step whose
// released Instances did not come back on the new revision inside the ready
// timeout: the primary's serving count against the count it is scaled to,
// the timeout, where the traffic stays, and the Instances of every member
// that do not serve their new revision, so the operator can read which
// Instance holds the cutover.
func reportCutoverTimeout(in ReconcileInputs, cs *v1beta1.CanaryStatus, timeout time.Duration) {
	emit(in.Recorder, in.ISVC, corev1.EventTypeWarning, EventReasonCanaryCutoverTimeout,
		"%s canary stopped after its last step: %d of %d %s Instances serve revision %s after the ready timeout of %s; the traffic stays on the new revision and the unit reads Failed (CapacityTimeout) until a new revision, a rollback or a resume; Instances not serving their new revision: %s",
		in.Component, readyCanaryCapacity(in), in.DesiredReplicas, in.Component, cs.CanaryRevisionHash, timeout, instancesOffTargetLabel(in.instancesOffTarget()))
}

// instancesOffTarget builds the names of the Instances off their new
// revision, member by member; nil when no replica was observed.
func (in ReconcileInputs) instancesOffTarget() map[v1beta1.ComponentType][]string {
	if in.InstancesOffTarget == nil {
		return nil
	}
	return in.InstancesOffTarget()
}

// instancesOffTargetLabel renders the unit's Instances that do not serve
// their new revision, member by member in the order a group picks its
// primary from the roles table, as "engine: Instance 3 (Updating); decoder:
// Instance 1 (Pending)"; "none observed" when no member reports one.
func instancesOffTargetLabel(instances map[v1beta1.ComponentType][]string) string {
	var parts []string
	for _, c := range v1beta1.RolesByPrimaryRank() {
		if names := instances[c]; len(names) > 0 {
			parts = append(parts, string(c)+": "+strings.Join(names, ", "))
		}
	}
	if len(parts) == 0 {
		return "none observed"
	}
	return strings.Join(parts, "; ")
}

// cutoverComplete reports whether the released cutover has landed on every
// member of the unit: the target revision has its full capacity Ready and no
// ready pod serves another revision, so the held stable instance has rolled
// and drained. The primary is read from its own ready counts; each secondary
// from the pair the dispatcher resolved for it.
func cutoverComplete(in ReconcileInputs) bool {
	if readyCanaryCapacity(in) < in.DesiredReplicas || servesOtherRevision(in.PerRevisionPods, in.CanaryRevisionHash) {
		return false
	}
	for c, m := range in.Secondaries {
		if m.ReadyCanaryCapacity < m.DesiredReplicas || servesOtherRevision(in.GroupReadyPerRevisionPods[c], m.CanaryRevisionHash) {
			return false
		}
	}
	return true
}

// servesOtherRevision reports whether any ready pod runs a revision other
// than hash.
func servesOtherRevision(readyPods map[string]int32, hash string) bool {
	for h, n := range readyPods {
		if h != "" && h != hash && n > 0 {
			return true
		}
	}
	return false
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
// there, rejecting cs.RolledBackRevisionHash. Traffic goes 100% to stable once
// stable has serving capacity to take it, and rests on the pods that serve
// until then; the controller reads
// cs.RolledBackRevisionHash and makes the IR roll every Instance back to the
// stable ControllerRevision, so the rejected-revision pods drain. While they
// drain → RollingBack; once gone → RolledBack (held until a different target
// appears).
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
	// Traffic moves only onto capacity that serves: the stable revision
	// takes everything once a pod of it serves. Until then each member's
	// traffic rests on the revision that serves, the rejected one included,
	// rather than on a revision with nothing behind it; with nothing serving
	// on either side the split stands as written.
	sides := unitSides(in, cs.RolledBackRevisionHash, stableHash)
	write := trafficWrite{context: "rollback", sides: sides, before: stableShares(in.ISVC, sides), remainder: true, recorded: cs.ObservedTrafficWeight}
	if in.PerRevisionPods[stableHash] > 0 {
		applyTraffic(in.ISVC, in.Component, cs.RolledBackRevisionHash, stableHash, "", stableProtocol, 0)
	} else {
		restOnServingRevision(in.ISVC, sides[0])
	}
	applyGroupStableTraffic(in, sides[1:])
	reportTrafficOffStable(in, write)
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
	// The revert is complete: every member's stable revision owns its
	// traffic again.
	recordRolledBackRevisions(in, stableHash)
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
func effectiveCanaryPlan(in ReconcileInputs) *v1beta1.GroupCanary {
	g := rollout.CanaryGroupFor(in.ISVC, in.Policies, in.Component)
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
