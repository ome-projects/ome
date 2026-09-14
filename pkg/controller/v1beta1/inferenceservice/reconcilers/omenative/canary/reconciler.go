package canary

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
)

// reconcileRequeue is how soon the controller re-checks an in-progress canary
// (capacity coming up, an Auto pause timer, or a drain window).
const reconcileRequeue = 10 * time.Second

// failedRequeue is the slow heartbeat once a canary step is escalated to Failed —
// the rollout is parked (stable keeps serving) until the operator acts, so there
// is no need to tight-poll.
const failedRequeue = 5 * time.Minute

// ReconcileInputs is the subset of state the canary executor needs. It mirrors
// coordination.ReconcileInputs. DesiredReplicas and ReadyCanaryInstances are
// Instance counts; PerRevisionPods supplies ready serving Pod counts and the
// capacity fallback when the exact Instance count is unavailable.
type ReconcileInputs struct {
	Client client.Client
	// Reader is the live API reader for one-off reads of types the manager does
	// not watch (the analysis auth Secret): reading those through the cached
	// Client would spin up a cluster-wide informer for the type.
	Reader             client.Reader
	ISVC               *v1beta1.InferenceService
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
	// SecondaryCapacityReady is true when every NON-primary component's canary
	// Instances have reached that component's step newCount. The primary uses its
	// exact complete-PodReady target Instance count. Single-component canaries
	// pass true.
	SecondaryCapacityReady bool
	Now                    time.Time
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
}

// The ready Pod count bounds the exact ready Instance count and remains the
// fallback for direct callers that do not provide topology-aware observation.
func readyCapacityCount(readyPods int32, readyInstances *int32) int32 {
	if readyInstances != nil && *readyInstances < readyPods {
		return *readyInstances
	}
	return readyPods
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
// no canary is in progress. Partition is the StatefulSet-style
// RollingUpdate.Partition the controller applies to the canary's component
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
}

// Reconcile advances the canary toward the declared plan, mutating isvc.Status
// in-memory. No-op (Active=false) when spec.rollout.canary is unset. Per step it
// performs capacity → traffic → pause, advancing on promotion; the final step
// (TrafficWeight 100) drains and scales the stable revision down.
func Reconcile(ctx context.Context, in ReconcileInputs) (*Result, error) {
	g := v1beta1.EffectiveCanaryGroup(in.ISVC)
	if g == nil || g.Canary == nil || len(g.Canary.Steps) == 0 {
		return &Result{Active: false}, nil
	}
	plan := g.Canary

	cs := in.ISVC.Status.Canary
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
		return pausedResult(cs, plan, in.DesiredReplicas), nil
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

	// Bind an active canary to the IR's authoritative target before handling
	// operator commands. A changed target starts the staged plan with fresh
	// per-target state while preserving the original stable identity.
	phase := in.ISVC.Status.Components[in.Component].RolloutPhase
	if cs != nil && int(cs.CurrentStep) < len(plan.Steps) &&
		cs.RolledBackRevisionHash == "" && phase != v1beta1.RolloutPhaseFailed {
		if cs.StableRevisionHash == "" && in.StableRevisionHash != "" {
			cs.StableRevisionHash = in.StableRevisionHash
		}
		// Retargets re-arm only under a pinned run: the run layer closes and
		// reopens the run on a target change in the same pass, so RunActive
		// here means the fresh run is already pinned. Without one, hold state
		// as-is rather than start a fresh canary on an unpinned plan.
		if in.RunActive && in.CanaryRevisionHash != "" && (targetChanged || in.CanaryRevisionHash != cs.CanaryRevisionHash) {
			resetCanaryStatus(cs, in.TargetID, in.CanaryRevisionHash, in.Now)
		}
	}

	// Rollback (ome.io/rollout-rollback): abandon the canary and return the
	// component to the stable revision, then HOLD there rejecting the rolled-back
	// revision. The revert itself is driven by the controller — seeing
	// cs.RolledBackRevisionHash it points the IR at the stable ControllerRevision
	// so every Instance rolls back. Here we record the rejected revision, shift
	// traffic to stable, and re-arm only when a different target appears (clearing
	// the annotation alone never retries the rejected revision).
	if cs != nil {
		if cs.RolledBackRevisionHash != "" {
			// Re-arm to a fresh canary ONLY when a genuinely NEW target appears.
			// During the revert the IR targets the stable revision, so the
			// observed target (in.CanaryRevisionHash) reports stable — that is the
			// revision we're holding on, not a new target. Exclude both the stable
			// revision and the rejected revision itself.
			stableHash := stableHashFor(cs, in.PerRevisionPods, cs.RolledBackRevisionHash)
			if in.RunActive && in.CanaryRevisionHash != "" &&
				(targetChanged || (in.CanaryRevisionHash != cs.RolledBackRevisionHash && in.CanaryRevisionHash != stableHash)) {
				if err := consumeAnnotation(ctx, in.Client, in.ISVC, constants.RolloutRollbackAnnotation); err != nil {
					return nil, err
				}
				resetCanaryStatus(cs, in.TargetID, in.CanaryRevisionHash, in.Now)
				// fall through: start a fresh canary toward the new target.
			} else {
				return reconcileRollback(in, cs), nil
			}
		} else if isRollbackRequested(in.ISVC) && int(cs.CurrentStep) < len(plan.Steps) {
			cs.RolledBackRevisionHash = cs.CanaryRevisionHash
			recordRollback(in.ISVC, in.Component, "manual")
			return reconcileRollback(in, cs), nil
		}
	}

	// Failed is a parked terminal: a step escalated to Failed (capacity gate timed
	// out before the canary pods were Ready, or analysis stalled inconclusive past
	// the ready-timeout) is HELD there — stable keeps serving until the operator
	// decides. Like the rolled-back hold above, re-arm only when a genuinely NEW
	// target appears; otherwise return without re-stamping StepEnteredTime or
	// re-evaluating. Re-stamping would reset the stall/ready-timeout clock anchored
	// on it, so the next pass would read "not stalled," flip back to Paused/Pending,
	// then re-fail a timeout later — oscillating instead of staying Failed. The
	// failed revision is cs.CanaryRevisionHash; exclude it and the stable revision
	// (the observed target reports stable while no canary pods remain).
	if cs != nil && in.ISVC.Status.Components[in.Component].RolloutPhase == v1beta1.RolloutPhaseFailed {
		stableHash := stableHashFor(cs, in.PerRevisionPods, cs.CanaryRevisionHash)
		if in.RunActive && in.CanaryRevisionHash != "" &&
			(targetChanged || (in.CanaryRevisionHash != cs.CanaryRevisionHash && in.CanaryRevisionHash != stableHash)) {
			resetCanaryStatus(cs, in.TargetID, in.CanaryRevisionHash, in.Now)
			// fall through: start a fresh canary toward the new target.
		} else {
			return &Result{Active: true, RequeueAfter: failedRequeue}, nil
		}
	}

	// Done sentinel: a finished canary KEEPS its status with CurrentStep ==
	// len(steps) rather than clearing it. EffectivePartition maps that to the
	// final 100% step (partition 0, all instances on the canary revision); a nil
	// status would instead re-default to step 0's partition and hold instances
	// back on the old revision after completion. No-op unless a new target
	// appears, in which case start a fresh canary.
	if cs != nil && int(cs.CurrentStep) >= len(plan.Steps) {
		if in.RunActive && in.CanaryRevisionHash != "" && (targetChanged || in.CanaryRevisionHash != cs.CanaryRevisionHash) {
			resetCanaryStatus(cs, in.TargetID, in.CanaryRevisionHash, in.Now)
		} else {
			// This return skips the main-path syncPromotedThrough, so converge any
			// promote residue here: a canary can complete while the durable record
			// is still waiting on observed annotation absence, and a done canary
			// evaluates no gates, so the record has nothing left to keep inert.
			syncPromotedThrough(ctx, in, cs)
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
			return &Result{Active: false}, nil
		}
	}

	// Initialize the state machine on first sight of a NEW canary. Don't start
	// one when the component is already fully converged on the target revision —
	// or when the target isn't known yet. Without a run identity, adding a canary
	// to an already-rolled-out ISVC remains a no-op rather than a phantom rollout.
	if cs == nil {
		if in.CanaryRevisionHash == "" || (in.TargetID == "" && readyCanaryCapacity(in) >= in.DesiredReplicas) {
			if in.CanaryRevisionHash != "" && in.DesiredReplicas > 0 {
				setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseStable)
			}
			return &Result{Active: false}, nil
		}
		// A diverged target with no pinned run: hold as-is. The run layer
		// opens (or parks) the run; starting the step machine here would
		// execute an unpinned plan, and the update gates are already holding
		// the forward roll fail-closed.
		if !in.RunActive {
			return &Result{Active: false}, nil
		}
		in.ISVC.Status.Canary = &v1beta1.CanaryStatus{
			TargetID:           in.TargetID,
			CanaryRevisionHash: in.CanaryRevisionHash,
			StableRevisionHash: in.StableRevisionHash,
			CurrentStep:        0,
			StepEnteredTime:    &metav1.Time{Time: in.Now},
		}
		cs = in.ISVC.Status.Canary
	}

	// Backfill a missing stable identity from the observed stable revision (a
	// canary re-armed after a completed one, or a status recorded before the
	// identity was persisted). Once set it is never re-inferred: the live pod
	// set stops naming the pre-canary stable as revisions retarget and drain.
	if cs.StableRevisionHash == "" && in.StableRevisionHash != "" {
		cs.StableRevisionHash = in.StableRevisionHash
	}

	// Finish any promotion whose advance already persisted: remove the applied
	// promote annotation (best-effort) and, once it is gone, clear the durable
	// record so manual promotion re-arms for later steps.
	syncPromotedThrough(ctx, in, cs)
	step := plan.Steps[cs.CurrentStep]

	newCount := resolveStepNewCount(step, in.DesiredReplicas)
	partition := partitionForNewCount(in.DesiredReplicas, newCount)

	// Capacity gate: don't shift traffic until the canary pods are Ready — the
	// primary's own newCount AND every secondary component's canary capacity (PD,
	// so a Ready router can't advance the step while the engine/decoder canary
	// pods behind it are still coming up). If the gate stays unsatisfied past the
	// ready-timeout, escalate to Failed instead of polling Pending forever — the
	// stable revision keeps serving since no traffic has shifted.
	readyCanary := readyCanaryCapacity(in)
	if readyCanary < newCount || !in.SecondaryCapacityReady {
		// Anchor the ready-timeout to the start of THIS capacity wait: re-stamp
		// StepEnteredTime on entering Pending. Anchoring on the untouched step
		// entry would let a long bake (analysis / pause) eat the whole budget and
		// park a healthy canary Failed the moment capacity dips mid-step. Mirrors
		// the serving-entry re-stamps below.
		if in.ISVC.Status.Components[in.Component].RolloutPhase != v1beta1.RolloutPhasePending {
			cs.StepEnteredTime = &metav1.Time{Time: in.Now}
		}
		if capacityGateExpired(cs, resolveReadyTimeout(in, plan), in.Now) {
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseFailed)
			return &Result{Active: true, Partition: partition, RequeueAfter: failedRequeue}, nil
		}
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePending)
		return &Result{Active: true, Partition: partition, RequeueAfter: reconcileRequeue}, nil
	}

	// Pre-step hold (repin clamp): the pinned plan was replaced mid-run and
	// the clamped step would raise exposure, so the traffic raise waits for
	// an explicit promote (value = canary hash). Capacity above already
	// converged toward the clamped step — capacity ahead of traffic is the
	// supported warm-up pattern; exposure is the traffic write below.
	if cs.PreStepHold {
		if in.ISVC.Annotations[constants.RolloutPromoteAnnotation] == cs.CanaryRevisionHash {
			if err := consumeAnnotation(ctx, in.Client, in.ISVC, constants.RolloutPromoteAnnotation); err != nil {
				return nil, err
			}
			cs.PreStepHold = false
		} else {
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePaused)
			return &Result{Active: true, Partition: partition, RequeueAfter: reconcileRequeue}, nil
		}
	}

	// Capacity satisfied: program this step's external traffic weight.
	applyTraffic(in.ISVC, in.Component, in.CanaryRevisionHash, in.StableRevisionHash,
		in.CanaryPairingProtocol, in.StablePairingProtocol, step.Traffic)

	// Final step (TrafficWeight 100): drain the stable revision, then complete.
	if int(cs.CurrentStep) == len(plan.Steps)-1 {
		// Anchor the drain window to the moment 100% traffic actually shifts
		// (capacity is met here — we are past the gate, applyTraffic just ran),
		// not to step entry. On slow capacity the final step is entered well
		// before traffic moves, so measuring the drain from step entry could
		// consume the whole window before cutover. Re-stamp once, on entering
		// Promoting.
		if in.ISVC.Status.Components[in.Component].RolloutPhase != v1beta1.RolloutPhasePromoting {
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
				setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseFailed)
				return &Result{Active: true, Partition: 0, RequeueAfter: failedRequeue}, nil
			case decHold:
				return &Result{Active: true, Partition: 0, RequeueAfter: stepRequeue(step)}, nil
			case decAdvance:
				// gate passed at 100% — fall through to the drain window + completion.
			}
		}
		if drainElapsed(cs, plan, in.Now) {
			// A promote that opens the final gate stays live through the drain
			// window (the gate re-evaluates every pass until completion), so it is
			// consumed at the completion edge. Consumption failure retries
			// completion rather than completing with a live promote.
			if err := consumeAnnotation(ctx, in.Client, in.ISVC, constants.RolloutPromoteAnnotation); err != nil {
				return nil, err
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
			// The promote annotation is durably consumed above and a done canary
			// evaluates no gates, so the durable promote record is spent: clear it
			// with the same status flush rather than leaving it as residue.
			cs.PromotedThrough = ""
			return &Result{Active: true, Complete: true, Partition: 0}, nil
		}
		return &Result{Active: true, Partition: 0, RequeueAfter: reconcileRequeue}, nil
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
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseFailed)
			return &Result{Active: true, Partition: partition, RequeueAfter: failedRequeue}, nil
		case decHold:
			setPhase(in.ISVC, in.Component, v1beta1.RolloutPhasePaused)
			return &Result{Active: true, Partition: partition, RequeueAfter: stepRequeue(step)}, nil
		case decAdvance:
			// fall through to advance.
		}
	}
	advanceStep(in)
	return &Result{Active: true, Stepped: true, Partition: partition, RequeueAfter: reconcileRequeue}, nil
}

// pausedResult reports a globally-paused canary's state without mutating it.
// The rollback signal echoes persisted status so the controller neither arms
// nor clears the IR's RollbackToRevision while paused, and the partition
// echoes the current step so the staged split stays where it is.
func pausedResult(cs *v1beta1.CanaryStatus, plan *v1beta1.GroupCanary, desiredReplicas int32) *Result {
	// Not started, or already done: inactive. A pause must not initialize the
	// state machine (initialization stamps the step timers) — a canary toward a
	// target that appeared while paused starts once the pause clears.
	if cs == nil || int(cs.CurrentStep) >= len(plan.Steps) {
		return &Result{Active: false}
	}
	if cs.RolledBackRevisionHash != "" {
		return &Result{Active: true, RolledBack: true, RequeueAfter: reconcileRequeue}
	}
	step := plan.Steps[cs.CurrentStep]
	partition := partitionForNewCount(desiredReplicas, resolveStepNewCount(step, desiredReplicas))
	return &Result{Active: true, Partition: partition, RequeueAfter: reconcileRequeue}
}

// resetCanaryStatus re-arms the state machine at step 0 toward a new target,
// dropping ALL prior-canary state (analysis failure budget, sampling
// timestamps, metric results, rollback hold, observed traffic) — carrying any
// of it over would let a stale failure budget roll back the new revision.
// StableRevisionHash is the one field PRESERVED: a mid-canary retarget does
// not change which revision was stable when the rollout began, and dropping
// it would make a later rollback target the partially-rolled intermediate.
func resetCanaryStatus(cs *v1beta1.CanaryStatus, targetID, hash string, now time.Time) {
	*cs = v1beta1.CanaryStatus{
		TargetID:           targetID,
		CanaryRevisionHash: hash,
		StableRevisionHash: cs.StableRevisionHash,
		StepEnteredTime:    &metav1.Time{Time: now},
	}
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
	if rollingBack {
		setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseRollingBack)
		return &Result{Active: true, RolledBack: true, RequeueAfter: reconcileRequeue}
	}
	setPhase(in.ISVC, in.Component, v1beta1.RolloutPhaseRolledBack)
	return &Result{Active: true, RolledBack: true}
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
func effectiveCanaryPlan(isvc *v1beta1.InferenceService) *v1beta1.GroupCanary {
	g := v1beta1.EffectiveCanaryGroup(isvc)
	if g == nil {
		return nil
	}
	return g.Canary
}

// capacityGateExpired reports whether the current step has waited on capacity
// past the timeout, measured from StepEnteredTime — re-stamped when the wait
// begins (on entering Pending), so the budget covers only the capacity wait. A
// nil status / missing step-entered time / non-positive timeout is never
// treated as expired, so a transient nil cannot wrongly fail a rollout.
func capacityGateExpired(cs *v1beta1.CanaryStatus, timeout time.Duration, now time.Time) bool {
	if cs == nil || cs.StepEnteredTime == nil || timeout <= 0 {
		return false
	}
	return !now.Before(cs.StepEnteredTime.Time.Add(timeout))
}
