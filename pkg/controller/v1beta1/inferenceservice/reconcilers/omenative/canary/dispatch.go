package canary

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils"
)

// DispatchDeps is the controller-side input to the canary executor, mirroring
// coordination.ReconcileInputs. The controller calls Dispatch once per reconcile
// when a canary group is set (spec.rollout.groups[].canary); coordination runs in
// the same pass on the non-canary Components.
type DispatchDeps struct {
	Client   client.Client
	Reader   client.Reader
	Recorder record.EventRecorder
	ISVC     *v1beta1.InferenceService
	// Policies is the set of RolloutPolicy objects the run layer observed
	// for this pass; the effective view resolves the spec's references
	// through it outside a pinned run.
	Policies rollout.Policies
	// Now is the wall-clock used for LastTransitionTime; callers may
	// override for tests. Zero defaults to time.Now().
	// Already an injected-time seam; kept as time.Time (single snapshot per pass).
	Now time.Time
	// Sampler is the controller's shared async analysis sampler (built once at
	// startup). nil disables metric-gated steps (they hold), which keeps a
	// controller without a configured sampler from advancing analysis steps ungated.
	Sampler *Sampler
	// StepSampler, when set, is what the executor reads the analysis gate
	// from in place of Sampler: a source that answers in the pass with
	// results dated by the caller's clock, as the replay driver scripts
	// it. Production leaves it nil.
	StepSampler stepSampler
	// BundledPrometheusAddress and QueryTimeout come from the canaryAnalysis operator
	// config (fetched per-reconcile by the controller), threaded to the sampler.
	BundledPrometheusAddress string
	QueryTimeout             time.Duration
	// DefaultReadyTimeout is the operator-configured capacity-gate bound
	// (rollout.defaultReadyTimeout); zero means no configured default.
	DefaultReadyTimeout time.Duration
	// Requeue and ParkedRequeue are the operator-configured cadences
	// (rollout.canaryRequeue, rollout.canaryParkedRequeue) at which an
	// in-progress step and a Failed park are re-checked. Zero means
	// unconfigured: the dispatch asks for the controller's rate-limited
	// requeue instead.
	Requeue       time.Duration
	ParkedRequeue time.Duration
	// MetricProviders are this cluster's logical provider bindings; a canary
	// source naming a providerRef resolves through them. Run open already
	// parks an unbound name, so an unbound name HERE is a mid-run config
	// regression — it degrades to a source-less (inconclusive) sample rather
	// than un-gating anything.
	MetricProviders map[string]controllerconfig.MetricProviderBinding
	// DefaultProvider optionally names the binding used when the CR declares
	// neither providerRef nor serverAddress (canaryAnalysis.defaultProvider);
	// empty keeps the BundledPrometheusAddress fallback.
	DefaultProvider string
	// ComponentRunnerPorts holds each Component's effective serving ports.
	// The per-revision routing Service publishes the port resolved from this set
	// so it targets the port the pods actually listen on.
	ComponentRunnerPorts map[v1beta1.ComponentType][]corev1.ContainerPort
	// ComponentServicePortAppProtocols holds each Component's declared
	// appProtocol per Service port name, from the merged spec its stable
	// Service uses. A Component missing from the map publishes none.
	ComponentServicePortAppProtocols map[v1beta1.ComponentType]map[string]string
	// Group is the canary group this dispatch drives. One group owns one canary
	// unit -- the router, or engine+decoder -- so concurrent dispatches touch
	// disjoint Components and cannot contend for a revision or step counter.
	Group *v1beta1.RolloutGroup
	// Base is the executor-owned state the pass read, as the status flush
	// guards it. The dispatch reads the live record through Reader before
	// it writes a replica's revert target and writes none when the pass's
	// copy is stale. nil guards nothing.
	Base *Base
}

// Outcome is what one canary dispatch hands the controller.
type Outcome struct {
	// RequeueAfter is the configured cadence at which to re-run. Requeue asks
	// for the controller's rate-limited requeue when no cadence is configured
	// for the wait the executor is in.
	RequeueAfter time.Duration
	Requeue      bool
	// Consume lists the operator annotations this pass applied, to be removed
	// once the pass's status write has landed.
	Consume []string
}

// wait is the outcome of a pass that must come back before deciding.
func (d DispatchDeps) wait() Outcome {
	if d.Requeue > 0 {
		return Outcome{RequeueAfter: d.Requeue}
	}
	return Outcome{Requeue: true}
}

// Dispatch runs the canary step machine for an InferenceService, mutating
// isvc.Status in-memory (phase, traffic, step) and returning a requeue hint. The
// step machine is ISVC-level; it is driven through the externally-routed
// (primary) Component — the router for PD, otherwise the single Component —
// and every member of the group publishes the step's traffic weight on its own
// per-revision Services. Per-component pod capacity is applied separately by
// the controller via EffectivePartition.
//
// No-op (requeue 0) when no canary plan is set.
func Dispatch(ctx context.Context, d DispatchDeps) (Outcome, error) {
	if d.Group == nil || d.Group.Canary == nil {
		return Outcome{}, nil
	}
	primary := primaryComponentOf(d.Group)
	if primary == "" {
		return Outcome{}, nil
	}
	// Observe per-revision pods for EVERY configured component (not just the
	// primary): the canary stages capacity on all of them. perRev supplies
	// revision presence and routing Service inputs; readyRev is the rollback,
	// re-arm and cutover view of live pods. Capacity itself is each replica's
	// own account of its Instances (observeCanaryRevisions), where a multi-pod
	// gang is one Instance and rotation is recorded.
	// d.Reader is the live API reader (no Pod field index) — useIndex=false skips
	// the doomed MatchingFields probe and goes straight to the label-selector List.
	perRev, readyRev, routingRev, observedPods, err := coordination.ObservePerRevisionPods(ctx, d.Reader, d.ISVC, configuredComponents(d.Group), false)
	if err != nil {
		return Outcome{}, err
	}
	pods := perRev[primary]

	revisions, err := observeCanaryRevisions(ctx, d.Reader, d.ISVC, primary)
	if err != nil {
		return Outcome{}, err
	}
	canaryHash := revisions.targetHash

	now := d.Now
	if now.IsZero() {
		now = time.Now()
	}

	// The secondaries are observed before the Services are kept, so the
	// sweep below and the rollback signal both work from the replica each
	// member's observation read: a pass reads a member's replica once.
	secondaryReady, secondaryFresh, secondaryObserved, secondaryWait, err := secondaryCapacityReady(ctx, d.Reader, d.ISVC, perRev, observedPods, primary, d.Group, now)
	if err != nil {
		return Outcome{}, err
	}

	// Ensure the per-revision Services the canary references, for EVERY Component
	// in the canary group — not just the primary. The downstream weighted-route
	// consumer references these by name, and coordination skips canary-owned
	// Components, so the canary engine is their sole producer; ensuring only the
	// primary's would leave a secondary's per-revision Service dangling. Mirrors
	// coordination's per-Component ensure+GC loop.
	for _, comp := range configuredComponents(d.Group) {
		// Ensure runs EVERY reconcile — it is the self-heal path. A
		// per-revision Service deleted out-of-band must be recreated even
		// when the live revision-hash set is unchanged; EnsurePerRevisionServices
		// uses CreateOrUpdate, whose Gets are cache-served and cheap, and
		// recreates a missing Service. Gating ensure on revision-set
		// convergence would strand the weighted-route consumer on a
		// dead backend. The revisions are walked in sorted order so a pass
		// is repeatable.
		for _, hash := range coordination.SortedRevisionHashes(perRev[comp]) {
			if hash == "" {
				continue
			}
			routing := routingRev[comp][hash]
			if _, err := coordination.EnsurePerRevisionServices(ctx, d.Client, d.ISVC, comp, hash, routing, d.ComponentRunnerPorts[comp], d.ComponentServicePortAppProtocols[comp]); err != nil {
				return Outcome{}, err
			}
		}
		// The sweep of unused per-revision Services runs unconditionally,
		// through the rule coordination prunes by: a revision keeps its
		// Services while a pod runs it or the member's replica rolls toward
		// it. A canary whose pods never came up, or whose pods are gone
		// while its revision is still the replica's target, keeps them; the
		// replica controller creates that revision's Services ahead of its
		// pods, and deleting them here would have the two controllers
		// create and delete them pass after pass. Gating the sweep on the
		// live revision-hash set matching Status.Components.<c>.Traffic is
		// unsound: perRev counts TOTAL pods while Traffic is written from
		// READY pods, so the pass on which a drained pod disappears is
		// exactly the pass on which the two sets agree, and skipping it
		// strands that revision's Services.
		if err := sweepMemberServices(ctx, d, comp, memberObservation(comp, primary, revisions, secondaryObserved), perRev[comp]); err != nil {
			return Outcome{}, err
		}
	}

	// Revision names and counters from an older IR generation describe a
	// different desired workload. Wait for one internally consistent snapshot.
	if !revisions.statusFresh {
		return d.wait(), nil
	}

	// A live non-current revision can appear before the IR status publishes its
	// new target. Wait for that revision pair so their roles cannot be inverted.
	// A run that records the unit at its stable revision starts no steps, so
	// its pass is not waited for: the pods of the other revision are Instances
	// rolling back to the revision the run names, and the split follows them.
	if revisions.fromIR && revisions.currentHash != "" && revisions.currentHash == canaryHash &&
		otherRevision(pods, canaryHash) != "" &&
		(rollout.CanaryStatusFor(&d.ISVC.Status, primary) == nil || rollout.CanaryStatusFor(&d.ISVC.Status, primary).StableRevisionHash == "") &&
		(!v1beta1.RolloutRunActive(d.ISVC) || groupRetargeted(d.ISVC, d.Group, primary)) {
		return d.wait(), nil
	}

	if !secondaryFresh {
		return d.wait(), nil
	}
	// The IR revision pair authoritatively assigns current=stable and
	// update=canary while the two revisions are distinct.
	stableHash := activeRunStableRevisionHash(d.ISVC, primary)
	if stableHash == "" {
		if revisions.currentHash != canaryHash {
			stableHash = revisions.currentHash
		}
		if sc := rollout.CanaryStatusFor(&d.ISVC.Status, primary); sc != nil && sc.StableRevisionHash != "" {
			// A distinct authoritative current revision can repair an active canary
			// whose persisted stable and target identities are the same.
			if revisions.fromIR && revisions.currentHash != "" && revisions.currentHash != canaryHash && sc.StableRevisionHash == canaryHash {
				stableHash = revisions.currentHash
			} else {
				stableHash = sc.StableRevisionHash
			}
		}
	}
	// No authority names a stable revision when the plan and the new target
	// land in one write before the IR promotes its current revision: the
	// staged partition then blocks that promotion for as long as the canary
	// stays unarmed. The pods still serving the non-target revision are the
	// stable identity the step machine shifts traffic away from.
	if stableHash == "" && revisions.fromIR {
		stableHash = otherRevision(pods, canaryHash)
	}
	// Resolve the two revisions' pairing protocols through the cached client:
	// a ControllerRevision's protocol is immutable from create, so a cache
	// read can only lag into "" (pairs with anything) for a just-minted CR,
	// which the meaningful-diff traffic write corrects on the next pass.
	primaryPrefix := irprojector.RoleReplicaPrefix(d.ISVC, primary)
	canaryProtocol, _, err := coordination.PairingProtocolForRevision(ctx, d.Client, d.ISVC.Namespace, primaryPrefix, primary, canaryHash)
	if err != nil {
		return Outcome{}, err
	}
	stableProtocol, _, err := coordination.PairingProtocolForRevision(ctx, d.Client, d.ISVC.Namespace, primaryPrefix, primary, stableHash)
	if err != nil {
		return Outcome{}, err
	}
	groupPods, groupStable := groupRollbackInputs(d.ISVC, d.Group, primary, perRev)
	secondaries, err := secondaryRevisions(ctx, d.Client, d.ISVC, primary, d.Group, secondaryObserved, groupStable, readyRev)
	if err != nil {
		return Outcome{}, err
	}
	// The names a stop at the cutover reports are built only on the pass
	// that stops, so the roll's other passes pay nothing for them.
	offTarget := func() map[v1beta1.ComponentType][]string {
		return instancesOffTarget(d.ISVC, d.Group, primary, revisions, secondaryObserved, observedPods)
	}
	in := ReconcileInputs{
		Client:                    d.Client,
		Reader:                    d.Reader,
		Recorder:                  d.Recorder,
		ISVC:                      d.ISVC,
		Policies:                  d.Policies,
		Group:                     d.Group,
		Component:                 primary,
		CanaryRevisionHash:        canaryHash,
		StableRevisionHash:        stableHash,
		CanaryPairingProtocol:     canaryProtocol,
		StablePairingProtocol:     stableProtocol,
		ReadyCanaryInstances:      revisions.servingTargetInstances(),
		ReadyStableInstances:      revisions.servingInstances(stableHash),
		CapacityWindowWait:        soonestWait(revisions.windowWait(observedPods[primary], now), secondaryWait),
		DesiredReplicas:           DesiredReplicas(revisions.replicas, componentExtension(d.ISVC, primary)),
		PerRevisionPods:           readyRev[primary],
		GroupTotalPerRevisionPods: groupPods,
		GroupStableRevisionHashes: groupStable,
		GroupReadyPerRevisionPods: readyRev,
		Crash:                     canaryCrash(d.ISVC, d.Group, primary, revisions, secondaryObserved, observedPods, now),
		CanaryRestartedAt:         canaryRestartedAt(d.ISVC, d.Group, primary, revisions, secondaryObserved, observedPods),
		InstancesOffTarget:        offTarget,
		SecondaryCapacityReady:    secondaryReady,
		Secondaries:               secondaries,
		Now:                       now,
		Sampler:                   d.sampler(),
		Prometheus:                resolveCanarySource(d.Group, d.MetricProviders, d.DefaultProvider),
		BundledPrometheusAddress:  d.BundledPrometheusAddress,
		QueryTimeout:              d.QueryTimeout,
		RunActive:                 v1beta1.RolloutRunActive(d.ISVC),
		TargetID:                  activeCanaryTargetID(d.ISVC, d.Group),
		DefaultReadyTimeout:       d.DefaultReadyTimeout,
		Requeue:                   d.Requeue,
		ParkedRequeue:             d.ParkedRequeue,
	}
	res, err := Reconcile(ctx, in)
	if err != nil {
		return Outcome{}, err
	}

	// With no roll active on the unit the rolled-out records are not the
	// memory of a traffic shift: each member's record follows the revision
	// its InferenceReplica has settled on. A rejection the unit rests on is
	// no roll either: the revert is complete, so a member it wrote nothing
	// for, one with no stable revision, is recorded on the revision its
	// replica settled on rather than left unrecorded until the next push.
	if !res.Active || restsRolledBack(d.ISVC, d.Group, primary) {
		recordSettledRevisions(d.ISVC, d.Group, primary, revisions, secondaryObserved, perRev, readyRev)
	}

	// Rollback signal: when the executor is rolling back, point EVERY group
	// Component's IR at its own stable ControllerRevision so it rolls every
	// Instance back to stable; otherwise clear the signal. (Reconcile shifted
	// traffic + set the phase; this drives the actual pod revert.) The canary
	// stages capacity on all group Components, so a PD rollback must revert all of
	// them — signaling only the primary leaves the engine/decoder serving the
	// rejected revision behind the rolled-back router.
	// A replica created pointed at the unit's stable side keeps that target
	// until every Instance of it has run a revision, so the step's partition
	// has a revision to hold the Instances on before the roll stages its share.
	// The signal is written before the status flush, so it follows the live
	// record alone: a pass whose copy of the record is stale decides from a
	// record a later pass has moved, the flush keeps the live record over its
	// decisions, and the revert targets are left as they stand for the pass
	// that reads the live record.
	current, err := d.recordCurrent(ctx)
	if err != nil {
		return Outcome{}, err
	}
	if !current {
		log.FromContext(ctx).V(1).Info("canary record read by this pass is stale; the replicas' revert targets are left as they stand",
			"InferenceService", d.ISVC.Name, "unit", primary)
		recordDispatch(d.Recorder, d.ISVC, primary, res)
		return Outcome{RequeueAfter: res.RequeueAfter, Requeue: res.Requeue, Consume: res.Consume}, nil
	}
	holdsSide := unitKeepsStableSide(d.ISVC, d.Group, primary)
	missingStable := false
	for _, comp := range configuredComponents(d.Group) {
		persistedStable := componentStableRevisionHash(d.ISVC, comp, primary)
		observed := memberObservation(comp, primary, revisions, secondaryObserved)
		keep := holdsSide && anInstanceHasNotRun(observed.rows)
		missing, err := reconcileRollbackSignal(ctx, d.Client, d.Reader, d.ISVC, comp, observed.stored, persistedStable, res.RolledBack, keep)
		if err != nil {
			return Outcome{}, err
		}
		missingStable = missingStable || missing
	}
	if missingStable {
		// A rollback with no stable revision to return to cannot be carried
		// out. Park it, loudly, rather than report a revert in progress that
		// never completes; a new target re-arms the unit. The request is
		// spent with the park: the rejected hash it recorded keeps a copy
		// still visible after the flush inert, so it is handed back for
		// removal once the flush has carried the park.
		if cs := rollout.CanaryStatusFor(&d.ISVC.Status, primary); cs != nil && cs.Failed == nil {
			emit(d.Recorder, d.ISVC, corev1.EventTypeWarning, EventReasonCanaryStableRevisionMissing,
				"%s cannot roll back: no ControllerRevision is retained for its stable revision; parked Failed until a new target appears", primary)
			parkFailed(in, cs, v1beta1.CanaryFailureStableRevisionMissing)
			if rollbackRequestSpendable(d.ISVC, d.Policies, primary, cs) {
				res.Consume = append(res.Consume, constants.RolloutRollbackAnnotation)
			}
		}
	}

	recordDispatch(d.Recorder, d.ISVC, primary, res)
	return Outcome{RequeueAfter: res.RequeueAfter, Requeue: res.Requeue, Consume: res.Consume}, nil
}

// recordCurrent reports whether the canary record this pass read is still
// the live one, read through the live reader. A service that is gone has no
// live record to follow, so nothing is written for it either.
func (d DispatchDeps) recordCurrent(ctx context.Context) (bool, error) {
	if d.Base == nil || d.Reader == nil {
		return true, nil
	}
	live := &v1beta1.InferenceService{}
	if err := d.Reader.Get(ctx, client.ObjectKeyFromObject(d.ISVC), live); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read the live record before the revert targets: %w", err)
	}
	return d.Base.Current(&live.Status), nil
}

// sweepMemberServices is the sweep of one member's unused per-revision
// Services, worked from the replica the member's observation read; a
// member the pass holds no observation of is read by the sweep itself.
func sweepMemberServices(ctx context.Context, d DispatchDeps, comp v1beta1.ComponentType, observed observedCanaryRevisions, liveHashes map[string]int32) error {
	if observed.stored == nil {
		return coordination.SweepPerRevisionServices(ctx, d.Client, d.Reader, d.ISVC, comp, liveHashes)
	}
	return coordination.SweepPerRevisionServicesOfReplica(ctx, d.Client, d.Reader, d.ISVC, comp, observed.stored, liveHashes)
}

// reconcileRollbackSignal sets (rolledBack) or clears one Component IR's
// Spec.Pacing.RollbackToRevision. When rolling back it points at the STABLE
// pre-canary ControllerRevision so the IR reconciler overrides its desired
// template with that revision and rolls every Instance back to it. stableHash
// is the persisted stable identity for this Component ("" when none is
// recorded). A standing hold writes its target once: a target already on
// the replica that names a retained ControllerRevision of the stable
// revision is read and left, with no history lookup and no write. The
// history is consulted only for a replica that carries no target, one
// naming another revision, or one whose ControllerRevision is gone.
//
// The target is resolved from the IR's ControllerRevision history, NOT from
// ir.Status.CurrentRevision: CurrentRevision is promoted to the canary revision
// as soon as the Component fully rolls forward onto it (every Instance Ready on
// target). The primary always reaches that state — the router (the PD primary)
// is typically a 1-replica proxy whose every step rounds the new-pod count up to
// its full replica count (partition 0), so it rolls fully onto the canary at the
// FIRST step. Echoing CurrentRevision back would then tell the IR to "roll back"
// to the canary revision it is already on — a no-op — so the canary pods never
// drain and the rollback wedges in RollingBack forever.
//
// It reports missingStable when a rollback names a stable hash whose
// ControllerRevision is not retained: there is nothing to point the
// workload at, and the caller parks the canary rather than leave the revert
// reported as in progress.
//
// keepStableSide keeps a target that names the stable revision on a replica
// no rollback holds: the projection created the replica pointed at the
// stable side its unit keeps, and an Instance of it has still to run a
// revision, so the target stays until the fleet has a side of its own.
//
// stored is the replica as the pass read it, and the object a write
// carries; a member the pass holds no observation of is read here. The
// write is guarded by the replica's resourceVersion, and the pass read the
// replica before it listed the pods and ran the steps: a replica another
// writer moved in between answers the write with a conflict, and the signal
// then decides once more from a fresh read, as it does for an unobserved
// member. A conflict on that read's write is the caller's.
func reconcileRollbackSignal(ctx context.Context, c client.Client, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, stored *v1beta1.InferenceReplica, stableHash string, rolledBack, keepStableSide bool) (missingStable bool, err error) {
	if c == nil || reads == nil {
		return false, nil
	}
	missingStable, err = writeRollbackSignal(ctx, c, reads, isvc, comp, stored, stableHash, rolledBack, keepStableSide)
	if stored == nil || !apierrors.IsConflict(err) {
		return missingStable, err
	}
	return writeRollbackSignal(ctx, c, reads, isvc, comp, nil, stableHash, rolledBack, keepStableSide)
}

// writeRollbackSignal is one decision and write of the rollback signal on
// stored, or on a fresh live read of the member when stored is nil.
func writeRollbackSignal(ctx context.Context, c client.Client, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, stored *v1beta1.InferenceReplica, stableHash string, rolledBack, keepStableSide bool) (missingStable bool, err error) {
	ir := stored
	if ir == nil {
		ir = &v1beta1.InferenceReplica{}
		key := irprojector.RoleReplicaKey(isvc, comp)
		if err := reads.Get(ctx, key, ir); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
	}
	var want *string
	if rolledBack {
		// An unknown stable identity must not clear a rollback signal that may
		// already carry the correct target, or guess from revision ordering.
		if stableHash == "" {
			return false, nil
		}
		retained, err := revertTargetRetained(ctx, c, ir, stableHash)
		if err != nil {
			return false, err
		}
		if retained {
			return false, nil
		}
		stable, err := stableRevisionName(ctx, reads, isvc, comp, ir, stableHash)
		if err != nil {
			return false, err
		}
		// Only signal a real stable target. An empty result (the stable CR was
		// GC'd or the replica does not control it, or only the canary revision
		// exists) must NOT fall back to the canary revision — that would be a
		// no-op roll. With no controlled match the pin is simply not written.
		if stable == "" {
			return true, nil
		}
		want = &stable
	} else if keepStableSide && revertTargetNamesStable(ir, stableHash) {
		return false, nil
	}
	var cur *string
	if ir.Spec.Pacing != nil {
		cur = ir.Spec.Pacing.RollbackToRevision
	}
	if utils.PtrStrEqual(cur, want) {
		return false, nil
	}
	if ir.Spec.Pacing == nil {
		ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	ir.Spec.Pacing.RollbackToRevision = want
	return false, c.Update(ctx, ir)
}

// revertTargetRetained reports whether the replica's revert target already
// names a ControllerRevision of the stable revision that is retained and
// that the replica controls. The ControllerRevision is read through the
// cache: it is immutable from create, so the cache can only lag its loss,
// which the next pass then reads from the history as the live reader does.
func revertTargetRetained(ctx context.Context, c client.Client, ir *v1beta1.InferenceReplica, stableHash string) (bool, error) {
	if !revertTargetNamesStable(ir, stableHash) {
		return false, nil
	}
	name := *ir.Spec.Pacing.RollbackToRevision
	cr := &appsv1.ControllerRevision{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ir.Namespace, Name: name}, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return metav1.IsControlledBy(cr, ir), nil
}

// revertTargetNamesStable reports whether the replica's revert target names
// a ControllerRevision of the stable revision.
func revertTargetNamesStable(ir *v1beta1.InferenceReplica, stableHash string) bool {
	if stableHash == "" || ir.Spec.Pacing == nil || ir.Spec.Pacing.RollbackToRevision == nil {
		return false
	}
	return query.RevisionFromName(*ir.Spec.Pacing.RollbackToRevision).Same(query.RevisionFromHash(stableHash))
}

// unitKeepsStableSide reports whether the unit keeps a stable side as this
// pass left it, read from the primary's record and rollout phase under the
// group's step count: a rollback, a step short of the last, or a stop with a
// stable revision.
func unitKeepsStableSide(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType) bool {
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	return cs != nil && holdsStableSide(cs, isvc.Status.Components[primary].RolloutPhase, len(g.Canary.Steps))
}

// memberObservation is a member's observation: the primary's own, else the
// secondary's; empty for a member no replica was observed for.
func memberObservation(comp, primary v1beta1.ComponentType, primaryObserved observedCanaryRevisions, secondaryObserved map[v1beta1.ComponentType]observedCanaryRevisions) observedCanaryRevisions {
	if comp == primary {
		return primaryObserved
	}
	return secondaryObserved[comp]
}

// anInstanceHasNotRun reports whether some row of a replica records no
// revision run: an Instance still being built for the first time, or one
// whose first build failed and awaits its retry, which the create pass
// opens as a fresh start. Neither has a revision of its own for a partition
// to hold it on, so the replica's target decides what they are built on.
func anInstanceHasNotRun(rows []v1beta1.OMENativeInstanceStatus) bool {
	for i := range rows {
		if rows[i].RunningRevision == "" {
			return true
		}
	}
	return false
}

// stableRevisionName resolves the ControllerRevision name of an exact stable
// revision hash. Rollback safety requires an explicit identity: revision
// ordering cannot distinguish a promoted revision from a newer transient one.
//
// Reading the history is what makes the rollback target survive the canary's
// forward roll — once every Instance is on the canary revision, neither the IR
// status nor the live pod set still names the stable revision, but its
// ControllerRevision is retained.
//
// Only a revision of the replica's history qualifies: one the replica
// controls, or one its service owns, which a replica projected again after
// a deletion reads as its own. The labels also match revisions another
// object controls, and the replica refuses a rollback target outside its
// history.
func stableRevisionName(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, ir *v1beta1.InferenceReplica, stableHash string) (string, error) {
	if stableHash == "" {
		return "", nil
	}
	history, err := memberRevisionHistory(ctx, reads, isvc, comp)
	if err != nil {
		return "", err
	}
	want := query.RevisionFromHash(stableHash)
	for i := range history {
		cr := &history[i]
		if !query.RevisionOf(cr).Same(want) {
			continue
		}
		if revision.RetainedBy(cr, ir.UID, irprojector.HistoryScope(ir)) {
			return cr.Name, nil
		}
		var controllerKind, controllerName, controllerUID string
		if ref := metav1.GetControllerOf(cr); ref != nil {
			controllerKind, controllerName, controllerUID = ref.Kind, ref.Name, string(ref.UID)
		}
		log.FromContext(ctx).V(1).Info("Revision matches the stable hash but belongs to neither the replica nor its service; not used as the rollback target",
			"revision", cr.Name, "controllerKind", controllerKind, "controllerName", controllerName,
			"controllerUID", controllerUID, "inferenceReplica", ir.Name, "inferenceReplicaUID", string(ir.UID))
	}
	return "", nil
}

// componentStableRevisionHash resolves one member's stable revision: the
// run-scoped identity first (the active run's target, then the rolled-back
// run's record), then the primary's canary record, then the Component's
// LatestRolledoutRevision. That last field is the durable per-Component
// identity every OMENative writer keeps, coordination for its Components and
// the canary executor at each completed promotion and rollback for its
// members, so a secondary resolves even when no run records it.
// restsRolledBack reports a unit resting on a rejection: its revert is
// complete and nothing of it is in flight, so no roll is active on the unit
// until a new target starts it again or a resume clears the rejection.
func restsRolledBack(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType) bool {
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	return rollout.StateOf(cs, isvc.Status.Components[primary].RolloutPhase, len(g.Canary.Steps)) == rollout.CanaryStateRolledBack
}

func componentStableRevisionHash(isvc *v1beta1.InferenceService, comp, primary v1beta1.ComponentType) string {
	if stable := activeRunStableRevisionHash(isvc, comp); stable != "" {
		return stable
	}
	if isvc.Status.Rollout != nil {
		if last := isvc.Status.Rollout.LastRun; last != nil && last.Outcome == v1beta1.RolloutRunRolledBack {
			for _, target := range last.TargetRevisions {
				if target.Component == comp && target.StableRevision != "" {
					return target.StableRevision
				}
			}
		}
	}
	if cs := rollout.CanaryStatusFor(&isvc.Status, primary); comp == primary && cs != nil && cs.StableRevisionHash != "" {
		return cs.StableRevisionHash
	}
	if status, ok := isvc.Status.Components[comp]; ok {
		return query.RevisionFromName(status.LatestRolledoutRevision).Hash()
	}
	return ""
}

func activeRunStableRevisionHash(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) string {
	if target, ok := activeRunTarget(isvc, comp); ok {
		return target.StableRevision
	}
	return ""
}

// groupRollbackInputs is the whole-unit view the rollback completion check
// reads: every configured member's pods per revision, ready or not, and its
// persisted stable revision, primary included.
func groupRollbackInputs(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType, totalRev map[v1beta1.ComponentType]map[string]int32) (map[v1beta1.ComponentType]map[string]int32, map[v1beta1.ComponentType]string) {
	members := configuredComponents(g)
	pods := make(map[v1beta1.ComponentType]map[string]int32, len(members))
	stable := make(map[v1beta1.ComponentType]string, len(members))
	for _, comp := range members {
		pods[comp] = totalRev[comp]
		stable[comp] = componentStableRevisionHash(isvc, comp, primary)
	}
	return pods, stable
}

// secondaryRevisions resolves each non-primary member's revision pair for the
// traffic writer: the target the capacity gate observed on its IR, so a
// member's targets name the revision whose capacity was verified, and the
// persisted stable revision the rollback check uses, so a rollback returns
// traffic to the revision the member reverts to. The capacity on the target
// is counted as the capacity gate counts it, the replica's own serving
// Instances, so the member's latest ready revision is published on the same
// evidence the gate reads, and the Instance count the replica is scaled to
// is what that capacity must reach for the cutover to be complete. The
// protocols come from the two ControllerRevisions through the cached client,
// as the primary's do.
func secondaryRevisions(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, primary v1beta1.ComponentType, g *v1beta1.RolloutGroup, observed map[v1beta1.ComponentType]observedCanaryRevisions, stable map[v1beta1.ComponentType]string, readyPerRev map[v1beta1.ComponentType]map[string]int32) (map[v1beta1.ComponentType]MemberRevisions, error) {
	out := map[v1beta1.ComponentType]MemberRevisions{}
	for _, comp := range configuredComponents(g) {
		if comp == primary {
			continue
		}
		target := observed[comp].targetHash
		m := MemberRevisions{
			CanaryRevisionHash:   target,
			StableRevisionHash:   stable[comp],
			ReadyCanaryCapacity:  canaryCapacity(observed[comp].servingTargetInstances(), readyPerRev[comp][target]),
			DesiredReplicas:      DesiredReplicas(observed[comp].replicas, componentExtension(isvc, comp)),
			ReadyStableInstances: observed[comp].servingInstances(stable[comp]),
		}
		prefix := irprojector.RoleReplicaPrefix(isvc, comp)
		var err error
		if m.CanaryPairingProtocol, _, err = coordination.PairingProtocolForRevision(ctx, reads, isvc.Namespace, prefix, comp, m.CanaryRevisionHash); err != nil {
			return nil, err
		}
		if m.StablePairingProtocol, _, err = coordination.PairingProtocolForRevision(ctx, reads, isvc.Namespace, prefix, comp, m.StableRevisionHash); err != nil {
			return nil, err
		}
		out[comp] = m
	}
	return out, nil
}

// sampler is the source the executor reads the analysis gate from: the
// step sampler a caller set, else the shared async sampler behind its nil
// guard.
func (d DispatchDeps) sampler() stepSampler {
	if d.StepSampler != nil {
		return d.StepSampler
	}
	return samplerOrNil(d.Sampler)
}

// samplerOrNil converts a possibly-nil *Sampler into the stepSampler interface,
// returning a true nil interface (not a non-nil interface wrapping a nil pointer)
// so the executor's `Sampler == nil` guard works and never dereferences nil.
func samplerOrNil(s *Sampler) stepSampler {
	if s == nil {
		return nil
	}
	return s
}

// resolveCanarySource resolves the canary group's shared metrics source into
// the concrete connection the sampler uses. A providerRef resolves through
// the cluster's bindings (binding headers under the CR's, CR key winning); no
// declared source falls back to the operator's default provider when one is
// named, else to nil (the sampler's BundledPrometheusAddress fallback). An
// unbound providerRef mid-run yields an empty source — inconclusive samples,
// never an un-gated step.
func resolveCanarySource(g *v1beta1.RolloutGroup, providers map[string]controllerconfig.MetricProviderBinding, defaultProvider string) *v1beta1.AnalysisPrometheus {
	if g == nil || g.Canary == nil {
		return nil
	}
	p := g.Canary.Prometheus
	if p == nil || (p.ProviderRef == nil && p.ServerAddress == "") {
		if defaultProvider != "" {
			if b, ok := providers[defaultProvider]; ok {
				return bindingSource(b, p)
			}
		}
		return p
	}
	if p.ProviderRef != nil {
		if b, ok := providers[p.ProviderRef.Name]; ok {
			return bindingSource(b, p)
		}
		return &v1beta1.AnalysisPrometheus{}
	}
	return p
}

// bindingSource materializes a provider binding as the sampler's connection,
// overlaying the CR's headers (CR key wins) and honoring an inline authRef
// over the binding's secret.
func bindingSource(b controllerconfig.MetricProviderBinding, overlay *v1beta1.AnalysisPrometheus) *v1beta1.AnalysisPrometheus {
	out := &v1beta1.AnalysisPrometheus{ServerAddress: b.ServerAddress, AuthRef: b.AuthSecretRef}
	if len(b.Headers) > 0 || (overlay != nil && len(overlay.Headers) > 0) {
		out.Headers = map[string]string{}
		for k, v := range b.Headers {
			out.Headers[k] = v
		}
		if overlay != nil {
			for k, v := range overlay.Headers {
				out.Headers[k] = v
			}
		}
	}
	if overlay != nil && overlay.AuthRef != nil {
		out.AuthRef = overlay.AuthRef
	}
	return out
}

// configuredComponents lists the Components the canary operates on — the canary
// group's own Components. The step machine runs through the primary
// (router>engine>decoder); the capacity gate observes EVERY Component in the
// group, so a PD pair stages capacity on engine+decoder together before traffic
// shifts (the secondaryCapacityReady gate), and every Component publishes the
// step's traffic weight on its own revisions.
func configuredComponents(g *v1beta1.RolloutGroup) []v1beta1.ComponentType {
	if g == nil {
		return nil
	}
	return g.Components
}

// secondaryCapacityReady reports whether every NON-primary Component's canary
// Instances have reached that Component's current-step staged count (its
// newCount under the held floor). The step machine and external traffic run
// through the primary, but traffic must not shift until EVERY Component's
// canary capacity is up — otherwise a Ready router can advance while the
// engine/decoder canary pods are still coming up. True when there are no
// secondaries. The step index mirrors EffectivePartition.
//
// Each secondary gates on ITS OWN canary target hash (revision hashes are
// per-Component, so the primary's hash never names a secondary's pods). A
// secondary that was not bumped has no distinct canary revision — its target hash
// resolves to the only revision present — so there is nothing to stage and it is
// treated as ready, rather than blocking the rollout forever.
//
// Capacity is each member's replica's own account: its serving Instances on
// the target revision (servingTargetInstances) against the Instance count it
// is scaled to. perRev detects a live peer revision while an equal
// current/target pair settles.
//
// It also returns each secondary's observed revision pair, so the traffic
// writer keys a member's targets to the very target its capacity was checked
// against. The pair is complete only when the gate reports a fresh view. The
// wait is how long until the soonest pod of a short member clears its
// window, zero when no short member is waiting on one.
func secondaryCapacityReady(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, perRev map[v1beta1.ComponentType]map[string]int32, observedPods map[v1beta1.ComponentType][]*corev1.Pod, primary v1beta1.ComponentType, g *v1beta1.RolloutGroup, now time.Time) (ready, fresh bool, observed map[v1beta1.ComponentType]observedCanaryRevisions, wait time.Duration, err error) {
	observed = map[v1beta1.ComponentType]observedCanaryRevisions{}
	if g == nil || g.Canary == nil || len(g.Canary.Steps) == 0 {
		return true, true, observed, 0, nil
	}
	plan := g.Canary
	idx := int32(0)
	if cs := rollout.CanaryStatusFor(&isvc.Status, primary); cs != nil {
		idx = rollout.StepOf(cs, isvc.Status.Components[primary].RolloutPhase, plan.Steps)
	}
	if int(idx) >= len(plan.Steps) {
		idx = int32(len(plan.Steps) - 1)
	}
	step := plan.Steps[idx]
	ready = true
	for _, c := range configuredComponents(g) {
		if c == primary {
			continue
		}
		revisions, err := observeCanaryRevisions(ctx, reads, isvc, c)
		if err != nil {
			return false, false, observed, 0, err
		}
		if !revisions.statusFresh {
			return false, false, observed, 0, nil
		}
		observed[c] = revisions
		secHash := revisions.targetHash
		if secHash == "" {
			continue
		}
		// Equal current and target revisions mean this Component was not bumped.
		// A live peer revision is allowed to disappear before the gate releases.
		if revisions.currentHash == secHash {
			if otherRevision(perRev[c], secHash) != "" {
				ready = false
			}
			continue
		}
		serving := revisions.servingTargetInstances()
		if serving == nil || *serving < stepStagedCount(step, DesiredReplicas(revisions.replicas, componentExtension(isvc, c))) {
			ready = false
			wait = soonestWait(wait, revisions.windowWait(observedPods[c], now))
		}
	}
	return ready, true, observed, wait, nil
}

// soonestWait is the shorter of two waits; a zero wait is nothing waiting
// and defers to the other.
func soonestWait(a, b time.Duration) time.Duration {
	switch {
	case a <= 0:
		return b
	case b <= 0 || a <= b:
		return a
	}
	return b
}

// observedCanaryRevisions is one replica's account of its own rollout: the
// revision pair, the Instance count it is scaled to, its declared Instance
// shape, its readiness window and its per-Instance rows.
type observedCanaryRevisions struct {
	currentHash string
	targetHash  string
	// replicas is the Instance count the replica is scaled to; nil when the
	// replica carries none.
	replicas *int32
	runners  []v1beta1.Runner
	// minReadySeconds is the Component's availability window: a target
	// Instance counts toward a step's capacity only once the replica reports
	// its pods available, Ready for at least that long.
	minReadySeconds int32
	// rows are the IR's per-Instance rows in the dense shape, the capacity
	// gate's count and the crash reading's anchors; empty when the
	// observation is not an IR's.
	rows []v1beta1.OMENativeInstanceStatus
	// stored is the replica as the live reader handed it, the form a write
	// of it carries and the object the Services sweep reads; nil when the
	// observation is not an IR's.
	stored      *v1beta1.InferenceReplica
	fromIR      bool
	statusFresh bool
}

// servingTargetInstances is the replica's account of the capacity its target
// revision has: the Instances whose row runs the target revision with the
// declared pod set in rotation and, under the Component's minReadySeconds,
// reported available (Ready for at least that long). A multi-pod gang is one
// Instance, counted once and only whole; a pod that is Ready but held out of
// rotation does not count. A non-IR observation returns nil.
func (r observedCanaryRevisions) servingTargetInstances() *int32 {
	if !r.fromIR {
		return nil
	}
	if r.targetHash == "" {
		serving := int32(0)
		return &serving
	}
	return r.servingInstances(r.targetHash)
}

// servingInstances is the replica's account of the capacity one revision
// has, counted as servingTargetInstances counts the target's. A non-IR
// observation or no revision returns nil.
func (r observedCanaryRevisions) servingInstances(hash string) *int32 {
	if !r.fromIR || hash == "" {
		return nil
	}
	serving := int32(0)
	revision, shape := query.RevisionFromHash(hash), instanceShape(r.runners)
	for i := range r.rows {
		if r.rowServesTarget(&r.rows[i], revision, shape) {
			serving++
		}
	}
	return &serving
}

// rowServesTarget reports whether one Instance row counts as capacity on
// the target revision: it runs target with the declared pod set (shape) in
// rotation and, under the Component's minReadySeconds, reported available.
// target and shape are the observation's own, resolved once for every row.
func (r observedCanaryRevisions) rowServesTarget(row *v1beta1.OMENativeInstanceStatus, target query.RevisionID, shape int32) bool {
	if !query.RevisionFromName(row.RunningRevision).Same(target) {
		return false
	}
	if !workloadstatus.InstanceMeetsThreshold(row.PodCount, row.ServingPodCount, shape) {
		return false
	}
	return r.minReadySeconds <= 0 || workloadstatus.InstanceMeetsThreshold(row.PodCount, row.AvailablePodCount, shape)
}

// instancesOffTarget names the Instances the replica does not count as
// serving on its target revision, each with the phase its row reports, in
// index order: the Instances a stop at the cutover names. Empty for a
// non-IR observation or a replica with no target.
func (r observedCanaryRevisions) instancesOffTarget() []string {
	if !r.fromIR || r.targetHash == "" {
		return nil
	}
	target, shape := query.RevisionFromHash(r.targetHash), instanceShape(r.runners)
	var names []string
	for i := range r.rows {
		row := &r.rows[i]
		if !r.rowServesTarget(row, target, shape) {
			names = append(names, fmt.Sprintf("Instance %d (%s)", row.Index, row.Phase))
		}
	}
	return names
}

// instancesOffTarget collects, per retargeted member, the Instances that do
// not serve the member's target revision, the primary first.
func instancesOffTarget(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType, primaryObserved observedCanaryRevisions, secondaryObserved map[v1beta1.ComponentType]observedCanaryRevisions, pods map[v1beta1.ComponentType][]*corev1.Pod) map[v1beta1.ComponentType][]string {
	out := map[v1beta1.ComponentType][]string{}
	canaryMembers(isvc, g, primary, primaryObserved, secondaryObserved, pods, func(comp v1beta1.ComponentType, observed observedCanaryRevisions, _ []*corev1.Pod) bool {
		if names := observed.instancesOffTarget(); len(names) > 0 {
			out[comp] = names
		}
		return true
	})
	return out
}

// instanceShape is the pod count one Instance of the declared runner set
// materializes; zero when the set is undeclared, which makes the serving
// threshold every live pod of the Instance.
func instanceShape(runners []v1beta1.Runner) int32 {
	var pods int32
	for _, runner := range runners {
		if runner.Size < 1 {
			return 0
		}
		pods += runner.Size
	}
	return pods
}

// windowWait is how long until the soonest target pod that serves but has not
// yet held Ready for the Component's minReadySeconds clears the window, zero
// when none is waiting. The crossing raises no pod event, so a gate short on
// the window alone wakes for it; the replica republishes its available count
// at the same instant, and that count is what the gate reads.
func (r observedCanaryRevisions) windowWait(pods []*corev1.Pod, now time.Time) time.Duration {
	if !r.fromIR || r.minReadySeconds <= 0 || r.targetHash == "" {
		return 0
	}
	var wait time.Duration
	for _, pod := range pods {
		if pod == nil || pod.Labels[query.LabelRevisionHash] != r.targetHash || !coordination.PodReadyAndServing(pod) {
			continue
		}
		if available, remaining := podreadiness.IsPodAvailable(pod, r.minReadySeconds, now); !available {
			wait = soonestWait(wait, remaining)
		}
	}
	return wait
}

// observeCanaryRevisions reads one replica's rollout account: the current and
// target revisions, the Instance count it is scaled to and its decoded
// per-Instance rows. The revision pair assigns stable and canary roles without
// relying on pod population, whose new-revision pods can precede the IR status
// update. A missing IR or empty revision pair is not authoritative and leaves
// statusFresh false so the caller waits without mutating rollout state.
//
// The per-Instance rows are decoded with the decoder the reader carries, so
// the capacity gate and the crash reading read each Instance as the replica
// published it; a payload that cannot be decoded is an error, as it is for
// every reader of the rows. The rows are decoded on a copy: the replica as
// read is kept for the Services sweep and the rollback signal, so a pass
// reads each member's replica once.
func observeCanaryRevisions(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType) (observedCanaryRevisions, error) {
	if reads != nil {
		ir := &v1beta1.InferenceReplica{}
		key := irprojector.RoleReplicaKey(isvc, c)
		switch decoded, _, err := irstatus.GetDecodedCopy(ctx, reads, key, ir); {
		case err == nil:
			currentHash := query.RevisionFromName(decoded.Status.CurrentRevision).Hash()
			targetHash := query.RevisionFromName(decoded.Status.UpdateRevision).Hash()
			if targetHash == "" {
				targetHash = currentHash
			}
			observed := observedCanaryRevisions{
				currentHash:     currentHash,
				targetHash:      targetHash,
				replicas:        decoded.Spec.Replicas,
				runners:         decoded.Spec.Runners,
				minReadySeconds: decoded.Spec.MinReadySeconds,
				rows:            decoded.Status.InstanceStatuses,
				stored:          ir,
				fromIR:          true,
				statusFresh:     decoded.Status.ObservedGeneration == decoded.Generation,
			}
			if !observed.statusFresh || targetHash != "" {
				return observed, nil
			}
		case !apierrors.IsNotFound(err):
			return observedCanaryRevisions{}, err
		}
	}
	return observedCanaryRevisions{}, nil
}

// canaryMembers visits the unit's members whose target pods are canary
// pods, the primary first and then each secondary the run retargeted, until
// visit returns false. A member the run left alone serves its stable
// revision on its "target", so its pods are not read.
func canaryMembers(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType, primaryObserved observedCanaryRevisions, secondaryObserved map[v1beta1.ComponentType]observedCanaryRevisions, pods map[v1beta1.ComponentType][]*corev1.Pod, visit func(v1beta1.ComponentType, observedCanaryRevisions, []*corev1.Pod) bool) {
	if memberRetargeted(isvc, primary) && !visit(primary, primaryObserved, pods[primary]) {
		return
	}
	for _, comp := range configuredComponents(g) {
		if comp == primary || !memberRetargeted(isvc, comp) {
			continue
		}
		if !visit(comp, secondaryObserved[comp], pods[comp]) {
			return
		}
	}
}

// canaryCrash reads the unit for a canary pod in a crash loop after it
// served, against the open of the run the canary is bound to. The first one
// found is the unit's verdict.
func canaryCrash(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType, primaryObserved observedCanaryRevisions, secondaryObserved map[v1beta1.ComponentType]observedCanaryRevisions, pods map[v1beta1.ComponentType][]*corev1.Pod, now time.Time) *CanaryCrash {
	since := crashAnchor(isvc, rollout.CanaryStatusFor(&isvc.Status, primary), now)
	var crash *CanaryCrash
	canaryMembers(isvc, g, primary, primaryObserved, secondaryObserved, pods, func(comp v1beta1.ComponentType, observed observedCanaryRevisions, pods []*corev1.Pod) bool {
		crash = crashedCanaryPod(comp, observed, pods, since)
		return crash == nil
	})
	return crash
}

// canaryRestartedAt reads the unit for the newest moment any canary pod
// died or came back, across every member whose pods are canary pods; zero
// when none has. The step machine measures a timed soak from it.
func canaryRestartedAt(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType, primaryObserved observedCanaryRevisions, secondaryObserved map[v1beta1.ComponentType]observedCanaryRevisions, pods map[v1beta1.ComponentType][]*corev1.Pod) time.Time {
	var latest time.Time
	canaryMembers(isvc, g, primary, primaryObserved, secondaryObserved, pods, func(_ v1beta1.ComponentType, observed observedCanaryRevisions, pods []*corev1.Pod) bool {
		latest = laterOf(latest, latestCanaryRestart(observed, pods))
		return true
	})
	return latest
}

// primaryComponentOf is the externally-routed Component the canary group drives
// its step machine through: the group's role with the lowest primary rank
// (router > engine > decoder). Secondary Components (a PD pair's
// engine/decoder) stage capacity, gate the step, and publish the step's weight
// on their own revisions.
func primaryComponentOf(g *v1beta1.RolloutGroup) v1beta1.ComponentType {
	return rollout.PrimaryOf(g)
}

// componentExtension is a Component's declared replica bounds and lifecycle;
// nil when the InferenceService does not declare the Component.
func componentExtension(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) *v1beta1.ComponentExtensionSpec {
	if view := v1beta1.SpecOf(&isvc.Spec, c); view != nil {
		return view.Extension
	}
	return nil
}

// otherRevision returns the non-canary revision hash with live pods (the stable
// revision). Empty when only the canary revision is present.
func otherRevision(pods map[string]int32, canaryHash string) string {
	best, bestN := "", int32(0)
	for h, n := range pods {
		if h == canaryHash || h == "" {
			continue
		}
		if n >= bestN {
			best, bestN = h, n
		}
	}
	return best
}
