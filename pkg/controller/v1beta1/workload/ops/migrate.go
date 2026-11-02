package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// MigrateRequeueInterval is the wait between Migrate passes while a
// surge migration is in flight, from the operator's
// lifecycle.requeue.operation. Exported so the dispatcher's pacing
// stays in lockstep. Zero means unconfigured: the caller requeues on
// the controller's rate-limited backoff instead.
func MigrateRequeueInterval(input workload.ReconcileInput) time.Duration {
	return input.Requeue.Operation
}

// ledgerOwnerObject / ledgerOwnerGVK resolve the object that owns the
// migration audit-ledger ConfigMap. The IR caller sets input.LedgerOwner
// to the parent ISVC (so the ledger + the operator's migration-request
// annotation share the user-facing resource while the IR owns the pods);
// every other caller leaves it nil and the ledger lives on OwnerObject.
func ledgerOwnerObject(input workload.ReconcileInput) client.Object {
	if input.LedgerOwner != nil {
		return input.LedgerOwner
	}
	return input.OwnerObject
}

func ledgerOwnerGVK(input workload.ReconcileInput) schema.GroupVersionKind {
	if input.LedgerOwner != nil {
		return input.LedgerOwnerGVK
	}
	return input.OwnerGVK
}

// Migrate drives one move one step further. The move is the record in
// status.migrations for requestUUID: the dispatcher picks the record,
// the pass resumes from its surge index and phase, and advances the
// phase through ReconcileInput.MutateMigration. A move takes many
// passes and survives a crash because the record is what it resumes
// from; the audit history is history only.
//
// A fresh record (no surge index yet) is admitted first: the source
// must be steady Ready, must name a node to leave, must fit under the
// operator's migration caps, and its revision's node affinity must not
// contradict the overlay. A source that cannot move (no row, a revision
// that never started, a revision whose retry record holds) is refused
// with the reason on the record. A wait that no operation in flight
// ends by itself (the Component pausing new surges, a source Failed at
// its running revision with its repair stopped) is named on the record,
// which stays Accepted and goes on once the cause clears. The move
// binds to the Instance: a source rebuilt off the request's node since
// the request leaves the node it runs on, and a source a gang roll
// rebuilt under another index is followed there by the record
// (followHandoffForPendingMigrations) and waits while that index is
// still the roll's (replacementClaimsIndex).
//
// Then, pass by pass: the surge index is the lowest unused, written to
// the record first, then the pair's rows (source Migrating, surge
// Creating) and the history's Started row; the surge is created from
// the source's running revision with the overlay off the node the move
// leaves; the pass waits until the surge is ContainersReady, in
// rotation and available, lets it serve and records SurgeReady; the
// source leaves traffic, the record reads Draining, and the source is
// deleted once it has left its Service; the surge is promoted Ready on
// that revision, the source's row removed, the Completed history row
// written and the record closed, in that order, so a crash in the tail
// re-runs steps that are safe to repeat.
//
// A multi-pod Instance moves as a whole: the surge copies the source's
// runner layout and worker template from its running revision; a gang
// surge waits one pass after its rows are written so the PodGroup pass
// creates the surge's PodGroup before the gang's pods render; the node
// to leave is the member the request names, else the routable member's;
// the rotation and drain gates read the routable leader only, since
// workers are never routed.
//
// Returns (done, accepted, err):
//   - done=true: the move finished (completed or failed); the caller
//     goes on to the next pass without a requeue from the migration
//     branch.
//   - done=false, accepted=true: the move is in flight (the record owns
//     its pair); the caller must requeue and must not fall through to
//     the update and create passes, which refuse a row the move owns on
//     their own.
//   - done=false, accepted=false: the move waits without taking the
//     pair (a fresh request whose source is not steady Ready because of
//     an update, repair or create in flight, or a wait named on the
//     record). The caller falls through so the operation in flight
//     converges; otherwise the dispatcher would requeue at the migrate
//     interval for good and the source would never reach Ready.
//
// The phase list below is the whole order of a pass: each phase does one
// job and reports whether the pass goes on.
func Migrate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, sourceIdx int32, requestUUID string, req *audit.MigrationRequest) (done bool, accepted bool, err error) {
	move, err := newMigrationPass(deps, input, plan, target, sourceIdx, requestUUID, req)
	if err != nil {
		return false, false, err
	}
	if move.record.Phase.Terminal() {
		// The request was taken and fully handled on an earlier pass, so
		// the caller goes on without falling through.
		return true, true, nil
	}
	_, err = runMigrationPhases(ctx, []migrationPhase{
		move.waitForPlacementPause,
		move.loadHistory,
		move.resumePair,
		move.admitFreshRequest,
		move.requeueGangForPodGroup,
		move.listPods,
		move.resumeCompletionTail,
		move.endOnSurgeThatCannotStart,
		move.confirmPairBeforeEffects,
		move.resolveSurgeRevision,
		move.finishWhenSourceIsGone,
		move.planSurge,
		move.refuseOverlayConflict,
		move.freeSurgePodNames,
		move.createSurgePods,
		move.waitForSurge,
		move.recordSurgeReady,
		move.drainSource,
		move.deleteSource,
		move.finish,
	})
	return move.done, move.accepted, err
}

// migrationPhase is one step of a move's pass. It reports stop when the
// pass ends with it, with the error that ended it, if any; the pass's
// answer (done, accepted) is read from the record the phases share.
type migrationPhase func(ctx context.Context) (stop bool, err error)

// runMigrationPhases runs the phases in order and reports whether one of
// them ended the pass, with its error. The first phase that stops, or
// fails, is the last one run.
func runMigrationPhases(ctx context.Context, phases []migrationPhase) (stopped bool, err error) {
	for _, phase := range phases {
		if stop, err := phase(ctx); stop || err != nil {
			return true, err
		}
	}
	return false, nil
}

// migrationPass is one pass of a move: what the pass read at its start
// and what its phases decide as they run. The source is the Instance
// the request names; the surge is the copy of it the move builds at a
// free Instance index, off the node the move leaves. The record in
// status.migrations says how far the move has come, and the next pass
// runs the phases again from the top.
type migrationPass struct {
	deps        workload.Deps
	input       workload.ReconcileInput
	plan        workload.ComponentPlan
	target      *appsv1.ControllerRevision
	sourceIdx   int32
	requestUUID string
	req         *audit.MigrationRequest

	// record is the move's entry in status.migrations, the one place a
	// move resumes from.
	record *workload.MigrationRecord
	// history is the owner's audit record of moves (the audit package's
	// Ledger), written as history and never resumed from.
	history *audit.Ledger
	// gang: the Instance is multi-pod, so the surge is a whole gang.
	gang bool

	source   *workload.InstanceStatus
	surge    *workload.InstanceStatus
	surgeIdx int32
	// pairConfirmed: this pass wrote or confirmed the pair's two rows on
	// the adapter's fresh copy, so the effects need no second check.
	pairConfirmed bool
	// pairWrittenThisPass: this pass wrote the pair's rows, on a fresh
	// allocation or on a surge row written back; a gang surge then waits
	// one pass for its PodGroup.
	pairWrittenThisPass bool

	sourcePods []*corev1.Pod
	surgePods  []*corev1.Pod
	// surgeRevision is the source's running revision, which the surge
	// renders, with its stored template; surgePlan is the surge Instance
	// as the plan would list it, with the overlay that keeps it off the
	// node the move leaves; surgePodNames are the pod names it asks for.
	surgeRevision *appsv1.ControllerRevision
	surgePayload  *revision.DataPayload
	surgeTemplate podTemplate
	surgePlan     workload.InstancePlan
	surgePodNames []podTarget

	// done: the record is terminal. accepted: the record owns its pair,
	// or the request was answered, so the caller requeues instead of
	// falling through to the update and create passes.
	done     bool
	accepted bool
}

// newMigrationPass checks what every pass needs and reads the record,
// the source row and the Instance's shape.
func newMigrationPass(deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, sourceIdx int32, requestUUID string, req *audit.MigrationRequest) (*migrationPass, error) {
	if deps.Client == nil {
		return nil, fmt.Errorf("Migrate: nil client")
	}
	if req == nil {
		return nil, fmt.Errorf("Migrate: nil request (instance=%d)", sourceIdx)
	}
	if input.MutateMigration == nil {
		return nil, fmt.Errorf("Migrate: MutateMigration not wired (uuid=%s)", requestUUID)
	}
	record := workload.FindMigrationRecord(input.ObservedState.Migrations, requestUUID)
	if record == nil {
		return nil, fmt.Errorf("Migrate: no status.migrations record for uuid=%s", requestUUID)
	}
	return &migrationPass{
		deps:        deps,
		input:       input,
		plan:        plan,
		target:      target,
		sourceIdx:   sourceIdx,
		requestUUID: requestUUID,
		req:         req,
		record:      record,
		source:      input.ObservedState.Instance(sourceIdx),
		gang:        isMultiPodInstance(plan, sourceIdx),
	}, nil
}

// waitForPlacementPause holds a request that has no surge yet while the
// Component pauses new surges. Placement owns the pause: the record
// names it as what the move waits on, and the move allocates once the
// pause is gone.
func (m *migrationPass) waitForPlacementPause(ctx context.Context) (bool, error) {
	if !m.input.PauseNewSurge || m.record.SurgeAllocated() {
		return false, nil
	}
	if err := deferMigration(ctx, m.deps, m.input, m.record, migrationPausedMessage, false); err != nil {
		return false, fmt.Errorf("Migrate: record placement pause (uuid=%s): %w", m.requestUUID, err)
	}
	return true, nil
}

// loadHistory reads the owner's audit record of moves, which the pass
// writes to as history; the record in status.migrations, not this, is
// what a move resumes from.
func (m *migrationPass) loadHistory(ctx context.Context) (bool, error) {
	history, err := audit.LoadLedgerForOwner(ctx, m.deps.Reader(), ledgerOwnerObject(m.input))
	if err != nil {
		return false, fmt.Errorf("Migrate: load audit ledger: %w", err)
	}
	m.history = history
	return false, nil
}

// resumePair picks up a move whose record already names its surge index:
// the record owns the pair, so the request counts as accepted. While the
// record is still before SurgeReady the source has not begun draining,
// so the pair's rows are written again (the writes repeat without
// effect) to close the crash window between the record's write and
// theirs, without ever bringing back a source row the completion tail
// already removed. A surge row missing from this pass's rows means the
// PodGroup pass ran without the surge index, so the write counts as
// fresh and a gang surge waits one pass for its PodGroup.
func (m *migrationPass) resumePair(ctx context.Context) (bool, error) {
	if !m.record.SurgeAllocated() {
		return false, nil
	}
	m.surgeIdx = *m.record.SurgeInstance
	m.surge = m.input.ObservedState.Instance(m.surgeIdx)
	m.accepted = true
	if workload.MigrationPhaseAtOrPast(m.record.Phase, workload.MigrationPhaseSurgeReady) {
		return false, nil
	}
	m.pairWrittenThisPass = m.surge == nil
	if m.input.ApplyInstanceMutationsWithRetryBlock != nil {
		confirmed, err := establishMigrationPair(ctx, m.input, m.source, m.surge, m.requestUUID, m.surgeIdx, m.plan.InstanceReadyTimeout)
		if err != nil {
			return false, fmt.Errorf("Migrate: ensure migration pair: %w", err)
		}
		if !confirmed {
			return true, nil
		}
		m.pairConfirmed = true
		return false, nil
	}
	if err := patchInstanceStatusMigrating(ctx, m.input, m.sourceIdx, m.surgeIdx, m.requestUUID, m.plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("Migrate: ensure source stamp (instance=%d): %w", m.sourceIdx, err)
	}
	if err := patchInstanceStatusMigrationSurge(ctx, m.input, m.surgeIdx, m.sourceIdx, m.requestUUID, m.plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("Migrate: ensure surge stamp (instance=%d): %w", m.surgeIdx, err)
	}
	return false, nil
}

// admitFreshRequest takes a record that has no surge yet through the
// admission phases, in order: refuse a source that cannot move, wait for
// a steady source, name the node to leave, check the request against the
// migration caps, check the overlay before any row is written, allocate
// the surge and write the record, write the pair's rows, record the
// acceptance. A record with a surge index was admitted on an earlier
// pass. The pass ends with the first phase that stops.
func (m *migrationPass) admitFreshRequest(ctx context.Context) (bool, error) {
	if m.record.SurgeAllocated() {
		return false, nil
	}
	return runMigrationPhases(ctx, []migrationPhase{
		m.refuseSourceThatCannotMove,
		m.waitForSteadySource,
		m.resolveNodeToLeave,
		m.admitUnderCapacity,
		m.checkOverlayBeforeWriting,
		m.allocateSurge,
		m.writePair,
		m.recordAccepted,
	})
}

// refuseSourceThatCannotMove answers a fresh request whose source no
// operation in flight will bring back: a source with no row, a source
// Failed on a revision it never started, or a source running a revision
// whose retry record holds. Each is closed Failed now with the reason on
// the record, rather than waited on until the deadline blames the surge.
func (m *migrationPass) refuseSourceThatCannotMove(ctx context.Context) (bool, error) {
	if m.source == nil {
		return m.reject(ctx, "source InstanceStatus missing")
	}
	if migrationSourceRevisionNotStarted(m.source) {
		return m.reject(ctx, migrationSourceRevisionNotStartedReason(m.input, m.source))
	}
	if migrationSourceRevisionHeld(m.input, m.source) {
		return m.reject(ctx, migrationSourceRevisionHeldReason(m.input, m.source))
	}
	return false, nil
}

// waitForSteadySource defers a fresh request without taking the pair
// while the source is not Ready with a recorded revision and no
// operation: a row written Migrating under a create would hold the
// create, and one written under an update or a repair would run over
// it. The operation in flight brings the row back, so the pass falls
// through to the other passes and the record is picked again next
// pass. A source Failed at its running revision with its repair
// stopped has no such operation, so the record names that wait. A
// source another row's surge still claims as its replacement waits
// too: a move written on it now would leave that source claiming a
// replacement it cannot recognize.
func (m *migrationPass) waitForSteadySource(ctx context.Context) (bool, error) {
	if !migrationSourceSteady(m.source) {
		if msg, waiting := migrationSourceRepairParked(m.input, m.source); waiting {
			if err := deferMigration(ctx, m.deps, m.input, m.record, msg, true); err != nil {
				return false, fmt.Errorf("Migrate: record parked source repair (uuid=%s): %w", m.requestUUID, err)
			}
		}
		return true, nil
	}
	return replacementClaimsIndex(m.input.ObservedState.InstanceStatuses, m.sourceIdx), nil
}

// resolveNodeToLeave names the node the move leaves: the request's node
// while the source still sits there, else the node the Instance was
// rebuilt on since the request. The request is corrected so the overlay,
// the history row and every later pass name the node the Instance leaves.
// A source pod with no node yet defers the pass without taking the pair;
// a source that names no node to leave is refused.
func (m *migrationPass) resolveNodeToLeave(ctx context.Context) (bool, error) {
	leaving, wait, rejection, err := resolveSourceNode(ctx, m.deps, m.input, m.plan, m.sourceIdx, m.req.FromNode)
	switch {
	case err != nil:
		return false, fmt.Errorf("Migrate: resolve source node (instance=%d): %w", m.sourceIdx, err)
	case wait:
		return true, nil
	case rejection != "":
		workload.RecordWarning(m.deps.Recorder, workload.EventTarget(m.input), workload.EventReasonMigrationFromNodeMismatch,
			"OMENative migration uuid=%s rejected: %s", m.requestUUID, rejection)
		return m.reject(ctx, rejection)
	case leaving != m.req.FromNode:
		workload.RecordNormal(m.deps.Recorder, workload.EventTarget(m.input), workload.EventReasonMigrationFromNodeMismatch,
			"OMENative migration uuid=%s: request.FromNode=%s does not match observed source node=%s; %s leaves %s",
			m.requestUUID, m.req.FromNode, leaving, workload.InstanceKey(m.input.Key.Component, m.sourceIdx), leaving)
		m.req.FromNode = leaving
	}
	return false, nil
}

// admitUnderCapacity checks the request against the operator's migration
// caps over the owner's records: moves in flight (a surge allocated, the
// record not terminal) and moves allocated inside the trailing window,
// this request's own record excluded; queued Accepted records hold
// nothing and are not counted. With no caps configured there is no
// bound to check against, so the request waits without taking the pair,
// and the pass asks for a requeue because the operator writing the key
// raises no event the controller watches. A request over a cap is
// refused.
func (m *migrationPass) admitUnderCapacity(ctx context.Context) (bool, error) {
	if m.input.MigrationAudit == nil {
		if err := holdMigrationForUnconfiguredCapacity(ctx, m.deps, m.input, m.record, m.requestUUID); err != nil {
			return false, fmt.Errorf("Migrate: hold for unconfigured capacity (uuid=%s): %w", m.requestUUID, err)
		}
		m.input.PassWake.Observe(MigrateRequeueInterval(m.input))
		return true, nil
	}
	if err := writeMigrationCapacityCondition(ctx, m.input, false); err != nil {
		return false, fmt.Errorf("Migrate: clear capacity condition (uuid=%s): %w", m.requestUUID, err)
	}
	if ok, reason := audit.ValidateCapacity(m.input.MigrationAudit, m.input.ObservedState.Migrations, m.requestUUID, m.deps.Now()); !ok {
		workload.RecordWarning(m.deps.Recorder, workload.EventTarget(m.input), workload.EventReasonRateLimited,
			"OMENative migration uuid=%s rejected: %s", m.requestUUID, reason)
		return m.reject(ctx, reason)
	}
	return false, nil
}

// checkOverlayBeforeWriting reads the source's running revision and
// refuses, before any row is written, a surge whose node affinity the
// overlay would contradict, on the leader and on a gang's worker
// template alike. A refusal after the rows were written would leave the
// source Migrating until the deadline cleanup, with the rejection out of
// the requester's sight. A revision not recorded yet, or already gone,
// defers the pass without taking the pair.
func (m *migrationPass) checkOverlayBeforeWriting(ctx context.Context) (bool, error) {
	_, payload, err := surgeRevisionAndSpec(ctx, m.deps, m.input, m.sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: resolve surge revision: %w", err)
	}
	if payload == nil {
		return true, nil
	}
	if m.overlayConflicts(payload.PodSpec, payload.WorkerPodSpec) {
		return m.rejectOverlayConflict(ctx)
	}
	return false, nil
}

// allocateSurge takes the lowest free Instance index for the surge and
// writes it to the record first, with the node the move leaves and the
// time execution starts, which the capacity caps count from. The record
// is what a later pass resumes from: a crash after this write resumes
// with the index and writes the pair's rows again, where the reverse
// order would leave a written source behind the fresh-request checks
// for good. From this write on the request is accepted.
func (m *migrationPass) allocateSurge(ctx context.Context) (bool, error) {
	m.surgeIdx = workload.AllocateSurgeIndex(m.input.ObservedState.InstanceStatuses)
	allocated := m.surgeIdx
	leaving := m.req.FromNode
	if err := m.input.MutateMigration(ctx, m.requestUUID, func(record *workload.MigrationRecord) bool {
		if record.SurgeInstance != nil && *record.SurgeInstance == allocated &&
			workload.MigrationPhaseAtOrPast(record.Phase, workload.MigrationPhaseSurgePending) {
			return false
		}
		record.SurgeInstance = &allocated
		record.FromNode = leaving
		if record.AllocatedAt == nil {
			now := metav1.NewTime(m.deps.Now())
			record.AllocatedAt = &now
		}
		if !workload.MigrationPhaseAtOrPast(record.Phase, workload.MigrationPhaseSurgePending) {
			record.Phase = workload.MigrationPhaseSurgePending
		}
		record.Message = "surge allocated; waiting for surge pods"
		return true
	}); err != nil {
		return false, fmt.Errorf("Migrate: record surge allocation (uuid=%s): %w", m.requestUUID, err)
	}
	m.accepted = true
	return false, nil
}

// writePair writes the pair's two rows: the source Migrating with the
// surge index on its operation, the surge Creating with the source's
// index on its own, so the pair reads from either side. On the batch
// path the write is fenced on both rows and the pass stops when they no
// longer read as it saw them.
func (m *migrationPass) writePair(ctx context.Context) (bool, error) {
	if m.input.ApplyInstanceMutationsWithRetryBlock != nil {
		confirmed, err := establishMigrationPair(ctx, m.input, m.source, nil, m.requestUUID, m.surgeIdx, m.plan.InstanceReadyTimeout)
		if err != nil {
			return false, fmt.Errorf("Migrate: stamp migration pair: %w", err)
		}
		if !confirmed {
			return true, nil
		}
		m.pairConfirmed = true
		return false, nil
	}
	if err := patchInstanceStatusMigrating(ctx, m.input, m.sourceIdx, m.surgeIdx, m.requestUUID, m.plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("Migrate: stamp source status (instance=%d): %w", m.sourceIdx, err)
	}
	if err := patchInstanceStatusMigrationSurge(ctx, m.input, m.surgeIdx, m.sourceIdx, m.requestUUID, m.plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("Migrate: stamp surge status (instance=%d): %w", m.surgeIdx, err)
	}
	return false, nil
}

// recordAccepted replaces the history's accept-time Started row, which
// names no surge index, with one that names it (the same request id
// writes in place), and publishes the acceptance. With the record, the
// rows and the history written, the pass owns the move.
func (m *migrationPass) recordAccepted(ctx context.Context) (bool, error) {
	m.history.UpsertEntry(audit.NewStartedEntry(m.req, m.requestUUID, m.surgeIdx))
	if err := audit.PersistLedgerForOwner(ctx, m.deps.Client, ledgerOwnerObject(m.input), ledgerOwnerGVK(m.input), m.history); err != nil {
		return false, fmt.Errorf("Migrate: persist Started ledger: %w", err)
	}
	workload.RecordNormal(m.deps.Recorder, workload.EventTarget(m.input), workload.EventReasonMigrationRequestAccepted,
		"OMENative %s migration accepted (uuid=%s, source-node=%s, surge-index=%d)",
		workload.InstanceKey(m.input.Key.Component, m.sourceIdx), m.requestUUID, m.req.FromNode, m.surgeIdx)
	m.pairWrittenThisPass = true
	return false, nil
}

// requeueGangForPodGroup ends the pass of a gang surge whose rows were
// written this pass: the PodGroup pass ran before this dispatch on a
// plan without the surge index, so the surge's PodGroup does not exist
// yet. The next pass finds the index in the plan, creates the PodGroup,
// and only then renders the gang's pods, which the gang scheduler needs
// after their PodGroup. A single-pod surge, or a gang without gang
// scheduling, goes on in the same pass. After the promote the plan
// releases the source's index, so the surge's own entry, which carries
// the same layout, proves the gang shape for the tail.
func (m *migrationPass) requeueGangForPodGroup(context.Context) (bool, error) {
	m.gang = m.gang || isMultiPodInstance(m.plan, m.surgeIdx)
	return m.pairWrittenThisPass && m.gang && m.input.DesiredSpec.GangSchedulingAvailable, nil
}

// listPods reads the source's and the surge's pods from the live reader.
func (m *migrationPass) listPods(ctx context.Context) (bool, error) {
	var err error
	if m.sourcePods, err = m.livePods(ctx, m.sourceIdx); err != nil {
		return false, fmt.Errorf("Migrate: list source pods: %w", err)
	}
	if m.surgePods, err = m.livePods(ctx, m.surgeIdx); err != nil {
		return false, fmt.Errorf("Migrate: list surge pods: %w", err)
	}
	return false, nil
}

// livePods lists the pods of the Instance at idx from the live reader,
// so a cold or lagging cache never reads as an empty Instance.
func (m *migrationPass) livePods(ctx context.Context, idx int32) ([]*corev1.Pod, error) {
	return query.LiveListPodsForInstance(ctx, m.deps.Reader(), m.input.Key.Namespace, m.input.Key.OwnerName, m.plan.Component, idx)
}

// resumeCompletionTail finishes a move whose surge was promoted while
// the source's row still stands: the record reads Draining, no source
// pod is left and the surge row reads Ready with no operation. The
// source's row is the ownership token of the cleanup, and the promoted
// surge may be a steady plan member by now, so the tail resumes from the
// written pair instead of rendering pods from the plan after the
// promote. Batch path only; the pair is confirmed before the source's
// resources are finalized and again before the record closes.
func (m *migrationPass) resumeCompletionTail(ctx context.Context) (bool, error) {
	if !migrationCompletionTailRecoverable(m.record, m.sourcePods, m.surge) {
		return false, nil
	}
	if m.input.ApplyInstanceMutationsWithRetryBlock == nil {
		return true, nil
	}
	confirmed, err := m.confirmRetainedPair(ctx)
	if err != nil {
		return false, fmt.Errorf("Migrate: confirm completion pair: %w", err)
	}
	if !confirmed {
		return true, nil
	}
	finalized, err := status.FinalizeAndRemove(ctx, m.deps, m.input, m.sourceIdx, m.source)
	if err != nil {
		return false, fmt.Errorf("Migrate: finalize source Instance in completion tail: %w", err)
	}
	if !finalized {
		return true, nil
	}
	confirmed, err = confirmMigrationCompletionPair(ctx, m.input, m.sourceIdx, m.surge)
	if err != nil {
		return false, fmt.Errorf("Migrate: recheck completion pair: %w", err)
	}
	if !confirmed {
		return true, nil
	}
	if err := completeMigrationTail(ctx, m.deps, m.input, m.history, m.req, m.requestUUID, m.sourceIdx, m.surgeIdx, m.surge.RunningRevision); err != nil {
		return false, err
	}
	m.done = true
	return true, nil
}

// confirmRetainedPair proves the pair as the completion tail found it:
// with the source's row gone, the promoted surge alone; with it still
// standing, both rows, the surge read as promoted.
func (m *migrationPass) confirmRetainedPair(ctx context.Context) (bool, error) {
	if m.source == nil {
		return confirmMigrationCompletionPair(ctx, m.input, m.sourceIdx, m.surge)
	}
	return confirmMigrationDrainPair(ctx, m.input, m.source, m.surge, m.requestUUID, m.surgeIdx, m.source.RunningRevision, true)
}

// endOnSurgeThatCannotStart closes the move once a surge pod sits in a
// kubelet waiting reason it cannot leave past the stuck-pod grace,
// rather than idling to the record's deadline. The close goes through
// the record, as an expiry's does, so the source is restored from what
// is observed and a record already terminal is never reopened. It runs
// ahead of the pair confirmation on purpose: it needs only the record
// and the surge pods, and its own writes are fenced on the rows, so a
// crash part way through is picked up on the next pass instead of
// waiting out the deadline. Live source pods are required because the
// restore puts the source back in rotation; a source already drained
// and deleted has nothing to restore and stays with the deadline.
func (m *migrationPass) endOnSurgeThatCannotStart(ctx context.Context) (bool, error) {
	if !m.record.SurgeAllocated() || len(m.sourcePods) == 0 {
		return false, nil
	}
	blocker, blocked := surgeWedgeBlocker(m.input, m.surgePods)
	if !blocked {
		return false, nil
	}
	if err := failMigrationOnWedgedSurge(ctx, m.deps, m.input, m.plan, m.record, m.surgeIdx, blocker); err != nil {
		return false, fmt.Errorf("Migrate: fail on wedged surge (uuid=%s): %w", m.requestUUID, err)
	}
	m.done = true
	return true, nil
}

// confirmPairBeforeEffects proves, on the batch path, that the pair's
// two rows still read as this pass saw them before the pass creates,
// routes or deletes anything; a pass that already wrote or confirmed
// them skips the check. With the source's pods gone the surge may
// already read promoted.
func (m *migrationPass) confirmPairBeforeEffects(ctx context.Context) (bool, error) {
	if m.input.ApplyInstanceMutationsWithRetryBlock == nil || m.pairConfirmed {
		return false, nil
	}
	targetRevision := ""
	if m.source != nil {
		targetRevision = m.source.RunningRevision
	}
	confirmed, err := confirmMigrationDrainPair(ctx, m.input, m.source, m.surge, m.requestUUID, m.surgeIdx, targetRevision, len(m.sourcePods) == 0)
	if err != nil {
		return false, fmt.Errorf("Migrate: confirm migration pair before effects: %w", err)
	}
	return !confirmed, nil
}

// resolveSurgeRevision reads the source's running revision and its
// stored template: a move changes placement, not the template, so the
// surge runs what the source runs, leader and worker templates alike. A
// source with no running revision yet waits.
func (m *migrationPass) resolveSurgeRevision(ctx context.Context) (bool, error) {
	rev, payload, err := surgeRevisionAndSpec(ctx, m.deps, m.input, m.sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: resolve surge revision: %w", err)
	}
	if rev == nil || payload == nil {
		return true, nil
	}
	m.surgeRevision, m.surgePayload = rev, payload
	return false, nil
}

// finishWhenSourceIsGone runs the completion tail once the source is
// drained and deleted: from then on the surge is the Instance, whatever
// its pods do next, and the repair rebuilds its pod set as any
// Instance's. The surge gates are not asked again, since they would hold
// the record in Draining with nothing left to restore.
func (m *migrationPass) finishWhenSourceIsGone(ctx context.Context) (bool, error) {
	if !migrationHandedOver(m.record, m.sourcePods) {
		return false, nil
	}
	return m.finish(ctx)
}

// planSurge describes the surge Instance: the source's runner layout
// (one pod, or a leader with its workers), the node exclusions the
// source carries, and the overlay that keeps the surge off the node the
// move leaves and draws it toward the nodes the request names. The
// template is the stored one of the source's revision, so the surge
// carries only the template keys that revision records, whatever the
// current template added since.
func (m *migrationPass) planSurge(context.Context) (bool, error) {
	tmpl, err := templateFromPayload(m.input, m.target, m.surgeRevision.Name, m.surgePayload)
	if err != nil {
		return false, fmt.Errorf("Migrate: resolve surge template: %w", err)
	}
	m.surgeTemplate = tmpl
	runners, excludedNodes := m.surgeLayout()
	m.surgePlan = workload.InstancePlan{
		Index:            m.surgeIdx,
		Incarnation:      1,
		Runners:          runners,
		ExcludedNodes:    excludedNodes,
		MigrationOverlay: m.overlay(),
	}
	m.surgePodNames = expectedPodNamesForInstance(m.input, m.plan, m.surgePlan)
	return false, nil
}

// surgeLayout mirrors the source's runner layout and node exclusions
// from its plan entry. After the promote the plan releases the source's
// index, so a tail pass may find no source entry; the surge's own entry
// carries the same layout, so a gang keeps its shape instead of
// collapsing to one pod. A single-pod surge is one "default" pod.
func (m *migrationPass) surgeLayout() ([]workload.RunnerPlan, []workload.NodeExclusion) {
	runners := []workload.RunnerPlan{{Name: "default", Size: 1}}
	var excludedNodes []workload.NodeExclusion
	if source := planInstance(m.plan, m.sourceIdx); source != nil {
		excludedNodes = source.ExcludedNodes
		if m.gang {
			runners = append([]workload.RunnerPlan(nil), source.Runners...)
		}
	} else if surge := planInstance(m.plan, m.surgeIdx); m.gang && surge != nil {
		runners = append([]workload.RunnerPlan(nil), surge.Runners...)
	}
	return runners, excludedNodes
}

// overlay is the placement the surge renders with: a required term away
// from the node the move leaves, a preference for the nodes the request
// names.
func (m *migrationPass) overlay() *workload.MigrationOverlay {
	return &workload.MigrationOverlay{FromNode: m.req.FromNode, HintTargetNodes: m.req.HintTargetNodes}
}

// refuseOverlayConflict refuses a surge whose node affinity would read as
// "on the node to leave, and not on it", on the leader and on a gang's
// worker template: such a pod sits Pending to the deadline, so the
// request is refused now.
func (m *migrationPass) refuseOverlayConflict(ctx context.Context) (bool, error) {
	if m.overlayConflicts(m.surgeTemplate.podSpec, m.surgeTemplate.workerPodSpec) {
		return m.rejectOverlayConflict(ctx)
	}
	return false, nil
}

// overlayConflicts reports whether the overlay off the node the move
// leaves contradicts a hard node affinity of the leader's or the
// worker's pod spec.
func (m *migrationPass) overlayConflicts(podSpec, workerPodSpec *corev1.PodSpec) bool {
	overlay := m.overlay()
	return WouldOverlayConflictWithNodeAffinity(podSpec, overlay) ||
		(workerPodSpec != nil && WouldOverlayConflictWithNodeAffinity(workerPodSpec, overlay))
}

// rejectOverlayConflict closes the request on the overlay conflict and
// warns.
func (m *migrationPass) rejectOverlayConflict(ctx context.Context) (bool, error) {
	reason := fmt.Sprintf("source PodSpec NodeAffinity requires kubernetes.io/hostname=%s; overlay's NotIn[%s] would make scheduling impossible", m.req.FromNode, m.req.FromNode)
	workload.RecordWarning(m.deps.Recorder, workload.EventTarget(m.input), workload.EventReasonMigrationNodeAffinityConflict,
		"OMENative migration uuid=%s rejected: %s", m.requestUUID, reason)
	return m.reject(ctx, reason)
}

// freeSurgePodNames stops the pass while a surge pod name is held by a
// pod that can never serve. A surge pod the kubelet refused to admit
// never ran and never will, so it is deleted and the surge is placed
// again before the record's deadline; the bookkeeping belongs to the
// source, which owns the operation, while the pods and their
// expectations are the surge's. A surge pod whose kubelet has stopped
// reporting is not known to be gone: its name is freed only by the
// force-delete sweep once the policy proves the node gone, the source
// keeps serving meanwhile, and the record's deadline stays the bound.
func (m *migrationPass) freeSurgePodNames(ctx context.Context) (bool, error) {
	recycling, err := recycleAdmissionRejectedTargets(ctx, m.deps, m.input, m.sourceIdx, m.surgeIdx,
		workload.InstanceOperationMigrate, m.surgePods, m.surgePodNames)
	if err != nil {
		return false, fmt.Errorf("recycle rejected migration surge pod (instance=%d): %w", m.surgeIdx, err)
	}
	if recycling {
		return true, nil
	}
	holding, _, err := recoverUnknownPhaseTargets(ctx, m.deps, m.input, m.surgeIdx, m.surgePods, m.surgePodNames)
	if err != nil {
		return false, fmt.Errorf("recover unknown-phase migration surge pod (instance=%d): %w", m.surgeIdx, err)
	}
	return holding, nil
}

// createSurgePods creates the surge pods that are missing, from the
// stored template of the source's revision, once the surge's
// expectations are settled, and ends the pass. The expectations are the
// surge's, not the source's, so surge creates and source deletes are
// counted apart. The pass goes on only when every surge pod exists.
func (m *migrationPass) createSurgePods(ctx context.Context) (bool, error) {
	existing := query.IndexPodsByName(m.surgePods)
	missing := make([]podTarget, 0, len(m.surgePodNames))
	for _, desired := range m.surgePodNames {
		if _, ok := existing[desired.Name]; !ok {
			missing = append(missing, desired)
		}
	}
	if len(missing) == 0 {
		return false, nil
	}
	if !m.deps.ExpectationsCache().Satisfied(m.input.Key.Namespace, m.input.Key.OwnerName, m.input.Key.Component, m.surgeIdx) {
		return true, nil
	}
	if _, err := createMissingPods(ctx, m.deps, m.input, m.plan, m.surgePlan, m.surgeIdx, missing, m.surgeTemplate); err != nil {
		return m.answerSurgeCreateError(ctx, err)
	}
	return true, nil
}

// answerSurgeCreateError reads a refused surge create. A write the
// apiserver refused for good can never be created, so the request is
// closed now rather than idling to its deadline, as a node-affinity
// conflict is. A refusal that may clear (no quota, throttled) and a
// create that is blocked for another reason wait for the next pass
// while the record has a deadline, with one warning naming the pod.
// Anything else is the pass's error.
func (m *migrationPass) answerSurgeCreateError(ctx context.Context, cerr error) (bool, error) {
	rejection, classified := asPodRejection(cerr)
	if classified && rejection.disposed {
		reason := fmt.Sprintf("surge pod %s rejected by the apiserver (%s): %s",
			rejection.podName, rejection.rejection.Reason, rejection.rejection.Message)
		return m.reject(ctx, reason)
	}
	blockedPod := ""
	var createErr *podCreateError
	switch {
	case classified:
		blockedPod = rejection.podName
	case errors.As(cerr, &createErr):
		blockedPod = createErr.podName
	}
	if blockedPod != "" &&
		!m.record.Deadline.IsZero() &&
		!errors.Is(cerr, context.Canceled) &&
		!errors.Is(cerr, context.DeadlineExceeded) {
		workload.RecordWarning(m.deps.Recorder, workload.EventTarget(m.input), workload.EventReasonMigrationSurgeCreateBlocked,
			"OMENative migration uuid=%s waiting to create surge pod %s before its deadline",
			m.requestUUID, blockedPod)
		logf.FromContext(ctx).V(1).Info("migration surge pod creation blocked",
			"uuid", m.requestUUID, "pod", blockedPod, "error", cerr.Error())
		return true, nil
	}
	return false, fmt.Errorf("Migrate: create surge pod set: %w", cerr)
}

// waitForSurge stops the pass until every surge pod's containers run,
// then lets the surge serve: the serving gate goes true and kubelet
// folds it into PodReady.
func (m *migrationPass) waitForSurge(ctx context.Context) (bool, error) {
	if !query.AllPodsRuntimeReady(m.surgePods) {
		return true, nil
	}
	for _, pod := range m.surgePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, m.deps.Client, m.deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("Migrate: serving=True on surge pod %s: %w", pod.Name, err)
		}
	}
	return false, nil
}

// recordSurgeReady waits until the surge is in rotation and available,
// the gates the expiry pass shares (surgeTailGatesPassed), confirms the
// pair, and moves the record to SurgeReady.
func (m *migrationPass) recordSurgeReady(ctx context.Context) (bool, error) {
	ready, err := surgeTailGatesPassed(ctx, m.deps, m.input, m.plan, m.surgeIdx, m.surgePods)
	if err != nil {
		return false, fmt.Errorf("Migrate: %w", err)
	}
	if !ready {
		return true, nil
	}
	confirmed, err := confirmMigrationDrainPair(ctx, m.input, m.source, m.surge, m.requestUUID, m.surgeIdx, m.surgeRevision.Name, len(m.sourcePods) == 0)
	if err != nil {
		return false, fmt.Errorf("Migrate: confirm migration pair before drain: %w", err)
	}
	if !confirmed {
		return true, nil
	}
	if err := advanceMigrationPhase(ctx, m.input, m.requestUUID, workload.MigrationPhaseSurgeReady, "surge in rotation and available"); err != nil {
		return false, fmt.Errorf("Migrate: record Phase=SurgeReady (uuid=%s): %w", m.requestUUID, err)
	}
	return false, nil
}

// drainSource takes the source out of traffic: the serving gate goes
// false on every source pod under the request's key (the entry goes
// with the pod when it is deleted), the record moves to Draining, and
// the pass stops until every routed source pod has left its
// per-revision Service. Workers have no routed Service and count as
// drained.
func (m *migrationPass) drainSource(ctx context.Context) (bool, error) {
	for _, pod := range m.sourcePods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodNotServing(ctx, m.deps.Client, m.deps.Reader(), pod, podreadiness.WriterMigrateSourceDrain, m.requestUUID); err != nil {
			return false, fmt.Errorf("Migrate: serving=False on source pod %s: %w", pod.Name, err)
		}
	}
	if err := advanceMigrationPhase(ctx, m.input, m.requestUUID, workload.MigrationPhaseDraining, "source draining"); err != nil {
		return false, fmt.Errorf("Migrate: record Phase=Draining (uuid=%s): %w", m.requestUUID, err)
	}
	for _, pod := range m.sourcePods {
		serviceName := drainServiceForPod(m.input, m.plan, pod)
		if serviceName == "" {
			continue
		}
		drained, err := drain.IsPodDrained(ctx, m.deps.Reader(), m.input.Key.Namespace, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("Migrate: check drain on source pod %s: %w", pod.Name, err)
		}
		if !drained {
			return true, nil
		}
	}
	return false, nil
}

// deleteSource deletes the drained source and ends the pass while any
// source pod stands. The pods are read again from the live reader, since
// an empty stale view would skip the deletes and leave the pods behind
// when the source's row is removed. A pod that sits Terminating on a
// node that stopped reporting never satisfies its delete expectation, so
// the force-delete escalation runs before the expectations gate; once
// every source pod is past its own deletion deadline the record waits
// on the kubelet and says so, and the pass returns without the deletes,
// which would skip every pod anyway.
func (m *migrationPass) deleteSource(ctx context.Context) (bool, error) {
	pods, err := m.livePods(ctx, m.sourceIdx)
	if err != nil {
		return false, fmt.Errorf("Migrate: live-list source pods: %w", err)
	}
	m.sourcePods = pods
	if len(pods) == 0 {
		return false, nil
	}
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			continue
		}
		if err := escalateStuckTerminating(ctx, m.deps, m.input, pod, m.sourceIdx); err != nil {
			logf.FromContext(ctx).V(1).Info("stuck-Terminating escalation deferred",
				"pod", pod.Name, "error", err.Error())
		}
	}
	if evidence.AllPodsTerminatingOverdue(pods, m.input.Now()) {
		if err := parkMigrationOnSourceTeardown(ctx, m.deps, m.input, m.record, pods); err != nil {
			return false, fmt.Errorf("Migrate: park on source teardown (uuid=%s): %w", m.requestUUID, err)
		}
		return true, nil
	}
	if !m.deps.ExpectationsCache().Satisfied(m.input.Key.Namespace, m.input.Key.OwnerName, m.input.Key.Component, m.sourceIdx) {
		return true, nil
	}
	return true, m.deleteSourcePods(ctx)
}

// deleteSourcePods deletes every source pod not already on its way out,
// recording the expectation before each call and giving it back when the
// call fails; a pod already gone is skipped.
func (m *migrationPass) deleteSourcePods(ctx context.Context) error {
	cache := m.deps.ExpectationsCache()
	key := m.input.Key
	for _, pod := range m.sourcePods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		cache.ExpectDeletes(key.Namespace, key.OwnerName, key.Component, m.sourceIdx, 1)
		if err := m.deps.Client.Delete(ctx, pod); err != nil {
			cache.ObservedDelete(key.Namespace, key.OwnerName, key.Component, m.sourceIdx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("Migrate: delete source pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// finish runs the completion tail (finishMigration) and ends the pass;
// done reports the record closed.
func (m *migrationPass) finish(ctx context.Context) (bool, error) {
	done, err := finishMigration(ctx, m.deps, m.input, m.history, m.req, m.requestUUID, m.source, m.surge, m.sourceIdx, m.surgeIdx, m.surgeRevision.Name)
	m.done = done
	return true, err
}

// reject closes the record Failed with reason (failMigration) and ends
// the pass. The request is answered, so the caller treats it as taken
// and does not fall through.
func (m *migrationPass) reject(ctx context.Context, reason string) (bool, error) {
	done, err := failMigration(ctx, m.deps, m.input, m.history, m.req, m.requestUUID, reason)
	m.done, m.accepted = done, true
	return true, err
}

// planInstance is the plan's entry for the Instance at idx, or nil when
// the plan lists no such Instance.
func planInstance(plan workload.ComponentPlan, idx int32) *workload.InstancePlan {
	for i := range plan.Instances {
		if plan.Instances[i].Index == idx {
			return &plan.Instances[i]
		}
	}
	return nil
}

// migrationHandedOver reports whether the source has left for good: the
// record reached Draining, so the surge was in rotation when the source
// drain began, and no source pod is left. Nothing can be restored from
// here, so the completion tail runs whatever the surge's pods do.
func migrationHandedOver(record *workload.MigrationRecord, sourcePods []*corev1.Pod) bool {
	return workload.MigrationPhaseAtOrPast(record.Phase, workload.MigrationPhaseDraining) && len(sourcePods) == 0
}

// finishMigration is the completion tail, in terminal order: the surge
// promoted Ready on revisionName, the source's resources finalized and
// its row removed under its ownership guard, the Completed history row,
// then the record's terminal write. The record is written last so a
// crash in the tail leaves it non-terminal and the next pass re-runs the
// steps, which are safe to repeat; a record already Completed would
// never be picked again and the cleanup would never finish. done reports
// the record closed.
func finishMigration(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	ledger *audit.Ledger,
	req *audit.MigrationRequest,
	requestUUID string,
	source, surge *workload.InstanceStatus,
	sourceIdx, surgeIdx int32,
	revisionName string,
) (bool, error) {
	promoted, perr := promoteMigrationSurge(ctx, input, source, surge, requestUUID, revisionName)
	if perr != nil {
		return false, fmt.Errorf("Migrate: promote surge to Ready: %w", perr)
	}
	if !promoted {
		return false, nil
	}
	if source != nil && !status.MigrationSourceOwnsRemoval(source, requestUUID, surgeIdx) {
		return false, nil
	}
	removed, rerr := status.FinalizeAndRemove(ctx, deps, input, sourceIdx, source)
	if rerr != nil {
		return false, fmt.Errorf("Migrate: finalize source Instance: %w", rerr)
	}
	if !removed {
		return false, nil
	}
	promotedSurge := *surge
	promotedSurge.RunningRevision = revisionName
	status.EnterReady(&promotedSurge, input.Now())
	promotedSurge.TargetRevision = ""
	promotedSurge.Operation = nil
	confirmed, cerr := confirmMigrationCompletionPair(ctx, input, sourceIdx, &promotedSurge)
	if cerr != nil {
		return false, fmt.Errorf("Migrate: confirm promoted migration pair: %w", cerr)
	}
	if !confirmed {
		return false, nil
	}
	if err := completeMigrationTail(ctx, deps, input, ledger, req, requestUUID, sourceIdx, surgeIdx, revisionName); err != nil {
		return false, err
	}
	return true, nil
}

// migrationCompletionTailRecoverable reports a move whose surge is
// already promoted while the record still reads Draining: no source pod
// is left and the surge row reads Ready on a revision with no operation,
// so the tail resumes from the rows instead of the plan.
func migrationCompletionTailRecoverable(
	record *workload.MigrationRecord,
	sourcePods []*corev1.Pod,
	surge *workload.InstanceStatus,
) bool {
	return record.Phase == workload.MigrationPhaseDraining &&
		len(sourcePods) == 0 &&
		surge != nil && surge.Phase == workload.InstancePhaseReady &&
		surge.Operation == nil && surge.RunningRevision != ""
}

// completeMigrationTail writes the end of a move: the Completed history
// row, then the record Completed with its message and time, then the
// event.
func completeMigrationTail(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	ledger *audit.Ledger,
	req *audit.MigrationRequest,
	requestUUID string,
	sourceIdx, surgeIdx int32,
	runningRevision string,
) error {
	ledger.UpsertEntry(audit.NewTerminalEntry(*ledger.InFlightEntryOrSeed(requestUUID, req, surgeIdx), audit.PhaseCompleted, "migrated"))
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, ledgerOwnerObject(input), ledgerOwnerGVK(input), ledger); err != nil {
		return fmt.Errorf("Migrate: persist Completed ledger: %w", err)
	}
	completedMsg := fmt.Sprintf("migrated to instance=%d", surgeIdx)
	if err := input.MutateMigration(ctx, requestUUID, func(m *workload.MigrationRecord) bool {
		if m.Phase.Terminal() {
			return false
		}
		m.Phase = workload.MigrationPhaseCompleted
		m.Message = completedMsg
		now := metav1.NewTime(deps.Now())
		m.CompletedAt = &now
		return true
	}); err != nil {
		return fmt.Errorf("Migrate: record Phase=Completed (uuid=%s): %w", requestUUID, err)
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationCompleted,
		"OMENative migration uuid=%s complete: %s -> instance=%d (revision=%s)",
		requestUUID, workload.InstanceKey(input.Key.Component, sourceIdx), surgeIdx, runningRevision)
	return nil
}

// establishMigrationPair writes the pair's two rows through the batch
// writer when the rows still read as a pair this pass may write; it
// reports whether the write landed.
func establishMigrationPair(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	surge *workload.InstanceStatus,
	requestUUID string,
	surgeIdx int32,
	timeout time.Duration,
) (bool, error) {
	if !migrationPairStampable(source, surge, requestUUID, surgeIdx) {
		return false, nil
	}
	return status.StartMigration(ctx, input, source, surge, requestUUID, surgeIdx, timeout)
}

// migrationPairStampable reports whether the pair's rows may be written:
// a source that already owns the pair with its surge row absent or
// matching, or a steady source with no surge row yet.
func migrationPairStampable(
	source *workload.InstanceStatus,
	surge *workload.InstanceStatus,
	requestUUID string,
	surgeIdx int32,
) bool {
	if source == nil {
		return false
	}
	sourceOwned := status.MigrationSourceOwnsRemoval(source, requestUUID, surgeIdx)
	surgeOwned := status.MigrationSurgeOwnsPromotion(surge, requestUUID, source.Index)
	if sourceOwned {
		return surge == nil || surgeOwned
	}
	return migrationSourceSteady(source) && surge == nil
}

// migrationSourceSteady reports whether a source row is one a fresh
// record may claim: Ready on a recorded revision with no operation at
// all. An operation of any kind, recognized or not, is a claim on the
// row, and the revision is the migration's own requirement, since the
// surge is created at whatever the source is running.
func migrationSourceSteady(source *workload.InstanceStatus) bool {
	return source != nil && source.Phase == workload.InstancePhaseReady &&
		source.Operation == nil && source.RunningRevision != ""
}

// migrationSourceRevisionNotStarted reports a source Failed with no
// operation on a revision it never started (a Create or Update attempt
// the deadline decision ended), or an attempt that decision stopped, in
// either phase its pods give it; only a corrected or released revision
// moves such a row.
func migrationSourceRevisionNotStarted(source *workload.InstanceStatus) bool {
	if source == nil || source.TargetRevision == "" || source.TargetRevision == source.RunningRevision {
		return false
	}
	if workload.OperationParked(source.Operation) {
		return true
	}
	return source.Phase == workload.InstancePhaseFailed && source.Operation == nil
}

// migrationSourceRevisionNotStartedReason words that rejection: the
// revision that has not started and where its retry record stands, so
// the operator knows what to correct or release before asking again.
func migrationSourceRevisionNotStartedReason(input workload.ReconcileInput, source *workload.InstanceStatus) string {
	failureReason := ""
	if source.LastFailure != nil {
		failureReason = source.LastFailure.Reason
	}
	return fmt.Sprintf("source %s is Failed on revision %s, which has not started (%s); %s",
		workload.InstanceKey(input.Key.Component, source.Index), source.TargetRevision,
		migrationRevisionLadder(input, source.TargetRevision, failureReason), migrationRevisionAction)
}

// migrationRevisionAction closes a rejection that only a change of
// revision answers: the operator corrects or releases it and asks again.
const migrationRevisionAction = "correct or release the revision, then request the move again"

// migrationSourceRevisionHeld reports a source whose running revision
// the retry record holds, with no operation in flight to move it off
// that revision (a repair stopped on the row, or an attempt the deadline
// decision stopped, is spent, not in flight).
func migrationSourceRevisionHeld(input workload.ReconcileInput, source *workload.InstanceStatus) bool {
	return source != nil && source.RunningRevision != "" &&
		(source.Operation == nil || SpentRepair(source) || workload.OperationParked(source.Operation)) &&
		revisionHeld(input, source.RunningRevision)
}

// migrationSourceRevisionHeldReason words that rejection: the running
// revision and its retry record, and that the move would only rebuild it.
func migrationSourceRevisionHeldReason(input workload.ReconcileInput, source *workload.InstanceStatus) string {
	return fmt.Sprintf("source %s runs revision %s, which is held after its attempts (%s); the move would rebuild that revision elsewhere; %s",
		workload.InstanceKey(input.Key.Component, source.Index), source.RunningRevision,
		migrationRevisionLadder(input, source.RunningRevision, ""), migrationRevisionAction)
}

// migrationRevisionLadder words where rev's retry record stands for a
// record message: the record's state and attempts when one exists, else
// that no retry is scheduled, with failureReason when known.
func migrationRevisionLadder(input workload.ReconcileInput, rev, failureReason string) string {
	if b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev); b != nil {
		ladder := fmt.Sprintf("retry block %s after %d failed attempt(s)", b.State, b.AttemptsStarted)
		if b.Reason != "" {
			ladder += ": " + b.Reason
		}
		return ladder
	}
	ladder := "no retry is scheduled"
	if failureReason != "" {
		ladder += ": " + failureReason
	}
	return ladder
}

// migrationSourceRepairParked reports a source Failed at the revision it
// runs with no operation in flight (a repair stopped on it, a spent
// attempt, or none open) and words the wait: the failure, where the
// repair and its retry schedule stand, and that the row coming back
// Ready is what lets the move go.
func migrationSourceRepairParked(input workload.ReconcileInput, source *workload.InstanceStatus) (string, bool) {
	if source == nil || source.Phase != workload.InstancePhaseFailed || source.RunningRevision == "" ||
		(source.TargetRevision != "" && source.TargetRevision != source.RunningRevision) ||
		(source.Operation != nil && !SpentRepair(source) && !workload.OperationParked(source.Operation)) {
		return "", false
	}
	failure := ""
	if summary := RepairFailureSummary(source); summary != "" {
		failure = " (" + summary + ")"
	}
	return fmt.Sprintf("waiting for source %s, Failed on its running revision %s%s, to be Ready again: %s",
		workload.InstanceKey(input.Key.Component, source.Index), source.RunningRevision, failure,
		migrationSourceRepairStanding(input, source)), true
}

// migrationSourceRepairStanding words what holds a Failed source's
// repair, in the order the restart pass decides a stopped repair's exits:
// the repair's own retry schedule, or no repair open at all. The running
// revision's retry record holds no repair of an Instance that served it;
// a held one rejects the move instead. Where nothing automatic releases
// the hold, the operator action that does is named.
func migrationSourceRepairStanding(input workload.ReconcileInput, source *workload.InstanceStatus) string {
	const reset = "; reset the Instance or publish a corrected revision"
	switch {
	case !SpentRepair(source):
		return "no repair is open on it"
	case workload.RepairWaitsOnWorkload(source):
		return "its repair is parked, " + RepairWaitingNote
	case input.UpdateRetryPolicy == nil:
		return "its repair is parked and no retry ladder is configured to re-arm it" + reset
	case RepairRetriesExhausted(input, source):
		return fmt.Sprintf("its repair is parked after %d re-arm(s) with every retry spent", source.Operation.RetryCount) + reset
	default:
		return "its repair is parked until the retry ladder re-arms it"
	}
}

// replacementClaimsIndex reports whether another row's operation names
// idx as its replacement. A gang surge keeps that claim on its source
// until the source is finalized and removed, past the point where the
// replacement row itself reads Ready with no operation.
func replacementClaimsIndex(statuses []workload.InstanceStatus, idx int32) bool {
	for i := range statuses {
		op := statuses[i].Operation
		if statuses[i].Index != idx && op != nil && op.SurgeIndex != nil && *op.SurgeIndex == idx {
			return true
		}
	}
	return false
}

// followHandoffForPendingMigrations rebinds every pending move of the
// Instance at sourceIdx to surgeIdx, the index a gang surge rebuilt the
// Instance under. A move binds to the Instance, not to an index: a
// Manual record still Accepted with no replacement of its own names the
// Instance wherever the roll put it, and goes on from there. A record
// already executing owns its source and is left alone, as is a terminal
// one. Idempotent, so the caller runs it on every pass that reaches the
// source's removal and a crash between the two writes loses nothing.
func followHandoffForPendingMigrations(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, sourceIdx, surgeIdx int32) error {
	if input.MutateMigration == nil {
		return nil
	}
	for i := range input.ObservedState.Migrations {
		record := &input.ObservedState.Migrations[i]
		if record.Trigger != workload.MigrationTriggerManual || record.Phase.Terminal() ||
			record.SurgeAllocated() || record.SourceInstance != sourceIdx {
			continue
		}
		followed := false
		if err := input.MutateMigration(ctx, record.RequestUUID, func(m *workload.MigrationRecord) bool {
			if m.Phase.Terminal() || m.SurgeAllocated() || m.SourceInstance != sourceIdx {
				return false
			}
			m.SourceInstance = surgeIdx
			m.Message = fmt.Sprintf("following the Instance rebuilt as instance=%d", surgeIdx)
			followed = true
			return true
		}); err != nil {
			return fmt.Errorf("rebind migration uuid=%s to instance=%d: %w", record.RequestUUID, surgeIdx, err)
		}
		if !followed {
			continue
		}
		record.SourceInstance = surgeIdx
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationSourceRebuilt,
			"OMENative migration uuid=%s follows %s, rebuilt as instance=%d",
			record.RequestUUID, workload.InstanceKey(input.Key.Component, sourceIdx), surgeIdx)
	}
	return nil
}

// confirmMigrationCompletionPair proves, with a write that changes
// nothing, that the source's row is gone and the surge still reads as
// the promoted copy on the adapter's fresh rows.
func confirmMigrationCompletionPair(
	ctx context.Context,
	input workload.ReconcileInput,
	sourceIdx int32,
	surge *workload.InstanceStatus,
) (bool, error) {
	if surge == nil || surge.RunningRevision == "" ||
		!status.MigrationPromotedSurgeMatches(surge, surge.RunningRevision) {
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return true, nil
	}
	if err := status.RequireOwner(input); err != nil {
		return false, err
	}
	sourceGuard, sourceState := status.Guard(input, sourceIdx, nil)
	surgeGuard, surgeState := status.Guard(input, surge.Index, surge)
	confirmed := false
	preflight := workload.InstanceMutation{
		Index:  sourceIdx,
		Mutate: func(*workload.InstanceStatus) bool { return false },
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			confirmed = sourceGuard(snapshot) && sourceState.Absent &&
				surgeGuard(snapshot) && surgeState.Matched
			if !confirmed {
				return false
			}
			currentSurge := snapshot.Instances[surge.Index]
			confirmed = status.MigrationPromotedSurgeMatches(&currentSurge, surge.RunningRevision)
			return confirmed
		},
	}
	err := status.Apply(ctx, input, []workload.InstanceMutation{preflight})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	return confirmed, err
}

// confirmMigrationDrainPair proves, with a write that changes nothing,
// that both rows of the pair still read on the adapter's fresh rows as
// this pass saw them: the source owning the pair, the surge claimed by
// the request or, when allowPromoted is set, already promoted to
// targetRevision.
func confirmMigrationDrainPair(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	surge *workload.InstanceStatus,
	requestUUID string,
	surgeIdx int32,
	targetRevision string,
	allowPromoted bool,
) (bool, error) {
	if !status.MigrationSourceOwnsRemoval(source, requestUUID, surgeIdx) {
		return false, nil
	}
	promoted := false
	if !status.MigrationSurgeOwnsPromotion(surge, requestUUID, source.Index) {
		if !allowPromoted || !status.MigrationPromotedSurgeMatches(surge, targetRevision) {
			return false, nil
		}
		promoted = true
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return true, nil
	}
	if err := status.RequireOwner(input); err != nil {
		return false, err
	}
	sourceGuard, sourceState := status.Guard(input, source.Index, source)
	surgeGuard, surgeState := status.Guard(input, surge.Index, surge)
	confirmed := false
	preflight := workload.InstanceMutation{
		Index:  source.Index,
		Mutate: func(*workload.InstanceStatus) bool { return false },
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			if !sourceGuard(snapshot) || !sourceState.Matched ||
				!surgeGuard(snapshot) || !surgeState.Matched {
				confirmed = false
				return false
			}
			currentSurge := snapshot.Instances[surge.Index]
			confirmed = migrationDrainTargetMatches(&currentSurge, promoted, requestUUID, source.Index, targetRevision)
			return confirmed
		},
	}
	err := status.Apply(ctx, input, []workload.InstanceMutation{preflight})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	return confirmed, err
}

// promoteMigrationSurge writes the surge Ready on targetRevision while
// the source still owns the pair, or confirms a surge already promoted
// and prunes the revision's retry record; it reports whether the surge
// reads promoted.
func promoteMigrationSurge(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	surge *workload.InstanceStatus,
	requestUUID string,
	targetRevision string,
) (bool, error) {
	if source == nil || surge == nil || !status.MigrationSourceOwnsRemoval(source, requestUUID, surge.Index) {
		return false, nil
	}
	active := status.MigrationSurgeOwnsPromotion(surge, requestUUID, source.Index)
	alreadyPromoted := status.MigrationPromotedSurgeMatches(surge, targetRevision)
	if !active && !alreadyPromoted {
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		if err := status.StampReadyOnRevision(ctx, input, surge.Index, targetRevision); err != nil {
			if errors.Is(err, workload.ErrStatusOwnerGone) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if alreadyPromoted {
		confirmed, err := confirmMigrationDrainPair(
			ctx, input, source, surge, requestUUID, surge.Index, targetRevision, true,
		)
		if err != nil || !confirmed {
			return false, err
		}
		if err := status.RetryBlockPruneOnPromote(ctx, input, targetRevision); err != nil {
			return false, err
		}
		return true, nil
	}
	return status.StampMigrationSurgeReady(ctx, input, source, surge, requestUUID, targetRevision)
}

// migrationDrainTargetMatches reads the surge side of a drain pair as the
// pass classified it: still claimed by the request, or already promoted
// to targetRevision.
func migrationDrainTargetMatches(row *workload.InstanceStatus, promoted bool, requestUUID string, sourceIdx int32, targetRevision string) bool {
	if promoted {
		return status.MigrationPromotedSurgeMatches(row, targetRevision)
	}
	return status.MigrationSurgeOwnsPromotion(row, requestUUID, sourceIdx)
}

// surgeTailGatesPassed reports whether the surge Instance passes the
// drive pass's pre-drain completion gates:
//
//   - every routable surge pod (the leader; workers are never routed —
//     drainServiceForPod returns "" for them) is in rotation in its
//     per-revision Service. Rotation reads the live reader so a cold
//     informer doesn't falsely report zero slices, and without it the
//     source swap can hit a zero-routable-endpoint window.
//   - the surge InstanceStatus reports full availability: its
//     AvailablePodCount covers every pod, which counts a pod only once it
//     is in rotation and, under a positive minReadySeconds window, has
//     been Ready for at least that long.
//
// SHARED between the Migrate drive pass and the expiry pass's Draining
// carve-out (migrationTailReady): the carve-out defers expiry exactly
// when the drive can finish the tail, so the two must evaluate the SAME
// gates — a weaker expiry-side copy would defer forever on a surge the
// drive blocks on (e.g., ready-but-never-in-rotation), stranding the
// record non-terminal past its deadline.
func surgeTailGatesPassed(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, surgeIdx int32, surgePods []*corev1.Pod) (bool, error) {
	for _, pod := range surgePods {
		serviceName := drainServiceForPod(input, plan, pod)
		if serviceName == "" {
			continue
		}
		inRotation, err := drain.IsPodInRotation(ctx, deps.Reader(), input.Key.Namespace, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check rotation on surge pod %s: %w", pod.Name, err)
		}
		if !inRotation {
			return false, nil
		}
	}
	surgeStatus := input.ObservedState.Instance(surgeIdx)
	publishedPods, publishedAvailable := workload.AdapterPublished(surgeStatus)
	if publishedPods == 0 || publishedAvailable < publishedPods {
		return false, nil
	}
	return true, nil
}

// advanceMigrationPhase moves the migration record for uuid forward to
// phase (forward-only: an already-at-or-past record is untouched, so a
// stale pass can never regress the phase). message replaces the
// record's current-blocker text.
func advanceMigrationPhase(ctx context.Context, input workload.ReconcileInput, uuid string, phase workload.MigrationPhase, message string) error {
	return input.MutateMigration(ctx, uuid, func(m *workload.MigrationRecord) bool {
		if workload.MigrationPhaseAtOrPast(m.Phase, phase) {
			return false
		}
		m.Phase = phase
		m.Message = message
		return true
	})
}

// resolveSourceNode live-reads the source pods and names the node the
// surge is kept off. A request binds to the Instance; its FromNode is
// where the requester saw it. A source still on FromNode leaves it. A
// source rebuilt elsewhere since the request (a roll, a repair, a node
// loss) leaves the node it runs on now, so the overlay never excludes a
// node the Instance already left while leaving it free to land where the
// Instance actually is. For a gang, FromNode names the member to
// relocate off, and when no member sits there any more the routable
// member's node stands in, since the whole gang moves anyway.
//
// Returns (node, defer, rejection, err): defer while a source pod is
// unscheduled or every source pod is Terminating; rejection for the
// shapes that name no node to leave, no live pod at all and a single-pod
// Instance whose live pods span several nodes.
func resolveSourceNode(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx int32, fromNode string) (string, bool, string, error) {
	listed, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, sourceIdx)
	if err != nil {
		return "", false, "", fmt.Errorf("list source pods: %w", err)
	}
	if len(listed) == 0 {
		return "", false, "source instance has no live pods", nil
	}
	// Terminating pods are seconds from gone (recreate churn leaves the
	// old pod Terminating beside its replacement) — resolving against
	// them would name a node the Instance is leaving anyway. All-Terminating
	// is transient (replacements pending): defer, don't reject.
	pods := make([]*corev1.Pod, 0, len(listed))
	for _, pod := range listed {
		if pod.DeletionTimestamp != nil {
			continue
		}
		pods = append(pods, pod)
	}
	if len(pods) == 0 {
		return "", true, "", nil
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })

	if isMultiPodInstance(plan, sourceIdx) {
		// Defer while any member is still unscheduled: surge sizing and
		// the gang readiness gate need the full layout settled, and an
		// as-yet-unscheduled member could still land on FromNode.
		routable := ""
		for _, pod := range pods {
			node := pod.Spec.NodeName
			if node == "" {
				return "", true, "", nil
			}
			if routable == "" && query.RoutedRunner(pod) {
				routable = node
			}
		}
		for _, pod := range pods {
			if pod.Spec.NodeName == fromNode {
				return fromNode, false, "", nil
			}
		}
		if routable == "" {
			routable = pods[0].Spec.NodeName
		}
		return routable, false, "", nil
	}

	var observed string
	for _, pod := range pods {
		node := pod.Spec.NodeName
		if node == "" {
			// Unscheduled — defer; next reconcile re-resolves.
			return "", true, "", nil
		}
		if observed == "" {
			observed = node
			continue
		}
		if observed != node {
			return "", false, "source pods span multiple nodes (" + observed + ", " + node + ")", nil
		}
	}
	return observed, false, "", nil
}

// migrationCapacityUnconfiguredMessage is what a held request carries on
// its record while it waits for the operator's caps. Persisting it makes
// the hold edge-triggered: the Warning fires on the pass that starts the
// hold, not on every pass of it.
const migrationCapacityUnconfiguredMessage = "waiting for migration capacity policy (no lifecycle.audit configured)"

// holdMigrationForUnconfiguredCapacity records that the request is
// waiting for caps the operator has not supplied: the record keeps its
// Accepted phase and carries the reason, a Warning fires once per hold,
// and the Component condition makes the hold visible without events.
func holdMigrationForUnconfiguredCapacity(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, entry *workload.MigrationRecord, uuid string) error {
	if err := writeMigrationCapacityCondition(ctx, input, true); err != nil {
		return err
	}
	opened, err := recordMigrationWait(ctx, input, entry, migrationCapacityUnconfiguredMessage)
	if err != nil {
		return fmt.Errorf("record capacity hold: %w", err)
	}
	if !opened {
		return nil
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationPolicyUnconfigured,
		"OMENative migration uuid=%s held: %s", uuid, migrationCapacityUnconfiguredMessage)
	return nil
}

// recordMigrationWait writes msg as the reason a non-terminal record
// waits. Edge-triggered on the message: the write, and the true return
// the caller's one event hangs on, happen only on the pass whose reason
// differs from the record's, so a wait of many passes costs one write.
// The closure's own answer is the edge: a record that already reads as
// waiting under a stale observation must not re-announce.
func recordMigrationWait(ctx context.Context, input workload.ReconcileInput, entry *workload.MigrationRecord, msg string) (bool, error) {
	if entry == nil || entry.Message == msg {
		return false, nil
	}
	opened := false
	if err := input.MutateMigration(ctx, entry.RequestUUID, func(m *workload.MigrationRecord) bool {
		if m.Phase.Terminal() || m.Message == msg {
			return false
		}
		m.Message = msg
		opened = true
		return true
	}); err != nil {
		return false, err
	}
	return opened, nil
}

// migrationPausedMessage is what a fresh record carries while the
// Component pauses new surges: placement execution holds the surge
// allowance, and nothing the move does ends that.
const migrationPausedMessage = "waiting for the placement pause to lift: placement execution holds new surge allocations"

// deferMigration records why a fresh record is deferred when no
// operation in flight on its source will end the wait by itself: the
// record carries the cause until it clears, so an expiry can name it,
// and one event per cause change says so — a Warning when warn is set.
func deferMigration(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, entry *workload.MigrationRecord, msg string, warn bool) error {
	opened, err := recordMigrationWait(ctx, input, entry, msg)
	if err != nil {
		return fmt.Errorf("record deferred migration: %w", err)
	}
	if !opened {
		return nil
	}
	emit := workload.RecordNormal
	if warn {
		emit = workload.RecordWarning
	}
	emit(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationDeferred,
		"OMENative migration uuid=%s deferred: %s", entry.RequestUUID, msg)
	return nil
}

// migrationParkedMessage is what a record waiting on its source teardown
// carries: the first source pod (by name) the kubelet has not removed
// past the pod's own deadline, and, when no force-delete policy is
// configured, that nothing automated will remove it. The same across
// passes, so the write below happens once per wait.
func migrationParkedMessage(input workload.ReconcileInput, sourcePods []*corev1.Pod) string {
	name := ""
	for _, pod := range sourcePods {
		if pod != nil && (name == "" || pod.Name < name) {
			name = pod.Name
		}
	}
	msg := fmt.Sprintf("parked on source teardown: pod %s is Terminating past its own deletion deadline", name)
	if input.ForceDelete == nil {
		msg += "; no forceDelete policy is configured, so only its kubelet or an operator can remove it"
	}
	return msg
}

// parkMigrationOnSourceTeardown records that the move is waiting on a
// kubelet that missed the source pods' own deadlines: the record keeps
// its Draining phase and carries the reason, and one Warning names the
// pod. The write and the event happen on the pass that starts the wait,
// not on every pass of it. The dispatcher reads the same pods to move
// the head past the record, which the event says; the pass keeps
// tending the record and completes it once the pods are gone.
func parkMigrationOnSourceTeardown(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, entry *workload.MigrationRecord, sourcePods []*corev1.Pod) error {
	msg := migrationParkedMessage(input, sourcePods)
	opened, err := recordMigrationWait(ctx, input, entry, msg)
	if err != nil {
		return fmt.Errorf("record parked migration: %w", err)
	}
	if !opened {
		return nil
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationParked,
		"OMENative migration uuid=%s %s; later migration requests for %s dispatch ahead of it",
		entry.RequestUUID, msg, workload.InstanceKey(input.Key.Component, entry.SourceInstance))
	return nil
}

// writeMigrationCapacityCondition writes the Component-scoped
// MigrationPolicyUnconfigured condition with the pass's capacity
// answer: True while a request waits for caps nobody configured, False
// once a pass judges one against the operator's caps.
func writeMigrationCapacityCondition(ctx context.Context, input workload.ReconcileInput, unconfigured bool) error {
	var generation int64
	if input.OwnerObject != nil {
		generation = input.OwnerObject.GetGeneration()
	}
	cond := metav1.Condition{
		Type:               string(workload.ConditionMigrationPolicyUnconfigured),
		Status:             metav1.ConditionFalse,
		Reason:             string(workload.ReasonMigrationCapacityConfigured),
		Message:            "Migration capacity policy is configured",
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: generation,
	}
	if unconfigured {
		cond.Status = metav1.ConditionTrue
		cond.Reason = string(workload.ReasonMigrationCapacityUnconfigured)
		cond.Message = "lifecycle.audit is not configured; migration requests wait instead of executing"
	}
	if input.WriteAggregateCondition == nil {
		return nil
	}
	return input.WriteAggregateCondition(ctx, cond)
}

// failMigration closes the request: the history's Started row is closed
// Failed with reason as its outcome (history, in the shape a completed
// or expired row has), then the record is written Failed with the
// message and the time, which is what the dispatcher reads: it stops
// picking the record and the capacity slot is free. History first: if
// the record write crashes, the record is still non-terminal, the next
// pass meets the same rejection, and the history write repeats without
// effect. Returns done=true so the reconciler stops, and warns so the
// operator sees the rejection on the object's events. The request
// annotation is the adapter's: it is consumed at accept time and never
// touched here.
func failMigration(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, ledger *audit.Ledger, req *audit.MigrationRequest, uuid, reason string) (bool, error) {
	surgeIdx := ledgerSurgeIndex(workload.FindMigrationRecord(input.ObservedState.Migrations, uuid))
	ledger.UpsertEntry(audit.NewTerminalEntry(*ledger.InFlightEntryOrSeed(uuid, req, surgeIdx), audit.PhaseFailed, reason))
	if err := audit.PersistLedgerForOwner(ctx, deps.Client, ledgerOwnerObject(input), ledgerOwnerGVK(input), ledger); err != nil {
		return false, fmt.Errorf("Migrate: persist Failed ledger (%s): %w", reason, err)
	}
	if err := input.MutateMigration(ctx, uuid, func(m *workload.MigrationRecord) bool {
		if m.Phase.Terminal() {
			return false
		}
		m.Phase = workload.MigrationPhaseFailed
		m.Message = reason
		completed := metav1.NewTime(deps.Now())
		m.CompletedAt = &completed
		return true
	}); err != nil {
		return false, fmt.Errorf("Migrate: record Phase=Failed (%s): %w", reason, err)
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationRequestRejected,
		"OMENative migration uuid=%s rejected: %s", uuid, reason)
	return true, nil
}

// isMultiPodInstance reports whether the Instance at sourceIdx in plan
// is multi-pod (TotalPods > 1). The move reads it to give the surge the
// gang shape: copy the source's runner layout, carry the worker pod spec
// from the running revision, and wait one pass after the pair is
// written so the PodGroup pass creates the surge's PodGroup before the
// gang's pods render. False when the plan lists no Instance at that
// index; a missing source row is refused by the move itself.
func isMultiPodInstance(plan workload.ComponentPlan, sourceIdx int32) bool {
	for _, inst := range plan.Instances {
		if inst.Index == sourceIdx {
			return inst.TotalPods() > 1
		}
	}
	return false
}

// surgeRevisionAndSpec returns the source's RunningRevision CR and its
// stored payload: the leader (single-pod) PodSpec, the worker PodSpec
// for a gang, the template metadata and the pairing protocol. (nil, nil,
// nil) means no recorded RunningRevision yet OR the CR was deleted —
// caller retries.
//
// Reads the source's running revision from input.ObservedState; fetches
// the CR via deps.Reader() (live read) so a stale cache doesn't return
// a deleted CR as still-present.
func surgeRevisionAndSpec(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, sourceIdx int32) (*appsv1.ControllerRevision, *revision.DataPayload, error) {
	source := input.ObservedState.Instance(sourceIdx)
	if source == nil || source.RunningRevision == "" {
		return nil, nil, nil
	}
	cr := &appsv1.ControllerRevision{}
	if err := deps.Reader().Get(ctx, client.ObjectKey{Namespace: input.Key.Namespace, Name: source.RunningRevision}, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("get source CR %s: %w", source.RunningRevision, err)
	}
	var payload revision.DataPayload
	if err := json.Unmarshal(cr.Data.Raw, &payload); err != nil {
		return nil, nil, fmt.Errorf("unmarshal source CR data: %w", err)
	}
	if payload.PodSpec == nil {
		return nil, nil, fmt.Errorf("source CR %s missing podSpec payload", source.RunningRevision)
	}
	return cr, &payload, nil
}

// patchInstanceStatusMigrating writes the source row Migrating; the
// operation's SurgeIndex points forward at the surge.
func patchInstanceStatusMigrating(ctx context.Context, input workload.ReconcileInput, idx, surgeIdx int32, uuid string, timeout time.Duration) error {
	return status.StampMigrationPin(ctx, input, idx, surgeIdx, status.MigrationRoleSource, uuid, timeout)
}

// patchInstanceStatusMigrationSurge writes the surge row Creating; its
// SurgeIndex points back at the source, so the pair reads from either
// side.
func patchInstanceStatusMigrationSurge(ctx context.Context, input workload.ReconcileInput, surgeIdx, sourceIdx int32, uuid string, timeout time.Duration) error {
	return status.StampMigrationPin(ctx, input, surgeIdx, sourceIdx, status.MigrationRoleSurge, uuid, timeout)
}

// drainServiceForPod is query.RoutedServiceForPod under the name the
// Update and Migrate drain gates read by.
func drainServiceForPod(input workload.ReconcileInput, plan workload.ComponentPlan, pod *corev1.Pod) string {
	return query.RoutedServiceForPod(input.Key.OwnerName, plan.Component, pod)
}

// podsDrainedFromRouting reports whether every pod in pods has left the
// EndpointSlices of the per-revision routed Service it belongs to. Pods with
// no routed Service — workers, or pods missing the revision-hash label — are
// trivially drained (drainServiceForPod returns "" for them). The caller is a
// destructive step: deleting a pod the data plane is still routing to sheds
// the requests in flight to it, so the whole set gates on one answer.
func podsDrainedFromRouting(
	ctx context.Context,
	drainer *drain.Batcher,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	idx int32,
	pods []*corev1.Pod,
) (bool, error) {
	for _, pod := range pods {
		serviceName := drainServiceForPod(input, plan, pod)
		if serviceName == "" {
			continue
		}
		drained, err := drainer.IsPodDrained(ctx, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", idx, pod.Name, err)
		}
		if !drained {
			return false, nil
		}
	}
	return true, nil
}

// A surge pod the kubelet cannot start ends the move before the record's
// deadline.
//
// A surge pod held in a waiting reason it cannot get itself out of never
// runs, so the replacement it exists for never serves. Both escalation
// paths skip a row a move owns on purpose (a serving source must not be
// failed for its copy's sake), which leaves the record as the only
// reader of this condition. The read therefore lives in the migration
// pass, which already holds the surge pods, and drives the same record
// close an expiry drives: the record is the authority, its deadline the
// backstop.

// migrationSourceUnhealthyReason is the LastFailure.Reason a source
// records when its move is closed and observation finds the source
// itself not runtime-ready. It names the source's own condition, not
// the close that exposed it; the surge's condition is what the record
// and the event carry.
const migrationSourceUnhealthyReason = "MigrationClosedSourceUnhealthy"

// surgeWedgeBlocker names the first surge pod held in a terminal kubelet
// waiting reason past the configured stuck-pod grace, in the phrasing
// the record carries as its outcome.
//
// The reason set and the grace are the ones every other reader of this
// condition uses, so a pod one pass acts on is one the others recognize.
// An unconfigured grace turns the read off, as it turns off the fast
// escalation. A pod already Terminating is on its way out (the terminal
// removal or the scale-down owns it), so it proves nothing about the
// move.
func surgeWedgeBlocker(input workload.ReconcileInput, surgePods []*corev1.Pod) (string, bool) {
	if input.StuckPodGrace <= 0 {
		return "", false
	}
	now := input.Now()
	for _, pod := range surgePods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		reason, stuck := evidence.PodStuckInTerminalWaiting(pod, now, input.StuckPodGrace)
		if !stuck {
			continue
		}
		return fmt.Sprintf("surge pod %s wedged in %s past the stuck-pod grace", pod.Name, reason), true
	}
	return "", false
}

// failMigrationOnWedgedSurge closes the record Failed on blocker through
// the shared record path (the surge's claim cleared so the next plan
// tears it down as a scale-down extra, the source restored to what
// observation finds with its drain hold released) and emits the one
// Warning that names the pod and the reason. No retry record is charged:
// the failure names the image or the node the surge landed on, not an
// attempt the source is allowed to make on its own revision.
func failMigrationOnWedgedSurge(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	rec *workload.MigrationRecord,
	surgeIdx int32,
	blocker string,
) error {
	if err := failMigrationThroughRecord(ctx, deps, input, plan, rec, blocker,
		migrationSourceUnhealthyReason, blocker+"; source pods not runtime-ready"); err != nil {
		return err
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonMigrationSurgeWedged,
		"OMENative migration uuid=%s failed: %s; tearing down surge instance=%d",
		rec.RequestUUID, blocker, surgeIdx)
	return nil
}
