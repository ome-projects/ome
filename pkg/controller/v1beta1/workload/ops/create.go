package ops

import (
	"context"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/holds"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// CreateRequeueInterval is how long to wait before re-checking pod
// readiness when an Instance is still coming up, from the operator's
// lifecycle.requeue.operation. Zero means unconfigured: the caller
// requeues on the controller's rate-limited backoff instead.
func CreateRequeueInterval(input workload.ReconcileInput) time.Duration {
	return input.Requeue.Operation
}

// Create drives Create / Scale-Up toward each desired Instance.
// Multi-pass:
//
//  1. Commit selected InstanceStatus entries as Phase=Creating, then render
//     and create their missing pods with ome.io/serving=False.
//  2. Wait for runtime Ready on every pod, flip ome.io/serving=True,
//     promote InstanceStatus to Phase=Ready (Operation=nil).
//
// When target != nil the Ready promote records
// RunningRevision=target.Name so detectUpdateTrigger's fast-path can
// short-circuit. Without it, per-pod podMatchesTarget false-positives
// against post-Render pod mutations and triggers a spurious recreate.
func Create(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	return createFiltered(ctx, deps, input, plan, target, func(int32) bool { return true })
}

// CreateFreshIndices runs the Create pass for ONLY surge-free Instance
// indices — those with no in-flight surge state the Update pass owns. It
// lets a scale-up of brand-new gangs proceed, a row that lost every pod be
// rebuilt, and a Failed row already on the roll target be re-proved, even
// while a rollout is mid-flight or held on another index, instead of
// starving any of them behind the rollout.
//
// A surge-free index is immune to both corruption modes the dispatcher's
// skip-Create-while-updating gate guards against. Its ActiveOrdinal is
// one no in-flight operation is moving: a genuine 0 for an index that
// never existed, the ordinal a demoted row was promoted at, which only an
// Update flips and none owns such a row, or the ordinal a Failed row with
// no operation settled on. And its promote mis-stamps no revision: its
// pods are created carrying the target rev-hash, so
// existingPodsMatchTargetRevision promotes correctly, onto the revision a
// demoted row on the target already records, and a Failed row is admitted
// only on the target it already records. See the surgeFreeIndex predicate.
//
// The pass builds nothing for the other indices, but it still frees a
// lost node's names for every planned Instance (recoverUnknownPhaseInstances):
// a Ready row whose pods sit on a node that stopped reporting is not
// surge-free and has no attempt of its own, so this scope is the only one
// that reaches it while another Instance's roll or repair holds the pass.
// With no index to build and nothing to free the pass writes nothing.
func CreateFreshIndices(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	return createFiltered(ctx, deps, input, plan, target, func(idx int32) bool {
		return surgeFreeIndex(input, idx, target)
	})
}

// CreateCommittedIndices runs the Create pass for ONLY the Instance
// indices whose committed create this pass would FINISH. It is the pass
// a paused Component runs: the set a committed create started
// materializing is completed and promoted, while an index that never
// opened one stays empty until the pause is cleared.
//
// No-op (returns ctrl.Result{}, nil) when no index qualifies.
func CreateCommittedIndices(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if !HasCommittedCreate(input, plan, target) {
		return ctrl.Result{}, nil
	}
	return createFiltered(ctx, deps, input, plan, target, func(idx int32) bool {
		return createFinishableAt(input.ObservedState.Instance(idx), target)
	})
}

// HasCommittedCreate reports whether any planned index carries a create
// this pass would finish rather than replace. The decision layer asks it
// to decide whether the pass is worth planning and the pass asks it again
// to decide whether it has work, so both read one predicate.
func HasCommittedCreate(input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) bool {
	for _, inst := range plan.Instances {
		if createFinishableAt(input.ObservedState.Instance(inst.Index), target) {
			return true
		}
	}
	return false
}

// createFinishableAt reports whether the row carries a create the pass
// would FINISH: the Creating phase and the Create operation the
// write-ahead stamp records before the first pod of the set is created,
// on an attempt the retirement rule would leave in place.
//
// The distinction matters because the pass does not only finish an
// attempt. One whose pods it OWNS — a first materialization, which is
// what an empty RunningRevision marks — committed to a revision the
// Component no longer converges to is retired and replaced by a fresh
// attempt at the current target, with a new identity, a new deadline and
// re-rendered pods. That is a new operation, so a paused Component
// leaves such a row alone and retires it on the unpause. An attempt
// naming no revision is in the same position: the retirement rule tells
// it apart from one to finish by weighing pod evidence, which is not a
// question a hold should be answering.
//
// A row that was serving before and refills its set at the revision it
// served is never retired, so a pause finishes it: the pass adds the
// missing pods under the same operation, and holding it instead would
// leave the Instance short of its set for the length of the hold. One
// rebuilding at a roll target is in the first position: its attempt
// renders a revision other than the one it served, and a pause leaves it
// alone until the unpause retires it.
func createFinishableAt(s *workload.InstanceStatus, target *appsv1.ControllerRevision) bool {
	if workload.StateOf(s) != workload.StateCreateCreatePods {
		return false
	}
	// Nothing to retarget to, so nothing can be retired: every committed
	// set is one to finish.
	if target == nil {
		return true
	}
	return !createAttemptOwnsPods(s) || s.Operation.TargetRevision == target.Name
}

// surgeFreeIndex reports whether the Instance at idx has NO in-flight
// surge state — i.e. its ObservedState entry is absent (never created),
// mid-Create (Phase=Creating with no operation, or a Create one), settled
// Pending with no operation: the shape a Ready row is demoted to for
// losing every pod, or Failed with no operation on the roll target. The
// Create pass owns such a row and no operation is moving it, so it is
// rebuilt or re-proved at this scope as the full pass would, whether or
// not the roll around it is admitted — a podless row on the target holds
// a slot in the roll's budget, and the start that slot denies must not
// stand between the row and its own rebuild; a Failed row on the target
// beside the wreckage of a withdrawn revision would otherwise wait on
// that wreckage's cleanup, which runs for as long as a drained pod of it
// stands. Updating / Migrating carry an in-flight surge (stale
// ActiveOrdinal / RunningRevision hazard); Ready-but-degraded is
// Restart/Recreate territory; a Failed row off the target is the roll's.
// A Creating entry carrying an Update or Migrate operation is a surge
// TARGET marker owned by another index's op — overwriting it with a
// Create stamp unpins the in-flight replacement gang from the plan and
// scale-down deletes it mid-surge. All excluded.
func surgeFreeIndex(input workload.ReconcileInput, idx int32, target *appsv1.ControllerRevision) bool {
	s := input.ObservedState.Instance(idx)
	if s == nil || workload.StateOf(s) == workload.StatePending {
		return true
	}
	if s.Phase == workload.InstancePhaseFailed && s.Operation == nil {
		return target != nil && s.RunningRevision == target.Name
	}
	return s.Phase == workload.InstancePhaseCreating &&
		(s.Operation == nil || s.Operation.Type == workload.InstanceOperationCreate)
}

// createFiltered is Create restricted to the Instance indices for which
// keep returns true. Create delegates with an always-true keep;
// CreateFreshIndices delegates with the surge-free predicate. Indices
// that keep rejects are left entirely untouched this pass (not listed,
// not promoted, not duplicated).
func createFiltered(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, keep func(idx int32) bool) (ctrl.Result, error) {
	if deps.Client == nil {
		return ctrl.Result{}, fmt.Errorf("Create: nil client")
	}

	existing, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component, true)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("Create: list pods: %w", err)
	}
	byInstance := query.BucketPodsByInstanceIdx(existing)
	return createFilteredBatched(ctx, deps, input, plan, target, keep, byInstance)
}

type createStartAction struct {
	instance workload.InstancePlan
	missing  []podTarget
	// terminal holds the dead pods still occupying missing targets' names;
	// they are recycled before the targets can be created.
	terminal []*corev1.Pod
	// revision is the one every pod of the attempt renders and stamps.
	revision       string
	firstCreate    bool
	statusChanges  bool
	transition     statusTransition
	statusMutation workload.InstanceMutation
}

type createReadyAction struct {
	instance workload.InstancePlan
	existing []*corev1.Pod
	// promote is false while the pod set is below the shared promote bar:
	// the action then only writes the serving gate, because a fresh pod
	// carries the lifecycle hold and cannot reach PodReady until it is
	// released. The Ready stamp waits for the pass that observes PodReady.
	promote        bool
	wasReady       bool
	statusChanges  bool
	markedServing  []*corev1.Pod
	transition     statusTransition
	statusMutation workload.InstanceMutation
	pruneRevision  string
}

type statusTransition struct {
	index    int32
	previous *workload.InstanceStatus
	current  *workload.InstanceStatus
}

type createPassAction struct {
	start *createStartAction
	ready *createReadyAction
}

// createFilteredBatched is the status-coalesced Create path. It preserves plan
// order while batching adjacent actions behind two ordering barriers:
//
//  1. Each adjacent group of selected Creating intents is committed in one
//     status update before any corresponding Pod create.
//  2. Serving conditions are written before each adjacent Ready group is
//     committed in one status update.
//
// ScaleUpPodBatchSize bounds missing Pods selected by a pass. Each Instance is
// atomic: all of its missing Pods are selected together or the Instance is
// deferred. The first eligible Instance may exceed a positive budget and
// proceeds alone. Ready promotions do not consume the budget.
//
// The pass reads what every decision shares once (newCreatePass), decides a
// start, a finish, a retirement or nothing for each planned Instance in plan
// order (createPass.decide), writes the retirements and asks for the next
// pass when there are any, else commits the starts and finishes group by
// group, and ends with one requeue for everything it still waits on.
func createFilteredBatched(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	target *appsv1.ControllerRevision,
	keep func(idx int32) bool,
	byInstance map[int32][]*corev1.Pod,
) (ctrl.Result, error) {
	pass, err := newCreatePass(ctx, deps, input, plan, target, keep, byInstance)
	if err != nil {
		return ctrl.Result{}, err
	}
	for _, planned := range plan.Instances {
		if err := pass.decide(ctx, planned); err != nil {
			return ctrl.Result{}, err
		}
	}
	if len(pass.retirements) > 0 {
		return pass.retireOlderCreates(ctx)
	}
	if ownerPresent, err := pass.commitActions(ctx); err != nil || !ownerPresent {
		return ctrl.Result{}, err
	}
	return pass.requeue(), pass.revisionErr
}

// createPass is one run of the Create pass over the Instances the caller
// admits: what the pass read once, the start, finish or retirement it
// decided for each Instance, and the waits it collected for its one requeue.
type createPass struct {
	deps       workload.Deps
	input      workload.ReconcileInput
	plan       workload.ComponentPlan
	target     *appsv1.ControllerRevision
	keep       func(idx int32) bool
	byInstance map[int32][]*corev1.Pod
	// heldByRoll is the set of Instances the roll's partition holds; a held
	// Instance that records a revision is rebuilt on it, not on the target.
	heldByRoll map[int32]bool
	// nodeDeathAt is the moment the force-delete policy frees a pod name a
	// node that stopped reporting still holds; zero when no name waits on it.
	nodeDeathAt time.Time

	actions     []createPassAction
	retirements []retiredCreateAttempt
	selection   scaleUpSelection
	// allReady stays true while every admitted Instance is settled; the pass
	// then needs no cadence and waits on watches and on the explicit waits.
	allReady bool
	// retryBlockWait is the earliest explicit wait a retry record named.
	retryBlockWait time.Duration
	revisionErr    error
}

// newCreatePass reads what every Instance's decision shares. The lost-node
// reading runs first and over every planned Instance, not only the admitted
// ones: a pod in phase Unknown still holds its name, so the missing-pod diff
// would read that name as taken and nothing else in the pass would free it;
// the force-delete policy decides when the name is freed, and the row
// reports the wait meanwhile. An Instance such a node holds is dropped from
// this pass's selection, which keeps the pass from reading as converged, and
// nodeDeathAt carries the policy boundary the requeue waits on, because the
// node raises no event.
func newCreatePass(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, keep func(idx int32) bool, byInstance map[int32][]*corev1.Pod) (*createPass, error) {
	held, nodeDeathAt, err := recoverUnknownPhaseInstances(ctx, deps, input, plan, byInstance)
	if err != nil {
		return nil, err
	}
	if len(held) > 0 {
		admitted := keep
		keep = func(idx int32) bool { return admitted(idx) && !held[idx] }
	}
	return &createPass{
		deps:        deps,
		input:       input,
		plan:        plan,
		target:      target,
		keep:        keep,
		byInstance:  byInstance,
		heldByRoll:  RollHeldIndices(input, plan, target),
		nodeDeathAt: nodeDeathAt,
		actions:     make([]createPassAction, 0, len(plan.Instances)),
		retirements: make([]retiredCreateAttempt, 0),
		selection:   scaleUpSelection{limit: input.ScaleUpPodBatchSize},
		allReady:    len(held) == 0,
	}, nil
}

// scaleUpSelection is the pod budget of one pass (lifecycle
// scaleUpPodBatchSize). Each wave is a stable prefix of the plan order: once
// an Instance does not fit, it leads the next wave instead of being bypassed
// by smaller later Instances.
type scaleUpSelection struct {
	limit        *int32
	selectedPods int32
	closed       bool
}

// admit takes podCost from the budget and reports whether the Instance is
// selected; a refusal closes the selection for the rest of the pass.
func (s *scaleUpSelection) admit(podCost int32) bool {
	if s.closed || !scaleUpPodBatchAdmits(s.limit, s.selectedPods, podCost) {
		s.closed = true
		return false
	}
	s.selectedPods += podCost
	if s.limit != nil && s.selectedPods >= *s.limit {
		s.closed = true
	}
	return true
}

// waitNoLaterThan keeps the earliest positive explicit wait for the requeue.
func (p *createPass) waitNoLaterThan(wait time.Duration) {
	if wait > 0 && (p.retryBlockWait == 0 || wait < p.retryBlockWait) {
		p.retryBlockWait = wait
	}
}

// createCandidate is one planned Instance as the pass reads it: the row it
// observed, the pods carrying the Instance's name and, once readPodSet has
// run, the revision the attempt renders, the pod set that revision asks for,
// the names no live pod holds and the pods a promote is decided on.
type createCandidate struct {
	pass     *createPass
	planned  workload.InstancePlan
	index    int32
	observed *workload.InstanceStatus
	existing []*corev1.Pod

	revision string
	// inst is the plan entry with the runners of the attempt's revision.
	inst    workload.InstancePlan
	desired []podTarget
	missing []podTarget
	podSet  []*corev1.Pod
	finish  *createReadyAction
}

// decide walks one planned Instance through the pass's questions in order
// and records the answer on the pass: left alone, retired, a start or a
// finish. The first step that settles the Instance is the last one run; an
// error ends the whole pass.
func (p *createPass) decide(ctx context.Context, planned workload.InstancePlan) error {
	c := &createCandidate{
		pass:     p,
		planned:  planned,
		index:    planned.Index,
		observed: p.input.ObservedState.Instance(planned.Index),
		existing: p.byInstance[planned.Index],
	}
	steps := []func(context.Context) (bool, error){
		c.requireAdmitted,
		c.leaveRejectedRow,
		c.leaveRowEndedByOwnershipConflict,
		c.leaveStoppedAttemptsSet,
		c.leaveMigrationPair,
		c.readPodSet,
		c.retireOnOlderRevision,
		c.removeLeftoverPods,
		c.planStart,
		c.adoptFailedRow,
		c.requireRuntimeReady,
		c.waitForPromoteBar,
		c.planPromote,
	}
	for _, step := range steps {
		if next, err := step(ctx); !next || err != nil {
			return err
		}
	}
	return nil
}

// requireAdmitted leaves an Instance outside the pass's scope untouched: not
// listed, not promoted, not duplicated.
func (c *createCandidate) requireAdmitted(context.Context) (bool, error) {
	return c.pass.keep(c.index), nil
}

// leaveRejectedRow leaves a row the apiserver refused as the rejection left
// it until the revision in force admits a fresh attempt; the retry record's
// next due time joins the requeue.
func (c *createCandidate) leaveRejectedRow(context.Context) (bool, error) {
	refused, wait := rejectionParked(c.pass.input, c.observed)
	if refused {
		c.pass.waitNoLaterThan(wait)
	}
	return !refused, nil
}

// leaveRowEndedByOwnershipConflict leaves a row keeping an attempt the
// PodGroup ownership reading ended: the roll replaces it at a corrective
// revision and the operator's reset clears it.
func (c *createCandidate) leaveRowEndedByOwnershipConflict(context.Context) (bool, error) {
	return !workload.EndedByOwnershipVerdict(c.observed), nil
}

// leaveStoppedAttemptsSet leaves the pods of an attempt the escalation
// stopped while they still ran to the roll: nothing of the set is refilled
// or promoted here. The pass keeps its cadence while a pod of the set is
// still coming up, as for any Instance it is not done with. A stopped
// attempt whose set is gone is a fresh start and goes on.
func (c *createCandidate) leaveStoppedAttemptsSet(context.Context) (bool, error) {
	if c.observed == nil || !workload.OperationParked(c.observed.Operation) || createFreshStart(c.observed, c.existing) {
		return true, nil
	}
	if !query.AllPodsRuntimeReady(c.existing) {
		c.pass.allReady = false
	}
	return false, nil
}

// leaveMigrationPair leaves both rows of a move to the move's record while
// their claims stand: the drive moves them, and only the record's completion
// or expiry ends a claim. The pass neither refills nor promotes either row,
// whatever its pods report, and keeps its cadence so the record's deadline
// is met when no drive paces the pair.
func (c *createCandidate) leaveMigrationPair(context.Context) (bool, error) {
	if workload.Owner(c.observed) != workload.OwnerMigrate {
		return true, nil
	}
	c.pass.allReady = false
	return false, nil
}

// readPodSet names the revision the attempt renders and the pod set it is
// short of: a gang refilled beside live pods keeps the width it runs, and one
// rebuilt at the target takes the target's.
func (c *createCandidate) readPodSet(context.Context) (bool, error) {
	p := c.pass
	c.revision = createRevision(p.input, c.index, c.existing, p.target, p.heldByRoll[c.index])
	c.inst = c.planned
	c.inst.Runners = p.plan.RunnersFor(c.planned, c.revision)
	c.desired = expectedPodNamesForInstance(p.input, p.plan, c.inst)
	c.missing = missingPodTargets(c.desired, c.existing)
	return true, nil
}

// onePromoteAway reports a fully materialized, runtime-ready attempt: its
// pods are already routed, so retiring it would drop live traffic and throw
// away a warmed accelerator. It is let through to promote; the update pass
// then rolls the Instance to the new target without a gap.
func (c *createCandidate) onePromoteAway() bool {
	return len(c.missing) == 0 && len(c.existing) > 0 && query.AllPodsRuntimeReady(c.existing)
}

// retireOnOlderRevision replaces an attempt committed to a revision other
// than the target with a fresh attempt at the target, once the target's
// retry record would admit the replacement: otherwise the old attempt would
// be torn down and nothing would take its place, so the record's due time
// joins the requeue instead.
func (c *createCandidate) retireOnOlderRevision(context.Context) (bool, error) {
	p := c.pass
	retired, ok := retireSupersededCreateMutation(p.input, c.index, c.existing, p.target, p.plan.InstanceReadyTimeout)
	if !ok || c.onePromoteAway() {
		return true, nil
	}
	p.allReady = false
	if wait, denied := retargetDeniedByRetryBlock(p.input, c.index, p.target); denied {
		p.waitNoLaterThan(wait)
		return false, nil
	}
	p.retirements = append(p.retirements, retired)
	return false, nil
}

// removeLeftoverPods deletes what the attempt this one replaced left behind:
// pods that hold the names the attempt needs and carry a revision it does
// not converge to.
func (c *createCandidate) removeLeftoverPods(ctx context.Context) (bool, error) {
	p := c.pass
	stale := retiredAttemptPods(p.input, c.index, c.existing, p.target)
	if len(stale) == 0 {
		return true, nil
	}
	if _, err := deleteSupersededRevisionPods(ctx, p.deps, p.input, c.index, stale, p.target.Name); err != nil {
		return false, err
	}
	p.allReady = false
	return false, nil
}

// planStart decides the start of an Instance short of its pod set: the retry
// record and the restart policy must allow the create and the pass's pod
// budget must have room. A denied fresh start is settled (it holds no pods
// and runs no deadline) and waits on the record's own due time; a denied
// rebuild, a deferred Instance and a selected one keep the cadence. An
// Instance holding every pod it expects goes on to its finish.
func (c *createCandidate) planStart(context.Context) (bool, error) {
	if len(c.missing) == 0 {
		return true, nil
	}
	p := c.pass
	allowed, treatedReady, retryAfter := allowCreateForMissingInstance(p.input, p.plan, c.inst, c.existing, c.revision)
	p.waitNoLaterThan(retryAfter)
	if !allowed {
		if !treatedReady {
			p.allReady = false
		}
		return false, nil
	}
	p.allReady = false
	if p.selection.admit(int32(len(c.missing))) {
		p.actions = append(p.actions, createPassAction{start: c.startAction()})
	}
	return false, nil
}

// startAction is the Creating write of a selected start and what the create
// needs once it lands: the names to create, the pods in a terminal phase
// still holding them, and the revision every pod renders.
func (c *createCandidate) startAction() *createStartAction {
	p := c.pass
	now := metav1.NewTime(p.input.Now())
	mutation := status.CreatingMutation(c.index, c.inst.Incarnation, p.plan.InstanceReadyTimeout, c.revision, now)
	action := &createStartAction{
		instance:      c.inst,
		missing:       c.missing,
		terminal:      terminalTargetPods(c.existing, c.missing),
		revision:      c.revision,
		firstCreate:   c.observed == nil,
		statusChanges: mutation.Mutate(c.rowCopy()),
		transition:    statusTransition{index: c.index},
	}
	mutation.OnCommit = action.transition.capture
	action.statusMutation = mutation
	return action
}

// rowCopy is a copy of the observed row, or an empty row at the index, for a
// mutation to try so the pass learns whether the write changes anything.
func (c *createCandidate) rowCopy() *workload.InstanceStatus {
	row := workload.InstanceStatus{Index: c.index}
	if c.observed != nil {
		row = *c.observed
	}
	return &row
}

// adoptFailedRow reads a Failed row that holds every pod it expects on the
// pods at its active ordinals: it is promoted only through
// failedRowAdoptable; any other Failed row is the roll's or the retry
// record's.
func (c *createCandidate) adoptFailedRow(context.Context) (bool, error) {
	c.podSet = c.existing
	if workload.StateOf(c.observed) != workload.StateFailed {
		return true, nil
	}
	c.podSet = podsAtTargets(c.existing, c.desired)
	if failedRowAdoptable(c.pass.input, c.observed, c.podSet, c.pass.target) {
		return true, nil
	}
	if !query.AllPodsRuntimeReady(c.existing) {
		c.pass.allReady = false
	}
	return false, nil
}

// requireRuntimeReady waits for ContainersReady on every pod of the set,
// which permits the serving gate write; the Ready write itself waits for the
// shared promote bar, which the gate write is a precondition of (kubelet
// folds the gate into PodReady).
func (c *createCandidate) requireRuntimeReady(context.Context) (bool, error) {
	if query.AllPodsRuntimeReady(c.podSet) {
		return true, nil
	}
	c.pass.allReady = false
	return false, nil
}

// waitForPromoteBar builds the finish and holds it below the promote bar: the
// action then writes the serving gate alone, because a fresh pod carries the
// lifecycle hold and cannot reach PodReady until it is released. A set
// waiting out its availability window becomes promotable at a computable
// instant and needs no poll; one still waiting for kubelet to fold the gate
// into PodReady has no such instant and keeps the cadence.
func (c *createCandidate) waitForPromoteBar(context.Context) (bool, error) {
	p := c.pass
	promotable, wait := query.PodSetPromotable(c.podSet, p.plan.MinReadySeconds, p.input.Now())
	c.finish = &createReadyAction{
		instance: c.inst,
		existing: c.podSet,
		promote:  promotable,
		wasReady: c.observed != nil && c.observed.Phase == workload.InstancePhaseReady,
	}
	if promotable {
		return true, nil
	}
	p.input.PromoteWindow.Observe(wait)
	if wait == 0 {
		p.allReady = false
	}
	p.actions = append(p.actions, createPassAction{ready: c.finish})
	return false, nil
}

// planPromote is the Ready write of a set past the promote bar, on the one
// known revision every active pod carries; the target's retry record is
// released with it when the row converges on the target for the first time
// (a row that already ran the target was rebuilt for a failure the record
// counts, and the record keeps counting until the rows outlive the failure).
// With no revision the pod labels identify, the finish keeps the gate write
// alone: a settled row is left for the update pass to replace under the
// rollout budgets, and an unfinished attempt is reported, since it cannot
// drop its operation without knowing what its pods run.
func (c *createCandidate) planPromote(context.Context) (bool, error) {
	p := c.pass
	action := c.finish
	readyRevision := podSetRevision(podsAtTargets(c.podSet, c.desired), c.observed, p.target)
	if readyRevision == "" {
		action.promote = false
		p.actions = append(p.actions, createPassAction{ready: action})
		if c.observed == nil || c.observed.Phase != workload.InstancePhaseReady || c.observed.Operation != nil {
			p.allReady = false
			p.revisionErr = errors.Join(p.revisionErr, fmt.Errorf("instance %d cannot become Ready from its pod labels: %w", c.index, query.ErrRevisionUnknown))
		}
		return false, nil
	}
	mutation := status.ReadyOnRevisionMutation(c.index, readyRevision, p.input.Now())
	action.statusChanges = mutation.Mutate(c.rowCopy())
	onTarget := p.target != nil && readyRevision == p.target.Name
	if onTarget && action.statusChanges && (c.observed == nil || c.observed.RunningRevision != p.target.Name) {
		action.pruneRevision = p.target.Name
	}
	action.transition.index = c.index
	mutation.OnCommit = action.transition.capture
	action.statusMutation = mutation
	p.actions = append(p.actions, createPassAction{ready: action})
	return false, nil
}

// retireOlderCreates writes every retirement in one status update, names
// both revisions in an event per Instance and asks for the next pass at
// once; the starts and finishes decided this pass wait for it.
func (p *createPass) retireOlderCreates(ctx context.Context) (ctrl.Result, error) {
	mutations := make([]workload.InstanceMutation, 0, len(p.retirements))
	for _, retired := range p.retirements {
		mutations = append(mutations, retired.mutation)
	}
	ownerPresent, err := applyInstanceMutationsWithOutcome(ctx, p.input, mutations)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("batch retire superseded Create attempts: %w", err)
	}
	if !ownerPresent {
		return ctrl.Result{}, nil
	}
	for _, retired := range p.retirements {
		workload.RecordNormal(p.deps.Recorder, workload.EventTarget(p.input), workload.EventReasonCreateAttemptSuperseded,
			"OMENative %s retired the Create attempt pinned to %s and restarted it at %s",
			workload.InstanceKey(p.plan.Component, retired.index), retired.from, p.target.Name)
	}
	return workload.RequeueNow(), nil
}

// commitActions writes the starts and finishes in plan order, each adjacent
// group in one status update, and reports false when the owner is gone.
func (p *createPass) commitActions(ctx context.Context) (bool, error) {
	for i := 0; i < len(p.actions); {
		var (
			ownerPresent bool
			err          error
			n            int
		)
		if p.actions[i].start != nil {
			starts := p.adjacentStarts(i)
			ownerPresent, err = processStartActions(ctx, p.deps, p.input, p.plan, p.target, starts)
			n = len(starts)
		} else {
			finishes := p.adjacentFinishes(i)
			ownerPresent, err = processReadyActions(ctx, p.deps, p.input, finishes)
			n = len(finishes)
		}
		if err != nil || !ownerPresent {
			return ownerPresent, err
		}
		i += n
	}
	return true, nil
}

// adjacentStarts is the run of starts from i that commit in one status
// update with the lead: the same answer on whether the write changes the
// row, and the same revision, because a revision-targeted start also moves
// that revision's retry record and only the batch writer commits both
// changes at once. Without a batch writer each start commits alone.
func (p *createPass) adjacentStarts(i int) []*createStartAction {
	lead := p.actions[i].start
	batching := p.input.ApplyInstanceMutationsWithRetryBlock != nil ||
		(lead.revision == "" && p.input.ApplyInstanceMutations != nil)
	starts := []*createStartAction{lead}
	for j := i + 1; batching && j < len(p.actions); j++ {
		next := p.actions[j].start
		if next == nil || next.statusChanges != lead.statusChanges || next.revision != lead.revision {
			break
		}
		starts = append(starts, next)
	}
	return starts
}

// adjacentFinishes is the run of finishes from i that commit in one status
// update with the lead: the same answer on whether the write changes the
// row. Without a batch writer each finish commits alone.
func (p *createPass) adjacentFinishes(i int) []*createReadyAction {
	lead := p.actions[i].ready
	batching := p.input.ApplyInstanceMutations != nil || p.input.ApplyInstanceMutationsWithRetryBlock != nil
	finishes := []*createReadyAction{lead}
	for j := i + 1; batching && j < len(p.actions); j++ {
		next := p.actions[j].ready
		if next == nil || next.statusChanges != lead.statusChanges {
			break
		}
		finishes = append(finishes, next)
	}
	return finishes
}

// requeue is the one result of the pass: the earliest of the cadence (only
// while an Instance is still coming up) and every explicit wait the pass
// collected, where an explicit wait always beats the bare rate-limited
// backoff; a pass with everything Ready and nothing due asks for no requeue
// and waits on watches. A server-suggested throttle delay is a lower bound on
// the answer, since a requeue earlier than the server asked only earns the
// 429 again.
func (p *createPass) requeue() ctrl.Result {
	var res ctrl.Result
	promoteWait := p.input.PromoteWindow.Pending()
	nodeDeathWait := until(p.input.Now(), p.nodeDeathAt)
	if !p.allReady || p.retryBlockWait > 0 || promoteWait > 0 || nodeDeathWait > 0 {
		cadence := time.Duration(0)
		if !p.allReady {
			cadence = CreateRequeueInterval(p.input)
		}
		res = workload.PassRequeue(cadence, p.retryBlockWait, promoteWait, nodeDeathWait)
	}
	return workload.NoSoonerThan(res, p.input.Pacing.Pending())
}

func processStartActions(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	target *appsv1.ControllerRevision,
	actions []*createStartAction,
) (bool, error) {
	rev := actions[0].revision
	tmpl, found, err := createTemplate(ctx, deps.Reader(), input, plan, target, rev)
	if err != nil {
		return true, fmt.Errorf("resolve create template (revision=%s): %w", rev, err)
	}
	mutations := make([]workload.InstanceMutation, 0, len(actions))
	for _, action := range actions {
		mutations = append(mutations, action.statusMutation)
	}
	ownerPresent, err := applyCreateIntentMutations(ctx, input, mutations, rev)
	if err != nil {
		return true, fmt.Errorf("batch patch status Creating: %w", err)
	}
	if !ownerPresent {
		return false, nil
	}

	for i, action := range actions {
		if err := ctx.Err(); err != nil {
			rollbackErr := rollbackStartActions(ctx, input, actions[i:])
			return true, errors.Join(err, rollbackErr)
		}
		if action.firstCreate {
			workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonInstanceCreated,
				"OMENative %s materialized; %d pod(s) requested",
				workload.InstanceKey(input.Key.Component, action.instance.Index), len(action.missing))
		}
		// A dead pod on a stable name must be gone before the name can be
		// reused; the create waits for a later pass.
		recycling, err := recycleTerminalPods(ctx, deps, input, action.instance.Index, action.instance.Index, workload.InstanceOperationCreate, action.terminal)
		if err != nil {
			rollbackErr := rollbackStartActions(ctx, input, actions[i+1:])
			return true, errors.Join(err, rollbackErr)
		}
		if recycling {
			continue
		}
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, action.instance.Index) {
			continue
		}
		if !found {
			if err := announceRevisionGone(ctx, deps, input, action.instance.Index, rev); err != nil {
				rollbackErr := rollbackStartActions(ctx, input, actions[i+1:])
				return true, errors.Join(err, rollbackErr)
			}
			continue
		}
		if _, err := createMissingPods(ctx, deps, input, plan, action.instance, action.instance.Index, action.missing, tmpl); err != nil {
			rejection, classified := asPodRejection(err)
			switch {
			case classified && rejection.rejection.Class == workload.APIRejectionThrottled:
				// The server asked for room. Stop creating this pass, roll
				// back the intents whose pods were never attempted, and wake
				// on the server's suggested delay instead of burning the
				// caller's backoff on an error that is not a fault. The delay
				// itself is already on the pass pacing.
				if rollbackErr := rollbackStartActions(ctx, input, actions[i+1:]); rollbackErr != nil {
					return true, rollbackErr
				}
				return true, nil
			case classified:
				// Permanent (already disposed) or capacity-blocked (waiting
				// with its clock parked): either way this Instance is
				// settled for the pass and its neighbours are unaffected.
				continue
			}
			rollbackErr := rollbackStartActions(ctx, input, actions[i+1:])
			return true, errors.Join(err, rollbackErr)
		}
	}
	return true, nil
}

func processReadyActions(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, actions []*createReadyAction) (bool, error) {
	completed := make([]*createReadyAction, 0, len(actions))
	for _, action := range actions {
		if err := ctx.Err(); err != nil {
			ownerPresent, commitErr := commitReadyActions(ctx, deps, input, completed)
			if !ownerPresent {
				return false, nil
			}
			if commitErr != nil {
				return true, commitErr
			}
			return true, err
		}
		rejected := false
		for _, pod := range action.existing {
			if podreadiness.IsServing(pod) {
				continue
			}
			changed, err := putPodBackIntoService(ctx, deps, input, action.instance.Index, pod)
			if err != nil {
				// A permanently refused gate patch disposes the row; its
				// promotion is void and the neighbours go on.
				if handled, derr := disposeRejectedRowWrite(ctx, deps, input, action.instance.Index, pod.Name, err); handled {
					if derr != nil {
						return true, fmt.Errorf("dispose rejected gate patch (instance=%d, pod=%s): %w", action.instance.Index, pod.Name, derr)
					}
					rejected = true
					break
				}
				ownerPresent, commitErr := commitReadyActions(ctx, deps, input, completed)
				if !ownerPresent {
					return false, nil
				}
				markErr := fmt.Errorf("mark serving (instance=%d, pod=%s): %w", action.instance.Index, pod.Name, err)
				if commitErr != nil {
					compensateErr := rollbackReadyServing(ctx, deps, []*createReadyAction{action})
					return true, errors.Join(commitErr, compensateErr)
				}
				return true, markErr
			}
			if changed {
				action.markedServing = append(action.markedServing, pod)
			}
		}
		// Below the promote bar the gate write above is the whole action:
		// there is no status mutation to commit until a later pass observes
		// the set PodReady past its availability window.
		if action.promote && !rejected {
			completed = append(completed, action)
		}
	}
	return commitReadyActions(ctx, deps, input, completed)
}

// putPodBackIntoService writes the serving gate on a pod the Create pass
// promotes. A hold a surge's drain step left on the pod outlives the
// attempt that wrote it, so it is lifted first: the gate write alone would
// leave the pod out of rotation under a hold nothing else releases.
func putPodBackIntoService(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, pod *corev1.Pod) (bool, error) {
	var activeOrdinal int32
	if row := input.ObservedState.Instance(idx); row != nil {
		activeOrdinal = row.ActiveOrdinal
	}
	if err := liftSurgeDrainHolds(ctx, deps, idx, activeOrdinal, []*corev1.Pod{pod}); err != nil {
		return false, err
	}
	return podreadiness.MarkPodServingWithChange(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady)
}

func commitReadyActions(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, actions []*createReadyAction) (bool, error) {
	if len(actions) == 0 {
		return true, nil
	}
	mutations := make([]workload.InstanceMutation, 0, len(actions))
	for _, action := range actions {
		mutations = append(mutations, action.statusMutation)
	}
	ownerPresent, err := applyInstanceMutationsWithOutcome(ctx, input, mutations)
	if err != nil {
		compensateErr := rollbackReadyServing(ctx, deps, actions[1:])
		return true, errors.Join(fmt.Errorf("batch patch status Ready: %w", err), compensateErr)
	}
	if !ownerPresent {
		return false, nil
	}
	prunedRevisions := make(map[string]struct{})
	for i, action := range actions {
		if action.pruneRevision != "" {
			if _, pruned := prunedRevisions[action.pruneRevision]; !pruned {
				if err := status.RetryBlockPruneOnPromote(ctx, input, action.pruneRevision); err != nil {
					rollbackStatusErr := rollbackReadyStatuses(ctx, input, actions[i+1:])
					rollbackServingErr := rollbackReadyServing(ctx, deps, actions[i+1:])
					return true, errors.Join(err, rollbackStatusErr, rollbackServingErr)
				}
				prunedRevisions[action.pruneRevision] = struct{}{}
			}
		}
		if !action.wasReady {
			workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonInstanceReady,
				"OMENative %s is Ready (%d pod(s) serving)",
				workload.InstanceKey(input.Key.Component, action.instance.Index), len(action.existing))
			if earliest := earliestPodCreation(action.existing); !earliest.IsZero() {
				obsmetrics.RecordPodCreateToReady(input.Key.Namespace, input.Key.OwnerName,
					string(input.Key.Component), time.Since(earliest).Seconds())
			}
		}
	}
	return true, nil
}

func rollbackReadyStatuses(ctx context.Context, input workload.ReconcileInput, actions []*createReadyAction) error {
	mutations := make([]workload.InstanceMutation, 0, len(actions))
	for _, action := range actions {
		if mutation, ok := action.transition.rollbackMutation(); ok {
			mutations = append(mutations, mutation)
		}
	}
	if len(mutations) == 0 {
		return nil
	}
	_, err := applyInstanceMutationsWithOutcome(ctx, input, mutations)
	if err != nil {
		return fmt.Errorf("rollback unprocessed Ready status: %w", err)
	}
	return nil
}

// rollbackReadyServing takes back the serving-gate writes an action issued
// before it failed part-way, so a pod is not left in rotation for an Instance
// the pass could not record. Only the gate writes this pass made are undone:
// a promoted Instance's pods were already in rotation before the pass (the
// promote bar reads PodReady, which the gate is a precondition of), so for
// those callers this is a no-op.
func rollbackReadyServing(ctx context.Context, deps workload.Deps, actions []*createReadyAction) error {
	var rollbackErrs []error
	for _, action := range actions {
		for _, pod := range action.markedServing {
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
				rollbackErrs = append(rollbackErrs,
					fmt.Errorf("rollback serving (instance=%d, pod=%s): %w", action.instance.Index, pod.Name, err))
			}
		}
	}
	return errors.Join(rollbackErrs...)
}

func rollbackStartActions(ctx context.Context, input workload.ReconcileInput, actions []*createStartAction) error {
	mutations := make([]workload.InstanceMutation, 0, len(actions))
	for _, action := range actions {
		if mutation, ok := action.transition.rollbackMutation(); ok {
			mutations = append(mutations, mutation)
		}
	}
	if len(mutations) == 0 {
		return nil
	}
	_, err := applyInstanceMutationsWithOutcome(ctx, input, mutations)
	if err != nil {
		return fmt.Errorf("rollback unattempted Creating status: %w", err)
	}
	return nil
}

func (transition *statusTransition) capture(previous, current *workload.InstanceStatus) {
	transition.previous = previous
	transition.current = current
}

func (transition *statusTransition) rollbackMutation() (workload.InstanceMutation, bool) {
	return status.CreateRollbackMutation(transition.index, transition.previous, transition.current)
}

func scaleUpPodBatchAdmits(limit *int32, selectedPods, candidatePods int32) bool {
	if limit == nil {
		return true
	}
	if *limit <= 0 {
		return false
	}
	if selectedPods == 0 {
		return true
	}
	remaining := *limit - selectedPods
	return remaining > 0 && candidatePods <= remaining
}

func applyInstanceMutations(ctx context.Context, input workload.ReconcileInput, mutations []workload.InstanceMutation) error {
	if len(mutations) == 0 {
		return nil
	}
	if input.ApplyInstanceMutations != nil {
		return input.ApplyInstanceMutations(ctx, mutations)
	}
	for _, mutation := range mutations {
		if mutation.Remove {
			if mutation.Mutate != nil {
				return fmt.Errorf("instance mutation for index %d sets both Remove and Mutate", mutation.Index)
			}
			if input.RemoveInstance == nil {
				return fmt.Errorf("instance status removal is not configured")
			}
			if _, err := input.RemoveInstance(ctx, mutation.Index); err != nil {
				return err
			}
			continue
		}
		if mutation.Mutate == nil {
			return fmt.Errorf("instance mutation for index %d has no operation", mutation.Index)
		}
		if input.MutateInstance == nil {
			return fmt.Errorf("instance status mutation is not configured")
		}
		if err := input.MutateInstance(ctx, mutation.Index, mutation.Mutate); err != nil {
			return err
		}
	}
	return nil
}

func applyInstanceMutationsWithOutcome(ctx context.Context, input workload.ReconcileInput, mutations []workload.InstanceMutation) (bool, error) {
	if len(mutations) == 0 {
		return true, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock != nil {
		err := input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, "", nil)
		if errors.Is(err, workload.ErrStatusOwnerGone) {
			return false, nil
		}
		return true, err
	}
	return true, applyInstanceMutations(ctx, input, mutations)
}

// applyCreateIntentMutations commits the Creating intents of one group of
// starts pinned to rev, with the attempt start on rev's RetryBlock.
func applyCreateIntentMutations(ctx context.Context, input workload.ReconcileInput, mutations []workload.InstanceMutation, rev string) (bool, error) {
	if len(mutations) == 0 {
		return true, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock != nil {
		var mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition
		if rev != "" {
			mutateRetryBlock = status.RetryBlockStartAttempt
		}
		err := input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, rev, mutateRetryBlock)
		if errors.Is(err, workload.ErrStatusOwnerGone) {
			return false, nil
		}
		return true, err
	}
	if err := applyInstanceMutations(ctx, input, mutations); err != nil {
		return true, err
	}
	if rev == "" {
		return true, nil
	}
	return true, status.RetryBlockAttemptStarted(ctx, input, rev)
}

// missingPodTargets diffs the desired pod names against the live pods. A
// terminal pod is absent for this purpose: it still holds its stable name,
// but nothing will ever run in it again, so the target must be recreated.
func missingPodTargets(desired []podTarget, existing []*corev1.Pod) []podTarget {
	existingByName := query.IndexPodsByName(query.ExcludeTerminalPods(existing))
	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}
	return missing
}

// allowCreateForMissingInstance evaluates the gates that precede a Creating
// intent. A denied revision is intentionally skipped rather than reported as
// an unready create.
//
// Every pod the pass would create carries rev, the revision the attempt
// renders (createRevision), so every one of them answers to that revision's
// RetryBlock: a Held block denies the whole revision, not merely the first
// attempt at it, and a row rebuilding a member it lost waits on the record
// exactly as a fresh start does. The milder states are asked of a fresh
// start only, because they govern OPENING an attempt — a backoff still
// running, and the one-attempt-at-a-time authorization — and asking them of
// a row already materializing would deny the very attempt they authorized.
//
// A denied fresh start is settled: it holds no pods and runs no deadline, so
// the pass reports it ready and waits on the block's own wake-up. A denied
// rebuild is not — it is short of its pod set, and while it carries an
// attempt that attempt's deadline is the backstop that ends the wait — so the
// pass keeps its cadence for it.
func allowCreateForMissingInstance(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, existing []*corev1.Pod, rev string) (allowed, treatedReady bool, retryAfter time.Duration) {
	if rev != "" {
		s := input.ObservedState.Instance(inst.Index)
		if block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev); block != nil {
			if createFreshStart(s, existing) {
				attemptInFlight := anyInFlightUpdateAt(input.ObservedState.InstanceStatuses, rev) ||
					anyInFlightCreateAttempt(input, allInstances)
				if denied, wait := evaluateRetryBlockGate(block, input.Now(), attemptInFlight); denied {
					return false, true, wait
				}
			} else if block.State == workload.RetryBlockHeld {
				return false, false, 0
			}
		}
	}
	// A promoted row short of its pod set beside live pods of it is the
	// restart pass's under RecreateInstanceOnPodRestart — Ready, or
	// written Pending for losing every pod — and so is a gang that lost a
	// member: filling either pod by pod would race the whole-Instance
	// rebuild at the running revision. A pod in a terminal phase is no
	// live pod: it holds only its name, so a row whose every pod is
	// terminal is rebuilt here as a row with no pod is, whatever the
	// policy, and the terminal pods are deleted ahead of the create.
	if plan.RestartPolicy == workload.RestartPolicyRecreateInstance && len(query.ExcludeTerminalPods(existing)) > 0 {
		s := input.ObservedState.Instance(inst.Index)
		if s != nil && (s.Phase == workload.InstancePhaseReady || workload.DemotedReady(s)) {
			return false, false, 0
		}
		if _, lost := instanceLostGangMember(input, plan, s, inst.TotalPods(), existing); lost {
			return false, false, 0
		}
	}
	return true, false, 0
}

// createFreshStart reports whether the row is one the Create pass would open
// a first attempt on: no row at all; a Failed row whose operation claims no
// verb — the shape a disposed attempt and the operator reset mailbox both
// leave behind; or an attempt parked after its disposition once no live pod
// of its set is left. While a pod of a parked set stands the row is the
// roll's, whatever phase the set gives it: a member refilled or promoted
// beside the parked ones would put two revisions in one set. Any other row
// is rebuilding — it holds pods, an attempt, or both.
func createFreshStart(s *workload.InstanceStatus, existing []*corev1.Pod) bool {
	if s == nil {
		return true
	}
	if workload.OperationParked(s.Operation) {
		return len(query.ExcludeTerminalPods(existing)) == 0
	}
	return workload.StateOf(s) == workload.StateFailed && workload.ClaimOf(s) == workload.OwnerNone
}

// failedRowAdoptable reports whether the Create pass may stamp a Failed row
// Ready on the target from the pods at its active ordinals. The row claims
// no verb, every one of those pods carries the target's hash, the target's
// ladder admits, and the row either was never promoted (the adoption) or
// already runs the target (the re-prove the wreckage cleanup leaves to
// this pass). A Failed row that ran another revision is never adopted; it
// returns through the roll, the repair resume or the park's own reading.
func failedRowAdoptable(input workload.ReconcileInput, row *workload.InstanceStatus, pods []*corev1.Pod, target *appsv1.ControllerRevision) bool {
	if workload.ClaimOf(row) != workload.OwnerNone || target == nil {
		return false
	}
	if row.RunningRevision != "" && row.RunningRevision != target.Name {
		return false
	}
	return existingPodsMatchTargetRevision(pods, target) && !LadderDeniesStart(input, target.Name)
}

// podsAtTargets is the subset of existing holding one of the names in
// targets: the pods at the Instance's active ordinals, the set a promote
// is judged on. A pod at another ordinal is a surge's the row left behind.
func podsAtTargets(existing []*corev1.Pod, targets []podTarget) []*corev1.Pod {
	names := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		names[t.Name] = struct{}{}
	}
	out := make([]*corev1.Pod, 0, len(existing))
	for _, pod := range existing {
		if pod == nil {
			continue
		}
		if _, ok := names[pod.Name]; ok {
			out = append(out, pod)
		}
	}
	return out
}

// createRevision names the revision a Create attempt on Instance idx renders
// and pins: a committed attempt's pin; else the running revision, for a refill
// beside live pods of it or an Instance the roll's partition holds; else the
// target.
func createRevision(input workload.ReconcileInput, idx int32, existing []*corev1.Pod, target *appsv1.ControllerRevision, heldByRoll bool) string {
	s := input.ObservedState.Instance(idx)
	if s != nil && s.Operation != nil && s.Operation.Type == workload.InstanceOperationCreate && s.Operation.TargetRevision != "" {
		return s.Operation.TargetRevision
	}
	// A refilled member joins the set it repairs, and an Instance the roll's
	// partition holds stays on the revision it runs: rolling either is the
	// update pass's job, and the roll may not take a held Instance. Any other
	// Instance with no live pod left is rebuilt at the target.
	if s != nil && s.RunningRevision != "" && (heldByRoll || len(query.ExcludeTerminalPods(existing)) > 0) {
		return s.RunningRevision
	}
	if target != nil {
		return target.Name
	}
	return ""
}

// createAttemptOwnsPods reports whether an Instance's pods belong to a Create
// attempt that the Create pass may retire and rebuild.
//
// Two halves. Ownership: the row is the Create pass's committed attempt —
// not an empty slot it has yet to materialize, and not a row another machine
// stamped Creating for its own replacement generation. What the attempt
// builds: a first materialization, which an empty RunningRevision marks, or
// the rebuild of a promoted Instance at a roll target, whose target revision
// is one other than the revision the Instance served; such a set has never
// served and is the attempt's own. A refill of a serving materialization
// keeps the served revision as its target: its pods belong to the set that
// served, and rolling those to a new revision is the update machinery's
// business. Retiring one here would announce a retirement the pod cleanup
// then declines forever.
func createAttemptOwnsPods(row *workload.InstanceStatus) bool {
	if workload.StateOf(row) != workload.StateCreateCreatePods {
		return false
	}
	if row.RunningRevision == "" {
		return true
	}
	target := row.Operation.TargetRevision
	return target != "" && target != row.RunningRevision
}

// retargetDeniedByRetryBlock reports whether the revision a retirement would
// pin its replacement to is currently denied, and how long until it is due.
// The in-flight question is asked exactly as the first-attempt gate asks it —
// an in-flight Create may carry no TargetRevision at all, so a
// revision-scoped reading would answer "nobody is attempting this" and let a
// second attempt start at a one-at-a-time revision. Only the row being
// retired is excluded: it is pinned to the revision being left behind, and
// counting it would deny its own retarget forever.
func retargetDeniedByRetryBlock(input workload.ReconcileInput, idx int32, target *appsv1.ControllerRevision) (time.Duration, bool) {
	block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name)
	if block == nil {
		return 0, false
	}
	others := make([]workload.InstanceStatus, 0, len(input.ObservedState.InstanceStatuses))
	for _, s := range input.ObservedState.InstanceStatuses {
		if s.Index != idx {
			others = append(others, s)
		}
	}
	attemptInFlight := anyInFlightUpdateAt(others, target.Name) || anyInFlightCreateAttempt(input, idx)
	denied, wait := evaluateRetryBlockGate(block, input.Now(), attemptInFlight)
	return wait, denied
}

// retiredCreateAttempt is one superseded Create attempt: the status write
// that replaces it and the revision it was pinned to, so the pass can name
// both revisions once the write lands.
type retiredCreateAttempt struct {
	index    int32
	from     string
	mutation workload.InstanceMutation
}

// retireSupersededCreateMutation replaces a Create attempt pinned to some
// revision other than the current target with a fresh attempt at that
// target: new operation identity, the target pinned, the deadline re-armed,
// all in the same write. A spec move is not a failure of the retired revision, so
// the row never passes through Failed and no RetryBlock is recorded against
// the revision it was building.
//
// An unpinned attempt is retired only when a pod label proves that the
// attempt belongs to another revision. The mutation is preconditioned on the
// retired attempt's exact identity, so it no-ops once the replacement has
// landed.
func retireSupersededCreateMutation(input workload.ReconcileInput, idx int32, existing []*corev1.Pod, target *appsv1.ControllerRevision, timeout time.Duration) (retiredCreateAttempt, bool) {
	if target == nil {
		return retiredCreateAttempt{}, false
	}
	row := input.ObservedState.Instance(idx)
	if !createAttemptOwnsPods(row) {
		return retiredCreateAttempt{}, false
	}
	if row.Operation.TargetRevision == target.Name {
		return retiredCreateAttempt{}, false
	}
	if row.Operation.TargetRevision == "" {
		targetRevision := query.RevisionOf(target)
		provenDifferent := false
		for _, pod := range existing {
			podRevision := query.RevisionFromPod(pod)
			if !podRevision.IsZero() && !podRevision.Same(targetRevision) {
				provenDifferent = true
				break
			}
		}
		if !provenDifferent {
			return retiredCreateAttempt{}, false
		}
	}

	now := metav1.NewTime(input.Now())
	replacement := workload.InstanceOperation{
		ID:             replacementCreateOperationID(idx, target, now),
		Type:           workload.InstanceOperationCreate,
		Step:           status.CreateStepCreatePods,
		StartedAt:      now,
		LastProgressAt: now,
		Deadline:       workload.DeadlineAt(now, timeout),
		TargetRevision: target.Name,
	}
	from := row.Operation.TargetRevision
	if from == "" {
		from = query.RevisionFromPod(firstRevisionLabeledPod(existing)).String()
	}
	return retiredCreateAttempt{index: idx, from: from, mutation: status.CreateReplacementMutation(row, replacement)}, true
}

// replacementCreateOperationID names the attempt that replaces a superseded
// one. The pinned revision is part of the identity because a replacement
// written in the same second as the attempt it retires would otherwise be
// indistinguishable from it, and every precondition in this pass reads the
// operation id.
func replacementCreateOperationID(idx int32, target *appsv1.ControllerRevision, now metav1.Time) string {
	return fmt.Sprintf("create-%d-%d-%s", idx, now.Unix(), query.RevisionOf(target).Hash())
}

// firstRevisionLabeledPod returns the first pod carrying a revision-hash
// label, or nil when none does.
func firstRevisionLabeledPod(pods []*corev1.Pod) *corev1.Pod {
	for _, pod := range pods {
		if !query.RevisionFromPod(pod).IsZero() {
			return pod
		}
	}
	return nil
}

// retiredAttemptPods returns the live pods an Instance's in-flight Create
// attempt cannot converge: they carry a revision other than the one the
// attempt is pinned to, which is what a retired attempt leaves behind.
// Restricted to an attempt that owns its pods (createAttemptOwnsPods): the
// pods of a refill at the revision the Instance served belong to a serving
// materialization, and rolling those is the update machinery's business,
// not Create's.
func retiredAttemptPods(input workload.ReconcileInput, idx int32, existing []*corev1.Pod, target *appsv1.ControllerRevision) []*corev1.Pod {
	if target == nil {
		return nil
	}
	row := input.ObservedState.Instance(idx)
	if !createAttemptOwnsPods(row) || row.Operation.TargetRevision != target.Name {
		return nil
	}
	pinned := query.RevisionOf(target)
	if pinned.IsZero() {
		return nil
	}
	var stale []*corev1.Pod
	for _, pod := range existing {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		rev := query.RevisionFromPod(pod)
		if rev.IsZero() || rev.Same(pinned) {
			continue
		}
		stale = append(stale, pod)
	}
	return stale
}

// earliestPodCreation returns the oldest CreationTimestamp across the
// Instance's pods, or the zero time when the slice is empty / unstamped.
func earliestPodCreation(pods []*corev1.Pod) time.Time {
	var earliest time.Time
	for _, p := range pods {
		ct := p.CreationTimestamp.Time
		if ct.IsZero() {
			continue
		}
		if earliest.IsZero() || ct.Before(earliest) {
			earliest = ct
		}
	}
	return earliest
}

// existingPodsMatchTargetRevision reports whether every existing pod
// for an Instance carries the rev-hash label of `target`. Used by the
// Create pass's Ready promote to avoid stamping
// RunningRevision=target.Name on pods that are actually on a different
// (typically intermediate) revision after a bump during an in-flight
// surge.
//
// Returns true when:
//   - existing is non-empty AND
//   - every pod's `ome.io/revision-hash` label equals
//     RevisionHashFromControllerRevisionName(target.Name).
//
// Returns false otherwise. An empty existing slice returns false (no
// pods to promote against; caller short-circuits earlier on the
// no-missing-pods branch anyway, so this only safeguards the path
// where the slice is unexpectedly empty).
//
// A pod missing the rev-hash label is treated as a mismatch — we
// can't prove it's on target, so refuse to promote. Legacy pods
// (pre-LabelRevisionHash) would hit this path and stay on their
// prior RunningRevision; a fresh recreate would label them and the
// next pass would promote correctly.
func existingPodsMatchTargetRevision(existing []*corev1.Pod, target *appsv1.ControllerRevision) bool {
	return existingPodsMatchRevision(existing, query.RevisionOf(target))
}

func existingPodsMatchRevision(existing []*corev1.Pod, rev query.RevisionID) bool {
	if len(existing) == 0 || query.RequireNamedRevision(rev) != nil {
		return false
	}
	for _, pod := range existing {
		if pod == nil {
			return false
		}
		if !query.RevisionFromPod(pod).Same(rev) {
			return false
		}
	}
	return true
}

// podSetRevision verifies one known revision against every active pod.
// The current target alone cannot identify a pod created at an older version.
func podSetRevision(pods []*corev1.Pod, row *workload.InstanceStatus, target *appsv1.ControllerRevision) string {
	revisions := []string{query.RevisionOf(target).Name()}
	if row != nil {
		revisions = append(revisions, row.RunningRevision, row.TargetRevision)
		if row.Operation != nil {
			revisions = append(revisions, row.Operation.TargetRevision)
		}
	}
	for _, name := range revisions {
		if existingPodsMatchRevision(pods, query.RevisionFromName(name)) {
			return name
		}
	}
	return ""
}

// podTarget describes one pod to create within an Instance.
type podTarget struct {
	Name    string
	Runner  workload.RunnerPlan
	Ordinal int32
}

// expectedPodNamesForInstance enumerates every (Runner, ordinal)
// tuple the Instance plan asks for.
//
// Single-pod Runners (Size==1) read the ordinal slot from the
// Instance's recorded ActiveOrdinal — SurgeThenDrain alternates
// between slots 0 and 1 across rollouts. Without this lookup the
// post-surge Create pass would hard-code ordinal 0 and create a
// duplicate pod alongside the (now ordinal-1) canonical pod.
//
// Multi-pod Runners (Size>1) keep enumerating every 0..Size-1
// ordinal; per-gang-member surge allocation isn't yet plumbed.
func expectedPodNamesForInstance(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) []podTarget {
	var targets []podTarget
	for _, runner := range inst.Runners {
		if runner.Size == 1 {
			ordinal := activeOrdinalForInstance(input, inst.Index)
			targets = append(targets, podTarget{
				Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, ordinal),
				Runner:  runner,
				Ordinal: ordinal,
			})
			continue
		}
		for o := int32(0); o < runner.Size; o++ {
			targets = append(targets, podTarget{
				Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, o),
				Runner:  runner,
				Ordinal: o,
			})
		}
	}
	return targets
}

// activeOrdinalForInstance returns the InstanceStatus.ActiveOrdinal
// for idx, or 0 when no status exists yet. Single-pod
// Create / Restart / recreateUpdate paths consult this to address the
// post-surge canonical slot (which may be 1, not 0).
func activeOrdinalForInstance(input workload.ReconcileInput, idx int32) int32 {
	if s := input.ObservedState.Instance(idx); s != nil {
		return s.ActiveOrdinal
	}
	return 0
}

// createMissingPods renders and creates targets, owning the per-pod
// ExpectCreates bookkeeping: idx is the expectations bucket (inst.Index for
// the Create pass and for a repair's rebuild), and callers must not call
// ExpectCreates themselves. tmpl pairs the template the pods render from
// with the revision they are labelled with: a non-zero revision lands on
// ome.io/revision-hash so per-revision Services can select the pod, and
// names the revision a rejection is charged against.
//
// Every create path in the engine funnels through here, so this is where an
// apiserver rejection is read rather than passed up opaquely: the classified
// outcome is a *podRejectionError, and only an unclassified (transient)
// rejection stays a *podCreateError, which callers propagate. The steps:
// provision every target's capacity; per target, render, record the
// expectation and create, and answer a refusal; last, release the waits a
// refused pass left on the row.
func createMissingPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, idx int32, targets []podTarget, tmpl podTemplate) (int, error) {
	if len(targets) == 0 {
		return 0, nil
	}
	// A gang whose PodGroup name this owner cannot write (held by another
	// controller, or by an object still being collected) gets no members:
	// every pod carries that name, so creating one would enlist it in a group
	// this owner does not control. Nothing created is the wait the caller
	// reports; the escalation pass owns the row from there.
	if input.Gangs.BlocksPods(idx) {
		return 0, nil
	}
	if err := query.RequireNamedRevision(tmpl.revision); err != nil {
		return 0, fmt.Errorf("cannot create pods for instance %d: %w", idx, err)
	}
	// Capacity the provisioner has not readied withholds the whole batch:
	// nothing created is the same wait, and the hold pass reports it.
	selectors, ready, err := provisionPods(ctx, deps, input, plan, inst, targets)
	if err != nil || !ready {
		return 0, err
	}
	set := newPodSetCreate(deps, input, plan, inst, idx, tmpl)
	created := 0
	for i, t := range targets {
		pod, err := set.render(ctx, t, selectors[i])
		if err != nil {
			return created, err
		}
		switch err := set.create(ctx, pod); {
		case err == nil:
			created++
		case apierrors.IsAlreadyExists(err):
			// A previous pass created it; the cache has not caught up yet.
		default:
			return created, set.answerRefusal(ctx, t, err)
		}
	}
	return created, set.releaseWaits(ctx)
}

// podSetCreate is the creation of one Instance's missing pods: the template
// and revision every pod renders, the plan carrying that revision's pairing
// protocol, the gang's peer host list built once, and the expectations
// bucket the creates are counted in.
type podSetCreate struct {
	deps  workload.Deps
	input workload.ReconcileInput
	plan  workload.ComponentPlan
	// renderPlan is plan with the pairing protocol of the revision being
	// rendered, which is part of the revision and not of the plan's current
	// template.
	renderPlan   workload.ComponentPlan
	inst         workload.InstancePlan
	idx          int32
	tmpl         podTemplate
	revisionHash string
}

func newPodSetCreate(deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, idx int32, tmpl podTemplate) *podSetCreate {
	renderPlan := plan
	renderPlan.PairingProtocol = tmpl.pairingProtocol
	// The gang's peer-DNS host list is the same for every pod of the
	// Instance, so it is built once here rather than once per pod inside
	// Render. inst is a value copy, so the list stays with this create. A
	// single-pod Instance has no peers.
	if inst.PeerHostnames == nil && inst.TotalPods() > 1 {
		inst.PeerHostnames = buildInstancePeerHostnames(input.Key.OwnerName, plan.Component, inst)
	}
	return &podSetCreate{
		deps:         deps,
		input:        input,
		plan:         plan,
		renderPlan:   renderPlan,
		inst:         inst,
		idx:          idx,
		tmpl:         tmpl,
		revisionHash: tmpl.revision.Hash(),
	}
}

// render builds the pod of one target: the runner's template, the
// provisioner's node selector merged in, the revision label checked against
// the template it identifies, and the advisory warning for a gang worker
// with no co-location rule.
func (s *podSetCreate) render(ctx context.Context, t podTarget, selector map[string]string) (*corev1.Pod, error) {
	template := s.tmpl.podSpec
	if t.Runner.Name == workload.RunnerWorker && s.tmpl.workerPodSpec != nil {
		template = s.tmpl.workerPodSpec
	}
	pod, err := RenderWithRevision(
		s.input.OwnerObject,
		s.input.OwnerGVK,
		s.input.Key,
		template,
		s.tmpl.meta,
		s.renderPlan,
		s.inst,
		t.Runner,
		t.Ordinal,
		s.revisionHash,
		s.deps.RenderHook,
	)
	if err != nil {
		return nil, fmt.Errorf("render pod %s: %w", t.Name, err)
	}
	if err := confine(pod, selector); err != nil {
		return nil, fmt.Errorf("confine pod %s: %w", t.Name, err)
	}
	if !query.RevisionFromPod(pod).Same(s.tmpl.revision) {
		return nil, fmt.Errorf("cannot create pod %s: revision label %q does not identify template %s", t.Name, pod.Labels[query.LabelRevisionHash], s.tmpl.revision.Name())
	}
	if err := maybeWarnGangSplitRisk(ctx, s.deps, s.input, s.plan, s.inst, t.Runner, pod, len(selector) > 0); err != nil {
		return nil, err
	}
	return pod, nil
}

// create issues the pod create with its expectation recorded first and given
// back on error: a failed create fires no watch event to decrement it.
func (s *podSetCreate) create(ctx context.Context, pod *corev1.Pod) error {
	key := s.input.Key
	s.deps.ExpectationsCache().ExpectCreates(key.Namespace, key.OwnerName, key.Component, s.idx, 1)
	err := s.deps.Client.Create(ctx, pod)
	if err != nil {
		s.deps.ExpectationsCache().ObservedCreate(key.Namespace, key.OwnerName, key.Component, s.idx)
	}
	return err
}

// answerRefusal reads the apiserver's refusal of one pod create into the
// outcome every create path agrees on. A permanent refusal ends the attempt,
// blaming the revision unless the pod carried a move's placement overlay the
// revision never asked for; a capacity refusal is recorded on the row, and
// an event names it when it becomes the row's reported wait, which the hold
// pass decides by precedence; an admission outage is recorded in the
// apiserver's words and retried on the ordinary create interval; a throttle
// deposits the server's delay on the pass, so whichever operation issued the
// create requeues no earlier. Each is returned classified; an unclassified
// refusal stays a plain create error the caller propagates.
func (s *podSetCreate) answerRefusal(ctx context.Context, t podTarget, err error) error {
	rejection := evidence.ClassifyAPIError(err)
	switch rejection.Class {
	case workload.APIRejectionPermanentWorkload, workload.APIRejectionPermanentEnvironment:
		blameRevision := s.inst.MigrationOverlay == nil
		if derr := disposeRejectedAttempt(ctx, s.deps, s.input, s.idx, s.tmpl.revision.Name(), t.Name, rejection, blameRevision); derr != nil {
			return fmt.Errorf("dispose rejected create (instance=%d, pod=%s): %w", s.idx, t.Name, derr)
		}
		return &podRejectionError{podName: t.Name, rejection: rejection, disposed: true, err: err}
	case workload.APIRejectionCapacityBlocked:
		recorded, incumbent, merr := status.RecordCapacityRefusal(ctx, s.input, s.idx)
		if merr != nil {
			return fmt.Errorf("record quota refusal (instance=%d, pod=%s): %w", s.idx, t.Name, merr)
		}
		if recorded && holds.Enters(workload.RejectionReasonQuotaExceeded, incumbent) {
			workload.RecordNormal(s.deps.Recorder, workload.EventTarget(s.input), workload.EventReasonInstanceQuotaBlocked,
				"OMENative %s waiting on capacity: %s", workload.InstanceKey(s.input.Key.Component, s.idx), rejection.Message)
		}
		return &podRejectionError{podName: t.Name, rejection: rejection, err: err}
	case workload.APIRejectionAdmissionUnavailable:
		if rerr := recordAdmissionWait(ctx, s.deps, s.input, s.plan, s.idx, t.Name, rejection); rerr != nil {
			return fmt.Errorf("record admission wait (instance=%d, pod=%s): %w", s.idx, t.Name, rerr)
		}
		return &podRejectionError{podName: t.Name, rejection: rejection, err: err}
	case workload.APIRejectionThrottled:
		s.input.Pacing.Observe(rejection.RetryAfter)
		return &podRejectionError{podName: t.Name, rejection: rejection, err: err}
	}
	return &podCreateError{podName: t.Name, err: err}
}

// releaseWaits retires the quota refusal and the admission wait a refused
// pass left on the row: every target was placed or already existed and went
// through admission, so the row is no longer blocked and its held deadline
// runs again from now.
func (s *podSetCreate) releaseWaits(ctx context.Context) error {
	if err := clearCapacityRefusal(ctx, s.input, s.idx); err != nil {
		return fmt.Errorf("clear quota refusal (instance=%d): %w", s.idx, err)
	}
	if err := releaseAdmissionWait(ctx, s.input, s.idx); err != nil {
		return fmt.Errorf("release admission wait (instance=%d): %w", s.idx, err)
	}
	return nil
}

// provisionPods asks deps.Provisioner to place every target before any
// is created, and reports ready only when all of them are: an Instance's
// pods start together, so one created ahead of a withheld peer would
// only hold capacity for an Instance that cannot run. Every target is
// asked even after one is withheld, so provisioning starts for all of
// them on the same pass. The selectors are index-aligned with targets;
// a nil provisioner places every target unconfined.
func provisionPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, targets []podTarget) ([]map[string]string, bool, error) {
	selectors := make([]map[string]string, len(targets))
	if deps.Provisioner == nil {
		return selectors, true, nil
	}
	ready := true
	for i, t := range targets {
		selector, placed, err := deps.Provisioner.Place(ctx, input, plan, inst, t.Runner, t.Ordinal)
		if err != nil {
			return nil, false, fmt.Errorf("place pod %s: %w", t.Name, err)
		}
		selectors[i] = selector
		ready = ready && placed
	}
	return selectors, ready, nil
}

// confine merges the provisioner's node selector into the rendered pod.
// A key the template already selects on with a different value would
// leave the pod unschedulable on the capacity readied for it, so it is
// an error rather than an override either way.
func confine(pod *corev1.Pod, selector map[string]string) error {
	if len(selector) == 0 {
		return nil
	}
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string, len(selector))
	}
	for k, v := range selector {
		if current, ok := pod.Spec.NodeSelector[k]; ok && current != v {
			return fmt.Errorf("node selector %s=%s conflicts with the provisioned %s", k, current, v)
		}
		pod.Spec.NodeSelector[k] = v
	}
	return nil
}

// until converts an absolute policy boundary into the delay a requeue
// takes. A zero or already-passed boundary is no delay at all — there is
// nothing left to wait for, and asking for a zero RequeueAfter would
// read as "no requeue requested".
func until(now, at time.Time) time.Duration {
	if at.IsZero() {
		return 0
	}
	if wait := at.Sub(now); wait > 0 {
		return wait
	}
	return 0
}

// maybeWarnGangSplitRisk emits a one-shot Warning when a multi-node gang
// WORKER pod is about to be created with no co-location podAffinity at
// all. Such a gang's pods may land in separate network / NVLink / TPU
// topology domains, which breaks the tightly-coupled collectives a
// multi-node runtime needs (NCCL/RCCL/NIXL all-reduce, multi-host TPU
// sessions). Advisory only — it never blocks the create.
//
// It reads the ALREADY-RENDERED pod, so it inherits injectGangDomainAffinity's
// exact decision instead of re-deriving it (no logic drift):
//   - a resolved Component topologyKey means the injector added a required
//     term -> not at risk;
//   - a hand-written operator podAffinity is preserved on the pod → not
//     at risk;
//   - a worker the provisioner confined shares its gang's capacity (see
//     Provisioner.Place) → not at risk;
//   - only a worker left with zero required podAffinity trips the warning.
//
// Accelerator-agnostic by construction: it inspects podAffinity, never the
// requested resource kind, so GPU/RDMA/TPU/any future gang are covered the
// same way. Announced once per episode on the Instance's own row, so the
// warning reaches an operator once per workload rather than once per
// controller process. nil-safe.
func maybeWarnGangSplitRisk(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, pod *corev1.Pod, confined bool) error {
	// Only a multi-node gang WORKER can split. The leader is the domain
	// anchor (carries no co-location term by design); single-pod Instances
	// have nothing to co-locate.
	if runner.Name != workload.RunnerWorker || !instanceHasLeader(inst) {
		return nil
	}
	// A required podAffinity term — OME-injected or operator-supplied —
	// or the provisioner's confinement means the gang is already pinned
	// to one domain.
	if pod == nil || confined || hasAnyRequiredPodAffinity(pod) {
		return nil
	}
	target := workload.EventTarget(input)
	if target == nil || deps.Recorder == nil {
		return nil
	}
	announced, err := status.Announce(ctx, input, inst.Index, workload.EventReasonGangSplitRisk)
	if err != nil || !announced {
		return err
	}
	deps.Recorder.Eventf(target, corev1.EventTypeWarning, string(workload.EventReasonGangSplitRisk),
		"OMENative component=%s is a multi-node gang but its worker pods have no co-location podAffinity: "+
			"the gang may be scheduled across separate network/NVLink/TPU topology domains, breaking the "+
			"tightly-coupled collectives a multi-node runtime needs (NCCL/RCCL/NIXL all-reduce, multi-host "+
			"TPU sessions). Set %s.topologyKey to the node label that identifies the required topology "+
			"domain, or declare a worker podAffinity.",
		plan.Component, plan.Component)
	return nil
}

// hasAnyRequiredPodAffinity reports whether pod declares at least one
// required (hard) podAffinity term — the signal that some co-location
// constraint, OME-injected or operator-supplied, is in force.
func hasAnyRequiredPodAffinity(pod *corev1.Pod) bool {
	return pod.Spec.Affinity != nil &&
		pod.Spec.Affinity.PodAffinity != nil &&
		len(pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0
}
