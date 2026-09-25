package escalation_test

// Scheduler-hold contract: a pod the scheduler reports it cannot place
// is waiting on cluster capacity, not failing. The escalation pass
// records the hold on the Instance operation, the deadline-parking step
// stops the InstanceReadyTimeout clock while it lasts, and only an
// operator-configured grace turns the hold into a terminal,
// environment-caused failure that never blames the revision.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const schedulerMessage = "0/3 nodes are available: 3 Insufficient nvidia.com/gpu"

// unschedulablePod builds a Pending pod the scheduler has ruled out,
// carrying the scheduler's own message and the moment it did so.
func unschedulablePod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.PodScheduled,
				Status:             corev1.ConditionFalse,
				Reason:             types.WaitingReasonUnschedulable,
				Message:            schedulerMessage,
				LastTransitionTime: metav1.NewTime(since),
			}},
		},
	}
}

// creatingInstance is a single-pod Create attempt with a live deadline —
// the shape a first rollout presents while its pod waits for placement.
func creatingInstance(deadline time.Time) []types.InstanceStatus {
	return []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 1,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationCreate,
			Deadline: metav1.NewTime(deadline),
		},
	}}
}

// TestEscalation_UnschedulablePodRecordsHold: with no grace configured
// the row is not failed — the hold is recorded as the operation's
// Waiting token and the scheduler's message lands on LastFailure, so
// `kubectl describe` names the constraint that has no placement.
func TestEscalation_UnschedulablePodRecordsHold(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (queued for placement, not failed)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
		t.Errorf("LastFailure: got %+v want the scheduler message %q", got.LastFailure, schedulerMessage)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (the cluster, not the revision, has no placement)", rec.blocks)
	}
}

// TestEscalation_UnschedulableHoldIsEdgeTriggered: a pod queued for
// hours re-derives the same token and message every pass, so the second
// pass writes nothing.
func TestEscalation_UnschedulableHoldIsEdgeTriggered(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	writes := 0
	inner := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
		return inner(ctx, idx, func(s *types.InstanceStatus) bool {
			changed := mutate(s)
			if changed {
				writes++
			}
			return changed
		})
	}
	pods := map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}

	for pass := 0; pass < 3; pass++ {
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1), pods); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	}

	if writes != 1 {
		t.Errorf("status writes across three passes: got %d want 1 (edge-triggered)", writes)
	}
}

// TestEscalation_UnschedulableHoldClearedOnSchedule: once the scheduler
// places the pod the hold is gone, so the token is cleared and the
// deadline clock restarts.
func TestEscalation_UnschedulableHoldClearedOnSchedule(t *testing.T) {
	now := time.Now()
	insts := creatingInstance(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = types.WaitingReasonUnschedulable
	input, rec := escalationFixture(insts)

	scheduled := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		}},
	}
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {scheduled}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want cleared", got)
	}
	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating", rec.store[0].Phase)
	}
}

// TestEscalation_UnschedulablePastGraceFailsEnvironmentCaused: past the
// configured grace the attempt ends. The evidence names the scheduler's
// message and the revision keeps a clean retry ladder — no placement is
// something no corrected revision can fix.
func TestEscalation_UnschedulablePastGraceFailsEnvironmentCaused(t *testing.T) {
	now := time.Now()
	insts := creatingInstance(now.Add(30 * time.Minute))
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
		t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, schedulerMessage) {
		t.Errorf("LastFailure.Message: got %+v want the scheduler message", got.LastFailure)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_UnschedulableWithoutGraceNeverEscalates: with the grace
// unconfigured an unplaceable pod waits for an operator. The parked
// deadline (already elapsed here, stamped before the park landed) must
// not fail it either.
func TestEscalation_UnschedulableWithoutGraceNeverEscalates(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(-time.Hour)))

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-24*time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (no grace configured, deadline parked)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestEscalation_UnschedulableGraceRunsFromCondition: the window is
// measured from the PodScheduled transition, not from pod creation, so a
// pod that only just became unplaceable is still inside a grace shorter
// than its own age.
func TestEscalation_UnschedulableGraceRunsFromCondition(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	input.UnschedulableGrace = 10 * time.Minute

	pod := unschedulablePod("engine-0-default-0", now.Add(-time.Minute))
	pod.CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Hour))
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {pod}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (one minute of hold against a ten minute grace)", rec.store[0].Phase)
	}
}

// TestReconcileGatedDeadlines_ParksUnschedulableInstance: the scheduler
// hold parks the InstanceReadyTimeout clock exactly as a quota block
// does, and releasing it restarts the clock from that point.
func TestReconcileGatedDeadlines_ParksUnschedulableInstance(t *testing.T) {
	now := time.Now()
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationCreate,
			Waiting:  types.WaitingReasonUnschedulable,
			Deadline: metav1.NewTime(now.Add(-time.Minute)),
		},
	}}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("unschedulable Deadline: got %v want zero (parked)", (*store)[0].Operation.Deadline)
	}

	released := append([]types.InstanceStatus(nil), (*store)...)
	released[0].Operation = &types.InstanceOperation{Type: types.InstanceOperationCreate}
	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, released, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines after placement: %v", err)
	}
	if (*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("released Deadline: got zero want a restarted clock")
	}
}

// TestReconcileGatedDeadlines_ParksUnschedulableGangSurgeSource: a gang
// surge creates its pods under the SURGE index while the governing
// operation and its deadline live on the SOURCE, so the source's clock
// must park on the surge row's hold.
func TestReconcileGatedDeadlines_ParksUnschedulableGangSurgeSource(t *testing.T) {
	now := time.Now()
	surge := int32(1)
	insts := []types.InstanceStatus{
		{
			Index: 0,
			Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{
				Type:       types.InstanceOperationUpdate,
				Step:       types.UpdateStepSurge,
				SurgeIndex: &surge,
				Deadline:   metav1.NewTime(now.Add(10 * time.Minute)),
			},
		},
		{
			Index: 1,
			Phase: types.InstancePhaseCreating,
			Operation: &types.InstanceOperation{
				Type:    types.InstanceOperationUpdate,
				Step:    types.UpdateStepGangSurgeTarget,
				Waiting: types.WaitingReasonUnschedulable,
			},
		},
	}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("source Deadline: got %v want zero (parked on the surge row's hold)", (*store)[0].Operation.Deadline)
	}
}

// migrationPair is the durable shape of a migration in flight: both rows
// carry a Migrate operation with its own deadline, and SurgeIndex points
// at the sibling from either side.
func migrationPair(deadline time.Time) []types.InstanceStatus {
	source, surge := int32(0), int32(1)
	op := func(sibling *int32) *types.InstanceOperation {
		return &types.InstanceOperation{
			Type:        types.InstanceOperationMigrate,
			Step:        "CreateSurge",
			RequestUUID: "mig-1",
			SurgeIndex:  sibling,
			Deadline:    metav1.NewTime(deadline),
		}
	}
	return []types.InstanceStatus{
		{Index: source, Phase: types.InstancePhaseMigrating, PodCount: 1, Operation: op(&surge)},
		{Index: surge, Phase: types.InstancePhaseCreating, PodCount: 1, Operation: op(&source)},
	}
}

// TestEscalation_UnschedulableMigrationSurgeHoldsTheSurgeRow: a
// migration's replacement has nowhere to land. The hold belongs to the
// row whose own pod cannot be placed — the surge — and the serving
// source is left untouched, since stamping a wait onto a healthy
// serving replica would be a status lie.
func TestEscalation_UnschedulableMigrationSurgeHoldsTheSurgeRow(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(migrationPair(now.Add(30 * time.Minute)))
	plan := types.ComponentPlan{Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
	}}

	if err := runEscalationPass(t, types.Deps{}, input, plan, map[int32][]*corev1.Pod{
		0: {servingPod("engine-0-default-0")},
		1: {unschedulablePod("engine-1-default-0", now.Add(-5*time.Minute))},
	}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[1].Operation; got == nil || got.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("surge Operation.Waiting: got %+v want %q", got, types.WaitingReasonUnschedulable)
	}
	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("source Operation.Waiting: got %+v want unset (its own pod is placed and serving)", got)
	}
	for _, s := range rec.store {
		if s.Phase == types.InstancePhaseFailed {
			t.Errorf("instance %d: got Failed want the migration still in flight", s.Index)
		}
	}

	// The park reaches BOTH rows: the surge through its own token, the
	// source through the SurgeIndex hop.
	parked := append([]types.InstanceStatus(nil), rec.store...)
	parkInput, store, _ := expireFixture(parked)
	if err := escalation.ReconcileGatedDeadlines(context.Background(), parkInput, parked, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	for _, s := range *store {
		if !s.Operation.Deadline.IsZero() {
			t.Errorf("instance %d Deadline: got %v want zero (parked)", s.Index, s.Operation.Deadline)
		}
	}
}

// TestEscalation_UnschedulableMigrationSurgePastGraceFails: past the
// grace the replacement's attempt ends. The surge row is stamped Failed
// with its Operation preserved — the migration record remains the pair's
// terminal authority — and no revision is blamed for a cluster with no
// room. The serving source keeps its phase.
func TestEscalation_UnschedulableMigrationSurgePastGraceFails(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(migrationPair(now.Add(30 * time.Minute)))
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"
	plan := types.ComponentPlan{Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
	}}

	if err := runEscalationPass(t, types.Deps{}, input, plan, map[int32][]*corev1.Pod{
		0: {servingPod("engine-0-default-0")},
		1: {unschedulablePod("engine-1-default-0", now.Add(-time.Hour))},
	}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[1].Phase != types.InstancePhaseFailed {
		t.Errorf("surge Phase: got %q want Failed", rec.store[1].Phase)
	}
	if rec.store[1].Operation == nil {
		t.Errorf("surge Operation: got nil want preserved (the migration record owns the pair)")
	}
	if rec.store[0].Phase != types.InstancePhaseMigrating {
		t.Errorf("source Phase: got %q want Migrating (it is serving)", rec.store[0].Phase)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_UnschedulableHoldIgnoresMessageChurn: the scheduler
// recomputes its message every cycle — node counts move as the cluster
// breathes — so the hold must be edge-triggered on the held STATE. The
// message reaches operators once, at hold start.
func TestEscalation_UnschedulableHoldIgnoresMessageChurn(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	writes := 0
	inner := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
		return inner(ctx, idx, func(s *types.InstanceStatus) bool {
			changed := mutate(s)
			if changed {
				writes++
			}
			return changed
		})
	}

	since := now.Add(-5 * time.Minute)
	for pass, message := range []string{
		schedulerMessage,
		"0/4 nodes are available: 4 Insufficient nvidia.com/gpu",
		"0/9 nodes are available: 9 Insufficient nvidia.com/gpu",
	} {
		pod := unschedulablePod("engine-0-default-0", since)
		pod.Status.Conditions[0].Message = message
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {pod}}); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	}

	if writes != 1 {
		t.Errorf("status writes across three churning messages: got %d want 1", writes)
	}
	if got := rec.store[0].LastFailure; got == nil || got.Message != schedulerMessage {
		t.Errorf("LastFailure.Message: got %+v want the message from hold start", got)
	}
}

// TestEscalation_UnschedulableYieldsToAnotherWaitingToken: an operation
// already parked on a different external wait keeps that token — the
// writer that set it owns it — and is never grace-escalated as
// unschedulable on evidence this code did not record.
func TestEscalation_UnschedulableYieldsToAnotherWaitingToken(t *testing.T) {
	now := time.Now()
	insts := creatingInstance(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = types.RejectionReasonQuotaExceeded
	refusedAt := metav1.NewTime(now)
	insts[0].Operation.CapacityRefusedAt = &refusedAt
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != types.RejectionReasonQuotaExceeded {
		t.Errorf("Operation.Waiting: got %+v want the quota token untouched", got.Operation)
	}
	if got.Phase == types.InstancePhaseFailed {
		t.Errorf("Phase: got Failed want the quota wait left to its own writer")
	}
}

// TestEscalation_UnschedulableGraceWarnsAfterAHoldWasRecorded: the hold
// records the Unschedulable reason on LastFailure a pass before the
// grace elapses. The escalation that follows must still announce itself
// — an operator watching events would otherwise see the row go Failed
// in silence.
func TestEscalation_UnschedulableGraceWarnsAfterAHoldWasRecorded(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	input.ObservedState.UpdateRevision = "own-engine-newhash"

	// Pass 1: inside the grace, so only the hold is recorded.
	input.UnschedulableGrace = time.Hour
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-10*time.Minute))}}); err != nil {
		t.Fatalf("escalation pass 1: %v", err)
	}
	if rec.store[0].LastFailure == nil {
		t.Fatalf("pass 1 must record the hold on LastFailure")
	}
	if len(rec.warns) != 0 {
		t.Fatalf("pass 1 warns: got %v want none", rec.warns)
	}

	// Pass 2: the hold outlasts the grace.
	input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	input.UnschedulableGrace = 5 * time.Minute
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-10*time.Minute))}}); err != nil {
		t.Fatalf("escalation pass 2: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", rec.store[0].Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed across both passes: got %v want exactly one", rec.warns)
	}
	if len(rec.warns) == 1 && !strings.Contains(rec.warns[0], schedulerMessage) {
		t.Errorf("warning: got %q want the scheduler message", rec.warns[0])
	}
}

// TestEscalation_UnschedulableGangPastGraceKeepsItsOperation: a gang is
// not a disposable attempt — the abandon path consumes its
// Failed-with-Operation continuation — so the grace escalation takes the
// plain stamp instead of the disposition. The revision is still
// blameless: a gang with nowhere to land says nothing about its pod
// template.
func TestEscalation_UnschedulableGangPastGraceKeepsItsOperation(t *testing.T) {
	now := time.Now()
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 2,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepGangSurgeTarget,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {
			unschedulablePod("engine-0-leader-0", now.Add(-time.Hour)),
			unschedulablePod("engine-0-worker-0", now.Add(-time.Hour)),
		}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.Operation == nil {
		t.Errorf("Operation: got nil want preserved (the abandon path consumes the continuation)")
	}
	if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
		t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_UnschedulableHoldClearedWhenPodDeleted: the pod going
// away ends the hold as surely as it being placed. The token is released
// and the parked clock restarts from the release, so the attempt gets a
// full window to try again rather than inheriting an expired one.
func TestEscalation_UnschedulableHoldClearedWhenPodDeleted(t *testing.T) {
	now := time.Now()
	insts := creatingInstance(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = types.WaitingReasonUnschedulable
	// Parked while the hold lasted.
	insts[0].Operation.Deadline = metav1.Time{}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute

	// The pod is gone: an empty bucket for the instance.
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want cleared", got)
	}
	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (a vanished pod is not a failure)", rec.store[0].Phase)
	}

	released := append([]types.InstanceStatus(nil), rec.store...)
	parkInput, store, _ := expireFixture(released)
	const timeout = 30 * time.Minute
	if err := escalation.ReconcileGatedDeadlines(context.Background(), parkInput, released, map[int32]bool{}, timeout); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	rearmed := (*store)[0].Operation.Deadline
	if rearmed.IsZero() {
		t.Fatalf("Deadline: got zero want a clock restarted at the release")
	}
	if remaining := time.Until(rearmed.Time); remaining < timeout-time.Minute {
		t.Errorf("Deadline: got %s of headroom want a full window measured from the release", remaining)
	}
}

// TestEscalation_UnschedulableDeleteRowIsLeftToDeleteBatch: a scale-down
// of a pod that never got placed still has an unschedulable pod under
// it, and DeleteBatch may be pacing the removal past any grace. The
// scheduler hold must not touch that row: DeleteBatch owns its progress
// and completion, and a Failed stamp over a teardown it is still driving
// is a status lie.
func TestEscalation_UnschedulableDeleteRowIsLeftToDeleteBatch(t *testing.T) {
	now := time.Now()
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseDeleting,
		PodCount: 1,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationDelete,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseDeleting {
		t.Errorf("Phase: got %q want Deleting (DeleteBatch owns the row)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != "" {
		t.Errorf("Operation: got %+v want no hold recorded on a teardown", got.Operation)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// routedViewClient is a fake client the source-rotation reading can
// read EndpointSlices from; with none seeded, every labeled source reads
// as routed.
func routedViewClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, discoveryv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("scheme: %v", err)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

// backoffPod builds a pod wedged in a terminal kubelet waiting state on
// a named revision — a leftover from a prior attempt when its revision
// is not the one in flight.
func backoffPod(name, revision string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{query.LabelRevisionHash: query.RevisionFromName(revision).Hash()},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
			}},
		},
	}
}

// TestEscalation_UnschedulableSurgeBlamesOnlyTheTarget: a single-pod
// surge shares its Instance index with the source, so the blamed set
// holds both. When the source is wedged on its own image and the
// replacement simply has nowhere to land, the hold escalation must judge
// the replacement alone — reading the source's pull failure would charge
// the target revision a RetryBlock for a shortage of nodes.
//
// The source is still in rotation: a source out of rotation is the
// surge's own report, and that report outranks the scheduler's on the
// row, so the grace would not be what ends the attempt.
func TestEscalation_UnschedulableSurgeBlamesOnlyTheTarget(t *testing.T) {
	now := time.Now()
	const targetRev = "own-engine-newhash"
	insts := []types.InstanceStatus{{
		Index:           0,
		Phase:           types.InstancePhaseUpdating,
		PodCount:        1,
		RunningRevision: "own-engine-oldhash",
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepSurge,
			TargetRevision: targetRev,
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = targetRev

	target := unschedulablePod("engine-0-default-1", now.Add(-time.Hour))
	target.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName(targetRev).Hash()}
	source := backoffPod("engine-0-default-0", "own-engine-oldhash")
	source.Status.Conditions = []corev1.PodCondition{{Type: query.ServingConditionType, Status: corev1.ConditionTrue}}
	if err := runEscalationPass(t, types.Deps{Client: routedViewClient(t)}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {source, target}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (no node is not the revision's fault)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want exactly one", rec.warns)
	}
}

// TestEscalation_UnschedulableSupersededLeftoverIsSilent: the
// disposition declines to act when the only workload-caused pod belongs
// to a superseded revision — charging the fresh target for a leftover's
// failure would wedge recovery. Nothing is written, so nothing may be
// announced either; an unconditional event would re-fire every pass for
// as long as the leftover lingers.
func TestEscalation_UnschedulableSupersededLeftoverIsSilent(t *testing.T) {
	now := time.Now()
	const targetRev = "own-engine-newhash"
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 1,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationCreate,
			TargetRevision: targetRev,
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = targetRev

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {
			backoffPod("engine-0-default-0", "own-engine-oldhash"),
			unschedulablePod("engine-0-default-1", now.Add(-time.Hour)),
		}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase == types.InstancePhaseFailed {
		t.Errorf("Phase: got Failed want untouched (the disposition wrote nothing)")
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none — nothing was written to announce", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none", rec.blocks)
	}
}

// TestEscalation_UnschedulableGangHoldIsOrderIndependent: pods arrive
// from an informer cache in map order, which is unstable between passes.
// A gang whose members are all unplaceable must still pick the same one
// every pass, or the recorded PodName alternates and the row rewrites
// its hold forever despite nothing having changed.
func TestEscalation_UnschedulableGangHoldIsOrderIndependent(t *testing.T) {
	now := time.Now()
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 3,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepGangSurgeTarget,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	writes := 0
	inner := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
		return inner(ctx, idx, func(s *types.InstanceStatus) bool {
			changed := mutate(s)
			if changed {
				writes++
			}
			return changed
		})
	}

	since := now.Add(-5 * time.Minute)
	leader := unschedulablePod("engine-0-leader-0", since)
	worker0 := unschedulablePod("engine-0-worker-0", since)
	worker1 := unschedulablePod("engine-0-worker-1", since)
	// Every permutation the cache could hand back.
	for pass, bucket := range [][]*corev1.Pod{
		{leader, worker0, worker1},
		{worker1, leader, worker0},
		{worker0, worker1, leader},
		{worker1, worker0, leader},
	} {
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 3),
			map[int32][]*corev1.Pod{0: bucket}); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	}

	if writes != 1 {
		t.Errorf("status writes across four cache orderings: got %d want 1", writes)
	}
	if got := rec.store[0].LastFailure; got == nil || got.PodName != leader.Name {
		t.Errorf("LastFailure.PodName: got %+v want the stable choice %s", got, leader.Name)
	}
}

// restartingInstance is a repair in flight with a live deadline — the
// shape Phase C presents while the rebuilt pod waits for placement.
func restartingInstance(deadline time.Time) []types.InstanceStatus {
	return []types.InstanceStatus{{
		Index:       0,
		Incarnation: 2,
		Phase:       types.InstancePhaseRestarting,
		PodCount:    1,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationRestart,
			Step:     "Drain",
			Reason:   "pod lost",
			Deadline: metav1.NewTime(deadline),
		},
	}}
}

// TestEscalation_UnschedulableRestartHoldsThenFailsPastGrace: a repair
// whose rebuilt pod has no placement is held, not failed — the token
// parks the InstanceReadyTimeout clock so the cluster's shortage is not
// charged to the repair, and the scheduler's own message reaches
// LastFailure. Past a configured grace the attempt ends
// environment-caused, so no revision is blamed.
func TestEscalation_UnschedulableRestartHoldsThenFailsPastGrace(t *testing.T) {
	now := time.Now()

	t.Run("held while the grace is unconfigured", func(t *testing.T) {
		input, rec := escalationFixture(restartingInstance(now.Add(30 * time.Minute)))
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseRestarting {
			t.Errorf("Phase: got %q want Restarting (queued for placement, not failed)", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
			t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
		}
		if !types.OperationExternallyHeld(got.Operation) {
			t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
		}
		if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
			t.Errorf("LastFailure: got %+v want the scheduler message", got.LastFailure)
		}
		if got.Operation != nil && got.Operation.Reason != "pod lost" {
			t.Errorf("Operation.Reason: got %q want the repair cause preserved", got.Operation.Reason)
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none", rec.blocks)
		}
	})

	t.Run("failed past the configured grace", func(t *testing.T) {
		input, rec := escalationFixture(restartingInstance(now.Add(30 * time.Minute)))
		input.UnschedulableGrace = 10 * time.Minute
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseFailed {
			t.Errorf("Phase: got %q want Failed", got.Phase)
		}
		if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
			t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
		}
	})
}

// TestEscalation_UnschedulableRollHoldsThenFailsPastGrace: an update
// whose pod the scheduler cannot place is waiting on cluster capacity.
// Both mechanisms answer it the same way — the hold takes the operation's
// waiting token, which parks the InstanceReadyTimeout clock so a
// shortage of nodes is not charged to the roll, and the scheduler's own
// message reaches LastFailure. Past a configured grace the attempt ends
// environment-caused, so the revision keeps a clean retry ladder.
func TestEscalation_UnschedulableRollHoldsThenFailsPastGrace(t *testing.T) {
	now := time.Now()
	for _, step := range []string{"InPlace", "Drain"} {
		t.Run(step+" held while the grace is unconfigured", func(t *testing.T) {
			input, rec := escalationFixture([]types.InstanceStatus{
				rollInFlight(0, step, 1, now.Add(30*time.Minute)),
			})
			if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
				map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			got := rec.store[0]
			if got.Phase != types.InstancePhaseUpdating {
				t.Errorf("Phase: got %q want Updating (queued for placement, not failed)", got.Phase)
			}
			if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
				t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
			}
			if !types.OperationExternallyHeld(got.Operation) {
				t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
			}
			if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
				t.Errorf("LastFailure: got %+v want the scheduler message", got.LastFailure)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none", rec.blocks)
			}
		})

		t.Run(step+" failed past the configured grace", func(t *testing.T) {
			input, rec := escalationFixture([]types.InstanceStatus{
				rollInFlight(0, step, 1, now.Add(30*time.Minute)),
			})
			input.UnschedulableGrace = 10 * time.Minute
			input.ObservedState.UpdateRevision = updateTarget().Name
			if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
				map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			got := rec.store[0]
			if got.Phase != types.InstancePhaseFailed {
				t.Errorf("Phase: got %q want Failed", got.Phase)
			}
			if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
				t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
			}
			if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, schedulerMessage) {
				t.Errorf("LastFailure.Message: got %+v want the scheduler message", got.LastFailure)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
			}
		})
	}
}

// TestEscalation_UnschedulableDrainHoldsThenFailsPastGrace: past the
// surge step the source is already out of rotation, so a replacement the
// scheduler cannot place leaves the Instance serving nothing — and it is
// still a wait on cluster capacity, not a fault of the roll. The hold
// takes the operation's waiting token, which parks the shared deadline so
// the drain is not failed for a shortage of nodes, and the scheduler's
// message reaches LastFailure. Past a configured grace the attempt ends
// environment-caused, with the revision's retry ladder untouched.
func TestEscalation_UnschedulableDrainHoldsThenFailsPastGrace(t *testing.T) {
	now := time.Now()

	t.Run("held while the grace is unconfigured", func(t *testing.T) {
		input, rec := escalationFixture([]types.InstanceStatus{
			rollInFlight(0, types.UpdateStepSurgeDrain, 1, now.Add(30*time.Minute)),
		})
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-1", now.Add(-5*time.Minute))}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseUpdating {
			t.Errorf("Phase: got %q want Updating (queued for placement, not failed)", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
			t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
		}
		if !types.OperationExternallyHeld(got.Operation) {
			t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
		}
		if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
			t.Errorf("LastFailure: got %+v want the scheduler message", got.LastFailure)
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none", rec.blocks)
		}
	})

	t.Run("failed past the configured grace", func(t *testing.T) {
		input, rec := escalationFixture([]types.InstanceStatus{
			rollInFlight(0, types.UpdateStepSurgeDrain, 1, now.Add(30*time.Minute)),
		})
		input.UnschedulableGrace = 10 * time.Minute
		input.ObservedState.UpdateRevision = updateTarget().Name
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-1", now.Add(-time.Hour))}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseFailed {
			t.Errorf("Phase: got %q want Failed", got.Phase)
		}
		if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
			t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
		}
		if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, schedulerMessage) {
			t.Errorf("LastFailure.Message: got %+v want the scheduler message", got.LastFailure)
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
		}
	})
}
