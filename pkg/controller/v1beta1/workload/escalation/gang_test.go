package escalation_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Gang contract: the Instance's PodGroup says whether the deterministic
// name is usable by this owner and whether the group has to be rebuilt.
// A name being collected parks the InstanceReadyTimeout clock; a name
// held by another controller ends the attempt, because nothing this
// controller does frees it; a group the scheduler has failed ends
// nothing, because the PodGroup pass resets it.
//
// Whether a gang can be PLACED is not read off the PodGroup: the gang
// scheduler writes that verdict onto the member pods, where the
// pod-level scheduler hold picks it up.

const (
	gangFailedMessage      = "PodGroup svc-engine-0 reported phase Failed: 2 of 2 members failed"
	gangConflictMessage    = "PodGroup svc-engine-0 is controlled by StatefulSet/legacy, not by this owner"
	gangTerminatingMessage = "PodGroup svc-engine-0 is terminating; the gang is announced again once it is collected"
	// coschedulingMessage is the shape the gang scheduler writes onto a
	// member it rejects because the whole gang cannot be placed.
	coschedulingMessage = "0/3 nodes are available: cannot find enough sibling pods for PodGroup svc-engine-0"
)

// gangObservations wires one Instance's PodGroup classification into the
// reconcile input, the way the PodGroup pass does before the dispatcher.
func gangObservations(idx int32, state types.GangState, message string) *types.GangObservations {
	g := types.NewGangObservations()
	g.Record(idx, types.GangObservation{Name: "svc-engine-0", State: state, Message: message})
	return g
}

// creatingGang is a two-pod Create attempt with a live deadline — the
// shape a first gang rollout presents while the scheduler decides.
func creatingGang(deadline time.Time) []types.InstanceStatus {
	return []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 2,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationCreate,
			Deadline: metav1.NewTime(deadline),
		},
	}}
}

// pendingGangPods are gang members that exist but are not yet placed,
// with no verdict of their own — so only the PodGroup can explain why
// the gang is not starting.
func pendingGangPods() map[int32][]*corev1.Pod {
	return map[int32][]*corev1.Pod{0: {
		{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-leader-0"},
			Status: corev1.PodStatus{Phase: corev1.PodPending}},
		{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-worker-0"},
			Status: corev1.PodStatus{Phase: corev1.PodPending}},
	}}
}

// TestEscalation_GangTerminatingParksWithoutEscalating: a name still
// held by an object being collected is the one gang wait, and it
// resolves itself — so it records the token and the PodGroup's message
// and never escalates, however long it lasts.
func TestEscalation_GangTerminatingParksWithoutEscalating(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = gangObservations(0, types.GangStateTerminating, gangTerminatingMessage)
	input.UnschedulableGrace = time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (the predecessor object is still being collected)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonPodGroupTerminating {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonPodGroupTerminating)
	}
	if got.LastFailure == nil || got.LastFailure.Message != gangTerminatingMessage {
		t.Errorf("LastFailure: got %+v want the PodGroup message", got.LastFailure)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestEscalation_GangHoldIsEdgeTriggered: a gang waiting on a name for
// hours re-derives the same token, message and moment every pass, so
// only the first pass writes.
func TestEscalation_GangHoldIsEdgeTriggered(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = gangObservations(0, types.GangStateTerminating, gangTerminatingMessage)
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

	for pass := 0; pass < 3; pass++ {
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), nil); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	}

	if writes != 1 {
		t.Errorf("status writes across three passes: got %d want 1 (edge-triggered)", writes)
	}
}

// TestEscalation_GangHoldClearedOnceCollected: with the old object gone
// the ensure pass announces the gang again, so the token is released by
// the authority that took it and the deadline clock restarts.
func TestEscalation_GangHoldClearedOnceCollected(t *testing.T) {
	now := time.Now()
	insts := creatingGang(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = types.WaitingReasonPodGroupTerminating
	input, rec := escalationFixture(insts)
	// The old object is gone, which is what an absent reading means.
	input.Gangs = gangObservations(0, types.GangStateNone, "")

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want cleared", got)
	}
	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating", rec.store[0].Phase)
	}
}

// TestEscalation_GangHoldTakesOverFromClearedPodToken: the pod-level
// hold ends and the gang's name goes into collection in the SAME pass.
// Eligibility is judged on the fresh row, so the gang token lands
// immediately — refusing on the pass-start copy would leave the row
// unheld and restart a whole InstanceReadyTimeout window next pass.
func TestEscalation_GangHoldTakesOverFromClearedPodToken(t *testing.T) {
	now := time.Now()
	insts := creatingGang(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = types.WaitingReasonUnschedulable
	input, rec := escalationFixture(insts)
	input.Gangs = gangObservations(0, types.GangStateTerminating, gangTerminatingMessage)

	// Every member is placed now, so the pod-level hold releases its
	// token on this same pass.
	scheduled := map[int32][]*corev1.Pod{0: {
		{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-leader-0"},
			Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			}}},
	}}
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), scheduled); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonPodGroupTerminating {
		t.Errorf("Operation.Waiting: got %+v want %q on the same pass the pod token cleared",
			got.Operation, types.WaitingReasonPodGroupTerminating)
	}
	if !types.OperationExternallyHeld(got.Operation) {
		t.Errorf("Operation: got %+v want still externally held so the deadline stays parked", got.Operation)
	}
}

// TestEscalation_GangOwnershipConflictFailsOnlyItsRow: a deterministic
// name held by another controller is terminal for the Instance that
// wanted it and invisible to every other Instance in the same pass.
func TestEscalation_GangOwnershipConflictFailsOnlyItsRow(t *testing.T) {
	now := time.Now()
	insts := append(creatingGang(now.Add(30*time.Minute)), types.InstanceStatus{
		Index:    1,
		Phase:    types.InstancePhaseCreating,
		PodCount: 2,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	})
	input, rec := escalationFixture(insts)
	input.Gangs = gangObservations(0, types.GangStateOwnershipConflict, gangConflictMessage)
	plan := singleInstancePlan(0, 2)
	plan.Instances = append(plan.Instances, types.InstancePlan{
		Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 2}},
	})

	pods := pendingGangPods()
	pods[1] = pods[0]
	if err := runEscalationPass(t, types.Deps{}, input, plan, pods); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != types.PodGroupOwnershipConflictReason {
		t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.PodGroupOwnershipConflictReason)
	}
	if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, "StatefulSet/legacy") {
		t.Errorf("LastFailure.Message: got %+v want the foreign controller named", got.LastFailure)
	}
	if rec.store[1].Phase != types.InstancePhaseCreating {
		t.Errorf("sibling Phase: got %q want Creating (the conflict is scoped to one row)", rec.store[1].Phase)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
}

// TestEscalation_GangHoldKeepsOffTeardowns: DeleteBatch owns a teardown's
// progress and may pace it for longer than any wait, and a gang on its
// way out has nothing left to announce.
func TestEscalation_GangHoldKeepsOffTeardowns(t *testing.T) {
	now := time.Now()
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseDeleting,
		PodCount: 2,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationDelete,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(-time.Hour)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.Gangs = gangObservations(0, types.GangStateTerminating, gangTerminatingMessage)

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseDeleting {
		t.Errorf("Phase: got %q want Deleting", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want none (DeleteBatch owns the teardown)", got.Operation)
	}
}

// TestEscalation_GangUnschedulableArrivesOnMemberPods: the gang
// scheduler reports an unplaceable gang by rejecting its MEMBERS, so the
// Instance is held on the pod-level scheduler token with the
// coscheduling explanation preserved — nothing has to read it off the
// PodGroup, whose status cannot express it.
func TestEscalation_GangUnschedulableArrivesOnMemberPods(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = types.NewGangObservations()

	rejected := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-leader-0"},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             types.WaitingReasonUnschedulable,
			Message:            coschedulingMessage,
			LastTransitionTime: metav1.NewTime(now.Add(-5 * time.Minute)),
		}}},
	}
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {rejected}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || got.LastFailure.Message != coschedulingMessage {
		t.Errorf("LastFailure: got %+v want the gang scheduler's own explanation", got.LastFailure)
	}
	if got.Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (queued for placement, not failed)", got.Phase)
	}
}

// TestReconcileGatedDeadlines_ParksGangSurgeSourceOnMemberRejection: a
// gang surge creates its members under the SURGE index while the
// governing operation and its deadline live on the SOURCE, so a member
// the gang scheduler rejected parks the source's clock too.
func TestReconcileGatedDeadlines_ParksGangSurgeSourceOnMemberRejection(t *testing.T) {
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

// TestReconcileGatedDeadlines_ParksGangTerminatingSurgeSource: the same
// SurgeIndex hop for the gang's own wait — a surge whose PodGroup name
// is still occupied must not burn the source's clock.
func TestReconcileGatedDeadlines_ParksGangTerminatingSurgeSource(t *testing.T) {
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
				Waiting: types.WaitingReasonPodGroupTerminating,
			},
		},
	}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("source Deadline: got %v want zero (parked on the surge row's gang hold)", (*store)[0].Operation.Deadline)
	}
}

// TestEscalation_GangFailedNeitherHoldsNorEnds: a failed group is
// recoverable — the PodGroup pass deletes it and builds a fresh one — so
// the row takes no token, keeps its phase, and announces nothing. Ending
// the attempt here would fail it again on every rebuild, because the
// replacement is created under the same deterministic name.
func TestEscalation_GangFailedNeitherHoldsNorEnds(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = gangObservations(0, types.GangStateFailed, gangFailedMessage)

	for pass := 0; pass < 2; pass++ {
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (the group is reset, not the row)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want none (a reset is not a wait)", got.Operation)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none across two passes", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none", rec.blocks)
	}
	// The deadline the broad backstop relies on must keep running: a reset
	// loop that never converges still has to end somewhere.
	if types.OperationExternallyHeld(got.Operation) {
		t.Errorf("Operation: got %+v want not externally held", got.Operation)
	}
}

// TestEscalation_GangTerminalKeepsOffRowsOwnedElsewhere: a teardown
// belongs to DeleteBatch and a migration to its record, and both stay
// planned while they run — so a foreign-owned PodGroup name reaches
// those rows and must not flip a phase out from under the machine
// driving them.
func TestEscalation_GangTerminalKeepsOffRowsOwnedElsewhere(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase types.InstancePhase
		op    *types.InstanceOperation
	}{
		{"teardown", types.InstancePhaseDeleting, &types.InstanceOperation{
			Type: types.InstanceOperationDelete, Step: "Drain"}},
		{"migration", types.InstancePhaseMigrating, &types.InstanceOperation{
			Type: types.InstanceOperationMigrate, Step: "CreateSurge"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			tc.op.Deadline = metav1.NewTime(now.Add(-time.Hour))
			insts := []types.InstanceStatus{{
				Index: 0, Phase: tc.phase, PodCount: 2, Operation: tc.op,
			}}
			input, rec := escalationFixture(insts)
			input.Gangs = gangObservations(0, types.GangStateOwnershipConflict, gangConflictMessage)

			if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			got := rec.store[0]
			if got.Phase != tc.phase {
				t.Errorf("Phase: got %q want %q (another machine owns this row)", got.Phase, tc.phase)
			}
			if len(rec.warns) != 0 {
				t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
			}
		})
	}
}

// TestEscalation_GangReadingSkipsSurgeSource: a gang-surge SOURCE is
// serving its own group while the attempt in flight builds members under
// the SURGE index. Its own group going into collection says nothing
// about that attempt, so the source is untouched and only the surge row
// carries a wait.
func TestEscalation_GangReadingSkipsSurgeSource(t *testing.T) {
	now := time.Now()
	surge := int32(1)
	insts := []types.InstanceStatus{
		{
			Index: 0, Phase: types.InstancePhaseUpdating, PodCount: 2,
			Operation: &types.InstanceOperation{
				Type:       types.InstanceOperationUpdate,
				Step:       types.UpdateStepSurge,
				SurgeIndex: &surge,
				Deadline:   metav1.NewTime(now.Add(10 * time.Minute)),
			},
		},
		{
			Index: 1, Phase: types.InstancePhaseCreating, PodCount: 2,
			Operation: &types.InstanceOperation{
				Type:     types.InstanceOperationUpdate,
				Step:     types.UpdateStepGangSurgeTarget,
				Deadline: metav1.NewTime(now.Add(10 * time.Minute)),
			},
		},
	}
	input, rec := escalationFixture(insts)
	g := types.NewGangObservations()
	g.Record(0, types.GangObservation{Name: "svc-engine-0", State: types.GangStateTerminating, Message: gangTerminatingMessage})
	g.Record(1, types.GangObservation{Name: "svc-engine-1", State: types.GangStateTerminating, Message: gangTerminatingMessage})
	input.Gangs = g
	plan := singleInstancePlan(0, 2)
	plan.Instances = append(plan.Instances, types.InstancePlan{
		Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 2}},
	})

	if err := runEscalationPass(t, types.Deps{}, input, plan, nil); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("source Operation.Waiting: got %+v want none (its replacement carries the wait)", got)
	}
	if got := rec.store[1].Operation; got == nil || got.Waiting != types.WaitingReasonPodGroupTerminating {
		t.Errorf("surge Operation.Waiting: got %+v want %q", got, types.WaitingReasonPodGroupTerminating)
	}
}

// TestEscalation_PodHoldWinsOverGangHold: a row whose members the
// scheduler refuses AND whose group name is being collected reports the
// pod hold. Both waits are live and the token is free, so the order the
// marks run in decides — and it favours the wait that can end: the
// collection resolves itself with nobody acting, while only the pod hold
// carries an operator-configured grace.
func TestEscalation_PodHoldWinsOverGangHold(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingGang(now.Add(30 * time.Minute)))
	input.Gangs = gangObservations(0, types.GangStateTerminating, gangTerminatingMessage)
	input.UnschedulableGrace = 10 * time.Minute

	rejected := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-engine-0-leader-0"},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             types.WaitingReasonUnschedulable,
			Message:            coschedulingMessage,
			LastTransitionTime: metav1.NewTime(now.Add(-time.Minute)),
		}}},
	}
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {rejected}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q, the wait a grace can end",
			got.Operation, types.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || got.LastFailure.Message != coschedulingMessage {
		t.Errorf("LastFailure: got %+v want the scheduler's own explanation", got.LastFailure)
	}
	if !types.OperationExternallyHeld(got.Operation) {
		t.Errorf("Operation: got %+v want externally held so the deadline stays parked", got.Operation)
	}
	if got.Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (inside the grace)", got.Phase)
	}
}

// TestEscalation_GangReadingsOnAFailedRowAreInert: a row already stamped
// Failed carries no attempt for a wait to park and no verdict left to
// raise, and the escalation pass skips it before it reads any gang
// evidence. Every reading therefore reaches it inert — no waiting token,
// no phase change, no warning and no RetryBlock — whichever one it is.
// Withholding the members and resetting the group are the PodGroup
// pass's business; the row is not where they show up.
func TestEscalation_GangReadingsOnAFailedRowAreInert(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   types.GangState
		message string
	}{
		{"terminating", types.GangStateTerminating, "PodGroup svc-engine-0 is terminating; the gang is announced again once it is collected"},
		{"ownership conflict", types.GangStateOwnershipConflict, "PodGroup svc-engine-0 is controlled by StatefulSet/other, not by this owner"},
		{"phase failed", types.GangStateFailed, gangFailedMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, rec := escalationFixture([]types.InstanceStatus{{
				Index:    0,
				Phase:    types.InstancePhaseFailed,
				PodCount: 2,
			}})
			input.Gangs = gangObservations(0, tc.state, tc.message)

			if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			got := rec.store[0]
			if got.Phase != types.InstancePhaseFailed {
				t.Errorf("Phase: got %q want Failed (a spent row is not re-ended)", got.Phase)
			}
			if got.Operation != nil {
				t.Errorf("Operation: got %+v want none (a Failed row has no clock to park)", got.Operation)
			}
			if len(rec.warns) != 0 {
				t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none", rec.blocks)
			}
		})
	}
}

// restartingGang is a two-pod repair in flight with a live deadline —
// the shape Phase B presents while it waits for the Instance's group.
func restartingGang(deadline time.Time) []types.InstanceStatus {
	return []types.InstanceStatus{{
		Index:       0,
		Incarnation: 2,
		Phase:       types.InstancePhaseRestarting,
		PodCount:    2,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationRestart,
			Step:     "Drain",
			Reason:   "pod lost",
			Deadline: metav1.NewTime(deadline),
		},
	}}
}

// TestEscalation_GangReadingsOnARepairRow: a repair meets the same four
// gang readings a create does and answers them the same way — a name
// being collected is a wait that resolves itself, a name held by another
// controller ends the repair, the scheduler's Failed phase resets the
// group without touching the row, and an absent object releases the
// token the row was carrying.
func TestEscalation_GangReadingsOnARepairRow(t *testing.T) {
	now := time.Now()

	t.Run("terminating parks the repair", func(t *testing.T) {
		input, rec := escalationFixture(restartingGang(now.Add(30 * time.Minute)))
		input.Gangs = gangObservations(0, types.GangStateTerminating, gangTerminatingMessage)
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), nil); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseRestarting {
			t.Errorf("Phase: got %q want Restarting", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonPodGroupTerminating {
			t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonPodGroupTerminating)
		}
		if !types.OperationExternallyHeld(got.Operation) {
			t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
		}
		if len(rec.warns) != 0 {
			t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
		}
	})

	t.Run("ownership conflict ends the repair", func(t *testing.T) {
		input, rec := escalationFixture(restartingGang(now.Add(30 * time.Minute)))
		input.Gangs = gangObservations(0, types.GangStateOwnershipConflict, gangConflictMessage)
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseFailed {
			t.Errorf("Phase: got %q want Failed", got.Phase)
		}
		if got.LastFailure == nil || got.LastFailure.Reason != types.PodGroupOwnershipConflictReason {
			t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.PodGroupOwnershipConflictReason)
		}
		if got.Operation == nil {
			t.Errorf("Operation: got none want the repair preserved for the abandon path")
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
		}
	})

	t.Run("the scheduler's failed phase leaves the row alone", func(t *testing.T) {
		input, rec := escalationFixture(restartingGang(now.Add(30 * time.Minute)))
		input.Gangs = gangObservations(0, types.GangStateFailed, gangFailedMessage)
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseRestarting {
			t.Errorf("Phase: got %q want Restarting (the group is reset, not the row)", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != "" {
			t.Errorf("Operation.Waiting: got %+v want none (a reset is not a wait)", got.Operation)
		}
		if len(rec.warns) != 0 || len(rec.blocks) != 0 {
			t.Errorf("warns=%v blocks=%v want neither", rec.warns, rec.blocks)
		}
	})

	t.Run("an absent group releases the token", func(t *testing.T) {
		insts := restartingGang(now.Add(30 * time.Minute))
		insts[0].Operation.Waiting = types.WaitingReasonPodGroupTerminating
		input, rec := escalationFixture(insts)
		input.Gangs = gangObservations(0, types.GangStateNone, "")
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Operation == nil || got.Operation.Waiting != "" {
			t.Errorf("Operation.Waiting: got %+v want cleared", got.Operation)
		}
		if got.Phase != types.InstancePhaseRestarting {
			t.Errorf("Phase: got %q want Restarting", got.Phase)
		}
	})
}

// TestEscalation_GangFailedReadingOnAnOpLessRowIsInert: the gang
// scheduler's Failed phase is absorbing, so the group is deleted and
// rebuilt by the PodGroup pass — that is where the reset and the
// withheld members live. On the row itself the reading has nothing to
// act on: an op-less row holds no clock to park and no attempt to end,
// whether it has no status yet, is parked at Pending, or is converged.
func TestEscalation_GangFailedReadingOnAnOpLessRowIsInert(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []types.InstanceStatus
	}{
		{"empty index", nil},
		{"pending row", []types.InstanceStatus{{Index: 0, Phase: types.InstancePhasePending, PodCount: 2}}},
		{"ready row", []types.InstanceStatus{{Index: 0, Phase: types.InstancePhaseReady, PodCount: 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, rec := escalationFixture(tc.rows)
			input.Gangs = gangObservations(0, types.GangStateFailed, gangFailedMessage)

			if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			if len(rec.store) != len(tc.rows) {
				t.Fatalf("rows: got %d want %d (the reading must not create or drop one)", len(rec.store), len(tc.rows))
			}
			for i := range rec.store {
				if rec.store[i].Phase != tc.rows[i].Phase {
					t.Errorf("Phase: got %q want %q", rec.store[i].Phase, tc.rows[i].Phase)
				}
				if rec.store[i].Operation != nil {
					t.Errorf("Operation: got %+v want none (an op-less row has no clock to park)", rec.store[i].Operation)
				}
				if rec.store[i].LastFailure != nil {
					t.Errorf("LastFailure: got %+v want none (the group resets itself)", rec.store[i].LastFailure)
				}
			}
			if len(rec.warns) != 0 {
				t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none (no revision is blamed)", rec.blocks)
			}
		})
	}
}

// TestEscalation_GangReadingsOnAMigrationPair: a pinned row takes no gang
// waiting token and receives no terminal verdict, whichever reading
// arrives — the record's Deadline is the one clock that ends the move, so
// a token here would park a deadline nothing consumes and a verdict here
// would end a row the record still owns. Withholding the members while
// the name is unusable, and resetting a failed group, are the PodGroup
// pass's business rather than the row's.
func TestEscalation_GangReadingsOnAMigrationPair(t *testing.T) {
	now := time.Now()
	plan := types.ComponentPlan{Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
		{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
	}}

	for _, tc := range []struct {
		name string
		// row is the pair index the reading lands on: 0 is the pinned
		// source, 1 the replacement.
		row     int32
		state   types.GangState
		message string
	}{
		{"source group failed", 0, types.GangStateFailed, gangFailedMessage},
		{"replacement name being collected", 1, types.GangStateTerminating, gangTerminatingMessage},
		{"replacement name held by another controller", 1, types.GangStateOwnershipConflict, gangConflictMessage},
		{"replacement group failed", 1, types.GangStateFailed, gangFailedMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, rec := escalationFixture(migrationPair(now.Add(30 * time.Minute)))
			input.Gangs = gangObservations(tc.row, tc.state, tc.message)

			if err := runEscalationPass(t, types.Deps{}, input, plan, map[int32][]*corev1.Pod{
				0: {servingPod("engine-0-default-0")},
			}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			for _, s := range rec.store {
				if s.Phase == types.InstancePhaseFailed {
					t.Errorf("instance %d: got Failed want the record left in charge of the move", s.Index)
				}
				if s.Operation == nil || s.Operation.Waiting != "" {
					t.Errorf("instance %d Operation: got %+v want no waiting token on a pinned row", s.Index, s.Operation)
				}
			}
			if len(rec.warns) != 0 {
				t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none", rec.blocks)
			}
		})
	}
}

// The gang surge target marker is an index of its own, but no attempt of
// its own: its whole lifecycle is driven by the source's gang surge, and
// the source is where the governing operation, the deadline and the
// budget accounting live. These tests pin what that means for the events
// that reach the marker — which ones the pair answers together, and which
// ones it has no reader for.

// gangSurgePair is the source (index 0, Step=Surge, pinned to the marker)
// and the replacement gang's marker at markerIndex running markerStep.
func gangSurgePair(now time.Time, markerIndex int32, markerStep string) []types.InstanceStatus {
	return []types.InstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           types.InstancePhaseUpdating,
			PodCount:        2,
			RunningRevision: gangPairSource,
			TargetRevision:  gangPairTarget,
			Operation: &types.InstanceOperation{
				Type:           types.InstanceOperationUpdate,
				Step:           types.UpdateStepSurge,
				SurgeIndex:     &markerIndex,
				TargetRevision: gangPairTarget,
				StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
				Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
			},
		},
		{
			Index:          markerIndex,
			Incarnation:    1,
			Phase:          types.InstancePhaseCreating,
			PodCount:       2,
			TargetRevision: gangPairTarget,
			Operation: &types.InstanceOperation{
				Type:           types.InstanceOperationUpdate,
				Step:           markerStep,
				TargetRevision: gangPairTarget,
				StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
				Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
			},
		},
	}
}

const (
	gangPairSource = "llama-70b-engine-priorrev"
	gangPairTarget = "llama-70b-engine-targetrv"
)

// TestEscalation_WedgedGangReplacementFailsBothHalvesOfThePair: all
// seven terminal waiting reasons say the replacement gang will never
// start, so the escalation ends the attempt — and it ends it on BOTH
// halves. The marker is where the wedged pods are, the source is where
// the governing operation lives, and the source's Failed-with-operation
// is what routes the dispatcher into the abandon path that tears the
// replacement gang down.
func TestEscalation_WedgedGangReplacementFailsBothHalvesOfThePair(t *testing.T) {
	for _, reason := range []string{
		"ImagePullBackOff", "ErrImagePull", "InvalidImageName",
		"CreateContainerConfigError", "CreateContainerError",
		"CrashLoopBackOff", "RunContainerError",
	} {
		t.Run(reason, func(t *testing.T) {
			now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
			fc := clocktesting.NewFakeClock(now)
			insts := gangSurgePair(now, 1, types.UpdateStepGangSurgeTarget)
			input, _ := dispositionFixtureInput(fc, &insts, gangPairTarget, nil, ledgerOwnerCM())
			input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), insts...)
			input.StuckPodGrace = time.Second

			// The source keeps serving; the replacement gang is wedged.
			wedged := []*corev1.Pod{
				waitingPod("engine-1-leader-0", reason, "node-a", now.Add(-time.Minute)),
				waitingPod("engine-1-worker-0", reason, "node-b", now.Add(-time.Minute)),
			}
			pods := map[int32][]*corev1.Pod{
				0: {servingPod("engine-0-leader-0"), servingPod("engine-0-worker-0")},
				1: wedged,
			}

			plan := types.ComponentPlan{Instances: []types.InstancePlan{
				{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
				{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
			}}
			if err := runEscalationPass(t, types.Deps{Client: fakeLedgerClient(t), Clock: fc}, input, plan, pods); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			for _, s := range insts {
				if s.Phase != types.InstancePhaseFailed {
					t.Errorf("instance %d Phase: got %q want Failed", s.Index, s.Phase)
				}
				if s.Operation == nil {
					t.Errorf("instance %d Operation: got none want it preserved (the abandon path reads it)", s.Index)
				}
			}
			if insts[1].LastFailure == nil || insts[1].LastFailure.Reason != reason {
				t.Errorf("marker LastFailure: got %+v want the wedged pod's %s", insts[1].LastFailure, reason)
			}
		})
	}
}

// TestEscalation_GangFailedReadingLeavesTheSurgeMarkerUntouched: the
// replacement group's Failed phase is absorbing, so the top-level
// PodGroup pass deletes it and rebuilds it under the same deterministic
// name — a recoverable reading, not a verdict on the attempt. The
// marker's own row therefore takes no phase change, no waiting token and
// no RetryBlock from it, and the deadline keeps running.
func TestEscalation_GangFailedReadingLeavesTheSurgeMarkerUntouched(t *testing.T) {
	now := time.Now()
	insts := gangSurgePair(now, 1, types.UpdateStepGangSurgeTarget)
	input, rec := escalationFixture(insts)
	input.Gangs = gangObservations(1, types.GangStateFailed, gangFailedMessage)

	plan := types.ComponentPlan{Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
		{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
	}}
	pods := map[int32][]*corev1.Pod{1: {
		{ObjectMeta: metav1.ObjectMeta{Name: "engine-1-leader-0"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
		{ObjectMeta: metav1.ObjectMeta{Name: "engine-1-worker-0"}, Status: corev1.PodStatus{Phase: corev1.PodPending}},
	}}
	if err := runEscalationPass(t, types.Deps{}, input, plan, pods); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	marker := rec.store[1]
	if marker.Phase != types.InstancePhaseCreating {
		t.Errorf("marker Phase: got %q want Creating (the group resets itself)", marker.Phase)
	}
	if marker.Operation == nil || marker.Operation.Waiting != "" {
		t.Errorf("marker Operation: got %+v want no waiting token", marker.Operation)
	}
	if marker.LastFailure != nil {
		t.Errorf("marker LastFailure: got %+v want none", marker.LastFailure)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (no revision is blamed for a group that rebuilds)", rec.blocks)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestEscalation_TerminatingMarkerPodEndsNothing: the stuck classifier
// skips a pod carrying a deletionTimestamp — a pod on its way out is not
// a pod wedged coming up — so a marker member stuck Terminating raises
// no verdict. The name it still occupies means no replacement is
// created either, and the source's pass never lists the marker's pods,
// so the wait is bounded only by the operation deadline.
func TestEscalation_TerminatingMarkerPodEndsNothing(t *testing.T) {
	for _, step := range []string{
		types.UpdateStepGangSurgeTarget,
		types.UpdateStepGangSurgeTargetCleanup,
	} {
		t.Run(step, func(t *testing.T) {
			now := time.Now()
			insts := gangSurgePair(now, 1, step)
			input, rec := escalationFixture(insts)
			input.StuckPodGrace = time.Second

			deleting := waitingPod("engine-1-leader-0", "ContainerCreating", "node-a", now.Add(-time.Hour))
			dt := metav1.NewTime(now.Add(-time.Hour))
			deleting.DeletionTimestamp = &dt
			plan := types.ComponentPlan{Instances: []types.InstancePlan{
				{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
				{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
			}}
			pods := map[int32][]*corev1.Pod{1: {deleting}}

			if err := runEscalationPass(t, types.Deps{}, input, plan, pods); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}

			marker := rec.store[1]
			if marker.Phase != types.InstancePhaseCreating {
				t.Errorf("marker Phase: got %q want Creating (a pod on its way out is not a wedge)", marker.Phase)
			}
			if marker.Operation == nil {
				t.Fatalf("marker Operation: got none want the attempt still open")
			}
			if !marker.Operation.Deadline.Equal(&insts[1].Operation.Deadline) {
				t.Errorf("marker Deadline: got %v want it still running", marker.Operation.Deadline)
			}
			if len(rec.warns) != 0 {
				t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
			}
		})
	}
}

// The non-surge update mechanisms — the in-place image patch and the
// recreate roll — reach the passes in this package through three seams:
// what a paused plan still selects, what the escalation pass records
// about a pod it cannot place, and what it makes of the Instance's
// PodGroup. These tests pin each of those for a roll in flight.

// rollInFlight is one Instance with an update already stamped on it, at
// the step the caller names.
func rollInFlight(idx int32, step string, incarnation int64, deadline time.Time) types.InstanceStatus {
	return types.InstanceStatus{
		Index:           idx,
		Incarnation:     incarnation,
		Phase:           types.InstancePhaseUpdating,
		PodCount:        1,
		RunningRevision: "prior-rev",
		Operation: &types.InstanceOperation{
			ID:             "update-" + step,
			Type:           types.InstanceOperationUpdate,
			Step:           step,
			TargetRevision: updateTarget().Name,
			Deadline:       metav1.NewTime(deadline),
		},
	}
}

// TestEscalation_GangReadingsOnARecreateRow: a gang recreate meets the
// same three PodGroup readings a create does and answers them the same
// way — a name being collected is a wait that resolves itself and parks
// the clock, a name held by another controller ends the roll with the
// operation preserved and no revision blamed, and the scheduler's Failed
// phase is the PodGroup pass's business and leaves the row untouched.
func TestEscalation_GangReadingsOnARecreateRow(t *testing.T) {
	now := time.Now()
	recreatingGang := func() []types.InstanceStatus {
		s := rollInFlight(0, "Drain", 2, now.Add(30*time.Minute))
		s.PodCount = 2
		return []types.InstanceStatus{s}
	}

	t.Run("terminating parks the recreate", func(t *testing.T) {
		input, rec := escalationFixture(recreatingGang())
		input.Gangs = gangObservations(0, types.GangStateTerminating, gangTerminatingMessage)
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), nil); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseUpdating {
			t.Errorf("Phase: got %q want Updating", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonPodGroupTerminating {
			t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonPodGroupTerminating)
		}
		if !types.OperationExternallyHeld(got.Operation) {
			t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
		}
		if len(rec.warns) != 0 {
			t.Errorf("WarnInstanceFailed: got %v want none (the wait resolves itself)", rec.warns)
		}
	})

	t.Run("ownership conflict ends the recreate", func(t *testing.T) {
		input, rec := escalationFixture(recreatingGang())
		input.Gangs = gangObservations(0, types.GangStateOwnershipConflict, gangConflictMessage)
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseFailed {
			t.Errorf("Phase: got %q want Failed", got.Phase)
		}
		if got.LastFailure == nil || got.LastFailure.Reason != types.PodGroupOwnershipConflictReason {
			t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.PodGroupOwnershipConflictReason)
		}
		if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, "StatefulSet/legacy") {
			t.Errorf("LastFailure.Message: got %+v want the foreign controller named", got.LastFailure)
		}
		if got.Operation == nil {
			t.Errorf("Operation: got none want it preserved (nothing this controller does frees the name)")
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
		}
	})

	t.Run("the scheduler's failed phase leaves the row alone", func(t *testing.T) {
		input, rec := escalationFixture(recreatingGang())
		input.Gangs = gangObservations(0, types.GangStateFailed, gangFailedMessage)
		for pass := 0; pass < 2; pass++ {
			if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pendingGangPods()); err != nil {
				t.Fatalf("escalation pass %d: %v", pass, err)
			}
			input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseUpdating {
			t.Errorf("Phase: got %q want Updating (the group is reset, not the row)", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != "" {
			t.Errorf("Operation.Waiting: got %+v want none (a reset is not a wait)", got.Operation)
		}
		if len(rec.warns) != 0 || len(rec.blocks) != 0 {
			t.Errorf("warns=%v blocks=%v want neither across two passes", rec.warns, rec.blocks)
		}
	})
}

// A SurgeThenDrain cycle reaches the passes in this package through three
// seams: what a paused or migration-carrying plan still selects, what the
// escalation pass makes of the source's PodGroup and of a pod nobody can
// place, and what it leaves behind for a corrective revision to pick up.
// These tests pin each of those for a surge in flight.

// surgeSourceRow is the source half of a gang surge at step: Updating,
// pinned to the replacement gang under SurgeIndex, with the target
// revision the attempt is committed to.
func surgeSourceRow(now time.Time, step string, markerIndex int32) []types.InstanceStatus {
	rows := gangSurgePair(now, markerIndex, types.UpdateStepGangSurgeTarget)
	rows[0].Operation.Step = step
	return rows
}

// TestEscalation_GangReadingsOnASurgeSource: a gang-surge source is never
// judged on its own group. Its serving pods already belong to that group;
// the attempt in flight builds members under the SURGE index, which
// carries the reading and the hold that matter. Holding or ending the
// source on its own group would act on the wrong half of the pair — so
// whichever reading arrives, the source keeps its phase, its step and an
// unparked clock at both surge steps.
//
// Re-ensuring the name and withholding the members it would place there
// is the top-level PodGroup pass's business, not this row's.
func TestEscalation_GangReadingsOnASurgeSource(t *testing.T) {
	now := time.Now()
	plan := types.ComponentPlan{Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
		{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
	}}

	for _, step := range []string{types.UpdateStepSurge, types.UpdateStepSurgeDrain} {
		for _, tc := range []struct {
			name    string
			state   types.GangState
			message string
		}{
			{"the name is being collected", types.GangStateTerminating, gangTerminatingMessage},
			{"the name is held by another controller", types.GangStateOwnershipConflict, gangConflictMessage},
			{"the scheduler failed the group", types.GangStateFailed, gangFailedMessage},
		} {
			t.Run(step+"/"+tc.name, func(t *testing.T) {
				input, rec := escalationFixture(surgeSourceRow(now, step, 1))
				input.Gangs = gangObservations(0, tc.state, tc.message)

				if err := runEscalationPass(t, types.Deps{}, input, plan, map[int32][]*corev1.Pod{
					0: {servingPod("engine-0-default-0"), servingPod("engine-0-default-1")},
				}); err != nil {
					t.Fatalf("escalation pass: %v", err)
				}

				got := rec.store[0]
				if got.Phase != types.InstancePhaseUpdating {
					t.Errorf("Phase: got %q want Updating (the source's own group does not judge it)", got.Phase)
				}
				if got.Operation == nil || got.Operation.Step != step {
					t.Errorf("Operation: got %+v want the surge still at %s", got.Operation, step)
				}
				if got.Operation == nil || got.Operation.Waiting != "" {
					t.Errorf("Operation.Waiting: got %+v want none; the replacement's reading owns the hold", got.Operation)
				}
				if len(rec.warns) != 0 || len(rec.blocks) != 0 {
					t.Errorf("warns=%v blocks=%v want neither", rec.warns, rec.blocks)
				}
			})
		}
	}
}
