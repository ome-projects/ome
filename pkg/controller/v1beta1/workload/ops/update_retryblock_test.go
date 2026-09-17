package ops

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// RetryBlock gate tests: DetectUpdateTrigger consults the
// persisted RetryBlock for the CURRENT target revision before firing a
// FRESH trigger. Held / RetryInProgress / not-yet-due Backoff deny; a
// due Backoff allows WITHOUT flipping state — the RetryInProgress flip
// happens at attempt-stamp time (flipRetryBlockOnAttemptStart), after
// the dispatcher's budget/coordination gates admit the start. A block
// for a DIFFERENT target revision is a different RetrySubject and never
// gates (the Failed-wedge fix stands), and a Failed-continuation
// (teardown/abandon of a failed candidate) is exempt.

// retryBlockCall records one MutateRetryBlock invocation.
type retryBlockCall struct {
	rev         string
	disposition workload.RetryBlockDisposition
	block       workload.RetryBlock
}

// retryGateFixture builds the steady-state from which an Update trigger
// fires absent a RetryBlock: Instance 0 Ready on an OLD revision while
// the target CR captures a different image. MutateRetryBlock is a
// recorder that snapshots (rev, disposition, mutated block).
func retryGateFixture(t *testing.T, t0 time.Time) (*workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *[]retryBlockCall, client.Client, *v1beta1.InferenceService) {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, c, isvc, legacyTargetSpecImage("llama:v2"))
	// Ready on a NON-target revision → the fast-path fires absent a block.
	ir.Status.InstanceStatuses[0].RunningRevision = "llama-70b-engine-oldrev"
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(t0)
	calls := &[]retryBlockCall{}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		var b workload.RetryBlock
		if existing := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev); existing != nil {
			b = *existing
		} else {
			b = workload.RetryBlock{TargetRevision: rev}
		}
		d := mutate(&b)
		*calls = append(*calls, retryBlockCall{rev: rev, disposition: d, block: b})
		return nil
	}
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	return &input, plan, tcr, calls, c, isvc
}

// TestDetectUpdate_RetryBlockHeld_Denies: a Held block for the current
// target denies the trigger with no wake-up and no writes of any kind.
func TestDetectUpdate_RetryBlockHeld_Denies(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := retryGateFixture(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockHeld},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Held block for the current target must deny the trigger")
	}
	if retryAfter != 0 {
		t.Errorf("Held is not time-bounded: retryAfter got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock must not be called on Held denial: %d calls", len(*calls))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("instance status mutated on denial: phase got %q want %q", s.Phase, v1beta1.OMENativeInstanceReady)
	}
}

// TestDetectUpdate_RetryBlockBackoffNotDue_DeniesWithRequeue: a Backoff
// block whose NextRetryAt is in the future denies AND reports exactly
// when to re-evaluate (fake clock → exact remaining interval).
func TestDetectUpdate_RetryBlockBackoffNotDue_DeniesWithRequeue(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	next := metav1.NewTime(t0.Add(37 * time.Second))
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, NextRetryAt: &next},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("not-yet-due Backoff must deny the trigger")
	}
	if retryAfter != 37*time.Second {
		t.Errorf("retryAfter: got %v want exactly 37s", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock must not be called before NextRetryAt: %d calls", len(*calls))
	}
}

// TestDetectUpdate_RetryBlockDue_AllowsWithoutFlip: a due Backoff block
// lets the trigger fire but the GATE records nothing — the
// RetryInProgress flip belongs to attempt-stamp time, after the
// dispatcher budgets admit the start.
func TestDetectUpdate_RetryBlockDue_AllowsWithoutFlip(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	next := metav1.NewTime(t0.Add(-1 * time.Second))
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &next},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("due Backoff must allow the trigger")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0 on allowed fire", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("the gate must not flip state (attempt-stamp time owns the flip): %d calls", len(*calls))
	}
}

// TestDetectUpdate_RetryBlockDueBoundaryExact: at exactly NextRetryAt
// the block is due — now.Before(next) is false at equality.
func TestDetectUpdate_RetryBlockDueBoundaryExact(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	next := metav1.NewTime(t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, NextRetryAt: &next},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("block at exactly NextRetryAt is due — trigger must fire")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0 at the due boundary", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("gate must not write at the due boundary: %d calls", len(*calls))
	}
}

// TestDetectUpdate_RetryBlockInProgress_DeniesSecondAttempt: while an
// authorized attempt is in flight the gate denies any further fresh
// trigger — exactly-one-attempt semantics.
func TestDetectUpdate_RetryBlockInProgress_DeniesSecondAttempt(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockRetryInProgress, AttemptsStarted: 1},
	}
	// A LIVE authorization: some Instance carries an in-flight Update
	// Operation at the target revision.
	input.ObservedState.InstanceStatuses = append(input.ObservedState.InstanceStatuses, workload.InstanceStatus{
		Index: 7, Phase: workload.InstancePhaseUpdating,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, TargetRevision: tcr.Name},
	})

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("RetryInProgress with a live in-flight attempt must deny a second attempt")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock must not be called on RetryInProgress denial: %d calls", len(*calls))
	}
}

// TestDetectUpdate_RetryBlockInProgressLeaked_SelfHeals: RetryInProgress
// with NO in-flight Update attempt at the revision (superseded surge,
// scale-down, crash) is a leaked authorization — the gate treats it as
// due so a later rollback to that revision is not silently denied
// forever.
func TestDetectUpdate_RetryBlockInProgressLeaked_SelfHeals(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockRetryInProgress, AttemptsStarted: 1},
	}
	// No instance carries an in-flight Update Operation at tcr.Name.

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("leaked RetryInProgress (no in-flight attempt) must self-heal and allow the trigger")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("gate must not write on self-heal (stamp re-confirms): %d calls", len(*calls))
	}
}

// TestDetectUpdate_NewRevisionPassesGate: a block for a DIFFERENT
// (older) target revision never gates the new target — the wedge
// regression guard: a corrective revision must roll even when the
// prior revision Held.
func TestDetectUpdate_NewRevisionPassesGate(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: "some-OTHER-rev", State: workload.RetryBlockHeld},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("a Held block for a different revision must NOT gate the new target")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock must not be called for an unrelated block: %d calls", len(*calls))
	}
}

// TestDetectUpdate_FailedContinuationPassesGate: Phase=Failed with an
// in-flight Update Operation is a CONTINUATION (teardown/abandon of the
// failed candidate) and must proceed regardless of the block — a Held
// block must not freeze the candidate gang mid-teardown. Mirrors the dispatcher's startingFresh carve-out.
func TestDetectUpdate_FailedContinuationPassesGate(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	input.ObservedState.InstanceStatuses[0].Phase = workload.InstancePhaseFailed
	input.ObservedState.InstanceStatuses[0].Operation = &workload.InstanceOperation{
		ID:             "update-0-1",
		Type:           workload.InstanceOperationUpdate,
		Step:           UpdateStepSurge,
		TargetRevision: tcr.Name,
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockHeld},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("Failed-continuation must pass the gate even when Held")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("gate must not write on the continuation exemption: %d calls", len(*calls))
	}
}

// TestPatchSurgingForUpdate_FlipsBackoffOnAttemptStart: the attempt-
// stamp helper owns the RetryInProgress flip — an existing Backoff
// block flips (exactly one Persist) when the fresh Update Operation is
// stamped, and NOTHING is recorded when no block exists (a fresh start
// with no prior failure needs no block).
func TestPatchSurgingForUpdate_FlipsBackoffOnAttemptStart(t *testing.T) {
	t.Run("existing Backoff block flips to RetryInProgress", func(t *testing.T) {
		t0 := time.Now()
		input, _, tcr, calls, _, _ := retryGateFixture(t, t0)
		next := metav1.NewTime(t0.Add(-1 * time.Second))
		input.ObservedState.RetryBlocks = []workload.RetryBlock{
			{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &next},
		}

		if err := patchInstanceStatusSurgingForUpdate(context.Background(), *input, 0, tcr.Name, 30*time.Minute); err != nil {
			t.Fatalf("stamp: %v", err)
		}
		if len(*calls) != 1 {
			t.Fatalf("MutateRetryBlock calls: got %d want exactly 1", len(*calls))
		}
		call := (*calls)[0]
		if call.rev != tcr.Name {
			t.Errorf("mutate rev: got %q want %q", call.rev, tcr.Name)
		}
		if call.disposition != workload.RetryBlockPersist {
			t.Errorf("disposition: got %v want Persist", call.disposition)
		}
		if call.block.State != workload.RetryBlockRetryInProgress {
			t.Errorf("mutated state: got %q want %q", call.block.State, workload.RetryBlockRetryInProgress)
		}
	})

	t.Run("no block records nothing", func(t *testing.T) {
		t0 := time.Now()
		input, _, tcr, calls, _, _ := retryGateFixture(t, t0)

		if err := patchInstanceStatusSurgingForUpdate(context.Background(), *input, 0, tcr.Name, 30*time.Minute); err != nil {
			t.Fatalf("stamp: %v", err)
		}
		for _, call := range *calls {
			if call.disposition != workload.RetryBlockUnchanged {
				t.Errorf("no-block start must persist nothing: disposition got %v want Unchanged", call.disposition)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// RetryBlock writer tests: recordUpdateFailureInRetryBlock counts
// attempts per WAVE — an existing Backoff block means this wave
// already recorded, so only the evidence refreshes. Policy nil (unconfigured)
// or exhausted → Held + WarnRetryHeld exactly once at the transition.
// These drive workload-caused waves (the charged arm); the uncharged,
// environment-caused arm is pinned in the types package and the gang
// abandon tests.
// ---------------------------------------------------------------------------

// retryHeldWarning records one WarnRetryHeld invocation.
type retryHeldWarning struct {
	rev      string
	attempts int32
	reason   string
}

// retryWriterInput builds the minimal ReconcileInput the writer needs: a
// fake clock, the recording MutateRetryBlock closure backed by
// ObservedState.RetryBlocks, an optional policy, and a WarnRetryHeld
// recorder. Same recording-closure pattern as retryGateFixture, without
// the fake-client scaffolding the pure writer doesn't touch.
func retryWriterInput(t0 time.Time, existing []workload.RetryBlock, policy *workload.RetryPolicy) (*workload.ReconcileInput, *[]retryBlockCall, *[]retryHeldWarning) {
	input := &workload.ReconcileInput{
		Clock:             clocktesting.NewFakeClock(t0),
		UpdateRetryPolicy: policy,
	}
	input.ObservedState.RetryBlocks = existing
	calls := &[]retryBlockCall{}
	warns := &[]retryHeldWarning{}
	input.WarnRetryHeld = func(rev string, attempts int32, reason string) {
		*warns = append(*warns, retryHeldWarning{rev: rev, attempts: attempts, reason: reason})
	}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		var b workload.RetryBlock
		if existing := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev); existing != nil {
			b = *existing
		} else {
			b = workload.RetryBlock{TargetRevision: rev}
		}
		d := mutate(&b)
		*calls = append(*calls, retryBlockCall{rev: rev, disposition: d, block: b})
		return nil
	}
	return input, calls, warns
}

// retryTestPolicy is the canonical test policy: 3 attempts, 1m initial
// delay, 30m cap, multiplier 2.
func retryTestPolicy() *workload.RetryPolicy {
	return &workload.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
}

// TestRecordUpdateFailure_FirstFailureBacksOff: (a) first same-target
// failure creates the block — AttemptsStarted=1, Backoff, persisted
// NextRetryAt = now + InitialDelay, both failure timestamps stamped.
func TestRecordUpdateFailure_FirstFailureBacksOff(t *testing.T) {
	t0 := time.Now()
	input, calls, warns := retryWriterInput(t0, nil, retryTestPolicy())

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "ImagePullBackOff", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0]
	if b.rev != "rev-bad" || b.disposition != workload.RetryBlockPersist {
		t.Errorf("call: got (rev=%q, disposition=%v) want (rev-bad, Persist)", b.rev, b.disposition)
	}
	if b.block.State != workload.RetryBlockBackoff {
		t.Errorf("state: got %q want Backoff", b.block.State)
	}
	if b.block.AttemptsStarted != 1 {
		t.Errorf("AttemptsStarted: got %d want 1", b.block.AttemptsStarted)
	}
	if b.block.NextRetryAt == nil || !b.block.NextRetryAt.Time.Equal(t0.Add(time.Minute)) {
		t.Errorf("NextRetryAt: got %v want %v", b.block.NextRetryAt, t0.Add(time.Minute))
	}
	if b.block.FirstFailureAt == nil || !b.block.FirstFailureAt.Time.Equal(t0) {
		t.Errorf("FirstFailureAt: got %v want %v", b.block.FirstFailureAt, t0)
	}
	if b.block.LastFailureAt == nil || !b.block.LastFailureAt.Time.Equal(t0) {
		t.Errorf("LastFailureAt: got %v want %v", b.block.LastFailureAt, t0)
	}
	if b.block.Reason != "ImagePullBackOff" {
		t.Errorf("Reason: got %q want ImagePullBackOff", b.block.Reason)
	}
	if len(*warns) != 0 {
		t.Errorf("WarnRetryHeld: got %d calls want 0 (attempts remain)", len(*warns))
	}
}

// TestRecordUpdateFailure_SecondWaveCounts: (b) the authorized retry
// (block RetryInProgress) failed — counts as a new wave: AttemptsStarted
// 1→2, back to Backoff with NextRetryAt = now + InitialDelay*Multiplier,
// FirstFailureAt preserved.
func TestRecordUpdateFailure_SecondWaveCounts(t *testing.T) {
	t0 := time.Now()
	first := metav1.NewTime(t0.Add(-10 * time.Minute))
	input, calls, warns := retryWriterInput(t0, []workload.RetryBlock{{
		TargetRevision:  "rev-bad",
		State:           workload.RetryBlockRetryInProgress,
		AttemptsStarted: 1,
		FirstFailureAt:  &first,
		Reason:          "old evidence",
	}}, retryTestPolicy())

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "still ImagePullBackOff", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0].block
	if b.AttemptsStarted != 2 {
		t.Errorf("AttemptsStarted: got %d want 2 (RetryInProgress failure counts the wave)", b.AttemptsStarted)
	}
	if b.State != workload.RetryBlockBackoff {
		t.Errorf("state: got %q want Backoff", b.State)
	}
	if b.NextRetryAt == nil || !b.NextRetryAt.Time.Equal(t0.Add(2*time.Minute)) {
		t.Errorf("NextRetryAt: got %v want %v (1m * 2^1)", b.NextRetryAt, t0.Add(2*time.Minute))
	}
	if b.FirstFailureAt == nil || !b.FirstFailureAt.Time.Equal(first.Time) {
		t.Errorf("FirstFailureAt: got %v want preserved %v", b.FirstFailureAt, first.Time)
	}
	if b.Reason != "still ImagePullBackOff" {
		t.Errorf("Reason: got %q want refreshed", b.Reason)
	}
	if len(*warns) != 0 {
		t.Errorf("WarnRetryHeld: got %d calls want 0", len(*warns))
	}
}

// TestRecordUpdateFailure_SameWaveRefreshOnly: (c) a sibling instance's
// failure in the SAME wave finds the block already Backoff — evidence
// refresh only: no increment, no NextRetryAt recompute.
func TestRecordUpdateFailure_SameWaveRefreshOnly(t *testing.T) {
	t0 := time.Now()
	next := metav1.NewTime(t0.Add(-30 * time.Second)) // set by the wave's first failure
	first := metav1.NewTime(t0.Add(-90 * time.Second))
	input, calls, warns := retryWriterInput(t0, []workload.RetryBlock{{
		TargetRevision:  "rev-bad",
		State:           workload.RetryBlockBackoff,
		AttemptsStarted: 1,
		NextRetryAt:     &next,
		FirstFailureAt:  &first,
		Reason:          "first instance failed",
	}}, retryTestPolicy())

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "second instance failed", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0]
	if b.disposition != workload.RetryBlockPersist {
		t.Errorf("disposition: got %v want Persist (evidence refresh)", b.disposition)
	}
	if b.block.AttemptsStarted != 1 {
		t.Errorf("AttemptsStarted: got %d want 1 (same wave — no increment)", b.block.AttemptsStarted)
	}
	if b.block.NextRetryAt == nil || !b.block.NextRetryAt.Time.Equal(next.Time) {
		t.Errorf("NextRetryAt: got %v want unchanged %v", b.block.NextRetryAt, next.Time)
	}
	if b.block.Reason != "second instance failed" {
		t.Errorf("Reason: got %q want refreshed", b.block.Reason)
	}
	if b.block.LastFailureAt == nil || !b.block.LastFailureAt.Time.Equal(t0) {
		t.Errorf("LastFailureAt: got %v want refreshed to %v", b.block.LastFailureAt, t0)
	}
	if len(*warns) != 0 {
		t.Errorf("WarnRetryHeld: got %d calls want 0", len(*warns))
	}
}

// TestRecordUpdateFailure_ExhaustionHolds: (d) the third counted wave
// exhausts MaxAttempts=3 → Held, NextRetryAt cleared, WarnRetryHeld
// exactly once with attempts=3. A later failure against the Held block
// refreshes evidence only — no second warning, no increment.
func TestRecordUpdateFailure_ExhaustionHolds(t *testing.T) {
	t0 := time.Now()
	input, calls, warns := retryWriterInput(t0, []workload.RetryBlock{{
		TargetRevision:  "rev-bad",
		State:           workload.RetryBlockRetryInProgress,
		AttemptsStarted: 2,
	}}, retryTestPolicy())

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "third strike", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0].block
	if b.State != workload.RetryBlockHeld {
		t.Errorf("state: got %q want Held (3 >= MaxAttempts)", b.State)
	}
	if b.AttemptsStarted != 3 {
		t.Errorf("AttemptsStarted: got %d want 3", b.AttemptsStarted)
	}
	if b.NextRetryAt != nil {
		t.Errorf("NextRetryAt: got %v want nil (Held has no time bound)", b.NextRetryAt)
	}
	if len(*warns) != 1 {
		t.Fatalf("WarnRetryHeld: got %d calls want exactly 1 (at the Held transition)", len(*warns))
	}
	if w := (*warns)[0]; w.rev != "rev-bad" || w.attempts != 3 || w.reason != "third strike" {
		t.Errorf("warning: got %+v want {rev-bad 3 third strike}", w)
	}

	// A subsequent failure against the persisted Held block: refresh only.
	input.ObservedState.RetryBlocks = []workload.RetryBlock{b}
	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "post-hold noise", true); err != nil {
		t.Fatalf("record on Held: %v", err)
	}
	held := (*calls)[1].block
	if held.State != workload.RetryBlockHeld || held.AttemptsStarted != 3 {
		t.Errorf("Held refresh: got (state=%q, attempts=%d) want (Held, 3)", held.State, held.AttemptsStarted)
	}
	if held.Reason != "post-hold noise" {
		t.Errorf("Held refresh Reason: got %q want refreshed", held.Reason)
	}
	if len(*warns) != 1 {
		t.Errorf("WarnRetryHeld after Held refresh: got %d calls want still 1", len(*warns))
	}
}

// TestRecordUpdateFailure_NilPolicyHoldsFirstFailure: (e) unconfigured
// policy fails safe — Held on the FIRST failure, never Backoff.
func TestRecordUpdateFailure_NilPolicyHoldsFirstFailure(t *testing.T) {
	t0 := time.Now()
	input, calls, warns := retryWriterInput(t0, nil, nil)

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "no policy configured", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0].block
	if b.State != workload.RetryBlockHeld {
		t.Errorf("state: got %q want Held (nil policy is always exhausted)", b.State)
	}
	if b.AttemptsStarted != 1 {
		t.Errorf("AttemptsStarted: got %d want 1", b.AttemptsStarted)
	}
	if b.NextRetryAt != nil {
		t.Errorf("NextRetryAt: got %v want nil", b.NextRetryAt)
	}
	if len(*warns) != 1 || (*warns)[0].attempts != 1 {
		t.Errorf("WarnRetryHeld: got %+v want exactly one call with attempts=1", *warns)
	}
}

// TestRecordUpdateFailure_UnwiredNoOp: (f) nil MutateRetryBlock (adapter
// opted out) and empty targetRev are both silent no-ops — no panic.
func TestRecordUpdateFailure_UnwiredNoOp(t *testing.T) {
	t0 := time.Now()

	unwired := &workload.ReconcileInput{Clock: clocktesting.NewFakeClock(t0)}
	if err := recordUpdateFailureInRetryBlock(context.Background(), *unwired, "rev-bad", "x", true); err != nil {
		t.Fatalf("nil closure must no-op: %v", err)
	}

	input, calls, _ := retryWriterInput(t0, nil, retryTestPolicy())
	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "", "x", true); err != nil {
		t.Fatalf("empty targetRev must no-op: %v", err)
	}
	if len(*calls) != 0 {
		t.Errorf("empty targetRev: got %d MutateRetryBlock calls want 0", len(*calls))
	}
}

// TestInstanceFailureReason pins the call-site evidence extraction:
// LastFailure.Message wins, ShortString covers message-less stuck-pod
// escalations, and the fallback covers a Failed instance with no
// recorded termination.
func TestInstanceFailureReason(t *testing.T) {
	if got := instanceFailureReason(nil, "fallback"); got != "fallback" {
		t.Errorf("nil status: got %q want fallback", got)
	}
	s := &workload.InstanceStatus{}
	if got := instanceFailureReason(s, "fallback"); got != "fallback" {
		t.Errorf("nil LastFailure: got %q want fallback", got)
	}
	s.LastFailure = &workload.InstanceTermination{PodName: "p-0", Reason: "ImagePullBackOff"}
	if got := instanceFailureReason(s, "fallback"); got != "pod p-0 stuck (ImagePullBackOff)" {
		t.Errorf("ShortString path: got %q", got)
	}
	s.LastFailure.Message = "DeadlineExceeded: Update/Surge exceeded InstanceReadyTimeout"
	if got := instanceFailureReason(s, "fallback"); got != s.LastFailure.Message {
		t.Errorf("Message path: got %q want %q", got, s.LastFailure.Message)
	}
}

// TestInstanceFailureWorkloadCaused pins the call-site cause attribution:
// only a LastFailure whose Reason is in the workload-caused set charges
// the ladder; an elapsed deadline, an ambiguous kubelet reason, and
// missing evidence do not.
func TestInstanceFailureWorkloadCaused(t *testing.T) {
	if instanceFailureWorkloadCaused(nil) {
		t.Error("nil status must not be workload-caused")
	}
	s := &workload.InstanceStatus{}
	if instanceFailureWorkloadCaused(s) {
		t.Error("nil LastFailure must not be workload-caused")
	}
	for reason, want := range map[string]bool{
		"ImagePullBackOff":           true,
		"ErrImagePull":               true,
		"InvalidImageName":           true,
		"CreateContainerConfigError": true,
		"DeadlineExceeded":           false,
		"CrashLoopBackOff":           false,
		"RunContainerError":          false,
		"":                           false,
	} {
		s.LastFailure = &workload.InstanceTermination{PodName: "p-0", Reason: reason}
		if got := instanceFailureWorkloadCaused(s); got != want {
			t.Errorf("reason %q: got %v want %v", reason, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Success prune: the promote helpers remove the promoted revision's block.
// ---------------------------------------------------------------------------

// TestPatchReadyOnRevision_PrunesBlock: promoting Ready on rev removes
// that rev's block (disposition Remove observed).
func TestPatchReadyOnRevision_PrunesBlock(t *testing.T) {
	t0 := time.Now()
	input, _, tcr, calls, _, _ := retryGateFixture(t, t0)

	if err := patchInstanceStatusReadyOnRevision(context.Background(), *input, 0, tcr.Name); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	if c := (*calls)[0]; c.rev != tcr.Name || c.disposition != workload.RetryBlockRemove {
		t.Errorf("prune: got (rev=%q, disposition=%v) want (%q, Remove)", c.rev, c.disposition, tcr.Name)
	}
}

// disposedBackfillFixture builds the deadline-disposed shape the
// empty-RunningRevision backfill can encounter: Instance 0 is
// Phase=Failed with NO Operation and NO RunningRevision, a Backoff
// RetryBlock for the target revision is already due, and the wedged
// attempt's pods are still present and spec-match the target. podReady
// controls whether those pods carry ContainersReady. Returns the pod
// separately so the caller threads it through instancePods.
func disposedBackfillFixture(t *testing.T, t0 time.Time, podReady bool) (*workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *[]retryBlockCall, client.Client, *v1beta1.InferenceService, *corev1.Pod) {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	// Deadline disposition left: Failed, Operation cleared, no
	// RunningRevision ever stamped (initial create never converged).
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceFailed
	c := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, c, isvc, legacyTargetSpecImage("llama:v2"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(t0)
	due := metav1.NewTime(t0.Add(-1 * time.Second))
	calls := recordRetryBlockCalls(&input, []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &due},
	})
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	// The wedged attempt's pod: spec-matches the target revision (a bad
	// image ref always spec-matches its own bad revision).
	pod := legacyPodForInstance(isvc, 0, podReady, false)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	if !podReady {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  "main",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
		}}
	}
	return &input, plan, tcr, calls, c, isvc, pod
}

// TestDetectUpdate_BackfillRefusesWedgedPods: the empty-RunningRevision
// backfill must NOT stamp Ready / prune the target's RetryBlock off a
// spec-match alone. A deadline-disposed create (Failed, no Operation,
// pods present in ImagePullBackOff) spec-matches the very revision that
// wedged it; stamping Ready here would prune the block and defuse the
// retry machinery while nothing is actually serving.
func TestDetectUpdate_BackfillRefusesWedgedPods(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc, pod := disposedBackfillFixture(t, t0, false /* podReady */)

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), []*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("spec-matching pods must not trigger a fresh update")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock calls: got %d want 0 (block must survive — pods are not runtime-ready)", len(*calls))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed {
		t.Errorf("phase: got %q want still Failed (no Ready stamp without proof)", s.Phase)
	}
	if s.RunningRevision != "" {
		t.Errorf("RunningRevision: got %q want empty (no backfill without proof)", s.RunningRevision)
	}
}

// TestDetectUpdate_BackfillAdoptsRuntimeReadyPods: the backfill's
// legitimate purpose survives the guard — pods genuinely running the
// target (runtime-ready) with a lost/never-written status record are
// adopted: Ready stamped, RunningRevision backfilled, and the target's
// block pruned (success at rev is real proof here).
func TestDetectUpdate_BackfillAdoptsRuntimeReadyPods(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc, pod := disposedBackfillFixture(t, t0, true /* podReady */)

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, legacyTargetSpecImage("llama:v2"), []*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("runtime-ready spec-matching pods must not trigger an update")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1 (success-prune)", len(*calls))
	}
	if call := (*calls)[0]; call.rev != tcr.Name || call.disposition != workload.RetryBlockRemove {
		t.Errorf("prune: got (rev=%q, disposition=%v) want (%q, Remove)", call.rev, call.disposition, tcr.Name)
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("phase: got %q want Ready (legitimate adoption)", s.Phase)
	}
	if s.RunningRevision != tcr.Name {
		t.Errorf("RunningRevision: got %q want %q (backfilled)", s.RunningRevision, tcr.Name)
	}
}

// TestPatchReadyOnRevisionWithOrdinal_PrunesBlock: the surge-promote
// variant prunes likewise.
func TestPatchReadyOnRevisionWithOrdinal_PrunesBlock(t *testing.T) {
	t0 := time.Now()
	input, _, tcr, calls, _, _ := retryGateFixture(t, t0)

	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(context.Background(), *input, 0, tcr.Name, 1); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	if c := (*calls)[0]; c.rev != tcr.Name || c.disposition != workload.RetryBlockRemove {
		t.Errorf("prune: got (rev=%q, disposition=%v) want (%q, Remove)", c.rev, c.disposition, tcr.Name)
	}
}
