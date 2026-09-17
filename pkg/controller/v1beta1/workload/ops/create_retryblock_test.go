package ops

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Create-pass RetryBlock gate tests: a deadline-disposed create leaves
// its instance Failed-with-no-Operation — a fresh start — so without a
// gate the Create pass re-materializes pods at the same bad revision
// forever, bypassing the RetryBlock entirely. The gate applies ONLY to
// disposed fresh-starts (Phase=Failed with nil Operation, or no status
// slot at all) AND only when a block for the create's target revision
// exists — genuinely-new scale-ups with no block are untouched.

// createGateFixture builds the Create-pass analogue of retryGateFixture:
// an ISVC whose engine IR carries the supplied instance statuses (none →
// no IR seeded at all), the target CR matching DesiredSpec, a fake
// clock, and the recording MutateRetryBlock closure.
func createGateFixture(t *testing.T, t0 time.Time, insts ...v1beta1.OMENativeInstanceStatus) (*workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *[]retryBlockCall, client.Client, *v1beta1.InferenceService) {
	t.Helper()
	legacyResetExpectations(t)
	isvc := legacyMinimalISVC("llama-70b", "prod", 1)
	objs := []client.Object{isvc}
	if len(insts) > 0 {
		objs = append(objs, legacyInstanceIR(isvc, workload.ComponentEngine, insts...))
	}
	c := legacyNewFakeClient(t, objs...)
	tcr := legacyEnsureTargetCR(t, c, isvc, legacyTargetSpecImage("test:v1"))
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

// disposedFreshStart is the post-disposition instance status: Failed
// with no Operation.
func disposedFreshStart() v1beta1.OMENativeInstanceStatus {
	return v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceFailed}
}

// createGatePods lists every pod in the fixture namespace.
func createGatePods(t *testing.T, c client.Client, ns string) []corev1.Pod {
	t.Helper()
	list := &corev1.PodList{}
	if err := c.List(context.Background(), list, client.InNamespace(ns)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return list.Items
}

// persistCalls filters the recorded MutateRetryBlock invocations down to
// actual writes (Persist / Remove) — idempotent Unchanged probes from
// the attempt-stamp flip don't count as block mutations.
func persistCalls(calls []retryBlockCall) []retryBlockCall {
	var out []retryBlockCall
	for _, c := range calls {
		if c.disposition != workload.RetryBlockUnchanged {
			out = append(out, c)
		}
	}
	return out
}

// TestCreate_RetryBlockHeld_DeniesFreshStart: (a) a Held block for the
// target revision denies re-materialization of a disposed fresh-start —
// no pods, no status writes, no requeue (Held has no time bound), and
// the operator warning stays the ONE emitted at the Held transition
// (the writer's dedup); repeated denied passes add nothing.
func TestCreate_RetryBlockHeld_DeniesFreshStart(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := createGateFixture(t, t0, disposedFreshStart())

	// Arrive at Held the way production does: the disposition writer with
	// a nil (unconfigured) policy Holds on the first failure and emits
	// WarnRetryHeld exactly once.
	warns := &[]retryHeldWarning{}
	input.WarnRetryHeld = func(rev string, attempts int32, reason string) {
		*warns = append(*warns, retryHeldWarning{rev: rev, attempts: attempts, reason: reason})
	}
	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, tcr.Name, "ImagePullBackOff", true); err != nil {
		t.Fatalf("seed Held block: %v", err)
	}
	if len(*warns) != 1 {
		t.Fatalf("WarnRetryHeld at Held transition: got %d want 1", len(*warns))
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{(*calls)[0].block}
	*calls = nil

	for pass := 0; pass < 2; pass++ {
		res, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr)
		if err != nil {
			t.Fatalf("create pass %d: %v", pass, err)
		}
		if res != (ctrl.Result{}) {
			t.Errorf("pass %d: Held denial must not requeue: got %+v", pass, res)
		}
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 0 {
		t.Errorf("Held block must deny materialization: got %d pod(s)", len(pods))
	}
	if len(*calls) != 0 {
		t.Errorf("denied create must not touch the block: %d MutateRetryBlock calls", len(*calls))
	}
	if len(*warns) != 1 {
		t.Errorf("denied passes must not re-warn: got %d want still 1", len(*warns))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Errorf("denied instance mutated: phase=%q op=%v", s.Phase, s.Operation)
	}
}

// TestCreate_NoStatusHeldBlock_Denied: a Held block also gates an
// instance with NO status slot — scaling up onto a held revision would
// materialize the same wedged pods.
func TestCreate_NoStatusHeldBlock_Denied(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := createGateFixture(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockHeld},
	}

	res, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Held denial must not requeue: got %+v", res)
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 0 {
		t.Errorf("Held block must deny a no-status scale-up: got %d pod(s)", len(pods))
	}
	if len(*calls) != 0 {
		t.Errorf("denied create must not touch the block: %d calls", len(*calls))
	}
}

// TestCreate_RetryBlockOtherRevision_Allows: (b) a block for a DIFFERENT
// revision is a different RetrySubject — the create toward the current
// target proceeds.
func TestCreate_RetryBlockOtherRevision_Allows(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := createGateFixture(t, t0, disposedFreshStart())
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: "some-OTHER-rev", State: workload.RetryBlockHeld},
	}

	if _, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr); err != nil {
		t.Fatalf("create: %v", err)
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 1 {
		t.Fatalf("a block for another revision must not gate: got %d pod(s) want 1", len(pods))
	}
	if writes := persistCalls(*calls); len(writes) != 0 {
		t.Errorf("no block for the target — nothing to flip: %d write(s)", len(writes))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("allowed create must stamp Creating: got %q", s.Phase)
	}
}

// TestCreate_RetryBlockBackoffNotDue_Requeues: (c) a not-yet-due Backoff
// denies AND surfaces exactly the remaining interval as the pass
// wake-up, mirroring how the update path folds retryAfter.
func TestCreate_RetryBlockBackoffNotDue_Requeues(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := createGateFixture(t, t0, disposedFreshStart())
	next := metav1.NewTime(t0.Add(37 * time.Second))
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, NextRetryAt: &next},
	}

	res, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 0 {
		t.Errorf("not-yet-due Backoff must deny materialization: got %d pod(s)", len(pods))
	}
	if res.RequeueAfter != 37*time.Second {
		t.Errorf("RequeueAfter: got %v want exactly 37s", res.RequeueAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("denied create must not touch the block: %d calls", len(*calls))
	}
}

// TestCreate_RetryBlockBackoffDue_AllowsAndFlips: (d) a due Backoff lets
// the create proceed, and the Creating attempt stamp — not the gate —
// flips the block to RetryInProgress so wave counting works for creates
// exactly as for updates.
func TestCreate_RetryBlockBackoffDue_AllowsAndFlips(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := createGateFixture(t, t0, disposedFreshStart())
	next := metav1.NewTime(t0.Add(-1 * time.Second))
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &next},
	}

	if _, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr); err != nil {
		t.Fatalf("create: %v", err)
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 1 {
		t.Fatalf("due Backoff must allow the create: got %d pod(s) want 1", len(pods))
	}
	writes := persistCalls(*calls)
	if len(writes) != 1 {
		t.Fatalf("attempt stamp must flip exactly once: got %d write(s)", len(writes))
	}
	w := writes[0]
	if w.rev != tcr.Name || w.disposition != workload.RetryBlockPersist {
		t.Errorf("flip write: got (rev=%q, disposition=%v) want (%q, Persist)", w.rev, w.disposition, tcr.Name)
	}
	if w.block.State != workload.RetryBlockRetryInProgress {
		t.Errorf("flipped state: got %q want %q", w.block.State, workload.RetryBlockRetryInProgress)
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("allowed create must stamp Creating: got %q", s.Phase)
	}
}

// TestCreate_NewInstanceNoBlock_Unaffected: (e) a genuinely-new instance
// (no status slot, no block) creates exactly as before the gate existed
// — and its attempt stamp records nothing (a fresh start with no prior
// failure needs no block).
func TestCreate_NewInstanceNoBlock_Unaffected(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := createGateFixture(t, t0)

	if _, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr); err != nil {
		t.Fatalf("create: %v", err)
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 1 {
		t.Fatalf("fresh scale-up must materialize: got %d pod(s) want 1", len(pods))
	}
	if writes := persistCalls(*calls); len(writes) != 0 {
		t.Errorf("no-block start must persist nothing: %d write(s)", len(writes))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("fresh create must stamp Creating: got %q", s.Phase)
	}
}

// TestCreate_RetryBlockInProgressLive_Denies: while an authorized
// attempt is in flight elsewhere (a sibling's live Create attempt), a
// disposed fresh-start stays denied — exactly-one-attempt semantics,
// same as the update gate.
func TestCreate_RetryBlockInProgressLive_Denies(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := createGateFixture(t, t0,
		disposedFreshStart(),
		v1beta1.OMENativeInstanceStatus{
			Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
		},
	)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockRetryInProgress, AttemptsStarted: 1},
	}

	if _, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, pod := range createGatePods(t, c, isvc.Namespace) {
		if pod.Labels[query.LabelInstanceIdx] == "0" {
			t.Errorf("live RetryInProgress must deny instance 0's fresh start: pod %s created", pod.Name)
		}
	}
	if writes := persistCalls(*calls); len(writes) != 0 {
		t.Errorf("denied create must not touch the block: %d write(s)", len(writes))
	}
}
