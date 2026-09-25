package ops

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Every pod of a multi-pod Instance is stamped with its gang's
// deterministic PodGroup name. When that name is held by another
// controller, or by an object still being collected, a member created
// against it would join a group this owner does not control — so the
// create choke point withholds the members and leaves the row to the
// escalation pass instead of failing the whole reconcile.

// blockedGangFixture is the gang-surge fixture with its rejected member
// removed, so the pass reaches the create step, and the surge index
// classified as unusable.
func blockedGangFixture(t *testing.T, state workload.GangState) *gangRejectionFixture {
	t.Helper()
	legacyResetExpectations(t)
	f := newGangRejectionFixture(t, "OutOfmemory")
	if err := f.client.Delete(context.Background(), f.dead); err != nil {
		t.Fatalf("clear the pre-existing surge pod: %v", err)
	}
	f.input.Gangs = workload.NewGangObservations()
	f.input.Gangs.Record(f.surgeIndex, workload.GangObservation{
		Name:    query.PodGroupName(f.isvcName, workload.ComponentEngine, f.surgeIndex),
		State:   state,
		Message: "PodGroup is not usable by this owner",
	})
	return f
}

func gangPodCount(t *testing.T, f *gangRejectionFixture) int {
	t.Helper()
	pods := &corev1.PodList{}
	if err := f.client.List(context.Background(), pods, client.InNamespace(f.namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return len(pods.Items)
}

// TestGangSurgeCreate_BlockedPodGroupNameWithholdsMembers: with no
// PodGroup callback wired, the create choke point itself is the gate —
// the replacement gang gets no members and the pass reports no progress
// rather than an error.
func TestGangSurgeCreate_BlockedPodGroupNameWithholdsMembers(t *testing.T) {
	f := blockedGangFixture(t, workload.GangStateOwnershipConflict)

	done, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while its PodGroup name was unusable")
	}
	if got := gangPodCount(t, f); got != 0 {
		t.Fatalf("created %d members against a PodGroup name this owner cannot write", got)
	}
}

// TestGangSurgeCreate_BlockedPodGroupEnsureDefersToEscalation: the
// inline surge ensure reports the same collision as an error. That is
// classified evidence about one row, so the pass defers instead of
// taking every other Instance down with it.
func TestGangSurgeCreate_BlockedPodGroupEnsureDefersToEscalation(t *testing.T) {
	f := blockedGangFixture(t, workload.GangStateTerminating)
	f.deps.EnsureGangPodGroup = func(context.Context, workload.ReconcileInput, workload.ComponentPlan, workload.InstancePlan) (string, error) {
		return "", fmt.Errorf("%w: PodGroup is terminating", workload.ErrGangNameUnusable)
	}

	done, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: got %v want the blocked name deferred, not an error", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while its PodGroup name was still occupied")
	}
	if got := gangPodCount(t, f); got != 0 {
		t.Fatalf("created %d members while the PodGroup name was still occupied", got)
	}
}

// TestGangSurgeCreate_UnblockedGangStillCreates: the gate is scoped to
// the states that make a name unusable. A group this owner controls and
// can write gets its members, so the gate cannot quietly stall a healthy
// surge.
func TestGangSurgeCreate_UnblockedGangStillCreates(t *testing.T) {
	f := blockedGangFixture(t, workload.GangStateNone)

	if _, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target); err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if got := gangPodCount(t, f); got == 0 {
		t.Fatal("a usable gang name must not withhold members")
	}
}

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
// (no status slot, no block) passes the gate and creates — and its
// attempt stamp records nothing (a fresh start with no prior failure
// needs no block).
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

// createGateGangPod fabricates one live gang member in the shape Render
// emits, addressed by runner and ordinal so a fixture can leave a named
// member of the set missing.
func createGateGangPod(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, idx int32, runner string, ordinal int32) {
	t.Helper()
	labels := legacyTestPodLabels(isvc.Name, workload.ComponentEngine, idx, runner, 1, ordinal)
	labels[query.LabelRevisionHash] = testRevisionHashLegacy
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, runner, ordinal),
			Namespace: isvc.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
	}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatalf("seed live gang member %s: %v", pod.Name, err)
	}
}

// pendingGangMemberLost seeds the shape the missing-member rebuild starts
// from: a row that was serving, demoted with its counters intact, whose gang
// has lost one of its two members.
func pendingGangMemberLost(t *testing.T, t0 time.Time) (*workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *[]retryBlockCall, client.Client, *v1beta1.InferenceService) {
	t.Helper()
	input, _, tcr, calls, c, isvc := createGateFixture(t, t0, v1beta1.OMENativeInstanceStatus{
		Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstancePending,
	})
	createGateGangPod(t, c, isvc, 0, "leader", 0)
	return input, legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain), tcr, calls, c, isvc
}

// TestCreate_RetryBlockHeld_DeniesMissingMemberRebuild: a Held block denies
// every pod at its revision, not only the first attempt at it. The rebuild of
// a member the row lost is held on the same record — nothing is created and
// the row is not stamped — and the pass keeps its cadence because a row short
// of its pod set has not converged.
func TestCreate_RetryBlockHeld_DeniesMissingMemberRebuild(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := pendingGangMemberLost(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockHeld, AttemptsStarted: 1, Reason: "ImagePullBackOff"},
	}

	res, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 1 {
		t.Errorf("Held block must deny the missing member: got %d pod(s) want the 1 survivor", len(pods))
	}
	if res.RequeueAfter != input.Requeue.Operation {
		t.Errorf("a row short of its pod set must keep the create cadence: got %v want %v",
			res.RequeueAfter, input.Requeue.Operation)
	}
	if len(*calls) != 0 {
		t.Errorf("denied rebuild must not touch the block: %d MutateRetryBlock call(s)", len(*calls))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstancePending || s.Operation != nil {
		t.Errorf("denied rebuild must not stamp an attempt: phase=%q op=%v", s.Phase, s.Operation)
	}
}

// TestCreate_RetryBlockReleased_RebuildsMissingMember: once the record
// releases the revision the rebuild proceeds unchanged — the missing member
// is created under a stamped Create attempt.
func TestCreate_RetryBlockReleased_RebuildsMissingMember(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, _, c, isvc := pendingGangMemberLost(t, t0)

	if _, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr); err != nil {
		t.Fatalf("create: %v", err)
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 2 {
		t.Fatalf("released revision must rebuild the missing member: got %d pod(s) want 2", len(pods))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("allowed rebuild must stamp Creating: got %q", s.Phase)
	}
}

// TestCreate_RetryBlockBackoffNotDue_AllowsMissingMemberRebuild: the milder
// states gate the opening of an attempt, not the pods of a row already
// holding some. A not-yet-due Backoff therefore leaves the rebuild alone.
func TestCreate_RetryBlockBackoffNotDue_AllowsMissingMemberRebuild(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, _, c, isvc := pendingGangMemberLost(t, t0)
	next := metav1.NewTime(t0.Add(time.Hour))
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &next},
	}

	if _, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr); err != nil {
		t.Fatalf("create: %v", err)
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 2 {
		t.Fatalf("a not-yet-due Backoff must not gate a rebuild: got %d pod(s) want 2", len(pods))
	}
}

// TestCreate_RetryBlockHeld_DeniesRemainderOfInFlightAttempt: a block that
// goes Held while an attempt is mid-materialization stops the rest of that
// attempt's set — the pods it has yet to create are pods at the held revision
// like any other. The attempt is left in place with no external wait
// recorded, so its own deadline stays the backstop.
func TestCreate_RetryBlockHeld_DeniesRemainderOfInFlightAttempt(t *testing.T) {
	t0 := time.Now()
	input, _, tcr, calls, c, isvc := createGateFixture(t, t0, v1beta1.OMENativeInstanceStatus{
		Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
	})
	// The attempt pins the revision it materializes; the fixture learns the
	// target's name only once it is built.
	input.ObservedState.InstanceStatuses[0].Operation.TargetRevision = tcr.Name
	if err := input.MutateInstance(context.Background(), 0, func(s *workload.InstanceStatus) bool {
		s.Operation.TargetRevision = tcr.Name
		return true
	}); err != nil {
		t.Fatalf("pin the attempt's target revision: %v", err)
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	createGateGangPod(t, c, isvc, 0, "leader", 0)
	pinPodRevision(t, c, isvc, query.PodName(isvc.Name, workload.ComponentEngine, 0, "leader", 0), tcr)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockHeld, AttemptsStarted: 1, Reason: "ImagePullBackOff"},
	}

	if _, err := Create(context.Background(), legacyTestDeps(c), *input, plan, tcr); err != nil {
		t.Fatalf("create: %v", err)
	}
	if pods := createGatePods(t, c, isvc.Namespace); len(pods) != 1 {
		t.Errorf("Held block must deny the rest of the set: got %d pod(s) want the 1 already created", len(pods))
	}
	if len(*calls) != 0 {
		t.Errorf("denied create must not touch the block: %d MutateRetryBlock call(s)", len(*calls))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Waiting != "" {
		t.Errorf("a gate denial records no external wait, so the attempt deadline keeps running: op=%+v", s.Operation)
	}
}

// pinPodRevision restamps a seeded pod with the target ControllerRevision's
// hash, so the pass reads it as a member of the attempt pinned to that
// revision rather than as wreckage a retired attempt left behind.
func pinPodRevision(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, name string, target *appsv1.ControllerRevision) {
	t.Helper()
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: name}
	if err := c.Get(context.Background(), key, pod); err != nil {
		t.Fatalf("get seeded pod %s: %v", name, err)
	}
	pod.Labels[query.LabelRevisionHash] = query.RevisionOf(target).Hash()
	if err := c.Update(context.Background(), pod); err != nil {
		t.Fatalf("pin revision hash on %s: %v", name, err)
	}
}

func TestCreateTransitionRollbackPreservesPublicationOnlyFields(t *testing.T) {
	previous := workload.InstanceStatus{
		Index: 4, Incarnation: 3, Phase: workload.InstancePhaseFailed,
		RunningRevision: "revision-a", PodCount: 2, ServingPodCount: 1,
		AvailablePodCount: 1, Admitted: true, ActiveOrdinal: 1,
	}
	committed := previous
	committed.Phase = workload.InstancePhaseCreating
	committed.Operation = &workload.InstanceOperation{ID: "create-4", Type: workload.InstanceOperationCreate}
	transition := statusTransition{index: 4, previous: &previous, current: &committed}
	mutation, ok := transition.rollbackMutation()
	if !ok || mutation.Mutate == nil || mutation.Remove {
		t.Fatalf("rollback mutation = %+v, want restoring mutation", mutation)
	}

	current := committed
	current.ReadyPodCount = 2
	current.ScheduledPodCount = 2
	observedNodes := []string{"node-a", "node-b"}
	current.NodesOccupied = observedNodes
	if !mutation.Mutate(&current) {
		t.Fatal("publication-only changes blocked lifecycle rollback")
	}
	want := previous
	want.ReadyPodCount = 2
	want.ScheduledPodCount = 2
	want.NodesOccupied = []string{"node-a", "node-b"}
	if !reflect.DeepEqual(current, want) {
		t.Fatalf("restored status:\n got: %+v\nwant: %+v", current, want)
	}
	observedNodes[0] = "mutated"
	if current.NodesOccupied[0] != "node-a" {
		t.Fatal("restored node observation aliases the committed status")
	}
}

func TestCreateTransitionRollbackIgnoresExactlyRemovableFields(t *testing.T) {
	committed := workload.InstanceStatus{
		Index: 2, Incarnation: 7, Phase: workload.InstancePhaseCreating,
		RunningRevision: "revision-a", TargetRevision: "revision-b",
		PodCount: 2, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true,
		Operation:     &workload.InstanceOperation{ID: "create-2", Type: workload.InstanceOperationCreate},
		ActiveOrdinal: 1,
	}
	transition := statusTransition{index: 2, current: &committed}
	mutation, ok := transition.rollbackMutation()
	if !ok || !mutation.Remove || mutation.Precondition == nil {
		t.Fatalf("rollback mutation = %+v, want conditional removal", mutation)
	}

	removable := []struct {
		name   string
		mutate func(*workload.InstanceStatus)
	}{
		{name: "ready pods", mutate: func(status *workload.InstanceStatus) { status.ReadyPodCount++ }},
		{name: "scheduled pods", mutate: func(status *workload.InstanceStatus) { status.ScheduledPodCount++ }},
		{name: "occupied nodes", mutate: func(status *workload.InstanceStatus) { status.NodesOccupied = []string{"node-a"} }},
	}
	for _, test := range removable {
		t.Run("allows "+test.name, func(t *testing.T) {
			current := committed
			test.mutate(&current)
			if !mutation.Precondition(&current) {
				t.Fatalf("%s changed rollback ownership", test.name)
			}
		})
	}

	retained := []struct {
		name   string
		mutate func(*workload.InstanceStatus)
	}{
		{name: "index", mutate: func(status *workload.InstanceStatus) { status.Index++ }},
		{name: "incarnation", mutate: func(status *workload.InstanceStatus) { status.Incarnation++ }},
		{name: "phase", mutate: func(status *workload.InstanceStatus) { status.Phase = workload.InstancePhaseReady }},
		{name: "running revision", mutate: func(status *workload.InstanceStatus) { status.RunningRevision = "revision-c" }},
		{name: "target revision", mutate: func(status *workload.InstanceStatus) { status.TargetRevision = "revision-c" }},
		{name: "pod count", mutate: func(status *workload.InstanceStatus) { status.PodCount++ }},
		{name: "serving pods", mutate: func(status *workload.InstanceStatus) { status.ServingPodCount++ }},
		{name: "available pods", mutate: func(status *workload.InstanceStatus) { status.AvailablePodCount++ }},
		{name: "admission", mutate: func(status *workload.InstanceStatus) { status.Admitted = false }},
		{name: "conditions", mutate: func(status *workload.InstanceStatus) {
			status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}}
		}},
		{name: "operation", mutate: func(status *workload.InstanceStatus) { status.Operation = nil }},
		{name: "active ordinal", mutate: func(status *workload.InstanceStatus) { status.ActiveOrdinal++ }},
		{name: "last failure", mutate: func(status *workload.InstanceStatus) {
			status.LastFailure = &workload.InstanceTermination{Reason: "Error"}
		}},
	}
	for _, test := range retained {
		t.Run("rejects "+test.name, func(t *testing.T) {
			current := committed
			test.mutate(&current)
			if mutation.Precondition(&current) {
				t.Fatalf("%s did not fence lifecycle rollback", test.name)
			}
		})
	}
}

// Every create path in the engine funnels through createMissingPods, so a
// classified apiserver rejection must reach the SAME outcome whichever
// operation issued the create. These tests pin that for the sites the
// Create pass does not cover: Restart Phase B, RecreatePod Phase B, the
// per-pod surge, the gang surge, and the migration surge.

// rejectPodCreates wraps c so every Pod create is answered with err.
func rejectPodCreates(t *testing.T, c client.Client, err error) client.Client {
	t.Helper()
	base, ok := c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", c)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return err
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
}

// siteQuotaError is the ResourceQuota admission refusal the apiserver
// returns when a namespace is out of capacity.
func siteQuotaError() error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "engine-pod", errors.New(
		"exceeded quota: team-quota, requested: requests.nvidia.com/gpu=8, used: requests.nvidia.com/gpu=56, limited: requests.nvidia.com/gpu=64"))
}

// siteInvalidError is the apiserver refusing the pod object itself.
func siteInvalidError() error {
	return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "engine-pod", nil)
}

// TestRestartPhaseB_QuotaExceeded_WaitsWithoutFailing: Restart's recreate
// phase hits the same quota wall as a fresh create. The pass must end
// without an error (so the dispatcher's restart interval owns the retry,
// not controller-runtime's escalating backoff) and record the quota as
// the operation's waiting reason rather than failing the Instance.
func TestRestartPhaseB_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	// Phase A already completed: the instance is Restarting at the bumped
	// incarnation and its old pod is gone, so Phase B does the create.
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Recreate", Reason: "pod lost",
		},
	}
	base := legacyNewFakeClient(t, isvc, ir)
	c := rejectPodCreates(t, base, siteQuotaError())
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	done, err := Restart(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], "pod lost")
	if err != nil {
		t.Fatalf("Restart: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("Restart reported done while blocked on quota")
	}

	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Fatalf("instance 0: got Failed want the restart still in flight")
	}
	if s.Operation == nil || !workload.OperationCapacityRefused(legacyFromV1beta1Op(s.Operation)) {
		t.Fatalf("Operation: got %+v want the quota refusal recorded", s.Operation)
	}
	// The restart's own cause must survive the wait: Reason says WHY the
	// operation exists and is part of the terminal-finalize identity
	// tuple, so a transient quota blip may not overwrite it.
	if s.Operation.Reason != "pod lost" {
		t.Errorf("Operation.Reason: got %q want the restart cause %q preserved", s.Operation.Reason, "pod lost")
	}
}

// TestRecreatePhaseB_Throttled_HonorsServerDelay: a 429 during the
// recreate rollout's Phase B is the apiserver pacing us. The pass ends
// clean and deposits the server's suggested delay on the pass pacing, so
// the dispatcher's requeue is floored by it instead of the error path
// escalating a backoff the server never asked for.
func TestRecreatePhaseB_Throttled_HonorsServerDelay(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	targetSpec := legacyTargetSpecImage("example.com/app:v2")
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, base, isvc, targetSpec)
	// Phase A already completed: Updating at the bumped incarnation with
	// no pods left, so Phase B does the create.
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepDrain, TargetRevision: tcr.Name,
		},
	}
	if err := base.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed Phase B status: %v", err)
	}
	c := rejectPodCreates(t, base, apierrors.NewTooManyRequests("apiserver is shedding load", 7))
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	input.Pacing = &workload.APIPacing{}
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, targetSpec)
	if err != nil {
		t.Fatalf("Update: %v (throttling is paced, not failed)", err)
	}
	if done {
		t.Fatal("Update reported done while throttled")
	}
	if got := input.Pacing.Pending(); got != 7*time.Second {
		t.Fatalf("pass pacing: got %v want the server's suggested 7s", got)
	}
	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Errorf("instance 0: got Failed want the rollout still in flight")
	}
	if s.Operation != nil && workload.OperationCapacityRefused(legacyFromV1beta1Op(s.Operation)) {
		t.Errorf("Operation.Waiting: got %q want unset (throttling writes no status)", s.Operation.Waiting)
	}
}

// recreatePhaseBFixture seeds a recreate whose Phase A is done: the
// Instance is Updating at the bumped incarnation with no pods left, so
// the pass's only work is the Phase B rebuild create. Returns the base
// client (unwrapped, for reading status back), the pass input and the
// target revision.
func recreatePhaseBFixture(t *testing.T) (client.Client, *v1beta1.InferenceService, workload.ReconcileInput, *appsv1.ControllerRevision, *corev1.PodSpec) {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	targetSpec := legacyTargetSpecImage("example.com/app:v2")
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, base, isvc, targetSpec)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepDrain, TargetRevision: tcr.Name,
		},
	}
	if err := base.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed Phase B status: %v", err)
	}
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = tcr.Name
	return base, isvc, input, tcr, targetSpec
}

// TestRecreatePhaseB_QuotaExceeded_WaitsWithoutFailing: the recreate has
// already drained and deleted the old incarnation, so a quota wall in
// front of the rebuild leaves the row serving nothing. It is still a
// wait, not a failure: the pass ends clean and the quota lands as the
// operation's waiting token, which is what parks the deadline clock
// while the namespace has no room.
func TestRecreatePhaseB_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	base, isvc, input, tcr, targetSpec := recreatePhaseBFixture(t)
	c := rejectPodCreates(t, base, siteQuotaError())
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, targetSpec)
	if err != nil {
		t.Fatalf("Update: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("Update reported done while blocked on quota")
	}

	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Fatalf("Phase: got %q want Updating (the recreate is still in flight)", s.Phase)
	}
	if s.Operation == nil || !workload.OperationCapacityRefused(legacyFromV1beta1Op(s.Operation)) {
		t.Fatalf("Operation: got %+v want the quota refusal recorded", s.Operation)
	}
}

// TestRecreatePhaseB_RejectionDispositions: the rebuild create meets the
// same classified rejections a fresh create does. A 422 is a statement
// about the revision, so the attempt ends with the revision blamed; a
// terminating namespace ends it with the revision blameless, since no
// corrected pod spec would be admitted either.
func TestRecreatePhaseB_RejectionDispositions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reject     error
		wantReason string
		wantBlocks int
	}{
		{
			name:       "invalid pod spec",
			reject:     siteInvalidError(),
			wantReason: workload.RejectionReasonInvalidPodSpec,
			wantBlocks: 1,
		},
		{
			name:       "namespace terminating",
			reject:     siteNamespaceTerminatingError("engine-pod"),
			wantReason: workload.RejectionReasonNamespaceTerminating,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, isvc, input, tcr, targetSpec := recreatePhaseBFixture(t)
			blocks := &[]string{}
			input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
				b := workload.RetryBlock{TargetRevision: rev}
				if d := mutate(&b); d != workload.RetryBlockUnchanged {
					*blocks = append(*blocks, rev)
				}
				return nil
			}
			c := rejectPodCreates(t, base, tc.reject)
			plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)

			if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, targetSpec); err != nil {
				t.Fatalf("Update: %v (a classified rejection is disposed, not returned)", err)
			}

			s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
			if s.Phase != v1beta1.OMENativeInstanceFailed {
				t.Fatalf("Phase: got %q want Failed", s.Phase)
			}
			if s.Operation != nil {
				t.Errorf("Operation: got %+v want cleared", s.Operation)
			}
			if s.LastFailure == nil || s.LastFailure.Reason != tc.wantReason {
				t.Errorf("LastFailure: got %+v want reason %s", s.LastFailure, tc.wantReason)
			}
			if len(*blocks) != tc.wantBlocks {
				t.Errorf("RetryBlock writes: got %+v want %d", *blocks, tc.wantBlocks)
			}
		})
	}
}

// TestSurgeCreate_QuotaExceeded_WaitsWithoutFailing: the per-pod surge's
// Phase 1 create is quota-blocked. The source stays serving and the
// rollout waits with its clock parked — it must not surface an error.
func TestSurgeCreate_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := makeCR(t, base, isvc, "llama-70b-engine-rev-abc12345")
	sourcePod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := base.Create(context.Background(), sourcePod); err != nil {
		t.Fatalf("seed source pod: %v", err)
	}
	c := rejectPodCreates(t, base, siteQuotaError())
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	plan := surgePlan()

	done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr,
		[]*corev1.Pod{sourcePod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("surgeUpdate reported done while blocked on quota")
	}
	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Fatalf("instance 0: got Failed want the surge still in flight")
	}
	if s.Operation == nil || !workload.OperationCapacityRefused(legacyFromV1beta1Op(s.Operation)) {
		t.Fatalf("Operation: got %+v want the quota refusal recorded", s.Operation)
	}
}

// gangSurgeSourceIndex is the source half of the gang surge fixture pair.
const gangSurgeSourceIndex = int32(0)

// gangSurgeRejectionFixture is a gang surge mid-flight: the source at
// index 0 pinned to the replacement gang's marker at index 2, both on
// the target revision, with a mutation store the pass writes through.
func gangSurgeRejectionFixture(t *testing.T) (workload.ReconcileInput, *terminalMutationStore, int32, string) {
	t.Helper()
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-quota", "test-ns"
	surgeIndex := int32(2)
	revision := "gang-quota-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-quota-engine-oldrev",
		TargetRevision:  revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: revision,
			SurgeIndex:     &surgeIndex,
		},
	}
	marker := workload.InstanceStatus{
		Index:          surgeIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		TargetRevision: revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-target-2",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTarget,
			TargetRevision: revision,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index: cloneTerminalStatus(source),
			surgeIndex:   cloneTerminalStatus(marker),
		},
	}
	input := workload.ReconcileInput{
		OwnerObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		Key: workload.Key{
			Namespace: namespace,
			OwnerName: isvcName,
			Component: workload.ComponentEngine,
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: []workload.InstanceStatus{cloneTerminalStatus(source), cloneTerminalStatus(marker)},
		},
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec: legacyTargetSpecImage("example.com/app:v2"),
		},
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			return store.apply(context.Background(),
				[]workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
		},
		FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	return input, store, surgeIndex, revision
}

// TestGangSurgeCreate_QuotaExceeded_WaitsWithoutFailing: the gang surge
// creates a whole replacement gang at once, so it is the most likely site
// to exhaust a quota. It must wait like every other site rather than
// erroring the pass — AND the wait must reach the SOURCE, whose operation
// and deadline govern the rollout while the pods are created under the
// surge index. The test drives the two readers that consume it (the
// deadline park, the escalation skip) to prove the source is held.
func TestGangSurgeCreate_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	input, store, surgeIndex, revision := gangSurgeRejectionFixture(t)
	base := legacyNewFakeClient(t)
	deps := legacyTestDeps(rejectPodCreates(t, base, siteQuotaError()))
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while blocked on quota")
	}
	blocked, found := store.statuses[surgeIndex]
	if !found {
		t.Fatalf("surge marker missing after the blocked pass")
	}
	if blocked.Phase == workload.InstancePhaseFailed {
		t.Fatalf("surge instance: got Failed want the surge still in flight")
	}
	if !workload.OperationCapacityRefused(blocked.Operation) {
		t.Fatalf("surge Operation: got %+v want the quota refusal recorded", blocked.Operation)
	}

	// The SOURCE carries the deadline the rollout is judged by, and the
	// readers that hold it (the deadline park, the escalation skip) live
	// in the parent workload package, which cannot be imported from here.
	// Their side of this contract is pinned by
	// TestGangSurgeSource_HeldWhileItsSurgeWaitsOnQuota.
	if blocked.Operation.SurgeIndex != nil {
		t.Errorf("surge row Operation.SurgeIndex: got %v want nil (the pin lives on the source)", blocked.Operation.SurgeIndex)
	}
	if src := store.statuses[gangSurgeSourceIndex]; src.Operation == nil || src.Operation.SurgeIndex == nil ||
		*src.Operation.SurgeIndex != surgeIndex {
		t.Errorf("source Operation: got %+v want the surge pin the readers follow", src.Operation)
	}
}

// TestMigrateSurge_InvalidPodSpec_FailsTheMigration: a surge pod the
// apiserver will never accept cannot be waited out, so the migration
// record is closed Failed immediately instead of idling to its deadline.
func TestMigrateSurge_InvalidPodSpec_FailsTheMigration(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	f := newSinglePodMigFixture(t)
	f.clk = clk
	const uuid = "mig-surge-invalid-spec"
	f.records = []workload.MigrationRecord{
		mkMigRecordWithDeadline(uuid, 0, "node-a", clk.Now().Add(time.Minute)),
	}
	f.c = rejectPodCreates(t, f.c, siteInvalidError())

	// passResult builds its own input, so drive Migrate directly to
	// observe the RetryBlock seam this test is about.
	legacyResetExpectations(t)
	record := f.record(t, uuid)
	req := &audit.MigrationRequest{
		SchemaVersion: audit.SchemaV1,
		Component:     string(f.component),
		Instance:      record.SourceInstance,
		FromNode:      record.FromNode,
		Reason:        record.Reason,
	}
	in := f.input(t)
	blocks := &[]string{}
	in.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			*blocks = append(*blocks, rev)
		}
		return nil
	}

	done, accepted, err := Migrate(context.Background(), f.deps(), in, f.plan, record.SourceInstance, uuid, req)
	if err != nil {
		t.Fatalf("migrate pass: %v", err)
	}
	if !accepted {
		t.Fatalf("migrate pass: accepted=false want true")
	}
	if !done {
		t.Fatalf("migrate pass: done=false want true (the request is closed, not waiting)")
	}
	rec := f.record(t, uuid)
	if rec.Phase != workload.MigrationPhaseFailed {
		t.Fatalf("record phase: got %s want Failed", rec.Phase)
	}
	if rec.CompletedAt == nil {
		t.Errorf("record CompletedAt: got nil want the close timestamp")
	}
	// The surge pod carries the request's placement overlay, so a 422 may
	// indict the overlay rather than the revision. Closing the request is
	// the whole remedy; holding the revision would wedge an innocent
	// rollout on an operator's bad migration request.
	if len(*blocks) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none for an overlay-bearing surge", *blocks)
	}
}

// siteNamespaceTerminatingError is the apiserver refusing a create
// because the namespace is on its way out.
func siteNamespaceTerminatingError(name string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    403,
		Reason:  metav1.StatusReasonForbidden,
		Message: `pods "` + name + `" is forbidden: unable to create new content in namespace prod because it is being terminated`,
		Details: &metav1.StatusDetails{
			Kind: "pods", Name: name,
			Causes: []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause, Message: "namespace prod is being terminated"}},
		},
	}}
}

// restartPhaseBRow is a repair whose Phase A is done: the old pod is
// gone and the bumped incarnation is waiting for its rebuild create.
func restartPhaseBRow() v1beta1.OMENativeInstanceStatus {
	return v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Recreate", Reason: "pod lost",
		},
	}
}

// TestRestartPhaseB_RejectionDispositions: the rebuild create meets the
// same classified rejections a fresh create does, and the repair answers
// them the same way — a 422 is permanent and ends the attempt with the
// revision blamed, a terminating namespace ends it with the revision
// blameless, and a 429 is pacing that writes nothing at all.
func TestRestartPhaseB_RejectionDispositions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reject     error
		wantFailed bool
		wantReason string
		wantBlocks int
	}{
		{
			name:       "invalid pod spec",
			reject:     siteInvalidError(),
			wantFailed: true,
			wantReason: workload.RejectionReasonInvalidPodSpec,
			wantBlocks: 1,
		},
		{
			name:       "namespace terminating",
			reject:     siteNamespaceTerminatingError("engine-pod"),
			wantFailed: true,
			wantReason: workload.RejectionReasonNamespaceTerminating,
		},
		{
			name:   "throttled",
			reject: apierrors.NewTooManyRequests("apiserver is shedding load", 7),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacyResetExpectations(t)
			isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
			ir.Status.InstanceStatuses[0] = restartPhaseBRow()
			base := legacyNewFakeClient(t, isvc, ir)
			c := rejectPodCreates(t, base, tc.reject)
			input := legacyTestInput(isvc, base, workload.ComponentEngine)
			// The repair pins no revision of its own, so the revision a
			// 422 blames is the owner's current one.
			input.ObservedState.UpdateRevision = "llama-70b-engine-newhash"
			blocks := &[]string{}
			input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
				b := workload.RetryBlock{TargetRevision: rev}
				if d := mutate(&b); d != workload.RetryBlockUnchanged {
					*blocks = append(*blocks, rev)
				}
				return nil
			}
			plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

			if _, err := Restart(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], "pod lost"); err != nil {
				t.Fatalf("Restart: %v (a classified rejection is disposed, not returned)", err)
			}

			s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
			if tc.wantFailed {
				if s.Phase != v1beta1.OMENativeInstanceFailed {
					t.Fatalf("Phase: got %q want Failed", s.Phase)
				}
				if s.LastFailure == nil || s.LastFailure.Reason != tc.wantReason {
					t.Errorf("LastFailure: got %+v want reason %s", s.LastFailure, tc.wantReason)
				}
			} else {
				if s.Phase != v1beta1.OMENativeInstanceRestarting {
					t.Errorf("Phase: got %q want Restarting (throttling is paced, not failed)", s.Phase)
				}
				if s.LastFailure != nil {
					t.Errorf("LastFailure: got %+v want none (throttling writes no status)", s.LastFailure)
				}
				if s.Operation == nil || s.Operation.Reason != "pod lost" {
					t.Errorf("Operation: got %+v want the repair intact", s.Operation)
				}
			}
			if len(*blocks) != tc.wantBlocks {
				t.Errorf("RetryBlock writes: got %+v want %d", *blocks, tc.wantBlocks)
			}
		})
	}
}

// gangNamespaceTerminatingError is the apiserver refusing a gang member
// because the namespace is on its way out.
func gangNamespaceTerminatingError(name string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    403,
		Reason:  metav1.StatusReasonForbidden,
		Message: `pods "` + name + `" is forbidden: unable to create new content in namespace test-ns because it is being terminated`,
		Details: &metav1.StatusDetails{
			Kind: "pods", Name: name,
			Causes: []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause, Message: "namespace test-ns is being terminated"}},
		},
	}}
}

// TestGangSurgeCreate_RejectionDispositions: a quota refusal is the one
// gang-member rejection worth waiting out. The permanent ones are not —
// a 422 and a terminating namespace both dispose the surge row on the
// first rejection rather than idling it to the deadline, and they differ
// only in whether a revision is to blame. A 429 is neither: it is pacing,
// and it writes nothing at all.
func TestGangSurgeCreate_RejectionDispositions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reject     error
		wantFailed bool
		wantReason string
		wantBlocks int
	}{
		{
			name:       "invalid pod spec",
			reject:     siteInvalidError(),
			wantFailed: true,
			wantReason: workload.RejectionReasonInvalidPodSpec,
			wantBlocks: 1,
		},
		{
			name:       "namespace terminating",
			reject:     gangNamespaceTerminatingError("engine-pod"),
			wantFailed: true,
			wantReason: workload.RejectionReasonNamespaceTerminating,
		},
		{
			name:   "throttled",
			reject: apierrors.NewTooManyRequests("apiserver is shedding load", 7),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, store, surgeIndex, revision := gangSurgeRejectionFixture(t)
			blocks := &[]string{}
			input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
				b := workload.RetryBlock{TargetRevision: rev}
				if d := mutate(&b); d != workload.RetryBlockUnchanged {
					*blocks = append(*blocks, rev)
				}
				return nil
			}
			base := legacyNewFakeClient(t)
			deps := legacyTestDeps(rejectPodCreates(t, base, tc.reject))
			plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

			if _, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target); err != nil {
				t.Fatalf("gangSurgeUpdate: %v (a classified rejection is disposed, not returned)", err)
			}

			marker, found := store.statuses[surgeIndex]
			if !found {
				t.Fatalf("surge marker missing after the rejected pass")
			}
			if tc.wantFailed {
				if marker.Phase != workload.InstancePhaseFailed {
					t.Fatalf("marker Phase: got %q want Failed (the rejection is permanent)", marker.Phase)
				}
				if marker.LastFailure == nil || marker.LastFailure.Reason != tc.wantReason {
					t.Errorf("marker LastFailure: got %+v want reason %s", marker.LastFailure, tc.wantReason)
				}
				if marker.Operation != nil {
					t.Errorf("marker Operation: got %+v want cleared with the disposal", marker.Operation)
				}
			} else {
				if marker.Phase == workload.InstancePhaseFailed {
					t.Errorf("marker Phase: got Failed want the surge still in flight (throttling is paced)")
				}
				if marker.LastFailure != nil {
					t.Errorf("marker LastFailure: got %+v want none (throttling writes no failure accounting)", marker.LastFailure)
				}
			}
			if len(*blocks) != tc.wantBlocks {
				t.Errorf("RetryBlock writes: got %v want %d (only a bad pod spec blames the revision)", *blocks, tc.wantBlocks)
			}
		})
	}
}

// renderForSplitTest renders a pod through the same path the create loop
// uses, so the split-risk check sees the injector's real output (an
// injected topologyKey term, a preserved user term, or nothing).
func renderForSplitTest(t *testing.T, ps *corev1.PodSpec, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan) *corev1.Pod {
	t.Helper()
	pod, err := testRender(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	return pod
}

// countGangSplitWarnings drains the recorder and returns how many
// GangSplitRisk Warning events it buffered.
func countGangSplitWarnings(rec *record.FakeRecorder) int {
	n := 0
	for drained := false; !drained; {
		select {
		case e := <-rec.Events:
			if strings.Contains(e, string(workload.EventReasonGangSplitRisk)) {
				n++
			}
		default:
			drained = true
		}
	}
	return n
}

// withUserPodAffinity stamps a hand-written required podAffinity term on
// ps — the "operator already co-located the gang themselves" shape.
func withUserPodAffinity(ps *corev1.PodSpec) *corev1.PodSpec {
	ps.Affinity = &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey:   "kubernetes.io/hostname",
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "x"}},
			}},
		},
	}
	return ps
}

// splitRiskInput wires a one-row status store under the ReconcileInput so
// the announcement has a row to record itself on, which is what decides
// whether the warning fires.
func splitRiskInput(owner client.Object, idx int32) workload.ReconcileInput {
	rows := map[int32]workload.InstanceStatus{
		idx: {Index: idx, Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{ID: "create-0", Type: workload.InstanceOperationCreate}},
	}
	return workload.ReconcileInput{
		OwnerObject: owner,
		MutateInstance: func(_ context.Context, index int32, mutate func(*workload.InstanceStatus) bool) error {
			row := rows[index]
			if mutate(&row) {
				rows[index] = row
			}
			return nil
		},
	}
}

func warnSplitRisk(t *testing.T, rec record.EventRecorder, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, pod *corev1.Pod) {
	t.Helper()
	if err := maybeWarnGangSplitRisk(context.Background(), workload.Deps{Recorder: rec}, input, plan, inst, runner, pod); err != nil {
		t.Fatalf("warn gang split risk: %v", err)
	}
}

func TestMaybeWarnGangSplitRisk(t *testing.T) {
	gangPlan, gangInst, gangRunners := multiPodPlan(3) // TopologyKey unset
	keyedPlan, keyedInst, keyedRunners := multiPodPlanWithTopologyKey(3, "topology.example.com/domain")
	singlePlan, singleInst, singleRunner := singlePodPlan()

	leader := gangRunners[0]
	worker := gangRunners[1]

	cases := []struct {
		name     string
		ps       *corev1.PodSpec
		plan     workload.ComponentPlan
		inst     workload.InstancePlan
		runner   workload.RunnerPlan
		wantWarn bool
	}{
		// The only risk shape: a gang worker that, after render, carries no
		// required podAffinity — no key resolved and no user term.
		{"gang worker, no key, no user affinity", basicPodSpec(), gangPlan, gangInst, worker, true},
		// A resolved topologyKey means the injector added a co-location term.
		{"gang worker, resolved topologyKey", basicPodSpec(), keyedPlan, keyedInst, keyedRunners[1], false},
		// A user-declared podAffinity is preserved on the pod — operator owns it.
		{"gang worker, user podAffinity", withUserPodAffinity(basicPodSpec()), gangPlan, gangInst, worker, false},
		// The leader is the domain anchor; it never carries the term.
		{"gang leader (anchor)", basicPodSpec(), gangPlan, gangInst, leader, false},
		// Single-pod Instances have nothing to co-locate.
		{"single-pod default", basicPodSpec(), singlePlan, singleInst, singleRunner, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := record.NewFakeRecorder(8)
			input := splitRiskInput(basicISVC(), tc.inst.Index)
			pod := renderForSplitTest(t, tc.ps, tc.plan, tc.inst, tc.runner)

			warnSplitRisk(t, rec, input, tc.plan, tc.inst, tc.runner, pod)

			got := countGangSplitWarnings(rec)
			want := 0
			if tc.wantWarn {
				want = 1
			}
			if got != want {
				t.Errorf("GangSplitRisk warnings: got %d want %d", got, want)
			}
		})
	}
}

// TestMaybeWarnGangSplitRisk_DedupPerRow pins that the warning fires
// once per episode on the Instance's own row — a 3-worker gang rendering
// three at-risk worker pods yields a single event, not three.
func TestMaybeWarnGangSplitRisk_DedupPerRow(t *testing.T) {
	rec := record.NewFakeRecorder(8)
	plan, inst, runners := multiPodPlan(3)
	input := splitRiskInput(basicISVC(), inst.Index)
	worker := runners[1]
	pod := renderForSplitTest(t, basicPodSpec(), plan, inst, worker)

	warnSplitRisk(t, rec, input, plan, inst, worker, pod)
	warnSplitRisk(t, rec, input, plan, inst, worker, pod)

	if got := countGangSplitWarnings(rec); got != 1 {
		t.Errorf("want exactly 1 warning after two calls (dedup), got %d", got)
	}
}

// TestMaybeWarnGangSplitRisk_NilSafe pins the nil-recorder / nil-target
// no-op contract so callers never have to branch.
func TestMaybeWarnGangSplitRisk_NilSafe(t *testing.T) {
	plan, inst, runners := multiPodPlan(3)
	worker := runners[1]
	pod := renderForSplitTest(t, basicPodSpec(), plan, inst, worker)

	// nil recorder, valid target → no panic.
	warnSplitRisk(t, nil, splitRiskInput(basicISVC(), inst.Index), plan, inst, worker, pod)
	// valid recorder, nil target (no OwnerObject/EventTarget) → no panic, no event.
	rec := record.NewFakeRecorder(8)
	warnSplitRisk(t, rec, workload.ReconcileInput{}, plan, inst, worker, pod)
	if got := countGangSplitWarnings(rec); got != 0 {
		t.Errorf("nil-target must emit nothing, got %d", got)
	}
}

func TestMaybeWarnGangSplitRisk_RecommendationIsProviderNeutral(t *testing.T) {
	rec := record.NewFakeRecorder(1)
	plan, inst, runners := multiPodPlan(2)
	pod := renderForSplitTest(t, basicPodSpec(), plan, inst, runners[1])

	warnSplitRisk(t, rec, splitRiskInput(basicISVC(), inst.Index), plan, inst, runners[1], pod)

	select {
	case event := <-rec.Events:
		if !strings.Contains(event, ".topologyKey") {
			t.Fatalf("recommendation does not name topologyKey: %q", event)
		}
		if strings.Contains(event, "cloud.google.com") {
			t.Fatalf("recommendation must not prescribe a provider-specific label: %q", event)
		}
	default:
		t.Fatal("expected GangSplitRisk event")
	}
}
