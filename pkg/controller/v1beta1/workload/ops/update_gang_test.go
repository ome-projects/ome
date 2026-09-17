package ops

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestEmptyGangSurgeTargetSlotIgnoresPodDerivedObservations(t *testing.T) {
	if !emptyGangSurgeTargetSlot(&workload.InstanceStatus{Index: 7}) {
		t.Fatal("zero-valued row should be an empty surge target slot")
	}
	if emptyGangSurgeTargetSlot(nil) {
		t.Fatal("nil row should not be an empty surge target slot")
	}

	for _, test := range []struct {
		name   string
		mutate func(*workload.InstanceStatus)
	}{
		{name: "ready pods", mutate: func(status *workload.InstanceStatus) { status.ReadyPodCount = 1 }},
		{name: "scheduled pods", mutate: func(status *workload.InstanceStatus) { status.ScheduledPodCount = 1 }},
		{name: "occupied nodes", mutate: func(status *workload.InstanceStatus) { status.NodesOccupied = []string{"node-a"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := &workload.InstanceStatus{Index: 7}
			test.mutate(status)
			if !emptyGangSurgeTargetSlot(status) {
				t.Fatalf("pod-derived %s should not own an empty surge target slot", test.name)
			}
		})
	}

	for _, test := range []struct {
		name   string
		mutate func(*workload.InstanceStatus)
	}{
		{name: "incarnation", mutate: func(status *workload.InstanceStatus) { status.Incarnation = 1 }},
		{name: "phase", mutate: func(status *workload.InstanceStatus) { status.Phase = workload.InstancePhaseReady }},
		{name: "running revision", mutate: func(status *workload.InstanceStatus) { status.RunningRevision = "revision-a" }},
		{name: "target revision", mutate: func(status *workload.InstanceStatus) { status.TargetRevision = "revision-b" }},
		{name: "operation", mutate: func(status *workload.InstanceStatus) { status.Operation = &workload.InstanceOperation{} }},
		{name: "pod count", mutate: func(status *workload.InstanceStatus) { status.PodCount = 1 }},
		{name: "serving pods", mutate: func(status *workload.InstanceStatus) { status.ServingPodCount = 1 }},
		{name: "available pods", mutate: func(status *workload.InstanceStatus) { status.AvailablePodCount = 1 }},
		{name: "admission", mutate: func(status *workload.InstanceStatus) { status.Admitted = true }},
		{name: "conditions", mutate: func(status *workload.InstanceStatus) { status.Conditions = []metav1.Condition{{Type: "Ready"}} }},
		{name: "last failure", mutate: func(status *workload.InstanceStatus) { status.LastFailure = &workload.InstanceTermination{} }},
	} {
		t.Run("rejects "+test.name, func(t *testing.T) {
			status := &workload.InstanceStatus{Index: 7}
			test.mutate(status)
			if emptyGangSurgeTargetSlot(status) {
				t.Fatalf("retained %s must keep ownership of the surge target slot", test.name)
			}
		})
	}
}

// gangSurgePod fabricates a surge-gang pod at the given instance index +
// runner ordinal with the OMENative selector labels LiveListPodsForInstance
// filters on.
func gangSurgePod(isvc, ns string, instIdx int32, runner string, revHash string) *corev1.Pod {
	labels := legacyTestPodLabels(isvc, workload.ComponentEngine, instIdx, runner, 1, 0)
	labels[query.LabelRevisionHash] = revHash
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc, workload.ComponentEngine, instIdx, runner, 0),
			Namespace: ns,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "bad:tag"}}},
	}
}

// gangAbandonInput builds a closure-backed ReconcileInput that records
// RemoveInstance calls and mutates an in-memory source InstanceStatus so the
// abandon path's reset is observable.
func gangAbandonInput(isvc, ns string, src *workload.InstanceStatus, removed *[]int32) workload.ReconcileInput {
	return workload.ReconcileInput{
		Key: workload.Key{
			Namespace: ns,
			OwnerName: isvc,
			Component: workload.ComponentEngine,
			SelectorLabels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			if idx == src.Index {
				mutate(src)
			}
			return nil
		},
		RemoveInstance: func(_ context.Context, idx int32) (bool, error) {
			*removed = append(*removed, idx)
			return true, nil
		},
	}
}

// TestAbandonFailedGangSurge_DeletesStalePodsFirst pins the first phase:
// while the wedged surge gang's pods still exist, abandon deletes them and
// does NOT yet drop the marker or reset the source.
func TestAbandonFailedGangSurge_DeletesStalePodsFirst(t *testing.T) {
	legacyResetExpectations(t)
	const isvc, ns = "gang-a", "test-ns"
	leader := gangSurgePod(isvc, ns, 2, "leader", "badrev")
	worker := gangSurgePod(isvc, ns, 2, "worker", "badrev")
	c := legacyNewFakeClient(t, leader, worker)

	src := &workload.InstanceStatus{
		Index:           0,
		Phase:           workload.InstancePhaseFailed,
		RunningRevision: "gang-a-engine-goodrev",
	}
	var removed []int32
	input := gangAbandonInput(isvc, ns, src, &removed)
	blockCalls := recordRetryBlockCalls(&input, nil)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	done, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, 2, src.RunningRevision, "gang-a-engine-badrev", "pod stuck", true)
	if err != nil {
		t.Fatalf("abandonFailedGangSurge: %v", err)
	}
	if done {
		t.Errorf("done: got true want false (abandon always requeues)")
	}
	// Both surge pods deleted.
	remaining, err := query.LiveListPodsForInstance(context.Background(), c, ns, isvc, workload.ComponentEngine, 2)
	if err != nil {
		t.Fatalf("list surge pods: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("surge pods: got %d want 0 (all deleted)", len(remaining))
	}
	// Marker not yet dropped, source not yet reset — that's the next pass.
	if len(removed) != 0 {
		t.Errorf("RemoveInstance calls: got %v want none while pods still present", removed)
	}
	if src.Phase != workload.InstancePhaseFailed {
		t.Errorf("source Phase: got %q want still Failed (reset happens after pods gone)", src.Phase)
	}
	// Writer-ordering invariant: no RetryBlock write while the failed
	// attempt's Operation is still in flight (reset pass records it).
	if len(*blockCalls) != 0 {
		t.Errorf("MutateRetryBlock calls: got %d want 0 before the reset pass", len(*blockCalls))
	}
}

// recordRetryBlockCalls wires a recording MutateRetryBlock closure (same
// pattern as retryGateFixture's) onto input, reading existing blocks from
// input.ObservedState.RetryBlocks.
func recordRetryBlockCalls(input *workload.ReconcileInput, existing []workload.RetryBlock) *[]retryBlockCall {
	input.ObservedState.RetryBlocks = existing
	calls := &[]retryBlockCall{}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		var b workload.RetryBlock
		if found := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev); found != nil {
			b = *found
		} else {
			b = workload.RetryBlock{TargetRevision: rev}
		}
		d := mutate(&b)
		*calls = append(*calls, retryBlockCall{rev: rev, disposition: d, block: b})
		return nil
	}
	return calls
}

// TestAbandonFailedGangSurge_ResetsSourceAfterPodsGone pins the second
// phase: once the surge gang's pods are gone, abandon drops the surge
// marker and resets the source to Ready on its running revision with the
// failed Operation cleared — so the next reconcile fires a fresh surge.
func TestAbandonFailedGangSurge_ResetsSourceAfterPodsGone(t *testing.T) {
	legacyResetExpectations(t)
	const isvc, ns = "gang-a", "test-ns"
	c := legacyNewFakeClient(t) // no surge pods — already deleted

	surgeIdx := int32(2)
	src := &workload.InstanceStatus{
		Index:           0,
		Phase:           workload.InstancePhaseFailed,
		RunningRevision: "gang-a-engine-goodrev",
		TargetRevision:  "gang-a-engine-badrev",
		Operation: &workload.InstanceOperation{
			Type:       workload.InstanceOperationUpdate,
			Step:       updateStepSurge,
			SurgeIndex: &surgeIdx,
		},
	}
	var removed []int32
	input := gangAbandonInput(isvc, ns, src, &removed)
	blockCalls := recordRetryBlockCalls(&input, nil)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	done, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, surgeIdx, src.RunningRevision, src.TargetRevision, "pod stuck", true)
	if err != nil {
		t.Fatalf("abandonFailedGangSurge: %v", err)
	}
	if done {
		t.Errorf("done: got true want false (abandon always requeues)")
	}
	if len(removed) != 1 || removed[0] != surgeIdx {
		t.Errorf("RemoveInstance calls: got %v want [%d]", removed, surgeIdx)
	}
	if src.Phase != workload.InstancePhaseReady {
		t.Errorf("source Phase: got %q want Ready", src.Phase)
	}
	if src.Operation != nil {
		t.Errorf("source Operation: got %+v want nil (failed surge cleared)", src.Operation)
	}
	if src.RunningRevision != "gang-a-engine-goodrev" {
		t.Errorf("source RunningRevision: got %q want gang-a-engine-goodrev (unchanged)", src.RunningRevision)
	}
	if src.TargetRevision != "" {
		t.Errorf("source TargetRevision: got %q want cleared", src.TargetRevision)
	}
	// The reset pass records the failed target's block (writer-ordering:
	// same pass as the Operation-clearing reset) and the reset's own prune
	// targets only the OLD running revision — the fresh block survives.
	if len(*blockCalls) != 2 {
		t.Fatalf("MutateRetryBlock calls: got %d want 2 (record failed target, prune old rev)", len(*blockCalls))
	}
	if rec := (*blockCalls)[0]; rec.rev != "gang-a-engine-badrev" || rec.disposition != workload.RetryBlockPersist {
		t.Errorf("record call: got (rev=%q, disposition=%v) want (gang-a-engine-badrev, Persist)", rec.rev, rec.disposition)
	}
	if prune := (*blockCalls)[1]; prune.rev != "gang-a-engine-goodrev" || prune.disposition != workload.RetryBlockRemove {
		t.Errorf("prune call: got (rev=%q, disposition=%v) want (gang-a-engine-goodrev, Remove)", prune.rev, prune.disposition)
	}
}

// TestAbandonFailedGangSurge_RecordsRetryBlockWithPolicy drives the reset
// pass with a configured RetryPolicy and asserts the recorded block is a
// counted Backoff wave: AttemptsStarted=1, NextRetryAt = now + initial
// delay off the fake clock, evidence stamped — all landed in the SAME
// pass that cleared the failed attempt's Operation.
func TestAbandonFailedGangSurge_RecordsRetryBlockWithPolicy(t *testing.T) {
	legacyResetExpectations(t)
	const isvc, ns = "gang-a", "test-ns"
	c := legacyNewFakeClient(t) // surge pods already deleted

	t0 := time.Now()
	surgeIdx := int32(2)
	src := &workload.InstanceStatus{
		Index:           0,
		Phase:           workload.InstancePhaseFailed,
		RunningRevision: "gang-a-engine-goodrev",
		TargetRevision:  "gang-a-engine-badrev",
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepSurge,
			SurgeIndex:     &surgeIdx,
			TargetRevision: "gang-a-engine-badrev",
		},
		LastFailure: &workload.InstanceTermination{PodName: "gang-a-engine-2-leader-0", Reason: "ImagePullBackOff"},
	}
	var removed []int32
	input := gangAbandonInput(isvc, ns, src, &removed)
	input.Clock = clocktesting.NewFakeClock(t0)
	input.UpdateRetryPolicy = retryTestPolicy()
	blockCalls := recordRetryBlockCalls(&input, nil)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	done, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, surgeIdx,
		src.RunningRevision, src.Operation.TargetRevision, instanceFailureReason(src, "gang surge abandoned"), instanceFailureWorkloadCaused(src))
	if err != nil {
		t.Fatalf("abandonFailedGangSurge: %v", err)
	}
	if done {
		t.Errorf("done: got true want false")
	}
	if src.Operation != nil || src.Phase != workload.InstancePhaseReady {
		t.Errorf("source: got (phase=%q, op=%+v) want (Ready, nil) — Operation cleared in the same pass", src.Phase, src.Operation)
	}
	if len(*blockCalls) != 2 {
		t.Fatalf("MutateRetryBlock calls: got %d want 2", len(*blockCalls))
	}
	rec := (*blockCalls)[0]
	if rec.rev != "gang-a-engine-badrev" || rec.disposition != workload.RetryBlockPersist {
		t.Fatalf("record call: got (rev=%q, disposition=%v) want (gang-a-engine-badrev, Persist)", rec.rev, rec.disposition)
	}
	if rec.block.State != workload.RetryBlockBackoff || rec.block.AttemptsStarted != 1 {
		t.Errorf("block: got (state=%q, attempts=%d) want (Backoff, 1)", rec.block.State, rec.block.AttemptsStarted)
	}
	if rec.block.NextRetryAt == nil || !rec.block.NextRetryAt.Time.Equal(t0.Add(time.Minute)) {
		t.Errorf("NextRetryAt: got %v want %v", rec.block.NextRetryAt, t0.Add(time.Minute))
	}
	if rec.block.Reason != "pod gang-a-engine-2-leader-0 stuck (ImagePullBackOff)" {
		t.Errorf("Reason: got %q want the LastFailure evidence", rec.block.Reason)
	}
	if prune := (*blockCalls)[1]; prune.rev != "gang-a-engine-goodrev" || prune.disposition != workload.RetryBlockRemove {
		t.Errorf("prune call: got (rev=%q, disposition=%v) want (gang-a-engine-goodrev, Remove)", prune.rev, prune.disposition)
	}
}

// gangAbandonWave drives one reset-pass abandon (surge pods already gone)
// for a Failed source whose LastFailure carries failure, with the blocks
// persisted by prior waves seeded into ObservedState. Returns the wave's
// MutateRetryBlock calls and WarnRetryHeld invocations.
func gangAbandonWave(t *testing.T, t0 time.Time, failure *workload.InstanceTermination, persisted []workload.RetryBlock) (*[]retryBlockCall, *[]retryHeldWarning) {
	t.Helper()
	legacyResetExpectations(t)
	const isvc, ns = "gang-a", "test-ns"
	c := legacyNewFakeClient(t) // surge pods already deleted
	surgeIdx := int32(2)
	src := &workload.InstanceStatus{
		Index:           0,
		Phase:           workload.InstancePhaseFailed,
		RunningRevision: "gang-a-engine-goodrev",
		TargetRevision:  "gang-a-engine-badrev",
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepSurge,
			SurgeIndex:     &surgeIdx,
			TargetRevision: "gang-a-engine-badrev",
		},
		LastFailure: failure,
	}
	var removed []int32
	input := gangAbandonInput(isvc, ns, src, &removed)
	input.Clock = clocktesting.NewFakeClock(t0)
	input.UpdateRetryPolicy = retryTestPolicy()
	calls := recordRetryBlockCalls(&input, persisted)
	warns := &[]retryHeldWarning{}
	input.WarnRetryHeld = func(rev string, attempts int32, reason string) {
		*warns = append(*warns, retryHeldWarning{rev: rev, attempts: attempts, reason: reason})
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	if _, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, surgeIdx,
		src.RunningRevision, src.Operation.TargetRevision,
		instanceFailureReason(src, "gang surge abandoned"), instanceFailureWorkloadCaused(src)); err != nil {
		t.Fatalf("abandonFailedGangSurge: %v", err)
	}
	if src.Operation != nil || src.Phase != workload.InstancePhaseReady {
		t.Fatalf("source: got (phase=%q, op=%+v) want (Ready, nil)", src.Phase, src.Operation)
	}
	return calls, warns
}

// nextGangAbandonWave models the machinery between two abandon waves: the
// retry gate denies until the recorded Backoff is due, admits at
// NextRetryAt, and the attempt stamp then flips the block to
// RetryInProgress for the new attempt. Returns the persisted block set the
// next wave observes.
func nextGangAbandonWave(t *testing.T, block workload.RetryBlock, now time.Time) []workload.RetryBlock {
	t.Helper()
	if block.NextRetryAt == nil {
		t.Fatalf("Backoff block without NextRetryAt: %+v", block)
	}
	wantAfter := block.NextRetryAt.Time.Sub(now)
	if denied, retryAfter := evaluateRetryBlockGate(&block, now, false); !denied || retryAfter != wantAfter {
		t.Fatalf("gate before NextRetryAt: got (denied=%v, retryAfter=%v) want (true, %v)", denied, retryAfter, wantAfter)
	}
	if denied, _ := evaluateRetryBlockGate(&block, block.NextRetryAt.Time, false); denied {
		t.Fatalf("gate at NextRetryAt must admit the next attempt: %+v", block)
	}
	if markRetryBlockAttemptStarted(&block) != workload.RetryBlockPersist || block.State != workload.RetryBlockRetryInProgress {
		t.Fatalf("attempt stamp must flip Backoff to RetryInProgress: %+v", block)
	}
	return []workload.RetryBlock{block}
}

// TestAbandonFailedGangSurge_WorkloadCausedWavesHold: ImagePullBackOff
// evidence charges the ladder on every abandoned wave — AttemptsStarted
// 1, 2, then Held at MaxAttempts with WarnRetryHeld exactly once — and
// the gate admits each intermediate retry once its Backoff is due.
func TestAbandonFailedGangSurge_WorkloadCausedWavesHold(t *testing.T) {
	t0 := time.Now()
	policy := retryTestPolicy()
	evidence := &workload.InstanceTermination{PodName: "gang-a-engine-2-leader-0", Reason: "ImagePullBackOff"}
	var persisted []workload.RetryBlock
	for wave := int32(1); wave <= policy.MaxAttempts; wave++ {
		calls, warns := gangAbandonWave(t, t0, evidence, persisted)
		if len(*calls) == 0 {
			t.Fatalf("wave %d: no MutateRetryBlock call", wave)
		}
		rec := (*calls)[0]
		if rec.rev != "gang-a-engine-badrev" || rec.disposition != workload.RetryBlockPersist {
			t.Fatalf("wave %d: record call got (rev=%q, disposition=%v) want (gang-a-engine-badrev, Persist)", wave, rec.rev, rec.disposition)
		}
		if rec.block.AttemptsStarted != wave {
			t.Errorf("wave %d: AttemptsStarted got %d want %d (every workload-caused wave counts)", wave, rec.block.AttemptsStarted, wave)
		}
		if wave < policy.MaxAttempts {
			if rec.block.State != workload.RetryBlockBackoff {
				t.Fatalf("wave %d: state got %q want Backoff", wave, rec.block.State)
			}
			if want := t0.Add(policy.NextRetryDelay(wave)); rec.block.NextRetryAt == nil || !rec.block.NextRetryAt.Time.Equal(want) {
				t.Errorf("wave %d: NextRetryAt got %v want %v", wave, rec.block.NextRetryAt, want)
			}
			if len(*warns) != 0 {
				t.Errorf("wave %d: WarnRetryHeld got %d calls want 0", wave, len(*warns))
			}
			persisted = nextGangAbandonWave(t, rec.block, t0)
			continue
		}
		if rec.block.State != workload.RetryBlockHeld || rec.block.NextRetryAt != nil {
			t.Errorf("wave %d: got (state=%q, next=%v) want (Held, nil)", wave, rec.block.State, rec.block.NextRetryAt)
		}
		if len(*warns) != 1 || (*warns)[0].attempts != policy.MaxAttempts {
			t.Errorf("wave %d: WarnRetryHeld got %+v want exactly one call with attempts=%d", wave, *warns, policy.MaxAttempts)
		}
		held := rec.block
		if denied, retryAfter := evaluateRetryBlockGate(&held, t0.Add(policy.MaxDelay), false); !denied || retryAfter != 0 {
			t.Errorf("Held must deny with no time bound: got (denied=%v, retryAfter=%v)", denied, retryAfter)
		}
	}
}

// TestAbandonFailedGangSurge_EnvironmentCausedWavesNeverHold:
// DeadlineExceeded evidence (no workload-caused pod) never charges the
// ladder — however many waves are abandoned, AttemptsStarted stays 0, the
// block never Holds and no Held warning fires — while each wave still
// paces the next attempt with the policy's first-rung delay, after which
// the gate admits it.
func TestAbandonFailedGangSurge_EnvironmentCausedWavesNeverHold(t *testing.T) {
	t0 := time.Now()
	policy := retryTestPolicy()
	evidence := &workload.InstanceTermination{
		Reason:  "DeadlineExceeded",
		Message: "DeadlineExceeded: Update/Surge exceeded InstanceReadyTimeout",
	}
	var persisted []workload.RetryBlock
	for wave := int32(1); wave <= 2*policy.MaxAttempts; wave++ {
		calls, warns := gangAbandonWave(t, t0, evidence, persisted)
		if len(*calls) == 0 {
			t.Fatalf("wave %d: no MutateRetryBlock call", wave)
		}
		rec := (*calls)[0]
		if rec.rev != "gang-a-engine-badrev" || rec.disposition != workload.RetryBlockPersist {
			t.Fatalf("wave %d: record call got (rev=%q, disposition=%v) want (gang-a-engine-badrev, Persist)", wave, rec.rev, rec.disposition)
		}
		if rec.block.State != workload.RetryBlockBackoff || rec.block.AttemptsStarted != 0 {
			t.Fatalf("wave %d: got (state=%q, attempts=%d) want (Backoff, 0) — environment faults never count toward Held", wave, rec.block.State, rec.block.AttemptsStarted)
		}
		if want := t0.Add(policy.InitialDelay); rec.block.NextRetryAt == nil || !rec.block.NextRetryAt.Time.Equal(want) {
			t.Errorf("wave %d: NextRetryAt got %v want %v (first-rung pacing)", wave, rec.block.NextRetryAt, want)
		}
		if rec.block.Reason != evidence.Message {
			t.Errorf("wave %d: Reason got %q want the deadline evidence", wave, rec.block.Reason)
		}
		if len(*warns) != 0 {
			t.Errorf("wave %d: WarnRetryHeld got %d calls want 0", wave, len(*warns))
		}
		persisted = nextGangAbandonWave(t, rec.block, t0)
	}
}

// drainRecorderEvents empties a FakeRecorder's channel.
func drainRecorderEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// TestGangSurge_PromoteEmitsCompletedEvent pins the completion edge's
// event reason: the promote pass must emit RecreateUpdateCompleted so
// reason-filtered monitoring observes gang rollouts finishing.
func TestGangSurge_PromoteEmitsCompletedEvent(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, ns = "gang-b", "test-ns"
	surgeIdx := int32(2)
	mkReadyPod := func(runner string) *corev1.Pod {
		pod := gangSurgePod(isvcName, ns, surgeIdx, runner, "newrev")
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
		}
		return pod
	}
	c := legacyNewFakeClient(t, mkReadyPod("leader"), mkReadyPod("worker")) // no source pods — promote pass

	src := &workload.InstanceStatus{
		Index:           0,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-b-engine-oldrev",
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           updateStepSurge,
			SurgeIndex:     &surgeIdx,
			TargetRevision: "gang-b-engine-newrev",
		},
	}
	var removed []int32
	input := gangAbandonInput(isvcName, ns, src, &removed)
	input.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		*src,
		{
			Index:          surgeIdx,
			Incarnation:    1,
			Phase:          workload.InstancePhaseCreating,
			TargetRevision: "gang-b-engine-newrev",
			Operation: &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepGangSurgeTarget,
				TargetRevision: "gang-b-engine-newrev",
			},
		},
	}
	input.EventTarget = legacyMinimalISVC(isvcName, ns, 1)
	rec := record.NewFakeRecorder(16)
	deps := legacyTestDeps(c)
	deps.Recorder = rec
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "gang-b-engine-newrev"}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true (promote pass)")
	}
	events := drainRecorderEvents(rec)
	completed, started := 0, 0
	for _, e := range events {
		if strings.Contains(e, string(workload.EventReasonRecreateUpdateCompleted)) {
			completed++
		}
		if strings.Contains(e, string(workload.EventReasonRecreateUpdateStarted)) {
			started++
		}
	}
	if completed != 1 || started != 0 {
		t.Errorf("promote events: got completed=%d started=%d want (1, 0); events=%v", completed, started, events)
	}
}

func TestAbandonFailedGangSurge_EventReasons(t *testing.T) {
	run := func(t *testing.T, failedTargetRev, wantPrefix string) []string {
		t.Helper()
		legacyResetExpectations(t)
		const isvcName, ns = "gang-c", "test-ns"
		c := legacyNewFakeClient(t) // surge pods already gone — reset pass
		surgeIdx := int32(2)
		src := &workload.InstanceStatus{
			Index:           0,
			Phase:           workload.InstancePhaseFailed,
			RunningRevision: "gang-c-engine-goodrev",
			Operation: &workload.InstanceOperation{
				Type:       workload.InstanceOperationUpdate,
				Step:       updateStepSurge,
				SurgeIndex: &surgeIdx,
			},
		}
		var removed []int32
		input := gangAbandonInput(isvcName, ns, src, &removed)
		input.EventTarget = legacyMinimalISVC(isvcName, ns, 1)
		recordRetryBlockCalls(&input, nil)
		rec := record.NewFakeRecorder(16)
		deps := legacyTestDeps(c)
		deps.Recorder = rec
		plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

		if _, err := abandonFailedGangSurge(context.Background(), deps, input, plan, 0, surgeIdx,
			src.RunningRevision, failedTargetRev, "pod stuck", true); err != nil {
			t.Fatalf("abandonFailedGangSurge: %v", err)
		}
		events := drainRecorderEvents(rec)
		matched := 0
		for _, e := range events {
			if strings.Contains(e, string(workload.EventReasonRecreateUpdateStarted)) {
				t.Errorf("abandon must not emit the Started reason; events=%v", events)
			}
			if strings.HasPrefix(e, wantPrefix+" "+string(eventReasonGangSurgeAbandoned)) {
				matched++
			}
		}
		if matched != 1 {
			t.Errorf("abandon events: got %d %q GangSurgeAbandoned, want 1; events=%v", matched, wantPrefix, events)
		}
		return events
	}

	t.Run("failed abandon warns", func(t *testing.T) {
		run(t, "gang-c-engine-badrev", corev1.EventTypeWarning)
	})
	t.Run("supersede abandon is normal", func(t *testing.T) {
		run(t, "", corev1.EventTypeNormal)
	})
}

// TestGangSurge_InheritsExcludedNodes pins the exclusion-memory contract
// through the REAL surge-synthesis path: gangSurgeUpdate's replacement-gang
// creation pass must render the surge pods with the source instance's
// ExcludedNodes as required hostname NotIn terms — the exclusion overlay
// follows the instance through the surge-replace cycle.
func TestGangSurge_InheritsExcludedNodes(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, c, isvc, legacyTargetSpecImage("llama:v2"))

	// Mid-surge shape: the surge was stamped on a prior pass (SurgeIndex
	// allocated, target pinned) but the replacement gang's pods don't
	// exist yet — the next gangSurgeUpdate pass creates them.
	surgeIdx := int32(2)
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	ir.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		ID:             "update-0-1",
		Type:           v1beta1.InstanceOperationType(workload.InstanceOperationUpdate),
		Step:           updateStepSurge,
		SurgeIndex:     &surgeIdx,
		TargetRevision: tcr.Name,
	}
	ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
		Index:          surgeIdx,
		Incarnation:    1,
		Phase:          v1beta1.OMENativeInstanceCreating,
		TargetRevision: tcr.Name,
		Operation: &v1beta1.InstanceOperation{
			Type:           v1beta1.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTarget,
			TargetRevision: tcr.Name,
		},
	})
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	plan.Instances[0].ExcludedNodes = []string{"node-bad-1", "node-bad-2"}

	done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if done {
		t.Errorf("done: got true want false (create pass requeues)")
	}

	surgePods, err := query.LiveListPodsForInstance(context.Background(), c, isvc.Namespace, isvc.Name, workload.ComponentEngine, surgeIdx)
	if err != nil {
		t.Fatalf("list surge pods: %v", err)
	}
	if len(surgePods) != 2 {
		t.Fatalf("surge gang pods: got %d want 2 (leader + worker)", len(surgePods))
	}
	for _, pod := range surgePods {
		got := hostnameNotInValues(pod)
		want := map[string]bool{"node-bad-1": false, "node-bad-2": false}
		for _, v := range got {
			if _, ok := want[v]; ok {
				want[v] = true
			}
		}
		for node, seen := range want {
			if !seen {
				t.Errorf("surge pod %s: excluded node %s missing from required NotIn terms (inheritance lost): got %v", pod.Name, node, got)
			}
		}
	}
}

// TestMigrateSurge_InheritsExcludedNodes pins the same contract through
// Migrate's surge synthesis: the in-flight migration's surge pod must
// render with the source instance plan's ExcludedNodes (plus the
// overlay's FromNode) as required hostname NotIn terms.
func TestMigrateSurge_InheritsExcludedNodes(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))

	// In-flight migration shape: source already stamped with the Migrate
	// Operation + SurgeIndex; the next Migrate pass creates the surge pod.
	surgeIdx := int32(2)
	irKey := types.NamespacedName{Namespace: isvc.Namespace, Name: legacyIRName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), irKey, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceMigrating
	ir.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		ID:         "migrate-0-1",
		Type:       v1beta1.InstanceOperationType(workload.InstanceOperationMigrate),
		SurgeIndex: &surgeIdx,
	}
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	// In-flight record: the executor resumes from status.migrations
	// (SurgeInstance allocated on a prior pass).
	input.ObservedState.Migrations = []workload.MigrationRecord{{
		RequestUUID: "uuid-mig-1", Trigger: workload.MigrationTriggerManual,
		Phase: workload.MigrationPhaseSurgePending, SourceInstance: 0,
		SurgeInstance: &surgeIdx, FromNode: "node-from",
	}}
	input.MutateMigration = func(_ context.Context, uuid string, mutate func(*workload.MigrationRecord) bool) error {
		mutate(&input.ObservedState.Migrations[0])
		return nil
	}
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	plan.Instances[0].ExcludedNodes = []string{"node-bad-1", "node-bad-2"}
	req := &audit.MigrationRequest{
		Component: string(workload.ComponentEngine),
		Instance:  0,
		FromNode:  "node-from",
	}

	done, accepted, err := Migrate(context.Background(), legacyTestDeps(c), input, plan, 0, "uuid-mig-1", req)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if done || !accepted {
		t.Errorf("(done, accepted): got (%v, %v) want (false, true) — surge create pass requeues", done, accepted)
	}

	surgePods, err := query.LiveListPodsForInstance(context.Background(), c, isvc.Namespace, isvc.Name, workload.ComponentEngine, surgeIdx)
	if err != nil {
		t.Fatalf("list surge pods: %v", err)
	}
	if len(surgePods) != 1 {
		t.Fatalf("surge pods: got %d want 1", len(surgePods))
	}
	got := hostnameNotInValues(surgePods[0])
	want := map[string]bool{"node-bad-1": false, "node-bad-2": false, "node-from": false}
	for _, v := range got {
		if _, ok := want[v]; ok {
			want[v] = true
		}
	}
	for node, seen := range want {
		if !seen {
			t.Errorf("surge pod: node %s missing from required NotIn terms (inheritance/overlay lost): got %v", node, got)
		}
	}
}
