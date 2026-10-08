package ops

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestEmptyGangSurgeTargetSlotIgnoresPodDerivedObservations(t *testing.T) {
	if !status.EmptyGangSurgeTargetSlot(&workload.InstanceStatus{Index: 7}) {
		t.Fatal("zero-valued row should be an empty surge target slot")
	}
	if status.EmptyGangSurgeTargetSlot(nil) {
		t.Fatal("nil row should not be an empty surge target slot")
	}

	for _, test := range []struct {
		name   string
		mutate func(*workload.InstanceStatus)
	}{
		{name: "ready pods", mutate: func(row *workload.InstanceStatus) { row.ReadyPodCount = 1 }},
		{name: "scheduled pods", mutate: func(row *workload.InstanceStatus) { row.ScheduledPodCount = 1 }},
		{name: "occupied nodes", mutate: func(row *workload.InstanceStatus) { row.NodesOccupied = []string{"node-a"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			row := &workload.InstanceStatus{Index: 7}
			test.mutate(row)
			if !status.EmptyGangSurgeTargetSlot(row) {
				t.Fatalf("pod-derived %s should not own an empty surge target slot", test.name)
			}
		})
	}

	for _, test := range []struct {
		name   string
		mutate func(*workload.InstanceStatus)
	}{
		{name: "incarnation", mutate: func(row *workload.InstanceStatus) { row.Incarnation = 1 }},
		{name: "phase", mutate: func(row *workload.InstanceStatus) { row.Phase = workload.InstancePhaseReady }},
		{name: "running revision", mutate: func(row *workload.InstanceStatus) { row.RunningRevision = "revision-a" }},
		{name: "target revision", mutate: func(row *workload.InstanceStatus) { row.TargetRevision = "revision-b" }},
		{name: "operation", mutate: func(row *workload.InstanceStatus) { row.Operation = &workload.InstanceOperation{} }},
		{name: "pod count", mutate: func(row *workload.InstanceStatus) { row.PodCount = 1 }},
		{name: "serving pods", mutate: func(row *workload.InstanceStatus) { row.ServingPodCount = 1 }},
		{name: "available pods", mutate: func(row *workload.InstanceStatus) { row.AvailablePodCount = 1 }},
		{name: "admission", mutate: func(row *workload.InstanceStatus) { row.Admitted = true }},
		{name: "conditions", mutate: func(row *workload.InstanceStatus) { row.Conditions = []metav1.Condition{{Type: "Ready"}} }},
		{name: "last failure", mutate: func(row *workload.InstanceStatus) { row.LastFailure = &workload.InstanceTermination{} }},
	} {
		t.Run("rejects "+test.name, func(t *testing.T) {
			row := &workload.InstanceStatus{Index: 7}
			test.mutate(row)
			if status.EmptyGangSurgeTargetSlot(row) {
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

	done, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, 2, src.RunningRevision, "gang-a-engine-badrev", "pod stuck", workload.CauseWorkload, gangSurgeEnd{})
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
			Step:       workload.UpdateStepSurge,
			SurgeIndex: &surgeIdx,
		},
	}
	var removed []int32
	input := gangAbandonInput(isvc, ns, src, &removed)
	blockCalls := recordRetryBlockCalls(&input, nil)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	done, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, surgeIdx, src.RunningRevision, src.TargetRevision, "pod stuck", workload.CauseWorkload, gangSurgeEnd{})
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

// A superseded gang surge whose source never ran a revision ends in the
// fresh-start Failed shape: the operation and target cleared, LastFailure
// kept, no running revision. Ready would record capacity nothing serves,
// since the source's own pods were never promoted. Both abandon tails
// write the same shape.
func TestAbandonFailedGangSurge_NeverPromotedSourceEndsFailed(t *testing.T) {
	const isvcName, namespace = "gang-never-promoted", "test-ns"
	const targetRevision = "gang-never-promoted-engine-newrev"
	surgeIndex := int32(2)
	failure := &workload.InstanceTermination{Reason: workload.PodGroupOwnershipConflictReason, Message: "PodGroup held by another owner"}
	wantFreshStart := func(t *testing.T, got workload.InstanceStatus) {
		t.Helper()
		if got.Phase != workload.InstancePhaseFailed || got.Operation != nil || got.RunningRevision != "" || got.TargetRevision != "" {
			t.Errorf("source = %+v, want Failed with no operation, target or running revision", got)
		}
		if got.LastFailure == nil || got.LastFailure.Reason != failure.Reason {
			t.Errorf("LastFailure = %+v, want the recorded failure kept", got.LastFailure)
		}
	}

	t.Run("standalone writer", func(t *testing.T) {
		legacyResetExpectations(t)
		c := legacyNewFakeClient(t) // the replacement's members are already gone
		src := &workload.InstanceStatus{
			Index: 0, Phase: workload.InstancePhaseUpdating, TargetRevision: targetRevision,
			Operation: &workload.InstanceOperation{
				Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepSurge,
				TargetRevision: targetRevision, SurgeIndex: &surgeIndex,
			},
			LastFailure: failure,
		}
		var removed []int32
		input := gangAbandonInput(isvcName, namespace, src, &removed)
		blockCalls := recordRetryBlockCalls(&input, nil)
		plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
		if _, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, surgeIndex,
			src.RunningRevision, "", "", workload.CauseUnattributed, gangSurgeEnd{}); err != nil {
			t.Fatalf("abandonFailedGangSurge: %v", err)
		}
		if len(removed) != 1 || removed[0] != surgeIndex {
			t.Errorf("RemoveInstance calls: got %v want [%d]", removed, surgeIndex)
		}
		wantFreshStart(t, *src)
		if len(*blockCalls) != 0 {
			t.Errorf("MutateRetryBlock calls: got %d want none (no revision to prune, no failure to record)", len(*blockCalls))
		}
	})

	t.Run("atomic tail", func(t *testing.T) {
		legacyResetExpectations(t)
		source := workload.InstanceStatus{
			Index: 0, Incarnation: 1, Phase: workload.InstancePhaseUpdating, TargetRevision: targetRevision,
			Operation: &workload.InstanceOperation{
				ID: "gang-update-0", Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepSurge,
				TargetRevision: targetRevision, SurgeIndex: &surgeIndex,
			},
			LastFailure: failure,
		}
		marker := workload.InstanceStatus{
			Index: surgeIndex, Incarnation: 1, Phase: workload.InstancePhaseCreating, TargetRevision: targetRevision,
			Operation: &workload.InstanceOperation{
				ID: "gang-update-target-2", Type: workload.InstanceOperationUpdate,
				Step: workload.UpdateStepGangSurgeTargetCleanup, TargetRevision: targetRevision,
			},
		}
		store := &terminalMutationStore{
			ownerUID: "owner-a",
			statuses: map[int32]workload.InstanceStatus{
				source.Index: cloneTerminalStatus(source),
				marker.Index: cloneTerminalStatus(marker),
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
			ApplyInstanceMutationsWithRetryBlock: store.apply,
			FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
		}
		plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
		if _, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(legacyNewFakeClient(t)), input, plan,
			source.Index, surgeIndex, source.RunningRevision, "", "", workload.CauseUnattributed, gangSurgeEnd{}); err != nil {
			t.Fatalf("abandonFailedGangSurge: %v", err)
		}
		if _, found := store.statuses[surgeIndex]; found {
			t.Error("the cleanup marker must be removed with the reset")
		}
		wantFreshStart(t, store.statuses[source.Index])
	})
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
			Step:           workload.UpdateStepSurge,
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
		src.RunningRevision, src.Operation.TargetRevision, instanceFailureReason(src, "gang surge abandoned"), instanceFailureCause(src), gangSurgeEnd{})
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
	return gangAbandonWaveUnderPolicy(t, t0, failure, persisted, retryTestPolicy())
}

// gangAbandonWaveUnderPolicy is gangAbandonWave under an explicit retry
// policy.
func gangAbandonWaveUnderPolicy(t *testing.T, t0 time.Time, failure *workload.InstanceTermination, persisted []workload.RetryBlock, policy *workload.RetryPolicy) (*[]retryBlockCall, *[]retryHeldWarning) {
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
			Step:           workload.UpdateStepSurge,
			SurgeIndex:     &surgeIdx,
			TargetRevision: "gang-a-engine-badrev",
		},
		LastFailure: failure,
	}
	var removed []int32
	input := gangAbandonInput(isvc, ns, src, &removed)
	input.Clock = clocktesting.NewFakeClock(t0)
	input.UpdateRetryPolicy = policy
	calls := recordRetryBlockCalls(&input, persisted)
	warns := &[]retryHeldWarning{}
	input.WarnRetryHeld = func(rev string, attempts int32, reason string) {
		*warns = append(*warns, retryHeldWarning{rev: rev, attempts: attempts, reason: reason})
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	if _, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, surgeIdx,
		src.RunningRevision, src.Operation.TargetRevision,
		instanceFailureReason(src, "gang surge abandoned"), instanceFailureCause(src), gangSurgeEnd{}); err != nil {
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
	if status.RetryBlockStartAttempt(&block) != workload.RetryBlockPersist || block.State != workload.RetryBlockRetryInProgress {
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

// TestAbandonFailedGangSurge_WavesHoldAtMaxAttempts: a wave the
// revision is not blamed for — DeadlineExceeded evidence with no
// workload-caused pod, or a crash-looping replacement gang — counts on
// the one ladder exactly as a workload-caused wave does: AttemptsStarted
// advances on every abandon, each wave paces the next attempt at its
// rung and the gate admits it when due, and the wave that reaches
// MaxAttempts Holds with WarnRetryHeld exactly once. The block records
// the evidence the abandon carried.
func TestAbandonFailedGangSurge_WavesHoldAtMaxAttempts(t *testing.T) {
	cases := []struct {
		name     string
		evidence *workload.InstanceTermination
	}{
		{"deadline", &workload.InstanceTermination{
			Reason:  "DeadlineExceeded",
			Message: "DeadlineExceeded: Update/Surge exceeded InstanceReadyTimeout",
		}},
		{"crash loop", &workload.InstanceTermination{PodName: "gang-a-engine-2-leader-0", Reason: "CrashLoopBackOff"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t0 := time.Now()
			policy := retryTestPolicy()
			wantReason := instanceFailureReason(&workload.InstanceStatus{LastFailure: tc.evidence}, "")
			var persisted []workload.RetryBlock
			for wave := int32(1); wave <= policy.MaxAttempts; wave++ {
				calls, warns := gangAbandonWave(t, t0, tc.evidence, persisted)
				if len(*calls) == 0 {
					t.Fatalf("wave %d: no MutateRetryBlock call", wave)
				}
				rec := (*calls)[0]
				if rec.rev != "gang-a-engine-badrev" || rec.disposition != workload.RetryBlockPersist {
					t.Fatalf("wave %d: record call got (rev=%q, disposition=%v) want (gang-a-engine-badrev, Persist)", wave, rec.rev, rec.disposition)
				}
				if rec.block.AttemptsStarted != wave {
					t.Errorf("wave %d: AttemptsStarted got %d want %d (every failed attempt counts)", wave, rec.block.AttemptsStarted, wave)
				}
				if rec.block.Reason != wantReason {
					t.Errorf("wave %d: Reason got %q want %q", wave, rec.block.Reason, wantReason)
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
			}
		})
	}
}

// TestAbandonFailedGangSurge_SchedulerHoldRecordsNoBlock: a replacement
// gang the scheduler never placed is the environment's failure, not an
// attempt at the revision. However many such waves are abandoned, no
// block is persisted, nothing counts and no Held warning fires; the
// source is still reset for a fresh surge.
func TestAbandonFailedGangSurge_SchedulerHoldRecordsNoBlock(t *testing.T) {
	t0 := time.Now()
	policy := retryTestPolicy()
	evidence := &workload.InstanceTermination{
		PodName: "gang-a-engine-2-leader-0",
		Reason:  workload.WaitingReasonUnschedulable,
		Message: workload.WaitingReasonUnschedulable + ": 0/3 nodes are available",
	}
	for wave := int32(1); wave <= policy.MaxAttempts+1; wave++ {
		calls, warns := gangAbandonWave(t, t0, evidence, nil)
		for _, rec := range *calls {
			// The reset prunes the source revision's own block; only the
			// failed target's block is the hold's to leave alone.
			if rec.rev == "gang-a-engine-badrev" && rec.disposition != workload.RetryBlockUnchanged {
				t.Fatalf("wave %d: a scheduler hold persisted a block: %+v", wave, rec)
			}
		}
		if len(*warns) != 0 {
			t.Fatalf("wave %d: WarnRetryHeld got %+v want none", wave, *warns)
		}
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
			Step:           workload.UpdateStepSurge,
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
				Step:       workload.UpdateStepSurge,
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
			src.RunningRevision, failedTargetRev, "pod stuck", workload.CauseWorkload, gangSurgeEnd{}); err != nil {
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
		Step:           workload.UpdateStepSurge,
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
	plan.Instances[0].ExcludedNodes = []workload.NodeExclusion{
		{Node: "node-bad-1", Revision: tcr.Name},
		{Node: "node-bad-2", Revision: tcr.Name},
	}

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
	// A migration surge renders the source's running revision, which is
	// the revision the exclusions were recorded for.
	runningRevision := ir.Status.InstanceStatuses[0].RunningRevision
	plan.Instances[0].ExcludedNodes = []workload.NodeExclusion{
		{Node: "node-bad-1", Revision: runningRevision},
		{Node: "node-bad-2", Revision: runningRevision},
	}
	req := &audit.MigrationRequest{
		Component: string(workload.ComponentEngine),
		Instance:  0,
		FromNode:  "node-from",
	}

	done, accepted, err := Migrate(context.Background(), legacyTestDeps(c), input, plan, nil, 0, "uuid-mig-1", req)
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

func TestGangSurgeRemovalOwnershipPredicates(t *testing.T) {
	surge := int32(9)
	gangSource := terminalStatusFixture(3)
	gangSource.Operation.Step = workload.UpdateStepSurgeDrain
	if !gangSurgeSourceOwnsRemoval(&gangSource, surge) || gangSurgeSourceOwnsRemoval(&gangSource, surge+1) {
		t.Fatal("gang source ownership did not bind the replacement index")
	}

	gangTarget := terminalStatusFixture(surge)
	gangTarget.Operation.Step = workload.UpdateStepGangSurgeTarget
	if !gangSurgeTargetOwnsRemoval(&gangTarget, false) || gangSurgeTargetOwnsRemoval(&gangTarget, true) {
		t.Fatal("gang target ownership accepted the wrong cleanup phase")
	}
	gangTarget.Operation.Step = workload.UpdateStepGangSurgeTargetCleanup
	if !gangSurgeTargetOwnsRemoval(&gangTarget, true) || gangSurgeTargetOwnsRemoval(&gangTarget, false) {
		t.Fatal("gang target cleanup ownership accepted the wrong deployment mode")
	}
}

func TestGangSurge_RestoresMissingTargetMarkerBeforeEffects(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-resume", "test-ns"
	surgeIndex := int32(2)
	revision := "gang-resume-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-resume-engine-oldrev",
		TargetRevision:  revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: revision,
			SurgeIndex:     &surgeIndex,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{source.Index: cloneTerminalStatus(source)},
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
			InstanceStatuses: []workload.InstanceStatus{cloneTerminalStatus(source)},
		},
		MutateInstance: func(context.Context, int32, func(*workload.InstanceStatus) bool) error {
			t.Fatal("native recovery bypassed the atomic status adapter")
			return nil
		},
		FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	c := legacyNewFakeClient(t)
	ensureCalls := 0
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = func(context.Context, workload.ReconcileInput, workload.ComponentPlan, workload.InstancePlan) (string, error) {
		ensureCalls++
		return "", nil
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil || done {
		t.Fatalf("resume pass: done=%v err=%v", done, err)
	}
	marker, found := store.statuses[surgeIndex]
	if !found || !gangSurgeTargetMarkerAt(&marker, revision) {
		t.Fatalf("restored target marker: %+v", marker)
	}
	if store.writes != 1 {
		t.Fatalf("target marker status writes=%d want 1", store.writes)
	}
	if ensureCalls != 0 {
		t.Fatalf("PodGroup ensure calls=%d want 0 before marker round-trip", ensureCalls)
	}
	pods, err := query.LiveListPodsForInstance(context.Background(), c, namespace, isvcName, workload.ComponentEngine, surgeIndex)
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 0 {
		t.Fatalf("surge effects ran before marker round-trip: %d pods", len(pods))
	}
}

func TestGangSurge_CachedTargetMissingAuthoritativelyRestoresBeforeEffects(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-stale-target", "test-ns"
	surgeIndex := int32(2)
	revision := "gang-stale-target-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-stale-target-engine-oldrev",
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
		statuses: map[int32]workload.InstanceStatus{source.Index: cloneTerminalStatus(source)},
	}
	finalizes := 0
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
		MutateInstance: func(context.Context, int32, func(*workload.InstanceStatus) bool) error {
			t.Fatal("target confirmation bypassed the atomic status adapter")
			return nil
		},
		FinalizeInstanceResources: func(context.Context, int32) (bool, error) {
			finalizes++
			return true, nil
		},
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	c := legacyNewFakeClient(t)
	ensureCalls := 0
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = func(context.Context, workload.ReconcileInput, workload.ComponentPlan, workload.InstancePlan) (string, error) {
		ensureCalls++
		return "", nil
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil || done {
		t.Fatalf("confirmation pass: done=%v err=%v", done, err)
	}
	persisted, found := store.statuses[surgeIndex]
	if !found || !gangSurgeTargetMarkerAt(&persisted, revision) {
		t.Fatalf("authoritative target marker was not restored: %+v", persisted)
	}
	if store.writes != 1 || finalizes != 0 || ensureCalls != 0 {
		t.Fatalf("writes=%d finalizes=%d PodGroup ensures=%d want 1,0,0", store.writes, finalizes, ensureCalls)
	}
	pods, err := query.LiveListPodsForInstance(context.Background(), c, namespace, isvcName, workload.ComponentEngine, surgeIndex)
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 0 {
		t.Fatalf("surge effects ran before restored marker round-trip: %d pods", len(pods))
	}
}

func TestGangSurge_RestartAfterTargetStampFailureRestoresCleanupBeforeRedirects(t *testing.T) {
	tests := []struct {
		name            string
		prepareSource   func(*workload.InstanceStatus)
		latestRevision  string
		wantSourcePhase workload.InstancePhase
	}{
		{
			name: "failed source",
			prepareSource: func(row *workload.InstanceStatus) {
				row.Phase = workload.InstancePhaseFailed
			},
			latestRevision:  "gang-restart-engine-badrev",
			wantSourcePhase: workload.InstancePhaseFailed,
		},
		{
			name:            "superseded target",
			prepareSource:   func(*workload.InstanceStatus) {},
			latestRevision:  "gang-restart-engine-newerrev",
			wantSourcePhase: workload.InstancePhaseUpdating,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			legacyResetExpectations(t)
			const isvcName, namespace = "gang-restart", "test-ns"
			const committedRevision = "gang-restart-engine-badrev"
			source := workload.InstanceStatus{
				Index:           0,
				Incarnation:     3,
				Phase:           workload.InstancePhaseReady,
				RunningRevision: "gang-restart-engine-oldrev",
			}
			store := &terminalMutationStore{
				ownerUID: "owner-a",
				statuses: map[int32]workload.InstanceStatus{source.Index: cloneTerminalStatus(source)},
			}
			finalizes := 0
			failTargetStamp := true
			targetStampErr := errors.New("injected target marker failure")
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
					InstanceStatuses: []workload.InstanceStatus{cloneTerminalStatus(source)},
				},
				MutateInstance: func(_ context.Context, index int32, mutate func(*workload.InstanceStatus) bool) error {
					if index != source.Index && failTargetStamp {
						failTargetStamp = false
						return targetStampErr
					}
					row, found := store.statuses[index]
					if !found {
						row = workload.InstanceStatus{Index: index}
					}
					if mutate(&row) {
						store.statuses[index] = row
					}
					return nil
				},
				FinalizeInstanceResources: func(context.Context, int32) (bool, error) {
					finalizes++
					return true, nil
				},
			}
			plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
			c := legacyNewFakeClient(t)
			deps := legacyTestDeps(c)
			committedTarget := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: committedRevision}}

			done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], committedTarget)
			if !errors.Is(err, targetStampErr) || done {
				t.Fatalf("source-stamp pass: done=%v err=%v", done, err)
			}
			persistedSource := store.statuses[source.Index]
			if persistedSource.Operation == nil || persistedSource.Operation.Step != workload.UpdateStepSurge || persistedSource.Operation.SurgeIndex == nil {
				t.Fatalf("source surge claim was not persisted: %+v", persistedSource)
			}
			surgeIndex := *persistedSource.Operation.SurgeIndex
			if _, found := store.statuses[surgeIndex]; found {
				t.Fatal("target marker unexpectedly persisted during the injected failure")
			}

			test.prepareSource(&persistedSource)
			store.statuses[source.Index] = persistedSource
			input.ObservedState.InstanceStatuses = []workload.InstanceStatus{cloneTerminalStatus(persistedSource)}
			input.ApplyInstanceMutationsWithRetryBlock = store.apply
			latestTarget := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: test.latestRevision}}
			done, err = gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], latestTarget)
			if err != nil || done {
				t.Fatalf("restart pass: done=%v err=%v", done, err)
			}
			marker, found := store.statuses[surgeIndex]
			if !found || !status.GangSurgeCleanupTargetClaimMatches(&marker, committedRevision) {
				t.Fatalf("restart exposed a non-terminal target marker: %+v", marker)
			}
			persistedSource = store.statuses[source.Index]
			if persistedSource.Phase != test.wantSourcePhase || persistedSource.Operation == nil || persistedSource.Operation.Step != workload.UpdateStepSurge {
				t.Fatalf("redirect ran before marker restoration: %+v", persistedSource)
			}
			if finalizes != 0 {
				t.Fatalf("terminal finalization ran before marker restoration: %d calls", finalizes)
			}
			if store.writes != 1 {
				t.Fatalf("atomic marker status writes=%d want 1", store.writes)
			}
		})
	}
}

func gangSurgeRecoverySource(surgeIndex int32, targetRevision string) workload.InstanceStatus {
	return workload.InstanceStatus{
		Index:           0,
		Incarnation:     4,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "recovery-engine-oldrev",
		TargetRevision:  targetRevision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: targetRevision,
			SurgeIndex:     &surgeIndex,
		},
	}
}

func gangSurgePromotedTarget(index int32, targetRevision string) workload.InstanceStatus {
	return workload.InstanceStatus{
		Index:           index,
		Incarnation:     1,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: targetRevision,
	}
}

func gangSurgeRecoveryInput(
	ownerUID types.UID,
	isvcName string,
	namespace string,
	store *terminalMutationStore,
	observed ...workload.InstanceStatus,
) workload.ReconcileInput {
	return workload.ReconcileInput{
		OwnerObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: isvcName, Namespace: namespace, UID: ownerUID,
		}},
		OwnerGVK: corev1.SchemeGroupVersion.WithKind("ConfigMap"),
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
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec:       &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example/leader:latest"}}},
			WorkerPodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example/worker:latest"}}},
		},
		ObservedState: workload.WorkloadObservedState{InstanceStatuses: observed},
		MutateInstance: func(_ context.Context, index int32, mutate func(*workload.InstanceStatus) bool) error {
			row, found := store.statuses[index]
			if !found {
				row = workload.InstanceStatus{Index: index}
			}
			if mutate(&row) {
				store.statuses[index] = cloneTerminalStatus(row)
			}
			return nil
		},
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
}

func gangSurgeReadyRecoveryPod(isvcName, namespace string, index int32, runner, revisionHash string) *corev1.Pod {
	pod := gangSurgePod(isvcName, namespace, index, runner, revisionHash)
	now := metav1.Now()
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: now},
	}
	return pod
}

func TestGangSurge_ResumesPromotedTargetWithPodlessSource(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-promoted-resume", "test-ns"
	const targetRevision = "gang-promoted-resume-engine-newrev"
	const targetHash = "newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	promoted := gangSurgePromotedTarget(surgeIndex, targetRevision)
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index:   cloneTerminalStatus(source),
			promoted.Index: cloneTerminalStatus(promoted),
		},
	}
	input := gangSurgeRecoveryInput("owner-a", isvcName, namespace, store,
		cloneTerminalStatus(source), cloneTerminalStatus(promoted))
	finalized := 0
	input.FinalizeInstanceResources = func(_ context.Context, index int32) (bool, error) {
		if index != source.Index {
			t.Fatalf("finalized index=%d want source %d", index, source.Index)
		}
		finalized++
		return true, nil
	}
	c := legacyNewFakeClient(t,
		gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "leader", targetHash),
		gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "worker", targetHash),
	)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetRevision}}

	done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
	if err != nil || !done {
		t.Fatalf("promoted-target resume: done=%v err=%v", done, err)
	}
	if _, found := store.statuses[source.Index]; found {
		t.Fatal("promoted-target resume retained source status")
	}
	persistedTarget, found := store.statuses[surgeIndex]
	if !found || !gangSurgePromotedTargetMatches(&persistedTarget, targetRevision) {
		t.Fatalf("promoted target changed: %+v", persistedTarget)
	}
	if finalized != 1 {
		t.Fatalf("source finalization calls=%d want 1", finalized)
	}
}

func TestGangSurge_PromotedTargetWaitsForResourceAbsence(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-promoted-finalize", "test-ns"
	const targetRevision = "gang-promoted-finalize-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	source.Operation.Step = workload.UpdateStepSurgeDrain
	promoted := gangSurgePromotedTarget(surgeIndex, targetRevision)
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index:   cloneTerminalStatus(source),
			promoted.Index: cloneTerminalStatus(promoted),
		},
	}
	input := gangSurgeRecoveryInput("owner-a", isvcName, namespace, store,
		cloneTerminalStatus(source), cloneTerminalStatus(promoted))
	resourcesAbsent := false
	finalizations := 0
	input.FinalizeInstanceResources = func(_ context.Context, index int32) (bool, error) {
		if index != source.Index {
			t.Fatalf("finalized index=%d want source %d", index, source.Index)
		}
		finalizations++
		return resourcesAbsent, nil
	}
	c := legacyNewFakeClient(t,
		gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "leader", "newrev"),
		gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "worker", "newrev"),
	)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetRevision}}

	done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
	if err != nil || done {
		t.Fatalf("delete accepted pass: done=%v err=%v", done, err)
	}
	if _, found := store.statuses[source.Index]; !found {
		t.Fatal("source marker was removed before resource absence")
	}

	resourcesAbsent = true
	input.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		cloneTerminalStatus(store.statuses[source.Index]),
		cloneTerminalStatus(store.statuses[surgeIndex]),
	}
	done, err = gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
	if err != nil || !done {
		t.Fatalf("absence pass: done=%v err=%v", done, err)
	}
	if _, found := store.statuses[source.Index]; found {
		t.Fatal("source marker remained after resource absence")
	}
	if finalizations != 2 {
		t.Fatalf("finalizations=%d want 2", finalizations)
	}
}

func TestGangSurge_PromotedPinnedTargetRecreatesMissingPodsBeforeNewerRevision(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-promoted-pinned", "test-ns"
	const committedRevision = "gang-promoted-pinned-engine-rev-v1hash"
	const latestRevision = "gang-promoted-pinned-engine-rev-v2hash"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, committedRevision)
	promoted := gangSurgePromotedTarget(surgeIndex, committedRevision)
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index:   cloneTerminalStatus(source),
			promoted.Index: cloneTerminalStatus(promoted),
		},
	}
	input := gangSurgeRecoveryInput("owner-a", isvcName, namespace, store,
		cloneTerminalStatus(source), cloneTerminalStatus(promoted))
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
		t.Fatal("incomplete promoted target reached resource finalization")
		return false, nil
	}
	c := legacyNewFakeClient(t,
		gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "leader", "v1hash"),
		legacyStoredRevision(t, namespace, committedRevision, input.DesiredSpec.PodSpec, input.DesiredSpec.WorkerPodSpec),
	)
	ensureCalls := 0
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = func(context.Context, workload.ReconcileInput, workload.ComponentPlan, workload.InstancePlan) (string, error) {
		ensureCalls++
		return "", nil
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	latest := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: latestRevision}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], latest)
	if err != nil || done {
		t.Fatalf("pinned promoted-target recovery: done=%v err=%v", done, err)
	}
	pods, err := query.LiveListPodsForInstance(context.Background(), c, namespace, isvcName, workload.ComponentEngine, surgeIndex)
	if err != nil || len(pods) != 2 {
		t.Fatalf("pinned replacement pods: count=%d err=%v", len(pods), err)
	}
	worker, found := query.IndexPodsByName(pods)[query.PodName(isvcName, workload.ComponentEngine, surgeIndex, "worker", 0)]
	if !found || worker.Labels[query.LabelRevisionHash] != "v1hash" {
		t.Fatalf("missing worker was not recreated on pinned revision: %+v", worker)
	}
	if ensureCalls != 1 {
		t.Fatalf("PodGroup ensure calls=%d want 1", ensureCalls)
	}
	persistedSource := store.statuses[source.Index]
	if persistedSource.Operation == nil || persistedSource.Operation.TargetRevision != committedRevision {
		t.Fatalf("source abandoned pinned completion: %+v", persistedSource)
	}
	persistedTarget := store.statuses[surgeIndex]
	if !gangSurgePromotedTargetMatches(&persistedTarget, committedRevision) {
		t.Fatalf("promoted target changed: %+v", persistedTarget)
	}
}

func TestGangSurge_PromotedTargetWithLiveSourceRollsBackWithoutEffects(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-promoted-conflict", "test-ns"
	const targetRevision = "gang-promoted-conflict-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	promoted := gangSurgePromotedTarget(surgeIndex, targetRevision)
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index:   cloneTerminalStatus(source),
			promoted.Index: cloneTerminalStatus(promoted),
		},
	}
	input := gangSurgeRecoveryInput("owner-a", isvcName, namespace, store,
		cloneTerminalStatus(source), cloneTerminalStatus(promoted))
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
		t.Fatal("conflicting promoted target reached resource finalization")
		return false, nil
	}
	objects := []client.Object{
		gangSurgeReadyRecoveryPod(isvcName, namespace, source.Index, "leader", "oldrev"),
		gangSurgeReadyRecoveryPod(isvcName, namespace, source.Index, "worker", "oldrev"),
		gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "leader", "newrev"),
		gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "worker", "newrev"),
	}
	c := legacyNewFakeClient(t, objects...)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetRevision}}

	done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
	if err != nil || done {
		t.Fatalf("promoted-target conflict: done=%v err=%v", done, err)
	}
	persistedSource := store.statuses[source.Index]
	if persistedSource.Phase != workload.InstancePhaseReady || persistedSource.Operation != nil ||
		persistedSource.RunningRevision != source.RunningRevision {
		t.Fatalf("source claim was not rolled back: %+v", persistedSource)
	}
	persistedTarget := store.statuses[surgeIndex]
	if !gangSurgePromotedTargetMatches(&persistedTarget, targetRevision) {
		t.Fatalf("promoted target changed: %+v", persistedTarget)
	}
	for _, index := range []int32{source.Index, surgeIndex} {
		pods, listErr := query.LiveListPodsForInstance(context.Background(), c, namespace, isvcName, workload.ComponentEngine, index)
		if listErr != nil || len(pods) != 2 {
			t.Fatalf("instance %d pods after rollback: count=%d err=%v", index, len(pods), listErr)
		}
	}
}

func TestGangSurge_OccupiedTargetRollsBackAndReallocates(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-occupied-recover", "test-ns"
	const targetRevision = "gang-occupied-recover-engine-newrev"
	surgeIndex := int32(2)
	source := gangSurgeRecoverySource(surgeIndex, targetRevision)
	occupied := workload.InstanceStatus{
		Index:           surgeIndex,
		Incarnation:     8,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: "unrelated-revision",
		ActiveOrdinal:   1,
	}
	occupiedIdentity := status.Capture(&occupied)
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index:   cloneTerminalStatus(source),
			occupied.Index: cloneTerminalStatus(occupied),
		},
	}
	input := gangSurgeRecoveryInput("owner-a", isvcName, namespace, store,
		cloneTerminalStatus(source), cloneTerminalStatus(occupied))
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
		t.Fatal("occupied target reached resource finalization")
		return false, nil
	}
	occupiedPod := gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "leader", "unrelated")
	c := legacyNewFakeClient(t, occupiedPod)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetRevision}}

	done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
	if err != nil || done {
		t.Fatalf("occupied-target rollback: done=%v err=%v", done, err)
	}
	persistedSource := store.statuses[source.Index]
	if persistedSource.Phase != workload.InstancePhaseReady || persistedSource.Operation != nil ||
		persistedSource.RunningRevision != source.RunningRevision {
		t.Fatalf("source claim was not rolled back: %+v", persistedSource)
	}
	if persistedOccupied := store.statuses[surgeIndex]; !occupiedIdentity.Matches(persistedOccupied) {
		t.Fatalf("occupied target changed: %+v", persistedOccupied)
	}
	pods, err := query.LiveListPodsForInstance(context.Background(), c, namespace, isvcName, workload.ComponentEngine, surgeIndex)
	if err != nil || len(pods) != 1 {
		t.Fatalf("occupied target pods after rollback: count=%d err=%v", len(pods), err)
	}

	input.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		cloneTerminalStatus(store.statuses[source.Index]),
		cloneTerminalStatus(store.statuses[surgeIndex]),
	}
	done, err = gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
	if err != nil || done {
		t.Fatalf("reallocation pass: done=%v err=%v", done, err)
	}
	persistedSource = store.statuses[source.Index]
	if persistedSource.Operation == nil || persistedSource.Operation.SurgeIndex == nil ||
		*persistedSource.Operation.SurgeIndex == surgeIndex {
		t.Fatalf("source did not reallocate away from occupied index %d: %+v", surgeIndex, persistedSource)
	}
	if persistedOccupied := store.statuses[surgeIndex]; !occupiedIdentity.Matches(persistedOccupied) {
		t.Fatalf("reallocation changed occupied target: %+v", persistedOccupied)
	}
}

func TestGangSurge_LegacyAdapterRecoversPromotedAndOccupiedTargets(t *testing.T) {
	tests := []struct {
		name         string
		targetStatus func(int32, string) workload.InstanceStatus
		withPods     bool
		wantRemoved  bool
	}{
		{
			name:         "promoted target",
			targetStatus: gangSurgePromotedTarget,
			withPods:     true,
			wantRemoved:  true,
		},
		{
			name: "occupied target",
			targetStatus: func(index int32, _ string) workload.InstanceStatus {
				return workload.InstanceStatus{Index: index, Incarnation: 9, Phase: workload.InstancePhaseReady, RunningRevision: "unrelated"}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			legacyResetExpectations(t)
			const isvcName, namespace = "gang-legacy-recovery", "test-ns"
			const targetRevision = "gang-legacy-recovery-engine-newrev"
			surgeIndex := int32(2)
			source := gangSurgeRecoverySource(surgeIndex, targetRevision)
			targetStatus := test.targetStatus(surgeIndex, targetRevision)
			store := &terminalMutationStore{statuses: map[int32]workload.InstanceStatus{
				source.Index:       cloneTerminalStatus(source),
				targetStatus.Index: cloneTerminalStatus(targetStatus),
			}}
			input := gangSurgeRecoveryInput("", isvcName, namespace, store,
				cloneTerminalStatus(source), cloneTerminalStatus(targetStatus))
			input.ApplyInstanceMutationsWithRetryBlock = nil
			input.RemoveInstance = func(_ context.Context, index int32) (bool, error) {
				if _, found := store.statuses[index]; !found {
					return false, nil
				}
				delete(store.statuses, index)
				return true, nil
			}
			var objects []client.Object
			if test.withPods {
				objects = append(objects,
					gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "leader", "newrev"),
					gangSurgeReadyRecoveryPod(isvcName, namespace, surgeIndex, "worker", "newrev"),
				)
			}
			c := legacyNewFakeClient(t, objects...)
			plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetRevision}}

			done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
			if err != nil {
				t.Fatal(err)
			}
			_, sourceFound := store.statuses[source.Index]
			if done != test.wantRemoved || sourceFound == test.wantRemoved {
				t.Fatalf("done=%v sourceFound=%v want removed=%v", done, sourceFound, test.wantRemoved)
			}
			persistedTarget := store.statuses[surgeIndex]
			if !status.Capture(&targetStatus).Matches(persistedTarget) {
				t.Fatalf("legacy recovery changed target: %+v", persistedTarget)
			}
		})
	}
}

func TestGangSurge_PodlessSourcePersistsTerminalMarkerBeforeFinalization(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-terminal", "test-ns"
	surgeIndex := int32(2)
	revision := "gang-terminal-engine-newrev"
	readySurgePod := func(runner string) *corev1.Pod {
		pod := gangSurgePod(isvcName, namespace, surgeIndex, runner, "newrev")
		now := metav1.Now()
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: now},
		}
		return pod
	}
	c := legacyNewFakeClient(t, readySurgePod("leader"), readySurgePod("worker"))

	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     4,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-terminal-engine-oldrev",
		TargetRevision:  revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: revision,
			SurgeIndex:     &surgeIndex,
		},
	}
	surge := workload.InstanceStatus{
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
			surge.Index:  cloneTerminalStatus(surge),
		},
	}
	finalizes := 0
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
			InstanceStatuses: []workload.InstanceStatus{cloneTerminalStatus(source), cloneTerminalStatus(surge)},
		},
		MutateInstance: func(_ context.Context, index int32, mutate func(*workload.InstanceStatus) bool) error {
			row, found := store.statuses[index]
			if found && mutate(&row) {
				store.statuses[index] = row
			}
			return nil
		},
		FinalizeInstanceResources: func(context.Context, int32) (bool, error) {
			finalizes++
			return true, nil
		},
	}
	statusFailure := errors.New("injected status failure")
	input.ApplyInstanceMutationsWithRetryBlock = func(ctx context.Context, mutations []workload.InstanceMutation, revision string, mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		for _, mutation := range mutations {
			if mutation.Index == source.Index && mutation.Remove {
				store.applyErr = statusFailure
			}
		}
		return store.apply(ctx, mutations, revision, mutateRetryBlock)
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
	if !errors.Is(err, statusFailure) || done {
		t.Fatalf("first pass: done=%v err=%v", done, err)
	}
	persistedSource, found := store.statuses[source.Index]
	if !found || persistedSource.Operation == nil || persistedSource.Operation.Step != workload.UpdateStepSurgeDrain {
		t.Fatalf("source marker after failed removal: %+v", persistedSource)
	}
	if finalizes != 1 || store.writes != 2 {
		t.Fatalf("first pass: finalizes=%d statusWrites=%d want 1,2", finalizes, store.writes)
	}

	store.applyErr = nil
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	input.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		cloneTerminalStatus(store.statuses[source.Index]),
		cloneTerminalStatus(store.statuses[surgeIndex]),
	}
	done, err = gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
	if err != nil || !done {
		t.Fatalf("retry pass: done=%v err=%v", done, err)
	}
	if _, found := store.statuses[source.Index]; found {
		t.Fatal("retry did not remove the terminal source status")
	}
	if finalizes != 2 {
		t.Fatalf("resource finalization calls=%d want 2", finalizes)
	}
}

func TestAbandonFailedGangSurge_PersistsCleanupMarkerAcrossRemovalFailure(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-abandon-terminal", "test-ns"
	surgeIndex := int32(2)
	targetRevision := "gang-abandon-terminal-engine-badrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     4,
		Phase:           workload.InstancePhaseFailed,
		RunningRevision: "gang-abandon-terminal-engine-goodrev",
		TargetRevision:  targetRevision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: targetRevision,
			SurgeIndex:     &surgeIndex,
		},
	}
	marker := workload.InstanceStatus{
		Index:          surgeIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		TargetRevision: targetRevision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-target-2",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTarget,
			TargetRevision: targetRevision,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index: cloneTerminalStatus(source),
			marker.Index: cloneTerminalStatus(marker),
		},
	}
	finalizes := 0
	warnings := 0
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
		MutateInstance: func(context.Context, int32, func(*workload.InstanceStatus) bool) error {
			t.Fatal("strong abandon tail used a standalone source status writer")
			return nil
		},
		MutateRetryBlock: func(context.Context, string, func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
			t.Fatal("strong abandon tail used a standalone RetryBlock writer")
			return nil
		},
		WarnRetryHeld: func(revision string, attempts int32, reason string) {
			if revision != targetRevision || attempts != 1 || reason != "pod stuck" {
				t.Fatalf("Held warning=(%q,%d,%q)", revision, attempts, reason)
			}
			warnings++
		},
		FinalizeInstanceResources: func(context.Context, int32) (bool, error) {
			finalizes++
			return true, nil
		},
	}
	statusFailure := errors.New("injected status failure")
	input.ApplyInstanceMutationsWithRetryBlock = func(ctx context.Context, mutations []workload.InstanceMutation, revision string, mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		if store.applyCall == 2 {
			store.applyErr = statusFailure
		}
		return store.apply(ctx, mutations, revision, mutateRetryBlock)
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	c := legacyNewFakeClient(t)

	done, err := abandonFailedGangSurge(
		context.Background(), legacyTestDeps(c), input, plan, source.Index, surgeIndex,
		source.RunningRevision, targetRevision, "pod stuck", workload.CauseWorkload, gangSurgeEnd{},
	)
	if !errors.Is(err, statusFailure) || done {
		t.Fatalf("first pass: done=%v err=%v", done, err)
	}
	persistedMarker, found := store.statuses[surgeIndex]
	if !found || persistedMarker.Operation == nil || persistedMarker.Operation.Step != workload.UpdateStepGangSurgeTargetCleanup {
		t.Fatalf("cleanup marker after failed removal: %+v", persistedMarker)
	}
	if finalizes != 1 || store.writes != 1 {
		t.Fatalf("first pass: finalizes=%d statusWrites=%d want 1,1", finalizes, store.writes)
	}
	if _, found := store.retryBlock[targetRevision]; found || warnings != 0 {
		t.Fatalf("failed atomic write committed RetryBlock or warning: blocks=%v warnings=%d", store.retryBlock, warnings)
	}
	persistedSource := store.statuses[source.Index]
	if persistedSource.Phase != workload.InstancePhaseFailed || persistedSource.Operation == nil {
		t.Fatalf("failed atomic write reset source: %+v", persistedSource)
	}

	store.applyErr = nil
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	input.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		cloneTerminalStatus(store.statuses[source.Index]),
		cloneTerminalStatus(store.statuses[surgeIndex]),
	}
	done, err = abandonFailedGangSurge(
		context.Background(), legacyTestDeps(c), input, plan, source.Index, surgeIndex,
		source.RunningRevision, targetRevision, "pod stuck", workload.CauseWorkload, gangSurgeEnd{},
	)
	if err != nil || done {
		t.Fatalf("retry pass: done=%v err=%v", done, err)
	}
	if _, found := store.statuses[surgeIndex]; found {
		t.Fatal("retry did not remove the terminal surge marker")
	}
	persistedSource = store.statuses[source.Index]
	if persistedSource.Phase != workload.InstancePhaseReady || persistedSource.Operation != nil {
		t.Fatalf("retry did not reset source: %+v", persistedSource)
	}
	block, found := store.retryBlock[targetRevision]
	if !found || block.State != workload.RetryBlockHeld || block.AttemptsStarted != 1 || warnings != 1 {
		t.Fatalf("atomic RetryBlock=%+v found=%v warnings=%d", block, found, warnings)
	}
	if finalizes != 2 || store.writes != 2 {
		t.Fatalf("resource finalization calls=%d writes=%d want 2,2", finalizes, store.writes)
	}
}

// TestAbandonFailedGangSurge_AbandonGraceBoundsUnservedMembers pins the
// grace the gang abandon deletes the retired replacement gang with. The
// source keeps its surge step, and so the budget, until the members are
// gone: a member that never carried the serving gate goes on the
// configured abandoned-replacement grace, one that has carried it keeps
// the pod's own grace, and one still in rotation leaves it a pass ahead
// of its deletion.
func TestAbandonFailedGangSurge_AbandonGraceBoundsUnservedMembers(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("llama-70b", "prod", 1)
	plan := gangSurgePlan()
	v1Name := "llama-70b-engine-rev-v1hash"
	v2Name := "llama-70b-engine-rev-v2hash"
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)

	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, isvc, ir)
	// The replacement gang at the surge index: the leader has been marked
	// serving, the worker never was.
	leader := gangPodAt(isvc, 1, "leader", v2Hash, false, true)
	worker := gangPodAt(isvc, 1, "worker", v2Hash, false, false)
	for _, pod := range []*corev1.Pod{leader, worker} {
		if err := c.Create(context.Background(), pod); err != nil {
			t.Fatalf("seed replacement pod %s: %v", pod.Name, err)
		}
	}
	input := gangInputWithRemove(isvc, c)
	input.AbandonedReplacementGrace = 7 * time.Second

	done, err := abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, 1, v1Name, v2Name, "", workload.CauseUnattributed, gangSurgeEnd{})
	if err != nil || done {
		t.Fatalf("abandon pass: done=%v err=%v", done, err)
	}
	// The serving leader leaves rotation first; nothing is deleted yet.
	if len(deletes) != 0 {
		t.Fatalf("deletes = %+v, want none while the serving leader leaves rotation", deletes)
	}
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(leader), fresh); err != nil {
		t.Fatalf("re-read leader: %v", err)
	}
	if podreadiness.IsServing(fresh) {
		t.Fatalf("the leader must leave rotation before it is deleted")
	}
	done, err = abandonFailedGangSurge(context.Background(), legacyTestDeps(c), input, plan, 0, 1, v1Name, v2Name, "", workload.CauseUnattributed, gangSurgeEnd{})
	if err != nil || done {
		t.Fatalf("delete pass: done=%v err=%v", done, err)
	}
	if len(deletes) != 2 {
		t.Fatalf("deletes = %+v, want both replacement members", deletes)
	}
	for _, d := range deletes {
		switch d.name {
		case leader.Name:
			if d.grace != nil {
				t.Errorf("leader that has served deleted with grace %d, want the pod's own grace", *d.grace)
			}
		case worker.Name:
			if d.grace == nil || *d.grace != 7 {
				t.Errorf("never-served worker deleted with grace %v, want 7", d.grace)
			}
		default:
			t.Errorf("unexpected delete of %s", d.name)
		}
	}
}

func TestAbandonFailedGangSurge_AtomicallyRemovesMarkerAndResetsSource(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-abandon-atomic", "test-ns"
	const runningRevision = "gang-abandon-atomic-engine-oldrev"
	const targetRevision = "gang-abandon-atomic-engine-newrev"
	surgeIndex := int32(2)
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     4,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: runningRevision,
		TargetRevision:  targetRevision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: targetRevision,
			SurgeIndex:     &surgeIndex,
		},
	}
	marker := workload.InstanceStatus{
		Index:          surgeIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		TargetRevision: targetRevision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-target-2",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTargetCleanup,
			TargetRevision: targetRevision,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index: cloneTerminalStatus(source),
			marker.Index: cloneTerminalStatus(marker),
		},
	}
	atomicCommits := 0
	finalizes := 0
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
		MutateInstance: func(context.Context, int32, func(*workload.InstanceStatus) bool) error {
			t.Fatal("strong abandon tail used a standalone source status writer")
			return nil
		},
		FinalizeInstanceResources: func(_ context.Context, index int32) (bool, error) {
			if index != surgeIndex {
				t.Fatalf("finalized index=%d want %d", index, surgeIndex)
			}
			finalizes++
			return true, nil
		},
	}
	input.ApplyInstanceMutationsWithRetryBlock = func(
		ctx context.Context,
		mutations []workload.InstanceMutation,
		revision string,
		mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition,
	) error {
		if len(mutations) == 2 {
			atomicCommits++
			if mutations[0].Index != source.Index || mutations[0].Mutate == nil || mutations[0].Remove ||
				mutations[1].Index != surgeIndex || !mutations[1].Remove || mutations[1].Mutate != nil {
				t.Fatalf("atomic mutations do not reset source and remove target: %+v", mutations)
			}
		}
		return store.apply(ctx, mutations, revision, mutateRetryBlock)
	}

	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	done, err := abandonFailedGangSurge(
		context.Background(), legacyTestDeps(legacyNewFakeClient(t)), input, plan,
		source.Index, surgeIndex, runningRevision, "", "", workload.CauseUnattributed, gangSurgeEnd{},
	)
	if err != nil || done {
		t.Fatalf("abandon: done=%v err=%v", done, err)
	}
	if atomicCommits != 1 || finalizes != 1 {
		t.Fatalf("atomic commits=%d finalizes=%d want 1,1", atomicCommits, finalizes)
	}
	if _, found := store.statuses[surgeIndex]; found {
		t.Fatal("atomic abandon retained target cleanup marker")
	}
	persistedSource := store.statuses[source.Index]
	if persistedSource.Phase != workload.InstancePhaseReady || persistedSource.Operation != nil ||
		persistedSource.RunningRevision != runningRevision || persistedSource.TargetRevision != "" {
		t.Fatalf("atomic abandon did not reset source: %+v", persistedSource)
	}
}

func TestGangSurge_CleanupMarkerRemainsTerminalWhenDesiredRevisionReturns(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-cleanup-revert", "test-ns"
	surgeIndex := int32(2)
	revision := "gang-cleanup-revert-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-cleanup-revert-engine-oldrev",
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
			Step:           workload.UpdateStepGangSurgeTargetCleanup,
			TargetRevision: revision,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index: cloneTerminalStatus(source),
			marker.Index: cloneTerminalStatus(marker),
		},
	}
	finalizes := 0
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
		MutateInstance: func(_ context.Context, index int32, mutate func(*workload.InstanceStatus) bool) error {
			row, found := store.statuses[index]
			if found && mutate(&row) {
				store.statuses[index] = row
			}
			return nil
		},
		FinalizeInstanceResources: func(context.Context, int32) (bool, error) {
			finalizes++
			return true, nil
		},
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	c := legacyNewFakeClient(t)
	ensureCalls := 0
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = func(context.Context, workload.ReconcileInput, workload.ComponentPlan, workload.InstancePlan) (string, error) {
		ensureCalls++
		return "", nil
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil || done {
		t.Fatalf("cleanup pass: done=%v err=%v", done, err)
	}
	if _, found := store.statuses[surgeIndex]; found {
		t.Fatal("terminal target marker was recreated instead of removed")
	}
	persistedSource := store.statuses[source.Index]
	if persistedSource.Phase != workload.InstancePhaseReady || persistedSource.Operation != nil {
		t.Fatalf("source was not reset after target cleanup: %+v", persistedSource)
	}
	if finalizes != 1 || ensureCalls != 0 {
		t.Fatalf("finalizes=%d PodGroup ensures=%d want 1,0", finalizes, ensureCalls)
	}
}

func TestGangSurge_StaleCachedCleanupDoesNotDeletePods(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-stale-cleanup", "test-ns"
	surgeIndex := int32(2)
	revision := "gang-stale-cleanup-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-stale-cleanup-engine-oldrev",
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
			Step:           workload.UpdateStepGangSurgeTargetCleanup,
			TargetRevision: revision,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{source.Index: cloneTerminalStatus(source)},
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
		MutateInstance: func(context.Context, int32, func(*workload.InstanceStatus) bool) error {
			t.Fatal("stale cleanup reached source reset")
			return nil
		},
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	leader := gangSurgePod(isvcName, namespace, surgeIndex, "leader", "newrev")
	worker := gangSurgePod(isvcName, namespace, surgeIndex, "worker", "newrev")
	c := legacyNewFakeClient(t, leader, worker)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target)
	if err != nil || done {
		t.Fatalf("stale cleanup pass: done=%v err=%v", done, err)
	}
	pods, err := query.LiveListPodsForInstance(context.Background(), c, namespace, isvcName, workload.ComponentEngine, surgeIndex)
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 2 {
		t.Fatalf("stale cleanup deleted %d pods", 2-len(pods))
	}
	if store.writes != 0 {
		t.Fatalf("status writes=%d want 0", store.writes)
	}
}

// atomicAbandonWave drives one strong-path abandon wave (surge pods already
// gone, marker already in the cleanup step) for a Failed source whose
// LastFailure carries failure, against store — which keeps the RetryBlocks
// persisted by prior waves. Returns the wave's WarnRetryHeld invocations.
func atomicAbandonWave(t *testing.T, store *terminalMutationStore, t0 time.Time, policy *workload.RetryPolicy, failure *workload.InstanceTermination) []retryHeldWarning {
	t.Helper()
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-abandon-cause", "test-ns"
	const runningRevision = "gang-abandon-cause-engine-goodrev"
	const targetRevision = "gang-abandon-cause-engine-badrev"
	surgeIndex := int32(2)
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     4,
		Phase:           workload.InstancePhaseFailed,
		RunningRevision: runningRevision,
		TargetRevision:  targetRevision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: targetRevision,
			SurgeIndex:     &surgeIndex,
		},
		LastFailure: failure,
	}
	marker := workload.InstanceStatus{
		Index:          surgeIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		TargetRevision: targetRevision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-target-2",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTargetCleanup,
			TargetRevision: targetRevision,
		},
	}
	store.statuses = map[int32]workload.InstanceStatus{
		source.Index: cloneTerminalStatus(source),
		marker.Index: cloneTerminalStatus(marker),
	}
	var warnings []retryHeldWarning
	input := workload.ReconcileInput{
		OwnerObject:       &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: store.ownerUID}},
		Clock:             clocktesting.NewFakeClock(t0),
		UpdateRetryPolicy: policy,
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
		MutateInstance: func(context.Context, int32, func(*workload.InstanceStatus) bool) error {
			t.Fatal("strong abandon tail used a standalone source status writer")
			return nil
		},
		MutateRetryBlock: func(context.Context, string, func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
			t.Fatal("strong abandon tail used a standalone RetryBlock writer")
			return nil
		},
		WarnRetryHeld: func(revision string, attempts int32, reason string) {
			warnings = append(warnings, retryHeldWarning{rev: revision, attempts: attempts, reason: reason})
		},
		FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	done, err := abandonFailedGangSurge(
		context.Background(), legacyTestDeps(legacyNewFakeClient(t)), input, plan, source.Index, surgeIndex,
		runningRevision, targetRevision, instanceFailureReason(&source, "gang surge abandoned"), instanceFailureCause(&source), gangSurgeEnd{},
	)
	if err != nil || done {
		t.Fatalf("abandon: done=%v err=%v", done, err)
	}
	if _, found := store.statuses[surgeIndex]; found {
		t.Fatal("atomic abandon retained the target cleanup marker")
	}
	if persisted := store.statuses[source.Index]; persisted.Phase != workload.InstancePhaseReady || persisted.Operation != nil {
		t.Fatalf("atomic abandon did not reset the source: %+v", persisted)
	}
	return warnings
}

// TestAbandonFailedGangSurge_AtomicTailClassifiesFailureCause: the atomic
// abandon tail applies the same cause attribution as the standalone one.
// Workload-caused and unattributed waves alike count on the one ladder
// inside the atomic write until Held; a scheduler hold touches no block;
// and without a policy an unattributed wave leaves no block at all.
func TestAbandonFailedGangSurge_AtomicTailClassifiesFailureCause(t *testing.T) {
	const targetRevision = "gang-abandon-cause-engine-badrev"
	t0 := time.Now()
	policy := retryTestPolicy()
	deadline := &workload.InstanceTermination{
		Reason:  "DeadlineExceeded",
		Message: "DeadlineExceeded: Update/Surge exceeded InstanceReadyTimeout",
	}
	unschedulable := &workload.InstanceTermination{
		PodName: "gang-abandon-cause-engine-2-leader-0",
		Reason:  workload.WaitingReasonUnschedulable,
		Message: workload.WaitingReasonUnschedulable + ": 0/3 nodes are available",
	}

	// flipForNextWave models the attempt stamp between waves: the gate
	// admits the revision at NextRetryAt and the due Backoff block flips to
	// RetryInProgress for the new attempt.
	flipForNextWave := func(t *testing.T, store *terminalMutationStore) {
		t.Helper()
		block := store.retryBlock[targetRevision]
		if block.NextRetryAt == nil {
			t.Fatalf("Backoff block without NextRetryAt: %+v", block)
		}
		if denied, _ := evaluateRetryBlockGate(&block, block.NextRetryAt.Time, false); denied {
			t.Fatalf("gate at NextRetryAt must admit the next attempt: %+v", block)
		}
		if status.RetryBlockStartAttempt(&block) != workload.RetryBlockPersist {
			t.Fatalf("attempt stamp must flip Backoff to RetryInProgress: %+v", block)
		}
		store.retryBlock[targetRevision] = block
	}

	t.Run("workload-caused waves hold at MaxAttempts", func(t *testing.T) {
		store := &terminalMutationStore{ownerUID: "owner-a"}
		evidence := &workload.InstanceTermination{PodName: "gang-abandon-cause-engine-2-leader-0", Reason: "ImagePullBackOff"}
		for wave := int32(1); wave <= policy.MaxAttempts; wave++ {
			warnings := atomicAbandonWave(t, store, t0, policy, evidence)
			block, found := store.retryBlock[targetRevision]
			if !found || block.AttemptsStarted != wave {
				t.Fatalf("wave %d: block=%+v found=%v want AttemptsStarted=%d", wave, block, found, wave)
			}
			if wave < policy.MaxAttempts {
				if block.State != workload.RetryBlockBackoff || len(warnings) != 0 {
					t.Fatalf("wave %d: got (state=%q, warnings=%d) want (Backoff, 0)", wave, block.State, len(warnings))
				}
				flipForNextWave(t, store)
				continue
			}
			if block.State != workload.RetryBlockHeld || len(warnings) != 1 || warnings[0].attempts != policy.MaxAttempts {
				t.Fatalf("wave %d: got (state=%q, warnings=%+v) want (Held, one warning with attempts=%d)", wave, block.State, warnings, policy.MaxAttempts)
			}
		}
	})

	t.Run("unattributed waves hold at MaxAttempts", func(t *testing.T) {
		store := &terminalMutationStore{ownerUID: "owner-a"}
		for wave := int32(1); wave <= policy.MaxAttempts; wave++ {
			warnings := atomicAbandonWave(t, store, t0, policy, deadline)
			block, found := store.retryBlock[targetRevision]
			if !found || block.AttemptsStarted != wave || block.Reason != deadline.Message {
				t.Fatalf("wave %d: block=%+v found=%v want AttemptsStarted=%d with the deadline evidence", wave, block, found, wave)
			}
			if wave < policy.MaxAttempts {
				if block.State != workload.RetryBlockBackoff || len(warnings) != 0 {
					t.Fatalf("wave %d: got (state=%q, warnings=%d) want (Backoff, 0)", wave, block.State, len(warnings))
				}
				if want := t0.Add(policy.NextRetryDelay(wave)); block.NextRetryAt == nil || !block.NextRetryAt.Time.Equal(want) {
					t.Fatalf("wave %d: NextRetryAt got %v want %v", wave, block.NextRetryAt, want)
				}
				flipForNextWave(t, store)
				continue
			}
			if block.State != workload.RetryBlockHeld || len(warnings) != 1 || warnings[0].attempts != policy.MaxAttempts {
				t.Fatalf("wave %d: got (state=%q, warnings=%+v) want (Held, one warning with attempts=%d)", wave, block.State, warnings, policy.MaxAttempts)
			}
		}
	})

	t.Run("scheduler hold leaves no block", func(t *testing.T) {
		store := &terminalMutationStore{ownerUID: "owner-a"}
		for wave := int32(1); wave <= policy.MaxAttempts+1; wave++ {
			warnings := atomicAbandonWave(t, store, t0, policy, unschedulable)
			if _, found := store.retryBlock[targetRevision]; found || len(warnings) != 0 {
				t.Fatalf("wave %d: a scheduler hold must leave no block: blocks=%v warnings=%d", wave, store.retryBlock, len(warnings))
			}
		}
	})

	t.Run("unattributed wave without a policy leaves no block", func(t *testing.T) {
		store := &terminalMutationStore{ownerUID: "owner-a"}
		warnings := atomicAbandonWave(t, store, t0, nil, deadline)
		if _, found := store.retryBlock[targetRevision]; found || len(warnings) != 0 {
			t.Fatalf("unconfigured policy must not Hold an attempt the revision is not blamed for: blocks=%v warnings=%d", store.retryBlock, len(warnings))
		}
	})
}

func TestGangSurge_StaleSnapshotDoesNotClaimOccupiedTarget(t *testing.T) {
	legacyResetExpectations(t)
	const targetRevision = "stale-claim-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: "stale-claim-engine-oldrev",
	}
	occupied := workload.InstanceStatus{
		Index:           1,
		Incarnation:     8,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: "unrelated-revision",
	}
	sourceIdentity := status.Capture(&source)
	occupiedIdentity := status.Capture(&occupied)
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index:   cloneTerminalStatus(source),
			occupied.Index: cloneTerminalStatus(occupied),
		},
		retryBlock: map[string]workload.RetryBlock{
			targetRevision: {TargetRevision: targetRevision, State: workload.RetryBlockBackoff},
		},
	}
	input := gangSurgeRecoveryInput("owner-a", "stale-claim", "test-ns", store, cloneTerminalStatus(source))
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetRevision}}

	done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(legacyNewFakeClient(t)), input, plan, plan.Instances[0], target)
	if err != nil || done {
		t.Fatalf("stale claim: done=%v err=%v", done, err)
	}
	if store.writes != 0 {
		t.Fatalf("stale claim status writes=%d want 0", store.writes)
	}
	if current := store.statuses[source.Index]; !sourceIdentity.Matches(current) {
		t.Fatalf("source changed without owning the target: %+v", current)
	}
	if current := store.statuses[occupied.Index]; !occupiedIdentity.Matches(current) {
		t.Fatalf("occupied target changed: %+v", current)
	}
	if block := store.retryBlock[targetRevision]; block.State != workload.RetryBlockBackoff {
		t.Fatalf("retry block state=%q want %q", block.State, workload.RetryBlockBackoff)
	}
}

func TestStartGangSurge_ClaimsPairInOneWrite(t *testing.T) {
	const targetRevision = "atomic-claim-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: "atomic-claim-engine-oldrev",
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{source.Index: cloneTerminalStatus(source)},
		retryBlock: map[string]workload.RetryBlock{
			targetRevision: {TargetRevision: targetRevision, State: workload.RetryBlockBackoff},
		},
	}
	input := gangSurgeRecoveryInput("owner-a", "atomic-claim", "test-ns", store, cloneTerminalStatus(source))

	claimed, err := status.StartGangSurge(context.Background(), input, &source, 1, targetRevision, workload.UpdateStrategySurgeThenDrain, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("atomic claim: claimed=%v err=%v", claimed, err)
	}
	if store.writes != 1 {
		t.Fatalf("atomic claim status writes=%d want 1", store.writes)
	}
	persistedSource := store.statuses[source.Index]
	if persistedSource.Operation == nil || persistedSource.Operation.Step != workload.UpdateStepSurge ||
		persistedSource.Operation.SurgeIndex == nil || *persistedSource.Operation.SurgeIndex != 1 {
		t.Fatalf("source claim: %+v", persistedSource)
	}
	persistedTarget := store.statuses[1]
	if !gangSurgeTargetMarkerAt(&persistedTarget, targetRevision) {
		t.Fatalf("target claim: %+v", persistedTarget)
	}
	if block := store.retryBlock[targetRevision]; block.State != workload.RetryBlockRetryInProgress {
		t.Fatalf("retry block state=%q want %q", block.State, workload.RetryBlockRetryInProgress)
	}
}

func TestStartGangSurge_RejectsAuthoritativeDrift(t *testing.T) {
	const targetRevision = "guarded-claim-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: "guarded-claim-engine-oldrev",
	}
	tests := []struct {
		name     string
		ownerUID types.UID
		prepare  func(map[int32]workload.InstanceStatus)
	}{
		{name: "owner replaced", ownerUID: "owner-b"},
		{
			name:     "source lifecycle changed",
			ownerUID: "owner-a",
			prepare: func(statuses map[int32]workload.InstanceStatus) {
				changed := statuses[source.Index]
				changed.Incarnation++
				statuses[source.Index] = changed
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statuses := map[int32]workload.InstanceStatus{source.Index: cloneTerminalStatus(source)}
			if test.prepare != nil {
				test.prepare(statuses)
			}
			authoritativeSource := statuses[source.Index]
			before := status.Capture(&authoritativeSource)
			store := &terminalMutationStore{ownerUID: test.ownerUID, statuses: statuses}
			input := gangSurgeRecoveryInput("owner-a", "guarded-claim", "test-ns", store, cloneTerminalStatus(source))

			claimed, err := status.StartGangSurge(context.Background(), input, &source, 1, targetRevision, workload.UpdateStrategySurgeThenDrain, time.Minute)
			if err != nil || claimed || store.writes != 0 {
				t.Fatalf("guarded claim: claimed=%v writes=%d err=%v", claimed, store.writes, err)
			}
			if current := store.statuses[source.Index]; !before.Matches(current) {
				t.Fatalf("authoritative source changed: %+v", current)
			}
			if _, found := store.statuses[1]; found {
				t.Fatal("target was created after the claim guard rejected")
			}
		})
	}
}

func TestStartGangSurge_WriteFailureLeavesPairUnchanged(t *testing.T) {
	const targetRevision = "failed-claim-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: "failed-claim-engine-oldrev",
	}
	writeErr := errors.New("status write failed")
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{source.Index: cloneTerminalStatus(source)},
		applyErr: writeErr,
	}
	input := gangSurgeRecoveryInput("owner-a", "failed-claim", "test-ns", store, cloneTerminalStatus(source))

	claimed, err := status.StartGangSurge(context.Background(), input, &source, 1, targetRevision, workload.UpdateStrategySurgeThenDrain, time.Minute)
	if !errors.Is(err, writeErr) || claimed || store.writes != 0 {
		t.Fatalf("failed claim: claimed=%v writes=%d err=%v", claimed, store.writes, err)
	}
	if current := store.statuses[source.Index]; !status.Capture(&source).Matches(current) {
		t.Fatalf("source changed after failed write: %+v", current)
	}
	if _, found := store.statuses[1]; found {
		t.Fatal("target was created after failed write")
	}
}

func TestPatchInstanceStatusGangSurgeTarget_DoesNotOverwriteOccupiedSlot(t *testing.T) {
	occupied := workload.InstanceStatus{
		Index:           2,
		Incarnation:     8,
		Phase:           workload.InstancePhaseReady,
		RunningRevision: "unrelated-revision",
	}
	input := workload.ReconcileInput{
		MutateInstance: func(_ context.Context, _ int32, mutate func(*workload.InstanceStatus) bool) error {
			if mutate(&occupied) {
				t.Fatal("occupied target slot was mutated")
			}
			return nil
		},
	}

	if err := status.StampGangSurgeTarget(context.Background(), input, occupied.Index, "new-revision", time.Minute); err != nil {
		t.Fatal(err)
	}
	if occupied.Phase != workload.InstancePhaseReady || occupied.RunningRevision != "unrelated-revision" || occupied.Operation != nil {
		t.Fatalf("occupied target slot changed: %+v", occupied)
	}
}

// gangSchedClient is legacyNewFakeClient plus the scheduler-plugins
// scheduling.x-k8s.io scheme so the test can assert PodGroup creation.
func gangSchedClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		v1beta1.AddToScheme,
		discoveryv1.AddToScheme,
		appsv1.AddToScheme,
		schedulingv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add scheme: %v", err)
		}
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(initObjs...).
		Build()
}

// TestGangSurgeUpdate_EnsuresPodGroupBeforeSurgePods pins that
// gangSurgeUpdate announces the surge index's PodGroup to the gang
// scheduler BEFORE creating that gang's pods.
//
// Scenario: a multi-node engine (minReplicas=4, canary capacity 50%).
// The canary surges a new gang at a fresh instance index; on a
// coscheduler-style cluster, surge pods created before their PodGroup
// exists are rejected with "0/1 nodes are available: 1 PodGroup not
// found" and never scheduled, so the canary capacity gate never opens
// and the rollout stalls at phase=Pending.
//
// The top-level EnsurePodGroups pass keys off plan.Instances, and the
// surge index only lands in the plan once its GangSurgeTarget status
// round-trips into ObservedState. In the window before that round-trip,
// gangSurgeUpdate creates the surge pods carrying the pod-group label, so
// it must ensure the PodGroup inline before the create.
func TestGangSurgeUpdate_EnsuresPodGroupBeforeSurgePods(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("gang-a", "prod", 1)
	plan := gangSurgePlan()
	plan.TopologyKey = "network.example.com/fabric-domain"

	v1Name := "gang-a-engine-rev-v1hash"
	v2Name := "gang-a-engine-rev-v2hash"

	// In-flight gang surge, BEFORE the surge pods are created: the source
	// (idx=0) carries Op{Surge, SurgeIndex=1}; idx=1 carries the
	// GangSurgeTarget marker (on IR).
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)

	c := gangSchedClient(t, isvc, ir)
	makeCR(t, c, isvc, v2Name)
	// Source gang still alive at idx=0 (capacity holds during the surge).
	for _, runner := range []string{"leader", "worker"} {
		hash := query.RevisionHashFromControllerRevisionName(v2Name)
		if err := c.Create(context.Background(), gangPodAt(isvc, 0, runner, hash, true, true)); err != nil {
			t.Fatalf("seed source pod (%s): %v", runner, err)
		}
	}

	input := gangInputWithRemove(isvc, c)
	// The cluster has the PodGroup CRD — same flag the IR reconciler threads.
	input.DesiredSpec.GangSchedulingAvailable = true

	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}

	// Wire the inline surge-PodGroup ensure exactly as the IR reconciler
	// does (deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup).
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup(deps)

	// Create-surge-gang pass: this is where the surge gang's pods are
	// created at idx=1.
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge create pass: %v", err)
	}

	// The surge pods exist...
	surgePods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "gang-a", workload.ComponentEngine, 1)
	if err != nil {
		t.Fatalf("list surge gang pods: %v", err)
	}
	if len(surgePods) == 0 {
		t.Fatalf("expected surge gang pods at idx=1; found none")
	}

	// ...and the PodGroup they reference MUST exist (else the gang scheduler
	// rejects them with "PodGroup not found").
	wantPG := query.PodGroupName("gang-a", workload.ComponentEngine, 1)
	pg := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: wantPG}, pg); err != nil {
		t.Fatalf("surge gang PodGroup %q missing when its pods exist: %v", wantPG, err)
	}
	// MinMember must cover the whole gang (leader + worker = 2).
	if pg.Spec.MinMember != 2 {
		t.Errorf("PodGroup %q MinMember=%d, want 2 (leader+worker)", wantPG, pg.Spec.MinMember)
	}
	if got := pg.Annotations[query.AnnotationTopologyKey]; got != plan.TopologyKey {
		t.Errorf("surge PodGroup %q topology annotation=%q, want %q", wantPG, got, plan.TopologyKey)
	}
	// Every surge pod must carry the pod-group label naming that PodGroup.
	for _, pod := range surgePods {
		if got := pod.Labels[query.LabelPodGroup]; got != wantPG {
			t.Errorf("surge pod %s pod-group label=%q, want %q", pod.Name, got, wantPG)
		}
	}
}

// TestGangSurgeUpdate_HoldsSourceDrainUntilReplacementPodReady pins that
// a gang surge must NOT drain the source gang
// out of serving until the REPLACEMENT gang is PodReady (containers ready AND
// the ome.io/serving readiness gate AND'd in by kubelet) — not merely
// ContainersReady. ome.io/serving is itself a readiness gate, so a just-served
// pod is not PodReady (not in Service rotation) until kubelet re-evaluates the
// gate. Draining the source on ContainersReady alone drops it out
// of rotation while the replacement is still not in it — an N-1 capacity
// TROUGH. Across Components rolled on independent timelines those offset
// troughs skew the live RatioBalanced ratio.
func TestGangSurgeUpdate_HoldsSourceDrainUntilReplacementPodReady(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("gang-b", "prod", 1)
	plan := gangSurgePlan()

	v1Name := "gang-b-engine-rev-v1hash"
	v2Name := "gang-b-engine-rev-v2hash"
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)

	c := gangSchedClient(t, isvc, ir)
	makeCR(t, c, isvc, v1Name)
	makeCR(t, c, isvc, v2Name)
	v1Hash := query.RevisionHashFromControllerRevisionName(v1Name)
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)

	// Source gang (idx=0, old rev) fully in rotation.
	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(context.Background(), gangPodAt(isvc, 0, runner, v1Hash, true, true)); err != nil {
			t.Fatalf("seed source pod (%s): %v", runner, err)
		}
	}
	// Replacement gang (idx=1, new rev): ContainersReady + serving, but NOT
	// yet PodReady — the window in which the source must still be held.
	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(context.Background(), gangPodAt(isvc, 1, runner, v2Hash, true, true)); err != nil {
			t.Fatalf("seed replacement pod (%s): %v", runner, err)
		}
	}

	input := gangInputWithRemove(isvc, c)
	input.DesiredSpec.GangSchedulingAvailable = true
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup(deps)

	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}

	srcServing := func(when string) (present int, serving int) {
		pods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "gang-b", workload.ComponentEngine, 0)
		if err != nil {
			t.Fatalf("list source pods (%s): %v", when, err)
		}
		for _, p := range pods {
			present++
			if podreadiness.IsServing(p) {
				serving++
			}
		}
		return
	}

	// Pass 1: replacement NOT PodReady → the source must stay fully serving.
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge pass 1: %v", err)
	}
	if present, serving := srcServing("pass1"); present == 0 || serving != present {
		t.Fatalf("source drained before replacement PodReady (present=%d serving=%d) — capacity trough", present, serving)
	}

	// Flip the replacement gang to PodReady (kubelet AND'd the serving gate).
	replPods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "gang-b", workload.ComponentEngine, 1)
	if err != nil {
		t.Fatalf("list replacement pods: %v", err)
	}
	for _, p := range replPods {
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		})
		if err := c.Status().Update(context.Background(), p); err != nil {
			t.Fatalf("flip replacement PodReady (%s): %v", p.Name, err)
		}
	}

	// Pass 2: replacement PodReady → NOW the source drains out of serving.
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge pass 2: %v", err)
	}
	if present, serving := srcServing("pass2"); present > 0 && serving > 0 {
		t.Errorf("source still serving after replacement PodReady (present=%d serving=%d) — drain did not fire", present, serving)
	}
}

// TestGangSurgeUpdate_HoldsSourceDrainForMinReadySeconds is the gang
// counterpart: a PodReady replacement gang inside the window leaves every
// source pod serving; after the window the source gang is drained.
func TestGangSurgeUpdate_HoldsSourceDrainForMinReadySeconds(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("llama-70b", "prod", 1)
	plan := gangSurgePlan()
	plan.MinReadySeconds = 20

	v1Name := "llama-70b-engine-rev-v1hash"
	v2Name := "llama-70b-engine-rev-v2hash"
	v1Hash := query.RevisionHashFromControllerRevisionName(v1Name)
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)
	c := legacyNewFakeClient(t, isvc, ir)
	makeCR(t, c, isvc, v2Name)

	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(context.Background(), gangPodAt(isvc, 0, runner, v1Hash, true, true)); err != nil {
			t.Fatalf("seed source gang pod (%s): %v", runner, err)
		}
	}
	for _, runner := range []string{"leader", "worker"} {
		p := gangPodAt(isvc, 1, runner, v2Hash, true, true)
		podReadyAt(p, minReadyWindowStart.Add(-10*time.Second))
		if err := c.Create(context.Background(), p); err != nil {
			t.Fatalf("seed surge gang pod (%s): %v", runner, err)
		}
	}

	clk := clocktesting.NewFakeClock(minReadyWindowStart)
	input := gangInputWithRemove(isvc, c)
	input.Clock = clk
	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}
	// Hold the pass at the post-drain Satisfied() gate so drained source
	// pods stay observable instead of being deleted in the same pass.
	workload.DefaultExpectations.ExpectDeletes("prod", "llama-70b", workload.ComponentEngine, 0, 1)

	sourcePods := func() []*corev1.Pod {
		pods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "llama-70b", workload.ComponentEngine, 0)
		if err != nil {
			t.Fatalf("list source gang pods: %v", err)
		}
		if len(pods) != 2 {
			t.Fatalf("source gang pods: got %d want 2", len(pods))
		}
		return pods
	}

	if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge pass inside window: %v", err)
	}
	for _, pod := range sourcePods() {
		if !podreadiness.IsServing(pod) {
			t.Fatalf("source gang pod %s left rotation before the replacement gang was Available", pod.Name)
		}
	}
	src := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if src.Operation == nil || src.Operation.Step != workload.UpdateStepSurge {
		t.Fatalf("source operation advanced past Surge inside the window: %+v", src.Operation)
	}

	clk.SetTime(minReadyWindowStart.Add(10 * time.Second))
	if _, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge pass after window: %v", err)
	}
	for _, pod := range sourcePods() {
		if podreadiness.IsServing(pod) {
			t.Fatalf("source gang pod %s still serving after the replacement gang became Available", pod.Name)
		}
	}
}

// quietGangSurgeFixture is the gang surge mid-flight whose replacement
// leader's kubelet has stopped reporting: phase Unknown, bound to a node
// the cluster never had, so node-death evidence reads it as gone. The
// input carries an owner the sweep's audit ledger can hang off and the
// force-delete policy under test.
func quietGangSurgeFixture(t *testing.T, policy *workload.ForceDeletePolicy) *gangRejectionFixture {
	t.Helper()
	legacyResetExpectations(t)
	f := newGangRejectionFixture(t, "OutOfmemory")
	ctx := context.Background()
	live := &corev1.Pod{}
	if err := f.client.Get(ctx, client.ObjectKeyFromObject(f.dead), live); err != nil {
		t.Fatalf("get replacement leader: %v", err)
	}
	live.Spec.NodeName = "node-gone"
	if err := f.client.Update(ctx, live); err != nil {
		t.Fatalf("bind replacement leader: %v", err)
	}
	live.Status = corev1.PodStatus{Phase: corev1.PodUnknown}
	if err := f.client.Status().Update(ctx, live); err != nil {
		t.Fatalf("quiet replacement leader: %v", err)
	}
	isvc := legacyMinimalISVC(f.isvcName, f.namespace, 1)
	isvc.UID = f.store.ownerUID
	f.input.OwnerObject, f.input.EventTarget = isvc, isvc
	f.input.OwnerGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")
	f.input.ForceDelete = policy
	return f
}

// assertGangSourceUntouched pins that the source row kept its in-flight
// surge: same step, no failure stamped, not Failed.
func assertGangSourceUntouched(t *testing.T, f *gangRejectionFixture) {
	t.Helper()
	src := f.store.statuses[0]
	if src.Phase != workload.InstancePhaseUpdating || src.Operation == nil || src.Operation.Step != workload.UpdateStepSurge {
		t.Errorf("source row: got phase=%s op=%+v want the surge still open at Surge", src.Phase, src.Operation)
	}
	if src.LastFailure != nil {
		t.Errorf("source LastFailure: got %+v want none (a quiet member is not the source's failure)", src.LastFailure)
	}
}

// TestGangSurgeUpdate_UnknownMemberSweptOnProvenNodeDeath: a replacement
// member whose kubelet has stopped reporting keeps its name, so the gang
// can never complete around it. With a force-delete policy configured and
// the node provably gone, the sweep frees the member on the pass that
// reads it — nothing else is created on that pass — and the same attempt
// rebuilds the gang under the surge index; the source row is untouched.
func TestGangSurgeUpdate_UnknownMemberSweptOnProvenNodeDeath(t *testing.T) {
	f := quietGangSurgeFixture(t, &workload.ForceDeletePolicy{OverdueSlack: 2 * time.Minute, NodeUnreachableThreshold: 5 * time.Minute})

	done, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while freeing a quiet member")
	}
	if !sitePodGone(t, f.client, f.dead) {
		t.Fatalf("quiet member %s must be force-deleted once its node is provably gone", f.dead.Name)
	}
	if got := gangPodCount(t, f); got != 0 {
		t.Fatalf("pass 1 created %d members while the name was being freed", got)
	}
	assertGangSourceUntouched(t, f)

	done, err = gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate pass 2: %v", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done on the rebuild pass")
	}
	rebuilt := &corev1.Pod{}
	if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.dead), rebuilt); err != nil {
		t.Fatalf("the same attempt must rebuild the member under the surge index: %v", err)
	}
	if rebuilt.Status.Phase == corev1.PodUnknown {
		t.Fatalf("the member's name still holds the quiet pod, not a fresh one")
	}
	if got := gangPodCount(t, f); got != 2 {
		t.Errorf("rebuilt gang: got %d members want the whole gang", got)
	}
	assertGangSourceUntouched(t, f)
}

// TestGangSurgeUpdate_UnknownMemberHeldWithoutForceDeletePolicy: with no
// force-delete policy nothing can prove the node dead, so the quiet member
// keeps its name and the gang surge neither deletes it nor creates the
// rest of the gang around it; the row waits, and the source is untouched.
func TestGangSurgeUpdate_UnknownMemberHeldWithoutForceDeletePolicy(t *testing.T) {
	f := quietGangSurgeFixture(t, nil)

	done, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while a member is in phase Unknown")
	}
	if sitePodGone(t, f.client, f.dead) {
		t.Fatalf("quiet member %s must not be deleted without a force-delete policy", f.dead.Name)
	}
	if got := gangPodCount(t, f); got != 1 {
		t.Fatalf("pass created %d more members around a name a silent node holds", got-1)
	}
	assertGangSourceUntouched(t, f)
}

// TestGangSurgeUpdate_DrainGateHoldsTheSourceGang is the gang counterpart of
// the drain-time gate: a PodReady replacement gang with a denying DrainGate
// leaves every source pod serving; once the gate allows, the source gang
// drains.
func TestGangSurgeUpdate_DrainGateHoldsTheSourceGang(t *testing.T) {
	legacyResetExpectations(t)
	isvc, _ := surgeISVCReady("gang-d", "prod", 1)
	plan := gangSurgePlan()

	v1Name := "gang-d-engine-rev-v1hash"
	v2Name := "gang-d-engine-rev-v2hash"
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)
	c := gangSchedClient(t, isvc, ir)
	makeCR(t, c, isvc, v1Name)
	makeCR(t, c, isvc, v2Name)
	v1Hash := query.RevisionHashFromControllerRevisionName(v1Name)
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)
	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(context.Background(), gangPodAt(isvc, 0, runner, v1Hash, true, true)); err != nil {
			t.Fatalf("seed source pod (%s): %v", runner, err)
		}
		repl := gangPodAt(isvc, 1, runner, v2Hash, true, true)
		repl.Status.Conditions = append(repl.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		if err := c.Create(context.Background(), repl); err != nil {
			t.Fatalf("seed replacement pod (%s): %v", runner, err)
		}
	}

	input := gangInputWithRemove(isvc, c)
	input.DesiredSpec.GangSchedulingAvailable = true
	input.DrainHolds = &workload.DrainHolds{}
	allow := false
	var asked [][]string
	input.DrainGate = func(sourcePods []string) (bool, workload.RolloutHoldGate, string) {
		asked = append(asked, sourcePods)
		if allow {
			return true, "", ""
		}
		return false, workload.RolloutHoldGatePairing, "peer cohort not serving"
	}
	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup(deps)
	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}
	srcServing := func(when string) (present int, serving int) {
		pods, err := query.LiveListPodsForInstance(context.Background(), c, "prod", "gang-d", workload.ComponentEngine, 0)
		if err != nil {
			t.Fatalf("list source pods (%s): %v", when, err)
		}
		for _, p := range pods {
			present++
			if podreadiness.IsServing(p) {
				serving++
			}
		}
		return
	}

	// Denied: the whole source gang keeps serving and the hold is recorded.
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge under a denying drain gate: %v", err)
	}
	if present, serving := srcServing("held"); present == 0 || serving != present {
		t.Fatalf("a held drain must leave the source gang serving (present=%d serving=%d)", present, serving)
	}
	if len(asked) != 1 || len(asked[0]) != 2 {
		t.Fatalf("the gate must be asked once with both source pods, got %v", asked)
	}
	if hold := input.DrainHolds.First(); hold == nil || hold.Gate != workload.RolloutHoldGatePairing {
		t.Fatalf("the hold must be recorded, got %+v", hold)
	}

	// Allowed: the source gang drains.
	allow = true
	if _, err := surgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], v2, nil); err != nil {
		t.Fatalf("gang surge under an allowing drain gate: %v", err)
	}
	if present, serving := srcServing("admitted"); present > 0 && serving > 0 {
		t.Errorf("an admitted drain must take the source gang out of rotation (present=%d serving=%d)", present, serving)
	}
}

// A gang surge that promotes its replacement rebinds every move still
// pending on its source to the replacement's index before the source is
// removed, and says so once per move. A move already executing, a
// terminal one and a move of another Instance are left as they are.
func TestGangSurge_PromotionRebindsPendingMovesToTheReplacement(t *testing.T) {
	legacyResetExpectations(t)
	const isvcName, ns = "gang-c", "test-ns"
	surgeIdx := int32(2)
	mkReadyPod := func(runner string) *corev1.Pod {
		pod := gangSurgePod(isvcName, ns, surgeIdx, runner, "newrev")
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
		}
		return pod
	}
	c := legacyNewFakeClient(t, mkReadyPod("leader"), mkReadyPod("worker"))

	src := &workload.InstanceStatus{
		Index:           0,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-c-engine-oldrev",
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			SurgeIndex:     &surgeIdx,
			TargetRevision: "gang-c-engine-newrev",
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
			TargetRevision: "gang-c-engine-newrev",
			Operation: &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           workload.UpdateStepGangSurgeTarget,
				TargetRevision: "gang-c-engine-newrev",
			},
		},
	}
	executingSurge := int32(1)
	records := []workload.MigrationRecord{
		{RequestUUID: "pending", Trigger: workload.MigrationTriggerManual, Phase: workload.MigrationPhaseAccepted, SourceInstance: 0, FromNode: "node-a"},
		{RequestUUID: "executing", Trigger: workload.MigrationTriggerManual, Phase: workload.MigrationPhaseSurgePending, SourceInstance: 0, SurgeInstance: &executingSurge},
		{RequestUUID: "done", Trigger: workload.MigrationTriggerManual, Phase: workload.MigrationPhaseCompleted, SourceInstance: 0},
		{RequestUUID: "other", Trigger: workload.MigrationTriggerManual, Phase: workload.MigrationPhaseAccepted, SourceInstance: 1},
	}
	input.ObservedState.Migrations = append([]workload.MigrationRecord(nil), records...)
	removedAtRebind := -1
	input.MutateMigration = func(_ context.Context, uuid string, mutate func(*workload.MigrationRecord) bool) error {
		for i := range records {
			if records[i].RequestUUID != uuid {
				continue
			}
			r := records[i]
			if mutate(&r) {
				records[i] = r
				removedAtRebind = len(removed)
			}
			return nil
		}
		return nil
	}
	input.EventTarget = legacyMinimalISVC(isvcName, ns, 1)
	rec := record.NewFakeRecorder(16)
	deps := legacyTestDeps(c)
	deps.Recorder = rec
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "gang-c-engine-newrev"}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if !done || len(removed) != 1 || removed[0] != 0 {
		t.Fatalf("expected the promote pass to finish and remove the source: done=%v removed=%v", done, removed)
	}
	if removedAtRebind != 0 {
		t.Fatalf("the pending move must be rebound before the source is removed; removed %d rows at the time", removedAtRebind)
	}
	byUUID := map[string]workload.MigrationRecord{}
	for _, r := range records {
		byUUID[r.RequestUUID] = r
	}
	if got := byUUID["pending"]; got.SourceInstance != surgeIdx || got.Phase != workload.MigrationPhaseAccepted || got.SurgeInstance != nil || got.FromNode != "node-a" {
		t.Errorf("the pending move must follow the Instance to index %d, otherwise untouched; got %+v", surgeIdx, got)
	}
	if got := byUUID["executing"]; got.SourceInstance != 0 || got.SurgeInstance == nil || *got.SurgeInstance != executingSurge {
		t.Errorf("a move already executing keeps its source; got %+v", got)
	}
	if got := byUUID["done"]; got.SourceInstance != 0 {
		t.Errorf("a terminal record is left as it is; got %+v", got)
	}
	if got := byUUID["other"]; got.SourceInstance != 1 {
		t.Errorf("a move of another Instance is left as it is; got %+v", got)
	}
	events := drainRecorderEvents(rec)
	rebound := 0
	for _, e := range events {
		if strings.Contains(e, string(workload.EventReasonMigrationSourceRebuilt)) {
			rebound++
		}
	}
	if rebound != 1 {
		t.Errorf("one event names the rebound move; got %d in %v", rebound, events)
	}
}

// TestGangSurgeDrain_SkipsSourceMembersAlreadyOutOfRotation: the source
// gang's drain flip reads the members it listed, and a member can be out
// of rotation before the flip writes: already terminating, or gone between
// the list and the write. Such a member is left alone instead of failing
// the pass, and the members still in rotation are taken out of it.
func TestGangSurgeDrain_SkipsSourceMembersAlreadyOutOfRotation(t *testing.T) {
	for _, tc := range []struct {
		name string
		// leave takes the source leader out of rotation by other means and
		// returns the pod as the pass lists it.
		leave func(t *testing.T, c client.Client, leader *corev1.Pod) *corev1.Pod
		// gone is true when the leader is not in the API.
		gone bool
	}{
		{
			name: "a member already terminating is left to its exit",
			leave: func(t *testing.T, c client.Client, leader *corev1.Pod) *corev1.Pod {
				t.Helper()
				return terminatingPod(t, c, leader)
			},
		},
		{
			name: "a member gone between the list and the flip",
			gone: true,
			leave: func(t *testing.T, c client.Client, leader *corev1.Pod) *corev1.Pod {
				t.Helper()
				listed := &corev1.Pod{}
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(leader), listed); err != nil {
					t.Fatalf("re-read leader: %v", err)
				}
				if err := c.Delete(context.Background(), listed); err != nil {
					t.Fatalf("lose the leader: %v", err)
				}
				return listed
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacyResetExpectations(t)
			isvc, _ := surgeISVCReady("llama-70b", "prod", 1)
			plan := gangSurgePlan()
			v1Name := "llama-70b-engine-rev-v1hash"
			v2Name := "llama-70b-engine-rev-v2hash"
			v1Hash := query.RevisionHashFromControllerRevisionName(v1Name)
			v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)

			base := legacyNewFakeClient(t, isvc, gangSurgeInFlightIR(isvc, v1Name, v2Name))
			makeCR(t, base, isvc, v2Name)
			leader := gangPodAt(isvc, 0, "leader", v1Hash, true, true)
			worker := gangPodAt(isvc, 0, "worker", v1Hash, true, true)
			for _, pod := range []*corev1.Pod{leader, worker} {
				if err := base.Create(context.Background(), pod); err != nil {
					t.Fatalf("seed source gang pod %s: %v", pod.Name, err)
				}
			}
			for _, runner := range []string{"leader", "worker"} {
				p := gangPodAt(isvc, 1, runner, v2Hash, true, true)
				p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
				if err := base.Create(context.Background(), p); err != nil {
					t.Fatalf("seed replacement gang pod %s: %v", runner, err)
				}
			}
			listed := tc.leave(t, base, leader)
			// The pass lists the source gang itself; a member gone since is
			// still in that list, as a pod the API lost after the list is.
			c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
				List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if err := cl.List(ctx, list, opts...); err != nil {
						return err
					}
					if pods, ok := list.(*corev1.PodList); ok && tc.gone {
						for _, p := range pods.Items {
							if p.Name == worker.Name {
								pods.Items = append(pods.Items, *listed.DeepCopy())
								break
							}
						}
					}
					return nil
				},
			})
			// An outstanding expectation holds the pass at the post-drain gate,
			// so the flip is observable on the worker before its deletion.
			workload.DefaultExpectations.ExpectDeletes("prod", "llama-70b", workload.ComponentEngine, 0, 1)

			input := gangInputWithRemove(isvc, c)
			v2 := &appsv1.ControllerRevision{}
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
				t.Fatalf("get v2 CR: %v", err)
			}
			done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], v2, nil)
			if err != nil {
				t.Fatalf("gang surge drain pass: %v (a member already out of rotation must not fail the flip)", err)
			}
			if done {
				t.Fatalf("done: got true want false (the source is still draining)")
			}

			fresh := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(worker), fresh); err != nil {
				t.Fatalf("re-read worker: %v", err)
			}
			if podreadiness.IsServing(fresh) {
				t.Fatalf("the worker, still in rotation, must be taken out of it by the flip")
			}
			if tc.gone {
				return
			}
			left := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(leader), left); err != nil {
				t.Fatalf("the terminating leader must be left to its exit: %v", err)
			}
			if !podreadiness.IsServing(left) {
				t.Fatalf("a member already terminating is out of rotation by its exit; its gate is not rewritten")
			}
		})
	}
}

// TestGangSurgeUpdate_TerminatingReplacementMemberHoldsTheHandoff: a
// replacement member being deleted is gone for the hand-over whatever its
// readiness says. The kubelet keeps the terminating container answering its
// probe while the pod is already out of its Service, so draining the source
// behind it would hand the Instance to a gang about to be short. The source
// keeps serving, nothing is refilled while the object holds its name, and
// once it is gone the same attempt refills the member; the whole gang then
// clears the bar and the source drains.
func TestGangSurgeUpdate_TerminatingReplacementMemberHoldsTheHandoff(t *testing.T) {
	legacyResetExpectations(t)
	ctx := context.Background()
	isvc, _ := surgeISVCReady("gang-b", "prod", 1)
	plan := gangSurgePlan()

	v1Name := "gang-b-engine-rev-v1hash"
	v2Name := "gang-b-engine-rev-v2hash"
	ir := gangSurgeInFlightIR(isvc, v1Name, v2Name)
	c := gangSchedClient(t, isvc, ir)
	makeCR(t, c, isvc, v1Name)
	makeCR(t, c, isvc, v2Name)
	v1Hash := query.RevisionHashFromControllerRevisionName(v1Name)
	v2Hash := query.RevisionHashFromControllerRevisionName(v2Name)

	// Source gang (idx=0) fully in rotation.
	for _, runner := range []string{"leader", "worker"} {
		if err := c.Create(ctx, gangPodAt(isvc, 0, runner, v1Hash, true, true)); err != nil {
			t.Fatalf("seed source pod (%s): %v", runner, err)
		}
	}
	// Replacement gang (idx=1): every member ContainersReady, serving and
	// PodReady, the shape that releases the source drain.
	var worker *corev1.Pod
	for _, runner := range []string{"leader", "worker"} {
		pod := gangPodAt(isvc, 1, runner, v2Hash, true, true)
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now(),
		})
		if err := c.Create(ctx, pod); err != nil {
			t.Fatalf("seed replacement pod (%s): %v", runner, err)
		}
		if runner == "worker" {
			worker = pod
		}
	}
	// The worker is deleted from outside while still Ready.
	terminatingPod(t, c, worker)

	deps := legacyTestDeps(c)
	deps.EnsureGangPodGroup = gang.EnsureSurgePodGroup(deps)
	v2 := &appsv1.ControllerRevision{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "prod", Name: v2Name}, v2); err != nil {
		t.Fatalf("get v2 CR: %v", err)
	}
	gangPods := func(idx int32) (present, serving int, pods []*corev1.Pod) {
		t.Helper()
		pods, err := query.LiveListPodsForInstance(ctx, c, "prod", "gang-b", workload.ComponentEngine, idx)
		if err != nil {
			t.Fatalf("list pods of instance %d: %v", idx, err)
		}
		for _, p := range pods {
			present++
			if podreadiness.IsServing(p) {
				serving++
			}
		}
		return present, serving, pods
	}
	pass := func(when string) {
		t.Helper()
		input := gangInputWithRemove(isvc, c)
		input.DesiredSpec.GangSchedulingAvailable = true
		if _, err := surgeUpdate(ctx, deps, input, plan, plan.Instances[0], v2, nil); err != nil {
			t.Fatalf("gang surge pass (%s): %v", when, err)
		}
	}

	// The replacement reads short: the source keeps serving, the step does
	// not advance, and the Terminating worker holds its name.
	pass("worker terminating")
	if present, serving, _ := gangPods(0); present != 2 || serving != 2 {
		t.Fatalf("source drained behind a replacement member that is being deleted (present=%d serving=%d)", present, serving)
	}
	if present, _, _ := gangPods(1); present != 2 {
		t.Fatalf("replacement pods: got %d want 2 (the Terminating worker holds its name)", present)
	}
	if src := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]; src.Operation == nil || src.Operation.Step != workload.UpdateStepSurge {
		t.Errorf("source operation: got %+v want Step=Surge (the hand-over must not advance)", src.Operation)
	}

	// The worker is gone: the same attempt refills it, and the source still
	// serves while the refill is not Ready.
	releaseTerminatingPod(t, c, worker)
	pass("worker gone")
	var refilled *corev1.Pod
	if _, _, pods := gangPods(1); true {
		for _, p := range pods {
			if p.Name == worker.Name {
				refilled = p
			}
		}
	}
	if refilled == nil || refilled.DeletionTimestamp != nil {
		t.Fatalf("the worker was not refilled once its object was gone")
	}
	if present, serving, _ := gangPods(0); present != 2 || serving != 2 {
		t.Fatalf("source drained while the refilled worker is not Ready (present=%d serving=%d)", present, serving)
	}

	// The refilled worker reports Ready: the whole gang clears the bar and
	// the source leaves rotation (and, with no routed Service to wait on, is
	// deleted in the same pass).
	refilled.Status.Conditions = append(refilled.Status.Conditions,
		corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
		corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()})
	if err := c.Status().Update(ctx, refilled); err != nil {
		t.Fatalf("mark refilled worker Ready: %v", err)
	}
	pass("worker ready")
	if present, serving, _ := gangPods(0); serving != 0 {
		t.Errorf("source still serving after the whole gang cleared the bar (present=%d serving=%d)", present, serving)
	}
}

// gangCleanupRejectionFixture is a gang surge superseded by a newer
// revision: the source still pins the replacement index, the marker is a
// surge target whose members exist, and the Component's revision in force
// has moved past the surge's target.
func gangCleanupRejectionFixture(t *testing.T) (workload.ReconcileInput, *terminalMutationStore, client.Client, int32, string) {
	t.Helper()
	legacyResetExpectations(t)
	const isvcName, namespace = "gang-cleanup", "test-ns"
	const surgeIndex, retired, currentRev = int32(2), "gang-cleanup-engine-retired", "gang-cleanup-engine-inforce"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-cleanup-engine-running",
		TargetRevision:  retired,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: retired,
			SurgeIndex:     ptr.To(surgeIndex),
		},
	}
	marker := workload.InstanceStatus{
		Index:          surgeIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		TargetRevision: retired,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-target-2",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTarget,
			TargetRevision: retired,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index: cloneTerminalStatus(source),
			surgeIndex:   cloneTerminalStatus(marker),
		},
	}
	leader := gangSurgePod(isvcName, namespace, surgeIndex, "leader", query.RevisionHashFromControllerRevisionName(retired))
	worker := gangSurgePod(isvcName, namespace, surgeIndex, "worker", query.RevisionHashFromControllerRevisionName(retired))
	c := legacyNewFakeClient(t, leader, worker)
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
			UpdateRevision:   currentRev,
			InstanceStatuses: []workload.InstanceStatus{cloneTerminalStatus(source), cloneTerminalStatus(marker)},
		},
		FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		return store.apply(ctx, []workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	return input, store, c, surgeIndex, currentRev
}

// rejectPodDeletesCounting wraps c so every pod delete is refused with err,
// counting the attempts.
func rejectPodDeletesCounting(t *testing.T, c client.Client, err error, attempts *int) client.Client {
	t.Helper()
	base, ok := c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", c)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				*attempts++
				return err
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
}

// gangCleanupRejectionPass drives one retirement pass of the fixture's
// superseded surge, re-reading the rows and the ladder from the test's
// stores as the engine would from the owner.
func gangCleanupRejectionPass(t *testing.T, input *workload.ReconcileInput, store *terminalMutationStore, deps workload.Deps, blocks map[string]workload.RetryBlock, surgeIndex int32) func() (bool, error) {
	t.Helper()
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	return func() (bool, error) {
		t.Helper()
		input.ObservedState.InstanceStatuses = nil
		for _, row := range store.statuses {
			input.ObservedState.InstanceStatuses = append(input.ObservedState.InstanceStatuses, cloneTerminalStatus(row))
		}
		sort.Slice(input.ObservedState.InstanceStatuses, func(i, j int) bool {
			return input.ObservedState.InstanceStatuses[i].Index < input.ObservedState.InstanceStatuses[j].Index
		})
		input.ObservedState.RetryBlocks = nil
		for _, b := range blocks {
			input.ObservedState.RetryBlocks = append(input.ObservedState.RetryBlocks, b)
		}
		input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
			b, found := blocks[rev]
			if !found {
				b = workload.RetryBlock{TargetRevision: rev}
			}
			switch mutate(&b) {
			case workload.RetryBlockPersist:
				blocks[rev] = b
			case workload.RetryBlockRemove:
				delete(blocks, rev)
			}
			return nil
		}
		source := input.ObservedState.Instance(0)
		return abandonFailedGangSurge(context.Background(), deps, *input, plan, 0, surgeIndex,
			source.RunningRevision, source.Operation.TargetRevision, "gang surge abandoned after a corrective edit", workload.CauseUnattributed, gangSurgeEnd{})
	}
}

// TestAbandonFailedGangSurge_RejectedDeleteParksRetirementOnce: the
// retirement of a superseded gang surge deletes the replacement's members
// under the marker's index. A permanent rejection of that delete ends the
// marker as a rejected create would: Failed with the rejection on
// LastFailure, the revision in force held, one Warning; the marker keeps
// the cleanup operation that pins it to its source, and the next pass
// issues nothing while the ladder denies.
func TestAbandonFailedGangSurge_RejectedDeleteParksRetirementOnce(t *testing.T) {
	input, store, base, surgeIndex, currentRev := gangCleanupRejectionFixture(t)
	attempts := 0
	c := rejectPodDeletesCounting(t, base, siteInvalidError(), &attempts)
	recorder := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: recorder}
	blocks := map[string]workload.RetryBlock{}
	pass := gangCleanupRejectionPass(t, &input, store, deps, blocks, surgeIndex)

	if _, err := pass(); err != nil {
		t.Fatalf("abandonFailedGangSurge: %v (a permanent rejection is disposed, not returned)", err)
	}
	if attempts != 1 {
		t.Fatalf("rejected deletes: got %d want 1", attempts)
	}
	marker := store.statuses[surgeIndex]
	if marker.Phase != workload.InstancePhaseFailed {
		t.Fatalf("marker Phase: got %q want Failed", marker.Phase)
	}
	if marker.Operation == nil || marker.Operation.Step != workload.UpdateStepGangSurgeTargetCleanup {
		t.Fatalf("marker Operation: got %+v want the cleanup operation kept (it pins the marker to its source)", marker.Operation)
	}
	if marker.LastFailure == nil || marker.LastFailure.Reason != workload.RejectionReasonInvalidPodSpec {
		t.Fatalf("marker LastFailure: got %+v want reason %s", marker.LastFailure, workload.RejectionReasonInvalidPodSpec)
	}
	if b, held := blocks[currentRev]; !held || b.State != workload.RetryBlockHeld {
		t.Fatalf("RetryBlock for %s: got %+v want Held (the revision in force is blamed)", currentRev, blocks)
	}
	if n := countEventsWithReason(drainRecorderEvents(recorder), workload.EventReasonInstanceRejected); n != 1 {
		t.Fatalf("InstanceRejected events after the rejection: got %d want 1", n)
	}

	if done, err := pass(); err != nil || done {
		t.Fatalf("pass on the parked retirement: done=%v err=%v want done=false err=nil", done, err)
	}
	if attempts != 1 {
		t.Fatalf("the rejected delete was issued again: %d attempts", attempts)
	}
	if n := countEventsWithReason(drainRecorderEvents(recorder), workload.EventReasonInstanceRejected); n != 0 {
		t.Fatalf("InstanceRejected events on the pass after: got %d want 0", n)
	}
	if _, found := store.statuses[surgeIndex]; !found {
		t.Fatalf("the marker was removed while its members still exist")
	}
}

// TestAbandonFailedGangSurge_ParkedRetirementCompletesOnceMembersAreGone:
// the park guards only the writes the apiserver refused, the unroute and
// the delete over the retired members. Once no member stands there is
// nothing left to refuse, so the retirement finalizes the marker and
// resets the source as any abandon does, while the ladder still denies.
func TestAbandonFailedGangSurge_ParkedRetirementCompletesOnceMembersAreGone(t *testing.T) {
	input, store, base, surgeIndex, currentRev := gangCleanupRejectionFixture(t)
	attempts := 0
	c := rejectPodDeletesCounting(t, base, siteInvalidError(), &attempts)
	recorder := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: recorder}
	blocks := map[string]workload.RetryBlock{}
	pass := gangCleanupRejectionPass(t, &input, store, deps, blocks, surgeIndex)

	if _, err := pass(); err != nil {
		t.Fatalf("abandonFailedGangSurge: %v (a permanent rejection is disposed, not returned)", err)
	}
	if marker := store.statuses[surgeIndex]; marker.Phase != workload.InstancePhaseFailed {
		t.Fatalf("marker after the rejection: got %q want Failed", marker.Phase)
	}
	if b, held := blocks[currentRev]; !held || b.State != workload.RetryBlockHeld {
		t.Fatalf("RetryBlock for %s: got %+v want Held", currentRev, blocks)
	}

	// The retired members leave on their own while the ladder still denies.
	if err := base.DeleteAllOf(context.Background(), &corev1.Pod{}, client.InNamespace(input.Key.Namespace)); err != nil {
		t.Fatalf("remove the retired members: %v", err)
	}

	if done, err := pass(); err != nil || done {
		t.Fatalf("pass with no member standing: done=%v err=%v want done=false err=nil (an abandon always requeues)", done, err)
	}
	if attempts != 1 {
		t.Fatalf("deletes issued with no member standing: %d attempts want the one refused", attempts)
	}
	if marker, found := store.statuses[surgeIndex]; found {
		t.Fatalf("the marker was kept with no member standing: %+v", marker)
	}
	source := store.statuses[0]
	if source.Phase != workload.InstancePhaseReady || source.Operation != nil || source.TargetRevision != "" {
		t.Fatalf("source after the retirement: got %+v want Ready on its running revision with the surge cleared", source)
	}
	if source.RunningRevision != "gang-cleanup-engine-running" {
		t.Fatalf("source RunningRevision: got %q want gang-cleanup-engine-running (unchanged)", source.RunningRevision)
	}
	if b, held := blocks[currentRev]; !held || b.State != workload.RetryBlockHeld {
		t.Fatalf("RetryBlock for %s after the completion: got %+v want still Held (finishing the retirement re-arms nothing)", currentRev, blocks)
	}
}
