package ops

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

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
			Step:           updateStepSurge,
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
			status, found := store.statuses[index]
			if found && mutate(&status) {
				store.statuses[index] = status
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
	if !found || persistedSource.Operation == nil || persistedSource.Operation.Step != updateStepSurgeDrain {
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
			Step:           updateStepSurge,
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
		source.RunningRevision, targetRevision, "pod stuck", true,
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
		source.RunningRevision, targetRevision, "pod stuck", true,
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
			Step:           updateStepSurge,
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
		source.Index, surgeIndex, runningRevision, "", "", false,
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
			Step:           updateStepSurge,
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
			status, found := store.statuses[index]
			if found && mutate(&status) {
				store.statuses[index] = status
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
			Step:           updateStepSurge,
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
			Step:           updateStepSurge,
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
		runningRevision, targetRevision, instanceFailureReason(&source, "gang surge abandoned"), instanceFailureWorkloadCaused(&source),
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
// Workload-caused waves charge the ladder inside the atomic write until
// Held; environment-caused waves pace the next attempt without ever
// counting, and without a policy leave no block at all.
func TestAbandonFailedGangSurge_AtomicTailClassifiesFailureCause(t *testing.T) {
	const targetRevision = "gang-abandon-cause-engine-badrev"
	t0 := time.Now()
	policy := retryTestPolicy()
	deadline := &workload.InstanceTermination{
		Reason:  "DeadlineExceeded",
		Message: "DeadlineExceeded: Update/Surge exceeded InstanceReadyTimeout",
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
		if markRetryBlockAttemptStarted(&block) != workload.RetryBlockPersist {
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

	t.Run("environment-caused waves pace without ever holding", func(t *testing.T) {
		store := &terminalMutationStore{ownerUID: "owner-a"}
		for wave := int32(1); wave <= 2*policy.MaxAttempts; wave++ {
			warnings := atomicAbandonWave(t, store, t0, policy, deadline)
			block, found := store.retryBlock[targetRevision]
			if !found || block.State != workload.RetryBlockBackoff || block.AttemptsStarted != 0 || len(warnings) != 0 {
				t.Fatalf("wave %d: block=%+v found=%v warnings=%d want (Backoff, 0 attempts, no warning)", wave, block, found, len(warnings))
			}
			if want := t0.Add(policy.InitialDelay); block.NextRetryAt == nil || !block.NextRetryAt.Time.Equal(want) {
				t.Fatalf("wave %d: NextRetryAt got %v want %v", wave, block.NextRetryAt, want)
			}
			flipForNextWave(t, store)
		}
	})

	t.Run("environment-caused wave without a policy leaves no block", func(t *testing.T) {
		store := &terminalMutationStore{ownerUID: "owner-a"}
		warnings := atomicAbandonWave(t, store, t0, nil, deadline)
		if _, found := store.retryBlock[targetRevision]; found || len(warnings) != 0 {
			t.Fatalf("unconfigured policy must not Hold an environment fault: blocks=%v warnings=%d", store.retryBlock, len(warnings))
		}
	})
}
