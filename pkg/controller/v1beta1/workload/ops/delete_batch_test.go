package ops

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

type deleteMutationStore struct {
	uid        types.UID
	generation int64
	statuses   map[int32]workload.InstanceStatus
	writes     int
	mutations  []int
}

func newDeleteMutationStore(owner client.Object, statuses []workload.InstanceStatus) *deleteMutationStore {
	store := &deleteMutationStore{
		uid: owner.GetUID(), generation: owner.GetGeneration(),
		statuses: make(map[int32]workload.InstanceStatus, len(statuses)),
	}
	for _, row := range statuses {
		store.statuses[row.Index] = status.CloneDeleteInstanceStatus(row)
	}
	return store
}

func (s *deleteMutationStore) apply(_ context.Context, mutations []workload.InstanceMutation, _ string, _ func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
	snapshot := workload.InstanceMutationSnapshot{
		OwnerUID: s.uid, OwnerGeneration: s.generation,
		Instances: make(map[int32]workload.InstanceStatus, len(s.statuses)),
	}
	for index, row := range s.statuses {
		snapshot.Instances[index] = status.CloneDeleteInstanceStatus(row)
	}
	for _, mutation := range mutations {
		if mutation.BatchPrecondition != nil && !mutation.BatchPrecondition(snapshot) {
			return workload.ErrStatusMutationPrecondition
		}
	}
	type commit struct {
		callback func(*workload.InstanceStatus, *workload.InstanceStatus)
		before   *workload.InstanceStatus
		after    *workload.InstanceStatus
	}
	commits := make([]commit, 0, len(mutations))
	for _, mutation := range mutations {
		row, found := s.statuses[mutation.Index]
		if mutation.Remove {
			if !found {
				continue
			}
			before := status.CloneDeleteInstanceStatus(row)
			delete(s.statuses, mutation.Index)
			commits = append(commits, commit{callback: mutation.OnCommit, before: &before})
			continue
		}
		before := status.CloneDeleteInstanceStatus(row)
		if !found {
			row.Index = mutation.Index
		}
		if !mutation.Mutate(&row) {
			continue
		}
		s.statuses[mutation.Index] = status.CloneDeleteInstanceStatus(row)
		after := status.CloneDeleteInstanceStatus(row)
		var beforePtr *workload.InstanceStatus
		if found {
			beforePtr = &before
		}
		commits = append(commits, commit{callback: mutation.OnCommit, before: beforePtr, after: &after})
	}
	if len(commits) == 0 {
		return nil
	}
	s.writes++
	s.mutations = append(s.mutations, len(mutations))
	for _, committed := range commits {
		if committed.callback != nil {
			committed.callback(committed.before, committed.after)
		}
	}
	return nil
}

func TestDeleteBatchAdmissionCommitsBeforeEffects(t *testing.T) {
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady},
	}
	pods := map[int32][]*corev1.Pod{
		0: {deleteBatchPod(0)}, 1: {deleteBatchPod(1)}, 2: {deleteBatchPod(2)},
	}
	objects := []client.Object{pods[0][0], pods[1][0], pods[2][0]}
	c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(objects...).Build()
	store := newDeleteMutationStore(owner, statuses)
	budget := int32(2)
	input := deleteBatchInput(owner, statuses)
	input.ScaleDownPodBatchSize = &budget
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	result, err := DeleteBatch(context.Background(), workload.Deps{Client: c, Expectations: workload.NewExpectations()},
		input, deleteBatchPlan(), []int32{0, 1, 2}, pods)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || !result.InProgress || result.SelectedPodCost != 2 || result.Deferred != 1 {
		t.Fatalf("result = %+v", result)
	}
	if store.writes != 1 || len(store.mutations) != 1 || store.mutations[0] != 2 {
		t.Fatalf("status writes/mutations = %d/%v, want 1/[2]", store.writes, store.mutations)
	}
	if !deleteOwned(store.statuses[2]) || !deleteOwned(store.statuses[1]) || deleteOwned(store.statuses[0]) {
		t.Fatalf("admitted statuses = %+v", store.statuses)
	}
	list := &corev1.PodList{}
	if err := c.List(context.Background(), list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 3 {
		t.Fatalf("admission pass deleted Pods: got %d want 3", len(list.Items))
	}
}

func TestDeleteBatchBlockedInstanceDoesNotBlockPeer(t *testing.T) {
	owner := deleteBatchOwner()
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	statuses := []workload.InstanceStatus{deleteOwnedStatus(1, t0), deleteOwnedStatus(0, t0)}
	pod0, pod1 := deleteBatchPod(0), deleteBatchPod(1)
	pods := map[int32][]*corev1.Pod{0: {pod0}, 1: {pod1}}
	c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod0, pod1).Build()
	expectations := workload.NewExpectations()
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 1, 1)
	store := newDeleteMutationStore(owner, statuses)
	input := deleteBatchInput(owner, statuses)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	result, err := DeleteBatch(context.Background(), workload.Deps{Client: c, Expectations: expectations},
		input, deleteBatchPlan(), nil, pods)
	if err != nil {
		t.Fatal(err)
	}
	if result.ImmediateRequeue || !result.InProgress {
		t.Fatalf("result = %+v", result)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod1), &corev1.Pod{}); err != nil {
		t.Fatalf("blocked instance Pod should remain: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod0), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("eligible peer Pod should be deleted, got %v", err)
	}
}

func TestDeleteBatchCompletionFinalizesThenRemovesOnce(t *testing.T) {
	owner := deleteBatchOwner()
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	statuses := []workload.InstanceStatus{deleteOwnedStatus(3, t0), deleteOwnedStatus(2, t0)}
	store := newDeleteMutationStore(owner, statuses)
	input := deleteBatchInput(owner, statuses)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	var finalized []int32
	input.FinalizeInstanceResources = func(_ context.Context, index int32) (bool, error) {
		if store.writes != 0 {
			t.Fatalf("status removed before resource finalization")
		}
		finalized = append(finalized, index)
		return true, nil
	}
	expectations := workload.NewExpectations()
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 2, 1)
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 3, 1)

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build(), Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || !result.InProgress {
		t.Fatalf("result = %+v", result)
	}
	if len(finalized) != 2 || finalized[0] != 3 || finalized[1] != 2 {
		t.Fatalf("finalized = %v, want [3 2]", finalized)
	}
	if store.writes != 1 || len(store.mutations) != 1 || store.mutations[0] != 2 || len(store.statuses) != 0 {
		t.Fatalf("store = writes:%d mutations:%v statuses:%v", store.writes, store.mutations, store.statuses)
	}
	for _, index := range []int32{2, 3} {
		if !expectations.Satisfied("prod", "llama", workload.ComponentEngine, index) {
			t.Errorf("expectations for confirmed-absent index %d were not forgotten", index)
		}
	}
}

func TestDeleteBatchAdmissionRejectsStaleOwnerOrLifecycleIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*deleteMutationStore)
	}{
		{name: "owner UID", mutate: func(store *deleteMutationStore) { store.uid = "replacement-uid" }},
		{name: "generation", mutate: func(store *deleteMutationStore) { store.generation++ }},
		{name: "incarnation", mutate: func(store *deleteMutationStore) {
			status := store.statuses[1]
			status.Incarnation++
			store.statuses[1] = status
		}},
		{name: "phase", mutate: func(store *deleteMutationStore) {
			status := store.statuses[1]
			status.Phase = workload.InstancePhaseUpdating
			store.statuses[1] = status
		}},
		{name: "operation", mutate: func(store *deleteMutationStore) {
			status := store.statuses[1]
			status.Operation = &workload.InstanceOperation{ID: "restart-1", Type: workload.InstanceOperationRestart}
			store.statuses[1] = status
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owner := deleteBatchOwner()
			statuses := []workload.InstanceStatus{{Index: 1, Incarnation: 2, Phase: workload.InstancePhaseReady}}
			store := newDeleteMutationStore(owner, statuses)
			test.mutate(store)
			input := deleteBatchInput(owner, statuses)
			input.ApplyInstanceMutationsWithRetryBlock = store.apply
			pod := deleteBatchPod(1)
			c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod).Build()
			result, err := DeleteBatch(context.Background(), workload.Deps{Client: c}, input, deleteBatchPlan(),
				[]int32{1}, map[int32][]*corev1.Pod{1: {pod}})
			if err != nil {
				t.Fatal(err)
			}
			if !result.ImmediateRequeue || store.writes != 0 {
				t.Fatalf("result/writes = %+v/%d, want replan with zero writes", result, store.writes)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
				t.Fatalf("stale admission affected Pod: %v", err)
			}
		})
	}
}

func TestDeleteBatchAdmissionAllowsConcurrentDerivedCounterChange(t *testing.T) {
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{{Index: 1, Incarnation: 2, Phase: workload.InstancePhaseReady, PodCount: 1}}
	store := newDeleteMutationStore(owner, statuses)
	status := store.statuses[1]
	status.PodCount = 9
	status.ReadyPodCount = 8
	store.statuses[1] = status
	input := deleteBatchInput(owner, statuses)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build()
	result, err := DeleteBatch(context.Background(), workload.Deps{Client: c}, input, deleteBatchPlan(),
		[]int32{1}, map[int32][]*corev1.Pod{1: deleteSelectionPods(1, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || store.writes != 1 || !deleteOwned(store.statuses[1]) {
		t.Fatalf("result/store = %+v/%+v", result, store)
	}
	if store.statuses[1].PodCount != 9 || store.statuses[1].ReadyPodCount != 8 {
		t.Fatalf("concurrent counters were lost: %+v", store.statuses[1])
	}
}

func TestDeleteBatchCompletionRejectsWholeStaleBatchAndKeepsExpectations(t *testing.T) {
	owner := deleteBatchOwner()
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	statuses := []workload.InstanceStatus{deleteOwnedStatus(1, t0), deleteOwnedStatus(0, t0)}
	store := newDeleteMutationStore(owner, statuses)
	changed := store.statuses[0]
	changed.Operation.ID = "replacement-delete"
	store.statuses[0] = changed
	input := deleteBatchInput(owner, statuses)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	finalized := 0
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalized++
		return true, nil
	}
	expectations := workload.NewExpectations()
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 0, 1)
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 1, 1)

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build(), Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || store.writes != 0 || len(store.statuses) != 2 || finalized != 0 {
		t.Fatalf("result/store = %+v/%+v", result, store)
	}
	for _, index := range []int32{0, 1} {
		if expectations.Satisfied("prod", "llama", workload.ComponentEngine, index) {
			t.Errorf("expectations for stale index %d were forgotten before confirmed removal", index)
		}
	}
}

func TestDeleteBatchPreflightConfirmsPriorRemovalBeforeEffects(t *testing.T) {
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{deleteOwnedStatus(1, time.Now())}
	store := newDeleteMutationStore(owner, nil)
	input := deleteBatchInput(owner, statuses)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	finalized := 0
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalized++
		return true, nil
	}
	expectations := workload.NewExpectations()
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 1, 1)

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build(), Expectations: expectations,
	}, input, deleteBatchPlan(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || store.writes != 0 || finalized != 0 {
		t.Fatalf("result/writes/finalized = %+v/%d/%d", result, store.writes, finalized)
	}
	if !expectations.Satisfied("prod", "llama", workload.ComponentEngine, 1) {
		t.Fatal("authoritatively absent status did not release delete expectations")
	}
}

func TestDeleteBatchRequiresOwnerAwareAtomicAdapter(t *testing.T) {
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady}}
	pod := deleteBatchPod(1)
	client := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod).Build()

	t.Run("owner UID", func(t *testing.T) {
		input := deleteBatchInput(&corev1.ConfigMap{}, statuses)
		input.ApplyInstanceMutationsWithRetryBlock = newDeleteMutationStore(owner, statuses).apply
		if _, err := DeleteBatch(context.Background(), workload.Deps{Client: client}, input, deleteBatchPlan(), []int32{1}, map[int32][]*corev1.Pod{1: {pod}}); err == nil {
			t.Fatal("expected missing owner UID to fail closed")
		}
	})

	t.Run("strong adapter", func(t *testing.T) {
		input := deleteBatchInput(owner, statuses)
		if _, err := DeleteBatch(context.Background(), workload.Deps{Client: client}, input, deleteBatchPlan(), []int32{1}, map[int32][]*corev1.Pod{1: {pod}}); err == nil {
			t.Fatal("expected missing owner-aware atomic adapter to fail closed")
		}
	})
}

func TestDeleteBatchIncompleteResourceFinalizationRetainsStatus(t *testing.T) {
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{deleteOwnedStatus(1, time.Now())}
	store := newDeleteMutationStore(owner, statuses)
	input := deleteBatchInput(owner, statuses)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	finalized := 0
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalized++
		return false, nil
	}
	expectations := workload.NewExpectations()
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 1, 1)

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build(), Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.InProgress || result.ImmediateRequeue || finalized != 1 || store.writes != 0 || len(store.statuses) != 1 {
		t.Fatalf("result/finalized/store = %+v/%d/%+v", result, finalized, store)
	}
	if !deleteOwned(store.statuses[1]) {
		t.Fatalf("incomplete finalization changed status: %+v", store.statuses[1])
	}
	if expectations.Satisfied("prod", "llama", workload.ComponentEngine, 1) {
		t.Fatal("incomplete finalization cleared delete expectations")
	}
}

func TestDeleteBatchResourceFinalizationFailureRetainsStatus(t *testing.T) {
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{deleteOwnedStatus(1, time.Now())}
	store := newDeleteMutationStore(owner, statuses)
	input := deleteBatchInput(owner, statuses)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
		return false, errors.New("injected PodGroup delete failure")
	}
	_, err := DeleteBatch(context.Background(), workload.Deps{
		Client: fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build(),
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{})
	if err == nil || store.writes != 0 || len(store.statuses) != 1 {
		t.Fatalf("err/store = %v/%+v", err, store)
	}
}

func TestDeleteBatchAdmissionMetricsRequireConfirmedCommit(t *testing.T) {
	component := workload.ComponentType("metric-admission-commit")
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{{Index: 4, Incarnation: 2, Phase: workload.InstancePhaseReady}}
	store := newDeleteMutationStore(owner, statuses)
	input := deleteBatchInput(owner, statuses)
	input.Key.Component = component
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	plan := deleteBatchPlan()
	plan.Component = component
	budget := int32(1)
	input.ScaleDownPodBatchSize = &budget
	pods := map[int32][]*corev1.Pod{4: deleteSelectionPods(4, 2)}
	client := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build()

	startBatchCount, startBatchSum := deleteMetricHistogram(t, "ome_omenative_scale_down_batch_pods", string(component))
	startOversized := deleteMetricCounter(t, "ome_omenative_scale_down_oversized_batch_total", string(component))

	store.uid = "replacement-uid"
	result, err := DeleteBatch(context.Background(), workload.Deps{Client: client}, input, plan, []int32{4}, pods)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || store.writes != 0 {
		t.Fatalf("rejected admission result/writes = %+v/%d", result, store.writes)
	}
	assertDeleteAdmissionMetrics(t, component, startBatchCount, startBatchSum, startOversized, 0)

	store.uid = owner.GetUID()
	result, err = DeleteBatch(context.Background(), workload.Deps{Client: client}, input, plan, []int32{4}, pods)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || store.writes != 1 || !deleteOwned(store.statuses[4]) {
		t.Fatalf("committed admission result/store = %+v/%+v", result, store)
	}
	assertDeleteAdmissionMetrics(t, component, startBatchCount, startBatchSum, startOversized, 1)

	result, err = DeleteBatch(context.Background(), workload.Deps{Client: client}, input, plan, []int32{4}, pods)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || store.writes != 1 {
		t.Fatalf("stale retry result/writes = %+v/%d", result, store.writes)
	}
	assertDeleteAdmissionMetrics(t, component, startBatchCount, startBatchSum, startOversized, 1)
}

func TestDeleteBatchDurationMetricRequiresConfirmedCompletion(t *testing.T) {
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)

	t.Run("confirmed completion records once", func(t *testing.T) {
		component := workload.ComponentType("metric-completion-commit")
		owner := deleteBatchOwner()
		statuses := []workload.InstanceStatus{deleteOwnedStatus(4, now.Add(-45*time.Second))}
		store := newDeleteMutationStore(owner, statuses)
		input := deleteBatchInput(owner, statuses)
		input.Key.Component = component
		input.ApplyInstanceMutationsWithRetryBlock = store.apply
		input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) { return true, nil }
		plan := deleteBatchPlan()
		plan.Component = component
		client := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build()

		startCount, startSum := deleteMetricHistogram(t, "ome_omenative_scale_down_instance_duration_seconds", string(component))
		result, err := DeleteBatch(context.Background(), workload.Deps{Client: client}, input, plan, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !result.ImmediateRequeue || store.writes != 1 || len(store.statuses) != 0 {
			t.Fatalf("completion result/store = %+v/%+v", result, store)
		}
		assertDeleteDurationMetric(t, component, startCount, startSum, 1, 45)

		result, err = DeleteBatch(context.Background(), workload.Deps{Client: client}, input, plan, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !result.ImmediateRequeue || store.writes != 1 {
			t.Fatalf("stale completion retry result/writes = %+v/%d", result, store.writes)
		}
		assertDeleteDurationMetric(t, component, startCount, startSum, 1, 45)
	})

	t.Run("resource finalization failure records nothing", func(t *testing.T) {
		component := workload.ComponentType("metric-completion-finalizer-failure")
		owner := deleteBatchOwner()
		statuses := []workload.InstanceStatus{deleteOwnedStatus(4, now.Add(-30*time.Second))}
		store := newDeleteMutationStore(owner, statuses)
		input := deleteBatchInput(owner, statuses)
		input.Key.Component = component
		input.ApplyInstanceMutationsWithRetryBlock = store.apply
		input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
			return false, errors.New("injected finalization failure")
		}
		plan := deleteBatchPlan()
		plan.Component = component
		startCount, startSum := deleteMetricHistogram(t, "ome_omenative_scale_down_instance_duration_seconds", string(component))

		_, err := DeleteBatch(context.Background(), workload.Deps{
			Client: fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build(),
		}, input, plan, nil, nil)
		if err == nil || store.writes != 0 {
			t.Fatalf("finalization err/writes = %v/%d", err, store.writes)
		}
		assertDeleteDurationMetric(t, component, startCount, startSum, 0, 0)
	})

	t.Run("status write failure records nothing", func(t *testing.T) {
		component := workload.ComponentType("metric-completion-status-failure")
		owner := deleteBatchOwner()
		statuses := []workload.InstanceStatus{deleteOwnedStatus(4, now.Add(-30*time.Second))}
		input := deleteBatchInput(owner, statuses)
		input.Key.Component = component
		input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) { return true, nil }
		input.ApplyInstanceMutationsWithRetryBlock = func(context.Context, []workload.InstanceMutation, string, func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
			return errors.New("injected status write failure")
		}
		plan := deleteBatchPlan()
		plan.Component = component
		startCount, startSum := deleteMetricHistogram(t, "ome_omenative_scale_down_instance_duration_seconds", string(component))

		_, err := DeleteBatch(context.Background(), workload.Deps{
			Client: fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build(),
		}, input, plan, nil, nil)
		if err == nil {
			t.Fatal("expected status write failure")
		}
		assertDeleteDurationMetric(t, component, startCount, startSum, 0, 0)
	})

	t.Run("unconfirmed status no-op records nothing", func(t *testing.T) {
		component := workload.ComponentType("metric-completion-unconfirmed")
		owner := deleteBatchOwner()
		statuses := []workload.InstanceStatus{deleteOwnedStatus(4, now.Add(-30*time.Second))}
		input := deleteBatchInput(owner, statuses)
		input.Key.Component = component
		input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) { return true, nil }
		input.ApplyInstanceMutationsWithRetryBlock = func(context.Context, []workload.InstanceMutation, string, func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
			return nil
		}
		plan := deleteBatchPlan()
		plan.Component = component
		startCount, startSum := deleteMetricHistogram(t, "ome_omenative_scale_down_instance_duration_seconds", string(component))

		_, err := DeleteBatch(context.Background(), workload.Deps{
			Client: fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build(),
		}, input, plan, nil, nil)
		if err == nil {
			t.Fatalf("expected unconfirmed adapter error, got %v", err)
		}
		assertDeleteDurationMetric(t, component, startCount, startSum, 0, 0)
	})
}

func assertDeleteAdmissionMetrics(t *testing.T, component workload.ComponentType, startCount uint64, startSum, startOversized float64, committed uint64) {
	t.Helper()
	count, sum := deleteMetricHistogram(t, "ome_omenative_scale_down_batch_pods", string(component))
	if count-startCount != committed || sum-startSum != float64(committed*2) {
		t.Fatalf("batch metric delta = count:%d sum:%g, want count:%d sum:%d", count-startCount, sum-startSum, committed, committed*2)
	}
	oversized := deleteMetricCounter(t, "ome_omenative_scale_down_oversized_batch_total", string(component))
	if oversized-startOversized != float64(committed) {
		t.Fatalf("oversized metric delta = %g, want %d", oversized-startOversized, committed)
	}
}

func assertDeleteDurationMetric(t *testing.T, component workload.ComponentType, startCount uint64, startSum float64, committed uint64, seconds float64) {
	t.Helper()
	count, sum := deleteMetricHistogram(t, "ome_omenative_scale_down_instance_duration_seconds", string(component))
	if count-startCount != committed || sum-startSum != seconds {
		t.Fatalf("duration metric delta = count:%d sum:%g, want count:%d sum:%g", count-startCount, sum-startSum, committed, seconds)
	}
}

func deleteMetricHistogram(t *testing.T, name, component string) (uint64, float64) {
	t.Helper()
	metric := deleteMetricForComponent(t, name, component)
	if metric == nil || metric.Histogram == nil {
		return 0, 0
	}
	return metric.Histogram.GetSampleCount(), metric.Histogram.GetSampleSum()
}

func deleteMetricCounter(t *testing.T, name, component string) float64 {
	t.Helper()
	metric := deleteMetricForComponent(t, name, component)
	if metric == nil || metric.Counter == nil {
		return 0
	}
	return metric.Counter.GetValue()
}

func deleteMetricForComponent(t *testing.T, name, component string) *dto.Metric {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := metric.GetLabel()
			if len(labels) == 1 && labels[0].GetName() == "component" && labels[0].GetValue() == component {
				return metric
			}
		}
	}
	return nil
}

func deleteBatchScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func deleteBatchOwner() *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "ir", Namespace: "prod", UID: types.UID("ir-uid"), Generation: 7,
	}}
}

func deleteBatchInput(owner client.Object, statuses []workload.InstanceStatus) workload.ReconcileInput {
	clock := clocktesting.NewFakeClock(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	return workload.ReconcileInput{
		OwnerObject:   owner,
		Key:           workload.Key{Namespace: "prod", OwnerName: "llama", Component: workload.ComponentEngine},
		ObservedState: workload.WorkloadObservedState{InstanceStatuses: statuses},
		Clock:         clock,
	}
}

func deleteBatchPlan() workload.ComponentPlan {
	return workload.ComponentPlan{Component: workload.ComponentEngine, InstanceReadyTimeout: time.Minute}
}

func deleteBatchPod(index int32) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "pod-" + string(rune('a'+index)), Namespace: "prod", UID: types.UID(fmt.Sprintf("pod-%d-uid", index)),
		Labels: map[string]string{query.LabelInstanceIdx: string(rune('0' + index))},
	}}
}

type deleteFailureClient struct {
	client.Client
	statusPatchErrorPod string
	statusPatchErr      error
	deleteHook          func(context.Context, client.Object, ...client.DeleteOption) error
	deleteCalls         []string
}

func (c *deleteFailureClient) Status() client.SubResourceWriter {
	return &deleteFailureStatusWriter{
		SubResourceWriter: c.Client.Status(),
		podName:           c.statusPatchErrorPod,
		err:               c.statusPatchErr,
	}
}

func (c *deleteFailureClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		c.deleteCalls = append(c.deleteCalls, obj.GetName())
	}
	if c.deleteHook != nil {
		return c.deleteHook(ctx, obj, opts...)
	}
	return c.Client.Delete(ctx, obj, opts...)
}

type deleteFailureStatusWriter struct {
	client.SubResourceWriter
	podName string
	err     error
}

func (w *deleteFailureStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && pod.Name == w.podName {
		return w.err
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

type deleteFailureReader struct {
	client.Reader
	endpointSliceListErr error
	serviceGetErr        error
	endpointSliceLists   int
	serviceGets          int
}

func (r *deleteFailureReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*discoveryv1.EndpointSliceList); ok {
		r.endpointSliceLists++
		if r.endpointSliceListErr != nil {
			return r.endpointSliceListErr
		}
	}
	return r.Reader.List(ctx, list, opts...)
}

func (r *deleteFailureReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Service); ok {
		r.serviceGets++
		if r.serviceGetErr != nil {
			return r.serviceGetErr
		}
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func newDeleteFailureBaseClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}).
		WithObjects(objects...).
		Build()
}

func deleteFailurePod(index int32, serving bool, revision string) *corev1.Pod {
	pod := deleteBatchPod(index)
	pod.UID = types.UID(fmt.Sprintf("pod-%d-uid", index))
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "example.com/model:latest"}}
	if revision != "" {
		pod.Labels[query.LabelRevisionHash] = revision
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:   podreadiness.ConditionType,
			Status: corev1.ConditionTrue,
		})
	}
	return pod
}

func deleteFailureSlice(namespace, name, service string, endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    map[string]string{discoveryv1.LabelServiceName: service},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   endpoints,
	}
}

func deleteFailureEndpoint(pod *corev1.Pod, ready bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: []string{"192.0.2.1"},
		Conditions: discoveryv1.EndpointConditions{
			Ready: ptr.To(ready),
		},
		TargetRef: &corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: pod.Namespace,
			Name:      pod.Name,
		},
	}
}

func TestDeleteBatchAdmissionAPIErrorsFailBeforeExternalEffects(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "413", err: apierrors.NewRequestEntityTooLargeError("status object too large")},
		{name: "429", err: apierrors.NewTooManyRequests("status write throttled", 1)},
		{name: "5xx", err: apierrors.NewServiceUnavailable("status storage unavailable")},
		{name: "context cancellation", err: context.Canceled},
		{name: "generic write error", err: errors.New("injected status write failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := deleteBatchOwner()
			statuses := []workload.InstanceStatus{{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady}}
			pod := deleteFailurePod(0, true, "rev-a")
			base := newDeleteFailureBaseClient(t, pod)
			c := &deleteFailureClient{Client: base}
			reads := &deleteFailureReader{Reader: base}
			finalized := 0
			adapterCalls := 0
			input := deleteBatchInput(owner, statuses)
			input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
				finalized++
				return true, nil
			}
			input.ApplyInstanceMutationsWithRetryBlock = func(context.Context, []workload.InstanceMutation, string, func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
				adapterCalls++
				return test.err
			}

			result, err := DeleteBatch(context.Background(), workload.Deps{
				Client: c, APIReader: reads, Expectations: workload.NewExpectations(),
			}, input, deleteBatchPlan(), []int32{0}, map[int32][]*corev1.Pod{0: {pod}})
			if err == nil || !errors.Is(err, test.err) {
				t.Fatalf("result/error = %+v/%v, want wrapped %v", result, err, test.err)
			}
			if adapterCalls != 1 || finalized != 0 || len(c.deleteCalls) != 0 {
				t.Fatalf("calls = adapter:%d finalizer:%d deletes:%v", adapterCalls, finalized, c.deleteCalls)
			}
			if reads.endpointSliceLists != 0 || reads.serviceGets != 0 {
				t.Fatalf("failed admission reached drain reads: slices=%d services=%d", reads.endpointSliceLists, reads.serviceGets)
			}
			got := &corev1.Pod{}
			if err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
				t.Fatalf("failed admission removed Pod: %v", err)
			}
			if !podreadiness.IsServing(got) {
				t.Fatalf("failed admission changed readiness: %+v", got.Status.Conditions)
			}
		})
	}
}

func TestDeleteBatchAdmissionOwnerGoneReplansBeforeExternalEffects(t *testing.T) {
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady}}
	pod := deleteFailurePod(0, true, "rev-a")
	base := newDeleteFailureBaseClient(t, pod)
	c := &deleteFailureClient{Client: base}
	reads := &deleteFailureReader{Reader: base}
	input := deleteBatchInput(owner, statuses)
	input.ApplyInstanceMutationsWithRetryBlock = func(context.Context, []workload.InstanceMutation, string, func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		return workload.ErrStatusOwnerGone
	}

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: c, APIReader: reads, Expectations: workload.NewExpectations(),
	}, input, deleteBatchPlan(), []int32{0}, map[int32][]*corev1.Pod{0: {pod}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || len(c.deleteCalls) != 0 || reads.endpointSliceLists != 0 || reads.serviceGets != 0 {
		t.Fatalf("owner-gone result/effects = %+v deletes:%v reads:%d/%d",
			result, c.deleteCalls, reads.endpointSliceLists, reads.serviceGets)
	}
	got := &corev1.Pod{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil || !podreadiness.IsServing(got) {
		t.Fatalf("owner-gone admission changed Pod: err=%v conditions=%+v", err, got.Status.Conditions)
	}
}

func TestDeleteBatchReadinessFailureStopsWholeWaveBeforeDrainOrDelete(t *testing.T) {
	for _, test := range []struct {
		name      string
		failedPod int32
	}{
		{name: "first Pod", failedPod: 2},
		{name: "middle Pod", failedPod: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := deleteBatchOwner()
			started := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
			statuses := []workload.InstanceStatus{
				deleteOwnedStatus(2, started),
				deleteOwnedStatus(1, started),
				deleteOwnedStatus(0, started),
			}
			pods := map[int32][]*corev1.Pod{}
			objects := make([]client.Object, 0, len(statuses))
			for _, status := range statuses {
				pod := deleteFailurePod(status.Index, true, "rev-a")
				pods[status.Index] = []*corev1.Pod{pod}
				objects = append(objects, pod)
			}
			base := newDeleteFailureBaseClient(t, objects...)
			c := &deleteFailureClient{
				Client:              base,
				statusPatchErrorPod: pods[test.failedPod][0].Name,
				statusPatchErr:      errors.New("injected serving-gate patch failure"),
			}
			reads := &deleteFailureReader{Reader: base}
			store := newDeleteMutationStore(owner, statuses)
			input := deleteBatchInput(owner, statuses)
			input.ApplyInstanceMutationsWithRetryBlock = store.apply

			_, err := DeleteBatch(context.Background(), workload.Deps{
				Client: c, APIReader: reads, Expectations: workload.NewExpectations(),
			}, input, deleteBatchPlan(), nil, pods)
			if err == nil || !strings.Contains(err.Error(), "mark not serving") {
				t.Fatalf("error = %v, want readiness failure", err)
			}
			if len(c.deleteCalls) != 0 {
				t.Fatalf("readiness failure issued Pod deletes: %v", c.deleteCalls)
			}
			if reads.endpointSliceLists != 0 || reads.serviceGets != 0 {
				t.Fatalf("readiness failure reached drain reads: slices=%d services=%d", reads.endpointSliceLists, reads.serviceGets)
			}
		})
	}
}

func TestDeleteBatchDrainReadFailureStopsBeforeDelete(t *testing.T) {
	for _, test := range []struct {
		name        string
		configure   func(*deleteFailureReader)
		wantMessage string
		wantLists   int
		wantGets    int
	}{
		{
			name: "EndpointSlice list",
			configure: func(reader *deleteFailureReader) {
				reader.endpointSliceListErr = errors.New("injected EndpointSlice read failure")
			},
			wantMessage: "list EndpointSlices",
			wantLists:   1,
		},
		{
			name: "empty-slice Service lookup",
			configure: func(reader *deleteFailureReader) {
				reader.serviceGetErr = errors.New("injected Service read failure")
			},
			wantMessage: "get service",
			wantLists:   1,
			wantGets:    1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := deleteBatchOwner()
			statuses := []workload.InstanceStatus{deleteOwnedStatus(0, time.Now())}
			pod := deleteFailurePod(0, false, "rev-a")
			base := newDeleteFailureBaseClient(t, pod)
			c := &deleteFailureClient{Client: base}
			reads := &deleteFailureReader{Reader: base}
			test.configure(reads)
			store := newDeleteMutationStore(owner, statuses)
			input := deleteBatchInput(owner, statuses)
			input.ApplyInstanceMutationsWithRetryBlock = store.apply

			_, err := DeleteBatch(context.Background(), workload.Deps{
				Client: c, APIReader: reads, Expectations: workload.NewExpectations(),
			}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {pod}})
			if err == nil || !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("error = %v, want %q", err, test.wantMessage)
			}
			if len(c.deleteCalls) != 0 {
				t.Fatalf("drain read failure issued Pod deletes: %v", c.deleteCalls)
			}
			if reads.endpointSliceLists != test.wantLists || reads.serviceGets != test.wantGets {
				t.Fatalf("drain reads = slices:%d services:%d, want %d/%d",
					reads.endpointSliceLists, reads.serviceGets, test.wantLists, test.wantGets)
			}
		})
	}
}

func TestDeleteBatchDeferredMembersDoNotBlockEligiblePeer(t *testing.T) {
	owner := deleteBatchOwner()
	started := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	statuses := []workload.InstanceStatus{
		deleteOwnedStatus(2, started),
		deleteOwnedStatus(1, started),
		deleteOwnedStatus(0, started),
	}
	pod2 := deleteFailurePod(2, false, "rev-a")
	pod1 := deleteFailurePod(1, false, "rev-a")
	pod0 := deleteFailurePod(0, false, "rev-a")
	service := query.PerRevisionServiceName("llama", workload.ComponentEngine, "rev-a")
	slice := deleteFailureSlice("prod", "rev-a-slice", service,
		deleteFailureEndpoint(pod2, true),
		deleteFailureEndpoint(pod1, false),
		deleteFailureEndpoint(pod0, false),
	)
	base := newDeleteFailureBaseClient(t, pod2, pod1, pod0, slice)
	c := &deleteFailureClient{Client: base}
	reads := &deleteFailureReader{Reader: base}
	expectations := workload.NewExpectations()
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 1, 1)
	store := newDeleteMutationStore(owner, statuses)
	input := deleteBatchInput(owner, statuses)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: c, APIReader: reads, Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{
		2: {pod2}, 1: {pod1}, 0: {pod0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.InProgress || result.ImmediateRequeue {
		t.Fatalf("result = %+v", result)
	}
	for _, pod := range []*corev1.Pod{pod2, pod1} {
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
			t.Fatalf("deferred Pod %s was deleted: %v", pod.Name, err)
		}
	}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pod0), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("eligible peer Pod was not deleted: %v", err)
	}
	if len(c.deleteCalls) != 1 || c.deleteCalls[0] != pod0.Name {
		t.Fatalf("delete calls = %v, want [%s]", c.deleteCalls, pod0.Name)
	}
	if reads.endpointSliceLists != 1 {
		t.Fatalf("EndpointSlice lists = %d, want one shared read", reads.endpointSliceLists)
	}
}

func TestDeleteBatchDeleteErrorsRollbackOnlyFailedExpectations(t *testing.T) {
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{deleteOwnedStatus(0, time.Now())}

	t.Run("NotFound", func(t *testing.T) {
		pod := deleteFailurePod(0, false, "")
		base := newDeleteFailureBaseClient(t)
		c := &deleteFailureClient{Client: base}
		c.deleteHook = func(context.Context, client.Object, ...client.DeleteOption) error {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, pod.Name)
		}
		expectations := workload.NewExpectations()
		store := newDeleteMutationStore(owner, statuses)
		input := deleteBatchInput(owner, statuses)
		input.ApplyInstanceMutationsWithRetryBlock = store.apply

		if _, err := DeleteBatch(context.Background(), workload.Deps{Client: c, Expectations: expectations},
			input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {pod}}); err != nil {
			t.Fatal(err)
		}
		if !expectations.Satisfied("prod", "llama", workload.ComponentEngine, 0) {
			t.Fatal("NotFound left a delete expectation that no watch event can satisfy")
		}
	})

	t.Run("transport failure", func(t *testing.T) {
		pod := deleteFailurePod(0, false, "")
		base := newDeleteFailureBaseClient(t)
		c := &deleteFailureClient{Client: base}
		c.deleteHook = func(context.Context, client.Object, ...client.DeleteOption) error {
			return errors.New("injected transport failure")
		}
		expectations := workload.NewExpectations()
		store := newDeleteMutationStore(owner, statuses)
		input := deleteBatchInput(owner, statuses)
		input.ApplyInstanceMutationsWithRetryBlock = store.apply

		_, err := DeleteBatch(context.Background(), workload.Deps{Client: c, Expectations: expectations},
			input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {pod}})
		if err == nil || !strings.Contains(err.Error(), "transport failure") {
			t.Fatalf("error = %v", err)
		}
		if !expectations.Satisfied("prod", "llama", workload.ComponentEngine, 0) {
			t.Fatal("failed Delete RPC left a phantom expectation")
		}
	})

	t.Run("partial success resumes across controller restarts", func(t *testing.T) {
		podA := deleteFailurePod(0, false, "")
		podA.Name = "pod-a"
		podB := deleteFailurePod(0, false, "")
		podB.Name = "pod-b"
		base := newDeleteFailureBaseClient(t, podA, podB)
		failSecond := true
		c := &deleteFailureClient{Client: base}
		c.deleteHook = func(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == podB.Name && failSecond {
				failSecond = false
				return errors.New("injected second-delete failure")
			}
			return base.Delete(ctx, obj, opts...)
		}
		firstExpectations := workload.NewExpectations()
		store := newDeleteMutationStore(owner, statuses)
		input := deleteBatchInput(owner, statuses)
		input.ApplyInstanceMutationsWithRetryBlock = store.apply

		_, err := DeleteBatch(context.Background(), workload.Deps{Client: c, Expectations: firstExpectations},
			input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {podA, podB}})
		if err == nil || !strings.Contains(err.Error(), "second-delete failure") {
			t.Fatalf("first pass error = %v", err)
		}
		if firstExpectations.Satisfied("prod", "llama", workload.ComponentEngine, 0) {
			t.Fatal("successful first delete expectation was rolled back with the failed peer")
		}
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(podA), &corev1.Pod{}); !apierrors.IsNotFound(err) {
			t.Fatalf("first Pod should be gone: %v", err)
		}
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(podB), &corev1.Pod{}); err != nil {
			t.Fatalf("failed second Pod delete should leave Pod: %v", err)
		}

		resumedExpectations := workload.NewExpectations()
		if _, err := DeleteBatch(context.Background(), workload.Deps{Client: c, Expectations: resumedExpectations},
			input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {podB}}); err != nil {
			t.Fatalf("post-restart resume pass: %v", err)
		}
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(podB), &corev1.Pod{}); !apierrors.IsNotFound(err) {
			t.Fatalf("post-restart resume pass did not delete remaining Pod: %v", err)
		}

		completionExpectations := workload.NewExpectations()
		finalized := 0
		input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
			finalized++
			return true, nil
		}
		result, err := DeleteBatch(context.Background(), workload.Deps{Client: c, Expectations: completionExpectations},
			input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{})
		if err != nil {
			t.Fatalf("post-restart completion pass: %v", err)
		}
		if !result.ImmediateRequeue || finalized != 1 || len(store.statuses) != 0 {
			t.Fatalf("completion result/finalized/statuses = %+v/%d/%v", result, finalized, store.statuses)
		}
	})
}

func TestDeleteBatchUnconfirmedCompletionKeepsExpectations(t *testing.T) {
	owner := deleteBatchOwner()
	statuses := []workload.InstanceStatus{deleteOwnedStatus(0, time.Now())}
	expectations := workload.NewExpectations()
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 0, 1)
	completionErr := errors.New("injected completion status failure")
	input := deleteBatchInput(owner, statuses)
	input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) { return true, nil }
	input.ApplyInstanceMutationsWithRetryBlock = func(_ context.Context, mutations []workload.InstanceMutation, _ string, _ func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		for _, mutation := range mutations {
			if mutation.Remove {
				return completionErr
			}
		}
		return nil
	}

	_, err := DeleteBatch(context.Background(), workload.Deps{
		Client: newDeleteFailureBaseClient(t), Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{})
	if err == nil || !errors.Is(err, completionErr) {
		t.Fatalf("completion error = %v, want %v", err, completionErr)
	}
	if expectations.Satisfied("prod", "llama", workload.ComponentEngine, 0) {
		t.Fatal("unconfirmed status removal cleared delete expectations")
	}
}

func TestCompleteDeleteBatchRejectsConcurrentPhaseDrift(t *testing.T) {
	owner := deleteBatchOwner()
	expected := deleteOwnedStatus(0, time.Now())
	current := status.CloneDeleteInstanceStatus(expected)
	current.Phase = workload.InstancePhaseFailed
	store := newDeleteMutationStore(owner, []workload.InstanceStatus{current})
	expectations := workload.NewExpectations()
	expectations.ExpectDeletes("prod", "llama", workload.ComponentEngine, 0, 1)
	input := deleteBatchInput(owner, []workload.InstanceStatus{expected})
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	committed, err := completeDeleteBatch(context.Background(), workload.Deps{Expectations: expectations}, input,
		[]deleteBatchCandidate{{status: expected}})
	if !errors.Is(err, workload.ErrStatusMutationPrecondition) {
		t.Fatalf("completion error = %v, want %v", err, workload.ErrStatusMutationPrecondition)
	}
	if committed {
		t.Fatal("completion committed after the Instance left Phase=Deleting")
	}
	if got := store.statuses[0]; got.Phase != workload.InstancePhaseFailed {
		t.Fatalf("concurrent status = %+v, want Phase=Failed retained", got)
	}
	if expectations.Satisfied("prod", "llama", workload.ComponentEngine, 0) {
		t.Fatal("rejected completion cleared delete expectations")
	}
}

func TestDeleteBatchStuckTerminatingEscalatesBeforeExpectationGate(t *testing.T) {
	isvc := fdISVC("batch-force-delete")
	statuses := []workload.InstanceStatus{deleteOwnedStatus(0, fdNow.Add(-20*time.Minute))}
	pod := fdTerminatingPod("batch-force-delete-pod", "dead-node", overdueTS)
	pod.Labels = map[string]string{query.LabelInstanceIdx: "0"}
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, isvc, fdStoredCopy(pod))
	store := newDeleteMutationStore(isvc, statuses)
	input := deleteBatchInput(isvc, statuses)
	input.OwnerGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")
	input.EventTarget = isvc
	input.ForceDelete = fdPolicy()
	input.Clock = clocktesting.NewFakeClock(fdNow)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	expectations := workload.NewExpectations()
	expectations.ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, 0, 1)
	recorder := record.NewFakeRecorder(8)

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: c, APIReader: c, Expectations: expectations, Recorder: recorder,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {pod}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.InProgress || result.ImmediateRequeue {
		t.Fatalf("result = %+v", result)
	}
	if len(deletes) != 1 || deletes[0].grace == nil || *deletes[0].grace != 0 ||
		deletes[0].uid == nil || *deletes[0].uid != pod.UID {
		t.Fatalf("force deletes = %+v, want one grace-0 UID-preconditioned delete", deletes)
	}
	if expectations.Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, 0) {
		t.Fatal("escalation unexpectedly cleared the pre-existing graceful-delete expectation")
	}
	if got := fdCountEvents(fdDrainEvents(recorder), workload.EventReasonPodForceDeleted); got != 1 {
		t.Fatalf("force-delete events = %d, want 1", got)
	}
}

func TestDeleteBatchWithoutPollReturnsForceDeleteEvidenceReadError(t *testing.T) {
	owner := deleteBatchOwner()
	status := deleteOwnedStatus(0, fdNow.Add(-20*time.Minute))
	pod := fdTerminatingPod("batch-node-read-error", "unreadable-node", overdueTS)
	pod.Labels = map[string]string{query.LabelInstanceIdx: "0"}
	base := newDeleteFailureBaseClient(t)
	reader := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Node); ok {
				return errors.New("injected Node read failure")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	store := newDeleteMutationStore(owner, []workload.InstanceStatus{status})
	input := deleteBatchInput(owner, []workload.InstanceStatus{status})
	input.ForceDelete = fdPolicy()
	input.Clock = clocktesting.NewFakeClock(fdNow)
	input.ScaleDownRequeueInterval = 0
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	_, err := DeleteBatch(context.Background(), workload.Deps{
		Client: base, APIReader: reader, Expectations: workload.NewExpectations(),
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {pod}})
	if err == nil || !strings.Contains(err.Error(), "injected Node read failure") {
		t.Fatalf("DeleteBatch error = %v, want force-delete evidence read failure", err)
	}
	if store.writes != 0 || store.statuses[0].Phase != workload.InstancePhaseDeleting {
		t.Fatalf("evidence read failure changed status: writes=%d status=%+v", store.writes, store.statuses[0])
	}
}

func TestDeleteBatchHighScaleBudgetBoundsEffectsAndUsesOneDrainObservation(t *testing.T) {
	const replicas = int32(2000)
	const budget = int32(100)
	owner := deleteBatchOwner()
	started := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	statuses := make([]workload.InstanceStatus, 0, replicas)
	pods := make(map[int32][]*corev1.Pod, replicas)
	endpoints := make([]discoveryv1.Endpoint, 0, replicas)
	for index := int32(0); index < replicas; index++ {
		statuses = append(statuses, deleteOwnedStatus(index, started))
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod",
			Name:      fmt.Sprintf("pod-%04d", index),
			UID:       types.UID(fmt.Sprintf("pod-%04d-uid", index)),
			Labels:    map[string]string{query.LabelRevisionHash: "rev-scale"},
		}}
		pods[index] = []*corev1.Pod{pod}
		endpoints = append(endpoints, deleteFailureEndpoint(pod, false))
	}
	service := query.PerRevisionServiceName("llama", workload.ComponentEngine, "rev-scale")
	slice := deleteFailureSlice("prod", "rev-scale-slice", service, endpoints...)
	base := newDeleteFailureBaseClient(t, slice)
	c := &deleteFailureClient{Client: base}
	c.deleteHook = func(context.Context, client.Object, ...client.DeleteOption) error { return nil }
	reads := &deleteFailureReader{Reader: base}
	store := newDeleteMutationStore(owner, statuses)
	input := deleteBatchInput(owner, statuses)
	input.ScaleDownPodBatchSize = ptr.To(budget)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: c, APIReader: reads, Expectations: workload.NewExpectations(),
	}, input, deleteBatchPlan(), nil, pods)
	if err != nil {
		t.Fatal(err)
	}
	if result.SelectedPodCost != budget || result.Deferred != int(replicas-budget) || !result.InProgress {
		t.Fatalf("result = %+v", result)
	}
	if reads.endpointSliceLists != 1 || reads.serviceGets != 0 {
		t.Fatalf("drain reads = slices:%d services:%d, want 1/0", reads.endpointSliceLists, reads.serviceGets)
	}
	if got := len(c.deleteCalls); got != int(budget) {
		t.Fatalf("Pod delete calls = %d, want %d", got, budget)
	}
	for index := int32(0); index < replicas-budget; index++ {
		if slices.Contains(c.deleteCalls, fmt.Sprintf("pod-%04d", index)) {
			t.Fatalf("deferred Pod %04d received a delete effect", index)
		}
	}
	if store.writes != 0 {
		t.Fatalf("resuming an owned wave wrote status %d times", store.writes)
	}
}

func TestDeleteBatchHighScaleFreshAdmissionWritesOnceBeforeEffects(t *testing.T) {
	const replicas = int32(2000)
	const budget = int32(100)
	owner := deleteBatchOwner()
	statuses := make([]workload.InstanceStatus, 0, replicas)
	extras := make([]int32, 0, replicas-1)
	pods := make(map[int32][]*corev1.Pod, replicas)
	for index := int32(0); index < replicas; index++ {
		statuses = append(statuses, workload.InstanceStatus{Index: index, Incarnation: 1, Phase: workload.InstancePhaseReady})
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: fmt.Sprintf("pod-%04d", index)}}
		pods[index] = []*corev1.Pod{pod}
		if index > 0 {
			extras = append(extras, index)
		}
	}
	base := newDeleteFailureBaseClient(t)
	c := &deleteFailureClient{Client: base}
	reads := &deleteFailureReader{Reader: base}
	store := newDeleteMutationStore(owner, statuses)
	input := deleteBatchInput(owner, statuses)
	input.ScaleDownPodBatchSize = ptr.To(budget)
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: c, APIReader: reads, Expectations: workload.NewExpectations(),
	}, input, deleteBatchPlan(), extras, pods)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ImmediateRequeue || result.SelectedPodCost != budget || result.Deferred != 1899 {
		t.Fatalf("result = %+v", result)
	}
	if store.writes != 1 || len(store.mutations) != 1 || store.mutations[0] != int(budget) {
		t.Fatalf("status commits/mutations = %d/%v, want one 100-Instance admission", store.writes, store.mutations)
	}
	for index := int32(0); index < replicas; index++ {
		status := store.statuses[index]
		selected := index >= replicas-budget
		if selected && (status.Phase != workload.InstancePhaseDeleting || status.Operation == nil || status.Operation.Type != workload.InstanceOperationDelete) {
			t.Fatalf("selected Instance %d was not admitted: %+v", index, status)
		}
		if !selected && (status.Phase != workload.InstancePhaseReady || status.Operation != nil) {
			t.Fatalf("retained/deferred Instance %d was mutated: %+v", index, status)
		}
	}
	if len(c.deleteCalls) != 0 || reads.endpointSliceLists != 0 || reads.serviceGets != 0 {
		t.Fatalf("admission external effects: deletes=%d slices=%d services=%d", len(c.deleteCalls), reads.endpointSliceLists, reads.serviceGets)
	}
}

func TestSelectDeleteBatchFreshGangAwarePrefix(t *testing.T) {
	tests := []struct {
		name      string
		indices   []int32
		costs     map[int32]int
		budget    *int32
		want      []int32
		deferred  int
		oversized bool
		wantCost  int32
	}{
		{name: "unbounded", indices: []int32{0, 1, 2}, costs: map[int32]int{0: 1, 1: 1, 2: 1}, want: []int32{2, 1, 0}, wantCost: 3},
		{name: "exact", indices: []int32{0, 1, 2}, costs: map[int32]int{0: 2, 1: 3, 2: 5}, budget: int32Pointer(8), want: []int32{2, 1}, deferred: 1, wantCost: 8},
		{name: "under", indices: []int32{0, 1}, costs: map[int32]int{0: 2, 1: 3}, budget: int32Pointer(8), want: []int32{1, 0}, wantCost: 5},
		{name: "oversized first", indices: []int32{0, 1}, costs: map[int32]int{0: 1, 1: 9}, budget: int32Pointer(8), want: []int32{1}, deferred: 1, oversized: true, wantCost: 9},
		{name: "non fitting gang closes prefix", indices: []int32{0, 1, 2}, costs: map[int32]int{0: 1, 1: 8, 2: 4}, budget: int32Pointer(5), want: []int32{2}, deferred: 2, wantCost: 4},
		{name: "podless costs one", indices: []int32{0, 1, 2}, costs: map[int32]int{}, budget: int32Pointer(2), want: []int32{2, 1}, deferred: 1, wantCost: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statuses := make([]workload.InstanceStatus, 0, len(test.indices))
			pods := make(map[int32][]*corev1.Pod, len(test.costs))
			for _, index := range test.indices {
				statuses = append(statuses, workload.InstanceStatus{Index: index, Incarnation: 1, Phase: workload.InstancePhaseReady})
				pods[index] = deleteSelectionPods(index, test.costs[index])
			}
			selection, err := selectDeleteBatch(statuses, test.indices, pods, test.budget)
			if err != nil {
				t.Fatalf("selectDeleteBatch: %v", err)
			}
			if got := selectedDeleteIndices(selection); !equalInt32Slices(got, test.want) {
				t.Fatalf("selected indices = %v, want %v", got, test.want)
			}
			if selection.deferred != test.deferred {
				t.Errorf("deferred = %d, want %d", selection.deferred, test.deferred)
			}
			if selection.oversized != test.oversized {
				t.Errorf("oversized = %t, want %t", selection.oversized, test.oversized)
			}
			var cost int32
			for _, candidate := range selection.candidates {
				cost += candidate.cost
			}
			if cost != test.wantCost {
				t.Errorf("selected cost = %d, want %d", cost, test.wantCost)
			}
		})
	}
}

func TestSelectDeleteBatchEightPodGangs(t *testing.T) {
	const instances = 20
	statuses := make([]workload.InstanceStatus, 0, instances)
	extras := make([]int32, 0, instances)
	pods := make(map[int32][]*corev1.Pod, instances)
	for index := int32(0); index < instances; index++ {
		statuses = append(statuses, workload.InstanceStatus{Index: index, Phase: workload.InstancePhaseReady})
		extras = append(extras, index)
		pods[index] = deleteSelectionPods(index, 8)
	}
	selection, err := selectDeleteBatch(statuses, extras, pods, int32Pointer(100))
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.candidates) != 12 || selection.deferred != 8 {
		t.Fatalf("selected/deferred = %d/%d, want 12/8", len(selection.candidates), selection.deferred)
	}
	var cost int32
	for _, candidate := range selection.candidates {
		cost += candidate.cost
	}
	if cost != 96 {
		t.Fatalf("cost = %d, want 96", cost)
	}
}

func TestSelectDeleteBatchDeleteOwnedOrderingAndFreshClosure(t *testing.T) {
	t0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	statuses := []workload.InstanceStatus{
		{Index: 9, Phase: workload.InstancePhaseReady},
		deleteOwnedStatus(2, t0.Add(time.Minute)),
		deleteOwnedStatus(7, t0),
		deleteOwnedStatus(5, t0),
	}
	pods := map[int32][]*corev1.Pod{
		2: deleteSelectionPods(2, 2),
		7: deleteSelectionPods(7, 2),
		5: deleteSelectionPods(5, 2),
		9: deleteSelectionPods(9, 1),
	}
	selection, err := selectDeleteBatch(statuses, []int32{9}, pods, int32Pointer(4))
	if err != nil {
		t.Fatal(err)
	}
	if selection.fresh {
		t.Fatal("Delete-owned work must close fresh admission")
	}
	if got, want := selectedDeleteIndices(selection), []int32{7, 5}; !equalInt32Slices(got, want) {
		t.Fatalf("selected = %v, want %v", got, want)
	}
	if selection.deferred != 2 {
		t.Fatalf("deferred = %d, want 2 (one owned and one fresh)", selection.deferred)
	}
}

func TestSelectDeleteBatchCountsTerminatingPods(t *testing.T) {
	now := metav1.Now()
	pods := deleteSelectionPods(3, 3)
	pods[1].DeletionTimestamp = &now
	selection, err := selectDeleteBatch(
		[]workload.InstanceStatus{{Index: 3, Phase: workload.InstancePhaseReady}},
		[]int32{3}, map[int32][]*corev1.Pod{3: pods}, int32Pointer(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.candidates) != 1 || selection.candidates[0].cost != 3 || !selection.oversized {
		t.Fatalf("selection = %+v, want one oversized cost-3 candidate", selection)
	}
}

func TestSelectDeleteBatchSortsCopiedPodSlices(t *testing.T) {
	original := []*corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-b", Name: "pod-a"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "pod-z"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "pod-a"}},
	}
	selection, err := selectDeleteBatch(
		[]workload.InstanceStatus{{Index: 3, Phase: workload.InstancePhaseReady}},
		[]int32{3}, map[int32][]*corev1.Pod{3: original}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.candidates) != 1 {
		t.Fatalf("selected candidates = %d, want 1", len(selection.candidates))
	}
	if got, want := podKeys(selection.candidates[0].pods), []string{"ns-a/pod-a", "ns-a/pod-z", "ns-b/pod-a"}; !equalStringSlices(got, want) {
		t.Fatalf("selected Pod order = %v, want %v", got, want)
	}
	if got, want := podKeys(original), []string{"ns-b/pod-a", "ns-a/pod-z", "ns-a/pod-a"}; !equalStringSlices(got, want) {
		t.Fatalf("authoritative Pod bucket was mutated: got %v, want %v", got, want)
	}
}

func TestSelectDeleteBatchRejectsInvalidBudget(t *testing.T) {
	for _, budget := range []int32{0, -1} {
		_, err := selectDeleteBatch(
			[]workload.InstanceStatus{{Index: 0, Phase: workload.InstancePhaseReady}},
			[]int32{0}, nil, &budget,
		)
		if err == nil {
			t.Errorf("budget %d: expected error", budget)
		}
	}
}

func TestSelectDeleteBatchTwoThousandDeterministic(t *testing.T) {
	const replicas = 2000
	statuses := make([]workload.InstanceStatus, 0, replicas-1)
	extras := make([]int32, 0, replicas-1)
	pods := make(map[int32][]*corev1.Pod, replicas-1)
	for index := int32(1); index < replicas; index++ {
		statuses = append(statuses, workload.InstanceStatus{Index: index, Phase: workload.InstancePhaseReady})
		extras = append(extras, index)
		pods[index] = deleteSelectionPods(index, 1)
	}
	selection, err := selectDeleteBatch(statuses, extras, pods, int32Pointer(100))
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.candidates) != 100 || selection.deferred != 1899 {
		t.Fatalf("selected/deferred = %d/%d, want 100/1899", len(selection.candidates), selection.deferred)
	}
	if selection.candidates[0].status.Index != 1999 || selection.candidates[99].status.Index != 1900 {
		t.Fatalf("selected bounds = %d..%d, want 1999..1900", selection.candidates[0].status.Index, selection.candidates[99].status.Index)
	}
}

func TestDeleteBatchTwoThousandWaveAndWriteBounds(t *testing.T) {
	tests := []struct {
		name          string
		podCost       int
		withPodGroups bool
		wantWaves     int
		wantPasses    int
		wantWrites    int
	}{
		{name: "single pod", podCost: 1, wantWaves: 20, wantPasses: 61, wantWrites: 40},
		{name: "eight pod gang without PodGroups", podCost: 8, wantWaves: 167, wantPasses: 502, wantWrites: 334},
		{name: "eight pod gang with PodGroups", podCost: 8, withPodGroups: true, wantWaves: 167, wantPasses: 669, wantWrites: 334},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			waves, passes, writes := driveImmediateDeleteConvergence(t, 1999, test.podCost, 100, test.withPodGroups)
			if waves != test.wantWaves || passes != test.wantPasses || writes != test.wantWrites {
				t.Fatalf("waves/passes/writes = %d/%d/%d, want %d/%d/%d",
					waves, passes, writes, test.wantWaves, test.wantPasses, test.wantWrites)
			}
		})
	}
}

func driveImmediateDeleteConvergence(t *testing.T, instances, podCost int, budgetValue int32, withPodGroups bool) (waves, passes, writes int) {
	t.Helper()
	owner := deleteBatchOwner()
	statuses := make([]workload.InstanceStatus, 0, instances)
	pods := make(map[int32][]*corev1.Pod, instances)
	for index := int32(1); index <= int32(instances); index++ {
		statuses = append(statuses, workload.InstanceStatus{Index: index, Incarnation: 1, Phase: workload.InstancePhaseReady})
		pods[index] = deleteSelectionPods(index, podCost)
		for ordinal, pod := range pods[index] {
			pod.Namespace = owner.Namespace
			pod.UID = k8stypes.UID(fmt.Sprintf("pod-%d-%d-uid", index, ordinal))
		}
	}
	store := newDeleteMutationStore(owner, statuses)
	client := newDeleteFailureBaseClient(t)
	expectations := workload.NewExpectations()
	podGroupDeleteAccepted := make(map[int32]bool)

	for {
		observed := make([]workload.InstanceStatus, 0, len(store.statuses))
		for _, row := range store.statuses {
			observed = append(observed, status.CloneDeleteInstanceStatus(row))
		}
		sort.Slice(observed, func(i, j int) bool { return observed[i].Index < observed[j].Index })
		extras := make([]int32, 0, len(observed))
		active := make([]int32, 0)
		for _, status := range observed {
			extras = append(extras, status.Index)
			if deleteOwned(status) {
				active = append(active, status.Index)
			}
		}
		fresh := len(observed) > 0 && len(active) == 0
		input := deleteBatchInput(owner, observed)
		input.ScaleDownPodBatchSize = &budgetValue
		input.ApplyInstanceMutationsWithRetryBlock = store.apply
		input.FinalizeInstanceResources = func(_ context.Context, index int32) (bool, error) {
			if !withPodGroups {
				return true, nil
			}
			if podGroupDeleteAccepted[index] {
				delete(podGroupDeleteAccepted, index)
				return true, nil
			}
			podGroupDeleteAccepted[index] = true
			return false, nil
		}

		result, err := DeleteBatch(context.Background(), workload.Deps{
			Client: client, Expectations: expectations,
		}, input, deleteBatchPlan(), extras, pods)
		if err != nil {
			t.Fatal(err)
		}
		passes++
		if len(observed) == 0 {
			if result.InProgress || result.ImmediateRequeue {
				t.Fatalf("final verification remained active: %+v", result)
			}
			break
		}
		if fresh {
			waves++
			if !result.ImmediateRequeue {
				t.Fatalf("wave %d admission did not end the pass: %+v", waves, result)
			}
			continue
		}
		for _, index := range active {
			if len(pods[index]) > 0 {
				pods[index] = nil
			}
		}
	}
	return waves, passes, store.writes
}

func deleteOwnedStatus(index int32, started time.Time) workload.InstanceStatus {
	return workload.InstanceStatus{
		Index:       index,
		Incarnation: 4,
		Phase:       workload.InstancePhaseDeleting,
		Operation: &workload.InstanceOperation{
			ID:        fmt.Sprintf("delete-%d", index),
			Type:      workload.InstanceOperationDelete,
			Step:      "Drain",
			StartedAt: metav1.NewTime(started),
		},
	}
}

func deleteSelectionPods(index int32, count int) []*corev1.Pod {
	pods := make([]*corev1.Pod, 0, count)
	for ordinal := 0; ordinal < count; ordinal++ {
		pods = append(pods, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d-%d", index, ordinal)}})
	}
	return pods
}

func selectedDeleteIndices(selection deleteBatchSelection) []int32 {
	indices := make([]int32, 0, len(selection.candidates))
	for _, candidate := range selection.candidates {
		indices = append(indices, candidate.status.Index)
	}
	return indices
}

func int32Pointer(value int32) *int32 { return &value }

func equalInt32Slices(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func podKeys(pods []*corev1.Pod) []string {
	keys := make([]string, 0, len(pods))
	for _, pod := range pods {
		keys = append(keys, pod.Namespace+"/"+pod.Name)
	}
	return keys
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type gracefulDeleteRecordingClient struct {
	client.Client
	deleteCalls int
	deleteUIDs  []*types.UID
	deleteErr   error
}

func (c *gracefulDeleteRecordingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.deleteCalls++
	deleteOptions := &client.DeleteOptions{}
	for _, opt := range opts {
		opt.ApplyToDelete(deleteOptions)
	}
	var uid *types.UID
	if deleteOptions.Preconditions != nil && deleteOptions.Preconditions.UID != nil {
		value := *deleteOptions.Preconditions.UID
		uid = &value
	}
	c.deleteUIDs = append(c.deleteUIDs, uid)
	if c.deleteErr != nil {
		return c.deleteErr
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func TestDeleteBatchGracefulDeleteUsesObservedPodUID(t *testing.T) {
	owner := deleteBatchOwner()
	status := deleteOwnedStatus(0, time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	pod := deleteBatchPod(0)
	base := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod).Build()
	recording := &gracefulDeleteRecordingClient{Client: base}
	expectations := workload.NewExpectations()
	store := newDeleteMutationStore(owner, []workload.InstanceStatus{status})
	input := deleteBatchInput(owner, []workload.InstanceStatus{status})
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	if _, err := DeleteBatch(context.Background(), workload.Deps{
		Client: recording, Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {pod}}); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	if recording.deleteCalls != 1 || len(recording.deleteUIDs) != 1 ||
		recording.deleteUIDs[0] == nil || *recording.deleteUIDs[0] != pod.UID {
		t.Fatalf("delete calls/UIDs = %d/%v, want one delete preconditioned on %q",
			recording.deleteCalls, recording.deleteUIDs, pod.UID)
	}
	if expectations.Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, status.Index) {
		t.Fatal("successful delete must retain its expectation until the Pod watch observes deletion")
	}
}

func TestDeleteBatchGracefulDeleteUIDConflictPreservesReplacement(t *testing.T) {
	owner := deleteBatchOwner()
	status := deleteOwnedStatus(0, time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	observed := deleteBatchPod(0)
	replacement := observed.DeepCopy()
	replacement.UID = types.UID("replacement-pod-uid")
	base := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(replacement).Build()
	recording := &gracefulDeleteRecordingClient{
		Client: base,
		deleteErr: apierrors.NewConflict(
			schema.GroupResource{Resource: "pods"}, observed.Name, apierrors.NewBadRequest("UID precondition does not match")),
	}
	expectations := workload.NewExpectations()
	store := newDeleteMutationStore(owner, []workload.InstanceStatus{status})
	input := deleteBatchInput(owner, []workload.InstanceStatus{status})
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	if _, err := DeleteBatch(context.Background(), workload.Deps{
		Client: recording, Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {observed}}); err != nil {
		t.Fatalf("UID conflict must be stale-snapshot success: %v", err)
	}
	if recording.deleteCalls != 1 || len(recording.deleteUIDs) != 1 ||
		recording.deleteUIDs[0] == nil || *recording.deleteUIDs[0] != observed.UID {
		t.Fatalf("delete calls/UIDs = %d/%v, want observed UID %q",
			recording.deleteCalls, recording.deleteUIDs, observed.UID)
	}
	if !expectations.Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, status.Index) {
		t.Fatal("UID conflict must roll back the delete expectation")
	}
	stored := &corev1.Pod{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(replacement), stored); err != nil {
		t.Fatalf("same-name replacement was deleted: %v", err)
	}
	if stored.UID != replacement.UID {
		t.Fatalf("stored Pod UID = %q, want replacement UID %q", stored.UID, replacement.UID)
	}
}

func TestDeleteBatchDrainUIDChangePreservesReplacement(t *testing.T) {
	owner := deleteBatchOwner()
	status := deleteOwnedStatus(0, time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	observed := deleteBatchPod(0)
	observed.Status.Conditions = []corev1.PodCondition{{
		Type: podreadiness.ConditionType, Status: corev1.ConditionTrue,
	}}
	replacement := observed.DeepCopy()
	replacement.UID = types.UID("replacement-pod-uid")
	base := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(replacement).Build()
	recording := &gracefulDeleteRecordingClient{Client: base}
	expectations := workload.NewExpectations()
	store := newDeleteMutationStore(owner, []workload.InstanceStatus{status})
	input := deleteBatchInput(owner, []workload.InstanceStatus{status})
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	result, err := DeleteBatch(context.Background(), workload.Deps{
		Client: recording, Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {observed}})
	if err != nil {
		t.Fatalf("UID change must request a fresh observation: %v", err)
	}
	if !result.ImmediateRequeue {
		t.Fatalf("UID change result = %+v, want immediate requeue", result)
	}
	if recording.deleteCalls != 0 {
		t.Fatalf("UID change issued %d delete call(s), want 0", recording.deleteCalls)
	}
	if !expectations.Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, status.Index) {
		t.Fatal("UID change must not leave a delete expectation")
	}
	stored := &corev1.Pod{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(replacement), stored); err != nil {
		t.Fatalf("get same-name replacement: %v", err)
	}
	if !podreadiness.IsServing(stored) {
		t.Fatal("stale drain marked the same-name replacement NotServing")
	}
}

func TestDeleteBatchGracefulDeleteRequiresObservedPodUID(t *testing.T) {
	owner := deleteBatchOwner()
	status := deleteOwnedStatus(0, time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	pod := deleteBatchPod(0)
	pod.UID = ""
	base := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod).Build()
	recording := &gracefulDeleteRecordingClient{Client: base}
	expectations := workload.NewExpectations()
	store := newDeleteMutationStore(owner, []workload.InstanceStatus{status})
	input := deleteBatchInput(owner, []workload.InstanceStatus{status})
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	_, err := DeleteBatch(context.Background(), workload.Deps{
		Client: recording, Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {pod}})
	if err == nil || !strings.Contains(err.Error(), "without an observed UID") {
		t.Fatalf("DeleteBatch error = %v, want missing observed UID", err)
	}
	if recording.deleteCalls != 0 {
		t.Fatalf("missing UID issued %d delete call(s), want zero", recording.deleteCalls)
	}
	if !expectations.Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, status.Index) {
		t.Fatal("missing UID must not leave a delete expectation")
	}
	stored := &corev1.Pod{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
		t.Fatalf("missing-UID Pod changed: %v", err)
	}
}

func TestDeleteBatchGracefulDeletePreservesOtherErrors(t *testing.T) {
	owner := deleteBatchOwner()
	status := deleteOwnedStatus(0, time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	pod := deleteBatchPod(0)
	base := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod).Build()
	recording := &gracefulDeleteRecordingClient{
		Client:    base,
		deleteErr: apierrors.NewServiceUnavailable("injected delete failure"),
	}
	expectations := workload.NewExpectations()
	store := newDeleteMutationStore(owner, []workload.InstanceStatus{status})
	input := deleteBatchInput(owner, []workload.InstanceStatus{status})
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	_, err := DeleteBatch(context.Background(), workload.Deps{
		Client: recording, Expectations: expectations,
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{0: {pod}})
	if err == nil || !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("DeleteBatch error = %v, want preserved ServiceUnavailable", err)
	}
	if recording.deleteCalls != 1 {
		t.Fatalf("delete calls = %d, want 1", recording.deleteCalls)
	}
	if !expectations.Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, status.Index) {
		t.Fatal("failed delete must roll back its expectation")
	}
}

func TestDeleteBatchResumedWaveRequiresAtomicStatusAdapter(t *testing.T) {
	owner := deleteBatchOwner()
	status := deleteOwnedStatus(0, time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	input := deleteBatchInput(owner, []workload.InstanceStatus{status})

	_, err := DeleteBatch(context.Background(), workload.Deps{
		Client: fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).Build(),
	}, input, deleteBatchPlan(), nil, map[int32][]*corev1.Pod{})
	if err == nil || !strings.Contains(err.Error(), "owner-aware atomic status adapter is required") {
		t.Fatalf("DeleteBatch error = %v, want missing atomic adapter", err)
	}
}

// overdueDrainStatus builds a Delete-owned row whose drain deadline has
// already elapsed at overdueDrainNow.
func overdueDrainStatus(index int32, deadline time.Time) workload.InstanceStatus {
	status := deleteOwnedStatus(index, deadline.Add(-time.Minute))
	status.Operation.Deadline = metav1.NewTime(deadline)
	return status
}

// overdueDrainTerminatingPod is a pod of index already on its way out.
func overdueDrainTerminatingPod(index int32, deletedAt time.Time) *corev1.Pod {
	pod := deleteBatchPod(index)
	stamp := metav1.NewTime(deletedAt)
	pod.DeletionTimestamp = &stamp
	pod.Finalizers = []string{"ome.io/test-hold"}
	return pod
}

// overdueDrainInput wires a delete pass whose clock sits at now.
func overdueDrainInput(statuses []workload.InstanceStatus, now time.Time) workload.ReconcileInput {
	input := deleteBatchInput(deleteBatchOwner(), statuses)
	input.Clock = clocktesting.NewFakeClock(now)
	return input
}

func overdueDrainEvents(rec *record.FakeRecorder) []string {
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

// An elapsed drain deadline announces itself once and changes nothing
// else: the row keeps its Deleting phase, the delete operation and the
// index, and the pod is not force-deleted. The second pass sees the
// record and stays silent.
func TestDeleteBatchOverdueDrainAnnouncesOncePerEpisode(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	deadline := t0.Add(time.Minute)
	now := deadline.Add(3 * time.Minute)
	statuses := []workload.InstanceStatus{overdueDrainStatus(1, deadline)}
	pod := overdueDrainTerminatingPod(1, t0)
	pods := map[int32][]*corev1.Pod{1: {pod}}
	c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod).Build()
	store := newDeleteMutationStore(deleteBatchOwner(), statuses)
	rec := record.NewFakeRecorder(16)
	expectations := workload.NewExpectations()

	var events []string
	for pass := 0; pass < 2; pass++ {
		input := overdueDrainInput(statuses, now)
		input.ApplyInstanceMutationsWithRetryBlock = store.apply
		result, err := DeleteBatch(context.Background(),
			workload.Deps{Client: c, Recorder: rec, Expectations: expectations},
			input, deleteBatchPlan(), nil, pods)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if !result.InProgress {
			t.Fatalf("pass %d: the wave must keep the index: %+v", pass, result)
		}
		events = append(events, overdueDrainEvents(rec)...)
	}

	overdue := 0
	for _, e := range events {
		if strings.Contains(e, string(workload.EventReasonDrainOverdue)) {
			overdue++
			if !strings.Contains(e, "instance=1") || !strings.Contains(e, "3m0s") ||
				!strings.Contains(e, pod.Name) || !strings.Contains(e, "still Terminating") {
				t.Errorf("event does not name the Instance, the pods and how long overdue: %q", e)
			}
		}
	}
	if overdue != 1 {
		t.Fatalf("DrainOverdue events = %d, want 1 (events=%v)", overdue, events)
	}
	row := store.statuses[1]
	if row.Phase != workload.InstancePhaseDeleting || !deleteOwned(row) {
		t.Fatalf("the row must stay Deleting under its delete operation: %+v", row)
	}
	if row.LastFailure == nil || row.LastFailure.Reason != workload.DrainOverdueReason ||
		!row.LastFailure.Time.Equal(&row.Operation.Deadline) || row.LastFailure.PodName != pod.Name {
		t.Fatalf("announcement record = %+v", row.LastFailure)
	}
	if store.writes != 1 {
		t.Fatalf("status writes = %d, want 1 (the second pass must write nothing)", store.writes)
	}
	live := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
		t.Fatalf("the announcement must not delete the pod: %v", err)
	}
}

// A drain inside its deadline says nothing.
func TestDeleteBatchDrainWithinDeadlineStaysSilent(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	deadline := t0.Add(time.Hour)
	statuses := []workload.InstanceStatus{overdueDrainStatus(1, deadline)}
	pod := overdueDrainTerminatingPod(1, t0)
	pods := map[int32][]*corev1.Pod{1: {pod}}
	c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(pod).Build()
	store := newDeleteMutationStore(deleteBatchOwner(), statuses)
	rec := record.NewFakeRecorder(16)
	input := overdueDrainInput(statuses, t0.Add(time.Minute))
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	if _, err := DeleteBatch(context.Background(),
		workload.Deps{Client: c, Recorder: rec, Expectations: workload.NewExpectations()},
		input, deleteBatchPlan(), nil, pods); err != nil {
		t.Fatal(err)
	}
	if events := overdueDrainEvents(rec); len(events) != 0 {
		t.Fatalf("events = %v, want none", events)
	}
	if store.statuses[1].LastFailure != nil {
		t.Fatalf("record = %+v, want none", store.statuses[1].LastFailure)
	}
}

// A fresh delete operation is a fresh episode: a row readmitted under a
// new operation announces again rather than reading the previous
// episode's marker as "already told".
func TestOverdueDrainReannouncesOnANewEpisode(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	second := metav1.NewTime(t0.Add(time.Hour))
	row := overdueDrainStatus(1, second.Time)
	row.Announced = []string{string(workload.EventReasonDrainOverdue) + "@delete-1-earlier"}
	pods := []*corev1.Pod{overdueDrainTerminatingPod(1, t0)}

	if _, ok := overdueDrain(row, pods, second.Add(time.Minute)); !ok {
		t.Fatal("a new operation must open a new episode")
	}
	if !status.AnnounceDrainOverdue(second, nil)(&row) {
		t.Fatal("the new episode's announcement must record")
	}
	if _, ok := overdueDrain(row, pods, second.Add(time.Minute)); ok {
		t.Fatal("the announced episode must not repeat")
	}
}

// The pass ignores rows it does not own: a row with no pods left is
// completing, and a non-Delete operation is another pass's business.
func TestOverdueDrainIgnoresRowsItDoesNotOwn(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	now := t0.Add(time.Hour)
	pods := []*corev1.Pod{overdueDrainTerminatingPod(1, t0)}

	if _, ok := overdueDrain(overdueDrainStatus(1, t0), nil, now); ok {
		t.Error("a row with no pods left is completing, not overdue")
	}
	updating := overdueDrainStatus(1, t0)
	updating.Phase = workload.InstancePhaseUpdating
	updating.Operation.Type = workload.InstanceOperationUpdate
	if _, ok := overdueDrain(updating, pods, now); ok {
		t.Error("a non-Delete operation belongs to the escalation pass")
	}
	undated := overdueDrainStatus(1, t0)
	undated.Operation.Deadline = metav1.Time{}
	if _, ok := overdueDrain(undated, pods, now); ok {
		t.Error("an operation with no deadline never expires")
	}
}

// The pod budget bounds how much deleting runs at once, not what the
// operator is told: a delete-owned row deferred out of this pass's wave
// is announced past its deadline like any other.
func TestDeleteBatchOverdueDrainAnnouncesDeferredRows(t *testing.T) {
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	deadline := t0.Add(time.Minute)
	now := deadline.Add(2 * time.Minute)
	statuses := []workload.InstanceStatus{overdueDrainStatus(1, deadline), overdueDrainStatus(2, deadline)}
	// The oldest row is selected first, so index 2 is the deferred one.
	statuses[0].Operation.StartedAt = metav1.NewTime(t0)
	statuses[1].Operation.StartedAt = metav1.NewTime(t0.Add(time.Second))
	first, second := overdueDrainTerminatingPod(1, t0), overdueDrainTerminatingPod(2, t0)
	pods := map[int32][]*corev1.Pod{1: {first}, 2: {second}}
	c := fake.NewClientBuilder().WithScheme(deleteBatchScheme(t)).WithObjects(first, second).Build()
	store := newDeleteMutationStore(deleteBatchOwner(), statuses)
	rec := record.NewFakeRecorder(16)
	budget := int32(1)
	input := overdueDrainInput(statuses, now)
	input.ScaleDownPodBatchSize = &budget
	input.ApplyInstanceMutationsWithRetryBlock = store.apply

	result, err := DeleteBatch(context.Background(),
		workload.Deps{Client: c, Recorder: rec, Expectations: workload.NewExpectations()},
		input, deleteBatchPlan(), nil, pods)
	if err != nil {
		t.Fatal(err)
	}
	if result.Deferred != 1 {
		t.Fatalf("deferred = %d, want 1 — the fixture must defer a row out of the wave", result.Deferred)
	}
	for _, index := range []int32{1, 2} {
		row := store.statuses[index]
		if row.LastFailure == nil || row.LastFailure.Reason != workload.DrainOverdueReason {
			t.Errorf("instance %d record = %+v, want the overdue announcement", index, row.LastFailure)
		}
	}
	overdue := 0
	for _, e := range overdueDrainEvents(rec) {
		if strings.Contains(e, string(workload.EventReasonDrainOverdue)) {
			overdue++
		}
	}
	if overdue != 2 {
		t.Fatalf("DrainOverdue events = %d, want 2 (the driven row and the deferred one)", overdue)
	}
}
