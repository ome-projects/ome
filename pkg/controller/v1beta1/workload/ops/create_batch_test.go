package ops_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

type batchMutationRecorder struct {
	statuses map[int32]workload.InstanceStatus
	batches  [][]int32
	events   []string
	fail     error
}

func newBatchMutationRecorder(observed []workload.InstanceStatus) *batchMutationRecorder {
	r := &batchMutationRecorder{statuses: make(map[int32]workload.InstanceStatus, len(observed))}
	for _, status := range observed {
		r.statuses[status.Index] = status
	}
	return r
}

func (r *batchMutationRecorder) apply(_ context.Context, mutations []workload.InstanceMutation) error {
	if len(mutations) == 0 {
		return nil
	}
	indices := make([]int32, 0, len(mutations))
	for _, mutation := range mutations {
		indices = append(indices, mutation.Index)
	}
	r.batches = append(r.batches, indices)
	r.events = append(r.events, fmt.Sprintf("status:%v", indices))
	if r.fail != nil {
		return r.fail
	}
	type committedMutation struct {
		onCommit func(previous, current *workload.InstanceStatus)
		previous *workload.InstanceStatus
		current  *workload.InstanceStatus
	}
	committed := make([]committedMutation, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation.Remove {
			status, found := r.statuses[mutation.Index]
			if !found || (mutation.Precondition != nil && !mutation.Precondition(&status)) {
				continue
			}
			delete(r.statuses, mutation.Index)
			if mutation.OnCommit != nil {
				previous := status
				committed = append(committed, committedMutation{onCommit: mutation.OnCommit, previous: &previous})
			}
			continue
		}
		status, ok := r.statuses[mutation.Index]
		var previous *workload.InstanceStatus
		if !ok {
			status = workload.InstanceStatus{Index: mutation.Index}
		} else {
			copy := status
			previous = &copy
		}
		if mutation.Precondition != nil && !mutation.Precondition(&status) {
			continue
		}
		if mutation.Mutate(&status) {
			r.statuses[mutation.Index] = status
			if mutation.OnCommit != nil {
				current := status
				committed = append(committed, committedMutation{onCommit: mutation.OnCommit, previous: previous, current: &current})
			}
		}
	}
	for _, mutation := range committed {
		mutation.onCommit(mutation.previous, mutation.current)
	}
	return nil
}

type podCreateObserver struct {
	client.Client
	beforeCreate func(*corev1.Pod) error
}

func (c *podCreateObserver) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && c.beforeCreate != nil {
		if err := c.beforeCreate(pod); err != nil {
			return err
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

type failingPodStatusClient struct {
	client.Client
	podName string
	err     error
}

func (c *failingPodStatusClient) Status() client.SubResourceWriter {
	return &failingPodStatusWriter{
		SubResourceWriter: c.Client.Status(),
		podName:           c.podName,
		err:               c.err,
	}
}

type failingPodStatusWriter struct {
	client.SubResourceWriter
	podName string
	err     error
}

func (w *failingPodStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && pod.Name == w.podName {
		return w.err
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func newReadyBatchFixture(t *testing.T, name string, replicas int32) (*v1beta1.InferenceService, client.Client, workload.ReconcileInput) {
	t.Helper()
	isvc := minimalISVC(name, "prod", int(replicas))
	statuses := make([]v1beta1.OMENativeInstanceStatus, 0, replicas)
	objects := []client.Object{isvc}
	for idx := int32(0); idx < replicas; idx++ {
		statuses = append(statuses, v1beta1.OMENativeInstanceStatus{
			Index:       idx,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{
				Type:      v1beta1.InstanceOperationCreate,
				StartedAt: metav1.Now(),
			},
		})
		objects = append(objects, podForInstance(isvc, idx, true, false))
	}
	objects = append(objects, instanceIR(isvc, workload.ComponentEngine, statuses...))
	c := newFakeClient(t, objects...)
	return isvc, c, buildTestInput(isvc, c, workload.ComponentEngine)
}

func requireMutationPodsServing(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, mutations []workload.InstanceMutation) error {
	for _, mutation := range mutations {
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, mutation.Index, "default", 0),
		}
		if err := c.Get(ctx, key, pod); err != nil {
			return fmt.Errorf("get ready pod for instance %d: %w", mutation.Index, err)
		}
		if !podreadiness.IsServing(pod) {
			return fmt.Errorf("Ready status for instance %d was committed before its Pod became serving", mutation.Index)
		}
	}
	return nil
}

func TestCreate_BatchedFreshStartsHonorConfiguredCapAndCommitBeforePods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batched", "prod", 5)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	observedCreates := make([]int32, 0, podBatchSize)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return fmt.Errorf("parse instance index on pod %s: %w", pod.Name, err)
		}
		idx := int32(idx64)
		status, ok := recorder.statuses[idx]
		if !ok || status.Phase != workload.InstancePhaseCreating || status.Operation == nil ||
			status.Operation.Type != workload.InstanceOperationCreate || status.Operation.Step != "CreatePods" {
			return fmt.Errorf("pod %s created before its Creating intent was committed: %+v", pod.Name, status)
		}
		recorder.events = append(recorder.events, fmt.Sprintf("pod:%d", idx))
		observedCreates = append(observedCreates, idx)
		return nil
	}}

	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(5), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("a capped pass with deferred fresh Instances must requeue")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1}) {
		t.Fatalf("Creating batches: got %v, want one batch [0 1]", recorder.batches)
	}
	if !reflect.DeepEqual(observedCreates, []int32{0, 1}) {
		t.Fatalf("created Instance indices: got %v, want [0 1]", observedCreates)
	}
	wantEvents := []string{"status:[0 1]", "pod:0", "pod:1"}
	if !reflect.DeepEqual(recorder.events, wantEvents) {
		t.Fatalf("write-ahead order: got %v, want %v", recorder.events, wantEvents)
	}

	pods := &corev1.PodList{}
	if err := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != int(podBatchSize) {
		t.Fatalf("pods created in capped pass: got %d, want %d", len(pods.Items), podBatchSize)
	}
}

func TestCreate_BatchedTwoThousandReplicaScaleUpSelectsOneConfiguredWave(t *testing.T) {
	resetExpectations(t)
	const replicas int32 = 2001
	const waveSize int32 = 100
	isvc := minimalISVC("batch-large-scale", "prod", int(replicas))
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	configuredWaveSize := waveSize
	input.ScaleUpPodBatchSize = &configuredWaveSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	result, err := ops.Create(context.Background(), workload.Deps{Client: base}, input, buildPlanSinglePodEngine(replicas), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("a large capped scale-up must requeue the deferred replicas")
	}
	if len(recorder.batches) != 1 || len(recorder.batches[0]) != int(waveSize) {
		t.Fatalf("Creating batches: got lengths %v, want one batch of %d", recorder.batches, waveSize)
	}
	for idx, got := range recorder.batches[0] {
		if got != int32(idx) {
			t.Fatalf("Creating batch index %d: got %d, want stable prefix index %d", idx, got, idx)
		}
	}
	pods := &corev1.PodList{}
	if err := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list Pods: %v", err)
	}
	if len(pods.Items) != int(waveSize) {
		t.Fatalf("Pods created in first 2,001-replica wave: got %d, want %d", len(pods.Items), waveSize)
	}
	if len(recorder.statuses) != int(waveSize) {
		t.Fatalf("Creating statuses in first wave: got %d, want %d", len(recorder.statuses), waveSize)
	}
}

func TestCreate_BatchedNilPodBudgetIsUnbounded(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-unbounded", "prod", 5)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ScaleUpPodBatchSize = nil
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(5), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	wantBatch := []int32{0, 1, 2, 3, 4}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], wantBatch) {
		t.Fatalf("Creating batches with nil budget: got %v, want %v", recorder.batches, wantBatch)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 5 {
		t.Fatalf("pods created with nil budget: got %d, want 5", len(pods.Items))
	}
}

func TestCreate_BatchedPodBudgetExactFit(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-exact-fit", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(10)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(3)
	plan.Instances[1].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}
	createdByInstance := map[int32]int{}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		createdByInstance[int32(idx64)]++
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("exact-fit Creating batch: got %v, want [[0 1 2]]", recorder.batches)
	}
	wantCreates := map[int32]int{0: 1, 1: 8, 2: 1}
	if !reflect.DeepEqual(createdByInstance, wantCreates) {
		t.Fatalf("exact-fit pod creates: got %v, want %v", createdByInstance, wantCreates)
	}
}

func TestCreate_BatchedZeroPodBudgetBlocksOversizedGang(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-zero", "prod", 1)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(0)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(1)
	plan.Instances[0].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}
	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("zero Pod budget must requeue deferred work")
	}
	if len(recorder.batches) != 0 {
		t.Fatalf("Creating batches with zero budget: got %v, want none", recorder.batches)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods created with zero budget: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_BatchedCreatingPersistenceFailureCreatesNoPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-failure", "prod", 3)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	recorder.fail = errors.New("status unavailable")
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if err == nil {
		t.Fatal("Create succeeded despite a failed Creating status batch")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1}) {
		t.Fatalf("attempted Creating batches: got %v, want [[0 1]]", recorder.batches)
	}

	pods := &corev1.PodList{}
	if listErr := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list pods: %v", listErr)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pods created after failed write-ahead status: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_BatchedMissingStatusOwnerCreatesNoPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-owner-gone", "prod", 3)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = func(context.Context, []workload.InstanceMutation) error {
		t.Fatal("owner-aware atomic status capability was bypassed")
		return nil
	}
	atomicCalls := 0
	input.ApplyInstanceMutationsWithRetryBlock = func(_ context.Context, mutations []workload.InstanceMutation, revision string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		atomicCalls++
		if len(mutations) != 2 {
			t.Fatalf("atomic batch length: got %d, want 2", len(mutations))
		}
		if !reflect.DeepEqual([]int32{mutations[0].Index, mutations[1].Index}, []int32{0, 1}) {
			t.Fatalf("atomic batch indices: got [%d %d], want [0 1]", mutations[0].Index, mutations[1].Index)
		}
		if revision != "" || mutate != nil {
			t.Fatalf("nil-target atomic write: revision=%q mutate=%v, want empty and nil", revision, mutate != nil)
		}
		return workload.ErrStatusOwnerGone
	}

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create after owner deletion: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("Create result after owner deletion: got %+v, want zero", result)
	}
	if atomicCalls != 1 {
		t.Fatalf("atomic status calls: got %d, want 1", atomicCalls)
	}
	pods := &corev1.PodList{}
	if listErr := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list pods: %v", listErr)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pods created after the status owner disappeared: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_BatchedAtomicRetryBlockFailureCreatesNoStatusOrPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-retry-failure", "prod", 2)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-retry-failure-engine-deadbeef", Namespace: isvc.Namespace},
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: target.Name,
		State:          workload.RetryBlockBackoff,
	}}
	retryErr := errors.New("retry block unavailable")
	retryCalls := 0
	var retryDisposition workload.RetryBlockDisposition
	var attemptedBatch []int32
	input.ApplyInstanceMutationsWithRetryBlock = func(_ context.Context, mutations []workload.InstanceMutation, revision string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		retryCalls++
		for _, mutation := range mutations {
			attemptedBatch = append(attemptedBatch, mutation.Index)
		}
		block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, revision)
		if block == nil {
			return fmt.Errorf("missing RetryBlock for %s", revision)
		}
		copy := *block
		retryDisposition = mutate(&copy)
		return retryErr
	}

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(2), target)
	if !errors.Is(err, retryErr) {
		t.Fatalf("Create error: got %v, want retry persistence failure", err)
	}
	if !reflect.DeepEqual(attemptedBatch, []int32{0, 1}) {
		t.Fatalf("attempted atomic batch: got %v, want [0 1]", attemptedBatch)
	}
	if len(recorder.batches) != 0 {
		t.Fatalf("separate InstanceStatus batches after atomic failure: got %v, want none", recorder.batches)
	}
	if retryCalls != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d, want 1", retryCalls)
	}
	if retryDisposition != workload.RetryBlockPersist {
		t.Fatalf("RetryBlock disposition: got %v, want Persist", retryDisposition)
	}
	for _, idx := range []int32{0, 1} {
		if _, ok := recorder.statuses[idx]; ok {
			t.Errorf("instance %d status committed despite atomic RetryBlock failure", idx)
		}
	}
	pods := &corev1.PodList{}
	if listErr := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list pods: %v", listErr)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pods created after failed RetryBlock write: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_InstanceOnlyFallbackRetryBlockFailureStopsAfterFirstStatus(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-retry-fallback", "prod", 2)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-retry-fallback-engine-deadbeef", Namespace: isvc.Namespace},
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: target.Name,
		State:          workload.RetryBlockBackoff,
	}}
	retryErr := errors.New("retry block unavailable")
	retryCalls := 0
	input.MutateRetryBlock = func(_ context.Context, revision string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		retryCalls++
		if revision != target.Name {
			return fmt.Errorf("retry revision: got %q, want %q", revision, target.Name)
		}
		block := input.ObservedState.RetryBlocks[0]
		if got := mutate(&block); got != workload.RetryBlockPersist {
			return fmt.Errorf("RetryBlock disposition: got %v, want Persist", got)
		}
		return retryErr
	}

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(2), target)
	if !errors.Is(err, retryErr) {
		t.Fatalf("Create error: got %v, want retry persistence failure", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0}}) {
		t.Fatalf("fallback Creating batches: got %v, want [[0]]", recorder.batches)
	}
	if retryCalls != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d, want 1", retryCalls)
	}
	if status := recorder.statuses[0]; status.Phase != workload.InstancePhaseCreating || status.Operation == nil {
		t.Fatalf("first fallback status: got %+v, want durable Creating intent", status)
	}
	if _, found := recorder.statuses[1]; found {
		t.Fatal("fallback path committed a later Creating status after RetryBlock failure")
	}
	pods := &corev1.PodList{}
	if listErr := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list Pods: %v", listErr)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("Pods created after failed RetryBlock write: got %d, want 0", len(pods.Items))
	}
}

func TestCreate_BatchedAtomicCreatingFailureDoesNotBlockReadyInstances(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-atomic-isolation", "prod", 2)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-atomic-isolation-engine-" + testRevisionHash, Namespace: isvc.Namespace},
	}
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
	})
	pod := podForInstance(isvc, 0, true, false)
	base := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{TargetRevision: target.Name, State: workload.RetryBlockBackoff}}
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	atomicErr := errors.New("atomic Creating status unavailable")
	input.ApplyInstanceMutationsWithRetryBlock = func(ctx context.Context, mutations []workload.InstanceMutation, _ string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		if mutate == nil {
			return recorder.apply(ctx, mutations)
		}
		block := workload.RetryBlock{TargetRevision: target.Name, State: workload.RetryBlockBackoff}
		if got := mutate(&block); got != workload.RetryBlockPersist {
			return fmt.Errorf("RetryBlock disposition: got %v, want Persist", got)
		}
		return atomicErr
	}

	_, err := ops.Create(context.Background(), workload.Deps{Client: base}, input, buildPlanSinglePodEngine(2), target)
	if !errors.Is(err, atomicErr) {
		t.Fatalf("Create error: got %v, want atomic failure", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0}) {
		t.Fatalf("unrelated Ready batch: got %v, want [[0]]", recorder.batches)
	}
	if status := recorder.statuses[0]; status.Phase != workload.InstancePhaseReady {
		t.Fatalf("ready instance was blocked by Creating failure: got %+v", status)
	}
	if _, ok := recorder.statuses[1]; ok {
		t.Fatal("missing instance status committed despite atomic Creating failure")
	}
	freshPod := &corev1.Pod{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), freshPod); err != nil {
		t.Fatalf("get ready Pod: %v", err)
	}
	if !podreadiness.IsServing(freshPod) {
		t.Fatal("ready instance Pod did not become serving")
	}
}

func TestCreate_BatchedActionsPreservePlanOrder(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-plan-order", "prod", 3)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       1,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
	})
	readyPod := podForInstance(isvc, 1, true, false)
	base := newFakeClient(t, isvc, ir, readyPod)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		recorder.events = append(recorder.events, fmt.Sprintf("pod:%d", idx))
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	wantEvents := []string{"status:[0]", "pod:0", "status:[1]", "status:[2]", "pod:2"}
	if !reflect.DeepEqual(recorder.events, wantEvents) {
		t.Fatalf("action order: got %v, want %v", recorder.events, wantEvents)
	}
	freshReadyPod := &corev1.Pod{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(readyPod), freshReadyPod); err != nil {
		t.Fatalf("get ready Pod: %v", err)
	}
	if !podreadiness.IsServing(freshReadyPod) {
		t.Fatal("ready instance Pod did not become serving")
	}
}

func TestCreate_BatchedCreatingFailurePreservesNoOpPrefix(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-creating-noop-prefix", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
	})
	base := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	statusErr := errors.New("Creating status unavailable")
	statusCalls := 0
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		statusCalls++
		if statusCalls == 2 {
			return statusErr
		}
		return recorder.apply(ctx, mutations)
	}
	input.MutateInstance = unexpectedPerInstanceMutation
	attempted := make([]int32, 0, 1)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		attempted = append(attempted, int32(idx))
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(2), nil)
	if !errors.Is(err, statusErr) {
		t.Fatalf("Create error: got %v, want Creating status failure", err)
	}
	if !reflect.DeepEqual(attempted, []int32{0}) {
		t.Fatalf("Pod-create attempts before status failure: got %v, want [0]", attempted)
	}
	if _, exists := recorder.statuses[1]; exists {
		t.Fatal("failing changed-status action committed unexpectedly")
	}
}

func TestCreate_BatchedReadyFailurePreservesNoOpPrefix(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-ready-noop-prefix", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady},
		v1beta1.OMENativeInstanceStatus{
			Index:       1,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceCreating,
			Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate},
		},
	)
	pod0 := podForInstance(isvc, 0, true, false)
	pod1 := podForInstance(isvc, 1, true, false)
	base := newFakeClient(t, isvc, ir, pod0, pod1)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	statusErr := errors.New("Ready status unavailable")
	statusCalls := 0
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		statusCalls++
		if statusCalls == 2 {
			return statusErr
		}
		return recorder.apply(ctx, mutations)
	}
	input.MutateInstance = unexpectedPerInstanceMutation

	_, err := ops.Create(context.Background(), workload.Deps{Client: base}, input, buildPlanSinglePodEngine(2), nil)
	if !errors.Is(err, statusErr) {
		t.Fatalf("Create error: got %v, want Ready status failure", err)
	}
	for idx := int32(0); idx < 2; idx++ {
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		if getErr := base.Get(context.Background(), key, pod); getErr != nil {
			t.Fatalf("get pod %d: %v", idx, getErr)
		}
		if !podreadiness.IsServing(pod) {
			t.Errorf("pod %d must retain the serving transition reached before its status result", idx)
		}
	}
	if status := recorder.statuses[0]; status.Phase != workload.InstancePhaseReady {
		t.Fatalf("no-op prefix status: got %+v, want Ready", status)
	}
	if status := recorder.statuses[1]; status.Phase != workload.InstancePhaseCreating {
		t.Fatalf("failed status action: got %+v, want Creating", status)
	}
}

func TestCreate_BatchedPodCreateFailureStopsAndRollsBackUnattemptedInstances(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-create-failure", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	createErr := errors.New("pod create unavailable")
	attempted := make([]int32, 0, 3)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		idx := int32(idx64)
		attempted = append(attempted, idx)
		if idx == 1 {
			return createErr
		}
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, createErr) {
		t.Fatalf("Create error: got %v, want Pod-create failure", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {2}}) {
		t.Fatalf("Creating and rollback batches: got %v, want [[0 1 2] [2]]", recorder.batches)
	}
	if !reflect.DeepEqual(attempted, []int32{0, 1}) {
		t.Fatalf("Pod-create attempts: got %v, want [0 1]", attempted)
	}
	for idx := int32(0); idx < 2; idx++ {
		status := recorder.statuses[idx]
		if status.Phase != workload.InstancePhaseCreating || status.Operation == nil {
			t.Errorf("instance %d after create attempts: got %+v, want durable Creating intent", idx, status)
		}
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		getErr := base.Get(context.Background(), key, pod)
		if idx == 1 {
			if getErr == nil {
				t.Errorf("failed instance %d unexpectedly has a Pod", idx)
			}
			continue
		}
		if getErr != nil {
			t.Errorf("healthy instance %d Pod was not created: %v", idx, getErr)
		}
	}
	if _, ok := recorder.statuses[2]; ok {
		t.Fatal("unattempted instance 2 retained a speculative Creating status")
	}
	unattemptedPod := &corev1.Pod{}
	unattemptedKey := client.ObjectKey{
		Namespace: isvc.Namespace,
		Name:      query.PodName(isvc.Name, workload.ComponentEngine, 2, "default", 0),
	}
	if getErr := base.Get(context.Background(), unattemptedKey, unattemptedPod); getErr == nil {
		t.Fatal("unattempted instance 2 unexpectedly has a Pod")
	}
}

func TestCreate_BatchedPodCreateFailureRestoresPriorUnattemptedStatus(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-restore-status", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{
			Index:           0,
			Incarnation:     3,
			Phase:           v1beta1.OMENativeInstanceFailed,
			RunningRevision: "revision-a",
			ActiveOrdinal:   1,
		},
		v1beta1.OMENativeInstanceStatus{
			Index:           1,
			Incarnation:     7,
			Phase:           v1beta1.OMENativeInstanceFailed,
			RunningRevision: "revision-b",
			ActiveOrdinal:   1,
			PodCount:        4,
			ReadyPodCount:   2,
			NodesOccupied:   []string{"worker-a", "worker-b"},
		},
	)
	base := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	prior := recorder.statuses[1]
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	createErr := errors.New("pod create unavailable")
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(*corev1.Pod) error {
		return createErr
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(2), nil)
	if !errors.Is(err, createErr) {
		t.Fatalf("Create error: got %v, want Pod-create failure", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1}, {1}}) {
		t.Fatalf("Creating and restore batches: got %v, want [[0 1] [1]]", recorder.batches)
	}
	if got := recorder.statuses[1]; !reflect.DeepEqual(got, prior) {
		t.Fatalf("unattempted instance status was not restored:\n got: %+v\nwant: %+v", got, prior)
	}
	if got := recorder.statuses[0]; got.Phase != workload.InstancePhaseCreating || got.Operation == nil {
		t.Fatalf("attempted instance status: got %+v, want Creating intent", got)
	}
}

func TestCreate_BatchedRollbackPreservesConcurrentStatusChange(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-concurrent-status", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	createErr := errors.New("pod create unavailable")
	concurrent := workload.InstanceStatus{
		Index:           2,
		Incarnation:     9,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "revision-concurrent",
	}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		if idx == 1 {
			recorder.statuses[2] = concurrent
			return createErr
		}
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, createErr) {
		t.Fatalf("Create error: got %v, want Pod-create failure", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {2}}) {
		t.Fatalf("Creating and conditional rollback batches: got %v, want [[0 1 2] [2]]", recorder.batches)
	}
	if got := recorder.statuses[2]; !reflect.DeepEqual(got, concurrent) {
		t.Fatalf("conditional rollback clobbered concurrent status:\n got: %+v\nwant: %+v", got, concurrent)
	}
}

func TestCreate_BatchedCancellationStopsFurtherPodCreates(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-cancel", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return recorder.apply(ctx, mutations)
	}
	input.MutateInstance = unexpectedPerInstanceMutation

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempted := make([]int32, 0, 2)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		idx := int32(idx64)
		attempted = append(attempted, idx)
		if idx == 1 {
			cancel()
			return context.Canceled
		}
		return nil
	}}

	_, err := ops.Create(ctx, workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Create error: got %v, want context cancellation", err)
	}
	if !reflect.DeepEqual(attempted, []int32{0, 1}) {
		t.Fatalf("Pod-create attempts after cancellation: got %v, want [0 1]", attempted)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}}) {
		t.Fatalf("durable Creating batches after cancellation: got %v, want [[0 1 2]]", recorder.batches)
	}
	if status := recorder.statuses[2]; status.Phase != workload.InstancePhaseCreating || status.Operation == nil {
		t.Fatalf("unattempted status after canceled recovery write: got %+v, want resumable Creating intent", status)
	}
	pods := &corev1.PodList{}
	if listErr := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list Pods: %v", listErr)
	}
	wantPodName := query.PodName(isvc.Name, workload.ComponentEngine, 0, "default", 0)
	if len(pods.Items) != 1 || pods.Items[0].Name != wantPodName {
		t.Fatalf("Pods after cancellation: got %+v, want only %s", pods.Items, wantPodName)
	}
}

func TestCreate_BatchedGlobalAPIFailureStopsFurtherPodCreates(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-api-overload", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	overloadErr := apierrors.NewTooManyRequests("apiserver overloaded", 1)
	attempted := make([]int32, 0, 2)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		idx := int32(idx64)
		attempted = append(attempted, idx)
		if idx == 1 {
			return overloadErr
		}
		return nil
	}}

	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create error: got %v, want nil — a throttled apiserver is paced, not failed", err)
	}
	// The server's suggested delay is a floor. A 1s suggestion is already
	// satisfied by the ordinary create interval, so the pass keeps it.
	if result.RequeueAfter != ops.CreateRequeueInterval || result.RequeueAfter < time.Second {
		t.Fatalf("RequeueAfter: got %v, want %v (>= the server's suggested 1s)", result.RequeueAfter, ops.CreateRequeueInterval)
	}
	if !reflect.DeepEqual(attempted, []int32{0, 1}) {
		t.Fatalf("Pod-create attempts after API overload: got %v, want [0 1]", attempted)
	}
	pods := &corev1.PodList{}
	if listErr := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list Pods: %v", listErr)
	}
	if len(pods.Items) != 1 || pods.Items[0].Labels[query.LabelInstanceIdx] != "0" {
		t.Fatalf("Pods after API overload: got %+v, want only instance 0", pods.Items)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {2}}) {
		t.Fatalf("Creating and API-failure rollback batches: got %v, want [[0 1 2] [2]]", recorder.batches)
	}
}

func TestCreate_BatchedTransportFailureStopsFurtherPodCreates(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-transport-failure", "prod", 3)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(3)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	transportErr := &net.DNSError{Err: "connection reset", Name: "kube-apiserver", IsTemporary: true}
	attempted := make([]int32, 0, 2)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		idx := int32(idx64)
		attempted = append(attempted, idx)
		if idx == 1 {
			return transportErr
		}
		return nil
	}}

	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, transportErr) {
		t.Fatalf("Create error: got %v, want transport failure", err)
	}
	if !reflect.DeepEqual(attempted, []int32{0, 1}) {
		t.Fatalf("Pod-create attempts after transport failure: got %v, want [0 1]", attempted)
	}
	pods := &corev1.PodList{}
	if listErr := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); listErr != nil {
		t.Fatalf("list Pods: %v", listErr)
	}
	if len(pods.Items) != 1 || pods.Items[0].Labels[query.LabelInstanceIdx] != "0" {
		t.Fatalf("Pods after transport failure: got %+v, want only instance 0", pods.Items)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {2}}) {
		t.Fatalf("Creating and transport-failure rollback batches: got %v, want [[0 1 2] [2]]", recorder.batches)
	}
}

func TestCreate_BatchedResumedCreatingConsumesMissingPodBudget(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-resume", "prod", 3)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationCreate,
		},
	})
	base := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(1)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	created := make([]int32, 0, 2)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		created = append(created, int32(idx64))
		return nil
	}}
	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("a pass with deferred missing Instances must requeue")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0}) {
		t.Fatalf("resumed Creating revalidation batch: got %v, want [[0]]", recorder.batches)
	}
	if !reflect.DeepEqual(created, []int32{0}) {
		t.Fatalf("created Instance indices: got %v, want only resumed index 0", created)
	}
}

func TestCreate_BatchedPartiallyMaterializedGangChargesOnlyMissingPods(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-partial-gang", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationCreate,
		},
	})
	base := newFakeClient(t,
		isvc,
		ir,
		gangPod(isvc, 0, "leader", 0, 1, false, false),
		gangPod(isvc, 0, "worker", 0, 1, false, false),
		gangPod(isvc, 0, "worker", 1, 1, false, false),
	)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(6)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	plan := buildPlanSinglePodEngine(2)
	plan.Instances[0].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}

	createdByInstance := map[int32]int{}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		createdByInstance[int32(idx64)]++
		return nil
	}}
	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0}, {1}}) {
		t.Fatalf("Creating batches: got %v, want resumed no-op gang [0] then fresh singleton [1]", recorder.batches)
	}
	wantCreates := map[int32]int{0: 5, 1: 1}
	if !reflect.DeepEqual(createdByInstance, wantCreates) {
		t.Fatalf("created missing pods: got %v, want %v", createdByInstance, wantCreates)
	}

	pods := &corev1.PodList{}
	if err := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	finalByInstance := map[string]int{}
	for i := range pods.Items {
		finalByInstance[pods.Items[i].Labels[query.LabelInstanceIdx]]++
	}
	wantFinal := map[string]int{"0": 8, "1": 1}
	if !reflect.DeepEqual(finalByInstance, wantFinal) {
		t.Fatalf("final pod topology: got %v, want %v", finalByInstance, wantFinal)
	}
}

func TestCreate_BatchedPodBudgetKeepsGangAtomicAndClosesStablePrefix(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-gang", "prod", 4)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(10)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(4)
	plan.Instances[1].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}
	plan.Instances[2].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}

	createdByInstance := map[int32]int{}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return fmt.Errorf("parse instance index on pod %s: %w", pod.Name, err)
		}
		idx := int32(idx64)
		status, ok := recorder.statuses[idx]
		if !ok || status.Phase != workload.InstancePhaseCreating {
			return fmt.Errorf("pod %s created before gang intent was committed", pod.Name)
		}
		createdByInstance[idx]++
		return nil
	}}

	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("the deferred second gang must cause a requeue")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1}) {
		t.Fatalf("Creating batch: got %v, want singleton 0 plus gang 1", recorder.batches)
	}
	if createdByInstance[0] != 1 || createdByInstance[1] != 8 {
		t.Fatalf("created pod weights: got %v, want instance 0=1 and instance 1=8", createdByInstance)
	}
	if createdByInstance[2] != 0 {
		t.Fatalf("second gang was split or admitted past the pod budget: got %d pod(s)", createdByInstance[2])
	}
	if createdByInstance[3] != 0 {
		t.Fatalf("later singleton bypassed the deferred gang: got %d pod(s)", createdByInstance[3])
	}
}

func TestCreate_BatchedPodBudgetAllowsOneOversizedGang(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-oversized-gang", "prod", 2)
	base := newFakeClient(t, isvc)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(5)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(2)
	plan.Instances[0].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 7},
	}

	createdByInstance := map[int32]int{}
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		createdByInstance[int32(idx64)]++
		return nil
	}}

	result, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("the deferred singleton must cause a requeue")
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0}) {
		t.Fatalf("Creating batch: got %v, want only oversized gang 0", recorder.batches)
	}
	if createdByInstance[0] != 8 || createdByInstance[1] != 0 {
		t.Fatalf("oversized gang admission: got %v, want instance 0=8 and instance 1=0", createdByInstance)
	}
}

func TestCreate_BatchedWavesAdvancePastOversizedGangAndConverge(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-converge", "prod", 3)
	c := newFakeClient(t, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(2)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	plan := buildPlanSinglePodEngine(3)
	plan.Instances[1].Runners = []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 2},
	}
	syncObserved := func() {
		input.ObservedState.InstanceStatuses = input.ObservedState.InstanceStatuses[:0]
		for _, instance := range plan.Instances {
			if status, ok := recorder.statuses[instance.Index]; ok {
				input.ObservedState.InstanceStatuses = append(input.ObservedState.InstanceStatuses, status)
			}
		}
	}
	observedPods := map[string]struct{}{}
	for pass, wantPods := range []int{1, 4, 5} {
		syncObserved()
		result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
		if err != nil {
			t.Fatalf("Create pass %d: %v", pass+1, err)
		}
		if result.RequeueAfter == 0 {
			t.Fatalf("Create pass %d did not requeue while Pods were becoming ready", pass+1)
		}

		pods := &corev1.PodList{}
		if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
			t.Fatalf("list Pods after pass %d: %v", pass+1, err)
		}
		if len(pods.Items) != wantPods {
			t.Fatalf("Pods after pass %d: got %d, want %d", pass+1, len(pods.Items), wantPods)
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if _, seen := observedPods[pod.Name]; seen {
				continue
			}
			idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
			if err != nil {
				t.Fatalf("parse Instance index on %s: %v", pod.Name, err)
			}
			workload.DefaultExpectations.ObservedCreate(isvc.Namespace, isvc.Name, workload.ComponentEngine, int32(idx64))
			observedPods[pod.Name] = struct{}{}
		}
	}

	wantCreatingBatches := [][]int32{{0}, {1}, {2}}
	if !reflect.DeepEqual(recorder.batches, wantCreatingBatches) {
		t.Fatalf("Creating waves: got %v, want %v", recorder.batches, wantCreatingBatches)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list Pods before Ready pass: %v", err)
	}
	for i := range pods.Items {
		makeNewPodReady(t, c, isvc.Namespace, pods.Items[i].Name, 1)
	}

	syncObserved()
	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Ready pass: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("converged Ready pass requeued: %+v", result)
	}
	wantAllBatches := [][]int32{{0}, {1}, {2}, {0, 1, 2}}
	if !reflect.DeepEqual(recorder.batches, wantAllBatches) {
		t.Fatalf("all mutation batches: got %v, want %v", recorder.batches, wantAllBatches)
	}
	for idx := int32(0); idx < 3; idx++ {
		status := recorder.statuses[idx]
		if status.Phase != workload.InstancePhaseReady || status.Operation != nil {
			t.Errorf("instance %d after convergence: got %+v", idx, status)
		}
	}
}

func TestCreate_BatchedGateDeniedInstanceConsumesNoBudget(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("batch-gate", "prod", 2)
	ir := instanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{
			Index:       0,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceFailed,
		},
		v1beta1.OMENativeInstanceStatus{
			Index:       1,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationCreate,
			},
		},
	)
	base := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-gate-engine-deadbeef", Namespace: isvc.Namespace},
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: target.Name,
		State:          workload.RetryBlockHeld,
	}}
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	podBatchSize := int32(1)
	input.ScaleUpPodBatchSize = &podBatchSize
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation

	created := make([]int32, 0, 1)
	observedClient := &podCreateObserver{Client: base, beforeCreate: func(pod *corev1.Pod) error {
		idx64, err := strconv.ParseInt(pod.Labels[query.LabelInstanceIdx], 10, 32)
		if err != nil {
			return err
		}
		created = append(created, int32(idx64))
		return nil
	}}
	_, err := ops.Create(context.Background(), workload.Deps{Client: observedClient}, input, buildPlanSinglePodEngine(2), target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{1}) {
		t.Fatalf("Creating batch after gate denial: got %v, want [[1]]", recorder.batches)
	}
	if !reflect.DeepEqual(created, []int32{1}) {
		t.Fatalf("created Instance indices: got %v, want eligible index 1", created)
	}
}

func TestCreate_BatchesSupersededAttemptRetirement(t *testing.T) {
	resetExpectations(t)
	const replicas = int32(3)
	isvc := minimalISVC("batch-retire", "prod", int(replicas))
	statuses := make([]v1beta1.OMENativeInstanceStatus, 0, replicas)
	objects := []client.Object{isvc}
	const priorRevision = "batch-retire-engine-bad0bad0"
	for idx := int32(0); idx < replicas; idx++ {
		statuses = append(statuses, v1beta1.OMENativeInstanceStatus{
			Index:       idx,
			Incarnation: 1,
			Phase:       v1beta1.OMENativeInstanceCreating,
			Operation: &v1beta1.InstanceOperation{
				ID:             fmt.Sprintf("create-%d", idx),
				Type:           v1beta1.InstanceOperationCreate,
				TargetRevision: priorRevision,
			},
		})
		leader := gangPod(isvc, idx, "leader", 0, 1, false, false)
		leader.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(priorRevision)
		objects = append(objects, leader)
	}
	objects = append(objects, instanceIR(isvc, workload.ComponentEngine, statuses...))
	base := newFakeClient(t, objects...)
	input := buildTestInput(isvc, base, workload.ComponentEngine)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.ApplyInstanceMutations = recorder.apply
	input.MutateInstance = unexpectedPerInstanceMutation
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-retire-engine-good0bad", Namespace: isvc.Namespace},
	}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  replicas,
	}
	for idx := int32(0); idx < replicas; idx++ {
		plan.Instances = append(plan.Instances, workload.InstancePlan{
			Index:       idx,
			Incarnation: 1,
			Runners:     []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		})
	}

	result, err := ops.Create(context.Background(), workload.Deps{Client: base}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("retirement batch must requeue immediately: %+v", result)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("retirement batches: got %v, want one batch [0 1 2]", recorder.batches)
	}
	for idx := int32(0); idx < replicas; idx++ {
		status := recorder.statuses[idx]
		if status.Phase != workload.InstancePhaseFailed || status.Operation != nil {
			t.Errorf("instance %d after retirement: got %+v", idx, status)
		}
	}
	pods := &corev1.PodList{}
	if err := base.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != int(replicas) {
		t.Fatalf("retirement must not create missing workers: got %d pods want %d leaders", len(pods.Items), replicas)
	}
}

func TestCreate_BatchedReadyPromotionsUseOneMutationBatch(t *testing.T) {
	resetExpectations(t)
	isvc, c, input := newReadyBatchFixture(t, "batch-ready", 3)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		if err := requireMutationPodsServing(ctx, c, isvc, mutations); err != nil {
			return err
		}
		return recorder.apply(ctx, mutations)
	}

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("fully ready batch requeued: %+v", result)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("Ready batches: got %v, want one batch [0 1 2]", recorder.batches)
	}
	for idx := int32(0); idx < 3; idx++ {
		status := recorder.statuses[idx]
		if status.Phase != workload.InstancePhaseReady || status.Operation != nil {
			t.Errorf("instance %d after Ready batch: got %+v", idx, status)
		}
	}
}

func TestCreate_BatchedReadyOnRevisionPromotesAndPrunesOnce(t *testing.T) {
	resetExpectations(t)
	isvc, c, input := newReadyBatchFixture(t, "batch-ready-revision", 3)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "batch-ready-revision-engine-" + testRevisionHash,
			Namespace: isvc.Namespace,
		},
	}
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	for idx := int32(0); idx < 3; idx++ {
		status := recorder.statuses[idx]
		status.TargetRevision = target.Name
		recorder.statuses[idx] = status
	}
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		if err := requireMutationPodsServing(ctx, c, isvc, mutations); err != nil {
			return err
		}
		return recorder.apply(ctx, mutations)
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: target.Name,
		State:          workload.RetryBlockHeld,
	}}
	pruneCalls := 0
	var pruneDisposition workload.RetryBlockDisposition
	input.MutateRetryBlock = func(_ context.Context, revision string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		if revision != target.Name {
			return fmt.Errorf("prune revision: got %q, want %q", revision, target.Name)
		}
		pruneCalls++
		block := workload.RetryBlock{TargetRevision: revision, State: workload.RetryBlockHeld}
		pruneDisposition = mutate(&block)
		return nil
	}

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("fully ready revision batch requeued: %+v", result)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("Ready-on-revision batches: got %v, want [[0 1 2]]", recorder.batches)
	}
	if pruneCalls != 1 || pruneDisposition != workload.RetryBlockRemove {
		t.Fatalf("RetryBlock prune: calls=%d disposition=%v, want one Remove", pruneCalls, pruneDisposition)
	}
	for idx := int32(0); idx < 3; idx++ {
		status := recorder.statuses[idx]
		if status.Phase != workload.InstancePhaseReady || status.Operation != nil || status.RunningRevision != target.Name || status.TargetRevision != "" {
			t.Errorf("instance %d after revision promotion: got %+v", idx, status)
		}
	}
}

func TestCreate_BatchedServingMarkFailureStopsAtFailedInstance(t *testing.T) {
	resetExpectations(t)
	isvc, base, input := newReadyBatchFixture(t, "batch-serving-failure", 3)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = recorder.apply
	markErr := errors.New("serving status unavailable")
	failedPodName := query.PodName(isvc.Name, workload.ComponentEngine, 1, "default", 0)
	c := &failingPodStatusClient{Client: base, podName: failedPodName, err: markErr}

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, markErr) {
		t.Fatalf("Create error: got %v, want serving-mark failure", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0}) {
		t.Fatalf("Ready prefix after serving-mark failure: got %v, want [[0]]", recorder.batches)
	}
	for idx, wantServing := range map[int32]bool{0: true, 1: false, 2: false} {
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		if getErr := base.Get(context.Background(), key, pod); getErr != nil {
			t.Fatalf("get pod %d: %v", idx, getErr)
		}
		if got := podreadiness.IsServing(pod); got != wantServing {
			t.Errorf("pod %d serving after failed mark: got %t, want %t", idx, got, wantServing)
		}
		status := recorder.statuses[idx]
		wantPhase := workload.InstancePhaseReady
		if idx >= 1 {
			wantPhase = workload.InstancePhaseCreating
		}
		if status.Phase != wantPhase {
			t.Errorf("instance %d phase after failed mark: got %s, want %s", idx, status.Phase, wantPhase)
		}
	}
}

func TestCreate_BatchedReadyPersistenceFailureRestoresFailFastServingPrefix(t *testing.T) {
	resetExpectations(t)
	isvc, c, input := newReadyBatchFixture(t, "batch-ready-failure", 3)
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	readyErr := errors.New("Ready status unavailable")
	recorder.fail = readyErr
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = func(ctx context.Context, mutations []workload.InstanceMutation) error {
		if err := requireMutationPodsServing(ctx, c, isvc, mutations); err != nil {
			return err
		}
		return recorder.apply(ctx, mutations)
	}

	_, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(3), nil)
	if !errors.Is(err, readyErr) {
		t.Fatalf("Create error: got %v, want Ready persistence failure", err)
	}
	if len(recorder.batches) != 1 || !reflect.DeepEqual(recorder.batches[0], []int32{0, 1, 2}) {
		t.Fatalf("attempted Ready batch: got %v, want [[0 1 2]]", recorder.batches)
	}
	for idx := int32(0); idx < 3; idx++ {
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		if getErr := c.Get(context.Background(), key, pod); getErr != nil {
			t.Fatalf("get pod %d: %v", idx, getErr)
		}
		wantServing := idx == 0
		if got := podreadiness.IsServing(pod); got != wantServing {
			t.Errorf("pod %d serving after Ready batch failure: got %t, want %t", idx, got, wantServing)
		}
		if status := recorder.statuses[idx]; status.Phase != workload.InstancePhaseCreating {
			t.Errorf("instance %d after failed Ready batch: got %+v, want Creating", idx, status)
		}
	}
}

func TestCreate_BatchedRetryBlockPruneFailureRestoresFailFastPrefix(t *testing.T) {
	resetExpectations(t)
	isvc, c, input := newReadyBatchFixture(t, "batch-prune-failure", 3)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "batch-prune-failure-engine-" + testRevisionHash, Namespace: isvc.Namespace},
	}
	recorder := newBatchMutationRecorder(input.ObservedState.InstanceStatuses)
	input.MutateInstance = unexpectedPerInstanceMutation
	input.ApplyInstanceMutations = recorder.apply
	pruneErr := errors.New("RetryBlock prune unavailable")
	pruneCalls := 0
	input.MutateRetryBlock = func(_ context.Context, _ string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		pruneCalls++
		block := workload.RetryBlock{TargetRevision: target.Name, State: workload.RetryBlockHeld}
		if got := mutate(&block); got != workload.RetryBlockRemove {
			return fmt.Errorf("prune disposition: got %v, want Remove", got)
		}
		return pruneErr
	}
	events := record.NewFakeRecorder(10)

	_, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: events}, input, buildPlanSinglePodEngine(3), target)
	if !errors.Is(err, pruneErr) {
		t.Fatalf("Create error: got %v, want prune failure", err)
	}
	if pruneCalls != 1 {
		t.Fatalf("RetryBlock prune calls: got %d, want 1", pruneCalls)
	}
	if !reflect.DeepEqual(recorder.batches, [][]int32{{0, 1, 2}, {1, 2}}) {
		t.Fatalf("Ready and rollback batches: got %v, want [[0 1 2] [1 2]]", recorder.batches)
	}
	for idx := int32(0); idx < 3; idx++ {
		wantPhase := workload.InstancePhaseCreating
		wantServing := false
		if idx == 0 {
			wantPhase = workload.InstancePhaseReady
			wantServing = true
		}
		if status := recorder.statuses[idx]; status.Phase != wantPhase {
			t.Errorf("instance %d after prune failure: got %+v, want phase %s", idx, status, wantPhase)
		}
		pod := &corev1.Pod{}
		key := client.ObjectKey{
			Namespace: isvc.Namespace,
			Name:      query.PodName(isvc.Name, workload.ComponentEngine, idx, "default", 0),
		}
		if getErr := c.Get(context.Background(), key, pod); getErr != nil {
			t.Fatalf("get pod %d: %v", idx, getErr)
		}
		if got := podreadiness.IsServing(pod); got != wantServing {
			t.Errorf("pod %d serving after prune failure: got %t, want %t", idx, got, wantServing)
		}
	}
	select {
	case event := <-events.Events:
		t.Fatalf("unexpected Ready event after failed first prune: %q", event)
	default:
	}
}

func unexpectedPerInstanceMutation(_ context.Context, idx int32, _ func(*workload.InstanceStatus) bool) error {
	return fmt.Errorf("unexpected per-Instance status mutation for index %d", idx)
}
