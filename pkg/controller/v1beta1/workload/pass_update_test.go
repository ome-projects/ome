package workload_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestReconcile_UpdateGateNil_AllowsAll asserts the dispatcher treats
// a nil UpdateGate as always-allowed — workload-side unit tests and
// the IR adapter (which doesn't wire coordination gates) rely on this.
// We assert by setting up a plan that would trigger Update if the
// target diff fires, and confirming Reconcile doesn't error.
func TestReconcile_UpdateGateNil_AllowsAll(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.UpdateGate = nil
	plan := minimalPlan()

	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_UpdateRan_RequeuesAndSkipsCreate pins the
// bump-during-bump dispatch rule: when ANY Update.call fires in the per-
// Instance loop (even one returning done=true), the dispatcher MUST
// return Requeue WITHOUT running the Create pass. The Create pass
// reads ObservedState (a snapshot) which is stale wrt the mutations
// the Update calls just committed; running Create on stale state
// corrupts RunningRevision (Create promotes pods to target.Name even
// when the pods are on a different revision) and creates duplicate
// pods (Create's scale-up reads the pre-promote ActiveOrdinal and
// thinks the canonical slot is missing).
//
// We exercise the path by seeding an Instance with Phase=Updating —
// DetectUpdateTrigger returns true on this phase, so Update fires —
// and asserting the dispatcher returns Requeue. The minimal stub
// MutateInstance keeps the test focused on the dispatcher contract
// (not on per-op behavior); the surge/recreate state machines have
// their own per-op tests.
func TestReconcile_UpdateRan_RequeuesAndSkipsCreate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		// Phase=Updating triggers DetectUpdateTrigger → true regardless
		// of RunningRevision; the dispatcher MUST call Update.
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "prior-rev"},
	}
	plan := minimalPlan()
	plan.UpdateStrategy = types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain}
	// Provide a non-nil target so the Update loop is even considered.
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		// Update can error if its per-op machine refuses (e.g., InPlaceOnly
		// + ineligible diff). For this dispatch-shape test we only care
		// about the Requeue contract; ignore op-level errors and only
		// assert when the dispatcher returns nil-error.
		t.Logf("Reconcile op error (expected for dispatch-shape test): %v", err)
	}
	// The load-bearing assertion: dispatcher returned Requeue=true (or
	// RequeueAfter > 0) — Create did NOT run. A dispatcher that fell
	// through to Create would return Create's result (possibly
	// Requeue=false).
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must Requeue when any Update fired; got %+v", result)
	}
}

// TestReconcile_ScaleUpDuringUpdate_CreatesFreshIndices pins scale-out
// during a rollout: with instance-0 mid-update (Phase=
// Updating) and the plan asking for indices [0,1,2], the dispatcher must
// materialize the brand-new (surge-free) indices 1 and 2 THIS reconcile
// — not starve them behind the in-flight rollout on index 0 — then
// requeue. Index 0's status must remain Updating: the full Create-pass
// promote does not run on it (CreateFreshIndices skips non-surge-free
// indices, and the gated full Create pass at the bottom is bypassed).
func TestReconcile_ScaleUpDuringUpdate_CreatesFreshIndices(t *testing.T) {
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
	}
	// Index 0's InstanceStatus on IR.
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "llama-70b-engine"},
		Status: v1beta1.InferenceReplicaStatus{
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
				Index:           0,
				Incarnation:     1,
				Phase:           v1beta1.OMENativeInstanceUpdating,
				RunningRevision: "llama-70b-engine-priorrev",
			}},
		},
	}
	// Index 0's existing in-flight pod, so the Update pass operates on a
	// real pod and the fresh-pass create can't accidentally collide.
	pod0 := enginePod(isvc.Name, isvc.Namespace, 0)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir, pod0).Build()
	// The test owns its expectations store: the process-wide default is
	// shared with every other test in the package, and a create another
	// test expected for the same owner would hold this one's fresh
	// indices back.
	deps := types.Deps{Client: c, Expectations: types.NewExpectations()}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "llama-70b-engine-priorrev"},
	}
	in.MutateInstance = roundTripMutateInstance(c, isvc, types.ComponentEngine)
	// Plan asks for 3 single-pod instances. Index 0 is mid-update; 1,2 brand-new.
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  3,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain},
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Dispatcher must requeue (index 0 still updating) — it must NOT
	// return Create's steady-state zero result.
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must requeue while index 0 updates; got %+v", result)
	}

	// Load-bearing: the brand-new indices were materialized THIS reconcile.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	got := map[string]bool{}
	for _, p := range pods.Items {
		got[p.Name] = true
	}
	if !got["llama-70b-engine-1-default-0"] {
		t.Errorf("fresh index 1 must be created during the in-flight rollout; got %v", got)
	}
	if !got["llama-70b-engine-2-default-0"] {
		t.Errorf("fresh index 2 must be created during the in-flight rollout; got %v", got)
	}

	// Index 0's status must still be Updating — the full Create promote
	// did not run on it.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s0 := instanceStatusByIndex(c, fresh, v1beta1.EngineComponent, 0)
	if s0 == nil || s0.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("index 0 must remain Phase=Updating (Create promote must not run on it); got %+v", s0)
	}
}

// TestHeldByPartition pins the partition count predicate: a hold
// candidate is held iff its rank in the candidate order is below a
// positive partition (PartitionHeldIndices owns candidate membership).
// A nil partition or partition<=0 holds nothing.
func TestHeldByPartition(t *testing.T) {
	part := func(n int32) *int32 { return &n }
	cases := []struct {
		name      string
		partition *int32
		rank      int32
		held      bool
	}{
		{"nil Partition", nil, 0, false},
		{"Partition=0 holds nothing", part(0), 0, false},
		{"Partition=2 holds rank 0", part(2), 0, true},
		{"Partition=2 holds rank 1", part(2), 1, true},
		{"Partition=2 rolls rank 2", part(2), 2, false},
		{"Partition=2 rolls rank 3", part(2), 3, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := workloadops.HeldByPartition(tc.partition, tc.rank); got != tc.held {
				t.Errorf("HeldByPartition(rank=%d) = %v, want %v", tc.rank, got, tc.held)
			}
		})
	}
}

// TestEffectivePartition pins the source precedence of the partition
// hold: the projected pacing partition wins whenever it is set — an
// explicit 0 releases every Instance even over a user partition — and
// only a nil pacing partition defers to the user's RollingUpdate.
func TestEffectivePartition(t *testing.T) {
	part := func(n int32) *int32 { return &n }
	cases := []struct {
		name   string
		pacing *types.WorkloadPacing
		ru     *types.RollingUpdate
		want   *int32
	}{
		{"neither set", nil, nil, nil},
		{"user partition only", nil, &types.RollingUpdate{Partition: part(2)}, part(2)},
		{"pacing partition only", &types.WorkloadPacing{Partition: part(3)}, nil, part(3)},
		{"pacing wins over user", &types.WorkloadPacing{Partition: part(3)}, &types.RollingUpdate{Partition: part(2)}, part(3)},
		{"pacing 0 releases over user", &types.WorkloadPacing{Partition: part(0)}, &types.RollingUpdate{Partition: part(2)}, part(0)},
		{"pacing block without partition defers", &types.WorkloadPacing{}, &types.RollingUpdate{Partition: part(2)}, part(2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := workloadops.EffectivePartition(tc.pacing, tc.ru)
			switch {
			case got == nil && tc.want == nil:
			case got == nil || tc.want == nil || *got != *tc.want:
				t.Errorf("EffectivePartition = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestReconcile_NoBudget_NoGate pins the raw uncapped dispatcher
// contract: with UpdateGate nil and a nil per-Component RollingUpdate
// (both PerComponentMax{Surge,Unavailable}Budget resolve to
// BudgetNoLimit), NOTHING throttles how many Instances start a fresh
// update in ONE pass. Seed 8 Ready Instances all on a prior revision and
// the dispatcher starts a fresh surge on ALL 8 in a single reconcile.
//
// This is the raw dispatch shape. In a cluster the per-Component
// RollingUpdate is inherited from the ServingRuntime or the configured
// cluster default, so PerComponentMax*Budget != -1 and the loop's
// projected-vs-budget check caps fresh starts per pass. With neither
// (nil RollingUpdate here) the dispatcher is uncapped by design — the
// group-level UpdateGate is the only other layer, and it is nil too. The
// test asserts that uncapped contract so a change that silently re-caps
// (or fails to dispatch) the raw path is caught.
func TestReconcile_NoBudget_NoGate(t *testing.T) {
	const replicas = int32(8)

	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	// No UpdateGate: the cross-Component coordination layer is out of the
	// loop, matching the IR adapter and webhook-bypassed shape.
	in.UpdateGate = nil
	in.MutateInstance = roundTripMutateInstance(c, isvc, types.ComponentEngine)

	// 8 Ready Instances, all on a prior revision → DetectUpdateTrigger's
	// cheap RunningRevision != target.Name fast-path fires for every one,
	// and startingFresh is true (Phase != Updating).
	observed := make([]types.InstanceStatus, 0, replicas)
	instances := make([]types.InstancePlan, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		observed = append(observed, types.InstanceStatus{
			Index: i, Incarnation: 1, Phase: types.InstancePhaseReady,
			RunningRevision: "llama-70b-engine-priorrev",
		})
		instances = append(instances, types.InstancePlan{
			Index: i, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}},
		})
	}
	in.ObservedState.InstanceStatuses = observed

	// SurgeThenDrain + nil RollingUpdate → BudgetNoLimit on both layers.
	plan := types.ComponentPlan{
		Component:      types.ComponentEngine,
		Replicas:       replicas,
		Instances:      instances,
		UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain},
	}
	// Sanity-check the premise: both per-Component budgets are uncapped.
	if got := escalation.PerComponentMaxSurgeBudget(plan.UpdateStrategy.RollingUpdate, replicas); got != escalation.BudgetNoLimit {
		t.Fatalf("premise: MaxSurge budget must be BudgetNoLimit for nil RollingUpdate, got %d", got)
	}
	if got := escalation.PerComponentMaxUnavailableBudget(plan.UpdateStrategy.RollingUpdate, replicas); got != escalation.BudgetNoLimit {
		t.Fatalf("premise: MaxUnavailable budget must be BudgetNoLimit for nil RollingUpdate, got %d", got)
	}

	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Every Instance surged this pass → all done=false → requeue.
	if result.RequeueAfter == 0 {
		t.Errorf("uncapped fleet dispatch leaves all instances updating; expected requeue, got %+v", result)
	}

	// Load-bearing: count Instances that started a fresh op in ONE pass.
	// Each fresh surge stamps Phase=Updating + Operation.Step=Surge. With
	// no budget and no gate, all 8 start in this single pass.
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	started := int32(0)
	for i := int32(0); i < replicas; i++ {
		s := instanceStatusByIndex(c, fresh, v1beta1.EngineComponent, i)
		if s != nil && s.Phase == v1beta1.OMENativeInstanceUpdating {
			started++
		}
	}
	if started != replicas {
		t.Errorf("uncapped dispatcher must start a fresh update on all %d instances in ONE pass; got %d", replicas, started)
	}
}

// TestReconcile_InPlaceMetadataUpdateProgressesWithinMaxUnavailable verifies
// that a metadata-only rollout holds its MaxUnavailable slot until a fresh
// observation promotes the current Instance, then advances to the next index.
// The runtime may report a different repository alias for the unchanged image.
func TestReconcile_InPlaceMetadataUpdateProgressesWithinMaxUnavailable(t *testing.T) {
	scheme := makeScheme(t)
	spec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}}
	runningName := "llama-70b-engine-oldhash"
	targetName := "llama-70b-engine-newhash"
	runningRaw, err := json.Marshal(revision.DataPayload{
		PodSpec: spec,
		PodMeta: &metav1.ObjectMeta{Annotations: map[string]string{"release": "one"}},
	})
	if err != nil {
		t.Fatalf("marshal running revision: %v", err)
	}
	targetRaw, err := json.Marshal(revision.DataPayload{
		PodSpec: spec,
		PodMeta: &metav1.ObjectMeta{Annotations: map[string]string{"release": "two"}},
	})
	if err != nil {
		t.Fatalf("marshal target revision: %v", err)
	}
	running := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: runningName, Namespace: "prod"}}
	running.Data.Raw = runningRaw
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetName, Namespace: "prod"}}
	target.Data.Raw = targetRaw

	labelsFor := func(index string) map[string]string {
		return map[string]string{
			constants.InferenceServicePodLabelKey: "llama-70b",
			constants.OMEComponentLabel:           string(types.ComponentEngine),
			query.LabelManagedBy:                  query.ManagedByOMENative,
			query.LabelInstanceIdx:                index,
			query.LabelInstanceIncarnation:        "1",
			query.LabelRunner:                     "default",
			query.LabelPodOrdinal:                 "0",
			query.LabelRevisionHash:               "oldhash",
		}
	}
	// A steady-state serving pod: kubelet has folded the satisfied serving
	// gate into PodReady, which is what the shared promote bar reads.
	readyConditions := []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}
	podFor := func(index string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "llama-70b-engine-" + index + "-default-0",
				Namespace:   "prod",
				Labels:      labelsFor(index),
				Annotations: map[string]string{"release": "one"},
			},
			Spec: *spec.DeepCopy(),
			Status: corev1.PodStatus{
				Conditions:        append([]corev1.PodCondition(nil), readyConditions...),
				ContainerStatuses: []corev1.ContainerStatus{{Name: "main", Image: "mirror.example.com/test:v1"}},
			},
		}
	}
	pod0 := podFor("0")
	pod1 := podFor("1")
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine", Namespace: "prod"},
		Status: v1beta1.InferenceReplicaStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: runningName},
			{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: runningName},
		}},
	}
	live := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithObjects(running, target, ir, pod0, pod1).
		Build()
	cached := &staleInPlacePodClient{
		Client: live,
		stale:  true,
		pods:   []*corev1.Pod{pod0.DeepCopy(), pod1.DeepCopy()},
	}
	deps := types.Deps{Client: cached, APIReader: live}
	markNotReady := false
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: types.UpdateStrategy{
			Type: types.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &types.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: &markNotReady,
			},
			RollingUpdate: &types.RollingUpdate{MaxUnavailable: intOrStringInt(1)},
		},
	}

	inputWithStatuses := func(statuses []types.InstanceStatus) types.ReconcileInput {
		input := minimalInput(t)
		input.DesiredSpec.Replicas = 2
		input.DesiredSpec.PodSpec = spec
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), statuses...)
		isvc := input.OwnerObject.(*v1beta1.InferenceService)
		input.MutateInstance = roundTripMutateInstance(live, isvc, types.ComponentEngine)
		return input
	}
	staleStatuses := make([]types.InstanceStatus, 0, len(ir.Status.InstanceStatuses))
	for _, status := range ir.Status.InstanceStatuses {
		staleStatuses = append(staleStatuses, v1beta1convert.InstanceStatusToWorkload(status))
	}
	staleInput := func() types.ReconcileInput {
		return inputWithStatuses(staleStatuses)
	}
	inputFromAPI := func() types.ReconcileInput {
		current := &v1beta1.InferenceReplica{}
		if err := live.Get(context.Background(), client.ObjectKeyFromObject(ir), current); err != nil {
			t.Fatalf("get InferenceReplica: %v", err)
		}
		statuses := make([]types.InstanceStatus, 0, len(current.Status.InstanceStatuses))
		for _, status := range current.Status.InstanceStatuses {
			statuses = append(statuses, v1beta1convert.InstanceStatusToWorkload(status))
		}
		return inputWithStatuses(statuses)
	}
	assertStatus := func(index int32, phase v1beta1.OMENativeInstancePhase, runningRevision string) {
		t.Helper()
		status := instanceStatusByIndex(live, inputFromAPI().OwnerObject.(*v1beta1.InferenceService), v1beta1.EngineComponent, index)
		if status == nil {
			t.Fatalf("instance %d status not found", index)
		}
		if status.Phase != phase || status.RunningRevision != runningRevision {
			t.Errorf("instance %d: phase=%q runningRevision=%q, want phase=%q runningRevision=%q",
				index, status.Phase, status.RunningRevision, phase, runningRevision)
		}
	}
	assertPodRevision := func(pod *corev1.Pod, annotation, hash string) {
		t.Helper()
		got := &corev1.Pod{}
		if err := live.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
			t.Fatalf("get pod %s: %v", pod.Name, err)
		}
		if got.Annotations["release"] != annotation || got.Labels[query.LabelRevisionHash] != hash {
			t.Errorf("pod %s: release=%q hash=%q, want release=%q hash=%q",
				pod.Name, got.Annotations["release"], got.Labels[query.LabelRevisionHash], annotation, hash)
		}
	}

	result, err := workload.Reconcile(context.Background(), deps, staleInput(), plan, target)
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if result.RequeueAfter != testRequeueIntervals.Operation {
		t.Errorf("first Reconcile requeue: got %v want %v", result.RequeueAfter, testRequeueIntervals.Operation)
	}
	assertStatus(0, v1beta1.OMENativeInstanceUpdating, runningName)
	assertStatus(1, v1beta1.OMENativeInstanceReady, runningName)
	assertPodRevision(pod0, "two", "newhash")
	assertPodRevision(pod1, "one", "oldhash")

	result, err = workload.Reconcile(context.Background(), deps, staleInput(), plan, target)
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Errorf("second Reconcile must retain a follow-up requeue, got %v", result.RequeueAfter)
	}
	assertStatus(0, v1beta1.OMENativeInstanceReady, targetName)
	assertStatus(1, v1beta1.OMENativeInstanceReady, runningName)
	assertPodRevision(pod1, "one", "oldhash")

	cached.stale = false
	if _, err := workload.Reconcile(context.Background(), deps, inputFromAPI(), plan, target); err != nil {
		t.Fatalf("third Reconcile: %v", err)
	}
	assertStatus(1, v1beta1.OMENativeInstanceUpdating, runningName)
	assertPodRevision(pod1, "two", "newhash")

	if _, err := workload.Reconcile(context.Background(), deps, inputFromAPI(), plan, target); err != nil {
		t.Fatalf("fourth Reconcile: %v", err)
	}
	assertStatus(1, v1beta1.OMENativeInstanceReady, targetName)
}

// TestReconcile_StrategyFlipMidSurge_RespectsUnavailabilityBudget pins the
// budget contract across a mid-rollout strategy change.
//
// The two per-Component budgets are accounted from InstanceOperation.Step,
// but the dispatcher chooses WHICH budget a fresh start is charged to from
// the live strategy. An Instance admitted under maxSurge and still carrying
// a surge step therefore contributes nothing to the unavailability count —
// even on the pass where the flipped strategy is about to drive it as an
// unavailable update. The dispatcher reads that as unused headroom and
// admits a fresh start on top.
//
// The invariant asserted here is mode-agnostic and holds under any fix: at
// the end of one pass, no more Instances may be driven as unavailable-mode
// updates than maxUnavailable allows, and no more as surge-mode updates
// than maxSurge allows. It does not presume whether the in-flight Instance
// keeps surging or is handed to the new mode — only that whichever happens,
// it is counted.
//
// The Instances here carry no recorded running-revision baseline, so
// InPlaceIfPossible resolves to recreate exactly as it does in production
// against an unrecorded baseline. Both flipped arms therefore drive the
// recreate drain; both are unavailable modes, which is the property the
// budget is denominated in. The genuine image-patch path is covered by the
// per-Instance tests in workload/ops.
func TestReconcile_StrategyFlipMidSurge_RespectsUnavailabilityBudget(t *testing.T) {
	const (
		maxSurge       = 1
		maxUnavailable = 1
	)
	for _, tc := range []struct {
		name     string
		strategy types.UpdateStrategyType
	}{
		{"SurgeThenDrain retained", types.UpdateStrategySurgeThenDrain},
		{"flipped to InPlaceIfPossible", types.UpdateStrategyInPlaceIfPossible},
		{"flipped to RecreatePod", types.UpdateStrategyRecreatePod},
		// InPlaceOnly is deliberately absent. These Instances carry no recorded
		// running-revision baseline, so InPlaceOnly cannot prove the diff is
		// image-only and the mode chooser errors out before the pass reaches any
		// budget decision — the row would assert nothing. Its dispatch is covered
		// by the per-Instance tests in workload/ops, which do seed a baseline.
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := makeScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			deps := types.Deps{Client: c}

			in := minimalInput(t)
			in.DesiredSpec.Replicas = 4
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				// Index 0 was admitted under maxSurge on a prior wake-up and
				// still carries its surge step.
				{
					Index: 0, Incarnation: 1,
					Phase: types.InstancePhaseUpdating, RunningRevision: "prior-rev",
					Operation: &types.InstanceOperation{
						Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge,
						TargetRevision: "llama-70b-engine-newtarget",
					},
				},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
				{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
				{Index: 3, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
			}
			// Record the mode each Instance was actually driven as, at the
			// moment its Update operation is stamped. Reading Operation after
			// the pass would miss it: with no pods in the fake client the ops
			// escalate and clear the operation before the pass returns.
			drivenAs := map[int32]string{}
			in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
				for i := range in.ObservedState.InstanceStatuses {
					if in.ObservedState.InstanceStatuses[i].Index != idx {
						continue
					}
					s := &in.ObservedState.InstanceStatuses[i]
					_ = fn(s)
					if _, seen := drivenAs[idx]; !seen &&
						s.Operation != nil && s.Operation.Type == types.InstanceOperationUpdate {
						drivenAs[idx] = s.Operation.Step
					}
					break
				}
				return nil
			}

			instances := make([]types.InstancePlan, 4)
			for i := range instances {
				instances[i] = types.InstancePlan{
					Index: int32(i), Incarnation: 1,
					Runners: []types.RunnerPlan{{Name: "default", Size: 1}},
				}
			}
			plan := types.ComponentPlan{
				Component: types.ComponentEngine,
				Replicas:  4,
				Instances: instances,
				UpdateStrategy: types.UpdateStrategy{
					Type: tc.strategy,
					RollingUpdate: &types.RollingUpdate{
						MaxSurge:       intOrStringInt(maxSurge),
						MaxUnavailable: intOrStringInt(maxUnavailable),
					},
				},
			}
			target := &appsv1.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
			}

			if _, err := workload.Reconcile(context.Background(), deps, in, plan, target); err != nil {
				// Per-op state machines can error against an empty fake client
				// (no live pods to drain). The contract under test is the
				// budget accounting, not the per-op outcome.
				t.Logf("Reconcile op error (expected against empty fake client): %v", err)
			}

			var unavailable, surging []int32
			for idx, step := range drivenAs {
				if unavailableModeStep(step) {
					unavailable = append(unavailable, idx)
					continue
				}
				surging = append(surging, idx)
			}
			slices.Sort(unavailable)
			slices.Sort(surging)

			if len(unavailable) > maxUnavailable {
				t.Errorf("%d Instances driven as unavailable-mode updates (indices %v, steps %v) against maxUnavailable=%d",
					len(unavailable), unavailable, drivenAs, maxUnavailable)
			}
			if len(surging) > maxSurge {
				t.Errorf("%d Instances driven as surge-mode updates (indices %v, steps %v) against maxSurge=%d",
					len(surging), surging, drivenAs, maxSurge)
			}
		})
	}
}

// runnerPod is enginePod for one Runner of instance idx, so a gang
// (leader + worker) can be seeded pod by pod.
func runnerPod(isvc, ns string, idx int32, runner string) *corev1.Pod {
	pod := enginePod(isvc, ns, idx)
	pod.Name = query.PodName(isvc, types.ComponentEngine, idx, runner, 0)
	pod.UID = ktypes.UID(pod.Name + "-uid")
	pod.Labels[query.LabelRunner] = runner
	return pod
}

// TestReconcile_GangInPlaceFallbackIsGatedAsRecreate pins the mechanism
// the update pass reports to the coordination gate. The gate waives its
// capacity checks for an in-place start because the patch returns the
// same pod; a gang under an in-place strategy never patches, it
// recreates, so its start must reach the gate as RecreatePod and be
// held whenever the gate would hold a recreate. A single-pod in-place
// start keeps its declared strategy. The gate here models the waiver:
// it admits an in-place mechanism and holds every other one on Ratio.
func TestReconcile_GangInPlaceFallbackIsGatedAsRecreate(t *testing.T) {
	gang := []types.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	single := []types.RunnerPlan{{Name: "default", Size: 1}}
	cases := []struct {
		name        string
		strategy    types.UpdateStrategyType
		runners     []types.RunnerPlan
		wantConsult types.UpdateStrategyType
		wantHeld    bool
	}{
		{"gang InPlaceIfPossible is consulted as a recreate and held", types.UpdateStrategyInPlaceIfPossible, gang, types.UpdateStrategyRecreatePod, true},
		{"gang InPlaceOnly is consulted as a recreate and held", types.UpdateStrategyInPlaceOnly, gang, types.UpdateStrategyRecreatePod, true},
		{"gang RecreatePod is held", types.UpdateStrategyRecreatePod, gang, types.UpdateStrategyRecreatePod, true},
		{"single-pod InPlaceIfPossible keeps the waiver", types.UpdateStrategyInPlaceIfPossible, single, types.UpdateStrategyInPlaceIfPossible, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := makeScheme(t)
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
			}
			in := minimalInput(t)
			// The running revision differs from the target by an image only,
			// so a single-pod start is genuinely an in-place patch.
			running := in.DesiredSpec.PodSpec.DeepCopy()
			running.Containers[0].Image = "test:v0"
			objs := []client.Object{isvc, revisionWithPodSpec(t, "llama-70b-engine-priorrev", isvc.Namespace, running)}
			for _, r := range tc.runners {
				objs = append(objs, runnerPod(isvc.Name, isvc.Namespace, 0, r.Name))
			}
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
				WithObjects(objs...).Build()
			deps := types.Deps{Client: c, Expectations: types.NewExpectations()}

			in.MutateInstance = roundTripMutateInstance(c, isvc, types.ComponentEngine)
			podCount := int32(len(tc.runners))
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{{
				Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady,
				RunningRevision: "llama-70b-engine-priorrev", PodCount: podCount, ServingPodCount: podCount,
			}}
			var consulted []types.UpdateStrategyType
			in.UpdateGate = func(strategy types.UpdateStrategyType, _, _ int32) (bool, types.RolloutHoldGate, string) {
				consulted = append(consulted, strategy)
				if strategy == types.UpdateStrategyInPlaceIfPossible || strategy == types.UpdateStrategyInPlaceOnly {
					return true, "", ""
				}
				return false, types.RolloutHoldGateRatio, "projected serving ratio leaves the band"
			}
			var hold *types.RolloutHold
			in.RecordRolloutHold = func(h *types.RolloutHold) { hold = h }

			plan := types.ComponentPlan{
				Component:      types.ComponentEngine,
				Replicas:       1,
				Instances:      []types.InstancePlan{{Index: 0, Incarnation: 1, Runners: tc.runners}},
				UpdateStrategy: types.UpdateStrategy{Type: tc.strategy},
			}
			target := &appsv1.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
			}

			_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
			if want := []types.UpdateStrategyType{tc.wantConsult}; !slices.Equal(consulted, want) {
				t.Errorf("gate consulted with %v, want %v", consulted, want)
			}
			if !tc.wantHeld {
				// The admitted start runs its op against the fake client; the
				// op's outcome belongs to the op's own tests.
				if err != nil {
					t.Logf("op error after an admitted start: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if hold == nil || hold.Gate != types.RolloutHoldGateRatio {
				t.Errorf("RolloutHold = %+v, want the gate's Ratio hold", hold)
			}
			// A held start touches nothing: every pod of the Instance is still
			// there and none is terminating.
			pods := &corev1.PodList{}
			if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			if len(pods.Items) != len(tc.runners) {
				t.Errorf("held start left %d pods, want %d", len(pods.Items), len(tc.runners))
			}
			for i := range pods.Items {
				if pods.Items[i].DeletionTimestamp != nil {
					t.Errorf("held start is draining %s", pods.Items[i].Name)
				}
			}
			if s := instanceStatusByIndex(c, isvc, v1beta1.EngineComponent, 0); s != nil && s.Phase != v1beta1.OMENativeInstanceReady {
				t.Errorf("held start moved the row to %s, want Ready", s.Phase)
			}
		})
	}
}

// TestReconcile_SinglePodInPlaceDiffIsGatedAsRecreate pins the mechanism a
// single-pod InPlaceIfPossible start reports to the coordination gate. The
// gate waives its capacity checks for an in-place start because the patch
// returns the same pod; a diff beyond regular-container images, or a
// running revision that is gone, resolves the start to a recreate, which
// drains the pod before anything returns, so it must reach the gate as
// RecreatePod and be held whenever the gate would hold a recreate. An
// image-only diff keeps the waiver. The gate here models the waiver: it
// admits an in-place mechanism and holds every other one on Ratio.
func TestReconcile_SinglePodInPlaceDiffIsGatedAsRecreate(t *testing.T) {
	const runningName = "llama-70b-engine-priorrev"
	cases := []struct {
		name        string
		running     func(target *corev1.PodSpec) *corev1.PodSpec // nil: the revision is gone
		wantConsult types.UpdateStrategyType
		wantHeld    bool
	}{
		{"image-only diff keeps the waiver", func(target *corev1.PodSpec) *corev1.PodSpec {
			running := target.DeepCopy()
			running.Containers[0].Image = "test:v0"
			return running
		}, types.UpdateStrategyInPlaceIfPossible, false},
		{"diff beyond images is consulted as a recreate and held", func(target *corev1.PodSpec) *corev1.PodSpec {
			running := target.DeepCopy()
			running.Containers[0].Env = []corev1.EnvVar{{Name: "MODE", Value: "batch"}}
			return running
		}, types.UpdateStrategyRecreatePod, true},
		{"gone running revision is consulted as a recreate and held", nil, types.UpdateStrategyRecreatePod, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := makeScheme(t)
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
			}
			in := minimalInput(t)
			objs := []client.Object{isvc, enginePod(isvc.Name, isvc.Namespace, 0)}
			if tc.running != nil {
				objs = append(objs, revisionWithPodSpec(t, runningName, isvc.Namespace, tc.running(in.DesiredSpec.PodSpec)))
			}
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
				WithObjects(objs...).Build()
			deps := types.Deps{Client: c, Expectations: types.NewExpectations()}

			in.MutateInstance = roundTripMutateInstance(c, isvc, types.ComponentEngine)
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{{
				Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady,
				RunningRevision: runningName, PodCount: 1, ServingPodCount: 1,
			}}
			var consulted []types.UpdateStrategyType
			in.UpdateGate = func(strategy types.UpdateStrategyType, _, _ int32) (bool, types.RolloutHoldGate, string) {
				consulted = append(consulted, strategy)
				if strategy == types.UpdateStrategyInPlaceIfPossible || strategy == types.UpdateStrategyInPlaceOnly {
					return true, "", ""
				}
				return false, types.RolloutHoldGateRatio, "projected serving ratio leaves the band"
			}
			var hold *types.RolloutHold
			in.RecordRolloutHold = func(h *types.RolloutHold) { hold = h }

			plan := types.ComponentPlan{
				Component:      types.ComponentEngine,
				Replicas:       1,
				Instances:      []types.InstancePlan{{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}}},
				UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategyInPlaceIfPossible},
			}
			target := &appsv1.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
			}

			_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
			if want := []types.UpdateStrategyType{tc.wantConsult}; !slices.Equal(consulted, want) {
				t.Errorf("gate consulted with %v, want %v", consulted, want)
			}
			if !tc.wantHeld {
				// The admitted start runs its op against the fake client; the
				// op's outcome belongs to the op's own tests.
				if err != nil {
					t.Logf("op error after an admitted start: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if hold == nil || hold.Gate != types.RolloutHoldGateRatio {
				t.Errorf("RolloutHold = %+v, want the gate's Ratio hold", hold)
			}
			// A held start touches nothing: the pod is still there, not
			// terminating, and the row is still Ready.
			pods := &corev1.PodList{}
			if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			if len(pods.Items) != 1 {
				t.Errorf("held start left %d pods, want 1", len(pods.Items))
			}
			for i := range pods.Items {
				if pods.Items[i].DeletionTimestamp != nil {
					t.Errorf("held start is draining %s", pods.Items[i].Name)
				}
			}
			if s := instanceStatusByIndex(c, isvc, v1beta1.EngineComponent, 0); s != nil && s.Phase != v1beta1.OMENativeInstanceReady {
				t.Errorf("held start moved the row to %s, want Ready", s.Phase)
			}
		})
	}
}

// inPlaceOnlyHarness converges replicas single-pod Instances on v1 under
// InPlaceOnly with the given unavailability budget and restart policy,
// with the runner container named as production names it so container
// restart evidence is read.
func inPlaceOnlyHarness(t *testing.T, replicas int32, maxUnavailable intstr.IntOrString, policy types.RestartPolicy) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(64)
	h.replicas = replicas
	maxSurge := intstr.FromInt32(0)
	h.lifecycle = types.Lifecycle{
		RestartPolicy: &policy,
		UpdateStrategy: &types.UpdateStrategy{
			Type:          types.UpdateStrategyInPlaceOnly,
			RollingUpdate: &types.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
		},
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}
	return h
}

// podUIDByInstance maps every planned Instance to the UID of its one pod.
func (h *recoveryHarness) podUIDByInstance(replicas int32) map[int32]ktypes.UID {
	h.t.Helper()
	uids := make(map[int32]ktypes.UID, replicas)
	for idx := int32(0); idx < replicas; idx++ {
		pods := h.podsOf(idx)
		if len(pods) != 1 {
			h.t.Fatalf("instance %d has %d live pods, want 1", idx, len(pods))
		}
		uids[idx] = pods[0].UID
	}
	return uids
}

// TestReconcile_InPlaceOnly_ImagePushRollsEveryInstance: four single-pod
// Instances serve v1 under InPlaceOnly when an image push lands. Each pod
// is patched where it runs: the kubelet kills the old container, the new
// image comes up under the same pod UID, and the Instance is promoted on
// the new revision once it is Ready again. The budget admits the next
// Instance as soon as the promoted one is back in rotation, so the roll
// walks every Instance one at a time, never surges, never recreates a pod
// and never opens a repair; it ends with every Instance Ready on the new
// revision. Under maxUnavailable 1 and 25 percent, both restart policies,
// and whether the node reports the new image under its own name or under
// the old one, as a node that holds one image under two tags does.
func TestReconcile_InPlaceOnly_ImagePushRollsEveryInstance(t *testing.T) {
	const replicas = 4
	budgets := []struct {
		name           string
		maxUnavailable intstr.IntOrString
	}{
		{"maxUnavailable-1", intstr.FromInt32(1)},
		{"maxUnavailable-25pct", intstr.FromString("25%")},
	}
	nodes := []struct {
		name            string
		imageNameOnNode map[string]string
	}{
		{"distinct-images", nil},
		{"second-tag-of-one-image", map[string]string{fixedImage: goodImage}},
	}
	for _, budget := range budgets {
		for _, policy := range []types.RestartPolicy{types.RestartPolicyNone, types.RestartPolicyRecreateInstance} {
			for _, node := range nodes {
				t.Run(fmt.Sprintf("%s/%s/%s", budget.name, policy, node.name), func(t *testing.T) {
					h := inPlaceOnlyHarness(t, replicas, budget.maxUnavailable, policy)
					h.imageNameOnNode = node.imageNameOnNode
					uids := h.podUIDByInstance(replicas)
					allowed := escalation.PerComponentMaxUnavailableBudget(h.lifecycle.UpdateStrategy.RollingUpdate, replicas)
					h.events = nil
					h.setTarget(h.revFixed, fixedImage)

					oneAtATimeInPlace := func() {
						if n := escalation.CurrentUnavailableInFlight(h.irStatuses()); n > allowed {
							h.dumpState("budget")
							t.Fatalf("%d Instances offline for the roll, budget %d", n, allowed)
						}
						offline := int32(0)
						for idx := int32(0); idx < replicas; idx++ {
							if !h.instanceServes(idx) {
								offline++
							}
							pods := h.podsOf(idx)
							if len(pods) != 1 || pods[0].UID != uids[idx] {
								h.dumpState("pod identity")
								t.Fatalf("instance %d no longer runs the pod it had before the push: an in-place roll patches, it does not recreate", idx)
							}
						}
						if offline > allowed {
							h.dumpState("floor")
							t.Fatalf("%d Instances out of rotation, want at most %d", offline, allowed)
						}
						if h.repairInFlight() {
							h.dumpState("repair")
							t.Fatalf("a repair opened on an Instance the roll was patching")
						}
					}
					if !h.runWithInvariant(160, func() bool { return h.settledOn(h.revFixed, replicas) }, oneAtATimeInPlace) {
						h.dumpState("after the push")
						t.Fatalf("the roll stopped: %d of %d Instances Ready on the new revision", len(h.landedOn(h.revFixed)), replicas)
					}
					for idx := int32(0); idx < replicas; idx++ {
						h.requirePodsRender(idx, h.revFixed, fixedImage)
					}
					if h.sawEvent(types.EventReasonRestartTriggered) {
						t.Fatalf("a restart was recorded during an in-place roll: %v", h.events)
					}
				})
			}
		}
	}
}

// livePodProbesNeverReady reports whether any live pod still carries the
// never-ready probe.
func (h *recoveryHarness) livePodProbesNeverReady() bool {
	for _, pod := range h.livePods() {
		if probesNeverReadyPath(pod) {
			return true
		}
	}
	return false
}

// inPlaceIfPossibleHarness is replicas single-pod Instances settled on v1
// under InPlaceIfPossible with maxSurge 0 and the given maxUnavailable.
// gated lists the Component in a rollout group with a run open.
func inPlaceIfPossibleHarness(t *testing.T, replicas, maxUnavailable int32, gated bool) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(256)
	h.replicas = replicas
	maxSurge := intstr.FromInt32(0)
	unavail := intstr.FromInt32(maxUnavailable)
	h.lifecycle = types.Lifecycle{
		UpdateStrategy: &types.UpdateStrategy{
			Type:          types.UpdateStrategyInPlaceIfPossible,
			RollingUpdate: &types.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &unavail},
		},
	}
	if gated {
		consults := 0
		h.gate = rollingGroupGate(h, replicas, 0, maxUnavailable, &consults)
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}
	return h
}

// requireRollWithinUnavailableBudget fails the test when more than budget
// Instances are offline for the roll, fewer than replicas-budget serve, or
// a repair has opened on an Instance the roll owns.
func requireRollWithinUnavailableBudget(t *testing.T, h *recoveryHarness, replicas, budget int32) {
	t.Helper()
	if n := escalation.CurrentUnavailableInFlight(h.irStatuses()); n > budget {
		h.dumpState("budget")
		t.Fatalf("%d Instances offline for the roll, budget %d", n, budget)
	}
	if n := h.servingInstances(); n < replicas-budget {
		h.dumpState("floor")
		t.Fatalf("%d Instances in rotation, floor %d", n, replicas-budget)
	}
	if h.repairInFlight() {
		h.dumpState("repair")
		t.Fatalf("a repair opened on an Instance the roll owns")
	}
}

// TestRollback_HeldInPlacePushOnNeverReadyRevisionLandsTheStartingRevision:
// four single-pod Instances serve v1 under InPlaceIfPossible with
// maxUnavailable 1. A push whose diff reaches past the container images
// (a readiness probe) rolls as a recreate, and the pod it rebuilds runs
// without ever turning Ready, so the roll parks at its budget: one
// Instance dark on the new revision, three serving v1. Re-applying v1
// then lands v1 on every Instance within that budget. The parked
// Instance's pod was rendered from the new revision, so no in-place patch
// brings it back: its attempt is rebuilt at v1, no pod of the new
// revision is left, the three serving Instances keep their pods and stay
// in rotation, and no repair opens. Standalone and listed in a rollout
// group with a run open.
func TestRollback_HeldInPlacePushOnNeverReadyRevisionLandsTheStartingRevision(t *testing.T) {
	const replicas = 4
	for _, shape := range rollShapes {
		t.Run(shape.name, func(t *testing.T) {
			h := inPlaceIfPossibleHarness(t, replicas, 1, shape.gated)
			uids := h.podUIDByInstance(replicas)

			// The new revision: v1's image behind a readiness probe its pod
			// never passes, a diff no image patch can apply or undo.
			stuckSpec := h.podSpec(goodImage)
			stuckSpec.Containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: neverReadyPath, Port: intstr.FromInt32(8080)},
			}}
			revStuck := h.ensureRevision(stuckSpec)
			stuckHash := query.RevisionOf(revStuck).Hash()
			h.setTarget(revStuck, goodImage)
			h.desired.PodSpec = stuckSpec

			withinBudget := func() { requireRollWithinUnavailableBudget(t, h, replicas, 1) }
			// Parked: the rebuilt Instance runs the new revision's pod with
			// no readiness, the other three are untouched on v1.
			parked := func() bool {
				s := h.instance(0)
				if s == nil || s.Phase != types.InstancePhaseUpdating || s.Operation == nil || s.Operation.TargetRevision != revStuck.Name {
					return false
				}
				pods := h.podsOf(0)
				if len(pods) != 1 || pods[0].Labels[query.LabelRevisionHash] != stuckHash || !probesNeverReadyPath(pods[0]) || podreadiness.IsContainersReady(pods[0]) {
					return false
				}
				for idx := int32(1); idx < replicas; idx++ {
					if !h.untouchedOn(idx, h.revV1) || !h.instanceServes(idx) {
						return false
					}
				}
				return true
			}
			if !h.runWithInvariant(20, parked, withinBudget) {
				h.dumpState("push")
				t.Fatalf("the push never parked at its budget with one Instance dark on %s", revStuck.Name)
			}
			for i := 0; i < 3; i++ {
				h.step()
				withinBudget()
			}
			if !parked() {
				h.dumpState("held")
				t.Fatalf("the parked roll moved while its only rebuilt pod never turned Ready")
			}

			// The rollback: v1 re-applied over the parked roll.
			h.setTarget(h.revV1, goodImage)
			floorHeld := func() {
				withinBudget()
				for idx := int32(1); idx < replicas; idx++ {
					if !h.untouchedOn(idx, h.revV1) {
						h.dumpState("rollback")
						t.Fatalf("instance %d left v1 during a rollback onto v1", idx)
					}
					if pods := h.podsOf(idx); len(pods) != 1 || pods[0].UID != uids[idx] {
						h.dumpState("rollback")
						t.Fatalf("instance %d no longer runs the pod it served v1 with: the rollback rebuilt an Instance that never left v1", idx)
					}
				}
			}
			if !h.runWithInvariant(40, func() bool { return h.settledOn(h.revV1, replicas) }, floorHeld) {
				h.dumpState("after the rollback")
				t.Fatalf("the rollback did not settle on %s: %d of %d Instances Ready on it, a pod of the rolled-back revision alive: %v",
					h.revV1.Name, len(h.landedOn(h.revV1)), replicas, h.livePodProbesNeverReady())
			}
			if h.livePodProbesNeverReady() {
				h.dumpState("leftover")
				t.Fatalf("a pod of the rolled-back revision is still alive after the rollback settled")
			}
			h.requirePodsRender(0, h.revV1, goodImage)
		})
	}
}

// TestRollback_AtTheLastInstanceOfAnInPlaceRollComesBackInPlace: four
// single-pod Instances listed in a rollout group roll an image push in
// place under InPlaceIfPossible with maxUnavailable 1. With three
// Instances Ready on the new revision and the last at most mid-patch, v1
// is re-applied. Every Instance comes back to v1 in place: the same pod
// UIDs throughout, at most one Instance out of rotation at a time, no
// repair, and no pod of the new revision left.
func TestRollback_AtTheLastInstanceOfAnInPlaceRollComesBackInPlace(t *testing.T) {
	const replicas = 4
	h := inPlaceIfPossibleHarness(t, replicas, 1, true)
	uids := h.podUIDByInstance(replicas)
	inPlaceWithinBudget := func() {
		requireRollWithinUnavailableBudget(t, h, replicas, 1)
		for idx := int32(0); idx < replicas; idx++ {
			if pods := h.podsOf(idx); len(pods) != 1 || pods[0].UID != uids[idx] {
				h.dumpState("pod identity")
				t.Fatalf("instance %d no longer runs the pod it had before the push: an in-place roll patches, it does not recreate", idx)
			}
		}
	}

	h.setTarget(h.revFixed, fixedImage)
	atTheLast := func() bool { return len(h.landedOn(h.revFixed)) == replicas-1 }
	if !h.runWithInvariant(80, atTheLast, inPlaceWithinBudget) {
		h.dumpState("push")
		t.Fatalf("the roll never reached its last Instance: %d of %d Ready on %s", len(h.landedOn(h.revFixed)), replicas, h.revFixed.Name)
	}

	h.setTarget(h.revV1, goodImage)
	if !h.runWithInvariant(80, func() bool { return h.settledOn(h.revV1, replicas) }, inPlaceWithinBudget) {
		h.dumpState("after the rollback")
		t.Fatalf("the rollback did not settle on %s: %d of %d Instances Ready on it", h.revV1.Name, len(h.landedOn(h.revV1)), replicas)
	}
	if h.livePodOnImage(fixedImage) {
		t.Fatalf("a pod of the rolled-back revision is still alive after the rollback settled")
	}
	for idx := int32(0); idx < replicas; idx++ {
		h.requirePodsRender(idx, h.revV1, goodImage)
	}
}

// TestReconcile_AdmissionOutageOnASurgeHoldsAtTheCadence: a surge roll on
// Instances that keep serving while the apiserver cannot take their
// replacement creates to admission. The source pod stays in rotation for
// the whole outage, so the row is serving and waiting at once. The wait
// is announced once, the row is written once, and every refused pass asks
// for the configured operation cadence. A status write is a watch event
// that wakes the controller at once, so a pass that rewrote the row would
// run the controller, and its events, at apiserver speed for as long as
// the outage lasted. When admission answers again the create lands, the
// token goes, and the roll completes.
func TestReconcile_AdmissionOutageOnASurgeHoldsAtTheCadence(t *testing.T) {
	const replicas = 2
	h := newRecoveryHarness(t, false)
	h.recorder = record.NewFakeRecorder(256)
	h.replicas = replicas
	h.useRollingBudget(types.UpdateStrategySurgeThenDrain)
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}
	h.events = nil

	h.admissionDown = true
	h.setTarget(h.revFixed, fixedImage)
	const refusedPasses = 6
	for i := 1; i <= refusedPasses; i++ {
		if i == 2 {
			// The first refused pass records the wait; from here on the
			// row's report is settled and nothing may rewrite it.
			h.statusWrites = 0
		}
		res, err := h.stepResult()
		if err != nil {
			t.Fatalf("refused pass %d: %v (an admission outage is a wait, not an error)", i, err)
		}
		if res.Requeue || res.RequeueAfter != testRequeueIntervals.Operation {
			t.Fatalf("refused pass %d: got %+v want RequeueAfter=%v, the configured operation cadence", i, res, testRequeueIntervals.Operation)
		}
	}
	if got := h.eventCount(types.EventReasonInstanceAdmissionUnavailable); got != 1 {
		t.Errorf("%s events over %d refused passes: got %d want 1, once per hold (%v)",
			types.EventReasonInstanceAdmissionUnavailable, refusedPasses, got, h.events)
	}
	if h.statusWrites != 0 {
		t.Errorf("status writes after the wait was recorded: got %d want 0; each is a watch event that wakes the controller at once", h.statusWrites)
	}
	held := 0
	for _, s := range h.irStatuses() {
		if s.Operation == nil || s.Operation.Waiting != types.RejectionReasonAdmissionUnavailable {
			continue
		}
		held++
		if s.LastFailure == nil || !strings.Contains(s.LastFailure.Message, "failed calling webhook") {
			t.Errorf("instance %d: LastFailure %+v, want the apiserver's words naming the webhook", s.Index, s.LastFailure)
		}
	}
	if held != 1 {
		t.Errorf("rows holding the admission wait: got %d want 1, the one surge maxSurge=1 admits", held)
	}
	if got := h.instancesServingOn(goodImage); got != replicas {
		t.Errorf("instances serving on the source image under the outage: got %d want %d", got, replicas)
	}

	h.admissionDown = false
	if !h.run(60, func() bool { return h.settledOn(h.revFixed, replicas) }) {
		h.dumpState("after admission returned")
		t.Fatalf("the roll never completed once admission answered again")
	}
	for _, s := range h.irStatuses() {
		if s.Operation != nil {
			t.Errorf("instance %d: operation %+v left open after the roll", s.Index, s.Operation)
		}
	}
}

// terminatingSurgeFixture is a Component a corrected push lands on while a
// pod of a superseded attempt is still Terminating: two single-pod engine
// Instances on the prior revision under SurgeThenDrain maxSurge 1. Every
// pass runs on the fixture's fake clock and records the hold the update
// pass reported, nil included.
type terminatingSurgeFixture struct {
	t      *testing.T
	c      client.Client
	clk    *clocktesting.FakeClock
	in     types.ReconcileInput
	deps   types.Deps
	plan   types.ComponentPlan
	target *appsv1.ControllerRevision
	holds  []*types.RolloutHold
}

const terminatingSurgeGrace = 30 * time.Second

// newTerminatingSurgeFixture seeds the Component with the rows given, a
// serving pod on the prior revision for each index in serving, and the
// extra pods; planned are the plan's Instances, two of them replicas.
func newTerminatingSurgeFixture(t *testing.T, rows []types.InstanceStatus, planned, serving []int32, extra ...*corev1.Pod) *terminatingSurgeFixture {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := &terminatingSurgeFixture{t: t, clk: clocktesting.NewFakeClock(now)}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"}}
	objs := []client.Object{isvc}
	for _, idx := range serving {
		objs = append(objs, servingEnginePod(idx))
	}
	for _, pod := range extra {
		objs = append(objs, pod)
	}
	f.c = fake.NewClientBuilder().WithScheme(makeScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(objs...).Build()
	f.deps = types.Deps{Client: f.c, Expectations: types.NewExpectations(), Clock: f.clk}
	f.in = minimalInput(t)
	f.in.Clock = f.clk
	// An unconfigured gate cadence, so the result carries only the wait
	// the pass itself owes.
	f.in.Requeue = types.RequeueIntervals{}
	f.in.DesiredSpec.Replicas = 2
	f.in.ObservedState.InstanceStatuses = rows
	f.in.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		for i := range f.in.ObservedState.InstanceStatuses {
			if f.in.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&f.in.ObservedState.InstanceStatuses[i])
				return nil
			}
		}
		return nil
	}
	f.in.RecordRolloutHold = func(h *types.RolloutHold) { f.holds = append(f.holds, h) }
	f.plan = types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		UpdateStrategy: types.UpdateStrategy{
			Type:          types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{MaxSurge: intOrStringInt(1), MaxUnavailable: intOrStringInt(0)},
		},
	}
	for _, idx := range planned {
		f.plan.Instances = append(f.plan.Instances, types.InstancePlan{Index: idx, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}})
	}
	f.target = &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"}}
	return f
}

// pass runs one reconcile and returns its result.
func (f *terminatingSurgeFixture) pass() ctrl.Result {
	f.t.Helper()
	res, err := workload.Reconcile(context.Background(), f.deps, f.in, f.plan, f.target)
	if err != nil {
		f.t.Fatalf("Reconcile: %v", err)
	}
	return res
}

// lastHold is the verdict the latest update pass reported; the test fails
// when no pass reported one.
func (f *terminatingSurgeFixture) lastHold() *types.RolloutHold {
	f.t.Helper()
	if len(f.holds) == 0 {
		f.t.Fatalf("the update pass reported no verdict")
	}
	return f.holds[len(f.holds)-1]
}

// surgePods lists the pods at the surge ordinal of any Instance on the
// target revision: the replacements the pass has opened.
func (f *terminatingSurgeFixture) surgePods() []string {
	f.t.Helper()
	list := &corev1.PodList{}
	if err := f.c.List(context.Background(), list, client.InNamespace("prod")); err != nil {
		f.t.Fatalf("list pods: %v", err)
	}
	var names []string
	for i := range list.Items {
		if list.Items[i].Labels[query.LabelRevisionHash] == "newtarget" {
			names = append(names, list.Items[i].Name)
		}
	}
	slices.Sort(names)
	return names
}

// listedPods is every pod the API lists for the Component, in name order.
func (f *terminatingSurgeFixture) listedPods() []string {
	f.t.Helper()
	list := &corev1.PodList{}
	if err := f.c.List(context.Background(), list, client.InNamespace("prod")); err != nil {
		f.t.Fatalf("list pods: %v", err)
	}
	var names []string
	for i := range list.Items {
		names = append(names, list.Items[i].Name)
	}
	slices.Sort(names)
	return names
}

// removePod lets the pod's kubelet finish: the object leaves the API.
func (f *terminatingSurgeFixture) removePod(pod *corev1.Pod) {
	f.t.Helper()
	stored := &corev1.Pod{}
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
		f.t.Fatalf("get %s: %v", pod.Name, err)
	}
	stored.Finalizers = nil
	if err := f.c.Update(context.Background(), stored); err != nil {
		f.t.Fatalf("finish termination of %s: %v", pod.Name, err)
	}
}

// terminatingEnginePod is a pod of Instance idx at ordinal, on revision
// rev, Terminating until deadline: the API has stamped its deletion at
// the request time plus its grace, and a finalizer stands in for the
// kubelet that has not reaped it yet.
func terminatingEnginePod(isvc, ns string, idx, ordinal int32, rev string, deadline time.Time, grace time.Duration) *corev1.Pod {
	pod := enginePod(isvc, ns, idx)
	pod.Name = query.PodName(isvc, types.ComponentEngine, idx, "default", ordinal)
	pod.UID = ktypes.UID(pod.Name + "-uid")
	pod.Labels[query.LabelPodOrdinal] = fmt.Sprintf("%d", ordinal)
	pod.Labels[query.LabelRevisionHash] = rev
	stamp := metav1.NewTime(deadline)
	seconds := int64(grace.Seconds())
	pod.DeletionTimestamp = &stamp
	pod.DeletionGracePeriodSeconds = &seconds
	pod.Finalizers = []string{"test.example.com/kubelet"}
	return pod
}

// liveEnginePod is a pod of Instance idx at ordinal on revision rev,
// alive and serving nothing: the shape a replacement a disposed attempt
// left behind has, and the shape a rebuilt pod has while it waits on an
// image.
func liveEnginePod(isvc, ns string, idx, ordinal int32, rev string, created time.Time) *corev1.Pod {
	pod := enginePod(isvc, ns, idx)
	pod.Name = query.PodName(isvc, types.ComponentEngine, idx, "default", ordinal)
	pod.UID = ktypes.UID(pod.Name + "-uid")
	pod.CreationTimestamp = metav1.NewTime(created)
	pod.Labels[query.LabelPodOrdinal] = fmt.Sprintf("%d", ordinal)
	pod.Labels[query.LabelRevisionHash] = rev
	pod.Status.Phase = corev1.PodPending
	return pod
}

// wreckedPushRows are the rows a corrected push finds after a strategy
// edit from RecreatePod to SurgeThenDrain landed while Instance 0 was
// rebuilding at a crashing revision: Instance 0 Failed with its rebuilt
// pod alive and serving nothing, and Instance 1 Failed with its source
// still serving beside the replacement the surge admitted after the edit
// left behind. Both attempts were disposed, so neither row carries an
// operation and nothing is Terminating.
func wreckedPushRows(now time.Time) []types.InstanceStatus {
	failure := &types.InstanceTermination{Reason: "ImagePullBackOff", Message: "manifest unknown", Time: metav1.NewTime(now.Add(-time.Minute))}
	return []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "llama-70b-engine-priorrev", TargetRevision: "llama-70b-engine-crashrev",
			PodCount: 1, ServingPodCount: 0, LastFailure: failure},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "llama-70b-engine-priorrev", TargetRevision: "llama-70b-engine-crashrev",
			PodCount: 2, ServingPodCount: 1, LastFailure: failure},
	}
}

// TestReconcile_CorrectedPushKeepsTheCeilingOverALiveExtraPod pins the
// surge ceiling over a replacement left alive: the Component already
// carries replicas plus maxSurge pods, so the dark Instance's start,
// first in line because it restores capacity, is denied while the extra
// pod holds the slot, and the Instance holding it starts instead by
// evicting it. The first pass creates nothing; the pass after the
// eviction opens exactly that Instance's surge, and the dark Instance
// still waits.
func TestReconcile_CorrectedPushKeepsTheCeilingOverALiveExtraPod(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rebuilt := liveEnginePod("llama-70b", "prod", 0, 0, "crashrev", now)
	wreckage := liveEnginePod("llama-70b", "prod", 1, 1, "crashrev", now)
	f := newTerminatingSurgeFixture(t, wreckedPushRows(now), []int32{0, 1}, []int32{1}, rebuilt, wreckage)

	f.pass()

	if got := f.surgePods(); len(got) != 0 {
		t.Fatalf("a surge opened beside the extra pod: %v", got)
	}
	if listed := f.listedPods(); len(listed) > 3 {
		t.Fatalf("the Component must never carry more than replicas plus maxSurge pods, lists %v", listed)
	}
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(wreckage), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the extra pod must be evicted by a start on its own Instance, got %v", err)
	}
	if row := f.in.ObservedState.Instance(1); row == nil || row.Phase != types.InstancePhaseUpdating || row.Operation == nil || row.Operation.Step != types.UpdateStepSurge {
		t.Fatalf("the Instance holding the extra pod must start the surge that replaces it, got %+v", row)
	}
	if row := f.in.ObservedState.Instance(0); row == nil || row.Phase != types.InstancePhaseFailed || row.Operation != nil {
		t.Fatalf("the dark Instance's start must wait for the slot, got %+v", row)
	}

	// The informer observes the eviction, so the surge may create into
	// the freed name.
	f.deps.ExpectationsCache().ObservedDelete("prod", "llama-70b", types.ComponentEngine, 1)
	f.clk.Step(time.Second)
	f.pass()

	if got := f.surgePods(); !slices.Equal(got, []string{"llama-70b-engine-1-default-1"}) {
		t.Fatalf("the pass after the eviction must open exactly the holding Instance's surge, got %v", got)
	}
	if listed := f.listedPods(); len(listed) > 3 {
		t.Fatalf("the Component must never carry more than replicas plus maxSurge pods, lists %v", listed)
	}
	if row := f.in.ObservedState.Instance(0); row == nil || row.Phase != types.InstancePhaseFailed || row.Operation != nil {
		t.Fatalf("the dark Instance's start must wait while the surge it was denied for is in flight, got %+v", row)
	}
}

// TestReconcile_SurgeHoldNamesTheLiveExtraPod pins the hold over a live
// extra pod: when no start is admitted, the Component reports the Budget
// hold naming the Terminating pod it waits on and the extra pod a start
// on its Instance will replace, and the pass asks to come back when the
// Terminating pod's grace elapses. Once it has, the Instance holding the
// extra pod starts, taking over the slot its own pod held.
func TestReconcile_SurgeHoldNamesTheLiveExtraPod(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "llama-70b-engine-priorrev", PodCount: 1, ServingPodCount: 1},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "llama-70b-engine-priorrev", PodCount: 2, ServingPodCount: 1},
	}
	wreckage := liveEnginePod("llama-70b", "prod", 1, 1, "crashrev", now)
	retired := terminatingEnginePod("llama-70b", "prod", 5, 0, "priorrev", now.Add(terminatingSurgeGrace), terminatingSurgeGrace)
	f := newTerminatingSurgeFixture(t, rows, []int32{0, 1}, []int32{0, 1}, wreckage, retired)

	res := f.pass()

	if got := f.surgePods(); len(got) != 0 {
		t.Fatalf("a surge opened over the held slots: %v", got)
	}
	hold := f.lastHold()
	if hold == nil || hold.Gate != types.RolloutHoldGateBudget || hold.Target != f.target.Name {
		t.Fatalf("the Component must report a Budget hold on %s, got %+v", f.target.Name, hold)
	}
	for _, want := range []string{retired.Name, wreckage.Name, "extra pod"} {
		if !strings.Contains(hold.Reason, want) {
			t.Fatalf("the hold must name %q, got %q", want, hold.Reason)
		}
	}
	if res.RequeueAfter != terminatingSurgeGrace {
		t.Fatalf("the pass must come back when the Terminating pod's grace elapses (%s), asked for %+v", terminatingSurgeGrace, res)
	}

	f.clk.Step(terminatingSurgeGrace + time.Second)
	f.pass()

	if got := f.surgePods(); len(got) != 0 {
		t.Fatalf("the start on the holding Instance evicts its extra pod before it creates anything, got %v", got)
	}
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(wreckage), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the extra pod must be evicted by a start on its own Instance, got %v", err)
	}
	if row := f.in.ObservedState.Instance(1); row == nil || row.Phase != types.InstancePhaseUpdating {
		t.Fatalf("the Instance holding the extra pod must be the one that starts, got %+v", row)
	}
	if row := f.in.ObservedState.Instance(0); row == nil || row.Phase != types.InstancePhaseReady {
		t.Fatalf("the other Instance must wait for the slot, got %+v", row)
	}
	if hold := f.lastHold(); hold != nil {
		t.Fatalf("the hold must clear once a start is admitted, still %+v", hold)
	}
}

// correctedPushRows are the rows a corrected push finds after an attempt
// at a crashing revision was disposed on Instance 1: both Instances on
// the prior revision, Instance 1 Failed with no operation and its source
// still serving beside the replacement the attempt left behind.
func correctedPushRows(now time.Time) []types.InstanceStatus {
	return []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "llama-70b-engine-priorrev", PodCount: 1, ServingPodCount: 1},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "llama-70b-engine-priorrev", PodCount: 2, ServingPodCount: 1,
			LastFailure: &types.InstanceTermination{Reason: "CrashLoopBackOff", Message: "replacement crashed after it served", Time: metav1.NewTime(now.Add(-time.Minute))}},
	}
}

// TestReconcile_CorrectedPushWaitsForATerminatingReplacement pins the
// surge ceiling over a draining pod: a replacement an attempt abandoned
// holds its surge slot while the API still lists it inside its deletion
// grace, so a corrected push opens no surge beside it. The Component
// reports the Budget hold naming that pod, and the pass asks to come
// back when the pod's grace elapses, since nothing in the cluster
// announces that instant.
func TestReconcile_CorrectedPushWaitsForATerminatingReplacement(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	abandoned := terminatingEnginePod("llama-70b", "prod", 1, 1, "crashrev", now.Add(terminatingSurgeGrace), terminatingSurgeGrace)
	f := newTerminatingSurgeFixture(t, correctedPushRows(now), []int32{0, 1}, []int32{0, 1}, abandoned)

	res := f.pass()

	if got := f.surgePods(); len(got) != 0 {
		t.Fatalf("a surge opened beside the Terminating replacement: %v", got)
	}
	hold := f.lastHold()
	if hold == nil || hold.Gate != types.RolloutHoldGateBudget || hold.Target != f.target.Name {
		t.Fatalf("the Component must report a Budget hold on %s, got %+v", f.target.Name, hold)
	}
	if !strings.Contains(hold.Reason, abandoned.Name) || !strings.Contains(hold.Reason, "Terminating pod") {
		t.Fatalf("the hold must name the Terminating replacement, got %q", hold.Reason)
	}
	if res.RequeueAfter != terminatingSurgeGrace {
		t.Fatalf("the pass must come back when the replacement's grace elapses (%s), asked for %+v", terminatingSurgeGrace, res)
	}
}

// TestReconcile_CorrectedPushOpensOnceTheReplacementIsGone pins the
// release: the first pass after the Terminating replacement leaves the
// API admits the surge the hold withheld, and the hold clears with it.
func TestReconcile_CorrectedPushOpensOnceTheReplacementIsGone(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	abandoned := terminatingEnginePod("llama-70b", "prod", 1, 1, "crashrev", now.Add(terminatingSurgeGrace), terminatingSurgeGrace)
	f := newTerminatingSurgeFixture(t, correctedPushRows(now), []int32{0, 1}, []int32{0, 1}, abandoned)

	f.pass()
	if hold := f.lastHold(); hold == nil {
		t.Fatalf("the first pass must hold while the replacement drains")
	}
	f.removePod(abandoned)
	f.clk.Step(time.Second)
	f.pass()

	if got := f.surgePods(); len(got) != 1 {
		t.Fatalf("exactly one surge must open once the replacement is gone, got %v", got)
	}
	if hold := f.lastHold(); hold != nil {
		t.Fatalf("the hold must clear once a surge is admitted, still %+v", hold)
	}
}

// TestReconcile_CorrectedPushOpensOnceTheGraceElapsedThoughThePodIsListed
// pins the bound on the wait: a replacement past its own deletion
// deadline, which the API stamped at the request time plus the pod's
// grace, stops counting even though its kubelet has not reaped it, so
// the surge opens and the hold clears without anything being
// force-deleted.
func TestReconcile_CorrectedPushOpensOnceTheGraceElapsedThoughThePodIsListed(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	abandoned := terminatingEnginePod("llama-70b", "prod", 1, 1, "crashrev", now.Add(terminatingSurgeGrace), terminatingSurgeGrace)
	f := newTerminatingSurgeFixture(t, correctedPushRows(now), []int32{0, 1}, []int32{0, 1}, abandoned)

	f.pass()
	if hold := f.lastHold(); hold == nil {
		t.Fatalf("the first pass must hold while the replacement drains")
	}
	f.clk.Step(terminatingSurgeGrace + time.Second)
	f.pass()

	stored := &corev1.Pod{}
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(abandoned), stored); err != nil {
		t.Fatalf("the replacement must still be listed past its grace: %v", err)
	}
	if got := f.surgePods(); len(got) != 1 {
		t.Fatalf("exactly one surge must open once the grace elapsed, got %v", got)
	}
	if hold := f.lastHold(); hold != nil {
		t.Fatalf("the hold must clear once a surge is admitted, still %+v", hold)
	}
}

// TestReconcile_TerminatingPodOfAnUnownedIndexHoldsTheRollOnlyWithinItsGrace
// pins the bound for a pod the plan does not own, the shape a
// scale-down leaves while its last pod drains: it holds a surge slot
// while inside its grace and releases it when the grace elapses, whether
// or not the object is gone.
func TestReconcile_TerminatingPodOfAnUnownedIndexHoldsTheRollOnlyWithinItsGrace(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "llama-70b-engine-priorrev", PodCount: 1, ServingPodCount: 1},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "llama-70b-engine-priorrev", PodCount: 1, ServingPodCount: 1},
	}
	retired := terminatingEnginePod("llama-70b", "prod", 5, 0, "priorrev", now.Add(terminatingSurgeGrace), terminatingSurgeGrace)
	f := newTerminatingSurgeFixture(t, rows, []int32{0, 1}, []int32{0, 1}, retired)

	res := f.pass()
	if got := f.surgePods(); len(got) != 0 {
		t.Fatalf("a surge opened beside the draining pod: %v", got)
	}
	hold := f.lastHold()
	if hold == nil || hold.Gate != types.RolloutHoldGateBudget || !strings.Contains(hold.Reason, retired.Name) {
		t.Fatalf("the hold must name the draining pod, got %+v", hold)
	}
	if res.RequeueAfter != terminatingSurgeGrace {
		t.Fatalf("the pass must come back when the pod's grace elapses (%s), asked for %+v", terminatingSurgeGrace, res)
	}

	f.clk.Step(terminatingSurgeGrace + time.Second)
	f.pass()
	if got := f.surgePods(); len(got) != 1 {
		t.Fatalf("exactly one surge must open once the grace elapsed, got %v", got)
	}
	if hold := f.lastHold(); hold != nil {
		t.Fatalf("the hold must clear once a surge is admitted, still %+v", hold)
	}
}

// TestReconcile_TerminatingMigrationSourceDoesNotHoldTheRoll pins that a
// migration pair's pods are the pair's own: both of its indices are the
// plan's, so the source draining beside its promoted replacement is not
// an extra pod and an unrelated roll opens at once.
func TestReconcile_TerminatingMigrationSourceDoesNotHoldTheRoll(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	surgeIndex := int32(2)
	rows := []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "llama-70b-engine-priorrev", PodCount: 1, ServingPodCount: 1},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseMigrating, RunningRevision: "llama-70b-engine-priorrev", PodCount: 1,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, Step: "CreateSurge", RequestUUID: "move-1", SurgeIndex: &surgeIndex}},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "llama-70b-engine-priorrev", PodCount: 1, ServingPodCount: 1,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, Step: "CreateSurge", RequestUUID: "move-1"}},
	}
	source := terminatingEnginePod("llama-70b", "prod", 1, 0, "priorrev", now.Add(terminatingSurgeGrace), terminatingSurgeGrace)
	destination := servingEnginePod(2)
	f := newTerminatingSurgeFixture(t, rows, []int32{0, 1, 2}, []int32{0}, source, destination)

	f.pass()

	if got := f.surgePods(); len(got) != 1 {
		t.Fatalf("the roll must open its surge beside a migration's draining source, got %v", got)
	}
	if hold := f.lastHold(); hold != nil {
		t.Fatalf("no hold may stand on a migration source's own pod, got %+v", hold)
	}
}

// servingSince marks pod Running, ContainersReady and PodReady since at,
// with the serving gate on: in rotation, as the kubelet reports it.
func servingSince(pod *corev1.Pod, at time.Time) {
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(at)},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(at)},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(at)},
	}
}

// healedEnginePod is the pod of Instance idx on revision rev whose runner
// restarted twice after readySince and has been Ready again, and serving,
// since readyAgain: the kubelet's record carries the last termination, of
// a run that itself began after the row entered Ready.
func healedEnginePod(idx int32, rev string, readySince, readyAgain time.Time) *corev1.Pod {
	pod := liveEnginePod("llama-70b", "prod", idx, 0, rev, readySince.Add(-time.Minute))
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, Ready: true, RestartCount: 2,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(readyAgain.Add(-time.Second))}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 137, Reason: "Error",
			StartedAt:  metav1.NewTime(readySince.Add(time.Minute)),
			FinishedAt: metav1.NewTime(readyAgain.Add(-2 * time.Second)),
		}},
	}}
	servingSince(pod, readyAgain)
	return pod
}

// TestReconcile_HealedRunnerStopsHoldingTheFloor pins the floor's release
// under SurgeThenDrain maxUnavailable 0: Instance 0 is on the target with a
// pod whose runner restarted twice after the row entered Ready and serves
// again on the same row, so its anchor never moves; Instance 1 is at
// Step=Surge with its replacement past the promote bar. While the pod has
// been Ready again for less than the window the Instance holds its slot,
// the Component is below its floor, the drain gate is not asked, the
// source keeps serving and the Budget hold names the Instance. Once the
// pod has held Ready for the window it serves: the gate is consulted with
// the source, the source leaves rotation and no hold stands.
func TestReconcile_HealedRunnerStopsHoldingTheFloor(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const window = time.Minute
	cases := []struct {
		name       string
		readyAgain time.Time
		held       bool
	}{
		{name: "Ready again for less than the window, the floor holds", readyAgain: now.Add(-window / 2), held: true},
		{name: "Ready again for the window, the source leaves rotation", readyAgain: now.Add(-window)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			readySince := now.Add(-10 * time.Minute)
			rows := []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "llama-70b-engine-newtarget",
					ReadySince: &metav1.Time{Time: readySince}, PodCount: 1, ServingPodCount: 1},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "llama-70b-engine-priorrev",
					TargetRevision: "llama-70b-engine-newtarget", PodCount: 2, ServingPodCount: 1,
					Operation: &types.InstanceOperation{
						ID: "update-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge,
						TargetRevision: "llama-70b-engine-newtarget", Strategy: types.UpdateStrategySurgeThenDrain,
						StartedAt: metav1.NewTime(now.Add(-5 * time.Minute)), LastProgressAt: metav1.NewTime(now.Add(-5 * time.Minute)),
						Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
					}},
			}
			healed := healedEnginePod(0, "newtarget", readySince, tc.readyAgain)
			replacement := liveEnginePod("llama-70b", "prod", 1, 1, "newtarget", now.Add(-5*time.Minute))
			replacement.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: constants.MainContainerName, Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now.Add(-5 * time.Minute))}},
			}}
			servingSince(replacement, now.Add(-4*time.Minute))
			f := newTerminatingSurgeFixture(t, rows, []int32{0, 1}, []int32{1}, healed, replacement)
			f.in.StuckPodGrace = window
			f.in.DrainHolds = &types.DrainHolds{}
			f.plan.RestartPolicy = types.RestartPolicyNone
			var asked [][]string
			f.in.DrainGate = func(sourcePods []string) (bool, types.RolloutHoldGate, string) {
				asked = append(asked, sourcePods)
				return true, "", ""
			}
			source := servingEnginePod(1)
			sourceServing := func() bool {
				fresh := &corev1.Pod{}
				err := f.c.Get(context.Background(), client.ObjectKeyFromObject(source), fresh)
				if apierrors.IsNotFound(err) {
					return false
				}
				if err != nil {
					t.Fatalf("read the source: %v", err)
				}
				return podreadiness.IsServing(fresh)
			}

			f.pass()

			row := f.in.ObservedState.Instance(1)
			if tc.held {
				if len(asked) != 0 {
					t.Fatalf("the drain gate was consulted while the Component was below its floor: %v", asked)
				}
				hold := f.lastHold()
				if hold == nil || hold.Gate != types.RolloutHoldGateBudget || hold.Target != f.target.Name ||
					!strings.Contains(hold.Reason, "no source leaves rotation") || !holdNamesInstance(hold, 0) {
					t.Fatalf("the Component must report the floor's Budget hold naming Instance 0, got %+v", hold)
				}
				if row == nil || row.Operation == nil || row.Operation.Step != types.UpdateStepSurge {
					t.Fatalf("a withheld drain must keep Instance 1 at Step=Surge, got %+v", row)
				}
				if !sourceServing() {
					t.Fatalf("a withheld drain must leave the source in rotation")
				}
				return
			}
			if len(asked) != 1 || len(asked[0]) != 1 || asked[0][0] != source.Name {
				t.Fatalf("the drain gate must be consulted once with the source pod, got %v", asked)
			}
			if hold := f.lastHold(); hold != nil {
				t.Fatalf("an Instance that serves again holds nothing, got %+v", hold)
			}
			if row == nil || row.Operation == nil || row.Operation.Step != types.UpdateStepSurgeDrain {
				t.Fatalf("an admitted drain must move Instance 1 to Step=SurgeDrain, got %+v", row)
			}
			if sourceServing() {
				t.Fatalf("an admitted drain must take the source out of rotation")
			}
		})
	}
}

// darkSince marks pod Running with its runner up but failing readiness
// since at: ContainersReady and PodReady False from then, the serving
// gate written at promotion still True, no restart on its record.
func darkSince(pod *corev1.Pod, at time.Time) {
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, Ready: false,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(at.Add(-time.Hour))}},
	}}
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(at)},
		{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(at)},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(at.Add(-time.Hour))},
	}
}

// TestReconcile_DarkRolledInstanceHoldsNoFloor pins the floor's reading of
// an Instance the roll moved onto the target whose promoted pod served
// and then failed readiness, under SurgeThenDrain maxUnavailable 0:
// Instance 0 is Ready on the target with that pod, Instance 1 at
// Step=Surge with its replacement past the promote bar. While the pod has
// been unready for less than the stuck-pod grace the Instance holds its
// slot: the Component is below its floor, the drain gate is not asked,
// the source keeps serving, the Budget hold names the Instance and the
// pass wakes for the grace left. Once the pod has been unready for the
// grace it is dark, and nothing it does lifts the shortfall: the gate is
// consulted with the source, the source leaves rotation and no hold
// stands.
func TestReconcile_DarkRolledInstanceHoldsNoFloor(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const grace = time.Minute
	cases := []struct {
		name       string
		unreadyFor time.Duration
		held       bool
	}{
		{name: "unready inside the grace, the floor holds", unreadyFor: grace / 2, held: true},
		{name: "unready for the grace, the source leaves rotation", unreadyFor: grace},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			readySince := now.Add(-10 * time.Minute)
			rows := []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "llama-70b-engine-newtarget",
					ReadySince: &metav1.Time{Time: readySince}, PodCount: 1, ServingPodCount: 0},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "llama-70b-engine-priorrev",
					TargetRevision: "llama-70b-engine-newtarget", PodCount: 2, ServingPodCount: 1,
					Operation: &types.InstanceOperation{
						ID: "update-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge,
						TargetRevision: "llama-70b-engine-newtarget", Strategy: types.UpdateStrategySurgeThenDrain,
						StartedAt: metav1.NewTime(now.Add(-5 * time.Minute)), LastProgressAt: metav1.NewTime(now.Add(-5 * time.Minute)),
						Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
					}},
			}
			dark := liveEnginePod("llama-70b", "prod", 0, 0, "newtarget", readySince.Add(-time.Minute))
			darkSince(dark, now.Add(-tc.unreadyFor))
			replacement := liveEnginePod("llama-70b", "prod", 1, 1, "newtarget", now.Add(-5*time.Minute))
			replacement.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: constants.MainContainerName, Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now.Add(-5 * time.Minute))}},
			}}
			servingSince(replacement, now.Add(-4*time.Minute))
			f := newTerminatingSurgeFixture(t, rows, []int32{0, 1}, []int32{1}, dark, replacement)
			f.in.StuckPodGrace = grace
			f.in.DrainHolds = &types.DrainHolds{}
			f.plan.RestartPolicy = types.RestartPolicyNone
			var asked [][]string
			f.in.DrainGate = func(sourcePods []string) (bool, types.RolloutHoldGate, string) {
				asked = append(asked, sourcePods)
				return true, "", ""
			}
			source := servingEnginePod(1)
			sourceServing := func() bool {
				fresh := &corev1.Pod{}
				err := f.c.Get(context.Background(), client.ObjectKeyFromObject(source), fresh)
				if apierrors.IsNotFound(err) {
					return false
				}
				if err != nil {
					t.Fatalf("read the source: %v", err)
				}
				return podreadiness.IsServing(fresh)
			}

			res := f.pass()

			row := f.in.ObservedState.Instance(1)
			if tc.held {
				if len(asked) != 0 {
					t.Fatalf("the drain gate was consulted while the Component was below its floor: %v", asked)
				}
				hold := f.lastHold()
				if hold == nil || hold.Gate != types.RolloutHoldGateBudget || hold.Target != f.target.Name ||
					!strings.Contains(hold.Reason, "no source leaves rotation") || !holdNamesInstance(hold, 0) {
					t.Fatalf("the Component must report the floor's Budget hold naming Instance 0, got %+v", hold)
				}
				if row == nil || row.Operation == nil || row.Operation.Step != types.UpdateStepSurge {
					t.Fatalf("a withheld drain must keep Instance 1 at Step=Surge, got %+v", row)
				}
				if !sourceServing() {
					t.Fatalf("a withheld drain must leave the source in rotation")
				}
				if left := grace - tc.unreadyFor; res.RequeueAfter <= 0 || res.RequeueAfter > left {
					t.Fatalf("the pass must wake for the grace left (%s), got %+v", left, res)
				}
				return
			}
			if len(asked) != 1 || len(asked[0]) != 1 || asked[0][0] != source.Name {
				t.Fatalf("the drain gate must be consulted once with the source pod, got %v", asked)
			}
			if hold := f.lastHold(); hold != nil {
				t.Fatalf("a dark Instance on the target holds nothing, got %+v", hold)
			}
			if row == nil || row.Operation == nil || row.Operation.Step != types.UpdateStepSurgeDrain {
				t.Fatalf("an admitted drain must move Instance 1 to Step=SurgeDrain, got %+v", row)
			}
			if sourceServing() {
				t.Fatalf("an admitted drain must take the source out of rotation")
			}
		})
	}
}
