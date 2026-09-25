package workload_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
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
			if got := escalation.HeldByPartition(tc.partition, tc.rank); got != tc.held {
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
			got := escalation.EffectivePartition(tc.pacing, tc.ru)
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
