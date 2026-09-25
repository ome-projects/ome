package workload_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestReconcile_RestartPass_OneLiveListForManyInstances pins the restart
// pass's read cost: it does a SINGLE live pod List for the
// whole Component and buckets by Instance index, not one live List per
// Instance. With three healthy Ready instances under the gang-default
// RecreateInstance policy, the live APIReader must see exactly one
// PodList call from the restart pass (the update / create passes read
// the cached Client, not the live reader).
func TestReconcile_RestartPass_OneLiveListForManyInstances(t *testing.T) {
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
	}
	pods := []client.Object{
		isvc,
		enginePod(isvc.Name, isvc.Namespace, 0),
		enginePod(isvc.Name, isvc.Namespace, 1),
		enginePod(isvc.Name, isvc.Namespace, 2),
	}
	// The restart pass reads the LIVE reader (APIReader), which has no Pod
	// field index — liveBucketPods calls ListOMENativePodsByName with
	// useIndex=false, so it goes straight to the label selector (ONE List).
	// No index registration here: the live path never probes MatchingFields,
	// so the assertion measures the per-Component-vs-per-Instance list count
	// without any index-fallback noise.
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(pods...).Build()
	counter := &listCountingReader{Reader: c}
	deps := workloadtypes.Deps{Client: c, APIReader: counter}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	plan := workloadtypes.ComponentPlan{
		Component:     workloadtypes.ComponentEngine,
		Replicas:      3,
		RestartPolicy: workloadtypes.RestartPolicyRecreateInstance,
		Instances: []workloadtypes.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	if _, err := workload.Reconcile(context.Background(), deps, in, plan, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// The live reader is used ONLY by the restart pass here (no restart
	// fires, so no destructive live ops run). It must list once, not once
	// per Instance.
	if counter.podListCalls != 1 {
		t.Errorf("restart pass must issue exactly 1 live pod List for 3 instances, got %d", counter.podListCalls)
	}
}

// TestReconcile_RestartPass_PerInstanceSemanticsPreserved proves the
// single List + per-Instance bucketing keeps exact per-Instance restart
// semantics: a Failed pod in index 1's bucket triggers a restart (status
// flips to Restarting on index 1) while healthy indices 0 and 2 are left
// untouched.
func TestReconcile_RestartPass_PerInstanceSemanticsPreserved(t *testing.T) {
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
	}
	objs := []client.Object{
		isvc,
		enginePod(isvc.Name, isvc.Namespace, 0),
		failedEnginePod(isvc.Name, isvc.Namespace, 1), // the restart trigger
		enginePod(isvc.Name, isvc.Namespace, 2),
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(objs...).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	in.MutateInstance = roundTripMutateInstance(c, isvc, workloadtypes.ComponentEngine)
	plan := workloadtypes.ComponentPlan{
		Component:     workloadtypes.ComponentEngine,
		Replicas:      3,
		RestartPolicy: workloadtypes.RestartPolicyRecreateInstance,
		Instances: []workloadtypes.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("a restart in flight must requeue; got %+v", result)
	}

	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
		t.Fatalf("get isvc: %v", err)
	}
	s1 := instanceStatusByIndex(c, fresh, v1beta1.EngineComponent, 1)
	if s1 == nil || s1.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("index 1 (Failed pod) must flip to Restarting; got %+v", s1)
	}
	for _, idx := range []int32{0, 2} {
		s := instanceStatusByIndex(c, fresh, v1beta1.EngineComponent, idx)
		// Healthy indices must not be dragged into a restart. The dispatcher
		// short-circuits on the first restarting Instance, so their status
		// stays at its observed Ready (or is simply never written).
		if s != nil && s.Phase == v1beta1.OMENativeInstanceRestarting {
			t.Errorf("healthy index %d must NOT be restarting; got %+v", idx, s)
		}
	}
}

// An in-flight Restart owns its existing Instance, but it must not prevent
// unrelated absent indices from beginning their Create lifecycle.
func TestReconcile_ScaleUpDuringRestart_CreatesFreshIndices(t *testing.T) {
	name := "scale-during-restart"
	targetName := name + "-engine-target"
	f := newRestartScaleTestFixture(t, name, 3, false, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workloadtypes.InstancePhaseRestarting,
		RunningRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "restart-0", Type: workloadtypes.InstanceOperationRestart, Step: "Drain",
		},
	}, []workloadtypes.RunnerPlan{{Name: "default", Size: 1}})

	result, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("an in-flight restart must schedule another pass; got %+v", result)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	for idx := int32(0); idx < 3; idx++ {
		name := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, idx, "default", 0)
		if !names[name] {
			t.Errorf("expected pod %q to be materialized in the restart pass; got %v", name, names)
		}
	}
	s0 := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if s0 == nil || s0.Phase != v1beta1.OMENativeInstanceRestarting || s0.Operation == nil ||
		s0.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Errorf("restart-owned index 0 must remain Restarting with its Restart operation; got %+v", s0)
	}
	for _, idx := range []int32{1, 2} {
		s := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, idx)
		if s == nil || s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil ||
			s.Operation.Type != v1beta1.InstanceOperationCreate {
			t.Errorf("fresh index %d must begin a Create operation; got %+v", idx, s)
		}
	}
}

// A committed partial gang is eligible for Restart even though the reconcile's
// observation still describes it as Create-owned. Once Restart takes ownership,
// the fresh-index Create pass must exclude that index while creating an
// unrelated absent index.
func TestReconcile_ScaleUpDuringRestart_ExcludesRestartSelectedCreateOwner(t *testing.T) {
	name := "scale-partial-gang"
	targetName := name + "-engine-target"
	leader := enginePod(name, "prod", 0)
	leader.Name = query.PodName(name, workloadtypes.ComponentEngine, 0, "leader", 0)
	leader.Labels[query.LabelRunner] = "leader"
	f := newRestartScaleTestFixture(t, name, 2, false, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating, PodCount: 1,
		RunningRevision: targetName, TargetRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "create-0", Type: workloadtypes.InstanceOperationCreate, Step: "CreatePods",
			TargetRevision: targetName,
		},
	}, []workloadtypes.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}, leader)

	result, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("the partial-gang restart must schedule another pass; got %+v", result)
	}

	s0 := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if s0 == nil || s0.Phase != v1beta1.OMENativeInstanceRestarting || s0.Incarnation != 2 ||
		s0.Operation == nil || s0.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("index 0 must remain owned by Restart at incarnation 2; got %+v", s0)
	}
	s1 := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 1)
	if s1 == nil || s1.Phase != v1beta1.OMENativeInstanceCreating || s1.Operation == nil ||
		s1.Operation.Type != v1beta1.InstanceOperationCreate {
		t.Errorf("unrelated absent index 1 must begin a Create operation; got %+v", s1)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	for _, runner := range []string{"leader", "worker"} {
		freshName := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 1, runner, 0)
		if !names[freshName] {
			t.Errorf("expected fresh-index pod %q; got %v", freshName, names)
		}
		restartName := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 0, runner, 0)
		if names[restartName] {
			t.Errorf("restart-selected index 0 must not be recreated from the stale Create observation; found %q in %v", restartName, names)
		}
	}
}

// A standard pause permits an existing Restart to advance, but it continues to
// prohibit Create work for absent desired indices.
func TestReconcile_PausedRestart_DoesNotCreateFreshIndices(t *testing.T) {
	name := "paused-restart"
	targetName := name + "-engine-target"
	f := newRestartScaleTestFixture(t, name, 2, true, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workloadtypes.InstancePhaseRestarting,
		RunningRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "restart-0", Type: workloadtypes.InstanceOperationRestart, Step: "Drain",
		},
	}, []workloadtypes.RunnerPlan{{Name: "default", Size: 1}})

	result, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("the permitted restart must schedule another pass; got %+v", result)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	restartName := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 0, "default", 0)
	if !names[restartName] {
		t.Errorf("paused reconcile must still advance the existing restart; got %v", names)
	}
	freshName := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 1, "default", 0)
	if names[freshName] {
		t.Errorf("paused reconcile must not materialize absent index 1; got %v", names)
	}
	if s := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 1); s != nil {
		t.Errorf("paused reconcile must not allocate status for absent index 1; got %+v", s)
	}
}

// A wedge is a property of the revision, so every Ready Instance
// qualifies for the crash-loop repair in the same pass. The
// unavailability budget paces it: one repair opens per pass, and the
// repair already in flight keeps the next pass from opening a second.
func TestReconcile_CrashLoopRepair_PacedByUnavailabilityBudget(t *testing.T) {
	ctx := context.Background()
	one := intstr.FromInt32(1)
	in, plan, pods := crashLoopRepairFixture(t, 3, &one)
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}

	if _, err := workload.Reconcile(ctx, deps, in, plan, updateTarget()); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if got := countRestarting(store, 3); got != 1 {
		t.Fatalf("repairs opened = %d, want 1 under a MaxUnavailable of 1", got)
	}

	// The admitted repair anchors the projection while it is in flight,
	// so the next pass opens none of its peers — whether or not the
	// first one finishes in that pass.
	store.sync(&in)
	if _, err := workload.Reconcile(ctx, deps, in, plan, updateTarget()); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	for _, idx := range []int32{1, 2} {
		s := store.status(idx)
		if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil {
			t.Fatalf("instance %d = %+v, want an untouched Ready row behind the in-flight repair", idx, s)
		}
	}
}

// An uncapped Component keeps the unpaced behavior: with no budget
// configured nothing bounds the repair.
func TestReconcile_CrashLoopRepair_UncappedRepairsEveryWedge(t *testing.T) {
	ctx := context.Background()
	in, plan, pods := crashLoopRepairFixture(t, 3, nil)
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}

	if _, err := workload.Reconcile(ctx, deps, in, plan, updateTarget()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := countRestarting(store, 3); got != 3 {
		t.Fatalf("repairs opened = %d, want 3 with no budget configured", got)
	}
}

// The cross-Component coordination gate is the second layer: a denial
// there holds the repair even where the per-Component budget allows it.
// A rebuild never surges, so the gate is consulted on the arm that
// bounds pods taken offline.
func TestReconcile_CrashLoopRepair_HeldByCoordinationGate(t *testing.T) {
	ctx := context.Background()
	in, plan, pods := crashLoopRepairFixture(t, 2, nil)
	consults := 0
	in.UpdateGate = func(strategy workloadtypes.UpdateStrategyType, surge, unavail int32) (bool, workloadtypes.RolloutHoldGate, string) {
		consults++
		if strategy != workloadtypes.UpdateStrategyRecreatePod {
			t.Errorf("a repair rebuilds pods in place; gate consulted with strategy %q", strategy)
		}
		return false, workloadtypes.RolloutHoldGateRatio, "peer Component is behind"
	}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workloadtypes.NewExpectations()}

	result, err := workload.Reconcile(ctx, deps, in, plan, updateTarget())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := countRestarting(store, 2); got != 0 {
		t.Fatalf("repairs opened = %d, want 0 while the gate denies", got)
	}
	if consults == 0 {
		t.Fatal("the coordination gate must be consulted before a repair opens")
	}
	// Nothing in the cluster changes while the gate denies, so the pass
	// must carry its own wake-up and say why it is waiting.
	if result.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue: no watch event will re-consult the gate", result)
	}
	assertRepairHeld(t, rec, "peer Component is behind")
}

// With the budget already spent by an in-flight update, every wedged
// row is denied and the pass opens nothing at all. That is the state
// with no watch event coming: the Component would sit wedged until an
// unrelated wake-up, so the pass requeues itself and reports the hold.
func TestReconcile_CrashLoopRepair_AllDeniedRequeuesAndReports(t *testing.T) {
	ctx := context.Background()
	one := intstr.FromInt32(1)
	in, plan, pods := crashLoopRepairFixture(t, 3, &one)
	// Index 0 is mid-update rather than wedged, so it spends the budget
	// without being a restart selection of its own.
	in.ObservedState.InstanceStatuses[0].Phase = workloadtypes.InstancePhaseUpdating
	in.ObservedState.InstanceStatuses[0].Operation = &workloadtypes.InstanceOperation{
		ID: "update-0", Type: workloadtypes.InstanceOperationUpdate, Step: "InPlace",
	}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workloadtypes.NewExpectations()}

	result, err := workload.Reconcile(ctx, deps, in, plan, updateTarget())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, idx := range []int32{1, 2} {
		if s := store.status(idx); s == nil || s.Phase != workloadtypes.InstancePhaseReady {
			t.Fatalf("instance %d = %+v, want an untouched Ready row: the budget is spent", idx, s)
		}
	}
	if result.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue: nothing else will wake the wedged Component", result)
	}
	assertRepairHeld(t, rec, "unavailability budget")
}

// TestRestartPass_IsolatesInstanceStatusWriteFailure: instance 0's status
// writes are rejected on every pass. Instance 1, selected in the same pass,
// must start and finish its restart regardless; each failing pass returns an
// error naming instance 0; once instance 0's writes succeed both converge.
func TestRestartPass_IsolatesInstanceStatusWriteFailure(t *testing.T) {
	h := newRestartHarness(t, 2)
	rejected := errors.New("status update rejected")
	h.mutateErr = map[int32]error{0: rejected}
	h.losePods(0, 1)

	_, err := h.stepResult()
	if err == nil {
		t.Fatal("the pass must fail while instance 0's status write is rejected")
	}
	if !errors.Is(err, rejected) {
		t.Fatalf("the returned error must wrap the rejected write; got %v", err)
	}
	if !strings.Contains(err.Error(), "restart instance 0") {
		t.Fatalf("the returned error must name instance 0; got %v", err)
	}
	if strings.Contains(err.Error(), "restart instance 1") {
		t.Fatalf("instance 1 did not fail and must not be reported; got %v", err)
	}
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Incarnation != 1 || s.Operation != nil {
		t.Fatalf("instance 0 must be untouched while its writes are rejected; got %+v", s)
	}
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("instance 0 must not be recreated before its Restarting write lands; got %d pods", len(pods))
	}
	if s := h.instance(1); s == nil || s.Phase != workloadtypes.InstancePhaseRestarting ||
		s.Operation == nil || s.Operation.Type != workloadtypes.InstanceOperationRestart {
		t.Fatalf("instance 1 must start its restart in the same pass; got %+v", s)
	}
	if !h.atIncarnation(1, 2) {
		h.dumpState("after first failing pass")
		t.Fatalf("instance 1 must have its replacement pod at incarnation 2")
	}

	// Two more passes for instance 1's promote: one writes the serving gate
	// on the replacement, the next observes kubelet folding it into PodReady.
	for pass := 0; pass < 2; pass++ {
		_, err = h.stepResult()
		if err == nil || !strings.Contains(err.Error(), "restart instance 0") {
			t.Fatalf("the pass must keep failing on instance 0; got %v", err)
		}
	}
	if s := h.instance(1); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || !h.atIncarnation(1, 2) {
		h.dumpState("after the failing passes")
		t.Fatalf("instance 1 must complete its restart while instance 0 keeps failing; got %+v", s)
	}
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("instance 0 must still be untouched; got %d pods", len(pods))
	}

	h.mutateErr = nil
	converged := h.run(10, func() bool {
		return h.allReady(2) && h.atIncarnation(0, 2) && h.atIncarnation(1, 2)
	})
	if !converged {
		h.dumpState("after instance 0's writes were accepted")
		t.Fatalf("both instances must converge once instance 0's writes succeed")
	}
}

// TestRestartPass_NoErrors_RequeuesAndCreatesFreshIndices pins the
// error-free contract of a multi-Instance restart pass: every selected
// Instance advances, the pass requeues at the restart interval, and a
// surge-free index added by a concurrent scale-up is materialized in the
// same pass instead of waiting behind the in-flight restarts.
func TestRestartPass_NoErrors_RequeuesAndCreatesFreshIndices(t *testing.T) {
	h := newRestartHarness(t, 2)
	h.losePods(0, 1)
	h.replicas = 3
	h.setTarget(h.revV1, goodImage)

	res, err := h.stepResult()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.Requeue || res.RequeueAfter != testRequeueIntervals.Operation { //nolint:staticcheck // a configured cadence must not also set the bare flag
		t.Fatalf("the restart pass must requeue at the restart interval; got %+v", res)
	}

	// The same pass with no configured cadence still carries a wake-up:
	// a restart the pass opened has nothing in the cluster to announce
	// its next step, and a fresh create that asks for no wake of its own
	// must not erase it.
	unconfigured := newRestartHarness(t, 2)
	unconfigured.requeue = workloadtypes.RequeueIntervals{}
	unconfigured.losePods(0, 1)
	unconfigured.replicas = 3
	unconfigured.setTarget(unconfigured.revV1, goodImage)
	res, err = unconfigured.stepResult()
	if err != nil {
		t.Fatalf("Reconcile with no configured cadence: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("unconfigured cadence: got RequeueAfter %v want 0", res.RequeueAfter)
	}
	if !res.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
		t.Fatalf("a restart pass with no cadence must still wake; got %+v", res)
	}
	for _, idx := range []int32{0, 1} {
		if s := h.instance(idx); s == nil || s.Phase != workloadtypes.InstancePhaseRestarting ||
			s.Operation == nil || s.Operation.Type != workloadtypes.InstanceOperationRestart {
			t.Fatalf("instance %d must be restarting; got %+v", idx, s)
		}
		if !h.atIncarnation(idx, 2) {
			h.dumpState("restart pass")
			t.Fatalf("instance %d must have its replacement pod at incarnation 2", idx)
		}
	}
	if s := h.instance(2); s == nil || s.Phase != workloadtypes.InstancePhaseCreating ||
		s.Operation == nil || s.Operation.Type != workloadtypes.InstanceOperationCreate {
		t.Fatalf("fresh index 2 must begin its Create in the restart pass; got %+v", s)
	}
	if pods := h.podsOf(2); len(pods) != 1 {
		t.Fatalf("fresh index 2 must be materialized in the restart pass; got %d pods", len(pods))
	}

	if !h.run(10, func() bool { return h.allReady(3) }) {
		h.dumpState("after scale-up during restart")
		t.Fatalf("restarted and fresh instances must all converge")
	}
}

func newRestartScaleTestFixture(
	t *testing.T,
	name string,
	replicas int32,
	paused bool,
	status workloadtypes.InstanceStatus,
	runners []workloadtypes.RunnerPlan,
	pods ...client.Object,
) *restartScaleTestFixture {
	t.Helper()
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", UID: types.UID("uid-" + name)},
	}
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"},
		Status: v1beta1.InferenceReplicaStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			v1beta1convert.InstanceStatusFromWorkload(status),
		}},
	}
	objects := []client.Object{isvc, ir}
	objects = append(objects, pods...)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(objects...).Build()

	input := minimalInput(t)
	input.OwnerObject = isvc
	input.EventTarget = isvc
	input.Key.OwnerName = isvc.Name
	input.DesiredSpec.Replicas = replicas
	input.DesiredSpec.MultiPod = len(runners) > 1
	input.DesiredSpec.Paused = paused
	input.ObservedState.UpdateRevision = status.RunningRevision
	input.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{cloneTestInstanceStatus(status)}
	input.MutateInstance = roundTripMutateInstance(c, isvc, workloadtypes.ComponentEngine)

	instances := make([]workloadtypes.InstancePlan, 0, replicas)
	for idx := int32(0); idx < replicas; idx++ {
		incarnation := int64(1)
		if idx == status.Index {
			incarnation = status.Incarnation
		}
		instances = append(instances, workloadtypes.InstancePlan{
			Index: idx, Incarnation: incarnation,
			Runners: append([]workloadtypes.RunnerPlan(nil), runners...),
		})
	}
	plan := workloadtypes.ComponentPlan{
		Component:            workloadtypes.ComponentEngine,
		Replicas:             replicas,
		RestartPolicy:        workloadtypes.RestartPolicyRecreateInstance,
		InstanceReadyTimeout: time.Minute,
		Paused:               paused,
		Instances:            instances,
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: status.RunningRevision, Namespace: isvc.Namespace},
	}
	return &restartScaleTestFixture{isvc: isvc, client: c, input: input, plan: plan, target: target}
}

// wedgedEnginePod is an engine pod parked in a terminal kubelet waiting
// reason for longer than any grace these fixtures configure.
func wedgedEnginePod(idx int32) *corev1.Pod {
	pod := enginePod("llama-70b", "prod", idx)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  constants.MainContainerName,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
	}}
	return pod
}

// crashLoopRepairFixture wires n Ready, operation-free rows whose pods
// are all wedged on the current revision — the shape one bad argument
// or one bad image produces across a whole Component.
func crashLoopRepairFixture(t *testing.T, n int32, maxUnavailable *intstr.IntOrString) (workloadtypes.ReconcileInput, workloadtypes.ComponentPlan, []client.Object) {
	t.Helper()
	in := minimalInput(t)
	in.StuckPodGrace = time.Minute
	in.ObservedState.CurrentRevision = crashLoopRepairRevision
	in.DesiredSpec.Replicas = n
	plan := workloadtypes.ComponentPlan{
		Component:     workloadtypes.ComponentEngine,
		Replicas:      n,
		RestartPolicy: workloadtypes.RestartPolicyNone,
		UpdateStrategy: workloadtypes.UpdateStrategy{
			Type:          workloadtypes.UpdateStrategyRecreatePod,
			RollingUpdate: &workloadtypes.RollingUpdate{MaxUnavailable: maxUnavailable},
		},
	}
	var pods []client.Object
	for idx := int32(0); idx < n; idx++ {
		in.ObservedState.InstanceStatuses = append(in.ObservedState.InstanceStatuses, workloadtypes.InstanceStatus{
			Index: idx, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady,
			PodCount: 1, RunningRevision: crashLoopRepairRevision,
		})
		plan.Instances = append(plan.Instances, workloadtypes.InstancePlan{
			Index: idx, Incarnation: 1,
			Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}},
		})
		pods = append(pods, wedgedEnginePod(idx))
	}
	return in, plan, pods
}

// installRepairStore routes every status adapter the restart pass uses
// into one store, so a pass's repairs are observable as committed rows.
func installRepairStore(in *workloadtypes.ReconcileInput) *testAtomicMutationStore {
	store := installTestAtomicMutationStore(in)
	in.ApplyInstanceMutations = func(ctx context.Context, muts []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, muts, "", nil)
	}
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		return store.apply(ctx, []workloadtypes.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	return store
}

// countRestarting reports how many rows the store holds in Restarting.
func countRestarting(store *testAtomicMutationStore, n int32) int {
	restarting := 0
	for idx := int32(0); idx < n; idx++ {
		if s := store.status(idx); s != nil && s.Phase == workloadtypes.InstancePhaseRestarting {
			restarting++
		}
	}
	return restarting
}

// assertRepairHeld drains the recorder and requires exactly one
// RepairHeld warning carrying detail.
func assertRepairHeld(t *testing.T, rec *record.FakeRecorder, detail string) {
	t.Helper()
	held := 0
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
			if strings.Contains(e, string(workloadtypes.EventReasonRepairHeld)) {
				held++
				if !strings.Contains(e, detail) {
					t.Errorf("RepairHeld must name the layer holding the repair; got %q", e)
				}
			}
			continue
		default:
		}
		break
	}
	if held != 1 {
		t.Fatalf("RepairHeld events = %d, want 1 (events=%v)", held, events)
	}
}

// newRestartHarness is the recovery harness with n single-pod Instances
// under RestartPolicy=RecreateInstance, converged Ready on v1.
func newRestartHarness(t *testing.T, replicas int32) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	policy := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle = workloadtypes.Lifecycle{RestartPolicy: &policy}
	h.replicas = replicas
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.allReady(replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on v1 for %d instances", replicas)
	}
	return h
}
