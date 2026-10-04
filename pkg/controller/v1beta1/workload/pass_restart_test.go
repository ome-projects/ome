package workload_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
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
// qualifies for the crash-loop repair in the same pass. For a pod set
// that still serves — a gang serving through its leader while a worker
// is wedged — the unavailability budget paces the repair: one opens per
// pass, and the repair already in flight keeps the next pass from
// opening a second.
func TestReconcile_CrashLoopRepair_PacedByUnavailabilityBudget(t *testing.T) {
	ctx := context.Background()
	one := intstr.FromInt32(1)
	in, plan, pods := gangCrashLoopFixture(t, 3, &one, 0, 1, 2)
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}

	if _, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget()); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if got := countRestarting(store, 3); got != 1 {
		t.Fatalf("repairs opened = %d, want 1 under a MaxUnavailable of 1", got)
	}

	// The admitted repair anchors the projection while it is in flight,
	// so the next pass opens none of its peers — whether or not the
	// first one finishes in that pass.
	store.sync(&in)
	if _, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget()); err != nil {
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

	if _, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := countRestarting(store, 3); got != 3 {
		t.Fatalf("repairs opened = %d, want 3 with no budget configured", got)
	}
}

// A crash-loop repair is never put to the cross-Component coordination
// gate. The gate counts a gang as serving only when every member is, so
// the parked member that makes the wedge already takes the Instance out
// of the gate's serving count: a gang whose worker is wedged while its
// leader serves is inside the gate's unavailability before its repair
// opens, and a consult would charge the repair for the outage it ends.
// The per-Component budget still decides it; with none configured, a
// gate that would deny everything holds nothing.
func TestReconcile_CrashLoopRepair_ServingGangIsNotPutToTheGate(t *testing.T) {
	ctx := context.Background()
	in, plan, pods := gangCrashLoopFixture(t, 2, nil, 0, 1)
	consults := 0
	in.UpdateGate = func(workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
		consults++
		return false, workloadtypes.RolloutHoldGateRatio, "peer Component is behind"
	}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workloadtypes.NewExpectations()}

	result, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := countRestarting(store, 2); got != 2 {
		t.Fatalf("repairs opened = %d, want 2: the gate already counts a gang with a wedged worker as unavailable, so its repair is not held behind its own outage", got)
	}
	if consults != 0 {
		t.Fatalf("gate consults = %d, want 0: a crash-loop repair is not put to the coordination gate", consults)
	}
	if result.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue for the repairs in flight", result)
	}
	assertNoRepairHeld(t, rec)
}

// A repair that takes a serving pod offline stays budgeted: a gang whose
// leader serves while its worker is wedged is held under a budget of
// zero, and the hold is announced naming the budget.
func TestReconcile_CrashLoopRepair_ServingGangStaysBudgeted(t *testing.T) {
	ctx := context.Background()
	zero := intstr.FromInt32(0)
	in, plan, pods := gangCrashLoopFixture(t, 1, &zero, 0)
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workloadtypes.NewExpectations()}

	result, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := countRestarting(store, 1); got != 0 {
		t.Fatalf("repairs opened = %d, want 0: the leader still serves, so the rebuild takes a serving pod offline", got)
	}
	if result.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue: nothing else will wake the held repair", result)
	}
	assertRepairHeld(t, rec, "unavailability budget 0")
}

// A repair of a pod set that serves nothing removes no serving capacity,
// so neither layer that paces capacity loss has a say: it is admitted
// without the per-Component unavailability budget — zero here, the
// lifecycle under which no serving pod may ever be taken offline — and
// without the coordination gate, which here answers as a rollout group's
// does while no run is open, holding every consult on the plan gate.
func TestReconcile_CrashLoopRepair_DarkPodSetIsAdmittedByNeitherLayer(t *testing.T) {
	ctx := context.Background()
	zero := intstr.FromInt32(0)
	in, plan, pods := crashLoopRepairFixture(t, 3, &zero)
	consults := 0
	in.UpdateGate = planHoldGate(&consults)
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workloadtypes.NewExpectations()}

	result, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := countRestarting(store, 3); got != 3 {
		t.Fatalf("repairs opened = %d, want 3: a pod set that serves nothing is admitted by neither the budget nor the gate", got)
	}
	if consults != 0 {
		t.Fatalf("gate consults = %d, want 0 for repairs that remove no serving capacity", consults)
	}
	if result.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue for the repairs in flight", result)
	}
	assertNoRepairHeld(t, rec)
}

// A dark repair the pass opened is charged to neither layer. The gang at
// index 0 is wedged whole and repairs outside the budget; the gang at
// index 1 serves through its leader while its worker is wedged, and is
// admitted against a budget of one with nothing in flight ahead of it.
// Neither repair is put to the coordination gate: both gangs are already
// out of its serving count.
func TestReconcile_CrashLoopRepair_DarkRepairIsChargedToNeitherLayer(t *testing.T) {
	ctx := context.Background()
	one := intstr.FromInt32(1)
	in, plan, pods := gangCrashLoopFixture(t, 2, &one, 1)
	consults := 0
	in.UpdateGate = func(workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
		consults++
		return true, "", ""
	}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
	store := installRepairStore(&in)
	rec := record.NewFakeRecorder(16)
	deps := workloadtypes.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workloadtypes.NewExpectations()}

	if _, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := countRestarting(store, 2); got != 2 {
		t.Fatalf("repairs opened = %d, want both: the dark gang's repair must not spend the budget the serving gang's needs", got)
	}
	if consults != 0 {
		t.Fatalf("gate consults = %d, want 0: a crash-loop repair is not put to the coordination gate", consults)
	}
	assertNoRepairHeld(t, rec)
}

// TestReconcile_CrashLoopRepair_DarkRowOpensUnderTheRealGateStack puts the
// repair admission in front of the coordination package's own gate stack,
// read over the published InferenceReplica the way the adapter wires it.
// Asked about the dark row's recreate, the stack answers a hold either way:
// with no run open the plan gate holds every consult, and with a run open
// the unavailability gate counts the dark Instance once as unavailability
// already open and again as the start it is asked about. The repair takes
// nothing further offline, so it is not asked, nor is the per-Component
// budget — zero here: it opens, the serving peer is untouched, and no
// hold is announced.
func TestReconcile_CrashLoopRepair_DarkRowOpensUnderTheRealGateStack(t *testing.T) {
	for _, tc := range []struct {
		name    string
		runOpen bool
		heldBy  v1beta1.RolloutHoldGate
	}{
		{name: "no run open", runOpen: false, heldBy: v1beta1.RolloutHoldGatePlan},
		{name: "run open", runOpen: true, heldBy: v1beta1.RolloutHoldGateBudget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			zero := intstr.FromInt32(0)
			in, plan, pods := crashLoopRepairFixture(t, 2, &zero)
			pods[1] = servingEnginePod(1)
			isvc := groupedISVC(tc.runOpen)
			in.OwnerObject, in.EventTarget = isvc, isvc
			objects := append([]client.Object{isvc, publishedEngineReplica(in, 2, 1)}, pods...)
			c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(objects...).Build()
			gate := func(strategy workloadtypes.UpdateStrategyType, surge, unavail int32) (bool, workloadtypes.RolloutHoldGate, string) {
				allowed, held, reason := coordination.EvaluateUpdateGate(ctx, c, isvc, v1beta1.EngineComponent, crashLoopRepairTarget(), nil, coordination.GroupDefaults{}, strategy, surge, unavail)
				return allowed, workloadtypes.RolloutHoldGate(held), reason
			}
			// The stack's own answer to the dark row's recreate is a hold,
			// which is why the admission must not ask it.
			if allowed, held, reason := gate(workloadtypes.UpdateStrategyRecreatePod, 0, 0); allowed || held != workloadtypes.RolloutHoldGate(tc.heldBy) {
				t.Fatalf("gate stack asked about the dark row = (%v, %s, %q), want a %s hold", allowed, held, reason, tc.heldBy)
			}
			in.UpdateGate = gate
			store := installRepairStore(&in)
			rec := record.NewFakeRecorder(16)
			deps := workloadtypes.Deps{Client: c, APIReader: c, Recorder: rec, Expectations: workloadtypes.NewExpectations()}

			if _, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget()); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if s := store.status(0); s == nil || s.Phase != workloadtypes.InstancePhaseRestarting {
				t.Fatalf("instance 0 = %+v, want its repair open: a dark row is not put to the gate stack", s)
			}
			if s := store.status(1); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil {
				t.Fatalf("instance 1 = %+v, want the serving peer untouched", s)
			}
			assertNoRepairHeld(t, rec)
		})
	}
}

// With the budget already spent by an in-flight update, every wedged
// gang whose leader still serves is denied and the pass opens nothing at
// all. That is the state with no watch event coming: the Component would
// sit wedged until an unrelated wake-up, so the pass requeues itself and
// reports the hold.
func TestReconcile_CrashLoopRepair_AllDeniedRequeuesAndReports(t *testing.T) {
	ctx := context.Background()
	one := intstr.FromInt32(1)
	in, plan, pods := gangCrashLoopFixture(t, 3, &one, 0, 1, 2)
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

	result, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget())
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
	input := minimalInput(t)
	objects := []client.Object{isvc, ir}
	if status.RunningRevision != "" {
		// The row names the revision it runs; the rebuild renders that
		// revision's stored template.
		objects = append(objects, revisionWithPodSpec(t, status.RunningRevision, isvc.Namespace, input.DesiredSpec.PodSpec))
	}
	objects = append(objects, pods...)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(objects...).Build()

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

// crashLoopRepairTarget is the roll target the repair tests reconcile
// toward: the revision the wedged rows already run. A repair is selected
// only while that revision is still the target; a Component wedged on a
// revision that is not the target is the update pass's.
func crashLoopRepairTarget() *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: crashLoopRepairRevision, Namespace: "prod"},
	}
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

// crashForm names how the old-revision Instance breaks: its pod dies at
// every start and is never Ready again, its pod dies after it is up and
// keeps coming back between crashes, or a gang loses a member outright.
type crashForm string

const (
	crashAtStart   crashForm = "crash-at-start"
	crashAfterUp   crashForm = "crash-after-ready"
	gangMemberLost crashForm = "member-lost"
)

// offTargetStrategies is every update strategy with the budget the closed
// loop runs it under: one surge, or one Instance offline at a time.
var offTargetStrategies = []struct {
	strategy       workloadtypes.UpdateStrategyType
	surge, unavail int32
}{
	{workloadtypes.UpdateStrategySurgeThenDrain, 1, 0},
	{workloadtypes.UpdateStrategyRecreatePod, 0, 1},
	{workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 1},
	{workloadtypes.UpdateStrategyInPlaceOnly, 0, 1},
}

// offTargetCrashHarness converges replicas Instances on v1 under the
// named strategy, budget and restart policy, with the runner container
// named as production names it so container-restart evidence is read.
func offTargetCrashHarness(t *testing.T, gang bool, strategy workloadtypes.UpdateStrategyType, surge, unavail int32, policy workloadtypes.RestartPolicy, replicas int32) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarnessFor(t, gang, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(64)
	h.replicas = replicas
	maxSurge := intstr.FromInt32(surge)
	maxUnavailable := intstr.FromInt32(unavail)
	h.lifecycle = workloadtypes.Lifecycle{
		RestartPolicy: &policy,
		UpdateStrategy: &workloadtypes.UpdateStrategy{
			Type:          strategy,
			RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
		},
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}
	return h
}

// breakInstance applies form to the Instance at idx on the running
// revision: the leader (or single) pod is pointed at the crash image, or
// the gang's worker is deleted.
func breakInstance(t *testing.T, h *recoveryHarness, idx int32, gang bool, form crashForm) {
	t.Helper()
	runner := ""
	if gang {
		runner = "leader"
	}
	switch form {
	case crashAtStart:
		h.crashPod(idx, runner, crashStartImage)
	case crashAfterUp:
		h.crashPod(idx, runner, crashLoadImage)
	case gangMemberLost:
		for _, pod := range h.podsOf(idx) {
			if pod.Labels[query.LabelRunner] != "worker" {
				continue
			}
			if err := h.c.Delete(h.ctx, pod); err != nil {
				t.Fatalf("delete worker %s: %v", pod.Name, err)
			}
			return
		}
		t.Fatalf("instance %d has no worker to lose", idx)
	}
}

// TestReconcile_OffTargetCrash_RollsForwardAcrossTheMatrix is the
// closed-loop lock for the ownership rule, over every shape the engine
// alone can express: single-pod and leader+worker Instances, every update
// strategy, both restart policies, and the two forms a persistent crash
// takes plus a lost gang member. One Instance on v1 breaks while the
// target moves to the fixed revision. Whatever evidence the restart
// trigger holds — a crash loop, a runner restart after Ready, a member
// short — the fix lands on the broken Instance through the update pass:
// no repair is recorded, the roll stays inside its budget, and every
// Instance ends Ready on the fixed revision with pods on the fixed image.
func TestReconcile_OffTargetCrash_RollsForwardAcrossTheMatrix(t *testing.T) {
	const replicas = 3
	for _, gang := range []bool{false, true} {
		shape := "single"
		forms := []crashForm{crashAtStart, crashAfterUp}
		if gang {
			shape = "gang"
			forms = append(forms, gangMemberLost)
		}
		for _, sc := range offTargetStrategies {
			for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
				for _, form := range forms {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", shape, sc.strategy, policy, form), func(t *testing.T) {
						h := offTargetCrashHarness(t, gang, sc.strategy, sc.surge, sc.unavail, policy, replicas)
						// The last Instance breaks: the roll reaches it last, so
						// the crash is observed on a Ready row with no operation
						// for as long as the roll takes to get there.
						victim := int32(replicas - 1)
						breakInstance(t, h, victim, gang, form)
						h.events = nil
						h.setTarget(h.revFixed, fixedImage)

						withinBudget := func() {
							if h.repairInFlight() {
								h.dumpState("repair opened")
								t.Fatalf("a repair opened on an Instance whose revision is not the roll target")
							}
							sts := h.irStatuses()
							if sc.strategy == workloadtypes.UpdateStrategySurgeThenDrain {
								if n := escalation.CurrentSurgeInFlight(sts); n > sc.surge {
									t.Fatalf("%d surges in flight, budget %d", n, sc.surge)
								}
							} else if n := escalation.CurrentUnavailableInFlight(sts); n > sc.unavail {
								t.Fatalf("%d Instances offline for the roll, budget %d", n, sc.unavail)
							}
						}
						if !h.runWithInvariant(80, func() bool { return h.settledOn(h.revFixed, replicas) }, withinBudget) {
							h.dumpState("after the bump")
							t.Fatalf("the Component never settled on the fixed revision with the broken Instance healed")
						}
						if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
							t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
						}
					})
				}
			}
		}
	}
}

// TestReconcile_OffTargetCrash_StartIsAdmittedLikeAnyOther pins the
// admission of the broken Instance's roll: it is a fresh start like its
// peers', paced by the same per-Component budget, and the coordination
// gate is consulted for it exactly as for a dark Failed row — not on a
// non-surge strategy, where the crash-looping pod set serves nothing and
// the gate already counts the outage, and once on a surge strategy, where
// the replacement genuinely adds a pod. The same gate stands in for a
// coordination group: every start that opens is admitted through it.
func TestReconcile_OffTargetCrash_StartIsAdmittedLikeAnyOther(t *testing.T) {
	const replicas = 3
	for _, sc := range offTargetStrategies[:2] {
		t.Run(string(sc.strategy), func(t *testing.T) {
			h := offTargetCrashHarness(t, false, sc.strategy, sc.surge, sc.unavail, workloadtypes.RestartPolicyNone, replicas)
			consults := 0
			h.gate = func(strategy workloadtypes.UpdateStrategyType, _, _ int32) (bool, workloadtypes.RolloutHoldGate, string) {
				if strategy != sc.strategy {
					t.Errorf("gate consulted with mechanism %q, want %q", strategy, sc.strategy)
				}
				consults++
				return true, "", ""
			}
			breakInstance(t, h, replicas-1, false, crashAtStart)
			h.setTarget(h.revFixed, fixedImage)
			if !h.run(80, func() bool { return h.settledOn(h.revFixed, replicas) }) {
				h.dumpState("after the bump")
				t.Fatalf("the Component never settled on the fixed revision")
			}
			want := replicas
			if sc.strategy != workloadtypes.UpdateStrategySurgeThenDrain {
				// The broken Instance serves nothing: its recreate opens no
				// new unavailability and skips the consult.
				want = replicas - 1
			}
			if consults != want {
				t.Fatalf("gate consults = %d, want %d", consults, want)
			}
		})
	}
}

// assertNoRepairHeld drains the recorder and fails on any RepairHeld
// warning: the repairs of this pass were owed no hold.
func assertNoRepairHeld(t *testing.T, rec *record.FakeRecorder) {
	t.Helper()
	for {
		select {
		case e := <-rec.Events:
			if strings.Contains(e, string(workloadtypes.EventReasonRepairHeld)) {
				t.Fatalf("a repair was announced held: %q", e)
			}
			continue
		default:
		}
		break
	}
}

// planHoldGate is the coordination gate of a Component whose rollout
// group has no run open: every consult is held on the plan gate, which is
// what the gate stack answers until a run pins the group's plan. consults
// counts the calls.
func planHoldGate(consults *int) func(workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
	return func(workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
		*consults++
		return false, workloadtypes.RolloutHoldGate(v1beta1.RolloutHoldGatePlan), "rollout plan not pinned: no active run for this rollout group"
	}
}

// gangPod renames an engine pod fixture as the named runner of a
// leader+worker gang.
func gangPod(pod *corev1.Pod, runner string) *corev1.Pod {
	idx, _ := query.InstanceIdxFromLabels(pod)
	pod.Name = query.PodName("llama-70b", workloadtypes.ComponentEngine, idx, runner, 0)
	pod.UID = types.UID(pod.Name + "-uid")
	pod.Labels[query.LabelRunner] = runner
	return pod
}

// gangCrashLoopFixture wires n Ready leader+worker gangs on the current
// revision under the given unavailability budget, with every worker
// wedged. A gang listed in servingLeaders keeps its leader in rotation,
// so a repair of it takes a serving pod offline; every other leader is
// wedged too, so that gang serves nothing.
func gangCrashLoopFixture(t *testing.T, n int32, maxUnavailable *intstr.IntOrString, servingLeaders ...int32) (workloadtypes.ReconcileInput, workloadtypes.ComponentPlan, []client.Object) {
	t.Helper()
	in, plan, _ := crashLoopRepairFixture(t, n, maxUnavailable)
	in.DesiredSpec.MultiPod = true
	serving := map[int32]bool{}
	for _, idx := range servingLeaders {
		serving[idx] = true
	}
	var pods []client.Object
	for idx := int32(0); idx < n; idx++ {
		plan.Instances[idx].Runners = []workloadtypes.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
		in.ObservedState.InstanceStatuses[idx].PodCount = 2
		leader := wedgedEnginePod(idx)
		if serving[idx] {
			leader = servingEnginePod(idx)
		}
		pods = append(pods, gangPod(leader, "leader"), gangPod(wedgedEnginePod(idx), "worker"))
	}
	return in, plan, pods
}

// groupedISVC is the owner the repair fixtures reconcile under, listed in
// a blueGreen rollout group; withRun pins that group as the active run.
func groupedISVC(withRun bool) *v1beta1.InferenceService {
	group := v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
		Spec:       v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{group}}},
	}
	if withRun {
		isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
			RunID:    "run-1",
			OpenedAt: metav1.Now(),
			Plan:     v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Source: v1beta1.RolloutPlanSourceInline, Group: group}}},
		}}
	}
	return isvc
}

// publishedEngineReplica is the engine InferenceReplica the gate stack
// reads: the fixture's rows under the counters the adapter publishes.
func publishedEngineReplica(in workloadtypes.ReconcileInput, replicas, serving int32) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine", Namespace: "prod"},
		Spec:       v1beta1.InferenceReplicaSpec{Replicas: &replicas},
		Status:     v1beta1.InferenceReplicaStatus{Replicas: replicas, ServingReplicas: serving},
	}
	for _, row := range in.ObservedState.InstanceStatuses {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1convert.InstanceStatusFromWorkload(row))
	}
	return ir
}

// servingInstances counts the Instances whose full pod set is in rotation
// (ContainersReady with the serving gate), which is how the Component's
// published ServingReplicas counts a gang: one member out of rotation
// takes the whole Instance out of the count.
func (h *recoveryHarness) servingInstances() int32 {
	perInstance := len(h.desired.Runners)
	serving := map[int32]int{}
	for _, pod := range h.livePods() {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || !podreadiness.IsContainersReady(pod) || !podreadiness.IsServing(pod) {
			continue
		}
		serving[idx]++
	}
	var n int32
	for _, count := range serving {
		if count >= perInstance {
			n++
		}
	}
	return n
}

// instanceInRotation reports whether every pod of the Instance at idx is
// in rotation: ContainersReady with the serving gate, none missing.
func (h *recoveryHarness) instanceInRotation(idx int32) bool {
	pods := h.podsOf(idx)
	if len(pods) != len(h.desired.Runners) {
		return false
	}
	for _, pod := range pods {
		if !podreadiness.IsContainersReady(pod) || !podreadiness.IsServing(pod) {
			return false
		}
	}
	return true
}

// coordinationUnavailabilityGate stands in for a coordination group's
// unavailability gate, read the way the group gate reads the Component's
// published counters: the Instances not serving are the unavailability
// already open, and a drain-first start is admitted while that count, plus
// the starts this pass has opened, plus this one, stays within
// maxUnavailable. A surge start never reaches it.
func coordinationUnavailabilityGate(h *recoveryHarness, replicas, maxUnavailable int32, consults *int) func(workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
	return func(strategy workloadtypes.UpdateStrategyType, _, inFlightUnavail int32) (bool, workloadtypes.RolloutHoldGate, string) {
		h.t.Helper()
		*consults++
		if strategy == workloadtypes.UpdateStrategySurgeThenDrain {
			h.t.Fatalf("a drain-first roll consulted the gate as a surge start")
		}
		unavailable := replicas - h.servingInstances()
		if unavailable < 0 {
			unavailable = 0
		}
		projected := unavailable + inFlightUnavail + 1
		if projected > maxUnavailable {
			return false, workloadtypes.RolloutHoldGateBudget, fmt.Sprintf(
				"unavailable budget %d exhausted (current %d, in-flight %d, would become %d)",
				maxUnavailable, unavailable, inFlightUnavail, projected)
		}
		return true, "", ""
	}
}

// TestReconcile_OffTargetCrash_GangStartIsExemptFromTheCoordinationGate
// pins the gate exemption on the gang shape. A gang serves through its
// leader: with the leader wedged the Instance is already out of the
// coordination gate's serving count, whatever the workers report, so the
// roll's drain-first start on it opens no new unavailability and must skip
// the gate consult. Consulting it would count the Instance's own outage a
// second time, and under maxUnavailable=1 the gate would deny the only
// start that can end the outage on every pass.
//
// The healthy Instance rolls first, so the wedged one is reached with the
// roll's own budget free and the gate as the only layer left to answer to.
func TestReconcile_OffTargetCrash_GangStartIsExemptFromTheCoordinationGate(t *testing.T) {
	const replicas = 2
	for _, sc := range offTargetStrategies[1:] {
		for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
			t.Run(fmt.Sprintf("%s/%s", sc.strategy, policy), func(t *testing.T) {
				h := offTargetCrashHarness(t, true, sc.strategy, sc.surge, sc.unavail, policy, replicas)
				consults := 0
				h.gate = coordinationUnavailabilityGate(h, replicas, sc.unavail, &consults)
				h.events = nil
				h.setTarget(h.revFixed, fixedImage)
				h.step()
				if first := h.instance(0); first == nil || first.Phase != workloadtypes.InstancePhaseUpdating {
					h.dumpState("after the bump")
					t.Fatalf("instance 0 = %+v, want the healthy Instance rolling first", first)
				}
				victim := int32(replicas - 1)
				breakInstance(t, h, victim, true, crashAtStart)

				withinBudget := func() {
					if h.repairInFlight() {
						h.dumpState("repair opened")
						t.Fatalf("a repair opened on an Instance whose revision is not the roll target")
					}
					if n := escalation.CurrentUnavailableInFlight(h.irStatuses()); n > sc.unavail {
						t.Fatalf("%d Instances offline for the roll, budget %d", n, sc.unavail)
					}
				}
				if !h.runWithInvariant(80, func() bool { return h.settledOn(h.revFixed, replicas) }, withinBudget) {
					h.dumpState("after the bump")
					t.Fatalf("the Component never settled on the fixed revision: the wedged gang was held behind its own outage")
				}
				if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
					t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
				}
				// Only the healthy Instance's start was anything the gate
				// had to pace.
				if consults != replicas-1 {
					t.Fatalf("gate consults = %d, want %d: the wedged gang serves nothing and skips the consult", consults, replicas-1)
				}
			})
		}
	}
}

// A repair that parked at Failed — its rebuilt pods wedged past the
// stuck-pod grace or the attempt deadline — has two exits of its own, and
// the stories below drive each of them end to end through Reconcile: the
// set coming up on its own resumes the promote, and a set that stays
// wedged is rebuilt again on the operator's retry ladder until it heals
// or the ladder is spent.

// retryLadder is the seconds-scale lifecycle.updateRetry the stories run
// under, read here for a parked repair's re-arms; the fake clock steps at
// least 30s per pass.
func retryLadder(maxAttempts int32, initial time.Duration) *workloadtypes.RetryPolicy {
	return &workloadtypes.RetryPolicy{MaxAttempts: maxAttempts, InitialDelay: initial, MaxDelay: 10 * time.Minute, Multiplier: 2}
}

// parkedRepair reports whether the Instance is Failed with its Restart
// operation preserved.
func parkedRepair(h *recoveryHarness, idx int32) bool {
	s := h.instance(idx)
	return s != nil && s.Phase == workloadtypes.InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationRestart
}

// servingReady reports whether the Instance is Ready with no operation
// and exactly n live pods, all in rotation.
func servingReady(h *recoveryHarness, idx int32, n int) bool {
	s := h.instance(idx)
	if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil {
		return false
	}
	serving := 0
	for _, pod := range h.podsOf(idx) {
		if podreadiness.IsServing(pod) {
			serving++
		}
	}
	return len(h.podsOf(idx)) == n && serving == n
}

// parkRepair drives a converged Instance into the parked shape: the pod
// set is lost, the rebuild wedges in reason, and the escalation fails the
// attempt with its Restart preserved.
func parkRepair(t *testing.T, h *recoveryHarness, reason string, sticky bool) {
	t.Helper()
	h.armWedge(reason, sticky)
	h.losePods(0)
	if !h.run(40, func() bool { return parkedRepair(h, 0) }) {
		h.dumpState("park")
		t.Fatalf("the rebuild never parked at Failed with its Restart preserved")
	}
}

// eventCount is how many of the events recorded so far carry reason.
func eventCount(h *recoveryHarness, reason string) int {
	n := 0
	for _, e := range h.events {
		if strings.Contains(e, reason) {
			n++
		}
	}
	return n
}

// The rebuilt set comes up on its own — the kubelet's retry succeeds once
// the missing ConfigMap key is back — and the parked repair finishes with
// its promote: serving gate written, Ready on the running revision, the
// incarnation the pods already carry, no operator action. The ladder is
// not consulted, so an unconfigured one changes nothing here.
func TestSpentRepair_RebuiltSetComesUp_ResumesThePromote(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ladder *workloadtypes.RetryPolicy
	}{
		{"ladder configured", retryLadder(3, 2*time.Minute)},
		{"ladder unconfigured", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestartHarness(t, 1)
			h.retryPolicy = tc.ladder
			parkRepair(t, h, "CreateContainerConfigError", false)
			parked := h.instance(0)

			h.clearWedge()
			if !h.run(20, func() bool { return servingReady(h, 0, 1) }) {
				h.dumpState("resume")
				t.Fatalf("the healed set was never promoted")
			}
			got := h.instance(0)
			if got.Incarnation != parked.Incarnation {
				t.Errorf("incarnation = %d, want %d: a set that came up is promoted, not rebuilt", got.Incarnation, parked.Incarnation)
			}
			if got.RunningRevision != h.revV1.Name {
				t.Errorf("running revision = %q, want %q", got.RunningRevision, h.revV1.Name)
			}
		})
	}
}

// A rebuilt set parked in a crash loop stays wedged even after the cause
// is gone — the pods cannot recover in place — so the parked repair
// re-arms on the ladder: not before the configured delay from the
// recorded failure, then as a whole rebuild at the next incarnation,
// which heals.
func TestSpentRepair_CrashLoopSet_ReArmsOnTheLadderAndHeals(t *testing.T) {
	h := newRestartHarness(t, 1)
	h.retryPolicy = retryLadder(3, 2*time.Minute)
	parkRepair(t, h, "CrashLoopBackOff", true)
	parked := h.instance(0)
	anchor := parked.LastFailure.Time.Time
	notBefore := anchor.Add(2 * time.Minute)

	// The cause clears at once; the parked pod stays wedged regardless.
	h.clearWedge()
	healed := h.runWithInvariant(40,
		func() bool { return servingReady(h, 0, 1) },
		func() {
			if h.clk.Now().Before(notBefore) && !parkedRepair(h, 0) {
				h.dumpState("early re-arm")
				t.Fatalf("the repair re-armed at %v, before the ladder's first delay elapsed at %v", h.clk.Now(), notBefore)
			}
		})
	if !healed {
		h.dumpState("re-arm")
		t.Fatalf("the re-armed repair never healed")
	}
	got := h.instance(0)
	if got.Incarnation != parked.Incarnation+1 {
		t.Errorf("incarnation = %d, want %d: one rebuild on the ladder", got.Incarnation, parked.Incarnation+1)
	}
}

// A gang whose members are all alive but crash-looping has no member to
// lose, so only the ladder rebuilds it; the rebuild is the whole gang.
func TestSpentRepair_Gang_AllMembersWedged_ReArmsOnTheLadder(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.retryPolicy = retryLadder(3, time.Minute)
	h.driveToReadyOnV1()
	parkRepair(t, h, "CrashLoopBackOff", true)
	parked := h.instance(0)
	if pods := h.podsOf(0); len(pods) != 2 {
		t.Fatalf("parked gang has %d pods, want both members present and wedged", len(pods))
	}

	h.clearWedge()
	if !h.run(40, func() bool { return servingReady(h, 0, 2) }) {
		h.dumpState("gang re-arm")
		t.Fatalf("the re-armed gang never healed")
	}
	if got := h.instance(0); got.Incarnation != parked.Incarnation+1 {
		t.Errorf("incarnation = %d, want %d", got.Incarnation, parked.Incarnation+1)
	}
}

// Under RestartPolicy=None the only repair is the crash-loop one, and its
// parked attempt re-arms the same way: the policy governs whether a repair
// starts, not whether one that started may try again.
func TestSpentRepair_CrashLoopRepairUnderPolicyNone_ReArmsOnTheLadder(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.retryPolicy = retryLadder(3, time.Minute)
	h.driveToReadyOnV1()

	// The running pod crash-loops, and so does every rebuild while the
	// cause stands.
	h.armWedge("CrashLoopBackOff", true)
	h.wedgeLivePods("CrashLoopBackOff")
	if !h.run(40, func() bool { return parkedRepair(h, 0) }) {
		h.dumpState("crash-loop park")
		t.Fatalf("the crash-loop repair never parked at Failed")
	}
	parked := h.instance(0)

	h.clearWedge()
	if !h.run(40, func() bool { return servingReady(h, 0, 1) }) {
		h.dumpState("crash-loop re-arm")
		t.Fatalf("the re-armed crash-loop repair never healed")
	}
	if got := h.instance(0); got.Incarnation != parked.Incarnation+1 {
		t.Errorf("incarnation = %d, want %d", got.Incarnation, parked.Incarnation+1)
	}
}

// A cause that never clears spends the ladder: each re-arm rebuilds once
// more, and after maxAttempts the row parks for good — no further
// incarnation, one RepairRetriesExhausted warning — until an operator
// resets it or publishes a corrected revision.
func TestSpentRepair_NeverHeals_ParksAfterTheLadderIsSpent(t *testing.T) {
	h := newRestartHarness(t, 1)
	h.retryPolicy = retryLadder(2, 30*time.Second)
	h.recorder = record.NewFakeRecorder(64)
	parkRepair(t, h, "CrashLoopBackOff", true)
	first := h.instance(0).Incarnation

	spent := func() bool {
		s := h.instance(0)
		return parkedRepair(h, 0) && s.Operation.RetryCount == 2
	}
	if !h.run(60, spent) {
		h.dumpState("ladder")
		t.Fatalf("the ladder was never spent")
	}
	final := h.instance(0)
	if final.Incarnation != first+2 {
		t.Errorf("incarnation = %d, want %d: one rebuild per re-arm", final.Incarnation, first+2)
	}
	h.settle(20)
	if got := h.instance(0); !parkedRepair(h, 0) || got.Incarnation != final.Incarnation {
		h.dumpState("parked for good")
		t.Fatalf("the spent ladder re-armed again: %+v", got)
	}
	if n := eventCount(h, string(workloadtypes.EventReasonRepairRetriesExhausted)); n != 1 {
		t.Errorf("RepairRetriesExhausted events = %d, want exactly 1", n)
	}
}

// No ladder configured is the fail-safe: a crash-looping set stays
// parked, with no rebuild and no attempt counted, until an operator acts.
func TestSpentRepair_NoLadder_StaysParked(t *testing.T) {
	h := newRestartHarness(t, 1)
	h.retryPolicy = nil
	h.recorder = record.NewFakeRecorder(64)
	parkRepair(t, h, "CrashLoopBackOff", true)
	parked := h.instance(0)

	h.settle(40)
	got := h.instance(0)
	if !parkedRepair(h, 0) || got.Incarnation != parked.Incarnation || got.Operation.RetryCount != 0 {
		h.dumpState("unconfigured")
		t.Fatalf("an unconfigured ladder must leave the repair parked as it was: %+v", got)
	}
	if n := eventCount(h, string(workloadtypes.EventReasonRepairRetriesExhausted)); n != 0 {
		t.Errorf("RepairRetriesExhausted events = %d, want none: there is no ladder to exhaust", n)
	}
}

// A repair parked on a cause the kubelet retries in place — a ConfigMap
// key the container cannot find, an image it cannot pull — owes no
// re-arm: a fresh pod set would wedge on the same cause. Past the
// ladder's first delay and past its whole span the row stays Failed with
// its reason, the wedged pods stay where the kubelet retries them, no
// Restarting stamp lands, and the status says once what the park waits
// for. The recovery is the kubelet's: with the key or image back the
// parked pods start, and the repair resumes its promote on the pods it
// already has, single pod and gang alike.
func TestSpentRepair_KubeletRetriedCause_WaitsForTheFixWithoutReArm(t *testing.T) {
	const note = "waiting for the configuration or image the pod needs; the kubelet retries in place"
	for _, tc := range []struct {
		name   string
		reason string
		gang   bool
	}{
		{"single pod/CreateContainerConfigError", "CreateContainerConfigError", false},
		{"single pod/ImagePullBackOff", "ImagePullBackOff", false},
		{"gang/CreateContainerConfigError", "CreateContainerConfigError", true},
		{"gang/ImagePullBackOff", "ImagePullBackOff", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var h *recoveryHarness
			pods := 1
			if tc.gang {
				h = newRecoveryHarness(t, true)
				h.retryPolicy = retryLadder(3, time.Minute)
				h.recorder = record.NewFakeRecorder(256)
				h.driveToReadyOnV1()
				pods = 2
			} else {
				h = newRestartHarness(t, 1)
				h.retryPolicy = retryLadder(3, time.Minute)
				h.recorder = record.NewFakeRecorder(256)
			}
			// The wedge is not sticky: the kubelet retries the parked pods
			// in place once the cause is gone.
			parkRepair(t, h, tc.reason, false)
			parked := h.instance(0)
			parkedPods := podKeys(h.podsOf(0))
			if len(h.podsOf(0)) != pods {
				t.Fatalf("parked set has %d pods, want %d present and wedged", len(h.podsOf(0)), pods)
			}
			restarts := eventCount(h, string(workloadtypes.EventReasonRestartTriggered))

			// Every rung of the configured ladder, and a little more.
			ladderSpan := time.Minute + 2*time.Minute + 4*time.Minute
			until := parked.LastFailure.Time.Add(ladderSpan + time.Minute)
			still := func() {
				got := h.instance(0)
				if !parkedRepair(h, 0) || got.Incarnation != parked.Incarnation || got.Operation.RetryCount != 0 {
					h.dumpState("re-arm")
					t.Fatalf("the park re-armed at %v: %+v", h.clk.Now(), got)
				}
				if got.LastFailure == nil || got.LastFailure.Reason != tc.reason {
					t.Fatalf("lastFailure = %+v, want the reason %s kept", got.LastFailure, tc.reason)
				}
				if keys := podKeys(h.podsOf(0)); keys != parkedPods {
					t.Fatalf("pods changed under the park: %s, want %s", keys, parkedPods)
				}
				if n := eventCount(h, string(workloadtypes.EventReasonRestartTriggered)); n != restarts {
					t.Fatalf("RestartTriggered events = %d, want %d: no re-arm", n, restarts)
				}
			}
			if !h.runWithInvariant(60, func() bool { return !h.clk.Now().Before(until) }, still) {
				t.Fatalf("the clock never reached the end of the ladder's span")
			}
			got := h.instance(0)
			if !strings.Contains(got.LastFailure.Message, note) {
				t.Errorf("lastFailure.message = %q, want it to say what the park waits for", got.LastFailure.Message)
			}
			if n := eventCount(h, string(workloadtypes.EventReasonRepairWaitingOnWorkload)); n != 1 {
				t.Errorf("RepairWaitingOnWorkload events = %d, want exactly 1", n)
			}
			if n := eventCount(h, string(workloadtypes.EventReasonRepairRetriesExhausted)); n != 0 {
				t.Errorf("RepairRetriesExhausted events = %d, want none: the ladder is not what holds the row", n)
			}

			// The key or image is back: the kubelet starts the parked pods.
			h.clearWedge()
			if !h.run(40, func() bool { return servingReady(h, 0, pods) }) {
				h.dumpState("resume")
				t.Fatalf("the Instance never returned to Ready once the cause was gone")
			}
			got = h.instance(0)
			if got.Incarnation != parked.Incarnation {
				t.Errorf("incarnation = %d, want %d: the pods that came up are promoted, not rebuilt", got.Incarnation, parked.Incarnation)
			}
			if got.RunningRevision != h.revV1.Name {
				t.Errorf("running revision = %q, want %q", got.RunningRevision, h.revV1.Name)
			}
			if keys := podKeys(h.podsOf(0)); keys != parkedPods {
				t.Errorf("pods after the heal = %s, want the parked set %s: no controller rebuild", keys, parkedPods)
			}
			if n := eventCount(h, string(workloadtypes.EventReasonRestartTriggered)); n != restarts {
				t.Errorf("RestartTriggered events = %d, want %d: the recovery is the kubelet's, not a re-arm", n, restarts)
			}
		})
	}
}

// A parked repair whose pod set is entirely gone or terminal has nothing
// left to resume or re-arm. The stories below drive its one exit end to
// end through Reconcile: the restart pass closes the spent operation and
// the Create pass rebuilds the row as the fresh start it now is, while a
// set that is still live keeps the park's own exits.

// spentRepairLossCases are the parks whose set may be lost: a crash loop
// with and without a ladder, and a cause the kubelet retries in place,
// which with no pod left has nothing to retry; each as a single pod and
// as a gang.
var spentRepairLossCases = []struct {
	name   string
	reason string
	ladder *workloadtypes.RetryPolicy
	gang   bool
}{
	{"single pod/crash loop/ladder configured", "CrashLoopBackOff", retryLadder(3, 2*time.Minute), false},
	{"single pod/crash loop/ladder unconfigured", "CrashLoopBackOff", nil, false},
	{"single pod/kubelet-retried cause/ladder configured", "ImagePullBackOff", retryLadder(3, 2*time.Minute), false},
	{"gang/crash loop/ladder configured", "CrashLoopBackOff", retryLadder(3, 2*time.Minute), true},
	{"gang/kubelet-retried cause/ladder configured", "ImagePullBackOff", retryLadder(3, 2*time.Minute), true},
}

// spentRepairExitPasses bounds the passes a closed park may take to serve
// again: the close, the recycle of a dead occupant, the create, the
// serving gate and the promote, with room for the pacing between them.
const spentRepairExitPasses = 12

// parkedRepairHarness is a converged Instance, single pod or gang, with
// its repair parked on reason: the shape every loss story starts from.
func parkedRepairHarness(t *testing.T, gang bool, reason string, ladder *workloadtypes.RetryPolicy, grace time.Duration) *recoveryHarness {
	t.Helper()
	var h *recoveryHarness
	if gang {
		h = newRecoveryHarness(t, true)
		h.retryPolicy = ladder
		h.recorder = record.NewFakeRecorder(256)
		h.podGrace = grace
		h.driveToReadyOnV1()
	} else {
		h = newRestartHarness(t, 1)
		h.retryPolicy = ladder
		h.recorder = record.NewFakeRecorder(256)
		h.podGrace = grace
	}
	parkRepair(t, h, reason, true)
	return h
}

// stillTerminating reports whether a pod of the Instance is still on its
// way out: deleted, with the object not yet gone.
func stillTerminating(h *recoveryHarness, idx int32) bool {
	for _, pod := range podObjectsOf(h, idx) {
		if pod.DeletionTimestamp != nil {
			return true
		}
	}
	return false
}

// assertFreshStartServes runs the passes a closed park takes to serve
// again and checks the exit's shape: no re-arm along the way, the row
// Ready on the running revision under its incarnation, a new set in
// place of the parked one, the close said once.
func assertFreshStartServes(t *testing.T, h *recoveryHarness, parked *workloadtypes.InstanceStatus, parkedKeys string, pods int, restarts int) {
	t.Helper()
	checks := 0
	served := h.runWithInvariant(spentRepairExitPasses, func() bool { return servingReady(h, 0, pods) }, func() {
		checks++
		if s := h.instance(0); s != nil && s.Phase == workloadtypes.InstancePhaseRestarting {
			h.dumpState("re-arm")
			t.Fatalf("the park re-armed instead of closing: %+v", s)
		}
	})
	if !served {
		h.dumpState("fresh start")
		t.Fatalf("the closed park never served again within %d passes", spentRepairExitPasses)
	}
	t.Logf("served again after %d passes", checks-1)
	got := h.instance(0)
	if got.Incarnation != parked.Incarnation {
		t.Errorf("incarnation = %d, want %d: the fresh start rebuilds under the row's incarnation, there was nothing to drain", got.Incarnation, parked.Incarnation)
	}
	if got.RunningRevision != h.revV1.Name {
		t.Errorf("running revision = %q, want %q", got.RunningRevision, h.revV1.Name)
	}
	if keys := podKeys(h.podsOf(0)); keys == parkedKeys {
		t.Errorf("the set serving is the parked one %s, want a rebuilt set", keys)
	}
	if n := eventCount(h, string(workloadtypes.EventReasonRepairClosed)); n != 1 {
		t.Errorf("RepairClosed events = %d, want exactly 1", n)
	}
	if n := eventCount(h, string(workloadtypes.EventReasonRestartTriggered)); n != restarts {
		t.Errorf("RestartTriggered events = %d, want %d: the exit is the fresh start, not a re-arm", n, restarts)
	}
}

// A parked repair whose pods the kubelet evicts after the park has no
// live pod left to resume or re-arm. The restart pass closes the spent
// operation on its next pass and the Create pass takes the row as the
// fresh start it now is: the dead occupants are recycled and a new set is
// built, which serves at the running revision under the row's
// incarnation. No re-arm opens, whatever parked the repair and whether
// or not a ladder is configured.
func TestSpentRepair_PodsEvicted_ClosesAndRebuildsAsAFreshStart(t *testing.T) {
	for _, tc := range spentRepairLossCases {
		t.Run(tc.name, func(t *testing.T) {
			h := parkedRepairHarness(t, tc.gang, tc.reason, tc.ladder, 0)
			parked := h.instance(0)
			parkedKeys := podKeys(h.podsOf(0))
			pods := len(h.podsOf(0))
			restarts := eventCount(h, string(workloadtypes.EventReasonRestartTriggered))

			// The cause is gone for any pod built from now on; the parked
			// set is evicted where it stands.
			h.clearWedge()
			h.evictPods(0)
			assertFreshStartServes(t, h, parked, parkedKeys, pods, restarts)
			if n := eventCount(h, string(workloadtypes.EventReasonTerminalPodRecycled)); n != 1 {
				t.Errorf("TerminalPodRecycled events = %d, want exactly 1: the Create pass recycles the evicted set", n)
			}
		})
	}
}

// A parked repair whose pods are all gone — deleted under it after the
// park — has nothing left to resume or re-arm, so the restart pass closes
// it on its next pass and the Create pass rebuilds the row as a fresh
// start, whatever parked it and whether or not a ladder is configured. A
// pod still terminating keeps the park: the name is held until the pod
// is gone, and the close follows the pod out.
func TestSpentRepair_NoPodsLeft_ClosesAndRebuildsAsAFreshStart(t *testing.T) {
	for _, tc := range spentRepairLossCases {
		for _, grace := range []time.Duration{0, 2 * time.Minute} {
			t.Run(fmt.Sprintf("%s/grace=%s", tc.name, grace), func(t *testing.T) {
				h := parkedRepairHarness(t, tc.gang, tc.reason, tc.ladder, grace)
				parked := h.instance(0)
				parkedKeys := podKeys(h.podsOf(0))
				pods := len(h.podsOf(0))
				restarts := eventCount(h, string(workloadtypes.EventReasonRestartTriggered))

				h.clearWedge()
				h.losePods(0)
				if grace > 0 {
					// The deleted pods terminate under their grace; the park
					// holds until the objects are gone.
					gone := h.runWithInvariant(10, func() bool { return !stillTerminating(h, 0) }, func() {
						if stillTerminating(h, 0) && !parkedRepair(h, 0) {
							h.dumpState("terminating")
							t.Fatalf("the park left at %v while its pods were still terminating: %+v", h.clk.Now(), h.instance(0))
						}
					})
					if !gone {
						t.Fatalf("the terminating pods never left")
					}
				}
				assertFreshStartServes(t, h, parked, parkedKeys, pods, restarts)
			})
		}
	}
}

// A parked repair whose set is still live is never closed: a wedged set
// waits for the ladder and is rebuilt by its re-arm, and a set that comes
// up on its own resumes the promote. Along either exit the row never
// reads as a fresh start and the close is never said.
func TestSpentRepair_LiveSetIsNeverClosed(t *testing.T) {
	neverClosed := func(t *testing.T, h *recoveryHarness) func() {
		return func() {
			if s := h.instance(0); s != nil && s.Phase == workloadtypes.InstancePhaseFailed && s.Operation == nil {
				h.dumpState("closed")
				t.Fatalf("the park was closed under a live set: %+v", s)
			}
		}
	}
	t.Run("wedged set re-arms on the ladder", func(t *testing.T) {
		h := parkedRepairHarness(t, false, "CrashLoopBackOff", retryLadder(3, 2*time.Minute), 0)
		parked := h.instance(0)

		h.clearWedge()
		if !h.runWithInvariant(40, func() bool { return servingReady(h, 0, 1) }, neverClosed(t, h)) {
			h.dumpState("re-arm")
			t.Fatalf("the re-armed repair never healed")
		}
		if got := h.instance(0); got.Incarnation != parked.Incarnation+1 {
			t.Errorf("incarnation = %d, want %d: the ladder's re-arm rebuilt the set", got.Incarnation, parked.Incarnation+1)
		}
		if n := eventCount(h, string(workloadtypes.EventReasonRepairClosed)); n != 0 {
			t.Errorf("RepairClosed events = %d, want none under a live set", n)
		}
	})
	t.Run("set that comes up resumes", func(t *testing.T) {
		h := newRestartHarness(t, 1)
		h.retryPolicy = retryLadder(3, 2*time.Minute)
		h.recorder = record.NewFakeRecorder(256)
		parkRepair(t, h, "CreateContainerConfigError", false)
		parked := h.instance(0)
		parkedKeys := podKeys(h.podsOf(0))

		h.clearWedge()
		if !h.runWithInvariant(20, func() bool { return servingReady(h, 0, 1) }, neverClosed(t, h)) {
			h.dumpState("resume")
			t.Fatalf("the healed set was never promoted")
		}
		got := h.instance(0)
		if got.Incarnation != parked.Incarnation {
			t.Errorf("incarnation = %d, want %d: a set that came up is promoted, not rebuilt", got.Incarnation, parked.Incarnation)
		}
		if keys := podKeys(h.podsOf(0)); keys != parkedKeys {
			t.Errorf("set after the heal = %s, want the parked set %s", keys, parkedKeys)
		}
		if n := eventCount(h, string(workloadtypes.EventReasonRepairClosed)); n != 0 {
			t.Errorf("RepairClosed events = %d, want none under a live set", n)
		}
	})
}

// podKeys names a pod set by identity — name and creation instant, sorted
// — so a rebuilt pod of the same name reads as a different pod.
func podKeys(pods []*corev1.Pod) string {
	keys := make([]string, 0, len(pods))
	for _, pod := range pods {
		keys = append(keys, wedgeKey(pod))
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// The reset mailbox stays the immediate escape from a parked repair: with
// the pods deleted and the spent operation cleared, the Create pass
// rebuilds the Instance on the next pass, ladder or no ladder.
func TestSpentRepair_ResetMailboxStillRebuilds(t *testing.T) {
	h := newRestartHarness(t, 1)
	h.retryPolicy = retryLadder(1, 30*time.Second)
	parkRepair(t, h, "CrashLoopBackOff", true)
	if !h.run(40, func() bool { return parkedRepair(h, 0) && h.instance(0).Operation.RetryCount == 1 }) {
		h.dumpState("ladder")
		t.Fatalf("the ladder was never spent")
	}

	// What the mailbox does: the pods go, the operation is cleared.
	h.clearWedge()
	h.losePods(0)
	cleared, err := status.ClearFailedInstanceOperation(h.ctx, h.mutateInstance(), 0)
	if err != nil || !cleared {
		t.Fatalf("clear the parked operation: cleared=%v err=%v", cleared, err)
	}
	if !h.run(30, func() bool { return servingReady(h, 0, 1) }) {
		h.dumpState("reset")
		t.Fatalf("the reset Instance was never rebuilt")
	}
}

// A revision Held after its retry budget is spent stays Held when the
// cause of its failures clears: only a new revision or the release
// mailbox admits another attempt at it. The Instance keeps serving the
// revision it ran.
func TestHeldRevision_StaysHeldWhenItsCauseHeals(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(3)

	// The wreckage of the last attempt may still be parked on the bad
	// revision; what the hold forbids is a new attempt.
	badPodsBefore := len(h.badPods())
	h.badImageHeals = true
	h.settle(12)
	if b := h.findBlock(h.revBad.Name); b == nil || b.State != workloadtypes.RetryBlockHeld {
		t.Fatalf("block = %+v, want Held: a healed cause releases nothing on its own", b)
	}
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != h.revV1.Name {
		h.dumpState("held")
		t.Fatalf("the Instance must keep serving v1 while the revision is Held: %+v", s)
	}
	if got := len(h.badPods()); got > badPodsBefore {
		h.dumpState("held")
		t.Fatalf("bad-revision pods grew from %d to %d: no attempt at a Held revision may open", badPodsBefore, got)
	}

	// The release mailbox's effect: the block is gone, and the next pass
	// attempts the revision again, which now converges.
	h.blocks = nil
	if !h.run(40, func() bool {
		s := h.instance(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.RunningRevision == h.revBad.Name
	}) {
		h.dumpState("released")
		t.Fatalf("the released revision never converged")
	}
}

// steadyStateGroupGates are the two answers a rollout group's gate stack
// gives at steady state, each of which reads the dark Instance as
// unavailability already open: the plan hold while no run is open, and
// the group's unavailability gate once a run is.
var steadyStateGroupGates = []struct {
	name  string
	build func(h *recoveryHarness, replicas, maxUnavailable int32, consults *int) func(workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string)
}{
	{"no run open", func(_ *recoveryHarness, _, _ int32, consults *int) func(workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
		return planHoldGate(consults)
	}},
	{"run open", coordinationUnavailabilityGate},
}

// breakWholeInstance parks every pod of the Instance at idx on the crash
// image for form: the shape a broken revision gives a gang, where no
// member serves.
func breakWholeInstance(t *testing.T, h *recoveryHarness, idx int32, form crashForm) {
	t.Helper()
	image := crashStartImage
	if form == crashAfterUp {
		image = crashLoadImage
	}
	for _, runner := range h.desired.Runners {
		name := ""
		if h.multiPod {
			name = runner.Name
		}
		h.crashPod(idx, name, image)
	}
}

// TestReconcile_GroupedCrashLoop_RepairsAtSteadyState is the closed-loop
// story for a Component listed in a rollout group: converged with no run
// open, one Instance falls into a crash loop on the running revision. The
// gate in front of the Component answers as the group's gate stack does
// — the plan hold while no run is open, the unavailability gate once one
// is — and either answer would hold the repair; the per-Component budget
// is one, or zero, the lifecycle under which no serving pod may ever be
// taken offline. The Instance's pod set serves nothing, so neither layer
// is asked: the repair opens, rebuilds the Instance Ready on its
// revision, and the healthy peer never leaves rotation. Single-pod and
// leader+worker shapes, both restart policies, both crash forms.
func TestReconcile_GroupedCrashLoop_RepairsAtSteadyState(t *testing.T) {
	const replicas = 2
	for _, gang := range []bool{false, true} {
		shape := "single"
		if gang {
			shape = "gang"
		}
		for _, maxUnavailable := range []int32{1, 0} {
			for _, gate := range steadyStateGroupGates {
				for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
					for _, form := range []crashForm{crashAtStart, crashAfterUp} {
						t.Run(fmt.Sprintf("%s/maxUnavailable=%d/%s/%s/%s", shape, maxUnavailable, gate.name, policy, form), func(t *testing.T) {
							h := offTargetCrashHarness(t, gang, workloadtypes.UpdateStrategyRecreatePod, 0, maxUnavailable, policy, replicas)
							consults := 0
							h.gate = gate.build(h, replicas, maxUnavailable, &consults)
							const peer, victim = int32(0), int32(replicas - 1)
							breakWholeInstance(t, h, victim, form)
							h.events = nil

							peerStaysInRotation := func() {
								if ceiling := replicas * len(h.desired.Runners); len(h.livePods()) > ceiling {
									t.Fatalf("%d live pods, want at most %d: a repair never surges", len(h.livePods()), ceiling)
								}
								if !h.instanceInRotation(peer) || !h.atIncarnation(peer, 1) {
									h.dumpState("peer out of rotation")
									t.Fatalf("the healthy peer left rotation while the wedged Instance was repaired")
								}
							}
							repaired := func() bool { return h.settledOn(h.revV1, replicas) && h.repairedAbove(victim, 1) }
							if !h.runWithInvariant(40, repaired, peerStaysInRotation) {
								h.dumpState("after the crash")
								t.Fatalf("the wedged Instance was never repaired: held by a budget or a gate it owes nothing to")
							}
							if !h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
								t.Fatalf("the repair must be recorded on the Instance: %v", h.events)
							}
							if h.sawEvent(workloadtypes.EventReasonRepairHeld) {
								t.Fatalf("a repair was announced held: %v", h.events)
							}
							if consults != 0 {
								t.Fatalf("gate consults = %d, want 0: the dark Instance's repair is not put to the gate", consults)
							}
						})
					}
				}
			}
		}
	}
}

// restartsRecorded counts the RestartTriggered events the harness has
// recorded since its events were last cleared.
func (h *recoveryHarness) restartsRecorded() int {
	n := 0
	for _, e := range h.events {
		if strings.Contains(e, string(workloadtypes.EventReasonRestartTriggered)) {
			n++
		}
	}
	return n
}

// rollStaysWithinBudget fails the test when the roll of the peers exceeds
// the budget it runs under: one surge in flight under SurgeThenDrain, one
// Instance offline otherwise.
func rollStaysWithinBudget(t *testing.T, h *recoveryHarness, strategy workloadtypes.UpdateStrategyType, surge, unavail int32) {
	t.Helper()
	statuses := h.irStatuses()
	if strategy == workloadtypes.UpdateStrategySurgeThenDrain {
		if n := escalation.CurrentSurgeInFlight(statuses); n > surge {
			t.Fatalf("%d surges in flight, budget %d", n, surge)
		}
		return
	}
	if n := escalation.CurrentUnavailableInFlight(statuses); n > unavail {
		t.Fatalf("%d Instances offline for the roll, budget %d", n, unavail)
	}
}

// TestReconcile_OpenRepair_TargetMovesBeforeTheCreate: an Instance
// crash-loops on the running revision and its repair opens; the pods are
// deleted and the rebuild has created none of its own when the fix is
// pushed. The rebuild renders the pushed revision: the Instance comes back
// once, on the fix, with the row's revision and the pods' labels naming it;
// no pod of the broken revision is created after the push, no roll opens on
// the Instance afterwards, and every peer is rolled within the budget.
// Single pod and gang, both restart policies, SurgeThenDrain and
// RecreatePod.
func TestReconcile_OpenRepair_TargetMovesBeforeTheCreate(t *testing.T) {
	const replicas = 3
	for _, gang := range []bool{false, true} {
		shape := "single"
		if gang {
			shape = "gang"
		}
		for _, sc := range offTargetStrategies[:2] {
			for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
				t.Run(fmt.Sprintf("%s/%s/%s", shape, sc.strategy, policy), func(t *testing.T) {
					h := offTargetCrashHarness(t, gang, sc.strategy, sc.surge, sc.unavail, policy, replicas)
					victim := int32(replicas - 1)
					before := h.instance(victim).Incarnation
					breakWholeInstance(t, h, victim, crashAtStart)
					h.events = nil
					podsDeleted := func() bool {
						return h.repairInFlight() && len(h.podsOf(victim)) == 0
					}
					if !h.run(5, podsDeleted) {
						h.dumpState("after the crash")
						t.Fatalf("the repair never opened and deleted the crash-looping pods")
					}

					h.setTarget(h.revFixed, fixedImage)
					brokenHash := query.RevisionOf(h.revV1).Hash()
					rebuiltOnceOnTheFix := func() {
						for _, pod := range h.podsOf(victim) {
							if pod.Labels[query.LabelRevisionHash] == brokenHash {
								h.dumpState("broken revision rebuilt")
								t.Fatalf("pod %s was created on the broken revision after the fix was pushed", pod.Name)
							}
						}
						if s := h.instance(victim); s != nil && s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationUpdate {
							h.dumpState("roll on the rebuilt Instance")
							t.Fatalf("a roll opened on the Instance the repair rebuilt: %+v", s.Operation)
						}
						rollStaysWithinBudget(t, h, sc.strategy, sc.surge, sc.unavail)
					}
					if !h.runWithInvariant(100, func() bool { return h.settledOn(h.revFixed, replicas) }, rebuiltOnceOnTheFix) {
						h.dumpState("after the push")
						t.Fatalf("the Component never settled on the fixed revision")
					}
					if s := h.instance(victim); s.Incarnation != before+1 {
						t.Fatalf("instance %d incarnation = %d, want %d: the Instance is rebuilt once", victim, s.Incarnation, before+1)
					}
					h.requirePodsRender(victim, h.revFixed, fixedImage)
					if n := h.restartsRecorded(); n != 1 {
						t.Fatalf("restarts recorded = %d, want exactly the repair that was already open: %v", n, h.events)
					}
					if h.sawEvent(workloadtypes.EventReasonRepairHeld) {
						t.Fatalf("a repair was announced held: %v", h.events)
					}
				})
			}
		}
	}
}

// TestReconcile_OpenRepair_TargetMovesAfterThePodsExist: the repair has
// created its pods on the running revision when the fix is pushed. The
// repair finishes on the revision those pods carry and the roll replaces
// the Instance at the fix from Ready: tearing down a starting pod set is
// the roll's job, under its budget. Single pod and gang.
func TestReconcile_OpenRepair_TargetMovesAfterThePodsExist(t *testing.T) {
	for _, gang := range []bool{false, true} {
		shape := "single"
		if gang {
			shape = "gang"
		}
		t.Run(shape, func(t *testing.T) {
			h := offTargetCrashHarness(t, gang, workloadtypes.UpdateStrategySurgeThenDrain, 1, 0, workloadtypes.RestartPolicyRecreateInstance, 1)
			before := h.instance(0).Incarnation
			breakWholeInstance(t, h, 0, crashAtStart)
			h.events = nil
			podsRebuilt := func() bool {
				return h.repairInFlight() && len(h.podsOf(0)) == len(h.desired.Runners)
			}
			if !h.run(6, podsRebuilt) {
				h.dumpState("after the crash")
				t.Fatalf("the repair never rebuilt the pod set")
			}

			h.setTarget(h.revFixed, fixedImage)
			if !h.run(20, func() bool { return h.repairedAbove(0, before) }) {
				h.dumpState("open repair after the push")
				t.Fatalf("the repair did not finish on the pods it had created")
			}
			if s := h.instance(0); s.RunningRevision != h.revV1.Name {
				t.Fatalf("repaired row records %q, want %s: the repair finishes on the revision its pods carry", s.RunningRevision, h.revV1.Name)
			}
			h.requirePodsRender(0, h.revV1, goodImage)
			if !h.run(80, func() bool { return h.settledOn(h.revFixed, 1) }) {
				h.dumpState("roll after the repair")
				t.Fatalf("the roll did not replace the repaired Instance at the fix")
			}
			if n := h.restartsRecorded(); n != 1 {
				t.Fatalf("restarts recorded = %d, want exactly the repair that was already open: %v", n, h.events)
			}
		})
	}
}

// TestReconcile_OpenRepair_PeersStayInRotationWhileTheTargetMoves: three
// Instances, the repair of one has rebuilt its pod set and that set is still
// starting when the target moves. The open repair owns the Component's
// wake-ups until it finishes, so no healthy peer leaves rotation while the
// repaired Instance serves nothing: the peers stay untouched on the running
// revision, no roll opens and no pod is added. Once the rebuilt set is up the
// repair finishes on the revision its pods carry, and the roll then lands the
// target on every Instance within its budget, with exactly the one repair on
// record. Every strategy the engine rolls a single-pod Instance under, both
// restart policies.
func TestReconcile_OpenRepair_PeersStayInRotationWhileTheTargetMoves(t *testing.T) {
	const replicas = 3
	const victim = int32(replicas - 1)
	peers := []int32{0, 1}
	for _, sc := range offTargetStrategies[:3] {
		for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
			t.Run(fmt.Sprintf("%s/%s", sc.strategy, policy), func(t *testing.T) {
				h := offTargetCrashHarness(t, false, sc.strategy, sc.surge, sc.unavail, policy, replicas)
				before := h.instance(victim).Incarnation
				breakWholeInstance(t, h, victim, crashAtStart)
				h.events = nil
				podsRebuilt := func() bool {
					pods := h.podsOf(victim)
					return h.repairInFlight() && len(pods) == 1 && pods[0].Spec.Containers[0].Image == goodImage
				}
				if !h.run(6, podsRebuilt) {
					h.dumpState("after the crash")
					t.Fatalf("the repair never rebuilt the pod set")
				}
				// The rebuilt pod stays in its startup window for as long as
				// the story needs: the readiness it is held on is lifted below.
				rebuilt := h.podsOf(victim)[0].Name
				h.readinessFails[rebuilt] = true

				h.setTarget(h.revFixed, fixedImage)
				peersUntouched := func() {
					if s := h.instance(victim); s == nil || s.Phase != workloadtypes.InstancePhaseRestarting {
						h.dumpState("repair left")
						t.Fatalf("the open repair must keep driving the repaired Instance: %+v", s)
					}
					for _, s := range h.irStatuses() {
						if s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationUpdate {
							h.dumpState("roll opened")
							t.Fatalf("a roll opened on instance %d while the repair was open", s.Index)
						}
					}
					for _, peer := range peers {
						if !h.untouchedOn(peer, h.revV1) || !h.instanceInRotation(peer) {
							h.dumpState("peer left rotation")
							t.Fatalf("instance %d left rotation while the repaired instance %d served nothing", peer, victim)
						}
					}
					if n := len(h.livePods()); n != replicas {
						h.dumpState("pod added")
						t.Fatalf("%d live pods during the open repair, want %d: nothing surges beside a repair", n, replicas)
					}
				}
				h.runWithInvariant(6, func() bool { return false }, peersUntouched)

				delete(h.readinessFails, rebuilt)
				if !h.run(20, func() bool { return h.repairedAbove(victim, before) }) {
					h.dumpState("open repair after the push")
					t.Fatalf("the repair did not finish on the pods it had created")
				}
				if s := h.instance(victim); s.RunningRevision != h.revV1.Name {
					t.Fatalf("repaired row records %q, want %s: the repair finishes on the revision its pods carry", s.RunningRevision, h.revV1.Name)
				}
				h.requirePodsRender(victim, h.revV1, goodImage)

				withinBudget := func() {
					if h.repairInFlight() {
						h.dumpState("second repair")
						t.Fatalf("a repair opened during the roll")
					}
					rollStaysWithinBudget(t, h, sc.strategy, sc.surge, sc.unavail)
				}
				if !h.runWithInvariant(100, func() bool { return h.settledOn(h.revFixed, replicas) }, withinBudget) {
					h.dumpState("roll after the repair")
					t.Fatalf("the roll did not land the target on every Instance")
				}
				if n := h.restartsRecorded(); n != 1 {
					t.Fatalf("restarts recorded = %d, want exactly the repair that was already open: %v", n, h.events)
				}
				if h.sawEvent(workloadtypes.EventReasonRepairHeld) {
					t.Fatalf("a repair was announced held: %v", h.events)
				}
			})
		}
	}
}

// landedOn lists the Instances Ready on rev with no operation, lowest
// index first: the Instances whose roll to rev is finished.
func (h *recoveryHarness) landedOn(rev *appsv1.ControllerRevision) []int32 {
	var out []int32
	for _, s := range h.irStatuses() {
		if s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.RunningRevision == rev.Name {
			out = append(out, s.Index)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// untouchedOn reports whether the Instance at idx is Ready on rev with no
// operation: no roll and no repair has taken it.
func (h *recoveryHarness) untouchedOn(idx int32, rev *appsv1.ControllerRevision) bool {
	s := h.instance(idx)
	return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.RunningRevision == rev.Name
}

// instanceServes reports whether the Instance at idx has its full pod set
// in rotation (ContainersReady with the serving gate), whatever else is
// alive under its index: a surge replacement standing beside a serving
// source does not take the source out of rotation.
func (h *recoveryHarness) instanceServes(idx int32) bool {
	serving := 0
	for _, pod := range h.podsOf(idx) {
		if podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod) {
			serving++
		}
	}
	return serving >= len(h.desired.Runners)
}

// rollingGroupGate stands in for the gate stack of a rollout group with a
// run open, read the way the group gate reads the Component's published
// counters. A surge start is admitted while the surges in flight, plus
// the surges this pass opened, plus this one, stay within maxSurge; a
// drain-first start while the Instances not serving, plus the starts this
// pass opened, plus this one, stay within maxUnavailable; an in-place
// start returns the same pod and is waived. consults counts the calls.
func rollingGroupGate(h *recoveryHarness, replicas, maxSurge, maxUnavailable int32, consults *int) func(workloadtypes.UpdateStrategyType, int32, int32) (bool, workloadtypes.RolloutHoldGate, string) {
	drainFirst := coordinationUnavailabilityGate(h, replicas, maxUnavailable, consults)
	return func(strategy workloadtypes.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, workloadtypes.RolloutHoldGate, string) {
		switch strategy {
		case workloadtypes.UpdateStrategySurgeThenDrain:
			*consults++
			projected := escalation.CurrentSurgeInFlight(h.irStatuses()) + inFlightSurge + 1
			if projected > maxSurge {
				return false, workloadtypes.RolloutHoldGateBudget, fmt.Sprintf("surge budget %d exhausted (would become %d)", maxSurge, projected)
			}
			return true, "", ""
		case workloadtypes.UpdateStrategyInPlaceIfPossible, workloadtypes.UpdateStrategyInPlaceOnly:
			*consults++
			return true, "", ""
		default:
			return drainFirst(strategy, inFlightSurge, inFlightUnavail)
		}
	}
}

// rollShapes are the two places a Component rolls from: standalone, with
// no gate in front of it, and listed in a rollout group with a run open.
var rollShapes = []struct {
	name  string
	gated bool
}{
	{"standalone", false},
	{"rolling group", true},
}

// requireDarkInstanceRollsFirst drives the roll on the fixed revision
// until done holds and checks, after every pass, the story of a fix
// pushed over a dark Instance: the roll stays inside its budget, no
// repair opens, every pod created after the bump runs the fix, the
// Instances in held never move, and until the first new landing every
// Instance in serving is untouched on v1 and in rotation, so the first
// landing is the dark Instance's and costs no capacity. Landings that
// predate the call are not counted.
func requireDarkInstanceRollsFirst(t *testing.T, h *recoveryHarness, victim int32, serving, held []int32, strategy workloadtypes.UpdateStrategyType, surge, unavail int32, done func() bool) {
	t.Helper()
	bumpedAt := h.clk.Now()
	fixedHash := query.RevisionOf(h.revFixed).Hash()
	alreadyLanded := map[int32]bool{}
	for _, idx := range h.landedOn(h.revFixed) {
		alreadyLanded[idx] = true
	}
	var firstLanded []int32
	invariant := func() {
		if h.repairInFlight() {
			h.dumpState("repair opened")
			t.Fatalf("a repair opened on an Instance whose revision is not the roll target")
		}
		sts := h.irStatuses()
		if strategy == workloadtypes.UpdateStrategySurgeThenDrain {
			if n := escalation.CurrentSurgeInFlight(sts); n > surge {
				t.Fatalf("%d surges in flight, budget %d", n, surge)
			}
		} else if n := escalation.CurrentUnavailableInFlight(sts); n > unavail {
			t.Fatalf("%d Instances offline for the roll, budget %d", n, unavail)
		}
		for _, pod := range h.livePods() {
			if !pod.CreationTimestamp.Time.After(bumpedAt) {
				continue
			}
			if pod.Labels[query.LabelRevisionHash] != fixedHash || pod.Spec.Containers[0].Image != fixedImage {
				h.dumpState("pod created off the fix")
				t.Fatalf("pod %s was created on %s after the fix was pushed", pod.Name, pod.Spec.Containers[0].Image)
			}
		}
		for _, idx := range held {
			if !h.untouchedOn(idx, h.revV1) {
				h.dumpState("held instance moved")
				t.Fatalf("instance %d is held and must not move", idx)
			}
		}
		if firstLanded != nil {
			return
		}
		var landed []int32
		for _, idx := range h.landedOn(h.revFixed) {
			if !alreadyLanded[idx] {
				landed = append(landed, idx)
			}
		}
		if len(landed) > 0 {
			firstLanded = landed
			for _, peer := range serving {
				if !h.untouchedOn(peer, h.revV1) {
					h.dumpState("first landing")
					t.Fatalf("instance %d was rolled before the dark instance %d landed the fix (landed %v)", peer, victim, landed)
				}
			}
			return
		}
		for _, peer := range serving {
			if !h.untouchedOn(peer, h.revV1) || !h.instanceServes(peer) {
				h.dumpState("peer taken first")
				t.Fatalf("instance %d was taken by the roll before the dark instance %d landed the fix", peer, victim)
			}
		}
	}
	if !h.runWithInvariant(100, done, invariant) {
		h.dumpState("after the bump")
		t.Fatalf("the roll never reached its end state")
	}
	if firstLanded == nil {
		t.Fatalf("no Instance landed the fix")
	}
	if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
		t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
	}
}

// TestReconcile_DarkInstance_RollsFirst is the closed-loop story for the
// order a roll takes its Instances in. Four Instances, the highest index
// crash-looping on the running revision, and a fix pushed as a new
// revision: the broken Instance serves nothing, so replacing it costs no
// capacity, and it is the first to land the fix and serve again; the
// healthy peers stay in rotation until then and roll after it within the
// budget; no pod is rebuilt on the broken revision; the Component ends
// with every Instance on the fix. Single-pod and leader+worker shapes,
// the surge and both drain-first strategies, both crash forms, standalone
// and listed in a rollout group with a run open.
func TestReconcile_DarkInstance_RollsFirst(t *testing.T) {
	const replicas = 4
	for _, gang := range []bool{false, true} {
		shape := "single"
		if gang {
			shape = "gang"
		}
		for _, sc := range offTargetStrategies[:3] {
			for _, roll := range rollShapes {
				for _, form := range []crashForm{crashAtStart, crashAfterUp} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", shape, sc.strategy, roll.name, form), func(t *testing.T) {
						h := offTargetCrashHarness(t, gang, sc.strategy, sc.surge, sc.unavail, workloadtypes.RestartPolicyNone, replicas)
						consults := 0
						if roll.gated {
							h.gate = rollingGroupGate(h, replicas, sc.surge, sc.unavail, &consults)
						}
						const victim = int32(replicas - 1)
						breakWholeInstance(t, h, victim, form)
						h.events = nil
						h.setTarget(h.revFixed, fixedImage)
						requireDarkInstanceRollsFirst(t, h, victim, []int32{0, 1, 2}, nil, sc.strategy, sc.surge, sc.unavail,
							func() bool { return h.settledOn(h.revFixed, replicas) })
					})
				}
			}
		}
	}
}

// TestReconcile_DarkInstance_HeldByPartitionStaysPut: the preference for
// a dark Instance never moves one the partition holds. Four Instances
// under a partition of two, the higher of the two held indices
// crash-looping on the running revision: the two released Instances roll
// while the dark Instance and its held peer stay untouched on their
// revision, neither rolled nor repaired; once the partition is lifted the
// dark Instance is the first of the two to land the fix.
func TestReconcile_DarkInstance_HeldByPartitionStaysPut(t *testing.T) {
	const replicas = 4
	for _, sc := range offTargetStrategies[:2] {
		t.Run(string(sc.strategy), func(t *testing.T) {
			h := offTargetCrashHarness(t, false, sc.strategy, sc.surge, sc.unavail, workloadtypes.RestartPolicyNone, replicas)
			partition := int32(2)
			h.lifecycle.UpdateStrategy.RollingUpdate.Partition = &partition
			const dark, heldPeer = int32(1), int32(0)
			breakWholeInstance(t, h, dark, crashAtStart)
			h.events = nil
			h.setTarget(h.revFixed, fixedImage)

			releasedLanded := func() bool {
				landed := h.landedOn(h.revFixed)
				return len(landed) == 2 && landed[0] == 2 && landed[1] == 3
			}
			heldStayPut := func() {
				for _, idx := range []int32{dark, heldPeer} {
					if !h.untouchedOn(idx, h.revV1) {
						h.dumpState("held instance moved")
						t.Fatalf("instance %d is held by the partition and must not move", idx)
					}
				}
			}
			if !h.runWithInvariant(100, releasedLanded, heldStayPut) {
				h.dumpState("under the partition")
				t.Fatalf("the released Instances never landed the fix under the partition")
			}
			if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
				t.Fatalf("a restart was recorded for a held Instance off the roll target: %v", h.events)
			}

			partition = 0
			requireDarkInstanceRollsFirst(t, h, dark, []int32{heldPeer}, nil, sc.strategy, sc.surge, sc.unavail,
				func() bool { return h.settledOn(h.revFixed, replicas) })
		})
	}
}

// TestReconcile_DarkInstance_PreferredInsideTheCanaryStep: a canary step
// releases a quota of Instances through the projected partition, and the
// preference works inside that quota. Four Instances, a step releasing
// two, the highest index crash-looping on the running revision: the dark
// Instance lands the fix first and its released peer second, the two
// Instances the step holds never move while it stands, and lifting the
// step rolls them within the budget.
func TestReconcile_DarkInstance_PreferredInsideTheCanaryStep(t *testing.T) {
	const replicas = 4
	for _, sc := range offTargetStrategies[:2] {
		t.Run(string(sc.strategy), func(t *testing.T) {
			h := offTargetCrashHarness(t, false, sc.strategy, sc.surge, sc.unavail, workloadtypes.RestartPolicyNone, replicas)
			const dark, releasedPeer = int32(3), int32(2)
			held := []int32{0, 1}
			breakWholeInstance(t, h, dark, crashAtStart)
			h.events = nil
			h.setTarget(h.revFixed, fixedImage)
			step := int32(2)
			h.desired.Pacing = &workloadtypes.WorkloadPacing{Partition: &step}

			quotaLanded := func() bool {
				landed := h.landedOn(h.revFixed)
				return len(landed) == 2 && landed[0] == releasedPeer && landed[1] == dark
			}
			requireDarkInstanceRollsFirst(t, h, dark, []int32{releasedPeer}, held, sc.strategy, sc.surge, sc.unavail, quotaLanded)

			step = 0
			withinBudget := func() {
				sts := h.irStatuses()
				if sc.strategy == workloadtypes.UpdateStrategySurgeThenDrain {
					if n := escalation.CurrentSurgeInFlight(sts); n > sc.surge {
						t.Fatalf("%d surges in flight, budget %d", n, sc.surge)
					}
				} else if n := escalation.CurrentUnavailableInFlight(sts); n > sc.unavail {
					t.Fatalf("%d Instances offline for the roll, budget %d", n, sc.unavail)
				}
			}
			if !h.runWithInvariant(100, func() bool { return h.settledOn(h.revFixed, replicas) }, withinBudget) {
				h.dumpState("after the step was lifted")
				t.Fatalf("the Component never settled on the fixed revision after the step was lifted")
			}
		})
	}
}

// wakeTestGrace is the stuck-pod grace the wake-up stories run under,
// and kubeletBackOff the kubelet's capped back-off between image pulls:
// the next pod event a controller that asked for no wake-up is woken by.
const (
	wakeTestGrace  = 10 * time.Second
	kubeletBackOff = 5 * time.Minute
)

// wakeCadences are the dispatcher cadences the wake-up stories run
// under: none, which leaves a pass with work left to the rate-limited
// backoff, and the one the chart ships.
var wakeCadences = []struct {
	name    string
	requeue workloadtypes.RequeueIntervals
}{
	{"no cadence", workloadtypes.RequeueIntervals{}},
	{"chart cadence", workloadtypes.RequeueIntervals{Operation: 5 * time.Second, Gate: 3 * time.Second}},
}

// graceWakeHarness converges one Instance Ready on v1 at a one-second
// step under a ten-second stuck-pod grace, so its pods are still inside
// the grace once it is Ready: the shape of a pod that comes up and fails
// within its first seconds.
func graceWakeHarness(t *testing.T, gang bool, policy workloadtypes.RestartPolicy, requeue workloadtypes.RequeueIntervals) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarnessFor(t, gang, constants.MainContainerName)
	h.recorder = record.NewFakeRecorder(64)
	h.stuckGrace = wakeTestGrace
	h.stepFloor = time.Second
	h.requeue = requeue
	zero, one := intstr.FromInt32(0), intstr.FromInt32(1)
	h.lifecycle = workloadtypes.Lifecycle{
		RestartPolicy: &policy,
		UpdateStrategy: &workloadtypes.UpdateStrategy{
			Type:          workloadtypes.UpdateStrategyRecreatePod,
			RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &zero, MaxUnavailable: &one},
		},
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 1) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1")
	}
	return h
}

// TestReconcile_CrashLoop_RepairOpensWhenTheGraceEnds is the closed-loop
// story of a wedge first read inside the stuck-pod grace. A Ready
// single-pod Instance's pod is parked in ImagePullBackOff a second or
// two into a ten-second grace, and the kubelet never touches it again:
// its back-off between pulls is minutes. Only the controller's own
// wake-ups bring it back, and the pass that read the pod inside the
// grace owes itself the remainder, so the repair opens the moment the
// grace ends - not on the next cadence tick, and not on the rate-limited
// backoff a pass with no cadence falls to, which doubles per pass - and
// the Instance is rebuilt Ready on its revision. Either restart policy,
// with and without a cadence.
func TestReconcile_CrashLoop_RepairOpensWhenTheGraceEnds(t *testing.T) {
	for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
		for _, cadence := range wakeCadences {
			t.Run(string(policy)+"/"+cadence.name, func(t *testing.T) {
				h := graceWakeHarness(t, false, policy, cadence.requeue)
				graceEnd := h.podOf(0, "").CreationTimestamp.Add(wakeTestGrace)
				if !h.clk.Now().Before(graceEnd) {
					t.Fatalf("the pod is already past its grace at Ready: the story needs a pod that fails inside it")
				}
				h.crashPod(0, "", badImage)
				h.events = nil
				repairOpened := func() bool { return h.restartsRecorded() > 0 }
				if !h.runOnWakeUps(20, kubeletBackOff, repairOpened) {
					h.dumpState("after the crash")
					t.Fatalf("no repair opened on a pod parked past the grace")
				}
				if late := h.clk.Now().Sub(graceEnd); late != 0 {
					t.Fatalf("the repair opened %s after the grace ended: the pass that read the pod inside the grace scheduled no wake-up for the grace end", late)
				}
				if !h.run(40, func() bool { return h.settledOn(h.revV1, 1) && h.atIncarnation(0, 2) }) {
					h.dumpState("after the repair")
					t.Fatalf("the Instance was never rebuilt Ready on its revision")
				}
			})
		}
	}
}

// TestReconcile_CrashLoop_GangWakesForTheOldestMembersGrace: a
// leader+worker gang whose members the apiserver admitted a second
// apart is parked whole, inside the grace. The gang is rebuilt as soon
// as any member is past the grace, so the pass wakes for the older
// member's grace end, whichever runner that is.
func TestReconcile_CrashLoop_GangWakesForTheOldestMembersGrace(t *testing.T) {
	for _, members := range [][2]string{{"leader", "worker"}, {"worker", "leader"}} {
		older, younger := members[0], members[1]
		for _, cadence := range wakeCadences {
			t.Run(younger+" admitted later/"+cadence.name, func(t *testing.T) {
				h := graceWakeHarness(t, true, workloadtypes.RestartPolicyNone, cadence.requeue)
				h.admitLater(0, younger, time.Second)
				graceEnd := h.podOf(0, older).CreationTimestamp.Add(wakeTestGrace)
				if !h.clk.Now().Before(graceEnd) {
					t.Fatalf("the gang is already past its grace at Ready: the story needs members that fail inside it")
				}
				h.crashPod(0, "leader", badImage)
				h.crashPod(0, "worker", badImage)
				h.events = nil
				repairOpened := func() bool { return h.restartsRecorded() > 0 }
				if !h.runOnWakeUps(20, kubeletBackOff, repairOpened) {
					h.dumpState("after the crash")
					t.Fatalf("no repair opened on a gang parked past the grace")
				}
				if late := h.clk.Now().Sub(graceEnd); late != 0 {
					t.Fatalf("the repair opened %s after the older member's grace ended, want the earliest grace end among the gang's parked pods", late)
				}
			})
		}
	}
}

// TestReconcile_CrashLoopInsideGrace_WakesForTheGraceLeft pins the
// wake-up a pass owes when it reads a Ready row's pod parked in a
// terminal waiting reason inside the stuck-pod grace: it opens nothing
// and asks to come back when the grace ends, in place of the
// rate-limited backoff a pass with no cadence falls to and ahead of a
// cadence longer than the remainder. For a gang that is the earliest
// grace end among its parked members, whichever member is the older.
func TestReconcile_CrashLoopInsideGrace_WakesForTheGraceLeft(t *testing.T) {
	ctx := context.Background()
	// Creation stamps carry whole seconds, as the apiserver stores them.
	now := time.Now().Truncate(time.Second)
	for _, tc := range []struct {
		name string
		// ages is how long ago each member was created: one entry for a
		// single-pod Instance, leader then worker for a gang.
		ages    []time.Duration
		requeue workloadtypes.RequeueIntervals
		want    time.Duration
	}{
		{name: "single pod ten seconds into a minute", ages: []time.Duration{10 * time.Second}, want: 50 * time.Second},
		{name: "ahead of a longer cadence", ages: []time.Duration{10 * time.Second}, requeue: workloadtypes.RequeueIntervals{Operation: 2 * time.Minute, Gate: time.Minute}, want: 50 * time.Second},
		{name: "gang, leader older", ages: []time.Duration{40 * time.Second, 10 * time.Second}, want: 20 * time.Second},
		{name: "gang, worker older", ages: []time.Duration{10 * time.Second, 40 * time.Second}, want: 20 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var in workloadtypes.ReconcileInput
			var plan workloadtypes.ComponentPlan
			var pods []client.Object
			if len(tc.ages) == 1 {
				in, plan, pods = crashLoopRepairFixture(t, 1, nil)
			} else {
				in, plan, pods = gangCrashLoopFixture(t, 1, nil)
			}
			in.Clock = clocktesting.NewFakeClock(now)
			in.Requeue = tc.requeue
			for i, age := range tc.ages {
				pods[i].(*corev1.Pod).CreationTimestamp = metav1.NewTime(now.Add(-age))
			}
			c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(pods...).Build()
			store := installRepairStore(&in)
			deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}

			result, err := workload.Reconcile(ctx, deps, in, plan, crashLoopRepairTarget())
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if got := countRestarting(store, 1); got != 0 {
				t.Fatalf("repairs opened = %d, want 0 while every parked pod is inside the grace", got)
			}
			if result.RequeueAfter != tc.want {
				t.Fatalf("result = %+v, want a wake-up in %s, when the oldest parked pod's grace ends", result, tc.want)
			}
		})
	}
}

// restartsInFlight counts the rows under an open Restart.
func (h *recoveryHarness) restartsInFlight() int {
	n := 0
	for _, s := range h.irStatuses() {
		if s.Phase == workloadtypes.InstancePhaseRestarting {
			n++
		}
	}
	return n
}

// TestReconcile_CrashLoopRepair_OpensAtMostTheBatchPerPass is the
// closed-loop story for lifecycle.repairBatchSize: a Component converged
// on v1 has every Instance fall into a crash loop at once, under a budget
// of zero and with no retry ladder configured, so nothing but the limit
// paces the repairs (a configured ladder counts a crash of a promoted pod
// set and admits one rebuild at a time). With a limit of
// three, exactly three repairs open on the first pass and the next three
// on the second, while the first three are still in flight and count for
// nothing; the rest follow on the passes after, and every Instance ends
// Ready on v1 at a bumped Incarnation with no pod beyond the ceiling. A
// gang counts as one: three leader+worker gangs open on the first pass.
// With no limit configured every Instance repairs on the first pass.
// Both restart policies, since the limit belongs to the crash-loop
// trigger and not to the policy.
func TestReconcile_CrashLoopRepair_OpensAtMostTheBatchPerPass(t *testing.T) {
	limit := int32(3)
	for _, tc := range []struct {
		name     string
		gang     bool
		replicas int32
		limit    *int32
		// opensPerPass is how many repairs each pass opens, from the pass
		// the first repair opens on until every Instance is under repair.
		opensPerPass []int
	}{
		{name: "single/limit=3", replicas: 7, limit: &limit, opensPerPass: []int{3, 3, 1}},
		{name: "gang/limit=3", gang: true, replicas: 4, limit: &limit, opensPerPass: []int{3, 1}},
		{name: "single/no limit", replicas: 7, opensPerPass: []int{7}},
	} {
		for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
			t.Run(fmt.Sprintf("%s/%s", tc.name, policy), func(t *testing.T) {
				h := offTargetCrashHarness(t, tc.gang, workloadtypes.UpdateStrategyRecreatePod, 0, 0, policy, tc.replicas)
				h.repairBatchSize = tc.limit
				h.retryPolicy = nil
				for idx := int32(0); idx < tc.replicas; idx++ {
					breakWholeInstance(t, h, idx, crashAtStart)
				}
				h.events = nil

				if !h.run(3, func() bool { return h.restartsRecorded() > 0 }) {
					h.dumpState("after the crash")
					t.Fatalf("no repair opened: a dark Instance's repair is held by nothing")
				}
				opened := 0
				for pass, want := range tc.opensPerPass {
					inFlight := 0
					if pass > 0 {
						inFlight = h.restartsInFlight()
						if inFlight < tc.opensPerPass[pass-1] {
							h.dumpState("before the next batch")
							t.Fatalf("pass %d starts with %d repairs in flight, want at least the %d the pass before opened", pass+1, inFlight, tc.opensPerPass[pass-1])
						}
						h.step()
					}
					if got := h.restartsRecorded() - opened; got != want {
						h.dumpState("after the batch")
						t.Fatalf("pass %d opened %d repairs with %d in flight, want %d: the limit counts the repairs a pass opens, in Instances", pass+1, got, inFlight, want)
					}
					opened += want
				}

				withinTheLimit := func() {
					if ceiling := int(tc.replicas) * len(h.desired.Runners); len(h.livePods()) > ceiling {
						t.Fatalf("%d live pods, want at most %d: a repair never surges", len(h.livePods()), ceiling)
					}
					if n := h.restartsRecorded(); n != int(tc.replicas) {
						t.Fatalf("restarts recorded = %d, want one repair per Instance and no second repair: %v", n, h.events)
					}
				}
				allRepaired := func() bool {
					if !h.settledOn(h.revV1, tc.replicas) {
						return false
					}
					for idx := int32(0); idx < tc.replicas; idx++ {
						if !h.repairedAbove(idx, 1) {
							return false
						}
					}
					return true
				}
				if !h.runWithInvariant(40, allRepaired, withinTheLimit) {
					h.dumpState("after the repairs")
					t.Fatalf("not every Instance returned to serving on v1")
				}
				if h.sawEvent(workloadtypes.EventReasonRepairHeld) {
					t.Fatalf("a repair waiting for the next pass is pacing, not a hold: %v", h.events)
				}
			})
		}
	}
}

// TestReconcile_DarkGang_WedgedLeaderRollsFirst: a gang serves through its
// leader, so a gang whose leader is wedged on the running revision serves
// nothing whatever its worker reports, and a fix pushed over it lands on
// that gang first. Two leader+worker gangs, the leader of the higher index
// crash-looping while its worker stays Ready, the fix pushed under each
// drain-first strategy with one Instance offline at a time, standalone
// and listed in a rollout group with a run open, both crash forms: the
// wedged gang is the first to land the fix, its healthy peer stays in
// rotation until then and rolls after it within the budget, no repair
// opens, and both gangs end on the fix.
func TestReconcile_DarkGang_WedgedLeaderRollsFirst(t *testing.T) {
	const replicas = 2
	for _, sc := range offTargetStrategies[1:] {
		for _, roll := range rollShapes {
			for _, form := range []crashForm{crashAtStart, crashAfterUp} {
				t.Run(fmt.Sprintf("%s/%s/%s", sc.strategy, roll.name, form), func(t *testing.T) {
					h := offTargetCrashHarness(t, true, sc.strategy, sc.surge, sc.unavail, workloadtypes.RestartPolicyNone, replicas)
					consults := 0
					if roll.gated {
						h.gate = rollingGroupGate(h, replicas, sc.surge, sc.unavail, &consults)
					}
					const victim = int32(replicas - 1)
					breakInstance(t, h, victim, true, form)
					h.events = nil
					h.setTarget(h.revFixed, fixedImage)
					requireDarkInstanceRollsFirst(t, h, victim, []int32{0}, nil, sc.strategy, sc.surge, sc.unavail,
						func() bool { return h.settledOn(h.revFixed, replicas) })
				})
			}
		}
	}
}

// TestReconcile_GangWithCrashLoopingWorker_RollsInPlanOrder: the
// preference reads service, not health. A gang whose worker crash-loops
// while its leader stays in rotation serves by the Component's own
// reading of its pod set, so replacing it takes a serving pod offline like
// any other start: it keeps its place in plan order, after the lower
// index, and lands the fix within the budget. Two leader+worker gangs, the
// worker of the higher index crash-looping on the running revision, the
// fix pushed standalone under each drain-first strategy with one Instance
// offline at a time: no repair opens under RestartPolicy None, every pod
// created after the push runs the fix, the roll never takes more than its
// budget offline, the lower index lands first, and both gangs end serving
// on the fix.
func TestReconcile_GangWithCrashLoopingWorker_RollsInPlanOrder(t *testing.T) {
	const replicas = 2
	for _, sc := range offTargetStrategies[1:] {
		t.Run(string(sc.strategy), func(t *testing.T) {
			h := offTargetCrashHarness(t, true, sc.strategy, sc.surge, sc.unavail, workloadtypes.RestartPolicyNone, replicas)
			const broken = int32(replicas - 1)
			h.crashPod(broken, "worker", crashStartImage)
			h.events = nil
			h.setTarget(h.revFixed, fixedImage)
			bumpedAt := h.clk.Now()
			fixedHash := query.RevisionOf(h.revFixed).Hash()
			var firstLanded []int32
			invariant := func() {
				if h.repairInFlight() {
					h.dumpState("repair opened")
					t.Fatalf("a repair opened on an Instance whose revision is not the roll target")
				}
				if n := escalation.CurrentUnavailableInFlight(h.irStatuses()); n > sc.unavail {
					t.Fatalf("%d Instances offline for the roll, budget %d", n, sc.unavail)
				}
				for _, pod := range h.livePods() {
					if !pod.CreationTimestamp.Time.After(bumpedAt) {
						continue
					}
					if pod.Labels[query.LabelRevisionHash] != fixedHash || pod.Spec.Containers[0].Image != fixedImage {
						h.dumpState("pod created off the fix")
						t.Fatalf("pod %s was created on %s after the fix was pushed", pod.Name, pod.Spec.Containers[0].Image)
					}
				}
				if firstLanded == nil {
					if landed := h.landedOn(h.revFixed); len(landed) > 0 {
						firstLanded = landed
					}
				}
			}
			if !h.runWithInvariant(100, func() bool { return h.settledOn(h.revFixed, replicas) }, invariant) {
				h.dumpState("after the bump")
				t.Fatalf("the Component never settled on the fixed revision")
			}
			if len(firstLanded) != 1 || firstLanded[0] != 0 {
				t.Fatalf("first to land the fix = %v, want the lower index: a gang in rotation through its leader keeps its place in plan order", firstLanded)
			}
			if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
				t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
			}
		})
	}
}

// The faults that wedge a running pod so the crash-loop repair opens on
// it once the stuck-pod grace has elapsed, under every restart policy.
var openRepairFaults = []struct {
	name  string
	image string
}{
	{"crash at start", crashStartImage},
	{"image cannot be pulled", badImage},
}

// scaleBesideRepairHarness is four single-pod Instances settled on v1
// under restart policy None, with the kubelet termination model on: a
// deleted pod stays Terminating for longer than a pass, so an operation
// that lands right after a repair opens finds the old pod still there
// and the rebuilt one not yet created, the window a scale in the field
// lands in when the operator acts on a row that reads Restarting.
func scaleBesideRepairHarness(t *testing.T) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	h.atomicStatus = true
	h.recorder = record.NewFakeRecorder(64)
	h.replicas = 4
	none := workloadtypes.RestartPolicyNone
	h.lifecycle.RestartPolicy = &none
	h.podGrace = 3 * recoveryStepFloor
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 4) }) {
		h.dumpState("initial create")
		t.Fatalf("four instances never settled on v1")
	}
	return h
}

// openRepairOn wedges the Instance at idx with image and runs until its
// repair is open: the row reads Restarting and its old pod is deleted,
// held Terminating by the termination model. Returns the Instance's
// incarnation before the repair.
func openRepairOn(t *testing.T, h *recoveryHarness, idx int32, image string) int64 {
	t.Helper()
	before := h.instance(idx).Incarnation
	h.crashPod(idx, "", image)
	h.events = nil
	if !h.run(10, h.repairInFlight) {
		h.dumpState("waiting for the repair")
		t.Fatalf("no repair opened on instance %d within the stuck-pod grace", idx)
	}
	if n := len(h.podsOf(idx)); n != 0 {
		h.dumpState("repair opened")
		t.Fatalf("instance %d still has %d live pod(s) after its repair opened; want the old pod Terminating", idx, n)
	}
	if n := len(podObjectsOf(h, idx)); n != 1 {
		h.dumpState("repair opened")
		t.Fatalf("instance %d has %d pod object(s) after its repair opened; want the old pod held Terminating", idx, n)
	}
	return before
}

// podObjectsOf is every pod object of the Instance at idx, Terminating
// ones included.
func podObjectsOf(h *recoveryHarness, idx int32) []*corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	var out []*corev1.Pod
	for i := range pods.Items {
		if got, ok := query.InstanceIdxFromLabels(&pods.Items[i]); ok && got == idx {
			out = append(out, &pods.Items[i])
		}
	}
	return out
}

// observedIndices is the sorted set of indices any row or any pod object
// of the Component carries.
func observedIndices(h *recoveryHarness) []int32 {
	h.t.Helper()
	seen := map[int32]struct{}{}
	for _, row := range h.irStatuses() {
		seen[row.Index] = struct{}{}
	}
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	for i := range pods.Items {
		if idx, ok := query.InstanceIdxFromLabels(&pods.Items[i]); ok {
			seen[idx] = struct{}{}
		}
	}
	out := make([]int32, 0, len(seen))
	for idx := range seen {
		out = append(out, idx)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func indicesString(indices []int32) string {
	parts := make([]string, 0, len(indices))
	for _, idx := range indices {
		parts = append(parts, fmt.Sprint(idx))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// requireIdentitiesKept fails when any of the Instances changed pods or
// incarnation since before was recorded.
func requireIdentitiesKept(t *testing.T, h *recoveryHarness, before map[int32]string, label string) {
	t.Helper()
	for idx, want := range before {
		if got := h.instanceIdentity(idx); got != want {
			h.dumpState(label)
			t.Fatalf("instance %d should have kept its pods across the %s:\n got %q\nwant %q", idx, label, got, want)
		}
	}
}

// A scale-up that lands while a repair is open adds exactly one Instance.
//
// Four single-pod Instances under restart policy None. The pod of
// Instance 2 is wedged on the running revision; once it has sat in its
// terminal waiting state past the stuck-pod grace the repair opens: the
// row reads Restarting and the old pod is deleted. In that window, the old
// pod still Terminating and the rebuilt one not yet created, the operator
// scales from four to five. The repair keeps index 2 and rebuilds it under
// its own index once the name clears; the create pass adds index 4 and no
// other; the healthy Instances keep their pods; the Component settles with
// five Instances Ready on v1 and a RestartTriggered event names Instance 2.
// No index above 4 exists at any point.
func TestScaleUp_BesideAnOpenRepair_AddsExactlyOneInstance(t *testing.T) {
	const broken = int32(2)
	for _, fault := range openRepairFaults {
		t.Run(fault.name, func(t *testing.T) {
			h := scaleBesideRepairHarness(t)
			incarnationBefore := openRepairOn(t, h, broken, fault.image)
			healthy := map[int32]string{}
			for _, idx := range []int32{0, 1, 3} {
				healthy[idx] = h.instanceIdentity(idx)
			}

			h.replicas = 5
			h.setTarget(h.revV1, goodImage)
			withinFive := func() {
				indices := observedIndices(h)
				if len(indices) > 0 && indices[len(indices)-1] > 4 {
					h.dumpState("scale-up beside the repair")
					t.Fatalf("an index above 4 exists: the scale-up minted a second new Instance beside the repair; indices %s", indicesString(indices))
				}
				if n := len(h.irStatuses()); n > 5 {
					h.dumpState("scale-up beside the repair")
					t.Fatalf("%d rows for a Component of five", n)
				}
			}
			if !h.runWithInvariant(40, func() bool { return h.settledOn(h.revV1, 5) }, withinFive) {
				h.dumpState("scale-up beside the repair")
				t.Fatalf("the component never settled with five instances on v1")
			}

			if got := observedIndices(h); indicesString(got) != "[0 1 2 3 4]" {
				t.Fatalf("indices after the scale-up = %s, want [0 1 2 3 4]: one new Instance beside the rebuilt one", indicesString(got))
			}
			requireIdentitiesKept(t, h, healthy, "scale-up beside the repair")
			if row := h.instance(broken); row.Incarnation != incarnationBefore+1 {
				t.Fatalf("instance %d incarnation = %d, want %d: rebuilt exactly once under its own index", broken, row.Incarnation, incarnationBefore+1)
			}
			if !h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
				t.Fatalf("no RestartTriggered event names the rebuilt Instance: %v", h.events)
			}
		})
	}
}

// A scale-down that lands while a repair is open removes the broken
// Instance first.
//
// The same opening: Instance 2's repair is open, its old pod Terminating.
// The operator scales from four to two. The plan keeps the two lowest
// Instances in rotation, 0 and 1; the broken Instance 2 and the highest
// healthy one, 3, leave, the delete wave taking the broken row over from
// its repair. The survivors keep their pods, nothing is rebuilt, and each
// removed row is gone by the end of the pass that finds its pods gone.
// The Component settles with two Instances Ready on v1.
func TestScaleDown_BesideAnOpenRepair_RemovesTheBrokenInstanceFirst(t *testing.T) {
	const broken = int32(2)
	for _, fault := range openRepairFaults {
		t.Run(fault.name, func(t *testing.T) {
			h := scaleBesideRepairHarness(t)
			openRepairOn(t, h, broken, fault.image)
			kept := map[int32]string{0: h.instanceIdentity(0), 1: h.instanceIdentity(1)}

			h.replicas = 2
			h.setTarget(h.revV1, goodImage)
			podsGoneBeforePass := map[int32]bool{}
			rowsFollowPods := func() {
				for _, idx := range []int32{broken, 3} {
					if podsGoneBeforePass[idx] && h.instance(idx) != nil {
						h.dumpState("scale-down beside the repair")
						t.Fatalf("instance %d has no pod left yet its row survived the pass that found them gone: %+v", idx, h.instance(idx))
					}
					podsGoneBeforePass[idx] = len(podObjectsOf(h, idx)) == 0
				}
				if len(h.irStatuses()) > 4 {
					h.dumpState("scale-down beside the repair")
					t.Fatalf("a scale-down created a row")
				}
			}
			if !h.runWithInvariant(40, func() bool { return h.settledOn(h.revV1, 2) }, rowsFollowPods) {
				h.dumpState("scale-down beside the repair")
				t.Fatalf("the component never settled with two instances on v1")
			}

			for _, idx := range []int32{broken, 3} {
				if h.instance(idx) != nil || len(podObjectsOf(h, idx)) != 0 {
					h.dumpState("scale-down beside the repair")
					t.Fatalf("instance %d should have left: row=%+v pods=%d", idx, h.instance(idx), len(podObjectsOf(h, idx)))
				}
			}
			requireIdentitiesKept(t, h, kept, "scale-down beside the repair")
			if got := observedIndices(h); indicesString(got) != "[0 1]" {
				t.Fatalf("indices after the scale-down = %s, want [0 1]", indicesString(got))
			}
		})
	}
}

// TestReconcile_GangWithParkedWorker_LandsTheFixInsideTheGroupBudget: the
// coordination gate counts a gang as serving only when every member is,
// so a gang whose worker is parked beside a serving leader is already
// inside the gate's unavailability; a drain-first start on that gang
// takes nothing further out of the gate's count and skips the consult,
// while the per-Component budget still charges it, because its leader
// does leave rotation. Two leader+worker gangs listed in a rollout group
// with a run open and a budget of one Instance offline, the worker of the
// higher index crash-looping on the running revision while its leader
// serves, the fix pushed under each drain-first strategy and both restart
// policies. With the worker parked for good, the roll cannot wait for the
// gang to serve again: the healthy gang's start is held by the gate while
// the broken gang is out of the count, the broken gang is the first to
// land the fix with its peer untouched and in rotation until then, the
// peer follows within the budget, no repair opens and both gangs end
// serving on the fix. With a worker that keeps coming back between
// crashes the gate's count changes pass by pass, so only the end state
// and the budget are asserted.
func TestReconcile_GangWithParkedWorker_LandsTheFixInsideTheGroupBudget(t *testing.T) {
	const replicas = 2
	for _, sc := range offTargetStrategies[1:] {
		for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
			for _, form := range []crashForm{crashAtStart, crashAfterUp} {
				t.Run(fmt.Sprintf("%s/%s/%s", sc.strategy, policy, form), func(t *testing.T) {
					h := offTargetCrashHarness(t, true, sc.strategy, sc.surge, sc.unavail, policy, replicas)
					consults, denials := 0, 0
					groupGate := rollingGroupGate(h, replicas, sc.surge, sc.unavail, &consults)
					h.gate = func(strategy workloadtypes.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, workloadtypes.RolloutHoldGate, string) {
						allowed, gate, reason := groupGate(strategy, inFlightSurge, inFlightUnavail)
						if !allowed {
							denials++
						}
						return allowed, gate, reason
					}
					const broken = int32(replicas - 1)
					image := crashStartImage
					if form == crashAfterUp {
						image = crashLoadImage
					}
					h.crashPod(broken, "worker", image)
					h.events = nil
					h.setTarget(h.revFixed, fixedImage)
					done := func() bool { return h.settledOn(h.revFixed, replicas) }
					if form == crashAfterUp {
						requireRollLandsWithinBudget(t, h, sc.strategy, sc.surge, sc.unavail, done)
						return
					}
					requireDarkInstanceRollsFirst(t, h, broken, []int32{0}, nil, sc.strategy, sc.surge, sc.unavail, done)
					if consults == 0 || denials == 0 {
						t.Fatalf("gate consults = %d, denials = %d: the healthy gang's start is still put to the gate, and held while the broken gang is out of the serving count", consults, denials)
					}
				})
			}
		}
	}
}

// requireRollLandsWithinBudget drives the roll on the fixed revision
// until done holds and checks, after every pass, that no repair opens,
// the roll stays inside its budget and every pod created after the bump
// runs the fix.
func requireRollLandsWithinBudget(t *testing.T, h *recoveryHarness, strategy workloadtypes.UpdateStrategyType, surge, unavail int32, done func() bool) {
	t.Helper()
	bumpedAt := h.clk.Now()
	fixedHash := query.RevisionOf(h.revFixed).Hash()
	invariant := func() {
		if h.repairInFlight() {
			h.dumpState("repair opened")
			t.Fatalf("a repair opened on an Instance whose revision is not the roll target")
		}
		rollStaysWithinBudget(t, h, strategy, surge, unavail)
		for _, pod := range h.livePods() {
			if !pod.CreationTimestamp.Time.After(bumpedAt) {
				continue
			}
			if pod.Labels[query.LabelRevisionHash] != fixedHash || pod.Spec.Containers[0].Image != fixedImage {
				h.dumpState("pod created off the fix")
				t.Fatalf("pod %s was created on %s after the fix was pushed", pod.Name, pod.Spec.Containers[0].Image)
			}
		}
	}
	if !h.runWithInvariant(100, done, invariant) {
		h.dumpState("after the bump")
		t.Fatalf("the roll never reached its end state")
	}
	if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
		t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
	}
}

// TestReconcile_GangWithParkedWorker_RepairOpensInsideTheGroupBudget: the
// steady-state form of the same self-count. A gang whose worker is
// parked beside a serving leader is already out of the coordination
// gate's serving count, so its crash-loop repair is not put to the gate:
// consulting it would charge the repair for the outage it ends, and under
// a group budget of one the gang would never be rebuilt. The per-Component
// budget still decides the repair, because the leader does leave
// rotation. Two leader+worker gangs converged with a rollout group in
// front of them, the worker of the higher index crash-looping on the
// running revision, both restart policies, both crash forms, the group's
// gate answering as it does with no run open and with one open: under a
// per-Component budget of one the repair opens, rebuilds the gang Ready
// on its revision while the healthy peer never leaves rotation, and no
// hold is announced; under a budget of zero the repair stays held by the
// budget, named as such, and the gang is never rebuilt. The held case is
// driven with the worker parked for good: a worker that comes back
// between crashes also arms RecreateInstanceOnPodRestart's own
// container-restart trigger, which no layer admits.
func TestReconcile_GangWithParkedWorker_RepairOpensInsideTheGroupBudget(t *testing.T) {
	const replicas = 2
	for _, maxUnavailable := range []int32{1, 0} {
		forms := []crashForm{crashAtStart, crashAfterUp}
		if maxUnavailable == 0 {
			forms = forms[:1]
		}
		for _, gate := range steadyStateGroupGates {
			for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
				for _, form := range forms {
					t.Run(fmt.Sprintf("maxUnavailable=%d/%s/%s/%s", maxUnavailable, gate.name, policy, form), func(t *testing.T) {
						h := offTargetCrashHarness(t, true, workloadtypes.UpdateStrategyRecreatePod, 0, maxUnavailable, policy, replicas)
						consults := 0
						h.gate = gate.build(h, replicas, maxUnavailable, &consults)
						const peer, broken = int32(0), int32(replicas - 1)
						image := crashStartImage
						if form == crashAfterUp {
							image = crashLoadImage
						}
						h.crashPod(broken, "worker", image)
						h.events = nil

						peerStaysInRotation := func() {
							if ceiling := replicas * len(h.desired.Runners); len(h.livePods()) > ceiling {
								t.Fatalf("%d live pods, want at most %d: a repair never surges", len(h.livePods()), ceiling)
							}
							if !h.instanceInRotation(peer) || !h.atIncarnation(peer, 1) {
								h.dumpState("peer out of rotation")
								t.Fatalf("the healthy peer left rotation while the broken gang was repaired")
							}
						}
						repaired := func() bool { return h.settledOn(h.revV1, replicas) && h.repairedAbove(broken, 1) }
						if maxUnavailable == 0 {
							if h.runWithInvariant(40, repaired, peerStaysInRotation) {
								t.Fatalf("the repair opened under a budget of zero: it takes the serving leader offline and must stay budgeted")
							}
							if !h.sawEventWith(workloadtypes.EventReasonRepairHeld, "unavailability budget 0") {
								t.Fatalf("the hold must be announced naming the budget: %v", h.events)
							}
						} else {
							if !h.runWithInvariant(40, repaired, peerStaysInRotation) {
								h.dumpState("after the crash")
								t.Fatalf("the broken gang was never repaired: held by a gate that already counts its outage")
							}
							if !h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
								t.Fatalf("the repair must be recorded on the Instance: %v", h.events)
							}
							if h.sawEvent(workloadtypes.EventReasonRepairHeld) {
								t.Fatalf("a repair was announced held: %v", h.events)
							}
						}
						if consults != 0 {
							t.Fatalf("gate consults = %d, want 0: a repair of an Instance the gate's serving count already excludes is not put to the gate", consults)
						}
					})
				}
			}
		}
	}
}

// sawEventWith reports whether the harness recorded an event of reason
// whose message carries detail.
func (h *recoveryHarness) sawEventWith(reason workloadtypes.EventReason, detail string) bool {
	for _, e := range h.events {
		if strings.Contains(e, string(reason)) && strings.Contains(e, detail) {
			return true
		}
	}
	return false
}

// TestReconcile_GroupedRoll_BreakAfterPush_KeepsHealthyInstancesServing is
// the closed-loop story for a Component bumped in a rolling group whose
// highest Instance breaks on the running revision right after the push,
// once the first Instance's surge is already in flight. The break is the
// only capacity the Component loses: at every pass the healthy Instances
// stay in rotation, so the Instances serving never drop below N-1, and no
// healthy Instance leaves its revision before the broken one lands the
// fix. The surge already in flight finishes its cycle, the broken Instance
// takes the slot it frees and is the next to land, serving is back at N
// from that landing to the end, no repair opens under either restart
// policy, no pod is created on the broken revision, and the surge budget
// holds throughout. The next-to-land order is pinned for a pod that dies
// at every start; a pod that dies after Ready is up between its crashes,
// serves by the Component's own reading of its pod set in those windows,
// and keeps its place in plan order, so only the floor is pinned for it.
// Single-pod and leader+worker shapes, both crash forms, both restart
// policies, standalone and listed in a rolling group.
func TestReconcile_GroupedRoll_BreakAfterPush_KeepsHealthyInstancesServing(t *testing.T) {
	const surge, unavail = int32(1), int32(0)
	for _, gang := range []bool{false, true} {
		shape, replicas := "single", int32(4)
		if gang {
			shape, replicas = "gang", 2
		}
		for _, roll := range rollShapes {
			for _, policy := range []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance} {
				for _, form := range []crashForm{crashAtStart, crashAfterUp} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", shape, roll.name, policy, form), func(t *testing.T) {
						h := offTargetCrashHarness(t, gang, workloadtypes.UpdateStrategySurgeThenDrain, surge, unavail, policy, replicas)
						consults := 0
						if roll.gated {
							h.gate = rollingGroupGate(h, replicas, surge, unavail, &consults)
						}
						h.events = nil
						bumpedAt := h.clk.Now()
						h.setTarget(h.revFixed, fixedImage)
						if !h.run(3, func() bool { return escalation.CurrentSurgeInFlight(h.irStatuses()) == 1 }) {
							h.dumpState("after the push")
							t.Fatalf("the push opened no surge")
						}
						victim := replicas - 1
						breakInstance(t, h, victim, gang, form)
						requireBreakAfterPushKeepsHealthyServing(t, h, victim, replicas, surge, bumpedAt, form == crashAtStart)
					})
				}
			}
		}
	}
}

// requireBreakAfterPushKeepsHealthyServing drives a roll that has one
// surge in flight and the Instance at victim freshly broken on v1 until
// the Component settles on the fix, and checks after every pass that the
// break is the only capacity the roll costs: serving never drops below
// replicas-1 while the broken Instance stands, and never below replicas
// once no pod runs its crash image; no repair opens; the surge budget
// holds; every pod created after bumpedAt runs the fix. With dark set,
// the broken Instance serves nothing at every pass, so the Instances
// neither in flight nor broken must stay untouched on v1 and in rotation
// until it lands: it is the next to land after the surge that was already
// open.
func requireBreakAfterPushKeepsHealthyServing(t *testing.T, h *recoveryHarness, victim, replicas, surge int32, bumpedAt time.Time, dark bool) {
	t.Helper()
	fixedHash := query.RevisionOf(h.revFixed).Hash()
	var healthy []int32
	for idx := int32(1); idx < victim; idx++ {
		healthy = append(healthy, idx)
	}
	brokenPods := func() int {
		n := 0
		for _, pod := range h.livePods() {
			if image := pod.Spec.Containers[0].Image; image == crashStartImage || image == crashLoadImage {
				n++
			}
		}
		return n
	}
	victimLanded := false
	invariant := func() {
		if h.repairInFlight() {
			h.dumpState("repair opened")
			t.Fatalf("a repair opened on an Instance whose revision is not the roll target")
		}
		if n := escalation.CurrentSurgeInFlight(h.irStatuses()); n > surge {
			t.Fatalf("%d surges in flight, budget %d", n, surge)
		}
		for _, pod := range h.livePods() {
			if !pod.CreationTimestamp.Time.After(bumpedAt) {
				continue
			}
			if pod.Labels[query.LabelRevisionHash] != fixedHash || pod.Spec.Containers[0].Image != fixedImage {
				h.dumpState("pod created off the fix")
				t.Fatalf("pod %s was created on %s after the fix was pushed", pod.Name, pod.Spec.Containers[0].Image)
			}
		}
		if !victimLanded && brokenPods() == 0 {
			victimLanded = true
		}
		floor := replicas - 1
		if victimLanded {
			floor = replicas
		}
		if n := h.servingInstances(); n < floor {
			h.dumpState("below the floor")
			t.Fatalf("%d Instances serving, floor %d (broken Instance landed the fix: %v)", n, floor, victimLanded)
		}
		if victimLanded || !dark {
			return
		}
		for _, peer := range healthy {
			if !h.untouchedOn(peer, h.revV1) || !h.instanceServes(peer) {
				h.dumpState("healthy peer taken first")
				t.Fatalf("instance %d left v1 before the broken instance %d landed the fix", peer, victim)
			}
		}
	}
	if !h.runWithInvariant(120, func() bool { return h.settledOn(h.revFixed, replicas) }, invariant) {
		h.dumpState("after the bump")
		t.Fatalf("the Component never settled on the fixed revision")
	}
	if !victimLanded {
		t.Fatalf("the broken Instance never landed the fix")
	}
	if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
		t.Fatalf("a restart was recorded for an Instance the roll owned: %v", h.events)
	}
}

// failedRepairWithoutPods reports the index of a row parked Failed with
// its Restart operation preserved and no live pod behind it, or -1 when no
// row reads that way. Such a row has no owner: the repair reads it as a
// spent attempt whose set is incomplete, the Create pass as a parked
// operation, and the roll as a row already on its revision.
func failedRepairWithoutPods(h *recoveryHarness) int32 {
	for _, s := range h.irStatuses() {
		if parkedRepair(h, s.Index) && len(h.podsOf(s.Index)) == 0 {
			return s.Index
		}
	}
	return -1
}

// A promoted runner that crash-loops after it served, on a pod older than
// the stuck-pod grace: the policy's repair and the stuck-pod escalation
// read that pod in the same reconcile. The repair owns the pod's
// replacement, so no pass leaves the row Failed with its Restart
// operation and no pod behind it, and the Instance serves again, rebuilt
// at a new incarnation, within a bounded number of passes. Two readings
// meet the pod: a single-pod Instance whose crash lands while the roll
// still has Instances to move, so its pod disagrees with the Component's
// current revision, and a gang whose worker crash-loops under a routed
// Service, where the leader's drain takes a further pass.
func TestCrashAfterPromotion_RepairNeverOutlivesItsPodSet(t *testing.T) {
	for _, tc := range []struct {
		name     string
		multiPod bool
		routed   bool
		replicas int32
		// midRoll crashes the first Instance the roll promotes while the
		// others still run the previous revision.
		midRoll bool
		runner  string
	}{
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1, the crash lands mid-roll", replicas: 4, midRoll: true},
		{name: "a leader+worker gang under a routed Service, the worker crash-loops", multiPod: true, routed: true, replicas: 1, runner: workloadtypes.RunnerWorker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarnessFor(t, tc.multiPod, constants.MainContainerName)
			h.recorder = record.NewFakeRecorder(256)
			h.replicas = tc.replicas
			h.routed = tc.routed
			policy := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &policy
			h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
			if !h.run(60, func() bool { return h.settledOn(h.revV1, tc.replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", tc.replicas)
			}
			// The pod set holds Ready well past the grace before it breaks.
			for i := 0; i < 3; i++ {
				h.step()
			}

			broken := int32(0)
			if tc.midRoll {
				h.setTarget(h.revFixed, fixedImage)
				promoted := func() bool {
					for _, s := range h.irStatuses() {
						if s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.RunningRevision == h.revFixed.Name {
							broken = s.Index
							return true
						}
					}
					return false
				}
				if !h.run(20, promoted) || h.currentRevision == h.revFixed.Name {
					h.dumpState("mid-roll")
					t.Fatalf("no Instance was promoted on %s while the roll still had Instances to move", h.revFixed.Name)
				}
				h.step()
			}
			before := h.instance(broken).Incarnation
			h.crashPod(broken, tc.runner, crashStartImage)

			const bound = 12
			servesAgain := func() bool {
				s := h.instance(broken)
				return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil &&
					s.Incarnation > before && servingReady(h, broken, len(h.desired.Runners))
			}
			pass := 0
			h.runWithInvariant(bound, servesAgain, func() {
				pass++
				if idx := failedRepairWithoutPods(h); idx >= 0 {
					h.dumpState("repair outlived its pod set")
					t.Fatalf("pass %d: Instance %d reads Failed with its Restart operation and no pod behind it: %+v (events=%v)",
						pass, idx, *h.instance(idx), h.events)
				}
			})
			if !servesAgain() {
				h.dumpState("not serving again")
				t.Fatalf("Instance %d did not serve again at a new incarnation within %d passes of its crash: %+v (events=%v)",
					broken, bound, *h.instance(broken), h.events)
			}
		})
	}
}

// A post-Ready crash whose rebuild wedges from birth parks the repair
// through the stuck-pod escalation, with the wedged set behind it. The
// park has two exits and needs nothing else:
// the repair re-arms on the ladder once its delay elapses and heals, or,
// with no ladder configured, a corrected revision takes the Instance.
// Neither path passes through a Failed row that holds its Restart
// operation with no pod.
func TestRepairParkedOnItsRebuild_ExitsThroughTheLadderOrTheCorrectedPush(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ladder *workloadtypes.RetryPolicy
		push   bool
	}{
		{name: "the ladder re-arms the repair", ladder: retryLadder(3, time.Minute)},
		{name: "no ladder, a corrected revision takes the Instance", push: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestartHarness(t, 1)
			h.recorder = record.NewFakeRecorder(256)
			h.retryPolicy = tc.ladder
			for i := 0; i < 3; i++ {
				h.step()
			}
			before := h.instance(0).Incarnation

			// The serving pod crash-loops; every pod built from now on parks
			// in the same loop and stays parked after the cause is gone.
			h.armWedge("CrashLoopBackOff", true)
			h.crashPod(0, "", crashStartImage)
			noOrphanedRepair := func() {
				if idx := failedRepairWithoutPods(h); idx >= 0 {
					h.dumpState("repair outlived its pod set")
					t.Fatalf("Instance %d reads Failed with its Restart operation and no pod behind it: %+v (events=%v)", idx, *h.instance(idx), h.events)
				}
			}
			if !h.runWithInvariant(20, func() bool { return parkedRepair(h, 0) }, noOrphanedRepair) {
				h.dumpState("park")
				t.Fatalf("the wedged rebuild never parked the repair at Failed (events=%v)", h.events)
			}
			parked := h.instance(0)
			if parked.Incarnation <= before {
				t.Fatalf("the park holds incarnation %d, want the rebuild's, above %d", parked.Incarnation, before)
			}
			if got := len(h.podsOf(0)); got != 1 {
				t.Fatalf("the parked repair has %d pods behind it, want its wedged rebuild", got)
			}

			h.clearWedge()
			if tc.push {
				h.setTarget(h.revFixed, fixedImage)
			}
			if !h.runWithInvariant(40, func() bool { return servingReady(h, 0, 1) }, noOrphanedRepair) {
				h.dumpState("exit")
				t.Fatalf("the parked repair never left Failed (events=%v)", h.events)
			}
			got := h.instance(0)
			if tc.push && got.RunningRevision != h.revFixed.Name {
				t.Errorf("running revision = %q, want the corrected %q", got.RunningRevision, h.revFixed.Name)
			}
			if !tc.push && got.Incarnation != parked.Incarnation+1 {
				t.Errorf("incarnation = %d, want %d: one rebuild on the ladder", got.Incarnation, parked.Incarnation+1)
			}
		})
	}
}

// podsBelowIncarnation reports whether a live pod of the Instance carries
// an incarnation below incarnation: the set a repair drains and deletes.
func podsBelowIncarnation(h *recoveryHarness, idx int32, incarnation int64) bool {
	for _, pod := range h.podsOf(idx) {
		if inc, ok := query.InstanceIncarnationFromLabels(pod); ok && inc < incarnation {
			return true
		}
	}
	return false
}

// A gang's repair drains its surviving leader over a further pass under a
// routed Service, and the attempt's deadline elapses in the pass the
// drain's delete lands. The repair owns the set it is deleting: no pass
// leaves the row Failed with its Restart operation and no pod behind it,
// and the Instance serves again, rebuilt at a new incarnation, within a
// bounded number of passes.
func TestDeadlineElapsesAsTheDrainDeletes_RepairKeepsItsPodSet(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.recorder = record.NewFakeRecorder(256)
	h.routed = true
	policy := workloadtypes.RestartPolicyRecreateInstance
	// The deadline ends inside the pass that drains the leader, so it is
	// read as elapsed in the pass the drain's delete lands.
	h.lifecycle = workloadtypes.Lifecycle{
		RestartPolicy:        &policy,
		InstanceReadyTimeout: &metav1.Duration{Duration: recoveryStepFloor / 2},
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.settledOn(h.revV1, 1) }) {
		h.dumpState("initial create")
		t.Fatalf("the gang never settled on v1")
	}
	for i := 0; i < 3; i++ {
		h.step()
	}
	before := h.instance(0).Incarnation

	// The worker is lost; the leader still serves and is drained first.
	h.loseRunner(0, "worker")

	const bound = 12
	servesAgain := func() bool {
		s := h.instance(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil &&
			s.Incarnation > before && servingReady(h, 0, 2)
	}
	deadlineElapsedDuringDrain := false
	passes := -1
	h.runWithInvariant(bound, servesAgain, func() {
		passes++
		if s := h.instance(0); s != nil && s.Phase == workloadtypes.InstancePhaseRestarting && s.Operation != nil &&
			h.clk.Now().After(s.Operation.Deadline.Time) && podsBelowIncarnation(h, 0, s.Incarnation) {
			deadlineElapsedDuringDrain = true
		}
		if idx := failedRepairWithoutPods(h); idx >= 0 {
			h.dumpState("repair outlived its pod set")
			t.Fatalf("after pass %d: Instance %d reads Failed with its Restart operation and no pod behind it: %+v (events=%v)",
				passes, idx, *h.instance(idx), h.events)
		}
	})
	if !deadlineElapsedDuringDrain {
		h.dumpState("no race")
		t.Fatalf("the deadline never elapsed while the repair still held the set it drains, so the story missed the race (events=%v)", h.events)
	}
	if !servesAgain() {
		h.dumpState("not serving again")
		t.Fatalf("Instance 0 did not serve again at a new incarnation within %d passes of losing its worker: %+v (events=%v)",
			bound, *h.instance(0), h.events)
	}
}

// A repair whose rebuilt pod never comes up is ended by the operation
// deadline with that pod behind it: the park keeps the Restart operation
// and its pod set, and the ladder re-arms the repair once its delay has
// elapsed, which heals the Instance once the fault is gone.
func TestDeadlineElapsesOnAWedgedRebuild_RepairParksWithItsPodSet(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.recorder = record.NewFakeRecorder(256)
	// Only the deadline ends the attempt: the fast escalation is off.
	h.stuckGrace = 0
	h.retryPolicy = retryLadder(3, time.Minute)
	policy := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle = workloadtypes.Lifecycle{
		RestartPolicy:        &policy,
		InstanceReadyTimeout: &metav1.Duration{Duration: 2 * time.Minute},
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.settledOn(h.revV1, 1) }) {
		h.dumpState("initial create")
		t.Fatalf("the Instance never settled on v1")
	}
	before := h.instance(0).Incarnation

	// The pod is deleted out from under the Instance; every pod built from
	// now on parks in a crash loop and stays parked after the cause is gone.
	h.armWedge("CrashLoopBackOff", true)
	h.losePods(0)
	noOrphanedRepair := func() {
		if idx := failedRepairWithoutPods(h); idx >= 0 {
			h.dumpState("repair outlived its pod set")
			t.Fatalf("Instance %d reads Failed with its Restart operation and no pod behind it: %+v (events=%v)", idx, *h.instance(idx), h.events)
		}
	}
	if !h.runWithInvariant(20, func() bool { return parkedRepair(h, 0) }, noOrphanedRepair) {
		h.dumpState("park")
		t.Fatalf("the deadline never parked the repair at Failed (events=%v)", h.events)
	}
	parked := h.instance(0)
	if parked.LastFailure == nil || parked.LastFailure.Reason != escalation.DeadlineExceededReason {
		t.Fatalf("the park records %+v, want the elapsed deadline", parked.LastFailure)
	}
	if parked.Incarnation <= before {
		t.Fatalf("the park holds incarnation %d, want the rebuild's, above %d", parked.Incarnation, before)
	}
	if got := len(h.podsOf(0)); got != 1 || !h.atIncarnation(0, parked.Incarnation) {
		t.Fatalf("the parked repair has %d pods behind it, want its wedged rebuild at incarnation %d", got, parked.Incarnation)
	}

	h.clearWedge()
	if !h.runWithInvariant(40, func() bool { return servingReady(h, 0, 1) }, noOrphanedRepair) {
		h.dumpState("exit")
		t.Fatalf("the parked repair never left Failed (events=%v)", h.events)
	}
	if got := h.instance(0); got.Incarnation != parked.Incarnation+1 {
		t.Errorf("incarnation = %d, want %d: one rebuild on the ladder", got.Incarnation, parked.Incarnation+1)
	}
}
