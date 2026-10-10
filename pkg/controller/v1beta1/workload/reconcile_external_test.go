package workload_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Reconcile is a thin dispatch layer over the workload/ops state
// machines. The tests below assert the orchestration shape — nil-deps
// guards, scale-down-then-create ordering, and the early-return
// requeue on partial scale-down — by driving Reconcile against a real
// fake client and the real workload/ops bodies.
//
// Per-op coverage (Update gate ordering, Restart trigger detection,
// Migrate surge lifecycle) lives in workload/ops/*_test.go; those tests
// drive the per-op functions directly and need no Reconcile wrapper.

// makeScheme builds the runtime.Scheme the fake client needs.
func makeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	return scheme
}

// stubInputCallbacks fills the callback fields that
// workload.ReconcileInput panics on if left nil. Tests only set
// behavior on the callbacks they care about; the rest stay no-ops.
func stubInputCallbacks(input *workloadtypes.ReconcileInput) {
	if input.MutateInstance == nil {
		input.MutateInstance = func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
			return nil
		}
	}
	if input.RemoveInstance == nil {
		input.RemoveInstance = func(_ context.Context, _ int32) (bool, error) { return false, nil }
	}
	if input.WriteAggregateCondition == nil {
		input.WriteAggregateCondition = func(_ context.Context, _ metav1.Condition) error { return nil }
	}
	if input.WarnInstanceFailed == nil {
		input.WarnInstanceFailed = func(_ int32, _, _ string) {}
	}
	if input.MutateMigration == nil {
		input.MutateMigration = func(_ context.Context, _ string, _ func(*workloadtypes.MigrationRecord) bool) error {
			return nil
		}
	}
}

type testAtomicMutationStore struct {
	owner    client.Object
	statuses []workloadtypes.InstanceStatus
	writes   int
}

func installTestAtomicMutationStore(input *workloadtypes.ReconcileInput) *testAtomicMutationStore {
	store := &testAtomicMutationStore{
		owner:    input.OwnerObject,
		statuses: cloneTestInstanceStatuses(input.ObservedState.InstanceStatuses),
	}
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	return store
}

func (s *testAtomicMutationStore) apply(_ context.Context, mutations []workloadtypes.InstanceMutation, _ string, mutateRetryBlock func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
	if mutateRetryBlock != nil {
		return fmt.Errorf("test atomic mutation store does not support RetryBlock mutations")
	}
	snapshot := workloadtypes.InstanceMutationSnapshot{Instances: make(map[int32]workloadtypes.InstanceStatus, len(s.statuses))}
	if s.owner != nil {
		snapshot.OwnerUID = s.owner.GetUID()
		snapshot.OwnerGeneration = s.owner.GetGeneration()
	}
	for _, status := range s.statuses {
		snapshot.Instances[status.Index] = cloneTestInstanceStatus(status)
	}
	for _, mutation := range mutations {
		if mutation.BatchPrecondition != nil && !mutation.BatchPrecondition(snapshot) {
			return workloadtypes.ErrStatusMutationPrecondition
		}
	}

	type committedMutation struct {
		callback func(*workloadtypes.InstanceStatus, *workloadtypes.InstanceStatus)
		before   *workloadtypes.InstanceStatus
		after    *workloadtypes.InstanceStatus
	}
	next := cloneTestInstanceStatuses(s.statuses)
	committed := make([]committedMutation, 0, len(mutations))
	for _, mutation := range mutations {
		position := -1
		for i := range next {
			if next[i].Index == mutation.Index {
				position = i
				break
			}
		}
		if mutation.Remove {
			if position < 0 {
				continue
			}
			before := cloneTestInstanceStatus(next[position])
			if mutation.Precondition != nil && !mutation.Precondition(&before) {
				continue
			}
			next = append(next[:position], next[position+1:]...)
			committed = append(committed, committedMutation{callback: mutation.OnCommit, before: &before})
			continue
		}

		status := workloadtypes.InstanceStatus{Index: mutation.Index}
		var before *workloadtypes.InstanceStatus
		if position >= 0 {
			status = cloneTestInstanceStatus(next[position])
			copy := cloneTestInstanceStatus(status)
			before = &copy
		}
		if mutation.Precondition != nil && !mutation.Precondition(&status) {
			continue
		}
		if !mutation.Mutate(&status) {
			continue
		}
		if position >= 0 {
			next[position] = cloneTestInstanceStatus(status)
		} else {
			next = append(next, cloneTestInstanceStatus(status))
		}
		after := cloneTestInstanceStatus(status)
		committed = append(committed, committedMutation{callback: mutation.OnCommit, before: before, after: &after})
	}
	if len(committed) == 0 {
		return nil
	}
	s.statuses = next
	s.writes++
	for _, mutation := range committed {
		if mutation.callback != nil {
			mutation.callback(mutation.before, mutation.after)
		}
	}
	return nil
}

func (s *testAtomicMutationStore) sync(input *workloadtypes.ReconcileInput) {
	input.ObservedState.InstanceStatuses = cloneTestInstanceStatuses(s.statuses)
}

func (s *testAtomicMutationStore) status(index int32) *workloadtypes.InstanceStatus {
	for i := range s.statuses {
		if s.statuses[i].Index == index {
			status := cloneTestInstanceStatus(s.statuses[i])
			return &status
		}
	}
	return nil
}

func cloneTestInstanceStatuses(statuses []workloadtypes.InstanceStatus) []workloadtypes.InstanceStatus {
	cloned := make([]workloadtypes.InstanceStatus, len(statuses))
	for i := range statuses {
		cloned[i] = cloneTestInstanceStatus(statuses[i])
	}
	return cloned
}

func cloneTestInstanceStatus(status workloadtypes.InstanceStatus) workloadtypes.InstanceStatus {
	converted := v1beta1convert.InstanceStatusFromWorkload(status)
	return v1beta1convert.InstanceStatusToWorkload(*converted.DeepCopy())
}

// minimalInput builds a ReconcileInput with the bare minimum fields
// the dispatcher reads. Tests pad observed state + plan as needed.
const testScaleDownRequeueInterval = 37 * time.Second

// testRequeueIntervals is the dispatcher cadence these tests run under,
// standing in for the operator config a deployed chart supplies. A test
// pinning the unconfigured path clears ReconcileInput.Requeue.
var testRequeueIntervals = workloadtypes.RequeueIntervals{
	Operation: 5 * time.Second,
	Gate:      3 * time.Second,
}

// recoveryStepFloor is the least the recovery harness advances its clock
// per pass.
const recoveryStepFloor = 30 * time.Second

func minimalInput(t *testing.T) workloadtypes.ReconcileInput {
	t.Helper()
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b", Namespace: "prod", UID: "uid-1",
	}}
	in := workloadtypes.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: workloadtypes.Key{
			Namespace: "prod",
			Component: workloadtypes.ComponentEngine,
			OwnerName: "llama-70b",
		},
		ScaleDownRequeueInterval: testScaleDownRequeueInterval,
		Requeue:                  testRequeueIntervals,
		DesiredSpec: workloadtypes.WorkloadDesiredSpec{
			Replicas: 1,
			PodSpec: &corev1.PodSpec{Containers: []corev1.Container{
				{Name: "main", Image: "test:v1"},
			}},
		},
	}
	stubInputCallbacks(&in)
	return in
}

// minimalPlan returns a ComponentPlan covering a single Instance at
// index 0 with a single-pod "default" Runner.
func minimalPlan() workloadtypes.ComponentPlan {
	return workloadtypes.ComponentPlan{
		Component: workloadtypes.ComponentEngine,
		Replicas:  1,
		Instances: []workloadtypes.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{
				{Name: "default", Size: 1},
			}},
		},
	}
}

func roundTripMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component workloadtypes.ComponentType) func(context.Context, int32, func(*workloadtypes.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-" + string(component)}
		create := false
		if err := c.Get(ctx, key, ir); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("get IR: %w", err)
			}
			ir = &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
			create = true
		}
		pos := -1
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		slot := v1beta1.OMENativeInstanceStatus{Index: idx}
		if pos != -1 {
			slot = ir.Status.InstanceStatuses[pos]
		}
		w := v1beta1convert.InstanceStatusToWorkload(slot)
		if !mutate(&w) {
			return nil
		}
		updated := v1beta1convert.InstanceStatusFromWorkload(w)
		if pos == -1 {
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, updated)
		} else {
			ir.Status.InstanceStatuses[pos] = updated
		}
		if create {
			bare := &v1beta1.InferenceReplica{ObjectMeta: ir.ObjectMeta}
			if err := c.Create(ctx, bare); err != nil {
				return fmt.Errorf("create IR: %w", err)
			}
			bare.Status = ir.Status
			ir = bare
		}
		return c.Status().Update(ctx, ir)
	}
}

// instanceStatusByIndex looks up an InstanceStatus on the component's InferenceReplica.
func instanceStatusByIndex(c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) *v1beta1.OMENativeInstanceStatus {
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-" + string(component)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		return nil
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == idx {
			return &ir.Status.InstanceStatuses[i]
		}
	}
	return nil
}

// TestReconcile_NilClient_Errors asserts the nil-deps.Client guard
// fires up front. The dispatcher MUST refuse to run without a client
// so the per-op state machines never NPE on a missing reader.
func TestReconcile_NilClient_Errors(t *testing.T) {
	in := minimalInput(t)
	_, err := workload.Reconcile(context.Background(), workloadtypes.Deps{}, in, minimalPlan(), nil)
	if err == nil {
		t.Fatalf("expected nil-Client to error, got nil")
	}
}

// TestReconcile_NilTarget_DrivesCreate covers the cold-start path:
// no observed instances, no target ControllerRevision, plan asks for
// one Instance. Reconcile MUST fall through to the Create pass without
// scale-down / restart / migration work; Create's first action is to
// allocate the Instance, which the fake client accepts.
//
// We pass target=nil because Create's signature allows it (MinReplicas=0
// would render a nil target; here we exercise the cold-create path
// where the renderer hasn't materialized a revision yet).
func TestReconcile_NilTarget_DrivesCreate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c}

	in := minimalInput(t)
	plan := minimalPlan()

	// Cold-start path: target nil short-circuits the Update pass.
	// Create's nil-target branch returns done=true without rendering.
	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_UnconfiguredCadenceUsesBackoff pins the unset path at
// the dispatcher: a pass that still has Instances coming up and no
// operator cadence asks for the controller's rate-limited backoff
// rather than an interval the binary invented, while the configured
// cadence produces an explicit wake-up.
func TestReconcile_UnconfiguredCadenceUsesBackoff(t *testing.T) {
	scheme := makeScheme(t)
	plan := minimalPlan()
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-v1", Namespace: "prod"},
	}

	configured := fake.NewClientBuilder().WithScheme(scheme).Build()
	res, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: configured}, minimalInput(t), plan, target)
	if err != nil {
		t.Fatalf("Reconcile with a configured cadence: %v", err)
	}
	if res.RequeueAfter != testRequeueIntervals.Operation {
		t.Fatalf("configured cadence: got RequeueAfter %v want %v", res.RequeueAfter, testRequeueIntervals.Operation)
	}

	unconfigured := fake.NewClientBuilder().WithScheme(scheme).Build()
	in := minimalInput(t)
	in.Requeue = workloadtypes.RequeueIntervals{}
	res, err = workload.Reconcile(context.Background(), workloadtypes.Deps{Client: unconfigured}, in, plan, target)
	if err != nil {
		t.Fatalf("Reconcile with no configured cadence: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("unconfigured cadence: got RequeueAfter %v want 0", res.RequeueAfter)
	}
	if !res.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
		t.Fatalf("a converging pass with no cadence must ride the rate-limited backoff; got %+v", res)
	}
}

// TestReconcile_HeldWorkWakesThePass pins the held-work sink at the
// dispatcher: work an op pass held on operator configuration — which no
// watch event announces — leaves a wake-up on the pass result even when
// every other pass is steady and asks for none.
func TestReconcile_HeldWorkWakesThePass(t *testing.T) {
	scheme := makeScheme(t)
	plan := minimalPlan()

	for _, tc := range []struct {
		name      string
		cadence   time.Duration
		wantAfter time.Duration
	}{
		{name: "configured cadence", cadence: 5 * time.Second, wantAfter: 5 * time.Second},
		{name: "unconfigured cadence rides the backoff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			in := minimalInput(t)
			in.Requeue = workloadtypes.RequeueIntervals{}
			// Nothing for the passes to do: the held work is the only
			// reason to come back.
			in.DesiredSpec.Replicas = 0
			wake := &workloadtypes.PassWake{}
			wake.Observe(tc.cadence)
			in.PassWake = wake

			res, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: c}, in, plan, nil)
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if res.RequeueAfter != tc.wantAfter {
				t.Fatalf("RequeueAfter: got %v want %v", res.RequeueAfter, tc.wantAfter)
			}
			if tc.wantAfter == 0 && !res.Requeue { //nolint:staticcheck // the bare backoff is what this asserts
				t.Fatalf("held work with no cadence must ride the rate-limited backoff; got %+v", res)
			}
		})
	}
}

// TestReconcile_PausedSkipsCreate proves InferenceReplica.spec.paused is a
// real circuit breaker: a missing Instance must not be allocated while the
// plan is paused.
func TestReconcile_PausedSkipsCreate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c}

	in := minimalInput(t)
	allocateCalls := 0
	in.MutateInstance = func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
		allocateCalls++
		return nil
	}
	plan := minimalPlan()
	plan.Paused = true

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("paused reconcile must stay quiescent, got %+v", result)
	}
	if allocateCalls != 0 {
		t.Fatalf("paused reconcile allocated %d Instances, want 0", allocateCalls)
	}
}

// TestReconcile_PausedStillScalesDown keeps the safety boundary explicit:
// pausing lifecycle churn must not prevent a deliberate replica reduction
// from deleting an extra Instance.
func TestReconcile_PausedStillScalesDown(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	store := installTestAtomicMutationStore(&in)
	plan := minimalPlan()
	plan.Paused = true

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("paused scale-down admission must requeue immediately, got %+v", result)
	}
	status := store.status(1)
	if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting || status.Operation == nil || status.Operation.Type != workloadtypes.InstanceOperationDelete {
		t.Fatalf("paused scale-down did not durably admit index 1: %+v", status)
	}
	if retained := store.status(0); retained == nil || retained.Phase != workloadtypes.InstancePhaseReady {
		t.Fatalf("paused scale-down changed retained index 0: %+v", retained)
	}
}

// TestReconcile_ScaleDownExtra_DrivesDelete covers an InstanceStatus whose
// index is outside the plan. Fresh Delete admission is committed before any
// Pod effect and immediately requeues for a new observation.
func TestReconcile_ScaleDownExtra_DrivesDelete(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Phase: workloadtypes.InstancePhaseReady}, // extra
	}
	plan := minimalPlan() // covers only index 0
	store := installTestAtomicMutationStore(&in)

	if extras := workload.ScaleDownExtras(in.ObservedState.InstanceStatuses, plan); len(extras) != 1 || extras[0] != 1 {
		t.Fatalf("ScaleDownExtras: got %v, want [1]", extras)
	}
	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || store.writes != 1 {
		t.Fatalf("scale-down admission result/writes = %+v/%d, want immediate requeue/1", result, store.writes)
	}
}

// TestReconcile_ScaleDownRetiresTheReadyRowOnAHeldRevisionFirst pins the
// victim of a scale-down under a held push: with one Instance Ready on
// the pushed revision, whose ladder holds, and the other Ready on the
// running revision, the plan keeps the running revision's row and the
// scale-down admits the pushed one, whatever its index. Keeping the
// pushed row would retire the only sound Instance and land the push by
// attrition.
func TestReconcile_ScaleDownRetiresTheReadyRowOnAHeldRevisionFirst(t *testing.T) {
	const running, pushed = "llama-70b-engine-aaaaaaaa", "llama-70b-engine-bbbbbbbb"
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c}

	in := minimalInput(t)
	in.DesiredSpec.Replicas = 1
	in.ObservedState.CurrentRevision = running
	in.ObservedState.UpdateRevision = pushed
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{{TargetRevision: pushed, State: workloadtypes.RetryBlockHeld, AttemptsStarted: 3}}
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady, RunningRevision: pushed, PodCount: 1, ServingPodCount: 1},
		{Index: 1, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady, RunningRevision: running, PodCount: 1, ServingPodCount: 1},
	}
	store := installTestAtomicMutationStore(&in)
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, in.DesiredSpec, in.ObservedState)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if extras := workload.ScaleDownExtras(in.ObservedState.InstanceStatuses, plan); len(extras) != 1 || extras[0] != 0 {
		t.Fatalf("the scale-down must retire the Ready row on the held revision, selected %v", extras)
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("scale-down admission must requeue, got %+v", result)
	}
	if status := store.status(0); status == nil || status.Phase != workloadtypes.InstancePhaseDeleting {
		t.Fatalf("the Instance on the held revision must be the one admitted to delete, got %+v", status)
	}
	if status := store.status(1); status == nil || status.Phase != workloadtypes.InstancePhaseReady || status.RunningRevision != running {
		t.Fatalf("the Instance on the running revision must be kept, got %+v", status)
	}
}

// TestReconcile_MigrationInFlight_NotScaleDown asserts that an
// Instance with Phase=Migrating is NOT scale-down-deleted, even when
// the plan doesn't cover its index — otherwise Migrate's surge
// would be ripped out by the dispatcher's first pass.
func TestReconcile_MigrationInFlight_NotScaleDown(t *testing.T) {
	observed := []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},
		{Index: 7, Phase: workloadtypes.InstancePhaseMigrating},
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ScaleDownExtras(observed, plan)
	for _, idx := range extras {
		if idx == 7 {
			t.Errorf("index 7 (Phase=Migrating) must not be in scale-down extras, got %v", extras)
		}
	}
}

// TestReconcile_MigrationOperationOwned_NotScaleDown is the dual to
// the above: an Instance whose Operation.Type=Migrate (the surge side)
// MUST be excluded even when Phase=Creating (surge mid-spin-up).
func TestReconcile_MigrationOperationOwned_NotScaleDown(t *testing.T) {
	observed := []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},
		{
			Index: 3,
			Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{
				Type: workloadtypes.InstanceOperationMigrate,
			},
		},
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ScaleDownExtras(observed, plan)
	for _, idx := range extras {
		if idx == 3 {
			t.Errorf("index 3 (Operation.Migrate) must not be in scale-down extras, got %v", extras)
		}
	}
}

// fixedMigrationTime anchors the work-selection ordering assertions.
func fixedMigrationTime() time.Time {
	return time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
}

// enginePod fabricates a single-pod "default" engine pod at ordinal 0
// with the labels Render stamps (managed-by + instance-idx + ordinal +
// component + isvc), so query selectors and instance-index filters
// recognize it.
func enginePod(isvc, ns string, idx int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc, workloadtypes.ComponentEngine, idx, "default", 0),
			Namespace: ns,
			UID:       types.UID(fmt.Sprintf("%s-engine-%d-uid", isvc, idx)),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           string(workloadtypes.ComponentEngine),
				query.LabelInstanceIdx:                fmt.Sprintf("%d", idx),
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     "default",
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelPodOrdinal:                 "0",
				query.LabelRevisionHash:               "priorrev",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
	}
}

// listCountingReader wraps a client.Reader and counts PodList List calls.
// Used to prove the dispatcher's restart pass lists pods ONCE per
// reconcile (via the live APIReader) instead of once per Instance.
type listCountingReader struct {
	client.Reader
	podListCalls int
}

func (r *listCountingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		r.podListCalls++
	}
	return r.Reader.List(ctx, list, opts...)
}

// failedEnginePod is enginePod with Phase=Failed — a restart trigger.
func failedEnginePod(isvc, ns string, idx int32) *corev1.Pod {
	pod := enginePod(isvc, ns, idx)
	pod.Status.Phase = corev1.PodFailed
	return pod
}

type restartScaleTestFixture struct {
	isvc   *v1beta1.InferenceService
	client client.Client
	input  workloadtypes.ReconcileInput
	plan   workloadtypes.ComponentPlan
	target *appsv1.ControllerRevision
}

func podNameSet(t *testing.T, c client.Client, namespace string) map[string]bool {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	names := make(map[string]bool, len(pods.Items))
	for i := range pods.Items {
		names[pods.Items[i].Name] = true
	}
	return names
}

func TestReconcile_PausedCompletesTheOpenRecreate(t *testing.T) {
	name := "paused-recreate"
	targetName := name + "-engine-target"
	f := newRestartScaleTestFixture(t, name, 1, true, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workloadtypes.InstancePhaseUpdating,
		RunningRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "update-0", Type: workloadtypes.InstanceOperationUpdate, Step: "Drain",
			TargetRevision: targetName,
		},
	}, []workloadtypes.RunnerPlan{{Name: "default", Size: 1}})
	f.plan.RestartPolicy = ""
	f.plan.UpdateStrategy = workloadtypes.UpdateStrategy{Type: workloadtypes.UpdateStrategyRecreatePod}

	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	want := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 0, "default", 0)
	if !names[want] {
		t.Errorf("paused reconcile left the recreate half-done; pods = %v", names)
	}
}

// TestReconcile_PausedCompletesTheCommittedCreate: a create that already
// committed finishes the set it started materializing, while an index
// that never opened one stays empty until the pause is cleared. Holding
// a half-built set would leave the Instance with nothing serving.
func TestReconcile_PausedCompletesTheCommittedCreate(t *testing.T) {
	name := "paused-create"
	targetName := name + "-engine-target"
	member := enginePod(name, "prod", 0)
	f := newRestartScaleTestFixture(t, name, 2, true, workloadtypes.InstanceStatus{
		Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating, PodCount: 1,
		RunningRevision: targetName,
		Operation: &workloadtypes.InstanceOperation{
			ID: "create-0", Type: workloadtypes.InstanceOperationCreate, Step: "CreatePods",
			TargetRevision: targetName,
		},
	}, []workloadtypes.RunnerPlan{{Name: "default", Size: 2}}, member)
	// The gang-member-loss repair owns a partial set under the recreate
	// policy; this test is about the Create pass finishing its own.
	f.plan.RestartPolicy = ""

	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	missing := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 0, "default", 1)
	if !names[missing] {
		t.Errorf("paused reconcile left the committed gang half-built; pods = %v", names)
	}
	absent := query.PodName(f.isvc.Name, workloadtypes.ComponentEngine, 1, "default", 0)
	if names[absent] {
		t.Errorf("paused reconcile materialized an index that never committed a create; pods = %v", names)
	}
	if s := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 1); s != nil {
		t.Errorf("paused reconcile allocated status for an uncommitted index: %+v", s)
	}
}

// TestReconcile_PausedLeavesASupersededCreateAlone: a revision edit
// arriving mid-pause does not reach a half-built set. Finishing it is
// not what the Create pass would do — it retires the attempt and opens a
// fresh one at the new revision, with a new identity, a new deadline and
// re-rendered pods — and that is the new operation a pause withholds.
// The retirement runs on the unpause.
func TestReconcile_PausedLeavesASupersededCreateAlone(t *testing.T) {
	name := "paused-superseded-create"
	pinned := name + "-engine-pinned"
	member := enginePod(name, "prod", 0)
	build := func(t *testing.T, paused bool) *restartScaleTestFixture {
		// RunningRevision empty: a first materialization, which is the
		// shape whose pods the attempt owns and can hand to a successor.
		f := newRestartScaleTestFixture(t, name, 1, paused, workloadtypes.InstanceStatus{
			Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating, PodCount: 1,
			Operation: &workloadtypes.InstanceOperation{
				ID: "create-0", Type: workloadtypes.InstanceOperationCreate, Step: "CreatePods",
				TargetRevision: pinned,
			},
		}, []workloadtypes.RunnerPlan{{Name: "default", Size: 2}}, member)
		f.plan.RestartPolicy = ""
		// The edit: the pass now converges to a revision the in-flight
		// attempt was not opened for.
		f.target = &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
			Name: name + "-engine-edited", Namespace: f.isvc.Namespace,
		}}
		return f
	}

	f := build(t, true)
	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile paused: %v", err)
	}
	got := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if got == nil || got.Operation == nil || got.Operation.ID != "create-0" ||
		got.Operation.TargetRevision != pinned {
		t.Errorf("paused reconcile replaced the in-flight attempt: %+v", got.Operation)
	}
	if names := podNameSet(t, f.client, f.isvc.Namespace); len(names) != 1 {
		t.Errorf("paused reconcile built pods for the edited revision; pods = %v", names)
	}

	f = build(t, false)
	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
		Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
	}, f.input, f.plan, f.target); err != nil {
		t.Fatalf("Reconcile unpaused: %v", err)
	}
	got = instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
	if got == nil || got.Operation == nil || got.Operation.ID == "create-0" {
		t.Errorf("unpaused reconcile did not retire the superseded attempt: %+v", got.Operation)
	}
}

// TestExecute_PausedRecordsAndReleasesTheHold: a paused pass reports the
// hold on the attempts the pause is actually holding, so the wait is
// visible on the operation and the deadline-parking step can stop its
// clock. The attempts a pause does not hold report nothing: scale-down
// runs while paused, a committed create finishes the set it started, and
// repair keeps running unless the pause is frozen. A row with no attempt
// has nothing to report. Clearing the pause releases the token.
func TestReconcile_PausedFinishesACreateItWouldNotRetire(t *testing.T) {
	name := "paused-lost-member"
	pinned := name + "-engine-pinned"
	missing := query.PodName(name, workloadtypes.ComponentEngine, 0, "default", 1)

	build := func(t *testing.T, runningRevision string) *restartScaleTestFixture {
		f := newRestartScaleTestFixture(t, name, 1, true, workloadtypes.InstanceStatus{
			Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating, PodCount: 1,
			RunningRevision: runningRevision,
			Operation: &workloadtypes.InstanceOperation{
				ID: "create-0", Type: workloadtypes.InstanceOperationCreate, Step: "CreatePods",
				TargetRevision: pinned,
			},
		}, []workloadtypes.RunnerPlan{{Name: "default", Size: 2}}, enginePod(name, "prod", 0))
		// The gang-member-loss repair owns a partial set under the
		// recreate policy; this is about the Create pass finishing its own.
		f.plan.RestartPolicy = ""
		if runningRevision == "" {
			// The attempt renders the revision it pins, so that revision's
			// stored template is what the set is finished from.
			stored := revisionWithPodSpec(t, pinned, f.isvc.Namespace, f.input.DesiredSpec.PodSpec)
			if err := f.client.Create(context.Background(), stored); err != nil {
				t.Fatalf("seed the pinned revision: %v", err)
			}
		}
		return f
	}

	t.Run("a serving Instance rebuilding a lost member", func(t *testing.T) {
		// RunningRevision set: the Instance was promoted once, so the
		// retirement rule leaves its attempt in place whatever the target
		// says.
		f := build(t, pinned)
		f.target = &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
			Name: name + "-engine-edited", Namespace: f.isvc.Namespace,
		}}
		recorder := record.NewFakeRecorder(8)

		if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
			Client: f.client, APIReader: f.client, Recorder: recorder,
			Expectations: workloadtypes.NewExpectations(),
		}, f.input, f.plan, f.target); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		if names := podNameSet(t, f.client, f.isvc.Namespace); !names[missing] {
			t.Errorf("paused reconcile left the Instance short of its set; pods = %v", names)
		}
		got := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 0)
		if got == nil || got.Operation == nil || got.Operation.ID != "create-0" ||
			got.Operation.TargetRevision != pinned {
			t.Errorf("paused reconcile replaced the attempt: %+v", got.Operation)
		}
		select {
		case ev := <-recorder.Events:
			t.Errorf("paused reconcile announced %q; finishing a set is not an event", ev)
		default:
		}
	})

	t.Run("no target to retarget to", func(t *testing.T) {
		// With no target the attempt cannot be retired, so the set is
		// finished at the revision the attempt pins.
		f := build(t, "")
		if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
			Client: f.client, APIReader: f.client, Expectations: workloadtypes.NewExpectations(),
		}, f.input, f.plan, nil); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if names := podNameSet(t, f.client, f.isvc.Namespace); !names[missing] {
			t.Errorf("paused reconcile with no target left the set half-built; pods = %v", names)
		}
		pod := &corev1.Pod{}
		if err := f.client.Get(context.Background(), client.ObjectKey{Namespace: f.isvc.Namespace, Name: missing}, pod); err != nil {
			t.Fatalf("get the finished member: %v", err)
		}
		if got := pod.Labels[query.LabelRevisionHash]; got != query.RevisionHashFromControllerRevisionName(pinned) {
			t.Errorf("the finished member carries revision %q, want the attempt's pin %s", got, pinned)
		}
	})
}

// TestPause_UnbackedReadyInstanceDemotesToPending: the pod of a converged
// Instance is deleted while the Component is paused. No pass may recreate it,
// and the row must fall back to Pending rather than keep claiming Ready.
func TestPause_UnbackedReadyInstanceDemotesToPending(t *testing.T) {
	h := newRecoveryHarness(t, false)
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged")
	}

	h.desired.Paused = true
	h.losePods(0)
	h.step()

	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhasePending || s.Operation != nil {
		h.dumpState("paused with no pods")
		t.Fatalf("demotion: got %+v, want Phase=Pending with no operation", s)
	}
	h.step()
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("paused pass recreated %d pod(s)", len(pods))
	}
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhasePending {
		t.Fatalf("demotion did not hold across passes: %+v", s)
	}

	h.desired.Paused = false
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("after unpause")
		t.Fatalf("Instance never recovered after unpause")
	}
}

// pausedUpdate is a single-pod Update attempt already carrying the
// operator's pause token — an attempt held at a step boundary, with its
// deadline parked by the adapter.
func pausedUpdate(waiting string) []workloadtypes.InstanceStatus {
	return []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseUpdating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:    workloadtypes.InstanceOperationUpdate,
			Step:    workloadtypes.UpdateStepSurge,
			Waiting: waiting,
		},
	}}
}

// pausedRecreate is the recreate roll's Phase B row under the same
// pause — the shape the gang arm judges, since a surge SOURCE is never
// read against its own group.
func pausedRecreate(waiting string) []workloadtypes.InstanceStatus {
	rows := pausedUpdate(waiting)
	rows[0].Operation.Step = "Drain"
	return rows
}

// runHoldEvidence drives the hold pass on its own, the way Execute runs
// it for a paused Component: the authorities report, and no repair
// follows.
func runHoldEvidence(t *testing.T, input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan, byIdx map[int32][]*corev1.Pod) error {
	t.Helper()
	return workload.HoldPassForTest(context.Background(), workloadtypes.Deps{}, input, plan,
		workload.SnapshotWithPodsForTest(input, byIdx))
}

// TestHoldEvidence_PausedRowRecordsTheSchedulerHold: a pod the scheduler
// cannot place while the Component is paused still gets its hold
// recorded. The scheduler reports a FACT about the Instance and the
// pause reports a decision, so the fact takes the row; the operator sees
// which constraint has no placement instead of a pause token that hides
// it.
func TestHoldEvidence_PausedRowRecordsTheSchedulerHold(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(pausedUpdate(workloadtypes.WaitingReasonPaused))

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workloadtypes.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q (the fact takes the paused row)", got.Operation, workloadtypes.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
		t.Errorf("LastFailure: got %+v want the scheduler message %q", got.LastFailure, schedulerMessage)
	}
	if got.Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating (the row is queued, not failed)", got.Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestHoldEvidence_PausedRowTakesNoRepairAction: the same row with the
// operator's placement grace long elapsed. Unpaused that is a terminal,
// environment-caused failure; paused it is only a hold, because the half
// that decides repair does not run.
func TestHoldEvidence_PausedRowTakesNoRepairAction(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(pausedUpdate(workloadtypes.WaitingReasonPaused))
	input.UnschedulableGrace = time.Minute

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase == workloadtypes.InstancePhaseFailed {
		t.Errorf("Phase: got Failed want Updating (a pause withholds the escalation)")
	}
	if got.Operation == nil || got.Operation.Waiting != workloadtypes.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workloadtypes.WaitingReasonUnschedulable)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock writes: got %v want none", rec.blocks)
	}
}

// TestHoldEvidence_PausedRowReleasesTheSchedulerHold: the release runs
// under a pause too. A hold that outlives the placement that ended it
// would keep naming a wait that is over, and the row would report it
// until the operator unpaused.
func TestHoldEvidence_PausedRowReleasesTheSchedulerHold(t *testing.T) {
	input, rec := escalationFixture(pausedUpdate(workloadtypes.WaitingReasonUnschedulable))

	placed := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodPending,
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
		},
	}
	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {placed}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want the scheduler token cleared", got.Operation)
	}
	if got.Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating", got.Phase)
	}
}

// TestHoldEvidence_PausedRowRecordsTheGangHold: the gang arm runs under
// a pause on the same terms. A deterministic name still being collected
// is a fact about the Instance, so it takes the paused row and parks the
// clock on the wait that is real.
func TestHoldEvidence_PausedRowRecordsTheGangHold(t *testing.T) {
	input, rec := escalationFixture(pausedRecreate(workloadtypes.WaitingReasonPaused))
	input.Gangs = gangObservations(0, workloadtypes.GangStateTerminating, gangTerminatingMessage)

	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1), nil); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workloadtypes.WaitingReasonPodGroupTerminating {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, workloadtypes.WaitingReasonPodGroupTerminating)
	}
	if got.Phase != workloadtypes.InstancePhaseUpdating {
		t.Errorf("Phase: got %q want Updating", got.Phase)
	}
}

// TestHoldEvidence_PausedRowKeepsTheIncumbentFact: precedence between
// two facts is unchanged by the pause. A row already reporting that its
// source is out of rotation keeps that token — first writer wins between
// authorities that both state something about the Instance, and only the
// pause is ordered below them.
func TestHoldEvidence_PausedRowKeepsTheIncumbentFact(t *testing.T) {
	now := time.Now()
	rows := pausedUpdate(workloadtypes.WaitingReasonSourceUnrouted)
	rows[0].Operation.TargetRevision = "own-engine-targethash"
	input, rec := escalationFixture(rows)

	// The source is out of rotation and the replacement has no
	// placement: two facts, and the one that got there first keeps the
	// row.
	source := servingPod("engine-0-default-0")
	source.Status.Conditions[1].Status = corev1.ConditionFalse
	if err := runHoldEvidence(t, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {source, unschedulablePod("engine-0-default-1", now.Add(-5*time.Minute))}}); err != nil {
		t.Fatalf("hold evidence pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != workloadtypes.WaitingReasonSourceUnrouted {
		t.Errorf("Operation.Waiting: got %+v want %q kept", got.Operation, workloadtypes.WaitingReasonSourceUnrouted)
	}
}

// Corrective-edit recovery: after a rollout toward a broken revision
// exhausts its retries (Held), a corrective spec edit — roll-forward or
// roll-back — must reconcile to convergence without operator action.
//
// These tests drive the REAL exhaustion flow — bad target revision →
// stuck pods → escalation → disposition/abandon → Backoff → retry →
// Held — through workload.Reconcile against a fake client with a
// simulated kubelet and a fake clock, then apply a corrective edit and
// assert the loop converges with zero manual intervention. The acceptance
// matrix covers:
//
//   - direction: roll-forward (corrective NEW revision) and roll-back
//     (target returns to the source's exact RunningRevision — zero
//     revision distance, so the update trigger correctly never fires
//     and recovery must arrive via Plan's wreckage scan);
//   - topology: single-pod (surge-ordinal wreckage) and gang
//     (GangSurgeTarget marker + replacement-gang wreckage);
//   - timing: the settled at-Held state and the dirty mid-wreckage
//     state (corrective edit lands while the source is
//     Failed-with-Operation and the crashed attempt's pods are live).
//
// A partial initial Create retarget exercises the same recovery loop before
// any serving source exists.
//
// The serving source must stay in rotation throughout every recovery:
// cleanup only removes superseded-revision debris, never the healthy
// pod (asserted per step via runWithInvariant).

const (
	recoveryNS    = "prod"
	recoveryOwner = "llama-70b"

	goodImage  = "registry.test/serving:v1"
	badImage   = "registry.test/serving:broken"
	fixedImage = "registry.test/serving:v2"
	// stuckImage runs but never passes readiness: no terminal waiting
	// reason, so nothing escalates it before the operation deadline.
	stuckImage = "registry.test/serving:never-ready"
	// crashStartImage dies at every start: the kubelet parks it in
	// CrashLoopBackOff and it is never Ready again. crashLoadImage dies
	// once it is up: it alternates between serving and CrashLoopBackOff,
	// the shape of a pod that keeps crashing under load. Both are what a
	// live pod is pointed at to induce the failure; the template keeps
	// its healthy image, so a rebuild or an in-place patch heals it.
	crashStartImage = "registry.test/serving:crash-start"
	crashLoadImage  = "registry.test/serving:crash-load"
	// crashAfterReadyImage reports ContainersReady once and then crashes
	// on every later pass the way crashStartImage does.
	crashAfterReadyImage = "registry.test/serving:crash-after-ready"
	// crashAfterServingImage serves (PodReady with the serving gate) for
	// two passes, long enough for the handoff to promote it, and then
	// crashes on every later pass the way crashStartImage does.
	crashAfterServingImage = "registry.test/serving:crash-after-serving"
	// flapAfterServingImage serves the same way and then alternates
	// between a crash and a run that is Ready again, the shape of a
	// promoted pod that keeps dying under load and coming back between
	// the kubelet's restarts.
	flapAfterServingImage = "registry.test/serving:flap-after-serving"
	// crashOnceAfterServingImage serves the same way, dies once, and
	// stays up after the kubelet's one restart, as does a rebuild of its
	// Instance: the shape of a promoted pod whose process was killed once.
	crashOnceAfterServingImage = "registry.test/serving:crash-once-after-serving"
	// correctedImage is a sound image a story publishes after a crashing
	// revision has been held: a revision born after that revision's
	// failures, as an operator's corrected push is.
	correctedImage = "registry.test/serving:corrected"

	// terminationFinalizer is the harness's stand-in for a kubelet that
	// has not finished terminating a deleted pod: the fake client keeps a
	// finalized object as Terminating until the finalizer is removed.
	terminationFinalizer = "test.workload.ome.io/terminating"
)

// recoveryHarness is the closed reconcile loop: fake client + simulated
// kubelet + fake clock + in-memory RetryBlock store + IR-backed
// InstanceStatus round-trip.
type recoveryHarness struct {
	t    *testing.T
	ctx  context.Context
	c    client.Client
	isvc *v1beta1.InferenceService
	clk  *clocktesting.FakeClock

	multiPod  bool
	replicas  int32
	lifecycle workloadtypes.Lifecycle
	desired   workloadtypes.WorkloadDesiredSpec
	target    *appsv1.ControllerRevision

	// mutateErr rejects every InstanceStatus write for the listed
	// indices — the API server refusing that Instance's status update
	// on every pass — while other indices round-trip normally.
	mutateErr map[int32]error

	blocks []workloadtypes.RetryBlock
	// requeue is the dispatcher cadence the harness runs under; a test
	// pinning the unconfigured path zeroes it.
	requeue         workloadtypes.RequeueIntervals
	currentRevision string
	heldWarnings    []string
	// failedWarnings records every InstanceFailed warning the passes
	// raised, as "instance=<idx> pod=<name> <reason>".
	failedWarnings []string

	revV1    *appsv1.ControllerRevision
	revBad   *appsv1.ControllerRevision
	revFixed *appsv1.ControllerRevision

	// podGrace enables the kubelet termination model: a deleted pod stays
	// Terminating until the grace its delete names has elapsed on the fake
	// clock, and podGrace is the grace a delete that names none is owed
	// (the pod's own terminationGracePeriodSeconds). Zero removes pods on
	// delete, as the bare fake client does.
	podGrace time.Duration
	// abandonGrace is the input's AbandonedReplacementGrace.
	abandonGrace time.Duration
	// stuckGrace is the input's StuckPodGrace.
	stuckGrace time.Duration
	// stepFloor is the least the clock advances per step: a pass that asks
	// for no later wake-up still moves the clock this far.
	// recoveryStepFloor unless a story needs a finer clock.
	stepFloor time.Duration
	// stepCap bounds how far one step moves the clock when the pass asks
	// for a longer wait: the kubelet model runs once per step, so a story
	// whose pods must keep serving, dying and restarting while the
	// controller waits out a window caps the step and takes more of
	// them. Zero leaves the step at the wait the pass asked for.
	stepCap time.Duration
	// kubeletLag is how far the clock moves between the kubelet's
	// reports and the pass that reads them: a kubelet dates a container
	// state before any pass observes it, so a record the pass dates
	// itself is later than the kubelet's. Zero reads both at one instant.
	kubeletLag time.Duration
	// bareRequeues counts the consecutive passes that asked for the
	// rate-limited backoff, which runOnWakeUps doubles per ask as the
	// controller's per-item rate limiter does.
	bareRequeues int
	// terminatingUntil is, per Terminating pod, the fake-clock instant the
	// kubelet model finishes its termination.
	terminatingUntil map[string]time.Time

	// container names the single container the template renders. The
	// restart trigger reads restart evidence off the runner container by
	// the name production renders it under, so a harness driving that
	// evidence renders that name.
	container string
	// recorder, when set, receives the pass's events; stepResult drains
	// them into events after every pass so the buffer never fills.
	recorder *record.FakeRecorder
	events   []string
	// gate, when set, is the coordination UpdateGate the passes consult
	// before a fresh start opens.
	gate func(strategy workloadtypes.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, workloadtypes.RolloutHoldGate, string)
	// planGate, when set, is the adapter's plan precondition the passes ask
	// before any fresh start, the starts exempt from gate included.
	planGate func() (bool, workloadtypes.RolloutHoldGate, string)
	// gangs, when set, is the PodGroup reading the passes see, as the
	// PodGroup pass would have recorded it ahead of the reconcile.
	gangs *workloadtypes.GangObservations
	// repairBatchSize is the input's RepairBatchSize: how many crash-loop
	// repairs a pass may open. Nil leaves the pass unbounded.
	repairBatchSize *int32
	// crashes counts, per pod identity (wedgeKey), the kubelet steps a
	// crash image has been observed on it; lastCrash is the instant of its
	// latest crash and lastRunStart when the run that crashed had started,
	// the two instants the kubelet reports on the container's last
	// termination. A rebuilt pod of the same name starts from zero.
	crashes      map[string]int
	lastCrash    map[string]time.Time
	lastRunStart map[string]metav1.Time
	// servedPasses counts, per pod identity, the kubelet passes it ended
	// PodReady; crashAfterServingImage and flapAfterServingImage crash once
	// it has served for crashAfterServed.
	servedPasses map[string]int
	// retryPolicy is the input's UpdateRetryPolicy; nil models an operator
	// config with no updateRetry block.
	retryPolicy *workloadtypes.RetryPolicy

	// wedge is the fault the kubelet model applies to every pod created at
	// or after wedgeSince: the pod parks in the named terminal waiting
	// reason instead of starting. Clearing it lets those pods start on the
	// next kubelet pass, the way a kubelet retry succeeds once a missing
	// ConfigMap key or image tag is back. With wedgeSticky the pods parked
	// while the wedge was armed stay parked after it clears — the cause is
	// gone but the pod cannot recover in place — so only a rebuilt pod
	// comes up; wedgedPods remembers them.
	wedge       string
	wedgeSince  time.Time
	wedgeSticky bool
	wedgedPods  map[string]string
	// wedgeRunner confines the armed wedge to the pods of one runner — a
	// gang whose leader alone reads the missing ConfigMap key — and empty
	// applies it to every pod.
	wedgeRunner string
	// evicted names, by wedgeKey, the pods the kubelet evicted: each is
	// reported phase Failed with its container terminated and stays so,
	// holding its name, until a pass deletes the object.
	evicted map[string]bool

	// admissionDown is the apiserver unable to reach its pod admission
	// webhook: every pod create fails closed with the dispatcher's own
	// error and nothing is created.
	admissionDown bool
	// shedCreates is how many pod creates the apiserver answers with a 429
	// before it takes writes again; each refused create counts one.
	shedCreates int
	// statusWrites counts the InstanceStatus writes the passes committed
	// through the one-row seam; a test zeroes it to measure a window.
	statusWrites int

	// badImageHeals makes the kubelet model treat the bad image as
	// pullable from now on: the registry has the tag again.
	badImageHeals bool

	// forceDelete is the input's ForceDeletePolicy; nil models an operator
	// config with no forceDelete block.
	forceDelete *workloadtypes.ForceDeletePolicy

	// nodeNames, when set, turns on the node model: the kubelet model
	// binds every new pod to the least-loaded node whose kubelet is alive.
	// deadNodes names the nodes whose kubelet has stopped — nothing on
	// them is updated or finishes terminating until the node recovers.
	// cordonedNodes names the nodes marked unschedulable: their pods keep
	// running, and no new pod is bound to them.
	nodeNames     []string
	deadNodes     map[string]bool
	cordonedNodes map[string]bool

	// readinessFails names, by UID, the pods whose readiness probe fails
	// from now on: the kubelet model keeps their container running and
	// takes the pod out of rotation. The fault lives in that container, so
	// a pod rebuilt under the same name, or a container restarted in place
	// by an image patch, starts sound.
	readinessFails map[types.UID]bool
	// readinessGate, when set, is a second readiness gate the template
	// declares beside the serving gate, as a load-balancer controller's
	// would be. The kubelet model writes it True on every pod created
	// before gateHeldSince and on none created at or after it, so a pod
	// rebuilt under the hold folds no Ready however healthy it is;
	// gateHeldRunner confines the hold to one runner's pods (empty: all).
	readinessGate  corev1.PodConditionType
	gateHeldSince  time.Time
	gateHeldRunner string
	// atomicStatus wires the owner-aware atomic status adapter the
	// scale-down wave requires; off, the passes take the one-row write
	// path.
	atomicStatus bool
	// kubeletGone names the pods whose node stopped reporting: the kubelet
	// model leaves their status exactly as it last wrote it, and the node
	// lifecycle controller model sets their Ready condition False.
	kubeletGone map[string]bool
	// imageNameOnNode maps a spec image to the name the node's runtime
	// reports for it: the node holds that image under a second reference
	// (a retag, a mirror alias) and names the one it stored first.
	imageNameOnNode map[string]string
	// startedImage is, per pod, the spec image its running container was
	// started from: the kubelet restarts the container when the spec names
	// another image, whatever name the runtime reports for either.
	startedImage map[string]string
	// crashOnceInComponent confines crashOnceAfterServingImage to one
	// crash in the Component: every other pod, and a pod the controller
	// builds after that crash, runs like a sound one.
	crashOnceInComponent bool
	// crashEverySet has every pod set built on crashOnceAfterServingImage
	// die once, rebuilt sets included, where the image otherwise dies
	// once per Instance.
	crashEverySet bool
	// crashedInstances counts, per Instance, the pods that crashed under
	// crashOnceAfterServingImage.
	crashedInstances map[int32]int
	// crashExitCode is the exit code the kubelet model reports for a
	// crashed runner; zero reads as a clean exit (Completed) the way a
	// runner that stops itself reports, any other value as an Error.
	crashExitCode *int32
	// killOnce names the pods whose runner the kubelet model kills once
	// on its next pass: the container restarts at once with its restart
	// count raised and the termination dated now, and runs on.
	killOnce map[string]bool
	// staysDown names the pods whose flapAfterServingImage runner stops
	// coming back: the kubelet model reports it crashed on every later
	// pass, the shape of a runner that dies and stays down.
	staysDown map[string]bool
	// crashAfterServed is how many passes a crashAfterServingImage pod
	// serves before it dies: two, unless a story needs the roll to reach
	// the next Instance before the promoted one crashes.
	crashAfterServed int
	// onceCrashed marks the Instances whose crashOnceAfterServingImage pod
	// has died its once; a rebuild of such an Instance stays up.
	onceCrashed map[int32]bool
	// minReadySeconds is the Component's minReadySeconds: how long a pod
	// must hold Ready before it is Available. Zero, as the default
	// configuration leaves it, makes Available the same as Ready.
	minReadySeconds int32
	// crashLeaderOnly confines a crash image to the leader of a gang: the
	// workers run it as a sound image, the shape of a runner fault that
	// lives in the leader's process alone.
	crashLeaderOnly bool
	// holds records every rollout hold the update pass reported, in
	// order; a pass that reported none appends nothing.
	holds []workloadtypes.RolloutHold
	// holdVerdicts counts the passes whose update pass reported a verdict,
	// a hold or none; a pass that reported none appends nothing to holds.
	holdVerdicts int
	// verdict is the most recent verdict the update pass reported: the
	// hold, or nil when it reported none.
	verdict *workloadtypes.RolloutHold

	// component is the Component the loop drives. Every Component runs
	// the same loop, so a story names one only to run under its pod
	// names, labels and InferenceReplica.
	component workloadtypes.ComponentType
	// unschedulableGrace is the input's UnschedulableGrace.
	unschedulableGrace time.Duration
	// placementHeld is a cluster with no room: the scheduler model leaves
	// every pod it has not yet placed Pending with
	// PodScheduled=False/Unschedulable carrying placementMessage, and
	// places them on the pass after the hold lifts.
	placementHeld    bool
	placementMessage string
	// unplaceable names the pods the scheduler model refuses while every
	// other pod places: no node has room for that pod set. A pod already
	// placed runs on; a pod created under the name is refused.
	unplaceable map[string]bool
	// exitsCleanOnStop names the pods whose process exits 0 on SIGTERM:
	// the kubelet model reports them Succeeded while they terminate and
	// removes them on the pass after, as a kubelet publishes a pod it
	// stopped.
	exitsCleanOnStop map[string]bool

	// routed turns on the endpoint-controller model: the per-revision
	// routed Services and the headless Service carry EndpointSlices that
	// mirror the pods, so rotation and availability read as on a cluster.
	routed bool
	// migrations is the owner's status.migrations: the records the passes
	// select work from and write back through the MutateMigration seam.
	migrations []workloadtypes.MigrationRecord
	// migrationAudit is the input's migration capacity policy.
	migrationAudit *workloadtypes.MigrationAuditPolicy

	// cacheLagOnce makes the next pass's first cached pod List answer with
	// the pods the previous pass opened on: the informer-backed cache
	// catching up only after the pass took its memoized opening read, so a
	// pass that re-reads live acts on fresher pods than the end-of-pass
	// bookkeeping judges. One pass, then off.
	cacheLagOnce bool
	// priorOpening is the pod list the previous pass opened on.
	priorOpening []corev1.Pod
}

func recoveryPodSpec(image string) *corev1.PodSpec {
	return &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
}

// podSpec is the harness template for image, rendered under the
// harness's container name.
func (h *recoveryHarness) podSpec(image string) *corev1.PodSpec {
	spec := &corev1.PodSpec{Containers: []corev1.Container{{Name: h.container, Image: image}}}
	if h.readinessGate != "" {
		spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: h.readinessGate}}
	}
	return spec
}

// newRecoveryHarness builds the harness plus the three ControllerRevisions
// (v1 good, bad, fixed) minted through the real revision machinery so
// names, hashes, and payloads match production exactly.
func newRecoveryHarness(t *testing.T, multiPod bool) *recoveryHarness {
	t.Helper()
	return newRecoveryHarnessFor(t, multiPod, "main")
}

// newRecoveryHarnessFor is newRecoveryHarness with the template's
// container name chosen by the test.
func newRecoveryHarnessFor(t *testing.T, multiPod bool, container string) *recoveryHarness {
	t.Helper()
	return newComponentRecoveryHarness(t, multiPod, container, workloadtypes.ComponentEngine)
}

// newComponentRecoveryHarness is newRecoveryHarnessFor driving the named
// Component.
func newComponentRecoveryHarness(t *testing.T, multiPod bool, container string, component workloadtypes.ComponentType) *recoveryHarness {
	t.Helper()
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: recoveryOwner, Namespace: recoveryNS, UID: "uid-1",
	}}
	h := &recoveryHarness{
		t:                t,
		ctx:              context.Background(),
		isvc:             isvc,
		clk:              clocktesting.NewFakeClock(time.Now()),
		multiPod:         multiPod,
		replicas:         1,
		requeue:          testRequeueIntervals,
		stuckGrace:       30 * time.Second,
		stepFloor:        recoveryStepFloor,
		terminatingUntil: map[string]time.Time{},
		container:        container,
		component:        component,
		crashes:          map[string]int{},
		onceCrashed:      map[int32]bool{},
		lastCrash:        map[string]time.Time{},
		lastRunStart:     map[string]metav1.Time{},
		servedPasses:     map[string]int{},
		retryPolicy:      &workloadtypes.RetryPolicy{MaxAttempts: 2, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2},
		readinessFails:   map[types.UID]bool{},
		kubeletGone:      map[string]bool{},
		startedImage:     map[string]string{},
		crashAfterServed: 2,
	}
	h.c = fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: h.interceptCreate, Delete: h.interceptDelete,
			Get: h.interceptGet, List: h.interceptList, Update: h.interceptUpdate,
		}).
		Build()
	h.revV1 = h.ensureRevision(h.podSpec(goodImage))
	h.revBad = h.ensureRevision(h.podSpec(badImage))
	h.revFixed = h.ensureRevision(h.podSpec(fixedImage))
	h.setTarget(h.revV1, goodImage)
	h.currentRevision = ""
	return h
}

func (h *recoveryHarness) revisionKey() revision.Key {
	return revision.Key{
		Namespace: recoveryNS,
		Name:      recoveryOwner + "-" + string(h.component),
	}
}

func (h *recoveryHarness) ensureRevision(spec *corev1.PodSpec) *appsv1.ControllerRevision {
	h.t.Helper()
	var workerSpec *corev1.PodSpec
	if h.multiPod {
		workerSpec = spec.DeepCopy()
	}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		h.ctx, h.c, h.c, h.isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		h.revisionKey(), spec, workerSpec, nil, nil, h.isvc.UID,
	)
	if err != nil {
		h.t.Fatalf("EnsureControllerRevision: %v", err)
	}
	// The API server stamps a revision's creation; the fake client does
	// not, so a revision published by a story is born on the fake clock.
	if cr.CreationTimestamp.IsZero() {
		cr.CreationTimestamp = metav1.NewTime(h.clk.Now())
		if err := h.c.Update(h.ctx, cr); err != nil {
			h.t.Fatalf("stamp ControllerRevision creation: %v", err)
		}
	}
	return cr
}

// pushRevision publishes a revision on image at the current clock and
// points the loop at it: the harness analogue of an operator's push.
func (h *recoveryHarness) pushRevision(image string) *appsv1.ControllerRevision {
	h.t.Helper()
	rev := h.ensureRevision(h.podSpec(image))
	h.setTarget(rev, image)
	return rev
}

// setTarget points the loop at (revision, image) — the harness analogue
// of the ISVC/runtime edit that re-renders the desired pod spec and
// republishes the target ControllerRevision.
func (h *recoveryHarness) setTarget(cr *appsv1.ControllerRevision, image string) {
	h.target = cr
	spec := h.podSpec(image)
	h.desired = workloadtypes.WorkloadDesiredSpec{
		Replicas:        h.replicas,
		MinReadySeconds: h.minReadySeconds,
		PodSpec:         spec,
		Lifecycle:       h.lifecycle,
	}
	if h.multiPod {
		h.desired.MultiPod = true
		h.desired.WorkerPodSpec = spec.DeepCopy()
		h.desired.Runners = []workloadtypes.Runner{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	} else {
		h.desired.Runners = []workloadtypes.Runner{{Name: "default", Size: 1}}
	}
}

// kubelet simulates node-side convergence for every live pod: the bad
// image parks in ImagePullBackOff forever; every other image becomes
// ContainersReady, and PodReady once the controller's serving gate is
// True (kubelet ANDs readiness gates into PodReady).
func (h *recoveryHarness) kubelet() {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("kubelet list: %v", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if h.deadNodes[pod.Spec.NodeName] {
			// The kubelet under this pod is stopped: nothing it reports
			// changes, and a pod deleted on it never finishes terminating.
			continue
		}
		if h.kubeletGone[pod.Name] {
			h.nodeControllerMarksNotReady(pod)
			continue
		}
		if pod.DeletionTimestamp != nil {
			if h.exitsCleanOnStop[pod.Name] && pod.Status.Phase != corev1.PodSucceeded {
				h.reportStoppedClean(pod)
				continue
			}
			h.finishTermination(pod)
			continue
		}
		if h.evicted[wedgeKey(pod)] {
			h.reportEvicted(pod)
			continue
		}
		unbound := len(h.nodeNames) > 0 && pod.Spec.NodeName == "" && !h.placementHeld
		if pod.CreationTimestamp.IsZero() || unbound {
			if pod.CreationTimestamp.IsZero() {
				// The stuck-pod grace runs from the pod's creation until it first
				// serves, against the fake clock; stamp the creation deterministically.
				pod.CreationTimestamp = metav1.NewTime(h.clk.Now())
			}
			if unbound {
				pod.Spec.NodeName = h.leastLoadedLiveNode(pod, pods.Items)
			}
			if err := h.c.Update(h.ctx, pod); err != nil {
				h.t.Fatalf("kubelet stamp creation %s: %v", pod.Name, err)
			}
		}
		if h.unplaced(pod) {
			if h.placementHeld || h.unplaceable[pod.Name] {
				h.reportUnschedulable(pod)
				continue
			}
			if _, _, refused := workloadtypes.PodUnschedulable(pod); refused {
				// The room is back: the scheduler binds the pod it refused.
				setPodScheduled(pod, corev1.ConditionTrue, "", "", h.clk.Now())
			}
		}
		image := pod.Spec.Containers[0].Image
		faulty := h.faultyImage(pod)
		wedged := h.wedgeReasonFor(pod)
		switch {
		case wedged != "":
			pod.Status.Phase = corev1.PodPending
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  pod.Spec.Containers[0].Name,
				Image: image,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  wedged,
					Message: "container cannot start: " + wedged,
				}},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
		case image == stuckImage:
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  pod.Spec.Containers[0].Name,
				Image: image,
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
		case image == badImage && !h.badImageHeals:
			pod.Status.Phase = corev1.PodPending
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  pod.Spec.Containers[0].Name,
				Image: image,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  "ImagePullBackOff",
					Message: "Back-off pulling image " + badImage,
				}},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
		case h.killOnce[pod.Name] || (faulty == crashOnceAfterServingImage && h.crashes[wedgeKey(pod)] == 0 && (h.crashEverySet || !h.onceCrashed[instanceIndexOf(pod)]) && h.servedPasses[wedgeKey(pod)] >= h.crashAfterServed && h.promotedOn(pod) && !h.componentCrashedBefore()):
			// The container died once, after its Instance was promoted on it:
			// the kubelet starts it again at once, as it does before a first
			// restart, and the new run has not passed readiness yet. The
			// termination names when the run that died had started, as the
			// kubelet reports it.
			delete(h.killOnce, pod.Name)
			h.crashes[wedgeKey(pod)]++
			h.onceCrashed[instanceIndexOf(pod)] = true
			if idx, ok := query.InstanceIdxFromLabels(pod); ok {
				if h.crashedInstances == nil {
					h.crashedInstances = map[int32]int{}
				}
				h.crashedInstances[idx]++
			}
			h.lastCrash[wedgeKey(pod)] = h.clk.Now()
			h.lastRunStart[wedgeKey(pod)] = priorRunStart(pod)
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:                 pod.Spec.Containers[0].Name,
				Image:                image,
				Ready:                false,
				RestartCount:         1,
				State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(h.clk.Now())}},
				LastTerminationState: corev1.ContainerState{Terminated: h.crashTermination(pod)},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
		case faulty == crashStartImage || h.crashesAfterRunning(pod) || (faulty == crashLoadImage && h.crashes[wedgeKey(pod)]%2 == 0):
			// The container died again: a run that ended after the pod
			// became Ready, and a kubelet back-off before the next start.
			// The termination names when the run that died had started,
			// as the kubelet reports it.
			h.crashes[wedgeKey(pod)]++
			h.lastCrash[wedgeKey(pod)] = h.clk.Now()
			h.lastRunStart[wedgeKey(pod)] = priorRunStart(pod)
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:         pod.Spec.Containers[0].Name,
				Image:        image,
				RestartCount: int32(h.crashes[wedgeKey(pod)]),
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  "CrashLoopBackOff",
					Message: "back-off restarting failed container",
				}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   1,
					Reason:     "Error",
					StartedAt:  h.lastRunStart[wedgeKey(pod)],
					FinishedAt: metav1.NewTime(h.clk.Now()),
				}},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
		case (faulty == crashLoadImage || faulty == flapAfterServingImage) && !h.lastCrash[wedgeKey(pod)].IsZero():
			// Back up between crashes: serving again, with the previous
			// run's termination still on the container status. Before its
			// first crash the pod is a healthy pod with no restart on its
			// record, as the kubelet reports one.
			h.crashes[wedgeKey(pod)]++
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:         pod.Spec.Containers[0].Name,
				Image:        image,
				Ready:        true,
				RestartCount: int32(h.crashes[wedgeKey(pod)] / 2),
				State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(h.clk.Now())}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   1,
					Reason:     "Error",
					StartedAt:  h.lastRunStart[wedgeKey(pod)],
					FinishedAt: metav1.NewTime(h.lastCrash[wedgeKey(pod)]),
				}},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, h.foldReady(pod), h.clk.Now())
		case h.imageChangedInPlace(pod):
			// The kubelet replaces a running container whose spec image
			// changed: the old process is killed, the restart count rises,
			// and the new container runs under the same pod UID without
			// having passed its readiness probe yet.
			h.restartContainerInPlace(pod)
		case h.readinessFails[pod.UID] || probesNeverReadyPath(pod):
			// The readiness probe fails, by fault or because the runner
			// probes a path this node never serves: the container keeps
			// running, and the kubelet clears ContainersReady and Ready
			// while the serving gate keeps the value the controller wrote.
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  pod.Spec.Containers[0].Name,
				Image: image,
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
		default:
			prior := priorContainerStatus(pod)
			pod.Status.Phase = corev1.PodRunning
			// A run starts at the pass that first reports it running, as the
			// kubelet stamps it; a run already reported keeps its start.
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:  pod.Spec.Containers[0].Name,
				Image: h.runtimeImageName(image),
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(h.clk.Now())}},
			}}
			carryRestartEvidence(&pod.Status.ContainerStatuses[0], prior)
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, h.foldReady(pod), h.clk.Now())
		}
		if podConditionTrue(pod, corev1.PodReady) {
			h.servedPasses[wedgeKey(pod)]++
		}
		if status := priorContainerStatus(pod); status != nil && status.State.Running != nil {
			h.startedImage[pod.Name] = image
		}
		// Pod status is a built-in subresource on the fake client — a
		// plain Update silently drops status changes.
		if err := h.c.Status().Update(h.ctx, pod); err != nil {
			h.t.Fatalf("kubelet update %s: %v", pod.Name, err)
		}
	}
}

// promotedOn reports whether the row of the pod's Instance is Ready on the
// revision the pod carries: the pod was promoted, so a crash from here on
// is a crash after the Instance entered Ready.
func (h *recoveryHarness) promotedOn(pod *corev1.Pod) bool {
	idx, ok := query.InstanceIdxFromLabels(pod)
	if !ok {
		return false
	}
	for _, s := range h.irStatuses() {
		if s.Index == idx {
			return s.Phase == workloadtypes.InstancePhaseReady && query.RevisionFromName(s.RunningRevision).Hash() == pod.Labels[query.LabelRevisionHash]
		}
	}
	return false
}

// priorRunStart is when the run the kubelet model last reported for the
// pod's container started, zero when it reported none or the run's start
// is unknown.
func priorRunStart(pod *corev1.Pod) metav1.Time {
	if prior := priorContainerStatus(pod); prior != nil && prior.State.Running != nil {
		return prior.State.Running.StartedAt
	}
	return metav1.Time{}
}

// priorContainerStatus is the status the kubelet model reported for the
// pod's container on its previous pass, or nil before the first report.
func priorContainerStatus(pod *corev1.Pod) *corev1.ContainerStatus {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == pod.Spec.Containers[0].Name {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	return nil
}

// imageChangedInPlace reports whether the pod's spec names an image other
// than the one its running container was started from: an in-place image
// patch the kubelet has not acted on yet.
func (h *recoveryHarness) imageChangedInPlace(pod *corev1.Pod) bool {
	prior := priorContainerStatus(pod)
	if prior == nil || prior.State.Running == nil {
		return false
	}
	started, known := h.startedImage[pod.Name]
	return known && started != pod.Spec.Containers[0].Image
}

// runtimeImageName is the name the node's runtime reports for a spec
// image: the alias the node stored it under, else the spec's own name.
func (h *recoveryHarness) runtimeImageName(image string) string {
	if alias, ok := h.imageNameOnNode[image]; ok {
		return alias
	}
	return image
}

// restartContainerInPlace reports the kubelet's replacement of a running
// container with the pod's new spec image: the old process ends with the
// kill signal's exit code, the restart count rises, and the new container
// is running but not yet Ready, so the pod leaves ContainersReady and
// PodReady for this pass. A readiness fault lived in the old container
// and goes with it.
func (h *recoveryHarness) restartContainerInPlace(pod *corev1.Pod) {
	delete(h.readinessFails, pod.UID)
	prior := priorContainerStatus(pod)
	now := metav1.NewTime(h.clk.Now())
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         pod.Spec.Containers[0].Name,
		Image:        h.runtimeImageName(pod.Spec.Containers[0].Image),
		Ready:        false,
		RestartCount: prior.RestartCount + 1,
		State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode:   143,
			Reason:     "Error",
			FinishedAt: now,
		}},
	}}
	setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
	setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
}

// carryRestartEvidence keeps the restart count, the last termination and
// the running run's start the kubelet reported before, as the kubelet
// does until the next restart.
func carryRestartEvidence(status, prior *corev1.ContainerStatus) {
	if prior == nil {
		return
	}
	status.RestartCount = prior.RestartCount
	status.LastTerminationState = prior.LastTerminationState
	if prior.State.Running != nil && status.State.Running != nil {
		status.State.Running.StartedAt = prior.State.Running.StartedAt
	}
}

// crashesAfterRunning reports whether the kubelet model crashes pod on
// this pass for an image that runs first and dies later: once it has
// reported ContainersReady for crashAfterReadyImage, once it has served
// for crashAfterServed passes for crashAfterServingImage and on every
// later pass while the pod still runs that image, and on every other
// pass from then on for flapAfterServingImage. An in-place patch to a
// sound image starts the container over, so a pod that has crashed
// before runs again once its image is not one of these.
func (h *recoveryHarness) crashesAfterRunning(pod *corev1.Pod) bool {
	switch h.faultyImage(pod) {
	case crashAfterReadyImage:
		return h.crashes[wedgeKey(pod)] > 0 || podConditionTrue(pod, corev1.ContainersReady)
	case crashAfterServingImage:
		return h.crashes[wedgeKey(pod)] > 0 || h.servedPasses[wedgeKey(pod)] >= h.crashAfterServed
	case flapAfterServingImage:
		// Every other pass once it has served: the kubelet's back-off,
		// then a run that comes up Ready again.
		return h.servedPasses[wedgeKey(pod)] >= h.crashAfterServed && (h.crashes[wedgeKey(pod)]%2 == 0 || h.staysDown[pod.Name])
	}
	return false
}

// instanceIndexOf is the Instance index pod's labels name, -1 for a pod
// that names none.
func instanceIndexOf(pod *corev1.Pod) int32 {
	if idx, ok := query.InstanceIdxFromLabels(pod); ok {
		return idx
	}
	return -1
}

// faultyImage is the image the crash model reads for pod: its spec image,
// or none for a worker of a gang whose crash is the leader's alone.
func (h *recoveryHarness) faultyImage(pod *corev1.Pod) string {
	if h.crashLeaderOnly && pod.Labels[query.LabelRunner] == workloadtypes.RunnerWorker {
		return ""
	}
	return pod.Spec.Containers[0].Image
}

func podConditionTrue(pod *corev1.Pod, condType corev1.PodConditionType) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == condType {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// servingGate is the PodReady value kubelet derives for a pod whose
// containers are ready: True once the controller's serving gate is on.
func (h *recoveryHarness) servingGate(pod *corev1.Pod) corev1.ConditionStatus {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == query.ServingConditionType && cond.Status == corev1.ConditionTrue {
			return corev1.ConditionTrue
		}
	}
	return corev1.ConditionFalse
}

// foldReady is the kubelet folding every readiness gate the pod declares
// into Ready: the serving gate as the controller wrote it, and the
// template's second gate, which the model satisfies unless the pod is
// under the hold (gateHeld), where no writer ever does.
func (h *recoveryHarness) foldReady(pod *corev1.Pod) corev1.ConditionStatus {
	ready := h.servingGate(pod)
	if h.readinessGate == "" || !declaresReadinessGate(pod, h.readinessGate) {
		return ready
	}
	if h.gateHeld(pod) {
		return corev1.ConditionFalse
	}
	setPodCondition(pod, h.readinessGate, corev1.ConditionTrue, h.clk.Now())
	return ready
}

// gateHeld reports whether the pod was created under the second gate's
// hold: at or after gateHeldSince, and of gateHeldRunner when one is named.
func (h *recoveryHarness) gateHeld(pod *corev1.Pod) bool {
	if h.gateHeldSince.IsZero() || pod.CreationTimestamp.Time.Before(h.gateHeldSince) {
		return false
	}
	return h.gateHeldRunner == "" || pod.Labels[query.LabelRunner] == h.gateHeldRunner
}

// declaresReadinessGate reports whether the pod's spec lists gate.
func declaresReadinessGate(pod *corev1.Pod, gate corev1.PodConditionType) bool {
	for _, g := range pod.Spec.ReadinessGates {
		if g.ConditionType == gate {
			return true
		}
	}
	return false
}

// crashPod points the live pod of runner at index idx at image, the way
// a pod is made to fail without touching the template: only the image
// field of a running pod is mutable. An empty runner names the single
// runner of a single-pod Instance.
func (h *recoveryHarness) crashPod(idx int32, runner, image string) {
	h.t.Helper()
	for _, pod := range h.podsOf(idx) {
		if runner != "" && pod.Labels[query.LabelRunner] != runner {
			continue
		}
		pod.Spec.Containers[0].Image = image
		if err := h.c.Update(h.ctx, pod); err != nil {
			h.t.Fatalf("point pod %s at %s: %v", pod.Name, image, err)
		}
		return
	}
	h.t.Fatalf("no pod of instance %d runner %q to crash", idx, runner)
}

// failReadiness makes the readiness probe of the live pods of runner at
// index idx fail from the next kubelet pass on (every pod of the Instance
// when runner is empty): the containers keep running and the pods leave
// rotation.
func (h *recoveryHarness) failReadiness(idx int32, runner string) {
	h.t.Helper()
	failed := 0
	for _, pod := range h.podsOf(idx) {
		if runner != "" && pod.Labels[query.LabelRunner] != runner {
			continue
		}
		h.readinessFails[pod.UID] = true
		failed++
	}
	if failed == 0 {
		h.t.Fatalf("no pod of instance %d runner %q to fail readiness on", idx, runner)
	}
}

// restoreReadiness clears the readiness fault failReadiness set on the live
// pods of runner at index idx (every pod of the Instance when runner is
// empty): the pods report Ready again from the next kubelet pass on.
func (h *recoveryHarness) restoreReadiness(idx int32, runner string) {
	h.t.Helper()
	restored := 0
	for _, pod := range h.podsOf(idx) {
		if runner != "" && pod.Labels[query.LabelRunner] != runner {
			continue
		}
		delete(h.readinessFails, pod.UID)
		restored++
	}
	if restored == 0 {
		h.t.Fatalf("no pod of instance %d runner %q to restore readiness on", idx, runner)
	}
}

// settledOn reports whether the Component is at rest on rev with n
// Instances: every row Ready on rev with no operation, and every live pod
// carrying rev's hash, the desired image and no crash image.
func (h *recoveryHarness) settledOn(rev *appsv1.ControllerRevision, n int32) bool {
	sts := h.irStatuses()
	if int32(len(sts)) != n {
		return false
	}
	for _, s := range sts {
		if s.Phase != workloadtypes.InstancePhaseReady || s.RunningRevision != rev.Name || s.Operation != nil {
			return false
		}
	}
	perInstance := len(h.desired.Runners)
	pods := h.livePods()
	if len(pods) != int(n)*perInstance {
		return false
	}
	hash := query.RevisionOf(rev).Hash()
	for _, pod := range pods {
		if pod.Labels[query.LabelRevisionHash] != hash || pod.Spec.Containers[0].Image != h.desired.PodSpec.Containers[0].Image {
			return false
		}
	}
	return true
}

// repairInFlight reports whether any row is under a Restart.
func (h *recoveryHarness) repairInFlight() bool {
	for _, s := range h.irStatuses() {
		if s.Phase == workloadtypes.InstancePhaseRestarting ||
			(s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationRestart) {
			return true
		}
	}
	return false
}

// drainEvents moves every event the recorder holds into events.
func (h *recoveryHarness) drainEvents() {
	if h.recorder == nil {
		return
	}
	for {
		select {
		case e := <-h.recorder.Events:
			h.events = append(h.events, e)
		default:
			return
		}
	}
}

// sawEvent reports whether an event carrying reason has been recorded.
func (h *recoveryHarness) sawEvent(reason workloadtypes.EventReason) bool {
	for _, e := range h.events {
		if strings.Contains(e, string(reason)) {
			return true
		}
	}
	return false
}

// armWedge starts the fault: every pod created from now on parks in
// reason. sticky keeps those pods parked after clearWedge.
func (h *recoveryHarness) armWedge(reason string, sticky bool) {
	h.wedge = reason
	h.wedgeSince = h.clk.Now()
	h.wedgeSticky = sticky
	if h.wedgedPods == nil {
		h.wedgedPods = map[string]string{}
	}
}

// clearWedge ends the fault: pods created from now on start normally, and
// so do the parked ones unless the wedge was sticky.
func (h *recoveryHarness) clearWedge() {
	h.wedge = ""
	if !h.wedgeSticky {
		h.wedgedPods = map[string]string{}
	}
}

// wedgeLivePods parks every live pod in reason, whatever its age: the
// fault hits a set that was already running, as a crash loop does.
func (h *recoveryHarness) wedgeLivePods(reason string) {
	if h.wedgedPods == nil {
		h.wedgedPods = map[string]string{}
	}
	for _, pod := range h.livePods() {
		h.wedgedPods[wedgeKey(pod)] = reason
	}
}

// wedgePod parks one live pod in reason, whatever its age: the fault hits
// the one pod of a running set whose container reads the missing key.
func (h *recoveryHarness) wedgePod(pod *corev1.Pod, reason string) {
	if h.wedgedPods == nil {
		h.wedgedPods = map[string]string{}
	}
	h.wedgedPods[wedgeKey(pod)] = reason
}

// wedgeKey identifies one pod object across the kubelet model's passes: the
// fake client mints no UID, and a rebuilt pod reuses its name, so the
// creation instant the model stamps is what tells the two apart.
// crashTermination is the termination the kubelet model reports for a
// runner that just died: the run that died had started at the pod's
// prior run start and ended now, with crashExitCode (1 when unset).
func (h *recoveryHarness) crashTermination(pod *corev1.Pod) *corev1.ContainerStateTerminated {
	code, reason := int32(1), "Error"
	if h.crashExitCode != nil {
		code = *h.crashExitCode
		if code == 0 {
			reason = "Completed"
		}
	}
	return &corev1.ContainerStateTerminated{
		ExitCode:   code,
		Reason:     reason,
		StartedAt:  h.lastRunStart[wedgeKey(pod)],
		FinishedAt: metav1.NewTime(h.clk.Now()),
	}
}

// componentCrashedBefore reports whether, under crashOnceInComponent, a
// pod of the Component already crashed.
func (h *recoveryHarness) componentCrashedBefore() bool {
	return h.crashOnceInComponent && len(h.crashedInstances) > 0
}

// killRunnerOnce has the kubelet model kill pod's runner once on its next
// pass, as an operator or a fault does to a serving pod.
func (h *recoveryHarness) killRunnerOnce(pod *corev1.Pod) {
	if h.killOnce == nil {
		h.killOnce = map[string]bool{}
	}
	h.killOnce[pod.Name] = true
}

// keepRunnerDown has the kubelet model report pod's flapping runner
// crashed on every later pass instead of bringing it back: a runner that
// dies and stays down.
func (h *recoveryHarness) keepRunnerDown(pod *corev1.Pod) {
	if h.staysDown == nil {
		h.staysDown = map[string]bool{}
	}
	h.staysDown[pod.Name] = true
}

func wedgeKey(pod *corev1.Pod) string {
	return pod.Name + "@" + pod.CreationTimestamp.UTC().Format(time.RFC3339)
}

// wedgeReasonFor is the kubelet model's fault decision for one pod: the
// armed reason for a pod created under the wedge, the remembered reason for
// a sticky pod parked under an earlier wedge, otherwise none.
func (h *recoveryHarness) wedgeReasonFor(pod *corev1.Pod) string {
	if reason, ok := h.wedgedPods[wedgeKey(pod)]; ok {
		return reason
	}
	if h.wedge == "" || pod.CreationTimestamp.Time.Before(h.wedgeSince) {
		return ""
	}
	if h.wedgeRunner != "" && pod.Labels[query.LabelRunner] != h.wedgeRunner {
		return ""
	}
	h.wedgedPods[wedgeKey(pod)] = h.wedge
	return h.wedge
}

// setPodCondition writes a pod condition the way the kubelet does: the
// transition time moves only when the status changes, so a condition
// that holds its value keeps the instant it last flipped.
func setPodCondition(pod *corev1.Pod, condType corev1.PodConditionType, status corev1.ConditionStatus, now time.Time) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == condType {
			if pod.Status.Conditions[i].Status != status {
				pod.Status.Conditions[i].LastTransitionTime = metav1.NewTime(now)
			}
			pod.Status.Conditions[i].Status = status
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type: condType, Status: status, LastTransitionTime: metav1.NewTime(now),
	})
}

// interceptCreate stamps the UID the API server mints onto a new pod; the
// delete wave preconditions every pod delete on the UID it observed.
func (h *recoveryHarness) interceptCreate(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	if pod, ok := obj.(*corev1.Pod); ok {
		if h.admissionDown {
			return webhookUnreachableCreateError()
		}
		if h.shedCreates > 0 {
			h.shedCreates--
			return apierrors.NewTooManyRequests("the apiserver shed the create", 10)
		}
		if pod.UID == "" {
			pod.UID = uuid.NewUUID()
		}
	}
	return cl.Create(ctx, obj, opts...)
}

// webhookUnreachableCreateError is the apiserver failing closed on a pod
// admission webhook it could not call: an InternalError carrying the
// dispatcher's phrase, the webhook's name and the transport error.
func webhookUnreachableCreateError() error {
	return apierrors.NewInternalError(fmt.Errorf(
		`failed calling webhook "pod-mutator.example.com": failed to call webhook: Post "https://ome-webhook.example.svc:443/mutate-pods?timeout=10s": dial tcp 10.0.0.1:443: connect: connection refused`))
}

// eventCount is how many recorded events carry reason.
func (h *recoveryHarness) eventCount(reason workloadtypes.EventReason) int {
	n := 0
	for _, e := range h.events {
		if strings.Contains(e, string(reason)) {
			n++
		}
	}
	return n
}

// interceptDelete is the kubelet termination model's apiserver half: with
// podGrace set, a pod's first delete pins it as Terminating (through the
// harness finalizer) and records when the grace its delete names elapses
// on the fake clock; the pod's own grace applies to a delete that names
// none. Without podGrace the delete goes straight to the fake client.
func (h *recoveryHarness) interceptDelete(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok || h.podGrace <= 0 {
		return cl.Delete(ctx, obj, opts...)
	}
	stored := &corev1.Pod{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(pod), stored); err != nil {
		return err
	}
	options := &client.DeleteOptions{}
	for _, opt := range opts {
		opt.ApplyToDelete(options)
	}
	if options.GracePeriodSeconds != nil && *options.GracePeriodSeconds == 0 {
		// A grace-0 delete removes the object without its kubelet's say,
		// so the termination model's stand-in finalizer comes off first.
		// On a pod already Terminating that write is itself what takes
		// the object out of the fake apiserver, and the delete behind it
		// then finds nothing; the request succeeded either way, as a
		// grace-0 delete does on a cluster.
		stored.Finalizers = removeString(stored.Finalizers, terminationFinalizer)
		if err := cl.Update(ctx, stored); err != nil {
			return err
		}
		delete(h.terminatingUntil, pod.Name)
		if err := cl.Delete(ctx, stored, opts...); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	if stored.DeletionTimestamp == nil {
		grace := h.podGrace
		if options.GracePeriodSeconds != nil {
			grace = time.Duration(*options.GracePeriodSeconds) * time.Second
		}
		h.terminatingUntil[pod.Name] = h.clk.Now().Add(grace)
		stored.Finalizers = append(stored.Finalizers, terminationFinalizer)
		if err := cl.Update(ctx, stored); err != nil {
			return err
		}
	}
	return cl.Delete(ctx, stored, opts...)
}

// kubeletStopped reports whether the kubelet under pod has stopped: its
// node is dead, or the pod itself was marked as one its kubelet has
// stopped reporting.
func (h *recoveryHarness) kubeletStopped(pod *corev1.Pod) bool {
	return h.deadNodes[pod.Spec.NodeName] || h.kubeletGone[pod.Name]
}

// showTeardown presents a Terminating pod the way the apiserver would.
// Its deletion deadline is the one the termination model recorded on the
// fake clock - the request plus the grace, as the apiserver stamps it -
// because the fake client stamps the wall clock, which no deadline the
// engine measures on the injected clock can be compared against. A pod
// whose kubelet has stopped also carries no finalizer: on a cluster
// nothing but the kubelet's missing acknowledgement keeps such a pod, and
// the harness finalizer that stands in for that kubelet is exactly the
// evidence that makes the force-delete sweep decline the pod, so it is
// hidden from every read.
func (h *recoveryHarness) showTeardown(pod *corev1.Pod) {
	if pod.DeletionTimestamp == nil {
		return
	}
	if until, ok := h.terminatingUntil[pod.Name]; ok {
		deadline := metav1.NewTime(until)
		pod.DeletionTimestamp = &deadline
	}
	if !h.kubeletStopped(pod) {
		return
	}
	pod.Finalizers = removeString(pod.Finalizers, terminationFinalizer)
	if len(pod.Finalizers) == 0 {
		pod.Finalizers = nil
	}
}

// interceptGet and interceptList are the read half of showTeardown.
func (h *recoveryHarness) interceptGet(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := cl.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if pod, ok := obj.(*corev1.Pod); ok {
		h.showTeardown(pod)
	}
	return nil
}

func (h *recoveryHarness) interceptList(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
	if err := cl.List(ctx, list, opts...); err != nil {
		return err
	}
	if pods, ok := list.(*corev1.PodList); ok {
		for i := range pods.Items {
			h.showTeardown(&pods.Items[i])
		}
	}
	return nil
}

// interceptUpdate puts the stored deletion metadata back on a whole-object
// write of a pod read through showTeardown: the stored deletion timestamp
// is immutable in the fake apiserver, and on a pod whose kubelet has
// stopped the hidden finalizer is what keeps the object there at all -
// only the kubelet model takes it off once its node is back.
func (h *recoveryHarness) interceptUpdate(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && pod.DeletionTimestamp != nil {
		stored := &corev1.Pod{}
		if err := cl.Get(ctx, client.ObjectKeyFromObject(pod), stored); err == nil {
			pod.DeletionTimestamp = stored.DeletionTimestamp
			if h.kubeletStopped(pod) {
				pod.Finalizers = stored.Finalizers
			}
		}
	}
	return cl.Update(ctx, obj, opts...)
}

// finishTermination is the kubelet termination model's node half: once
// the recorded grace has elapsed the finalizer comes off and the object
// leaves, exactly as a kubelet that only stops the pod when its grace
// runs out.
func (h *recoveryHarness) finishTermination(pod *corev1.Pod) {
	h.t.Helper()
	if h.podGrace <= 0 || h.clk.Now().Before(h.terminatingUntil[pod.Name]) {
		return
	}
	pod.Finalizers = nil
	if err := h.c.Update(h.ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		h.t.Fatalf("kubelet finish termination %s: %v", pod.Name, err)
	}
	delete(h.terminatingUntil, pod.Name)
	delete(h.exitsCleanOnStop, pod.Name)
}

// reportStoppedClean is the kubelet publishing a terminating pod whose
// process exited 0 on SIGTERM: phase Succeeded, the container Terminated
// with exit 0, readiness withdrawn. The object leaves on the next pass; a
// kubelet removes a pod it has stopped without waiting out the grace.
func (h *recoveryHarness) reportStoppedClean(pod *corev1.Pod) {
	h.t.Helper()
	now := h.clk.Now()
	pod.Status.Phase = corev1.PodSucceeded
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  pod.Spec.Containers[0].Name,
		Image: pod.Spec.Containers[0].Image,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode:   0,
			Reason:     "Completed",
			FinishedAt: metav1.NewTime(now),
		}},
	}}
	setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, now)
	setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, now)
	h.terminatingUntil[pod.Name] = now
	if err := h.c.Status().Update(h.ctx, pod); err != nil {
		h.t.Fatalf("kubelet report stopped %s: %v", pod.Name, err)
	}
}

// evictPods is the kubelet evicting every live pod of the Instances at
// indices under node pressure: each is reported phase Failed where it
// stands and keeps its name until a pass deletes the object.
func (h *recoveryHarness) evictPods(indices ...int32) {
	h.t.Helper()
	evict := map[int32]bool{}
	for _, idx := range indices {
		evict[idx] = true
	}
	if h.evicted == nil {
		h.evicted = map[string]bool{}
	}
	for _, pod := range h.livePods() {
		if idx, ok := query.InstanceIdxFromLabels(pod); !ok || !evict[idx] {
			continue
		}
		h.evicted[wedgeKey(pod)] = true
		h.reportEvicted(pod)
	}
}

// reportEvicted is the kubelet publishing a pod it evicted: phase Failed
// with the eviction on the pod-level reason, the container terminated,
// readiness withdrawn. A pod already reported keeps its record.
func (h *recoveryHarness) reportEvicted(pod *corev1.Pod) {
	h.t.Helper()
	if pod.Status.Phase == corev1.PodFailed {
		return
	}
	now := h.clk.Now()
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = "Evicted"
	pod.Status.Message = "The node was low on resource: memory."
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  pod.Spec.Containers[0].Name,
		Image: pod.Spec.Containers[0].Image,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode:   137,
			Reason:     "Error",
			FinishedAt: metav1.NewTime(now),
		}},
	}}
	setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, now)
	setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, now)
	if err := h.c.Status().Update(h.ctx, pod); err != nil {
		h.t.Fatalf("kubelet report evicted %s: %v", pod.Name, err)
	}
}

// preempt is the scheduler taking the pod of runner at idx for a pod of
// higher priority: the DisruptionTarget condition lands, then a graceful
// delete under the pod's own grace. The process exits clean on SIGTERM,
// so with podGrace set the kubelet model reports the pod Succeeded on its
// next pass and removes it on the one after; with none the object is
// gone at once.
func (h *recoveryHarness) preempt(idx int32, runner string) {
	h.t.Helper()
	pod := h.podOf(idx, runner)
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.DisruptionTarget,
		Status:             corev1.ConditionTrue,
		Reason:             corev1.PodReasonPreemptionByScheduler,
		Message:            "preempted by a pod of higher priority",
		LastTransitionTime: metav1.NewTime(h.clk.Now()),
	})
	if err := h.c.Status().Update(h.ctx, pod); err != nil {
		h.t.Fatalf("stamp DisruptionTarget on %s: %v", pod.Name, err)
	}
	if h.exitsCleanOnStop == nil {
		h.exitsCleanOnStop = map[string]bool{}
	}
	h.exitsCleanOnStop[pod.Name] = true
	if err := h.c.Delete(h.ctx, pod); err != nil {
		h.t.Fatalf("preempt %s: %v", pod.Name, err)
	}
}

// holdPlacement is a cluster with no room from now on: the scheduler
// model refuses every pod it has not yet placed with message.
func (h *recoveryHarness) holdPlacement(message string) {
	h.placementHeld, h.placementMessage = true, message
}

// releasePlacement is the room coming back: the pods the scheduler
// refused are placed on the next kubelet pass.
func (h *recoveryHarness) releasePlacement() {
	h.placementHeld = false
}

// unplaced reports whether the scheduler model has yet to bind the pod:
// nothing has reported on it, or the scheduler refused it.
func (h *recoveryHarness) unplaced(pod *corev1.Pod) bool {
	if pod.Status.Phase == "" {
		return true
	}
	_, _, refused := workloadtypes.PodUnschedulable(pod)
	return refused
}

// reportUnschedulable is the scheduler model refusing a pod: Pending,
// nothing started, PodScheduled=False with the Unschedulable reason and
// the placement message.
func (h *recoveryHarness) reportUnschedulable(pod *corev1.Pod) {
	h.t.Helper()
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = nil
	setPodScheduled(pod, corev1.ConditionFalse, corev1.PodReasonUnschedulable, h.placementMessage, h.clk.Now())
	if err := h.c.Status().Update(h.ctx, pod); err != nil {
		h.t.Fatalf("scheduler refuse %s: %v", pod.Name, err)
	}
}

// setPodScheduled writes the PodScheduled condition, moving its transition
// time only when the status changes, as the scheduler writes it.
func setPodScheduled(pod *corev1.Pod, status corev1.ConditionStatus, reason, message string, now time.Time) {
	for i := range pod.Status.Conditions {
		cond := &pod.Status.Conditions[i]
		if cond.Type != corev1.PodScheduled {
			continue
		}
		if cond.Status != status {
			cond.LastTransitionTime = metav1.NewTime(now)
		}
		cond.Status, cond.Reason, cond.Message = status, reason, message
		return
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type: corev1.PodScheduled, Status: status, Reason: reason, Message: message, LastTransitionTime: metav1.NewTime(now),
	})
}

// useNodes turns on the node model with the named nodes, each with a
// live kubelet.
func (h *recoveryHarness) useNodes(names ...string) {
	h.t.Helper()
	h.nodeNames = append([]string(nil), names...)
	h.deadNodes = map[string]bool{}
	for _, name := range names {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
		setNodeReady(node, corev1.ConditionTrue, h.clk.Now())
		if err := h.c.Create(h.ctx, node); err != nil {
			h.t.Fatalf("create node %s: %v", name, err)
		}
	}
}

// leastLoadedLiveNode is the node model's placement for pod: the node
// with the fewest pods bound to it among those whose kubelet is alive,
// that are not cordoned, and that the pod's required node affinity
// admits, earliest declared first on a tie. A node with a stopped kubelet
// is never chosen, as a scheduler honoring its unreachable taint would
// not, and neither is a cordoned one.
func (h *recoveryHarness) leastLoadedLiveNode(pod *corev1.Pod, pods []corev1.Pod) string {
	h.t.Helper()
	bound := map[string]int{}
	for i := range pods {
		bound[pods[i].Spec.NodeName]++
	}
	chosen := ""
	for _, name := range h.nodeNames {
		if h.deadNodes[name] || h.cordonedNodes[name] || !nodeAffinityAdmits(pod, name) {
			continue
		}
		if chosen == "" || bound[name] < bound[chosen] {
			chosen = name
		}
	}
	if chosen == "" {
		h.t.Fatalf("no node with a live kubelet to place pod %s on", pod.Name)
	}
	return chosen
}

// nodeAffinityAdmits is the scheduler model's required node-affinity
// filter over the hostname label: the node is admitted when the pod
// requires nothing, or when one of its required terms holds, a term
// holding when every hostname expression in it does. Expressions on other
// labels are not modelled and hold trivially.
func nodeAffinityAdmits(pod *corev1.Pod, node string) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return true
	}
	for _, term := range terms {
		holds := true
		for _, expr := range term.MatchExpressions {
			if expr.Key != corev1.LabelHostname {
				continue
			}
			named := false
			for _, value := range expr.Values {
				if value == node {
					named = true
					break
				}
			}
			switch expr.Operator {
			case corev1.NodeSelectorOpIn:
				holds = holds && named
			case corev1.NodeSelectorOpNotIn:
				holds = holds && !named
			}
		}
		if holds {
			return true
		}
	}
	return false
}

// failNode stops the kubelet under name. The node model freezes every
// pod bound to it, and the control plane's half follows: the node's
// Ready condition goes Unknown with a fresh transition time, and PodReady
// is withdrawn from the pods on it while their own last report still says
// Ready — the shape the node lifecycle controller leaves behind.
func (h *recoveryHarness) failNode(name string) {
	h.t.Helper()
	h.deadNodes[name] = true
	h.setNodeCondition(name, corev1.ConditionUnknown)
	for _, pod := range h.podsOnNode(name) {
		for i := range pod.Status.Conditions {
			cond := &pod.Status.Conditions[i]
			if cond.Type != corev1.PodReady || cond.Status != corev1.ConditionTrue {
				continue
			}
			cond.Status = corev1.ConditionFalse
			cond.LastTransitionTime = metav1.NewTime(h.clk.Now())
			if err := h.c.Status().Update(h.ctx, pod); err != nil {
				h.t.Fatalf("withdraw PodReady from %s: %v", pod.Name, err)
			}
		}
	}
}

// recoverNode starts the kubelet under name again: the node posts Ready
// and the node model resumes reporting whatever pods it still holds.
func (h *recoveryHarness) recoverNode(name string) {
	h.t.Helper()
	delete(h.deadNodes, name)
	h.setNodeCondition(name, corev1.ConditionTrue)
}

// cordonNode marks name unschedulable, as kubectl cordon does: the pods
// on it keep running and the scheduler model places nothing new there.
func (h *recoveryHarness) cordonNode(name string) {
	h.t.Helper()
	node := &corev1.Node{}
	if err := h.c.Get(h.ctx, types.NamespacedName{Name: name}, node); err != nil {
		h.t.Fatalf("get node %s: %v", name, err)
	}
	node.Spec.Unschedulable = true
	if err := h.c.Update(h.ctx, node); err != nil {
		h.t.Fatalf("cordon node %s: %v", name, err)
	}
	if h.cordonedNodes == nil {
		h.cordonedNodes = map[string]bool{}
	}
	h.cordonedNodes[name] = true
}

// cordonedInstances projects the Instances with a pod on a cordoned node
// the way the adapter does, from the Nodes and pods the API holds.
func (h *recoveryHarness) cordonedInstances() map[int32]struct{} {
	h.t.Helper()
	nodes, err := query.CordonedNodes(h.ctx, h.c)
	if err != nil {
		h.t.Fatalf("list cordoned nodes: %v", err)
	}
	return query.CordonedInstances(query.BucketPodsByInstanceIdx(h.livePods()), nodes)
}

func (h *recoveryHarness) setNodeCondition(name string, ready corev1.ConditionStatus) {
	h.t.Helper()
	node := &corev1.Node{}
	if err := h.c.Get(h.ctx, types.NamespacedName{Name: name}, node); err != nil {
		h.t.Fatalf("get node %s: %v", name, err)
	}
	setNodeReady(node, ready, h.clk.Now())
	if err := h.c.Status().Update(h.ctx, node); err != nil {
		h.t.Fatalf("update node %s: %v", name, err)
	}
}

// setNodeReady writes the node's Ready condition with transition time now.
func setNodeReady(node *corev1.Node, status corev1.ConditionStatus, now time.Time) {
	node.Status.Conditions = []corev1.NodeCondition{{
		Type: corev1.NodeReady, Status: status, LastTransitionTime: metav1.NewTime(now),
	}}
}

// podsOnNode returns every pod object bound to name, Terminating ones
// included.
func (h *recoveryHarness) podsOnNode(name string) []*corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	var out []*corev1.Pod
	for i := range pods.Items {
		if pods.Items[i].Spec.NodeName == name {
			out = append(out, &pods.Items[i])
		}
	}
	return out
}

// removeString returns list without every occurrence of value.
func removeString(list []string, value string) []string {
	out := list[:0]
	for _, item := range list {
		if item != value {
			out = append(out, item)
		}
	}
	return out
}

// livePodOnImage reports whether a live (not Terminating) pod runs image.
func (h *recoveryHarness) livePodOnImage(image string) bool {
	for _, pod := range h.livePods() {
		if pod.Spec.Containers[0].Image == image {
			return true
		}
	}
	return false
}

// surgeOpen reports whether the Instance carries an in-flight surge step.
func (h *recoveryHarness) surgeOpen(idx int32) bool {
	s := h.instance(idx)
	return s != nil && s.Phase == workloadtypes.InstancePhaseUpdating &&
		s.Operation != nil && s.Operation.Step == workloadtypes.UpdateStepSurge
}

func (h *recoveryHarness) irKey() types.NamespacedName {
	return types.NamespacedName{Namespace: recoveryNS, Name: recoveryOwner + "-" + string(h.component)}
}

func (h *recoveryHarness) irStatuses() []workloadtypes.InstanceStatus {
	h.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := h.c.Get(h.ctx, h.irKey(), ir); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		h.t.Fatalf("get IR: %v", err)
	}
	out := make([]workloadtypes.InstanceStatus, 0, len(ir.Status.InstanceStatuses))
	for _, s := range ir.Status.InstanceStatuses {
		out = append(out, v1beta1convert.InstanceStatusToWorkload(s))
	}
	return out
}

func (h *recoveryHarness) removeInstance() func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		ir := &v1beta1.InferenceReplica{}
		if err := h.c.Get(ctx, h.irKey(), ir); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		pos := -1
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		if pos == -1 {
			return false, nil
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses[:pos], ir.Status.InstanceStatuses[pos+1:]...)
		if err := h.c.Status().Update(ctx, ir); err != nil {
			return false, err
		}
		return true, nil
	}
}

// mutateInstance is the harness's status-write seam: the IR round-trip
// for every index, except those mutateErr rejects.
func (h *recoveryHarness) mutateInstance() func(context.Context, int32, func(*workloadtypes.InstanceStatus) bool) error {
	delegate := roundTripMutateInstance(h.c, h.isvc, h.component)
	return func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		if err := h.mutateErr[idx]; err != nil {
			return err
		}
		committed := false
		err := delegate(ctx, idx, func(s *workloadtypes.InstanceStatus) bool {
			committed = mutate(s)
			return committed
		})
		if err == nil && committed {
			h.statusWrites++
		}
		return err
	}
}

func (h *recoveryHarness) buildInput() workloadtypes.ReconcileInput {
	in := workloadtypes.ReconcileInput{
		OwnerObject: h.isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: h.isvc,
		Key: workloadtypes.Key{
			Namespace: recoveryNS,
			Component: h.component,
			OwnerName: recoveryOwner,
		},
		DesiredSpec: h.desired,
		ObservedState: workloadtypes.WorkloadObservedState{
			InstanceStatuses:  h.irStatuses(),
			RetryBlocks:       append([]workloadtypes.RetryBlock(nil), h.blocks...),
			Migrations:        append([]workloadtypes.MigrationRecord(nil), h.migrations...),
			CurrentRevision:   h.currentRevision,
			UpdateRevision:    h.target.Name,
			CordonedInstances: h.cordonedInstances(),
		},
		MutateInstance:     h.mutateInstance(),
		MutateMigration:    h.mutateMigration,
		MigrationAudit:     h.migrationAudit,
		RemoveInstance:     h.removeInstance(),
		UpdateGate:         h.gate,
		PlanGate:           h.planGate,
		Gangs:              h.gangs,
		UpdateRetryPolicy:  h.retryPolicy,
		StuckPodGrace:      h.stuckGrace,
		UnschedulableGrace: h.unschedulableGrace,
		ForceDelete:        h.forceDelete,
		Requeue:            h.requeue,
		Clock:              h.clk,
		RepairBatchSize:    h.repairBatchSize,

		AbandonedReplacementGrace: h.abandonGrace,
		DrainHolds:                &workloadtypes.DrainHolds{},
		RecordRolloutHold: func(hold *workloadtypes.RolloutHold) {
			h.holdVerdicts++
			h.verdict = nil
			if hold != nil {
				h.holds = append(h.holds, *hold)
				h.verdict = &h.holds[len(h.holds)-1]
			}
		},
		WarnRetryHeld: func(rev string, attempts int32, reason string) {
			h.heldWarnings = append(h.heldWarnings, fmt.Sprintf("%s attempts=%d %s", rev, attempts, reason))
		},
		WarnInstanceFailed: func(idx int32, podName, reason string) {
			h.failedWarnings = append(h.failedWarnings, fmt.Sprintf("instance=%d pod=%s %s", idx, podName, reason))
		},
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
			h.mutateRetryBlock(rev, mutate)
			return nil
		},
	}
	if h.atomicStatus {
		in.ApplyInstanceMutationsWithRetryBlock = h.applyInstanceMutations
	}
	stubInputCallbacks(&in)
	return in
}

// mutateRetryBlock applies one RetryBlock mutation to the harness's block
// list, which stands in for the IR's RetryBlocks.
func (h *recoveryHarness) mutateRetryBlock(rev string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) {
	pos := -1
	b := workloadtypes.RetryBlock{TargetRevision: rev}
	for i := range h.blocks {
		if h.blocks[i].TargetRevision == rev {
			pos, b = i, h.blocks[i]
			break
		}
	}
	switch mutate(&b) {
	case workloadtypes.RetryBlockPersist:
		if pos == -1 {
			h.blocks = append(h.blocks, b)
		} else {
			h.blocks[pos] = b
		}
	case workloadtypes.RetryBlockRemove:
		if pos != -1 {
			h.blocks = append(h.blocks[:pos], h.blocks[pos+1:]...)
		}
	}
}

// applyInstanceMutations is the harness's owner-aware atomic status
// adapter: one IR read, every mutation applied to its slot with the
// precondition, removal and commit-callback semantics of the in-memory
// store, one status write, and the RetryBlock mutation applied to the
// harness's block list alongside it. A row whose status writes mutateErr
// rejects fails the whole batch, as the API server refusing the IR status
// update would.
func (h *recoveryHarness) applyInstanceMutations(ctx context.Context, mutations []workloadtypes.InstanceMutation, targetRevision string, mutateRetryBlock func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
	for _, mutation := range mutations {
		if err := h.mutateErr[mutation.Index]; err != nil {
			return err
		}
	}
	ir := &v1beta1.InferenceReplica{}
	create := false
	if err := h.c.Get(ctx, h.irKey(), ir); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get IR: %w", err)
		}
		ir = &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: h.irKey().Namespace, Name: h.irKey().Name}}
		create = true
	}
	store := &testAtomicMutationStore{owner: h.isvc, statuses: v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses)}
	if err := store.apply(ctx, mutations, targetRevision, nil); err != nil {
		return err
	}
	if mutateRetryBlock != nil {
		h.mutateRetryBlock(targetRevision, mutateRetryBlock)
	}
	if store.writes == 0 {
		return nil
	}
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, len(store.statuses))
	for _, row := range store.statuses {
		rows = append(rows, v1beta1convert.InstanceStatusFromWorkload(row))
	}
	if create {
		if err := h.c.Create(ctx, ir); err != nil {
			return fmt.Errorf("create IR: %w", err)
		}
	}
	ir.Status.InstanceStatuses = rows
	return h.c.Status().Update(ctx, ir)
}

// step runs one reconcile: kubelet convergence, fresh expectations
// (watch caught up), Reconcile, aggregate CurrentRevision, clock
// advance (the op's own requeue interval, or the step floor). A
// Reconcile error fails the test.
func (h *recoveryHarness) step() {
	h.t.Helper()
	if _, err := h.stepResult(); err != nil {
		h.t.Fatalf("Reconcile: %v", err)
	}
}

// stepResult is step returning the pass's result and error, for tests
// that expect a pass to fail.
func (h *recoveryHarness) stepResult() (ctrl.Result, error) {
	h.t.Helper()
	res, err := h.pass()
	// The floor keeps stuck-pod grace and Backoff windows crossing in
	// a handful of passes, and lets an 80-pass wedge run outlive the 30m
	// Operation deadline — proving the deadline backstop is also inert.
	advance := h.stepFloor
	if res.RequeueAfter > advance {
		advance = res.RequeueAfter
	}
	if h.stepCap > 0 && advance > h.stepCap {
		advance = h.stepCap
	}
	h.clk.Step(advance)
	return res, err
}

// pass runs one reconcile and leaves the clock where it is: kubelet
// convergence, fresh expectations (watch caught up), Reconcile,
// aggregate CurrentRevision.
func (h *recoveryHarness) pass() (ctrl.Result, error) {
	h.t.Helper()
	h.kubelet()
	if h.routed {
		h.endpointController()
	}
	if h.kubeletLag > 0 {
		h.clk.Step(h.kubeletLag)
	}
	deps := workloadtypes.Deps{Client: h.cachedClient(), APIReader: h.c, Expectations: workloadtypes.NewExpectations()}
	if h.recorder != nil {
		deps.Recorder = h.recorder
	}
	in := h.buildInput()
	plan, err := workload.BuildPlan(h.component, h.desired, in.ObservedState)
	if err != nil {
		h.t.Fatalf("BuildPlan: %v", err)
	}
	res, err := workload.Reconcile(h.ctx, deps, in, plan, h.target)
	h.drainEvents()
	h.publish(plan)
	if status.RolloutComplete(h.irStatuses(), h.target.Name) {
		h.currentRevision = h.target.Name
	}
	return res, err
}

// cachedClient is the informer-backed client a pass reads through: the
// fake client itself or, for one pass after cacheLagOnce, a view whose
// first pod List is the list the previous pass opened on.
func (h *recoveryHarness) cachedClient() client.Client {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods for the cache view: %v", err)
	}
	var cached client.Client = h.c
	if h.cacheLagOnce && h.priorOpening != nil {
		cached = &laggingPodListClient{Client: h.c, stale: h.priorOpening}
	}
	h.cacheLagOnce = false
	h.priorOpening = pods.Items
	return cached
}

// laggingPodListClient answers the first pod List with an older list and
// every later read from the client it wraps.
type laggingPodListClient struct {
	client.Client
	stale  []corev1.Pod
	served bool
}

func (c *laggingPodListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	pods, ok := list.(*corev1.PodList)
	if !ok || c.served {
		return c.Client.List(ctx, list, opts...)
	}
	c.served = true
	pods.Items = make([]corev1.Pod, 0, len(c.stale))
	for i := range c.stale {
		pods.Items = append(pods.Items, *c.stale[i].DeepCopy())
	}
	return nil
}

// runOnWakeUps is run for a controller that reconciles only when it is
// woken: by the wait its last pass asked for, or, when the pass asked
// for none, by the kubelet's next status change nextPodEvent away. The
// clock is left at the pass that satisfied pred, so a test can read
// when the controller acted.
func (h *recoveryHarness) runOnWakeUps(max int, nextPodEvent time.Duration, pred func() bool) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		if pred() {
			return true
		}
		res, err := h.pass()
		if err != nil {
			h.t.Fatalf("Reconcile: %v", err)
		}
		if pred() {
			return true
		}
		h.clk.Step(h.sleepAfter(res, nextPodEvent))
	}
	return pred()
}

// sleepAfter is how long a controller with no pod event coming sleeps
// after a pass: the explicit wait the pass asked for, the rate-limited
// backoff a bare requeue asks for, or the next pod event when the pass
// asked for nothing, whichever comes first.
func (h *recoveryHarness) sleepAfter(res ctrl.Result, nextPodEvent time.Duration) time.Duration {
	wait := nextPodEvent
	switch {
	case res.RequeueAfter > 0:
		h.bareRequeues = 0
		if res.RequeueAfter < wait {
			wait = res.RequeueAfter
		}
	case res.Requeue: //nolint:staticcheck // the bare backoff has no non-deprecated spelling
		h.bareRequeues++
		if backoff := rateLimitedBackoff(h.bareRequeues); backoff < wait {
			wait = backoff
		}
	default:
		h.bareRequeues = 0
	}
	return wait
}

// The controller's default per-item rate limiter: a bare requeue sleeps
// the base delay doubled for every consecutive bare requeue before it,
// up to the cap.
const (
	rateLimitedBackoffBase = 5 * time.Millisecond
	rateLimitedBackoffCap  = 1000 * time.Second
)

// rateLimitedBackoff is the sleep the controller's per-item rate limiter
// imposes on the nth consecutive bare requeue.
func rateLimitedBackoff(n int) time.Duration {
	backoff := rateLimitedBackoffBase
	for i := 1; i < n && backoff < rateLimitedBackoffCap; i++ {
		backoff *= 2
	}
	if backoff > rateLimitedBackoffCap {
		return rateLimitedBackoffCap
	}
	return backoff
}

// podOf is the live pod of runner at index idx. An empty runner names
// the single pod of a single-pod Instance.
func (h *recoveryHarness) podOf(idx int32, runner string) *corev1.Pod {
	h.t.Helper()
	for _, pod := range h.podsOf(idx) {
		if runner == "" || pod.Labels[query.LabelRunner] == runner {
			return pod
		}
	}
	h.t.Fatalf("no pod of instance %d runner %q", idx, runner)
	return nil
}

// admitLater moves the creation of the pod of runner at idx later by
// skew: the apiserver admits a gang's pods one at a time, so the
// members' ages differ. The creation stays in the past.
func (h *recoveryHarness) admitLater(idx int32, runner string, skew time.Duration) {
	h.t.Helper()
	pod := h.podOf(idx, runner)
	admitted := pod.CreationTimestamp.Add(skew)
	if admitted.After(h.clk.Now()) {
		h.t.Fatalf("pod %s would be admitted %s from now", pod.Name, admitted.Sub(h.clk.Now()))
	}
	pod.CreationTimestamp = metav1.NewTime(admitted)
	if err := h.c.Update(h.ctx, pod); err != nil {
		h.t.Fatalf("restamp creation of %s: %v", pod.Name, err)
	}
}

// publish is the status write the InferenceReplica adapter makes after
// every pass: the phase the publication derives and the pod counters of
// the pass's pod observation land on the rows the next pass reads. Only
// the fields the adapter persists are written, a row whose status writes
// mutateErr rejects is left as it is, and the harness has no
// EndpointSlices, so the Available counters read zero.
func (h *recoveryHarness) publish(plan workloadtypes.ComponentPlan) {
	h.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := h.c.Get(h.ctx, h.irKey(), ir); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		h.t.Fatalf("publish: get IR: %v", err)
	}
	if len(ir.Status.InstanceStatuses) == 0 {
		return
	}
	list := &corev1.PodList{}
	if err := h.c.List(h.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("publish: list pods: %v", err)
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, &list.Items[i])
	}
	observation, err := workload.NewOwnedPublicationObservation(
		v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses),
		workload.NewCachedSelectorPodObservation(pods, query.BucketPodsByInstanceIdx(pods)),
		h.availableByPod(),
		status.AvailabilityWindow{MinReadySeconds: plan.MinReadySeconds, Now: h.clk.Now()},
	)
	if err != nil {
		h.t.Fatalf("publish: build observation: %v", err)
	}
	publication, _, err := observation.TakeInlineV1Publication(status.DesiredPodCountByInstance(plan), h.target.Name)
	if err != nil {
		h.t.Fatalf("publish: consume observation: %v", err)
	}
	for i := range ir.Status.InstanceStatuses {
		if h.mutateErr[ir.Status.InstanceStatuses[i].Index] != nil {
			continue
		}
		ir.Status.InstanceStatuses[i].Phase = v1beta1convert.InstancePhaseFromWorkload(publication[i].Phase)
		ir.Status.InstanceStatuses[i].PodCount = publication[i].PodCount
		ir.Status.InstanceStatuses[i].ServingPodCount = publication[i].ServingPodCount
		ir.Status.InstanceStatuses[i].AvailablePodCount = publication[i].AvailablePodCount
		ir.Status.InstanceStatuses[i].Admitted = publication[i].Admitted
	}
	if err := h.c.Status().Update(h.ctx, ir); err != nil {
		h.t.Fatalf("publish: write IR status: %v", err)
	}
}

// run steps until pred returns true, up to max iterations. Returns
// whether pred was ever satisfied.
func (h *recoveryHarness) run(max int, pred func() bool) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		if pred() {
			return true
		}
		h.step()
	}
	return pred()
}

// runWithInvariant is run with a per-step invariant check: inv is
// evaluated after every step (and before the first), so a transient
// violation mid-recovery fails the test even if the end state is fine.
func (h *recoveryHarness) runWithInvariant(max int, pred func() bool, inv func()) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		inv()
		if pred() {
			return true
		}
		h.step()
	}
	inv()
	return pred()
}

// servingPods returns the live pods currently carrying the serving
// gate (in LB rotation).
func (h *recoveryHarness) servingPods() []*corev1.Pod {
	var out []*corev1.Pod
	for _, p := range h.livePods() {
		if podreadiness.IsServing(p) {
			out = append(out, p)
		}
	}
	return out
}

func (h *recoveryHarness) findBlock(rev string) *workloadtypes.RetryBlock {
	for i := range h.blocks {
		if h.blocks[i].TargetRevision == rev {
			return &h.blocks[i]
		}
	}
	return nil
}

func (h *recoveryHarness) livePods() []*corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	out := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			out = append(out, &pods.Items[i])
		}
	}
	return out
}

func (h *recoveryHarness) badPods() []*corev1.Pod {
	var out []*corev1.Pod
	for _, p := range h.livePods() {
		if p.Spec.Containers[0].Image == badImage {
			out = append(out, p)
		}
	}
	return out
}

// converged reports full convergence on rev: exactly one InstanceStatus,
// Phase=Ready, RunningRevision=rev, no Operation, and zero bad-image
// pods left anywhere (wreckage cleaned).
func (h *recoveryHarness) converged(rev string) bool {
	sts := h.irStatuses()
	if len(sts) != 1 {
		return false
	}
	s := sts[0]
	if s.Phase != workloadtypes.InstancePhaseReady || s.RunningRevision != rev || s.Operation != nil {
		return false
	}
	return len(h.badPods()) == 0
}

func (h *recoveryHarness) dumpState(label string) {
	h.t.Logf("--- %s ---", label)
	for _, s := range h.irStatuses() {
		op := "nil"
		if s.Operation != nil {
			op = fmt.Sprintf("{type=%s step=%s target=%s surgeIdx=%v}", s.Operation.Type, s.Operation.Step, s.Operation.TargetRevision, s.Operation.SurgeIndex)
		}
		h.t.Logf("  instance %d: phase=%s running=%s target=%s op=%s", s.Index, s.Phase, s.RunningRevision, s.TargetRevision, op)
	}
	for _, b := range h.blocks {
		h.t.Logf("  block %s: state=%s attempts=%d", b.TargetRevision, b.State, b.AttemptsStarted)
	}
	for _, p := range h.livePods() {
		h.t.Logf("  pod %s image=%s rev=%s", p.Name, p.Spec.Containers[0].Image, p.Labels[query.LabelRevisionHash])
	}
}

// driveToReadyOnV1 converges the initial create on the good revision.
func (h *recoveryHarness) driveToReadyOnV1() {
	h.t.Helper()
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("initial create")
		h.t.Fatalf("initial create never converged on v1")
	}
}

// driveToHeld publishes the bad revision and drives the real exhaustion
// flow until retryBlocks[bad]=Held.
func (h *recoveryHarness) driveToHeld() {
	h.t.Helper()
	h.setTarget(h.revBad, badImage)
	held := func() bool {
		b := h.findBlock(h.revBad.Name)
		return b != nil && b.State == workloadtypes.RetryBlockHeld
	}
	if !h.run(120, held) {
		h.dumpState("exhaustion")
		h.t.Fatalf("retry exhaustion never reached Held for %s", h.revBad.Name)
	}
	if len(h.heldWarnings) != 1 || !strings.HasPrefix(h.heldWarnings[0], h.revBad.Name) {
		h.t.Fatalf("WarnRetryHeld: got %v, want exactly one warning for %s", h.heldWarnings, h.revBad.Name)
	}
}

// settle runs a few extra passes so the post-Held state reaches its
// steady shape (in-flight abandon/dispose passes complete; nothing new
// starts because the Held gate denies the bad target).
func (h *recoveryHarness) settle(n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		h.step()
	}
}

func TestCorrectiveRecovery_Gang_InitialCreateRetarget(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.setTarget(h.revBad, badImage)
	h.step()

	initial := h.livePods()
	if len(initial) != 2 {
		t.Fatalf("initial Create: got %d pods want leader and worker", len(initial))
	}
	removedWorker := false
	for _, pod := range initial {
		if pod.Labels[query.LabelRunner] != "worker" {
			continue
		}
		if err := h.c.Delete(h.ctx, pod); err != nil {
			t.Fatalf("delete worker %s: %v", pod.Name, err)
		}
		removedWorker = true
		break
	}
	if !removedWorker {
		t.Fatal("initial Create did not produce a worker pod")
	}

	h.setTarget(h.revFixed, fixedImage)
	converged := h.runWithInvariant(80,
		func() bool { return h.converged(h.revFixed.Name) },
		func() {
			revisionsByInstance := map[string]map[string]struct{}{}
			for _, pod := range h.livePods() {
				index := pod.Labels[query.LabelInstanceIdx]
				if revisionsByInstance[index] == nil {
					revisionsByInstance[index] = map[string]struct{}{}
				}
				if hash := pod.Labels[query.LabelRevisionHash]; hash != "" {
					revisionsByInstance[index][hash] = struct{}{}
				}
			}
			for index, revisions := range revisionsByInstance {
				if len(revisions) > 1 {
					t.Fatalf("instance %s contains pods from multiple revisions: %v; statuses=%+v", index, revisions, h.irStatuses())
				}
			}
		})
	if !converged {
		h.dumpState("initial Create retarget wedged")
		t.Fatalf("gang initial Create did not converge after a corrective edit")
	}
}

// ---------------------------------------------------------------------------
// Roll-forward: corrective NEW revision.
// ---------------------------------------------------------------------------

// TestCorrectiveRecovery_SinglePod_RollForward covers the corrective
// release of a single-pod instance: Held + Phase=Failed + the
// bad-revision pod still occupying the surge ordinal, then a corrective
// NEW revision. The rollout must converge on the corrective revision
// with zero manual intervention: reclassifyByRevisionHash
// (ops/update_surge.go) routes the dead pod — labeled a third revision,
// neither RunningRevision nor the pinned target — into the drain set,
// the stale-slot eviction clears the surge ordinal, and the fresh surge
// drives to the fixed revision. The serving source must
// stay in rotation throughout: the recovery is SurgeThenDrain, so
// capacity never drops to zero while the wreckage is cleaned.
func TestCorrectiveRecovery_SinglePod_RollForward(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (single-pod)")

	h.setTarget(h.revFixed, fixedImage)
	converged := h.runWithInvariant(80,
		func() bool { return h.converged(h.revFixed.Name) },
		func() {
			if len(h.servingPods()) == 0 {
				t.Fatalf("no pod serving mid-recovery: the cleanup pulled the healthy source out of rotation before the corrective surge was ready")
			}
		})
	if !converged {
		h.dumpState("roll-forward wedged (single-pod)")
		t.Fatalf("single-pod roll-forward after Held did not converge")
	}
}

// TestCorrectiveRecovery_SinglePod_StuckSurgeAbandonedOnBoundedGrace covers
// the corrective edit behind a stuck surge. The replacement of an in-flight
// surge runs but never becomes ContainersReady: nothing escalates it before
// the operation deadline, and it holds the Instance's surge slot. A
// corrective revision abandons it, and the replacement it never marked
// serving must go on the configured abandoned-replacement grace rather than
// on the pod's own termination grace, which here is far longer than the
// run: the kubelet model keeps a deleted pod Terminating for the grace its
// delete names. The corrective replacement lands within that bound, the
// source serves throughout, and the Instance converges on the corrective
// revision.
func TestCorrectiveRecovery_SinglePod_StuckSurgeAbandonedOnBoundedGrace(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.podGrace = 10 * time.Minute
	h.abandonGrace = 10 * time.Second
	h.driveToReadyOnV1()

	revStuck := h.ensureRevision(recoveryPodSpec(stuckImage))
	h.setTarget(revStuck, stuckImage)
	if !h.run(10, func() bool { return h.surgeOpen(0) && h.livePodOnImage(stuckImage) }) {
		h.dumpState("surge toward the never-Ready revision")
		t.Fatalf("the surge toward the never-Ready revision never opened")
	}
	h.settle(3)
	if !h.surgeOpen(0) || h.instance(0).Phase == workloadtypes.InstancePhaseFailed {
		h.dumpState("stuck surge")
		t.Fatalf("a Running, never-Ready replacement must hold the surge open, not escalate")
	}

	// Every pass advances the clock by at least the dispatcher cadence, so
	// this bound is a few cadences past the abandoned-replacement grace and
	// far short of the pod's own grace.
	h.setTarget(h.revFixed, fixedImage)
	landed := h.runWithInvariant(8,
		func() bool { return h.livePodOnImage(fixedImage) },
		func() {
			if len(h.servingPods()) == 0 {
				t.Fatalf("no pod serving while the stuck surge is abandoned: the source left rotation before the corrective replacement was ready")
			}
		})
	if !landed {
		h.dumpState("corrective edit behind the stuck surge")
		t.Fatalf("the corrective replacement did not land within the abandoned-replacement grace: the abandoned never-Ready replacement held the surge slot for its own termination grace")
	}
	if h.livePodOnImage(stuckImage) {
		t.Fatalf("the abandoned never-Ready replacement is still live beside the corrective one")
	}
	if !h.run(80, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("corrective convergence")
		t.Fatalf("the corrective revision did not converge after the stuck surge was abandoned")
	}
}

// TestCorrectiveRecovery_Gang_RollForward pins gang recovery from the
// settled at-Held state (which the abandon path leaves CLEAN: source
// Ready on v1, marker removed, surge pods deleted).
func TestCorrectiveRecovery_Gang_RollForward(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (gang)")

	h.setTarget(h.revFixed, fixedImage)
	if !h.run(80, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("roll-forward wedged")
		t.Fatalf("gang roll-forward after Held did not converge")
	}
}

// TestCorrectiveRecovery_Gang_RollForward_MidWreckage pins the dirty
// timing: the corrective revision lands while the source is
// Failed-with-Operation toward the bad revision, the GangSurgeTarget
// marker still exists, and the crashed surge gang's pods are still
// live. The Failed continuation must dispatch, abandon the stale surge,
// and re-surge toward the corrective revision.
func TestCorrectiveRecovery_Gang_RollForward_MidWreckage(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()

	h.setTarget(h.revBad, badImage)
	if !h.run(60, func() bool { return h.gangMidWreckage() }) {
		h.dumpState("exhaustion")
		t.Fatalf("never reached the mid-wreckage state (source Failed-with-op + marker + live bad pods)")
	}
	h.dumpState("mid-wreckage (gang)")

	h.setTarget(h.revFixed, fixedImage)
	if !h.run(80, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("roll-forward wedged")
		t.Fatalf("gang mid-wreckage roll-forward did not converge")
	}
}

// gangMidWreckage reports the dirty gang state: source instance
// Phase=Failed with a preserved gang-surge Update Operation, the surge
// marker present, and the bad gang's pods live.
func (h *recoveryHarness) gangMidWreckage() bool {
	var source, marker *workloadtypes.InstanceStatus
	sts := h.irStatuses()
	for i := range sts {
		s := &sts[i]
		if s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationUpdate && s.Operation.SurgeIndex != nil {
			source = s
		}
		if s.Operation != nil && s.Operation.Step == workloadtypes.UpdateStepGangSurgeTarget {
			marker = s
		}
	}
	return source != nil && source.Phase == workloadtypes.InstancePhaseFailed &&
		marker != nil && len(h.badPods()) > 0
}

// ---------------------------------------------------------------------------
// Roll-back: corrective target == the exact prior revision — zero
// revision distance, so the update trigger never fires and recovery is
// owned by the wreckage-cleanup path.
// ---------------------------------------------------------------------------

// TestCorrectiveRecovery_SinglePod_RollBack reproduces the undo path
// from the settled at-Held state: the operator reverts the spec so the
// target revision equals the instance's RunningRevision. Stock
// Deployments recover here for free; OMENative must too.
// The update trigger stays revision-diff-keyed and correctly declines
// (pinned below) — recovery arrives via Plan's wreckage scan
// (UpdateItem.CleanupOnly -> ops.CleanupWreckage), which deletes the
// dead surge pod without ever touching the serving source: the
// original v1 pod stays in rotation through the whole undo.
func TestCorrectiveRecovery_SinglePod_RollBack(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (single-pod)")

	// The healthy v1 pod that must keep serving through the undo.
	serving := h.servingPods()
	if len(serving) != 1 || serving[0].Spec.Containers[0].Image != goodImage {
		t.Fatalf("precondition: exactly the good v1 pod serving at Held, got %d serving", len(serving))
	}
	originalPod := serving[0].Name

	// Pin the trigger contract: the pure evaluation declines
	// (RunningRevision == target fast-path) — cleanup is Plan's wreckage
	// scan, not a widened trigger.
	h.setTarget(h.revV1, goodImage)
	in := h.buildInput()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, h.desired, in.ObservedState)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	dec := workloadops.EvaluateUpdateTrigger(in, plan.Instances[0], h.target, nil)
	if dec.Trigger || dec.AdoptRevision || dec.RetryAfter != 0 {
		t.Fatalf("expected the update trigger to keep declining (RunningRevision==target fast-path), got %+v", dec)
	}

	converged := h.runWithInvariant(60,
		func() bool { return h.converged(h.revV1.Name) },
		func() {
			for _, p := range h.servingPods() {
				if p.Name != originalPod {
					t.Fatalf("pod %s entered rotation during the undo; only the original %s may serve", p.Name, originalPod)
				}
			}
			if len(h.servingPods()) == 0 {
				t.Fatalf("the original pod left rotation during the undo (capacity outage)")
			}
		})
	if !converged {
		h.dumpState("roll-back wedged (single-pod)")
		t.Fatalf("single-pod roll-back after Held did not converge")
	}
	if got := h.servingPods(); len(got) != 1 || got[0].Name != originalPod {
		t.Fatalf("the original pod must still be the serving pod after the undo, got %v", got)
	}
}

// TestCorrectiveRecovery_Gang_RollBack_MidWreckage is the gang variant
// of the undo path at the dirty timing: the roll-back lands while the
// source is Failed-with-Operation toward the bad revision, the
// GangSurgeTarget marker is present, and the crashed replacement gang's
// pods are live. Convergence must be automatic: either the
// Failed continuation's abandon tears the wreckage down, or — once the
// source is re-adopted Ready and its operation cleared — the orphaned
// marker falls out of the plan's pinned set (marker-liveness invariant,
// plan.go updateSurgeInFlightIndices) and the scale-down pass reaps the
// dead gang.
func TestCorrectiveRecovery_Gang_RollBack_MidWreckage(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()

	h.setTarget(h.revBad, badImage)
	if !h.run(60, func() bool { return h.gangMidWreckage() }) {
		h.dumpState("exhaustion")
		t.Fatalf("never reached the mid-wreckage state")
	}
	h.dumpState("mid-wreckage (gang)")

	h.setTarget(h.revV1, goodImage)
	if !h.run(80, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("roll-back wedged (gang)")
		t.Fatalf("gang mid-wreckage roll-back did not converge")
	}
}

// TestCorrectiveRecovery_Gang_RollBack_PostHeld: the gang roll-back from
// the SETTLED at-Held state needs no work: the final abandon left the
// source Ready on v1 with no wreckage, so a target equal to v1 is already
// met. The dirty timing is covered by
// TestCorrectiveRecovery_Gang_RollBack_MidWreckage.
func TestCorrectiveRecovery_Gang_RollBack_PostHeld(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (gang)")

	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("post-Held roll-back")
		t.Fatalf("gang roll-back from the clean at-Held state must converge trivially")
	}
}

// A row parked at Failed holds no attempt, so most of what an operator
// can change mid-flight has no reader on it. These tests pin that: the
// row, its RetryBlock and the cluster come out of the pass exactly as
// they went in, whichever knob moved.

// failedRowTarget is the revision the parked row is held on.
const failedRowTarget = "llama-70b-engine-target01"

// failedRowFixture is a disposed fresh start: one Instance stamped
// Failed with no operation, held on its target revision by a Held
// RetryBlock, and no pods. Without the block the Create pass would
// re-materialize it, so the block is what keeps the row parked for the
// length of a pass.
func failedRowFixture(t *testing.T) (workloadtypes.ReconcileInput, workloadtypes.ComponentPlan, *appsv1.ControllerRevision, *testAtomicMutationStore, *[]string) {
	t.Helper()
	in := minimalInput(t)
	in.ObservedState.UpdateRevision = failedRowTarget
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseFailed, PodCount: 1},
	}
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{
		{TargetRevision: failedRowTarget, State: workloadtypes.RetryBlockHeld},
	}
	removed := recordRetryBlockRemovals(&in)
	store := installTestAtomicMutationStore(&in)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: failedRowTarget}}
	return in, minimalPlan(), target, store, removed
}

// runFailedRowPass reconciles the fixture once and asserts the row came
// out untouched: no status write, still Failed with no operation, no
// pod materialized, and its block intact.
func runFailedRowPass(t *testing.T, in workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan, target *appsv1.ControllerRevision, store *testAtomicMutationStore, removed *[]string) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: c}, in, plan, target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if store.writes != 0 {
		t.Errorf("status writes: got %d want 0", store.writes)
	}
	got := store.status(0)
	if got == nil || got.Phase != workloadtypes.InstancePhaseFailed || got.Operation != nil {
		t.Errorf("row: got %+v want Failed with no operation", got)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(in.Key.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("pods: got %d want none; the Held block denies the fresh start", len(pods.Items))
	}
	if len(*removed) != 0 {
		t.Errorf("RetryBlock removals: got %v want none; the row's block is for a live revision", *removed)
	}
}

// TestFailedRow_OperatorConfigChangesLeaveItParked: the requeue cadence,
// the gang schedule clamp and the migration audit caps are all consulted
// by work in flight — a wake-up to schedule, a PodGroup to build, a
// migration to admit. A Failed row has none of those, so changing any of
// them reaches it with no reader.
func TestFailedRow_OperatorConfigChangesLeaveItParked(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*workloadtypes.ReconcileInput, *workloadtypes.ComponentPlan)
	}{
		{
			name: "requeue cadence",
			change: func(in *workloadtypes.ReconcileInput, _ *workloadtypes.ComponentPlan) {
				in.Requeue = workloadtypes.RequeueIntervals{Operation: 90 * time.Second, Gate: 45 * time.Second}
			},
		},
		{
			name: "gang schedule clamp",
			change: func(_ *workloadtypes.ReconcileInput, plan *workloadtypes.ComponentPlan) {
				plan.GangScheduleTimeout = &workloadtypes.GangScheduleTimeoutClamp{Min: time.Minute, Max: 10 * time.Minute}
			},
		},
		{
			name: "migration audit caps",
			change: func(in *workloadtypes.ReconcileInput, _ *workloadtypes.ComponentPlan) {
				in.MigrationAudit = &workloadtypes.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 2, Window: time.Hour}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, plan, target, store, removed := failedRowFixture(t)
			tc.change(&in, &plan)
			runFailedRowPass(t, in, plan, target, store, removed)
		})
	}
}

// TestFailedRow_ExcludedAnnotationEditDoesNotRedriveIt: an edit to an
// annotation the revision payload filters out leaves the hash where it
// was, so the row's target does not move and the pass has nothing new to
// act on. The hash check is half the claim — without it, a pass that
// wrote nothing would only mean the edit never reached the renderer.
func TestFailedRow_ExcludedAnnotationEditDoesNotRedriveIt(t *testing.T) {
	in, plan, target, store, removed := failedRowFixture(t)
	before := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"}}
	after := &metav1.ObjectMeta{Annotations: map[string]string{
		"ome.io/base-model-name":                   "llama-7b",
		constants.ReleaseHeldRevisionAnnotationKey: failedRowTarget,
	}}
	hBefore, _, err := revision.Hash(in.DesiredSpec.PodSpec, before, nil, "")
	if err != nil {
		t.Fatalf("hash before the edit: %v", err)
	}
	hAfter, _, err := revision.Hash(in.DesiredSpec.PodSpec, after, nil, "")
	if err != nil {
		t.Fatalf("hash after the edit: %v", err)
	}
	if hBefore != hAfter {
		t.Fatalf("an excluded annotation moved the hash: before=%s after=%s", hBefore, hAfter)
	}

	in.DesiredSpec.PodTemplateObjectMeta = after
	runFailedRowPass(t, in, plan, target, store, removed)
}

// TestFailedRow_SupersedePruneKeepsTheBlockItWaitsOn: the end-of-pass
// prune is what bounds persisted RetryBlock history, and it is scoped to
// revisions nothing targets anymore. However much history it drops, the
// block holding this row — which is for a live revision — is never a
// candidate, and the row itself is not written either way.
func TestFailedRow_SupersedePruneKeepsTheBlockItWaitsOn(t *testing.T) {
	in, plan, target, store, removed := failedRowFixture(t)
	in.ObservedState.RetryBlocks = append(in.ObservedState.RetryBlocks,
		workloadtypes.RetryBlock{TargetRevision: "llama-70b-engine-stale001", State: workloadtypes.RetryBlockHeld},
		workloadtypes.RetryBlock{TargetRevision: "llama-70b-engine-stale002", State: workloadtypes.RetryBlockBackoff},
	)

	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: c}, in, plan, target); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	want := []string{"llama-70b-engine-stale001", "llama-70b-engine-stale002"}
	if len(*removed) != len(want) {
		t.Fatalf("pruned: got %v want %v", *removed, want)
	}
	for i, rev := range want {
		if (*removed)[i] != rev {
			t.Fatalf("pruned: got %v want %v", *removed, want)
		}
	}
	if store.writes != 0 {
		t.Errorf("status writes: got %d want 0", store.writes)
	}
	if got := store.status(0); got == nil || got.Phase != workloadtypes.InstancePhaseFailed || got.Operation != nil {
		t.Errorf("row: got %+v want Failed with no operation", got)
	}
}

type staleInPlacePodClient struct {
	client.Client
	stale bool
	pods  []*corev1.Pod
}

func (c *staleInPlacePodClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if !c.stale {
		return c.Client.List(ctx, list, opts...)
	}
	pods, ok := list.(*corev1.PodList)
	if !ok {
		return c.Client.List(ctx, list, opts...)
	}
	pods.Items = make([]corev1.Pod, 0, len(c.pods))
	for _, pod := range c.pods {
		pods.Items = append(pods.Items, *pod.DeepCopy())
	}
	return nil
}

// crashLoopRepairRevision is the revision the wedged pods below carry;
// its hash matches the label enginePod stamps.
const crashLoopRepairRevision = "llama-70b-engine-priorrev"

// Teardown-mode dispatcher contract (owner deletion): the planned index
// set is treated as empty so EVERY observed Instance runs the ordinary
// Delete pipeline, and nothing else runs — no Paused gate, no
// Restart / Migrate / Update / Create. The caller owns completion
// detection and finalizer decisions, so the dispatcher only reports
// "deletes still in flight" (the configured poll interval) or "all observed
// Instances resolved" (zero result).

// TestReconcile_Teardown_DeletesEveryObservedInstance drives teardown
// against three observed Instances (with live pods) while the plan
// covers only index 0. All three indices — INCLUDING the planned one —
// must be Delete-dispatched, no pod may be created, and neither the
// migration detector nor the update pass may run.
func TestReconcile_Teardown_DeletesEveryObservedInstance(t *testing.T) {
	scheme := makeScheme(t)
	in := minimalInput(t)
	objs := []client.Object{
		enginePod("llama-70b", "prod", 0),
		enginePod("llama-70b", "prod", 1),
		enginePod("llama-70b", "prod", 2),
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	store := installTestAtomicMutationStore(&in)
	// A non-terminal Manual record that would be dispatched on a normal
	// reconcile; teardown must never reach the migration pass.
	in.ObservedState.Migrations = []workloadtypes.MigrationRecord{{
		RequestUUID: "teardown-mig", Trigger: workloadtypes.MigrationTriggerManual,
		Phase: workloadtypes.MigrationPhaseAccepted, SourceInstance: 0,
	}}
	migrationMutated := false
	in.MutateMigration = func(_ context.Context, _ string, _ func(*workloadtypes.MigrationRecord) bool) error {
		migrationMutated = true
		return nil
	}

	plan := minimalPlan() // covers only index 0
	plan.MigrationMode = workloadtypes.MigrationModeAuto
	// A non-nil target would arm the Update pass on a normal reconcile;
	// teardown must never reach it.
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	result, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("teardown admission must requeue immediately, got %+v", result)
	}
	for _, idx := range []int32{0, 1, 2} {
		status := store.status(idx)
		if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting || status.Operation == nil || status.Operation.Type != workloadtypes.InstanceOperationDelete {
			t.Errorf("instance %d was not durably admitted for teardown: %+v", idx, status)
		}
	}
	if migrationMutated {
		t.Errorf("teardown must not run the migration pass")
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 3 {
		t.Fatalf("admission pass must have zero Pod effects; got %d Pods", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("effect pass: %v", err)
	}
	if result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("teardown effect pass must use delete cadence, got %+v", result)
	}
	pods = &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods after effect pass: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("teardown must delete every Pod and create none; %d remain", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		t.Fatalf("completion pass: %v", err)
	}
	if !result.Requeue || len(store.statuses) != 0 {
		t.Fatalf("teardown completion result/statuses = %+v/%+v", result, store.statuses)
	}
}

// TestReconcile_Teardown_DeletesMigratingPair pins the mid-migration
// teardown contract: a Phase=Migrating source AND its
// Operation.Type=Migrate surge — both excluded from the NORMAL
// scale-down pass so Migrate isn't ripped apart — must each get a
// Delete dispatch under Teardown. Excluding them would leave their
// pods with no Delete op, no drain, no force-delete escalation: the
// teardown wedges forever (strict hold) or falls to un-drained GC at
// the deadline.
func TestReconcile_Teardown_DeletesMigratingPair(t *testing.T) {
	scheme := makeScheme(t)
	objs := []client.Object{
		enginePod("llama-70b", "prod", 0), // migration source
		enginePod("llama-70b", "prod", 7), // surge
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseMigrating},
		{Index: 7, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{Type: workloadtypes.InstanceOperationMigrate}},
	}
	store := installTestAtomicMutationStore(&in)

	plan := minimalPlan() // covers only index 0
	plan.MigrationMode = workloadtypes.MigrationModeAuto

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("mid-migration teardown admission must requeue immediately, got %+v", result)
	}
	for _, idx := range []int32{0, 7} {
		status := store.status(idx)
		if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting || status.Operation == nil || status.Operation.Type != workloadtypes.InstanceOperationDelete {
			t.Errorf("mid-migration instance %d was not durably admitted: %+v", idx, status)
		}
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("admission pass must leave the migration pair untouched; %d Pods remain", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("effect pass: %v", err)
	}
	if result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("mid-migration teardown effect result = %+v", result)
	}
	pods = &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods after effect pass: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("teardown must delete the source and surge Pods; %d remain", len(pods.Items))
	}
}

// TestReconcile_Teardown_NoInstancesNoPods_CleanReturn pins the empty
// case: nothing observed, nothing live → immediate zero result, no
// callback fires. The caller's completion check owns the rest.
func TestReconcile_Teardown_NoInstancesNoPods_CleanReturn(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	mutations, removals := 0, 0
	in.MutateInstance = func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
		mutations++
		return nil
	}
	in.RemoveInstance = func(_ context.Context, _ int32) (bool, error) {
		removals++
		return false, nil
	}

	result, err := workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("empty teardown must return a zero result, got %+v", result)
	}
	if mutations != 0 || removals != 0 {
		t.Errorf("empty teardown must fire no status callbacks; mutations=%d removals=%d", mutations, removals)
	}
}

// TestReconcile_Teardown_PodlessInstances_RemovedAndDone pins the
// converged shape: observed Instances whose Pods are already gone still use a
// durable admission pass, a batched removal pass, and a final verification.
func TestReconcile_Teardown_PodlessInstances_RemovedAndDone(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workloadtypes.InstancePhaseDeleting},
	}
	store := installTestAtomicMutationStore(&in)

	result, err := workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("admission pass: %v", err)
	}
	if !result.Requeue || len(store.statuses) != 2 {
		t.Fatalf("podless admission result/statuses = %+v/%+v", result, store.statuses)
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("completion pass: %v", err)
	}
	if !result.Requeue || len(store.statuses) != 0 {
		t.Fatalf("podless completion result/statuses = %+v/%+v", result, store.statuses)
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("verification pass: %v", err)
	}
	if !result.IsZero() {
		t.Errorf("fully resolved teardown must return zero, got %+v", result)
	}
}

// TestReconcile_Teardown_IgnoresPaused pins that teardown short-circuits
// ahead of the Paused gate: an operator pause must not hold a deletion's
// Delete dispatch (deletion is the stronger intent).
func TestReconcile_Teardown_IgnoresPaused(t *testing.T) {
	scheme := makeScheme(t)
	pod := enginePod("llama-70b", "prod", 0)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	deps := workloadtypes.Deps{Client: c, Expectations: workloadtypes.NewExpectations()}

	in := minimalInput(t)
	in.Teardown = true
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	store := installTestAtomicMutationStore(&in)
	plan := minimalPlan()
	plan.Paused = true

	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("admission pass: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("paused teardown admission must requeue immediately, got %+v", result)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("admission pass must leave the Pod untouched; %d remain", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("effect pass: %v", err)
	}
	if result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("paused teardown effect pass must use delete cadence, got %+v", result)
	}
	pods = &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatalf("list pods after effect pass: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("paused teardown must still delete the Pod; %d remain", len(pods.Items))
	}
}

// Terminal-pod recovery: a pod the kubelet rejects at admission (phase
// Failed, no container ever started) still occupies its stable name. The
// owning operation must delete it and recreate the target within the same
// attempt, paced by the update retry ladder and bounded by the attempt's
// deadline; a gang recycles as a whole.
//
// The harness is the closed-loop recoveryHarness with a kubelet that
// rejects admissions on demand: fake client + fake clock + IR-backed
// InstanceStatus round-trip + in-memory RetryBlocks.

const (
	admissionRejectReason = "UnexpectedAdmissionError"
	// admissionStep is the loop's clock advance per pass — the op requeue
	// interval, so pacing can be observed at that resolution.
	admissionStep = 5 * time.Second
)

// admission records one pod object the kubelet saw for the first time.
type admission struct {
	name        string
	incarnation string
	at          time.Time
	rejected    bool
	// op is the Instance's operation that issued the pod (nil when none).
	op *workloadtypes.InstanceOperation
}

type admissionHarness struct {
	t    *testing.T
	ctx  context.Context
	c    client.Client
	isvc *v1beta1.InferenceService
	clk  *clocktesting.FakeClock

	desired workloadtypes.WorkloadDesiredSpec
	target  *appsv1.ControllerRevision
	policy  *workloadtypes.RetryPolicy

	// reject decides whether the kubelet rejects the n-th pod object
	// (1-based) it sees under a name.
	reject func(name string, n int) bool
	seen   map[string]int

	admissions      []admission
	currentRevision string
	blocks          []workloadtypes.RetryBlock
	phases          map[workloadtypes.InstancePhase]int
	opTypes         map[workloadtypes.InstanceOperationType]int
}

func newAdmissionHarness(t *testing.T, multiPod bool, reject func(name string, n int) bool) *admissionHarness {
	t.Helper()
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: recoveryOwner, Namespace: recoveryNS, UID: "uid-1",
	}}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc).Build()
	h := &admissionHarness{
		t:    t,
		ctx:  context.Background(),
		c:    c,
		isvc: isvc,
		// Status timestamps round-trip at second precision.
		clk:     clocktesting.NewFakeClock(time.Now().Truncate(time.Second)),
		policy:  &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2},
		reject:  reject,
		seen:    map[string]int{},
		phases:  map[workloadtypes.InstancePhase]int{},
		opTypes: map[workloadtypes.InstanceOperationType]int{},
	}
	spec := recoveryPodSpec(goodImage)
	var workerSpec *corev1.PodSpec
	if multiPod {
		workerSpec = spec.DeepCopy()
	}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		h.ctx, h.c, h.c, h.isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		revision.Key{Namespace: recoveryNS, Name: recoveryOwner + "-" + string(workloadtypes.ComponentEngine)},
		spec, workerSpec, nil, nil, h.isvc.UID,
	)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	h.target = cr
	// BuildPlan carries only the per-resource readiness window — the operator
	// ConfigMap fallback is the adapter's job — so the harness sets one.
	h.desired = workloadtypes.WorkloadDesiredSpec{
		Replicas: 1,
		PodSpec:  spec,
		Lifecycle: workloadtypes.Lifecycle{
			InstanceReadyTimeout: &metav1.Duration{Duration: 30 * time.Minute},
		},
	}
	if multiPod {
		h.desired.MultiPod = true
		h.desired.WorkerPodSpec = workerSpec
		h.desired.Runners = []workloadtypes.Runner{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	} else {
		h.desired.Runners = []workloadtypes.Runner{{Name: "default", Size: 1}}
	}
	return h
}

func (h *admissionHarness) irKey() types.NamespacedName {
	return types.NamespacedName{Namespace: recoveryNS, Name: recoveryOwner + "-" + string(workloadtypes.ComponentEngine)}
}

func (h *admissionHarness) irStatuses() []workloadtypes.InstanceStatus {
	h.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := h.c.Get(h.ctx, h.irKey(), ir); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		h.t.Fatalf("get IR: %v", err)
	}
	out := make([]workloadtypes.InstanceStatus, 0, len(ir.Status.InstanceStatuses))
	for _, s := range ir.Status.InstanceStatuses {
		out = append(out, v1beta1convert.InstanceStatusToWorkload(s))
	}
	return out
}

// status returns the single Instance's status, or nil before it exists.
func (h *admissionHarness) status() *workloadtypes.InstanceStatus {
	for _, s := range h.irStatuses() {
		if s.Index == 0 {
			return &s
		}
	}
	return nil
}

func (h *admissionHarness) removeInstance() func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		ir := &v1beta1.InferenceReplica{}
		if err := h.c.Get(ctx, h.irKey(), ir); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index != idx {
				continue
			}
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses[:i], ir.Status.InstanceStatuses[i+1:]...)
			return true, h.c.Status().Update(ctx, ir)
		}
		return false, nil
	}
}

func (h *admissionHarness) buildInput() workloadtypes.ReconcileInput {
	in := workloadtypes.ReconcileInput{
		OwnerObject: h.isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: h.isvc,
		Key: workloadtypes.Key{
			Namespace: recoveryNS,
			Component: workloadtypes.ComponentEngine,
			OwnerName: recoveryOwner,
		},
		DesiredSpec: h.desired,
		ObservedState: workloadtypes.WorkloadObservedState{
			InstanceStatuses: h.irStatuses(),
			RetryBlocks:      append([]workloadtypes.RetryBlock(nil), h.blocks...),
			CurrentRevision:  h.currentRevision,
			UpdateRevision:   h.target.Name,
		},
		MutateInstance:    roundTripMutateInstance(h.c, h.isvc, workloadtypes.ComponentEngine),
		RemoveInstance:    h.removeInstance(),
		UpdateRetryPolicy: h.policy,
		StuckPodGrace:     30 * time.Second,
		Clock:             h.clk,
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
			pos := -1
			b := workloadtypes.RetryBlock{TargetRevision: rev}
			for i := range h.blocks {
				if h.blocks[i].TargetRevision == rev {
					pos, b = i, h.blocks[i]
					break
				}
			}
			switch mutate(&b) {
			case workloadtypes.RetryBlockPersist:
				if pos == -1 {
					h.blocks = append(h.blocks, b)
				} else {
					h.blocks[pos] = b
				}
			case workloadtypes.RetryBlockRemove:
				if pos != -1 {
					h.blocks = append(h.blocks[:pos], h.blocks[pos+1:]...)
				}
			}
			return nil
		},
	}
	stubInputCallbacks(&in)
	return in
}

// kubelet decides admission for every pod object it sees for the first
// time: a rejected pod parks in phase Failed with no container started; an
// admitted pod runs, becomes ContainersReady, and PodReady once the
// controller's serving gate is True. Terminal pods are never touched again.
func (h *admissionHarness) kubelet() {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("kubelet list: %v", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || query.IsTerminalPod(pod) {
			continue
		}
		if pod.Status.Phase == "" {
			pod.CreationTimestamp = metav1.NewTime(h.clk.Now())
			if err := h.c.Update(h.ctx, pod); err != nil {
				h.t.Fatalf("kubelet stamp creation %s: %v", pod.Name, err)
			}
			h.seen[pod.Name]++
			rejected := h.reject != nil && h.reject(pod.Name, h.seen[pod.Name])
			var op *workloadtypes.InstanceOperation
			if s := h.status(); s != nil {
				op = s.Operation
			}
			h.admissions = append(h.admissions, admission{
				name:        pod.Name,
				incarnation: pod.Labels[query.LabelInstanceIncarnation],
				at:          h.clk.Now(),
				rejected:    rejected,
				op:          op,
			})
			if rejected {
				pod.Status.Phase = corev1.PodFailed
				pod.Status.Reason = admissionRejectReason
				pod.Status.Message = "Pod was rejected: node resources unavailable"
				pod.Status.ContainerStatuses = nil
				setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
				setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
				if err := h.c.Status().Update(h.ctx, pod); err != nil {
					h.t.Fatalf("kubelet reject %s: %v", pod.Name, err)
				}
				continue
			}
		}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  pod.Spec.Containers[0].Name,
			Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, h.clk.Now())
		ready := corev1.ConditionFalse
		for _, cond := range pod.Status.Conditions {
			if cond.Type == query.ServingConditionType && cond.Status == corev1.ConditionTrue {
				ready = corev1.ConditionTrue
			}
		}
		setPodCondition(pod, corev1.PodReady, ready, h.clk.Now())
		if err := h.c.Status().Update(h.ctx, pod); err != nil {
			h.t.Fatalf("kubelet update %s: %v", pod.Name, err)
		}
	}
}

func (h *admissionHarness) pods() []corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	return pods.Items
}

// step runs one pass: kubelet, fresh expectations (watch caught up),
// Reconcile, bookkeeping, then the clock advances by the pass's requeue
// (at least admissionStep).
func (h *admissionHarness) step() {
	h.t.Helper()
	h.kubelet()
	deps := workloadtypes.Deps{Client: h.c, APIReader: h.c, Expectations: workloadtypes.NewExpectations(), Clock: h.clk}
	in := h.buildInput()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, h.desired, in.ObservedState)
	if err != nil {
		h.t.Fatalf("BuildPlan: %v", err)
	}
	res, err := workload.Reconcile(h.ctx, deps, in, plan, h.target)
	if err != nil {
		h.t.Fatalf("Reconcile: %v", err)
	}
	if status.RolloutComplete(h.irStatuses(), h.target.Name) {
		h.currentRevision = h.target.Name
	}
	h.observe()
	advance := admissionStep
	if res.RequeueAfter > advance {
		advance = res.RequeueAfter
	}
	h.clk.Step(advance)
}

// observe records the phases and operations seen after a pass and checks
// the per-pass invariants: a pod created this pass was issued by an
// attempt that has not passed its deadline, and a recycling operation
// (Create or Restart) never creates a replacement while a terminal pod of
// the same Instance is still standing. A SurgeThenDrain Update is exempt
// from the second check: its replacement legitimately runs at the other
// ordinal next to the pod it replaces.
func (h *admissionHarness) observe() {
	h.t.Helper()
	s := h.status()
	if s != nil {
		h.phases[s.Phase]++
		if s.Operation != nil {
			h.opTypes[s.Operation.Type]++
		}
	}
	fresh, terminal := 0, 0
	pods := h.pods()
	for i := range pods {
		switch {
		case query.IsTerminalPod(&pods[i]):
			terminal++
		case pods[i].Status.Phase == "":
			fresh++
		}
	}
	if fresh > 0 {
		if s == nil || s.Operation == nil {
			h.t.Fatalf("a pod was created with no operation owning the Instance: %+v", s)
		}
		if h.clk.Now().After(s.Operation.Deadline.Time) {
			h.t.Fatalf("a pod was created at %v, after the attempt's deadline %v", h.clk.Now(), s.Operation.Deadline.Time)
		}
	}
	recycling := s != nil && s.Operation != nil &&
		(s.Operation.Type == workloadtypes.InstanceOperationCreate || s.Operation.Type == workloadtypes.InstanceOperationRestart)
	if recycling && fresh > 0 && terminal > 0 {
		h.dump("mixed fresh and terminal pods")
		h.t.Fatalf("%s created a replacement next to %d terminal pod(s) of the same Instance", s.Operation.Type, terminal)
	}
}

func (h *admissionHarness) run(max int, pred func() bool) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		if pred() {
			return true
		}
		h.step()
	}
	return pred()
}

// converged reports Phase=Ready on the target with no operation and every
// pod live and ContainersReady.
func (h *admissionHarness) converged() bool {
	s := h.status()
	if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.RunningRevision != h.target.Name || s.Operation != nil {
		return false
	}
	pods := h.pods()
	if len(pods) == 0 {
		return false
	}
	for i := range pods {
		if query.IsTerminalPod(&pods[i]) || status.CountReadyPods([]*corev1.Pod{&pods[i]}) == 0 {
			return false
		}
	}
	return true
}

func (h *admissionHarness) dump(label string) {
	h.t.Logf("--- %s at %v ---", label, h.clk.Now())
	if s := h.status(); s != nil {
		op := "nil"
		if s.Operation != nil {
			op = fmt.Sprintf("{type=%s step=%s retries=%d deadline=%v}", s.Operation.Type, s.Operation.Step, s.Operation.RetryCount, s.Operation.Deadline.Time)
		}
		h.t.Logf("  instance %d: phase=%s incarnation=%d op=%s", s.Index, s.Phase, s.Incarnation, op)
	}
	for _, p := range h.pods() {
		h.t.Logf("  pod %s phase=%s incarnation=%s", p.Name, p.Status.Phase, p.Labels[query.LabelInstanceIncarnation])
	}
	for _, a := range h.admissions {
		id := "-"
		if a.op != nil {
			id = fmt.Sprintf("%s#%d", a.op.ID, a.op.RetryCount)
		}
		h.t.Logf("  admission %s at %v rejected=%v op=%s", a.name, a.at, a.rejected, id)
	}
}

// (a) A single-pod Instance whose pod dies at admission is repaired within
// the same Create attempt: the dead pod is deleted, the target recreated,
// and the Instance reaches Ready when the replacement does.
func TestTerminalPodRecovery_SinglePod_RecycledWithinAttempt(t *testing.T) {
	h := newAdmissionHarness(t, false, func(_ string, n int) bool { return n == 1 })
	if !h.run(40, h.converged) {
		h.dump("single-pod recycle wedged")
		t.Fatalf("Instance never reached Ready after its pod died at admission")
	}
	if len(h.admissions) != 2 || !h.admissions[0].rejected || h.admissions[1].rejected || h.admissions[0].name != h.admissions[1].name {
		h.dump("admissions")
		t.Fatalf("expected one rejected admission followed by one admitted replacement of the same name, got %d admissions", len(h.admissions))
	}
	first, second := h.admissions[0].op, h.admissions[1].op
	if first == nil || second == nil || first.ID != second.ID || first.Type != workloadtypes.InstanceOperationCreate {
		h.dump("attempts")
		t.Fatalf("the replacement must be issued by the original Create attempt, got %+v then %+v", first, second)
	}
	if second.RetryCount != 1 {
		t.Fatalf("the replacement must follow one recorded recycle, got RetryCount=%d", second.RetryCount)
	}
	if h.phases[workloadtypes.InstancePhaseFailed] > 0 {
		t.Fatalf("the attempt must not escalate to Failed while it is repairing itself")
	}
	if s := h.status(); s.LastFailure == nil || s.LastFailure.Reason != admissionRejectReason {
		t.Fatalf("LastFailure must preserve the admission failure, got %+v", s.LastFailure)
	}
}

// (b) A gang whose every member dies at admission is deleted and recreated
// together, by Create at the same incarnation, and reaches Ready.
func TestTerminalPodRecovery_Gang_AllMembersRecycledTogether(t *testing.T) {
	h := newAdmissionHarness(t, true, func(_ string, n int) bool { return n == 1 })
	if !h.run(40, h.converged) {
		h.dump("gang recycle wedged")
		t.Fatalf("gang never reached Ready after both members died at admission")
	}
	if len(h.admissions) != 4 {
		h.dump("admissions")
		t.Fatalf("expected two rejected admissions and two admitted replacements, got %d", len(h.admissions))
	}
	if !h.admissions[2].at.Equal(h.admissions[3].at) {
		t.Fatalf("replacements must be created together, got %v and %v", h.admissions[2].at, h.admissions[3].at)
	}
	if h.opTypes[workloadtypes.InstanceOperationRestart] > 0 {
		t.Fatalf("total loss is Create's to rebuild; no Restart may run")
	}
	if s := h.status(); s.Incarnation != 1 {
		t.Fatalf("a whole-gang recycle by Create keeps the incarnation, got %d", s.Incarnation)
	}
	for _, p := range h.pods() {
		if p.Labels[query.LabelInstanceIncarnation] != "1" {
			t.Fatalf("pod %s carries incarnation %q, want 1", p.Name, p.Labels[query.LabelInstanceIncarnation])
		}
	}
}

// (d) Repeated rejections space recreates by the retry ladder within one
// attempt, and no recreate outlives the attempt's deadline; the expired
// attempt is escalated and disposed, and only a different operation may
// touch the Instance afterwards.
func TestTerminalPodRecovery_RecreatesFollowLadderWithinDeadline(t *testing.T) {
	h := newAdmissionHarness(t, false, func(string, int) bool { return true })
	h.desired.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 4 * time.Minute}
	for i := 0; i < 60; i++ {
		h.step()
	}

	// Group admissions by attempt, in order of first appearance.
	var attempts [][]admission
	byID := map[string]int{}
	for _, a := range h.admissions {
		if a.op == nil {
			h.dump("admission without an attempt")
			t.Fatalf("admission of %s at %v was not issued by an attempt", a.name, a.at)
		}
		pos, ok := byID[a.op.ID]
		if !ok {
			pos = len(attempts)
			byID[a.op.ID] = pos
			attempts = append(attempts, nil)
		}
		attempts[pos] = append(attempts[pos], a)
	}
	if len(attempts) < 2 {
		h.dump("attempts")
		t.Fatalf("expected the expired attempt to be disposed and a fresh one started, got %d attempt(s)", len(attempts))
	}
	first := attempts[0]
	if len(first) < 4 {
		h.dump("first attempt")
		t.Fatalf("expected several paced recreates within the first attempt, got %d admission(s)", len(first))
	}
	for i := range first {
		if a := first[i]; a.at.After(a.op.Deadline.Time) {
			t.Fatalf("recreate %d at %v outlived the attempt's deadline %v", i, a.at, a.op.Deadline.Time)
		}
		if first[i].op.RetryCount != int32(i) {
			t.Fatalf("recreate %d must follow %d recorded recycle(s), got RetryCount=%d", i, i, first[i].op.RetryCount)
		}
	}
	// The first recycle is immediate; each later one waits the ladder delay
	// measured from the previous recycle.
	if gap := first[1].at.Sub(first[0].at); gap > 2*admissionStep {
		t.Fatalf("first recycle must be immediate, got %v", gap)
	}
	for i := 1; i+1 < len(first); i++ {
		want := h.policy.NextRetryDelay(int32(i))
		gap := first[i+1].at.Sub(first[i].at)
		if gap < want || gap > want+admissionStep {
			h.dump("ladder")
			t.Fatalf("recreate %d -> %d spaced %v, want the ladder delay %v", i, i+1, gap, want)
		}
	}
	if h.phases[workloadtypes.InstancePhaseFailed] == 0 {
		t.Fatalf("the expired attempt must escalate to Failed at its deadline")
	}
	if second := attempts[1]; !second[0].at.After(first[0].op.Deadline.Time) {
		t.Fatalf("the fresh attempt's first recreate at %v must follow the expired attempt's deadline %v", second[0].at, first[0].op.Deadline.Time)
	}
}

// admissionsByIncarnation counts admitted-or-rejected pod objects per
// incarnation label.
func (h *admissionHarness) admissionsByIncarnation() map[string]int {
	out := map[string]int{}
	for _, a := range h.admissions {
		out[a.incarnation]++
	}
	return out
}

// requireNoGapFill fails when a pod was created at an incarnation older
// than one already seen: a gang gap filled next to survivors instead of a
// whole-gang rebuild at a bumped incarnation.
func (h *admissionHarness) requireNoGapFill() {
	h.t.Helper()
	newest := int64(0)
	for _, a := range h.admissions {
		inc, err := strconv.ParseInt(a.incarnation, 10, 64)
		if err != nil {
			h.t.Fatalf("pod %s carries an unreadable incarnation label %q: %v", a.name, a.incarnation, err)
		}
		if inc < newest {
			h.dump("gap fill")
			h.t.Fatalf("pod %s was created at incarnation %d after incarnation %d existed: a gap fill next to survivors", a.name, inc, newest)
		}
		newest = inc
	}
}

// (c) A gang with one dead member and one live member is rebuilt as a
// whole by Restart: incarnation bump, the survivor drained and deleted,
// both members recreated together — never a single-member gap fill next
// to the survivor.
func TestTerminalPodRecovery_Gang_SurvivorRebuiltAsWhole(t *testing.T) {
	leader := query.PodName(recoveryOwner, workloadtypes.ComponentEngine, 0, "leader", 0)
	worker := query.PodName(recoveryOwner, workloadtypes.ComponentEngine, 0, "worker", 0)
	h := newAdmissionHarness(t, true, func(name string, n int) bool { return name == leader && n == 1 })
	if !h.run(40, h.converged) {
		h.dump("gang with survivor wedged")
		t.Fatalf("gang never reached Ready after one member died at admission")
	}
	if h.opTypes[workloadtypes.InstanceOperationRestart] == 0 {
		t.Fatalf("a gang with a survivor must be rebuilt by Restart, not gap-filled by Create")
	}
	if s := h.status(); s.Incarnation != 2 {
		t.Fatalf("the rebuild must bump the incarnation, got %d", s.Incarnation)
	}
	h.requireNoGapFill()
	if byInc := h.admissionsByIncarnation(); byInc["1"] != 2 || byInc["2"] != 2 || len(h.admissions) != 4 {
		h.dump("admissions")
		t.Fatalf("expected the initial pair at incarnation 1 and one whole-gang recreate at incarnation 2, got %v", byInc)
	}
	seenWorker := 0
	for _, a := range h.admissions {
		if a.name == worker {
			seenWorker++
		}
	}
	if seenWorker != 2 {
		t.Fatalf("the surviving worker must be drained and recreated with the gang, got %d worker admissions", seenWorker)
	}
	for _, p := range h.pods() {
		if p.Labels[query.LabelInstanceIncarnation] != "2" {
			t.Fatalf("pod %s still runs at incarnation %q after the rebuild", p.Name, p.Labels[query.LabelInstanceIncarnation])
		}
	}
}

// (c, spent attempt) When the dead member keeps dying, the Restart attempt
// expires to Failed with its operation preserved; that spent attempt must
// re-arm into a new whole-gang Restart (another incarnation bump, survivor
// drained again) instead of wedging or letting Create gap-fill.
func TestTerminalPodRecovery_Gang_SpentRestartReArms(t *testing.T) {
	leader := query.PodName(recoveryOwner, workloadtypes.ComponentEngine, 0, "leader", 0)
	worker := query.PodName(recoveryOwner, workloadtypes.ComponentEngine, 0, "worker", 0)
	h := newAdmissionHarness(t, true, func(name string, n int) bool { return name == leader && n <= 5 })
	h.desired.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 2 * time.Minute}
	if !h.run(80, h.converged) {
		h.dump("spent restart wedged")
		t.Fatalf("gang never reached Ready after its Restart attempt expired")
	}
	if h.phases[workloadtypes.InstancePhaseFailed] == 0 {
		t.Fatalf("the first Restart attempt must expire to Failed before re-arming")
	}
	s := h.status()
	if s.Incarnation < 3 {
		t.Fatalf("the re-armed Restart must bump the incarnation again, got %d", s.Incarnation)
	}
	h.requireNoGapFill()
	seenWorker := 0
	for _, a := range h.admissions {
		if a.name == worker {
			seenWorker++
		}
	}
	if seenWorker != int(s.Incarnation) {
		h.dump("worker admissions")
		t.Fatalf("every rebuild must drain and recreate the survivor: %d worker admissions across %d incarnations", seenWorker, s.Incarnation)
	}
}

// Dispatcher-level contracts for the migration expiry pass: it runs
// BEFORE the drive pass (an expired record is consumed, never driven),
// it runs regardless of MigrationMode (a mode flip to Never cannot
// strand a non-terminal record), and the post-expiry unpinned surge is
// an ordinary scale-down extra. The full expiry transition (pair
// unpinning, source restore from observation, surge teardown, retry
// acceptance) is covered by the workload/ops expiry tests.

// expiryDispatchFixture wires a Reconcile input holding one Manual
// record and observation stubs that record what the dispatcher did.
func expiryDispatchFixture(t *testing.T, rec workloadtypes.MigrationRecord) (workloadtypes.ReconcileInput, workloadtypes.Deps, *[]workloadtypes.MigrationRecord, *bool) {
	t.Helper()
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	clk := clocktesting.NewFakeClock(time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC))

	records := []workloadtypes.MigrationRecord{rec}
	migrateStamped := false

	in := minimalInput(t)
	in.Clock = clk
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady, RunningRevision: "rev-1"},
	}
	in.ObservedState.Migrations = append([]workloadtypes.MigrationRecord(nil), records...)
	in.MutateMigration = func(_ context.Context, uuid string, mutate func(*workloadtypes.MigrationRecord) bool) error {
		for i := range records {
			if records[i].RequestUUID == uuid {
				r := records[i]
				if mutate(&r) {
					records[i] = r
				}
				return nil
			}
		}
		return nil
	}
	in.MutateInstance = func(_ context.Context, _ int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		s := workloadtypes.InstanceStatus{Index: 0, Phase: workloadtypes.InstancePhaseReady}
		if mutate(&s) && s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationMigrate {
			migrateStamped = true
		}
		return nil
	}
	return in, workloadtypes.Deps{Client: c, Clock: clk}, &records, &migrateStamped
}

// pastDeadlineRecord builds a non-terminal Manual record whose Deadline
// already elapsed relative to the fixture clock.
func pastDeadlineRecord(uuid string) workloadtypes.MigrationRecord {
	base := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	return workloadtypes.MigrationRecord{
		RequestUUID:    uuid,
		Trigger:        workloadtypes.MigrationTriggerManual,
		Phase:          workloadtypes.MigrationPhaseAccepted,
		SourceInstance: 0,
		FromNode:       "node-a",
		StartedAt:      metav1.NewTime(base.Add(-time.Hour)),
		Deadline:       metav1.NewTime(base.Add(-30 * time.Minute)),
	}
}

// TestReconcile_ExpiryBeforeDrive pins the precedence rule: a record
// past its Deadline is consumed by the expiry pass BEFORE the drive
// pass can pick it — the record closes Failed, no Migrate op is ever
// stamped, and the dispatcher requeues immediately so the next pass
// rebuilds plan + ObservedState from the post-expiry status.
func TestReconcile_ExpiryBeforeDrive(t *testing.T) {
	in, deps, records, migrateStamped := expiryDispatchFixture(t, pastDeadlineRecord("u-expired"))
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{{TargetRevision: "superseded"}}
	prunes := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		prunes++
		return nil
	}
	plan := minimalPlan()
	plan.MigrationMode = workloadtypes.MigrationModeAuto

	res, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Requeue {
		t.Errorf("dispatcher must requeue immediately after an expiry; got %+v", res)
	}
	got := (*records)[0]
	if got.Phase != workloadtypes.MigrationPhaseFailed || got.CompletedAt == nil {
		t.Fatalf("expired record must close Failed; got %+v", got)
	}
	if *migrateStamped {
		t.Errorf("the drive pass must never stamp a Migrate op for an expired record")
	}
	if prunes != 1 {
		t.Errorf("non-scale-down immediate requeue pruned %d RetryBlocks, want 1", prunes)
	}
}

// TestReconcile_ExpiryRunsUnderModeNever pins that the expiry pass is
// NOT gated on MigrationMode: a mode flip to Never after a record was
// accepted must not strand the non-terminal record forever.
func TestReconcile_ExpiryRunsUnderModeNever(t *testing.T) {
	in, deps, records, migrateStamped := expiryDispatchFixture(t, pastDeadlineRecord("u-never-mode"))
	plan := minimalPlan()
	plan.MigrationMode = workloadtypes.MigrationModeNever

	res, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Requeue {
		t.Errorf("dispatcher must requeue after the expiry; got %+v", res)
	}
	if got := (*records)[0]; got.Phase != workloadtypes.MigrationPhaseFailed {
		t.Fatalf("record must expire under MigrationMode=Never too; got %+v", got)
	}
	if *migrateStamped {
		t.Errorf("no Migrate op may be stamped under MigrationMode=Never")
	}
}

// TestReconcile_NotYetExpiredRecordNotConsumed is the negative
// dispatcher control: a non-terminal record whose Deadline is still in
// the future is left alone by the expiry pass. MigrationMode=Never
// isolates the expiry pass (the drive pass — which owns a live record
// — is skipped, and would in this podless stub environment terminally
// reject it for unrelated reasons).
func TestReconcile_NotYetExpiredRecordNotConsumed(t *testing.T) {
	rec := pastDeadlineRecord("u-live")
	rec.Deadline = metav1.NewTime(time.Date(2026, 7, 24, 13, 0, 0, 0, time.UTC)) // 1h ahead of the fixture clock
	in, deps, records, _ := expiryDispatchFixture(t, rec)
	plan := minimalPlan()
	plan.MigrationMode = workloadtypes.MigrationModeNever

	if _, err := workload.Reconcile(context.Background(), deps, in, plan, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := (*records)[0]; got.Phase.Terminal() {
		t.Fatalf("record before its Deadline must not be expired; got %+v", got)
	}
}

// TestScaleDownExtras_UnpinnedSurgeIsExtra pins the teardown
// hand-off: once expiry clears the surge's Migrate op, the surviving
// Creating-phase status is an ordinary scale-down extra (contrast
// TestReconcile_MigrationOperationOwned_NotScaleDown, where the pinned
// op excludes it) — the standard Delete pipeline owns the teardown. The
// restored source stays covered by the plan and can never become an
// extra.
func TestScaleDownExtras_UnpinnedSurgeIsExtra(t *testing.T) {
	observed := []workloadtypes.InstanceStatus{
		{Index: 0, Phase: workloadtypes.InstancePhaseReady},    // restored source, in plan
		{Index: 1, Phase: workloadtypes.InstancePhaseCreating}, // unpinned surge, op cleared
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ScaleDownExtras(observed, plan)
	if len(extras) != 1 || extras[0] != 1 {
		t.Fatalf("unpinned surge must be the sole scale-down extra; got %v", extras)
	}
}

// The end-of-pass block (escalation, then the RetryBlock supersede-prune)
// is per-Instance and re-derives its own evidence, so it must survive an
// op pass that errored for one Instance. These tests drive
// workload.Execute with an op pass that fails and assert what the pass
// still does — and, just as importantly, what it must leave alone.

// rejectedStatusWrite is the 422 an apiserver returns for a status write
// it will never accept, whatever the pass retries.
func rejectedStatusWrite() error {
	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ome.io", Kind: "InferenceReplica"},
		"llama-70b-engine",
		field.ErrorList{field.Invalid(field.NewPath("status", "instances"), "", "rejected")},
	)
}

// restartOnlyDecision selects exactly one Instance for the restart pass
// and leaves the end-of-pass block enabled.
func restartOnlyDecision(inst workloadtypes.InstancePlan) workload.Decision {
	return workload.Decision{
		Actions: []workload.PlannedAction{{
			Kind:     workload.ActionRestart,
			Restarts: []workload.RestartSelection{{Instance: inst, Reason: "pod count 0 below desired 1"}},
		}},
		Escalate: true,
	}
}

// threeInstancePlan covers indices 0..2 with one pod each.
func threeInstancePlan() workloadtypes.ComponentPlan {
	plan := workloadtypes.ComponentPlan{Component: workloadtypes.ComponentEngine, Replicas: 3}
	for idx := int32(0); idx < 3; idx++ {
		plan.Instances = append(plan.Instances, workloadtypes.InstancePlan{
			Index: idx, Incarnation: 1,
			Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}},
		})
	}
	return plan
}

// TestExecuteErroredOpPassStillEscalatesExpiredDeadline: the restart pass
// fails on instance 1 (its status write is rejected 422 and the
// classifier has no attempt to dispose, so the error propagates), and the
// pass still escalates instance 0's elapsed operation deadline and prunes
// the superseded RetryBlock. Instance 2 is parked on a quota wait: the
// pass must leave its Waiting token and its zeroed deadline exactly as
// they were, because escalation only reads holds and re-derives them from
// live evidence.
func TestExecuteErroredOpPassStillEscalatesExpiredDeadline(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	surgeIdx := int32(10)
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{
			Index: 0, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseUpdating,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "update-0",
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           workloadtypes.UpdateStepSurge,
				SurgeIndex:     &surgeIdx,
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       metav1.NewTime(now.Add(-time.Minute)),
			},
		},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseReady},
		{
			Index: 2, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{
				ID:                "create-2",
				Type:              workloadtypes.InstanceOperationCreate,
				Step:              "CreatePods",
				StartedAt:         metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt:    metav1.NewTime(now.Add(-2 * time.Minute)),
				Waiting:           workloadtypes.RejectionReasonQuotaExceeded,
				CapacityRefusedAt: ptrTime(now.Add(-2 * time.Minute)),
			},
		},
	}
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{{TargetRevision: "superseded"}}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	rejected := rejectedStatusWrite()
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		if idx == 1 {
			return rejected
		}
		return store.apply(ctx, []workloadtypes.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }
	prunes := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		prunes++
		return nil
	}
	plan := threeInstancePlan()
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}
	snapshot := workload.SnapshotWithPodsForTest(in, nil)

	result, err := workload.Execute(ctx, deps, in, plan, nil, snapshot, restartOnlyDecision(plan.Instances[1]))

	if err == nil {
		t.Fatal("the pass must still report the restart failure")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("returned error must carry the apiserver rejection; got %v", err)
	}
	if !strings.Contains(err.Error(), "restart instance 1") {
		t.Fatalf("returned error must name the failed Instance; got %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %+v, want the errored pass's empty result", result)
	}
	if got := store.status(0); got == nil || got.Phase != workloadtypes.InstancePhaseFailed ||
		got.LastFailure == nil || got.LastFailure.Reason != escalation.DeadlineExceededReason || got.Operation == nil {
		t.Fatalf("expired Instance was not escalated: %+v", got)
	}
	if got := store.status(1); got == nil || got.Phase != workloadtypes.InstancePhaseReady || got.Operation != nil {
		t.Fatalf("instance 1 must be untouched while its writes are rejected: %+v", got)
	}
	held := store.status(2)
	if held == nil || held.Phase != workloadtypes.InstancePhaseCreating || held.Operation == nil {
		t.Fatalf("held Instance changed phase: %+v", held)
	}
	if held.Operation.Waiting != workloadtypes.RejectionReasonQuotaExceeded {
		t.Fatalf("held Instance lost its Waiting token: %q", held.Operation.Waiting)
	}
	if !held.Operation.Deadline.IsZero() {
		t.Fatalf("held Instance's parked deadline was re-armed: %v", held.Operation.Deadline)
	}
	if warnings != 1 || prunes != 1 {
		t.Fatalf("end-of-pass effects: warnings=%d prunes=%d, want 1/1", warnings, prunes)
	}
}

// TestExecuteErroredPodReadDoesNotFabricateStuckEvidence: the restart
// pass fails because the Pod list failed, so the escalation pass reading
// the same memoized source fails too, before it derives any evidence — a
// read failure never invents a stuck pod — so nothing is stamped Failed,
// and the joined error keeps the op error first.
func TestExecuteErroredPodReadDoesNotFabricateStuckEvidence(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	surgeIdx := int32(10)
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.StuckPodGrace = time.Minute
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{
			Index: 0, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseUpdating,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "update-0",
				Type:           workloadtypes.InstanceOperationUpdate,
				Step:           workloadtypes.UpdateStepSurge,
				SurgeIndex:     &surgeIdx,
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       metav1.NewTime(now.Add(-time.Minute)),
			},
		},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseReady},
	}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		return store.apply(ctx, []workloadtypes.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }

	shed := apierrors.NewServiceUnavailable("the apiserver shed the list")
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return shed
			},
		}).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}
	snapshot := workload.NewObservedSnapshot(deps, in, workloadtypes.ComponentEngine, in.ObservedState.InstanceStatuses)

	plan := minimalPlan()
	plan.Instances = append(plan.Instances, workloadtypes.InstancePlan{
		Index: 1, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}},
	})
	_, err := workload.Execute(ctx, deps, in, plan, nil, snapshot, restartOnlyDecision(plan.Instances[1]))

	if err == nil {
		t.Fatal("the pass must report both the restart failure and the escalation read failure")
	}
	opFirst := strings.Index(err.Error(), "restart instance 1")
	escalationSecond := strings.Index(err.Error(), "list pods for escalation pass")
	if opFirst < 0 || escalationSecond < 0 {
		t.Fatalf("joined error must name both failures; got %v", err)
	}
	if opFirst > escalationSecond {
		t.Fatalf("the op error must be reported first; got %v", err)
	}
	if got := store.status(0); got != nil && got.Phase == workloadtypes.InstancePhaseFailed {
		t.Fatalf("a failed pod read must not stamp Failed: %+v", got)
	}
	if warnings != 0 {
		t.Fatalf("WarnInstanceFailed fired %d times on a failed read", warnings)
	}
}

// TestExecuteErroredSiblingKeepsRelocationFirstTry: which
// DisposeExpiredAttempt branch runs is decided by the blamed pod set, not
// by whether a sibling's operation errored this pass. A single-pod Create
// parked Running-but-unready on one node, under Auto migration policy
// with budget, still takes the relocation branch on its first expiry
// while the restart pass fails for another Instance — a terminal
// AutoRecover directive naming the suspect node, no RetryBlock, and the
// born-terminal status mirror.
func TestExecuteErroredSiblingKeepsRelocationFirstTry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	const suspectNode = "node-a"
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.UpdateRevision = "llama-70b-engine-abcde"
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{
			Index: 0, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseCreating,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "create-0",
				Type:           workloadtypes.InstanceOperationCreate,
				Step:           "CreatePods",
				TargetRevision: "llama-70b-engine-abcde",
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       metav1.NewTime(now.Add(-time.Minute)),
			},
		},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseReady, RunningRevision: "llama-70b-engine-abcde"},
	}
	in.Disposition = workloadtypes.DispositionDeps{AutoMigrateMaxAttempts: 3}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	rejected := rejectedStatusWrite()
	in.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
		if idx == 1 {
			return rejected
		}
		return store.apply(ctx, []workloadtypes.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
	}
	blocks := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		blocks++
		return nil
	}
	var mirrored []workloadtypes.MigrationRecord
	in.AppendMigration = func(_ context.Context, rec workloadtypes.MigrationRecord) error {
		mirrored = append(mirrored, rec)
		return nil
	}
	plan := minimalPlan()
	plan.MigrationMode = workloadtypes.MigrationModeAuto
	plan.Instances = append(plan.Instances, workloadtypes.InstancePlan{
		Index: 1, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}},
	})
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations()}
	limbo := nodeScopedLimboPod("llama-70b-engine-0-default-0", in.Key.Namespace, suspectNode, "abcde", now.Add(-2*time.Minute))
	snapshot := workload.SnapshotWithPodsForTest(in, map[int32][]*corev1.Pod{0: {limbo}})

	_, err := workload.Execute(ctx, deps, in, plan, nil, snapshot, restartOnlyDecision(plan.Instances[1]))
	if err == nil || !strings.Contains(err.Error(), "restart instance 1") {
		t.Fatalf("the sibling restart must still fail the pass; got %v", err)
	}

	ledger, lerr := audit.LoadLedgerForOwner(ctx, c, in.OwnerObject)
	if lerr != nil {
		t.Fatalf("load ledger: %v", lerr)
	}
	if len(ledger.Entries) != 1 {
		t.Fatalf("ledger entries: got %d want the first relocation directive", len(ledger.Entries))
	}
	e := ledger.Entries[0]
	if e.Phase != audit.PhaseCompleted || e.Reason != audit.ReasonAutoRecover || e.Outcome != audit.OutcomeRelocateRecreate {
		t.Errorf("entry: got phase=%q reason=%q outcome=%q want a terminal AutoRecover relocate-recreate directive",
			e.Phase, e.Reason, e.Outcome)
	}
	if e.FromNode != suspectNode || e.SourceInstance != 0 || e.SurgeInstance != 0 {
		t.Errorf("entry identity: got fromNode=%q source=%d surge=%d want %s/0/0 (a directive allocates no surge)",
			e.FromNode, e.SourceInstance, e.SurgeInstance, suspectNode)
	}
	if len(mirrored) != 1 || mirrored[0].Trigger != workloadtypes.MigrationTriggerAuto ||
		mirrored[0].Phase != workloadtypes.MigrationPhaseRelocated || mirrored[0].Attempt != 1 {
		t.Errorf("status mirror: got %+v want one born-terminal Auto record on attempt 1", mirrored)
	}
	if blocks != 0 {
		t.Errorf("MutateRetryBlock calls: got %d want none (relocation never holds a revision)", blocks)
	}
	if got := store.status(0); got == nil || got.Phase != workloadtypes.InstancePhaseFailed || got.Operation != nil {
		t.Fatalf("relocated Instance: got %+v want Failed with the operation cleared", got)
	}
}

// nodeScopedLimboPod is the readiness-limbo shape pinned to one node and
// one revision — the node-scoped fault the relocation branch is for.
func nodeScopedLimboPod(name, namespace, node, revisionHash string, since time.Time) *corev1.Pod {
	pod := runningNotReadyPod(name, since)
	pod.Namespace = namespace
	pod.Spec.NodeName = node
	pod.Labels = map[string]string{query.LabelRevisionHash: revisionHash}
	return pod
}

// ptrTime is the recorded-refusal stamp a quota-held fixture carries.
func ptrTime(t time.Time) *metav1.Time {
	at := metav1.NewTime(t)
	return &at
}
func TestReconcileDeleteOwnedReboundFinishesBeforeRecreate(t *testing.T) {
	ctx := context.Background()
	scheme := makeScheme(t)
	in := minimalInput(t)
	plan := minimalPlan()
	plan.InstanceReadyTimeout = time.Minute
	started := metav1.NewTime(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	in.Clock = clocktesting.NewFakeClock(started.Add(2 * time.Minute))
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{{
		Index:       0,
		Incarnation: 1,
		Phase:       workloadtypes.InstancePhaseDeleting,
		Operation: &workloadtypes.InstanceOperation{
			ID:             "delete-0-rebound",
			Type:           workloadtypes.InstanceOperationDelete,
			Step:           "Drain",
			StartedAt:      started,
			LastProgressAt: started,
			Deadline:       metav1.NewTime(started.Add(time.Minute)),
		},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: in.Key.Namespace,
		Name:      query.PodName(in.Key.OwnerName, workloadtypes.ComponentEngine, 0, "default", 0),
		UID:       "rebound-pod-uid",
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: in.Key.OwnerName,
			constants.OMEComponentLabel:           string(workloadtypes.ComponentEngine),
			query.LabelManagedBy:                  query.ManagedByOMENative,
			query.LabelInstanceIdx:                "0",
		},
	}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	expectations := workloadtypes.NewExpectations()
	deps := workloadtypes.Deps{Client: base, APIReader: base, Expectations: expectations}
	store := installTestAtomicMutationStore(&in)
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }
	finalized := 0
	in.FinalizeInstanceResources = func(context.Context, int32) (bool, error) {
		finalized++
		return true, nil
	}
	in.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{
		Pods:       []*corev1.Pod{pod},
		ByInstance: map[int32][]*corev1.Pod{0: {pod}},
	}

	result, err := workload.Reconcile(ctx, deps, in, plan, nil)
	if err != nil {
		t.Fatalf("delete pass: %v", err)
	}
	if result.Requeue || result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("delete pass result = %+v", result)
	}
	status := store.status(0)
	if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting ||
		status.Operation == nil || status.Operation.Type != workloadtypes.InstanceOperationDelete {
		t.Fatalf("delete pass altered durable status: writes=%d status=%+v", store.writes, store.status(0))
	}
	// The row's drain is past its deadline, which is announced on the
	// record and nowhere else: no failure, no phase move, no attempt
	// spent. Everything the rest of this test drives still runs.
	if status.LastFailure == nil || status.LastFailure.Reason != workloadtypes.DrainOverdueReason {
		t.Fatalf("overdue drain not announced on the record: %+v", status.LastFailure)
	}
	if warnings != 0 {
		t.Fatalf("delete pass emitted %d generic failure warnings", warnings)
	}
	if err := base.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("delete pass did not remove old Pod: %v", err)
	}
	if finalized != 0 {
		t.Fatalf("resources finalized before Pods disappeared: %d", finalized)
	}

	// One write per overdue episode. Writes are counted cumulatively,
	// so the later passes assert their own against this baseline.
	drained := store.writes
	if drained != 1 {
		t.Fatalf("delete pass writes = %d, want exactly 1 (the announcement)", drained)
	}
	expectations.ObservedDelete(in.Key.Namespace, in.Key.OwnerName, in.Key.Component, 0)
	store.sync(&in)
	in.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	result, err = workload.Reconcile(ctx, deps, in, plan, nil)
	if err != nil {
		t.Fatalf("completion pass: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("completion pass must force a fresh plan, got %+v", result)
	}
	if finalized != 1 || store.writes != drained+1 || store.status(0) != nil {
		t.Fatalf("completion state = finalized:%d writes:%d status:%+v", finalized, store.writes, store.status(0))
	}
	pods := &corev1.PodList{}
	if err := base.List(ctx, pods, client.InNamespace(in.Key.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("completion pass recreated %d Pods from its stale plan", len(pods.Items))
	}

	store.sync(&in)
	result, err = workload.Reconcile(ctx, deps, in, plan, nil)
	if err != nil {
		t.Fatalf("recreate pass: %v", err)
	}
	if result.RequeueAfter != testRequeueIntervals.Operation {
		t.Fatalf("recreate pass result = %+v", result)
	}
	status = store.status(0)
	if status == nil || status.Phase != workloadtypes.InstancePhaseCreating || store.writes != drained+2 {
		t.Fatalf("recreated status/writes = %+v/%d", status, store.writes)
	}
	pods = &corev1.PodList{}
	if err := base.List(ctx, pods, client.InNamespace(in.Key.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("recreate pass Pods = %d, want 1", len(pods.Items))
	}
}

func TestReconcileFreshScaleDownAdmissionEndsThePass(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	expired := metav1.NewTime(now.Add(-time.Minute))
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{
			Index:       1,
			Incarnation: 1,
			PodCount:    1,
			Phase:       workloadtypes.InstancePhaseRestarting,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "restart-1",
				Type:           workloadtypes.InstanceOperationRestart,
				Step:           "WaitReady",
				StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
				Deadline:       expired,
			},
		},
	}
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{{TargetRevision: "superseded"}}
	in.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{ByInstance: map[int32][]*corev1.Pod{}}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }
	prunes := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		prunes++
		return nil
	}
	plan := minimalPlan()
	plan.InstanceReadyTimeout = time.Minute
	client := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()

	result, err := workload.Reconcile(ctx, workloadtypes.Deps{Client: client, APIReader: client}, in, plan, nil)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("result = %+v, want immediate requeue", result)
	}
	status := store.status(1)
	if status == nil || status.Phase != workloadtypes.InstancePhaseDeleting || status.Operation == nil ||
		status.Operation.Type != workloadtypes.InstanceOperationDelete || status.LastFailure != nil {
		t.Fatalf("admitted status = %+v, want durable Delete ownership", status)
	}
	if store.writes != 1 {
		t.Fatalf("status writes = %d, want only the Delete admission", store.writes)
	}
	if warnings != 0 || prunes != 0 {
		t.Fatalf("post-boundary effects: warnings=%d RetryBlock prunes=%d, want 0/0", warnings, prunes)
	}
}

func TestExecuteActiveScaleDownExcludesDeferredExtrasFromEscalation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	expiredOperation := func(id string) *workloadtypes.InstanceOperation {
		return &workloadtypes.InstanceOperation{
			ID:             id,
			Type:           workloadtypes.InstanceOperationRestart,
			Step:           "WaitReady",
			StartedAt:      metav1.NewTime(now.Add(-2 * time.Minute)),
			LastProgressAt: metav1.NewTime(now.Add(-2 * time.Minute)),
			Deadline:       metav1.NewTime(now.Add(-time.Minute)),
		}
	}
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseRestarting, Operation: expiredOperation("retained")},
		{Index: 1, Incarnation: 1, PodCount: 1, Phase: workloadtypes.InstancePhaseRestarting, Operation: expiredOperation("deferred-extra")},
		{
			Index:       2,
			Incarnation: 1,
			PodCount:    1,
			Phase:       workloadtypes.InstancePhaseDeleting,
			Operation: &workloadtypes.InstanceOperation{
				ID:             "active-delete",
				Type:           workloadtypes.InstanceOperationDelete,
				Step:           "Drain",
				StartedAt:      metav1.NewTime(now.Add(-time.Minute)),
				LastProgressAt: metav1.NewTime(now.Add(-time.Minute)),
			},
		},
	}
	in.ObservedState.RetryBlocks = []workloadtypes.RetryBlock{{TargetRevision: "superseded"}}
	store := installTestAtomicMutationStore(&in)
	in.ApplyInstanceMutations = func(ctx context.Context, mutations []workloadtypes.InstanceMutation) error {
		return store.apply(ctx, mutations, "", nil)
	}
	warnings := 0
	in.WarnInstanceFailed = func(int32, string, string) { warnings++ }
	prunes := 0
	in.MutateRetryBlock = func(context.Context, string, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		prunes++
		return nil
	}
	// The retained repair keeps a live pod: an elapsed deadline parks a set,
	// and a repair with none behind it waits instead.
	retained := enginePod(in.Key.OwnerName, in.Key.Namespace, 0)
	pod := enginePod(in.Key.OwnerName, in.Key.Namespace, 2)
	client := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(retained, pod).Build()
	deps := workloadtypes.Deps{Client: client, APIReader: client, Expectations: workloadtypes.NewExpectations()}
	snapshot := workload.SnapshotWithPodsForTest(in, map[int32][]*corev1.Pod{0: {retained}, 2: {pod}})
	decision := workload.Decision{
		Actions:  []workload.PlannedAction{{Kind: workload.ActionScaleDown, Extras: []int32{1, 2}}},
		Escalate: true,
	}

	result, err := workload.Execute(ctx, deps, in, minimalPlan(), nil, snapshot, decision)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Requeue || result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("result = %+v, want configured scale-down poll", result)
	}
	if got := store.status(0); got == nil || got.Phase != workloadtypes.InstancePhaseFailed {
		t.Fatalf("retained expired Instance was not escalated: %+v", got)
	}
	if got := store.status(1); got == nil || got.Phase != workloadtypes.InstancePhaseRestarting || got.LastFailure != nil {
		t.Fatalf("deferred scale-down extra was mutated: %+v", got)
	}
	if got := store.status(2); got == nil || got.Phase != workloadtypes.InstancePhaseDeleting {
		t.Fatalf("active Delete ownership changed: %+v", got)
	}
	if store.writes != 1 || warnings != 1 || prunes != 1 {
		t.Fatalf("end-of-pass effects: writes=%d warnings=%d prunes=%d, want 1/1/1", store.writes, warnings, prunes)
	}
}

func TestReconcileActiveScaleDownWithoutPollSchedulesForceDeleteBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	in := minimalInput(t)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ScaleDownRequeueInterval = 0
	in.ForceDelete = &workloadtypes.ForceDeletePolicy{
		OverdueSlack:             2 * time.Minute,
		NodeUnreachableThreshold: 5 * time.Minute,
	}
	in.ObservedState.InstanceStatuses = []workloadtypes.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady},
		{
			Index:       1,
			Incarnation: 1,
			Phase:       workloadtypes.InstancePhaseDeleting,
			Operation: &workloadtypes.InstanceOperation{
				ID:        "active-delete",
				Type:      workloadtypes.InstanceOperationDelete,
				Step:      "Drain",
				StartedAt: metav1.NewTime(now.Add(-time.Minute)),
			},
		},
	}
	deletionTimestamp := metav1.NewTime(now.Add(-time.Minute))
	pod := enginePod(in.Key.OwnerName, in.Key.Namespace, 1)
	pod.DeletionTimestamp = &deletionTimestamp
	in.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{
		Pods:       []*corev1.Pod{pod},
		ByInstance: map[int32][]*corev1.Pod{1: {pod}},
	}
	store := installTestAtomicMutationStore(&in)
	client := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()

	result, err := workload.Reconcile(ctx, workloadtypes.Deps{
		Client: client, APIReader: client, Expectations: workloadtypes.NewExpectations(),
	}, in, minimalPlan(), nil)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Requeue || result.RequeueAfter != time.Minute+time.Nanosecond {
		t.Fatalf("result = %+v, want force-delete boundary in 1m+1ns", result)
	}
	if got := store.status(1); got == nil || got.Phase != workloadtypes.InstancePhaseDeleting {
		t.Fatalf("active Delete status changed: %+v", got)
	}
}

// The restart pass gives every selected Instance its turn each pass. These
// tests drive two single-pod Instances under RecreateInstance through the
// closed-loop recoveryHarness: both lose their pod so both are selected for
// restart in the same pass, and one Instance's status writes are rejected
// on every pass.

// losePods deletes every live pod of the given Instances so the next pass
// selects each of them for restart (pod count below desired).
func (h *recoveryHarness) losePods(indices ...int32) {
	h.t.Helper()
	lose := map[int32]bool{}
	for _, idx := range indices {
		lose[idx] = true
	}
	for _, pod := range h.livePods() {
		if idx, ok := query.InstanceIdxFromLabels(pod); !ok || !lose[idx] {
			continue
		}
		if err := h.c.Delete(h.ctx, pod); err != nil {
			h.t.Fatalf("delete pod %s: %v", pod.Name, err)
		}
	}
}

func (h *recoveryHarness) instance(idx int32) *workloadtypes.InstanceStatus {
	sts := h.irStatuses()
	for i := range sts {
		if sts[i].Index == idx {
			return &sts[i]
		}
	}
	return nil
}

func (h *recoveryHarness) podsOf(idx int32) []*corev1.Pod {
	var out []*corev1.Pod
	for _, pod := range h.livePods() {
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == idx {
			out = append(out, pod)
		}
	}
	return out
}

// allReady reports whether exactly n Instances exist, each Ready with no
// Operation and exactly one live pod.
func (h *recoveryHarness) allReady(n int32) bool {
	sts := h.irStatuses()
	if len(sts) != int(n) {
		return false
	}
	for _, s := range sts {
		if s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || len(h.podsOf(s.Index)) != 1 {
			return false
		}
	}
	return true
}

// atIncarnation reports whether the Instance's status and its live pods all
// carry the given incarnation.
func (h *recoveryHarness) atIncarnation(idx int32, incarnation int64) bool {
	s := h.instance(idx)
	if s == nil || s.Incarnation != incarnation {
		return false
	}
	pods := h.podsOf(idx)
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if pod.Labels[query.LabelInstanceIncarnation] != strconv.FormatInt(incarnation, 10) {
			return false
		}
	}
	return true
}

// unavailableModeStep reports whether an Operation step belongs to a mode
// that takes its Instance's pod out of rotation (in-place patch, or the
// recreate drain). Surge steps do not: the replacement rotates in before
// the source rotates out.
func unavailableModeStep(step string) bool {
	switch step {
	case workloadtypes.UpdateStepSurge, workloadtypes.UpdateStepSurgeDrain, "SurgeDrainSettle",
		workloadtypes.UpdateStepGangSurgeTarget, workloadtypes.UpdateStepGangSurgeTargetCleanup:
		return false
	case "":
		return false
	default:
		return true
	}
}

// Op-less rows: an index with no status row (Empty), a paused row that
// lost its pods (Pending) and a converged row (Ready) all carry no
// attempt. Most of what reaches them — a pod condition flipping, a
// PodGroup verdict, an operator edit the revision payload filters out —
// therefore has no reader, and the pass must come out of it having
// written nothing. These tests are the proof of that inertness, one
// event shape at a time.

// opLessRowFixture is a paused Component holding the given rows, so the
// lifecycle passes are parked and whatever the pass does write is the
// event's own doing.
func opLessRowFixture(t *testing.T, rows []workloadtypes.InstanceStatus) (workloadtypes.ReconcileInput, workloadtypes.ComponentPlan, *appsv1.ControllerRevision, *testAtomicMutationStore) {
	t.Helper()
	in := minimalInput(t)
	in.DesiredSpec.Paused = true
	in.ObservedState.UpdateRevision = opLessRowRevision
	in.ObservedState.CurrentRevision = opLessRowRevision
	in.ObservedState.InstanceStatuses = rows
	store := installTestAtomicMutationStore(&in)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: opLessRowRevision}}
	plan := minimalPlan()
	plan.Paused = true
	return in, plan, target, store
}

// opLessRowRevision is the revision every op-less fixture row is both
// running and targeting, so nothing about the revision is in question.
const opLessRowRevision = "llama-70b-engine-noop0001"

// TestPendingRow_PodObservationsAreInert: a Pending row is a
// status-only demotion — it holds no operation and claims no pods, and
// it exists only under pause, which is the reconcile shape the truth
// pass that writes it runs on. A pod of that index turning ready,
// entering or leaving the serving rotation, or disappearing altogether
// therefore reaches nothing: the promote is the Create pass re-deriving
// the whole set once the pause clears, not any one of these
// observations.
func TestPendingRow_PodObservationsAreInert(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arrange puts the index's pod into the observed shape.
		arrange func(*testing.T, *recoveryHarness)
		want    int
	}{
		{name: "pod ready", arrange: func(*testing.T, *recoveryHarness) {}, want: 1},
		{name: "pod serving on", arrange: func(t *testing.T, h *recoveryHarness) {
			h.flipServing(t, 0, corev1.ConditionTrue)
		}, want: 1},
		{name: "pod serving off", arrange: func(t *testing.T, h *recoveryHarness) {
			h.flipServing(t, 0, corev1.ConditionFalse)
		}, want: 1},
		{name: "pod deleted", arrange: func(_ *testing.T, h *recoveryHarness) { h.losePods(0) }, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarness(t, false)
			if !h.run(30, func() bool { return h.allReady(1) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never converged")
			}
			h.desired.Paused = true
			h.forcePhase(t, 0, workloadtypes.InstancePhasePending)
			tc.arrange(t, h)

			h.step()

			got := h.instance(0)
			if got == nil || got.Phase != workloadtypes.InstancePhasePending || got.Operation != nil {
				t.Fatalf("row: got %+v want Pending with no operation", got)
			}
			if n := len(h.podsOf(0)); n != tc.want {
				t.Errorf("pods: got %d want %d (the observation creates and deletes nothing)", n, tc.want)
			}
		})
	}
}

// forcePhase stamps a phase onto an existing row through the harness's
// own status round-trip, standing in for the truth pass that writes it.
func (h *recoveryHarness) forcePhase(t *testing.T, idx int32, phase workloadtypes.InstancePhase) {
	t.Helper()
	if err := h.mutateInstance()(h.ctx, idx, func(s *workloadtypes.InstanceStatus) bool {
		s.Phase = phase
		return true
	}); err != nil {
		t.Fatalf("force phase %q on instance %d: %v", phase, idx, err)
	}
}

// flipServing sets the controller-owned serving gate on the index's
// single pod, the condition the load-balancer rotation is keyed on.
func (h *recoveryHarness) flipServing(t *testing.T, idx int32, status corev1.ConditionStatus) {
	t.Helper()
	pods := h.podsOf(idx)
	if len(pods) != 1 {
		t.Fatalf("pods of instance %d: got %d want 1", idx, len(pods))
	}
	setPodCondition(pods[0], query.ServingConditionType, status, h.clk.Now())
	if err := h.c.Status().Update(h.ctx, pods[0]); err != nil {
		t.Fatalf("flip the serving gate: %v", err)
	}
}

// TestOpLessRows_ExcludedAnnotationEditStartsNoWork: an edit to an
// annotation the revision payload filters out leaves the hash where it
// was, so the target does not move and no pass has anything new to act
// on. The hash check is half the claim — without it a pass that wrote
// nothing would only mean the edit never reached the renderer; the
// paused fixture is the other half, isolating the edit from the
// lifecycle work an unpaused index would do anyway.
func TestOpLessRows_ExcludedAnnotationEditStartsNoWork(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []workloadtypes.InstanceStatus
		want workloadtypes.InstancePhase
	}{
		{"empty index", nil, ""},
		{"pending row", []workloadtypes.InstanceStatus{{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhasePending}}, workloadtypes.InstancePhasePending},
		{"ready row", []workloadtypes.InstanceStatus{{Index: 0, Incarnation: 1, Phase: workloadtypes.InstancePhaseReady, PodCount: 1, RunningRevision: opLessRowRevision}}, workloadtypes.InstancePhaseReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, plan, target, store := opLessRowFixture(t, tc.rows)

			before := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/base-model-name": "llama-7b"}}
			after := &metav1.ObjectMeta{Annotations: map[string]string{
				"ome.io/base-model-name":                   "llama-7b",
				constants.ReleaseHeldRevisionAnnotationKey: "llama-70b-engine-old00001",
			}}
			hBefore, _, err := revision.Hash(in.DesiredSpec.PodSpec, before, nil, "")
			if err != nil {
				t.Fatalf("hash before the edit: %v", err)
			}
			hAfter, _, err := revision.Hash(in.DesiredSpec.PodSpec, after, nil, "")
			if err != nil {
				t.Fatalf("hash after the edit: %v", err)
			}
			if hBefore != hAfter {
				t.Fatalf("an excluded annotation moved the hash: before=%s after=%s", hBefore, hAfter)
			}
			in.DesiredSpec.PodTemplateObjectMeta = after

			c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
			if _, err := workload.Reconcile(context.Background(), workloadtypes.Deps{Client: c, APIReader: c}, in, plan, target); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			if store.writes != 0 {
				t.Errorf("status writes: got %d want 0", store.writes)
			}
			got := store.status(0)
			if tc.want == "" {
				if got != nil {
					t.Errorf("row: got %+v want none (the edit plans nothing for an empty index)", got)
				}
			} else if got == nil || got.Phase != tc.want || got.Operation != nil {
				t.Errorf("row: got %+v want %q with no operation", got, tc.want)
			}
			pods := &corev1.PodList{}
			if err := c.List(context.Background(), pods, client.InNamespace(in.Key.Namespace)); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			if len(pods.Items) != 0 {
				t.Errorf("pods: got %d want none", len(pods.Items))
			}
		})
	}
}

// TestUnpause_EmptyIndexIsMaterializedLikeAPendingRow: clearing the
// pause lets the Create pass materialize a planned index whose pods are
// missing. An index with no status row at all takes the same path a
// demoted Pending row takes — nothing about the unpause is special to
// having a row.
func TestUnpause_EmptyIndexIsMaterializedLikeAPendingRow(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.desired.Paused = true

	h.step()
	if s := h.instance(0); s != nil {
		t.Fatalf("paused pass materialized the index: %+v", s)
	}
	if pods := h.livePods(); len(pods) != 0 {
		t.Fatalf("paused pass created %d pod(s)", len(pods))
	}

	h.desired.Paused = false
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("after unpause")
		t.Fatalf("the index was never materialized after the unpause")
	}
}

// TestReadyRow_ServingGateFlipsStampNoTransition: the serving gate is
// the load-balancer rotation, not the Instance lifecycle. A pod entering
// it is what the Create promote already wrote, and a pod leaving it
// drops the serving count the coordination gate reads — but neither is
// a per-Instance transition, so the converged row keeps its phase and
// stays operation-free through both.
func TestReadyRow_ServingGateFlipsStampNoTransition(t *testing.T) {
	for _, serving := range []bool{true, false} {
		name := "serving off"
		if serving {
			name = "serving on"
		}
		t.Run(name, func(t *testing.T) {
			h := newRecoveryHarness(t, false)
			if !h.run(30, func() bool { return h.allReady(1) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never converged")
			}
			before := h.instance(0)

			status := corev1.ConditionFalse
			if serving {
				status = corev1.ConditionTrue
			}
			h.flipServing(t, 0, status)

			h.step()

			got := h.instance(0)
			if got == nil || got.Phase != workloadtypes.InstancePhaseReady || got.Operation != nil {
				t.Fatalf("row: got %+v want Ready with no operation", got)
			}
			if got.Incarnation != before.Incarnation {
				t.Errorf("Incarnation: got %d want %d (no repair is armed)", got.Incarnation, before.Incarnation)
			}
			if got.RunningRevision != before.RunningRevision {
				t.Errorf("RunningRevision: got %q want %q", got.RunningRevision, before.RunningRevision)
			}
		})
	}
}

// TestPauseFreeze_RecreatePolicyTotalLossDemotesThenRepairs: the only pod
// of a converged Instance under RecreateInstanceOnPodRestart is deleted
// while the Component is frozen. Freeze suspends the repair, never the
// truth: the row is demoted to Pending with its running revision kept, and
// stays so with nothing recreated for the length of the freeze. Once the
// pause is downgraded to a plain one the restart pass repairs the row at
// the revision it ran, with the incarnation bump, although the Component's
// target moved on under the freeze.
func TestPauseFreeze_RecreatePolicyTotalLossDemotesThenRepairs(t *testing.T) {
	h := newRecoveryHarness(t, false)
	policy := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &policy
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.allReady(1) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged")
	}
	ready := h.instance(0)
	readyIncarnation, running := ready.Incarnation, ready.RunningRevision
	if running != h.revV1.Name {
		t.Fatalf("converged row records %q, want %s", running, h.revV1.Name)
	}

	h.setTarget(h.revFixed, fixedImage)
	h.desired.Paused = true
	h.desired.PauseFreeze = true
	h.losePods(0)
	h.step()

	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhasePending || s.Operation != nil ||
		s.RunningRevision != running || s.Incarnation != readyIncarnation {
		h.dumpState("frozen with no pods")
		t.Fatalf("demotion: got %+v, want Phase=Pending with no operation, the running revision and the incarnation kept", s)
	}
	for i := 0; i < 2; i++ {
		h.step()
	}
	if pods := h.podsOf(0); len(pods) != 0 {
		t.Fatalf("frozen passes recreated %d pod(s)", len(pods))
	}
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhasePending || s.Incarnation != readyIncarnation {
		t.Fatalf("demotion did not hold across frozen passes: %+v", s)
	}

	h.desired.PauseFreeze = false
	repaired := func() bool {
		s := h.instance(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil && s.Incarnation > readyIncarnation
	}
	if !h.run(30, repaired) {
		h.dumpState("after the downgrade to a plain pause")
		t.Fatalf("the plain pause never repaired the demoted Instance")
	}
	s := h.instance(0)
	if s.RunningRevision != running {
		t.Fatalf("repaired row records %q, want the running revision %s: repair never advances a rollout", s.RunningRevision, running)
	}
	pods := h.podsOf(0)
	if len(pods) != 1 {
		t.Fatalf("repaired Instance has %d live pod(s), want 1", len(pods))
	}
	if got, want := pods[0].Labels[query.LabelRevisionHash], query.RevisionFromName(running).Hash(); got != want {
		t.Fatalf("rebuilt pod revision label = %q, want %q (the running revision, not the target minted under the freeze)", got, want)
	}
	if got := pods[0].Spec.Containers[0].Image; got != goodImage {
		t.Fatalf("rebuilt pod image = %q, want %q: the rebuild renders the running revision's template, not the one minted under the freeze", got, goodImage)
	}
	if got := pods[0].Labels[query.LabelInstanceIncarnation]; got != strconv.FormatInt(s.Incarnation, 10) {
		t.Fatalf("rebuilt pod incarnation label = %q, want %d", got, s.Incarnation)
	}
}

// loseRunner deletes the live pod of runner at index idx, the way an
// operator or a node takes one member of a gang away.
func (h *recoveryHarness) loseRunner(idx int32, runner string) {
	h.t.Helper()
	for _, pod := range h.podsOf(idx) {
		if pod.Labels[query.LabelRunner] != runner {
			continue
		}
		if err := h.c.Delete(h.ctx, pod); err != nil {
			h.t.Fatalf("delete pod %s: %v", pod.Name, err)
		}
		return
	}
	h.t.Fatalf("no pod of instance %d runner %q to lose", idx, runner)
}

// loseNodeUnder stops the kubelet under the live pods of runner at index
// idx (every pod of the Instance when runner is empty): from the next pass
// on, the kubelet model never touches them again and the node lifecycle
// controller model revokes their Ready condition.
func (h *recoveryHarness) loseNodeUnder(idx int32, runner string) {
	h.t.Helper()
	lost := 0
	for _, pod := range h.podsOf(idx) {
		if runner != "" && pod.Labels[query.LabelRunner] != runner {
			continue
		}
		h.kubeletGone[pod.Name] = true
		lost++
	}
	if lost == 0 {
		h.t.Fatalf("no pod of instance %d runner %q under the lost node", idx, runner)
	}
}

// nodeControllerMarksNotReady is the node lifecycle controller's half of a
// lost node: once the node misses its grace, the pod's Ready condition is
// set False and nothing else on the pod changes, since the kubelet that
// wrote the container statuses and the controller that wrote the serving
// gate are not looking at the node.
func (h *recoveryHarness) nodeControllerMarksNotReady(pod *corev1.Pod) {
	h.t.Helper()
	if !podConditionTrue(pod, corev1.PodReady) {
		return
	}
	setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
	if err := h.c.Status().Update(h.ctx, pod); err != nil {
		h.t.Fatalf("node controller marks %s not ready: %v", pod.Name, err)
	}
}

// publication is the status the InferenceReplica adapter would publish
// from the harness's pods: the live counters overlaid on the persisted
// rows, and the Component counters derived from the same observation.
// The harness has no EndpointSlices, so the Available counters read zero.
func (h *recoveryHarness) publication() ([]workloadtypes.InstanceStatus, workload.ComponentCounters) {
	h.t.Helper()
	in := h.buildInput()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, h.desired, in.ObservedState)
	if err != nil {
		h.t.Fatalf("BuildPlan: %v", err)
	}
	list := &corev1.PodList{}
	if err := h.c.List(h.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, &list.Items[i])
	}
	observation, err := workload.NewOwnedPublicationObservation(
		h.irStatuses(),
		workload.NewCachedSelectorPodObservation(pods, query.BucketPodsByInstanceIdx(pods)),
		h.availableByPod(),
		status.AvailabilityWindow{MinReadySeconds: plan.MinReadySeconds, Now: h.clk.Now()},
	)
	if err != nil {
		h.t.Fatalf("publication observation: %v", err)
	}
	rows, counters, err := observation.TakeInlineV1Publication(status.DesiredPodCountByInstance(plan), h.target.Name)
	if err != nil {
		h.t.Fatalf("publication: %v", err)
	}
	return rows, counters
}

// publishedRow is the published row of Instance idx.
func (h *recoveryHarness) publishedRow(rows []workloadtypes.InstanceStatus, idx int32) workloadtypes.InstanceStatus {
	h.t.Helper()
	for _, row := range rows {
		if row.Index == idx {
			return row
		}
	}
	h.t.Fatalf("no published row for instance %d", idx)
	return workloadtypes.InstanceStatus{}
}

// podSetSignature identifies the live pod set by name and creation
// instant, so a rebuild under a reused name is as visible as a delete.
func (h *recoveryHarness) podSetSignature() string {
	h.t.Helper()
	var keys []string
	for _, pod := range h.livePods() {
		keys = append(keys, wedgeKey(pod))
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// instanceIdentity is the row's incarnation and its live pod set by name
// and creation instant, so a rebuild under a reused name is as visible as
// a delete; empty when the row is gone.
func (h *recoveryHarness) instanceIdentity(idx int32) string {
	h.t.Helper()
	row := h.instance(idx)
	if row == nil {
		return ""
	}
	var keys []string
	for _, pod := range h.podsOf(idx) {
		keys = append(keys, wedgeKey(pod))
	}
	sort.Strings(keys)
	return fmt.Sprintf("incarnation=%d pods=%s", row.Incarnation, strings.Join(keys, ","))
}

// repairedAbove reports whether Instance idx is Ready with no operation
// at an incarnation above incarnation, holding its full pod set.
func (h *recoveryHarness) repairedAbove(idx int32, incarnation int64) bool {
	s := h.instance(idx)
	return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil &&
		s.Incarnation > incarnation && len(h.podsOf(idx)) == len(h.desired.Runners)
}

// requirePodsRender asserts that every live pod of Instance idx is the
// revision rev end to end: rev's hash on its revision label and image in
// its runner container. A pod whose label and image name different
// revisions is routed, counted and drained as one revision while running
// another.
func (h *recoveryHarness) requirePodsRender(idx int32, rev *appsv1.ControllerRevision, image string) {
	h.t.Helper()
	pods := h.podsOf(idx)
	if len(pods) != len(h.desired.Runners) {
		h.dumpState("pod set")
		h.t.Fatalf("instance %d has %d live pod(s), want %d", idx, len(pods), len(h.desired.Runners))
	}
	hash := query.RevisionOf(rev).Hash()
	for _, pod := range pods {
		if got := pod.Labels[query.LabelRevisionHash]; got != hash {
			h.t.Fatalf("pod %s revision label = %q, want %q", pod.Name, got, hash)
		}
		if got := pod.Spec.Containers[0].Image; got != image {
			h.t.Fatalf("pod %s labeled revision %s runs image %q, want %q: the rebuild rendered a template other than the revision it stamps", pod.Name, rev.Name, got, image)
		}
	}
}

// TestRepair_RebuildsTheRevisionItStamps_UnderPause: a repair rebuilds
// exactly the revision it stamps. A plain pause withholds the roll and
// keeps repairing, so an Instance that loses a pod while the Component's
// template has moved on is rebuilt at its own revision; the rebuilt pods
// must be rendered from that revision's stored template, not from the
// current one, so the revision label, the per-revision Service selecting
// on it and the image the pod runs agree. Single pod and gang, under the
// pod-loss trigger of RecreateInstanceOnPodRestart and the crash-loop
// repair that belongs to every policy. Once the pause clears the roll
// replaces the repaired Instance at the new revision as usual.
func TestRepair_RebuildsTheRevisionItStamps_UnderPause(t *testing.T) {
	recreate := workloadtypes.RestartPolicyRecreateInstance
	none := workloadtypes.RestartPolicyNone
	cases := []struct {
		name     string
		multiPod bool
		policy   workloadtypes.RestartPolicy
		induce   func(h *recoveryHarness)
	}{
		{"single pod lost under RecreateInstanceOnPodRestart", false, recreate, func(h *recoveryHarness) { h.losePods(0) }},
		{"single pod crash-looping under None", false, none, func(h *recoveryHarness) { h.crashPod(0, "", crashStartImage) }},
		{"gang worker lost under RecreateInstanceOnPodRestart", true, recreate, func(h *recoveryHarness) { h.loseRunner(0, "worker") }},
		{"gang worker crash-looping under None", true, none, func(h *recoveryHarness) { h.crashPod(0, "worker", crashStartImage) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarness(t, tc.multiPod)
			policy := tc.policy
			h.lifecycle.RestartPolicy = &policy
			h.driveToReadyOnV1()
			before := h.instance(0).Incarnation

			h.setTarget(h.revFixed, fixedImage)
			h.desired.Paused = true
			tc.induce(h)
			if !h.run(40, func() bool { return h.repairedAbove(0, before) }) {
				h.dumpState("repair under pause")
				t.Fatalf("the paused Component never repaired the Instance")
			}
			if s := h.instance(0); s.RunningRevision != h.revV1.Name {
				t.Fatalf("repaired row records %q, want %s: a repair never advances the roll", s.RunningRevision, h.revV1.Name)
			}
			h.requirePodsRender(0, h.revV1, goodImage)

			h.desired.Paused = false
			if !h.run(80, func() bool { return h.settledOn(h.revFixed, 1) }) {
				h.dumpState("roll after the pause")
				t.Fatalf("the roll did not replace the repaired Instance at the new revision once the pause cleared")
			}
		})
	}
}

// TestRepair_RebuildsTheRevisionItStamps_HeldTarget: the roll target is
// Held, so a healthy Instance still on the stable revision that loses its
// pod is repaired at its own revision. The rebuild must render the stable
// revision's template: rendering the current template would run the held
// image under the stable revision's label, inside the stable revision's
// Service. The partition keeps one Instance on the stable revision while
// the other exhausts the held target's attempts.
func TestRepair_RebuildsTheRevisionItStamps_HeldTarget(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.replicas = 2
	policy := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &policy
	partition := int32(1)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategySurgeThenDrain,
		RollingUpdate: &workloadtypes.RollingUpdate{Partition: &partition},
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 2) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on two Instances")
	}
	h.driveToHeld()
	h.settle(5)
	stable := h.instance(0)
	if stable == nil || stable.Phase != workloadtypes.InstancePhaseReady || stable.Operation != nil || stable.RunningRevision != h.revV1.Name {
		h.dumpState("at Held")
		t.Fatalf("the partitioned Instance must still be Ready on the stable revision at Held, got %+v", stable)
	}
	before := stable.Incarnation

	h.losePods(0)
	// Stop at the first rebuilt pod as well as at the finished repair, so
	// a rebuild rendered from the wrong template is reported as such
	// rather than as a repair that never finished.
	rebuilt := func() bool { return len(h.podsOf(0)) > 0 }
	if !h.run(40, func() bool { return h.repairedAbove(0, before) || rebuilt() }) {
		h.dumpState("repair under a Held target")
		t.Fatalf("the stable Instance was never repaired while the target is Held")
	}
	h.requirePodsRender(0, h.revV1, goodImage)
	if !h.run(40, func() bool { return h.repairedAbove(0, before) }) {
		h.dumpState("repair under a Held target")
		t.Fatalf("the stable Instance was never repaired while the target is Held")
	}
	if s := h.instance(0); s.RunningRevision != h.revV1.Name {
		t.Fatalf("repaired row records %q, want %s", s.RunningRevision, h.revV1.Name)
	}
	h.requirePodsRender(0, h.revV1, goodImage)
}

// TestRepair_HoldsWhenTheStampedRevisionIsGone: a repair whose stamped
// revision has no ControllerRevision left cannot render the revision it
// stamps, and must not render the current template under that revision's
// label instead. It creates nothing, says so once, and the attempt runs to
// its deadline; from Failed the ordinary roll replaces the Instance at the
// current revision once the pause clears.
func TestRepair_HoldsWhenTheStampedRevisionIsGone(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.recorder = record.NewFakeRecorder(64)
	policy := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &policy
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 5 * time.Minute}
	h.driveToReadyOnV1()
	if err := h.c.Delete(h.ctx, h.revV1); err != nil {
		t.Fatalf("delete the running revision: %v", err)
	}

	h.setTarget(h.revFixed, fixedImage)
	h.desired.Paused = true
	h.losePods(0)
	h.settle(3)
	if pods := h.livePods(); len(pods) != 0 {
		t.Fatalf("the repair rendered %s under revision label %q with the revision's template gone", pods[0].Spec.Containers[0].Image, pods[0].Labels[query.LabelRevisionHash])
	}
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseRestarting {
		t.Fatalf("row = %+v, want the repair held open", s)
	}
	if !h.sawEvent(workloadtypes.EventReasonRepairRevisionGone) {
		t.Fatalf("no %s event: the held repair must say why it creates nothing; events = %v", workloadtypes.EventReasonRepairRevisionGone, h.events)
	}
	announced := 0
	for _, e := range h.events {
		if strings.Contains(e, string(workloadtypes.EventReasonRepairRevisionGone)) {
			announced++
		}
	}
	if announced != 1 {
		t.Fatalf("%s announced %d times, want once per attempt", workloadtypes.EventReasonRepairRevisionGone, announced)
	}

	h.desired.Paused = false
	if !h.run(80, func() bool { return h.settledOn(h.revFixed, 1) }) {
		h.dumpState("after the deadline")
		t.Fatalf("the Instance never reached the current revision after the held repair's deadline")
	}
}

// ---------------------------------------------------------------------------
// A new revision that crashes: the attempts at it are bounded.
// ---------------------------------------------------------------------------

// crashRevision publishes a revision on image and points the loop at it.
func (h *recoveryHarness) crashRevision(image string) *appsv1.ControllerRevision {
	h.t.Helper()
	return h.pushRevision(image)
}

// trackPodsOnImage returns a per-step hook that records every live pod UID
// on image into seen, so a run can count how many attempts materialized.
func (h *recoveryHarness) trackPodsOnImage(image string, seen map[types.UID]struct{}) func() {
	return func() {
		for _, pod := range h.livePods() {
			if pod.Spec.Containers[0].Image == image {
				seen[pod.UID] = struct{}{}
			}
		}
	}
}

// namesTheCrash reports whether a recorded reason tells the operator the
// container crashed: the kubelet's waiting reason on a disposed single-pod
// attempt, or the crashed container's exit on a retired gang replacement,
// whose LastFailure the gang abandon carries into the block.
func namesTheCrash(reason string) bool {
	return reason == "CrashLoopBackOff" || strings.Contains(reason, "failed (Error, exit 1)")
}

// attemptOpen reports whether the row carries an attempt in flight: an
// operation that is not a spent one parked after its disposition.
func attemptOpen(s *workloadtypes.InstanceStatus) bool {
	return s != nil && s.Operation != nil && !workloadtypes.OperationParked(s.Operation)
}

// heldOn reports whether the RetryBlock for rev is Held.
func (h *recoveryHarness) heldOn(rev string) bool {
	b := h.findBlock(rev)
	return b != nil && b.State == workloadtypes.RetryBlockHeld
}

// useStrategy sets the Component's update strategy for the next setTarget.
func (h *recoveryHarness) useStrategy(strategy workloadtypes.UpdateStrategyType) {
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{Type: strategy}
	h.setTarget(h.target, h.desired.PodSpec.Containers[0].Image)
}

// crashBoundCase is one (shape, strategy) path of a new revision that
// crashes at start under the retry ladder.
type crashBoundCase struct {
	name     string
	multiPod bool
	strategy workloadtypes.UpdateStrategyType
	// keepsServing is true where the strategy leaves the source serving
	// while the replacement is retried (surge shapes).
	keepsServing bool
}

var crashBoundCases = []crashBoundCase{
	{name: "single-pod SurgeThenDrain", strategy: workloadtypes.UpdateStrategySurgeThenDrain, keepsServing: true},
	{name: "single-pod RecreatePod", strategy: workloadtypes.UpdateStrategyRecreatePod},
	{name: "single-pod InPlaceIfPossible", strategy: workloadtypes.UpdateStrategyInPlaceIfPossible},
	{name: "single-pod InPlaceOnly", strategy: workloadtypes.UpdateStrategyInPlaceOnly},
	{name: "gang SurgeThenDrain", multiPod: true, strategy: workloadtypes.UpdateStrategySurgeThenDrain, keepsServing: true},
	{name: "gang RecreatePod", multiPod: true, strategy: workloadtypes.UpdateStrategyRecreatePod},
	{name: "gang InPlaceIfPossible", multiPod: true, strategy: workloadtypes.UpdateStrategyInPlaceIfPossible},
}

// TestCrashLoopBound_NewRevisionCrashesAtStart pins the rule for a
// revision whose container dies on every start: each attempt the
// disposition ends without a relocation directive counts toward
// updateRetry.maxAttempts like any failed attempt, the revision is Held
// at the bound with one RetryHeld warning naming the crash loop, no
// further attempt materializes while it is Held, and a corrected
// revision releases it.
func TestCrashLoopBound_NewRevisionCrashesAtStart(t *testing.T) {
	for _, tc := range crashBoundCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarness(t, tc.multiPod)
			h.useStrategy(tc.strategy)
			h.driveToReadyOnV1()

			seen := map[types.UID]struct{}{}
			revCrash := h.crashRevision(crashStartImage)
			held := h.runWithInvariant(120, func() bool { return h.heldOn(revCrash.Name) }, func() {
				h.trackPodsOnImage(crashStartImage, seen)()
				if tc.keepsServing && len(h.servingPods()) == 0 {
					t.Fatalf("the source left rotation while the crashing replacement was retried")
				}
			})
			if !held {
				h.dumpState("crash loop never held")
				t.Fatalf("the crashing revision was never Held: attempts at it are unbounded")
			}
			b := h.findBlock(revCrash.Name)
			if b.AttemptsStarted != h.retryPolicy.MaxAttempts || !namesTheCrash(b.Reason) {
				t.Fatalf("held block: got (attempts=%d, reason=%q) want (%d, a reason naming the crash)", b.AttemptsStarted, b.Reason, h.retryPolicy.MaxAttempts)
			}
			if len(h.heldWarnings) != 1 || !strings.HasPrefix(h.heldWarnings[0], revCrash.Name) {
				t.Fatalf("WarnRetryHeld: got %v, want exactly one warning for %s", h.heldWarnings, revCrash.Name)
			}
			attemptsAtHeld := len(seen)
			h.settle(2)
			h.runWithInvariant(8, func() bool { return false }, func() {
				h.trackPodsOnImage(crashStartImage, seen)()
				if s := h.instance(0); s == nil || attemptOpen(s) || s.Phase == workloadtypes.InstancePhaseUpdating {
					h.dumpState("attempt after Held")
					t.Fatalf("an attempt at the held revision was reopened: %+v", s)
				}
			})
			if len(seen) != attemptsAtHeld {
				h.dumpState("attempt after Held")
				t.Fatalf("a new replacement materialized after Held: %d pods seen before, %d after", attemptsAtHeld, len(seen))
			}

			h.setTarget(h.revFixed, fixedImage)
			if !h.run(80, func() bool { return h.converged(h.revFixed.Name) }) {
				h.dumpState("release by corrected revision")
				t.Fatalf("the corrected revision did not converge after the crash loop was Held")
			}
		})
	}
}

// TestCrashLoopBound_NewRevisionCrashesAfterReady pins the same rule for a
// replacement that reports ContainersReady and then dies before it ever
// serves: the surge never hands off, the attempt is disposed on the crash
// loop and counted, and the revision is Held at the bound.
func TestCrashLoopBound_NewRevisionCrashesAfterReady(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()

	revCrash := h.crashRevision(crashAfterReadyImage)
	held := h.runWithInvariant(120, func() bool { return h.heldOn(revCrash.Name) }, func() {
		if len(h.servingPods()) == 0 {
			t.Fatalf("the source left rotation while the replacement crashed after readiness")
		}
	})
	if !held {
		h.dumpState("crash after ready never held")
		t.Fatalf("a replacement that crashes after reporting ready was never Held")
	}
	b := h.findBlock(revCrash.Name)
	if b.AttemptsStarted != h.retryPolicy.MaxAttempts || b.Reason != "CrashLoopBackOff" {
		t.Fatalf("held block: got (attempts=%d, reason=%q) want (%d, CrashLoopBackOff)", b.AttemptsStarted, b.Reason, h.retryPolicy.MaxAttempts)
	}
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.RunningRevision != h.revV1.Name {
		h.dumpState("held row")
		t.Fatalf("the row must be Failed on its running v1 revision, got %+v", s)
	}
}

// TestCrashLoopBound_CrashAfterPromotionIsNotThisBound pins the edge of
// the rule: a replacement that serves long enough to be promoted makes
// its revision the running one, so a later crash is a running-revision
// failure for the repair path, not an attempt at a target revision. No
// RetryBlock for the promoted revision reaches Held.
func TestCrashLoopBound_CrashAfterPromotionIsNotThisBound(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()

	revCrash := h.crashRevision(crashAfterServingImage)
	promoted := h.run(40, func() bool {
		s := h.instance(0)
		return s != nil && s.RunningRevision == revCrash.Name
	})
	if !promoted {
		h.dumpState("promotion")
		t.Fatalf("a replacement that served was never promoted")
	}
	h.settle(20)
	if h.heldOn(revCrash.Name) {
		h.dumpState("held after promotion")
		t.Fatalf("the promoted revision was Held for a crash that happened after it became the running revision")
	}
}

// TestCrashLoopBound_NoRetryPolicyKeepsRetrying pins the arm with no
// updateRetry configured: the crashing revision carries no RetryBlock,
// is never Held, and attempts keep materializing. The source keeps
// serving throughout.
func TestCrashLoopBound_NoRetryPolicyKeepsRetrying(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.retryPolicy = nil
	h.driveToReadyOnV1()

	revCrash := h.crashRevision(crashStartImage)
	reopened := 0
	h.runWithInvariant(40, func() bool { return false }, func() {
		if len(h.servingPods()) == 0 {
			t.Fatalf("the source left rotation while the crashing replacement was retried")
		}
		// A disposed attempt that is opened again is one more retry.
		if s := h.instance(0); s != nil && s.Phase == workloadtypes.InstancePhaseFailed {
			reopened++
		}
	})
	if h.findBlock(revCrash.Name) != nil {
		t.Fatalf("with no updateRetry configured the crashing revision must carry no RetryBlock, got %+v", *h.findBlock(revCrash.Name))
	}
	if reopened < 3 {
		h.dumpState("unbounded retries")
		t.Fatalf("with no updateRetry configured the revision must keep being retried; only %d attempts were disposed and reopened", reopened)
	}
}

// TestCrashLoopBound_ReleasingTheHeldBlockReopensTheRevision pins the
// operator release: removing the Held block (what the release annotation
// does) admits a fresh attempt at the same revision, which is counted
// from zero again.
func TestCrashLoopBound_ReleasingTheHeldBlockReopensTheRevision(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()

	revCrash := h.crashRevision(crashStartImage)
	if !h.run(120, func() bool { return h.heldOn(revCrash.Name) }) {
		h.dumpState("crash loop never held")
		t.Fatalf("the crashing revision was never Held")
	}
	h.settle(4)

	h.blocks = nil
	attemptReopened := false
	counted := h.runWithInvariant(20, func() bool {
		b := h.findBlock(revCrash.Name)
		return attemptReopened && b != nil && b.AttemptsStarted == 1
	}, func() {
		if s := h.instance(0); s != nil && s.Operation != nil && s.Operation.TargetRevision == revCrash.Name {
			attemptReopened = true
		}
	})
	if !counted {
		h.dumpState("release")
		t.Fatalf("removing the Held block did not admit a fresh attempt counted from zero: reopened=%v block=%+v", attemptReopened, h.findBlock(revCrash.Name))
	}
}

// TestCrashLoopBound_PeersKeepServingWhileTheTargetIsRetried pins the
// peer story inside one Component: two Instances roll toward a crashing
// revision under maxSurge=1; while the attempts are retried both sources
// keep serving and at most one replacement is alive, and once the
// revision is Held both Instances are Ready on their running revision
// with no operation. A corrected revision then converges both.
func TestCrashLoopBound_PeersKeepServingWhileTheTargetIsRetried(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.replicas = 2
	maxSurge, maxUnavailable := intstr.FromInt(1), intstr.FromInt(0)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategySurgeThenDrain,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.allReady(2) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on v1 for two instances")
	}

	revCrash := h.crashRevision(crashStartImage)
	held := h.runWithInvariant(160, func() bool { return h.heldOn(revCrash.Name) }, func() {
		if got := len(h.servingPods()); got != 2 {
			h.dumpState("serving dropped")
			t.Fatalf("both sources must keep serving while the crashing replacement is retried, got %d serving", got)
		}
		alive := 0
		for _, pod := range h.livePods() {
			if pod.Spec.Containers[0].Image == crashStartImage {
				alive++
			}
		}
		if alive > 1 {
			t.Fatalf("more replacements alive than maxSurge admits: %d", alive)
		}
	})
	if !held {
		h.dumpState("crash loop never held")
		t.Fatalf("the crashing revision was never Held")
	}
	h.settle(4)
	for _, s := range h.irStatuses() {
		if s.Operation != nil || s.RunningRevision != h.revV1.Name || s.Phase == workloadtypes.InstancePhaseUpdating {
			h.dumpState("peers at Held")
			t.Fatalf("instance %d is not parked on v1 with no operation: %+v", s.Index, s)
		}
	}

	h.setTarget(h.revFixed, fixedImage)
	if !h.run(120, func() bool {
		if !h.allReady(2) {
			return false
		}
		for _, s := range h.irStatuses() {
			if s.RunningRevision != h.revFixed.Name {
				return false
			}
		}
		return true
	}) {
		h.dumpState("corrected revision")
		t.Fatalf("both instances did not converge on the corrected revision")
	}
}

// A push whose pods pass readiness, are promoted, and then crash. The
// roll has already moved such an Instance onto the target revision, so
// there is no attempt left to retry: the Instance is the roll's open
// work, and the per-Component budget holds the rest of the roll on the
// running revision until it serves again or an operator moves the target.

// rollBudgetCase is one shape of that push: the strategy with a budget of
// one on the arm it spends, the Component's size, and how many Instances
// the roll may legitimately have off the running revision at once.
type rollBudgetCase struct {
	name     string
	multiPod bool
	replicas int32
	strategy workloadtypes.UpdateStrategyType
	// image is the crashing revision's image: one that stays down after
	// its first crash, or one that comes back Ready between crashes.
	image string
	// offAllowed bounds the Instances off the running revision at any
	// pass: the budget itself under SurgeThenDrain, where the promoted
	// Instance holds the surge slot and no further source leaves
	// rotation; one more under RecreatePod, for the start the roll admits
	// while the promoted Instance still serves, before its crash can be
	// seen.
	offAllowed int
	// minReadySeconds and stuckGrace set the window a promoted pod that
	// restarted must hold Ready again, minReadySeconds floored by the
	// grace: a pod that comes back between crashes holds the slot while
	// it is up because it never holds Ready for the window. A zero grace
	// leaves the harness default in place.
	minReadySeconds int32
	stuckGrace      time.Duration
}

func rollBudgetCases() []rollBudgetCase {
	return []rollBudgetCase{
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1", replicas: 4, strategy: workloadtypes.UpdateStrategySurgeThenDrain, image: crashAfterServingImage, offAllowed: 1},
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1, the pod comes back between crashes but never holds Ready for the window", replicas: 4, strategy: workloadtypes.UpdateStrategySurgeThenDrain, image: flapAfterServingImage, offAllowed: 1, minReadySeconds: 60},
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1, no minReadySeconds, the pod comes back between crashes but never holds Ready for the stuck-pod grace", replicas: 4, strategy: workloadtypes.UpdateStrategySurgeThenDrain, image: flapAfterServingImage, offAllowed: 1, stuckGrace: 90 * time.Second},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1", replicas: 4, strategy: workloadtypes.UpdateStrategyRecreatePod, image: crashAfterServingImage, offAllowed: 2},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1, no minReadySeconds, the pod comes back between crashes but never holds Ready for the stuck-pod grace", replicas: 4, strategy: workloadtypes.UpdateStrategyRecreatePod, image: flapAfterServingImage, offAllowed: 2, stuckGrace: 90 * time.Second},
		{name: "two gangs under SurgeThenDrain maxSurge 1", multiPod: true, replicas: 2, strategy: workloadtypes.UpdateStrategySurgeThenDrain, image: crashAfterServingImage, offAllowed: 1},
		{name: "three gangs under RecreatePod maxUnavailable 1", multiPod: true, replicas: 3, strategy: workloadtypes.UpdateStrategyRecreatePod, image: crashAfterServingImage, offAllowed: 2},
	}
}

// useRollingBudget sets the Component's strategy with a budget of one on
// the arm the strategy spends: one surge pod for SurgeThenDrain, one
// Instance offline for every other strategy.
func (h *recoveryHarness) useRollingBudget(strategy workloadtypes.UpdateStrategyType) {
	maxSurge, maxUnavailable := intstr.FromInt(0), intstr.FromInt(1)
	if strategy == workloadtypes.UpdateStrategySurgeThenDrain {
		maxSurge, maxUnavailable = intstr.FromInt(1), intstr.FromInt(0)
	}
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          strategy,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
	}
	h.setTarget(h.target, h.desired.PodSpec.Containers[0].Image)
}

// instancesServingOn counts the Instances whose full pod set is in
// rotation on image, whatever else shares the index: a surge replacement
// beside its source leaves the source counted.
func (h *recoveryHarness) instancesServingOn(image string) int {
	byIndex := map[int32]int{}
	for _, pod := range h.livePods() {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok || pod.Spec.Containers[0].Image != image {
			continue
		}
		if podreadiness.IsServing(pod) && podreadiness.IsContainersReady(pod) {
			byIndex[idx]++
		}
	}
	serving := 0
	for _, pods := range byIndex {
		if pods >= len(h.desired.Runners) {
			serving++
		}
	}
	return serving
}

// promotedPodCrashed reports whether a pod of an Instance promoted onto
// rev has crashed.
func (h *recoveryHarness) promotedPodCrashed(rev *appsv1.ControllerRevision) bool {
	for _, s := range h.irStatuses() {
		if s.RunningRevision != rev.Name {
			continue
		}
		for _, pod := range h.podsOf(s.Index) {
			if h.crashes[wedgeKey(pod)] > 0 {
				return true
			}
		}
	}
	return false
}

// lastHold is the most recent rollout hold the update pass reported, or
// nil when it has reported none.
func (h *recoveryHarness) lastHold() *workloadtypes.RolloutHold {
	if len(h.holds) == 0 {
		return nil
	}
	return &h.holds[len(h.holds)-1]
}

// TestCrashAfterPromotion_RollHoldsAtTheBudget pins the operator's end
// state for a push whose revision crashes after it has been promoted: an
// Instance the roll moved onto the target that does not serve it holds
// its slot in the roll's budget. Under SurgeThenDrain no further surge is
// admitted and no further source leaves rotation, so at most the budget
// is off the running revision, which keeps serving at N minus the
// budget; under RecreatePod the one start admitted while the promoted
// Instance still served finishes and nothing further starts. The
// Component's status names the hold with the Instances behind it and the
// revision: the Budget hold, or the ladder's once the revision's retry
// ladder denies the starts before the budget is asked. Once the hold is
// reported, pods of that revision are created only on the revision's
// retry ladder: each further attempt it admits rebuilds one crashed
// Instance's set, a rebuilt set that comes back serving may admit one
// more start, and nothing is created once the ladder holds. A corrected
// revision then lands on every Instance.
func TestCrashAfterPromotion_RollHoldsAtTheBudget(t *testing.T) {
	for _, tc := range rollBudgetCases() {
		t.Run(tc.name, func(t *testing.T) {
			// The runner is rendered under its production name: a restart
			// after Ready is read off that container alone.
			h := newRecoveryHarnessFor(t, tc.multiPod, constants.MainContainerName)
			h.replicas = tc.replicas
			// No repair: under RecreateInstanceOnPodRestart the crashed
			// Instance would be rebuilt and the story could not tell the
			// roll's hold from the repair's.
			noRepair := workloadtypes.RestartPolicyNone
			h.lifecycle.RestartPolicy = &noRepair
			// The promoted pod serves long enough for the roll to reach the
			// next Instance before it dies, so the hold has to act on a
			// start already in flight as well as on the next one.
			h.crashAfterServed = 4
			h.minReadySeconds = tc.minReadySeconds
			if tc.stuckGrace > 0 {
				h.stuckGrace = tc.stuckGrace
			}
			h.useRollingBudget(tc.strategy)
			if !h.run(60, func() bool { return h.settledOn(h.revV1, tc.replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", tc.replicas)
			}

			revCrash := h.crashRevision(tc.image)
			seen := map[types.UID]struct{}{}
			crashed, offAtCrash := false, 0
			holdsSeen, podsAtHold, podsAtHeld := 0, -1, -1
			h.runWithInvariant(40, func() bool { return false }, func() {
				h.trackPodsOnImage(tc.image, seen)()
				if podsAtHeld < 0 && h.heldOn(revCrash.Name) {
					podsAtHeld = len(seen)
				}
				off := int(tc.replicas) - h.instancesServingOn(goodImage)
				if off > tc.offAllowed {
					h.dumpState("roll past the budget")
					t.Fatalf("%d Instances are off the running revision, budget %d: the roll kept replacing Instances with a revision that crashes", off, tc.offAllowed)
				}
				if crashed && off > offAtCrash {
					h.dumpState("roll continued after the crash")
					t.Fatalf("a further Instance left the running revision after a promoted Instance crashed: %d off now, %d at the crash", off, offAtCrash)
				}
				if !crashed && h.promotedPodCrashed(revCrash) {
					crashed, offAtCrash = true, off
				}
				if len(h.holds) > holdsSeen {
					holdsSeen = len(h.holds)
					if crashed && podsAtHold < 0 {
						podsAtHold = len(seen)
					}
				}
			})
			if !crashed {
				h.dumpState("no crash")
				t.Fatalf("the promoted revision never crashed; the story did not run")
			}
			// The ladder's hold is the RetryBlock gate while it paces the
			// crashed set's rebuilds and the Held gate once it has given the
			// revision up; either names the Instances behind it.
			hold := h.lastHold()
			if hold == nil || (hold.Gate != workloadtypes.RolloutHoldGateBudget && hold.Gate != workloadtypes.RolloutHoldGateRetryBlock && hold.Gate != workloadtypes.RolloutHoldGateHeld) || hold.Target != revCrash.Name {
				h.dumpState("hold")
				t.Fatalf("the Component must report a Budget, RetryBlock or Held hold on %s, got %+v", revCrash.Name, hold)
			}
			if !strings.Contains(hold.Reason, "not serving") || !strings.Contains(hold.Reason, revCrash.Name) {
				t.Fatalf("the hold must name the Instances on the crashing revision that are not serving, got %q", hold.Reason)
			}
			if podsAtHold < 0 {
				h.dumpState("no hold")
				t.Fatalf("no hold was reported after the promoted revision crashed")
			}
			// After the hold only the ladder creates pods of the revision: one
			// set per further attempt it admits, and one more start per rebuilt
			// set that comes back serving; nothing once it holds.
			if rebuilt, admits := len(seen)-podsAtHold, 2*int(h.retryPolicy.MaxAttempts-1)*len(h.desired.Runners); rebuilt > admits {
				h.dumpState("attempt after the hold")
				t.Fatalf("pods of the crashing revision were created after the hold beyond the ladder's attempts: %d at the hold, %d after, at most %d admitted", podsAtHold, len(seen), admits)
			}
			if podsAtHeld >= 0 && len(seen) != podsAtHeld {
				h.dumpState("attempt after the ladder held")
				t.Fatalf("pods of the crashing revision were created after its ladder held: %d when it held, %d after", podsAtHeld, len(seen))
			}
			if floor := int(tc.replicas) - tc.offAllowed; h.instancesServingOn(goodImage) < floor {
				h.dumpState("floor")
				t.Fatalf("the running revision serves on %d Instances, floor %d", h.instancesServingOn(goodImage), floor)
			}

			h.setTarget(h.revFixed, fixedImage)
			if !h.run(120, func() bool { return h.settledOn(h.revFixed, tc.replicas) }) {
				h.dumpState("corrected revision")
				t.Fatalf("the corrected revision did not land on every Instance")
			}
		})
	}
}

// notServingHolds counts the rollout holds the update pass has reported
// that name an Instance on the target revision as not serving.
func (h *recoveryHarness) notServingHolds() int {
	n := 0
	for _, hold := range h.holds {
		if strings.Contains(hold.Reason, "not serving") {
			n++
		}
	}
	return n
}

// podReadyTransition is when the pod's Ready condition last changed, as
// the kubelet stamped it; zero when the pod carries no Ready condition.
func podReadyTransition(pod *corev1.Pod) time.Time {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.LastTransitionTime.Time
		}
	}
	return time.Time{}
}

// TestCrashOnceAfterPromotion_RollResumesWhenTheInstanceServesAgain pins
// the rule for a push whose promoted pod dies once and comes back: the
// Instance holds its slot in the roll's budget until it has proven
// itself again. The window it must serve is the Component's
// minReadySeconds floored by the stuck-pod grace, measured from the
// pod's newest Ready transition the way a Deployment measures
// availability. A pass that reads the pod Ready again for less than
// that window still holds the slot, names the Instance in a Budget hold
// and opens no start; the first pass that reads it Ready for the whole
// window releases the slot and the roll goes on without operator
// action. With neither a minReadySeconds nor a grace configured, Ready
// is Available and the comeback itself releases the slot. Either way
// the revision lands on every Instance with one pod per Instance, the
// pod that was restarted keeps its identity with the one restart on its
// record, no row reads Failed, and restart policy None opens no repair
// for a container the kubelet restarted.
func TestCrashOnceAfterPromotion_RollResumesWhenTheInstanceServesAgain(t *testing.T) {
	cases := []struct {
		name            string
		strategy        workloadtypes.UpdateStrategyType
		minReadySeconds int32
		stuckGrace      time.Duration
	}{
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1, no minReadySeconds, the comeback must hold Ready for the stuck-pod grace", strategy: workloadtypes.UpdateStrategySurgeThenDrain, stuckGrace: 90 * time.Second},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1, no minReadySeconds, the comeback must hold Ready for the stuck-pod grace", strategy: workloadtypes.UpdateStrategyRecreatePod, stuckGrace: 90 * time.Second},
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1, minReadySeconds above the grace, the comeback must hold Ready for minReadySeconds", strategy: workloadtypes.UpdateStrategySurgeThenDrain, minReadySeconds: 90, stuckGrace: 30 * time.Second},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1, minReadySeconds above the grace, the comeback must hold Ready for minReadySeconds", strategy: workloadtypes.UpdateStrategyRecreatePod, minReadySeconds: 90, stuckGrace: 30 * time.Second},
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1, no minReadySeconds and no grace, the comeback itself releases the slot", strategy: workloadtypes.UpdateStrategySurgeThenDrain},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1, no minReadySeconds and no grace, the comeback itself releases the slot", strategy: workloadtypes.UpdateStrategyRecreatePod},
	}
	const replicas = int32(4)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The runner is rendered under its production name: a restart
			// after Ready is read off that container alone.
			h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
			h.replicas = replicas
			noRepair := workloadtypes.RestartPolicyNone
			h.lifecycle.RestartPolicy = &noRepair
			// The promoted pod serves long enough for the roll to reach the
			// next Instance before it dies.
			h.crashAfterServed = 4
			h.minReadySeconds = tc.minReadySeconds
			h.stuckGrace = tc.stuckGrace
			h.useRollingBudget(tc.strategy)
			if !h.run(80, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}

			revOnce := h.crashRevision(crashOnceAfterServingImage)
			// The window the comeback must serve: minReadySeconds floored by
			// the stuck-pod grace.
			window := max(time.Duration(tc.minReadySeconds)*time.Second, tc.stuckGrace)
			seen := map[types.UID]struct{}{}
			var restartedUID types.UID
			var restartedIndex int32
			// passStart is the clock the pass under inspection ran at: the
			// invariant runs after the step moved the clock on.
			passStart := h.clk.Now()
			holds, pods, cameBack := h.notServingHolds(), 0, false
			h.runWithInvariant(100, func() bool { return h.settledOn(revOnce, replicas) }, func() {
				h.trackPodsOnImage(crashOnceAfterServingImage, seen)()
				if h.repairInFlight() {
					h.dumpState("repair")
					t.Fatalf("restart policy None must open no repair for a container the kubelet restarted")
				}
				for _, s := range h.irStatuses() {
					if s.Phase == workloadtypes.InstancePhaseFailed {
						h.dumpState("failed row")
						t.Fatalf("Instance %d reads Failed for a container the kubelet restarted", s.Index)
					}
				}
				var restarted *corev1.Pod
				for _, pod := range h.livePods() {
					if h.crashes[wedgeKey(pod)] > 0 {
						restarted = pod
					}
				}
				if restarted != nil && restartedUID == "" {
					restartedUID = restarted.UID
					restartedIndex, _ = query.InstanceIdxFromLabels(restarted)
				}
				if restarted != nil && podConditionTrue(restarted, corev1.PodReady) {
					cameBack = true
					// The slot is held exactly while the pass reads the pod
					// Ready again for less than the window.
					availableAt := podReadyTransition(restarted).Add(window)
					if held := h.notServingHolds() > holds; held && !passStart.Before(availableAt) {
						h.dumpState("held while serving")
						t.Fatalf("the roll was held on Instance %d, Ready again since %s, at %s: a pod Ready for the window serves and releases its slot: %q",
							restartedIndex, podReadyTransition(restarted).Format(time.RFC3339), passStart.Format(time.RFC3339), h.lastHold().Reason)
					} else if held {
						if hold := h.lastHold(); hold.Gate != workloadtypes.RolloutHoldGateBudget || hold.Target != revOnce.Name || !strings.Contains(hold.Reason, strconv.Itoa(int(restartedIndex))) {
							t.Fatalf("a pass inside the window must report a Budget hold on %s naming Instance %d, got %+v", revOnce.Name, restartedIndex, hold)
						}
						if len(seen) > pods {
							h.dumpState("start inside the window")
							t.Fatalf("a start opened while Instance %d was held inside its window", restartedIndex)
						}
					} else if passStart.Before(availableAt) && window > 0 && len(seen) > pods {
						h.dumpState("start inside the window")
						t.Fatalf("a start opened at %s while Instance %d had been Ready again only since %s, window %s",
							passStart.Format(time.RFC3339), restartedIndex, podReadyTransition(restarted).Format(time.RFC3339), window)
					}
				}
				holds, pods = h.notServingHolds(), len(seen)
				passStart = h.clk.Now()
			})
			if !cameBack {
				h.dumpState("no comeback")
				t.Fatalf("the promoted pod never crashed and came back; the story did not run")
			}
			if !h.settledOn(revOnce, replicas) {
				h.dumpState("roll stalled")
				t.Fatalf("the roll did not land %s on every Instance after Instance %d came back; last hold %+v", revOnce.Name, restartedIndex, h.lastHold())
			}
			if len(seen) != int(replicas) {
				t.Fatalf("the roll created %d pods of %s for %d Instances; one per Instance is the whole roll", len(seen), revOnce.Name, replicas)
			}
			var restarted *corev1.Pod
			for _, pod := range h.livePods() {
				if pod.UID == restartedUID {
					restarted = pod
				}
			}
			if restarted == nil {
				t.Fatalf("the pod the kubelet restarted once must keep its identity; %s is gone", restartedUID)
			}
			if status := priorContainerStatus(restarted); status == nil || status.RestartCount != 1 {
				t.Fatalf("the pod the kubelet restarted once must carry that one restart, got %+v", status)
			}
		})
	}
}

// holdNamesInstance reports whether hold's not-serving clause names idx:
// the indices between "Instance"/"Instances" and " on revision".
func holdNamesInstance(hold *workloadtypes.RolloutHold, idx int32) bool {
	if hold == nil {
		return false
	}
	reason := hold.Reason
	start := strings.LastIndex(reason, "; Instance")
	end := strings.LastIndex(reason, " on revision ")
	if start < 0 || end < 0 || end <= start {
		return false
	}
	clause := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(reason[start+len("; "):end], "Instances"), "Instance"))
	for _, field := range strings.Split(clause, ",") {
		if strings.TrimSpace(field) == strconv.Itoa(int(idx)) {
			return true
		}
	}
	return false
}

// rebuiltAfterFailure returns, by Instance index, the live pods of the
// Instances promoted onto rev that answer the row's recorded failure: a
// pod created no earlier than the failure.
func (h *recoveryHarness) rebuiltAfterFailure(rev *appsv1.ControllerRevision) map[int32]*corev1.Pod {
	out := map[int32]*corev1.Pod{}
	for _, s := range h.irStatuses() {
		if s.RunningRevision != rev.Name || s.LastFailure == nil || s.LastFailure.Time.IsZero() {
			continue
		}
		for _, pod := range h.podsOf(s.Index) {
			if !pod.CreationTimestamp.Time.Before(s.LastFailure.Time.Time) {
				out[s.Index] = pod
			}
		}
	}
	return out
}

// everyPromotedCrashed reports whether every Instance promoted onto rev
// has died on it: a failure recorded on the row, or a live pod of the
// Instance that has crashed.
func (h *recoveryHarness) everyPromotedCrashed(rev *appsv1.ControllerRevision) bool {
	promoted := false
	for _, s := range h.irStatuses() {
		if s.RunningRevision != rev.Name {
			continue
		}
		promoted = true
		crashed := s.LastFailure != nil
		for _, pod := range h.podsOf(s.Index) {
			crashed = crashed || h.crashes[wedgeKey(pod)] > 0
		}
		if !crashed {
			return false
		}
	}
	return promoted
}

// firstFailureAt is the earliest failure recorded on an Instance promoted
// onto rev, zero when none recorded one.
func (h *recoveryHarness) firstFailureAt(rev *appsv1.ControllerRevision) time.Time {
	var first time.Time
	for _, s := range h.irStatuses() {
		if s.RunningRevision != rev.Name || s.LastFailure == nil || s.LastFailure.Time.IsZero() {
			continue
		}
		if at := s.LastFailure.Time.Time; first.IsZero() || at.Before(first) {
			first = at
		}
	}
	return first
}

// rollStartsOn records into started the Instances the roll has started
// toward rev: those with a live pod on image while their row still runs
// another revision. A rebuild of an Instance already on rev is a repair,
// not a start, and is never recorded.
func (h *recoveryHarness) rollStartsOn(image string, rev *appsv1.ControllerRevision, started map[int32]struct{}) {
	for _, pod := range h.livePods() {
		if pod.Spec.Containers[0].Image != image {
			continue
		}
		idx := instanceIndexOf(pod)
		if s := h.instance(idx); s != nil && s.RunningRevision != rev.Name {
			started[idx] = struct{}{}
		}
	}
}

// TestCrashOnceAfterPromotion_RebuiltInstanceServesTheWindowBeforeTheRollResumes
// pins the rule under RecreateInstanceOnPodRestart, where the promoted
// pod's one death rebuilds its Instance instead of leaving the kubelet's
// restart in place: the rebuilt pod is created inside the window after
// the failure the row recorded, so it is the set's comeback and holds the
// slot until it has held Ready for the window since its readiness moved.
// A pass inside that window names the Instance in a Budget hold, opens no
// start and drains no source; the first pass past it releases the slot,
// and the roll lands the revision on every Instance.
func TestCrashOnceAfterPromotion_RebuiltInstanceServesTheWindowBeforeTheRollResumes(t *testing.T) {
	cases := []struct {
		name     string
		strategy workloadtypes.UpdateStrategyType
	}{
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1", strategy: workloadtypes.UpdateStrategySurgeThenDrain},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1", strategy: workloadtypes.UpdateStrategyRecreatePod},
	}
	const replicas = int32(4)
	// No minReadySeconds: the stuck-pod grace alone is the window.
	const window = 90 * time.Second
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
			h.replicas = replicas
			rebuild := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &rebuild
			// The promoted pod serves long enough for the roll to reach the
			// next Instance before it dies.
			h.crashAfterServed = 4
			h.stuckGrace = window
			h.useRollingBudget(tc.strategy)
			if !h.run(80, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}

			revOnce := h.crashRevision(crashOnceAfterServingImage)
			started := map[int32]struct{}{}
			// passStart is the clock the pass under inspection ran at: the
			// invariant runs after the step moved the clock on.
			passStart := h.clk.Now()
			holds, starts, off := h.notServingHolds(), 0, int(replicas)-h.instancesServingOn(goodImage)
			rebuilt := false
			h.runWithInvariant(200, func() bool { return h.settledOn(revOnce, replicas) }, func() {
				h.rollStartsOn(crashOnceAfterServingImage, revOnce, started)
				offNow := int(replicas) - h.instancesServingOn(goodImage)
				for idx, pod := range h.rebuiltAfterFailure(revOnce) {
					if !podConditionTrue(pod, corev1.PodReady) {
						continue
					}
					rebuilt = true
					availableAt := podReadyTransition(pod).Add(window)
					moved := len(started) > starts || offNow > off
					// The pass's hold speaks for this Instance only when it
					// names it: another Instance's rebuild may be inside its
					// own window while this one has served its.
					var hold *workloadtypes.RolloutHold
					if h.notServingHolds() > holds {
						hold = h.lastHold()
					}
					switch held := holdNamesInstance(hold, idx); {
					case held && !passStart.Before(availableAt):
						h.dumpState("held while serving")
						t.Fatalf("the roll was held on Instance %d, rebuilt and Ready since %s, at %s: a rebuild that has held Ready for the window serves and releases its slot: %q",
							idx, podReadyTransition(pod).Format(time.RFC3339), passStart.Format(time.RFC3339), hold.Reason)
					case held:
						if hold.Gate != workloadtypes.RolloutHoldGateBudget || hold.Target != revOnce.Name {
							t.Fatalf("a pass inside the window must report a Budget hold on %s naming Instance %d, got %+v", revOnce.Name, idx, hold)
						}
						if moved {
							h.dumpState("start inside the window")
							t.Fatalf("a start opened or a source left rotation while the rebuild of Instance %d was held inside its window", idx)
						}
					case passStart.Before(availableAt) && moved:
						h.dumpState("start inside the window")
						t.Fatalf("a start opened at %s while the rebuild of Instance %d had been Ready only since %s, window %s",
							passStart.Format(time.RFC3339), idx, podReadyTransition(pod).Format(time.RFC3339), window)
					}
				}
				holds, starts, off = h.notServingHolds(), len(started), offNow
				passStart = h.clk.Now()
			})
			if !rebuilt {
				h.dumpState("no rebuild")
				t.Fatalf("no Instance was rebuilt after its promoted pod died; the story did not run")
			}
			if !h.settledOn(revOnce, replicas) {
				h.dumpState("roll stalled")
				t.Fatalf("the roll did not land %s on every Instance after the rebuild served the window; last hold %+v", revOnce.Name, h.lastHold())
			}
		})
	}
}

// TestCrashAfterPromotion_RebuiltInstanceDiesAgainInsideTheWindow_NothingStarts
// pins the push whose rebuilt Instances keep dying under
// RecreateInstanceOnPodRestart: every rebuild is created inside the
// window after the failure the row recorded, so each is the set's
// comeback and none serves the roll before it dies again. Through the
// window after that failure no further Instance leaves the running
// revision: at most the budget, plus the one start admitted while the
// promoted pod still looked healthy, is off it, the running revision
// keeps serving on the rest, and a corrected revision lands everywhere.
func TestCrashAfterPromotion_RebuiltInstanceDiesAgainInsideTheWindow_NothingStarts(t *testing.T) {
	cases := []struct {
		name     string
		strategy workloadtypes.UpdateStrategyType
		// offAllowed bounds the Instances off the running revision at any
		// pass: the budget under SurgeThenDrain, where no source leaves
		// rotation beside the promoted crash; one more under RecreatePod,
		// for the start admitted while the promoted pod still served.
		offAllowed int
	}{
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1", strategy: workloadtypes.UpdateStrategySurgeThenDrain, offAllowed: 1},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1", strategy: workloadtypes.UpdateStrategyRecreatePod, offAllowed: 2},
	}
	const replicas = int32(4)
	// No minReadySeconds: the stuck-pod grace alone is the window, long
	// enough that a rebuild serves its passes and dies inside it.
	const window = 300 * time.Second
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
			h.replicas = replicas
			rebuild := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &rebuild
			h.crashAfterServed = 4
			h.stuckGrace = window
			h.useRollingBudget(tc.strategy)
			if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}

			revCrash := h.crashRevision(crashAfterServingImage)
			started := map[int32]struct{}{}
			var failedAt time.Time
			offAtCrash, startsAtCrash, rebuilt := 0, 0, false
			windowPassed := func() bool { return !failedAt.IsZero() && !h.clk.Now().Before(failedAt.Add(window)) }
			h.runWithInvariant(60, windowPassed, func() {
				h.rollStartsOn(crashAfterServingImage, revCrash, started)
				off := int(replicas) - h.instancesServingOn(goodImage)
				if off > tc.offAllowed {
					h.dumpState("roll past the allowance")
					t.Fatalf("%d Instances are off the running revision, allowance %d: the roll kept replacing Instances with a revision that keeps crashing", off, tc.offAllowed)
				}
				if len(h.rebuiltAfterFailure(revCrash)) > 0 {
					rebuilt = true
				}
				if failedAt.IsZero() {
					if at := h.firstFailureAt(revCrash); !at.IsZero() {
						failedAt, offAtCrash, startsAtCrash = at, off, len(started)
					}
					return
				}
				if windowPassed() {
					return
				}
				if off > offAtCrash {
					h.dumpState("roll continued inside the window")
					t.Fatalf("a further Instance left the running revision inside the window after a promoted Instance failed: %d off now, %d at the failure", off, offAtCrash)
				}
				if len(started) > startsAtCrash {
					h.dumpState("start inside the window")
					t.Fatalf("a start opened inside the window after a promoted Instance failed: %d Instances started, %d at the failure", len(started), startsAtCrash)
				}
			})
			if failedAt.IsZero() || !rebuilt {
				h.dumpState("no rebuild")
				t.Fatalf("no promoted Instance failed and was rebuilt; the story did not run")
			}
			if floor := int(replicas) - tc.offAllowed; h.instancesServingOn(goodImage) < floor {
				h.dumpState("floor")
				t.Fatalf("the running revision serves on %d Instances, floor %d", h.instancesServingOn(goodImage), floor)
			}

			h.setTarget(h.revFixed, fixedImage)
			if !h.run(150, func() bool { return h.settledOn(h.revFixed, replicas) }) {
				h.dumpState("corrected revision")
				t.Fatalf("the corrected revision did not land on every Instance")
			}
		})
	}
}

// TestCorrectedPushAfterAHeldCrash_Converges pins the push that corrects
// a revision the roll is holding on: the hold applies only to promoted
// sets of the revision being rolled, measured since their newest restart
// or rebuild, so a failure of a set on the superseded revision, whether
// the crash the policy rebuilt it for or the crash loop the escalation
// failed it for, never holds the new roll. An Instance promoted onto the
// corrected revision whose pods carry no restart is serving the moment
// it is Ready: under every strategy and restart policy, and whether the
// crashing pod stayed down or came back between its crashes, no pass
// reports a hold naming such an Instance, and the corrected revision
// lands on every Instance.
func TestCorrectedPushAfterAHeldCrash_Converges(t *testing.T) {
	cases := []struct {
		name     string
		strategy workloadtypes.UpdateStrategyType
		policy   workloadtypes.RestartPolicy
		image    string
	}{
		{name: "SurgeThenDrain maxSurge 1, restart policy None, the pod stays down", strategy: workloadtypes.UpdateStrategySurgeThenDrain, policy: workloadtypes.RestartPolicyNone, image: crashAfterServingImage},
		{name: "SurgeThenDrain maxSurge 1, restart policy None, the pod comes back between crashes", strategy: workloadtypes.UpdateStrategySurgeThenDrain, policy: workloadtypes.RestartPolicyNone, image: flapAfterServingImage},
		{name: "RecreatePod maxUnavailable 1, restart policy None, the pod stays down", strategy: workloadtypes.UpdateStrategyRecreatePod, policy: workloadtypes.RestartPolicyNone, image: crashAfterServingImage},
		{name: "RecreatePod maxUnavailable 1, restart policy None, the pod comes back between crashes", strategy: workloadtypes.UpdateStrategyRecreatePod, policy: workloadtypes.RestartPolicyNone, image: flapAfterServingImage},
		{name: "SurgeThenDrain maxSurge 1, RecreateInstanceOnPodRestart", strategy: workloadtypes.UpdateStrategySurgeThenDrain, policy: workloadtypes.RestartPolicyRecreateInstance, image: crashAfterServingImage},
		{name: "RecreatePod maxUnavailable 1, RecreateInstanceOnPodRestart", strategy: workloadtypes.UpdateStrategyRecreatePod, policy: workloadtypes.RestartPolicyRecreateInstance, image: crashAfterServingImage},
	}
	const replicas = int32(4)
	// No minReadySeconds: the stuck-pod grace alone is the window, long
	// enough that a hold misread onto the corrected revision would stall
	// its roll for the whole run, and that the crashing pod is escalated
	// while it is held.
	const window = 300 * time.Second
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
			h.replicas = replicas
			policy := tc.policy
			h.lifecycle.RestartPolicy = &policy
			h.crashAfterServed = 4
			h.stuckGrace = window
			h.useRollingBudget(tc.strategy)
			if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}

			revCrash := h.pushRevision(tc.image)
			// The roll is holding once a promoted pod has crashed and either a
			// pass reported the hold or the policy has rebuilt the Instance;
			// the correction follows the crash loop, as an operator's does,
			// so every Instance the roll had promoted onto the crashing
			// revision has died on it before the corrected revision exists.
			held := func() bool {
				return (len(h.rebuiltAfterFailure(revCrash)) > 0 || (h.promotedPodCrashed(revCrash) && h.notServingHolds() > 0)) &&
					h.everyPromotedCrashed(revCrash)
			}
			if !h.run(60, held) {
				h.dumpState("no hold")
				t.Fatalf("the roll never held on the crashing revision; the story did not run")
			}
			if tc.policy == workloadtypes.RestartPolicyNone {
				// Under restart policy None the escalation is what records the
				// superseded set's failure, so the window passes before the
				// correction is pushed; the policy's rebuild recorded it at the
				// first crash, and the correction follows inside the window.
				h.run(12, func() bool { return false })
			}

			revCorrected := h.pushRevision(correctedImage)
			// A hold speaks for the state the pass read: only a hold reported
			// this pass that names an Instance already promoted onto the
			// corrected revision before the pass, with its pod Ready and no
			// restart on it, holds a fresh set; a set still coming up holds
			// as any start does. The failure record carries no revision, so
			// the roll places a failure by when it was recorded: one older
			// than the corrected revision is a superseded set's and never
			// holds; one recorded after the corrected revision existed is
			// read as the set's own, and holds for one window at most.
			holds := len(h.holds)
			promoted := map[int32]bool{}
			h.runWithInvariant(80, func() bool { return h.settledOn(revCorrected, replicas) }, func() {
				reported := len(h.holds) > holds
				holds = len(h.holds)
				hold := h.lastHold()
				if reported && hold.Target == revCorrected.Name {
					for _, s := range h.irStatuses() {
						if !promoted[s.Index] || !holdNamesInstance(hold, s.Index) {
							continue
						}
						if s.LastFailure != nil && s.LastFailure.Time.After(revCorrected.CreationTimestamp.Time) {
							continue
						}
						for _, pod := range h.podsOf(s.Index) {
							if _, restarted := workloadops.RunnerRestartedSinceReady(pod, s.ReadySince); restarted {
								continue
							}
							h.dumpState("corrected set held")
							t.Fatalf("Instance %d, promoted onto the corrected revision with no restart on its pod, is held by the roll: %q", s.Index, hold.Reason)
						}
					}
				}
				for _, s := range h.irStatuses() {
					ready := s.RunningRevision == revCorrected.Name && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil
					for _, pod := range h.podsOf(s.Index) {
						ready = ready && podConditionTrue(pod, corev1.PodReady)
					}
					promoted[s.Index] = ready && len(h.podsOf(s.Index)) > 0
				}
			})
			if !h.settledOn(revCorrected, replicas) {
				h.dumpState("corrected revision")
				t.Fatalf("the corrected revision did not land on every Instance; last hold %+v", h.lastHold())
			}
		})
	}
}

// TestCrashAfterPromotion_TheHoldIsReportedOnEveryPassWhileTheRollWaits
// pins that every wait says why: while the roll is held on account of a
// promoted set that is unproven or being rebuilt, every pass that
// reports an update verdict and admits no start reports the hold naming
// that set. Under RecreateInstanceOnPodRestart the policy's rebuild keeps
// an attempt open beside the held roll, and an attempt that merely polls
// is not forward progress: the hold stays reported through it. Under
// SurgeThenDrain the withheld drain is the reported hold.
func TestCrashAfterPromotion_TheHoldIsReportedOnEveryPassWhileTheRollWaits(t *testing.T) {
	cases := []struct {
		name     string
		strategy workloadtypes.UpdateStrategyType
	}{
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1", strategy: workloadtypes.UpdateStrategySurgeThenDrain},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1", strategy: workloadtypes.UpdateStrategyRecreatePod},
	}
	const replicas = int32(4)
	// No minReadySeconds: the stuck-pod grace alone is the window, long
	// enough that the rebuilds cycle several times inside it.
	const window = 300 * time.Second
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
			h.replicas = replicas
			rebuild := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &rebuild
			h.crashAfterServed = 4
			h.stuckGrace = window
			h.useRollingBudget(tc.strategy)
			if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}

			revCrash := h.crashRevision(crashAfterServingImage)
			started := map[int32]struct{}{}
			var failedAt time.Time
			verdicts, holds, starts, silent, named := h.holdVerdicts, len(h.holds), 0, 0, false
			windowPassed := func() bool { return !failedAt.IsZero() && !h.clk.Now().Before(failedAt.Add(window)) }
			h.runWithInvariant(60, windowPassed, func() {
				h.rollStartsOn(crashAfterServingImage, revCrash, started)
				reported, admitted := h.holdVerdicts > verdicts, len(started) > starts
				if len(h.holds) > holds {
					for _, s := range h.irStatuses() {
						if s.RunningRevision == revCrash.Name && holdNamesInstance(h.lastHold(), s.Index) {
							named = true
						}
					}
				}
				if !failedAt.IsZero() && !windowPassed() && reported && !admitted && len(h.holds) == holds {
					silent++
					h.dumpState("silent wait")
				}
				verdicts, holds, starts = h.holdVerdicts, len(h.holds), len(started)
				if failedAt.IsZero() {
					failedAt = h.firstFailureAt(revCrash)
				}
			})
			if failedAt.IsZero() {
				h.dumpState("no failure")
				t.Fatalf("no promoted Instance failed; the story did not run")
			}
			if !named {
				h.dumpState("hold")
				t.Fatalf("no pass reported a hold naming an Instance promoted onto %s", revCrash.Name)
			}
			if silent > 0 {
				t.Fatalf("%d pass(es) reported an update verdict with no hold while the roll was held and admitted no start; every wait says why", silent)
			}
		})
	}
}

// Node loss under a serving Instance. The kubelet under one of the
// Component's nodes stops: the node model freezes the pods on it, the
// node's Ready condition goes Unknown with a transition time, and the
// control plane withdraws PodReady from those pods while their own last
// report still says Ready. With the force-delete policy configured, the
// Instance must be rebuilt on other nodes once the node has been
// unreachable for the threshold and serve on its running revision, with
// no graceful delete ever issued on the dead node (no kubelet is there to
// honor one), and the pods the node held must be gone when its kubelet
// returns. The peer Instance on a live node stays in rotation throughout.
// Without the policy nothing is rebuilt: the Instance waits for its node.

const (
	nodeLossThreshold = 90 * time.Second
	nodeLossSlack     = 30 * time.Second
	nodeLossDeadNode  = "node-a"
)

// nodeLossStory is one shape of the node-loss story: the pod shape and the
// restart policy, which together decide whether the rebuild is the
// restart pass's whole-Instance repair or the Create pass's missing-pod
// self-heal.
type nodeLossStory struct {
	name     string
	multiPod bool
	policy   workloadtypes.RestartPolicy
}

func nodeLossStories() []nodeLossStory {
	return []nodeLossStory{
		{"single pod under None", false, workloadtypes.RestartPolicyNone},
		{"single pod under RecreateInstanceOnPodRestart", false, workloadtypes.RestartPolicyRecreateInstance},
		{"gang under RecreateInstanceOnPodRestart", true, workloadtypes.RestartPolicyRecreateInstance},
		{"gang under None", true, workloadtypes.RestartPolicyNone},
	}
}

// newNodeLossHarness is the harness with two Instances converged on v1
// over four nodes. The node model spreads pods one per node in creation
// order, so Instance 0 sits on node-a (its gang spanning node-a and
// node-b) and Instance 1 on nodes the failure never touches.
func newNodeLossHarness(t *testing.T, story nodeLossStory, policy *workloadtypes.ForceDeletePolicy) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, story.multiPod)
	h.replicas = 2
	restart := story.policy
	h.lifecycle.RestartPolicy = &restart
	h.podGrace = 30 * time.Second
	h.forceDelete = policy
	h.recorder = record.NewFakeRecorder(256)
	h.useNodes(nodeLossDeadNode, "node-b", "node-c", "node-d")
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.settledOn(h.revV1, 2) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on v1")
	}
	if on := h.podsOnNode(nodeLossDeadNode); len(on) != 1 || !strings.Contains(on[0].Name, "-engine-0-") {
		h.dumpState("placement")
		t.Fatalf("node model placed %d pod(s) on %s, want exactly one pod of Instance 0", len(on), nodeLossDeadNode)
	}
	return h
}

// podIdentities names pod objects across passes the way the kubelet model
// tells a rebuilt pod from the one it replaced: name plus the creation
// instant the model stamped.
func podIdentities(pods []*corev1.Pod) map[string]bool {
	out := make(map[string]bool, len(pods))
	for _, pod := range pods {
		out[wedgeKey(pod)] = true
	}
	return out
}

// requirePeerInRotation is the per-step invariant of every node-loss
// story: Instance 1 keeps the very pods it had, each live, serving and
// PodReady, and its row is untouched.
func (h *recoveryHarness) requirePeerInRotation(peer map[string]bool, incarnation int64) {
	h.t.Helper()
	pods := h.podsOf(1)
	if len(pods) != len(peer) {
		h.dumpState("peer")
		h.t.Fatalf("peer Instance 1 has %d live pod(s), want its original %d", len(pods), len(peer))
	}
	for _, pod := range pods {
		if !peer[wedgeKey(pod)] {
			h.t.Fatalf("peer pod %s was replaced; the untouched Instance must keep its pods", pod.Name)
		}
		if !podreadiness.IsServing(pod) || !podConditionTrue(pod, corev1.PodReady) {
			h.t.Fatalf("peer pod %s left the rotation", pod.Name)
		}
	}
	if s := h.instance(1); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.Incarnation != incarnation {
		h.t.Fatalf("peer Instance 1 changed: %+v", s)
	}
}

// requireNoGracefulDeleteOn fails when a pod bound to node is Terminating:
// a graceful delete issued there would wait on a kubelet that is gone.
func (h *recoveryHarness) requireNoGracefulDeleteOn(node string) {
	h.t.Helper()
	for _, pod := range h.podsOnNode(node) {
		if pod.DeletionTimestamp != nil {
			h.t.Fatalf("pod %s was deleted gracefully on the dead node %s; only a force delete can free it", pod.Name, node)
		}
	}
}

// servingElsewhere reports whether Instance 0 is Ready with no operation on
// its running revision, every pod of it live, serving and PodReady on a
// node other than the dead one.
func (h *recoveryHarness) servingElsewhere(deadNode string) bool {
	s := h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != h.revV1.Name {
		return false
	}
	pods := h.podsOf(0)
	if len(pods) != len(h.desired.Runners) {
		return false
	}
	for _, pod := range pods {
		if pod.Spec.NodeName == deadNode || !podreadiness.IsServing(pod) || !podConditionTrue(pod, corev1.PodReady) {
			return false
		}
	}
	return true
}

func TestNodeLoss_ServingInstanceIsRebuiltElsewhereUnderTheForceDeletePolicy(t *testing.T) {
	for _, story := range nodeLossStories() {
		t.Run(story.name, func(t *testing.T) {
			h := newNodeLossHarness(t, story, &workloadtypes.ForceDeletePolicy{
				NodeUnreachableThreshold: nodeLossThreshold,
				OverdueSlack:             nodeLossSlack,
			})
			peer := podIdentities(h.podsOf(1))
			peerIncarnation := h.instance(1).Incarnation
			before := h.instance(0).Incarnation
			invariant := func() {
				h.requirePeerInRotation(peer, peerIncarnation)
				h.requireNoGracefulDeleteOn(nodeLossDeadNode)
			}

			h.failNode(nodeLossDeadNode)
			failedAt := h.clk.Now()

			// The names are freed at the threshold: the pass that observes
			// the withdrawn Ready requeues no later than the evidence
			// boundary, and the pass at the boundary force-deletes.
			freed := func() bool { return h.sawEvent(workloadtypes.EventReasonPodForceDeleted) }
			if !h.runWithInvariant(10, freed, invariant) {
				h.dumpState("node down past the threshold")
				t.Fatalf("the pods on %s were never force-deleted after the node went silent under a configured policy", nodeLossDeadNode)
			}
			if elapsed := h.clk.Now().Sub(failedAt); elapsed > nodeLossThreshold+recoveryStepFloor {
				t.Fatalf("stale pods freed %v after the node went silent, want within the %v threshold plus one pass", elapsed, nodeLossThreshold)
			}
			if on := h.podsOnNode(nodeLossDeadNode); len(on) != 0 {
				t.Fatalf("%d pod object(s) still bound to %s after the force delete", len(on), nodeLossDeadNode)
			}

			if !h.runWithInvariant(40, func() bool { return h.servingElsewhere(nodeLossDeadNode) }, invariant) {
				h.dumpState("rebuild off the dead node")
				t.Fatalf("Instance 0 never came back Ready and serving on another node while the kubelet of %s was down", nodeLossDeadNode)
			}
			h.requirePodsRender(0, h.revV1, goodImage)
			if s := h.instance(0); story.policy == workloadtypes.RestartPolicyRecreateInstance && s.Incarnation <= before {
				t.Fatalf("RecreateInstanceOnPodRestart rebuilds the whole Instance under a bumped incarnation, got %d (was %d)", s.Incarnation, before)
			} else if story.policy == workloadtypes.RestartPolicyNone && s.Incarnation != before {
				t.Fatalf("policy None re-materializes the missing pod in place, got incarnation %d (was %d)", s.Incarnation, before)
			}

			// The kubelet returns: it finds nothing of this Component to run,
			// and the rebuilt Instance stays where it is.
			rebuilt := podIdentities(h.podsOf(0))
			h.recoverNode(nodeLossDeadNode)
			for i := 0; i < 3; i++ {
				h.step()
				invariant()
			}
			if on := h.podsOnNode(nodeLossDeadNode); len(on) != 0 {
				t.Fatalf("%d stale pod object(s) on %s after its kubelet returned", len(on), nodeLossDeadNode)
			}
			if !h.settledOn(h.revV1, 2) {
				h.dumpState("after the node returned")
				t.Fatalf("the Component did not stay settled on v1 after the node returned")
			}
			for _, pod := range h.podsOf(0) {
				if !rebuilt[wedgeKey(pod)] {
					t.Fatalf("pod %s of the rebuilt Instance was replaced again after the node returned", pod.Name)
				}
			}
		})
	}
}

func TestNodeLoss_WithoutTheForceDeletePolicyTheInstanceWaitsForItsNode(t *testing.T) {
	for _, story := range nodeLossStories() {
		t.Run(story.name, func(t *testing.T) {
			h := newNodeLossHarness(t, story, nil)
			peer := podIdentities(h.podsOf(1))
			peerIncarnation := h.instance(1).Incarnation
			stale := podIdentities(h.podsOf(0))
			before := h.instance(0).Incarnation

			h.failNode(nodeLossDeadNode)
			failedAt := h.clk.Now()
			// Long past any threshold a policy would carry.
			for h.clk.Now().Sub(failedAt) < 10*nodeLossThreshold {
				h.step()
				h.requirePeerInRotation(peer, peerIncarnation)
				h.requireNoGracefulDeleteOn(nodeLossDeadNode)
			}
			if h.sawEvent(workloadtypes.EventReasonPodForceDeleted) || h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
				t.Fatalf("without a force-delete policy nothing may free or rebuild the Instance; events: %v", h.events)
			}
			if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.Incarnation != before {
				t.Fatalf("Instance 0 changed while parked on its dead node: %+v", s)
			}
			pods := h.podsOf(0)
			if len(pods) != len(stale) {
				t.Fatalf("Instance 0 has %d pod(s), want its original %d parked on the dead node", len(pods), len(stale))
			}
			for _, pod := range pods {
				if !stale[wedgeKey(pod)] {
					t.Fatalf("pod %s of the parked Instance was replaced", pod.Name)
				}
			}

			// The kubelet returns and reports the parked pods again: the
			// Instance is whole on the very pods it had.
			h.recoverNode(nodeLossDeadNode)
			if !h.run(5, func() bool { return h.settledOn(h.revV1, 2) && h.servingPodsOf(0) == len(h.desired.Runners) }) {
				h.dumpState("after the node returned")
				t.Fatalf("the parked Instance did not resume serving once its kubelet returned")
			}
			for _, pod := range h.podsOf(0) {
				if !stale[wedgeKey(pod)] {
					t.Fatalf("pod %s was replaced after the node returned; the parked Instance keeps its pods", pod.Name)
				}
			}
			h.requirePeerInRotation(peer, peerIncarnation)
		})
	}
}

// servingPodsOf counts the live pods of Instance idx that are serving and
// PodReady.
func (h *recoveryHarness) servingPodsOf(idx int32) int {
	n := 0
	for _, pod := range h.podsOf(idx) {
		if podreadiness.IsServing(pod) && podConditionTrue(pod, corev1.PodReady) {
			n++
		}
	}
	return n
}

// TestScaleDown_RemovesTheInstanceThatServesNothingFirst: four Instances
// at rest, and a pod of Instance 0 leaves rotation while its row stays
// Ready — its readiness probe fails, or it crash-loops under restart policy
// None with a stuck-pod grace no pass reaches. A scale-down by one removes
// Instance 0, not the healthy Instance 3; a scale-down by two removes
// Instance 0 and then Instance 3. The survivors keep the pods they had,
// nothing is rebuilt, and the rows settle at the new count. Single pod,
// and gang where the leader is the pod that leaves rotation.
func TestScaleDown_RemovesTheInstanceThatServesNothingFirst(t *testing.T) {
	shapes := []struct {
		name     string
		multiPod bool
		runner   string
	}{
		{"single pod", false, ""},
		{"gang leader", true, "leader"},
	}
	faults := []struct {
		name  string
		apply func(h *recoveryHarness, runner string)
	}{
		{"readiness probe fails", func(h *recoveryHarness, runner string) { h.failReadiness(0, runner) }},
		{"crash-loops under policy None", func(h *recoveryHarness, runner string) { h.crashPod(0, runner, crashStartImage) }},
	}
	reductions := []struct {
		name    string
		down    int32
		removed []int32
		kept    []int32
	}{
		{"down by one", 1, []int32{0}, []int32{1, 2, 3}},
		{"down by two", 2, []int32{0, 3}, []int32{1, 2}},
	}
	for _, shape := range shapes {
		for _, fault := range faults {
			for _, reduction := range reductions {
				t.Run(shape.name+"/"+fault.name+"/"+reduction.name, func(t *testing.T) {
					h := newRecoveryHarness(t, shape.multiPod)
					h.atomicStatus = true
					h.replicas = 4
					none := workloadtypes.RestartPolicyNone
					h.lifecycle.RestartPolicy = &none
					// The stuck-pod grace outlasts the story: the controller is woken
					// by the kubelet's back-off ticks, not by the grace end, so a
					// crash-looping pod opens no repair.
					h.stuckGrace = 24 * time.Hour
					h.setTarget(h.revV1, goodImage)
					if !h.run(40, func() bool { return h.settledOn(h.revV1, 4) }) {
						h.dumpState("initial create")
						t.Fatalf("four instances never settled on v1")
					}
					before := map[int32]string{}
					for _, idx := range reduction.kept {
						before[idx] = h.instanceIdentity(idx)
					}

					fault.apply(h, shape.runner)
					res, err := h.pass()
					if err != nil {
						t.Fatalf("Reconcile: %v", err)
					}
					h.clk.Step(h.sleepAfter(res, kubeletBackOff))
					row := h.instance(0)
					if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil ||
						row.PodCount == 0 || row.ServingPodCount >= row.PodCount {
						h.dumpState("after the fault")
						t.Fatalf("instance 0 after the fault = %+v, want a Ready row with a pod out of rotation", row)
					}
					if n := len(h.irStatuses()); n != 4 {
						t.Fatalf("rows after the fault = %d, want 4", n)
					}

					h.replicas = 4 - reduction.down
					h.setTarget(h.revV1, goodImage)
					atRest := func() bool {
						rows := h.irStatuses()
						if int32(len(rows)) != h.replicas {
							return false
						}
						for _, row := range rows {
							if row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil {
								return false
							}
						}
						return len(h.livePods()) == int(h.replicas)*len(h.desired.Runners)
					}
					if !h.runOnWakeUps(40, kubeletBackOff, atRest) {
						h.dumpState("scale-down")
						t.Fatalf("the component never came to rest at %d instances", h.replicas)
					}
					requireRemovedAndKept := func() {
						t.Helper()
						for _, idx := range reduction.removed {
							if h.instance(idx) != nil || len(h.podsOf(idx)) != 0 {
								h.dumpState("scale-down")
								t.Fatalf("instance %d serves nothing, or is the highest healthy index, and should have left; row=%+v pods=%d",
									idx, h.instance(idx), len(h.podsOf(idx)))
							}
						}
						for _, idx := range reduction.kept {
							if got := h.instanceIdentity(idx); got != before[idx] {
								h.dumpState("scale-down")
								t.Fatalf("instance %d should have kept its pods:\n got %q\nwant %q", idx, got, before[idx])
							}
						}
						if !h.settledOn(h.revV1, h.replicas) {
							h.dumpState("scale-down")
							t.Fatalf("the component is not settled on v1 at %d instances", h.replicas)
						}
					}
					requireRemovedAndKept()
					for i := 0; i < 4; i++ {
						h.step()
						requireRemovedAndKept()
					}
				})
			}
		}
	}
}

// TestScaleDown_RemovesTheInstanceOnACordonedNodeFirst: four healthy
// Instances, every pod on a node of its own, and the node under one pod
// of Instance 1 is cordoned. A scale-down by one removes Instance 1, not
// the highest index; a scale-down by two removes Instance 1 and then the
// highest index. The survivors keep the pods they had, nothing lands on
// the cordoned node, and the rows settle at the new count. Single pod,
// and gang where only the worker sits on the cordoned node.
func TestScaleDown_RemovesTheInstanceOnACordonedNodeFirst(t *testing.T) {
	shapes := []struct {
		name     string
		multiPod bool
		runner   string
	}{
		{"single pod", false, "default"},
		{"gang worker", true, "worker"},
	}
	reductions := []struct {
		name    string
		down    int32
		removed []int32
		kept    []int32
	}{
		{"down by one", 1, []int32{1}, []int32{0, 2, 3}},
		{"down by two", 2, []int32{1, 3}, []int32{0, 2}},
	}
	for _, shape := range shapes {
		for _, reduction := range reductions {
			t.Run(shape.name+"/"+reduction.name, func(t *testing.T) {
				h := newRecoveryHarness(t, shape.multiPod)
				h.atomicStatus = true
				h.replicas = 4
				h.useNodes("node-a", "node-b", "node-c", "node-d", "node-e", "node-f", "node-g", "node-h")
				h.setTarget(h.revV1, goodImage)
				if !h.run(40, func() bool { return h.settledOn(h.revV1, 4) }) {
					h.dumpState("initial create")
					t.Fatalf("four instances never settled on v1")
				}
				cordoned := ""
				for _, pod := range h.podsOf(1) {
					if pod.Labels[query.LabelRunner] == shape.runner {
						cordoned = pod.Spec.NodeName
					}
				}
				if cordoned == "" || len(h.podsOnNode(cordoned)) != 1 {
					h.dumpState("placement")
					t.Fatalf("story shape: the %s pod of Instance 1 needs a node of its own, got %q", shape.runner, cordoned)
				}
				before := map[int32]string{}
				for _, idx := range reduction.kept {
					before[idx] = h.instanceIdentity(idx)
				}

				h.cordonNode(cordoned)
				h.replicas = 4 - reduction.down
				h.setTarget(h.revV1, goodImage)
				atRest := func() bool {
					rows := h.irStatuses()
					if int32(len(rows)) != h.replicas {
						return false
					}
					for _, row := range rows {
						if row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil {
							return false
						}
					}
					return len(h.livePods()) == int(h.replicas)*len(h.desired.Runners)
				}
				if !h.run(40, atRest) {
					h.dumpState("scale-down")
					t.Fatalf("the component never came to rest at %d instances", h.replicas)
				}
				for _, idx := range reduction.removed {
					if h.instance(idx) != nil || len(h.podsOf(idx)) != 0 {
						h.dumpState("scale-down")
						t.Fatalf("instance %d should have left; row=%+v pods=%d", idx, h.instance(idx), len(h.podsOf(idx)))
					}
				}
				for _, idx := range reduction.kept {
					if got := h.instanceIdentity(idx); got != before[idx] {
						h.dumpState("scale-down")
						t.Fatalf("instance %d should have kept its pods:\n got %q\nwant %q", idx, got, before[idx])
					}
				}
				if on := h.podsOnNode(cordoned); len(on) != 0 {
					h.dumpState("scale-down")
					t.Fatalf("%d pod(s) still on the cordoned node %s", len(on), cordoned)
				}
				if !h.settledOn(h.revV1, h.replicas) {
					h.dumpState("scale-down")
					t.Fatalf("the component is not settled on v1 at %d instances", h.replicas)
				}
			})
		}
	}
}

// TestNodeLoss_ServingFollowsThePodReadyCondition: the kubelet under a
// serving Instance stops. The node lifecycle controller sets the pod's
// Ready condition False, while the container statuses and the serving
// gate, last written by that kubelet and by this controller, keep their
// values. On the next pass the Instance stops counting as serving: its
// serving count drops, the Component's serving replicas drop, its peers
// are untouched, and nothing is rebuilt, because no restart policy frees
// the names a dead node holds without a force-delete policy. Single pod
// and gang, under both restart policies.
func TestNodeLoss_ServingFollowsThePodReadyCondition(t *testing.T) {
	recreate := workloadtypes.RestartPolicyRecreateInstance
	none := workloadtypes.RestartPolicyNone
	cases := []struct {
		name     string
		multiPod bool
		runner   string
		policy   workloadtypes.RestartPolicy
	}{
		{"single pod under policy None", false, "", none},
		{"single pod under RecreateInstanceOnPodRestart", false, "", recreate},
		{"gang worker under policy None", true, "worker", none},
		{"gang leader under RecreateInstanceOnPodRestart", true, "leader", recreate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarness(t, tc.multiPod)
			h.replicas = 2
			policy := tc.policy
			h.lifecycle.RestartPolicy = &policy
			h.setTarget(h.revV1, goodImage)
			if !h.run(40, func() bool { return h.settledOn(h.revV1, 2) }) {
				h.dumpState("initial create")
				t.Fatalf("two instances never settled on v1")
			}
			perInstance := int32(len(h.desired.Runners))
			rows, counters := h.publication()
			for _, row := range rows {
				if row.ServingPodCount != perInstance {
					t.Fatalf("instance %d serves %d of %d pods before the fault", row.Index, row.ServingPodCount, perInstance)
				}
			}
			if counters.ServingReplicas != 2 {
				t.Fatalf("serving replicas before the fault = %d, want 2", counters.ServingReplicas)
			}
			pods := h.podSetSignature()
			incarnation := h.instance(0).Incarnation

			h.loseNodeUnder(0, tc.runner)
			h.step()

			requireServingDropped := func() {
				t.Helper()
				rows, counters := h.publication()
				if got := h.publishedRow(rows, 0).ServingPodCount; got != perInstance-1 {
					t.Fatalf("instance 0 serving pods = %d, want %d: the pod the control plane marked not Ready still counts as serving", got, perInstance-1)
				}
				if got := h.publishedRow(rows, 1).ServingPodCount; got != perInstance {
					t.Fatalf("instance 1 serving pods = %d, want %d: the peer is untouched by the lost node", got, perInstance)
				}
				if counters.ServingReplicas != 1 {
					t.Fatalf("serving replicas = %d, want 1", counters.ServingReplicas)
				}
			}
			requireNothingRebuilt := func() {
				t.Helper()
				for _, s := range h.irStatuses() {
					if s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil {
						h.dumpState("after the node loss")
						t.Fatalf("instance %d: phase=%s op=%+v, want Ready with no operation", s.Index, s.Phase, s.Operation)
					}
				}
				if got := h.instance(0).Incarnation; got != incarnation {
					t.Fatalf("instance 0 incarnation = %d, want %d: nothing frees a dead node's pods without a force-delete policy", got, incarnation)
				}
				if got := h.podSetSignature(); got != pods {
					t.Fatalf("pod set changed:\n got %s\nwant %s", got, pods)
				}
			}
			for _, pod := range h.podsOf(0) {
				if !h.kubeletGone[pod.Name] {
					continue
				}
				if !podConditionTrue(pod, corev1.ContainersReady) || !podreadiness.IsServing(pod) || podConditionTrue(pod, corev1.PodReady) {
					t.Fatalf("pod %s: ContainersReady=%v serving=%v Ready=%v, want the kubelet's and the controller's last writes under a revoked Ready",
						pod.Name, podConditionTrue(pod, corev1.ContainersReady), podreadiness.IsServing(pod), podConditionTrue(pod, corev1.PodReady))
				}
			}
			requireServingDropped()
			requireNothingRebuilt()
			for i := 0; i < 6; i++ {
				h.step()
				requireServingDropped()
				requireNothingRebuilt()
			}
		})
	}
}

// restartEvents counts the RestartTriggered events the harness has
// recorded so far.
func (h *recoveryHarness) restartEvents() int {
	n := 0
	for _, e := range h.events {
		if strings.Contains(e, string(workloadtypes.EventReasonRestartTriggered)) {
			n++
		}
	}
	return n
}

// A freeze that lands before a crash loop opens no repair, and lifting
// the freeze is what opens it: the pass after the lift reads the same
// crash evidence the frozen passes declined, records the repair's event
// and rebuilds the Instance at its running revision. The pod's own
// kubelet evidence dates the wedge, so no fresh pod change is needed for
// the repair to open once the freeze is gone. Single pod under
// RestartPolicy None, the policy whose only repair is the crash-loop one.
func TestPauseFreeze_LiftedOverACrashLoop_OpensTheRepairAtOnce(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.recorder = record.NewFakeRecorder(64)
	h.driveToReadyOnV1()
	beforeIncarnation := h.instance(0).Incarnation
	crashed := h.podsOf(0)[0].UID

	h.desired.Paused, h.desired.PauseFreeze = true, true
	h.crashPod(0, "", crashStartImage)
	// The frozen passes outlive the stuck-pod grace, so the wedge is one
	// the restart pass would act on and the freeze is all that withholds it.
	for i := 0; i < 4; i++ {
		h.step()
	}
	if h.restartEvents() != 0 {
		t.Fatalf("a frozen pass opened a repair: events = %v", h.events)
	}
	if pods := h.podsOf(0); len(pods) != 1 || pods[0].UID != crashed {
		t.Fatalf("the frozen passes touched the crash-looping pod: %d live pod(s)", len(pods))
	}
	if s := h.instance(0); s == nil || s.Operation != nil || s.Incarnation != beforeIncarnation {
		t.Fatalf("the frozen passes changed the row: %+v", s)
	}

	h.desired.Paused, h.desired.PauseFreeze = false, false
	h.step()
	if h.restartEvents() != 1 {
		t.Fatalf("the first pass after the lift recorded %d RestartTriggered event(s), want 1; events = %v", h.restartEvents(), h.events)
	}
	if !h.run(20, func() bool { return h.repairedAbove(0, beforeIncarnation) }) {
		h.dumpState("after the lift")
		t.Fatalf("the repair the lift opened never rebuilt the Instance to Ready")
	}
	if s := h.instance(0); s.RunningRevision != h.revV1.Name {
		t.Fatalf("repaired row records %q, want the running revision %s", s.RunningRevision, h.revV1.Name)
	}
	h.requirePodsRender(0, h.revV1, goodImage)
	if h.restartEvents() != 1 {
		t.Fatalf("the Instance was repaired %d times, want once; events = %v", h.restartEvents(), h.events)
	}
}

// A freeze that lands on a gang whose Restart is already open does not
// strand the gang: the open repair runs on under the freeze until the
// gang is rebuilt, Ready and serving at its running revision, and lifting
// the freeze afterwards changes nothing — no second repair, the same
// incarnation.
func TestPauseFreeze_LandsOnAnOpenGangRestart_TheRepairFinishesUnderIt(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.recorder = record.NewFakeRecorder(64)
	policy := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &policy
	h.driveToReadyOnV1()
	beforeIncarnation := h.instance(0).Incarnation

	h.loseRunner(0, "worker")
	if !h.run(5, h.repairInFlight) {
		h.dumpState("after the loss")
		t.Fatalf("the lost member opened no repair")
	}
	h.desired.Paused, h.desired.PauseFreeze = true, true
	if !h.run(20, func() bool { return h.repairedAbove(0, beforeIncarnation) }) {
		h.dumpState("frozen with the repair open")
		t.Fatalf("the repair that was open when the freeze landed never finished under it")
	}
	h.requirePodsRender(0, h.revV1, goodImage)
	repairedIncarnation := h.instance(0).Incarnation
	if h.restartEvents() != 1 {
		t.Fatalf("%d RestartTriggered event(s) under the freeze, want the one that was open; events = %v", h.restartEvents(), h.events)
	}

	h.desired.Paused, h.desired.PauseFreeze = false, false
	h.settle(4)
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.Incarnation != repairedIncarnation {
		t.Fatalf("the lift moved the repaired gang: %+v", s)
	}
	if !h.settledOn(h.revV1, 1) {
		h.dumpState("after the lift")
		t.Fatalf("the gang did not stay settled on its running revision after the lift")
	}
	if h.restartEvents() != 1 {
		t.Fatalf("the lift opened another repair; events = %v", h.events)
	}
}

// A pause that lands on a gang whose Restart is open, with a fix pushed
// during the hold: the repair finishes under the pause at the revision
// the gang was running, the pushed revision reaches no pod until the
// unpause, and the unpause rolls the repaired gang to the fix with no
// second repair of the revision it was running.
func TestPause_LandsOnAnOpenGangRestart_APushDuringTheHoldRollsItAfterTheUnpause(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.recorder = record.NewFakeRecorder(64)
	policy := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &policy
	one := intstr.FromInt32(1)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategySurgeThenDrain,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &one, MaxUnavailable: &one},
	}
	h.driveToReadyOnV1()
	beforeIncarnation := h.instance(0).Incarnation

	h.loseRunner(0, "worker")
	if !h.run(5, h.repairInFlight) {
		h.dumpState("after the loss")
		t.Fatalf("the lost member opened no repair")
	}
	h.desired.Paused = true
	h.setTarget(h.revFixed, fixedImage)
	h.desired.Paused = true
	if !h.run(20, func() bool { return h.repairedAbove(0, beforeIncarnation) }) {
		h.dumpState("paused with the repair open and the fix pushed")
		t.Fatalf("the repair that was open when the pause landed never finished under it")
	}
	h.requirePodsRender(0, h.revV1, goodImage)
	h.settle(3)
	if h.livePodOnImage(fixedImage) {
		t.Fatalf("the fix pushed under the pause reached a pod before the unpause")
	}
	if h.restartEvents() != 1 {
		t.Fatalf("%d RestartTriggered event(s) under the pause, want the one that was open; events = %v", h.restartEvents(), h.events)
	}

	h.desired.Paused = false
	if !h.run(80, func() bool { return h.settledOn(h.revFixed, 1) }) {
		h.dumpState("after the unpause")
		t.Fatalf("the unpause did not roll the repaired gang to the pushed revision")
	}
	if h.restartEvents() != 1 {
		t.Fatalf("the roll after the unpause repaired the gang again; events = %v", h.events)
	}
}

// A pod the scheduler preempts on a settled Component comes back, and the
// row says why for as long as the pod cannot be placed. The row leaves
// Ready in the pass that observes the loss; the replacement is created as
// soon as the name is free, under restart policy None as under
// RecreateInstanceOnPodRestart; while the cluster has no room the row
// carries the Unschedulable wait with the scheduler's message and reads
// Failed at no point inside the configured grace; the Instance the
// scheduler did not take keeps serving on the pod it had; once the room
// is back the pod that waited is placed and the row returns Ready with no
// operation open. Every Component runs the same loop, so the story runs
// for the engine, the decoder and the router, with the kubelet publishing
// the stopped pod as Succeeded first and with the pod gone at once.
func TestPreemptedPod_ReturnsAndSaysWhyUntilPlaced(t *testing.T) {
	const noRoom = "0/3 nodes are available: 1 node(s) had untolerated taint(s), 2 Insufficient cpu."
	components := []workloadtypes.ComponentType{workloadtypes.ComponentEngine, workloadtypes.ComponentDecoder, workloadtypes.ComponentRouter}
	stories := []struct {
		name         string
		policy       workloadtypes.RestartPolicy
		stoppedFirst bool
	}{
		{"policy None, the kubelet reports the stopped pod first", workloadtypes.RestartPolicyNone, true},
		{"policy None, the pod is gone at once", workloadtypes.RestartPolicyNone, false},
		{"RecreateInstanceOnPodRestart, the kubelet reports the stopped pod first", workloadtypes.RestartPolicyRecreateInstance, true},
		{"RecreateInstanceOnPodRestart, the pod is gone at once", workloadtypes.RestartPolicyRecreateInstance, false},
	}
	for _, component := range components {
		for _, story := range stories {
			t.Run(string(component)+"/"+story.name, func(t *testing.T) {
				h := newComponentRecoveryHarness(t, false, "main", component)
				h.recorder = record.NewFakeRecorder(64)
				h.replicas = 2
				policy := story.policy
				h.lifecycle.RestartPolicy = &policy
				h.unschedulableGrace = 15 * time.Minute
				if story.stoppedFirst {
					h.podGrace = 30 * time.Second
				}
				h.setTarget(h.revV1, goodImage)
				if !h.run(30, func() bool { return h.settledOn(h.revV1, 2) }) {
					h.dumpState("initial create")
					t.Fatalf("the Component never settled on v1")
				}
				victim, sibling := h.podOf(0, ""), h.podOf(1, "")
				untouched := func() {
					if s := h.instance(0); s != nil && s.Phase == workloadtypes.InstancePhaseFailed {
						h.dumpState("failed inside the grace")
						t.Fatalf("the Instance read Failed while the scheduler's wait was inside the grace: %+v", s.LastFailure)
					}
					if s := h.instance(1); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil {
						t.Fatalf("the Instance the scheduler did not take left Ready: %+v", s)
					}
					if pods := h.podsOf(1); len(pods) != 1 || pods[0].UID != sibling.UID {
						t.Fatalf("the pod of the Instance the scheduler did not take was replaced")
					}
				}

				h.holdPlacement(noRoom)
				h.preempt(0, "")
				h.step()
				// The loss is owned by the Create pass under None and by the restart
				// pass under RecreateInstanceOnPodRestart, and the row says so in
				// the pass that observed it.
				wantPhase := workloadtypes.InstancePhaseCreating
				if story.policy == workloadtypes.RestartPolicyRecreateInstance {
					wantPhase = workloadtypes.InstancePhaseRestarting
				}
				if s := h.instance(0); s == nil || s.Phase != wantPhase {
					h.dumpState("after the loss")
					t.Fatalf("the row reads %+v after the pass that observed its pod taken, want phase %s", s, wantPhase)
				}
				replaced := func() bool {
					pods := h.podsOf(0)
					return len(pods) == 1 && pods[0].UID != victim.UID
				}
				if !h.runWithInvariant(3, replaced, untouched) {
					h.dumpState("waiting for the replacement")
					t.Fatalf("no replacement pod was created within three passes of the loss")
				}
				replacement := h.podOf(0, "")
				if replacement.Name != victim.Name {
					t.Fatalf("the replacement is %s, want the stable name %s", replacement.Name, victim.Name)
				}

				h.step()
				s := h.instance(0)
				if s == nil || s.Operation == nil || s.Operation.Waiting != workloadtypes.WaitingReasonUnschedulable {
					h.dumpState("after the scheduler's verdict")
					t.Fatalf("the row does not report the Unschedulable wait after the pass that read the scheduler's verdict: %+v", s)
				}
				if s.LastFailure == nil || s.LastFailure.Reason != workloadtypes.WaitingReasonUnschedulable ||
					s.LastFailure.PodName != replacement.Name || s.LastFailure.Message != noRoom {
					t.Fatalf("the row's record does not name the held pod and the scheduler's message: %+v", s.LastFailure)
				}
				h.runWithInvariant(6, func() bool { return false }, func() {
					untouched()
					if s := h.instance(0); s == nil || s.Operation == nil || s.Operation.Waiting != workloadtypes.WaitingReasonUnschedulable {
						t.Fatalf("the wait left the row while the cluster still had no room: %+v", s)
					}
				})

				h.releasePlacement()
				if !h.runWithInvariant(10, func() bool { return h.settledOn(h.revV1, 2) }, untouched) {
					h.dumpState("after the room came back")
					t.Fatalf("the Instance did not return Ready once its pod could be placed")
				}
				if pods := h.podsOf(0); len(pods) != 1 || pods[0].UID != replacement.UID {
					t.Fatalf("the pod that was placed is not the one that waited")
				}
				wantRepairs := 0
				if story.policy == workloadtypes.RestartPolicyRecreateInstance {
					wantRepairs = 1
				}
				if h.restartEvents() != wantRepairs {
					t.Fatalf("%d repair(s) opened, want %d; events = %v", h.restartEvents(), wantRepairs, h.events)
				}
				// Under None the story must have driven the reading it names: the
				// Create pass recycles a stopped pod still occupying its name and
				// rebuilds a name with no pod behind it without one. A repair
				// drains the pod set itself and announces no recycle.
				if story.policy == workloadtypes.RestartPolicyNone &&
					h.sawEvent(workloadtypes.EventReasonTerminalPodRecycled) != story.stoppedFirst {
					t.Fatalf("terminal-pod recycle seen = %v, want %v; events = %v",
						!story.stoppedFirst, story.stoppedFirst, h.events)
				}
			})
		}
	}
}

// An operator scales a Component whose push has failed: the pushed
// revision cannot be pulled, so its first attempt ended with an Instance
// row at Failed while the retry ladder is still running. The scale lands
// on that fleet. A scale-down removes the stuck Instance first and leaves
// the serving Instances' pods alone; a scale-up adds exactly one Instance
// and never has more pods alive than the replica count plus the surge
// budget. In both the healthy Instances keep serving and the status keeps
// reporting the push that has not landed.

// pushRetryLadder is the retry ladder the deployed chart configures: three
// attempts, a minute apart at first.
var pushRetryLadder = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}

// newFailedPushHarness is three single-pod Instances serving v1 under
// SurgeThenDrain with maxSurge=1 and maxUnavailable=0, pushed onto the
// bad revision until Instance 0 reads Failed: its surge replacement sat in
// ImagePullBackOff past the stuck-pod grace and the attempt was disposed,
// while its v1 pod kept serving beside its peers.
func newFailedPushHarness(t *testing.T) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	h.atomicStatus = true
	h.podGrace = recoveryStepFloor
	h.replicas = 3
	maxSurge, maxUnavailable := intstr.FromInt(1), intstr.FromInt(0)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategySurgeThenDrain,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
	}
	h.retryPolicy = pushRetryLadder
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 3) }) {
		h.dumpState("initial create")
		t.Fatalf("three instances never settled on v1")
	}
	h.setTarget(h.revBad, badImage)
	failed := func() bool {
		row := h.instance(0)
		return row != nil && row.Phase == workloadtypes.InstancePhaseFailed
	}
	if !h.run(40, failed) {
		h.dumpState("push")
		t.Fatalf("the push onto the unpullable revision never failed its first attempt")
	}
	if got := len(h.servingPods()); got != 3 {
		h.dumpState("push")
		t.Fatalf("serving pods after the failed attempt = %d, want the three v1 pods", got)
	}
	return h
}

// servingPodOnV1 is the v1 pod of Instance idx that is in rotation, or
// nil when the Instance serves nothing on v1.
func (h *recoveryHarness) servingPodOnV1(idx int32) *corev1.Pod {
	for _, pod := range h.podsOf(idx) {
		if pod.Spec.Containers[0].Image == goodImage && podreadiness.IsServing(pod) {
			return pod
		}
	}
	return nil
}

// requireServingOnV1 asserts that Instance idx still serves through the
// very pod it served through before the operation.
func (h *recoveryHarness) requireServingOnV1(idx int32, before types.UID) {
	h.t.Helper()
	pod := h.servingPodOnV1(idx)
	if pod == nil {
		h.dumpState("serving lost")
		h.t.Fatalf("instance %d no longer serves on v1", idx)
	}
	if pod.UID != before {
		h.dumpState("pod replaced")
		h.t.Fatalf("instance %d serves through a different pod than before the operation", idx)
	}
}

// TestScaleWhilePushFailed_ScaleDownRemovesTheStuckInstanceFirst: a scale
// from three to two while Instance 0 is Failed removes Instance 0, the
// stuck one, and nothing else. Instances 1 and 2 keep the v1 pods they
// serve through, the serving count never drops below two, and the retry
// block for the pushed revision stays on the status.
func TestScaleWhilePushFailed_ScaleDownRemovesTheStuckInstanceFirst(t *testing.T) {
	h := newFailedPushHarness(t)
	before := map[int32]types.UID{}
	for _, idx := range []int32{1, 2} {
		pod := h.servingPodOnV1(idx)
		if pod == nil {
			t.Fatalf("instance %d does not serve on v1 before the scale", idx)
		}
		before[idx] = pod.UID
	}

	h.replicas = 2
	h.setTarget(h.revBad, badImage)
	atRest := func() bool {
		rows := h.irStatuses()
		if len(rows) != 2 || h.instance(0) != nil || len(h.podsOf(0)) != 0 {
			return false
		}
		for _, row := range rows {
			if row.Phase == workloadtypes.InstancePhaseDeleting {
				return false
			}
		}
		return true
	}
	keepServing := func() {
		t.Helper()
		if got := len(h.servingPods()); got < 2 {
			h.dumpState("scale-down")
			t.Fatalf("serving pods dropped to %d during the scale-down; the two healthy Instances must keep serving", got)
		}
		for idx, uid := range before {
			h.requireServingOnV1(idx, uid)
		}
	}
	if !h.runWithInvariant(40, atRest, keepServing) {
		h.dumpState("scale-down")
		t.Fatalf("the component never came to rest at two instances with the stuck one gone")
	}
	for i := 0; i < 4; i++ {
		h.step()
		keepServing()
		if !atRest() {
			h.dumpState("after the scale-down")
			t.Fatalf("the component left its two-instance shape after the scale-down")
		}
	}
	if h.findBlock(h.revBad.Name) == nil {
		t.Fatalf("the status stopped reporting the push that has not landed: no retry block for %s", h.revBad.Name)
	}
}

// TestScaleWhilePushFailed_ScaleUpStaysUnderTheCeiling: a scale from three
// to four while Instance 0 is Failed adds exactly one Instance, index 3,
// rendered from the pushed revision, and at no point are more pods alive
// than the four replicas plus the surge budget of one. The three v1 pods
// keep serving until the ladder is spent and the revision is Held.
func TestScaleWhilePushFailed_ScaleUpStaysUnderTheCeiling(t *testing.T) {
	h := newFailedPushHarness(t)
	before := map[int32]types.UID{}
	for _, idx := range []int32{0, 1, 2} {
		pod := h.servingPodOnV1(idx)
		if pod == nil {
			t.Fatalf("instance %d does not serve on v1 before the scale", idx)
		}
		before[idx] = pod.UID
	}

	h.replicas = 4
	h.setTarget(h.revBad, badImage)
	const ceiling = 4 + 1
	underTheCeiling := func() {
		t.Helper()
		if alive := len(h.livePods()); alive > ceiling {
			h.dumpState("over the ceiling")
			t.Fatalf("%d pods alive, more than the %d the replica count plus the surge budget allow", alive, ceiling)
		}
		for idx, uid := range before {
			h.requireServingOnV1(idx, uid)
		}
		for _, row := range h.irStatuses() {
			if row.Index > 3 {
				h.dumpState("extra index")
				t.Fatalf("instance %d exists; a scale from three to four adds index 3 only", row.Index)
			}
		}
		for _, pod := range h.livePods() {
			if pod.Spec.Containers[0].Image == goodImage && pod.Labels["ome.io/instance-index"] == "3" {
				t.Fatalf("the new instance was rendered from the running revision instead of the pushed one")
			}
		}
	}
	held := h.runWithInvariant(80, func() bool { return h.heldOn(h.revBad.Name) }, underTheCeiling)
	if !held {
		h.dumpState("ladder")
		t.Fatalf("the pushed revision was never Held after the scale-up")
	}
	h.settle(4)
	underTheCeiling()
	if h.instance(3) == nil {
		h.dumpState("after Held")
		t.Fatalf("the scale-up never added instance 3")
	}
	for _, row := range h.irStatuses() {
		if row.Phase == workloadtypes.InstancePhaseFailed && row.Index != 0 && row.Index != 3 {
			h.dumpState("after Held")
			t.Fatalf("instance %d reads Failed; only the stuck instance and the new one may", row.Index)
		}
	}
}

// A scale-down lands on a gang Component whose SurgeThenDrain push has
// failed: the first gang's replacement parked in ImagePullBackOff, the
// escalation left the gang's row at Failed with its surge operation and
// the replacement's marker still present, while the gang's own v1 pods
// serve beside the other gang's. The scale to one keeps the Ready gang
// the roll never reached and retires the failed pair, source and marker,
// as one scale-down unit; the wave records the attempt on the revision's
// ladder the way the abandon would have, so with a bound of one the
// revision is Held and the survivor is left alone. The healthy gang's
// pods are never touched, the Component never serves below the count it
// was scaled to, no further pod is made on the unpullable revision, and
// the status keeps reporting the push that has not landed.

// newFailedGangSurgeHarness is two leader-plus-worker gangs serving v1
// under SurgeThenDrain with maxSurge=1 and maxUnavailable=0 on a ladder
// of one attempt, pushed onto the bad revision until the first gang's
// attempt has failed: Instance 0 reads Failed with its surge operation,
// the replacement's marker is present and the replacement's pods are
// still parked, and all four v1 pods serve.
func newFailedGangSurgeHarness(t *testing.T) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, true)
	h.atomicStatus = true
	h.replicas = 2
	maxSurge, maxUnavailable := intstr.FromInt(1), intstr.FromInt(0)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategySurgeThenDrain,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
	}
	h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 1, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 2) }) {
		h.dumpState("initial create")
		t.Fatalf("two gangs never settled on v1")
	}
	h.setTarget(h.revBad, badImage)
	if !h.run(60, h.gangMidWreckage) {
		h.dumpState("push")
		t.Fatalf("the push onto the unpullable revision never left Instance 0 Failed with its replacement's marker present")
	}
	if got := len(h.servingPods()); got != 4 {
		h.dumpState("push")
		t.Fatalf("serving pods after the failed attempt = %d, want the four v1 pods", got)
	}
	if h.findBlock(h.revBad.Name) != nil {
		t.Fatalf("the failed attempt is not yet on the ladder; the story needs the wave to be its record")
	}
	return h
}

// TestScaleWhileGangSurgeFailed_ScaleDownRetiresTheFailedPairFirst: a
// scale from two to one while Instance 0 is Failed with its replacement's
// marker present retires Instance 0 and the marker together and keeps
// Instance 1, the gang the roll never reached. Instance 1 serves through
// the very pods it served through before the scale, the pair leaves in
// the same wave, the wave's record Holds the revision at its bound of one
// attempt with the pull failure as its reason, no pod is made on the
// pushed revision after the scale, and no attempt reopens on the survivor.
func TestScaleWhileGangSurgeFailed_ScaleDownRetiresTheFailedPairFirst(t *testing.T) {
	h := newFailedGangSurgeHarness(t)
	source := h.instance(0)
	surgeIdx := *source.Operation.SurgeIndex
	healthy := map[types.UID]struct{}{}
	for _, pod := range h.podsOf(1) {
		if pod.Spec.Containers[0].Image != goodImage || !podreadiness.IsServing(pod) {
			t.Fatalf("instance 1 must serve on v1 before the scale, got pod %s", pod.Name)
		}
		healthy[pod.UID] = struct{}{}
	}
	if len(healthy) != 2 {
		t.Fatalf("instance 1 serves through %d pods before the scale, want the gang's two", len(healthy))
	}
	badSeen := map[types.UID]struct{}{}
	h.trackPodsOnImage(badImage, badSeen)()
	badBefore := len(badSeen)

	h.replicas = 1
	h.setTarget(h.revBad, badImage)
	atRest := func() bool {
		rows := h.irStatuses()
		return len(rows) == 1 && rows[0].Index == 1 && rows[0].Phase == workloadtypes.InstancePhaseReady && rows[0].Operation == nil &&
			len(h.podsOf(0)) == 0 && len(h.podsOf(surgeIdx)) == 0 && len(h.badPods()) == 0
	}
	invariant := func() {
		t.Helper()
		h.trackPodsOnImage(badImage, badSeen)()
		pods := h.podsOf(1)
		if len(pods) != 2 {
			h.dumpState("healthy gang")
			t.Fatalf("instance 1 has %d pods, want the two it serves through", len(pods))
		}
		for _, pod := range pods {
			if _, same := healthy[pod.UID]; !same || !podreadiness.IsServing(pod) || pod.DeletionTimestamp != nil {
				h.dumpState("healthy gang")
				t.Fatalf("instance 1's pod %s is not the serving v1 pod it was before the scale", pod.Name)
			}
		}
		if row := h.instance(1); row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil {
			h.dumpState("healthy row")
			t.Fatalf("instance 1 must stay Ready with no operation, got %+v", row)
		}
		// The pair leaves as one unit: a wave that holds one of them holds
		// the other, or has already removed it.
		deletingOrGone := func(idx int32) bool {
			row := h.instance(idx)
			return row == nil || row.Phase == workloadtypes.InstancePhaseDeleting
		}
		if deletingOrGone(0) != deletingOrGone(surgeIdx) {
			h.dumpState("pair split")
			t.Fatalf("the failed source and its marker must be retired together: source deleting-or-gone=%t, marker deleting-or-gone=%t",
				deletingOrGone(0), deletingOrGone(surgeIdx))
		}
		if row := h.instance(0); row != nil && row.Phase != workloadtypes.InstancePhaseFailed && row.Phase != workloadtypes.InstancePhaseDeleting {
			h.dumpState("source")
			t.Fatalf("the failed source must go from Failed to Deleting, never be reset by the scale, got %+v", row)
		}
	}
	if !h.runWithInvariant(60, atRest, invariant) {
		h.dumpState("scale-down")
		t.Fatalf("the component never came to rest at the one gang that serves")
	}
	for i := 0; i < 6; i++ {
		h.step()
		invariant()
		if !atRest() {
			h.dumpState("after the scale-down")
			t.Fatalf("the component left its one-instance shape after the scale-down")
		}
	}
	block := h.findBlock(h.revBad.Name)
	if block == nil {
		t.Fatalf("the status stopped reporting the push that has not landed: no retry block for %s", h.revBad.Name)
	}
	if block.State != workloadtypes.RetryBlockHeld || block.AttemptsStarted != 1 || block.Reason != "ImagePullBackOff" {
		t.Fatalf("block after the scale = (state=%s attempts=%d reason=%q), want Held after the one attempt the scale ended, with its pull failure",
			block.State, block.AttemptsStarted, block.Reason)
	}
	if len(h.heldWarnings) != 1 {
		t.Fatalf("held warnings = %v, want the one the wave's record raised", h.heldWarnings)
	}
	if len(badSeen) != badBefore {
		t.Fatalf("%d pods were made on the unpullable revision after the scale; a retired attempt is not retried on the survivor", len(badSeen)-badBefore)
	}
	if row := h.instance(1); row.RunningRevision != h.revV1.Name {
		t.Fatalf("the survivor must still record the sound revision, got %+v", row)
	}
}

// A two-pod gang (leader plus worker) is pushed onto a revision that
// cannot be pulled under RecreatePod. The first gang is drained and its
// replacement parks in ImagePullBackOff. The attempt must end the way a
// single-pod attempt ends: disposed by the stuck-pod grace or the
// operation deadline, counted on the revision's retry ladder, the row
// left at Failed with the pull failure as its reason, and after the last
// attempt the revision Held so nothing more is tried. The other gang
// keeps serving throughout, and a corrected push lands on both.

// gangPushEnding is how the attempt is ended: by the fast escalator on
// the parked pod, or by the operation deadline with fast escalation off.
type gangPushEnding struct {
	name       string
	stuckGrace time.Duration
	deadline   time.Duration
}

var gangPushEndings = []gangPushEnding{
	{name: "stuck-pod grace", stuckGrace: 30 * time.Second},
	{name: "operation deadline", stuckGrace: 0, deadline: 2 * time.Minute},
}

// newGangPushHarness is two leader-plus-worker gangs serving v1 under
// RecreatePod with maxUnavailable=1.
func newGangPushHarness(t *testing.T, ending gangPushEnding) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, true)
	h.replicas = 2
	h.stuckGrace = ending.stuckGrace
	maxSurge, maxUnavailable := intstr.FromInt(0), intstr.FromInt(1)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategyRecreatePod,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
	}
	if ending.deadline > 0 {
		h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: ending.deadline}
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 2) }) {
		h.dumpState("initial create")
		t.Fatalf("two gangs never settled on v1")
	}
	return h
}

// TestGangPush_UnpullableRevisionIsHeldWithinTheLadder pins the ladder
// for a gang: every attempt at the unpullable revision ends within its
// grace or deadline and is counted, the revision is Held at the bound
// with the pull failure as its reason, the stuck gang's row reads Failed
// with no attempt in flight, the healthy gang never leaves rotation, and
// a corrected push converges both gangs.
func TestGangPush_UnpullableRevisionIsHeldWithinTheLadder(t *testing.T) {
	for _, ending := range gangPushEndings {
		t.Run(ending.name, func(t *testing.T) {
			h := newGangPushHarness(t, ending)
			h.setTarget(h.revBad, badImage)
			seen := map[types.UID]struct{}{}
			sawFailed := false
			held := h.runWithInvariant(160, func() bool { return h.heldOn(h.revBad.Name) }, func() {
				h.trackPodsOnImage(badImage, seen)()
				if got := len(h.servingPods()); got < 2 {
					h.dumpState("serving lost")
					t.Fatalf("serving pods dropped to %d; the healthy gang must keep serving while the other is retried", got)
				}
				if row := h.instance(0); row != nil && row.Phase == workloadtypes.InstancePhaseFailed {
					sawFailed = true
				}
			})
			if !held {
				h.dumpState("never held")
				t.Fatalf("the unpullable revision was never Held: the gang's attempts are not counted on the ladder")
			}
			if !sawFailed {
				t.Fatalf("no pass ever showed the stuck gang's row at Failed")
			}
			block := h.findBlock(h.revBad.Name)
			if block.AttemptsStarted != h.retryPolicy.MaxAttempts || block.Reason != "ImagePullBackOff" {
				t.Fatalf("held block: got (attempts=%d, reason=%q) want (%d, ImagePullBackOff)", block.AttemptsStarted, block.Reason, h.retryPolicy.MaxAttempts)
			}
			if want := 2 * int(h.retryPolicy.MaxAttempts); len(seen) != want {
				h.dumpState("attempts")
				t.Fatalf("%d pods were made on the unpullable revision, want %d: one gang per attempt of the ladder", len(seen), want)
			}
			h.settle(4)
			row := h.instance(0)
			if row == nil || row.Phase != workloadtypes.InstancePhaseFailed || attemptOpen(row) ||
				row.LastFailure == nil || row.LastFailure.Reason != "ImagePullBackOff" {
				h.dumpState("held row")
				t.Fatalf("the stuck gang must park at Failed with no attempt in flight and the pull failure as its reason, got %+v", row)
			}
			if peer := h.instance(1); peer == nil || peer.Phase != workloadtypes.InstancePhaseReady || peer.RunningRevision != h.revV1.Name {
				h.dumpState("peer")
				t.Fatalf("the healthy gang must still be Ready on v1, got %+v", peer)
			}

			h.setTarget(h.revFixed, fixedImage)
			if !h.run(80, func() bool { return h.settledOn(h.revFixed, 2) && len(h.badPods()) == 0 }) {
				h.dumpState("corrected push")
				t.Fatalf("the corrected push did not land on both gangs after the unpullable revision was Held")
			}
			// The failure the held revision left on the stuck gang's row is
			// that revision's: retiring the gang's old set under the
			// corrected roll charges nothing to the corrected revision.
			h.settle(4)
			if block := h.findBlock(h.revFixed.Name); block != nil {
				h.dumpState("corrected revision charged")
				t.Fatalf("the corrected revision carries a RetryBlock the held revision's failure left behind: %+v", *block)
			}
		})
	}
}

// A push onto a revision whose pods cannot start is charged to the
// revision's retry ladder the same way whatever the Instance's shape: a
// single pod, a leader-plus-worker gang on an image neither member can
// pull, or a gang whose leader cannot pull while its worker comes up. The
// first attempt that parks past the stuck-pod grace leaves the row Failed
// with the pull failure as its reason and no attempt in flight, records
// one attempt on the block, and leaves the wedged pod set where it is
// until the ladder's backoff is due; the next attempt opens as the
// ladder's, and once the ladder is spent the push is Held with its reason
// while the Instance the roll never reached keeps serving the running
// revision through the very pods it served through before the push.

// unpullablePushShape is one Instance shape the ladder is pinned on.
type unpullablePushShape struct {
	name     string
	multiPod bool
	// workerGood keeps the worker on the running image, so only the
	// leader of the gang cannot pull while the worker comes up beside it.
	workerGood bool
}

var unpullablePushShapes = []unpullablePushShape{
	{name: "single pod"},
	{name: "gang whose leader and worker cannot pull", multiPod: true},
	{name: "gang whose leader cannot pull beside a worker that comes up", multiPod: true, workerGood: true},
}

// TestUnpullablePush_ChargesTheLadderWhateverTheInstanceShape pins the
// ladder's contract across the shapes under RecreatePod with
// maxUnavailable=1 on two Instances: after the first failed attempt the
// operator reads a Failed row with the pull failure and a block with one
// attempt, the wedged set is not rebuilt before the backoff is due, the
// next attempt opens as the ladder's, the revision is Held at the bound
// with the pull failure as its reason after exactly one pod set per
// attempt, and the Instance the roll never reached serves throughout
// through its original pods.
func TestUnpullablePush_ChargesTheLadderWhateverTheInstanceShape(t *testing.T) {
	for _, shape := range unpullablePushShapes {
		t.Run(shape.name, func(t *testing.T) {
			h := newRecoveryHarness(t, shape.multiPod)
			h.replicas = 2
			h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: 90 * time.Second, MaxDelay: 30 * time.Minute, Multiplier: 2}
			maxSurge, maxUnavailable := intstr.FromInt(0), intstr.FromInt(1)
			h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
				Type:          workloadtypes.UpdateStrategyRecreatePod,
				RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
			}
			h.setTarget(h.revV1, goodImage)
			if !h.run(40, func() bool { return h.settledOn(h.revV1, 2) }) {
				h.dumpState("initial create")
				t.Fatalf("two Instances never settled on v1")
			}
			podsPerInstance := len(h.desired.Runners)
			survivor := map[types.UID]struct{}{}
			for _, pod := range h.podsOf(1) {
				survivor[pod.UID] = struct{}{}
			}
			if len(survivor) != podsPerInstance {
				t.Fatalf("instance 1 serves through %d pods before the push, want %d", len(survivor), podsPerInstance)
			}

			h.setTarget(h.revBad, badImage)
			badPodsPerAttempt := podsPerInstance
			if shape.workerGood {
				h.desired.WorkerPodSpec = h.podSpec(goodImage)
				badPodsPerAttempt = 1
			}
			seen := map[types.UID]struct{}{}
			keepServing := func() {
				t.Helper()
				h.trackPodsOnImage(badImage, seen)()
				if got := len(h.servingPods()); got < podsPerInstance {
					h.dumpState("serving lost")
					t.Fatalf("serving pods dropped to %d; the Instance the roll never reached must keep serving while the other is retried", got)
				}
			}

			// The first attempt: Failed with the pull failure, one attempt on
			// the block, nothing in flight.
			firstAttemptCharged := func() bool {
				row, block := h.instance(0), h.findBlock(h.revBad.Name)
				return row != nil && row.Phase == workloadtypes.InstancePhaseFailed && !attemptOpen(row) &&
					row.LastFailure != nil && row.LastFailure.Reason == "ImagePullBackOff" &&
					block != nil && block.State == workloadtypes.RetryBlockBackoff && block.AttemptsStarted == 1 && block.NextRetryAt != nil
			}
			if !h.runWithInvariant(40, firstAttemptCharged, keepServing) {
				h.dumpState("first attempt")
				t.Fatalf("the first attempt onto the unpullable revision was never charged: no Failed row with the pull failure and one attempt on the block")
			}
			if len(seen) != badPodsPerAttempt {
				h.dumpState("first attempt")
				t.Fatalf("%d pods were made on the unpullable revision during the first attempt, want %d: one pod set per attempt", len(seen), badPodsPerAttempt)
			}

			// The wedged set stays where it is until the backoff is due: the
			// row keeps its Failed reading and no rebuild opens.
			wedged := map[types.UID]struct{}{}
			for _, pod := range h.podsOf(0) {
				wedged[pod.UID] = struct{}{}
			}
			if len(wedged) != podsPerInstance {
				h.dumpState("wedged set")
				t.Fatalf("the failed attempt left %d pods on instance 0, want its %d", len(wedged), podsPerInstance)
			}
			due := h.findBlock(h.revBad.Name).NextRetryAt.Time
			parkedPasses := 0
			for h.clk.Now().Before(due) {
				h.step()
				parkedPasses++
				keepServing()
				if !firstAttemptCharged() {
					h.dumpState("inside the backoff")
					t.Fatalf("the row left its Failed reading before the backoff was due: %+v, block %+v", h.instance(0), h.findBlock(h.revBad.Name))
				}
				for _, pod := range h.podsOf(0) {
					if _, same := wedged[pod.UID]; !same {
						h.dumpState("inside the backoff")
						t.Fatalf("instance 0 was rebuilt before the backoff was due: pod %s is not of the failed attempt", pod.Name)
					}
				}
			}
			if parkedPasses == 0 {
				t.Fatalf("no pass ran inside the backoff; the story did not observe the park")
			}

			// The next attempt opens as the ladder's and replaces the wedged set.
			reopened := func() bool {
				row, block := h.instance(0), h.findBlock(h.revBad.Name)
				return row != nil && row.Phase == workloadtypes.InstancePhaseUpdating &&
					block != nil && block.State == workloadtypes.RetryBlockRetryInProgress && block.AttemptsStarted == 1
			}
			if !h.runWithInvariant(10, reopened, keepServing) {
				h.dumpState("second attempt")
				t.Fatalf("the ladder's next attempt never opened once the backoff was due")
			}

			// The ladder's end: Held at the bound with the pull failure, one
			// pod set per attempt, the stuck row parked with no attempt in
			// flight and the survivor untouched on v1.
			if !h.runWithInvariant(80, func() bool { return h.heldOn(h.revBad.Name) }, keepServing) {
				h.dumpState("never held")
				t.Fatalf("the unpullable revision was never Held: the attempts are not counted on the ladder")
			}
			block := h.findBlock(h.revBad.Name)
			if block.AttemptsStarted != h.retryPolicy.MaxAttempts || block.Reason != "ImagePullBackOff" {
				t.Fatalf("held block: got (attempts=%d, reason=%q) want (%d, ImagePullBackOff)", block.AttemptsStarted, block.Reason, h.retryPolicy.MaxAttempts)
			}
			if len(h.heldWarnings) != 1 {
				t.Fatalf("held warnings = %v, want the one the hold raised", h.heldWarnings)
			}
			h.settle(4)
			keepServing()
			if want := badPodsPerAttempt * int(h.retryPolicy.MaxAttempts); len(seen) != want {
				h.dumpState("attempts")
				t.Fatalf("%d pods were made on the unpullable revision, want %d: one pod set per attempt of the ladder and none after it was Held", len(seen), want)
			}
			row := h.instance(0)
			if row == nil || row.Phase != workloadtypes.InstancePhaseFailed || attemptOpen(row) ||
				row.LastFailure == nil || row.LastFailure.Reason != "ImagePullBackOff" {
				h.dumpState("held row")
				t.Fatalf("the stuck Instance must park at Failed with no attempt in flight and the pull failure as its reason, got %+v", row)
			}
			peer := h.instance(1)
			if peer == nil || peer.Phase != workloadtypes.InstancePhaseReady || peer.Operation != nil || peer.RunningRevision != h.revV1.Name {
				h.dumpState("peer")
				t.Fatalf("the Instance the roll never reached must still be Ready on v1, got %+v", peer)
			}
			for _, pod := range h.podsOf(1) {
				if _, same := survivor[pod.UID]; !same || !podreadiness.IsServing(pod) {
					h.dumpState("peer pods")
					t.Fatalf("instance 1 must serve through the pods it served through before the push, got %s", pod.Name)
				}
			}
		})
	}
}

// endpointController is the EndpointSlice controller's model, run between
// the kubelet and the pass once the routed model is on: one slice per
// routed per-revision Service holding that revision's routable pods, and
// one for the headless Service holding every pod. An endpoint is Ready
// and Serving while its pod is PodReady, and Terminating while the pod is
// being deleted; a Service with no pod left loses its slice.
func (h *recoveryHarness) endpointController() {
	h.t.Helper()
	list := &corev1.PodList{}
	if err := h.c.List(h.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("endpoint controller: list pods: %v", err)
	}
	headless := query.HeadlessServiceName(recoveryOwner, h.component)
	byService := map[string][]*corev1.Pod{}
	for i := range list.Items {
		pod := &list.Items[i]
		byService[headless] = append(byService[headless], pod)
		if svc := query.RoutedServiceForPod(recoveryOwner, h.component, pod); svc != "" {
			byService[svc] = append(byService[svc], pod)
		}
	}
	slices := &discoveryv1.EndpointSliceList{}
	if err := h.c.List(h.ctx, slices, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("endpoint controller: list slices: %v", err)
	}
	seen := map[string]bool{}
	for svc, pods := range byService {
		endpoints := make([]discoveryv1.Endpoint, 0, len(pods))
		for _, pod := range pods {
			ready := podConditionTrue(pod, corev1.PodReady)
			serving := ready
			terminating := pod.DeletionTimestamp != nil
			endpoints = append(endpoints, discoveryv1.Endpoint{
				Addresses:  []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &serving, Terminating: &terminating},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			})
		}
		name := svc + "-endpoints"
		seen[name] = true
		existing := &discoveryv1.EndpointSlice{}
		err := h.c.Get(h.ctx, types.NamespacedName{Namespace: recoveryNS, Name: name}, existing)
		switch {
		case err == nil:
			existing.Endpoints = endpoints
			if err := h.c.Update(h.ctx, existing); err != nil {
				h.t.Fatalf("endpoint controller: update slice %s: %v", name, err)
			}
		case apierrors.IsNotFound(err):
			slice := &discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name: name, Namespace: recoveryNS,
					Labels: map[string]string{discoveryv1.LabelServiceName: svc},
				},
				AddressType: discoveryv1.AddressTypeIPv4,
				Endpoints:   endpoints,
			}
			if err := h.c.Create(h.ctx, slice); err != nil {
				h.t.Fatalf("endpoint controller: create slice %s: %v", name, err)
			}
		default:
			h.t.Fatalf("endpoint controller: get slice %s: %v", name, err)
		}
	}
	for i := range slices.Items {
		if seen[slices.Items[i].Name] {
			continue
		}
		if err := h.c.Delete(h.ctx, &slices.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			h.t.Fatalf("endpoint controller: delete slice %s: %v", slices.Items[i].Name, err)
		}
	}
}

// availableByPod is the set of pods the headless Service publishes as
// available, the set the adapter folds into the Available counters.
// Empty without the routed model, where no slice exists.
func (h *recoveryHarness) availableByPod() map[string]struct{} {
	h.t.Helper()
	if !h.routed {
		return map[string]struct{}{}
	}
	available, err := status.AvailablePodSet(h.ctx, h.c, recoveryNS, query.HeadlessServiceName(recoveryOwner, h.component))
	if err != nil {
		h.t.Fatalf("available pod set: %v", err)
	}
	return available
}

// mutateMigration is the harness's status.migrations write seam: the
// record for uuid is handed to mutate on a copy and kept when mutate
// reports a change, as the adapter's round-trip does. A uuid with no
// record is a no-op.
func (h *recoveryHarness) mutateMigration(_ context.Context, uuid string, mutate func(*workloadtypes.MigrationRecord) bool) error {
	for i := range h.migrations {
		if h.migrations[i].RequestUUID != uuid {
			continue
		}
		record := h.migrations[i]
		if mutate(&record) {
			record.RequestUUID = uuid
			h.migrations[i] = record
		}
		return nil
	}
	return nil
}

// requestMove files an operator's move of Instance idx off the node its
// routable pod runs on, as the accept pass records it: a Manual record,
// Accepted, naming no replacement yet. Returns the node the request names.
func (h *recoveryHarness) requestMove(uuid string, idx int32) string {
	h.t.Helper()
	fromNode := ""
	for _, pod := range h.podsOf(idx) {
		if query.RoutedRunner(pod) {
			fromNode = pod.Spec.NodeName
		}
	}
	if fromNode == "" {
		h.t.Fatalf("instance %d has no placed routable pod to move off", idx)
	}
	h.migrations = append(h.migrations, workloadtypes.MigrationRecord{
		RequestUUID:    uuid,
		Trigger:        workloadtypes.MigrationTriggerManual,
		Phase:          workloadtypes.MigrationPhaseAccepted,
		SourceInstance: idx,
		FromNode:       fromNode,
		Reason:         "maintenance",
		StartedAt:      metav1.NewTime(h.clk.Now()),
		Deadline:       metav1.NewTime(h.clk.Now().Add(12 * time.Hour)),
	})
	return fromNode
}

// migration is the record for uuid, or nil.
func (h *recoveryHarness) migration(uuid string) *workloadtypes.MigrationRecord {
	for i := range h.migrations {
		if h.migrations[i].RequestUUID == uuid {
			return &h.migrations[i]
		}
	}
	return nil
}

// instancesWithLivePods counts the Instances that have a live pod: the
// Component's footprint, which a surge raises by one.
func (h *recoveryHarness) instancesWithLivePods() int {
	seen := map[int32]struct{}{}
	for _, pod := range h.livePods() {
		if idx, ok := query.InstanceIdxFromLabels(pod); ok {
			seen[idx] = struct{}{}
		}
	}
	return len(seen)
}

// instancesFullyServing counts the Instances whose whole pod set is Ready
// and in the serving gate: the capacity the Component serves with.
func (h *recoveryHarness) instancesFullyServing() int {
	byInstance := map[int32][]*corev1.Pod{}
	for _, pod := range h.livePods() {
		if idx, ok := query.InstanceIdxFromLabels(pod); ok {
			byInstance[idx] = append(byInstance[idx], pod)
		}
	}
	n := 0
	for _, pods := range byInstance {
		if len(pods) < len(h.desired.Runners) {
			continue
		}
		serving := true
		for _, pod := range pods {
			if !podreadiness.ReadyAndServing(pod) {
				serving = false
				break
			}
		}
		if serving {
			n++
		}
	}
	return n
}

// A move requested as a push lands on a gang Component, before any
// Instance moved: the roll's SurgeThenDrain rebuilds the gang the move
// names under another index, and the move follows the Instance there.
// Both finish. The record names its replacement Instance and is Completed
// once, in the status and in the ledger; every Instance ends on the
// pushed revision, the moved one among them and off the node it left; no
// Instance is left mid-operation; the Component never serves below its
// replica count and never has more than one Instance over it. Three
// orderings of the one story: the push seen a pass before the move, under
// no rollout group and paced by a rolling group's gate, and the push and
// the move seen in the same pass.
func TestMoveRequestedAsAGangPushLands_FollowsTheInstanceAndCompletes(t *testing.T) {
	const move = "move-under-push"
	stories := []struct {
		name          string
		grouped       bool
		pushSeenFirst bool
	}{
		{"the push is seen a pass before the move", false, true},
		{"the push is seen a pass before the move, paced by a rolling group", true, true},
		{"the push and the move are seen in one pass", false, false},
	}
	for _, story := range stories {
		t.Run(story.name, func(t *testing.T) {
			h := newRecoveryHarness(t, true)
			h.recorder = record.NewFakeRecorder(256)
			h.atomicStatus = true
			h.routed = true
			h.replicas = 2
			h.useNodes("node-a", "node-b", "node-c")
			h.migrationAudit = &workloadtypes.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 10, Window: time.Hour}
			h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
			if story.grouped {
				// A rolling group lets one surge of its Components be in
				// flight at a time.
				h.gate = func(_ workloadtypes.UpdateStrategyType, inFlightSurge, _ int32) (bool, workloadtypes.RolloutHoldGate, string) {
					if inFlightSurge > 0 {
						return false, workloadtypes.RolloutHoldGateBudget, "group surge budget 1 exhausted"
					}
					return true, "", ""
				}
			}
			if !h.run(40, func() bool { return h.settledOn(h.revV1, 2) }) {
				h.dumpState("initial create")
				t.Fatalf("the Component never settled on v1")
			}

			h.setTarget(h.revFixed, fixedImage)
			if story.pushSeenFirst {
				h.step()
				if s := h.instance(0); s == nil || s.Operation == nil || s.Operation.SurgeIndex == nil {
					h.dumpState("after the push")
					t.Fatalf("the roll did not open a gang surge on Instance 0 in the pass that saw the push: %+v", s)
				}
			}
			requested := h.requestMove(move, 0)

			invariant := func() {
				if n := h.instancesWithLivePods(); n > 3 {
					h.dumpState("over the surge budget")
					t.Fatalf("%d Instances had pods alive, want at most the replica count plus one surge", n)
				}
				if n := h.instancesFullyServing(); n < 2 {
					h.dumpState("below the floor")
					t.Fatalf("%d Instances fully serving, want the replica count throughout", n)
				}
				if rec := h.migration(move); rec != nil && rec.Phase == workloadtypes.MigrationPhaseFailed {
					h.dumpState("the move failed")
					t.Fatalf("the move failed instead of following the Instance the roll rebuilt (replacement named: %v): %s",
						rec.SurgeInstance != nil, rec.Message)
				}
			}
			moveDone := func() bool {
				rec := h.migration(move)
				return rec != nil && rec.Phase.Terminal()
			}
			if !h.runWithInvariant(120, moveDone, invariant) {
				h.dumpState("the move never finished")
				t.Fatalf("the move did not finish; record=%+v", h.migration(move))
			}
			rec := *h.migration(move)
			if rec.Phase != workloadtypes.MigrationPhaseCompleted || rec.SurgeInstance == nil || rec.CompletedAt == nil {
				t.Fatalf("the record must be Completed naming its replacement Instance; got %+v", rec)
			}
			// The replacement as the move left it: the whole gang, Ready on a
			// recorded revision, off the node the Instance left. The roll may
			// rebuild it under yet another index afterwards.
			if moved := h.instance(*rec.SurgeInstance); moved == nil || moved.Phase != workloadtypes.InstancePhaseReady ||
				moved.Operation != nil || moved.RunningRevision == "" {
				h.dumpState("after the move")
				t.Fatalf("the replacement Instance %d must be Ready on a recorded revision with no operation; got %+v", *rec.SurgeInstance, moved)
			}
			replacement := h.podsOf(*rec.SurgeInstance)
			if len(replacement) != len(h.desired.Runners) {
				t.Fatalf("the replacement Instance has %d pods, want the whole gang of %d", len(replacement), len(h.desired.Runners))
			}
			t.Logf("the request named %s; the Instance left %s", requested, rec.FromNode)
			for _, pod := range replacement {
				if pod.Spec.NodeName == "" || pod.Spec.NodeName == rec.FromNode {
					t.Fatalf("replacement pod %s sits on %q, the node the Instance left", pod.Name, pod.Spec.NodeName)
				}
			}

			if !h.runWithInvariant(120, func() bool { return h.settledOn(h.revFixed, 2) }, invariant) {
				h.dumpState("the roll never finished")
				t.Fatalf("the push did not land on every Instance after the move")
			}
			if len(h.migrations) != 1 {
				t.Fatalf("the move is recorded once; status carries %d records", len(h.migrations))
			}
			ledger, err := audit.LoadLedgerForOwner(h.ctx, h.c, h.isvc)
			if err != nil {
				t.Fatalf("load the audit ledger: %v", err)
			}
			rows := 0
			for _, entry := range ledger.Entries {
				if entry.RequestUUID != move {
					continue
				}
				rows++
				if entry.Phase != audit.PhaseCompleted || entry.SurgeInstance != *rec.SurgeInstance {
					t.Fatalf("the ledger row must be Completed naming replacement %d; got %+v", *rec.SurgeInstance, entry)
				}
			}
			if rows != 1 {
				t.Fatalf("the ledger carries %d rows for the move, want one", rows)
			}
			if !h.sawEvent(workloadtypes.EventReasonMigrationCompleted) {
				t.Fatalf("no MigrationCompleted event was recorded; events = %v", h.events)
			}
		})
	}
}

// rowSettledOn reports whether Instance idx is Ready with no operation,
// records rev as its running revision and holds exactly one live pod.
func (h *recoveryHarness) rowSettledOn(idx int32, rev *appsv1.ControllerRevision) bool {
	s := h.instance(idx)
	return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil &&
		s.RunningRevision == rev.Name && len(h.podsOf(idx)) == 1
}

// requirePeersOn fails the test unless every Instance other than idx is
// settled Ready on rev with no operation.
func (h *recoveryHarness) requirePeersOn(idx int32, rev *appsv1.ControllerRevision, label string) {
	h.t.Helper()
	for _, s := range h.irStatuses() {
		if s.Index == idx {
			continue
		}
		if s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil || s.RunningRevision != rev.Name {
			h.dumpState(label)
			h.t.Fatalf("Instance %d left %s while Instance %d on the target serves nothing: %+v", s.Index, rev.Name, idx, s)
		}
	}
}

// TestPodlessRolledRow_RebuiltUnderTheBudgetItHolds pins the canary
// shape: a two-Instance single-pod Component rolls one Instance onto the
// target under a partition of one, the step is promoted, and the rolled
// Instance's only pod is deleted as the partition drops to zero. The
// podless row counts against the roll's budget as an Instance on the
// target that does not serve it, which denies the other Instance's
// start; the row must still be rebuilt, from the target's template, and
// the roll must then finish. Until the rebuilt pod serves, the other
// Instance stays in rotation on the stable revision: the slot the
// podless row holds keeps the roll from creeping forward, and only the
// row's own rebuild is let through. No repair policy, so the Create pass
// owns the rebuild. SurgeThenDrain with maxSurge one and RecreatePod with
// maxUnavailable one: the two arms the budget is charged on.
func TestPodlessRolledRow_RebuiltUnderTheBudgetItHolds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy workloadtypes.UpdateStrategyType
	}{
		{name: "SurgeThenDrain maxSurge 1", strategy: workloadtypes.UpdateStrategySurgeThenDrain},
		{name: "RecreatePod maxUnavailable 1", strategy: workloadtypes.UpdateStrategyRecreatePod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarness(t, false)
			h.replicas = 2
			noRepair := workloadtypes.RestartPolicyNone
			h.lifecycle.RestartPolicy = &noRepair
			h.useRollingBudget(tc.strategy)
			if !h.run(40, func() bool { return h.settledOn(h.revV1, 2) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for two Instances")
			}

			// The canary step: Instance 1 moves to the target, Instance 0 is held.
			partition := int32(1)
			h.lifecycle.UpdateStrategy.RollingUpdate.Partition = &partition
			h.setTarget(h.revFixed, fixedImage)
			if !h.run(60, func() bool { return h.rowSettledOn(1, h.revFixed) && h.rowSettledOn(0, h.revV1) }) {
				h.dumpState("canary step")
				t.Fatalf("the canary step never came to rest with Instance 1 on the target and Instance 0 held")
			}

			// The step is promoted and the canary pod is deleted by hand at
			// the same moment.
			partition = 0
			h.setTarget(h.revFixed, fixedImage)
			h.losePods(1)

			heldStays := func() {
				if h.rowSettledOn(1, h.revFixed) {
					return
				}
				h.requirePeersOn(1, h.revV1, "roll crept forward")
			}
			rebuilt := false
			finished := h.runWithInvariant(80, func() bool {
				if !rebuilt && len(h.podsOf(1)) == 1 {
					rebuilt = true
					h.requirePodsRender(1, h.revFixed, fixedImage)
				}
				return h.settledOn(h.revFixed, 2)
			}, heldStays)
			if !finished {
				h.dumpState("after the canary pod was deleted")
				t.Fatalf("the podless Instance was never rebuilt and the roll never finished: rebuilt=%v, budget holds naming it=%d", rebuilt, h.notServingHolds())
			}
			if h.notServingHolds() == 0 {
				t.Fatalf("the budget never held the roll on the podless Instance; the story did not run")
			}
		})
	}
}

// TestPodlessRolledRow_RebuiltAtTheTargetMidRoll: three single-pod
// Instances roll under SurgeThenDrain with maxSurge one, and the first
// Instance the roll moved onto the target loses its pod before the next
// start is admitted. The podless row holds the roll's one surge slot, so
// no further Instance leaves the stable revision, and the row is rebuilt
// from the target's template under the target's revision label: the
// revision the roll put it on, not the one its peers still run. The roll
// then lands on all three.
func TestPodlessRolledRow_RebuiltAtTheTargetMidRoll(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.replicas = 3
	noRepair := workloadtypes.RestartPolicyNone
	h.lifecycle.RestartPolicy = &noRepair
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	if !h.run(60, func() bool { return h.settledOn(h.revV1, 3) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for three Instances")
	}

	h.setTarget(h.revFixed, fixedImage)
	moved := int32(-1)
	if !h.run(60, func() bool {
		for _, s := range h.irStatuses() {
			if h.rowSettledOn(s.Index, h.revFixed) {
				moved = s.Index
				return true
			}
		}
		return false
	}) {
		h.dumpState("first Instance")
		t.Fatalf("no Instance was moved onto the target")
	}
	h.requirePeersOn(moved, h.revV1, "roll moved more than one Instance before the first served")

	h.losePods(moved)

	peersStay := func() {
		if h.rowSettledOn(moved, h.revFixed) {
			return
		}
		h.requirePeersOn(moved, h.revV1, "roll crept forward")
	}
	rebuilt := false
	finished := h.runWithInvariant(100, func() bool {
		if !rebuilt && len(h.podsOf(moved)) == 1 {
			rebuilt = true
			h.requirePodsRender(moved, h.revFixed, fixedImage)
		}
		return h.settledOn(h.revFixed, 3)
	}, peersStay)
	if !finished {
		h.dumpState("after the moved Instance lost its pod")
		t.Fatalf("Instance %d was never rebuilt and the roll never finished: rebuilt=%v, budget holds naming it=%d", moved, rebuilt, h.notServingHolds())
	}
	if h.notServingHolds() == 0 {
		t.Fatalf("the budget never held the roll on the podless Instance; the story did not run")
	}
}

// A scale-down lands while a RecreatePod attempt is in flight on a
// revision that cannot be pulled: Instance 0's v1 pod has been drained and
// its replacement sits in ImagePullBackOff inside the stuck-pod grace, so
// the attempt is open and nothing has ended it yet. The wave retires
// Instance 0, the one Instance that serves nothing, and the attempt ends
// with its Instance: the ladder records the failed wave with the pull
// failure the pod showed, so the survivors' v1 pods are left alone — Held
// at the bound, paced by the backoff below it — and the status keeps
// reporting the push that has not landed.

// newStuckRecreateHarness is three single-pod Instances serving v1 under
// RecreatePod with maxUnavailable=1 and the given ladder, pushed onto the
// bad revision until Instance 0's replacement is parked in
// ImagePullBackOff with the attempt still open. The stuck-pod grace is
// longer than the story, so only the scale can end the attempt.
func newStuckRecreateHarness(t *testing.T, ladder *workloadtypes.RetryPolicy) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	h.atomicStatus = true
	h.replicas = 3
	h.stuckGrace = time.Hour
	maxSurge, maxUnavailable := intstr.FromInt(0), intstr.FromInt(1)
	h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{
		Type:          workloadtypes.UpdateStrategyRecreatePod,
		RollingUpdate: &workloadtypes.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
	}
	h.retryPolicy = ladder
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 3) }) {
		h.dumpState("initial create")
		t.Fatalf("three instances never settled on v1")
	}
	h.setTarget(h.revBad, badImage)
	parked := func() bool {
		row := h.instance(0)
		if row == nil || row.Phase != workloadtypes.InstancePhaseUpdating || row.Operation == nil ||
			row.Operation.TargetRevision != h.revBad.Name {
			return false
		}
		pods := h.podsOf(0)
		if len(pods) != 1 || pods[0].Spec.Containers[0].Image != badImage || len(pods[0].Status.ContainerStatuses) != 1 {
			return false
		}
		waiting := pods[0].Status.ContainerStatuses[0].State.Waiting
		return waiting != nil && waiting.Reason == "ImagePullBackOff"
	}
	if !h.run(40, parked) {
		h.dumpState("push")
		t.Fatalf("the recreate onto the unpullable revision never parked Instance 0's replacement in ImagePullBackOff")
	}
	if got := len(h.servingPods()); got != 2 {
		h.dumpState("push")
		t.Fatalf("serving pods with the attempt open = %d, want the two v1 pods the roll has not reached", got)
	}
	if h.findBlock(h.revBad.Name) != nil {
		t.Fatalf("a first attempt opens with no retry block; the story needs the wave to be the ladder's first record")
	}
	return h
}

// scaleStuckRecreateToTwo scales the harness from three to two while the
// attempt is open and runs the wave to rest: two rows, Instance 0 and its
// pods gone, none Deleting. Throughout, and for several passes after,
// while untouched() holds Instances 1 and 2 serve through the very v1
// pods they served through before the scale and no pod on the pushed
// revision appears on them.
func scaleStuckRecreateToTwo(t *testing.T, h *recoveryHarness, untouched func() bool) {
	t.Helper()
	before := map[int32]types.UID{}
	for _, idx := range []int32{1, 2} {
		pod := h.servingPodOnV1(idx)
		if pod == nil {
			t.Fatalf("instance %d does not serve on v1 before the scale", idx)
		}
		before[idx] = pod.UID
	}
	h.replicas = 2
	h.setTarget(h.revBad, badImage)
	atRest := func() bool {
		rows := h.irStatuses()
		if len(rows) != 2 || h.instance(0) != nil || len(h.podsOf(0)) != 0 {
			return false
		}
		for _, row := range rows {
			if row.Phase == workloadtypes.InstancePhaseDeleting {
				return false
			}
		}
		return true
	}
	keepServing := func() {
		t.Helper()
		if !untouched() {
			return
		}
		if got := len(h.servingPods()); got < 2 {
			h.dumpState("scale-down")
			t.Fatalf("serving pods dropped to %d during the scale-down; the two Instances the roll never reached must keep serving", got)
		}
		for idx, uid := range before {
			h.requireServingOnV1(idx, uid)
			for _, pod := range h.podsOf(idx) {
				if pod.Spec.Containers[0].Image == badImage {
					h.dumpState("survivor taken")
					t.Fatalf("instance %d was taken onto the pushed revision after the scale retired the attempt's Instance", idx)
				}
			}
		}
	}
	if !h.runWithInvariant(40, atRest, keepServing) {
		h.dumpState("scale-down")
		t.Fatalf("the component never came to rest at two instances with the stuck one gone")
	}
	for i := 0; i < 4; i++ {
		h.step()
		keepServing()
		if !atRest() {
			h.dumpState("after the scale-down")
			t.Fatalf("the component left its two-instance shape after the scale-down")
		}
	}
}

// TestScaleWhileRecreateAttemptOpen_LastAttemptEndsHeld: with a ladder of
// one attempt, the scale that retires the attempt's Instance leaves the
// revision Held with the pull failure as its reason, no attempt reopens on
// a survivor, and the status still reports the push that has not landed.
func TestScaleWhileRecreateAttemptOpen_LastAttemptEndsHeld(t *testing.T) {
	ladder := &workloadtypes.RetryPolicy{MaxAttempts: 1, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
	h := newStuckRecreateHarness(t, ladder)
	scaleStuckRecreateToTwo(t, h, func() bool { return true })

	block := h.findBlock(h.revBad.Name)
	if block == nil {
		t.Fatalf("the status stopped reporting the push that has not landed: no retry block for %s", h.revBad.Name)
	}
	if block.State != workloadtypes.RetryBlockHeld || block.AttemptsStarted != 1 || block.Reason != "ImagePullBackOff" {
		t.Fatalf("block after the scale = (state=%s attempts=%d reason=%q), want Held after the one attempt the scale ended, with its pull failure",
			block.State, block.AttemptsStarted, block.Reason)
	}
	if len(h.heldWarnings) != 1 {
		t.Fatalf("held warnings = %v, want the one the wave's record raised", h.heldWarnings)
	}
	for _, idx := range []int32{1, 2} {
		row := h.instance(idx)
		if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil || row.RunningRevision != h.revV1.Name {
			h.dumpState("survivor row")
			t.Fatalf("instance %d must stay Ready on v1 with no attempt open while the revision is Held, got %+v", idx, row)
		}
	}
}

// TestScaleWhileRecreateAttemptOpen_BelowTheBoundTheNextAttemptWaits: with
// attempts left on the ladder, the attempt the scale ended counts as one
// and the survivors are left alone until the backoff the ladder records
// for it; the next attempt then opens on a survivor, no sooner.
func TestScaleWhileRecreateAttemptOpen_BelowTheBoundTheNextAttemptWaits(t *testing.T) {
	h := newStuckRecreateHarness(t, pushRetryLadder)
	var due, recordedAt time.Time
	scaleStuckRecreateToTwo(t, h, func() bool {
		if block := h.findBlock(h.revBad.Name); block != nil && due.IsZero() {
			if block.State != workloadtypes.RetryBlockBackoff || block.AttemptsStarted != 1 ||
				block.Reason != "ImagePullBackOff" || block.NextRetryAt == nil {
				t.Fatalf("block as the wave recorded it = %+v, want Backoff after the one attempt the scale ended, with a retry ahead", block)
			}
			due, recordedAt = block.NextRetryAt.Time, h.clk.Now()
		}
		return due.IsZero() || h.clk.Now().Before(due)
	})
	if due.IsZero() {
		t.Fatalf("the status stopped reporting the push that has not landed: no retry block for %s", h.revBad.Name)
	}
	if !due.After(recordedAt) {
		t.Fatalf("the backoff was recorded due at %s, not ahead of %s", due.Format(time.RFC3339), recordedAt.Format(time.RFC3339))
	}
	reopened := func() bool {
		for _, idx := range []int32{1, 2} {
			if row := h.instance(idx); row != nil && row.Phase == workloadtypes.InstancePhaseUpdating {
				return true
			}
		}
		return false
	}
	if !h.run(20, reopened) {
		h.dumpState("backoff")
		t.Fatalf("the ladder has attempts left; the next must open on a survivor once the backoff is due")
	}
	if h.clk.Now().Before(due) {
		t.Fatalf("the next attempt opened at %s, before the backoff due at %s", h.clk.Now().Format(time.RFC3339), due.Format(time.RFC3339))
	}
	if block := h.findBlock(h.revBad.Name); block == nil || block.State != workloadtypes.RetryBlockRetryInProgress || block.AttemptsStarted != 1 {
		t.Fatalf("block with the next attempt open = %+v, want RetryInProgress after the one attempt the scale ended", block)
	}
}

// A push whose gang leaders crash after promotion, on a Component small
// enough that the budget lets the roll move every gang: with
// maxUnavailable 1 and minReadySeconds 0 the second gang's start is
// admitted while the first still serves, so both gangs end on the
// crashing revision and the Component's current revision follows them.
// From then on nothing is left for the roll to admit, so no budget hold
// is recorded, and the crashing leaders disagree with no current
// revision. The product must still say why the Component is down: a row
// on the crashing revision reads Failed naming the crash, and an
// InstanceFailed warning names the Instance.

// TestCrashAfterPromotion_WholeGangComponentMovedSaysWhy pins that rule
// on two leader+worker gangs under RecreatePod maxUnavailable 1 whose
// pushed leader comes back Ready between its crashes.
func TestCrashAfterPromotion_WholeGangComponentMovedSaysWhy(t *testing.T) {
	const replicas = 2
	h := newRecoveryHarnessFor(t, true, constants.MainContainerName)
	h.replicas = replicas
	noRepair := workloadtypes.RestartPolicyNone
	h.lifecycle.RestartPolicy = &noRepair
	// The leader serves long enough after promotion for the roll to move
	// the second gang before it dies, and only the leader dies; it dies
	// inside the stuck-pod grace, as a crash a few seconds after a
	// promotion reads on a cluster.
	h.crashAfterServed = 10
	h.crashLeaderOnly = true
	h.stuckGrace = 10 * time.Minute
	h.stepCap = recoveryStepFloor
	h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
	if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}

	revCrash := h.crashRevision(flapAfterServingImage)
	moved, crashed := false, false
	saysWhy := func() bool {
		for _, s := range h.irStatuses() {
			if s.Phase == workloadtypes.InstancePhaseFailed && s.RunningRevision == revCrash.Name {
				return true
			}
		}
		return false
	}
	// A repair may delete the crashed leader in the pass that saw it
	// crash, so the crash is read off the kubelet model's ledger, which
	// only a crash image writes to.
	h.runWithInvariant(200, saysWhy, func() {
		moved = moved || h.currentRevision == revCrash.Name
		crashed = crashed || len(h.crashes) > 0
	})
	if !moved {
		h.dumpState("roll")
		t.Fatalf("the roll did not move both gangs onto the crashing revision; the story did not run")
	}
	if !crashed {
		h.dumpState("no crash")
		t.Fatalf("the promoted revision never crashed; the story did not run")
	}
	if !saysWhy() {
		h.dumpState("silent")
		t.Fatalf("no row on the crashing revision %s reads Failed: status never said why the Component is down (holds=%d, failed warnings=%v, held warnings=%v)",
			revCrash.Name, len(h.holds), h.failedWarnings, h.heldWarnings)
	}
	if len(h.failedWarnings) == 0 {
		t.Fatalf("no InstanceFailed warning named the crashed gang; warnings=%v", h.failedWarnings)
	}
}

// A push whose gang leaders crash after promotion, on a Component large
// enough that the budget stops the roll before every gang has moved,
// under RecreateInstanceOnPodRestart: the policy rebuilds a gang whose
// runner restarted after Ready, and the leader dies again a few passes
// after every rebuild, inside the stuck-pod grace, so no pod ever reads
// as wedged and the repair alone would rebuild the gang forever. The
// running revision's retry ladder counts each crash and paces each
// rebuild: the leader's first comeback releases the gang's slot and the
// roll admits one further start, the ladder holds after its attempts
// with a RetryHeld warning, the crashed gang parks Failed naming the
// crash with an InstanceFailed warning, no repair opens on the revision
// from the hold on, and the roll keeps its floor. A corrected revision
// then lands on every gang.

// TestCrashAfterPromotion_GangPushStopsAtItsBudgetAndSaysWhy pins that
// rule on leader+worker gangs under RecreatePod maxUnavailable 1 and
// under SurgeThenDrain maxSurge 1.
func TestCrashAfterPromotion_GangPushStopsAtItsBudgetAndSaysWhy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		replicas   int32
		strategy   workloadtypes.UpdateStrategyType
		offAllowed int
	}{
		// offAllowed leaves one gang on the running revision: the budget's
		// arm, the start admitted while the promoted gang still serves under
		// RecreatePod, and the one start the first comeback admits.
		{name: "four gangs under RecreatePod maxUnavailable 1", replicas: 4, strategy: workloadtypes.UpdateStrategyRecreatePod, offAllowed: 3},
		{name: "three gangs under SurgeThenDrain maxSurge 1", replicas: 3, strategy: workloadtypes.UpdateStrategySurgeThenDrain, offAllowed: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarnessFor(t, true, constants.MainContainerName)
			h.replicas = tc.replicas
			repair := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &repair
			// The leader serves long enough for the roll to reach the next
			// gang before it dies, only the leader dies, and it dies well
			// inside the stuck-pod grace.
			h.crashAfterServed = 4
			h.crashLeaderOnly = true
			h.stuckGrace = 10 * time.Minute
			// The roll waits out the stuck-pod grace after each promote;
			// the leaders keep serving and dying meanwhile.
			h.stepCap = recoveryStepFloor
			h.useRollingBudget(tc.strategy)
			if !h.run(60, func() bool { return h.settledOn(h.revV1, tc.replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", tc.replicas)
			}

			revCrash := h.crashRevision(flapAfterServingImage)
			crashed, offAtCrash := false, 0
			restartsAtHeld := -1
			saysWhy := func() bool {
				for _, s := range h.irStatuses() {
					if s.Phase == workloadtypes.InstancePhaseFailed && s.RunningRevision == revCrash.Name {
						return len(h.failedWarnings) > 0
					}
				}
				return false
			}
			h.runWithInvariant(80, func() bool { return restartsAtHeld >= 0 && saysWhy() }, func() {
				off := int(tc.replicas) - h.instancesServingOn(goodImage)
				if off > tc.offAllowed {
					h.dumpState("roll past the budget")
					t.Fatalf("%d Instances are off the running revision, budget %d: the roll kept replacing gangs with a revision that crashes", off, tc.offAllowed)
				}
				if crashed && off > offAtCrash+1 {
					h.dumpState("roll continued after the crash")
					t.Fatalf("more than the one start the comeback admits left the running revision after a promoted gang crashed: %d off now, %d at the crash", off, offAtCrash)
				}
				if !crashed && h.promotedPodCrashed(revCrash) {
					crashed, offAtCrash = true, off
				}
				if restartsAtHeld < 0 && len(h.heldWarnings) > 0 {
					restartsAtHeld = h.restartsRecorded()
				}
			})
			if !crashed {
				h.dumpState("no crash")
				t.Fatalf("the promoted revision never crashed; the story did not run")
			}
			if restartsAtHeld < 0 {
				h.dumpState("ladder never held")
				t.Fatalf("the retry ladder never held the crashing revision %s: no RetryHeld warning (failed warnings=%v, repairs=%d)", revCrash.Name, h.failedWarnings, h.restartsRecorded())
			}
			if !saysWhy() {
				h.dumpState("silent")
				t.Fatalf("no row on the crashing revision %s reads Failed with an InstanceFailed warning (failed warnings=%v, held warnings=%v)", revCrash.Name, h.failedWarnings, h.heldWarnings)
			}
			// A repair in flight owns the passes it runs in; the roll reports
			// its hold once the pass reaches the update pass again: the
			// budget's, or the ladder's when the ladder denies the starts
			// before the budget is asked. A held ladder that leaves the pass
			// nothing to run is itself the status that says why.
			h.run(20, func() bool { return h.lastHold() != nil || h.heldOn(revCrash.Name) })
			if got := h.restartsRecorded(); got != restartsAtHeld {
				h.dumpState("repair after the hold")
				t.Fatalf("repairs opened after the ladder held: %d at the hold, %d after", restartsAtHeld, got)
			}
			hold := h.lastHold()
			if hold != nil && (hold.Target != revCrash.Name || (hold.Gate != workloadtypes.RolloutHoldGateBudget && hold.Gate != workloadtypes.RolloutHoldGateRetryBlock && hold.Gate != workloadtypes.RolloutHoldGateHeld)) {
				h.dumpState("hold")
				t.Fatalf("the Component must report a Budget, RetryBlock or Held hold on %s, got %+v", revCrash.Name, hold)
			}
			if hold == nil && !h.heldOn(revCrash.Name) {
				h.dumpState("hold")
				t.Fatalf("the Component must report a Budget hold on %s or hold its ladder", revCrash.Name)
			}
			if floor := int(tc.replicas) - tc.offAllowed; h.instancesServingOn(goodImage) < floor {
				h.dumpState("floor")
				t.Fatalf("the running revision serves on %d gangs, floor %d", h.instancesServingOn(goodImage), floor)
			}

			h.setTarget(h.revFixed, fixedImage)
			if !h.run(120, func() bool { return h.settledOn(h.revFixed, tc.replicas) }) {
				h.dumpState("corrected revision")
				t.Fatalf("the corrected revision did not land on every gang")
			}
		})
	}
}

// A break of a pod set that held the crash window is a steady-state
// repair, not the revision's failure: the repair opens at once, past the
// stuck-pod grace, and the running revision's retry ladder records
// nothing. Two breaks read so: a pod of a long-serving Instance pointed at
// an image that crashes at start, and a promoted pod set on the
// revision's own image whose runner first dies only after the window.

// TestSteadyStateBreak_IsRepairedAtOnceAndRecordsNothing pins that rule
// under both restart policies, on a single-pod Instance and on a
// leader+worker gang.
func TestSteadyStateBreak_IsRepairedAtOnceAndRecordsNothing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		multiPod bool
		policy   workloadtypes.RestartPolicy
		// late crashes the revision's own image after the pod set held the
		// window; false points a pod at an image that crashes at start.
		late bool
	}{
		{name: "single-pod Instance, pod pointed at a crashing image, policy None", policy: workloadtypes.RestartPolicyNone},
		{name: "single-pod Instance, pod pointed at a crashing image, policy RecreateInstanceOnPodRestart", policy: workloadtypes.RestartPolicyRecreateInstance},
		{name: "gang, leader pointed at a crashing image, policy None", multiPod: true, policy: workloadtypes.RestartPolicyNone},
		{name: "gang, leader pointed at a crashing image, policy RecreateInstanceOnPodRestart", multiPod: true, policy: workloadtypes.RestartPolicyRecreateInstance},
		{name: "single-pod Instance, the revision's own image dies after the window, policy None", late: true, policy: workloadtypes.RestartPolicyNone},
		{name: "single-pod Instance, the revision's own image dies after the window, policy RecreateInstanceOnPodRestart", late: true, policy: workloadtypes.RestartPolicyRecreateInstance},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const replicas = 2
			h := newRecoveryHarnessFor(t, tc.multiPod, constants.MainContainerName)
			h.recorder = record.NewFakeRecorder(64)
			h.replicas = replicas
			policy := tc.policy
			h.lifecycle.RestartPolicy = &policy
			// The pod set holds the window (the stuck-pod grace, with no
			// minReadySeconds) several times over before it breaks.
			h.crashAfterServed = 6
			h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
			if tc.late {
				h.crashRevision(flapAfterServingImage)
				h.currentRevision = ""
			}
			if !h.run(60, func() bool { return h.settledOn(h.target, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled for %d instances", replicas)
			}
			for i := 0; i < 3; i++ {
				h.step()
			}
			before := map[int32]int64{}
			for _, s := range h.irStatuses() {
				before[s.Index] = s.Incarnation
			}
			h.events = nil
			const broken = int32(0)
			if !tc.late {
				h.crashPod(broken, map[bool]string{true: "leader", false: ""}[tc.multiPod], crashStartImage)
			}

			repairedAt, pass := -1, 0
			h.runWithInvariant(12, func() bool { return false }, func() {
				pass++
				if len(h.blocks) != 0 {
					h.dumpState("ladder")
					t.Fatalf("a steady-state break counted on the retry ladder: blocks=%+v", h.blocks)
				}
				if repairedAt < 0 && h.restartsRecorded() > 0 {
					repairedAt = pass
				}
			})
			if repairedAt < 0 {
				h.dumpState("no repair")
				t.Fatalf("no repair opened on the broken Instance within 12 passes (events=%v)", h.events)
			}
			rebuilt := false
			for _, s := range h.irStatuses() {
				if s.Index == broken && s.Incarnation > before[broken] {
					rebuilt = true
				}
			}
			if !rebuilt {
				h.dumpState("not rebuilt")
				t.Fatalf("the broken Instance was not rebuilt at a new incarnation")
			}
			for _, w := range append(append([]string(nil), h.failedWarnings...), h.heldWarnings...) {
				t.Fatalf("a steady-state break was escalated: %s", w)
			}
		})
	}
}

// One restart of a promoted runner is not a crash of the revision: under
// RecreateInstanceOnPodRestart the policy rebuilds the Instance once, at
// once, and under restart policy None the kubelet's restart is the whole
// repair; the running revision's retry ladder records nothing, no row
// reads Failed, nothing is escalated or held, and the roll lands the
// revision on every Instance. The kubelet dates the restart before the
// pass that reads it, as it does on a cluster, and the runner dies ten
// seconds after its Instance entered Ready, inside the crash window.

// TestCrashOnceAfterPromotion_IsNeverACrashOfTheRevision pins that rule
// under both restart policies, on single-pod Instances and on
// leader+worker gangs, under both rolling budgets.
func TestCrashOnceAfterPromotion_IsNeverACrashOfTheRevision(t *testing.T) {
	cases := []struct {
		name     string
		strategy workloadtypes.UpdateStrategyType
		multiPod bool
		replicas int32
		policy   workloadtypes.RestartPolicy
	}{
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1, policy RecreateInstanceOnPodRestart", strategy: workloadtypes.UpdateStrategySurgeThenDrain, replicas: 4, policy: workloadtypes.RestartPolicyRecreateInstance},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1, policy RecreateInstanceOnPodRestart", strategy: workloadtypes.UpdateStrategyRecreatePod, replicas: 4, policy: workloadtypes.RestartPolicyRecreateInstance},
		{name: "two leader+worker gangs under RecreatePod maxUnavailable 1, policy RecreateInstanceOnPodRestart", strategy: workloadtypes.UpdateStrategyRecreatePod, multiPod: true, replicas: 2, policy: workloadtypes.RestartPolicyRecreateInstance},
		{name: "two leader+worker gangs under SurgeThenDrain maxSurge 1, policy RecreateInstanceOnPodRestart", strategy: workloadtypes.UpdateStrategySurgeThenDrain, multiPod: true, replicas: 2, policy: workloadtypes.RestartPolicyRecreateInstance},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1, policy None", strategy: workloadtypes.UpdateStrategyRecreatePod, replicas: 4, policy: workloadtypes.RestartPolicyNone},
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1, policy None", strategy: workloadtypes.UpdateStrategySurgeThenDrain, replicas: 4, policy: workloadtypes.RestartPolicyNone},
		{name: "two leader+worker gangs under RecreatePod maxUnavailable 1, policy None", strategy: workloadtypes.UpdateStrategyRecreatePod, multiPod: true, replicas: 2, policy: workloadtypes.RestartPolicyNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replicas := tc.replicas
			h := newRecoveryHarnessFor(t, tc.multiPod, constants.MainContainerName)
			h.recorder = record.NewFakeRecorder(256)
			h.replicas = replicas
			h.crashLeaderOnly = tc.multiPod
			policy := tc.policy
			h.lifecycle.RestartPolicy = &policy
			h.stepFloor = 5 * time.Second
			h.kubeletLag = time.Second
			h.crashAfterServed = 2
			h.crashOnceInComponent = true
			h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
			h.useRollingBudget(tc.strategy)
			if !h.run(200, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}

			revOnce := h.crashRevision(crashOnceAfterServingImage)
			var restartedUID types.UID
			restartedIndex := int32(-1)
			rebuilds := 0
			h.runWithInvariant(400, func() bool {
				return restartedIndex >= 0 && h.settledOn(revOnce, replicas) && h.restartsRecorded() == rebuilds && !h.repairInFlight()
			}, func() {
				for _, s := range h.irStatuses() {
					if s.Phase == workloadtypes.InstancePhaseFailed {
						h.dumpState("failed row")
						t.Fatalf("Instance %d reads Failed for a runner the kubelet restarted once (events=%v)", s.Index, h.events)
					}
				}
				for _, w := range append(append([]string(nil), h.failedWarnings...), h.heldWarnings...) {
					t.Fatalf("one restart of a promoted runner was escalated: %s", w)
				}
				if b := h.findBlock(revOnce.Name); b != nil {
					h.dumpState("counted")
					t.Fatalf("one restart of a promoted runner counted on %s's ladder: %+v", revOnce.Name, *b)
				}
				for idx := range h.crashedInstances {
					if restartedIndex < 0 {
						restartedIndex = idx
					}
				}
				for _, pod := range h.livePods() {
					if h.crashes[wedgeKey(pod)] > 0 && restartedUID == "" {
						restartedUID = pod.UID
					}
				}
				if restartedIndex >= 0 && tc.policy == workloadtypes.RestartPolicyRecreateInstance && rebuilds == 0 {
					rebuilds = h.restartsRecorded()
				}
			})
			if restartedIndex < 0 {
				h.dumpState("no crash")
				t.Fatalf("the promoted pod never crashed; the story did not run")
			}
			if !h.settledOn(revOnce, replicas) {
				h.dumpState("roll stalled")
				t.Fatalf("the roll did not land %s on every Instance; last hold %+v", revOnce.Name, h.lastHold())
			}
			var restarted *corev1.Pod
			for _, pod := range h.livePods() {
				if pod.UID == restartedUID {
					restarted = pod
				}
			}
			switch tc.policy {
			case workloadtypes.RestartPolicyRecreateInstance:
				if got := h.restartsRecorded(); got != 1 {
					h.dumpState("rebuilds")
					t.Fatalf("RecreateInstanceOnPodRestart rebuilt Instance %d %d times for one restart (events=%v)", restartedIndex, got, h.events)
				}
				for _, pod := range h.livePods() {
					if h.crashes[wedgeKey(pod)] > 0 {
						t.Fatalf("the pod the kubelet restarted once was not replaced under RecreateInstanceOnPodRestart: %s", pod.Name)
					}
				}
			default:
				if got := h.restartsRecorded(); got != 0 {
					t.Fatalf("restart policy None opened %d repair(s) for a runner the kubelet restarted (events=%v)", got, h.events)
				}
				if restarted == nil {
					t.Fatalf("the pod the kubelet restarted once must keep its identity under restart policy None; %s is gone", restartedUID)
				}
			}
		})
	}
}

// One kill of a serving runner, seconds after its Instance entered Ready
// and with no roll in flight, is not a crash of the running revision:
// under restart policy None the kubelet's restart is the whole repair and
// under RecreateInstanceOnPodRestart the policy rebuilds the Instance
// once, at once; the running revision's retry ladder records nothing, no
// row reads Failed and nothing is escalated or held.

// TestRunnerKilledOnceAfterReady_IsNeverACrashOfTheRevision pins that
// rule under both restart policies, on a single-pod Instance and on a
// leader+worker gang.
func TestRunnerKilledOnceAfterReady_IsNeverACrashOfTheRevision(t *testing.T) {
	for _, tc := range []struct {
		name     string
		multiPod bool
		policy   workloadtypes.RestartPolicy
	}{
		{name: "single-pod Instance, policy None", policy: workloadtypes.RestartPolicyNone},
		{name: "single-pod Instance, policy RecreateInstanceOnPodRestart", policy: workloadtypes.RestartPolicyRecreateInstance},
		{name: "gang, the leader killed, policy None", multiPod: true, policy: workloadtypes.RestartPolicyNone},
		{name: "gang, the leader killed, policy RecreateInstanceOnPodRestart", multiPod: true, policy: workloadtypes.RestartPolicyRecreateInstance},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const replicas = int32(2)
			h := newRecoveryHarnessFor(t, tc.multiPod, constants.MainContainerName)
			h.recorder = record.NewFakeRecorder(256)
			h.replicas = replicas
			policy := tc.policy
			h.lifecycle.RestartPolicy = &policy
			h.stepFloor = 5 * time.Second
			h.kubeletLag = time.Second
			h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
			h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
			if !h.run(200, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}
			var victim *corev1.Pod
			for _, pod := range h.livePods() {
				if idx, ok := query.InstanceIdxFromLabels(pod); ok && idx == 0 && (!tc.multiPod || strings.Contains(pod.Name, "leader")) {
					victim = pod
				}
			}
			if victim == nil {
				t.Fatalf("no pod of Instance 0 to kill")
			}
			h.killRunnerOnce(victim)
			h.events = nil
			h.runWithInvariant(60, func() bool {
				return h.crashes[wedgeKey(victim)] > 0 && h.settledOn(h.revV1, replicas) && !h.repairInFlight()
			}, func() {
				for _, s := range h.irStatuses() {
					if s.Phase == workloadtypes.InstancePhaseFailed {
						h.dumpState("failed row")
						t.Fatalf("Instance %d reads Failed for a runner killed once (events=%v)", s.Index, h.events)
					}
				}
				for _, w := range append(append([]string(nil), h.failedWarnings...), h.heldWarnings...) {
					t.Fatalf("one kill of a serving runner was escalated: %s", w)
				}
				if len(h.blocks) != 0 {
					h.dumpState("counted")
					t.Fatalf("one kill of a serving runner counted on a retry ladder: %+v", h.blocks)
				}
			})
			if h.crashes[wedgeKey(victim)] == 0 {
				t.Fatalf("the kubelet model never killed %s; the story did not run", victim.Name)
			}
			if !h.settledOn(h.revV1, replicas) {
				h.dumpState("not settled")
				t.Fatalf("the Component did not settle after one kill of %s (events=%v)", victim.Name, h.events)
			}
			alive := false
			for _, pod := range h.livePods() {
				if pod.UID == victim.UID {
					alive = true
				}
			}
			switch tc.policy {
			case workloadtypes.RestartPolicyRecreateInstance:
				if got := h.restartsRecorded(); got != 1 {
					t.Fatalf("RecreateInstanceOnPodRestart rebuilt the Instance %d times for one kill (events=%v)", got, h.events)
				}
				if alive {
					t.Fatalf("the killed pod %s was not replaced under RecreateInstanceOnPodRestart", victim.Name)
				}
			default:
				if got := h.restartsRecorded(); got != 0 {
					t.Fatalf("restart policy None opened %d repair(s) for a runner killed once (events=%v)", got, h.events)
				}
				if !alive {
					t.Fatalf("the killed pod %s must keep its identity under restart policy None", victim.Name)
				}
			}
		})
	}
}

// A repair whose rebuilt pod no node has room for stays open for as long
// as the scheduler refuses it, its deadline parked by the scheduler hold.
// The update attempt already in flight on another Instance keeps
// advancing beside it: its replacement gets the serving gate once it is
// ContainersReady, so the attempt never runs out its deadline with a
// healthy replacement held out of rotation and no row reads Failed. Under
// maxUnavailable 1 the attempt completes while the repair waits; under
// maxUnavailable 0 the Instance under repair holds the floor, so the
// replacement serves beside its source. Once the rebuilt pod places, the
// repair completes and the roll lands on every Instance.

// TestRepairWaitingOnPlacement_UpdateInFlightKeepsAdvancing pins that
// rule under both budgets, past the operation deadline.
func TestRepairWaitingOnPlacement_UpdateInFlightKeepsAdvancing(t *testing.T) {
	const replicas = int32(3)
	const deadline = 4 * recoveryStepFloor
	for _, tc := range []struct {
		name           string
		maxUnavailable int
		// completes: the attempt reaches Ready on the target while the
		// repair waits; otherwise its replacement serves beside its source.
		completes bool
	}{
		{name: "maxUnavailable 1, the attempt completes while the repair waits", maxUnavailable: 1, completes: true},
		{name: "maxUnavailable 0, the replacement serves beside its source", maxUnavailable: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
			h.recorder = record.NewFakeRecorder(256)
			h.replicas = replicas
			policy := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &policy
			h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: deadline}
			h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
			maxUnavailable := intstr.FromInt(tc.maxUnavailable)
			h.lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable = &maxUnavailable
			if !h.run(80, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}

			h.setTarget(h.revFixed, fixedImage)
			hash := query.RevisionOf(h.revFixed).Hash()
			// The repair opens on the pass that first reads a replacement
			// ContainersReady: the runner of an Instance already promoted
			// onto the target dies on the kubelet step that starts the
			// replacement, and no node has room for its rebuilt pod.
			var victim, replacement *corev1.Pod
			h.runWithInvariant(60, func() bool { return victim != nil }, func() {
				if victim != nil {
					return
				}
				var promoted, starting *corev1.Pod
				for _, pod := range h.livePods() {
					if pod.Labels[query.LabelRevisionHash] != hash {
						continue
					}
					switch {
					case h.promotedOn(pod):
						promoted = pod
					case pod.Status.Phase == "":
						starting = pod
					}
				}
				if promoted == nil || starting == nil {
					return
				}
				victim, replacement = promoted, starting
				h.killRunnerOnce(victim)
				h.unplaceable = map[string]bool{victim.Name: true}
			})
			if victim == nil {
				h.dumpState("no trigger")
				t.Fatalf("the roll never started a replacement beside an Instance promoted onto the target")
			}
			repairIdx, surgeIdx := instanceIndexOf(victim), instanceIndexOf(replacement)
			row := func(idx int32) *workloadtypes.InstanceStatus {
				for _, s := range h.irStatuses() {
					if s.Index == idx {
						return &s
					}
				}
				return nil
			}

			until := h.clk.Now().Add(3 * deadline)
			h.runWithInvariant(60, func() bool { return !h.clk.Now().Before(until) }, func() {
				for _, s := range h.irStatuses() {
					if s.Phase == workloadtypes.InstancePhaseFailed {
						h.dumpState("failed row")
						t.Fatalf("Instance %d reads Failed while the repair of Instance %d waits on placement (warnings=%v)", s.Index, repairIdx, h.failedWarnings)
					}
				}
			})
			if h.clk.Now().Before(until) {
				h.dumpState("deadline not crossed")
				t.Fatalf("the story stopped at %s, before the operation deadline had passed", h.clk.Now())
			}
			if r := row(repairIdx); r == nil || r.Phase != workloadtypes.InstancePhaseRestarting {
				h.dumpState("repair closed")
				t.Fatalf("the repair of Instance %d must stay open while its rebuilt pod has no room; got %+v", repairIdx, r)
			}
			stored := &corev1.Pod{}
			if err := h.c.Get(h.ctx, client.ObjectKeyFromObject(replacement), stored); err != nil || !podreadiness.IsServing(stored) {
				h.dumpState("replacement out of rotation")
				t.Fatalf("the replacement %s must serve while the repair waits (err=%v)", replacement.Name, err)
			}
			r := row(surgeIdx)
			if tc.completes {
				if r == nil || r.Phase != workloadtypes.InstancePhaseReady || r.Operation != nil || r.RunningRevision != h.revFixed.Name {
					h.dumpState("attempt not complete")
					t.Fatalf("Instance %d must complete its update while the repair waits; got %+v", surgeIdx, r)
				}
			} else {
				if r == nil || r.Operation == nil || r.Operation.Step != workloadtypes.UpdateStepSurge {
					h.dumpState("attempt moved past the floor")
					t.Fatalf("Instance %d must hold at Step=Surge below the floor; got %+v", surgeIdx, r)
				}
				if h.instancesServingOn(goodImage) < int(replicas)-1 {
					h.dumpState("source out of rotation")
					t.Fatalf("the source of Instance %d must stay in rotation below the floor", surgeIdx)
				}
			}

			delete(h.unplaceable, victim.Name)
			if !h.run(80, func() bool { return h.settledOn(h.revFixed, replicas) && !h.repairInFlight() }) {
				h.dumpState("after placement")
				t.Fatalf("the repair did not complete and the roll did not land once the rebuilt pod placed")
			}
		})
	}
}

// TestRunnerKilledTwiceAfterPromotion_ServesTheWindowAndTheRollResumes
// pins the floor's release for a promoted runner killed more than once
// under restart policy None: the row never leaves Ready, so its anchor
// stays where promotion set it and the kubelet's record reads the runner
// as restarted again from the second kill on. The Instance holds its slot
// while the pod is down and while it is Ready again for less than the
// window, so no further source leaves rotation; once the pod has held
// Ready for the window it serves, the floor lifts and the roll lands the
// revision on every Instance. The pod keeps its identity throughout.
func TestRunnerKilledTwiceAfterPromotion_ServesTheWindowAndTheRollResumes(t *testing.T) {
	const replicas = int32(4)
	// No minReadySeconds: the stuck-pod grace alone is the window.
	const window = 90 * time.Second
	h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
	h.replicas = replicas
	noRepair := workloadtypes.RestartPolicyNone
	h.lifecycle.RestartPolicy = &noRepair
	h.stuckGrace = window
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	if !h.run(80, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}

	h.setTarget(h.revFixed, fixedImage)
	hash := query.RevisionOf(h.revFixed).Hash()
	var victimUID types.UID
	victimIndex := int32(-1)
	kills, heldInsideWindow := 0, false
	var healedAt time.Time
	// passStart is the clock the pass under inspection ran at: the
	// invariant runs after the step moved the clock on.
	passStart := h.clk.Now()
	holds := h.notServingHolds()
	h.runWithInvariant(200, func() bool { return h.settledOn(h.revFixed, replicas) }, func() {
		defer func() {
			holds = h.notServingHolds()
			passStart = h.clk.Now()
		}()
		var victim *corev1.Pod
		for _, pod := range h.livePods() {
			if victimUID != "" {
				if pod.UID == victimUID {
					victim = pod
				}
				continue
			}
			// The kills land on the first pod promoted onto the pushed
			// revision.
			if idx, ok := query.InstanceIdxFromLabels(pod); ok && pod.Labels[query.LabelRevisionHash] == hash && h.promotedOn(pod) {
				victimUID, victimIndex, victim = pod.UID, idx, pod
			}
		}
		if victimUID == "" {
			return
		}
		if victim == nil {
			h.dumpState("victim gone")
			t.Fatalf("the killed pod %s must keep its identity under restart policy None", victimUID)
		}
		// The first kill lands once another Instance's surge is in flight,
		// so the drain it owes is what the floor withholds; the second once
		// the runner is Ready again after the first.
		surgeInFlight := false
		for _, s := range h.irStatuses() {
			if s.Index != victimIndex && s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationUpdate && s.Operation.Step == workloadtypes.UpdateStepSurge {
				surgeInFlight = true
			}
		}
		ready := podConditionTrue(victim, corev1.PodReady)
		switch {
		case kills == 0 && ready && surgeInFlight, kills == 1 && h.crashes[wedgeKey(victim)] == 1 && ready:
			h.killRunnerOnce(victim)
			kills++
		case kills == 2 && h.crashes[wedgeKey(victim)] == 2 && ready && healedAt.IsZero():
			healedAt = podReadyTransition(victim)
		}
		if h.notServingHolds() == holds {
			return
		}
		hold := h.lastHold()
		if !holdNamesInstance(hold, victimIndex) {
			return
		}
		if !healedAt.IsZero() && !passStart.Before(healedAt.Add(window)) {
			h.dumpState("held while serving")
			t.Fatalf("the roll was held on Instance %d, Ready again since %s, at %s: a pod Ready for the window serves and releases its slot: %q",
				victimIndex, healedAt.Format(time.RFC3339), passStart.Format(time.RFC3339), hold.Reason)
		}
		if kills > 0 && strings.Contains(hold.Reason, "no source leaves rotation") {
			heldInsideWindow = true
		}
	})
	if kills < 2 || healedAt.IsZero() {
		h.dumpState("no comeback")
		t.Fatalf("the promoted runner was killed %d time(s) and came back at %v; the story did not run", kills, healedAt)
	}
	if !heldInsideWindow {
		h.dumpState("no hold")
		t.Fatalf("no pass withheld the drain on Instance %d while its killed runner was inside the window", victimIndex)
	}
	if !h.settledOn(h.revFixed, replicas) {
		h.dumpState("roll stalled")
		t.Fatalf("the roll did not land %s on every Instance after Instance %d served the window; last hold %+v", h.revFixed.Name, victimIndex, h.lastHold())
	}
	for _, s := range h.irStatuses() {
		if s.Phase == workloadtypes.InstancePhaseFailed {
			h.dumpState("failed row")
			t.Fatalf("Instance %d reads Failed for a runner the kubelet restarted", s.Index)
		}
	}
	if got := h.restartsRecorded(); got != 0 {
		t.Fatalf("restart policy None opened %d repair(s) for a runner killed twice (events=%v)", got, h.events)
	}
	alive := false
	for _, pod := range h.livePods() {
		if pod.UID == victimUID {
			alive = true
		}
	}
	if !alive {
		t.Fatalf("the killed pod %s must keep its identity under restart policy None", victimUID)
	}
}

// A pushed revision whose runner dies once on every set after it is
// Ready, under RecreateInstanceOnPodRestart: the policy rebuilds the
// first set at once, and from the rebuilt set's death on the running
// revision's retry ladder counts the crash and paces the rebuild, so the
// status says why before any further crash-revision pod is created, the
// ladder holds after its attempts, and nothing rebuilds the revision past
// the hold. The runner exits cleanly, as a runner that stops itself does.

// TestCrashAfterPromotion_EverySetDiesOnceAfterReady_LadderSaysWhy pins
// that rule on single-pod Instances and on leader+worker gangs, under
// SurgeThenDrain maxSurge 1 and RecreatePod maxUnavailable 1.
func TestCrashAfterPromotion_EverySetDiesOnceAfterReady_LadderSaysWhy(t *testing.T) {
	cases := []struct {
		name     string
		strategy workloadtypes.UpdateStrategyType
		multiPod bool
		exitCode int32
		// atomic drives the batched status writer the InferenceReplica
		// adapter wires, the path a gang surge promotes its target through.
		atomic bool
	}{
		{name: "four single-pod Instances under SurgeThenDrain maxSurge 1, clean exit", strategy: workloadtypes.UpdateStrategySurgeThenDrain, exitCode: 0},
		{name: "four single-pod Instances under RecreatePod maxUnavailable 1, clean exit", strategy: workloadtypes.UpdateStrategyRecreatePod, exitCode: 0},
		{name: "four leader+worker gangs under SurgeThenDrain maxSurge 1, clean exit", strategy: workloadtypes.UpdateStrategySurgeThenDrain, multiPod: true, exitCode: 0},
		{name: "four leader+worker gangs under SurgeThenDrain maxSurge 1, clean exit, batched status writes", strategy: workloadtypes.UpdateStrategySurgeThenDrain, multiPod: true, exitCode: 0, atomic: true},
		{name: "four leader+worker gangs under RecreatePod maxUnavailable 1, clean exit", strategy: workloadtypes.UpdateStrategyRecreatePod, multiPod: true, exitCode: 0},
		{name: "four leader+worker gangs under RecreatePod maxUnavailable 1, clean exit, batched status writes", strategy: workloadtypes.UpdateStrategyRecreatePod, multiPod: true, exitCode: 0, atomic: true},
		{name: "four leader+worker gangs under RecreatePod maxUnavailable 1, exit 1", strategy: workloadtypes.UpdateStrategyRecreatePod, multiPod: true, exitCode: 1},
	}
	const replicas = int32(4)
	const attempts = int32(3)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarnessFor(t, tc.multiPod, constants.MainContainerName)
			h.recorder = record.NewFakeRecorder(512)
			h.atomicStatus = tc.atomic
			h.replicas = replicas
			h.crashLeaderOnly = tc.multiPod
			rebuild := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &rebuild
			h.stepFloor = 5 * time.Second
			// The roll waits out the proven window after each promote; the
			// runners keep serving and dying on their own clock meanwhile.
			h.stepCap = h.stepFloor
			h.kubeletLag = time.Second
			// Every set dies twenty seconds after its Instance entered Ready,
			// inside the stuck-pod grace that bounds the crash window.
			h.crashAfterServed = 4
			h.crashEverySet = true
			code := tc.exitCode
			h.crashExitCode = &code
			h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: attempts, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
			h.useRollingBudget(tc.strategy)
			if !h.run(200, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}

			revCrash := h.crashRevision(crashOnceAfterServingImage)
			seen := map[types.UID]struct{}{}
			// setsSeen records, per Instance, every crash-revision pod UID
			// any pass observed; a UID beyond the ones at the loop's first
			// sighting is a further set.
			setsSeen := map[int32]map[types.UID]struct{}{}
			setsAtLoop := map[int32]int{}
			deaths := 0
			saidWhyAt, heldAt := -1, -1
			var uidsAtHeld int
			pass := 0
			parked := false
			h.runWithInvariant(400, func() bool { return heldAt >= 0 && pass >= heldAt+12 }, func() {
				pass++
				h.trackPodsOnImage(crashOnceAfterServingImage, seen)()
				for _, st := range h.irStatuses() {
					if st.Phase == workloadtypes.InstancePhaseFailed {
						parked = true
					}
				}
				for _, pod := range h.livePods() {
					idx, ok := query.InstanceIdxFromLabels(pod)
					if !ok || pod.Spec.Containers[0].Image != crashOnceAfterServingImage {
						continue
					}
					if setsSeen[idx] == nil {
						setsSeen[idx] = map[types.UID]struct{}{}
					}
					setsSeen[idx][pod.UID] = struct{}{}
				}
				deaths = 0
				for idx, n := range h.crashedInstances {
					if n > deaths {
						deaths = n
					}
					if n >= 2 {
						if _, noted := setsAtLoop[idx]; !noted {
							setsAtLoop[idx] = len(setsSeen[idx])
						}
					}
				}
				b := h.findBlock(revCrash.Name)
				saysWhy := b != nil || len(h.heldWarnings) > 0 || len(h.failedWarnings) > 0
				if saysWhy && saidWhyAt < 0 {
					saidWhyAt = pass
				}
				if len(h.heldWarnings) > 0 && heldAt < 0 {
					heldAt, uidsAtHeld = pass, len(seen)
				}
				// Once an Instance's rebuilt set has died too, the revision
				// is looping: the ladder counts it before a third set of that
				// Instance is built.
				for idx, at := range setsAtLoop {
					if len(setsSeen[idx]) > at && saidWhyAt < 0 {
						h.dumpState("third set before any word")
						t.Fatalf("Instance %d got a further crash-revision set (%d pods seen for it, %d at its second death) before the status said why", idx, len(setsSeen[idx]), at)
					}
				}
				if heldAt >= 0 && len(seen) > uidsAtHeld {
					h.dumpState("rebuilt after the hold")
					t.Fatalf("a crash-revision pod was created after the ladder held (%d pods, %d at the hold)", len(seen), uidsAtHeld)
				}
				if b != nil && b.AttemptsStarted > attempts {
					t.Fatalf("the ladder counted %d attempts, bound %d", b.AttemptsStarted, attempts)
				}
			})
			if deaths < 2 {
				h.dumpState("no loop")
				t.Fatalf("the story did not run: %d crash-revision pod(s) died", deaths)
			}
			if saidWhyAt < 0 {
				h.dumpState("silent")
				t.Fatalf("the status never said why: no RetryBlock for %s, no RetryHeld or InstanceFailed warning (deaths=%d, pods=%d)", revCrash.Name, deaths, len(seen))
			}
			if heldAt < 0 {
				h.dumpState("never held")
				t.Fatalf("the ladder never held %s (deaths=%d, pods=%d, blocks=%+v)", revCrash.Name, deaths, len(seen), h.blocks)
			}
			b := h.findBlock(revCrash.Name)
			if b == nil || b.State != workloadtypes.RetryBlockHeld || b.AttemptsStarted != attempts {
				t.Fatalf("the held ladder must record %d attempts, got %+v", attempts, b)
			}
			// The row the hold parks reads Failed with an InstanceFailed
			// warning naming the crash; a parked row whose pod set comes up
			// again may resume Ready under the hold, which keeps saying why.
			if !parked || len(h.failedWarnings) == 0 {
				h.dumpState("no Failed row")
				t.Fatalf("a row on the held revision must read Failed with an InstanceFailed warning (failed warnings=%v)", h.failedWarnings)
			}
		})
	}
}

// A promoted pod that dies under load and comes back between restarts:
// once the ladder holds, the row reads Failed while the pod is down and
// Ready naming the loop while it serves, and leaves that state on its own.
func TestCrashLoopAfterPromotion_HeldRowFollowsItsServingPod(t *testing.T) {
	const (
		replicas = 4
		// offAllowed leaves one Instance on the running revision: the
		// budget's arm, the start admitted while the promoted pod still
		// served, and the one start the first comeback admits.
		offAllowed = 3
		window     = 10 * time.Minute
	)
	h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
	h.replicas = replicas
	repair := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &repair
	h.crashAfterServed = 4
	// The proven window is the stuck-pod grace, which outlives every pod's
	// age for the whole story, so no crash is read as a wedge and the loop
	// is the ladder's story alone; an up-window is one pass, inside it.
	h.stuckGrace = window
	h.stepCap = recoveryStepFloor
	h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
	if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}

	revCrash := h.crashRevision(flapAfterServingImage)
	namesLoop := func(s workloadtypes.InstanceStatus) bool {
		return s.LastFailure != nil && strings.Contains(s.LastFailure.Message, "crash loop")
	}
	// remembersCrash reports whether the row's failure record is of its
	// current set: dated after the row entered Ready. A rebuilt set that
	// has not crashed yet is plain Ready until it does.
	remembersCrash := func(s workloadtypes.InstanceStatus) bool {
		return s.LastFailure != nil && s.ReadySince != nil && s.LastFailure.Time.After(s.ReadySince.Time)
	}
	// crashRows are the settled rows on the crashing revision, each with
	// the one live pod of its set.
	type crashRow struct {
		row workloadtypes.InstanceStatus
		pod *corev1.Pod
	}
	crashRows := func() []crashRow {
		var out []crashRow
		for _, s := range h.irStatuses() {
			if s.RunningRevision != revCrash.Name || s.Operation != nil {
				continue
			}
			if pods := h.podsOf(s.Index); len(pods) == 1 {
				out = append(out, crashRow{row: s, pod: pods[0]})
			}
		}
		return out
	}
	serves := func(pod *corev1.Pod) bool {
		return podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod)
	}
	proven := func(pod *corev1.Pod) bool {
		available, _ := podreadiness.IsPodAvailable(pod, int32(window/time.Second), h.clk.Now())
		return available
	}
	seen := map[types.UID]struct{}{}
	servingReadyAfterHold, downFailedAfterHold := false, false
	uidsAtHeld := -1
	// observe enforces the rule on every pass: a serving pod's row is never
	// Failed, and under the hold it names the loop until proven and reads
	// Failed while down; the roll's bound and the no-rebuild promise hold.
	observe := func() {
		h.trackPodsOnImage(flapAfterServingImage, seen)()
		held := h.heldOn(revCrash.Name)
		if held && uidsAtHeld < 0 {
			uidsAtHeld = len(seen)
		}
		if uidsAtHeld >= 0 && len(seen) != uidsAtHeld {
			h.dumpState("attempt after the hold")
			t.Fatalf("a crash-revision pod was created after the ladder held: %d at the hold, %d after", uidsAtHeld, len(seen))
		}
		if off := replicas - h.instancesServingOn(goodImage); off > offAllowed {
			h.dumpState("roll past the budget")
			t.Fatalf("%d Instances are off the running revision, allowance %d", off, offAllowed)
		}
		for _, cr := range crashRows() {
			switch {
			case serves(cr.pod):
				if cr.row.Phase == workloadtypes.InstancePhaseFailed {
					h.dumpState("failed while serving")
					t.Fatalf("Instance %d reads Failed while its only pod is Ready and serving (ladder held=%v, record %+v)", cr.row.Index, held, cr.row.LastFailure)
				}
				if held && cr.row.Phase == workloadtypes.InstancePhaseReady && remembersCrash(cr.row) {
					if !proven(cr.pod) && !namesLoop(cr.row) {
						h.dumpState("silent while serving")
						t.Fatalf("Instance %d serves between crashes under the held ladder without naming the crash loop: %+v", cr.row.Index, cr.row.LastFailure)
					}
					servingReadyAfterHold = true
				}
			case held && !podreadiness.IsContainersReady(cr.pod):
				if cr.row.Phase != workloadtypes.InstancePhaseFailed {
					h.dumpState("down but not Failed")
					t.Fatalf("Instance %d reads %s while its pod is down under the held ladder, want Failed", cr.row.Index, cr.row.Phase)
				}
				downFailedAfterHold = true
			}
		}
	}
	h.runWithInvariant(120, func() bool { return servingReadyAfterHold && downFailedAfterHold }, observe)
	if !h.heldOn(revCrash.Name) {
		h.dumpState("ladder never held")
		t.Fatalf("the retry ladder never held %s (held warnings=%v, failed warnings=%v)", revCrash.Name, h.heldWarnings, h.failedWarnings)
	}
	if !servingReadyAfterHold || !downFailedAfterHold {
		h.dumpState("states")
		t.Fatalf("under the held ladder the row must read Ready naming the loop while the pod serves (seen=%v) and Failed while it is down (seen=%v)", servingReadyAfterHold, downFailedAfterHold)
	}
	if len(h.failedWarnings) == 0 {
		t.Fatalf("an InstanceFailed warning must name the parked Instance; got none")
	}
	// The roll holds as it does for any crashing revision: a hold on the
	// revision, and the running revision serving on the floor.
	h.runWithInvariant(20, func() bool { return h.lastHold() != nil }, observe)
	if hold := h.lastHold(); hold == nil || hold.Target != revCrash.Name ||
		(hold.Gate != workloadtypes.RolloutHoldGateBudget && hold.Gate != workloadtypes.RolloutHoldGateRetryBlock && hold.Gate != workloadtypes.RolloutHoldGateHeld) {
		h.dumpState("hold")
		t.Fatalf("the Component must report a Budget, RetryBlock or Held hold on %s, got %+v", revCrash.Name, hold)
	}
	if floor := replicas - offAllowed; h.instancesServingOn(goodImage) < floor {
		h.dumpState("floor")
		t.Fatalf("the running revision serves on %d Instances, floor %d", h.instancesServingOn(goodImage), floor)
	}

	// The loop stops: the pods hold Ready past the proven window and every
	// row returns to plain Ready with the note cleared, and stays there.
	h.crashAfterServed = 1 << 20
	plainReady := func() bool {
		rows := crashRows()
		if len(rows) == 0 {
			return false
		}
		for _, cr := range rows {
			if cr.row.Phase != workloadtypes.InstancePhaseReady || namesLoop(cr.row) || !proven(cr.pod) {
				return false
			}
		}
		return true
	}
	if !h.runWithInvariant(40, plainReady, observe) {
		h.dumpState("loop stopped")
		t.Fatalf("the rows did not return to plain Ready once their pods held Ready for the window: %+v", crashRows())
	}
	h.runWithInvariant(5, func() bool { return false }, func() {
		observe()
		if !plainReady() {
			h.dumpState("plain Ready")
			t.Fatalf("a row left plain Ready while its pod kept serving: %+v", crashRows())
		}
	})

	// The pods die and stay down: every row parks Failed and stays there,
	// and the held ladder rebuilds nothing.
	h.crashAfterServed = 4
	for _, cr := range crashRows() {
		h.keepRunnerDown(cr.pod)
	}
	allFailed := func() bool {
		rows := crashRows()
		if len(rows) == 0 {
			return false
		}
		for _, cr := range rows {
			if cr.row.Phase != workloadtypes.InstancePhaseFailed {
				return false
			}
		}
		return true
	}
	if !h.runWithInvariant(10, allFailed, observe) {
		h.dumpState("stays down")
		t.Fatalf("the rows did not park Failed once their pods died and stayed down: %+v", crashRows())
	}
	h.runWithInvariant(5, func() bool { return false }, func() {
		observe()
		if !allFailed() {
			h.dumpState("stays Failed")
			t.Fatalf("a park whose pod stays down must stay Failed: %+v", crashRows())
		}
	})

	h.setTarget(h.revFixed, fixedImage)
	if !h.run(120, func() bool { return h.settledOn(h.revFixed, replicas) }) {
		h.dumpState("corrected revision")
		t.Fatalf("the corrected revision did not land on every Instance")
	}
}

// componentCounters is the Component publication the adapter writes from
// the rows and the pods as they stand: the counters the status surface
// reports beside the rows.
func (h *recoveryHarness) componentCounters() workload.ComponentCounters {
	h.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := h.c.Get(h.ctx, h.irKey(), ir); err != nil {
		h.t.Fatalf("counters: get IR: %v", err)
	}
	list := &corev1.PodList{}
	if err := h.c.List(h.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("counters: list pods: %v", err)
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, &list.Items[i])
	}
	observation, err := workload.NewOwnedPublicationObservation(
		v1beta1convert.InstanceStatusSliceToWorkload(ir.Status.InstanceStatuses),
		workload.NewCachedSelectorPodObservation(pods, query.BucketPodsByInstanceIdx(pods)),
		h.availableByPod(),
		status.AvailabilityWindow{MinReadySeconds: h.minReadySeconds, Now: h.clk.Now()},
	)
	if err != nil {
		h.t.Fatalf("counters: build observation: %v", err)
	}
	plan, err := workload.BuildPlan(h.component, h.desired, h.buildInput().ObservedState)
	if err != nil {
		h.t.Fatalf("counters: build plan: %v", err)
	}
	_, counters, err := observation.TakeInlineV1Publication(status.DesiredPodCountByInstance(plan), h.target.Name)
	if err != nil {
		h.t.Fatalf("counters: consume observation: %v", err)
	}
	return counters
}

// terminatingPodsOf is the pods of Instance idx the apiserver still holds
// under a deletion timestamp.
func (h *recoveryHarness) terminatingPodsOf(idx int32) []*corev1.Pod {
	h.t.Helper()
	list := &corev1.PodList{}
	if err := h.c.List(h.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	var out []*corev1.Pod
	for i := range list.Items {
		pod := &list.Items[i]
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == idx && pod.DeletionTimestamp != nil {
			out = append(out, pod)
		}
	}
	return out
}

// TestRollbackOverSurge_ResumedManagerClosesTheAbandonedSurge pins the end
// state of a rollback that lands over a single-pod surge whose replacement
// is not yet runtime-ready while the manager restarts: the manager that
// applied the rollback deleted the replacement and died with that delete
// in flight, and a manager that resumes with nothing in memory reads the
// row from status alone and closes the abandoned surge the moment the
// replacement is gone. Every row ends Ready on the starting revision with
// no operation open, and the Component counts every Instance as updated.
// Every pass here runs with a fresh expectations cache, which is all a
// restarted manager loses between passes.
func TestRollbackOverSurge_ResumedManagerClosesTheAbandonedSurge(t *testing.T) {
	const replicas = int32(2)
	h := newRecoveryHarness(t, false)
	h.replicas = replicas
	// Deletes linger as Terminating until their grace elapses, as on a
	// cluster; the abandoned replacement goes on the bounded grace.
	h.podGrace = 10 * time.Minute
	h.abandonGrace = 30 * time.Second
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	if !h.run(30, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}

	// A replacement that never passes readiness keeps the surge uncommitted
	// for as long as the story needs.
	revNew := h.ensureRevision(h.podSpec(stuckImage))
	h.setTarget(revNew, stuckImage)
	if !h.run(10, func() bool { return h.surgeOpen(0) && h.livePodOnImage(stuckImage) }) {
		h.dumpState("surge toward the new revision")
		t.Fatalf("the surge toward the new revision never opened")
	}
	if h.surgeOpen(1) {
		t.Fatalf("the second Instance surged beside the first under a surge budget of one")
	}

	// The starting revision is re-applied while the replacement is not yet
	// runtime-ready: the pass abandons the surge and deletes the
	// replacement, and the manager dies with that delete in flight.
	h.setTarget(h.revV1, goodImage)
	h.step()
	if got := h.terminatingPodsOf(0); len(got) != 1 || got[0].Spec.Containers[0].Image != stuckImage {
		h.dumpState("rollback over the surge")
		t.Fatalf("the rollback must leave the abandoned replacement terminating; terminating pods of Instance 0: %d", len(got))
	}
	if row := h.instance(0); row == nil || row.Phase != workloadtypes.InstancePhaseUpdating || row.Operation == nil || row.Operation.TargetRevision != revNew.Name {
		t.Fatalf("the row must still carry the surge pinned to %s while its replacement terminates, got %+v", revNew.Name, row)
	}
	if got := h.componentCounters().UpdatedReplicas; got != replicas-1 {
		t.Fatalf("a row mid-surge toward a superseded revision is not updated: UpdatedReplicas = %d, want %d", got, replicas-1)
	}

	// The resumed manager closes the surge once the replacement is gone and
	// has nothing else to do.
	if !h.run(10, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("resumed manager")
		t.Fatalf("a row was left mid-operation after the rollback: %+v", h.irStatuses())
	}
	for _, row := range h.irStatuses() {
		if row.Operation != nil {
			t.Fatalf("Instance %d keeps an operation open after the rollback settled: %+v", row.Index, row.Operation)
		}
	}
	if got := h.componentCounters().UpdatedReplicas; got != replicas {
		t.Fatalf("UpdatedReplicas = %d after the rollback settled, want %d", got, replicas)
	}
	if h.livePodOnImage(stuckImage) {
		t.Fatalf("a pod of the superseded revision is still live after the rollback settled")
	}
}

// TestRollbackOverGangSurge_ResumedManagerKeepsTheRolloutOpenUntilTheReplacementIsGone
// pins a rollback landing over a gang surge: every row runs the starting
// revision, yet the revision rollup stays withdrawn and the pinned source
// is not counted updated until the replacement gang is gone and the source
// is reset. Each pass starts with a fresh expectations cache, as a
// restarted manager does, and recomputes the rollup from status alone.
func TestRollbackOverGangSurge_ResumedManagerKeepsTheRolloutOpenUntilTheReplacementIsGone(t *testing.T) {
	const replicas = int32(2)
	h := newRecoveryHarness(t, true)
	h.replicas = replicas
	// Deletes linger as Terminating until their grace elapses, as on a
	// cluster; the abandoned replacement goes on the bounded grace.
	h.podGrace = 10 * time.Minute
	h.abandonGrace = 30 * time.Second
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}
	if h.currentRevision != h.revV1.Name {
		t.Fatalf("the starting revision must be promoted before the push, got %q", h.currentRevision)
	}

	// A replacement gang that never passes readiness keeps the surge
	// uncommitted for as long as the story needs.
	revNew := h.ensureRevision(h.podSpec(stuckImage))
	h.setTarget(revNew, stuckImage)
	if !h.run(20, func() bool { return h.surgeOpen(0) && h.gangSurgeMarker() != nil && h.livePodOnImage(stuckImage) }) {
		h.dumpState("gang surge toward the new revision")
		t.Fatalf("the gang surge toward the new revision never opened")
	}

	// The starting revision is re-applied while the replacement gang
	// stands; rollup is the current revision the adapter would publish,
	// carried from pass to pass as the persisted value is.
	h.setTarget(h.revV1, goodImage)
	rollup, withdrawn := h.currentRevision, ""
	readRollup := func() string {
		next := status.CurrentRevisionFor(h.irStatuses(), rollup, h.target.Name, h.blocks, withdrawn)
		withdrawn = status.WithdrawnRevisionAfter(withdrawn, rollup, next, h.target.Name)
		rollup = next
		return rollup
	}
	openWhilePinned := func() {
		t.Helper()
		pinned := false
		for _, row := range h.irStatuses() {
			if op := row.Operation; op != nil && op.Type == workloadtypes.InstanceOperationUpdate && op.TargetRevision == revNew.Name {
				pinned = true
			}
		}
		got := readRollup()
		if pinned && got == h.revV1.Name {
			h.dumpState("rollup landed while pinned")
			t.Fatalf("the revision rollup names the starting revision while a row is still pinned to the withdrawn revision: %+v", h.irStatuses())
		}
		if h.livePodOnImage(stuckImage) && got != "" {
			t.Fatalf("the revision rollup must be withdrawn while a pod of the withdrawn revision is live, got %q", got)
		}
		if pinned && h.componentCounters().UpdatedReplicas >= replicas {
			t.Fatalf("a source pinned to a withdrawn revision counts as updated")
		}
	}
	if !h.runWithInvariant(40, func() bool { return h.settledOn(h.revV1, replicas) }, openWhilePinned) {
		h.dumpState("resumed manager")
		t.Fatalf("the rollback never settled on the starting revision: %+v", h.irStatuses())
	}
	if h.gangSurgeMarker() != nil {
		t.Fatalf("the replacement gang's marker row outlived the abandon: %+v", h.irStatuses())
	}
	if h.livePodOnImage(stuckImage) {
		t.Fatalf("a pod of the withdrawn revision is still live after the rollback settled")
	}
	if got := readRollup(); got != h.revV1.Name {
		t.Fatalf("the revision rollup must land on the starting revision once the rollback settled, got %q", got)
	}
	if got := h.componentCounters().UpdatedReplicas; got != replicas {
		t.Fatalf("UpdatedReplicas = %d after the rollback settled, want %d", got, replicas)
	}
}

// gangSurgeMarker is the row claiming a replacement gang's index, live or
// in cleanup, or nil when no gang surge is in flight.
func (h *recoveryHarness) gangSurgeMarker() *workloadtypes.InstanceStatus {
	sts := h.irStatuses()
	for i := range sts {
		if op := sts[i].Operation; op != nil && op.Type == workloadtypes.InstanceOperationUpdate &&
			(op.Step == workloadtypes.UpdateStepGangSurgeTarget || op.Step == workloadtypes.UpdateStepGangSurgeTargetCleanup) {
			return &sts[i]
		}
	}
	return nil
}

// TestPolicyFlipMidHold_FixLandsWithoutRebuildingTheBrokenRevision pins a
// roll held because the Instance already on the pushed revision crashed
// under load, with the restart policy edited from None to
// RecreateInstanceOnPodRestart while the roll is held: the newly enabled
// policy rebuilds the crashed set on the revision the Component still
// wants, and once the fix is pushed no pod of the broken revision is
// created again and no repair opens on a bumped Instance. A rebuild that
// has created no pod yet renders the fix; a rebuilt set that exists
// finishes on the revision it carries and the roll replaces it at the fix.
func TestPolicyFlipMidHold_FixLandsWithoutRebuildingTheBrokenRevision(t *testing.T) {
	cases := []struct {
		name string
		// rebuiltBeforeFix pushes the fix after the repair's rebuilt set
		// exists; otherwise the fix lands while the crashed pod is still
		// terminating and the rebuild has created nothing yet.
		rebuiltBeforeFix bool
	}{
		{name: "the fix is pushed after the rebuilt set exists", rebuiltBeforeFix: true},
		{name: "the fix is pushed while the crashed pod is still terminating"},
	}
	const replicas = int32(2)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
			h.recorder = record.NewFakeRecorder(256)
			h.replicas = replicas
			none := workloadtypes.RestartPolicyNone
			h.lifecycle.RestartPolicy = &none
			// Deletes linger as Terminating until their grace elapses; the
			// promoted pod serves long enough for the roll to reach the
			// next Instance before it dies.
			h.podGrace = time.Minute
			h.crashAfterServed = 4
			h.stuckGrace = 90 * time.Second
			h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
			if !h.run(40, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}

			revBroken := h.crashRevision(crashOnceAfterServingImage)
			if !h.run(60, func() bool { return h.notServingHolds() > 0 }) {
				h.dumpState("held")
				t.Fatalf("the roll was never held on the crashed Instance")
			}
			if hold := h.lastHold(); hold.Gate != workloadtypes.RolloutHoldGateBudget || !holdNamesInstance(hold, 0) {
				t.Fatalf("the hold must be the budget's and name Instance 0, got %+v", hold)
			}
			if h.repairInFlight() || h.eventCount(workloadtypes.EventReasonRestartTriggered) != 0 {
				t.Fatalf("restart policy None must open no repair for a runner the kubelet restarted")
			}

			// The policy flips while the roll is held: the restarted runner's
			// Instance is rebuilt on the revision the Component still wants.
			recreate := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &recreate
			h.setTarget(h.target, crashOnceAfterServingImage)
			if !h.run(10, func() bool { return h.repairInFlight() }) {
				h.dumpState("policy flip")
				t.Fatalf("the policy flip opened no repair on the crashed Instance")
			}
			if got := h.eventCount(workloadtypes.EventReasonRestartTriggered); got != 1 {
				t.Fatalf("RestartTriggered events after the flip = %d, want 1", got)
			}
			if tc.rebuiltBeforeFix {
				if !h.run(10, func() bool { return h.instance(0).Incarnation == 2 && len(h.podsOf(0)) == 1 }) {
					h.dumpState("rebuild")
					t.Fatalf("the repair never rebuilt the crashed set")
				}
				if pod := h.podsOf(0)[0]; pod.Spec.Containers[0].Image != crashOnceAfterServingImage {
					t.Fatalf("before the fix the rebuild renders the revision the Component wants, got %s", pod.Spec.Containers[0].Image)
				}
			} else if len(h.terminatingPodsOf(0)) != 1 || len(h.podsOf(0)) != 0 {
				h.dumpState("repair drain")
				t.Fatalf("the repair must have the crashed pod terminating and nothing rebuilt yet")
			}

			// The fix is pushed. From here no pod of the broken revision may
			// be created and no repair may open on a bumped Instance.
			before := map[types.UID]struct{}{}
			for _, pod := range h.livePods() {
				if pod.Spec.Containers[0].Image == crashOnceAfterServingImage {
					before[pod.UID] = struct{}{}
				}
			}
			restartsBefore := h.eventCount(workloadtypes.EventReasonRestartTriggered)
			h.setTarget(h.revFixed, fixedImage)
			late := map[types.UID]string{}
			brokenPromotedAfterFix := false
			landed := h.runWithInvariant(120, func() bool { return h.settledOn(h.revFixed, replicas) }, func() {
				for _, pod := range h.livePods() {
					if _, seen := before[pod.UID]; !seen && pod.Spec.Containers[0].Image == crashOnceAfterServingImage {
						late[pod.UID] = pod.Name
					}
				}
				if row := h.instance(0); !tc.rebuiltBeforeFix && row != nil && row.Phase == workloadtypes.InstancePhaseReady && row.RunningRevision == revBroken.Name {
					brokenPromotedAfterFix = true
				}
			})
			if !landed {
				h.dumpState("fix")
				t.Fatalf("the fix did not land on every Instance; last hold %+v", h.lastHold())
			}
			if len(late) > 0 {
				t.Fatalf("pod(s) of the broken revision created after the fix push: %v", late)
			}
			if got := h.eventCount(workloadtypes.EventReasonRestartTriggered); got != restartsBefore {
				t.Fatalf("a repair opened after the fix push: RestartTriggered events %d, want %d", got, restartsBefore)
			}
			if brokenPromotedAfterFix {
				t.Fatalf("a rebuild that had created no pod when the fix landed must come back on the fix, not on the broken revision")
			}
		})
	}
}

// Force-delete of a drained pod no operation waits on. A roll to a
// revision whose replacement never comes up is rolled back once the row
// has failed. The wreckage sweep drains and deletes the abandoned
// replacement, and the kubelet under it has stopped - before the delete
// or after it - so the pod stays Terminating. The Component converges on
// its running revision on the other nodes: for a single pod the drained
// replacement then belongs to no operation at all, and for a gang the
// abandon of the replacement gang waits on the member the dead node
// holds. Under the force-delete policy the Component pass force-deletes
// the pod, with the event and the audit row, on the first pass at or
// after the later of its two boundaries - the pod's own deletion deadline
// plus the slack, and the node unreachable for the threshold - and that
// pass is woken by the boundary itself. Before it nothing is deleted, and
// the pods on live nodes are never touched. Without the policy the pod
// stays until its kubelet returns.

const (
	abandonedPodThreshold = 90 * time.Second
	abandonedPodSlack     = 30 * time.Second
)

// abandonedReplacementStory is one shape of the story: the pod shape, and
// whether the node under the replacement died before the wreckage sweep
// deleted it, which puts the deletion deadline plus the slack last, or
// after, which puts the node threshold last.
type abandonedReplacementStory struct {
	name           string
	multiPod       bool
	nodeDiesBefore bool
}

func abandonedReplacementStories() []abandonedReplacementStory {
	return []abandonedReplacementStory{
		{"single pod, the node dies after the drain", false, false},
		{"single pod, the node died before the drain", false, true},
		{"gang, the node dies after the drain", true, false},
		{"gang, the node died before the drain", true, true},
	}
}

// newAbandonedReplacementHarness converges two Instances on v1 over six
// nodes under SurgeThenDrain, rolls them toward a revision whose image
// never pulls, and returns once the first replacement is bound to a node
// and wedging there. The retry ladder is off, so the park that follows
// stands until the rollback.
func newAbandonedReplacementHarness(t *testing.T, story abandonedReplacementStory, policy *workloadtypes.ForceDeletePolicy) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, story.multiPod)
	h.replicas = 2
	recreate := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &recreate
	h.podGrace = 30 * time.Second
	h.stuckGrace = 30 * time.Second
	h.retryPolicy = nil
	h.forceDelete = policy
	h.recorder = record.NewFakeRecorder(256)
	h.useNodes("node-a", "node-b", "node-c", "node-d", "node-e", "node-f")
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.settledOn(h.revV1, 2) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never converged on v1")
	}
	h.setTarget(h.revBad, badImage)
	if !h.run(20, h.replacementBound) {
		h.dumpState("roll to the bad image")
		t.Fatalf("no replacement was bound to a node under the image that never pulls")
	}
	return h
}

// replacementBound reports whether the replacement pod the story strands
// is bound to a node.
func (h *recoveryHarness) replacementBound() bool {
	for _, pod := range h.podsOnImage(badImage) {
		if (!h.multiPod || pod.Labels[query.LabelRunner] == "leader") && pod.Spec.NodeName != "" {
			return true
		}
	}
	return false
}

// anyRowFailed reports whether any row is parked at Failed.
func (h *recoveryHarness) anyRowFailed() bool {
	for _, s := range h.irStatuses() {
		if s.Phase == workloadtypes.InstancePhaseFailed {
			return true
		}
	}
	return false
}

// podsOnImage returns every pod object, Terminating ones included, whose
// container runs image.
func (h *recoveryHarness) podsOnImage(image string) []*corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	var out []*corev1.Pod
	for i := range pods.Items {
		if pods.Items[i].Spec.Containers[0].Image == image {
			out = append(out, &pods.Items[i])
		}
	}
	return out
}

// abandonedReplacementPod is the replacement pod the story strands: the
// one pod of a single-pod replacement, the leader of a replacement gang.
// Its node holds nothing of the serving revision.
func (h *recoveryHarness) abandonedReplacementPod() *corev1.Pod {
	h.t.Helper()
	var victim *corev1.Pod
	for _, pod := range h.podsOnImage(badImage) {
		if !h.multiPod || pod.Labels[query.LabelRunner] == "leader" {
			victim = pod
		}
	}
	if victim == nil || victim.Spec.NodeName == "" {
		h.dumpState("replacement")
		h.t.Fatalf("no bound replacement pod on %s", badImage)
	}
	for _, pod := range h.podsOnNode(victim.Spec.NodeName) {
		if pod.Spec.Containers[0].Image == goodImage {
			h.t.Fatalf("node model placed serving pod %s beside the replacement on %s", pod.Name, victim.Spec.NodeName)
		}
	}
	return victim
}

// terminatingPod reports whether the pod object is still in the API and
// Terminating.
func (h *recoveryHarness) terminatingPod(pod *corev1.Pod) bool {
	h.t.Helper()
	cur := &corev1.Pod{}
	if err := h.c.Get(h.ctx, client.ObjectKeyFromObject(pod), cur); err != nil {
		if apierrors.IsNotFound(err) {
			return false
		}
		h.t.Fatalf("get pod %s: %v", pod.Name, err)
	}
	return cur.UID == pod.UID && cur.DeletionTimestamp != nil
}

// nodeNotReadySince is the transition time the API holds for name's
// NodeReady condition - the instant the node-death clock runs from.
func (h *recoveryHarness) nodeNotReadySince(name string) time.Time {
	h.t.Helper()
	node := &corev1.Node{}
	if err := h.c.Get(h.ctx, types.NamespacedName{Name: name}, node); err != nil {
		h.t.Fatalf("get node %s: %v", name, err)
	}
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status != corev1.ConditionTrue {
			return cond.LastTransitionTime.Time
		}
	}
	h.t.Fatalf("node %s is not NotReady", name)
	return time.Time{}
}

// deletionDeadline is the deletion deadline the API presents for pod.
func (h *recoveryHarness) deletionDeadline(pod *corev1.Pod) time.Time {
	h.t.Helper()
	cur := &corev1.Pod{}
	if err := h.c.Get(h.ctx, client.ObjectKeyFromObject(pod), cur); err != nil {
		h.t.Fatalf("get pod %s: %v", pod.Name, err)
	}
	if cur.DeletionTimestamp == nil {
		h.t.Fatalf("pod %s is not Terminating", pod.Name)
	}
	return cur.DeletionTimestamp.Time
}

// requireServingPodsKept fails when the serving revision's pods are not
// exactly the ones in kept, each live.
func (h *recoveryHarness) requireServingPodsKept(kept map[string]bool) {
	h.t.Helper()
	pods := h.podsOnImage(goodImage)
	if len(pods) != len(kept) {
		h.dumpState("serving pods")
		h.t.Fatalf("serving revision has %d pod(s), want its original %d", len(pods), len(kept))
	}
	for _, pod := range pods {
		if !kept[wedgeKey(pod)] || pod.DeletionTimestamp != nil {
			h.t.Fatalf("serving pod %s was replaced or deleted; pods on live nodes are never touched", pod.Name)
		}
	}
}

// forceDeleteRecord returns the force-delete audit row for pod, or nil.
func (h *recoveryHarness) forceDeleteRecord(pod *corev1.Pod) *audit.Entry {
	h.t.Helper()
	ledger, err := audit.LoadLedgerForOwner(h.ctx, h.c, h.isvc)
	if err != nil {
		h.t.Fatalf("load audit ledger: %v", err)
	}
	for i := range ledger.Entries {
		if ledger.Entries[i].Reason == audit.ReasonForceDelete && ledger.Entries[i].RequestUUID == string(pod.UID) {
			return &ledger.Entries[i]
		}
	}
	return nil
}

// strandAbandonedReplacement lets the wedged replacement park its row at
// Failed, rolls the Component back to v1 and leaves the replacement pod
// Terminating on a dead node: the kubelet under it stops while the pod is
// still wedging, or once the pod is Terminating, as the story says. A
// gang's failed replacement is abandoned by the update pass as soon as
// its row fails, with or without the rollback, so the first timing is
// what puts the deletion deadline plus the slack past the node threshold.
// Returns the pod and the instant it turns actionable under policy: the
// later of its deletion deadline plus the slack, strictly past, and the
// node's recorded transition plus the threshold, both read back from the
// API as the pass reads them.
func (h *recoveryHarness) strandAbandonedReplacement(story abandonedReplacementStory, policy *workloadtypes.ForceDeletePolicy) (*corev1.Pod, time.Time) {
	h.t.Helper()
	victim := h.abandonedReplacementPod()
	deadNode := victim.Spec.NodeName
	if story.nodeDiesBefore {
		h.failNode(deadNode)
	}
	if !h.run(40, h.anyRowFailed) {
		h.dumpState("roll to the bad image")
		h.t.Fatalf("no row parked at Failed under the image that never pulls")
	}
	h.setTarget(h.revV1, goodImage)
	if !h.run(10, func() bool { return h.terminatingPod(victim) }) {
		h.dumpState("rollback")
		h.t.Fatalf("the rollback never drained and deleted the replacement %s", victim.Name)
	}
	if !story.nodeDiesBefore {
		h.failNode(deadNode)
	}
	slackAt := h.deletionDeadline(victim).Add(policy.OverdueSlack + time.Nanosecond)
	nodeAt := h.nodeNotReadySince(deadNode).Add(policy.NodeUnreachableThreshold)
	if story.nodeDiesBefore == nodeAt.After(slackAt) {
		h.t.Fatalf("story shape: node threshold at %v, slack boundary at %v; the timing must make the other clock last",
			nodeAt.Format(time.TimeOnly), slackAt.Format(time.TimeOnly))
	}
	return victim, laterOf(slackAt, nodeAt)
}

func laterOf(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func TestForceDelete_DrainedPodNoOperationWaitsOnIsSweptAtThePolicyBoundary(t *testing.T) {
	for _, story := range abandonedReplacementStories() {
		t.Run(story.name, func(t *testing.T) {
			policy := &workloadtypes.ForceDeletePolicy{
				NodeUnreachableThreshold: abandonedPodThreshold,
				OverdueSlack:             abandonedPodSlack,
			}
			h := newAbandonedReplacementHarness(t, story, policy)
			serving := podIdentities(h.podsOnImage(goodImage))
			victim, boundary := h.strandAbandonedReplacement(story, policy)
			deadNode := victim.Spec.NodeName

			untouched := func() {
				t.Helper()
				if h.sawEvent(workloadtypes.EventReasonPodForceDeleted) {
					t.Fatalf("a pod was force-deleted at %v, before the policy boundary %v", h.clk.Now().Format(time.TimeOnly), boundary.Format(time.TimeOnly))
				}
				if !h.terminatingPod(victim) {
					t.Fatalf("the drained pod %s left the API before the boundary without a force-delete", victim.Name)
				}
				h.requireServingPodsKept(serving)
			}
			// A controller with nothing else to do is woken only by the
			// wait its last pass asked for; an idle hour stands in for
			// the resync a pass that asked for nothing would wait on. A
			// pass that would oversleep the boundary is noted, and the
			// clock is taken to the boundary anyway so the sweep's own
			// claim is judged first.
			lateWake := ""
			for h.clk.Now().Before(boundary) {
				untouched()
				res, err := h.pass()
				if err != nil {
					t.Fatalf("Reconcile: %v", err)
				}
				wait := h.sleepAfter(res, time.Hour)
				if wake := h.clk.Now().Add(wait); wake.After(boundary) {
					if lateWake == "" {
						lateWake = fmt.Sprintf("the pass at %v asked to wake at %v, past the policy boundary %v",
							h.clk.Now().Format(time.TimeOnly), wake.Format(time.TimeOnly), boundary.Format(time.TimeOnly))
					}
					wait = boundary.Sub(h.clk.Now())
				}
				h.clk.Step(wait)
			}
			untouched()

			if _, err := h.pass(); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if !h.sawEvent(workloadtypes.EventReasonPodForceDeleted) {
				h.dumpState("at the boundary")
				t.Fatalf("the drained pod %s on %s was not force-deleted at the policy boundary %v, with no operation waiting on it",
					victim.Name, deadNode, boundary.Format(time.TimeOnly))
			}
			if lateWake != "" {
				t.Fatalf("%s: a Terminating pod on a dying node must wake the pass at its boundary", lateWake)
			}
			named := false
			for _, e := range h.events {
				if strings.Contains(e, string(workloadtypes.EventReasonPodForceDeleted)) && strings.Contains(e, victim.Name) && strings.Contains(e, deadNode) {
					named = true
				}
			}
			if !named {
				t.Fatalf("the force-delete event must name pod %s and node %s; events: %v", victim.Name, deadNode, h.events)
			}
			idx, _ := query.InstanceIdxFromLabels(victim)
			if e := h.forceDeleteRecord(victim); e == nil || e.Outcome != audit.OutcomeForceDeleteUnreachable || e.FromNode != deadNode || e.SourceInstance != idx {
				t.Fatalf("audit row for %s = %+v, want a force-delete-unreachable row from %s on instance %d", victim.Name, e, deadNode, idx)
			}
			if on := h.podsOnNode(deadNode); len(on) != 0 {
				t.Fatalf("%d pod object(s) still bound to %s after the force delete", len(on), deadNode)
			}
			h.requireServingPodsKept(serving)

			// The Component is at rest on v1, and stays so when the kubelet
			// returns to a node with nothing of it left.
			if !h.run(10, func() bool { return h.settledOn(h.revV1, 2) }) {
				h.dumpState("after the force delete")
				t.Fatalf("the Component did not come to rest on v1 once the dead node's pod was freed")
			}
			h.recoverNode(deadNode)
			for i := 0; i < 3; i++ {
				h.step()
				h.requireServingPodsKept(serving)
			}
			if !h.settledOn(h.revV1, 2) || len(h.podsOnNode(deadNode)) != 0 {
				h.dumpState("after the node returned")
				t.Fatalf("the Component moved after the node returned")
			}
		})
	}
}

func TestForceDelete_WithoutThePolicyADrainedPodOnADeadNodeWaitsForItsKubelet(t *testing.T) {
	for _, story := range abandonedReplacementStories() {
		if story.nodeDiesBefore {
			continue
		}
		t.Run(story.name, func(t *testing.T) {
			h := newAbandonedReplacementHarness(t, story, nil)
			serving := podIdentities(h.podsOnImage(goodImage))
			victim, boundary := h.strandAbandonedReplacement(story, &workloadtypes.ForceDeletePolicy{
				NodeUnreachableThreshold: abandonedPodThreshold,
				OverdueSlack:             abandonedPodSlack,
			})
			deadNode := victim.Spec.NodeName

			// Long past the boundary such a policy would carry.
			for h.clk.Now().Before(boundary.Add(10 * abandonedPodThreshold)) {
				h.step()
				if h.sawEvent(workloadtypes.EventReasonPodForceDeleted) {
					t.Fatalf("without a force-delete policy nothing may force-delete; events: %v", h.events)
				}
				if !h.terminatingPod(victim) {
					t.Fatalf("the drained pod %s left the API while its kubelet was down and no policy was configured", victim.Name)
				}
				h.requireServingPodsKept(serving)
			}

			// The kubelet returns and finishes the termination it owed.
			h.recoverNode(deadNode)
			if !h.run(10, func() bool { return h.settledOn(h.revV1, 2) && len(h.podsOnNode(deadNode)) == 0 }) {
				h.dumpState("after the node returned")
				t.Fatalf("the returning kubelet did not finish the termination, or the Component did not come to rest on v1")
			}
			h.requireServingPodsKept(serving)
		})
	}
}

// Teardown with a Terminating pod whose index no row covers: the wave has
// no candidate to escalate it through, so the sweep force-deletes it on
// actionable node-death evidence with the event and the audit row, and
// inside the threshold the pass wakes at the boundary and touches nothing.
func TestReconcile_Teardown_SweepsATerminatingPodOfARowlessIndex(t *testing.T) {
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	const deadNode = "dead-node"
	policy := &workloadtypes.ForceDeletePolicy{OverdueSlack: 2 * time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	cases := []struct {
		name        string
		notReadyFor time.Duration
		deleted     bool
		wake        time.Duration
	}{
		{"node unreachable past the threshold", 10 * time.Minute, true, 0},
		{"node unreachable inside the threshold", 2 * time.Minute, false, 3 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := minimalInput(t)
			in.Teardown = true
			in.Clock = clocktesting.NewFakeClock(now)
			in.ForceDelete = policy
			in.ObservedState.InstanceStatuses = nil
			installTestAtomicMutationStore(&in)

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: deadNode}}
			setNodeReady(node, corev1.ConditionUnknown, now.Add(-tc.notReadyFor))
			// The apiserver holds the pod until a kubelet acknowledges the
			// termination; a stand-in finalizer keeps the fake object the
			// same way, and the live read presents the pod as the
			// apiserver would, Terminating with no finalizer of its own.
			pod := enginePod(in.Key.OwnerName, in.Key.Namespace, 3)
			pod.Spec.NodeName = deadNode
			stored := pod.DeepCopy()
			stored.Finalizers = []string{"test.example.com/kubelet"}
			c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(in.OwnerObject, node, stored).Build()
			deletedAt := metav1.NewTime(now.Add(-10 * time.Minute))
			observed := pod.DeepCopy()
			observed.DeletionTimestamp = &deletedAt
			in.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{
				Pods:       []*corev1.Pod{observed},
				ByInstance: map[int32][]*corev1.Pod{3: {observed}},
			}
			rec := record.NewFakeRecorder(16)

			result, err := workload.Reconcile(context.Background(), workloadtypes.Deps{
				Client: c, APIReader: c, Expectations: workloadtypes.NewExpectations(), Recorder: rec,
			}, in, minimalPlan(), nil)
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			events := drainFakeEvents(rec)
			cur := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), cur); err != nil {
				t.Fatalf("get pod: %v", err)
			}
			ledger, lerr := audit.LoadLedgerForOwner(context.Background(), c, in.OwnerObject)
			if lerr != nil {
				t.Fatalf("load ledger: %v", lerr)
			}
			if !tc.deleted {
				if len(events) != 0 || cur.DeletionTimestamp != nil || len(ledger.Entries) != 0 {
					t.Fatalf("inside the threshold nothing may be deleted, evented or ledgered: events=%v deleted=%v entries=%d", events, cur.DeletionTimestamp != nil, len(ledger.Entries))
				}
				if result.Requeue || result.RequeueAfter != tc.wake { //nolint:staticcheck // the bare backoff has no non-deprecated spelling
					t.Fatalf("result = %+v, want a wake-up at the node threshold in %v", result, tc.wake)
				}
				return
			}
			if cur.DeletionTimestamp == nil {
				t.Fatalf("the rowless pod was not force-deleted on actionable node-death evidence")
			}
			if len(events) != 1 || !strings.Contains(events[0], string(workloadtypes.EventReasonPodForceDeleted)) ||
				!strings.Contains(events[0], pod.Name) || !strings.Contains(events[0], deadNode) {
				t.Fatalf("events = %v, want one PodForceDeleted naming %s and %s", events, pod.Name, deadNode)
			}
			if len(ledger.Entries) != 1 || ledger.Entries[0].Reason != audit.ReasonForceDelete ||
				ledger.Entries[0].RequestUUID != string(pod.UID) || ledger.Entries[0].FromNode != deadNode || ledger.Entries[0].SourceInstance != 3 {
				t.Fatalf("ledger = %+v, want one force-delete row for %s from %s on instance 3", ledger.Entries, pod.UID, deadNode)
			}
		})
	}
}

// drainFakeEvents moves every event the recorder holds into a slice.
func drainFakeEvents(rec *record.FakeRecorder) []string {
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

// A crash-loop park follows its pod set through a corrected roll. While
// the roll waits to take a parked gang, the gang serving again between
// crashes returns its row to Ready in the pass that observes every member
// serving, whichever pass each member came back in; the roll then takes
// the gang as the serving Instance it is.
func TestCrashLoopAfterPromotion_ParkedGangFollowsItsSetThroughTheCorrectedRoll(t *testing.T) {
	const replicas = 2
	h := newRecoveryHarnessFor(t, true, constants.MainContainerName)
	h.replicas = replicas
	repair := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &repair
	// Only the leader dies, a few passes after it serves and inside a
	// grace that outlives every pod; the worker stays up, so the gang is
	// whole again in the pass its leader comes back.
	h.crashAfterServed = 4
	h.crashLeaderOnly = true
	h.stuckGrace = 10 * time.Minute
	h.stepCap = recoveryStepFloor
	h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
	if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d gangs", replicas)
	}

	revCrash := h.crashRevision(flapAfterServingImage)
	// gangServes reports whether every member of the Instance is
	// ContainersReady and in rotation.
	gangServes := func(idx int32) bool {
		pods := h.podsOf(idx)
		if len(pods) < len(h.desired.Runners) {
			return false
		}
		for _, pod := range pods {
			if !podreadiness.IsContainersReady(pod) || !podreadiness.IsServing(pod) {
				return false
			}
		}
		return true
	}
	// The ladder holds and the second gang is parked with its leader down
	// while the first is settled on the crashing revision: the push lands
	// on that shape, so the roll takes the first gang and the parked one
	// waits behind the unavailability budget.
	parkedBehind := func() bool {
		first, second := h.instance(0), h.instance(1)
		return h.heldOn(revCrash.Name) && first != nil && second != nil &&
			first.RunningRevision == revCrash.Name && first.Operation == nil &&
			second.RunningRevision == revCrash.Name && second.Operation == nil &&
			second.Phase == workloadtypes.InstancePhaseFailed && !gangServes(1)
	}
	if !h.run(160, parkedBehind) {
		h.dumpState("parked gang")
		t.Fatalf("the story never reached a held ladder with the second gang parked on %s (held=%v, rows=%+v)", revCrash.Name, h.heldOn(revCrash.Name), h.irStatuses())
	}

	h.setTarget(h.revFixed, fixedImage)
	servedWhileParkedBehind := false
	observe := func() {
		second := h.instance(1)
		if second == nil || second.Operation != nil || second.RunningRevision != revCrash.Name || !gangServes(1) {
			return
		}
		if second.Phase == workloadtypes.InstancePhaseFailed {
			h.dumpState("failed while serving")
			t.Fatalf("Instance 1 reads Failed while its gang serves again (leader and worker ContainersReady and in rotation) and the corrected roll is on another Instance; the park must return to Ready in the pass that observes the set serving (rows=%+v)", h.irStatuses())
		}
		if first := h.instance(0); second.Phase == workloadtypes.InstancePhaseReady && first != nil && first.RunningRevision != h.revFixed.Name {
			servedWhileParkedBehind = true
		}
	}
	if !h.runWithInvariant(120, func() bool { return h.settledOn(h.revFixed, replicas) }, observe) {
		h.dumpState("corrected revision")
		t.Fatalf("the corrected revision did not land on every gang")
	}
	if !servedWhileParkedBehind {
		h.dumpState("shape")
		t.Fatalf("the parked gang never served again while the roll was on the other Instance; the story did not exercise the unpark")
	}
}

// A crash loop over several Instances of a pushed revision is one ladder:
// its attempt rebuilds every Instance that crashed on the revision, one
// rebuild at a time, and the hold that announces the attempt names each
// of them, so every rebuild is one the status announced before it opened.
func TestCrashLoopAfterPromotion_TheLadderNamesEveryInstanceItsAttemptRebuilds(t *testing.T) {
	const replicas = 4
	h := newRecoveryHarnessFor(t, false, constants.MainContainerName)
	h.replicas = replicas
	repair := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &repair
	h.crashAfterServed = 4
	h.stuckGrace = 10 * time.Minute
	h.stepCap = recoveryStepFloor
	// A backoff long enough for the next promoted Instance to crash inside
	// it, so one attempt has two Instances to rebuild.
	h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: 2 * time.Minute, MaxDelay: 10 * time.Minute, Multiplier: 2}
	h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
	if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}

	revCrash := h.crashRevision(flapAfterServingImage)
	// announced is, per attempt time the ladder announced, the Instances
	// the hold named as the attempt's rebuilds. A hold reported while the
	// attempt runs names the Instances still to rebuild under it, and adds
	// to the latest time announced.
	announced := map[time.Time]map[int32]bool{}
	var times []time.Time
	var latest time.Time
	verdicts := h.holdVerdicts
	readAnnouncement := func() {
		if block := h.findBlock(revCrash.Name); block != nil && block.State == workloadtypes.RetryBlockBackoff && block.NextRetryAt != nil {
			at := block.NextRetryAt.Time
			if _, ok := announced[at]; !ok {
				announced[at] = map[int32]bool{}
				times = append(times, at)
			}
			latest = at
		}
		if h.holdVerdicts == verdicts {
			return
		}
		verdicts = h.holdVerdicts
		hold := h.verdict
		if hold == nil || hold.Gate != workloadtypes.RolloutHoldGateRetryBlock || hold.Target != revCrash.Name || latest.IsZero() {
			return
		}
		for _, idx := range namedRebuilds(hold.Reason) {
			announced[latest][idx] = true
		}
	}
	// The first crash-revision pod of an Instance is the roll's; every
	// later one is a rebuild, owed to an announced attempt that named its
	// Instance.
	seen := map[types.UID]struct{}{}
	rolled := map[int32]bool{}
	rebuilt := map[int32]int{}
	checkRebuilds := func() {
		for _, pod := range h.livePods() {
			if pod.Spec.Containers[0].Image != flapAfterServingImage {
				continue
			}
			if _, ok := seen[pod.UID]; ok {
				continue
			}
			seen[pod.UID] = struct{}{}
			idx := instanceIndexOf(pod)
			if !rolled[idx] {
				rolled[idx] = true
				continue
			}
			rebuilt[idx]++
			var window time.Time
			for _, at := range times {
				if !at.After(h.clk.Now()) && at.After(window) {
					window = at
				}
			}
			if window.IsZero() {
				h.dumpState("rebuild before any announcement")
				t.Fatalf("crash-revision pod %s of Instance %d appeared at %s before the ladder announced any attempt", pod.Name, idx, h.clk.Now().Format(time.RFC3339))
			}
			if !announced[window][idx] {
				var named []int32
				for i := range announced[window] {
					named = append(named, i)
				}
				h.dumpState("unannounced rebuild")
				t.Fatalf("crash-revision pod %s of Instance %d appeared at %s under the attempt announced for %s, which named Instances %v: the rebuild was not announced for its Instance (last hold %+v)",
					pod.Name, idx, h.clk.Now().Format(time.RFC3339), window.Format(time.RFC3339), named, h.lastHold())
			}
		}
	}
	h.runWithInvariant(200, func() bool { return h.heldOn(revCrash.Name) }, func() {
		readAnnouncement()
		checkRebuilds()
	})
	if !h.heldOn(revCrash.Name) {
		h.dumpState("ladder never held")
		t.Fatalf("the retry ladder never held %s (held warnings=%v, rebuilt=%v)", revCrash.Name, h.heldWarnings, rebuilt)
	}
	if len(rebuilt) < 2 {
		h.dumpState("one Instance")
		t.Fatalf("the story rebuilt %v; two Instances must crash and be rebuilt under one announced attempt", rebuilt)
	}

	h.setTarget(h.revFixed, fixedImage)
	if !h.run(120, func() bool { return h.settledOn(h.revFixed, replicas) }) {
		h.dumpState("corrected revision")
		t.Fatalf("the corrected revision did not land on every Instance")
	}
}

// namedRebuilds is the Instances a ladder hold names as the rebuilds of
// its attempt, none when it names none.
func namedRebuilds(reason string) []int32 {
	const clause = "the attempt rebuilds Instance"
	i := strings.Index(reason, clause)
	if i < 0 {
		return nil
	}
	rest := strings.TrimPrefix(strings.TrimPrefix(reason[i+len(clause):], "s"), " ")
	if j := strings.Index(rest, ";"); j >= 0 {
		rest = rest[:j]
	}
	var out []int32
	for _, field := range strings.Split(rest, ",") {
		var idx int32
		if _, err := fmt.Sscan(strings.TrimSpace(field), &idx); err == nil {
			out = append(out, idx)
		}
	}
	return out
}

// A push of two leader+worker gangs under RecreatePod maxUnavailable 1
// moves both gangs onto a revision whose leader dies after promotion and
// comes back between crashes, under RecreateInstanceOnPodRestart. With no
// Instance left to start the budget is never asked for a start, and the
// policy's repair owns the passes it runs in; the hold the roll stands
// behind is still reported within one cadence of the first crash after
// Ready, on the crashing revision and naming the gang whose pushed pod
// restarted: the Budget hold, or the revision's ladder once it has
// counted the crash, so status says why the roll parked at once.
func TestCrashAfterPromotion_FullyRolledGangSaysWhyWithinACadence(t *testing.T) {
	const replicas = 2
	h := newRecoveryHarnessFor(t, true, constants.MainContainerName)
	h.replicas = replicas
	repair := workloadtypes.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &repair
	// The leader serves long enough for the roll to move the second gang
	// before it dies, only the leader dies, and it dies well inside the
	// stuck-pod grace, so the loop is the ladder's story alone.
	h.crashAfterServed = 4
	h.crashLeaderOnly = true
	h.stuckGrace = 10 * time.Minute
	h.stepCap = recoveryStepFloor
	h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
	if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d instances", replicas)
	}

	revCrash := h.crashRevision(flapAfterServingImage)
	crashedIndex := int32(-1)
	// crashedGang is the index of the first gang promoted onto the
	// crashing revision whose pod has crashed, -1 before any has.
	crashedGang := func() int32 {
		for _, s := range h.irStatuses() {
			if s.RunningRevision != revCrash.Name {
				continue
			}
			for _, pod := range h.podsOf(s.Index) {
				if h.crashes[wedgeKey(pod)] > 0 {
					return s.Index
				}
			}
		}
		return -1
	}
	namesTheCrashedGang := func(hold *workloadtypes.RolloutHold) bool {
		if hold == nil || hold.Target != revCrash.Name {
			return false
		}
		saysWhy := hold.Gate == workloadtypes.RolloutHoldGateBudget || hold.Gate == workloadtypes.RolloutHoldGateRetryBlock || hold.Gate == workloadtypes.RolloutHoldGateHeld
		return saysWhy && strings.Contains(hold.Reason, "not serving") && strings.Contains(hold.Reason, fmt.Sprintf("Instance %d", crashedIndex))
	}
	// holdsBeforeCrash counts the holds reported before the pass that
	// first reads the crash, so that pass's own report counts.
	holdsBeforeCrash, stepsSinceCrash, namedAfter := 0, -1, -1
	h.runWithInvariant(120, func() bool { return namedAfter >= 0 }, func() {
		if crashedIndex < 0 {
			if idx := crashedGang(); idx >= 0 {
				crashedIndex, stepsSinceCrash = idx, 0
			} else {
				holdsBeforeCrash = len(h.holds)
			}
		} else {
			stepsSinceCrash++
		}
		if crashedIndex >= 0 && namedAfter < 0 && len(h.holds) > holdsBeforeCrash && namesTheCrashedGang(h.lastHold()) {
			namedAfter = stepsSinceCrash
		}
	})
	if crashedIndex < 0 {
		h.dumpState("no crash")
		t.Fatalf("no promoted gang crashed; the story did not run")
	}
	if namedAfter < 0 {
		h.dumpState("silent")
		t.Fatalf("status never said why the roll parked: no Budget or ladder hold on %s named gang %d, whose pushed leader restarted after Ready (holds=%v, failed warnings=%v)",
			revCrash.Name, crashedIndex, h.holds, h.failedWarnings)
	}
	if namedAfter > 1 {
		t.Fatalf("the hold named gang %d only %d cadences after its leader crashed; the roll's standing is owed within one", crashedIndex, namedAfter)
	}
}

// Two leader+worker gangs are pushed under a rolling budget of one onto a
// revision whose leader dies once after every promotion, under the gang's
// default restart policy. The roll completes before the first death, so
// no start is left for the budget to deny; the policy rebuilds the first
// dead set at once, and the death of the rebuilt set counts on the
// revision's ladder. From the pass after that count until the ladder
// holds, every pass reports the ladder as the hold the roll stands
// behind: the RetryBlock gate on the crashing revision naming the failed
// attempts, through the backoff, through the rebuild the attempt opens,
// which it names as the attempt's, and while the rebuilt set proves
// itself. No row parks Failed and no RetryHeld warning is raised before
// the ladder has spent its attempts; once it has, the row the hold parks
// reads Failed with an InstanceFailed warning.
func TestCrashAfterPromotion_FullyRolledGangLadderSaysWhyFromItsFirstRebuild(t *testing.T) {
	for _, tc := range []struct {
		strategy workloadtypes.UpdateStrategyType
		// served is how many passes a promoted leader serves before it
		// dies: long enough for the roll to land the second gang first,
		// which a gang surge takes more passes to do.
		served int
	}{
		{strategy: workloadtypes.UpdateStrategyRecreatePod, served: 6},
		{strategy: workloadtypes.UpdateStrategySurgeThenDrain, served: 12},
	} {
		t.Run(string(tc.strategy), func(t *testing.T) {
			const replicas = int32(2)
			const attempts = int32(3)
			h := newRecoveryHarnessFor(t, true, constants.MainContainerName)
			h.recorder = record.NewFakeRecorder(512)
			h.replicas = replicas
			// Only the leader dies, well inside the crash window, and every
			// set built on the revision dies once.
			h.crashLeaderOnly = true
			h.crashEverySet = true
			h.crashAfterServed = tc.served
			h.stuckGrace = 10 * time.Minute
			h.stepFloor = 5 * time.Second
			h.stepCap = h.stepFloor
			h.kubeletLag = time.Second
			h.retryPolicy = &workloadtypes.RetryPolicy{MaxAttempts: attempts, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
			h.useRollingBudget(tc.strategy)
			if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d gangs", replicas)
			}

			revCrash := h.crashRevision(crashOnceAfterServingImage)
			if !h.run(80, func() bool { return h.settledOn(revCrash, replicas) }) {
				h.dumpState("push")
				t.Fatalf("the push never landed on both gangs")
			}
			if len(h.crashedInstances) > 0 {
				h.dumpState("early crash")
				t.Fatalf("a promoted leader died before the roll completed; the story needs a completed roll")
			}

			namesTheLadder := func(hold *workloadtypes.RolloutHold) bool {
				return hold != nil && hold.Target == revCrash.Name &&
					(hold.Gate == workloadtypes.RolloutHoldGateRetryBlock || hold.Gate == workloadtypes.RolloutHoldGateHeld) &&
					strings.Contains(hold.Reason, "failed attempt")
			}
			failedRow := func() bool {
				for _, s := range h.irStatuses() {
					if s.Phase == workloadtypes.InstancePhaseFailed && s.RunningRevision == revCrash.Name {
						return true
					}
				}
				return false
			}
			// rebuilding lists the Instances whose set the ladder's attempt
			// is rebuilding this pass: a Restart in flight at the revision.
			rebuilding := func() []int32 {
				var out []int32
				for _, s := range h.irStatuses() {
					if s.Phase == workloadtypes.InstancePhaseRestarting && s.RunningRevision == revCrash.Name {
						out = append(out, s.Index)
					}
				}
				return out
			}
			seen := map[types.UID]struct{}{}
			pass, countedAt, setsAtCount := 0, -1, 0
			rebuiltUnderTheLadder := false
			var silent []string
			done := func() bool { return h.heldOn(revCrash.Name) && failedRow() && len(h.failedWarnings) > 0 }
			h.runWithInvariant(400, done, func() {
				pass++
				h.trackPodsOnImage(crashOnceAfterServingImage, seen)()
				b := h.findBlock(revCrash.Name)
				if countedAt < 0 {
					if b != nil {
						countedAt, setsAtCount = pass, len(seen)
					}
					return
				}
				// The pass that counted the crash reported the roll's standing
				// as it was before the count; every pass after it reads the
				// ladder.
				if pass == countedAt {
					return
				}
				if b.AttemptsStarted > attempts {
					t.Fatalf("the ladder counted %d attempts, bound %d", b.AttemptsStarted, attempts)
				}
				if b.State != workloadtypes.RetryBlockHeld && (failedRow() || len(h.heldWarnings) > 0) {
					h.dumpState("parked early")
					t.Fatalf("a row parked Failed or the ladder was announced held after %d of %d attempts (failed warnings=%v, held warnings=%v)", b.AttemptsStarted, attempts, h.failedWarnings, h.heldWarnings)
				}
				if len(seen) > setsAtCount {
					rebuiltUnderTheLadder = true
				}
				if !namesTheLadder(h.verdict) {
					silent = append(silent, fmt.Sprintf("pass %d (block %s after %d attempt(s), rebuilding %v): %+v", pass, b.State, b.AttemptsStarted, rebuilding(), h.verdict))
					return
				}
				if b.State == workloadtypes.RetryBlockRetryInProgress {
					named := map[int32]bool{}
					for _, idx := range namedRebuilds(h.verdict.Reason) {
						named[idx] = true
					}
					for _, idx := range rebuilding() {
						if !named[idx] {
							h.dumpState("unnamed rebuild")
							t.Fatalf("the ladder's attempt is rebuilding Instance %d and the hold does not name it: %+v", idx, h.verdict)
						}
					}
				}
			})
			if countedAt < 0 {
				h.dumpState("no count")
				t.Fatalf("the ladder never counted a crash of a promoted set on %s; the story did not run (crashed=%v)", revCrash.Name, h.crashedInstances)
			}
			if !rebuiltUnderTheLadder {
				h.dumpState("no rebuild")
				t.Fatalf("no set was rebuilt under the ladder; the story did not run")
			}
			if len(silent) > 0 {
				h.dumpState("silent")
				t.Fatalf("%d pass(es) after the ladder counted the crash did not report the ladder as the hold the roll stands behind:\n%s", len(silent), strings.Join(silent, "\n"))
			}
			if !done() {
				h.dumpState("ladder end")
				t.Fatalf("the ladder never held %s with a Failed row and an InstanceFailed warning (blocks=%+v, failed warnings=%v)", revCrash.Name, h.blocks, h.failedWarnings)
			}
			if b := h.findBlock(revCrash.Name); b.AttemptsStarted != attempts {
				t.Fatalf("the held ladder must record %d attempts, got %+v", attempts, b)
			}
		})
	}
}

// unreadyRollPolicies are the restart policies a roll over an unready
// Instance is driven under.
var unreadyRollPolicies = []workloadtypes.RestartPolicy{workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance}

// unreadyVictimHarness converges replicas single-pod Instances on v1 under
// the named strategy, budget and restart policy, fails the readiness probe
// of the highest Instance's pod, and lets the kubelet report it: the row
// stays Ready with its pod out of rotation. The stuck-pod grace outlasts
// a step, so the fault is read inside its window first.
func unreadyVictimHarness(t *testing.T, policy workloadtypes.RestartPolicy, strategy workloadtypes.UpdateStrategyType, surge, unavail, replicas int32) (*recoveryHarness, int32) {
	t.Helper()
	return unreadyVictimHarnessUnder(t, false, &policy, "", strategy, surge, unavail, replicas)
}

// unreadyVictimHarnessUnder is unreadyVictimHarness over the Instance's
// shape, with a nil policy left to the plan to resolve to the shape's
// default and the fault confined to the named runner (every pod when "").
func unreadyVictimHarnessUnder(t *testing.T, gang bool, policy *workloadtypes.RestartPolicy, runner string, strategy workloadtypes.UpdateStrategyType, surge, unavail, replicas int32) (*recoveryHarness, int32) {
	t.Helper()
	converge := workloadtypes.RestartPolicyNone
	if policy != nil {
		converge = *policy
	}
	h := offTargetCrashHarness(t, gang, strategy, surge, unavail, converge, replicas)
	if policy == nil {
		h.lifecycle.RestartPolicy = nil
		h.setTarget(h.revV1, goodImage)
	}
	h.stuckGrace = 5 * time.Minute
	victim := replicas - 1
	h.failReadiness(victim, runner)
	h.step()
	row := h.instance(victim)
	if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil || row.ServingPodCount >= row.PodCount {
		h.dumpState("after the fault")
		t.Fatalf("instance %d after the fault = %+v, want a Ready row with its pod out of rotation", victim, row)
	}
	return h, victim
}

// TestReconcile_UnreadyVictimPastTheWindow_DrainFirstRollTakesItFirst:
// four Instances serve v1 under a drain-first strategy with
// maxUnavailable 1, listed alone in a rollout group, and the highest
// Instance's pod stops passing readiness while its row stays Ready. A
// push inside the stuck-pod grace starts nothing: the unready pod spends
// the group's one unavailable slot and the gate denies every start. Once
// the pod has been unready for the grace it is dark: its start skips the
// gate consult, lands the fix first at no cost in capacity, and the peers
// roll after it within the budget; no repair opens under either restart
// policy. RecreatePod and InPlaceIfPossible, under None and
// RecreateInstanceOnPodRestart.
func TestReconcile_UnreadyVictimPastTheWindow_DrainFirstRollTakesItFirst(t *testing.T) {
	const replicas = 4
	for _, policy := range unreadyRollPolicies {
		for _, sc := range offTargetStrategies[1:3] {
			t.Run(string(policy)+"/"+string(sc.strategy), func(t *testing.T) {
				h, victim := unreadyVictimHarness(t, policy, sc.strategy, sc.surge, sc.unavail, replicas)
				consults := 0
				h.gate = coordinationUnavailabilityGate(h, replicas, sc.unavail, &consults)
				h.events = nil
				h.setTarget(h.revFixed, fixedImage)

				// Inside the window nothing changes: the push is held at the gate.
				for i := 0; i < 3; i++ {
					h.step()
					for idx := int32(0); idx < replicas; idx++ {
						if !h.untouchedOn(idx, h.revV1) {
							h.dumpState("inside the window")
							t.Fatalf("instance %d moved while the unready pod was inside its window", idx)
						}
					}
				}
				if h.verdict == nil || h.verdict.Gate != workloadtypes.RolloutHoldGateBudget {
					t.Fatalf("hold inside the window = %+v, want the gate's budget denial", h.verdict)
				}

				h.clk.Step(h.stuckGrace)
				requireDarkInstanceRollsFirst(t, h, victim, []int32{0, 1, 2}, nil, sc.strategy, sc.surge, sc.unavail,
					func() bool { return h.settledOn(h.revFixed, replicas) })
				if n := h.servingInstances(); n != replicas {
					h.dumpState("after the roll")
					t.Fatalf("%d Instances serve after the roll, want %d: the roll replaced the unready pod", n, replicas)
				}
			})
		}
	}
}

// TestReconcile_UnpinnedPlanHoldsTheDarkInstanceToo: four Instances serve
// v1 under a drain-first strategy with maxUnavailable 1, listed in a
// rollout group whose run is not pinned, and the highest Instance's pod
// has been unready past the stuck-pod grace while its row stays Ready. A
// push moves nothing while the plan is unpinned, the dark Instance
// included: its loss of service waives the capacity consults, whose count already
// carries it, not the pin, and the hold reads Plan. Once a run pins the
// plan the dark Instance rolls first at no cost in capacity and the peers
// follow within the budget; no repair opens under either restart policy.
// RecreatePod and InPlaceIfPossible, under None and
// RecreateInstanceOnPodRestart.
func TestReconcile_UnpinnedPlanHoldsTheDarkInstanceToo(t *testing.T) {
	const replicas = 4
	for _, policy := range unreadyRollPolicies {
		for _, sc := range offTargetStrategies[1:3] {
			t.Run(string(policy)+"/"+string(sc.strategy), func(t *testing.T) {
				h, victim := unreadyVictimHarness(t, policy, sc.strategy, sc.surge, sc.unavail, replicas)
				h.clk.Step(h.stuckGrace)
				gateConsults, pinConsults := 0, 0
				// The group's gate holds every start it is asked about on the
				// plan, as it does while no run is open; the plan seam says the
				// same for the starts that never reach the gate.
				h.gate = planHoldGate(&gateConsults)
				h.planGate = unpinnedPlanGate(&pinConsults)
				h.events = nil
				h.setTarget(h.revFixed, fixedImage)

				for i := 0; i < 3; i++ {
					h.step()
					for idx := int32(0); idx < replicas; idx++ {
						if !h.untouchedOn(idx, h.revV1) {
							h.dumpState("unpinned")
							t.Fatalf("instance %d moved while the plan was unpinned", idx)
						}
					}
					if h.livePodOnImage(fixedImage) {
						h.dumpState("unpinned")
						t.Fatal("a pod of the pushed revision exists while the plan is unpinned")
					}
				}
				if pinConsults == 0 {
					t.Fatal("the plan seam was never asked while the plan was unpinned")
				}
				if h.verdict == nil || h.verdict.Gate != workloadtypes.RolloutHoldGate(v1beta1.RolloutHoldGatePlan) {
					t.Fatalf("hold while unpinned = %+v, want the plan gate's hold", h.verdict)
				}

				// A run pins the plan: the gate answers on capacity again and the
				// plan seam admits.
				h.planGate = nil
				h.gate = coordinationUnavailabilityGate(h, replicas, sc.unavail, &gateConsults)
				requireDarkInstanceRollsFirst(t, h, victim, []int32{0, 1, 2}, nil, sc.strategy, sc.surge, sc.unavail,
					func() bool { return h.settledOn(h.revFixed, replicas) })
				if n := h.servingInstances(); n != replicas {
					h.dumpState("after the roll")
					t.Fatalf("%d Instances serve after the roll, want %d: the roll replaced the unready pod", n, replicas)
				}
			})
		}
	}
}

// TestRollback_OverAnUnreadyVictimPastTheWindow_SettlesOnTheStartingRevision:
// four Instances serve v1 under SurgeThenDrain with maxSurge 1, and the
// highest Instance's pod has been unready past the stuck-pod grace while
// its row stays Ready. A push surges the dark Instance first; once the
// first Instance has landed the new revision, v1 is re-applied. The
// rollback settles with every row Ready on v1 and no operation open: the
// starting revision is never held behind the unready pod, no pod of the
// new revision remains, the capacity floor the fault lowered holds
// throughout, and the Instance the roll replaced serves again. No repair
// opens under either restart policy. Standalone and listed in a rollout
// group, under None and RecreateInstanceOnPodRestart.
func TestRollback_OverAnUnreadyVictimPastTheWindow_SettlesOnTheStartingRevision(t *testing.T) {
	const replicas = 4
	for _, policy := range unreadyRollPolicies {
		for _, roll := range rollShapes {
			t.Run(string(policy)+"/"+roll.name, func(t *testing.T) {
				h, victim := unreadyVictimHarness(t, policy, workloadtypes.UpdateStrategySurgeThenDrain, 1, 0, replicas)
				if roll.gated {
					consults := 0
					h.gate = rollingGroupGate(h, replicas, 1, 0, &consults)
				}
				h.clk.Step(h.stuckGrace)
				h.events = nil
				h.setTarget(h.revFixed, fixedImage)

				floorHolds := func() {
					if h.repairInFlight() || h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
						h.dumpState("repair")
						t.Fatalf("a repair opened on the unready Instance under policy None: %v", h.events)
					}
					if n := h.servingInstances(); n < replicas-1 {
						h.dumpState("floor")
						t.Fatalf("%d Instances in rotation, floor %d", n, replicas-1)
					}
				}
				if !h.runWithInvariant(40, func() bool { return len(h.landedOn(h.revFixed)) > 0 }, floorHolds) {
					h.dumpState("push")
					t.Fatalf("no Instance landed the new revision")
				}
				if landed := h.landedOn(h.revFixed); landed[0] != victim {
					t.Fatalf("instances on the new revision = %v, want the dark instance %d to land first", landed, victim)
				}

				h.setTarget(h.revV1, goodImage)
				if !h.runWithInvariant(80, func() bool { return h.settledOn(h.revV1, replicas) }, floorHolds) {
					h.dumpState("after the rollback")
					t.Fatalf("the rollback did not settle on %s: %d of %d Instances Ready on it", h.revV1.Name, len(h.landedOn(h.revV1)), replicas)
				}
				if h.livePodOnImage(fixedImage) {
					t.Fatalf("a pod of the rolled-back revision is still alive after the rollback settled")
				}
				if n := h.servingInstances(); n != replicas {
					h.dumpState("after the rollback")
					t.Fatalf("%d Instances serve after the rollback, want %d: the roll replaced the unready pod", n, replicas)
				}
			})
		}
	}
}

// podUIDsOf is the sorted UIDs of pods: the identity of a pod set.
func podUIDsOf(pods []*corev1.Pod) string {
	uids := make([]string, 0, len(pods))
	for _, pod := range pods {
		uids = append(uids, string(pod.UID))
	}
	sort.Strings(uids)
	return strings.Join(uids, ",")
}

// TestReconcile_IdleInstanceUnreadyPastTheWindow_NoPolicyRebuildsIt: an
// Instance at rest whose pod keeps running but stops passing readiness
// leaves rotation at once and, once unready for the stuck-pod grace, is
// dark: its row stays Ready with the serving count down, the Component's
// floor drops by that Instance, and nothing rebuilds it under any restart
// policy — readiness is the runtime's own signal, not a runner restart —
// so the same pod stays, no repair opens, no restart event is raised, no
// attempt is charged to the running revision's ladder and the peers are
// untouched. Single-pod and gang Instances, under each policy and under
// the shape's default.
func TestReconcile_IdleInstanceUnreadyPastTheWindow_NoPolicyRebuildsIt(t *testing.T) {
	const replicas = 3
	none, recreate := workloadtypes.RestartPolicyNone, workloadtypes.RestartPolicyRecreateInstance
	policies := []struct {
		name   string
		policy *workloadtypes.RestartPolicy
	}{{string(none), &none}, {string(recreate), &recreate}, {"default", nil}}
	shapes := []struct {
		name   string
		gang   bool
		runner string
	}{{"single", false, ""}, {"gang leader", true, "leader"}, {"gang worker", true, "worker"}}
	for _, shape := range shapes {
		for _, pc := range policies {
			t.Run(shape.name+"/"+pc.name, func(t *testing.T) {
				h, victim := unreadyVictimHarnessUnder(t, shape.gang, pc.policy, shape.runner, workloadtypes.UpdateStrategySurgeThenDrain, 1, 0, replicas)
				before := podUIDsOf(h.podsOf(victim))
				h.events = nil
				h.clk.Step(h.stuckGrace)
				for i := 0; i < 6; i++ {
					h.step()
					if h.repairInFlight() || h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
						h.dumpState("repair")
						t.Fatalf("a repair opened on the dark Instance: %v", h.events)
					}
					row := h.instance(victim)
					if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil || row.ServingPodCount >= row.PodCount || row.RunningRevision != h.revV1.Name {
						h.dumpState("row")
						t.Fatalf("instance %d = %+v, want Ready on v1 with its serving count down and nothing open", victim, row)
					}
					if after := podUIDsOf(h.podsOf(victim)); after != before {
						h.dumpState("pods")
						t.Fatalf("instance %d's pods changed from %s to %s: a pod that fails readiness is not replaced", victim, before, after)
					}
					if b := h.findBlock(h.revV1.Name); b != nil {
						t.Fatalf("no attempt is charged to the running revision's ladder; got %+v", b)
					}
					if n := h.servingInstances(); n != replicas-1 {
						t.Fatalf("%d Instances serve, want %d: the dark Instance is out and its peers untouched", n, replicas-1)
					}
					for peer := int32(0); peer < victim; peer++ {
						if !h.untouchedOn(peer, h.revV1) {
							t.Fatalf("instance %d moved while nothing was pushed", peer)
						}
					}
				}
			})
		}
	}
}

// TestReconcile_UnreadyVictimInsideTheWindow_RollTakesItFirst: four
// Instances serve v1 and the highest Instance's pod stops passing
// readiness while its row stays Ready; the push lands while the pod is
// still inside the stuck-pod grace, so its start is charged and
// consulted like any other. The roll takes the Instance out of rotation
// first all the same, under a surge and a drain-first strategy alike:
// the fix lands on it while every serving peer is untouched and in
// rotation, before the pod's grace has run, and the roll then settles
// within its budget with every Instance serving; no repair opens under
// policy None. Standalone, and listed in a rollout group for the surge
// strategy, whose gate paces surges alone; a gated drain-first push
// inside the grace is held by the gate until the pod is dark.
func TestReconcile_UnreadyVictimInsideTheWindow_RollTakesItFirst(t *testing.T) {
	const replicas = 4
	for _, sc := range offTargetStrategies[:3] {
		for _, roll := range rollShapes {
			if roll.gated && sc.strategy != workloadtypes.UpdateStrategySurgeThenDrain {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s", sc.strategy, roll.name), func(t *testing.T) {
				h, victim := unreadyVictimHarness(t, workloadtypes.RestartPolicyNone, sc.strategy, sc.surge, sc.unavail, replicas)
				if roll.gated {
					consults := 0
					h.gate = rollingGroupGate(h, replicas, sc.surge, sc.unavail, &consults)
				}
				requireUnreadyVictimRollsFirstInsideTheWindow(t, h, victim, []int32{0, 1, 2}, sc.strategy, sc.surge, sc.unavail, replicas)
			})
		}
	}
}

// TestReconcile_UnreadyGangLeaderInsideTheWindow_RollTakesItFirst: a gang
// serves through its leader. Two leader+worker gangs serve v1 and the
// higher gang's leader stops passing readiness beside its serving worker,
// its row staying Ready; a push inside the stuck-pod grace takes that
// gang first under a drain-first strategy: the fix lands on it while the
// serving gang is untouched and in rotation, before the leader's grace
// has run, and the roll settles within its budget with both gangs
// serving; no repair opens under policy None.
func TestReconcile_UnreadyGangLeaderInsideTheWindow_RollTakesItFirst(t *testing.T) {
	const replicas = 2
	for _, sc := range offTargetStrategies[1:3] {
		t.Run(string(sc.strategy), func(t *testing.T) {
			h := offTargetCrashHarness(t, true, sc.strategy, sc.surge, sc.unavail, workloadtypes.RestartPolicyNone, replicas)
			h.stuckGrace = 5 * time.Minute
			const victim = int32(replicas - 1)
			h.failReadiness(victim, "leader")
			h.step()
			row := h.instance(victim)
			if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil || row.ServingPodCount >= row.PodCount {
				h.dumpState("after the fault")
				t.Fatalf("gang %d after the fault = %+v, want a Ready row with its leader out of rotation", victim, row)
			}
			requireUnreadyVictimRollsFirstInsideTheWindow(t, h, victim, []int32{0}, sc.strategy, sc.surge, sc.unavail, replicas)
		})
	}
}

// requireUnreadyVictimRollsFirstInsideTheWindow pushes the fix over an
// Instance whose promoted pod is unready and still inside its grace, and
// requires the roll to land the fix on it first (requireDarkInstanceRollsFirst),
// before the grace has run, then to settle with every Instance serving.
func requireUnreadyVictimRollsFirstInsideTheWindow(t *testing.T, h *recoveryHarness, victim int32, serving []int32, strategy workloadtypes.UpdateStrategyType, surge, unavail, replicas int32) {
	t.Helper()
	var unreadySince time.Time
	for _, pod := range h.podsOf(victim) {
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.ContainersReady && cond.Status == corev1.ConditionFalse {
				unreadySince = cond.LastTransitionTime.Time
			}
		}
	}
	if unreadySince.IsZero() {
		t.Fatalf("instance %d has no pod reporting its containers not ready", victim)
	}
	h.events = nil
	h.setTarget(h.revFixed, fixedImage)
	var firstLanding time.Time
	done := func() bool {
		if firstLanding.IsZero() && len(h.landedOn(h.revFixed)) > 0 {
			firstLanding = h.clk.Now()
		}
		return h.settledOn(h.revFixed, replicas)
	}
	requireDarkInstanceRollsFirst(t, h, victim, serving, nil, strategy, surge, unavail, done)
	if firstLanding.IsZero() || firstLanding.Sub(unreadySince) >= h.stuckGrace {
		t.Fatalf("the fix landed first %s after the pod left rotation, not inside the grace of %s: the order must not wait for the pod to be dark", firstLanding.Sub(unreadySince), h.stuckGrace)
	}
	if n := h.servingInstances(); n != replicas {
		h.dumpState("after the roll")
		t.Fatalf("%d Instances serve after the roll, want %d: the roll replaced the unready pod", n, replicas)
	}
}

// Single-pod Instances serve one revision with nothing in flight, under
// restart policy None, and one Instance's pod is taken by hand three
// times, each time once the Instance is back and serving: deleted, with
// the kubelet reporting the stopped pod first, or evicted. Each loss
// exhausts the unavailability budget, and while the Instance is short the
// pass reports the Budget hold naming it, so status says why the
// Component is below its floor. Once the pod is back in rotation and
// every row is Ready with no operation open, the next pass reports no
// hold and neither does any pass after it, inside the window the rebuilt
// pod has yet to hold Ready for: a roll with nothing left to start has
// nothing to stand behind, and the status writer publishes the nil
// verdict as it publishes a hold. Under SurgeThenDrain maxSurge 1 and
// RecreatePod maxUnavailable 1, on four engine Instances and on two
// router Instances, which run the same loop.
func TestSteadyService_PodLossHoldClearsOnceTheInstanceServesAgain(t *testing.T) {
	const repetitions = 3
	components := []struct {
		component workloadtypes.ComponentType
		replicas  int32
	}{
		{component: workloadtypes.ComponentEngine, replicas: 4},
		{component: workloadtypes.ComponentRouter, replicas: 2},
	}
	for _, shape := range components {
		for _, strategy := range []workloadtypes.UpdateStrategyType{workloadtypes.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategyRecreatePod} {
			for _, story := range []struct {
				name  string
				evict bool
			}{
				{name: "the pod is deleted and the kubelet reports it stopped first"},
				{name: "the pod is evicted", evict: true},
			} {
				t.Run(string(shape.component)+"/"+string(strategy)+"/"+story.name, func(t *testing.T) {
					replicas := shape.replicas
					h := newComponentRecoveryHarness(t, false, "main", shape.component)
					h.recorder = record.NewFakeRecorder(512)
					h.replicas = replicas
					none := workloadtypes.RestartPolicyNone
					h.lifecycle.RestartPolicy = &none
					// Several passes fit inside the window a rebuilt pod has
					// yet to hold Ready for.
					h.stuckGrace = 60 * time.Second
					h.stepFloor = 5 * time.Second
					h.stepCap = h.stepFloor
					h.kubeletLag = time.Second
					if !story.evict {
						h.podGrace = 30 * time.Second
					}
					h.useRollingBudget(strategy)
					if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
						h.dumpState("initial create")
						t.Fatalf("initial create never settled on v1 for %d Instances", replicas)
					}
					serving := func() bool {
						return h.settledOn(h.revV1, replicas) && h.instancesServingOn(goodImage) == int(replicas)
					}
					take := func() {
						pod := h.podOf(0, "")
						if story.evict {
							h.evictPods(0)
							return
						}
						if h.exitsCleanOnStop == nil {
							h.exitsCleanOnStop = map[string]bool{}
						}
						h.exitsCleanOnStop[pod.Name] = true
						if err := h.c.Delete(h.ctx, pod); err != nil {
							t.Fatalf("delete %s: %v", pod.Name, err)
						}
					}
					for rep := 0; rep < repetitions; rep++ {
						take()
						namedWhileShort := false
						if !h.runWithInvariant(40, serving, func() {
							if serving() {
								return
							}
							if hold := h.verdict; hold != nil && hold.Gate == workloadtypes.RolloutHoldGateBudget && hold.Target == h.revV1.Name && holdNamesInstance(hold, 0) {
								namedWhileShort = true
							}
						}) {
							h.dumpState("loss")
							t.Fatalf("loss %d: Instance 0 never came back serving", rep)
						}
						if !namedWhileShort {
							h.dumpState("silent loss")
							t.Fatalf("loss %d: no pass reported the Budget hold naming Instance 0 while it was short; the story did not run (holds=%v)", rep, h.holds)
						}
						for i := 0; i < 3; i++ {
							h.step()
							if !serving() {
								h.dumpState("left serving")
								t.Fatalf("loss %d: the Component left its settled state with nothing taking it", rep)
							}
							if h.verdict != nil {
								h.dumpState("hold left behind")
								t.Fatalf("loss %d: a hold is left behind while every Instance is Ready and serving: %+v", rep, h.verdict)
							}
						}
					}
				})
			}
		}
	}
}

// Four single-pod Instances serve the revision the Component is at with
// nothing in flight, and that revision's retry ladder is Held. A pass
// with nothing to run reports the Held ladder as the roll's hold, pass
// after pass: a converged subject whose ladder gave up on its revision
// says so rather than reading idle, and the status writer publishes what
// the pass reports.
func TestConvergedSubject_HeldLadderOnItsRevisionIsReportedWithNothingToRun(t *testing.T) {
	const replicas = int32(4)
	h := newRecoveryHarness(t, false)
	h.replicas = replicas
	// The rows stay inside the window the block's prune waits out.
	h.stuckGrace = 10 * time.Minute
	h.stepFloor = 5 * time.Second
	h.stepCap = h.stepFloor
	h.kubeletLag = time.Second
	h.setTarget(h.revV1, goodImage)
	if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
		h.dumpState("initial create")
		t.Fatalf("initial create never settled on v1 for %d Instances", replicas)
	}
	h.blocks = append(h.blocks, workloadtypes.RetryBlock{TargetRevision: h.target.Name, State: workloadtypes.RetryBlockHeld, AttemptsStarted: 3, Reason: "Error"})
	for i := 0; i < 3; i++ {
		verdicts := h.holdVerdicts
		h.step()
		if h.holdVerdicts == verdicts {
			t.Fatalf("pass %d reported no verdict at the update position", i+1)
		}
		if hold := h.verdict; hold == nil || hold.Gate != workloadtypes.RolloutHoldGateHeld || hold.Target != h.target.Name || !strings.Contains(hold.Reason, "3 failed attempt") {
			h.dumpState("held ladder")
			t.Fatalf("pass %d must report the Held ladder on %s, got %+v", i+1, h.target.Name, hold)
		}
		if !h.settledOn(h.revV1, replicas) {
			h.dumpState("moved")
			t.Fatalf("pass %d moved a settled Instance under a Held ladder with nothing to run", i+1)
		}
	}
}

// darkRollShapes are the shapes a roll over a dark Instance is driven
// under: each drain-first arm under its unavailability budget and the
// surge arm under its floor, standalone and listed in a rollout group
// whose gate counts surges alone or waives an in-place start.
var darkRollShapes = []struct {
	name           string
	strategy       workloadtypes.UpdateStrategyType
	surge, unavail int32
	gated          bool
}{
	{"surge standalone", workloadtypes.UpdateStrategySurgeThenDrain, 1, 0, false},
	{"surge rolling group", workloadtypes.UpdateStrategySurgeThenDrain, 1, 0, true},
	{"recreate standalone", workloadtypes.UpdateStrategyRecreatePod, 0, 1, false},
	{"in-place standalone", workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 1, false},
	{"in-place rolling group", workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 1, true},
}

// rollbackHooks are the moments of a roll a rollback is started at: the
// first pod of the new revision alive, and the roll's last step, every
// Instance but one landed on it.
var rollbackHooks = []struct {
	name    string
	reached func(h *recoveryHarness, replicas int32) bool
}{
	{"rotating", func(h *recoveryHarness, _ int32) bool { return h.livePodOnImage(fixedImage) }},
	{"last", func(h *recoveryHarness, replicas int32) bool { return int32(len(h.landedOn(h.revFixed))) >= replicas-1 }},
}

// keepReadinessCause re-arms the readiness fault on the Instance's pods
// each time a fresh set of them serves at rest: the cause outlives the
// pod it was set in, as a fault in the Instance's own configuration does.
func (h *recoveryHarness) keepReadinessCause(idx int32) {
	row := h.instance(idx)
	if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil {
		return
	}
	for _, pod := range h.podsOf(idx) {
		if podConditionTrue(pod, corev1.PodReady) {
			h.readinessFails[pod.UID] = true
		}
	}
}

// unreadyInsideGraceAt reports whether a pod of the Instance had, at the
// given instant, failed readiness for less than the stuck-pod grace: out
// of rotation, not yet dark.
func (h *recoveryHarness) unreadyInsideGraceAt(idx int32, at time.Time) bool {
	for _, pod := range h.podsOf(idx) {
		if evidence.UnreadyGraceLeft(pod, at, h.stuckGrace) > 0 {
			return true
		}
	}
	return false
}

// TestReconcile_RollOverADarkInstanceOnTheTarget_ReachesItsEndAndTheRollbackLands
// is the closed-loop story of a push over an Instance whose readiness
// cause outlives its pods. Four Instances; the highest one's promoted pod
// keeps running and fails readiness, and every fresh set of that Instance
// serves and then fails the same way. Once dark the Instance rolls first
// at no cost in capacity; on the target its new set goes dark again, and
// from then on it holds no slot in the roll's budget and opens no floor
// hold: a hold may name it only while its pod is inside the grace, and
// the roll reaches its end past it, the first pod of the new revision and
// then every Instance but one landed. A rollback started at either moment
// lands: every Instance settles Ready on the starting revision with no
// operation, no pod of the new revision remains, the peers serve, and the
// victim's row stays Ready with its serving and ready counts below its
// pod count while its cause stays. No repair opens under either restart
// policy, and the capacity floor the fault lowered holds throughout.
func TestReconcile_RollOverADarkInstanceOnTheTarget_ReachesItsEndAndTheRollbackLands(t *testing.T) {
	const replicas = 4
	for _, policy := range unreadyRollPolicies {
		for _, sc := range darkRollShapes {
			for _, hook := range rollbackHooks {
				t.Run(string(policy)+"/"+sc.name+"/"+hook.name, func(t *testing.T) {
					h, victim := unreadyVictimHarness(t, policy, sc.strategy, sc.surge, sc.unavail, replicas)
					if sc.gated {
						consults := 0
						h.gate = rollingGroupGate(h, replicas, sc.surge, sc.unavail, &consults)
					}
					h.clk.Step(h.stuckGrace)
					h.events = nil
					floor := int32(replicas-1) - sc.unavail
					// A step runs its pass and then moves the clock, so the
					// verdict it leaves was read at the clock the previous
					// check saw.
					passAt := h.clk.Now()
					invariant := func() {
						readAt := passAt
						passAt = h.clk.Now()
						h.keepReadinessCause(victim)
						if h.repairInFlight() || h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
							h.dumpState("repair")
							t.Fatalf("a repair opened on the dark Instance: %v", h.events)
						}
						if n := h.servingInstances(); n < floor {
							h.dumpState("floor")
							t.Fatalf("%d Instances in rotation, floor %d", n, floor)
						}
						if holdNamesInstance(h.verdict, victim) && !h.unreadyInsideGraceAt(victim, readAt) {
							h.dumpState("held on the dark Instance")
							t.Fatalf("the roll is held on the dark Instance %d: %q", victim, h.verdict.Reason)
						}
					}
					h.setTarget(h.revFixed, fixedImage)
					if !h.runWithInvariant(120, func() bool { return hook.reached(h, replicas) }, invariant) {
						h.dumpState("roll")
						t.Fatalf("the roll never reached its %s step; last hold %+v", hook.name, h.lastHold())
					}

					h.setTarget(h.revV1, goodImage)
					landed := func() bool {
						row := h.instance(victim)
						return h.settledOn(h.revV1, replicas) && row != nil &&
							row.ServingPodCount < row.PodCount && row.ReadyPodCount < row.PodCount
					}
					if !h.runWithInvariant(120, landed, invariant) {
						h.dumpState("after the rollback")
						t.Fatalf("the rollback did not settle on %s with the victim's row reporting its loss; victim = %+v", h.revV1.Name, h.instance(victim))
					}
					if h.livePodOnImage(fixedImage) {
						t.Fatalf("a pod of the rolled-back revision is still alive after the rollback settled")
					}
					if n := h.servingInstances(); n != replicas-1 || h.instanceServes(victim) {
						h.dumpState("after the rollback")
						t.Fatalf("%d Instances serve after the rollback, want %d with the victim out of rotation", n, replicas-1)
					}
				})
			}
		}
	}
}

// A one-down, no-surge roll stops at the Instance a bad push breaks and the
// next push lands there first. Three single-pod Instances under RecreatePod
// with maxSurge 0 and maxUnavailable 1; a push whose container dies at every
// start takes one Instance down and the ladder holds the revision there: the
// row carries the disposed attempt parked on the hold, Failed with no attempt
// in flight, the two others serve v1 untouched, no second Instance is taken
// and no pod of the crashing revision is made past the bound. The corrected
// push lands on the parked Instance first, since replacing a set that serves
// nothing costs no capacity, with both peers still serving v1, then on the
// peers one at a time; nothing rebuilds the crashing revision once the fix is
// the target and no repair opens on the parked Instance.
func TestOneDownNoSurge_BadPushParksOneInstanceAndTheFixLandsThereFirst(t *testing.T) {
	const replicas = 3
	h := offTargetCrashHarness(t, false, workloadtypes.UpdateStrategyRecreatePod, 0, 1, workloadtypes.RestartPolicyNone, replicas)
	h.retryPolicy = pushRetryLadder

	seen := map[types.UID]struct{}{}
	revCrash := h.crashRevision(crashStartImage)
	oneDown := func() {
		h.trackPodsOnImage(crashStartImage, seen)()
		if n := h.servingInstances(); n < replicas-1 {
			h.dumpState("two down")
			t.Fatalf("%d Instances in rotation, want at least %d: at most one Instance may be out of rotation", n, replicas-1)
		}
	}
	if !h.runWithInvariant(120, func() bool { return h.heldOn(revCrash.Name) }, oneDown) {
		h.dumpState("crash loop never held")
		t.Fatalf("the crashing revision was never Held: attempts at it are unbounded")
	}
	parked := int32(-1)
	for _, s := range h.irStatuses() {
		if !workloadtypes.OperationParked(s.Operation) {
			if !h.untouchedOn(s.Index, h.revV1) {
				h.dumpState("second Instance taken")
				t.Fatalf("instance %d = %+v, want it untouched on v1: the bad push may cost one Instance", s.Index, s)
			}
			continue
		}
		if parked >= 0 {
			h.dumpState("two parked")
			t.Fatalf("instances %d and %d both carry the parked attempt; the bad push may cost one Instance", parked, s.Index)
		}
		parked = s.Index
		if s.Phase != workloadtypes.InstancePhaseFailed || s.Operation.TargetRevision != revCrash.Name ||
			s.Operation.Waiting != string(workloadtypes.RolloutHoldGateHeld) || !s.Operation.Deadline.IsZero() || s.LastFailure == nil {
			h.dumpState("parked row")
			t.Fatalf("the parked row = %+v, want Failed with the attempt at %s parked on the hold and the crash recorded", s, revCrash.Name)
		}
	}
	if parked < 0 {
		h.dumpState("no parked row")
		t.Fatal("no Instance carries the attempt the ladder held")
	}
	for _, pod := range h.podsOnImage(crashStartImage) {
		if idx, _ := query.InstanceIdxFromLabels(pod); idx != parked {
			t.Fatalf("pod %s of the crashing revision belongs to instance %d, want only the parked instance %d", pod.Name, idx, parked)
		}
	}
	if len(seen) > int(pushRetryLadder.MaxAttempts) {
		t.Fatalf("%d pods were made on the crashing revision, want at most the ladder's bound %d", len(seen), pushRetryLadder.MaxAttempts)
	}
	var peers []int32
	for idx := int32(0); idx < replicas; idx++ {
		if idx != parked {
			peers = append(peers, idx)
		}
	}
	// Held has no time bound: nothing re-opens the parked row and no peer moves.
	h.runWithInvariant(6, func() bool { return false }, func() {
		oneDown()
		if s := h.instance(parked); attemptOpen(s) || s.Phase == workloadtypes.InstancePhaseUpdating {
			h.dumpState("attempt after Held")
			t.Fatalf("an attempt at the held revision was reopened: %+v", s)
		}
		for _, peer := range peers {
			if !h.untouchedOn(peer, h.revV1) {
				h.dumpState("peer taken under the hold")
				t.Fatalf("instance %d left v1 while the crashing revision is held", peer)
			}
		}
	})
	if len(seen) > int(pushRetryLadder.MaxAttempts) {
		t.Fatalf("a pod of the crashing revision was made under the hold: %d seen, bound %d", len(seen), pushRetryLadder.MaxAttempts)
	}

	h.events = nil
	h.setTarget(h.revFixed, fixedImage)
	requireDarkInstanceRollsFirst(t, h, parked, peers, nil, workloadtypes.UpdateStrategyRecreatePod, 0, 1,
		func() bool { return h.settledOn(h.revFixed, replicas) })
	if n := h.servingInstances(); n != replicas {
		h.dumpState("after the fix")
		t.Fatalf("%d Instances serve after the fix, want %d", n, replicas)
	}
	if pods := h.podsOnImage(crashStartImage); len(pods) != 0 {
		t.Fatalf("%d pod(s) of the crashing revision are still alive after the fix landed", len(pods))
	}
	if h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
		t.Fatalf("a repair opened on the parked Instance; it is the update pass's once the fix is the target: %v", h.events)
	}
}

// A roll onto a revision whose runner dies after serving, under a
// minReadySeconds window the runner never holds: the recreate attempt
// never promotes, its deadline ends it, and the revision's retry ladder
// paces the next one. Between the two the kubelet keeps restarting the
// runner in place and the pod set serves between crashes. The phase says
// what the pods do: the row keeps the attempt, parked on the wait the
// ladder names, reads Failed while the set sits in the kubelet's back-off
// and not Failed while the full set serves, keeps its reading across a
// restart in place inside the stuck-pod grace, records the crash, and the
// ladder is charged one attempt per disposal until it holds, where the
// phase keeps following the pods. The next attempt replaces the pods as a
// fresh recreate, the Instance the roll never reached serves throughout,
// and a corrected revision lands through the ordinary roll.
func TestCrashBeforePromotion_DisposedRollAttemptFollowsItsPods(t *testing.T) {
	for _, shape := range []struct {
		name     string
		multiPod bool
	}{
		{name: "single pod"},
		{name: "gang whose leader dies", multiPod: true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			const replicas = 2
			h := newRecoveryHarnessFor(t, shape.multiPod, constants.MainContainerName)
			h.replicas = replicas
			repair := workloadtypes.RestartPolicyRecreateInstance
			h.lifecycle.RestartPolicy = &repair
			// The runner serves one pass and dies, and the promote needs a
			// window of several passes, so no attempt at the crashing
			// revision promotes: only the attempt deadline ends it.
			h.crashAfterServed = 1
			h.crashLeaderOnly = shape.multiPod
			h.stuckGrace = 10 * time.Minute
			h.stepCap = recoveryStepFloor
			h.minReadySeconds = int32(4 * recoveryStepFloor / time.Second)
			h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 4 * recoveryStepFloor}
			h.retryPolicy = pushRetryLadder
			h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
			if !h.run(60, func() bool { return h.settledOn(h.revV1, replicas) }) {
				h.dumpState("initial create")
				t.Fatalf("initial create never settled on v1 for %d instances", replicas)
			}
			peerPods := map[types.UID]bool{}
			for _, pod := range h.podsOf(1) {
				peerPods[pod.UID] = true
			}

			revCrash := h.crashRevision(flapAfterServingImage)
			parkedOn := func(op *workloadtypes.InstanceOperation) bool {
				return workloadtypes.OperationParked(op) && op.TargetRevision == revCrash.Name && workloadtypes.ParkedWaitingReason(op.Waiting)
			}
			waitingReason := func(pod *corev1.Pod) string {
				for _, cs := range pod.Status.ContainerStatuses {
					if cs.State.Waiting != nil {
						return cs.State.Waiting.Reason
					}
				}
				return ""
			}
			// crashSet reports whether Instance 0 holds its full pod set on
			// the crashing revision, whether every member of it serves, and
			// whether the set is dark: its routed runner in the kubelet's
			// back-off, which reads out of service at once.
			crashSet := func() (full, serving, dark bool) {
				live := 0
				serving = true
				for _, pod := range h.podsOf(0) {
					if pod.Spec.Containers[0].Image != flapAfterServingImage || pod.DeletionTimestamp != nil {
						continue
					}
					live++
					if !podreadiness.IsContainersReady(pod) || !podreadiness.IsServing(pod) {
						serving = false
					}
					if query.RoutedRunner(pod) && !podreadiness.ReadyAndServing(pod) && waitingReason(pod) == "CrashLoopBackOff" {
						dark = true
					}
				}
				full = live == len(h.desired.Runners)
				return full, full && serving, dark
			}
			attempts := func() int32 {
				if block := h.findBlock(revCrash.Name); block != nil {
					return block.AttemptsStarted
				}
				return 0
			}
			seen := map[types.UID]struct{}{}
			var sawParkedServing, sawParkedDown, sawHeldServing, sawFreshAttempt bool
			var lastParkedPhase workloadtypes.InstancePhase
			observe := func() {
				h.trackPodsOnImage(flapAfterServingImage, seen)()
				if peer := h.instance(1); peer == nil || peer.Phase != workloadtypes.InstancePhaseReady || peer.RunningRevision != h.revV1.Name {
					h.dumpState("peer taken")
					t.Fatalf("Instance 1 must stay Ready on v1 while the roll parks on Instance 0, got %+v", peer)
				}
				for _, pod := range h.podsOf(1) {
					if !peerPods[pod.UID] {
						h.dumpState("peer pod replaced")
						t.Fatalf("Instance 1 serves through a pod the roll never made: %s", pod.Name)
					}
				}
				row := h.instance(0)
				if row == nil || row.RunningRevision == revCrash.Name {
					return
				}
				full, serving, dark := crashSet()
				if row.Phase == workloadtypes.InstancePhaseFailed && serving {
					h.dumpState("failed while serving")
					t.Fatalf("Instance 0 reads Failed while its full pod set is Ready and in rotation: %+v", *row)
				}
				if !parkedOn(row.Operation) {
					lastParkedPhase = ""
					if attempts() > 0 && row.Phase == workloadtypes.InstancePhaseUpdating && row.Operation != nil &&
						row.Operation.TargetRevision == revCrash.Name && row.Operation.Waiting == "" {
						sawFreshAttempt = true
					}
					return
				}
				if row.LastFailure == nil {
					t.Fatalf("a parked attempt records the crash that ended it: %+v", *row)
				}
				held := h.heldOn(revCrash.Name)
				switch {
				case serving:
					if row.Phase != workloadtypes.InstancePhaseUpdating {
						h.dumpState("parked while serving")
						t.Fatalf("a parked attempt whose full set serves reads as the roll in progress, got %s: %+v", row.Phase, *row)
					}
					sawParkedServing = true
					if held {
						sawHeldServing = true
					}
				case dark:
					if row.Phase != workloadtypes.InstancePhaseFailed {
						h.dumpState("parked while down")
						t.Fatalf("a parked attempt whose set is in the kubelet's back-off reads Failed, got %s: %+v", row.Phase, *row)
					}
					sawParkedDown = true
				case full && lastParkedPhase != "":
					if row.Phase != lastParkedPhase {
						h.dumpState("parked while restarting")
						t.Fatalf("a parked attempt whose runner restarts in place inside the grace keeps reading %s, got %s: %+v", lastParkedPhase, row.Phase, *row)
					}
				}
				lastParkedPhase = row.Phase
			}
			reached := h.runWithInvariant(240, func() bool {
				return h.heldOn(revCrash.Name) && sawHeldServing && sawParkedDown
			}, observe)
			if !reached {
				h.dumpState("ladder")
				t.Fatalf("the story never reached a held ladder with the parked attempt observed serving and down (held=%v serving=%v down=%v attempts=%d)",
					h.heldOn(revCrash.Name), sawHeldServing, sawParkedDown, attempts())
			}
			if !sawParkedServing || !sawFreshAttempt {
				t.Fatalf("the story must observe the parked attempt serving (%v) and a fresh attempt replacing it (%v)", sawParkedServing, sawFreshAttempt)
			}
			block := h.findBlock(revCrash.Name)
			if block.AttemptsStarted != pushRetryLadder.MaxAttempts {
				t.Fatalf("attempts counted = %d, want one per disposal up to the ladder's bound %d", block.AttemptsStarted, pushRetryLadder.MaxAttempts)
			}
			if want := int(pushRetryLadder.MaxAttempts) * len(h.desired.Runners); len(seen) != want {
				h.dumpState("attempts")
				t.Fatalf("%d pods were made on the crashing revision, want %d: one pod set per attempt of the ladder", len(seen), want)
			}
			row := h.instance(0)
			if !parkedOn(row.Operation) || row.Operation.Waiting != string(workloadtypes.RolloutHoldGateHeld) {
				t.Fatalf("under a held ladder the parked attempt names the hold as its wait, got %+v", row.Operation)
			}
			// The phase keeps following the pods under the hold.
			h.runWithInvariant(6, func() bool { return false }, observe)

			// A runner the kubelet restarts in place, back inside the stuck-pod
			// grace, moves nothing: the row keeps reading as the roll in
			// progress and the pass writes no status.
			if !h.runWithInvariant(6, func() bool {
				_, serving, _ := crashSet()
				return serving && h.instance(0).Phase == workloadtypes.InstancePhaseUpdating
			}, observe) {
				h.dumpState("serving under the hold")
				t.Fatalf("the parked set never read serving under the hold")
			}
			var routed *corev1.Pod
			for _, pod := range h.podsOf(0) {
				if pod.Spec.Containers[0].Image == flapAfterServingImage && query.RoutedRunner(pod) {
					routed = pod
				}
			}
			h.killRunnerOnce(routed)
			writes := h.statusWrites
			h.step()
			var restarted *corev1.Pod
			for _, pod := range h.podsOf(0) {
				if pod.Name == routed.Name {
					restarted = pod
				}
			}
			if restarted == nil || restarted.Status.Phase != corev1.PodRunning || podreadiness.IsContainersReady(restarted) || waitingReason(restarted) != "" {
				t.Fatalf("the kubelet model must report the runner restarting in place, got %+v", restarted)
			}
			if row := h.instance(0); !parkedOn(row.Operation) || row.Phase != workloadtypes.InstancePhaseUpdating {
				h.dumpState("restart in place")
				t.Fatalf("a parked attempt whose runner restarts in place inside the grace keeps reading Updating, got %+v", *row)
			}
			if h.statusWrites != writes {
				t.Fatalf("the restart in place must write no status, got %d write(s)", h.statusWrites-writes)
			}

			h.setTarget(h.revFixed, fixedImage)
			if !h.run(80, func() bool {
				return h.settledOn(h.revFixed, replicas) && len(h.podsOnImage(flapAfterServingImage)) == 0
			}) {
				h.dumpState("corrected push")
				t.Fatalf("the corrected push did not land on every Instance after the crashing revision was held")
			}
		})
	}
}

// ensureRevisionWithMeta is ensureRevision with the pod template's own
// metadata folded into the hash: a metadata-only edit mints a revision an
// in-place step lands by patching the pod, replacing no container.
func (h *recoveryHarness) ensureRevisionWithMeta(spec *corev1.PodSpec, meta *metav1.ObjectMeta) *appsv1.ControllerRevision {
	h.t.Helper()
	var workerSpec *corev1.PodSpec
	if h.multiPod {
		workerSpec = spec.DeepCopy()
	}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		h.ctx, h.c, h.c, h.isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		h.revisionKey(), spec, workerSpec, meta, nil, h.isvc.UID,
	)
	if err != nil {
		h.t.Fatalf("EnsureControllerRevision: %v", err)
	}
	if cr.CreationTimestamp.IsZero() {
		cr.CreationTimestamp = metav1.NewTime(h.clk.Now())
		if err := h.c.Update(h.ctx, cr); err != nil {
			h.t.Fatalf("stamp ControllerRevision creation: %v", err)
		}
	}
	return cr
}

// pushMetadataRevision publishes a revision that differs from the running
// template by the pod's release annotation alone and points the loop at
// it: the push an in-place step lands by patching metadata.
func (h *recoveryHarness) pushMetadataRevision(release string) *appsv1.ControllerRevision {
	h.t.Helper()
	meta := &metav1.ObjectMeta{Annotations: map[string]string{"release": release}}
	rev := h.ensureRevisionWithMeta(h.podSpec(goodImage), meta)
	h.setTarget(rev, goodImage)
	h.desired.PodTemplateObjectMeta = meta
	return rev
}

// livePodOnRevision reports whether a live pod carries rev's hash label.
func (h *recoveryHarness) livePodOnRevision(rev *appsv1.ControllerRevision) bool {
	hash := query.RevisionOf(rev).Hash()
	for _, pod := range h.livePods() {
		if pod.Labels[query.LabelRevisionHash] == hash {
			return true
		}
	}
	return false
}

// TestReconcile_MetadataPushOverADarkInstance_InPlaceStepRecreatesIt: four
// Instances serve v1 under InPlaceIfPossible with maxUnavailable 1, and
// the highest Instance's promoted pod has failed readiness past the
// stuck-pod grace while its row stays Ready. A push that changes the pod
// template's metadata alone is a diff an in-place step patches without
// replacing a container, which would leave that pod exactly as dark as it
// found it with the one unavailable slot spent on the wait. The roll takes
// the dark Instance first as a recreate: its pod is replaced at the target
// and serves again, the peers are patched in place after it within the
// budget, and the roll completes with every Instance serving on the new
// revision; no repair opens under policy None. Standalone and listed in a
// rollout group whose gate waives an in-place start.
func TestReconcile_MetadataPushOverADarkInstance_InPlaceStepRecreatesIt(t *testing.T) {
	const replicas = 4
	for _, shape := range []struct {
		name  string
		gated bool
	}{{"standalone", false}, {"rolling group", true}} {
		t.Run(shape.name, func(t *testing.T) {
			h, victim := unreadyVictimHarness(t, workloadtypes.RestartPolicyNone, workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 1, replicas)
			if shape.gated {
				consults := 0
				h.gate = rollingGroupGate(h, replicas, 0, 1, &consults)
			}
			h.clk.Step(h.stuckGrace)
			h.events = nil
			before := map[int32]string{}
			for idx := int32(0); idx < replicas; idx++ {
				before[idx] = podUIDsOf(h.podsOf(idx))
			}
			rev := h.pushMetadataRevision("two")
			var firstLanded []int32
			invariant := func() {
				if h.repairInFlight() || h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
					h.dumpState("repair")
					t.Fatalf("a repair opened on the dark Instance under policy None: %v", h.events)
				}
				if n := escalation.CurrentUnavailableInFlight(h.irStatuses()); n > 1 {
					h.dumpState("budget")
					t.Fatalf("%d Instances offline for the roll, budget 1", n)
				}
				if n := h.servingInstances(); n < replicas-2 {
					h.dumpState("floor")
					t.Fatalf("%d Instances in rotation, floor %d", n, replicas-2)
				}
				if firstLanded != nil {
					return
				}
				if landed := h.landedOn(rev); len(landed) > 0 {
					firstLanded = landed
					for peer := int32(0); peer < victim; peer++ {
						if !h.untouchedOn(peer, h.revV1) {
							h.dumpState("first landing")
							t.Fatalf("instance %d was rolled before the dark instance %d landed (landed %v)", peer, victim, landed)
						}
					}
				}
			}
			if !h.runWithInvariant(100, func() bool { return h.settledOn(rev, replicas) }, invariant) {
				h.dumpState("roll")
				t.Fatalf("the roll never completed over the dark Instance; last hold %+v", h.lastHold())
			}
			if firstLanded == nil || firstLanded[0] != victim {
				t.Fatalf("instances landed first = %v, want the dark instance %d", firstLanded, victim)
			}
			if podUIDsOf(h.podsOf(victim)) == before[victim] {
				t.Fatalf("instance %d keeps its dark pod: a patch that replaces no container cannot restore it", victim)
			}
			for peer := int32(0); peer < victim; peer++ {
				if podUIDsOf(h.podsOf(peer)) != before[peer] {
					t.Fatalf("instance %d was rebuilt; a serving peer is patched in place", peer)
				}
			}
			if n := h.servingInstances(); n != replicas {
				h.dumpState("after the roll")
				t.Fatalf("%d Instances serve after the roll, want %d", n, replicas)
			}
			if !h.sawEvent(workloadtypes.EventReasonInPlaceUpdateNotPossible) {
				t.Fatalf("the fallback to recreate must be announced: %v", h.events)
			}
		})
	}
}

// The rows the Create pass does not own. A Failed row that ran a revision,
// and an attempt parked after its disposition while any pod of its set
// stands, belong to the roll and its ladder: the Create pass neither
// promotes nor refills them. The one Failed row it adopts was never
// promoted, holds every pod at its active ordinal on the target, and its
// target's ladder admits.

// parkedRetryLadder paces the parked stories: a first backoff long enough
// that the parked attempt is observed over several passes before the
// ladder admits a fresh one.
var parkedRetryLadder = &workloadtypes.RetryPolicy{MaxAttempts: 3, InitialDelay: 10 * time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}

// attemptParkedAt reports whether Instance 0 carries an attempt parked
// after its disposition, pinned to rev.
func (h *recoveryHarness) attemptParkedAt(rev *appsv1.ControllerRevision) bool {
	row := h.instance(0)
	return row != nil && workloadtypes.OperationParked(row.Operation) && row.Operation.TargetRevision == rev.Name
}

// podsOfOnImage is the live pods of Instance idx running image.
func (h *recoveryHarness) podsOfOnImage(idx int32, image string) []*corev1.Pod {
	var out []*corev1.Pod
	for _, pod := range h.podsOf(idx) {
		if pod.Spec.Containers[0].Image == image {
			out = append(out, pod)
		}
	}
	return out
}

// newParkedRecreateHarness is one Instance, a single pod or a
// leader+worker gang, serving v1 under RecreatePod with restartPolicy
// None, pushed onto the unpullable revision until its attempt is parked:
// the drained set was rebuilt at the pushed revision, every member sits
// in ImagePullBackOff, the stuck-pod grace disposed the attempt, and the
// row keeps it parked, reading Failed, with the pushed revision's block in
// Backoff. prepare runs before the initial create, for a story that needs
// the node model or a termination grace.
func newParkedRecreateHarness(t *testing.T, multiPod bool, prepare func(h *recoveryHarness)) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, multiPod)
	h.recorder = record.NewFakeRecorder(256)
	none := workloadtypes.RestartPolicyNone
	h.lifecycle.RestartPolicy = &none
	h.retryPolicy = parkedRetryLadder
	h.stepCap = recoveryStepFloor
	if prepare != nil {
		prepare(h)
	}
	h.useRollingBudget(workloadtypes.UpdateStrategyRecreatePod)
	h.driveToReadyOnV1()
	h.setTarget(h.revBad, badImage)
	if !h.run(40, func() bool { return h.attemptParkedAt(h.revBad) }) {
		h.dumpState("push")
		t.Fatalf("the recreate onto the unpullable revision never parked its attempt")
	}
	row := h.instance(0)
	if row.Phase != workloadtypes.InstancePhaseFailed || row.RunningRevision != h.revV1.Name {
		h.dumpState("parked")
		t.Fatalf("a parked attempt whose set is wedged reads Failed on the running revision, got %+v", *row)
	}
	if block := h.findBlock(h.revBad.Name); block == nil || block.State != workloadtypes.RetryBlockBackoff {
		h.dumpState("parked")
		t.Fatalf("the disposition counts the wave on the pushed revision's ladder, got %+v", block)
	}
	if pods := h.podsOf(0); len(pods) != len(h.desired.Runners) || len(h.podsOfOnImage(0, badImage)) != len(pods) {
		h.dumpState("parked")
		t.Fatalf("the parked set is the whole pod set at the pushed revision, got %d pod(s), %d on the pushed image", len(pods), len(h.podsOfOnImage(0, badImage)))
	}
	if len(h.servingPods()) != 0 {
		t.Fatalf("a set parked before its promotion carries no serving gate")
	}
	h.events = nil
	return h
}

// requireParkedRowLeftAlone is the per-step invariant of the parked
// stories: the row keeps the parked attempt and reads Failed, no Create
// attempt opens over it, no pod of the Instance runs the running
// revision, and the Create pass promotes nothing.
func (h *recoveryHarness) requireParkedRowLeftAlone() {
	h.t.Helper()
	row := h.instance(0)
	if row == nil || !h.attemptParkedAt(h.revBad) || row.Phase != workloadtypes.InstancePhaseFailed {
		h.dumpState("parked row taken")
		h.t.Fatalf("the parked attempt is the roll's while a pod of its set stands; the row moved: %+v", row)
	}
	if workloadtypes.StateOf(row) == workloadtypes.StateCreateCreatePods {
		h.t.Fatalf("a Create attempt opened over the parked one: %+v", *row)
	}
	if pods := h.podsOfOnImage(0, goodImage); len(pods) != 0 {
		h.dumpState("mixed revisions")
		h.t.Fatalf("%s was rebuilt at the running revision beside the parked set's members on the pushed one", pods[0].Name)
	}
	if h.sawEvent(workloadtypes.EventReasonInstanceReady) {
		h.t.Fatalf("the Create pass promoted the parked row: %v", h.events)
	}
	if len(h.servingPods()) != 0 {
		h.t.Fatalf("a pod of the parked set was marked serving outside any attempt's promote")
	}
}

// requireFreshAttemptLands heals the pushed image and lets the ladder
// admit a fresh attempt: the Instance settles on the pushed revision
// through pods that fresh attempt made, none of the parked set among
// them, and the Create pass never promoted the row on the way.
func (h *recoveryHarness) requireFreshAttemptLands(parkedSet map[string]bool) {
	h.t.Helper()
	h.badImageHeals = true
	if !h.run(80, func() bool { return h.settledOn(h.revBad, 1) }) {
		h.dumpState("fresh attempt")
		h.t.Fatalf("the ladder's fresh attempt never settled the Instance on the pushed revision")
	}
	for _, pod := range h.podsOf(0) {
		if parkedSet[wedgeKey(pod)] {
			h.t.Fatalf("%s of the parked set serves the promoted Instance; a parked set is replaced by the attempt that promotes", pod.Name)
		}
	}
	if h.sawEvent(workloadtypes.EventReasonInstanceReady) {
		h.t.Fatalf("the Create pass promoted the row; only the fresh attempt's promote may: %v", h.events)
	}
	if !h.sawEvent(workloadtypes.EventReasonRecreateUpdateCompleted) {
		h.t.Fatalf("the fresh recreate attempt never reported its promote: %v", h.events)
	}
}

// TestParkedRecreate_HealedSetIsNotPromotedByTheCreatePass: the parked
// set's image becomes pullable and every member reports ContainersReady
// on the pushed revision. A set parked before its promotion carries no
// serving gate and is promoted only by a later attempt: the row keeps the
// parked attempt and reads Failed, no member is marked serving, the
// ladder keeps its count, and the fresh attempt the ladder admits
// replaces the set and promotes.
func TestParkedRecreate_HealedSetIsNotPromotedByTheCreatePass(t *testing.T) {
	for _, shape := range []struct {
		name     string
		multiPod bool
	}{
		{name: "single pod"},
		{name: "gang", multiPod: true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			h := newParkedRecreateHarness(t, shape.multiPod, nil)
			parkedSet := podIdentities(h.podsOf(0))
			block := *h.findBlock(h.revBad.Name)
			h.badImageHeals = true
			healed := false
			h.runWithInvariant(6, func() bool { return false }, func() {
				t.Helper()
				h.requireParkedRowLeftAlone()
				if b := h.findBlock(h.revBad.Name); b == nil || b.State != block.State || b.AttemptsStarted != block.AttemptsStarted {
					t.Fatalf("the pushed revision's ladder moved without an attempt: %+v -> %+v", block, b)
				}
				ready := 0
				for _, pod := range h.podsOf(0) {
					if podreadiness.IsContainersReady(pod) {
						ready++
					}
				}
				if ready == len(h.desired.Runners) {
					healed = true
				}
			})
			if !healed {
				t.Fatalf("the healed set never reported ContainersReady in full; the story observed nothing")
			}
			h.requireFreshAttemptLands(parkedSet)
		})
	}
}

// TestRollback_AtTheLastStepOfAMetadataRollOverADarkInstance_Lands: the
// metadata push above, rolled back once every Instance but one has landed
// it. The dark Instance was replaced first, so the roll reaches that step
// instead of stalling on it; the rollback settles every Instance on the
// starting revision with no pod of the new revision left and the capacity
// floor held. With the readiness cause lifted by the replacement the
// Instance serves on the start; with the cause outliving every pod of the
// Instance, each fresh set serves and goes dark again, and the row ends
// Ready with its serving count below its pod count, held behind by nothing.
func TestRollback_AtTheLastStepOfAMetadataRollOverADarkInstance_Lands(t *testing.T) {
	const replicas = 4
	for _, cause := range []struct {
		name string
		kept bool
	}{{"cause lifted with the pod", false}, {"cause kept", true}} {
		t.Run(cause.name, func(t *testing.T) {
			h, victim := unreadyVictimHarness(t, workloadtypes.RestartPolicyNone, workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 1, replicas)
			h.clk.Step(h.stuckGrace)
			h.events = nil
			passAt := h.clk.Now()
			invariant := func() {
				readAt := passAt
				passAt = h.clk.Now()
				if cause.kept {
					h.keepReadinessCause(victim)
				}
				if h.repairInFlight() || h.sawEvent(workloadtypes.EventReasonRestartTriggered) {
					h.dumpState("repair")
					t.Fatalf("a repair opened on the dark Instance: %v", h.events)
				}
				if n := h.servingInstances(); n < replicas-2 {
					h.dumpState("floor")
					t.Fatalf("%d Instances in rotation, floor %d", n, replicas-2)
				}
				if holdNamesInstance(h.verdict, victim) && !h.unreadyInsideGraceAt(victim, readAt) {
					h.dumpState("held on the dark Instance")
					t.Fatalf("the roll is held on the dark Instance %d: %q", victim, h.verdict.Reason)
				}
			}
			rev := h.pushMetadataRevision("two")
			if !h.runWithInvariant(120, func() bool { return int32(len(h.landedOn(rev))) >= replicas-1 }, invariant) {
				h.dumpState("roll")
				t.Fatalf("the roll never reached its last step; last hold %+v", h.lastHold())
			}

			h.setTarget(h.revV1, goodImage)
			landed := func() bool {
				if !h.settledOn(h.revV1, replicas) {
					return false
				}
				row := h.instance(victim)
				if cause.kept {
					return row != nil && row.ServingPodCount < row.PodCount && row.ReadyPodCount < row.PodCount
				}
				return h.servingInstances() == replicas
			}
			if !h.runWithInvariant(120, landed, invariant) {
				h.dumpState("after the rollback")
				t.Fatalf("the rollback did not settle on %s; victim = %+v", h.revV1.Name, h.instance(victim))
			}
			if h.livePodOnRevision(rev) {
				t.Fatalf("a pod of the rolled-back revision is still alive after the rollback settled")
			}
			if cause.kept && h.instanceServes(victim) {
				t.Fatalf("instance %d serves while its cause stays", victim)
			}
		})
	}
}

// A gang source stamped Failed at its drain step is past the point of no
// return: it has left rotation behind a replacement gang that is in
// rotation, so the replacement is the Instance's only serving set. The
// stories below pin that an operator edit reaching the row there never
// takes that set down: the cycle finishes on its pin and the edit takes
// effect from the promoted replacement.

// failedGangSurgeAtDrainStep drives a gang Component through a push whose
// replacement clears the promote bar and takes the source out of rotation,
// then fails a replacement member's readiness until the operation deadline
// stamps the source Failed at its drain step with the operation kept. It
// returns the harness, the replacement's index and the replacement's pods.
func failedGangSurgeAtDrainStep(t *testing.T) (*recoveryHarness, int32, []*corev1.Pod) {
	t.Helper()
	h := newRecoveryHarness(t, true)
	h.atomicStatus = true
	h.routed = true
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	h.driveToReadyOnV1()
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 4 * time.Minute}
	h.setTarget(h.revFixed, fixedImage)

	atDrainStep := func() bool {
		s := h.instance(0)
		return s != nil && s.Operation != nil && s.Operation.SurgeIndex != nil &&
			s.Operation.Step == workloadtypes.UpdateStepSurgeDrain
	}
	if !h.run(20, atDrainStep) {
		h.dumpState("surge never reached the drain step")
		t.Fatalf("the gang surge never took the source out of rotation")
	}
	surgeIdx := *h.instance(0).Operation.SurgeIndex
	replacement := h.podsOf(surgeIdx)
	if len(replacement) != 2 {
		h.dumpState("replacement gang")
		t.Fatalf("replacement gang: got %d pods, want the leader and the worker", len(replacement))
	}
	for _, pod := range replacement {
		if !podreadiness.IsServing(pod) {
			t.Fatalf("replacement pod %s is not in rotation at the drain step", pod.Name)
		}
	}

	h.failReadiness(surgeIdx, "worker")
	failedAtDrainStep := func() bool {
		s := h.instance(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseFailed && s.Operation != nil &&
			s.Operation.SurgeIndex != nil && s.Operation.Step == workloadtypes.UpdateStepSurgeDrain
	}
	if !h.run(20, failedAtDrainStep) {
		h.dumpState("deadline never stamped the source Failed")
		t.Fatalf("the operation deadline never stamped the drain-step source Failed")
	}
	return h, surgeIdx, replacement
}

// gangHandoffKept is the invariant both stories hold from the edit to the
// end of the handoff: every replacement pod is the same object, alive and
// in rotation; the replacement's row is never handed to the delete wave;
// and the source row, while it exists, still carries its surge continuation
// at the drain step rather than a recreate.
func (h *recoveryHarness) gangHandoffKept(surgeIdx int32, replacement []*corev1.Pod) func() {
	return func() {
		h.t.Helper()
		live := map[types.UID]*corev1.Pod{}
		for _, pod := range h.podsOf(surgeIdx) {
			live[pod.UID] = pod
		}
		for _, want := range replacement {
			pod, ok := live[want.UID]
			if !ok {
				h.dumpState("replacement pod gone")
				h.t.Fatalf("replacement pod %s was deleted behind a drained source", want.Name)
			}
			if !podreadiness.IsServing(pod) {
				h.dumpState("replacement pod unrouted")
				h.t.Fatalf("replacement pod %s left rotation behind a drained source", want.Name)
			}
		}
		if s := h.instance(surgeIdx); s != nil && s.Operation != nil && s.Operation.Type == workloadtypes.InstanceOperationDelete {
			h.dumpState("replacement handed to the delete wave")
			h.t.Fatalf("the replacement's row was handed to the delete wave while it was the only serving set")
		}
		if s := h.instance(0); s != nil && (s.Incarnation != 1 || s.Operation == nil || s.Operation.SurgeIndex == nil ||
			s.Operation.Step == workloadtypes.UpdateStepDrain) {
			h.dumpState("source left the surge machine")
			h.t.Fatalf("the drain-step source left the surge machine: %+v", s)
		}
	}
}

// gangHandedOver reports the end of the handoff: the source row is gone and
// the replacement is Ready on the pinned revision with no operation.
func (h *recoveryHarness) gangHandedOver(surgeIdx int32, pinned string) func() bool {
	return func() bool {
		if h.instance(0) != nil {
			return false
		}
		s := h.instance(surgeIdx)
		return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.RunningRevision == pinned && s.Operation == nil
	}
}

// The roll is pinned back onto the running revision while a gang source
// sits Failed at its drain step. Zero revision distance reaches the row
// through no trigger of its own, and past the Surge step the cycle finishes
// on its pin: the replacement keeps serving through the rollback, is
// promoted once the drained source is gone, and the rolled-back target then
// re-enters with a fresh surge from the promoted gang.
func TestFailedGangSurgeAtDrainStep_RollbackKeepsTheReplacementAndFinishesTheHandoff(t *testing.T) {
	h, surgeIdx, replacement := failedGangSurgeAtDrainStep(t)
	kept := h.gangHandoffKept(surgeIdx, replacement)

	h.setTarget(h.revV1, goodImage)
	h.step()
	kept()

	h.restoreReadiness(surgeIdx, "worker")
	if !h.runWithInvariant(40, h.gangHandedOver(surgeIdx, h.revFixed.Name), kept) {
		h.dumpState("handoff never finished")
		t.Fatalf("the handoff never finished on the pinned revision after the rollback")
	}
	if !h.run(60, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("rollback never re-entered")
		t.Fatalf("the rolled-back target never re-entered from the promoted replacement")
	}
}

// The strategy is switched to RecreatePod while a gang source sits Failed
// at its drain step. The edit pins nothing on an attempt past its point of
// no return: no recreate opens on the drained source, the replacement keeps
// serving, the handoff finishes under the pinned strategy, and the failed
// attempt charges no ladder; the edit reaches the Instance at its next
// attempt.
func TestFailedGangSurgeAtDrainStep_StrategyEditKeepsTheReplacementAndFinishesTheHandoff(t *testing.T) {
	h, surgeIdx, replacement := failedGangSurgeAtDrainStep(t)
	kept := h.gangHandoffKept(surgeIdx, replacement)

	h.useStrategy(workloadtypes.UpdateStrategyRecreatePod)
	h.step()
	kept()

	h.restoreReadiness(surgeIdx, "worker")
	if !h.runWithInvariant(40, h.gangHandedOver(surgeIdx, h.revFixed.Name), kept) {
		h.dumpState("handoff never finished")
		t.Fatalf("the handoff never finished under the pinned strategy after the edit")
	}
	h.settle(3)
	if !h.converged(h.revFixed.Name) {
		h.dumpState("not settled on the pinned revision")
		t.Fatalf("the Instance did not settle on the pinned revision after the handoff")
	}
	if b := h.findBlock(h.revFixed.Name); b != nil {
		t.Fatalf("the finished handoff charged the revision's ladder: %+v", *b)
	}
}

// TestParkedRecreate_LostGangMemberIsNotRefilledBesideTheParkedSet: a
// parked gang loses its worker. The gang is the roll's to replace whole
// at the target when its ladder admits: no member is rebuilt at the
// running revision beside the parked leader on the pushed one, the row
// keeps the parked attempt, and the fresh attempt rebuilds both members
// at the pushed revision.
func TestParkedRecreate_LostGangMemberIsNotRefilledBesideTheParkedSet(t *testing.T) {
	h := newParkedRecreateHarness(t, true, nil)
	parkedSet := podIdentities(h.podsOf(0))
	leader := h.podOf(0, "leader")
	h.loseRunner(0, "worker")
	h.runWithInvariant(6, func() bool { return false }, func() {
		t.Helper()
		h.requireParkedRowLeftAlone()
		pods := h.podsOf(0)
		if len(pods) != 1 || pods[0].UID != leader.UID {
			h.dumpState("short gang")
			t.Fatalf("the parked leader alone holds the index until the roll's next attempt, got %d pod(s)", len(pods))
		}
	})
	h.requireFreshAttemptLands(parkedSet)
	for _, pod := range h.podsOf(0) {
		if pod.Labels[query.LabelRevisionHash] != h.revBad.Labels[query.LabelRevisionHash] && !query.RevisionFromPod(pod).Same(query.RevisionOf(h.revBad)) {
			t.Fatalf("%s is not on the pushed revision after the fresh attempt", pod.Name)
		}
	}
}

// TestParkedRecreate_ForceDeletedMemberIsNotRecreatedInTheSweepsPass: a
// parked gang's worker is deleted by another hand on a node whose kubelet
// has stopped, so it wedges Terminating with no finalizer; the configured
// force-delete policy frees its name. The sweep acts on the pod, not on
// the row: the pass that force-deletes the worker rebuilds nothing, the
// row keeps the parked attempt, and the fresh attempt the ladder admits
// rebuilds the gang at the pushed revision.
func TestParkedRecreate_ForceDeletedMemberIsNotRecreatedInTheSweepsPass(t *testing.T) {
	h := newParkedRecreateHarness(t, true, func(h *recoveryHarness) {
		h.useNodes("node-a", "node-b")
		h.podGrace = recoveryStepFloor
		h.forceDelete = &workloadtypes.ForceDeletePolicy{OverdueSlack: recoveryStepFloor, NodeUnreachableThreshold: recoveryStepFloor}
	})
	parkedSet := podIdentities(h.podsOf(0))
	worker := h.podOf(0, "worker")
	leader := h.podOf(0, "leader")
	if worker.Spec.NodeName == "" || worker.Spec.NodeName == leader.Spec.NodeName {
		t.Fatalf("story shape: the worker needs a node of its own, got worker=%q leader=%q", worker.Spec.NodeName, leader.Spec.NodeName)
	}
	h.failNode(worker.Spec.NodeName)
	if err := h.c.Delete(h.ctx, worker); err != nil {
		t.Fatalf("delete worker: %v", err)
	}
	swept := false
	for i := 0; i < 10 && !swept; i++ {
		h.step()
		h.requireParkedRowLeftAlone()
		if !h.sawEvent(workloadtypes.EventReasonPodForceDeleted) {
			if !h.terminatingPod(worker) {
				t.Fatalf("the worker left the API before the policy acted")
			}
			continue
		}
		swept = true
		if h.terminatingPod(worker) {
			t.Fatalf("the sweep reported a force-delete but the worker still stands")
		}
		for _, pod := range h.podsOf(0) {
			if pod.Labels[query.LabelRunner] == "worker" {
				h.dumpState("refilled in the sweep's pass")
				t.Fatalf("the pass that freed the worker's name rebuilt %s under the parked attempt", pod.Name)
			}
		}
	}
	if !swept {
		h.dumpState("sweep")
		t.Fatalf("the force-delete policy never freed the wedged worker")
	}
	h.runWithInvariant(6, func() bool { return false }, func() {
		t.Helper()
		h.requireParkedRowLeftAlone()
		for _, pod := range h.podsOf(0) {
			if pod.Labels[query.LabelRunner] == "worker" {
				t.Fatalf("the freed worker name was refilled under the parked attempt by %s", pod.Name)
			}
		}
	})
	h.requireFreshAttemptLands(parkedSet)
}

// TestParkedRecreate_TerminalMemberDoesNotOpenACreateAttempt: the parked
// gang's leader is evicted and sits in phase Failed while the worker
// stands. A dead member of a parked set is not the Create pass's to
// recycle while a live member remains: no Create attempt opens, nothing
// is rebuilt at the running revision, and the fresh attempt the ladder
// admits drains the dead leader with the rest and rebuilds the gang.
func TestParkedRecreate_TerminalMemberDoesNotOpenACreateAttempt(t *testing.T) {
	h := newParkedRecreateHarness(t, true, nil)
	parkedSet := podIdentities(h.podsOf(0))
	leader := h.podOf(0, "leader")
	if h.evicted == nil {
		h.evicted = map[string]bool{}
	}
	h.evicted[wedgeKey(leader)] = true
	h.reportEvicted(leader)
	h.runWithInvariant(6, func() bool { return false }, func() {
		t.Helper()
		h.requireParkedRowLeftAlone()
		if h.sawEvent(workloadtypes.EventReasonTerminalPodRecycled) {
			t.Fatalf("the dead leader was recycled under a Create attempt: %v", h.events)
		}
		dead := false
		for _, pod := range h.podsOf(0) {
			if pod.UID == leader.UID && pod.Status.Phase == corev1.PodFailed {
				dead = true
			}
		}
		if !dead {
			h.dumpState("dead leader")
			t.Fatalf("the dead leader was removed outside any attempt")
		}
	})
	h.requireFreshAttemptLands(parkedSet)
}

// newHeldSurgeHarness is one single-pod Instance serving v1 whose surge
// onto the unpullable revision exhausted its ladder: the revision is Held,
// the row is Failed with no operation on the running revision, the source
// serves alone, and the last wedged replacement still stands beside it.
func newHeldSurgeHarness(t *testing.T) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	h.recorder = record.NewFakeRecorder(256)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	row := h.instance(0)
	if row == nil || row.Phase != workloadtypes.InstancePhaseFailed || row.Operation != nil || row.RunningRevision != h.revV1.Name {
		h.dumpState("held")
		t.Fatalf("a Held single-pod surge leaves its source Failed with no operation on the running revision, got %+v", row)
	}
	if serving := h.servingPods(); len(serving) != 1 || serving[0].Spec.Containers[0].Image != goodImage {
		h.dumpState("held")
		t.Fatalf("the source alone serves at Held, got %d serving pod(s)", len(serving))
	}
	if bad := h.badPods(); len(bad) != 1 {
		h.dumpState("held")
		t.Fatalf("the last wedged replacement stands beside the source at Held, got %d", len(bad))
	}
	h.events = nil
	return h
}

// requireFailedSourceUntouched is the per-step invariant of the Held
// surge stories: the row stays Failed with no operation on the running
// revision at its ordinal, the source alone serves, no replacement
// carries the serving gate, the hold stands and nothing promotes.
func (h *recoveryHarness) requireFailedSourceUntouched(source *corev1.Pod) {
	h.t.Helper()
	row := h.instance(0)
	if row == nil || row.Phase != workloadtypes.InstancePhaseFailed || row.Operation != nil ||
		row.RunningRevision != h.revV1.Name || row.ActiveOrdinal != 0 {
		h.dumpState("source adopted")
		h.t.Fatalf("a Failed row that ran a revision is never adopted; the row moved: %+v", row)
	}
	serving := h.servingPods()
	if len(serving) != 1 || serving[0].UID != source.UID {
		h.dumpState("rotation")
		h.t.Fatalf("the source alone serves through the hold, got %d serving pod(s)", len(serving))
	}
	for _, pod := range h.badPods() {
		if podreadiness.IsServing(pod) {
			h.t.Fatalf("the replacement %s was marked serving outside the surge", pod.Name)
		}
	}
	if !h.heldOn(h.revBad.Name) {
		h.t.Fatalf("the Held block on %s was dropped", h.revBad.Name)
	}
	if h.sawEvent(workloadtypes.EventReasonInstanceReady) {
		h.t.Fatalf("the Create pass promoted the Failed source: %v", h.events)
	}
}

// TestHeldSurge_LateReadyReplacementIsNotAdoptedByTheCreatePass: the
// registry comes back and the wedged replacement reports ContainersReady
// beside the Failed source. A replacement becoming runtime-ready does not
// rescue a Failed source: the row stays Failed on the running revision,
// the source alone serves, and the replacement is never marked serving.
func TestHeldSurge_LateReadyReplacementIsNotAdoptedByTheCreatePass(t *testing.T) {
	h := newHeldSurgeHarness(t)
	source := h.servingPods()[0]
	h.badImageHeals = true
	healed := false
	h.runWithInvariant(8, func() bool { return false }, func() {
		t.Helper()
		h.requireFailedSourceUntouched(source)
		for _, pod := range h.badPods() {
			if podreadiness.IsContainersReady(pod) {
				healed = true
			}
		}
	})
	if !healed {
		t.Fatalf("the healed replacement never reported ContainersReady; the story observed nothing")
	}
}

// TestHeldSurge_SourceLeftAloneStaysFailedWhileTheHoldStands: the wedged
// replacement is deleted by another hand and is gone, leaving the Failed
// source as the Instance's only pod. The source alone serving does not
// return the row to Ready: it stays Failed with no operation on the
// running revision while the Held block stands, and only the roll may
// move it from there.
func TestHeldSurge_SourceLeftAloneStaysFailedWhileTheHoldStands(t *testing.T) {
	h := newHeldSurgeHarness(t)
	source := h.servingPods()[0]
	replacement := h.badPods()[0]
	if err := h.c.Delete(h.ctx, replacement); err != nil {
		t.Fatalf("delete replacement: %v", err)
	}
	h.runWithInvariant(8, func() bool { return false }, func() {
		t.Helper()
		h.requireFailedSourceUntouched(source)
		if pods := h.podsOf(0); len(pods) != 1 || pods[0].UID != source.UID {
			h.dumpState("source alone")
			t.Fatalf("the source is the Instance's only pod, got %d", len(pods))
		}
	})
}

// TestFailedSurgeSource_RolledBackReturnsToReadyWhileItsWreckageTerminates:
// a single-pod surge onto an image that never pulls parks the Instance at
// Failed with its wedged replacement standing; the replacement's node dies
// and the template is rolled back to the running revision, so the wreckage
// cleanup drains and deletes the replacement, which its dead kubelet never
// finishes. The Instance's own pod serves the roll target throughout, so
// the row returns to Ready on its running revision with no operation while
// the drained pod is still in the API: the promote reads the pods at the
// active ordinal and leaves the terminating alien to the sweep, and nothing
// touches the serving pod or removes the drained one before the policy
// boundary.
func TestFailedSurgeSource_RolledBackReturnsToReadyWhileItsWreckageTerminates(t *testing.T) {
	story := abandonedReplacementStory{name: "single pod, the node dies after the drain"}
	policy := &workloadtypes.ForceDeletePolicy{NodeUnreachableThreshold: abandonedPodThreshold, OverdueSlack: abandonedPodSlack}
	h := newAbandonedReplacementHarness(t, story, policy)
	serving := podIdentities(h.podsOnImage(goodImage))
	victim, boundary := h.strandAbandonedReplacement(story, policy)
	idx, ok := query.InstanceIdxFromLabels(victim)
	if !ok {
		t.Fatalf("the drained pod %s carries no instance index", victim.Name)
	}
	// Passes with the clock still: no deadline is owed for the row to come
	// back, and the drained pod stays short of the policy boundary.
	returned := false
	for i := 0; i < 6 && !returned; i++ {
		if _, err := h.pass(); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		row := h.instance(idx)
		returned = row != nil && row.Phase == workloadtypes.InstancePhaseReady && row.Operation == nil && row.RunningRevision == h.revV1.Name
	}
	if h.clk.Now().After(boundary) {
		t.Fatalf("story shape: the clock crossed the policy boundary %v while the row was read", boundary.Format(time.TimeOnly))
	}
	if !h.terminatingPod(victim) || h.sawEvent(workloadtypes.EventReasonPodForceDeleted) {
		h.dumpState("drained pod")
		t.Fatalf("the drained pod %s must still be Terminating in the API before the policy boundary", victim.Name)
	}
	if !returned {
		h.dumpState("rolled back")
		t.Fatalf("the Instance never returned to Ready on its serving pod while its drained replacement stood: %+v", h.instance(idx))
	}
	h.requireServingPodsKept(serving)
}

// The escalation judges an attempt on the observation the pass opened
// with; a promote the same pass lands from a fresher read wins, and the
// deadline stamp leaves the promoted row alone.
func TestDeadlineOnThePromotePass_InPlaceRowStaysReady(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.retryPolicy = nil
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 30 * time.Minute}
	h.useStrategy(workloadtypes.UpdateStrategyInPlaceIfPossible)
	h.driveToReadyOnV1()

	h.setTarget(h.revFixed, fixedImage)
	// The patched container is back up and the roll has restored the gate;
	// the kubelet has not folded it into PodReady yet, so nothing promoted.
	restored := func() bool {
		row, pods := h.instance(0), h.podsOf(0)
		return row != nil && row.Operation != nil && row.Operation.Step == workloadtypes.UpdateStepInPlace &&
			len(pods) == 1 && pods[0].Spec.Containers[0].Image == fixedImage &&
			podreadiness.IsContainersReady(pods[0]) && podreadiness.IsServing(pods[0]) && !podreadiness.IsPodReady(pods[0])
	}
	if !h.run(40, restored) {
		h.dumpState("in-place gate restored")
		t.Fatalf("the in-place roll never restored the serving gate on the patched pod")
	}

	h.clk.Step(31 * time.Minute)
	h.cacheLagOnce = true
	h.step()

	row := h.instance(0)
	if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil || row.RunningRevision != h.revFixed.Name {
		h.dumpState("deadline on the promote pass")
		t.Fatalf("row = %+v, want Ready on %s with no operation: the promote landed before the deadline stamp", row, h.revFixed.Name)
	}
	if len(h.failedWarnings) != 0 {
		t.Fatalf("InstanceFailed warnings = %v, want none for a row the pass promoted", h.failedWarnings)
	}
}

func TestDeadlineOnThePromotePass_SinglePodSurgeRowStaysReady(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.routed = true
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 30 * time.Minute}
	h.useStrategy(workloadtypes.UpdateStrategySurgeThenDrain)
	h.driveToReadyOnV1()

	h.setTarget(h.revFixed, fixedImage)
	// The drained source is gone and the replacement is the only pod left;
	// the next pass would promote it.
	sourceGone := func() bool {
		row, pods := h.instance(0), h.podsOf(0)
		return row != nil && row.Operation != nil && row.Operation.Step == workloadtypes.UpdateStepSurgeDrain &&
			len(pods) == 1 && pods[0].Labels[query.LabelPodOrdinal] == "1"
	}
	if !h.run(40, sourceGone) {
		h.dumpState("surge drain")
		t.Fatalf("the surge never deleted its drained source")
	}
	// The replacement is lost first, so the drain step rebuilds it and the
	// rebuilt pod reaches ContainersReady on the pass the deadline elapses.
	if err := h.c.Delete(h.ctx, h.podsOf(0)[0]); err != nil {
		t.Fatalf("lose the replacement: %v", err)
	}
	h.step()
	if pods := h.podsOf(0); len(pods) != 1 || podreadiness.IsServing(pods[0]) {
		h.dumpState("replacement rebuilt")
		t.Fatalf("pods = %d, want the one rebuilt replacement not yet serving", len(pods))
	}

	h.clk.Step(31 * time.Minute)
	h.step()

	row := h.instance(0)
	if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil || row.RunningRevision != h.revFixed.Name {
		h.dumpState("deadline on the promote pass")
		t.Fatalf("row = %+v, want Ready on %s with no operation: the promote landed before the deadline stamp", row, h.revFixed.Name)
	}
	if len(h.failedWarnings) != 0 {
		t.Fatalf("InstanceFailed warnings = %v, want none for a row the pass promoted", h.failedWarnings)
	}
	if b := h.findBlock(h.revFixed.Name); b != nil {
		t.Fatalf("RetryBlock for %s = %+v, want none: an attempt that won its promote counts no wave on its revision's ladder", h.revFixed.Name, b)
	}
}

func TestDeadlineOnThePromotePass_CreateRowStaysReady(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 30 * time.Minute}
	h.setTarget(h.revV1, goodImage)
	// The create pass has written the serving gate on the ContainersReady
	// pod; the kubelet has not folded it into PodReady yet.
	gated := func() bool {
		row, pods := h.instance(0), h.podsOf(0)
		return row != nil && row.Phase == workloadtypes.InstancePhaseCreating && len(pods) == 1 &&
			podreadiness.IsServing(pods[0]) && !podreadiness.IsPodReady(pods[0])
	}
	if !h.run(10, gated) {
		h.dumpState("create gate written")
		t.Fatalf("the create pass never wrote the serving gate")
	}

	h.clk.Step(31 * time.Minute)
	h.cacheLagOnce = true
	h.step()

	row := h.instance(0)
	if row == nil || row.Phase != workloadtypes.InstancePhaseReady || row.Operation != nil || row.RunningRevision != h.revV1.Name {
		h.dumpState("deadline on the promote pass")
		t.Fatalf("row = %+v, want Ready on %s with no operation: the promote landed before the deadline stamp", row, h.revV1.Name)
	}
	if len(h.failedWarnings) != 0 {
		t.Fatalf("InstanceFailed warnings = %v, want none for a row the pass promoted", h.failedWarnings)
	}
	if b := h.findBlock(h.revV1.Name); b != nil {
		t.Fatalf("RetryBlock for %s = %+v, want none: an attempt that won its promote counts no wave on its revision's ladder", h.revV1.Name, b)
	}
}

// A surge holds its source out of rotation under its own writer for the
// endpoint-convergence window before deleting it; a deadline that elapses
// inside that window, with the replacement Ready and routed, finds the
// Instance serving and fails nothing.
func TestDeadlineInsideTheSurgeDrainWindow_DoesNotFailTheSurge(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.routed = true
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 30 * time.Minute}
	h.useStrategy(workloadtypes.UpdateStrategySurgeThenDrain)
	h.driveToReadyOnV1()

	h.setTarget(h.revFixed, fixedImage)
	// The step has advanced: the replacement serves and the source's gate
	// is off under the surge's writer, with the source still routed.
	draining := func() bool {
		row := h.instance(0)
		if row == nil || row.Operation == nil || row.Operation.Step != workloadtypes.UpdateStepSurgeDrain {
			return false
		}
		for _, pod := range h.podsOf(0) {
			if pod.Labels[query.LabelPodOrdinal] == "0" && podreadiness.HeldNotServing(pod) {
				return true
			}
		}
		return false
	}
	if !h.run(40, draining) {
		h.dumpState("surge drain")
		t.Fatalf("the surge never reached its drain step")
	}

	h.clk.Step(31 * time.Minute)
	h.step()

	row := h.instance(0)
	if row == nil || row.Phase == workloadtypes.InstancePhaseFailed || row.LastFailure != nil {
		h.dumpState("deadline inside the drain window")
		t.Fatalf("row = %+v, want the surge still in flight or promoted, not Failed", row)
	}
	if len(h.failedWarnings) != 0 {
		t.Fatalf("InstanceFailed warnings = %v, want none for a serving Instance", h.failedWarnings)
	}
	if !h.run(20, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("after the drain window")
		t.Fatalf("the surge did not complete on %s after the deadline elapsed inside its drain window", h.revFixed.Name)
	}
}

// After the hand-over of a gang surge, with the replacement in rotation and
// the source out of it, the replacement is the Instance's only serving set.
// The stories below pin what happens when that set dies there: it is
// rebuilt under a fresh deadline, every rebuild counts on the pinned
// revision's ladder, and the ladder's limit fails the Instance and holds.

// gangReplacementLostReason is the failure a gang source records when its
// replacement dies after the hand-over.
const gangReplacementLostReason = "ReplacementLost"

// gangSurgeHandedOver drives a gang Component through a push to the first
// pass past the hand-over: the replacement gang is complete and in rotation
// and the source has left the serving gate at the drain step. It returns
// the harness and the replacement's index.
func gangSurgeHandedOver(t *testing.T) (*recoveryHarness, int32) {
	t.Helper()
	h := newRecoveryHarness(t, true)
	h.atomicStatus = true
	h.routed = true
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	h.driveToReadyOnV1()
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 4 * time.Minute}
	h.setTarget(h.revFixed, fixedImage)
	handedOver := func() bool {
		s := h.instance(0)
		return s != nil && s.Operation != nil && s.Operation.SurgeIndex != nil &&
			s.Operation.Step == workloadtypes.UpdateStepSurgeDrain
	}
	if !h.run(20, handedOver) {
		h.dumpState("hand-over")
		t.Fatalf("the gang surge never took the source out of rotation")
	}
	surgeIdx := *h.instance(0).Operation.SurgeIndex
	if got := len(h.podsOf(surgeIdx)); got != 2 {
		h.dumpState("replacement gang")
		t.Fatalf("replacement gang: got %d pods, want the leader and the worker", got)
	}
	for _, pod := range h.podsOf(surgeIdx) {
		if !podreadiness.IsServing(pod) {
			t.Fatalf("replacement pod %s is not in rotation after the hand-over", pod.Name)
		}
	}
	for _, pod := range h.podsOf(0) {
		if podreadiness.IsServing(pod) {
			t.Fatalf("source pod %s is still in rotation after the hand-over", pod.Name)
		}
	}
	return h, surgeIdx
}

// requireAttemptClosedByLoss checks the pass after a loss at the drain
// step: the source is Failed with its surge kept, the loss is its recorded
// failure, the pinned revision's ladder counts the attempt, and nothing was
// rebuilt under the attempt that died.
func (h *recoveryHarness) requireAttemptClosedByLoss(surgeIdx int32, survivors int) {
	h.t.Helper()
	s := h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.Operation == nil ||
		s.Operation.SurgeIndex == nil || *s.Operation.SurgeIndex != surgeIdx ||
		s.Operation.Step != workloadtypes.UpdateStepSurgeDrain {
		h.dumpState("attempt not closed")
		h.t.Fatalf("the loss did not close the attempt: source row %+v", s)
	}
	if s.LastFailure == nil || s.LastFailure.Reason != gangReplacementLostReason {
		h.t.Fatalf("the loss is not the source's recorded failure: %+v", s.LastFailure)
	}
	b := h.findBlock(h.revFixed.Name)
	if b == nil || b.State != workloadtypes.RetryBlockBackoff || b.AttemptsStarted != 1 {
		h.dumpState("ladder")
		h.t.Fatalf("the lost attempt was not counted on the pinned revision's ladder: %+v", b)
	}
	if got := len(h.podsOf(surgeIdx)); got != survivors {
		h.dumpState("rebuilt under the dead attempt")
		h.t.Fatalf("replacement pods after the loss: got %d, want %d (nothing is rebuilt under the attempt that died)", got, survivors)
	}
}

// rearmedRebuild reports the source re-armed for its next attempt: Updating
// at the drain step with the retry counted on the operation.
func (h *recoveryHarness) rearmedRebuild(retry int32) func() bool {
	return func() bool {
		s := h.instance(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseUpdating && s.Operation != nil &&
			s.Operation.Step == workloadtypes.UpdateStepSurgeDrain && s.Operation.RetryCount == retry
	}
}

// requireFreshWindow checks the re-armed attempt runs under a fresh
// deadline, a full InstanceReadyTimeout from its new start and later than
// the window of the attempt that died, as the ladder's attempt in progress.
func (h *recoveryHarness) requireFreshWindow(before workloadtypes.InstanceOperation) {
	h.t.Helper()
	op := h.instance(0).Operation
	if !op.StartedAt.After(before.StartedAt.Time) || !op.Deadline.After(before.Deadline.Time) {
		h.t.Fatalf("the rebuild kept the dead attempt's window: started %v deadline %v, before started %v deadline %v",
			op.StartedAt, op.Deadline, before.StartedAt, before.Deadline)
	}
	if want := op.StartedAt.Add(4 * time.Minute); !op.Deadline.Time.Equal(want) {
		h.t.Fatalf("the rebuild's deadline is %v, want a full timeout from its start %v", op.Deadline.Time, want)
	}
	b := h.findBlock(h.revFixed.Name)
	if b == nil || b.State != workloadtypes.RetryBlockRetryInProgress || b.AttemptsStarted != 1 {
		h.t.Fatalf("the re-armed attempt is not the ladder's attempt in progress: %+v", b)
	}
}

// The whole replacement gang is deleted after the hand-over, while the
// source is out of rotation behind it. The loss closes the attempt: the
// source is Failed with its surge kept and the pinned revision's ladder
// counts the attempt. Once the ladder admits the next one, the gang is
// rebuilt under a fresh deadline with the retry counted on the operation,
// and the hand-over finishes on the pinned revision.
func TestGangSurgeAfterHandover_ReplacementLostIsRebuiltUnderAFreshDeadlineAndCounted(t *testing.T) {
	h, surgeIdx := gangSurgeHandedOver(t)
	before := *h.instance(0).Operation

	h.losePods(surgeIdx)
	h.step()
	h.requireAttemptClosedByLoss(surgeIdx, 0)

	if !h.run(10, h.rearmedRebuild(1)) {
		h.dumpState("never re-armed")
		t.Fatalf("the ladder's backoff elapsed and the replacement was not rebuilt")
	}
	h.requireFreshWindow(before)
	if got := len(h.podsOf(surgeIdx)); got != 2 {
		h.dumpState("rebuild")
		t.Fatalf("rebuilt replacement: got %d pods, want the leader and the worker", got)
	}
	if !h.run(40, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("handoff never finished")
		t.Fatalf("the rebuilt replacement was never promoted on the pinned revision")
	}
	if b := h.findBlock(h.revFixed.Name); b != nil {
		t.Fatalf("the finished handoff left the ladder standing: %+v", *b)
	}
}

// One member of the replacement gang is deleted after the hand-over. The
// loss closes the attempt as a whole loss does and counts it; the surviving
// member keeps serving; once the ladder admits the next attempt the member
// is rebuilt under a fresh deadline, and the hand-over finishes on the
// pinned revision.
func TestGangSurgeAfterHandover_LostMemberIsRebuiltUnderAFreshDeadlineAndCounted(t *testing.T) {
	h, surgeIdx := gangSurgeHandedOver(t)
	before := *h.instance(0).Operation
	leader := h.podOf(surgeIdx, "leader").UID

	h.loseRunner(surgeIdx, "worker")
	h.step()
	h.requireAttemptClosedByLoss(surgeIdx, 1)
	if pod := h.podOf(surgeIdx, "leader"); pod.UID != leader || !podreadiness.IsServing(pod) {
		t.Fatalf("the surviving leader did not keep serving through the loss")
	}

	if !h.run(10, h.rearmedRebuild(1)) {
		h.dumpState("never re-armed")
		t.Fatalf("the ladder's backoff elapsed and the lost member was not rebuilt")
	}
	h.requireFreshWindow(before)
	pods := h.podsOf(surgeIdx)
	if len(pods) != 2 {
		h.dumpState("rebuild")
		t.Fatalf("rebuilt replacement: got %d pods, want the leader and the worker", len(pods))
	}
	for _, pod := range pods {
		if pod.Labels[query.LabelRunner] == "leader" && pod.UID != leader {
			t.Fatalf("the surviving leader was rebuilt along with the lost member")
		}
	}
	if !h.run(40, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("handoff never finished")
		t.Fatalf("the rebuilt replacement was never promoted on the pinned revision")
	}
}

// The whole replacement gang dies after the hand-over and the loss closes
// the attempt; the operator then re-applies the starting revision while the
// attempt is parked. The pin is withdrawn, so nothing is rebuilt at it when
// the ladder's backoff comes due: the pair ends, the source gang returns to
// rotation on the starting revision with its drain hold lifted, the marker
// is removed, and no pod of the withdrawn revision is created again.
func TestGangSurgeAfterHandover_ReplacementLostThenRollbackEndsThePairOnTheStartingRevision(t *testing.T) {
	h, surgeIdx := gangSurgeHandedOver(t)
	h.losePods(surgeIdx)
	h.step()
	h.requireAttemptClosedByLoss(surgeIdx, 0)
	source := map[types.UID]struct{}{}
	for _, pod := range h.podsOf(0) {
		source[pod.UID] = struct{}{}
	}

	h.setTarget(h.revV1, goodImage)
	noWithdrawnPod := h.noPodOnRevisionInvariant(h.revFixed)
	ended := func() bool {
		s := h.instance(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil &&
			s.RunningRevision == h.revV1.Name && h.instance(surgeIdx) == nil
	}
	if !h.runWithInvariant(10, ended, noWithdrawnPod) {
		h.dumpState("pair never ended")
		t.Fatalf("the rollback did not end the pair whose replacement was lost: %+v", h.instance(0))
	}
	pods := h.podsOf(0)
	if len(pods) != 2 {
		t.Fatalf("source gang after the rollback: got %d pods, want the leader and the worker it kept", len(pods))
	}
	for _, pod := range pods {
		if _, kept := source[pod.UID]; !kept {
			t.Fatalf("source pod %s was rebuilt; the source gang runs the starting revision and keeps its pods", pod.Name)
		}
		if !podreadiness.IsServing(pod) {
			t.Fatalf("source pod %s is not back in rotation after the pair ended", pod.Name)
		}
	}
	// The ladder's backoff elapses with the revision withdrawn: nothing
	// re-arms, and no repair opens on the source.
	h.runWithInvariant(15, func() bool { return false }, noWithdrawnPod)
	if got := len(h.podsOf(surgeIdx)); got != 0 {
		t.Fatalf("replacement index %d holds %d pod(s) after the ladder's clock; a withdrawn revision is not retried", surgeIdx, got)
	}
	if n := h.restartsRecorded(); n != 0 {
		t.Fatalf("restarts recorded = %d, want none: the rollback owns the Instance: %v", n, h.events)
	}
	if !ended() {
		h.dumpState("after the ladder's clock")
		t.Fatalf("the Instance left Ready on the starting revision after the ladder's clock: %+v", h.instance(0))
	}
}

// The drained source is deleted and the whole replacement gang dies before
// the promote; the loss closes the attempt. The operator then re-applies
// the starting revision. The pin is withdrawn and no source pod stands, so
// the pair ends as a fresh start: the source row re-enters Failed with no
// attempt on the revision it ran, which is the roll target again, and the
// Create pass rebuilds the gang there without a repair; no pod of the
// withdrawn revision is created again.
func TestGangSurgeAfterHandover_ReplacementLostWithSourceGoneThenRollbackRebuildsAtTheTarget(t *testing.T) {
	h, surgeIdx := gangSurgeHandedOver(t)
	sourceGone := func() bool { return len(h.podsOf(0)) == 0 && h.instance(0) != nil && h.instance(0).Operation != nil }
	if !h.run(30, sourceGone) {
		h.dumpState("source never drained")
		t.Fatalf("the drained source was never deleted behind the serving replacement")
	}
	h.losePods(surgeIdx)
	h.step()
	h.requireAttemptClosedByLoss(surgeIdx, 0)

	h.setTarget(h.revV1, goodImage)
	noWithdrawnPod := h.noPodOnRevisionInvariant(h.revFixed)
	rebuilt := func() bool {
		s := h.instance(0)
		return s != nil && s.Operation == nil && s.RunningRevision == h.revV1.Name &&
			h.instance(surgeIdx) == nil && len(h.podsOf(0)) == 2
	}
	if !h.runWithInvariant(15, rebuilt, noWithdrawnPod) {
		h.dumpState("never rebuilt at the target")
		t.Fatalf("the source was not rebuilt at the starting revision after the rollback: %+v", h.instance(0))
	}
	h.requirePodsRender(0, h.revV1, goodImage)
	if !h.runWithInvariant(40, func() bool { return h.converged(h.revV1.Name) }, noWithdrawnPod) {
		h.dumpState("never converged")
		t.Fatalf("the rebuilt gang never settled Ready on the starting revision")
	}
	if n := h.restartsRecorded(); n != 0 {
		t.Fatalf("restarts recorded = %d, want none: the fresh start is the Create pass's, not a repair's: %v", n, h.events)
	}
}

// noPodOnRevisionInvariant fails the test as soon as a live pod carries
// rev's hash: the invariant a rollback leaves behind for the revision it
// withdrew.
func (h *recoveryHarness) noPodOnRevisionInvariant(rev *appsv1.ControllerRevision) func() {
	hash := query.RevisionOf(rev).Hash()
	return func() {
		for _, pod := range h.livePods() {
			if pod.Labels[query.LabelRevisionHash] == hash {
				h.dumpState("withdrawn revision retried")
				h.t.Fatalf("pod %s was created on the withdrawn revision %s after the rollback", pod.Name, rev.Name)
			}
		}
	}
}

// A replacement member is deleted by hand with a grace period after the
// hand-over and exits cleanly, so it reads Succeeded while it is removed.
// The recycle takes the dead member, and once the object is gone the
// attempt recreates it at once, as it stands: the source never reads
// Failed, the recycle is counted once, no wave lands on the pinned
// revision's ladder, nothing is announced as a rebuilt replacement, and
// the hand-over finishes with the whole gang on the pinned revision.
func TestGangSurgeAfterHandover_RecycledMemberIsRecreatedAtOnce(t *testing.T) {
	for _, runner := range []string{"leader", "worker"} {
		t.Run(runner, func(t *testing.T) {
			h, surgeIdx := gangSurgeHandedOver(t)
			// Deletes are graceful from here: the kubelet publishes the
			// stopped pod before it removes the object.
			h.podGrace = 30 * time.Second
			victim := h.podOf(surgeIdx, runner)
			h.deleteExitingClean(victim)

			attemptStands := func() {
				s := h.instance(0)
				if s == nil || s.Phase != workloadtypes.InstancePhaseUpdating || s.Operation == nil {
					h.dumpState("attempt closed")
					t.Fatalf("the recycled member's removal closed the attempt: %+v", s)
				}
				if b := h.findBlock(h.revFixed.Name); b != nil {
					h.dumpState("ladder charged")
					t.Fatalf("the recycled member's removal was counted as a lost replacement: %+v", *b)
				}
			}
			recreated := func() bool {
				pods := h.podsOf(surgeIdx)
				if len(pods) != 2 {
					return false
				}
				for _, pod := range pods {
					if pod.Name == victim.Name && pod.UID != victim.UID {
						return true
					}
				}
				return false
			}
			if !h.runWithInvariant(8, recreated, attemptStands) {
				h.dumpState("not recreated at once")
				t.Fatalf("the recycled %s was not recreated as soon as its object was gone", runner)
			}
			if s := h.instance(0); s.Operation.RetryCount != 1 {
				t.Fatalf("RetryCount = %d, want the recycle counted once and no re-arm", s.Operation.RetryCount)
			}
			if !h.run(40, func() bool { return h.converged(h.revFixed.Name) }) {
				h.dumpState("handoff never finished")
				t.Fatalf("the hand-over did not finish on the pinned revision after the recreate")
			}
			for _, e := range h.events {
				if strings.Contains(e, "GangSurgeRebuilt") {
					t.Fatalf("a recycled member was announced as a rebuilt replacement: %s", e)
				}
			}
		})
	}
}

// deleteExitingClean is a pod deleted by hand with a grace period whose
// process exits 0 on SIGTERM: the kubelet publishes it Succeeded while it
// is removed, then removes it.
func (h *recoveryHarness) deleteExitingClean(pod *corev1.Pod) {
	h.t.Helper()
	if h.exitsCleanOnStop == nil {
		h.exitsCleanOnStop = map[string]bool{}
	}
	h.exitsCleanOnStop[pod.Name] = true
	if err := h.c.Delete(h.ctx, pod); err != nil {
		h.t.Fatalf("delete %s: %v", pod.Name, err)
	}
}

// The rebuilt replacement dies again, after it came up and the drained
// source was deleted, with the ladder at its limit. The second loss holds
// the pinned revision: the pair is abandoned, the Instance is marked Failed
// with no attempt, and nothing is rebuilt until a corrected revision or an
// operator reset.
func TestGangSurgeAfterHandover_ReplacementLostAgainHoldsTheRevisionAndFailsTheInstance(t *testing.T) {
	h, surgeIdx := gangSurgeHandedOver(t)
	h.losePods(surgeIdx)
	h.step()
	h.requireAttemptClosedByLoss(surgeIdx, 0)

	cameUpAgain := func() bool {
		s := h.instance(0)
		if s == nil || s.Phase != workloadtypes.InstancePhaseUpdating || s.Operation == nil ||
			s.Operation.RetryCount != 1 || len(h.podsOf(0)) != 0 {
			return false
		}
		rebuilt := h.podsOf(surgeIdx)
		if len(rebuilt) != 2 {
			return false
		}
		for _, pod := range rebuilt {
			if !podreadiness.IsServing(pod) {
				return false
			}
		}
		return true
	}
	if !h.run(30, cameUpAgain) {
		h.dumpState("rebuild never came up")
		t.Fatalf("the rebuilt replacement never came up behind the deleted source")
	}
	h.losePods(surgeIdx)
	if !h.run(10, func() bool { return h.heldOn(h.revFixed.Name) }) {
		h.dumpState("ladder never held")
		t.Fatalf("the second loss did not hold the pinned revision")
	}
	h.settle(6)
	s := h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil || s.RunningRevision != h.revV1.Name {
		h.dumpState("not held at Failed")
		t.Fatalf("the exhausted ladder did not mark the Instance Failed with its attempt released: %+v", s)
	}
	if h.instance(surgeIdx) != nil {
		t.Fatalf("the replacement index was not released with the held surge")
	}
	if pods := h.livePods(); len(pods) != 0 {
		h.dumpState("rebuilt after the hold")
		t.Fatalf("%d pod(s) were rebuilt after the ladder held", len(pods))
	}
	if !h.heldOn(h.revFixed.Name) {
		t.Fatalf("the hold on %s did not last", h.revFixed.Name)
	}
	if len(h.heldWarnings) != 1 || !strings.HasPrefix(h.heldWarnings[0], h.revFixed.Name) {
		t.Fatalf("WarnRetryHeld: got %v, want exactly one warning for %s", h.heldWarnings, h.revFixed.Name)
	}
}

// The whole replacement gang is deleted after the hand-over and the
// apiserver sheds the rebuild's creates on two consecutive passes. Members
// missing since the last published count are a rebuild still pending, not
// a new loss: the ladder shows the one wave the loss counted, the source
// stays as the loss left it, and the third pass creates the members and
// re-arms the attempt under a fresh deadline.
func TestGangSurgeAfterHandover_ThrottledRebuildChargesNoFurtherWave(t *testing.T) {
	h, surgeIdx := gangSurgeHandedOver(t)
	before := *h.instance(0).Operation

	h.losePods(surgeIdx)
	h.step()
	h.requireAttemptClosedByLoss(surgeIdx, 0)
	closed := *h.instance(0)

	h.shedCreates = 2
	for pass := 1; pass <= 2; pass++ {
		h.step()
		if h.shedCreates != 2-pass {
			t.Fatalf("pass %d: the apiserver shed %d create(s), want one per pass", pass, 2-pass-h.shedCreates+1)
		}
		s := h.instance(0)
		if s.Phase != closed.Phase || s.Operation == nil || s.Operation.RetryCount != closed.Operation.RetryCount ||
			!s.Operation.Deadline.Equal(&closed.Operation.Deadline) {
			h.dumpState("source moved under the throttle")
			t.Fatalf("pass %d: the source did not stay as the loss left it: %+v", pass, s)
		}
		if b := h.findBlock(h.revFixed.Name); b == nil || b.State != workloadtypes.RetryBlockBackoff || b.AttemptsStarted != 1 {
			h.dumpState("ladder under the throttle")
			t.Fatalf("pass %d: the throttled rebuild moved the ladder: %+v", pass, b)
		}
		if got := len(h.podsOf(surgeIdx)); got != 0 {
			t.Fatalf("pass %d: %d replacement pod(s) exist behind a shed create", pass, got)
		}
	}

	h.step()
	if got := len(h.podsOf(surgeIdx)); got != 2 {
		h.dumpState("rebuild after the throttle")
		t.Fatalf("the pass after the throttle created %d pod(s), want the leader and the worker", got)
	}
	if !h.rearmedRebuild(1)() {
		h.dumpState("not re-armed")
		t.Fatalf("the rebuild did not re-arm the attempt once its members were created: %+v", h.instance(0))
	}
	h.requireFreshWindow(before)
	if !h.run(40, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("handoff never finished")
		t.Fatalf("the rebuilt replacement was never promoted on the pinned revision")
	}
}

// TestGangVerdict_FailedCreateOpensNoSurgeToTheSameRevision: a gang's
// Create ended by the gang verdict leaves a row that never ran a
// revision. A never-promoted row has no source for a surge to keep, so
// the passes that follow must not open a gang surge to the same
// revision: a second gang beside the failed one is a phantom that
// doubles the capacity asked for and serves nothing. Nor does the name
// freeing re-drive the attempt: the row stays Failed with its Create
// kept until the reset mailbox or a corrective revision arrives.
func TestGangVerdict_FailedCreateOpensNoSurgeToTheSameRevision(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.useStrategy(workloadtypes.UpdateStrategySurgeThenDrain)
	h.stuckGrace = time.Hour
	h.setTarget(h.revBad, badImage)
	h.step()
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseCreating || len(h.podsOf(0)) != 2 {
		h.dumpState("after the first materialization")
		t.Fatalf("the gang's first materialization did not open: %+v", s)
	}

	h.gangs = gangObservations(0, workloadtypes.GangStateOwnershipConflict, gangConflictMessage)
	h.step()
	s := h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.LastFailure == nil || s.LastFailure.Reason != workloadtypes.PodGroupOwnershipConflictReason {
		h.dumpState("after the verdict")
		t.Fatalf("the gang verdict did not end the Create: %+v", s)
	}
	ended, warnings := *s.Operation, len(h.failedWarnings)

	stays := func(label string) {
		t.Helper()
		if marker := h.gangSurgeMarker(); marker != nil {
			t.Errorf("%s: a gang surge opened over the never-promoted row: marker %+v", label, marker)
		}
		got := h.instance(0)
		if got == nil || got.Phase != workloadtypes.InstancePhaseFailed || got.Operation == nil || got.Operation.ID != ended.ID || got.Incarnation != s.Incarnation {
			t.Errorf("%s: the row did not keep the ended Create: %+v", label, got)
		}
		if pods := h.podsOf(1); len(pods) != 0 {
			t.Errorf("%s: a second gang was built beside the failed one: %d pod(s) at index 1", label, len(pods))
		}
		if len(h.failedWarnings) != warnings {
			t.Errorf("%s: the conflict was announced again: %v", label, h.failedWarnings[warnings:])
		}
	}
	for i := 0; i < 3; i++ {
		h.step()
	}
	stays("while the name stays foreign")

	h.gangs = nil
	for i := 0; i < 3; i++ {
		h.step()
	}
	stays("after the name freed")
}

// TestGangVerdict_FailedRecreateIsNotRedrivenOnItsOwn: a gang's recreate
// ended by the gang verdict is not driven again while the name stays
// foreign, and not when it frees either. A pass with nothing new leaves
// the incarnation where the verdict found it and announces the conflict
// no second time; the row waits for the reset mailbox or a corrective
// revision.
func TestGangVerdict_FailedRecreateIsNotRedrivenOnItsOwn(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.useStrategy(workloadtypes.UpdateStrategyRecreatePod)
	h.stuckGrace = time.Hour
	h.driveToReadyOnV1()

	h.pushRevision(badImage)
	if !h.run(20, func() bool {
		s := h.instance(0)
		return s != nil && s.Incarnation == 2 && len(h.podsOf(0)) == 2
	}) {
		h.dumpState("recreate never rebuilt the gang")
		t.Fatalf("the recreate did not rebuild the gang at the bumped incarnation")
	}

	h.gangs = gangObservations(0, workloadtypes.GangStateOwnershipConflict, gangConflictMessage)
	h.step()
	s := h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.LastFailure == nil || s.LastFailure.Reason != workloadtypes.PodGroupOwnershipConflictReason {
		h.dumpState("after the verdict")
		t.Fatalf("the gang verdict did not end the recreate: %+v", s)
	}
	ended, warnings := *s.Operation, len(h.failedWarnings)

	stays := func(label string) {
		t.Helper()
		got := h.instance(0)
		if got == nil || got.Phase != workloadtypes.InstancePhaseFailed || got.Incarnation != s.Incarnation || got.Operation == nil || got.Operation.ID != ended.ID {
			t.Errorf("%s: the row did not keep the ended recreate at incarnation %d: %+v", label, s.Incarnation, got)
		}
		if len(h.failedWarnings) != warnings {
			t.Errorf("%s: the conflict was announced again: %v", label, h.failedWarnings[warnings:])
		}
	}
	for i := 0; i < 4; i++ {
		h.step()
	}
	stays("while the name stays foreign")

	h.gangs = nil
	for i := 0; i < 4; i++ {
		h.step()
	}
	stays("after the name freed")
}

// TestGangVerdict_CorrectiveRevisionResumesTheFailedCreate: a corrective
// revision is one of the two ways out of a Create the gang verdict
// ended. With the name this controller's again, the roll replaces the
// kept Create with a recreate at the new revision, no surge, since the
// row never ran a revision and has no source to keep, and the gang is
// rebuilt at its own index and promoted on the corrective revision.
func TestGangVerdict_CorrectiveRevisionResumesTheFailedCreate(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.useStrategy(workloadtypes.UpdateStrategySurgeThenDrain)
	h.stuckGrace = time.Hour
	h.setTarget(h.revBad, badImage)
	h.step()
	h.gangs = gangObservations(0, workloadtypes.GangStateOwnershipConflict, gangConflictMessage)
	h.step()
	s := h.instance(0)
	if s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.Operation == nil || s.Operation.Type != workloadtypes.InstanceOperationCreate {
		h.dumpState("after the verdict")
		t.Fatalf("the gang verdict did not end the Create with its operation kept: %+v", s)
	}
	h.gangs = nil
	h.step()
	h.step()
	if got := h.instance(0); got == nil || got.Phase != workloadtypes.InstancePhaseFailed || got.Operation == nil || got.Operation.ID != s.Operation.ID {
		t.Fatalf("the freed name re-drove the ended Create: %+v", got)
	}

	fix := h.pushRevision(goodImage)
	h.step()
	got := h.instance(0)
	if got == nil || got.Phase != workloadtypes.InstancePhaseUpdating || got.Operation == nil ||
		got.Operation.Type != workloadtypes.InstanceOperationUpdate || got.Operation.Step != workloadtypes.UpdateStepDrain ||
		got.Operation.TargetRevision != fix.Name || got.Incarnation != s.Incarnation+1 {
		h.dumpState("after the corrective revision")
		t.Fatalf("the corrective revision did not replace the kept Create with a recreate at the fix: %+v", got)
	}
	if marker := h.gangSurgeMarker(); marker != nil {
		t.Errorf("the corrective roll surged over the never-promoted row: marker %+v", marker)
	}
	if !h.run(30, func() bool { return h.converged(fix.Name) }) {
		h.dumpState("corrective rollout")
		t.Fatalf("the gang never converged on the corrective revision")
	}
	if pods := h.podsOf(1); len(pods) != 0 {
		t.Errorf("a second gang was built beside the rebuilt one: %d pod(s) at index 1", len(pods))
	}
	if pods := h.podsOf(0); len(pods) != 2 {
		t.Errorf("the gang was not rebuilt at its own index: %d pod(s) at index 0", len(pods))
	}
}

// surgeHandedOver drives a single-pod surge to its drain step and returns
// the harness there: the replacement serves and the source is out of
// rotation, draining, with both pods standing.
func surgeHandedOver(t *testing.T) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	h.routed = true
	h.recorder = record.NewFakeRecorder(256)
	h.useRollingBudget(workloadtypes.UpdateStrategySurgeThenDrain)
	h.driveToReadyOnV1()
	h.lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 4 * time.Minute}
	h.setTarget(h.revFixed, fixedImage)
	handedOver := func() bool {
		s := h.instance(0)
		return s != nil && s.Operation != nil && s.Operation.Step == workloadtypes.UpdateStepSurgeDrain && len(h.podsOf(0)) == 2
	}
	if !h.run(20, handedOver) {
		h.dumpState("hand-over")
		t.Fatalf("the surge never took the source out of rotation behind a serving replacement")
	}
	for _, pod := range h.podsOf(0) {
		if replacement := pod.Spec.Containers[0].Image == fixedImage; replacement != podreadiness.IsServing(pod) {
			t.Fatalf("pod %s serving=%v after the hand-over; the replacement serves and the source is out of rotation", pod.Name, !replacement)
		}
	}
	return h
}

// losePod deletes one live pod, the way a node or an operator takes it
// away.
func (h *recoveryHarness) losePod(pod *corev1.Pod) {
	h.t.Helper()
	if err := h.c.Delete(h.ctx, pod); err != nil {
		h.t.Fatalf("delete pod %s: %v", pod.Name, err)
	}
}

// noLivePodOnRevision fails the test as soon as a live pod carries rev's
// hash: what a withdrawal leaves behind for the revision it took away.
func (h *recoveryHarness) noLivePodOnRevision(rev *appsv1.ControllerRevision) func() {
	hash := query.RevisionOf(rev).Hash()
	return func() {
		for _, pod := range h.livePods() {
			if pod.Labels[query.LabelRevisionHash] == hash {
				h.dumpState("withdrawn revision rebuilt")
				h.t.Fatalf("pod %s was created on the withdrawn revision %s", pod.Name, rev.Name)
			}
		}
	}
}

// A single-pod surge hands over, the operator re-applies the starting
// revision, and the replacement is lost before the source's drain
// converges. Nothing is rebuilt at the withdrawn revision: the roll ends,
// the source returns to rotation on the starting revision with the pod it
// had, and the row is Ready there.
func TestSurgeAfterHandover_RollbackThenReplacementLostEndsTheRollOnTheStartingRevision(t *testing.T) {
	h := surgeHandedOver(t)
	source, replacement := h.podsOfOnImage(0, goodImage), h.podsOfOnImage(0, fixedImage)
	if len(source) != 1 || len(replacement) != 1 {
		t.Fatalf("hand-over shape: %d source pod(s), %d replacement pod(s), want one of each", len(source), len(replacement))
	}
	h.setTarget(h.revV1, goodImage)
	h.losePod(replacement[0])
	noWithdrawnPod := h.noLivePodOnRevision(h.revFixed)
	ended := func() bool {
		s := h.instance(0)
		return s != nil && s.Phase == workloadtypes.InstancePhaseReady && s.Operation == nil &&
			s.RunningRevision == h.revV1.Name && h.instanceInRotation(0)
	}
	if !h.runWithInvariant(5, ended, noWithdrawnPod) {
		h.dumpState("roll never ended")
		t.Fatalf("the rollback did not end the roll whose replacement was lost: %+v", h.instance(0))
	}
	if !h.sawEvent(workloadtypes.EventReasonSurgeAbandoned) {
		t.Fatalf("no SurgeAbandoned event for the ended roll: %v", h.events)
	}
	if pods := h.podsOf(0); len(pods) != 1 || pods[0].UID != source[0].UID {
		t.Fatalf("the source was rebuilt or joined; it runs the starting revision and keeps its pod: %d pod(s)", len(pods))
	}
	// The clock passes: nothing re-opens and no repair runs on the source.
	h.runWithInvariant(15, func() bool { return false }, noWithdrawnPod)
	if !ended() {
		h.dumpState("after the clock")
		t.Fatalf("the Instance left Ready on the starting revision: %+v", h.instance(0))
	}
	if n := h.restartsRecorded(); n != 0 {
		t.Fatalf("restarts recorded = %d, want none: the rollback owns the Instance: %v", n, h.events)
	}
}

// A single-pod surge hands over, the operator pushes a corrected revision,
// and the replacement is lost before the source's drain converges. The roll
// ends with the source back in rotation on the starting revision, and the
// corrected revision rolls from there through the ordinary gates until the
// Instance serves it; no pod of the withdrawn revision is created again.
func TestSurgeAfterHandover_CorrectivePushThenReplacementLostRollsToTheCorrectedRevision(t *testing.T) {
	h := surgeHandedOver(t)
	replacement := h.podsOfOnImage(0, fixedImage)
	if len(replacement) != 1 {
		t.Fatalf("hand-over shape: %d replacement pod(s), want one", len(replacement))
	}
	const correctedImage = "registry.test/serving:v3"
	corrected := h.pushRevision(correctedImage)
	h.losePod(replacement[0])
	noWithdrawnPod := h.noLivePodOnRevision(h.revFixed)
	h.step()
	noWithdrawnPod()
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseReady || s.Operation != nil ||
		s.RunningRevision != h.revV1.Name || !h.instanceInRotation(0) {
		h.dumpState("roll never ended")
		t.Fatalf("the corrected push did not end the roll whose replacement was lost with the source back in rotation: %+v", h.instance(0))
	}
	if !h.sawEvent(workloadtypes.EventReasonSurgeAbandoned) {
		t.Fatalf("no SurgeAbandoned event for the ended roll: %v", h.events)
	}
	if !h.runWithInvariant(40, func() bool { return h.converged(corrected.Name) && h.instanceInRotation(0) }, noWithdrawnPod) {
		h.dumpState("never converged")
		t.Fatalf("the Instance never settled on the corrected revision: %+v", h.instance(0))
	}
	h.requirePodsRender(0, corrected, correctedImage)
	if n := h.restartsRecorded(); n != 0 {
		t.Fatalf("restarts recorded = %d, want none: the roll owns the Instance: %v", n, h.events)
	}
}

// A single-pod surge hands over and the drained source is deleted; the
// replacement is then lost in the same window in which the operator
// re-applies the starting revision. The roll ends as a fresh start: the row
// re-enters Failed with no attempt on the revision it ran, and the Create
// pass rebuilds the Instance there without a repair; no pod of the
// withdrawn revision is created again.
func TestSurgeAfterHandover_ReplacementLostWithSourceGoneThenRollbackRebuildsAtTheStartingRevision(t *testing.T) {
	h := surgeHandedOver(t)
	sourceGone := func() bool {
		s, pods := h.instance(0), h.podsOf(0)
		return s != nil && s.Operation != nil && s.Operation.Step == workloadtypes.UpdateStepSurgeDrain &&
			len(pods) == 1 && pods[0].Spec.Containers[0].Image == fixedImage
	}
	if !h.run(10, sourceGone) {
		h.dumpState("source never drained")
		t.Fatalf("the drained source was never deleted behind the serving replacement")
	}
	h.setTarget(h.revV1, goodImage)
	h.losePod(h.podsOf(0)[0])
	noWithdrawnPod := h.noLivePodOnRevision(h.revFixed)
	h.step()
	noWithdrawnPod()
	if s := h.instance(0); s == nil || s.Phase != workloadtypes.InstancePhaseFailed || s.Operation != nil || s.RunningRevision != h.revV1.Name {
		h.dumpState("no fresh start")
		t.Fatalf("the roll did not end as a fresh start on the revision the source ran: %+v", h.instance(0))
	}
	if !h.runWithInvariant(20, func() bool { return h.converged(h.revV1.Name) && h.instanceInRotation(0) }, noWithdrawnPod) {
		h.dumpState("never rebuilt")
		t.Fatalf("the Instance was not rebuilt at the starting revision after the rollback: %+v", h.instance(0))
	}
	h.requirePodsRender(0, h.revV1, goodImage)
	if n := h.restartsRecorded(); n != 0 {
		t.Fatalf("restarts recorded = %d, want none: the fresh start is the Create pass's, not a repair's: %v", n, h.events)
	}
}
