package workload_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// Reconcile is a thin dispatch layer over the workload/ops state
// machines. The tests below assert the orchestration shape — nil-deps
// guards, scale-down-then-create ordering, and the early-return
// requeue on partial scale-down — by driving Reconcile against a real
// fake client and the real workload/ops bodies. Per-op bug-fix
// coverage already lives in workload/ops/*_test.go and is unchanged
// by the dispatch refactor (the op bodies move with their tests; only
// the dispatcher's call site changes).
//
// Heavier per-op coverage (Update gate ordering, Restart trigger
// detection, Migrate surge lifecycle) stays in workload/ops/*_test.go;
// those tests already drive the per-op functions directly and don't
// need a Reconcile wrapper.

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
func stubInputCallbacks(input *workload.ReconcileInput) {
	if input.MutateInstance == nil {
		input.MutateInstance = func(_ context.Context, _ int32, _ func(*workload.InstanceStatus) bool) error {
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
		input.MutateMigration = func(_ context.Context, _ string, _ func(*workload.MigrationRecord) bool) error {
			return nil
		}
	}
}

type testAtomicMutationStore struct {
	owner    client.Object
	statuses []workload.InstanceStatus
	writes   int
}

func installTestAtomicMutationStore(input *workload.ReconcileInput) *testAtomicMutationStore {
	store := &testAtomicMutationStore{
		owner:    input.OwnerObject,
		statuses: cloneTestInstanceStatuses(input.ObservedState.InstanceStatuses),
	}
	input.ApplyInstanceMutationsWithRetryBlock = store.apply
	return store
}

func (s *testAtomicMutationStore) apply(_ context.Context, mutations []workload.InstanceMutation, _ string, mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
	if mutateRetryBlock != nil {
		return fmt.Errorf("test atomic mutation store does not support RetryBlock mutations")
	}
	snapshot := workload.InstanceMutationSnapshot{Instances: make(map[int32]workload.InstanceStatus, len(s.statuses))}
	if s.owner != nil {
		snapshot.OwnerUID = s.owner.GetUID()
		snapshot.OwnerGeneration = s.owner.GetGeneration()
	}
	for _, status := range s.statuses {
		snapshot.Instances[status.Index] = cloneTestInstanceStatus(status)
	}
	for _, mutation := range mutations {
		if mutation.BatchPrecondition != nil && !mutation.BatchPrecondition(snapshot) {
			return workload.ErrStatusMutationPrecondition
		}
	}

	type committedMutation struct {
		callback func(*workload.InstanceStatus, *workload.InstanceStatus)
		before   *workload.InstanceStatus
		after    *workload.InstanceStatus
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

		status := workload.InstanceStatus{Index: mutation.Index}
		var before *workload.InstanceStatus
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

func (s *testAtomicMutationStore) sync(input *workload.ReconcileInput) {
	input.ObservedState.InstanceStatuses = cloneTestInstanceStatuses(s.statuses)
}

func (s *testAtomicMutationStore) status(index int32) *workload.InstanceStatus {
	for i := range s.statuses {
		if s.statuses[i].Index == index {
			status := cloneTestInstanceStatus(s.statuses[i])
			return &status
		}
	}
	return nil
}

func cloneTestInstanceStatuses(statuses []workload.InstanceStatus) []workload.InstanceStatus {
	cloned := make([]workload.InstanceStatus, len(statuses))
	for i := range statuses {
		cloned[i] = cloneTestInstanceStatus(statuses[i])
	}
	return cloned
}

func cloneTestInstanceStatus(status workload.InstanceStatus) workload.InstanceStatus {
	converted := v1beta1convert.InstanceStatusFromWorkload(status)
	return v1beta1convert.InstanceStatusToWorkload(*converted.DeepCopy())
}

// minimalInput builds a ReconcileInput with the bare minimum fields
// the dispatcher reads. Tests pad observed state + plan as needed.
const testScaleDownRequeueInterval = 37 * time.Second

func minimalInput(t *testing.T) workload.ReconcileInput {
	t.Helper()
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b", Namespace: "prod", UID: "uid-1",
	}}
	in := workload.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: workload.Key{
			Namespace: "prod",
			Component: workload.ComponentEngine,
			OwnerName: "llama-70b",
		},
		ScaleDownRequeueInterval: testScaleDownRequeueInterval,
		DesiredSpec: workload.WorkloadDesiredSpec{
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
func minimalPlan() workload.ComponentPlan {
	return workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{
				{Name: "default", Size: 1},
			}},
		},
	}
}

func roundTripMutateInstance(c client.Client, isvc *v1beta1.InferenceService, component workload.ComponentType) func(context.Context, int32, func(*workload.InstanceStatus) bool) error {
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
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
	_, err := workload.Reconcile(context.Background(), workload.Deps{}, in, minimalPlan(), nil)
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
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	plan := minimalPlan()

	// Cold-start path: target nil short-circuits the Update pass.
	// Create's nil-target branch returns done=true without rendering.
	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_PausedSkipsCreate proves InferenceReplica.spec.paused is a
// real circuit breaker: a missing Instance must not be allocated while the
// plan is paused.
func TestReconcile_PausedSkipsCreate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	allocateCalls := 0
	in.MutateInstance = func(_ context.Context, _ int32, _ func(*workload.InstanceStatus) bool) error {
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
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Phase: workload.InstancePhaseReady},
		{Index: 1, Phase: workload.InstancePhaseReady},
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
	if status == nil || status.Phase != workload.InstancePhaseDeleting || status.Operation == nil || status.Operation.Type != workload.InstanceOperationDelete {
		t.Fatalf("paused scale-down did not durably admit index 1: %+v", status)
	}
	if retained := store.status(0); retained == nil || retained.Phase != workload.InstancePhaseReady {
		t.Fatalf("paused scale-down changed retained index 0: %+v", retained)
	}
}

// TestReconcile_ScaleDownExtra_DrivesDelete covers an InstanceStatus whose
// index is outside the plan. Fresh Delete admission is committed before any
// Pod effect and immediately requeues for a new observation.
func TestReconcile_ScaleDownExtra_DrivesDelete(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Phase: workload.InstancePhaseReady},
		{Index: 1, Phase: workload.InstancePhaseReady}, // extra
	}
	plan := minimalPlan() // covers only index 0
	store := installTestAtomicMutationStore(&in)

	if extras := workload.ExtraInstanceIndices(in.ObservedState.InstanceStatuses, plan, false); len(extras) != 1 || extras[0] != 1 {
		t.Fatalf("ExtraInstanceIndices: got %v, want [1]", extras)
	}
	result, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || store.writes != 1 {
		t.Fatalf("scale-down admission result/writes = %+v/%d, want immediate requeue/1", result, store.writes)
	}
}

// TestReconcile_MigrationInFlight_NotScaleDown asserts that an
// Instance with Phase=Migrating is NOT scale-down-deleted, even when
// the plan doesn't cover its index — otherwise Migrate's surge
// would be ripped out by the dispatcher's first pass.
func TestReconcile_MigrationInFlight_NotScaleDown(t *testing.T) {
	observed := []workload.InstanceStatus{
		{Index: 0, Phase: workload.InstancePhaseReady},
		{Index: 7, Phase: workload.InstancePhaseMigrating},
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ExtraInstanceIndices(observed, plan, false)
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
	observed := []workload.InstanceStatus{
		{Index: 0, Phase: workload.InstancePhaseReady},
		{
			Index: 3,
			Phase: workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{
				Type: workload.InstanceOperationMigrate,
			},
		},
	}
	plan := minimalPlan() // covers only index 0

	extras := workload.ExtraInstanceIndices(observed, plan, false)
	for _, idx := range extras {
		if idx == 3 {
			t.Errorf("index 3 (Operation.Migrate) must not be in scale-down extras, got %v", extras)
		}
	}
}

// TestReconcile_SkipsMigrateWhenModeNever asserts the dispatcher
// short-circuits the migration pass when plan.MigrationMode==never,
// even with a dispatchable non-terminal Manual record present.
func TestReconcile_SkipsMigrateWhenModeNever(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.Migrations = []workload.MigrationRecord{{
		RequestUUID: "u-never", Trigger: workload.MigrationTriggerManual,
		Phase: workload.MigrationPhaseAccepted, SourceInstance: 0,
	}}
	migrationMutated := false
	in.MutateMigration = func(_ context.Context, _ string, _ func(*workload.MigrationRecord) bool) error {
		migrationMutated = true
		return nil
	}
	plan := minimalPlan()
	plan.MigrationMode = workload.MigrationModeNever

	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if migrationMutated {
		t.Errorf("migration pass must NOT fire when MigrationMode=never")
	}
}

// TestReconcile_MigrationWorkSelection is the structural work-loop
// contract: the dispatcher selects ONLY non-terminal Manual records.
// Terminal records (any trigger) and Auto records (born terminal —
// Relocated) are records, never work; they must never be picked even
// when they are the only records present. This replaces the retired
// reason-string filter stack (nextInFlightFromLedger's AutoRecover /
// ForceDelete guards) with a structural exclusion.
func TestReconcile_MigrationWorkSelection(t *testing.T) {
	scheme := makeScheme(t)

	terminalAndAuto := []workload.MigrationRecord{
		// Terminal Manual — finished work, never re-picked.
		{RequestUUID: "u-done", Trigger: workload.MigrationTriggerManual,
			Phase: workload.MigrationPhaseCompleted, SourceInstance: 0},
		// Terminal Manual failure — same.
		{RequestUUID: "u-failed", Trigger: workload.MigrationTriggerManual,
			Phase: workload.MigrationPhaseFailed, SourceInstance: 0},
		// Auto relocation record — born terminal, structurally excluded.
		{RequestUUID: "u-auto", Trigger: workload.MigrationTriggerAuto,
			Phase: workload.MigrationPhaseRelocated, SourceInstance: 0},
	}
	if rec := workload.NextManualMigration(terminalAndAuto); rec != nil {
		t.Fatalf("terminal/Auto records must never be selected as work; picked %q", rec.RequestUUID)
	}

	// Dispatcher-level proof: with only terminal/Auto records the
	// migration pass performs zero record mutations.
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	in := minimalInput(t)
	in.ObservedState.Migrations = terminalAndAuto
	migrationMutated := false
	in.MutateMigration = func(_ context.Context, _ string, _ func(*workload.MigrationRecord) bool) error {
		migrationMutated = true
		return nil
	}
	plan := minimalPlan()
	plan.MigrationMode = workload.MigrationModeAuto
	if _, err := workload.Reconcile(context.Background(), workload.Deps{Client: c}, in, plan, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if migrationMutated {
		t.Errorf("terminal/Auto records must not reach the executor")
	}

	// Positive control: a non-terminal Manual record IS selected — and
	// the oldest StartedAt wins when several are in flight.
	older := metav1.NewTime(fixedMigrationTime())
	newer := metav1.NewTime(fixedMigrationTime().Add(time.Minute))
	work := append(append([]workload.MigrationRecord(nil), terminalAndAuto...),
		workload.MigrationRecord{RequestUUID: "u-newer", Trigger: workload.MigrationTriggerManual,
			Phase: workload.MigrationPhaseAccepted, SourceInstance: 0, StartedAt: newer},
		workload.MigrationRecord{RequestUUID: "u-older", Trigger: workload.MigrationTriggerManual,
			Phase: workload.MigrationPhaseSurgePending, SourceInstance: 0, StartedAt: older},
	)
	rec := workload.NextManualMigration(work)
	if rec == nil || rec.RequestUUID != "u-older" {
		t.Fatalf("expected oldest non-terminal Manual record u-older, got %+v", rec)
	}
}

// fixedMigrationTime anchors the work-selection ordering assertions.
func fixedMigrationTime() time.Time {
	return time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
}

// TestReconcile_UpdateGateNil_AllowsAll asserts the dispatcher treats
// a nil UpdateGate as always-allowed — workload-side unit tests and
// the IR adapter (which doesn't wire coordination gates) rely on this.
// We assert by setting up a plan that would trigger Update if the
// target diff fires, and confirming Reconcile doesn't error.
func TestReconcile_UpdateGateNil_AllowsAll(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.UpdateGate = nil
	plan := minimalPlan()

	_, err := workload.Reconcile(context.Background(), deps, in, plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_FreshMigrateOnNonReadySource_FallsThroughToUpdate pins
// the Migrate-defer fall-through: when a fresh Accepted
// migration record exists (no surge allocated, no stamped Operation)
// but the source InstanceStatus is NOT in a state where Migrate can
// accept it (Phase=Updating, Creating, Restarting, or any Operation !=
// Migrate in flight), Migrate defers without taking ownership. The
// dispatcher MUST then fall through to the Update / Create passes so
// the in-flight op can converge — otherwise the dispatcher loops
// indefinitely in the Migrate-defer branch, the in-flight Update never
// completes, and the source never reaches Ready so the migration can
// never proceed (silent deadlock).
//
// Reproduction shape: user adds NodeAffinity to ISVC.Spec.Engine
// PodSpec while the source pod is Running on the to-be-pinned node.
// The controller fires Update (Phase=Updating). A migration request is
// accepted into status.migrations before the Update converges. The
// dispatcher picks the record; Migrate sees Phase=Updating, returns
// done=false without stamping. Without the fall-through the dispatcher
// requeues at MigrateRequeueInterval indefinitely and Update never
// runs.
//
// Test shape: instrument MutateInstance to count calls. Without the
// fall-through: zero mutations (Migrate's defer doesn't mutate; Update
// never runs). With it: Update fires and at minimum touches
// MutateInstance to stamp Op.Step=Surge.
func TestReconcile_FreshMigrateOnNonReadySource_FallsThroughToUpdate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		// Phase=Updating: an in-flight Update from a recent spec edit
		// (e.g., adding NodeAffinity to ISVC PodSpec). Migrate's fresh-
		// request branch defers because source.Phase != Ready.
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseUpdating, RunningRevision: "prior-rev"},
	}
	plan := minimalPlan()
	plan.UpdateStrategy = workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain}
	plan.MigrationMode = workload.MigrationModeAuto

	// A fresh Accepted record — the dispatcher does NOT pre-check source
	// state; that's Migrate's job. The point of the test is the
	// post-Migrate fall-through.
	in.ObservedState.Migrations = []workload.MigrationRecord{{
		RequestUUID:    "fresh-uuid-during-update",
		Trigger:        workload.MigrationTriggerManual,
		Phase:          workload.MigrationPhaseAccepted,
		SourceInstance: 0,
		FromNode:       "node5",
	}}

	// Count MutateInstance calls to detect Update firing. Migrate's
	// defer path doesn't mutate; Update's stamp does. Without the
	// dispatcher fix mutateCount is 0; with it, Update fires and the
	// count is > 0.
	mutateCount := 0
	in.MutateInstance = func(_ context.Context, _ int32, fn func(*workload.InstanceStatus) bool) error {
		mutateCount++
		// Apply mutation against the seeded ObservedState so subsequent
		// reads inside the same reconcile see the result.
		for i := range in.ObservedState.InstanceStatuses {
			if in.ObservedState.InstanceStatuses[i].Index == 0 {
				_ = fn(&in.ObservedState.InstanceStatuses[i])
				break
			}
		}
		return nil
	}

	// Provide a non-nil target so the Update loop is considered.
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}

	_, err := workload.Reconcile(context.Background(), deps, in, plan, target)
	if err != nil {
		// Per-op machines (Update's surge body) can error against the
		// empty fake client (no CRs, no pods to drain). The contract this
		// test pins is the dispatcher-level fall-through, not the op-body
		// success. Log and continue to the load-bearing assertion.
		t.Logf("Reconcile op error (expected against empty fake client): %v", err)
	}

	// Load-bearing assertion: MutateInstance was called at least once,
	// which means the dispatcher reached the Update pass after Migrate
	// deferred. Without the fix the dispatcher returns immediately
	// after Migrate's silent defer and mutateCount stays 0.
	if mutateCount == 0 {
		t.Errorf("expected dispatcher to fall through to Update after Migrate deferred; MutateInstance was never called (silent Migrate-defer deadlock)")
	}
}

// TestReconcile_UpdateRan_RequeuesAndSkipsCreate pins the X-2
// (bump-during-bump) fix: when ANY Update.call fires in the per-
// Instance loop (even one returning done=true), the dispatcher MUST
// return Requeue WITHOUT running the Create pass. The Create pass
// reads ObservedState (a snapshot) which is now stale wrt the mutations
// the Update calls just committed; running Create on stale state
// corrupts RunningRevision (Create promotes pods to target.Name even
// when the pods are on a different revision) and creates duplicate
// pods (Create's scale-up reads the pre-promote ActiveOrdinal and
// thinks the canonical slot is missing). Both are the X-2 corruption
// modes.
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
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		// Phase=Updating triggers DetectUpdateTrigger → true regardless
		// of RunningRevision; the dispatcher MUST call Update.
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseUpdating, RunningRevision: "prior-rev"},
	}
	plan := minimalPlan()
	plan.UpdateStrategy = workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain}
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
	// RequeueAfter > 0) — Create did NOT run. Without the X-2 fix, the
	// dispatcher returned Create's result (possibly Requeue=false).
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("dispatcher must Requeue when any Update fired (X-2 guard); got %+v", result)
	}
}

// TestReconcile_ScaleUpDuringUpdate_CreatesFreshIndices pins the
// scale-out-during-rollout fix: with instance-0 mid-update (Phase=
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
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseUpdating, RunningRevision: "llama-70b-engine-priorrev"},
	}
	in.MutateInstance = roundTripMutateInstance(c, isvc, workload.ComponentEngine)
	// Plan asks for 3 single-pod instances. Index 0 is mid-update; 1,2 brand-new.
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  3,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain},
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

// TestHeldByPartition pins the RollingUpdate.Partition count predicate:
// a hold candidate is held iff its rank in the candidate order is below
// a positive Partition (PartitionHeldIndices owns candidate membership).
// A nil RollingUpdate, nil Partition, or Partition<=0 holds nothing.
func TestHeldByPartition(t *testing.T) {
	part := func(n int32) *workload.RollingUpdate { return &workload.RollingUpdate{Partition: &n} }
	cases := []struct {
		name string
		ru   *workload.RollingUpdate
		rank int32
		held bool
	}{
		{"nil RollingUpdate", nil, 0, false},
		{"nil Partition", &workload.RollingUpdate{}, 0, false},
		{"Partition=0 holds nothing", part(0), 0, false},
		{"Partition=2 holds rank 0", part(2), 0, true},
		{"Partition=2 holds rank 1", part(2), 1, true},
		{"Partition=2 rolls rank 2", part(2), 2, false},
		{"Partition=2 rolls rank 3", part(2), 3, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := workload.HeldByPartition(tc.ru, tc.rank); got != tc.held {
				t.Errorf("HeldByPartition(rank=%d) = %v, want %v", tc.rank, got, tc.held)
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
// This is the raw fleet-dispatch shape with the webhook out of the
// loop. The real-cluster guard against a mass single-pass dispatch is the
// webhook's 25% default-budget defaulter, which stamps a non-nil RollingUpdate
// (percent MaxSurge/MaxUnavailable) so PerComponentMax*Budget != -1 and
// the loop's projected-vs-budget check caps fresh starts per pass. When
// that defaulter is bypassed (nil RollingUpdate here), the dispatcher is
// uncapped by design — the group-level UpdateGate is the only remaining
// layer, and it is nil too. We assert the CURRENT behavior explicitly so
// a regression that silently re-caps (or fails to dispatch) the raw path
// is caught. No XFAIL: uncapped dispatch is the documented raw contract.
func TestReconcile_NoBudget_NoGate(t *testing.T) {
	const replicas = int32(8)

	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b", Namespace: "prod", UID: "uid-1"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc).Build()
	deps := workload.Deps{Client: c}

	in := minimalInput(t)
	// No UpdateGate: the cross-Component coordination layer is out of the
	// loop, matching the IR adapter and webhook-bypassed shape.
	in.UpdateGate = nil
	in.MutateInstance = roundTripMutateInstance(c, isvc, workload.ComponentEngine)

	// 8 Ready Instances, all on a prior revision → DetectUpdateTrigger's
	// cheap RunningRevision != target.Name fast-path fires for every one,
	// and startingFresh is true (Phase != Updating).
	observed := make([]workload.InstanceStatus, 0, replicas)
	instances := make([]workload.InstancePlan, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		observed = append(observed, workload.InstanceStatus{
			Index: i, Incarnation: 1, Phase: workload.InstancePhaseReady,
			RunningRevision: "llama-70b-engine-priorrev",
		})
		instances = append(instances, workload.InstancePlan{
			Index: i, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		})
	}
	in.ObservedState.InstanceStatuses = observed

	// SurgeThenDrain + nil RollingUpdate → BudgetNoLimit on both layers.
	plan := workload.ComponentPlan{
		Component:      workload.ComponentEngine,
		Replicas:       replicas,
		Instances:      instances,
		UpdateStrategy: workload.UpdateStrategy{Type: workload.UpdateStrategySurgeThenDrain},
	}
	// Sanity-check the premise: both per-Component budgets are uncapped.
	if got := workload.PerComponentMaxSurgeBudget(plan.UpdateStrategy.RollingUpdate, replicas); got != workload.BudgetNoLimit {
		t.Fatalf("premise: MaxSurge budget must be BudgetNoLimit for nil RollingUpdate, got %d", got)
	}
	if got := workload.PerComponentMaxUnavailableBudget(plan.UpdateStrategy.RollingUpdate, replicas); got != workload.BudgetNoLimit {
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

// enginePod fabricates a single-pod "default" engine pod at ordinal 0
// with the labels Render stamps (managed-by + instance-idx + ordinal +
// component + isvc), so query selectors and instance-index filters
// recognize it.
func enginePod(isvc, ns string, idx int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc, workload.ComponentEngine, idx, "default", 0),
			Namespace: ns,
			UID:       types.UID(fmt.Sprintf("%s-engine-%d-uid", isvc, idx)),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
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

// TestReconcile_RestartPass_OneLiveListForManyInstances pins the
// O(gangs^2) fix: the restart pass does a SINGLE live pod List for the
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
	deps := workload.Deps{Client: c, APIReader: counter}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady},
	}
	plan := workload.ComponentPlan{
		Component:     workload.ComponentEngine,
		Replicas:      3,
		RestartPolicy: workload.RestartPolicyRecreateInstance,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
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
// single-List + bucket refactor preserves exact per-Instance restart
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
	deps := workload.Deps{Client: c, APIReader: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: workload.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: workload.InstancePhaseReady},
		{Index: 2, Incarnation: 1, Phase: workload.InstancePhaseReady},
	}
	in.MutateInstance = roundTripMutateInstance(c, isvc, workload.ComponentEngine)
	plan := workload.ComponentPlan{
		Component:     workload.ComponentEngine,
		Replicas:      3,
		RestartPolicy: workload.RestartPolicyRecreateInstance,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
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

type restartScaleTestFixture struct {
	isvc   *v1beta1.InferenceService
	client client.Client
	input  workload.ReconcileInput
	plan   workload.ComponentPlan
	target *appsv1.ControllerRevision
}

func newRestartScaleTestFixture(
	t *testing.T,
	name string,
	replicas int32,
	paused bool,
	status workload.InstanceStatus,
	runners []workload.RunnerPlan,
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
	input.ObservedState.InstanceStatuses = []workload.InstanceStatus{cloneTestInstanceStatus(status)}
	input.MutateInstance = roundTripMutateInstance(c, isvc, workload.ComponentEngine)

	instances := make([]workload.InstancePlan, 0, replicas)
	for idx := int32(0); idx < replicas; idx++ {
		incarnation := int64(1)
		if idx == status.Index {
			incarnation = status.Incarnation
		}
		instances = append(instances, workload.InstancePlan{
			Index: idx, Incarnation: incarnation,
			Runners: append([]workload.RunnerPlan(nil), runners...),
		})
	}
	plan := workload.ComponentPlan{
		Component:            workload.ComponentEngine,
		Replicas:             replicas,
		RestartPolicy:        workload.RestartPolicyRecreateInstance,
		InstanceReadyTimeout: time.Minute,
		Paused:               paused,
		Instances:            instances,
	}
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: status.RunningRevision, Namespace: isvc.Namespace},
	}
	return &restartScaleTestFixture{isvc: isvc, client: c, input: input, plan: plan, target: target}
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

// An in-flight Restart owns its existing Instance, but it must not prevent
// unrelated absent indices from beginning their Create lifecycle.
func TestReconcile_ScaleUpDuringRestart_CreatesFreshIndices(t *testing.T) {
	name := "scale-during-restart"
	targetName := name + "-engine-target"
	f := newRestartScaleTestFixture(t, name, 3, false, workload.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workload.InstancePhaseRestarting,
		RunningRevision: targetName,
		Operation: &workload.InstanceOperation{
			ID: "restart-0", Type: workload.InstanceOperationRestart, Step: "Drain",
		},
	}, []workload.RunnerPlan{{Name: "default", Size: 1}})

	result, err := workload.Reconcile(context.Background(), workload.Deps{
		Client: f.client, APIReader: f.client, Expectations: workload.NewExpectations(),
	}, f.input, f.plan, f.target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("an in-flight restart must schedule another pass; got %+v", result)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	for idx := int32(0); idx < 3; idx++ {
		name := query.PodName(f.isvc.Name, workload.ComponentEngine, idx, "default", 0)
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
	leader.Name = query.PodName(name, workload.ComponentEngine, 0, "leader", 0)
	leader.Labels[query.LabelRunner] = "leader"
	f := newRestartScaleTestFixture(t, name, 2, false, workload.InstanceStatus{
		Index: 0, Incarnation: 1, Phase: workload.InstancePhaseCreating, PodCount: 1,
		RunningRevision: targetName, TargetRevision: targetName,
		Operation: &workload.InstanceOperation{
			ID: "create-0", Type: workload.InstanceOperationCreate, Step: "CreatePods",
			TargetRevision: targetName,
		},
	}, []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}, leader)

	result, err := workload.Reconcile(context.Background(), workload.Deps{
		Client: f.client, APIReader: f.client, Expectations: workload.NewExpectations(),
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
		freshName := query.PodName(f.isvc.Name, workload.ComponentEngine, 1, runner, 0)
		if !names[freshName] {
			t.Errorf("expected fresh-index pod %q; got %v", freshName, names)
		}
		restartName := query.PodName(f.isvc.Name, workload.ComponentEngine, 0, runner, 0)
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
	f := newRestartScaleTestFixture(t, name, 2, true, workload.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: workload.InstancePhaseRestarting,
		RunningRevision: targetName,
		Operation: &workload.InstanceOperation{
			ID: "restart-0", Type: workload.InstanceOperationRestart, Step: "Drain",
		},
	}, []workload.RunnerPlan{{Name: "default", Size: 1}})

	result, err := workload.Reconcile(context.Background(), workload.Deps{
		Client: f.client, APIReader: f.client, Expectations: workload.NewExpectations(),
	}, f.input, f.plan, f.target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("the permitted restart must schedule another pass; got %+v", result)
	}

	names := podNameSet(t, f.client, f.isvc.Namespace)
	restartName := query.PodName(f.isvc.Name, workload.ComponentEngine, 0, "default", 0)
	if !names[restartName] {
		t.Errorf("paused reconcile must still advance the existing restart; got %v", names)
	}
	freshName := query.PodName(f.isvc.Name, workload.ComponentEngine, 1, "default", 0)
	if names[freshName] {
		t.Errorf("paused reconcile must not materialize absent index 1; got %v", names)
	}
	if s := instanceStatusByIndex(f.client, f.isvc, v1beta1.EngineComponent, 1); s != nil {
		t.Errorf("paused reconcile must not allocate status for absent index 1; got %+v", s)
	}
}
