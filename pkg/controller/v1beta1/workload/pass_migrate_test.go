package workload_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestReconcile_SkipsMigrateWhenModeNever asserts the dispatcher
// short-circuits the migration pass when plan.MigrationMode==never,
// even with a dispatchable non-terminal Manual record present.
func TestReconcile_SkipsMigrateWhenModeNever(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.Migrations = []types.MigrationRecord{{
		RequestUUID: "u-never", Trigger: types.MigrationTriggerManual,
		Phase: types.MigrationPhaseAccepted, SourceInstance: 0,
	}}
	migrationMutated := false
	in.MutateMigration = func(_ context.Context, _ string, _ func(*types.MigrationRecord) bool) error {
		migrationMutated = true
		return nil
	}
	plan := minimalPlan()
	plan.MigrationMode = types.MigrationModeNever

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
// when they are the only records present. The exclusion is structural
// (phase and trigger), not a reason-string filter.
func TestReconcile_MigrationWorkSelection(t *testing.T) {
	scheme := makeScheme(t)

	terminalAndAuto := []types.MigrationRecord{
		// Terminal Manual — finished work, never re-picked.
		{RequestUUID: "u-done", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseCompleted, SourceInstance: 0},
		// Terminal Manual failure — same.
		{RequestUUID: "u-failed", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseFailed, SourceInstance: 0},
		// Auto relocation record — born terminal, structurally excluded.
		{RequestUUID: "u-auto", Trigger: types.MigrationTriggerAuto,
			Phase: types.MigrationPhaseRelocated, SourceInstance: 0},
	}
	if rec := types.NextManualMigration(terminalAndAuto); rec != nil {
		t.Fatalf("terminal/Auto records must never be selected as work; picked %q", rec.RequestUUID)
	}

	// Dispatcher-level proof: with only terminal/Auto records the
	// migration pass performs zero record mutations.
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	in := minimalInput(t)
	in.ObservedState.Migrations = terminalAndAuto
	migrationMutated := false
	in.MutateMigration = func(_ context.Context, _ string, _ func(*types.MigrationRecord) bool) error {
		migrationMutated = true
		return nil
	}
	plan := minimalPlan()
	plan.MigrationMode = types.MigrationModeAuto
	if _, err := workload.Reconcile(context.Background(), types.Deps{Client: c}, in, plan, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if migrationMutated {
		t.Errorf("terminal/Auto records must not reach the executor")
	}

	// Positive control: a non-terminal Manual record IS selected — and
	// the oldest StartedAt wins when several are in flight.
	older := metav1.NewTime(fixedMigrationTime())
	newer := metav1.NewTime(fixedMigrationTime().Add(time.Minute))
	work := append(append([]types.MigrationRecord(nil), terminalAndAuto...),
		types.MigrationRecord{RequestUUID: "u-newer", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseAccepted, SourceInstance: 0, StartedAt: newer},
		types.MigrationRecord{RequestUUID: "u-older", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseSurgePending, SourceInstance: 0, StartedAt: older},
	)
	rec := types.NextManualMigration(work)
	if rec == nil || rec.RequestUUID != "u-older" {
		t.Fatalf("expected oldest non-terminal Manual record u-older, got %+v", rec)
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
// Shape: a spec edit (NodeAffinity on the Engine PodSpec, say) while the
// source pod is Running fires Update (Phase=Updating), and a migration
// request is accepted into status.migrations before the Update
// converges. The dispatcher picks the record; Migrate sees
// Phase=Updating and returns done=false without stamping. Without the
// fall-through the dispatcher would requeue at MigrateRequeueInterval
// indefinitely and Update would never run.
//
// Test shape: instrument MutateInstance to count calls. Without the
// fall-through: zero mutations (Migrate's defer doesn't mutate; Update
// never runs). With it: Update fires and at minimum touches
// MutateInstance to stamp Op.Step=Surge.
func TestReconcile_FreshMigrateOnNonReadySource_FallsThroughToUpdate(t *testing.T) {
	scheme := makeScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	deps := types.Deps{Client: c}

	in := minimalInput(t)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		// Phase=Updating: an in-flight Update from a recent spec edit
		// (e.g., adding NodeAffinity to ISVC PodSpec). Migrate's fresh-
		// request branch defers because source.Phase != Ready.
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "prior-rev"},
	}
	plan := minimalPlan()
	plan.UpdateStrategy = types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain}
	plan.MigrationMode = types.MigrationModeAuto

	// A fresh Accepted record — the dispatcher does NOT pre-check source
	// state; that's Migrate's job. The point of the test is the
	// post-Migrate fall-through.
	in.ObservedState.Migrations = []types.MigrationRecord{{
		RequestUUID:    "fresh-uuid-during-update",
		Trigger:        types.MigrationTriggerManual,
		Phase:          types.MigrationPhaseAccepted,
		SourceInstance: 0,
		FromNode:       "node5",
	}}

	// Count MutateInstance calls to detect Update firing. Migrate's
	// defer path doesn't mutate; Update's stamp does, so a dispatcher
	// that falls through to Update leaves the count > 0.
	mutateCount := 0
	in.MutateInstance = func(_ context.Context, _ int32, fn func(*types.InstanceStatus) bool) error {
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
	// deferred. A dispatcher that returned on Migrate's silent defer
	// would leave mutateCount at 0.
	if mutateCount == 0 {
		t.Errorf("expected dispatcher to fall through to Update after Migrate deferred; MutateInstance was never called (silent Migrate-defer deadlock)")
	}
}

// parkedDrainFixture wires a Reconcile input whose Manual record u-parked
// is Draining on a source pod the kubelet is not removing: the pod is
// Terminating (finalizer-pinned so the fake client keeps the object) and
// the fixture clock runs an hour past its deletion, so the pod is well
// past its own deadline. Its serving surge sits at index 1. withQueued
// adds u-queued, a fresh Manual request for the steady Instance at index
// 2, whose recorded revision the fake client carries so the request can
// allocate its surge.
type parkedDrainFixture struct {
	client       client.Client
	deps         types.Deps
	input        types.ReconcileInput
	plan         types.ComponentPlan
	records      []types.MigrationRecord
	parkedSource *corev1.Pod
}

func newParkedDrainFixture(t *testing.T, withQueued bool) *parkedDrainFixture {
	t.Helper()
	const parked, queued = "u-parked", "u-queued"
	const queuedRevision = "llama-70b-engine-revb"
	scheme := makeScheme(t)

	parkedSource := enginePod("llama-70b", "prod", 0)
	parkedSource.Spec.NodeName = "node-a"
	parkedSource.Finalizers = []string{"example.com/hold"}
	parkedSurge := enginePod("llama-70b", "prod", 1)
	parkedSurge.Spec.NodeName = "node-c"
	queuedSource := enginePod("llama-70b", "prod", 2)
	queuedSource.Spec.NodeName = "node-b"
	queuedSource.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}
	raw, err := json.Marshal(revision.DataPayload{PodSpec: &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
	}})
	if err != nil {
		t.Fatalf("marshal revision payload: %v", err)
	}
	queuedCR := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: queuedRevision, Namespace: "prod"}}
	queuedCR.Data.Raw = raw
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(parkedSource, parkedSurge, queuedSource, queuedCR).Build()
	if err := c.Delete(context.Background(), parkedSource); err != nil {
		t.Fatalf("delete the parked source pod: %v", err)
	}
	clk := clocktesting.NewFakeClock(time.Now().Add(time.Hour))
	now := clk.Now()

	f := &parkedDrainFixture{
		client:       c,
		deps:         types.Deps{Client: c, Clock: clk, Expectations: types.NewExpectations()},
		parkedSource: parkedSource,
	}
	surgeIdx := int32(1)
	f.records = []types.MigrationRecord{{
		RequestUUID: parked, Trigger: types.MigrationTriggerManual,
		Phase: types.MigrationPhaseDraining, SourceInstance: 0, SurgeInstance: &surgeIdx, FromNode: "node-a",
		StartedAt: metav1.NewTime(now.Add(-time.Hour)), Deadline: metav1.NewTime(now.Add(time.Hour)),
	}}
	if withQueued {
		f.records = append(f.records, types.MigrationRecord{
			RequestUUID: queued, Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseAccepted, SourceInstance: 2, FromNode: "node-b",
			StartedAt: metav1.NewTime(now.Add(-time.Minute)), Deadline: metav1.NewTime(now.Add(time.Hour)),
		})
	}

	in := minimalInput(t)
	in.Clock = clk
	in.MigrationAudit = &types.MigrationAuditPolicy{MaxInFlight: 3, MaxPerWindow: 10, Window: time.Hour}
	migratePin := func(uuid string) *types.InstanceOperation {
		return &types.InstanceOperation{Type: types.InstanceOperationMigrate, Step: "CreateSurge", RequestUUID: uuid}
	}
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseMigrating, RunningRevision: "llama-70b-engine-reva", Operation: migratePin(parked)},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseCreating, Operation: migratePin(parked)},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: queuedRevision},
	}
	in.ObservedState.Migrations = append([]types.MigrationRecord(nil), f.records...)
	in.MutateMigration = func(_ context.Context, uuid string, mutate func(*types.MigrationRecord) bool) error {
		for i := range f.records {
			if f.records[i].RequestUUID == uuid {
				r := f.records[i]
				if mutate(&r) {
					f.records[i] = r
				}
				return nil
			}
		}
		return nil
	}
	f.input = in
	f.plan = types.ComponentPlan{
		Component:     types.ComponentEngine,
		Replicas:      2,
		MigrationMode: types.MigrationModeAuto,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	return f
}

func (f *parkedDrainFixture) record(t *testing.T, uuid string) types.MigrationRecord {
	t.Helper()
	for _, r := range f.records {
		if r.RequestUUID == uuid {
			return r
		}
	}
	t.Fatalf("record %s missing", uuid)
	return types.MigrationRecord{}
}

// TestReconcile_ParkedDrainTendsTheRecordAndDispatchesTheNext pins the
// dispatch-head rule at the dispatcher: a Draining record whose source
// pod is Terminating past its own deletion deadline is parked on a
// kubelet that is not removing the pod, and a parked record must not
// hold the head. The pass still tends it — the drive re-reads the pair
// and runs the escalation it is configured for — but the queued Manual
// request behind it is dispatched in the same pass: its surge index is
// allocated while the parked record keeps its Draining phase and its
// pods untouched.
func TestReconcile_ParkedDrainTendsTheRecordAndDispatchesTheNext(t *testing.T) {
	f := newParkedDrainFixture(t, true)
	if _, err := workload.Reconcile(context.Background(), f.deps, f.input, f.plan, nil); err != nil {
		// The queued request's surge materialization runs against a
		// minimal fake client; the contract pinned here is the dispatch
		// that precedes it.
		t.Logf("Reconcile op error (tolerated against the minimal fake client): %v", err)
	}

	if got := f.record(t, "u-queued"); !got.SurgeAllocated() || got.Phase != types.MigrationPhaseSurgePending {
		t.Fatalf("the queued request must dispatch behind the parked record; got %+v", got)
	}
	if got := f.record(t, "u-parked"); got.Phase != types.MigrationPhaseDraining {
		t.Fatalf("the parked record keeps its Draining phase; got %+v", got)
	}
	live := &corev1.Pod{}
	if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.parkedSource), live); err != nil {
		t.Fatalf("the parked source pod is left to its kubelet, never force-deleted without a policy: %v", err)
	}
}

// TestReconcile_ParkedDrainAlonePacesThePass: a parked record with
// nothing queued behind it is still a migration in flight. It paces the
// pass at the Migrate interval and ends it there — the Update and Create
// passes do not run against the pair it still owns — exactly as a record
// at the head does; only the head is what it gives up.
func TestReconcile_ParkedDrainAlonePacesThePass(t *testing.T) {
	f := newParkedDrainFixture(t, false)
	createReached := false
	f.input.MutateInstance = func(_ context.Context, idx int32, _ func(*types.InstanceStatus) bool) error {
		if idx == 2 {
			createReached = true
		}
		return nil
	}
	// Index 2 has no row in the observed state, so a Create pass that ran
	// would materialize it.
	f.input.ObservedState.InstanceStatuses = f.input.ObservedState.InstanceStatuses[:2]
	res, err := workload.Reconcile(context.Background(), f.deps, f.input, f.plan, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != testRequeueIntervals.Operation {
		t.Fatalf("a parked record paces the pass at the Migrate interval; got %+v", res)
	}
	if createReached {
		t.Fatalf("a migration in flight ends the pass ahead of the Create pass, parked or not")
	}
	if got := f.record(t, "u-parked"); got.Phase != types.MigrationPhaseDraining {
		t.Fatalf("the parked record keeps its Draining phase; got %+v", got)
	}
}
