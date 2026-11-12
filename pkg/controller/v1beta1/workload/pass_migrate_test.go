package workload_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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

	_, err := workload.Reconcile(context.Background(), deps, in, plan, desiredRevisionForTest(t, c, in))
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
	if _, err := workload.Reconcile(context.Background(), types.Deps{Client: c}, in, plan, desiredRevisionForTest(t, c, in)); err != nil {
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
	// The nodes the pods are bound to exist: a pod on a Node the cluster
	// does not hold is read as gone, which is not this fixture's story.
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(parkedSource, parkedSurge, queuedSource, queuedCR,
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-c"}}).Build()
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

// A record waiting on its source teardown with nothing queued behind it
// is still a migration in flight, and it still paces the pass at the
// Migrate interval. It does not end the pass: it waits on a kubelet, not
// on anything the engine does, so the Create pass runs beside the pair it
// owns and a newly planned index is materialized without waiting for it.
func TestReconcile_DrainWaitingOnItsKubeletAloneLetsTheCreatePassRun(t *testing.T) {
	f := newParkedDrainFixture(t, false)
	createReached := false
	f.input.MutateInstance = func(_ context.Context, idx int32, _ func(*types.InstanceStatus) bool) error {
		if idx == 2 {
			createReached = true
		}
		return nil
	}
	// Index 2 has neither a row nor a pod, so the Create pass materializes
	// it when it runs.
	f.input.ObservedState.InstanceStatuses = f.input.ObservedState.InstanceStatuses[:2]
	if err := f.client.Delete(context.Background(), enginePod(f.input.Key.OwnerName, f.input.Key.Namespace, 2)); err != nil {
		t.Fatalf("empty index 2: %v", err)
	}
	res, err := workload.Reconcile(context.Background(), f.deps, f.input, f.plan, desiredRevisionForTest(t, f.client, f.input))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != testRequeueIntervals.Operation {
		t.Fatalf("a record waiting on its kubelet still paces the pass at the Migrate interval; got %+v", res)
	}
	if !createReached {
		t.Fatalf("a record waiting on its kubelet lets the Create pass run beside it")
	}
	if got := f.record(t, "u-parked"); got.Phase != types.MigrationPhaseDraining {
		t.Fatalf("the waiting record keeps its Draining phase; got %+v", got)
	}
}

// teardownWaitFixture is a four-Instance engine under SurgeThenDrain maxSurge
// 1 whose Instance 0 is being moved: the replacement serves at index 4,
// the source pod sits Terminating on a node whose kubelet is gone, and the
// record reads Draining. With the source pod past its own deletion
// deadline the move waits on that kubelet; inFlight leaves the pod inside
// its grace, which is a move still making progress. Instances 1 to 3 serve the prior
// revision. replicas above 4 plans the extra indices after the pair's.
type teardownWaitFixture struct {
	client  client.Client
	deps    types.Deps
	input   types.ReconcileInput
	plan    types.ComponentPlan
	target  *appsv1.ControllerRevision
	records []types.MigrationRecord
	// created lists the indices the Create pass wrote, in order.
	created []int32
}

const teardownWaitUUID = "u-teardown-wait"

func newTeardownWaitFixture(t *testing.T, replicas int32, inFlight bool) *teardownWaitFixture {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clocktesting.NewFakeClock(now)
	in := minimalInput(t)
	in.Clock = clk
	in.DesiredSpec.Replicas = replicas
	in.MigrationAudit = &types.MigrationAuditPolicy{MaxInFlight: 3, MaxPerWindow: 10, Window: time.Hour}

	prior := in.Key.OwnerName + "-engine-priorrev"
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithObjects(teardownWaitObjects(t, in, prior, inFlight)...).Build()

	f := &teardownWaitFixture{
		client: c,
		deps:   types.Deps{Client: c, Clock: clk, Expectations: types.NewExpectations()},
	}
	in.ObservedState.InstanceStatuses = teardownWaitRows(prior)
	f.records = []types.MigrationRecord{teardownWaitRecord(now)}
	in.ObservedState.Migrations = append([]types.MigrationRecord(nil), f.records...)
	f.input = in
	f.routeWrites()
	f.plan = teardownWaitPlan(replicas)
	f.target = desiredRevisionForTest(t, c, f.input)
	return f
}

// teardownWaitObjects is the cluster the fixture reads: the source pod of
// Instance 0 Terminating on node-a with a finalizer holding it, the prior
// revision, the serving pods of Instances 1 to 4, and the replacement's
// routed Service listing it as a Ready endpoint, which the move's tail
// gate reads every pass. inFlight leaves the source pod inside its
// deletion grace; otherwise it is past its own deletion deadline.
func teardownWaitObjects(t *testing.T, in types.ReconcileInput, prior string, inFlight bool) []client.Object {
	t.Helper()
	now := in.Clock.Now()
	source := enginePod(in.Key.OwnerName, in.Key.Namespace, 0)
	source.Spec.NodeName = "node-a"
	source.Finalizers = []string{"example.com/hold"}
	deadline := now.Add(-time.Minute)
	if inFlight {
		deadline = now.Add(time.Minute)
	}
	deleted := metav1.NewTime(deadline)
	source.DeletionTimestamp = &deleted
	priorPayload, err := json.Marshal(revision.DataPayload{PodSpec: &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
	}})
	if err != nil {
		t.Fatalf("marshal revision payload: %v", err)
	}
	priorCR := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: prior, Namespace: in.Key.Namespace}}
	priorCR.Data.Raw = priorPayload
	// The node the source pod is bound to exists with its kubelet gone: a pod
	// on a Node the cluster does not hold is read as gone, which is not this
	// fixture's story.
	objs := []client.Object{source, priorCR, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}}
	for _, idx := range []int32{1, 2, 3, 4} {
		objs = append(objs, servingEnginePod(idx))
	}
	// The replacement is in rotation: its routed Service lists it as a
	// Ready endpoint, which the move's tail gate reads every pass.
	replacement := servingEnginePod(4)
	ready := true
	return append(objs, &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "replacement-slice", Namespace: in.Key.Namespace,
			Labels: map[string]string{discoveryv1.LabelServiceName: query.PerRevisionServiceName(in.Key.OwnerName, types.ComponentEngine, replacement.Labels[query.LabelRevisionHash])},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.4"},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &ready},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: replacement.Namespace, Name: replacement.Name},
		}},
	})
}

// The pair of the fixture's move: Instance 0 is the source, index 4 the
// replacement.
const (
	teardownWaitSourceIdx int32 = 0
	teardownWaitSurgeIdx  int32 = 4
)

// teardownWaitMove is the move's operation as one row of the pair carries
// it, naming the other row as its sibling.
func teardownWaitMove(sibling int32) *types.InstanceOperation {
	return &types.InstanceOperation{Type: types.InstanceOperationMigrate, Step: "CreateSurge", RequestUUID: teardownWaitUUID, SurgeIndex: &sibling}
}

// teardownWaitRows is the Component's status: the source Migrating on the
// prior revision, Instances 1 to 3 Ready on it, and the replacement
// Creating with its one pod Ready and available.
func teardownWaitRows(prior string) []types.InstanceStatus {
	return []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseMigrating, RunningRevision: prior, Operation: teardownWaitMove(teardownWaitSurgeIdx)},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: prior},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: prior},
		{Index: 3, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: prior},
		{Index: 4, Incarnation: 1, Phase: types.InstancePhaseCreating, Operation: teardownWaitMove(teardownWaitSourceIdx), PodCount: 1, ReadyPodCount: 1, AvailablePodCount: 1},
	}
}

// teardownWaitRecord is the move's record, Draining for an hour with an
// hour left before its deadline.
func teardownWaitRecord(now time.Time) types.MigrationRecord {
	surgeIdx := teardownWaitSurgeIdx
	return types.MigrationRecord{
		RequestUUID: teardownWaitUUID, Trigger: types.MigrationTriggerManual,
		Phase: types.MigrationPhaseDraining, SourceInstance: teardownWaitSourceIdx, SurgeInstance: &surgeIdx, FromNode: "node-a",
		StartedAt: metav1.NewTime(now.Add(-time.Hour)), Deadline: metav1.NewTime(now.Add(time.Hour)),
	}
}

// routeWrites lands the pass's writes in the fixture: a record mutation on
// the record with that uuid in f.records, a row mutation on the row with
// that index, and a mutation of an index with no row creates the row and
// notes the index in created.
func (f *teardownWaitFixture) routeWrites() {
	f.input.MutateMigration = func(_ context.Context, uuid string, mutate func(*types.MigrationRecord) bool) error {
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
	f.input.MutateInstance = func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		for i := range f.input.ObservedState.InstanceStatuses {
			if f.input.ObservedState.InstanceStatuses[i].Index == idx {
				_ = fn(&f.input.ObservedState.InstanceStatuses[i])
				return nil
			}
		}
		row := types.InstanceStatus{Index: idx}
		if fn(&row) {
			f.created = append(f.created, idx)
			f.input.ObservedState.InstanceStatuses = append(f.input.ObservedState.InstanceStatuses, row)
		}
		return nil
	}
}

// teardownWaitPlan plans the engine under SurgeThenDrain maxSurge 1 with
// the pair's two indices beside the count, the way the planner keeps a
// move's replacement above the replica cap; a count above four plans the
// next free indices after them.
func teardownWaitPlan(replicas int32) types.ComponentPlan {
	plan := types.ComponentPlan{
		Component:            types.ComponentEngine,
		Replicas:             replicas,
		MigrationMode:        types.MigrationModeAuto,
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: types.UpdateStrategy{
			Type:          types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{MaxSurge: intOrStringInt(1), MaxUnavailable: intOrStringInt(0)},
		},
	}
	indices := []int32{0, 1, 2, 3, 4}
	for idx := int32(5); idx <= replicas; idx++ {
		indices = append(indices, idx)
	}
	for _, idx := range indices {
		plan.Instances = append(plan.Instances, types.InstancePlan{Index: idx, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}})
	}
	return plan
}

// keepRunningRevision puts every serving Instance on the target, so the
// pass carries no push.
func (f *teardownWaitFixture) keepRunningRevision() {
	for i := range f.input.ObservedState.InstanceStatuses {
		if f.input.ObservedState.InstanceStatuses[i].Phase == types.InstancePhaseReady {
			f.input.ObservedState.InstanceStatuses[i].RunningRevision = f.target.Name
		}
	}
}

func (f *teardownWaitFixture) pass(t *testing.T) ctrl.Result {
	t.Helper()
	res, err := workload.Reconcile(context.Background(), f.deps, f.input, f.plan, f.target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

// started lists the serving Instances whose row now carries an Update.
func (f *teardownWaitFixture) started() []int32 {
	var out []int32
	for _, idx := range []int32{1, 2, 3} {
		if row := f.input.ObservedState.Instance(idx); row != nil && row.Operation != nil && row.Operation.Type == types.InstanceOperationUpdate {
			out = append(out, idx)
		}
	}
	return out
}

// requirePairKept fails unless both rows of the move still carry its
// operation and the record still reads Draining.
func (f *teardownWaitFixture) requirePairKept(t *testing.T) {
	t.Helper()
	source := f.input.ObservedState.Instance(0)
	if source == nil || source.Phase != types.InstancePhaseMigrating || source.Operation == nil || source.Operation.Type != types.InstanceOperationMigrate {
		t.Fatalf("the source row stays the move's; got %+v", source)
	}
	surge := f.input.ObservedState.Instance(4)
	if surge == nil || surge.Phase != types.InstancePhaseCreating || surge.Operation == nil || surge.Operation.Type != types.InstanceOperationMigrate {
		t.Fatalf("the replacement row stays the move's; got %+v", surge)
	}
	if got := f.records[0]; got.Phase != types.MigrationPhaseDraining {
		t.Fatalf("the record keeps its Draining phase; got %+v", got)
	}
}

// Four Instances serve version 1 and the move of Instance 0 waits on a
// kubelet that is gone: its source pod is Terminating past its own
// deletion deadline on that node, and nothing but that kubelet or an
// operator can remove it. The operator pushes version 2: one of the other three Instances
// starts its surge under the budget of one in the same pass, while the
// pair's two rows and the record stay the move's. The operator raises the
// count from 4 to 6: the two new Instances are created in the same pass.
// The record still names its wait and keeps the pass's cadence.
func TestReconcile_MoveWaitingOnItsKubeletYieldsToThePushAndTheScaleUp(t *testing.T) {
	t.Run("push", func(t *testing.T) {
		f := newTeardownWaitFixture(t, 4, false)
		res := f.pass(t)
		if got := f.started(); len(got) != 1 {
			t.Fatalf("the push starts one Instance beside the waiting move under maxSurge 1; started %v", got)
		}
		f.requirePairKept(t)
		if !strings.Contains(f.records[0].Message, "parked on source teardown") {
			t.Fatalf("the record names its wait; got %q", f.records[0].Message)
		}
		if res.RequeueAfter <= 0 || res.RequeueAfter > testRequeueIntervals.Operation {
			t.Fatalf("the pass keeps the waiting record's cadence; got %+v", res)
		}
	})
	t.Run("scale-up", func(t *testing.T) {
		f := newTeardownWaitFixture(t, 6, false)
		f.keepRunningRevision()
		res := f.pass(t)
		if len(f.created) != 2 || f.created[0] != 5 || f.created[1] != 6 {
			t.Fatalf("the scale-up creates its two Instances beside the waiting move; created %v", f.created)
		}
		for _, idx := range []int32{5, 6} {
			pod := &corev1.Pod{}
			key := client.ObjectKey{Namespace: f.input.Key.Namespace, Name: enginePod(f.input.Key.OwnerName, f.input.Key.Namespace, idx).Name}
			if err := f.client.Get(context.Background(), key, pod); err != nil {
				t.Fatalf("the new Instance %d has its pod requested: %v", idx, err)
			}
		}
		f.requirePairKept(t)
		if res.RequeueAfter <= 0 || res.RequeueAfter > testRequeueIntervals.Operation {
			t.Fatalf("the pass keeps the waiting record's cadence; got %+v", res)
		}
	})
}

// The same move with its source pod still inside its deletion grace is a
// move making progress: it serializes the Component's pass, so a push
// starts nothing and a scale-up creates nothing until the move ends.
func TestReconcile_MoveInFlightStillSerializesThePass(t *testing.T) {
	t.Run("push", func(t *testing.T) {
		f := newTeardownWaitFixture(t, 4, true)
		res := f.pass(t)
		if got := f.started(); len(got) != 0 {
			t.Fatalf("a move in flight ends the pass ahead of the roll; started %v", got)
		}
		f.requirePairKept(t)
		if res.RequeueAfter != testRequeueIntervals.Operation {
			t.Fatalf("a move in flight paces the pass at the Migrate interval; got %+v", res)
		}
	})
	t.Run("scale-up", func(t *testing.T) {
		f := newTeardownWaitFixture(t, 6, true)
		f.keepRunningRevision()
		res := f.pass(t)
		if len(f.created) != 0 {
			t.Fatalf("a move in flight ends the pass ahead of the Create pass; created %v", f.created)
		}
		f.requirePairKept(t)
		if res.RequeueAfter != testRequeueIntervals.Operation {
			t.Fatalf("a move in flight paces the pass at the Migrate interval; got %+v", res)
		}
	})
}

// A pause does not spend a move's window. Three single-pod Instances serve
// version 1 under a thirty-minute ready timeout. The operator asks the third
// off its node, the move surges a replacement whose readiness probe never
// passes, and ten seconds later the operator pauses the Component. The
// pause clears the record's deadline with the deadlines of the move's two
// rows. Thirty-one minutes under the pause pass. The first pass after the
// pause clears keeps the move open: its record reads SurgePending with a
// full window measured from that pass. The move closes Failed only when
// that window runs out, with the source back at Ready. The deadline step
// the InferenceReplica reconciler runs around every pass runs here too.
func TestReconcile_MovePausedPastItsWindowKeepsTheMoveAndRunsAFreshWindow(t *testing.T) {
	const move = "move-off-node"
	const moved = int32(2)
	const window = 30 * time.Minute
	h := newRecoveryHarness(t, false)
	h.atomicStatus = true
	h.routed = true
	h.replicas = 3
	h.lifecycle = types.Lifecycle{InstanceReadyTimeout: &metav1.Duration{Duration: window}}
	h.useNodes("node-a", "node-b", "node-c", "node-d")
	h.migrationAudit = &types.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 10, Window: time.Hour}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 3) }) {
		h.dumpState("initial create")
		t.Fatalf("the Component never settled on v1")
	}

	// The adapter's deadline step: under the pause every row is held and
	// the move records follow; a released clock starts again from now.
	holdDeadlines := func(paused bool) {
		t.Helper()
		in := h.buildInput()
		rows := h.irStatuses()
		held := map[int32]bool{}
		if paused {
			for _, row := range rows {
				held[row.Index] = true
			}
		}
		if err := escalation.ReconcileGatedDeadlines(h.ctx, in, rows, held, window); err != nil {
			t.Fatalf("ReconcileGatedDeadlines: %v", err)
		}
		if err := escalation.ReconcilePausedMigrationDeadlines(h.ctx, in, paused, window); err != nil {
			t.Fatalf("ReconcilePausedMigrationDeadlines: %v", err)
		}
	}
	// passLikeTheAdapter runs one reconcile the way the InferenceReplica
	// reconciler does: the clocks are held before a paused pass and settled
	// after every pass. The test moves the clock itself between passes.
	passLikeTheAdapter := func() {
		t.Helper()
		if h.desired.Paused {
			holdDeadlines(true)
		}
		if _, err := h.pass(); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		holdDeadlines(h.desired.Paused)
	}

	// The move is admitted and surges its replacement, which never passes
	// its readiness probe.
	h.requestMove(move, moved)
	h.migration(move).Deadline = metav1.NewTime(h.clk.Now().Add(window))
	passLikeTheAdapter()
	rec := h.migration(move)
	if rec.Phase != types.MigrationPhaseSurgePending || rec.SurgeInstance == nil {
		h.dumpState("after the move request")
		t.Fatalf("the move must be admitted at once; got %+v", *rec)
	}
	replacement := h.podsOf(*rec.SurgeInstance)
	if len(replacement) == 0 {
		h.dumpState("after the move request")
		t.Fatalf("the move created no replacement pod at index %d", *rec.SurgeInstance)
	}
	for _, pod := range replacement {
		h.readinessFails[pod.UID] = true
	}

	// Ten seconds later the operator pauses the Component: the record's
	// clock is cleared with the pair's.
	h.clk.Step(10 * time.Second)
	h.desired.Paused = true
	passLikeTheAdapter()
	if rec = h.migration(move); rec.Phase != types.MigrationPhaseSurgePending || !rec.Deadline.IsZero() {
		t.Fatalf("under the pause the record must stand SurgePending with no deadline; got %+v", *rec)
	}

	// Thirty-one minutes pass under the pause: the move stands.
	h.clk.Step(window + time.Minute)
	passLikeTheAdapter()
	if rec = h.migration(move); rec.Phase.Terminal() || !rec.Deadline.IsZero() {
		t.Fatalf("a paused pass changed the record; got %+v", *rec)
	}

	// The pause clears. The first pass keeps the move and gives it a full
	// window measured from this pass; the source is still being moved.
	h.desired.Paused = false
	cleared := h.clk.Now()
	passLikeTheAdapter()
	rec = h.migration(move)
	if rec.Phase.Terminal() {
		t.Fatalf("the move was closed in the pass the pause cleared: %s", rec.Message)
	}
	if rec.Phase != types.MigrationPhaseSurgePending {
		t.Fatalf("the move must still read SurgePending after the pause; got %+v", *rec)
	}
	if want := cleared.Add(window); !rec.Deadline.Time.Equal(want) {
		t.Fatalf("the move's deadline = %s, want %s: a full window from the pass the pause clears", rec.Deadline.Time, want)
	}
	if source := h.instance(moved); source == nil || source.Phase != types.InstancePhaseMigrating {
		t.Fatalf("the source must still be Migrating after the pause; got %+v", source)
	}

	// That window runs out with the replacement still not Ready: the expiry
	// closes the move and restores the source.
	h.clk.Step(window + time.Minute)
	passLikeTheAdapter()
	rec = h.migration(move)
	if rec.Phase != types.MigrationPhaseFailed || !strings.Contains(rec.Message, "deadline exceeded in phase SurgePending") {
		t.Fatalf("the move must close on the window measured from the unpause; got %+v", *rec)
	}
	if source := h.instance(moved); source == nil || source.Phase != types.InstancePhaseReady {
		t.Fatalf("the source must be restored to Ready when the move closes; got %+v", source)
	}
}

// A move's deadline comes due while another Instance's repair is in
// flight. Three single-pod Instances serve; the operator asks the third
// off its node, so a replacement is surged at a fourth index and never
// comes up; then the second Instance loses its pod, so the restart policy
// rebuilds it, and the rebuilt pod is still coming up when the move's
// deadline passes. The move is closed in the pass its deadline comes due:
// the record reads Failed naming the deadline, the source returns to
// Ready on the revision it kept serving, the replacement index is
// released and torn down, and the repair goes on untouched until its
// rebuilt pod serves. A repair in flight consumes the pass, so the expiry
// stands ahead of it.
func TestReconcile_MoveDeadlineDueBesideARepairClosesTheMoveThatPass(t *testing.T) {
	const move = "move-off-node"
	const repaired, moved = int32(1), int32(2)
	h := newRecoveryHarness(t, false)
	h.recorder = record.NewFakeRecorder(256)
	h.atomicStatus = true
	h.routed = true
	h.replicas = 3
	recreate := types.RestartPolicyRecreateInstance
	h.lifecycle.RestartPolicy = &recreate
	h.useNodes("node-a", "node-b", "node-c", "node-d")
	h.migrationAudit = &types.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 10, Window: time.Hour}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 3) }) {
		h.dumpState("initial create")
		t.Fatalf("the Component never settled on v1")
	}

	h.requestMove(move, moved)
	h.step()
	rec := h.migration(move)
	if rec == nil || rec.Phase != types.MigrationPhaseSurgePending || rec.SurgeInstance == nil {
		h.dumpState("after the move request")
		t.Fatalf("the move did not surge a replacement: %+v", rec)
	}
	replacement := *rec.SurgeInstance
	// The replacement runs but never passes its readiness probe, so the
	// move never gets past waiting for it. No repair reading acts on that
	// shape; only the clock moves the move.
	h.failReadiness(replacement, "")

	generation := h.instance(repaired).Incarnation
	h.losePods(repaired)
	h.step()
	row := h.instance(repaired)
	if row == nil || row.Phase != types.InstancePhaseRestarting || row.Operation == nil || row.Operation.Type != types.InstanceOperationRestart {
		h.dumpState("after the pod loss")
		t.Fatalf("the restart policy did not open a repair on instance %d: %+v", repaired, row)
	}
	if pods := h.podsOf(repaired); len(pods) != 1 || podConditionTrue(pods[0], corev1.PodReady) {
		h.dumpState("after the pod loss")
		t.Fatalf("instance %d should have one rebuilt pod still coming up; got %d pod(s)", repaired, len(pods))
	}
	if n := h.restartsRecorded(); n != 1 {
		t.Fatalf("restarts recorded = %d, want the one repair the pod loss opened: %v", n, h.events)
	}
	// The rebuilt pod runs but fails its readiness probe until the fault
	// is removed, so the repair stays in flight.
	h.failReadiness(repaired, "")

	// The deadline comes due with the rebuilt pod still coming up.
	h.migration(move).Deadline = metav1.NewTime(h.clk.Now().Add(-time.Second))
	h.events = nil
	h.step()
	rec = h.migration(move)
	if !rec.Phase.Terminal() {
		phaseAtDeadline := rec.Phase
		openPasses := 0
		for openPasses < 5 && !h.migration(move).Phase.Terminal() {
			h.step()
			openPasses++
		}
		stillOpen := !h.migration(move).Phase.Terminal()
		h.restoreReadiness(repaired, "")
		closedAfter := 0
		for closedAfter < 10 && !h.migration(move).Phase.Terminal() {
			h.step()
			closedAfter++
		}
		h.dumpState("the move outlived its deadline")
		t.Fatalf("the move read %s in the pass its deadline came due; still open after %d more passes with the repair in flight: %v; closed %d pass(es) after the rebuilt pod was let start, with the repair complete: %v (record %+v)",
			phaseAtDeadline, openPasses, stillOpen, closedAfter, !h.repairInFlight(), *h.migration(move))
	}
	if rec.Phase != types.MigrationPhaseFailed || rec.CompletedAt == nil ||
		rec.Message != "deadline exceeded in phase SurgePending: surge pods never became ready" {
		t.Fatalf("the move must close Failed naming its deadline; got %+v", *rec)
	}
	if !h.sawEventWith(types.EventReasonMigrationExpired, "deadline exceeded in phase SurgePending") {
		t.Fatalf("no MigrationExpired event named the deadline: %v", h.events)
	}
	if source := h.instance(moved); source == nil || source.Phase != types.InstancePhaseReady || source.Operation != nil || source.RunningRevision != h.revV1.Name {
		h.dumpState("after the expiry")
		t.Fatalf("the source must return to Ready on the revision it kept serving; got %+v", source)
	}
	if surge := h.instance(replacement); surge == nil || surge.Operation != nil {
		h.dumpState("after the expiry")
		t.Fatalf("the replacement index must be released by the expiry; got %+v", surge)
	}
	if row := h.instance(repaired); row == nil || row.Phase != types.InstancePhaseRestarting || row.Operation == nil ||
		row.Operation.Type != types.InstanceOperationRestart || row.Incarnation != generation+1 {
		h.dumpState("after the expiry")
		t.Fatalf("the repair must go on untouched beside the expiry; got %+v", row)
	}

	// The released replacement is torn down while the repair still waits.
	if !h.run(10, func() bool { return h.instance(replacement) == nil && len(podObjectsOf(h, replacement)) == 0 }) {
		h.dumpState("tearing down the replacement")
		t.Fatalf("the released replacement %d was not torn down", replacement)
	}
	if row := h.instance(repaired); row == nil || row.Phase != types.InstancePhaseRestarting || row.Incarnation != generation+1 {
		h.dumpState("after the teardown")
		t.Fatalf("the repair must still be the one that opened on the pod loss; got %+v", row)
	}

	// The rebuilt pod passes its readiness probe: the repair completes and
	// the Component is whole.
	h.restoreReadiness(repaired, "")
	if !h.run(20, func() bool { return h.settledOn(h.revV1, 3) }) {
		h.dumpState("after the rebuilt pod started")
		t.Fatalf("the Component never settled back on three Instances")
	}
	if row := h.instance(repaired); row.Incarnation != generation+1 {
		t.Fatalf("instance %d generation number = %d, want %d: the Instance is rebuilt once", repaired, row.Incarnation, generation+1)
	}
	if n := h.restartsRecorded(); n != 0 {
		t.Fatalf("%d repair(s) opened after the pod loss's, want none: %v", n, h.events)
	}
	if !h.sawEvent(types.EventReasonRestartCompleted) {
		t.Fatalf("the repair never reported complete: %v", h.events)
	}
}

// A move requested while a roll is in flight waits for the roll. Four
// single-pod Instances serve version 1 under SurgeThenDrain with a surge
// budget of two. The operator pushes version 2, so Instances 0 and 1
// surge replacements that run but stay not Ready, and then asks Instance
// 2 off its node. The request is recorded, and the record and one event
// say it waits for the roll. Its deadline does not run while the roll is
// in flight: thirty-one minutes later the record is still Accepted. Once
// both surge replacements are released the move starts on the next pass,
// ahead of the roll's next surge, with its deadline measured from then,
// and the push waits for the move as it always has.
func TestReconcile_MoveRequestedDuringARollWaitsUntilNoSurgeIsInFlight(t *testing.T) {
	const move = "move-off-node"
	const moved = int32(2)
	const window = 30 * time.Minute
	h := newMoveWaitsForRollHarness(t)
	moveWaitsForRollPush(t, h)
	moveWaitsForRollRequest(t, h, move, moved, window)
	moveWaitsForRollWindowPasses(t, h, move, window)
	surgesReleased := moveWaitsForRollSurgesReleased(t, h, move)
	moveWaitsForRollStarts(t, h, move, moved, window, surgesReleased)
	moveWaitsForRollFinishes(t, h, move)
}

// newMoveWaitsForRollHarness builds the story's Component: four single-pod
// Instances settled on version 1 under SurgeThenDrain with a surge budget of
// two, nodes for a move and a migration capacity policy.
func newMoveWaitsForRollHarness(t *testing.T) *recoveryHarness {
	t.Helper()
	h := newRecoveryHarness(t, false)
	h.recorder = record.NewFakeRecorder(256)
	h.atomicStatus = true
	h.routed = true
	h.replicas = 4
	maxSurge := intstr.FromInt32(2)
	maxUnavailable := intstr.FromInt32(0)
	h.lifecycle = types.Lifecycle{
		UpdateStrategy: &types.UpdateStrategy{
			Type:          types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
		},
		// Long enough that the roll's own attempts outlive the move's
		// thirty-minute window.
		InstanceReadyTimeout: &metav1.Duration{Duration: 2 * time.Hour},
	}
	h.useNodes("node-a", "node-b", "node-c", "node-d", "node-e")
	h.migrationAudit = &types.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 10, Window: time.Hour}
	h.setTarget(h.revV1, goodImage)
	if !h.run(40, func() bool { return h.settledOn(h.revV1, 4) }) {
		h.dumpState("initial create")
		t.Fatalf("the Component never settled on v1")
	}
	return h
}

// moveWaitsForRollSurging counts the roll's surge replacements in flight.
func moveWaitsForRollSurging(h *recoveryHarness) int32 {
	return escalation.CurrentSurgeInFlight(h.irStatuses())
}

// moveWaitsForRollReplacementsReady lets the first two Instances'
// replacements pass readiness, or holds them short of it.
func moveWaitsForRollReplacementsReady(t *testing.T, h *recoveryHarness, ready bool) {
	t.Helper()
	for _, idx := range []int32{0, 1} {
		pods := h.podsOfOnImage(idx, fixedImage)
		if len(pods) == 0 {
			t.Fatalf("instance %d has no replacement on the pushed image", idx)
		}
		for _, pod := range pods {
			if ready {
				delete(h.readinessFails, pod.UID)
			} else {
				h.readinessFails[pod.UID] = true
			}
		}
	}
}

// moveWaitsForRollPush pushes version 2: the first two Instances surge
// replacements under the budget of two; the replacements run but never
// pass readiness, so the roll holds both slots.
func moveWaitsForRollPush(t *testing.T, h *recoveryHarness) {
	t.Helper()
	h.setTarget(h.revFixed, fixedImage)
	h.step()
	if n := moveWaitsForRollSurging(h); n != 2 {
		h.dumpState("after the push")
		t.Fatalf("%d surge(s) in flight after the push, want 2", n)
	}
	moveWaitsForRollReplacementsReady(t, h, false)
}

// moveWaitsForRollRequest asks the Instance off its node while the roll's
// surges are in flight: the request is recorded with the window as its
// deadline, and the record and one event say the move waits for the roll.
func moveWaitsForRollRequest(t *testing.T, h *recoveryHarness, move string, moved int32, window time.Duration) {
	t.Helper()
	h.requestMove(move, moved)
	accepted := h.clk.Now()
	h.migration(move).Deadline = metav1.NewTime(accepted.Add(window))
	h.events = nil
	h.step()
	rec := h.migration(move)
	if rec.Phase != types.MigrationPhaseAccepted || rec.SurgeInstance != nil {
		h.dumpState("after the move request")
		t.Fatalf("the move must wait for the roll, not start beside it; got %+v", *rec)
	}
	if !strings.Contains(rec.Message, "waiting for the roll") || !strings.Contains(rec.Message, h.revFixed.Name) {
		t.Fatalf("the record must say it waits for the roll to %s; got %q", h.revFixed.Name, rec.Message)
	}
	if !h.sawEventWith(types.EventReasonMigrationDeferred, "waiting for the roll") {
		t.Fatalf("no event said the move waits for the roll: %v", h.events)
	}
}

// moveWaitsForRollWindowPasses lets thirty-one minutes pass with the roll
// still in flight. The window the request was accepted with is behind the
// record, and the record is still open: its deadline does not run while it
// waits for the roll, and a pass of the wait says nothing.
func moveWaitsForRollWindowPasses(t *testing.T, h *recoveryHarness, move string, window time.Duration) {
	t.Helper()
	h.clk.Step(window + time.Minute)
	h.events = nil
	h.step()
	rec := h.migration(move)
	if rec.Phase.Terminal() {
		t.Fatalf("the move was closed while the roll was in flight: %s", rec.Message)
	}
	if rec.Phase != types.MigrationPhaseAccepted || rec.SurgeInstance != nil {
		t.Fatalf("the move must still wait for the roll; got %+v", *rec)
	}
	if n := moveWaitsForRollSurging(h); n != 2 {
		h.dumpState("while the move waits")
		t.Fatalf("%d surge(s) in flight while the move waits, want the roll's 2", n)
	}
	if n := len(h.events); n != 0 {
		t.Fatalf("a pass of the wait raised %d event(s), want none: %v", n, h.events)
	}
}

// moveWaitsForRollSurgesReleased lets the replacements pass readiness: the
// roll's first two Instances promote and both surge slots are released. It
// returns the time of the release; the record has not moved yet.
func moveWaitsForRollSurgesReleased(t *testing.T, h *recoveryHarness, move string) time.Time {
	t.Helper()
	moveWaitsForRollReplacementsReady(t, h, true)
	if !h.run(20, func() bool { return moveWaitsForRollSurging(h) == 0 }) {
		h.dumpState("the roll never released its surges")
		t.Fatalf("the roll's surges were never released")
	}
	surgesReleased := h.clk.Now()
	if rec := h.migration(move); rec.Phase != types.MigrationPhaseAccepted {
		t.Fatalf("the move changed phase before the pass after the surges were released; got %+v", *rec)
	}
	return surgesReleased
}

// moveWaitsForRollStarts runs the pass after the release: the move starts,
// its replacement is allocated and its deadline is the accepted window
// measured from the surges' release. The push waits for the move, so no
// further surge opens in that pass.
func moveWaitsForRollStarts(t *testing.T, h *recoveryHarness, move string, moved int32, window time.Duration, surgesReleased time.Time) {
	t.Helper()
	h.step()
	rec := h.migration(move)
	if rec.Phase != types.MigrationPhaseSurgePending || rec.SurgeInstance == nil {
		h.dumpState("the pass after the surges were released")
		t.Fatalf("the move must start in the pass after the surges were released; got %+v", *rec)
	}
	if rec.Deadline.Time.Before(surgesReleased.Add(window)) {
		t.Fatalf("the move's deadline = %s, want at least %s: the window measured from the surges' release", rec.Deadline.Time, surgesReleased.Add(window))
	}
	if n := moveWaitsForRollSurging(h); n != 0 {
		h.dumpState("the pass after the surges were released")
		t.Fatalf("%d surge(s) opened beside the move, want none: the push waits for the move", n)
	}
	if source := h.instance(moved); source == nil || source.Phase != types.InstancePhaseMigrating {
		t.Fatalf("the source must be Migrating once the move starts; got %+v", source)
	}
}

// moveWaitsForRollFinishes drives the move to completion; the roll then
// finishes on every Instance.
func moveWaitsForRollFinishes(t *testing.T, h *recoveryHarness, move string) {
	t.Helper()
	if !h.run(60, func() bool { return h.migration(move).Phase.Terminal() }) {
		h.dumpState("the move never finished")
		t.Fatalf("the move did not finish; record=%+v", *h.migration(move))
	}
	if rec := h.migration(move); rec.Phase != types.MigrationPhaseCompleted {
		t.Fatalf("the move must complete; got %+v", *rec)
	}
	if !h.run(80, func() bool { return h.settledOn(h.revFixed, 4) }) {
		h.dumpState("the roll never finished")
		t.Fatalf("the push did not land on every Instance after the move")
	}
}
