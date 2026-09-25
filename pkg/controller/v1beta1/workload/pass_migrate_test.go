package workload_test

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
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
