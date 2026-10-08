package replay

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestRemoveInstanceForgetsTheExpectation pins the adapter behavior the
// driver stands in for: a row that left the owner status takes its
// create/delete expectation with it, so a later Instance reusing the index
// does not inherit a wait nobody will satisfy.
func TestRemoveInstanceForgetsTheExpectation(t *testing.T) {
	ctx := context.Background()
	scenario, err := Parse([]byte(minimalScenario), Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	d, err := newDriver(ctx, scenario, Options{})
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	if err := d.store.MutateInstance(ctx, 0, func(s *types.InstanceStatus) bool {
		s.Phase = types.InstancePhaseCreating
		return true
	}); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	d.expectations.ExpectCreates(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0, 1)
	if d.expectations.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatalf("an outstanding create must not read as satisfied")
	}

	input := types.ReconcileInput{OwnerObject: d.owner}
	d.store.Install(&input)
	d.wrapCallbacks(&input)
	if _, err := input.RemoveInstance(ctx, 0); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !d.expectations.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatalf("removing the row left its expectation behind")
	}
}

// resumedSurge seeds a surge already in flight: the row carries the
// operation and the replacement exists beside the serving source.
const resumedSurge = `
scenario: resumed-surge
arrows: [T-surge-target-ready]
initial:
  spec:
    replicas: 1
    strategy: SurgeThenDrain
    image: registry.example.com/runtime:v2
    instanceReadyTimeout: 30m
  currentRevision: registry.example.com/runtime:v1
  rows:
    - index: 0
      phase: Updating
      runningRevision: registry.example.com/runtime:v1
      targetRevision: current
      readySince: 0s
      operation: {type: Update, step: Surge, strategy: SurgeThenDrain, startedAt: 2m}
  pods:
    - {index: 0, ordinal: 0, previousImage: registry.example.com/runtime:v1, ready: true, serving: true, routed: true}
    - {index: 0, ordinal: 1, phase: Pending}
timeline:
  - tick: 1
`

func mustDriver(t *testing.T, text string) *driver {
	t.Helper()
	scenario, err := Parse([]byte(text), Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	d, err := newDriver(context.Background(), scenario, Options{})
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	return d
}

func driverError(t *testing.T, text string) error {
	t.Helper()
	scenario, err := Parse([]byte(text), Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = newDriver(context.Background(), scenario, Options{})
	return err
}

// TestRevisionImageNamesTwoForms pins the revision vocabulary a seed may
// use: the initial spec's template, or the image whose template minted
// the revision. Anything else is a typo, not a third symbol.
func TestRevisionImageNamesTwoForms(t *testing.T) {
	if got, err := revisionImage("current", "registry.example.com/runtime:v3"); err != nil || got != "registry.example.com/runtime:v3" {
		t.Fatalf("current: got (%q, %v)", got, err)
	}
	if got, err := revisionImage("registry.example.com/runtime:v1", "unused"); err != nil || got != "registry.example.com/runtime:v1" {
		t.Fatalf("image: got (%q, %v)", got, err)
	}
	if got, err := revisionImage("", "unused"); err != nil || got != "" {
		t.Fatalf("empty: got (%q, %v)", got, err)
	}
	if _, err := revisionImage("previous", "unused"); err == nil {
		t.Fatal("a bare word is not a revision name")
	}
}

// TestSeededOperationIsTheEngineStamp pins that a seeded operation carries
// what the engine's own stamp writes: the id shape, the pinned target, the
// deadline derived from instanceReadyTimeout, and a state of the machine.
func TestSeededOperationIsTheEngineStamp(t *testing.T) {
	d := mustDriver(t, resumedSurge)
	rows := d.store.Rows()
	if len(rows) != 1 || rows[0].Operation == nil {
		t.Fatalf("expected one row with an operation, got %+v", rows)
	}
	row, op := rows[0], rows[0].Operation
	if types.StateOf(&row) != types.StateUpdateSurge {
		t.Fatalf("state: got %q", types.StateOf(&row))
	}
	start := traceStart.Add(2 * time.Minute)
	if want := fmt.Sprintf("update-0-%d", start.Unix()); op.ID != want {
		t.Fatalf("id: got %q want %q", op.ID, want)
	}
	if op.TargetRevision == "" || op.TargetRevision != row.TargetRevision {
		t.Fatalf("an Update operation takes the row's target: op=%q row=%q", op.TargetRevision, row.TargetRevision)
	}
	if row.RunningRevision == "" || row.RunningRevision == row.TargetRevision {
		t.Fatalf("the running revision must be the previous image's: running=%q target=%q", row.RunningRevision, row.TargetRevision)
	}
	if !op.StartedAt.Time.Equal(start) || !op.LastProgressAt.Time.Equal(start) {
		t.Fatalf("timing: started=%v lastProgress=%v want %v", op.StartedAt.Time, op.LastProgressAt.Time, start)
	}
	if want := start.Add(30 * time.Minute); !op.Deadline.Time.Equal(want) {
		t.Fatalf("deadline: got %v want %v", op.Deadline.Time, want)
	}
	if d.currentRevision != row.RunningRevision {
		t.Fatalf("initial.currentRevision must resolve to the previous image's revision: got %q", d.currentRevision)
	}
}

// TestSeededRowOutsideTheMachineFailsTheRun keeps a seed honest: a shape
// the engine's ownership table (types.StateOf) does not name is nothing any pass drives.
func TestSeededRowOutsideTheMachineFailsTheRun(t *testing.T) {
	bogusStep := strings.Replace(resumedSurge, "step: Surge", "step: Sideways", 1)
	if err := driverError(t, bogusStep); err == nil || !strings.Contains(err.Error(), "names no state") {
		t.Fatalf("an unknown step must fail the run: %v", err)
	}
	noPhase := strings.Replace(resumedSurge, "      phase: Updating\n", "", 1)
	if err := driverError(t, noPhase); err == nil || !strings.Contains(err.Error(), "needs the phase") {
		t.Fatalf("an operation without a phase must fail the run: %v", err)
	}
	creatingWithoutOp := `
scenario: creating-without-op
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows: [{index: 0, phase: Creating}]
timeline:
  - tick: 1
`
	if err := driverError(t, creatingWithoutOp); err == nil || !strings.Contains(err.Error(), "names no state") {
		t.Fatalf("Creating with no operation is not a state: %v", err)
	}
}

// TestSeededMigratePinNeedsItsRecord pins that a Migrate pin is only ever
// seeded beside the status.migrations record it resumes from.
func TestSeededMigratePinNeedsItsRecord(t *testing.T) {
	const pinned = `
scenario: pinned
arrows: [T-ready-migrate]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1, instanceReadyTimeout: 30m}
  rows:
    - index: 0
      phase: Migrating
      runningRevision: current
      operation: {type: Migrate, step: CreateSurge, surgeIndex: 1, requestUUID: move-1}
timeline:
  - tick: 1
`
	if err := driverError(t, pinned); err == nil || !strings.Contains(err.Error(), "initial.migrations") {
		t.Fatalf("a pin without its record must fail the run: %v", err)
	}
	withRecord := strings.Replace(pinned, "  rows:\n", "  migrations:\n    - {uuid: move-1, instance: 0, surgeIndex: 1}\n  rows:\n", 1)
	d := mustDriver(t, withRecord)
	if got := d.store.Rows()[0].Operation.ID; got != fmt.Sprintf("migrate-move-1-%d", traceStart.Unix()) {
		t.Fatalf("migrate pin id: got %q", got)
	}
}

// TestSeedMigrationsDerivesTheRecord pins the record shape a seed produces:
// an allocated record reads SurgePending with its allocation instant, a
// queued one reads Accepted, and both carry the deadline the acceptance
// derives from instanceReadyTimeout.
func TestSeedMigrationsDerivesTheRecord(t *testing.T) {
	d := &driver{opts: DefaultOptions(), spec: SpecState{InstanceReadyTimeout: "30m"}}
	one := int32(1)
	if err := d.seedMigrations([]MigrationSpec{
		{Instance: 0, SurgeIndex: &one, FromNode: "node-0", StartedAt: "1m"},
		{UUID: "queued", Instance: 2},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if len(d.migrations) != 2 {
		t.Fatalf("got %d records", len(d.migrations))
	}
	allocated, queued := d.migrations[0], d.migrations[1]
	if allocated.RequestUUID != "migration-1" || allocated.Phase != types.MigrationPhaseSurgePending || allocated.SurgeInstance == nil || *allocated.SurgeInstance != 1 {
		t.Fatalf("allocated record: %+v", allocated)
	}
	start := traceStart.Add(time.Minute)
	if allocated.AllocatedAt == nil || !allocated.AllocatedAt.Time.Equal(start) || !allocated.Deadline.Time.Equal(start.Add(30*time.Minute)) {
		t.Fatalf("allocated timing: %+v", allocated)
	}
	if queued.RequestUUID != "queued" || queued.Phase != types.MigrationPhaseAccepted || queued.AllocatedAt != nil || queued.SurgeInstance != nil {
		t.Fatalf("queued record: %+v", queued)
	}
	if err := d.seedMigrations([]MigrationSpec{{UUID: "queued", Instance: 2}}); err == nil {
		t.Fatal("a duplicate uuid must fail the run")
	}
}

// TestSeededTerminatingPodIsHeldOpen pins that a pod seeded Terminating is
// the object pod.terminating leaves: deleted, stamped on the injected
// clock, and held by the driver's finalizer.
func TestSeededTerminatingPodIsHeldOpen(t *testing.T) {
	const terminating = `
scenario: terminating
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods: [{index: 0, ready: true, terminating: true}]
timeline:
  - tick: 1
`
	d := mustDriver(t, terminating)
	name := d.podName(PodRef{Index: 0})
	hold := d.terminating[name]
	if hold == nil || hold.kubeletStuck || !hold.at.Equal(traceStart) {
		t.Fatalf("teardown record: %+v", hold)
	}
	pod := &corev1.Pod{}
	if err := d.cli.Get(context.Background(), client.ObjectKey{Namespace: d.opts.Namespace, Name: name}, pod); err != nil {
		t.Fatalf("get: %v", err)
	}
	if pod.DeletionTimestamp == nil || !contains(pod.Finalizers, podLifecycleFinalizer) {
		t.Fatalf("pod must read Terminating behind the finalizer: deletion=%v finalizers=%v", pod.DeletionTimestamp, pod.Finalizers)
	}
}
