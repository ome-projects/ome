package inferencereplica

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

// Tests for the status.migrations adapter seam: the MutateMigration
// RMW closure, the observed-state mirror, the ledger upgrade import,
// the terminal-entry trim, and the aggregator-preservation regression.

// acceptedEntry is the shape the accept pass persists.
func acceptedEntry(uuid string) v1beta1.MigrationStatus {
	return v1beta1.MigrationStatus{
		RequestUUID:    uuid,
		Trigger:        v1beta1.MigrationTriggerManual,
		Phase:          v1beta1.MigrationPhaseAccepted,
		SourceInstance: 0,
		FromNode:       "node-a",
		Reason:         "maintenance",
		StartedAt:      metav1.NewTime(migrationTestNow),
		Deadline:       metav1.NewTime(migrationTestNow.Add(migrationTestTimeout)),
	}
}

// TestBuildMutateMigration_PersistsAndMirrors pins the seam: the
// callback's mutation lands on the apiserver (proven by re-read) and
// the committed slice mirrors back onto the caller's in-memory IR.
func TestBuildMutateMigration_PersistsAndMirrors(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.Migrations = []v1beta1.MigrationStatus{acceptedEntry("u-1")}
	r, c := newReconciler(t, ir)

	mutate := buildMutateMigration(r.statusWriter(), r.Client, ir)
	idx := int32(3)
	g.Expect(mutate(context.Background(), "u-1", func(m *workload.MigrationRecord) bool {
		g.Expect(m.Phase).To(gomega.Equal(workload.MigrationPhaseAccepted))
		m.SurgeInstance = &idx
		m.Phase = workload.MigrationPhaseSurgePending
		m.Message = "surge allocated"
		return true
	})).To(gomega.Succeed())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.Migrations).To(gomega.HaveLen(1))
	e := got.Status.Migrations[0]
	g.Expect(e.Phase).To(gomega.Equal(v1beta1.MigrationPhaseSurgePending))
	g.Expect(e.SurgeInstance).NotTo(gomega.BeNil())
	g.Expect(*e.SurgeInstance).To(gomega.Equal(int32(3)))
	g.Expect(e.Message).To(gomega.Equal("surge allocated"))
	g.Expect(ir.Status.Migrations).To(gomega.Equal(got.Status.Migrations),
		"the committed Migrations must be mirrored back onto the in-memory IR")
}

// TestBuildMutateMigration_MissingEntryNoOp pins that mutating an
// absent UUID is a clean no-op — no write, callback never invoked.
func TestBuildMutateMigration_MissingEntryNoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	r, c := newReconciler(t, ir)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	before := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, before)).To(gomega.Succeed())

	mutate := buildMutateMigration(r.statusWriter(), r.Client, ir)
	called := false
	g.Expect(mutate(context.Background(), "u-gone", func(_ *workload.MigrationRecord) bool {
		called = true
		return true
	})).To(gomega.Succeed())
	g.Expect(called).To(gomega.BeFalse(), "callback must not run against a phantom entry")

	after := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, after)).To(gomega.Succeed())
	g.Expect(after.ResourceVersion).To(gomega.Equal(before.ResourceVersion), "zero writes")
}

// TestBuildMutateMigration_UnchangedWritesNothing pins the false-return
// short-circuit.
func TestBuildMutateMigration_UnchangedWritesNothing(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.Migrations = []v1beta1.MigrationStatus{acceptedEntry("u-1")}
	r, c := newReconciler(t, ir)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	before := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, before)).To(gomega.Succeed())

	mutate := buildMutateMigration(r.statusWriter(), r.Client, ir)
	g.Expect(mutate(context.Background(), "u-1", func(m *workload.MigrationRecord) bool {
		m.Message = "scratch" // even a scribbling callback writes nothing on false
		return false
	})).To(gomega.Succeed())

	after := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, after)).To(gomega.Succeed())
	g.Expect(after.ResourceVersion).To(gomega.Equal(before.ResourceVersion))
	g.Expect(after.Status.Migrations[0].Message).To(gomega.BeEmpty())
}

func TestBuildMutateMigration_SameNameReplacementAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	original := baselineIR("llama-engine", "prod", 1)
	replacement := original.DeepCopy()
	replacement.UID = "replacement-uid"
	replacement.Status.Migrations = []v1beta1.MigrationStatus{acceptedEntry("u-1")}
	r, c := newReconciler(t, replacement)
	key := types.NamespacedName{Name: replacement.Name, Namespace: replacement.Namespace}
	before := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, before)).To(gomega.Succeed())

	called := false
	mutate := buildMutateMigration(r.statusWriter(), r.Client, original)
	err := mutate(context.Background(), "u-1", func(*workload.MigrationRecord) bool {
		called = true
		return true
	})
	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())
	g.Expect(called).To(gomega.BeFalse())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.ResourceVersion).To(gomega.Equal(before.ResourceVersion))
	g.Expect(got.Status.Migrations).To(gomega.Equal(before.Status.Migrations))
}

func TestBuildAppendMigration_SameNameReplacementAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	original := baselineIR("llama-engine", "prod", 1)
	replacement := original.DeepCopy()
	replacement.UID = "replacement-uid"
	replacement.Status.Migrations = []v1beta1.MigrationStatus{acceptedEntry("u-1")}
	r, c := newReconciler(t, replacement)
	key := types.NamespacedName{Name: replacement.Name, Namespace: replacement.Namespace}
	before := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, before)).To(gomega.Succeed())

	appendRec := buildAppendMigration(r.statusWriter(), r.Client, original)
	err := appendRec(context.Background(), workload.MigrationRecord{RequestUUID: "u-2"})
	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.ResourceVersion).To(gomega.Equal(before.ResourceVersion))
	g.Expect(got.Status.Migrations).To(gomega.Equal(before.Status.Migrations))
}

func TestSyncMigrationEntries_SameNameReplacementAborts(t *testing.T) {
	g := gomega.NewWithT(t)
	original := baselineIR("llama-engine", "prod", 1)
	replacement := original.DeepCopy()
	replacement.UID = "replacement-uid"
	replacement.Status.Migrations = []v1beta1.MigrationStatus{acceptedEntry("u-1")}
	r, c := newReconciler(t, replacement)
	key := types.NamespacedName{Name: replacement.Name, Namespace: replacement.Namespace}
	before := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, before)).To(gomega.Succeed())

	err := r.syncMigrationEntries(context.Background(), r.Log, original, nil, migrationTestTimeout)
	g.Expect(errors.Is(err, workload.ErrStatusOwnerGone)).To(gomega.BeTrue())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.ResourceVersion).To(gomega.Equal(before.ResourceVersion))
	g.Expect(got.Status.Migrations).To(gomega.Equal(before.Status.Migrations))
}

// TestBuildAppendMigration_AppendsIdempotently pins the AppendMigration
// seam the disposition's Auto mirror lands through: the record persists
// (proven by re-read), the committed slice mirrors back onto the
// in-memory IR, and a re-delivery of the same RequestUUID writes
// nothing.
func TestBuildAppendMigration_AppendsIdempotently(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	r, c := newReconciler(t, ir)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	appendRec := buildAppendMigration(r.statusWriter(), r.Client, ir)
	now := metav1.NewTime(migrationTestNow.Local())
	completed := now
	rec := workload.MigrationRecord{
		RequestUUID:    "u-auto",
		Trigger:        workload.MigrationTriggerAuto,
		SourceInstance: 0,
		FromNode:       "node-a",
		Phase:          workload.MigrationPhaseRelocated,
		Attempt:        1,
		Reason:         audit.ReasonAutoRecover,
		StartedAt:      now,
		Deadline:       now,
		CompletedAt:    &completed,
	}
	g.Expect(appendRec(context.Background(), rec)).To(gomega.Succeed())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Status.Migrations).To(gomega.HaveLen(1))
	e := got.Status.Migrations[0]
	g.Expect(e.RequestUUID).To(gomega.Equal("u-auto"))
	g.Expect(e.Trigger).To(gomega.Equal(v1beta1.MigrationTriggerAuto))
	g.Expect(e.Phase).To(gomega.Equal(v1beta1.MigrationPhaseRelocated))
	g.Expect(e.Attempt).To(gomega.Equal(int32(1)))
	g.Expect(e.Succeeded).To(gomega.BeNil())
	g.Expect(ir.Status.Migrations).To(gomega.Equal(got.Status.Migrations),
		"the committed slice must mirror back onto the in-memory IR")

	// Re-delivery of the same uuid: clean no-op, zero writes.
	rv := got.ResourceVersion
	g.Expect(appendRec(context.Background(), rec)).To(gomega.Succeed())
	after := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, after)).To(gomega.Succeed())
	g.Expect(after.ResourceVersion).To(gomega.Equal(rv))
	g.Expect(after.Status.Migrations).To(gomega.HaveLen(1))
}

// TestMigrationsFromIR_RoundTrip pins the observed-state mirror:
// migrationsFromIR converts field-for-field onto the workload shape and
// the v1beta1 -> workload -> v1beta1 round-trip is the identity, so the
// RMW closure cannot lose fields (HintTargetNodes included — the API
// addition this branch makes).
func TestMigrationsFromIR_RoundTrip(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	surge := int32(2)
	succeeded := true
	completed := metav1.NewTime(migrationTestNow.Add(10 * time.Minute))
	in := []v1beta1.MigrationStatus{
		{
			RequestUUID:     "u-full",
			Trigger:         v1beta1.MigrationTriggerManual,
			SourceInstance:  1,
			SurgeInstance:   &surge,
			FromNode:        "node-a",
			HintTargetNodes: []string{"node-b", "node-c"},
			Phase:           v1beta1.MigrationPhaseDraining,
			Attempt:         2,
			Reason:          "maintenance",
			Message:         "source draining",
			StartedAt:       metav1.NewTime(migrationTestNow),
			Deadline:        metav1.NewTime(migrationTestNow.Add(migrationTestTimeout)),
			CompletedAt:     &completed,
			Succeeded:       &succeeded,
		},
		acceptedEntry("u-min"),
	}
	ir.Status.Migrations = in

	records := migrationsFromIR(ir)
	g.Expect(records).To(gomega.HaveLen(2))
	w := records[0]
	g.Expect(w.RequestUUID).To(gomega.Equal("u-full"))
	g.Expect(w.Trigger).To(gomega.Equal(workload.MigrationTriggerManual))
	g.Expect(w.SurgeInstance).To(gomega.HaveValue(gomega.Equal(int32(2))))
	g.Expect(w.HintTargetNodes).To(gomega.Equal([]string{"node-b", "node-c"}))
	g.Expect(w.Phase).To(gomega.Equal(workload.MigrationPhaseDraining))

	// Pointer safety: the mirror must not alias the IR's fields.
	g.Expect(w.SurgeInstance).NotTo(gomega.BeIdenticalTo(in[0].SurgeInstance))
	g.Expect(w.CompletedAt).NotTo(gomega.BeIdenticalTo(in[0].CompletedAt))

	// Round-trip identity.
	for i := range records {
		g.Expect(migrationFromWorkload(records[i])).To(gomega.Equal(in[i]))
	}
}

// TestAggregateStatus_PreservesMigrations pins the status aggregator
// against silently dropping the migration authority: an IR whose status
// carries Migrations must still carry them after a full
// aggregateAndWriteStatus pass (which re-reads, recomputes counters and
// conditions, and issues a Status().Update). Mirrors
// TestAggregateStatus_PreservesRetryBlocks.
func TestAggregateStatus_PreservesMigrations(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady},
	}
	entry := acceptedEntry("u-keep")
	// Whole-second local-zone times so the fake client's serialization
	// round-trip compares DeepEqual (same trick as retryblock's mt()).
	entry.StartedAt = metav1.NewTime(migrationTestNow.Local())
	entry.Deadline = metav1.NewTime(migrationTestNow.Add(migrationTestTimeout).Local())
	ir.Status.Migrations = []v1beta1.MigrationStatus{entry}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	r, c := newReconciler(t, ir, pod0)

	plan := workload.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  1,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	g.Expect(writeStatus(r, ir, plan, nil, false, nil)).To(gomega.Succeed())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, got)).To(gomega.Succeed())
	g.Expect(got.Status.Migrations).To(gomega.Equal([]v1beta1.MigrationStatus{entry}),
		"aggregateAndWriteStatus must never drop or rewrite persisted Migrations")
}

// startedRow builds a ledger Started row.
func startedRow(uuid, component string, source, surge int32, reason, startedAt string) audit.Entry {
	return audit.Entry{
		RequestUUID:    uuid,
		Component:      component,
		SourceInstance: source,
		SurgeInstance:  surge,
		Phase:          audit.PhaseStarted,
		Reason:         reason,
		FromNode:       "node-a",
		StartedAt:      startedAt,
	}
}

// TestSyncMigrationEntries_UpgradeImport pins the one-shot upgrade
// import: pre-upgrade in-flight ledger Started rows synthesize
// Accepted entries (real surge index carried over — the exact shape
// TestMigrate_ResumeAfterCrash_PreStamp resumes to completion; the
// accept-time -1 sentinel imports unset for fresh allocation), while
// terminal-countered rows, AutoRecover directives, and ForceDelete
// sweeps are ignored. Idempotent: a second pass changes nothing.
func TestSyncMigrationEntries_UpgradeImport(t *testing.T) {
	g := gomega.NewWithT(t)
	rowTime := migrationTestNow.Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	inflightRow := startedRow("u-inflight", "engine", 0, 7, "fragmentation", rowTime)
	inflightRow.HintTargetNodes = []string{"node-h1", "node-h2"}
	ledger := &audit.Ledger{Entries: []audit.Entry{
		// In-flight with a real surge index -> imports with SurgeInstance=7.
		inflightRow,
		// T2 accept sentinel -> imports with SurgeInstance unset.
		startedRow("u-sentinel", "engine", 1, -1, "maintenance", rowTime),
		// Terminal counterpart present -> ignored.
		startedRow("u-done", "engine", 2, 3, "done-before", rowTime),
		{RequestUUID: "u-done", Component: "engine", Phase: audit.PhaseCompleted,
			StartedAt: rowTime, CompletedAt: rowTime, Outcome: "migrated"},
		// Relocation directive -> record, never work.
		startedRow("u-auto", "engine", 4, -1, audit.ReasonAutoRecover, rowTime),
		// Sibling component -> the decoder IR's import, not ours.
		startedRow("u-decoder", "decoder", 0, 5, "fragmentation", rowTime),
	}}
	ir := baselineIR("llama-engine", "default", 1)
	parent := migrationParent(nil, false)
	cm := ledgerCMForOwner(t, parent.Name, parent.Namespace, ledger)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent, cm)

	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout)).To(gomega.Succeed())

	fresh := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}
	g.Expect(c.Get(context.Background(), key, fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.HaveLen(2), "only the two importable rows synthesize entries")

	byUUID := map[string]v1beta1.MigrationStatus{}
	for _, e := range fresh.Status.Migrations {
		byUUID[e.RequestUUID] = e
	}
	inflight := byUUID["u-inflight"]
	g.Expect(inflight.Trigger).To(gomega.Equal(v1beta1.MigrationTriggerManual))
	g.Expect(inflight.Phase).To(gomega.Equal(v1beta1.MigrationPhaseAccepted))
	g.Expect(inflight.SurgeInstance).To(gomega.HaveValue(gomega.Equal(int32(7))),
		"a real recorded surge index must resume in place")
	g.Expect(inflight.SourceInstance).To(gomega.Equal(int32(0)))
	g.Expect(inflight.FromNode).To(gomega.Equal("node-a"))
	g.Expect(inflight.HintTargetNodes).To(gomega.Equal([]string{"node-h1", "node-h2"}),
		"the row's placement hints must survive the import")
	g.Expect(inflight.Reason).To(gomega.Equal("fragmentation"))
	g.Expect(inflight.StartedAt.Time.UTC().Format(time.RFC3339)).To(gomega.Equal(rowTime),
		"StartedAt carries the row's timestamp")
	g.Expect(inflight.AllocatedAt).NotTo(gomega.BeNil(),
		"an allocated legacy row is executing — it must import with AllocatedAt for the capacity gate")
	g.Expect(inflight.AllocatedAt.Time.UTC().Format(time.RFC3339)).To(gomega.Equal(rowTime),
		"AllocatedAt takes the row's StartedAt (best available execution timestamp)")
	g.Expect(inflight.Deadline.Time).To(gomega.BeTemporally("==", migrationTestNow.Add(migrationTestTimeout)),
		"Deadline re-arms from import time")

	sentinel := byUUID["u-sentinel"]
	g.Expect(sentinel.SurgeInstance).To(gomega.BeNil(),
		"the -1 accept sentinel must import unset so the executor allocates fresh")
	g.Expect(sentinel.AllocatedAt).To(gomega.BeNil(),
		"an unallocated sentinel is queued — it must never count toward capacity")

	// Idempotent: second pass adds nothing.
	rvBefore := fresh.ResourceVersion
	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout)).To(gomega.Succeed())
	after := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, after)).To(gomega.Succeed())
	g.Expect(after.Status.Migrations).To(gomega.HaveLen(2))
	g.Expect(after.ResourceVersion).To(gomega.Equal(rvBefore), "second import pass must write nothing")
}

// TestSyncMigrationEntries_TrimsAgedTerminal pins the trim rule:
// terminal entries older than the capacity rate window are pruned —
// provided the ledger also remembers them terminally (each aged UUID
// gets a terminal ledger row here; the ledger-less direction is
// TestSyncMigrationEntries_TrimRequiresLedgerTerminal_NoResurrection).
// Fresh terminal entries, terminal entries lacking CompletedAt, and
// non-terminal entries (however old) are kept.
func TestSyncMigrationEntries_TrimsAgedTerminal(t *testing.T) {
	g := gomega.NewWithT(t)
	old := metav1.NewTime(migrationTestNow.Add(-audit.CapacityRateWindow - time.Hour))
	recent := metav1.NewTime(migrationTestNow.Add(-time.Minute))

	mk := func(uuid string, phase v1beta1.MigrationPhase, completedAt *metav1.Time) v1beta1.MigrationStatus {
		e := acceptedEntry(uuid)
		e.Phase = phase
		e.CompletedAt = completedAt
		e.StartedAt = old
		return e
	}
	// An aged Auto record whose relocation never confirmed (Succeeded
	// unset): CompletedAt is stamped at birth, so it ages out on the
	// same clock as every other terminal entry.
	agedAutoUnsucceeded := mk("u-aged-auto-unsucceeded", v1beta1.MigrationPhaseRelocated, &old)
	agedAutoUnsucceeded.Trigger = v1beta1.MigrationTriggerAuto

	ir := baselineIR("llama-engine", "default", 1)
	ir.Status.Migrations = []v1beta1.MigrationStatus{
		mk("u-aged-completed", v1beta1.MigrationPhaseCompleted, &old),
		mk("u-aged-failed", v1beta1.MigrationPhaseFailed, &old),
		mk("u-aged-relocated", v1beta1.MigrationPhaseRelocated, &old),
		agedAutoUnsucceeded,
		mk("u-fresh-completed", v1beta1.MigrationPhaseCompleted, &recent),
		mk("u-terminal-no-completedat", v1beta1.MigrationPhaseFailed, nil),
		mk("u-old-but-inflight", v1beta1.MigrationPhaseDraining, nil),
	}
	// The ledger remembers every aged terminal UUID — the trim
	// precondition. Relocated (Auto) entries mirror the AutoRecover
	// Completed rows the disposition persists ledger-first.
	oldRow := old.Time.UTC().Format(time.RFC3339)
	terminalRow := func(uuid, phase, reason string) audit.Entry {
		return audit.Entry{
			RequestUUID: uuid, Component: "engine", Phase: phase, Reason: reason,
			StartedAt: oldRow, CompletedAt: oldRow, Outcome: "closed",
		}
	}
	ledger := &audit.Ledger{Entries: []audit.Entry{
		terminalRow("u-aged-completed", audit.PhaseCompleted, ""),
		terminalRow("u-aged-failed", audit.PhaseFailed, ""),
		terminalRow("u-aged-relocated", audit.PhaseCompleted, audit.ReasonAutoRecover),
		terminalRow("u-aged-auto-unsucceeded", audit.PhaseCompleted, audit.ReasonAutoRecover),
	}}
	parent := migrationParent(nil, false)
	cm := ledgerCMForOwner(t, parent.Name, parent.Namespace, ledger)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent, cm)

	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout)).To(gomega.Succeed())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh)).To(gomega.Succeed())
	var kept []string
	for _, e := range fresh.Status.Migrations {
		kept = append(kept, e.RequestUUID)
	}
	g.Expect(kept).To(gomega.ConsistOf("u-fresh-completed", "u-terminal-no-completedat", "u-old-but-inflight"),
		"aged terminal entries prune; non-terminal and CompletedAt-less terminal entries never do")
	g.Expect(ir.Status.Migrations).To(gomega.Equal(fresh.Status.Migrations),
		"the trimmed slice must mirror back onto the in-memory IR")
}

// TestSyncMigrationEntries_TrimRequiresLedgerTerminal_NoResurrection
// pins the trim invariant ("status may forget only what the ledger
// remembers") and THE PHANTOM-RESURRECTION REGRESSION it backstops:
// an aged terminal entry whose UUID has only a Started ledger row (the
// expiry's terminal mirror never landed) must be RETAINED — trimming
// it would free the UUID for the upgrade import, which would
// re-synthesize the Started row as fresh Accepted work an hour after
// the operator saw Failed, and an unsolicited migration would execute.
// Once the terminal row lands (the hard expiry mirror guarantees
// it), the entry trims and the import stays blocked by
// HasCompletedOrFailedRequest.
func TestSyncMigrationEntries_TrimRequiresLedgerTerminal_NoResurrection(t *testing.T) {
	g := gomega.NewWithT(t)
	old := metav1.NewTime(migrationTestNow.Add(-audit.CapacityRateWindow - time.Hour))
	rowTime := old.Time.UTC().Format(time.RFC3339)

	// Ledger: only the Started row — the terminal mirror never landed.
	ledger := &audit.Ledger{Entries: []audit.Entry{
		startedRow("u-ghost", "engine", 0, 7, "fragmentation", rowTime),
	}}
	entry := acceptedEntry("u-ghost")
	entry.Phase = v1beta1.MigrationPhaseFailed
	entry.StartedAt = old
	entry.CompletedAt = &old

	ir := baselineIR("llama-engine", "default", 1)
	ir.Status.Migrations = []v1beta1.MigrationStatus{entry}
	parent := migrationParent(nil, false)
	cm := ledgerCMForOwner(t, parent.Name, parent.Namespace, ledger)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent, cm)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	// Pass 1: the aged terminal entry is retained (backstop) — and in
	// particular NEVER resurrected as an Accepted import (a trim that
	// freed the UUID would let the import re-synthesize it Accepted in
	// the same pass).
	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout)).To(gomega.Succeed())
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.HaveLen(1))
	g.Expect(fresh.Status.Migrations[0].RequestUUID).To(gomega.Equal("u-ghost"))
	g.Expect(fresh.Status.Migrations[0].Phase).To(gomega.Equal(v1beta1.MigrationPhaseFailed),
		"the entry must stay terminal — a trim without a terminal ledger row resurrects the UUID as fresh work")

	// Heal: the terminal mirror lands (what the hard expiry mirror
	// guarantees before any record closes terminal).
	healed := loadTestLedger(t, c, parent)
	healed.UpsertEntry(audit.NewTerminalEntry(*healed.InFlightEntry("u-ghost"), audit.PhaseFailed, "expired"))
	g.Expect(audit.PersistLedgerForOwner(context.Background(), c, parent, isvcGVK, healed)).To(gomega.Succeed())

	// Pass 2: the trim proceeds and the import does NOT resurrect —
	// HasCompletedOrFailedRequest blocks the Started row.
	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout)).To(gomega.Succeed())
	after := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, after)).To(gomega.Succeed())
	g.Expect(after.Status.Migrations).To(gomega.BeEmpty(),
		"once the ledger remembers the terminal outcome the entry trims and the import stays blocked")
}
