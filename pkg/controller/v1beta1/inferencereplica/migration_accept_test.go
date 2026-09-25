package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// migrationTestRecordWindow is the status-record horizon the seam tests
// age terminal records against, standing in for the operator config a
// deployed chart supplies.
const migrationTestRecordWindow = time.Hour

// validMigrationRequestJSON renders a schema-v1 request for component
// at instance idx from node "node-a", carrying placement hints.
func validMigrationRequestJSON(component string, idx int32) string {
	raw, _ := json.Marshal(audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       component,
		Instance:        idx,
		FromNode:        "node-a",
		HintTargetNodes: []string{"node-x", "node-y"},
		Reason:          "maintenance",
	})
	return string(raw)
}

// newConsumeFixture builds a fake-client Reconciler (fake clock + fake
// recorder) around the given IR/parent and re-reads both through the
// client so ResourceVersions are stamped for subsequent Updates.
func newConsumeFixture(t *testing.T, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, extra ...client.Object) (*Reconciler, client.Client, *record.FakeRecorder, *v1beta1.InferenceReplica, *v1beta1.InferenceService) {
	t.Helper()
	objs := []client.Object{ir, parent}
	objs = append(objs, extra...)
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()
	rec := record.NewFakeRecorder(16)
	r := &Reconciler{
		Client:               c,
		APIReader:            c,
		Log:                  logf.Log.WithName("test"),
		Recorder:             rec,
		Expectations:         workloadtypes.NewExpectations(),
		InstanceStatusTarget: irstatus.EncodingDenseV1,
		Clock:                clocktesting.NewFakeClock(migrationTestNow),
	}
	freshIR := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), freshIR); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	freshParent := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(parent), freshParent); err != nil {
		t.Fatalf("get parent: %v", err)
	}
	return r, c, rec, freshIR, freshParent
}

// loadTestLedger reads the parent-owned audit ledger from the fake
// client; missing ConfigMap yields an empty ledger.
func loadTestLedger(t *testing.T, c client.Client, parent *v1beta1.InferenceService) *audit.Ledger {
	t.Helper()
	ledger, err := audit.LoadLedgerForOwner(context.Background(), c, parent)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	return ledger
}

// migrationAnnotationsOf returns the parent's surviving migration-request
// annotation keys.
func migrationAnnotationsOf(t *testing.T, c client.Client, parent *v1beta1.InferenceService) []string {
	t.Helper()
	fresh := &v1beta1.InferenceService{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(parent), fresh); err != nil {
		t.Fatalf("get parent: %v", err)
	}
	var keys []string
	for k := range fresh.Annotations {
		if strings.HasPrefix(k, audit.MigrationRequestAnnotationPrefix) {
			keys = append(keys, k)
		}
	}
	return keys
}

func TestConsumeMigrationRequests_FreshValidAccepted(t *testing.T) {
	g := gomega.NewWithT(t)
	key := audit.MigrationRequestAnnotationPrefix + "uuid-1"
	ir := baselineIR("llama-engine", "default", 1)
	parent := migrationParent(map[string]string{key: validMigrationRequestJSON("engine", 0)}, false)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent)

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workloadtypes.MigrationModeAuto, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	// Status entry: Accepted, Manual, correct Deadline, no surge index.
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.HaveLen(1))
	entry := fresh.Status.Migrations[0]
	g.Expect(entry.RequestUUID).To(gomega.Equal("uuid-1"))
	g.Expect(entry.Trigger).To(gomega.Equal(v1beta1.MigrationTriggerManual))
	g.Expect(entry.Phase).To(gomega.Equal(v1beta1.MigrationPhaseAccepted))
	g.Expect(entry.SourceInstance).To(gomega.Equal(int32(0)))
	g.Expect(entry.SurgeInstance).To(gomega.BeNil())
	g.Expect(entry.FromNode).To(gomega.Equal("node-a"))
	g.Expect(entry.HintTargetNodes).To(gomega.Equal([]string{"node-x", "node-y"}),
		"the born entry must carry the request's placement hints — the annotation is consumed here and the executor renders the surge from the record alone")
	g.Expect(entry.Reason).To(gomega.Equal("maintenance"))
	g.Expect(entry.StartedAt.Time).To(gomega.BeTemporally("==", migrationTestNow))
	g.Expect(entry.Deadline.Time).To(gomega.BeTemporally("==", migrationTestNow.Add(migrationTestTimeout)))
	// In-memory mirror observed the commit.
	g.Expect(ir.Status.Migrations).To(gomega.HaveLen(1))

	// Ledger Started row with the unallocated surge sentinel.
	ledger := loadTestLedger(t, c, parent)
	g.Expect(ledger.Entries).To(gomega.HaveLen(1))
	g.Expect(ledger.Entries[0].RequestUUID).To(gomega.Equal("uuid-1"))
	g.Expect(ledger.Entries[0].Phase).To(gomega.Equal(audit.PhaseStarted))
	g.Expect(ledger.Entries[0].SurgeInstance).To(gomega.Equal(unallocatedSurgeIndex))

	// Mailbox consumed.
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty())
	g.Expect(parent.Annotations).NotTo(gomega.HaveKey(key), "in-memory parent must mirror the delete")
}

func TestConsumeMigrationRequests_RedeliveryEntryPresent_DeletedNoDuplicate(t *testing.T) {
	g := gomega.NewWithT(t)
	key := audit.MigrationRequestAnnotationPrefix + "uuid-1"
	ir := baselineIR("llama-engine", "default", 1)
	ir.Status.Migrations = []v1beta1.MigrationStatus{{
		RequestUUID: "uuid-1",
		Trigger:     v1beta1.MigrationTriggerManual,
		Phase:       v1beta1.MigrationPhaseAccepted,
		StartedAt:   metav1.NewTime(migrationTestNow),
		Deadline:    metav1.NewTime(migrationTestNow.Add(migrationTestTimeout)),
	}}
	parent := migrationParent(map[string]string{key: validMigrationRequestJSON("engine", 0)}, false)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent)

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workloadtypes.MigrationModeAuto, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.HaveLen(1), "re-delivery must not duplicate the entry")
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty())
	// Dedup cleanup writes nothing to the ledger.
	g.Expect(loadTestLedger(t, c, parent).Entries).To(gomega.BeEmpty())
}

func TestConsumeMigrationRequests_TerminalInLedger_DeletedNoEntry(t *testing.T) {
	g := gomega.NewWithT(t)
	key := audit.MigrationRequestAnnotationPrefix + "uuid-1"
	ir := baselineIR("llama-engine", "default", 1)
	parent := migrationParent(map[string]string{key: validMigrationRequestJSON("engine", 0)}, false)

	ledger := &audit.Ledger{Entries: []audit.Entry{{
		RequestUUID: "uuid-1",
		Component:   "engine",
		Phase:       audit.PhaseCompleted,
		StartedAt:   migrationTestNow.Add(-time.Hour).UTC().Format(time.RFC3339),
		CompletedAt: migrationTestNow.Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		Outcome:     "migrated",
	}}}
	raw, err := json.Marshal(ledger)
	if err != nil {
		t.Fatalf("marshal ledger: %v", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: audit.ConfigMapNameForOwner(parent), Namespace: "default"},
		Data:       map[string]string{audit.LedgerKey: string(raw)},
	}
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent, cm)

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workloadtypes.MigrationModeAuto, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.BeEmpty(), "terminal UUID must not resurrect as a status entry")
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty())
}

func TestConsumeMigrationRequests_Invalid_FailedRowWarningDeleted(t *testing.T) {
	g := gomega.NewWithT(t)
	for name, value := range map[string]string{
		"garbage-json":      `{not-json`,
		"unknown-schema":    `{"schemaVersion":"v9","component":"engine","instance":0,"from_node":"n"}`,
		"unknown-component": `{"schemaVersion":"v1","component":"sidecar","instance":0,"from_node":"n"}`,
		"negative-instance": `{"schemaVersion":"v1","component":"engine","instance":-2,"from_node":"n"}`,
	} {
		t.Run(name, func(t *testing.T) {
			key := audit.MigrationRequestAnnotationPrefix + "uuid-bad"
			ir := baselineIR("llama-engine", "default", 1)
			parent := migrationParent(map[string]string{key: value}, false)
			r, c, rec, ir, parent := newConsumeFixture(t, ir, parent)

			requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workloadtypes.MigrationModeAuto, migrationTestTimeout)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(requeue).To(gomega.BeFalse())

			fresh := &v1beta1.InferenceReplica{}
			g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
			g.Expect(fresh.Status.Migrations).To(gomega.BeEmpty())

			ledger := loadTestLedger(t, c, parent)
			g.Expect(ledger.HasCompletedOrFailedRequest("uuid-bad")).To(gomega.BeTrue(),
				"invalid request must land a terminal Failed ledger row")

			// Unknown schemaVersion keeps its distinct event reason
			// (dashboards filter on it); every other rejection folds
			// into MigrationRequestRejected.
			wantReason := "MigrationRequestRejected"
			if name == "unknown-schema" {
				wantReason = "UnsupportedSchemaVersion"
			}
			select {
			case ev := <-rec.Events:
				g.Expect(ev).To(gomega.ContainSubstring(wantReason))
				g.Expect(ev).To(gomega.ContainSubstring("uuid-bad"))
			default:
				t.Fatalf("expected a Warning event on the ISVC")
			}
			g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty())
		})
	}
}

func TestConsumeMigrationRequests_ParentNil_NoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "default", 1)
	r, _ := newReconciler(t, ir)
	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, nil, workloadtypes.MigrationModeAuto, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())
	g.Expect(ir.Status.Migrations).To(gomega.BeEmpty())
}

func TestConsumeMigrationRequests_TwoValid_OnlyLowestKeyConsumed(t *testing.T) {
	g := gomega.NewWithT(t)
	keyA := audit.MigrationRequestAnnotationPrefix + "aaaa"
	keyB := audit.MigrationRequestAnnotationPrefix + "bbbb"
	ir := baselineIR("llama-engine", "default", 1)
	parent := migrationParent(map[string]string{
		keyB: validMigrationRequestJSON("engine", 1),
		keyA: validMigrationRequestJSON("engine", 0),
	}, false)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent)

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workloadtypes.MigrationModeAuto, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.HaveLen(1), "one Manual accept per pass")
	g.Expect(fresh.Status.Migrations[0].RequestUUID).To(gomega.Equal("aaaa"), "lowest-sorted key first")
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.ConsistOf(keyB),
		"the second request stays in the mailbox for the next pass")
}

func TestConsumeMigrationRequests_SiblingComponentLeftAlone(t *testing.T) {
	g := gomega.NewWithT(t)
	key := audit.MigrationRequestAnnotationPrefix + "uuid-dec"
	ir := baselineIR("llama-engine", "default", 1)
	parent := migrationParent(map[string]string{key: validMigrationRequestJSON("decoder", 0)}, true)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent)

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workloadtypes.MigrationModeAuto, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.BeEmpty(), "a declared sibling's request is not this IR's work")
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.ConsistOf(key),
		"the annotation stays for the decoder IR's pass")
	g.Expect(loadTestLedger(t, c, parent).Entries).To(gomega.BeEmpty())
}

// TestReconcile_ConsumesMigrationRequestAnnotation pins the Reconcile
// wiring end-to-end: a full pass with a resolvable parent consumes the
// mailbox into a status entry stamped with the default
// InstanceReadyTimeout deadline, AND the same pass dispatches the
// entry (the post-accept ObservedState refresh) — on this fresh IR
// with no instance 0, the executor rejects it terminally onto the
// entry itself. The record, not the mailbox or the ledger, carries the
// outcome.
func TestReconcile_ConsumesMigrationRequestAnnotation(t *testing.T) {
	g := gomega.NewWithT(t)
	key := audit.MigrationRequestAnnotationPrefix + "uuid-wire"
	ir := baselineIR("llama-engine", "default", 1)
	parent := migrationParent(map[string]string{key: validMigrationRequestJSON("engine", 0)}, false)
	r, c, _, _, _ := newConsumeFixture(t, ir, parent)
	// The IR carries no instanceReadyTimeout of its own, so the entry's
	// Deadline must come from the operator's lifecycle.instanceReadyTimeout.
	withLifecycleConfig(r, `{"instanceReadyTimeout":"20m"}`)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.HaveLen(1))
	entry := fresh.Status.Migrations[0]
	g.Expect(entry.RequestUUID).To(gomega.Equal("uuid-wire"))
	g.Expect(entry.Deadline.Time).To(
		gomega.BeTemporally("==", migrationTestNow.Add(20*time.Minute)))
	g.Expect(entry.Phase).To(gomega.Equal(v1beta1.MigrationPhaseFailed),
		"same-pass dispatch must reject a migration of a nonexistent instance onto the entry")
	g.Expect(entry.Message).To(gomega.ContainSubstring("source InstanceStatus missing"))
	g.Expect(entry.CompletedAt).NotTo(gomega.BeNil())
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty())
}

// TestConsumeMigrationRequests_NeverMode_BornTerminalFailed pins the
// Mode=Never mailbox contract: a valid request is consumed as a
// BORN-TERMINAL Failed entry naming the policy — not parked Accepted
// until the Deadline expires it with a misleading surge message. Same
// visibility surfaces as an accept (status entry, ledger row, event,
// annotation consumed), answered immediately.
func TestConsumeMigrationRequests_NeverMode_BornTerminalFailed(t *testing.T) {
	g := gomega.NewWithT(t)
	key := audit.MigrationRequestAnnotationPrefix + "uuid-never"
	ir := baselineIR("llama-engine", "default", 1)
	parent := migrationParent(map[string]string{key: validMigrationRequestJSON("engine", 0)}, false)
	r, c, rec, ir, parent := newConsumeFixture(t, ir, parent)

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workloadtypes.MigrationModeNever, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	// Status entry: born terminal, policy named in the message.
	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.HaveLen(1))
	entry := fresh.Status.Migrations[0]
	g.Expect(entry.RequestUUID).To(gomega.Equal("uuid-never"))
	g.Expect(entry.Trigger).To(gomega.Equal(v1beta1.MigrationTriggerManual))
	g.Expect(entry.Phase).To(gomega.Equal(v1beta1.MigrationPhaseFailed))
	g.Expect(entry.Message).To(gomega.ContainSubstring("MigrationPolicy Mode=Never"),
		"the terminal answer must name the policy that rejected the request")
	g.Expect(entry.SourceInstance).To(gomega.Equal(int32(0)))
	g.Expect(entry.FromNode).To(gomega.Equal("node-a"))
	g.Expect(entry.Reason).To(gomega.Equal("maintenance"))
	g.Expect(entry.SurgeInstance).To(gomega.BeNil(), "no surge is ever allocated under Never")
	g.Expect(entry.StartedAt.Time).To(gomega.BeTemporally("==", migrationTestNow))
	g.Expect(entry.CompletedAt).NotTo(gomega.BeNil())
	g.Expect(entry.CompletedAt.Time).To(gomega.BeTemporally("==", migrationTestNow))
	g.Expect(entry.Deadline.Time).To(gomega.BeTemporally("==", migrationTestNow),
		"a born-terminal entry carries no future deadline to expire")

	// The dispatcher structurally never sees it as work.
	g.Expect(workloadtypes.NextManualMigration(migrationsFromIR(fresh))).To(gomega.BeNil(),
		"a born-terminal entry must never be selectable for dispatch")

	// Ledger: terminal Failed audit row mirroring the rejection.
	ledger := loadTestLedger(t, c, parent)
	g.Expect(ledger.HasCompletedOrFailedRequest("uuid-never")).To(gomega.BeTrue(),
		"Never rejection must land a terminal Failed ledger row")
	g.Expect(ledger.Entries).To(gomega.HaveLen(1))
	g.Expect(ledger.Entries[0].Phase).To(gomega.Equal(audit.PhaseFailed))
	g.Expect(ledger.Entries[0].Outcome).To(gomega.ContainSubstring("MigrationPolicy Mode=Never"))

	// Warning event on the ISVC (existing rejection reason).
	select {
	case ev := <-rec.Events:
		g.Expect(ev).To(gomega.ContainSubstring("MigrationRequestRejected"))
		g.Expect(ev).To(gomega.ContainSubstring("uuid-never"))
		g.Expect(ev).To(gomega.ContainSubstring("MigrationPolicy Mode=Never"))
	default:
		t.Fatalf("expected a Warning event on the ISVC")
	}

	// Mailbox consumed.
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty())
	g.Expect(parent.Annotations).NotTo(gomega.HaveKey(key), "in-memory parent must mirror the delete")
}

// TestReconcile_MigrationModeResolution pins the accept-gate's mode
// resolution through the full Reconcile wiring: the component-level
// MigrationPolicy override (IR.Spec.Lifecycle, projected from
// ISVC.spec.<component>.lifecycle) is the same effective mode the
// dispatcher reads from ComponentPlan — Never rejects at accept; the
// unset default resolves Auto and accepts (the entry then fails
// downstream on this fresh IR for a NON-policy reason, proving the
// gate did not fire).
func TestReconcile_MigrationModeResolution(t *testing.T) {
	cases := map[string]struct {
		lifecycle   *v1beta1.LifecycleSpec
		wantMessage string
	}{
		"component-level Never override rejects at accept": {
			lifecycle:   &v1beta1.LifecycleSpec{MigrationPolicy: &v1beta1.MigrationPolicy{Mode: v1beta1.MigrationPolicyModeNever}},
			wantMessage: "migrations disabled by MigrationPolicy Mode=Never",
		},
		"unset policy defaults to Auto and accepts": {
			lifecycle:   nil,
			wantMessage: "source InstanceStatus missing",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			key := audit.MigrationRequestAnnotationPrefix + "uuid-mode"
			ir := baselineIR("llama-engine", "default", 1)
			ir.Spec.Lifecycle = tc.lifecycle
			parent := migrationParent(map[string]string{key: validMigrationRequestJSON("engine", 0)}, false)
			r, c, _, _, _ := newConsumeFixture(t, ir, parent)

			_, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
			})
			g.Expect(err).NotTo(gomega.HaveOccurred())

			fresh := &v1beta1.InferenceReplica{}
			g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
			g.Expect(fresh.Status.Migrations).To(gomega.HaveLen(1))
			entry := fresh.Status.Migrations[0]
			g.Expect(entry.Phase).To(gomega.Equal(v1beta1.MigrationPhaseFailed))
			g.Expect(entry.Message).To(gomega.ContainSubstring(tc.wantMessage))
			g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty())
		})
	}
}

// TestConsumeMigrationRequests_StaleParentSnapshot_DeletionStillLands
// pins the hot-parent contract: the batched annotation delete runs
// against a FRESH parent read under conflict retry, so a pass-start
// snapshot gone stale (concurrent writer bumped the RV) neither
// short-circuits the pass nor loses the concurrent write.
func TestConsumeMigrationRequests_StaleParentSnapshot_DeletionStillLands(t *testing.T) {
	g := gomega.NewWithT(t)
	key := audit.MigrationRequestAnnotationPrefix + "uuid-1"
	ir := baselineIR("llama-engine", "default", 1)
	parent := migrationParent(map[string]string{key: validMigrationRequestJSON("engine", 0)}, false)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent)

	// Bump the parent behind the snapshot's back so the RV goes stale.
	fresh := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(parent), fresh)).To(gomega.Succeed())
	if fresh.Labels == nil {
		fresh.Labels = map[string]string{}
	}
	fresh.Labels["race"] = "yes"
	g.Expect(c.Update(context.Background(), fresh)).To(gomega.Succeed())

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workloadtypes.MigrationModeAuto, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse(), "a stale snapshot must not short-circuit the pass — the delete re-reads the parent")
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty(),
		"the consumed annotation must be deleted despite the stale snapshot")
	// The concurrent writer's change survives the annotation delete.
	after := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(parent), after)).To(gomega.Succeed())
	g.Expect(after.Labels).To(gomega.HaveKeyWithValue("race", "yes"))
	// The status entry committed.
	freshIR := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), freshIR)).To(gomega.Succeed())
	g.Expect(freshIR.Status.Migrations).To(gomega.HaveLen(1))
}

// The mailbox consumer must load the parent-owned ledger via the live
// reader: a cache-lagged snapshot misses terminal rows written by
// sibling IRs, resurrecting a completed migration as fresh work. The
// terminal row here exists ONLY behind APIReader, so dedup happens iff
// the live reader is the load source.
func TestConsumeMigrationRequests_LedgerLoadedViaLiveReader(t *testing.T) {
	g := gomega.NewWithT(t)
	key := audit.MigrationRequestAnnotationPrefix + "uuid-1"
	ir := baselineIR("llama-engine", "default", 1)
	parent := migrationParent(map[string]string{key: validMigrationRequestJSON("engine", 0)}, false)
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent)

	ledger := &audit.Ledger{Entries: []audit.Entry{{
		RequestUUID: "uuid-1",
		Component:   "engine",
		Phase:       audit.PhaseCompleted,
		StartedAt:   migrationTestNow.Add(-time.Hour).UTC().Format(time.RFC3339),
		CompletedAt: migrationTestNow.Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		Outcome:     "migrated",
	}}}
	raw, err := json.Marshal(ledger)
	if err != nil {
		t.Fatalf("marshal ledger: %v", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: audit.ConfigMapNameForOwner(parent), Namespace: "default"},
		Data:       map[string]string{audit.LedgerKey: string(raw)},
	}
	// The live reader is the apiserver: it sees every object, not just the
	// ledger under test. Seeding only the ConfigMap would make the parent
	// look deleted to the conflict-retry re-reads on this path.
	r.APIReader = fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cm, parent, ir).Build()

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workloadtypes.MigrationModeAuto, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.BeEmpty(),
		"terminal UUID visible only through the live reader must dedup, not resurrect")
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty())
}

// Tests for the status.migrations adapter seam: the MutateMigration
// RMW closure, the observed-state mirror, the ledger upgrade import and
// the terminal-entry trim.

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
	g.Expect(mutate(context.Background(), "u-1", func(m *workloadtypes.MigrationRecord) bool {
		g.Expect(m.Phase).To(gomega.Equal(workloadtypes.MigrationPhaseAccepted))
		m.SurgeInstance = &idx
		m.Phase = workloadtypes.MigrationPhaseSurgePending
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
	g.Expect(mutate(context.Background(), "u-gone", func(_ *workloadtypes.MigrationRecord) bool {
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
	g.Expect(mutate(context.Background(), "u-1", func(m *workloadtypes.MigrationRecord) bool {
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
	err := mutate(context.Background(), "u-1", func(*workloadtypes.MigrationRecord) bool {
		called = true
		return true
	})
	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())
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
	err := appendRec(context.Background(), workloadtypes.MigrationRecord{RequestUUID: "u-2"})
	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())

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

	err := r.syncMigrationEntries(context.Background(), r.Log, original, nil, migrationTestTimeout, migrationTestRecordWindow)
	g.Expect(errors.Is(err, workloadtypes.ErrStatusOwnerGone)).To(gomega.BeTrue())

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
	rec := workloadtypes.MigrationRecord{
		RequestUUID:    "u-auto",
		Trigger:        workloadtypes.MigrationTriggerAuto,
		SourceInstance: 0,
		FromNode:       "node-a",
		Phase:          workloadtypes.MigrationPhaseRelocated,
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
// RMW closure cannot lose fields (HintTargetNodes included).
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
	g.Expect(w.Trigger).To(gomega.Equal(workloadtypes.MigrationTriggerManual))
	g.Expect(w.SurgeInstance).To(gomega.HaveValue(gomega.Equal(int32(2))))
	g.Expect(w.HintTargetNodes).To(gomega.Equal([]string{"node-b", "node-c"}))
	g.Expect(w.Phase).To(gomega.Equal(workloadtypes.MigrationPhaseDraining))

	// Pointer safety: the mirror must not alias the IR's fields.
	g.Expect(w.SurgeInstance).NotTo(gomega.BeIdenticalTo(in[0].SurgeInstance))
	g.Expect(w.CompletedAt).NotTo(gomega.BeIdenticalTo(in[0].CompletedAt))

	// Round-trip identity.
	for i := range records {
		g.Expect(migrationFromWorkload(records[i])).To(gomega.Equal(in[i]))
	}
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
		// Accept-time sentinel (surge index -1) -> imports with SurgeInstance unset.
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

	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout, migrationTestRecordWindow)).To(gomega.Succeed())

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
	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout, migrationTestRecordWindow)).To(gomega.Succeed())
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
	old := metav1.NewTime(migrationTestNow.Add(-migrationTestRecordWindow - time.Hour))
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

	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout, migrationTestRecordWindow)).To(gomega.Succeed())

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

// TestSyncMigrationEntries_TrimsAgedRelocatedWithoutLedgerRow pins that a
// born-terminal Relocated (Auto) record ages out on the window alone. Its
// AutoRecover ledger row is pruned when the rebuilt instance is observed
// Ready, and the upgrade import never synthesizes Auto rows, so the
// ledger-witness backstop guarding Completed/Failed entries has nothing
// to protect here.
func TestSyncMigrationEntries_TrimsAgedRelocatedWithoutLedgerRow(t *testing.T) {
	g := gomega.NewWithT(t)
	born := metav1.NewTime(migrationTestNow.Add(-migrationTestRecordWindow - 2*time.Hour))
	succeededAt := metav1.NewTime(migrationTestNow.Add(-migrationTestRecordWindow - time.Hour))

	entry := acceptedEntry("u-relocated-succeeded")
	entry.Trigger = v1beta1.MigrationTriggerAuto
	entry.Phase = v1beta1.MigrationPhaseRelocated
	entry.Reason = audit.ReasonAutoRecover
	entry.StartedAt = born
	entry.Deadline = born
	entry.CompletedAt = &succeededAt
	succeeded := true
	entry.Succeeded = &succeeded

	ir := baselineIR("llama-engine", "default", 1)
	ir.Status.Migrations = []v1beta1.MigrationStatus{entry}
	parent := migrationParent(nil, false)
	// The ledger no longer holds the UUID: the success prune removed its
	// AutoRecover row when the instance came Ready.
	cm := ledgerCMForOwner(t, parent.Name, parent.Namespace, &audit.Ledger{})
	r, c, _, ir, parent := newConsumeFixture(t, ir, parent, cm)

	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout, migrationTestRecordWindow)).To(gomega.Succeed())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}, fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.BeEmpty(),
		"an aged Relocated record must trim without a ledger witness; otherwise every successful auto-relocation is retained forever")
	g.Expect(ir.Status.Migrations).To(gomega.BeEmpty(),
		"the trimmed slice must mirror back onto the in-memory IR")
}

// TestSyncMigrationEntries_TrimRequiresLedgerTerminal_NoResurrection
// pins the trim invariant ("status may forget only what the ledger
// remembers") and the phantom resurrection it prevents:
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
	old := metav1.NewTime(migrationTestNow.Add(-migrationTestRecordWindow - time.Hour))
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
	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout, migrationTestRecordWindow)).To(gomega.Succeed())
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
	g.Expect(r.syncMigrationEntries(context.Background(), r.Log, ir, parent, migrationTestTimeout, migrationTestRecordWindow)).To(gomega.Succeed())
	after := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, after)).To(gomega.Succeed())
	g.Expect(after.Status.Migrations).To(gomega.BeEmpty(),
		"once the ledger remembers the terminal outcome the entry trims and the import stays blocked")
}
