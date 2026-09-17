package inferencereplica

import (
	"context"
	"encoding/json"
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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

// migrationTestNow is the fixed instant every consume test's fake
// clock reads. Arbitrary but stable so Deadline assertions are exact.
var migrationTestNow = time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)

const migrationTestTimeout = 42 * time.Minute

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

// migrationParent builds the parent ISVC ("llama", matching baselineIR's
// ParentRef) carrying the given annotations. withDecoder adds a Decoder
// block so sibling-component routing can be exercised.
func migrationParent(annotations map[string]string, withDecoder bool) *v1beta1.InferenceService {
	parent := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "llama",
			Namespace:   "default",
			UID:         types.UID("llama-isvc-uid"),
			Annotations: annotations,
		},
	}
	if withDecoder {
		parent.Spec.Decoder = &v1beta1.DecoderSpec{}
	}
	return parent
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
		Expectations:         workload.NewExpectations(),
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

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workload.MigrationModeAuto, migrationTestTimeout)
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

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workload.MigrationModeAuto, migrationTestTimeout)
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

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workload.MigrationModeAuto, migrationTestTimeout)
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

			requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workload.MigrationModeAuto, migrationTestTimeout)
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
	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, nil, workload.MigrationModeAuto, migrationTestTimeout)
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

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workload.MigrationModeAuto, migrationTestTimeout)
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

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workload.MigrationModeAuto, migrationTestTimeout)
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
		gomega.BeTemporally("==", migrationTestNow.Add(workload.InstanceReadyTimeoutOrDefault(nil))))
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

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workload.MigrationModeNever, migrationTestTimeout)
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
	g.Expect(workload.NextManualMigration(migrationsFromIR(fresh))).To(gomega.BeNil(),
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

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workload.MigrationModeAuto, migrationTestTimeout)
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

	requeue, err := r.consumeMigrationRequests(context.Background(), r.Log, ir, parent, workload.MigrationModeAuto, migrationTestTimeout)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(requeue).To(gomega.BeFalse())

	fresh := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh)).To(gomega.Succeed())
	g.Expect(fresh.Status.Migrations).To(gomega.BeEmpty(),
		"terminal UUID visible only through the live reader must dedup, not resurrect")
	g.Expect(migrationAnnotationsOf(t, c, parent)).To(gomega.BeEmpty())
}
