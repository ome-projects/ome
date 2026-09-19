package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Ledger round-trip + annotation-parsing tests. The ledger is
// audit-only; dedup and trim behavior guard the history surface.

func TestExtractRequestUUID(t *testing.T) {
	if got := ExtractRequestUUID(MigrationRequestAnnotationPrefix + "abc-123"); got != "abc-123" {
		t.Errorf("uuid: got %q want abc-123", got)
	}
	if got := ExtractRequestUUID("ome.io/something-else"); got != "" {
		t.Errorf("non-migration key should return empty, got %q", got)
	}
}

func TestParseMigrationRequest_Valid(t *testing.T) {
	raw, _ := json.Marshal(MigrationRequest{
		SchemaVersion:   SchemaV1,
		Component:       "engine",
		Instance:        0,
		FromNode:        "node5",
		HintTargetNodes: []string{"node3", "node7"},
		Reason:          "fragmentation",
	})
	req, err := ParseMigrationRequest(string(raw))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if req.Instance != 0 || req.FromNode != "node5" || req.Component != "engine" {
		t.Errorf("parsed request mismatch: %+v", req)
	}
	if len(req.HintTargetNodes) != 2 {
		t.Errorf("hint targets: got %d want 2", len(req.HintTargetNodes))
	}
}

func TestParseMigrationRequest_UnsupportedSchema(t *testing.T) {
	raw := `{"schemaVersion":"v99","component":"engine","instance":0,"from_node":"n"}`
	if _, err := ParseMigrationRequest(raw); err == nil {
		t.Fatal("expected UnsupportedSchemaVersion error")
	}
}

func TestAuditLedger_TrimDropsOldestTerminalButKeepsStarted(t *testing.T) {
	l := &Ledger{}
	// Seed an in-flight entry first — must survive trim.
	l.UpsertEntry(Entry{RequestUUID: "in-flight-pinned", Phase: PhaseStarted})
	for i := 0; i < MaxTerminalEntries+50; i++ {
		l.UpsertEntry(Entry{
			RequestUUID: fmt.Sprintf("done-%04d", i),
			Phase:       PhaseCompleted,
		})
	}
	terminalCount := 0
	startedSurvived := false
	for _, e := range l.Entries {
		switch e.Phase {
		case PhaseCompleted, PhaseFailed:
			terminalCount++
		case PhaseStarted:
			if e.RequestUUID == "in-flight-pinned" {
				startedSurvived = true
			}
		}
	}
	if terminalCount != MaxTerminalEntries {
		t.Errorf("terminal entries: got %d want %d", terminalCount, MaxTerminalEntries)
	}
	if !startedSurvived {
		t.Errorf("Started entry must survive trim; ledger=%v", l.Entries)
	}
	// The oldest terminal (done-0000) should have been dropped.
	for _, e := range l.Entries {
		if e.RequestUUID == "done-0000" {
			t.Errorf("oldest terminal entry should have been trimmed; got %+v", e)
		}
	}
}

func TestAuditLedger_DedupAcrossPersist(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b", Namespace: "prod", UID: types.UID("owner-uid"),
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()
	gvk := corev1.SchemeGroupVersion.WithKind("ConfigMap")

	ledger, err := LoadLedgerForOwner(context.Background(), c, owner)
	if err != nil {
		t.Fatalf("load empty ledger: %v", err)
	}
	if len(ledger.Entries) != 0 {
		t.Fatalf("expected empty ledger, got %d entries", len(ledger.Entries))
	}

	ledger.UpsertEntry(Entry{RequestUUID: "abc", Phase: PhaseCompleted})
	if err := PersistLedgerForOwner(context.Background(), c, owner, gvk, ledger); err != nil {
		t.Fatalf("persist: %v", err)
	}

	reloaded, err := LoadLedgerForOwner(context.Background(), c, owner)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !reloaded.HasCompletedOrFailedRequest("abc") {
		t.Errorf("expected reloaded ledger to record abc as completed")
	}
	if reloaded.HasCompletedOrFailedRequest("xyz") {
		t.Errorf("unrelated UUID should not be flagged")
	}
}

func newLedgerTestClient(t *testing.T) (client.Client, *corev1.ConfigMap, schema.GroupVersionKind) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b", Namespace: "prod", UID: types.UID("owner-uid"),
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owner).Build()
	return c, owner, corev1.SchemeGroupVersion.WithKind("ConfigMap")
}

func TestPersistLedgerForOwner_ConflictOnConcurrentUpdate(t *testing.T) {
	c, owner, gvk := newLedgerTestClient(t)
	ctx := context.Background()

	seed, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("seed load: %v", err)
	}
	seed.UpsertEntry(Entry{RequestUUID: "seed", Phase: PhaseCompleted})
	if err := PersistLedgerForOwner(ctx, c, owner, gvk, seed); err != nil {
		t.Fatalf("seed persist: %v", err)
	}

	// Two writers load the same revision; A lands first, B must 409
	// rather than erase A's row.
	a, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	b, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	a.UpsertEntry(Entry{RequestUUID: "from-a", Phase: PhaseCompleted})
	if err := PersistLedgerForOwner(ctx, c, owner, gvk, a); err != nil {
		t.Fatalf("persist A: %v", err)
	}
	b.UpsertEntry(Entry{RequestUUID: "from-b", Phase: PhaseCompleted})
	err = PersistLedgerForOwner(ctx, c, owner, gvk, b)
	if !apierrors.IsConflict(err) {
		t.Fatalf("persist B: want conflict, got %v", err)
	}

	// B's retry path: reload, re-apply, persist. Both rows survive.
	b2, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("reload B: %v", err)
	}
	b2.UpsertEntry(Entry{RequestUUID: "from-b", Phase: PhaseCompleted})
	if err := PersistLedgerForOwner(ctx, c, owner, gvk, b2); err != nil {
		t.Fatalf("persist B retry: %v", err)
	}
	final, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("final load: %v", err)
	}
	for _, uuid := range []string{"seed", "from-a", "from-b"} {
		if !final.HasCompletedOrFailedRequest(uuid) {
			t.Errorf("entry %q lost after concurrent persists", uuid)
		}
	}
}

func TestPersistLedgerForOwner_ConflictOnCreateRace(t *testing.T) {
	c, owner, gvk := newLedgerTestClient(t)
	ctx := context.Background()

	// Loaded before the CM existed.
	stale, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// Concurrent writer creates the CM with its own entry.
	other, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("load other: %v", err)
	}
	other.UpsertEntry(Entry{RequestUUID: "winner", Phase: PhaseCompleted})
	if err := PersistLedgerForOwner(ctx, c, owner, gvk, other); err != nil {
		t.Fatalf("persist other: %v", err)
	}

	stale.UpsertEntry(Entry{RequestUUID: "loser", Phase: PhaseCompleted})
	err = PersistLedgerForOwner(ctx, c, owner, gvk, stale)
	if !apierrors.IsConflict(err) {
		t.Fatalf("want conflict on create race, got %v", err)
	}
	final, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("final load: %v", err)
	}
	if !final.HasCompletedOrFailedRequest("winner") {
		t.Errorf("winner's entry erased by create race")
	}
}

func TestPersistLedgerForOwner_SequentialPersistsSamePass(t *testing.T) {
	c, owner, gvk := newLedgerTestClient(t)
	ctx := context.Background()

	l, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	l.UpsertEntry(Entry{RequestUUID: "u1", Phase: PhaseStarted})
	if err := PersistLedgerForOwner(ctx, c, owner, gvk, l); err != nil {
		t.Fatalf("first persist: %v", err)
	}
	// Same in-memory ledger persisted again (migrate.go's Started →
	// Completed tail within one pass) must chain, not self-conflict.
	l.UpsertEntry(Entry{RequestUUID: "u1", Phase: PhaseCompleted})
	if err := PersistLedgerForOwner(ctx, c, owner, gvk, l); err != nil {
		t.Fatalf("second persist: %v", err)
	}
	final, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("final load: %v", err)
	}
	if !final.HasCompletedOrFailedRequest("u1") {
		t.Errorf("second persist of the same ledger did not land")
	}
}

func TestPersistLedgerForOwner_ConflictWhenDeletedSinceLoad(t *testing.T) {
	c, owner, gvk := newLedgerTestClient(t)
	ctx := context.Background()

	l, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	l.UpsertEntry(Entry{RequestUUID: "u1", Phase: PhaseStarted})
	if err := PersistLedgerForOwner(ctx, c, owner, gvk, l); err != nil {
		t.Fatalf("persist: %v", err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: ConfigMapNameForOwner(owner), Namespace: owner.Namespace,
	}}
	if err := c.Delete(ctx, cm); err != nil {
		t.Fatalf("delete cm: %v", err)
	}
	l.UpsertEntry(Entry{RequestUUID: "u1", Phase: PhaseCompleted})
	if err := PersistLedgerForOwner(ctx, c, owner, gvk, l); !apierrors.IsConflict(err) {
		t.Fatalf("want conflict when CM deleted since load, got %v", err)
	}
}

func TestPersistLedgerForOwner_ReplacesStaleControllerRef(t *testing.T) {
	c, owner, gvk := newLedgerTestClient(t)
	ctx := context.Background()

	// A predecessor owner (same name, different UID) left its
	// controller ref behind; a non-controller ref must survive.
	truePtr := true
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigMapNameForOwner(owner),
			Namespace: owner.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind,
					Name: owner.Name, UID: types.UID("stale-owner-uid"),
					Controller: &truePtr, BlockOwnerDeletion: &truePtr,
				},
				{
					APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind,
					Name: "bystander", UID: types.UID("bystander-uid"),
				},
			},
		},
		Data: map[string]string{LedgerKey: `{"entries":[]}`},
	}
	if err := c.Create(ctx, cm); err != nil {
		t.Fatalf("seed cm: %v", err)
	}

	l, err := LoadLedgerForOwner(ctx, c, owner)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	l.UpsertEntry(Entry{RequestUUID: "u1", Phase: PhaseStarted})
	if err := PersistLedgerForOwner(ctx, c, owner, gvk, l); err != nil {
		t.Fatalf("persist: %v", err)
	}

	got := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: owner.Namespace, Name: cm.Name}, got); err != nil {
		t.Fatalf("get cm: %v", err)
	}
	controllers := 0
	bystanderKept := false
	for _, ref := range got.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			controllers++
			if ref.UID != owner.UID {
				t.Errorf("controller ref points at %q, want current owner %q", ref.UID, owner.UID)
			}
		}
		if ref.UID == types.UID("bystander-uid") {
			bystanderKept = true
		}
	}
	if controllers != 1 {
		t.Errorf("controller refs: got %d want exactly 1 (%+v)", controllers, got.OwnerReferences)
	}
	if !bystanderKept {
		t.Errorf("non-controller ownerRef dropped during adoption: %+v", got.OwnerReferences)
	}
}

func TestInFlightEntry_ReturnsCopyNotAlias(t *testing.T) {
	l := &Ledger{}
	l.UpsertEntry(Entry{RequestUUID: "u1", Phase: PhaseStarted, FromNode: "n1"})

	e := l.InFlightEntry("u1")
	if e == nil {
		t.Fatal("expected in-flight entry")
	}
	e.Phase = PhaseFailed
	if l.Entries[0].Phase != PhaseStarted {
		t.Errorf("mutating InFlightEntry result leaked into the ledger: %+v", l.Entries[0])
	}

	// Compaction must not re-point a held result at a different row.
	held := l.InFlightEntry("u1")
	for i := 0; i < MaxTerminalEntries+10; i++ {
		l.UpsertEntry(Entry{RequestUUID: fmt.Sprintf("t-%04d", i), Phase: PhaseCompleted})
	}
	if held.RequestUUID != "u1" || held.Phase != PhaseStarted {
		t.Errorf("held entry mutated by trim/upsert compaction: %+v", held)
	}
}
