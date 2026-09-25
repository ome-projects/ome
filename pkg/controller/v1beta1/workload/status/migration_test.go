package status

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The pin writes one side of the pair: the source goes Migrating and the
// surge Creating, each naming the other as SurgeIndex under the
// request's UUID with the deadline the timeout gives. A second pin of
// the same request is a no-op, and a source slot the writer had to
// append is never resurrected.
func TestStampMigrationPin_PinsEachSideOnceAndResurrectsNoSource(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	w := newRowWriter(types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady, Incarnation: 2})

	if err := StampMigrationPin(ctx, w.input(now), 0, 1, MigrationRoleSource, "req-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := StampMigrationPin(ctx, w.input(now), 1, 0, MigrationRoleSurge, "req-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	source, surge := w.rows[0], w.rows[1]
	if source.Phase != types.InstancePhaseMigrating || source.Incarnation != 2 ||
		source.Operation == nil || source.Operation.Type != types.InstanceOperationMigrate ||
		source.Operation.RequestUUID != "req-1" ||
		source.Operation.SurgeIndex == nil || *source.Operation.SurgeIndex != 1 {
		t.Fatalf("source = %+v, want Migrating pinned to surge 1 under req-1", source)
	}
	if surge.Phase != types.InstancePhaseCreating || surge.Incarnation != 1 ||
		surge.Operation == nil || surge.Operation.SurgeIndex == nil || *surge.Operation.SurgeIndex != 0 {
		t.Fatalf("surge = %+v, want Creating at Incarnation 1 pinned to source 0", surge)
	}
	if !source.Operation.Deadline.Time.Equal(now.Add(time.Hour)) {
		t.Fatalf("deadline = %v, want now+timeout", source.Operation.Deadline.Time)
	}

	writes := w.writes
	if err := StampMigrationPin(ctx, w.input(now), 0, 1, MigrationRoleSource, "req-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := StampMigrationPin(ctx, w.input(now), 1, 0, MigrationRoleSurge, "req-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if w.writes != writes {
		t.Fatalf("writes = %d, want the same request pinned again to be a no-op (%d)", w.writes, writes)
	}

	empty := newRowWriter()
	if err := StampMigrationPin(ctx, empty.input(now), 4, 5, MigrationRoleSource, "req-2", time.Hour); err != nil {
		t.Fatal(err)
	}
	if empty.writes != 0 {
		t.Fatal("a source slot with no phase must not be resurrected")
	}
}

func TestMigrationSourceRemovalOwnership(t *testing.T) {
	surge := int32(9)
	migration := terminalStatusFixture(3)
	migration.Phase = types.InstancePhaseMigrating
	migration.TargetRevision = ""
	migration.Operation.Type = types.InstanceOperationMigrate
	migration.Operation.RequestUUID = "migration-a"
	if !MigrationSourceOwnsRemoval(&migration, "migration-a", surge) ||
		MigrationSourceOwnsRemoval(&migration, "migration-b", surge) ||
		MigrationSourceOwnsRemoval(&migration, "migration-a", surge+1) {
		t.Fatal("migration source ownership did not bind request and surge identities")
	}
	wrongMigrationPhase := migration
	wrongMigrationPhase.Phase = types.InstancePhaseReady
	if MigrationSourceOwnsRemoval(&wrongMigrationPhase, "migration-a", surge) {
		t.Fatal("migration source ownership accepted a non-migrating lifecycle")
	}
}

// migrationPair is a source Ready on a revision and a surge slot the
// pair may claim, plus the input an owner-fenced batch write needs.
func migrationPair(store *terminalMutationStore) (source types.InstanceStatus, input types.ReconcileInput) {
	source = types.InstanceStatus{Index: 0, Incarnation: 2, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"}
	store.statuses = map[int32]types.InstanceStatus{source.Index: cloneTerminalStatus(source)}
	input = types.ReconcileInput{
		OwnerObject:                          &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	return source, input
}

// The pair is written once, fenced on the source the pass read; a source
// whose lifecycle moved since, or an owner replaced since, writes nothing.
func TestStartMigration_WritesThePairOnlyAgainstTheSourceItRead(t *testing.T) {
	store := &terminalMutationStore{ownerUID: "owner-a"}
	source, input := migrationPair(store)

	started, err := StartMigration(context.Background(), input, &source, nil, "req-1", 1, time.Hour)
	if err != nil || !started || store.writes != 1 {
		t.Fatalf("start = %v, %v (writes=%d), want the pair written once", started, err, store.writes)
	}
	if !MigrationSourceOwnsRemoval(ptr(store.statuses[0]), "req-1", 1) || !MigrationSurgeOwnsPromotion(ptr(store.statuses[1]), "req-1", 0) {
		t.Fatalf("pair = %+v / %+v, want source pinned to surge 1 and surge pinned to source 0", store.statuses[0], store.statuses[1])
	}

	drifted := &terminalMutationStore{ownerUID: "owner-a"}
	source, input = migrationPair(drifted)
	moved := drifted.statuses[0]
	moved.Incarnation++
	drifted.statuses[0] = moved
	started, err = StartMigration(context.Background(), input, &source, nil, "req-1", 1, time.Hour)
	if err != nil || started || drifted.writes != 0 {
		t.Fatalf("start against a drifted source = %v, %v (writes=%d), want nothing written", started, err, drifted.writes)
	}

	replaced := &terminalMutationStore{ownerUID: "owner-b"}
	source, input = migrationPair(replaced)
	started, err = StartMigration(context.Background(), input, &source, nil, "req-1", 1, time.Hour)
	if err != nil || started || replaced.writes != 0 {
		t.Fatalf("start against a replaced owner = %v, %v (writes=%d), want nothing written", started, err, replaced.writes)
	}
}

// The promote lands only while both sides are still the pair the pass
// read; a surge that has since been re-pinned is left alone.
func TestStampMigrationSurgeReady_RequiresBothSidesOfThePair(t *testing.T) {
	store := &terminalMutationStore{ownerUID: "owner-a"}
	source, input := migrationPair(store)
	if _, err := StartMigration(context.Background(), input, &source, nil, "req-1", 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	pinnedSource, surge := store.statuses[0], store.statuses[1]

	drifted := surge
	drifted.Operation = &types.InstanceOperation{ID: "other", Type: types.InstanceOperationMigrate, RequestUUID: "req-2"}
	store.statuses[1] = drifted
	promoted, err := StampMigrationSurgeReady(context.Background(), input, &pinnedSource, &surge, "req-1", "rev-a")
	if err != nil || promoted || store.writes != 1 {
		t.Fatalf("promote against a drifted surge = %v, %v (writes=%d), want nothing written", promoted, err, store.writes)
	}

	store.statuses[1] = surge
	promoted, err = StampMigrationSurgeReady(context.Background(), input, &pinnedSource, &surge, "req-1", "rev-a")
	if err != nil || !promoted || store.writes != 2 {
		t.Fatalf("promote = %v, %v (writes=%d), want the surge promoted", promoted, err, store.writes)
	}
	if !MigrationPromotedSurgeMatches(ptr(store.statuses[1]), "rev-a") {
		t.Fatalf("surge = %+v, want Ready on rev-a with the pin cleared", store.statuses[1])
	}
}

// The pin is dropped only for its own request; another request's pin, an
// unpinned row and a slot the writer had to append are all left alone.
func TestClearMigrationPin_DropsOnlyTheRequestsOwnPin(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	pin := &types.InstanceOperation{ID: "migrate-req-1", Type: types.InstanceOperationMigrate, RequestUUID: "req-1"}
	w := newRowWriter(types.InstanceStatus{Index: 0, Phase: types.InstancePhaseMigrating, Operation: pin})

	if err := ClearMigrationPin(context.Background(), w.input(now), 0, "req-2"); err != nil {
		t.Fatal(err)
	}
	if w.writes != 0 || w.rows[0].Operation == nil {
		t.Fatal("another request's pin must be left alone")
	}
	if err := ClearMigrationPin(context.Background(), w.input(now), 0, "req-1"); err != nil {
		t.Fatal(err)
	}
	if w.writes != 1 || w.rows[0].Operation != nil || w.rows[0].Phase != types.InstancePhaseMigrating {
		t.Fatalf("row = %+v, want the pin dropped and nothing else moved", w.rows[0])
	}
	if err := ClearMigrationPin(context.Background(), w.input(now), 0, "req-1"); err != nil {
		t.Fatal(err)
	}
	if err := ClearMigrationPin(context.Background(), w.input(now), 7, "req-1"); err != nil {
		t.Fatal(err)
	}
	if w.writes != 1 {
		t.Fatal("an unpinned row and an appended slot must write nothing")
	}
}

// The restore returns the source to the phase its pods justify and drops
// the pin in the same write; an already-restored row and an appended
// slot write nothing.
func TestStampMigrationSourceRestored_FollowsTheLivePodsOnce(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	pin := func() *types.InstanceOperation {
		return &types.InstanceOperation{ID: "migrate-req-1", Type: types.InstanceOperationMigrate, RequestUUID: "req-1"}
	}

	healthy := newRowWriter(types.InstanceStatus{Index: 0, Phase: types.InstancePhaseMigrating, Operation: pin()})
	if err := StampMigrationSourceRestored(context.Background(), healthy.input(now), 0, "req-1", true, "Expired", "detail"); err != nil {
		t.Fatal(err)
	}
	if row := healthy.rows[0]; row.Phase != types.InstancePhaseReady || row.Operation != nil || row.LastFailure != nil {
		t.Fatalf("row = %+v, want Ready with the pin dropped and no record", row)
	}
	if err := StampMigrationSourceRestored(context.Background(), healthy.input(now), 0, "req-1", true, "Expired", "detail"); err != nil {
		t.Fatal(err)
	}
	if healthy.writes != 1 {
		t.Fatal("a restored row must not be written again")
	}

	wedged := newRowWriter(types.InstanceStatus{Index: 0, Phase: types.InstancePhaseMigrating, Operation: pin()})
	if err := StampMigrationSourceRestored(context.Background(), wedged.input(now), 0, "req-1", false, "Expired", "detail"); err != nil {
		t.Fatal(err)
	}
	row := wedged.rows[0]
	if row.Phase != types.InstancePhaseFailed || row.Operation != nil ||
		row.LastFailure == nil || row.LastFailure.Reason != "Expired" || !row.LastFailure.Time.Time.Equal(now) {
		t.Fatalf("row = %+v, want Failed with the record and the pin dropped", row)
	}

	empty := newRowWriter()
	if err := StampMigrationSourceRestored(context.Background(), empty.input(now), 3, "req-1", true, "Expired", "detail"); err != nil {
		t.Fatal(err)
	}
	if empty.writes != 0 {
		t.Fatal("a source slot with no phase must not be resurrected")
	}
}

func ptr(row types.InstanceStatus) *types.InstanceStatus { return &row }
