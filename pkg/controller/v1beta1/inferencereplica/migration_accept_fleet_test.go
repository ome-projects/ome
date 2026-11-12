package inferencereplica

import (
	"context"
	"flag"
	"path/filepath"
	"testing"

	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateMigrationSyncGoldens = flag.Bool("update-migration-sync", false, "re-record testdata/migration_sync_allocations.json from this run")

// migrationSyncRows is the largest reference replica: 4,000 Instances in
// one object.
const migrationSyncRows = 4000

// migrationSyncColumnBound is the row bound the operator configures for the
// column form of the status.
const migrationSyncColumnBound = 20000

// migrationSyncShape is one replica the migration sync runs over: its rows,
// the migration records the stored object and the pass's copy carry, and
// the migration request annotations on the parent.
type migrationSyncShape struct {
	name     string
	rows     int
	rowsOf   func(int) []v1beta1.OMENativeInstanceStatus
	records  []v1beta1.MigrationStatus
	requests map[string]string
}

// migrationSyncShapes are the budget's shapes at 4,000 Instances: at rest
// with every row equal, at rest as a reference replica stores it (every row
// with its own readySince), and the same replica with one migration in
// flight.
func migrationSyncShapes() []migrationSyncShape {
	return []migrationSyncShape{
		{name: "uniform/4000/at-rest", rows: migrationSyncRows, rowsOf: irstatustest.UniformRows},
		{name: "readySince/4000/at-rest", rows: migrationSyncRows, rowsOf: irstatustest.ReadySinceRows},
		{name: "readySince/4000/in-flight", rows: migrationSyncRows, rowsOf: irstatustest.ReadySinceRows,
			records: []v1beta1.MigrationStatus{acceptedEntry("u-in-flight")}},
	}
}

// migrationSyncFixture is one replica behind the fake apiserver with the
// reconciler's two views of it: the cache the pass's copy came from, and
// the live reader whose reads of the replica are counted.
type migrationSyncFixture struct {
	reconciler *Reconciler
	cache      client.Client
	// passCopy is the replica as the pass holds it: decoded from the cache.
	passCopy *v1beta1.InferenceReplica
	parent   *v1beta1.InferenceService
	// liveReads counts the live reads of the replica; writes counts every
	// write through either view.
	liveReads *fakeclient.Calls
	writes    *fakeclient.Calls
}

// newMigrationSyncFixture stores the replica in the column form production
// writes, with the shape's records, and decodes the pass's copy from it. No
// audit record exists, as on a replica that never migrated.
func newMigrationSyncFixture(tb testing.TB, shape migrationSyncShape) *migrationSyncFixture {
	tb.Helper()
	ir := baselineIR("llama-engine", "default", int32(shape.rows))
	ir.Status = *irstatustest.LogicalStatus(shape.rowsOf(shape.rows))
	for _, entry := range shape.records {
		ir.Status.Migrations = append(ir.Status.Migrations, *entry.DeepCopy())
	}
	selection, err := irstatus.SelectCandidate(&ir.Status, migrationSyncColumnBound)
	if err != nil || selection.Selected != irstatus.EncodingColumnarV2 {
		tb.Fatalf("%s: SelectCandidate() = %+v, %v; the column form must be selected", shape.name, selection, err)
	}
	stored := ir.DeepCopy()
	stored.Status = *selection.SelectedCandidate().Status
	parent := migrationParent(shape.requests, false)

	writes := fakeclient.CountingWrites()
	cache := fakeclient.New(tb,
		fakeclient.WithObjects(stored, parent),
		fakeclient.WithStatusSubresource(&v1beta1.InferenceReplica{}),
		writes)
	liveReads := fakeclient.CountingVerb(fakeclient.Get).OnKind(&v1beta1.InferenceReplica{})
	live := fakeclient.Wrap(tb, cache, liveReads)

	decoder := irstatus.NewDecoder(migrationSyncColumnBound)
	passCopy := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(context.Background(), irstatus.NewReader(cache, decoder), client.ObjectKeyFromObject(stored), passCopy); err != nil {
		tb.Fatalf("%s: decode the pass's copy: %v", shape.name, err)
	}
	return &migrationSyncFixture{
		reconciler: &Reconciler{
			Client:                cache,
			APIReader:             live,
			Log:                   logf.Log.WithName("migration-sync"),
			Recorder:              record.NewFakeRecorder(16),
			Expectations:          workloadtypes.NewExpectations(),
			InstanceStatusDecoder: decoder,
			InstanceStatusTarget:  irstatus.EncodingColumnarV2,
			Clock:                 clocktesting.NewFakeClock(migrationTestNow),
		},
		cache:     cache,
		passCopy:  passCopy,
		parent:    parent,
		liveReads: liveReads,
		writes:    writes,
	}
}

// sync runs one migration sync of the pass over the fixture's copy.
func (f *migrationSyncFixture) sync(tb testing.TB) {
	tb.Helper()
	if err := f.reconciler.syncMigrationEntries(context.Background(), f.reconciler.Log, f.passCopy, f.parent, migrationTestTimeout, migrationTestRecordWindow); err != nil {
		tb.Fatalf("syncMigrationEntries: %v", err)
	}
}

// syncCountingLiveReads runs one sync and returns the live reads of the
// replica it made.
func (f *migrationSyncFixture) syncCountingLiveReads(tb testing.TB) int {
	tb.Helper()
	before := f.liveReads.Count()
	f.sync(tb)
	return f.liveReads.Count() - before
}

// storedRecords reads the migration records the stored replica carries.
func (f *migrationSyncFixture) storedRecords(tb testing.TB) []v1beta1.MigrationStatus {
	tb.Helper()
	fresh := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(context.Background(), irstatus.NewReader(f.cache, f.reconciler.InstanceStatusDecoder), client.ObjectKeyFromObject(f.passCopy), fresh); err != nil {
		tb.Fatalf("read the stored replica: %v", err)
	}
	return fresh.Status.Migrations
}

// BenchmarkMigrationSync is the cost of the migration sync of one replica
// pass at fleet size, with the live reads of the replica it makes beside
// the time.
func BenchmarkMigrationSync(b *testing.B) {
	for _, shape := range migrationSyncShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newMigrationSyncFixture(b, shape)
			b.ReportAllocs()
			start := f.liveReads.Count()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f.sync(b)
			}
			b.StopTimer()
			b.ReportMetric(float64(f.liveReads.Count()-start)/float64(b.N), "live-reads/op")
		})
	}
}

// TestMigrationSyncStaysWithinBudget holds the allocations and the live
// reads of the migration sync of one pass at 4,000 Instances to the
// recorded golden. A read count of 0 or 1 has no slack under the golden's
// tolerance, so the reads are held exactly. A pass with nothing to import
// or trim writes nothing.
func TestMigrationSyncStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "migration_sync_allocations.json"), *updateMigrationSyncGoldens)
	defer golden.Close()
	for _, shape := range migrationSyncShapes() {
		f := newMigrationSyncFixture(t, shape)
		golden.Check(shape.name+"/allocations", func() { f.sync(t) })
		reads := f.syncCountingLiveReads(t)
		golden.CheckCount(shape.name+"/live-reads", int64(reads))
		t.Logf("%s: %d live reads of the replica per sync", shape.name, reads)
		if n := f.writes.Count(); n != 0 {
			t.Errorf("%s: the sync wrote %d times; a pass with nothing to import or trim writes nothing", shape.name, n)
		}
	}
}

// smallMigrationSyncShape is a replica of eight Instances for the behaviour
// tests, with the given records stored and the given requests on the parent.
func smallMigrationSyncShape(records []v1beta1.MigrationStatus, requests map[string]string) migrationSyncShape {
	return migrationSyncShape{name: "uniform/8", rows: 8, rowsOf: irstatustest.UniformRows, records: records, requests: requests}
}

func TestSyncMigrationEntries_ReplicaAtRestIsNotReadLive(t *testing.T) {
	f := newMigrationSyncFixture(t, smallMigrationSyncShape(nil, nil))

	reads := f.syncCountingLiveReads(t)

	if reads != 0 {
		t.Errorf("live reads of the replica = %d, want 0: the copy carries no record, nothing is importable and the parent carries no request", reads)
	}
	if n := f.writes.Count(); n != 0 {
		t.Errorf("writes = %d, want 0", n)
	}
	if len(f.passCopy.Status.Migrations) != 0 {
		t.Errorf("the copy's records = %+v, want none", f.passCopy.Status.Migrations)
	}
}

func TestSyncMigrationEntries_RecordInFlightIsRefreshedFromLive(t *testing.T) {
	live := acceptedEntry("u-moving")
	live.Phase = v1beta1.MigrationPhaseSurgeReady
	f := newMigrationSyncFixture(t, smallMigrationSyncShape([]v1beta1.MigrationStatus{live}, nil))
	// The copy is one write behind: it still shows the record as Accepted.
	f.passCopy.Status.Migrations[0].Phase = v1beta1.MigrationPhaseAccepted

	reads := f.syncCountingLiveReads(t)

	if reads != 1 {
		t.Errorf("live reads of the replica = %d, want 1: a copy with a record is read live so the pass acts on the record's live phase", reads)
	}
	if n := f.writes.Count(); n != 0 {
		t.Errorf("writes = %d, want 0: nothing to import or trim", n)
	}
	if got := f.passCopy.Status.Migrations; len(got) != 1 || got[0].Phase != v1beta1.MigrationPhaseSurgeReady {
		t.Errorf("the copy's records = %+v, want u-moving at SurgeReady, the live phase", got)
	}
}

func TestSyncMigrationEntries_RequestOnTheParentIsRefreshedFromLive(t *testing.T) {
	requests := map[string]string{audit.MigrationRequestAnnotationPrefix + "u-asked": `{"component":"engine","instance":0}`}
	f := newMigrationSyncFixture(t, smallMigrationSyncShape([]v1beta1.MigrationStatus{acceptedEntry("u-asked")}, requests))
	// The copy is one write behind: the previous pass accepted the request
	// and the record is not in the cache yet.
	f.passCopy.Status.Migrations = nil

	reads := f.syncCountingLiveReads(t)

	if reads != 1 {
		t.Errorf("live reads of the replica = %d, want 1: a request on the parent is classified against the records the replica carries live", reads)
	}
	if n := f.writes.Count(); n != 0 {
		t.Errorf("writes = %d, want 0", n)
	}
	if got := f.passCopy.Status.Migrations; len(got) != 1 || got[0].RequestUUID != "u-asked" {
		t.Errorf("the copy's records = %+v, want the accepted record u-asked mirrored onto it", got)
	}
}

func TestSyncMigrationEntries_ReplacedReplicaAtRestIsLeftAlone(t *testing.T) {
	f := newMigrationSyncFixture(t, smallMigrationSyncShape([]v1beta1.MigrationStatus{acceptedEntry("u-1")}, nil))
	// The pass holds the copy of a replica that was deleted and re-created
	// under the same name; the copy is at rest.
	f.passCopy.UID = "deleted-uid"
	f.passCopy.Status.Migrations = nil

	reads := f.syncCountingLiveReads(t)

	if reads != 0 {
		t.Errorf("live reads of the replica = %d, want 0: a copy at rest is not read, so the replacement is found by the first write of the pass", reads)
	}
	if n := f.writes.Count(); n != 0 {
		t.Errorf("writes = %d, want 0: nothing lands on the replacement", n)
	}
	if got := f.storedRecords(t); len(got) != 1 || got[0].RequestUUID != "u-1" {
		t.Errorf("stored records = %+v, want the replacement's own record u-1 untouched", got)
	}
}
