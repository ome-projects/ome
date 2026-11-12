package irstatus

import (
	"encoding/json"
	"flag"
	"fmt"
	"path/filepath"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateFleetGoldens = flag.Bool("update", false, "re-record testdata/fleet_allocations.json and testdata/fleet_status_bytes.json from this run")

// fleetRows is the benchmark's largest shape: 4,000 Instances on one
// InferenceReplica.
const fleetRows = 4000

// fleetGangSize is the pods per Instance of the gang shape: a leader and
// seven workers.
const fleetGangSize = 8

// fleetFixture is one shape at fleet size with both stored forms built once:
// the name the benchmarks and the goldens share, the logical status, the
// writer's selection over it, and the serialized bytes of each form.
type fleetFixture struct {
	name      string
	logical   *v1beta1.InferenceReplicaStatus
	selection *Selection
	dense     []byte
	columnar  []byte
}

// fleetShape is one row set at one size: the name the benchmarks and the
// goldens share, and the rows.
type fleetShape struct {
	name string
	rows []v1beta1.OMENativeInstanceStatus
}

// fleetShapes are the shapes at n Instances: at rest with every row equal
// (uniform); at rest as a reference replica stores it, every row carrying
// its own readySince (readySince); and mid-roll with the representative mix
// of phases, revisions and records (representative).
func fleetShapes(n int) []fleetShape {
	return []fleetShape{
		{name: fmt.Sprintf("uniform/%d", n), rows: uniformRows(n)},
		{name: fmt.Sprintf("readySince/%d", n), rows: readySinceRows(n)},
		{name: fmt.Sprintf("representative/%d", n), rows: representativeRows(n)},
	}
}

// fleetBytesShapes are the shapes whose stored bytes are held, at a quarter
// of the largest replica and at the largest: the three fleet shapes and a
// replica of gangs at rest, every gang carrying its own readySince.
func fleetBytesShapes() []fleetShape {
	var shapes []fleetShape
	for _, n := range []int{fleetRows / 4, fleetRows} {
		shapes = append(shapes, fleetShapes(n)...)
		shapes = append(shapes, fleetShape{name: fmt.Sprintf("gangReadySince/%d", n), rows: irstatustest.ReadySinceGangRows(n, fleetGangSize)})
	}
	return shapes
}

// selectShape is the writer's selection over one shape's rows; every shape
// here is eligible for columns.
func selectShape(tb testing.TB, shape fleetShape) *Selection {
	tb.Helper()
	selection, err := SelectCandidate(logicalStatus(shape.rows), testLimit)
	if err != nil || selection.ColumnarV2 == nil {
		tb.Fatalf("%s: SelectCandidate() = %+v, %v", shape.name, selection, err)
	}
	return selection
}

// fleetFixtures are the shapes at 4,000 Instances with both stored forms
// built once.
func fleetFixtures(tb testing.TB) []fleetFixture {
	tb.Helper()
	shapes := fleetShapes(fleetRows)
	fixtures := make([]fleetFixture, 0, len(shapes))
	for _, shape := range shapes {
		selection := selectShape(tb, shape)
		fixtures = append(fixtures, fleetFixture{
			name:      shape.name,
			logical:   logicalStatus(shape.rows),
			selection: selection,
			dense:     mustJSON(tb, selection.DenseV1.Status),
			columnar:  mustJSON(tb, selection.ColumnarV2.Status),
		})
	}
	return fixtures
}

// fleetOp is one codec path at fleet size: the name the benchmark and the
// allocation golden share, the operation, and the stored bytes a decode reads.
type fleetOp struct {
	name  string
	op    func() error
	bytes int
}

// fleetEncodeOps is one status write's encode per fixture: the plain list
// alone, and the writer's selection, which builds both shapes and keeps the
// smaller one.
func fleetEncodeOps(tb testing.TB) []fleetOp {
	tb.Helper()
	var ops []fleetOp
	for _, fixture := range fleetFixtures(tb) {
		ops = append(ops,
			fleetOp{name: fixture.name + "/denseV1", op: encodeDenseV1Op(fixture.logical)},
			fleetOp{name: fixture.name + "/selected", op: encodeSelectedOp(fixture.logical)})
	}
	return ops
}

// fleetDecodeOps is one read's decode per fixture and stored shape, and the
// columns once more through the Decoder a controller reads a fetched object
// with, which drops the payload once the rows exist.
func fleetDecodeOps(tb testing.TB) []fleetOp {
	tb.Helper()
	var ops []fleetOp
	for _, fixture := range fleetFixtures(tb) {
		ops = append(ops,
			fleetOp{name: fixture.name + "/denseV1", op: decodeStatusOp(fixture.dense), bytes: len(fixture.dense)},
			fleetOp{name: fixture.name + "/columnarV2", op: decodeStatusOp(fixture.columnar), bytes: len(fixture.columnar)},
			fleetOp{name: fixture.name + "/columnarV2/fetched", op: decodeFetchedOp(fixture.columnar), bytes: len(fixture.columnar)})
	}
	return ops
}

// fleetSelectOps is the writer's selection per shape at a quarter of the
// largest replica and at the largest: both candidates built from the
// logical status and the smaller one kept, before the client serializes
// the chosen one.
func fleetSelectOps() []fleetOp {
	var ops []fleetOp
	for _, n := range []int{fleetRows / 4, fleetRows} {
		for _, shape := range fleetShapes(n) {
			ops = append(ops, fleetOp{name: shape.name, op: selectCandidateOp(logicalStatus(shape.rows))})
		}
	}
	return ops
}

// selectCandidateOp is one selection over logical.
func selectCandidateOp(logical *v1beta1.InferenceReplicaStatus) func() error {
	return func() error {
		_, err := SelectCandidate(logical, testLimit)
		return err
	}
}

// decodeFetchedOp is the decode a controller runs on a fetched object: the
// stored bytes into the object, then the Decoder over it.
func decodeFetchedOp(data []byte) func() error {
	return func() error {
		var ir v1beta1.InferenceReplica
		if err := json.Unmarshal(data, &ir.Status); err != nil {
			return err
		}
		_, err := NewDecoder(testLimit).Decode(&ir)
		return err
	}
}

// BenchmarkFleetEncodeStatus is the cost of one status write's encode at
// fleet size.
func BenchmarkFleetEncodeStatus(b *testing.B) {
	for _, op := range fleetEncodeOps(b) {
		b.Run(op.name, func(b *testing.B) { benchmarkOp(b, op.op) })
	}
}

// BenchmarkFleetSelectCandidate is the cost of one status write's selection
// at 1,000 and 4,000 Instances.
func BenchmarkFleetSelectCandidate(b *testing.B) {
	for _, op := range fleetSelectOps() {
		b.Run(op.name, func(b *testing.B) { benchmarkOp(b, op.op) })
	}
}

// BenchmarkFleetDecodeStatus is the cost of one read's decode at fleet size
// for each stored shape, with the stored bytes of that shape reported beside
// the time.
func BenchmarkFleetDecodeStatus(b *testing.B) {
	for _, op := range fleetDecodeOps(b) {
		b.Run(op.name, func(b *testing.B) {
			b.ReportMetric(float64(op.bytes), "status-bytes")
			benchmarkOp(b, op.op)
		})
	}
}

// TestFleetStatusBytesStayWithinBudget holds the bytes one replica's status
// takes at 1,000 and 4,000 Instances, single-pod and gangs of eight, in the
// form the writer selects, to the recorded golden: the bytes the API server
// stores and serves per replica. It also reports what the per-row readySince
// costs over the same rows without it.
func TestFleetStatusBytesStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "fleet_status_bytes.json"), *updateFleetGoldens)
	defer golden.Close()
	selected := map[string]int{}
	for _, shape := range fleetBytesShapes() {
		selection := selectShape(t, shape)
		candidate := selection.SelectedCandidate()
		selected[shape.name] = candidate.Size
		t.Logf("%s: %s selected, status %d B (DenseV1 %d B, ColumnarV2 %d B)",
			shape.name, candidate.Encoding, candidate.Size, selection.DenseV1.Size, selection.ColumnarV2.Size)
		golden.CheckCount(shape.name+"/selected", int64(candidate.Size))
	}
	withReadySince, without := selected["readySince/4000"], selected["uniform/4000"]
	t.Logf("readySince on 4,000 rows at rest: %d B over the same rows without it, %.1f B per row, %.1f%% of the status",
		withReadySince-without, float64(withReadySince-without)/fleetRows, 100*float64(withReadySince-without)/float64(withReadySince))
}

// TestFleetCodecAllocationsStayWithinBudget holds the allocations of one
// encode and one decode at 4,000 rows, and the allocations and the bytes of
// one status write's selection at 1,000 and 4,000 rows, to the recorded
// golden, so a change in the codec's cost is made on purpose and carries its
// reason.
func TestFleetCodecAllocationsStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "fleet_allocations.json"), *updateFleetGoldens)
	defer golden.Close()
	for _, op := range fleetEncodeOps(t) {
		golden.Check("encode/"+op.name, mustRun(t, op.op))
	}
	for _, op := range fleetDecodeOps(t) {
		golden.Check("decode/"+op.name, mustRun(t, op.op))
	}
	for _, op := range fleetSelectOps() {
		golden.Check("select/"+op.name, mustRun(t, op.op))
		golden.CheckBytes("select/"+op.name+"/bytes", mustRun(t, op.op))
	}
}

func mustRun(t *testing.T, op func() error) func() {
	return func() {
		if err := op(); err != nil {
			t.Fatal(err)
		}
	}
}
