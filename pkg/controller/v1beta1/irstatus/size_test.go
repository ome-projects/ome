package irstatus

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
)

// The codec's numeric release gates. Each is asserted below.
const (
	// steadyStateGateBytes caps the complete selected status of the steady
	// and representative fixtures at 2,000 and 5,000 rows: 256 KiB, a quarter
	// of a conservative 1 MiB qualification envelope.
	steadyStateGateBytes = 256 * 1024
	// etcdDefaultRequestLimitBytes is etcd's default --max-request-bytes; the
	// mass-failure selected status must sit strictly below it.
	etcdDefaultRequestLimitBytes = 1572864
	// steadyRepresentationCeilingPercent and
	// representativeRepresentationCeilingPercent cap the columnar per-Instance
	// representation relative to the dense rows of the same fixture.
	steadyRepresentationCeilingPercent         = 10
	representativeRepresentationCeilingPercent = 25
	// completeStatusCeilingPercent caps the complete selected status relative
	// to the complete DenseV1 status of the same fixture.
	completeStatusCeilingPercent = 50
	// encodeCostCeiling and decodeCostCeiling bound the ColumnarV2 encode and
	// decode paths against the DenseV1 baselines, in time and bytes allocated;
	// growthCeiling bounds each measure from 2,000 to 5,000 rows.
	encodeCostCeiling = 3.0
	decodeCostCeiling = 2.0
	growthCeiling     = 3.25
)

// Recorded per-Instance representation sizes of the deterministic fixtures,
// in bytes. Re-record by running TestRecordedFixtureSizes with -v and copying
// the measured values it prints; the fixtures are reviewed golden inputs, so
// a change needs size review.
const (
	uniform2000DenseV1Bytes           = 322891
	uniform2000ColumnarV2Bytes        = 363
	uniform5000DenseV1Bytes           = 808891
	uniform5000ColumnarV2Bytes        = 363
	representative2000DenseV1Bytes    = 337077
	representative2000ColumnarV2Bytes = 25058
	representative5000DenseV1Bytes    = 836272
	representative5000ColumnarV2Bytes = 56809
	massFailureDenseV1Bytes           = 1608780
	massFailureColumnarV2Bytes        = 1054943
)

// representationSizes holds the serialized bytes of only the per-Instance
// representation in each candidate, which is the part of the status this
// codec owns, alongside the complete status sizes.
type representationSizes struct {
	denseRows, columnar         int
	denseStatus, columnarStatus int
	selected                    Encoding
}

func measure(t testing.TB, rows []v1beta1.OMENativeInstanceStatus) representationSizes {
	t.Helper()
	selection, err := SelectCandidate(logicalStatus(rows), testLimit)
	if err != nil {
		t.Fatalf("SelectCandidate() error = %v", err)
	}
	if selection.ColumnarV2 == nil {
		t.Fatalf("fixture is not eligible for ColumnarV2: %q", selection.IneligibleReason)
	}
	return representationSizes{
		denseRows:      len(mustJSON(t, selection.DenseV1.Status.InstanceStatuses)),
		columnar:       len(mustJSON(t, selection.ColumnarV2.Status.InstanceStatusColumns)),
		denseStatus:    selection.DenseV1.Size,
		columnarStatus: selection.ColumnarV2.Size,
		selected:       selection.Selected,
	}
}

func (s representationSizes) log(t testing.TB, name string) {
	t.Helper()
	t.Logf("%s: instance representation DenseV1 %d B, ColumnarV2 %d B (%.2f%% of dense); complete status DenseV1 %d B, ColumnarV2 %d B; selected %s",
		name, s.denseRows, s.columnar, 100*float64(s.columnar)/float64(s.denseRows), s.denseStatus, s.columnarStatus, s.selected)
}

// sizeFixture is one deterministic fixture with its recorded representation
// sizes and, when the steady-state gates apply to it, the ceiling on its
// columnar representation as a percentage of the dense rows.
type sizeFixture struct {
	name            string
	rows            []v1beta1.OMENativeInstanceStatus
	dense, columnar int
	ceilingPercent  int
}

// ladderFixtures are the fixtures whose sizes are pinned.
func ladderFixtures() []sizeFixture {
	return []sizeFixture{
		{name: "uniform/2000", rows: uniformRows(2000), dense: uniform2000DenseV1Bytes, columnar: uniform2000ColumnarV2Bytes, ceilingPercent: steadyRepresentationCeilingPercent},
		{name: "uniform/5000", rows: uniformRows(5000), dense: uniform5000DenseV1Bytes, columnar: uniform5000ColumnarV2Bytes, ceilingPercent: steadyRepresentationCeilingPercent},
		{name: "representative/2000", rows: representativeRows(2000), dense: representative2000DenseV1Bytes, columnar: representative2000ColumnarV2Bytes, ceilingPercent: representativeRepresentationCeilingPercent},
		{name: "representative/5000", rows: representativeRows(5000), dense: representative5000DenseV1Bytes, columnar: representative5000ColumnarV2Bytes, ceilingPercent: representativeRepresentationCeilingPercent},
		{name: "massFailure/4000", rows: massFailureRows(), dense: massFailureDenseV1Bytes, columnar: massFailureColumnarV2Bytes},
	}
}

// adversarialRows defeats grouping: every revision, incarnation, and count is
// unique per row, admission and the active ordinal alternate, and the phase
// cycles through every value, so each group carries one or two indices and
// the dense rows win.
func adversarialRows(n int) []v1beta1.OMENativeInstanceStatus {
	phases := []v1beta1.OMENativeInstancePhase{
		v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceUpdating,
		v1beta1.OMENativeInstanceRestarting, v1beta1.OMENativeInstanceMigrating, v1beta1.OMENativeInstanceFailed, v1beta1.OMENativeInstanceDeleting,
	}
	rows := uniformRows(n)
	for i := range rows {
		rows[i].Phase = phases[i%len(phases)]
		rows[i].RunningRevision = fmt.Sprintf("example-engine-%08d", i)
		rows[i].TargetRevision = fmt.Sprintf("example-engine-%08d", n+i)
		rows[i].Incarnation = int64(i + 1)
		rows[i].PodCount, rows[i].ServingPodCount, rows[i].AvailablePodCount = int32(i+1), int32(i+2), int32(i+3)
		rows[i].Admitted = i%2 == 0
		rows[i].ActiveOrdinal = int32(i % 2)
	}
	return rows
}

func TestRecordedFixtureSizes(t *testing.T) {
	t.Parallel()
	for _, fixture := range ladderFixtures() {
		sizes := measure(t, fixture.rows)
		sizes.log(t, fixture.name)
		if sizes.denseRows != fixture.dense || sizes.columnar != fixture.columnar {
			t.Errorf("%s: measured (%d, %d) B, recorded (%d, %d) B", fixture.name, sizes.denseRows, sizes.columnar, fixture.dense, fixture.columnar)
		}
	}
}

// The steady and representative fixtures at 2,000 and 5,000 rows must select
// columns, stay under the 256 KiB complete-status gate and half of the
// complete DenseV1 status, and keep the per-Instance representation under the
// steady (10%) or representative (25%) ceiling.
func TestSteadyStateSizeGates(t *testing.T) {
	t.Parallel()
	gated := 0
	for _, fixture := range ladderFixtures() {
		if fixture.ceilingPercent == 0 {
			continue
		}
		gated++
		sizes := measure(t, fixture.rows)
		ceiling := fixture.ceilingPercent
		if sizes.selected != EncodingColumnarV2 {
			t.Errorf("%s: selected %s, want ColumnarV2", fixture.name, sizes.selected)
		}
		if sizes.columnarStatus > steadyStateGateBytes {
			t.Errorf("%s: complete selected status %d B exceeds the %d B steady-state gate", fixture.name, sizes.columnarStatus, steadyStateGateBytes)
		}
		if 100*sizes.columnarStatus > completeStatusCeilingPercent*sizes.denseStatus {
			t.Errorf("%s: complete selected status %d B is more than %d%% of the DenseV1 status %d B", fixture.name, sizes.columnarStatus, completeStatusCeilingPercent, sizes.denseStatus)
		}
		if 100*sizes.columnar > ceiling*sizes.denseRows {
			t.Errorf("%s: columnar representation %d B is more than %d%% of the dense rows %d B", fixture.name, sizes.columnar, ceiling, sizes.denseRows)
		}
	}
	if gated != 4 {
		t.Fatalf("steady-state gates covered %d fixtures, want the steady and representative fixtures at 2,000 and 5,000 rows", gated)
	}
}

// rowFamilyBytes attributes the serialized dense rows to the record families
// measured separately: lastFailure records and operation records,
// each counted with its key, and the healthy-row remainder.
type rowFamilyBytes struct{ lastFailure, operation, remainder int }

func measureRowFamilies(t testing.TB, rows []v1beta1.OMENativeInstanceStatus) rowFamilyBytes {
	t.Helper()
	var families rowFamilyBytes
	for i := range rows {
		if rows[i].LastFailure != nil {
			families.lastFailure += len(`,"lastFailure":`) + len(mustJSON(t, rows[i].LastFailure))
		}
		if rows[i].Operation != nil {
			families.operation += len(`,"operation":`) + len(mustJSON(t, rows[i].Operation))
		}
	}
	families.remainder = len(mustJSON(t, rows)) - families.lastFailure - families.operation
	return families
}

// The mass-failure fixture must keep its shape, its DenseV1 status must exceed
// etcd's default request limit, and its selected status must sit strictly
// below that limit; the headroom is reported.
func TestMassFailureRequestLimitGate(t *testing.T) {
	t.Parallel()
	rows := massFailureRows()
	phases := map[v1beta1.OMENativeInstancePhase]int{}
	revisions := map[string]int{}
	operations := 0
	deadlineRecord := v1beta1.InstanceTermination{Reason: irstatustest.MassFailureDeadlineReason, Message: irstatustest.MassFailureDeadlineMessage, Time: fixtureTime}
	for i, row := range rows {
		phases[row.Phase]++
		revisions[row.RunningRevision]++
		if row.Operation != nil {
			operations++
			if row.Operation.Type != v1beta1.InstanceOperationRestart {
				t.Fatalf("row %d carries a %s operation", row.Index, row.Operation.Type)
			}
		}
		if row.LastFailure == nil || row.PodCount > 1 {
			t.Fatalf("row %d is not a single-pod row with a preserved failure", row.Index)
		}
		// A recovered row keeps its Pod-level record; a parked row carries the
		// deadline record. Both are the controller's own formats.
		want := deadlineRecord
		if row.Phase == v1beta1.OMENativeInstanceReady {
			want = v1beta1.InstanceTermination{PodName: irstatustest.MassFailurePodName(i), Reason: irstatustest.MassFailurePodReason, Time: fixtureTime}
		}
		if !equality.Semantic.DeepEqual(*row.LastFailure, want) {
			t.Fatalf("row %d failure record %s is not the controller's record for phase %s", row.Index, mustJSON(t, row.LastFailure), row.Phase)
		}
	}
	if len(rows) != irstatustest.MassFailureRowCount || phases[v1beta1.OMENativeInstanceReady] != irstatustest.MassFailureReadyCount ||
		phases[v1beta1.OMENativeInstanceRestarting] != irstatustest.MassFailureRestartingCount || phases[v1beta1.OMENativeInstanceFailed] != irstatustest.MassFailureFailedCount {
		t.Fatalf("fixture shape = %d rows, phases %v", len(rows), phases)
	}
	if operations != irstatustest.MassFailureOperationCount || len(revisions) != 2 ||
		revisions[irstatustest.MassFailureRevision] != irstatustest.MassFailureRowCount-1 || revisions[irstatustest.MassFailureOtherRevision] != 1 {
		t.Fatalf("fixture shape = %d operations, revisions %v", operations, revisions)
	}

	sizes := measure(t, rows)
	sizes.log(t, "mass failure 4000 rows")
	families := measureRowFamilies(t, rows)
	t.Logf("mass failure attribution: lastFailure %d B (%.1f B/record), operation %d B (%.1f B/record), healthy rows %d B (%.1f B/row); per-row dense %.1f B, per-row columnar %.1f B",
		families.lastFailure, float64(families.lastFailure)/float64(len(rows)), families.operation, float64(families.operation)/float64(operations),
		families.remainder, float64(families.remainder)/float64(len(rows)), float64(sizes.denseRows)/float64(len(rows)), float64(sizes.columnar)/float64(len(rows)))
	if sizes.denseStatus <= etcdDefaultRequestLimitBytes {
		t.Errorf("mass failure: DenseV1 status %d B does not exceed etcd's default request limit %d B; the fixture is too small to need ColumnarV2", sizes.denseStatus, etcdDefaultRequestLimitBytes)
	}
	if sizes.selected != EncodingColumnarV2 {
		t.Fatalf("mass failure: selected %s, want ColumnarV2 for complete sizes %d vs %d", sizes.selected, sizes.denseStatus, sizes.columnarStatus)
	}
	headroom := etcdDefaultRequestLimitBytes - sizes.columnarStatus
	t.Logf("mass failure: selected ColumnarV2 status %d B against the %d B request limit; headroom %d B (%.1f%%); DenseV1 status %d B is %d B over the limit",
		sizes.columnarStatus, etcdDefaultRequestLimitBytes, headroom, 100*float64(headroom)/float64(etcdDefaultRequestLimitBytes), sizes.denseStatus, sizes.denseStatus-etcdDefaultRequestLimitBytes)
	if sizes.columnarStatus >= etcdDefaultRequestLimitBytes {
		t.Errorf("mass failure: selected status %d B is not strictly below the %d B request limit", sizes.columnarStatus, etcdDefaultRequestLimitBytes)
	}
}

// Whatever the fixture, the selected candidate is never larger than DenseV1,
// and columns are selected exactly when they are strictly smaller. The
// adversarial and single-row fixtures make dense win, so the rule is
// exercised in both directions.
func TestSelectedRepresentationNeverLargerThanDenseV1(t *testing.T) {
	t.Parallel()
	catalogue := append(ladderFixtures(),
		sizeFixture{name: "uniform/1", rows: uniformRows(1)},
		sizeFixture{name: "uniform/100", rows: uniformRows(100)},
		sizeFixture{name: "uniform/1000", rows: uniformRows(1000)},
		sizeFixture{name: "wireExample/500", rows: wireExampleRows()},
		sizeFixture{name: "fullFeature/8", rows: fullFeatureRows()},
		sizeFixture{name: "adversarial/2000", rows: adversarialRows(2000)},
	)
	denseWins := 0
	for _, fixture := range catalogue {
		selection, err := SelectCandidate(logicalStatus(fixture.rows), testLimit)
		if err != nil {
			t.Fatalf("%s: SelectCandidate() error = %v", fixture.name, err)
		}
		if selection.ColumnarV2 == nil {
			t.Fatalf("%s: not eligible for ColumnarV2: %q", fixture.name, selection.IneligibleReason)
		}
		selected := selection.SelectedCandidate()
		t.Logf("%s: DenseV1 %d B, ColumnarV2 %d B, selected %s (%d B)", fixture.name, selection.DenseV1.Size, selection.ColumnarV2.Size, selection.Selected, selected.Size)
		if selected.Size > selection.DenseV1.Size {
			t.Errorf("%s: selected %s at %d B is larger than DenseV1 at %d B", fixture.name, selection.Selected, selected.Size, selection.DenseV1.Size)
		}
		if want := selectEncoding(selection.DenseV1.Size, selection.ColumnarV2.Size); selection.Selected != want {
			t.Errorf("%s: selected %s, want %s for complete sizes %d vs %d", fixture.name, selection.Selected, want, selection.DenseV1.Size, selection.ColumnarV2.Size)
		}
		if selection.Selected == EncodingDenseV1 {
			denseWins++
		}
	}
	if denseWins == 0 {
		t.Fatal("no fixture made DenseV1 win; the strict-smaller rule was exercised in one direction only")
	}
}

// From 2,000 to 5,000 rows the complete selected status grows by no more than
// the growth ceiling; the row count itself grows 2.5x.
func TestSizeGrowthFromTwoToFiveThousandRows(t *testing.T) {
	t.Parallel()
	for _, family := range []struct {
		name string
		rows func(int) []v1beta1.OMENativeInstanceStatus
	}{
		{name: "uniform", rows: uniformRows},
		{name: "representative", rows: representativeRows},
	} {
		small, large := measure(t, family.rows(2000)), measure(t, family.rows(5000))
		ratio := float64(large.columnarStatus) / float64(small.columnarStatus)
		t.Logf("%s: complete selected status %d B at 2000 rows, %d B at 5000 rows, growth %.2fx (DenseV1 %.2fx)",
			family.name, small.columnarStatus, large.columnarStatus, ratio, float64(large.denseStatus)/float64(small.denseStatus))
		if ratio > growthCeiling {
			t.Errorf("%s: complete selected status grew %.2fx from 2000 to 5000 rows, ceiling %.2fx", family.name, ratio, growthCeiling)
		}
	}
}

// costSample is one reading of a codec path.
type costSample struct {
	nsPerOp    float64
	bytesPerOp float64
}

// codecCostGatesEnv selects how TestCodecCostAgainstDenseV1Baseline treats
// the measured ratios. Set to codecCostGatesStrict (the qualification lane),
// a ratio above its ceiling fails the test; otherwise the test measures once,
// reports the ratios, and passes, so machine speed never gates the unit lane.
const (
	codecCostGatesEnv    = "OME_CODEC_COST_GATES"
	codecCostGatesStrict = "strict"
	// strictCostSamples is the number of interleaved testing.Benchmark
	// samples per path whose median strict mode gates; surveyIterations is
	// the fixed iteration count of the single non-strict sample.
	strictCostSamples = 3
	surveyIterations  = 10
)

// The four measured paths, each as one operation. The DenseV1 encode baseline
// normalizes a write copy and serializes one complete dense status; the
// ColumnarV2 encode measurement builds both candidates, selects one, and
// serializes the selection. The DenseV1 decode baseline deserializes a
// complete dense status; the ColumnarV2 decode measurement deserializes a
// complete columnar status, validates it, and expands its rows.
func encodeDenseV1Op(logical *v1beta1.InferenceReplicaStatus) func() error {
	return func() error {
		write := logical.DeepCopy()
		ClearPodDerivedObservations(write.InstanceStatuses)
		_, err := json.Marshal(write)
		return err
	}
}

func encodeSelectedOp(logical *v1beta1.InferenceReplicaStatus) func() error {
	return func() error {
		selection, err := SelectCandidate(logical, testLimit)
		if err != nil {
			return err
		}
		_, err = json.Marshal(selection.SelectedCandidate().Status)
		return err
	}
}

func decodeStatusOp(data []byte) func() error {
	return func() error {
		var status v1beta1.InferenceReplicaStatus
		if err := json.Unmarshal(data, &status); err != nil {
			return err
		}
		_, _, err := DecodeStatus(&status, testLimit)
		return err
	}
}

// benchmarkOp drives op under the benchmark harness.
func benchmarkOp(b *testing.B, op func() error) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := op(); err != nil {
			b.Fatal(err)
		}
	}
}

// costSampler takes one reading of a codec path.
type costSampler func(t *testing.T, op func() error) costSample

// benchmarkSample is one auto-scaled testing.Benchmark reading.
func benchmarkSample(t *testing.T, op func() error) costSample {
	t.Helper()
	result := testing.Benchmark(func(b *testing.B) { benchmarkOp(b, op) })
	return costSample{nsPerOp: float64(result.NsPerOp()), bytesPerOp: float64(result.AllocedBytesPerOp())}
}

// surveySample is one fixed-count reading after a warm-up call, measured the
// way the benchmark harness measures: wall time and total bytes allocated per
// operation. The fixed count keeps the non-strict run short.
func surveySample(t *testing.T, op func() error) costSample {
	t.Helper()
	if err := op(); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	for i := 0; i < surveyIterations; i++ {
		if err := op(); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	return costSample{
		nsPerOp:    float64(elapsed.Nanoseconds()) / surveyIterations,
		bytesPerOp: float64(after.TotalAlloc-before.TotalAlloc) / surveyIterations,
	}
}

// codecCosts measures the four paths for one fixture with interleaved
// samples and returns per-path medians, so a transient stall on one sample
// does not decide a gate.
func codecCosts(t *testing.T, rows []v1beta1.OMENativeInstanceStatus, sample costSampler, samples int) map[string]costSample {
	t.Helper()
	logical := logicalStatus(rows)
	selection, err := SelectCandidate(logical, testLimit)
	if err != nil || selection.ColumnarV2 == nil {
		t.Fatalf("SelectCandidate() = %+v, %v", selection, err)
	}
	dense, columnar := mustJSON(t, selection.DenseV1.Status), mustJSON(t, selection.ColumnarV2.Status)
	paths := []struct {
		name string
		op   func() error
	}{
		{name: "encode/denseV1", op: encodeDenseV1Op(logical)},
		{name: "encode/selected", op: encodeSelectedOp(logical)},
		{name: "decode/denseV1", op: decodeStatusOp(dense)},
		{name: "decode/columnarV2", op: decodeStatusOp(columnar)},
	}
	readings := map[string][]costSample{}
	for s := 0; s < samples; s++ {
		for _, path := range paths {
			readings[path.name] = append(readings[path.name], sample(t, path.op))
		}
	}
	medians := map[string]costSample{}
	for name, values := range readings {
		medians[name] = costSample{nsPerOp: median(values, func(s costSample) float64 { return s.nsPerOp }), bytesPerOp: median(values, func(s costSample) float64 { return s.bytesPerOp })}
	}
	return medians
}

func median(samples []costSample, pick func(costSample) float64) float64 {
	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		values = append(values, pick(sample))
	}
	sort.Float64s(values)
	return values[len(values)/2]
}

// The ColumnarV2 paths are bounded against the DenseV1 baselines at 2,000 and
// 5,000 rows, and each ColumnarV2 measure grows by no more than the growth
// ceiling between them. The ratios depend on the machine, so they gate only
// under OME_CODEC_COST_GATES=strict (the qualification lane), where medians
// of interleaved testing.Benchmark samples keep a transient stall out of the
// verdict; otherwise one short fixed-count sample per path reports the ratios
// and the test passes. The encoded byte sizes are gated deterministically by
// the tests above; -short skips the measurement.
func TestCodecCostAgainstDenseV1Baseline(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark-backed gate; skipped under -short")
	}
	strict := os.Getenv(codecCostGatesEnv) == codecCostGatesStrict
	sample, samples := costSampler(surveySample), 1
	mode := "advisory: ratios are reported, not gated"
	if strict {
		sample, samples = benchmarkSample, strictCostSamples
		mode = "strict: a ratio above its ceiling fails"
	}
	t.Logf("%s=%q; %s", codecCostGatesEnv, os.Getenv(codecCostGatesEnv), mode)
	for _, family := range []struct {
		name string
		rows func(int) []v1beta1.OMENativeInstanceStatus
	}{
		{name: "uniform", rows: uniformRows},
		{name: "representative", rows: representativeRows},
	} {
		costs := map[int]map[string]costSample{}
		for _, n := range []int{2000, 5000} {
			costs[n] = codecCosts(t, family.rows(n), sample, samples)
			for _, path := range []string{"encode/denseV1", "encode/selected", "decode/denseV1", "decode/columnarV2"} {
				t.Logf("%s/%d %s: median %.0f ns/op, %.0f B/op", family.name, n, path, costs[n][path].nsPerOp, costs[n][path].bytesPerOp)
			}
			assertCostRatio(t, strict, fmt.Sprintf("%s/%d encode", family.name, n), costs[n]["encode/selected"], costs[n]["encode/denseV1"], encodeCostCeiling)
			assertCostRatio(t, strict, fmt.Sprintf("%s/%d decode", family.name, n), costs[n]["decode/columnarV2"], costs[n]["decode/denseV1"], decodeCostCeiling)
		}
		for _, path := range []string{"encode/selected", "decode/columnarV2"} {
			assertCostRatio(t, strict, fmt.Sprintf("%s %s growth 2000->5000", family.name, path), costs[5000][path], costs[2000][path], growthCeiling)
		}
	}
}

// assertCostRatio reports the measured-to-baseline ratios of one comparison
// and, in strict mode, fails when either exceeds the ceiling.
func assertCostRatio(t *testing.T, strict bool, name string, measured, baseline costSample, ceiling float64) {
	t.Helper()
	timeRatio := measured.nsPerOp / baseline.nsPerOp
	bytesRatio := measured.bytesPerOp / baseline.bytesPerOp
	t.Logf("%s: time %.2fx, bytes allocated %.2fx (ceiling %.2fx)", name, timeRatio, bytesRatio, ceiling)
	exceeded := func(measure string, ratio float64) {
		if strict {
			t.Errorf("%s: %s ratio %.2fx exceeds the %.2fx ceiling", name, measure, ratio, ceiling)
			return
		}
		t.Logf("%s: %s ratio %.2fx exceeds the %.2fx ceiling; advisory unless %s=%s", name, measure, ratio, ceiling, codecCostGatesEnv, codecCostGatesStrict)
	}
	if timeRatio > ceiling {
		exceeded("time", timeRatio)
	}
	if bytesRatio > ceiling {
		exceeded("bytes-allocated", bytesRatio)
	}
}

func benchmarkCases() []struct {
	name string
	rows []v1beta1.OMENativeInstanceStatus
} {
	return []struct {
		name string
		rows []v1beta1.OMENativeInstanceStatus
	}{
		{name: "uniform/2000", rows: uniformRows(2000)},
		{name: "uniform/5000", rows: uniformRows(5000)},
		{name: "representative/2000", rows: representativeRows(2000)},
		{name: "representative/5000", rows: representativeRows(5000)},
		{name: "massFailure/4000", rows: massFailureRows()},
	}
}

func BenchmarkEncodeColumns(b *testing.B) {
	for _, bc := range benchmarkCases() {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := EncodeColumns(bc.rows, testLimit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecodeColumns(b *testing.B) {
	for _, bc := range benchmarkCases() {
		columns := mustEncode(b, bc.rows, testLimit)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := DecodeColumns(columns, testLimit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEncodeStatusDenseV1Baseline(b *testing.B) {
	for _, bc := range benchmarkCases() {
		op := encodeDenseV1Op(logicalStatus(bc.rows))
		b.Run(bc.name, func(b *testing.B) { benchmarkOp(b, op) })
	}
}

func BenchmarkEncodeStatusSelectCandidate(b *testing.B) {
	for _, bc := range benchmarkCases() {
		op := encodeSelectedOp(logicalStatus(bc.rows))
		b.Run(bc.name, func(b *testing.B) { benchmarkOp(b, op) })
	}
}

func BenchmarkDecodeStatusDenseV1Baseline(b *testing.B) {
	for _, bc := range benchmarkCases() {
		op := decodeStatusOp(mustJSON(b, logicalStatus(bc.rows)))
		b.Run(bc.name, func(b *testing.B) { benchmarkOp(b, op) })
	}
}

func BenchmarkDecodeStatusColumnarV2(b *testing.B) {
	for _, bc := range benchmarkCases() {
		selection, err := SelectCandidate(logicalStatus(bc.rows), testLimit)
		if err != nil || selection.ColumnarV2 == nil {
			b.Fatalf("SelectCandidate() = %+v, %v", selection, err)
		}
		op := decodeStatusOp(mustJSON(b, selection.ColumnarV2.Status))
		b.Run(bc.name, func(b *testing.B) { benchmarkOp(b, op) })
	}
}
