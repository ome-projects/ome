package irstatus

import (
	"math"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestReadySinceColumnsMatchTheWireExample records the wire form of the
// readySince columns: three Instances Ready eleven seconds apart become one
// base time, one index set and three offsets at the end of the document,
// and no entry.
func TestReadySinceColumnsMatchTheWireExample(t *testing.T) {
	t.Parallel()
	columns := mustEncode(t, readySinceRows(3), testLimit)
	if columns.Entries != nil {
		t.Fatalf("the readySince of a row at rest must not take an entry: %+v", columns.Entries)
	}
	got := string(mustJSON(t, columns))
	wantTail := `"admitted":"0-2","readySinceBase":"2026-03-04T05:06:07Z",` +
		`"readySinceIndexes":"0-2","readySinceOffsets":[0,11,22]}`
	if !strings.HasSuffix(got, wantTail) || strings.Count(got, "readySince") != 3 {
		t.Fatalf("wire JSON differs:\n want the tail %s\n got  %s", wantTail, got)
	}
	assertRowsEqual(t, readySinceRows(3), mustDecode(t, columns, testLimit))
}

// TestReadySinceColumnsCarryTheTimesExactlyOrNotAtAll states when the three
// columns are written: whole-second times whose span fits an int32 of
// seconds go to the columns, from any row order and however sparse; a
// sub-second time or a wider span keeps every time in the entries, so a
// decode returns the rows exactly either way.
func TestReadySinceColumnsCarryTheTimesExactlyOrNotAtAll(t *testing.T) {
	t.Parallel()
	timeAt := func(d time.Duration) *metav1.Time { at := fixtureTimePlus(d); return &at }
	tests := []struct {
		name        string
		rows        func() []v1beta1.OMENativeInstanceStatus
		wantColumns bool
		wantIndexes string
		wantOffsets []int32
	}{
		{name: "sparse times in a non-ascending order", rows: func() []v1beta1.OMENativeInstanceStatus {
			rows := uniformRows(12)
			rows[9].ReadySince = timeAt(5 * time.Second)
			rows[3].ReadySince = timeAt(time.Minute)
			rows[0], rows[11] = rows[11], rows[0]
			return rows
		}, wantColumns: true, wantIndexes: "3,9", wantOffsets: []int32{55, 0}},
		{name: "the widest span the columns carry", rows: func() []v1beta1.OMENativeInstanceStatus {
			rows := uniformRows(2)
			rows[0].ReadySince = timeAt(0)
			rows[1].ReadySince = timeAt(math.MaxInt32 * time.Second)
			return rows
		}, wantColumns: true, wantIndexes: "0-1", wantOffsets: []int32{0, math.MaxInt32}},
		{name: "a span one second too wide", rows: func() []v1beta1.OMENativeInstanceStatus {
			rows := uniformRows(2)
			rows[0].ReadySince = timeAt(0)
			rows[1].ReadySince = timeAt((math.MaxInt32 + 1) * time.Second)
			return rows
		}},
		{name: "a sub-second time", rows: func() []v1beta1.OMENativeInstanceStatus {
			rows := uniformRows(3)
			rows[0].ReadySince = timeAt(0)
			rows[1].ReadySince = timeAt(time.Second + time.Millisecond)
			return rows
		}},
		{name: "no time at all", rows: func() []v1beta1.OMENativeInstanceStatus { return uniformRows(3) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rows := test.rows()
			columns := mustEncode(t, rows, testLimit)
			inColumns := columns.ReadySinceBase != nil
			if inColumns != test.wantColumns {
				t.Fatalf("readySince columns present = %t, want %t: %+v", inColumns, test.wantColumns, columns)
			}
			entryTimes := 0
			for _, entry := range columns.Entries {
				if entry.ReadySince != nil {
					entryTimes++
				}
			}
			rowTimes := 0
			for _, row := range rows {
				if row.ReadySince != nil {
					rowTimes++
				}
			}
			if test.wantColumns {
				if *columns.ReadySinceIndexes != test.wantIndexes || !equalInt32s(columns.ReadySinceOffsets, test.wantOffsets) || entryTimes != 0 {
					t.Fatalf("readySince columns = (%s, %v) with %d entry times, want (%s, %v) and none",
						*columns.ReadySinceIndexes, columns.ReadySinceOffsets, entryTimes, test.wantIndexes, test.wantOffsets)
				}
			} else if entryTimes != rowTimes {
				t.Fatalf("%d of %d times are in the entries", entryTimes, rowTimes)
			}
			assertRowsEqual(t, rows, mustDecode(t, columns, testLimit))
		})
	}
}

func equalInt32s(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDecodeTakesTheReadySinceOfAnEntryWithoutTheColumns: a payload with no
// readySince columns gives each row the readySince of its entry, and a row
// without an entry has none.
func TestDecodeTakesTheReadySinceOfAnEntryWithoutTheColumns(t *testing.T) {
	t.Parallel()
	later := fixtureTimePlus(22 * time.Second)
	columns := &v1beta1.InstanceStatusColumns{
		Members: "0-2",
		Phases:  []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-2"}},
		Entries: []v1beta1.InstanceStatusEntry{{Index: 0, ReadySince: &fixtureTime}, {Index: 2, ReadySince: &later}},
	}
	for name, decode := range map[string]func(*v1beta1.InstanceStatusColumns, uint64) ([]v1beta1.OMENativeInstanceStatus, error){
		"copying": DecodeColumns, "taking": consumeColumns,
	} {
		rows, err := decode(columns, testLimit)
		if err != nil {
			t.Fatalf("%s decode: %v", name, err)
		}
		if rows[0].ReadySince == nil || !rows[0].ReadySince.Equal(&fixtureTime) || rows[1].ReadySince != nil ||
			rows[2].ReadySince == nil || !rows[2].ReadySince.Equal(&later) {
			t.Fatalf("%s decode read the entry times as %v, %v, %v", name, rows[0].ReadySince, rows[1].ReadySince, rows[2].ReadySince)
		}
	}
}

// TestReadySinceFormDecidesWhereTheTimesAreStored: the same rows encode to
// the three columns under ReadySinceInColumns and to one entry per timed
// row under ReadySinceInEntries, the form for a served schema without the
// columns; both decode to the rows exactly, and the writer's selection
// takes the entries form as a ColumnarV2 payload all the same.
func TestReadySinceFormDecidesWhereTheTimesAreStored(t *testing.T) {
	t.Parallel()
	rows := readySinceRows(50)
	inColumns, err := EncodeColumnsStoring(rows, testLimit, ReadySinceInColumns)
	if err != nil {
		t.Fatalf("EncodeColumnsStoring(columns): %v", err)
	}
	if inColumns.ReadySinceBase == nil || inColumns.Entries != nil {
		t.Fatalf("under ReadySinceInColumns the times take the columns and no entry: %s", mustJSON(t, inColumns))
	}
	if string(mustJSON(t, mustEncode(t, rows, testLimit))) != string(mustJSON(t, inColumns)) {
		t.Fatal("EncodeColumns stores the times in the columns")
	}
	inEntries, err := EncodeColumnsStoring(rows, testLimit, ReadySinceInEntries)
	if err != nil {
		t.Fatalf("EncodeColumnsStoring(entries): %v", err)
	}
	if inEntries.ReadySinceBase != nil || inEntries.ReadySinceIndexes != nil || inEntries.ReadySinceOffsets != nil {
		t.Fatalf("under ReadySinceInEntries no readySince column is written: %s", mustJSON(t, inEntries))
	}
	if len(inEntries.Entries) != len(rows) {
		t.Fatalf("under ReadySinceInEntries every timed row costs one entry: %d entries for %d rows", len(inEntries.Entries), len(rows))
	}
	for i, entry := range inEntries.Entries {
		if entry.Index != rows[i].Index || entry.ReadySince == nil || !entry.ReadySince.Equal(rows[i].ReadySince) {
			t.Fatalf("entry %d = %+v, want the readySince of row %d (%v)", i, entry, rows[i].Index, rows[i].ReadySince)
		}
	}
	assertRowsEqual(t, rows, mustDecode(t, inColumns, testLimit))
	assertRowsEqual(t, rows, mustDecode(t, inEntries, testLimit))

	selection, err := SelectCandidateStoring(logicalStatus(rows), testLimit, ReadySinceInEntries)
	if err != nil {
		t.Fatalf("SelectCandidateStoring: %v", err)
	}
	selected := selection.SelectedCandidate()
	if selected.Encoding != EncodingColumnarV2 || selected.Status.InstanceStatusColumns.ReadySinceBase != nil ||
		len(selected.Status.InstanceStatusColumns.Entries) != len(rows) {
		t.Fatalf("the selection under ReadySinceInEntries is the column form with one entry per timed row, got %s", mustJSON(t, selected.Status))
	}
	if ReadySinceInColumns.String() != "columns" || ReadySinceInEntries.String() != "entries" {
		t.Fatalf("the forms name themselves columns and entries, got %q and %q", ReadySinceInColumns, ReadySinceInEntries)
	}
}
