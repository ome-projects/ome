package irstatus

import (
	"encoding/json"
	"math"
	"runtime"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// decodeBaseColumns is a valid ten-member payload with two phases, two
// revisions, two incarnations, counts, admission, an ordinal, and entries.
func decodeBaseColumns(t *testing.T) *v1beta1.InstanceStatusColumns {
	t.Helper()
	rows := uniformRows(10)
	rows[3].Phase = v1beta1.OMENativeInstanceUpdating
	rows[3].TargetRevision = fixtureOtherRevision
	rows[7].RunningRevision = fixtureOtherRevision
	rows[7].Incarnation = 2
	rows[7].ActiveOrdinal = 1
	rows[8].Admitted = false
	rows[2].ReadySince = &fixtureTime
	rows[5].LastFailure = &v1beta1.InstanceTermination{PodName: "example-engine-5-0", Reason: "Error", Time: fixtureTime}
	return mustEncode(t, rows, testLimit)
}

func TestDecodeColumnsRejectsInvalidPayloads(t *testing.T) {
	t.Parallel()
	phase := func(value v1beta1.OMENativeInstancePhase, indexes string) v1beta1.InstanceStatusPhaseGroup {
		return v1beta1.InstanceStatusPhaseGroup{Value: value, Indexes: indexes}
	}
	revision := func(value, indexes string) v1beta1.InstanceStatusRevisionGroup {
		return v1beta1.InstanceStatusRevisionGroup{Value: value, Indexes: indexes}
	}
	incarnation := func(value int64, indexes string) v1beta1.InstanceStatusIncarnationGroup {
		return v1beta1.InstanceStatusIncarnationGroup{Value: value, Indexes: indexes}
	}
	count := func(value int32, indexes string) v1beta1.InstanceStatusCountGroup {
		return v1beta1.InstanceStatusCountGroup{Value: value, Indexes: indexes}
	}
	str := func(s string) *string { return &s }

	tests := []struct {
		name       string
		mutate     func(*v1beta1.InstanceStatusColumns)
		limit      uint64
		wantReason ErrorReason
	}{
		// Members and the configured bound.
		{name: "zero limit", mutate: func(*v1beta1.InstanceStatusColumns) {}, limit: 0, wantReason: ErrorReasonCardinalityLimit},
		{name: "members above limit", mutate: func(*v1beta1.InstanceStatusColumns) {}, limit: 9, wantReason: ErrorReasonCardinalityLimit},
		{name: "empty members", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "" }, wantReason: ErrorReasonRangeSyntax},
		{name: "members whitespace", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "0-9 " }, wantReason: ErrorReasonRangeSyntax},
		{name: "members leading zero", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "00-9" }, wantReason: ErrorReasonRangeSyntax},
		{name: "members signed", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "+0-9" }, wantReason: ErrorReasonRangeSyntax},
		{name: "members trailing comma", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "0-9," }, wantReason: ErrorReasonRangeSyntax},
		{name: "members descending", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "9-0" }, wantReason: ErrorReasonRangeOrder},
		{name: "members degenerate range", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "0-8,9-9" }, wantReason: ErrorReasonRangeOrder},
		{name: "members overlap", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "0-5,5-9" }, wantReason: ErrorReasonRangeOrder},
		{name: "members adjacent", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "0-4,5-9" }, wantReason: ErrorReasonCanonicalOrder},
		{name: "members int32 boundary", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "0-2147483648" }, wantReason: ErrorReasonRangeOverflow},
		{name: "members arithmetic overflow", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = strings.Repeat("9", 40) }, wantReason: ErrorReasonRangeOverflow},

		// Phases: required, exact, disjoint, canonical.
		{name: "missing phases", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Phases = nil }, wantReason: ErrorReasonCoverage},
		{name: "empty phases", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Phases = []v1beta1.InstanceStatusPhaseGroup{} }, wantReason: ErrorReasonCoverage},
		{name: "phase missing member", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceReady, "0-8")}
		}, wantReason: ErrorReasonCoverage},
		{name: "phase extra member", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceReady, "0-8,10")}
		}, wantReason: ErrorReasonCoverage},
		{name: "phase duplicate member", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceFailed, "3"), phase(v1beta1.OMENativeInstanceReady, "0-9")}
		}, wantReason: ErrorReasonCoverage},
		{name: "phase overlap within coverage", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceFailed, "0-4"), phase(v1beta1.OMENativeInstanceReady, "0-4")}
			c.Members = "0-4"
			c.RunningRevisions, c.TargetRevisions, c.Incarnations = nil, nil, nil
			c.PodCounts, c.ServingPodCounts, c.AvailablePodCounts = nil, nil, nil
			c.Admitted, c.ActiveOrdinalOne, c.Entries = nil, nil, nil
		}, wantReason: ErrorReasonCoverage},
		{name: "phase duplicate value", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceReady, "0-4"), phase(v1beta1.OMENativeInstanceReady, "5-9")}
		}, wantReason: ErrorReasonCanonicalOrder},
		{name: "phase unsorted", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceUpdating, "3"), phase(v1beta1.OMENativeInstanceReady, "0-2,4-9")}
		}, wantReason: ErrorReasonCanonicalOrder},
		{name: "phase unsupported", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase("Unknown", "0-9")}
		}, wantReason: ErrorReasonValueDomain},
		{name: "phase empty indexes", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceReady, "")}
		}, wantReason: ErrorReasonRangeSyntax},
		{name: "phase indexes noncanonical", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceReady, "0-2,3-9")}
		}, wantReason: ErrorReasonCanonicalOrder},
		{name: "phase cardinality above members", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceReady, "0-10")}
			c.Members = "0-9"
		}, wantReason: ErrorReasonCardinalityLimit},
		{name: "more phase groups than members", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Members = "0"
			c.Phases = []v1beta1.InstanceStatusPhaseGroup{phase(v1beta1.OMENativeInstanceFailed, "0"), phase(v1beta1.OMENativeInstanceReady, "0")}
		}, wantReason: ErrorReasonCardinalityLimit},

		// Revisions.
		{name: "empty revision value", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RunningRevisions = []v1beta1.InstanceStatusRevisionGroup{revision("", "0-9")}
		}, wantReason: ErrorReasonValueDomain},
		{name: "explicit empty revisions", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RunningRevisions = []v1beta1.InstanceStatusRevisionGroup{}
		}, wantReason: ErrorReasonValueDomain},
		{name: "revision overlap", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RunningRevisions = []v1beta1.InstanceStatusRevisionGroup{revision("rev-a", "0-6"), revision("rev-b", "6-9")}
		}, wantReason: ErrorReasonCoverage},
		{name: "revision outside members", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.TargetRevisions = []v1beta1.InstanceStatusRevisionGroup{revision("rev-a", "10")}
		}, wantReason: ErrorReasonCoverage},
		{name: "revision unsorted", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RunningRevisions = []v1beta1.InstanceStatusRevisionGroup{revision("rev-b", "0-4"), revision("rev-a", "5-9")}
		}, wantReason: ErrorReasonCanonicalOrder},
		{name: "revision duplicate value", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RunningRevisions = []v1beta1.InstanceStatusRevisionGroup{revision("rev-a", "0-4"), revision("rev-a", "5-9")}
		}, wantReason: ErrorReasonCanonicalOrder},

		// Incarnations and counts.
		{name: "zero incarnation group", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Incarnations = []v1beta1.InstanceStatusIncarnationGroup{incarnation(0, "0-9")}
		}, wantReason: ErrorReasonValueDomain},
		{name: "incarnations lexical not numeric", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Incarnations = []v1beta1.InstanceStatusIncarnationGroup{incarnation(10, "0-4"), incarnation(9, "5-9")}
		}, wantReason: ErrorReasonCanonicalOrder},
		{name: "incarnation overlap", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Incarnations = []v1beta1.InstanceStatusIncarnationGroup{incarnation(1, "0-9"), incarnation(2, "7")}
		}, wantReason: ErrorReasonCoverage},
		{name: "zero count group", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.PodCounts = []v1beta1.InstanceStatusCountGroup{count(0, "0-9")}
		}, wantReason: ErrorReasonValueDomain},
		{name: "negative count group", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.ServingPodCounts = []v1beta1.InstanceStatusCountGroup{count(-1, "0-9")}
		}, wantReason: ErrorReasonValueDomain},
		{name: "counts unsorted", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.AvailablePodCounts = []v1beta1.InstanceStatusCountGroup{count(2, "0-4"), count(1, "5-9")}
		}, wantReason: ErrorReasonCanonicalOrder},

		// Admission and ordinal sets.
		{name: "explicit empty admitted", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Admitted = str("") }, wantReason: ErrorReasonRangeSyntax},
		{name: "admitted outside members", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Admitted = str("0-7,12") }, wantReason: ErrorReasonCoverage},
		{name: "admitted above members", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Admitted = str("0-10") }, wantReason: ErrorReasonCardinalityLimit},
		{name: "ordinal outside members", mutate: func(c *v1beta1.InstanceStatusColumns) { c.ActiveOrdinalOne = str("10") }, wantReason: ErrorReasonCoverage},
		{name: "ordinal noncanonical", mutate: func(c *v1beta1.InstanceStatusColumns) { c.ActiveOrdinalOne = str("7,8") }, wantReason: ErrorReasonCanonicalOrder},

		// Entries.
		{name: "explicit empty entries", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Entries = []v1beta1.InstanceStatusEntry{} }, wantReason: ErrorReasonValueDomain},
		{name: "entry outside members", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Entries[1].Index = 11 }, wantReason: ErrorReasonCoverage},
		{name: "duplicate entry", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Entries[1].Index = 2 }, wantReason: ErrorReasonCoverage},
		{name: "entries unsorted", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Entries[0], c.Entries[1] = c.Entries[1], c.Entries[0] }, wantReason: ErrorReasonCanonicalOrder},
		{name: "empty entry", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Entries[0] = v1beta1.InstanceStatusEntry{Index: 2} }, wantReason: ErrorReasonCoverage},
		{name: "entry with only empty conditions", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Entries[0] = v1beta1.InstanceStatusEntry{Index: 2, Conditions: []metav1.Condition{}}
		}, wantReason: ErrorReasonCoverage},
		{name: "more entries than members", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Entries = make([]v1beta1.InstanceStatusEntry, 11)
			for i := range c.Entries {
				c.Entries[i] = v1beta1.InstanceStatusEntry{Index: int32(i), ReadySince: &fixtureTime}
			}
		}, wantReason: ErrorReasonCardinalityLimit},

		// Row order.
		{name: "explicit empty row order", mutate: func(c *v1beta1.InstanceStatusColumns) { c.RowOrder = []int32{} }, wantReason: ErrorReasonValueDomain},
		{name: "redundant ascending row order", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RowOrder = []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		}, wantReason: ErrorReasonCanonicalOrder},
		{name: "short row order", mutate: func(c *v1beta1.InstanceStatusColumns) { c.RowOrder = []int32{9, 0} }, wantReason: ErrorReasonCoverage},
		{name: "long row order", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RowOrder = []int32{9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 8}
		}, wantReason: ErrorReasonCoverage},
		{name: "row order duplicate", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RowOrder = []int32{9, 0, 1, 2, 3, 4, 5, 6, 7, 7}
		}, wantReason: ErrorReasonCoverage},
		{name: "row order non-member", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RowOrder = []int32{10, 0, 1, 2, 3, 4, 5, 6, 7, 8}
		}, wantReason: ErrorReasonCoverage},
		{name: "row order negative", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RowOrder = []int32{-1, 0, 1, 2, 3, 4, 5, 6, 7, 8}
		}, wantReason: ErrorReasonCoverage},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			columns := decodeBaseColumns(t)
			test.mutate(columns)
			limit := test.limit
			if limit == 0 && test.name != "zero limit" {
				limit = testLimit
			}
			rows, err := DecodeColumns(columns, limit)
			if rows != nil {
				t.Fatalf("failed decode returned %d rows", len(rows))
			}
			assertCodecReason(t, err, test.wantReason)
		})
	}
}

func TestDecodeColumnsAcceptsCanonicalVariants(t *testing.T) {
	t.Parallel()
	str := func(s string) *string { return &s }
	tests := []struct {
		name   string
		mutate func(*v1beta1.InstanceStatusColumns)
		check  func(*testing.T, []v1beta1.OMENativeInstanceStatus)
	}{
		{name: "base payload", mutate: func(*v1beta1.InstanceStatusColumns) {}},
		{name: "negative incarnation", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Incarnations = []v1beta1.InstanceStatusIncarnationGroup{{Value: -5, Indexes: "0-6,8-9"}, {Value: 2, Indexes: "7"}}
		}, check: func(t *testing.T, rows []v1beta1.OMENativeInstanceStatus) {
			if rows[0].Incarnation != -5 || rows[7].Incarnation != 2 {
				t.Fatalf("incarnations = %d, %d", rows[0].Incarnation, rows[7].Incarnation)
			}
		}},
		{name: "int32 count boundary", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.PodCounts = []v1beta1.InstanceStatusCountGroup{{Value: math.MaxInt32, Indexes: "0-9"}}
		}, check: func(t *testing.T, rows []v1beta1.OMENativeInstanceStatus) {
			if rows[9].PodCount != math.MaxInt32 {
				t.Fatalf("podCount = %d", rows[9].PodCount)
			}
		}},
		{name: "entry with empty conditions and a record", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Entries[0].Conditions = []metav1.Condition{}
		}, check: func(t *testing.T, rows []v1beta1.OMENativeInstanceStatus) {
			if rows[2].Conditions != nil || rows[2].ReadySince == nil {
				t.Fatalf("entry normalization failed: %+v", rows[2])
			}
		}},
		{name: "non-ascending row order", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RowOrder = []int32{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
		}, check: func(t *testing.T, rows []v1beta1.OMENativeInstanceStatus) {
			for i, row := range rows {
				if row.Index != int32(9-i) {
					t.Fatalf("row %d index = %d, want %d", i, row.Index, 9-i)
				}
			}
			if rows[2].ActiveOrdinal != 1 || rows[6].Phase != v1beta1.OMENativeInstanceUpdating || rows[7].ReadySince == nil {
				t.Fatalf("column values did not follow the row order: %+v", rows)
			}
		}},
		{name: "no optional columns", mutate: func(c *v1beta1.InstanceStatusColumns) {
			*c = v1beta1.InstanceStatusColumns{Members: c.Members, Phases: []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceDeleting, Indexes: "0-9"}}}
		}, check: func(t *testing.T, rows []v1beta1.OMENativeInstanceStatus) {
			for _, row := range rows {
				if row.Phase != v1beta1.OMENativeInstanceDeleting || row.Admitted || row.PodCount != 0 || row.RunningRevision != "" {
					t.Fatalf("defaults not applied: %+v", row)
				}
			}
		}},
		{name: "full admission", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Admitted = str("0-9") }, check: func(t *testing.T, rows []v1beta1.OMENativeInstanceStatus) {
			if !rows[8].Admitted {
				t.Fatal("admitted not applied")
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			columns := decodeBaseColumns(t)
			test.mutate(columns)
			rows := mustDecode(t, columns, testLimit)
			if len(rows) != 10 {
				t.Fatalf("decoded %d rows, want 10", len(rows))
			}
			if test.check != nil {
				test.check(t, rows)
			}
			assertColumnsEqual(t, columns, mustEncode(t, rows, testLimit))
		})
	}
}

func TestDecodeColumnsNilPayload(t *testing.T) {
	t.Parallel()
	rows, err := DecodeColumns(nil, testLimit)
	if rows != nil {
		t.Fatal("nil payload returned rows")
	}
	assertCodecReason(t, err, ErrorReasonRepresentationUnion)
}

func TestDecodeColumnsLimitBoundaries(t *testing.T) {
	t.Parallel()
	payload := func(members string) *v1beta1.InstanceStatusColumns {
		return &v1beta1.InstanceStatusColumns{Members: members, Phases: []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: members}}}
	}
	if rows := mustDecode(t, payload("0-4999"), 5000); len(rows) != 5000 {
		t.Fatalf("limit boundary decoded %d rows", len(rows))
	}
	if rows := mustDecode(t, payload("0-4998"), 5000); len(rows) != 4999 {
		t.Fatalf("limit minus one decoded %d rows", len(rows))
	}
	rows, err := DecodeColumns(payload("0-5000"), 5000)
	if rows != nil {
		t.Fatal("limit plus one returned rows")
	}
	assertCodecReason(t, err, ErrorReasonCardinalityLimit)
	rows, err = DecodeColumns(payload("0-4999"), 4999)
	if rows != nil {
		t.Fatal("lowered limit returned rows")
	}
	assertCodecReason(t, err, ErrorReasonCardinalityLimit)
}

// allocatedBytes measures the heap bytes one call allocates. The caller is a
// sequential test, so no parallel test allocates concurrently.
func allocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// The decoder must reject a decompression bomb on cardinality before it
// allocates or enumerates anything proportional to the claimed range.
func TestDecodeColumnsBoundsCardinalityBeforeExpansion(t *testing.T) {
	const budget = 16 << 10
	bombs := map[string]*v1beta1.InstanceStatusColumns{
		"members full domain": {
			Members: "0-2147483647",
			Phases:  []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-2147483647"}},
		},
		"one member, full-domain phase": {
			Members: "0",
			Phases:  []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-2147483647"}},
		},
		"one member, full-domain admitted": {
			Members:  "0",
			Phases:   []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0"}},
			Admitted: func() *string { s := "0-2147483647"; return &s }(),
		},
		"valid members, oversized trailing set": {
			Members:          "0-4999",
			Phases:           []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-4999"}},
			ActiveOrdinalOne: func() *string { s := "0-5000"; return &s }(),
		},
	}
	for name, columns := range bombs {
		var rows []v1beta1.OMENativeInstanceStatus
		var err error
		bytes := allocatedBytes(func() { rows, err = DecodeColumns(columns, 5000) })
		if rows != nil {
			t.Fatalf("%s: returned %d rows", name, len(rows))
		}
		assertCodecReason(t, err, ErrorReasonCardinalityLimit)
		allocs := testing.AllocsPerRun(20, func() { _, _ = DecodeColumns(columns, 5000) })
		t.Logf("%s: %d bytes, %.0f allocations before rejection", name, bytes, allocs)
		if bytes > budget || allocs > 32 {
			t.Fatalf("%s: rejection allocated %d bytes in %.0f allocations, want under %d bytes", name, bytes, allocs, budget)
		}
	}

	// A valid payload of the same member count does allocate its rows, which
	// shows the bound above is a property of validation, not of the fixture.
	valid := &v1beta1.InstanceStatusColumns{
		Members: "0-4999",
		Phases:  []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-4999"}},
	}
	bytes := allocatedBytes(func() { _, _ = DecodeColumns(valid, 5000) })
	t.Logf("valid 5000-row decode allocated %d bytes", bytes)
	if bytes < 5000*64 {
		t.Fatalf("valid decode allocated only %d bytes; the measurement is not observing row allocation", bytes)
	}
}

// The wire object still round-trips through typed JSON; a payload that is
// valid JSON but semantically corrupt fails after decoding with a reason.
func TestDecodeColumnsFromJSONFailsClosed(t *testing.T) {
	t.Parallel()
	tests := map[string]ErrorReason{
		`{"members":"0-2","phases":[{"value":"Ready","indexes":"0-2"}],"rowOrder":[0,1,2]}`:                         ErrorReasonCanonicalOrder,
		`{"members":"0-2","phases":[{"value":"Ready","indexes":"0-2"}],"entries":[{"index":1}]}`:                    ErrorReasonCoverage,
		`{"members":"0-2","phases":[{"value":"Ready","indexes":"0-2"}],"incarnations":[{"value":0,"indexes":"0"}]}`: ErrorReasonValueDomain,
		`{"members":"0-2","phases":[{"value":"Ready","indexes":"0-1"}]}`:                                            ErrorReasonCoverage,
		`{"members":"1,0","phases":[{"value":"Ready","indexes":"0-1"}]}`:                                            ErrorReasonRangeOrder,
		`{"members":"0-2"}`: ErrorReasonCoverage,
	}
	for raw, want := range tests {
		var columns v1beta1.InstanceStatusColumns
		if err := json.Unmarshal([]byte(raw), &columns); err != nil {
			t.Fatalf("unmarshal %s: %v", raw, err)
		}
		rows, err := DecodeColumns(&columns, testLimit)
		if rows != nil {
			t.Fatalf("%s: returned rows", raw)
		}
		assertCodecReason(t, err, want)
	}
}
