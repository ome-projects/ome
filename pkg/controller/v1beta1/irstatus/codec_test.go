package irstatus

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const testLimit = 5000

func TestRoundTripPreservesEveryRetainedField(t *testing.T) {
	t.Parallel()
	fixtures := map[string][]v1beta1.OMENativeInstanceStatus{
		"full feature":   fullFeatureRows(),
		"wire example":   wireExampleRows(),
		"representative": representativeRows(1000),
		"uniform":        uniformRows(2000),
		"mass failure":   massFailureRows(),
		"single row":     uniformRows(1),
	}
	for name, rows := range fixtures {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			original := mustJSON(t, rows)
			columns := mustEncode(t, rows, testLimit)
			decoded := mustDecode(t, columns, testLimit)
			assertRowsEqual(t, rows, decoded)
			if !bytes.Equal(original, mustJSON(t, rows)) {
				t.Fatal("encode mutated its input rows")
			}
			again := mustEncode(t, decoded, testLimit)
			assertColumnsEqual(t, columns, again)
			if len(columns.Entries) > 0 {
				columns.Entries[0].Index = -1
				for i := range decoded {
					if decoded[i].Index == -1 {
						t.Fatal("decoded rows alias the payload")
					}
				}
			}
		})
	}
}

func TestDecodedRowsShareNothingWithThePayload(t *testing.T) {
	t.Parallel()
	rows := fullFeatureRows()
	columns := mustEncode(t, rows, testLimit)
	decoded := mustDecode(t, columns, testLimit)
	for i := range columns.Entries {
		entry := &columns.Entries[i]
		if entry.Operation != nil {
			entry.Operation.ID = "mutated"
		}
		if entry.LastFailure != nil {
			entry.LastFailure.PodName = "mutated"
		}
		if len(entry.Conditions) > 0 {
			entry.Conditions[0].Type = "mutated"
		}
		if entry.ReadySince != nil {
			*entry.ReadySince = fixtureTimePlus(48 * time.Hour)
		}
	}
	assertRowsEqual(t, rows, decoded)
}

func TestEncodeIsDeterministicAcrossMapTraversal(t *testing.T) {
	t.Parallel()
	rows := representativeRows(1500)
	want := mustJSON(t, mustEncode(t, rows, testLimit))
	for i := 0; i < 32; i++ {
		if got := mustJSON(t, mustEncode(t, rows, testLimit)); !bytes.Equal(want, got) {
			t.Fatalf("encode %d differs:\n first %s\n later %s", i, want, got)
		}
	}
}

func TestEncodeCanonicalOrderAndMerging(t *testing.T) {
	t.Parallel()
	rows := []v1beta1.OMENativeInstanceStatus{
		{Index: 2, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-b", Incarnation: 3, PodCount: 5},
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-b", Incarnation: -2, PodCount: 5},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-a", Incarnation: 10, PodCount: 1},
		{Index: 4, Phase: v1beta1.OMENativeInstanceCreating, RunningRevision: "rev-c", Incarnation: 9, PodCount: 5},
		{Index: 3, Phase: v1beta1.OMENativeInstancePending, RunningRevision: "rev-B", Incarnation: 3, PodCount: 5},
		{Index: 6, Phase: v1beta1.OMENativeInstanceFailed, Operation: &v1beta1.InstanceOperation{ID: "op-6", Type: v1beta1.InstanceOperationDelete, Step: "Drain"}},
		{Index: 5, Phase: v1beta1.OMENativeInstanceFailed, Operation: &v1beta1.InstanceOperation{ID: "op-5", Type: v1beta1.InstanceOperationDelete, Step: "Drain"}},
	}
	columns := mustEncode(t, rows, testLimit)

	if got, want := columns.Members, "0-6"; got != want {
		t.Errorf("members = %q, want merged %q", got, want)
	}
	if got, want := columns.RowOrder, []int32{2, 0, 1, 4, 3, 6, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("rowOrder = %v, want %v", got, want)
	}
	wantPhases := []v1beta1.InstanceStatusPhaseGroup{
		{Value: v1beta1.OMENativeInstanceCreating, Indexes: "4"},
		{Value: v1beta1.OMENativeInstanceFailed, Indexes: "5-6"},
		{Value: v1beta1.OMENativeInstancePending, Indexes: "3"},
		{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-2"},
	}
	if !reflect.DeepEqual(columns.Phases, wantPhases) {
		t.Errorf("phases = %+v, want bytewise order %+v", columns.Phases, wantPhases)
	}
	wantRevisions := []v1beta1.InstanceStatusRevisionGroup{
		{Value: "rev-B", Indexes: "3"},
		{Value: "rev-a", Indexes: "1"},
		{Value: "rev-b", Indexes: "0,2"},
		{Value: "rev-c", Indexes: "4"},
	}
	if !reflect.DeepEqual(columns.RunningRevisions, wantRevisions) {
		t.Errorf("runningRevisions = %+v, want bytewise order %+v", columns.RunningRevisions, wantRevisions)
	}
	wantIncarnations := []v1beta1.InstanceStatusIncarnationGroup{
		{Value: -2, Indexes: "0"},
		{Value: 3, Indexes: "2-3"},
		{Value: 9, Indexes: "4"},
		{Value: 10, Indexes: "1"},
	}
	if !reflect.DeepEqual(columns.Incarnations, wantIncarnations) {
		t.Errorf("incarnations = %+v, want numeric order %+v", columns.Incarnations, wantIncarnations)
	}
	wantCounts := []v1beta1.InstanceStatusCountGroup{{Value: 1, Indexes: "1"}, {Value: 5, Indexes: "0,2-4"}}
	if !reflect.DeepEqual(columns.PodCounts, wantCounts) {
		t.Errorf("podCounts = %+v, want %+v", columns.PodCounts, wantCounts)
	}
	if len(columns.Entries) != 2 || columns.Entries[0].Index != 5 || columns.Entries[1].Index != 6 {
		t.Errorf("entries are not in ascending index order: %+v", columns.Entries)
	}
	if columns.TargetRevisions != nil || columns.ServingPodCounts != nil || columns.AvailablePodCounts != nil || columns.Admitted != nil || columns.ActiveOrdinalOne != nil {
		t.Errorf("all-default columns must be absent: %+v", columns)
	}
	assertRowsEqual(t, rows, mustDecode(t, columns, testLimit))
}

func TestEncodeMatchesWireExample(t *testing.T) {
	t.Parallel()
	columns := mustEncode(t, wireExampleRows(), testLimit)
	admitted := "0-499"
	activeOrdinalOne := "6,43,175"
	exitCode := int32(143)
	want := &v1beta1.InstanceStatusColumns{
		Members:            "0-499",
		Phases:             []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-499"}},
		RunningRevisions:   []v1beta1.InstanceStatusRevisionGroup{{Value: fixtureRevision, Indexes: "0-499"}},
		Incarnations:       []v1beta1.InstanceStatusIncarnationGroup{{Value: 1, Indexes: "0-8,10-114,116-499"}, {Value: 2, Indexes: "9,115"}},
		PodCounts:          []v1beta1.InstanceStatusCountGroup{{Value: 1, Indexes: "0-499"}},
		ServingPodCounts:   []v1beta1.InstanceStatusCountGroup{{Value: 1, Indexes: "0-499"}},
		AvailablePodCounts: []v1beta1.InstanceStatusCountGroup{{Value: 1, Indexes: "0-499"}},
		Admitted:           &admitted,
		ActiveOrdinalOne:   &activeOrdinalOne,
		Entries: []v1beta1.InstanceStatusEntry{{
			Index:       9,
			LastFailure: &v1beta1.InstanceTermination{PodName: "example-engine-9", ContainerName: fixtureContainer, Reason: "Error", ExitCode: &exitCode, Time: fixtureTime},
		}},
	}
	assertColumnsEqual(t, want, columns)
	if !reflect.DeepEqual(want, columns) {
		t.Fatalf("typed payload differs from the example:\n want %+v\n got  %+v", want, columns)
	}
	wantJSON := `{"members":"0-499","phases":[{"value":"Ready","indexes":"0-499"}],` +
		`"runningRevisions":[{"value":"example-engine-2f32f6fe","indexes":"0-499"}],` +
		`"incarnations":[{"value":1,"indexes":"0-8,10-114,116-499"},{"value":2,"indexes":"9,115"}],` +
		`"podCounts":[{"value":1,"indexes":"0-499"}],"servingPodCounts":[{"value":1,"indexes":"0-499"}],` +
		`"availablePodCounts":[{"value":1,"indexes":"0-499"}],"admitted":"0-499","activeOrdinalOne":"6,43,175",` +
		`"entries":[{"index":9,"lastFailure":{"podName":"example-engine-9","containerName":"ome-container","reason":"Error","exitCode":143,"time":"2026-03-04T05:06:07Z"}}]}`
	if got := string(mustJSON(t, columns)); got != wantJSON {
		t.Fatalf("wire JSON differs:\n want %s\n got  %s", wantJSON, got)
	}
}

func TestRowOrderRoundTripsEveryPermutation(t *testing.T) {
	t.Parallel()
	base := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 3, Phase: v1beta1.OMENativeInstanceUpdating, TargetRevision: fixtureRevision},
		{Index: 4, Phase: v1beta1.OMENativeInstanceFailed, Incarnation: 2},
		{Index: 9, Phase: v1beta1.OMENativeInstanceReady, Admitted: true},
	}
	permutations := 0
	var permute func(prefix []int, remaining []int)
	permute = func(prefix []int, remaining []int) {
		if len(remaining) == 0 {
			permutations++
			rows := make([]v1beta1.OMENativeInstanceStatus, len(prefix))
			ascending := true
			for i, position := range prefix {
				rows[i] = base[position]
				if i > 0 && position < prefix[i-1] {
					ascending = false
				}
			}
			columns := mustEncode(t, rows, testLimit)
			if ascending != (columns.RowOrder == nil) {
				t.Fatalf("permutation %v: rowOrder present = %t, want %t", prefix, columns.RowOrder != nil, !ascending)
			}
			assertRowsEqual(t, rows, mustDecode(t, columns, testLimit))
			return
		}
		for i := range remaining {
			next := append(append([]int{}, remaining[:i]...), remaining[i+1:]...)
			permute(append(append([]int{}, prefix...), remaining[i]), next)
		}
	}
	permute(nil, []int{0, 1, 2, 3})
	if permutations != 24 {
		t.Fatalf("exercised %d permutations, want 24", permutations)
	}
}

func TestDecodeDefaultsMatchDenseV1(t *testing.T) {
	t.Parallel()
	columns := &v1beta1.InstanceStatusColumns{
		Members: "0-2,9",
		Phases:  []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstancePending, Indexes: "0-2,9"}},
	}
	rows := mustDecode(t, columns, testLimit)
	want := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstancePending},
		{Index: 1, Phase: v1beta1.OMENativeInstancePending},
		{Index: 2, Phase: v1beta1.OMENativeInstancePending},
		{Index: 9, Phase: v1beta1.OMENativeInstancePending},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("defaults differ:\n want %+v\n got  %+v", want, rows)
	}
	assertColumnsEqual(t, columns, mustEncode(t, rows, testLimit))
}

func TestTypedValueBoundaries(t *testing.T) {
	t.Parallel()
	rows := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstancePending, Incarnation: math.MinInt64, PodCount: math.MaxInt32},
		{Index: 1, Phase: v1beta1.OMENativeInstanceCreating, Incarnation: math.MaxInt64, ServingPodCount: math.MaxInt32},
		{Index: 2, Phase: v1beta1.OMENativeInstanceReady, Incarnation: -1, AvailablePodCount: math.MaxInt32, ActiveOrdinal: 1},
		{Index: 3, Phase: v1beta1.OMENativeInstanceUpdating, Incarnation: 1, ActiveOrdinal: 0},
		{Index: 4, Phase: v1beta1.OMENativeInstanceRestarting, RunningRevision: strings.Repeat("r", 253)},
		{Index: 5, Phase: v1beta1.OMENativeInstanceMigrating, TargetRevision: strings.Repeat("t", 253)},
		{Index: 6, Phase: v1beta1.OMENativeInstanceFailed},
		{Index: math.MaxInt32, Phase: v1beta1.OMENativeInstanceDeleting},
	}
	columns := mustEncode(t, rows, testLimit)
	assertRowsEqual(t, rows, mustDecode(t, columns, testLimit))

	// The typed wire must carry the full int64 and int32 domains through JSON.
	var reparsed v1beta1.InstanceStatusColumns
	if err := json.Unmarshal(mustJSON(t, columns), &reparsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	assertRowsEqual(t, rows, mustDecode(t, &reparsed, testLimit))
	if got := reparsed.Incarnations[0].Value; got != math.MinInt64 {
		t.Fatalf("int64 boundary lost precision: %d", got)
	}
}

func TestObservedEncodingAndDecodeStatus(t *testing.T) {
	t.Parallel()
	columns := mustEncode(t, uniformRows(3), testLimit)
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	unknown := v1beta1.InstanceStatusEncoding("ColumnarV3")
	empty := v1beta1.InstanceStatusEncoding("")
	tests := []struct {
		name         string
		status       *v1beta1.InferenceReplicaStatus
		limit        uint64
		wantEncoding Encoding
		wantRows     int
		wantReason   ErrorReason
	}{
		{name: "nil status", status: nil, limit: 1, wantEncoding: EncodingDenseV1},
		{name: "empty status", status: &v1beta1.InferenceReplicaStatus{}, limit: 1, wantEncoding: EncodingDenseV1},
		{name: "dense rows have no cardinality check", status: logicalStatus(uniformRows(10)), limit: 1, wantEncoding: EncodingDenseV1, wantRows: 10},
		{name: "columnar", status: columnarStatus(columns), limit: 3, wantEncoding: EncodingColumnarV2, wantRows: 3},
		{name: "columnar above limit", status: columnarStatus(columns), limit: 2, wantReason: ErrorReasonCardinalityLimit},
		{name: "unmarked with columns", status: &v1beta1.InferenceReplicaStatus{InstanceStatusColumns: columns}, limit: testLimit, wantReason: ErrorReasonRepresentationUnion},
		{name: "marker without columns", status: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &marker}, limit: testLimit, wantReason: ErrorReasonRepresentationUnion},
		{name: "marker with dense rows", status: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &marker, InstanceStatusColumns: columns, InstanceStatuses: uniformRows(1)}, limit: testLimit, wantReason: ErrorReasonRepresentationUnion},
		{name: "unknown marker", status: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &unknown, InstanceStatusColumns: columns}, limit: testLimit, wantReason: ErrorReasonUnknownEncoding},
		{name: "empty marker", status: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &empty, InstanceStatusColumns: columns}, limit: testLimit, wantReason: ErrorReasonUnknownEncoding},
		{name: "corrupt columns", status: columnarStatus(&v1beta1.InstanceStatusColumns{Members: "0-2"}), limit: testLimit, wantReason: ErrorReasonCoverage},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rows, encoding, err := DecodeStatus(test.status, test.limit)
			if test.wantReason != "" {
				if rows != nil || encoding != "" {
					t.Fatalf("failed decode returned rows=%d encoding=%q", len(rows), encoding)
				}
				assertCodecReason(t, err, test.wantReason)
				if _, observeErr := ObservedEncoding(test.status); observeErr == nil && test.wantReason != ErrorReasonCardinalityLimit && test.wantReason != ErrorReasonCoverage {
					t.Fatal("ObservedEncoding accepted an invalid union")
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeStatus() error = %v", err)
			}
			if encoding != test.wantEncoding || len(rows) != test.wantRows {
				t.Fatalf("DecodeStatus() = (%d rows, %q), want (%d rows, %q)", len(rows), encoding, test.wantRows, test.wantEncoding)
			}
			observed, err := ObservedEncoding(test.status)
			if err != nil || observed != test.wantEncoding {
				t.Fatalf("ObservedEncoding() = (%q, %v), want %q", observed, err, test.wantEncoding)
			}
		})
	}
}

func TestEncodeColumnsRejectsIneligibleRows(t *testing.T) {
	t.Parallel()
	identity := func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus { return rows }
	tests := []struct {
		name       string
		mutate     func([]v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus
		limit      uint64
		wantReason ErrorReason
	}{
		{name: "zero rows", mutate: func([]v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus { return nil }, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "empty rows", mutate: func([]v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			return []v1beta1.OMENativeInstanceStatus{}
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "zero limit", mutate: identity, limit: 0, wantReason: ErrorReasonCardinalityLimit},
		{name: "above limit", mutate: identity, limit: 2, wantReason: ErrorReasonCardinalityLimit},
		{name: "negative index", mutate: func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			rows[1].Index = -1
			return rows
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "duplicate index", mutate: func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			rows[2].Index = 0
			return rows
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "negative pod count", mutate: func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			rows[0].PodCount = -1
			return rows
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "negative serving count", mutate: func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			rows[0].ServingPodCount = -1
			return rows
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "negative available count", mutate: func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			rows[0].AvailablePodCount = -1
			return rows
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "unsupported phase", mutate: func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			rows[0].Phase = "Unknown"
			return rows
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "empty phase", mutate: func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			rows[0].Phase = ""
			return rows
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "ordinal two", mutate: func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			rows[0].ActiveOrdinal = 2
			return rows
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
		{name: "negative ordinal", mutate: func(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
			rows[0].ActiveOrdinal = -1
			return rows
		}, limit: testLimit, wantReason: ErrorReasonValueDomain},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			columns, err := EncodeColumns(test.mutate(uniformRows(3)), test.limit)
			if columns != nil {
				t.Fatalf("ineligible rows produced columns: %+v", columns)
			}
			assertCodecReason(t, err, test.wantReason)
		})
	}

	// A negative nonzero incarnation and the three Pod-derived fields never
	// make rows ineligible: the first is representable, the others are not stored.
	rows := uniformRows(2)
	rows[0].Incarnation = -3
	rows[1].ReadyPodCount, rows[1].ScheduledPodCount, rows[1].NodesOccupied = 4, 4, []string{"node-a"}
	columns := mustEncode(t, rows, testLimit)
	decoded := mustDecode(t, columns, testLimit)
	if decoded[0].Incarnation != -3 || decoded[1].ReadyPodCount != 0 || decoded[1].ScheduledPodCount != 0 || decoded[1].NodesOccupied != nil {
		t.Fatalf("unexpected decode of eligible rows: %+v", decoded)
	}
}

func TestExplicitEmptyConditionsNormalizeLikeDenseV1(t *testing.T) {
	t.Parallel()
	rows := uniformRows(2)
	rows[0].Conditions = []metav1.Condition{}
	rows[1].Conditions = []metav1.Condition{}
	rows[1].ReadySince = &fixtureTime
	columns := mustEncode(t, rows, testLimit)
	if len(columns.Entries) != 1 || columns.Entries[0].Index != 1 || columns.Entries[0].Conditions != nil {
		t.Fatalf("empty conditions must not create or populate an entry: %+v", columns.Entries)
	}
	decoded := mustDecode(t, columns, testLimit)
	assertRowsEqual(t, rows, decoded)
	if decoded[0].Conditions != nil || decoded[1].Conditions != nil {
		t.Fatal("decoded rows carry an explicit empty conditions slice")
	}
}

// rowFieldDispositions is the codec's disposition for every JSON field of a
// dense row. A new row field must be added here with its column, its entry
// placement, or an explicit not-stored decision.
var rowFieldDispositions = map[string]string{
	"index":             "members",
	"incarnation":       "incarnations",
	"phase":             "phases",
	"runningRevision":   "runningRevisions",
	"targetRevision":    "targetRevisions",
	"podCount":          "podCounts",
	"readyPodCount":     "not stored",
	"servingPodCount":   "servingPodCounts",
	"availablePodCount": "availablePodCounts",
	"scheduledPodCount": "not stored",
	"admitted":          "admitted",
	"nodesOccupied":     "not stored",
	"conditions":        "entries",
	"readySince":        "entries",
	"operation":         "entries",
	"activeOrdinal":     "activeOrdinalOne",
	"lastFailure":       "entries",
}

func TestEveryRowFieldHasACodecDisposition(t *testing.T) {
	t.Parallel()
	jsonNames := func(kind reflect.Type) []string {
		names := make([]string, 0, kind.NumField())
		for i := 0; i < kind.NumField(); i++ {
			name := strings.Split(kind.Field(i).Tag.Get("json"), ",")[0]
			if name == "" || name == "-" {
				t.Fatalf("%s.%s has no JSON name", kind.Name(), kind.Field(i).Name)
			}
			names = append(names, name)
		}
		return names
	}

	rowFields := jsonNames(reflect.TypeOf(v1beta1.OMENativeInstanceStatus{}))
	seen := make(map[string]bool, len(rowFields))
	for _, name := range rowFields {
		if _, ok := rowFieldDispositions[name]; !ok {
			t.Errorf("row field %q has no codec disposition", name)
		}
		seen[name] = true
	}
	for name := range rowFieldDispositions {
		if !seen[name] {
			t.Errorf("disposition %q names no row field", name)
		}
	}

	targets := make(map[string]bool)
	for _, target := range rowFieldDispositions {
		targets[target] = true
	}
	for _, name := range jsonNames(reflect.TypeOf(v1beta1.InstanceStatusColumns{})) {
		if name != "rowOrder" && !targets[name] {
			t.Errorf("column %q is not the disposition of any row field", name)
		}
	}
	for _, name := range jsonNames(reflect.TypeOf(v1beta1.InstanceStatusEntry{})) {
		if name != "index" && rowFieldDispositions[name] != "entries" {
			t.Errorf("entry field %q is not an entry-disposition row field", name)
		}
	}
}
