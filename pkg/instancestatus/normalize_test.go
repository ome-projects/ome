package instancestatus

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestNormalizeDenseV1(t *testing.T) {
	t.Parallel()

	exitCode := int32(137)
	status := &v1beta1.InferenceReplicaStatus{
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
			{
				Index:             2,
				Incarnation:       3,
				Phase:             v1beta1.OMENativeInstanceMigrating,
				RunningRevision:   "engine-old",
				TargetRevision:    "engine-new",
				PodCount:          2,
				ServingPodCount:   1,
				AvailablePodCount: 1,
				Admitted:          true,
				ActiveOrdinal:     1,
				Conditions:        []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}},
				Operation:         &v1beta1.InstanceOperation{ID: "migration-1"},
				LastFailure:       &v1beta1.InstanceTermination{PodName: "engine-2", ExitCode: &exitCode},
			},
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		},
	}

	got, err := Normalize(status)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if got.Encoding != EncodingDenseV1 {
		t.Fatalf("Encoding = %q, want %q", got.Encoding, EncodingDenseV1)
	}
	if !reflect.DeepEqual(got.Rows, status.InstanceStatuses) {
		t.Fatalf("Rows = %#v, want %#v", got.Rows, status.InstanceStatuses)
	}

	got.Rows[0].Conditions[0].Type = "mutated"
	got.Rows[0].Operation.ID = "mutated"
	*got.Rows[0].LastFailure.ExitCode = 1
	if status.InstanceStatuses[0].Conditions[0].Type != "Ready" ||
		status.InstanceStatuses[0].Operation.ID != "migration-1" ||
		*status.InstanceStatuses[0].LastFailure.ExitCode != 137 {
		t.Fatal("Normalize() result aliases the source DenseV1 status")
	}
}

func TestNormalizeDenseV1PreservesCompatibilityFields(t *testing.T) {
	t.Parallel()

	status := &v1beta1.InferenceReplicaStatus{
		InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
			Index:             0,
			Phase:             v1beta1.OMENativeInstanceReady,
			ReadyPodCount:     -1,
			ScheduledPodCount: -2,
		}},
	}

	got, err := Normalize(status)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if !reflect.DeepEqual(got.Rows, status.InstanceStatuses) {
		t.Fatalf("Rows = %#v, want compatibility fields preserved from %#v", got.Rows, status.InstanceStatuses)
	}
}

func TestNormalizeColumnarV2(t *testing.T) {
	t.Parallel()

	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	admitted := "0-1,5"
	activeOrdinalOne := "2,5"
	status := &v1beta1.InferenceReplicaStatus{
		InstanceStatusEncoding: &encoding,
		InstanceStatusColumns: &v1beta1.InstanceStatusColumns{
			Members:  "0-2,5",
			RowOrder: []int32{5, 2, 0, 1},
			Phases: []v1beta1.InstanceStatusPhaseGroup{
				{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-1,5"},
				{Value: v1beta1.OMENativeInstanceMigrating, Indexes: "2"},
			},
			RunningRevisions: []v1beta1.InstanceStatusStringGroup{{Value: "engine-old", Indexes: "0-2,5"}},
			TargetRevisions:  []v1beta1.InstanceStatusStringGroup{{Value: "engine-new", Indexes: "2,5"}},
			Incarnations:     []v1beta1.InstanceStatusInt64Group{{Value: 3, Indexes: "2,5"}},
			PodCounts:        []v1beta1.InstanceStatusCountGroup{{Value: 2, Indexes: "0-2,5"}},
			ServingPodCounts: []v1beta1.InstanceStatusCountGroup{
				{Value: 2, Indexes: "0-1"},
				{Value: 1, Indexes: "2,5"},
			},
			AvailablePodCounts: []v1beta1.InstanceStatusCountGroup{{Value: 2, Indexes: "0-1"}},
			Admitted:           &admitted,
			ActiveOrdinalOne:   &activeOrdinalOne,
			Entries: []v1beta1.InstanceStatusColumnEntry{{
				Index:      2,
				Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}},
				Operation:  &v1beta1.InstanceOperation{ID: "migration-2"},
			}},
		},
	}

	got, err := Normalize(status)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	want := Result{
		Encoding: EncodingColumnarV2,
		Rows: []v1beta1.OMENativeInstanceStatus{
			{Index: 5, Incarnation: 3, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "engine-old", TargetRevision: "engine-new", PodCount: 2, ServingPodCount: 1, Admitted: true, ActiveOrdinal: 1},
			{Index: 2, Incarnation: 3, Phase: v1beta1.OMENativeInstanceMigrating, RunningRevision: "engine-old", TargetRevision: "engine-new", PodCount: 2, ServingPodCount: 1, ActiveOrdinal: 1, Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}}, Operation: &v1beta1.InstanceOperation{ID: "migration-2"}},
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "engine-old", PodCount: 2, ServingPodCount: 2, AvailablePodCount: 2, Admitted: true},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "engine-old", PodCount: 2, ServingPodCount: 2, AvailablePodCount: 2, Admitted: true},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Normalize() = %#v, want %#v", got, want)
	}

	got.Rows[1].Conditions[0].Type = "mutated"
	got.Rows[1].Operation.ID = "mutated"
	entry := status.InstanceStatusColumns.Entries[0]
	if entry.Conditions[0].Type != "Ready" || entry.Operation.ID != "migration-2" {
		t.Fatal("Normalize() result aliases the source ColumnarV2 status")
	}
}

func TestNormalizeColumnarV2DefaultsToAscendingMemberOrder(t *testing.T) {
	t.Parallel()

	status := columnarStatus("1,3-4", []v1beta1.InstanceStatusPhaseGroup{{
		Value:   v1beta1.OMENativeInstanceReady,
		Indexes: "1,3-4",
	}})
	got, err := Normalize(status)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	want := []int32{1, 3, 4}
	for i, row := range got.Rows {
		if row.Index != want[i] {
			t.Fatalf("Rows[%d].Index = %d, want %d", i, row.Index, want[i])
		}
	}
}

func TestNormalizeDenseAndColumnarParity(t *testing.T) {
	t.Parallel()

	condition := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse}
	dense := &v1beta1.InferenceReplicaStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
		{Index: 3, Incarnation: 2, Phase: v1beta1.OMENativeInstanceMigrating, RunningRevision: "old", TargetRevision: "new", PodCount: 2, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true, ActiveOrdinal: 1, Conditions: []metav1.Condition{condition}, Operation: &v1beta1.InstanceOperation{ID: "op-3"}},
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "old", PodCount: 2, ServingPodCount: 2, AvailablePodCount: 2, Admitted: true},
		{Index: 2, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "old", PodCount: 2, ServingPodCount: 2},
	}}
	columnar := columnarStatus("0,2-3", []v1beta1.InstanceStatusPhaseGroup{
		{Value: v1beta1.OMENativeInstanceReady, Indexes: "0,2"},
		{Value: v1beta1.OMENativeInstanceMigrating, Indexes: "3"},
	})
	columns := columnar.InstanceStatusColumns
	columns.RowOrder = []int32{3, 0, 2}
	columns.RunningRevisions = []v1beta1.InstanceStatusStringGroup{{Value: "old", Indexes: "0,2-3"}}
	columns.TargetRevisions = []v1beta1.InstanceStatusStringGroup{{Value: "new", Indexes: "3"}}
	columns.Incarnations = []v1beta1.InstanceStatusInt64Group{{Value: 2, Indexes: "3"}}
	columns.PodCounts = []v1beta1.InstanceStatusCountGroup{{Value: 2, Indexes: "0,2-3"}}
	columns.ServingPodCounts = []v1beta1.InstanceStatusCountGroup{{Value: 2, Indexes: "0,2"}, {Value: 1, Indexes: "3"}}
	columns.AvailablePodCounts = []v1beta1.InstanceStatusCountGroup{{Value: 2, Indexes: "0"}, {Value: 1, Indexes: "3"}}
	admitted := "0,3"
	active := "3"
	columns.Admitted = &admitted
	columns.ActiveOrdinalOne = &active
	columns.Entries = []v1beta1.InstanceStatusColumnEntry{{Index: 3, Conditions: []metav1.Condition{condition}, Operation: &v1beta1.InstanceOperation{ID: "op-3"}}}

	denseResult, err := Normalize(dense)
	if err != nil {
		t.Fatalf("Normalize(DenseV1) error = %v", err)
	}
	columnarResult, err := Normalize(columnar)
	if err != nil {
		t.Fatalf("Normalize(ColumnarV2) error = %v", err)
	}
	if !reflect.DeepEqual(denseResult.Rows, columnarResult.Rows) {
		t.Fatalf("normalized rows differ:\nDenseV1: %#v\nColumnarV2: %#v", denseResult.Rows, columnarResult.Rows)
	}
}

func TestNormalizeEmptyDenseV1(t *testing.T) {
	t.Parallel()

	got, err := Normalize(&v1beta1.InferenceReplicaStatus{})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if got.Encoding != EncodingDenseV1 || got.Rows == nil || len(got.Rows) != 0 {
		t.Fatalf("Normalize() = %#v, want DenseV1 with non-nil empty rows", got)
	}
}

func TestNormalizeRejectsRepresentationContradictions(t *testing.T) {
	t.Parallel()

	columnar := v1beta1.InstanceStatusEncodingColumnarV2
	unknown := v1beta1.InstanceStatusEncoding("FutureV3")
	tests := []struct {
		name   string
		status *v1beta1.InferenceReplicaStatus
		reason ErrorReason
	}{
		{name: "nil status", reason: ErrorReasonRepresentation},
		{name: "columns without encoding", status: &v1beta1.InferenceReplicaStatus{InstanceStatusColumns: &v1beta1.InstanceStatusColumns{}}, reason: ErrorReasonRepresentation},
		{name: "columnar without columns", status: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &columnar}, reason: ErrorReasonRepresentation},
		{name: "columnar with dense rows", status: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &columnar, InstanceStatusColumns: &v1beta1.InstanceStatusColumns{}, InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0}}}, reason: ErrorReasonRepresentation},
		{name: "unknown encoding", status: &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &unknown}, reason: ErrorReasonUnknownEncoding},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Normalize(test.status)
			assertReason(t, err, test.reason)
		})
	}
}

func TestNormalizeRejectsMalformedColumnarV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*v1beta1.InstanceStatusColumns)
		reason ErrorReason
	}{
		{name: "empty members", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "" }, reason: ErrorReasonIndexSyntax},
		{name: "noncanonical members", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Members = "0,1" }, reason: ErrorReasonIndexCanonical},
		{name: "phase coverage gap", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Phases[0].Indexes = "0" }, reason: ErrorReasonCoverage},
		{name: "missing phases", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Phases = nil }, reason: ErrorReasonCoverage},
		{name: "phase overlap", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Phases = append(c.Phases, v1beta1.InstanceStatusPhaseGroup{Value: v1beta1.OMENativeInstanceFailed, Indexes: "1"})
		}, reason: ErrorReasonCoverage},
		{name: "invalid phase", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Phases[0].Value = "Unknown" }, reason: ErrorReasonValueDomain},
		{name: "row order missing member", mutate: func(c *v1beta1.InstanceStatusColumns) { c.RowOrder = []int32{0} }, reason: ErrorReasonRowOrder},
		{name: "row order duplicate", mutate: func(c *v1beta1.InstanceStatusColumns) { c.RowOrder = []int32{0, 0} }, reason: ErrorReasonRowOrder},
		{name: "row order outsider", mutate: func(c *v1beta1.InstanceStatusColumns) { c.RowOrder = []int32{0, 2} }, reason: ErrorReasonRowOrder},
		{name: "empty revision value", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RunningRevisions = []v1beta1.InstanceStatusStringGroup{{Indexes: "0"}}
		}, reason: ErrorReasonValueDomain},
		{name: "overlapping revision groups", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.RunningRevisions = []v1beta1.InstanceStatusStringGroup{{Value: "a", Indexes: "0-1"}, {Value: "b", Indexes: "1"}}
		}, reason: ErrorReasonCoverage},
		{name: "zero incarnation", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Incarnations = []v1beta1.InstanceStatusInt64Group{{Indexes: "0"}}
		}, reason: ErrorReasonValueDomain},
		{name: "negative incarnation", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Incarnations = []v1beta1.InstanceStatusInt64Group{{Value: -1, Indexes: "0"}}
		}, reason: ErrorReasonValueDomain},
		{name: "negative pod count", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.PodCounts = []v1beta1.InstanceStatusCountGroup{{Value: -1, Indexes: "0"}}
		}, reason: ErrorReasonValueDomain},
		{name: "boolean outsider", mutate: func(c *v1beta1.InstanceStatusColumns) { outsider := "2"; c.Admitted = &outsider }, reason: ErrorReasonCoverage},
		{name: "entry outsider", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Entries = []v1beta1.InstanceStatusColumnEntry{{Index: 2, Operation: &v1beta1.InstanceOperation{ID: "a"}}}
		}, reason: ErrorReasonCoverage},
		{name: "duplicate entries", mutate: func(c *v1beta1.InstanceStatusColumns) {
			c.Entries = []v1beta1.InstanceStatusColumnEntry{{Index: 0, Operation: &v1beta1.InstanceOperation{ID: "a"}}, {Index: 0, Operation: &v1beta1.InstanceOperation{ID: "b"}}}
		}, reason: ErrorReasonCoverage},
		{name: "empty entry", mutate: func(c *v1beta1.InstanceStatusColumns) { c.Entries = []v1beta1.InstanceStatusColumnEntry{{Index: 0}} }, reason: ErrorReasonValueDomain},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			status := columnarStatus("0-1", []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-1"}})
			test.mutate(status.InstanceStatusColumns)
			_, err := Normalize(status)
			assertReason(t, err, test.reason)
		})
	}
}

func TestNormalizeRejectsAmplification(t *testing.T) {
	t.Parallel()

	status := columnarStatus("0-3", []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0-3"}})
	_, err := normalizeWithLimit(status, 3)
	assertReason(t, err, ErrorReasonCardinality)
	_, err = normalizeWithLimit(status, 0)
	assertReason(t, err, ErrorReasonCardinality)
	_, err = normalizeWithLimit(columnarStatus("0", []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0"}}), DefaultMaxRows+1)
	assertReason(t, err, ErrorReasonCardinality)
}

func TestNormalizeRejectsOversizedRawIndexSetBeforeTokenizing(t *testing.T) {
	t.Parallel()

	oversized := "0" + strings.Repeat(",0", DefaultMaxRows*6)
	status := columnarStatus(oversized, []v1beta1.InstanceStatusPhaseGroup{{
		Value:   v1beta1.OMENativeInstanceReady,
		Indexes: oversized,
	}})
	_, err := Normalize(status)
	assertReason(t, err, ErrorReasonCardinality)
}

func TestNormalizeRejectsMalformedDenseV1(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rows []v1beta1.OMENativeInstanceStatus
	}{
		{name: "negative index", rows: []v1beta1.OMENativeInstanceStatus{{Index: -1, Phase: v1beta1.OMENativeInstanceReady}}},
		{name: "duplicate index", rows: []v1beta1.OMENativeInstanceStatus{{Index: 1, Phase: v1beta1.OMENativeInstanceReady}, {Index: 1, Phase: v1beta1.OMENativeInstanceReady}}},
		{name: "invalid phase", rows: []v1beta1.OMENativeInstanceStatus{{Index: 1, Phase: "Unknown"}}},
		{name: "negative count", rows: []v1beta1.OMENativeInstanceStatus{{Index: 1, Phase: v1beta1.OMENativeInstanceReady, PodCount: -1}}},
		{name: "negative incarnation", rows: []v1beta1.OMENativeInstanceStatus{{Index: 1, Incarnation: -1, Phase: v1beta1.OMENativeInstanceReady}}},
		{name: "invalid active ordinal", rows: []v1beta1.OMENativeInstanceStatus{{Index: 1, Phase: v1beta1.OMENativeInstanceReady, ActiveOrdinal: 2}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Normalize(&v1beta1.InferenceReplicaStatus{InstanceStatuses: test.rows})
			if err == nil {
				t.Fatal("Normalize() error = nil, want malformed DenseV1 rejection")
			}
		})
	}
}

func FuzzNormalizeColumnarFieldsNeverPanic(f *testing.F) {
	for _, seed := range []struct {
		members  string
		phases   string
		optional string
		boolean  string
		orderA   int32
		orderB   int32
	}{
		{members: "0", phases: "0", optional: "0", boolean: "0", orderA: 0, orderB: 0},
		{members: "0-1", phases: "0-1", optional: "0", boolean: "1", orderA: 1, orderB: 0},
		{members: "0-3", phases: "0-3", optional: "1-2", boolean: "3", orderA: 3, orderB: 0},
		{members: "0,2-4", phases: "0,2-4", optional: "4", boolean: "2", orderA: 4, orderB: 2},
		{members: "", phases: "00", optional: "3-1", boolean: "2147483648", orderA: -1, orderB: 5},
		{members: "sensitive", phases: "0,1", optional: "", boolean: "sensitive", orderA: 1, orderB: 1},
	} {
		f.Add(seed.members, seed.phases, seed.optional, seed.boolean, seed.orderA, seed.orderB)
	}
	f.Fuzz(func(t *testing.T, members, phases, optional, boolean string, orderA, orderB int32) {
		status := columnarStatus(members, []v1beta1.InstanceStatusPhaseGroup{{
			Value:   v1beta1.OMENativeInstanceReady,
			Indexes: phases,
		}})
		status.InstanceStatusColumns.RunningRevisions = []v1beta1.InstanceStatusStringGroup{{Value: "revision", Indexes: optional}}
		status.InstanceStatusColumns.Admitted = &boolean
		status.InstanceStatusColumns.RowOrder = []int32{orderA, orderB}
		result, err := normalizeWithLimit(status, 64)
		if err != nil {
			if _, ok := ErrorReasonOf(err); !ok {
				t.Fatalf("unclassified error: %T: %v", err, err)
			}
			if result.Encoding != "" || result.Rows != nil {
				t.Fatalf("failed normalization returned partial result %#v", result)
			}
			return
		}
		if len(result.Rows) > 64 {
			t.Fatalf("normalizeWithLimit() returned %d rows, limit 64", len(result.Rows))
		}
		seen := make(map[int32]struct{}, len(result.Rows))
		for _, row := range result.Rows {
			if row.Index < 0 {
				t.Fatalf("negative normalized index %d", row.Index)
			}
			if _, duplicate := seen[row.Index]; duplicate {
				t.Fatalf("duplicate normalized index %d", row.Index)
			}
			seen[row.Index] = struct{}{}
		}
	})
}

func TestDecodeErrorDoesNotExposePayload(t *testing.T) {
	t.Parallel()

	secret := "sensitive-revision-token"
	status := columnarStatus(secret, []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: secret}})
	_, err := Normalize(status)
	if err == nil {
		t.Fatal("Normalize() error = nil, want syntax error")
	}
	if got := err.Error(); got == "" || contains(got, secret) {
		t.Fatalf("error %q exposes status payload", got)
	}
	var decodeErr *DecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("error type = %T, want *DecodeError", err)
	}
}

func columnarStatus(members string, phases []v1beta1.InstanceStatusPhaseGroup) *v1beta1.InferenceReplicaStatus {
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	return &v1beta1.InferenceReplicaStatus{
		InstanceStatusEncoding: &encoding,
		InstanceStatusColumns: &v1beta1.InstanceStatusColumns{
			Members: members,
			Phases:  phases,
		},
	}
}

func assertReason(t *testing.T, err error, want ErrorReason) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want reason %q", want)
	}
	if got, ok := ErrorReasonOf(err); !ok || got != want {
		t.Fatalf("ErrorReasonOf(%v) = (%q, %t), want (%q, true)", err, got, ok, want)
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
