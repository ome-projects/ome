package irstatus

import (
	"encoding/json"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
)

// The row fixtures live in irstatustest so the real-API-server suites measure
// the same deterministic inputs; these names keep the codec tests terse.
const (
	fixtureRevision      = irstatustest.Revision
	fixtureOtherRevision = irstatustest.OtherRevision
	fixtureContainer     = irstatustest.Container
)

var fixtureTime = irstatustest.Time

func fixtureTimePlus(d time.Duration) metav1.Time { return irstatustest.TimePlus(d) }

func uniformRows(n int) []v1beta1.OMENativeInstanceStatus { return irstatustest.UniformRows(n) }
func massFailureRows() []v1beta1.OMENativeInstanceStatus  { return irstatustest.MassFailureRows() }
func representativeRows(n int) []v1beta1.OMENativeInstanceStatus {
	return irstatustest.RepresentativeRows(n)
}
func fullFeatureRows() []v1beta1.OMENativeInstanceStatus { return irstatustest.FullFeatureRows() }
func wireExampleRows() []v1beta1.OMENativeInstanceStatus { return irstatustest.WireExampleRows() }

func logicalStatus(rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplicaStatus {
	return irstatustest.LogicalStatus(rows)
}

func columnarStatus(columns *v1beta1.InstanceStatusColumns) *v1beta1.InferenceReplicaStatus {
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	return &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &encoding, InstanceStatusColumns: columns}
}

func mustEncode(t testing.TB, rows []v1beta1.OMENativeInstanceStatus, limit uint64) *v1beta1.InstanceStatusColumns {
	t.Helper()
	columns, err := EncodeColumns(rows, limit)
	if err != nil {
		t.Fatalf("EncodeColumns() error = %v", err)
	}
	return columns
}

func mustDecode(t testing.TB, columns *v1beta1.InstanceStatusColumns, limit uint64) []v1beta1.OMENativeInstanceStatus {
	t.Helper()
	rows, err := DecodeColumns(columns, limit)
	if err != nil {
		t.Fatalf("DecodeColumns() error = %v", err)
	}
	return rows
}

func mustJSON(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

// assertRowsEqual compares rows with API semantics: nil and empty slices are
// the same, times compare by instant, and pointers compare by value.
func assertRowsEqual(t testing.TB, want, got []v1beta1.OMENativeInstanceStatus) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !equality.Semantic.DeepEqual(want[i], got[i]) {
			t.Fatalf("row %d differs:\n want %s\n got  %s", i, mustJSON(t, want[i]), mustJSON(t, got[i]))
		}
		if wantJSON, gotJSON := mustJSON(t, want[i]), mustJSON(t, got[i]); string(wantJSON) != string(gotJSON) {
			t.Fatalf("row %d wire differs:\n want %s\n got  %s", i, wantJSON, gotJSON)
		}
	}
}

func assertColumnsEqual(t testing.TB, want, got *v1beta1.InstanceStatusColumns) {
	t.Helper()
	if wantJSON, gotJSON := mustJSON(t, want), mustJSON(t, got); string(wantJSON) != string(gotJSON) {
		t.Fatalf("columns differ:\n want %s\n got  %s", wantJSON, gotJSON)
	}
}
