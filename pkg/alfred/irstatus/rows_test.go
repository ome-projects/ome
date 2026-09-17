package irstatus

import (
	"reflect"
	"strconv"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	codec "sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func TestRowsPreservesLogicalOrderAndOwnsCopies(t *testing.T) {
	// Catches reading only instanceStatuses, sorting sparse rows, or aliasing
	// the captured status through the returned rows.
	want := []v1beta1.OMENativeInstanceStatus{
		{Index: 7, Phase: v1beta1.OMENativeInstanceReady, Incarnation: 2, PodCount: 1, Admitted: true,
			Operation: &v1beta1.InstanceOperation{ID: "exception"}},
		{Index: 2, Phase: v1beta1.OMENativeInstancePending},
	}
	columns, err := codec.EncodeColumns(want, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	for _, test := range []struct {
		name   string
		status v1beta1.InferenceReplicaStatus
	}{
		{name: "dense", status: v1beta1.InferenceReplicaStatus{InstanceStatuses: want}},
		{name: "columnar", status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &marker, InstanceStatusColumns: columns}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := test.status.DeepCopy()
			got, err := Rows(&test.status)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("Rows() = %#v, %v; want %#v", got, err, want)
			}
			got[0].Operation.ID = "changed"
			got[0].Index = 99
			if !reflect.DeepEqual(test.status, *before) {
				t.Fatal("Rows mutated or aliased source status")
			}
		})
	}
}

func TestRowsRejectsMalformedUnionAndCardinality(t *testing.T) {
	// Catches treating malformed or oversized payloads as an empty valid row set.
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	unknown := v1beta1.InstanceStatusEncoding("Future")
	for _, test := range []struct {
		name   string
		status v1beta1.InferenceReplicaStatus
	}{
		{name: "unknown", status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &unknown}},
		{name: "mixed", status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &marker, InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 1}}}},
		{name: "missing columns", status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &marker}},
		{name: "expansion bomb", status: v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &marker, InstanceStatusColumns: &v1beta1.InstanceStatusColumns{Members: "0-2147483647"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, err := Rows(&test.status)
			if err == nil || len(rows) != 0 {
				t.Fatalf("Rows() = %d rows, %v; want no rows and error", len(rows), err)
			}
		})
	}
	for _, n := range []int{10_000, 10_001} {
		rows := make([]v1beta1.OMENativeInstanceStatus, n)
		for i := range rows {
			rows[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: v1beta1.OMENativeInstancePending}
		}
		columns, err := codec.EncodeColumns(rows, 10_001)
		if err != nil {
			t.Fatal(err)
		}
		for _, status := range []v1beta1.InferenceReplicaStatus{
			{InstanceStatuses: rows},
			{InstanceStatusEncoding: &marker, InstanceStatusColumns: columns},
		} {
			got, err := Rows(&status)
			if (err == nil) != (n == 10_000) || (err == nil && len(got) != n) || (err != nil && len(got) != 0) {
				t.Fatalf("%s rows = %d, %v", strconv.Itoa(n), len(got), err)
			}
		}
	}
}
