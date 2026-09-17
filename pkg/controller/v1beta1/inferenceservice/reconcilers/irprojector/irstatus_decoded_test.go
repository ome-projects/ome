package irprojector

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func decodedTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return scheme
}

func decodedFixtureRows() []v1beta1.OMENativeInstanceStatus {
	return []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "svc-engine-aaaaaaaa", PodCount: 1, ServingPodCount: 1, Admitted: true},
		{Index: 2, Incarnation: 1, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: "svc-engine-aaaaaaaa", TargetRevision: "svc-engine-bbbbbbbb", PodCount: 1},
	}
}

func denseIR(rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: InferenceReplicaName("svc", v1beta1.EngineComponent)},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas:         2,
			UpdateRevision:   "svc-engine-bbbbbbbb",
			InstanceStatuses: rows,
		},
	}
}

func columnarIR(t *testing.T, rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	t.Helper()
	ir := denseIR(nil)
	columns, err := irstatus.EncodeColumns(rows, uint64(len(rows)))
	if err != nil {
		t.Fatalf("encode columns: %v", err)
	}
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatusEncoding = &encoding
	ir.Status.InstanceStatusColumns = columns
	return ir
}

func TestDecodedComponentIRAgreesAcrossEncodings(t *testing.T) {
	ctx := context.Background()
	rows := decodedFixtureRows()
	dense := fake.NewClientBuilder().WithScheme(decodedTestScheme(t)).WithObjects(denseIR(rows)).Build()
	columnar := fake.NewClientBuilder().WithScheme(decodedTestScheme(t)).WithObjects(columnarIR(t, rows)).Build()
	decoder := irstatus.NewDecoder(uint64(len(rows)))

	denseIR, denseEncoding, err := DecodedComponentIR(ctx, irstatus.NewReader(dense, decoder), "prod", "svc", v1beta1.EngineComponent)
	if err != nil || denseIR == nil || denseEncoding != irstatus.EncodingDenseV1 {
		t.Fatalf("dense read: ir=%v encoding=%q err=%v", denseIR != nil, denseEncoding, err)
	}
	columnarIR, columnarEncoding, err := DecodedComponentIR(ctx, irstatus.NewReader(columnar, decoder), "prod", "svc", v1beta1.EngineComponent)
	if err != nil || columnarIR == nil || columnarEncoding != irstatus.EncodingColumnarV2 {
		t.Fatalf("columnar read: ir=%v encoding=%q err=%v", columnarIR != nil, columnarEncoding, err)
	}
	if !equality.Semantic.DeepEqual(columnarIR.Status, denseIR.Status) {
		t.Fatalf("decoded status differs by stored encoding:\n dense:    %+v\n columnar: %+v", denseIR.Status, columnarIR.Status)
	}
	if columnarIR.Status.InstanceStatusColumns != nil || columnarIR.Status.InstanceStatusEncoding != nil {
		t.Fatal("decoded object must carry the dense logical shape only")
	}

	status, err := DecodedComponentIRStatus(ctx, irstatus.NewReader(columnar, decoder), "prod", "svc", v1beta1.EngineComponent)
	if err != nil || !equality.Semantic.DeepEqual(status.InstanceStatuses, rows) {
		t.Fatalf("DecodedComponentIRStatus rows = %+v (err %v), want %+v", status, err, rows)
	}
}

func TestDecodedComponentIRAbsentAndNilReader(t *testing.T) {
	ctx := context.Background()
	if ir, encoding, err := DecodedComponentIR(ctx, nil, "prod", "svc", v1beta1.EngineComponent); ir != nil || encoding != "" || err != nil {
		t.Fatalf("nil reader must degrade to no observation, got ir=%v encoding=%q err=%v", ir, encoding, err)
	}
	empty := fake.NewClientBuilder().WithScheme(decodedTestScheme(t)).Build()
	if ir, _, err := DecodedComponentIR(ctx, irstatus.NewReader(empty, irstatus.NewDecoder(4)), "prod", "svc", v1beta1.EngineComponent); ir != nil || err != nil {
		t.Fatalf("missing IR must be (nil, nil), got ir=%v err=%v", ir, err)
	}
	if status, err := DecodedComponentIRStatus(ctx, empty, "prod", "svc", v1beta1.EngineComponent); status != nil || err != nil {
		t.Fatalf("missing IR status must be (nil, nil), got %v err=%v", status, err)
	}
}

func TestDecodedComponentIRFailsClosed(t *testing.T) {
	ctx := context.Background()
	rows := decodedFixtureRows()

	malformed := columnarIR(t, rows)
	malformed.Status.InstanceStatusColumns.Phases = nil
	c := fake.NewClientBuilder().WithScheme(decodedTestScheme(t)).WithObjects(malformed).Build()
	if ir, _, err := DecodedComponentIR(ctx, irstatus.NewReader(c, irstatus.NewDecoder(4)), "prod", "svc", v1beta1.EngineComponent); err == nil || ir != nil {
		t.Fatalf("malformed payload must be an error with no object, got ir=%v err=%v", ir, err)
	} else if reason, ok := irstatus.ErrorReasonOf(err); !ok || reason != irstatus.ErrorReasonCoverage {
		t.Fatalf("malformed payload error reason = %q (%v), want %s", reason, err, irstatus.ErrorReasonCoverage)
	}

	c = fake.NewClientBuilder().WithScheme(decodedTestScheme(t)).WithObjects(columnarIR(t, rows)).Build()
	if _, _, err := DecodedComponentIR(ctx, c, "prod", "svc", v1beta1.EngineComponent); err == nil {
		t.Fatal("a reader without a configured bound must fail closed on ColumnarV2")
	} else if reason, _ := irstatus.ErrorReasonOf(err); reason != irstatus.ErrorReasonCardinalityLimit {
		t.Fatalf("unbounded read error reason = %q, want %s", reason, irstatus.ErrorReasonCardinalityLimit)
	}

	// The raw accessor never decodes: it returns the stored representation
	// for callers that inspect no rows.
	raw, err := ComponentIR(ctx, c, "prod", "svc", v1beta1.EngineComponent)
	if err != nil || raw == nil || raw.Status.InstanceStatusColumns == nil || len(raw.Status.InstanceStatuses) != 0 {
		t.Fatalf("raw accessor must leave the ColumnarV2 representation untouched: %+v err=%v", raw, err)
	}
}
