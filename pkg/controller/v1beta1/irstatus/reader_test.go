package irstatus

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func readerTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return scheme
}

func columnarFixture(t *testing.T, rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	t.Helper()
	columns, err := EncodeColumns(rows, uint64(len(rows)))
	if err != nil {
		t.Fatalf("encode columns: %v", err)
	}
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "model-engine", Namespace: "default"},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas:               int32(len(rows)),
			InstanceStatusEncoding: &encoding,
			InstanceStatusColumns:  columns,
		},
	}
}

func denseFixture(rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "model-engine", Namespace: "default"},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas:         int32(len(rows)),
			InstanceStatuses: rows,
		},
	}
}

func readerRows() []v1beta1.OMENativeInstanceStatus {
	return []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "model-engine-aaaa", PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true},
		{Index: 1, Incarnation: 2, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: "model-engine-aaaa", PodCount: 1, LastFailure: &v1beta1.InstanceTermination{PodName: "model-engine-1", Reason: "Error"}},
	}
}

func TestDecoderDecodeDenseIsIdentity(t *testing.T) {
	ir := denseFixture(readerRows())
	want := ir.DeepCopy()
	encoding, err := Decoder{}.Decode(ir)
	if err != nil {
		t.Fatalf("decode dense: %v", err)
	}
	if encoding != EncodingDenseV1 {
		t.Fatalf("encoding = %q, want DenseV1", encoding)
	}
	if !equality.Semantic.DeepEqual(ir, want) {
		t.Fatalf("dense object changed by decode:\n got: %#v\nwant: %#v", ir.Status, want.Status)
	}
}

func TestDecoderDecodeColumnarRewritesToLogicalShape(t *testing.T) {
	rows := readerRows()
	ir := columnarFixture(t, rows)
	encoding, err := NewDecoder(uint64(len(rows))).Decode(ir)
	if err != nil {
		t.Fatalf("decode columnar: %v", err)
	}
	if encoding != EncodingColumnarV2 {
		t.Fatalf("encoding = %q, want ColumnarV2", encoding)
	}
	if ir.Status.InstanceStatusEncoding != nil || ir.Status.InstanceStatusColumns != nil {
		t.Fatalf("decoded object still carries the ColumnarV2 marker or columns: %#v", ir.Status)
	}
	if !equality.Semantic.DeepEqual(ir.Status.InstanceStatuses, rows) {
		t.Fatalf("decoded rows differ:\n got: %#v\nwant: %#v", ir.Status.InstanceStatuses, rows)
	}
	if observed, err := ObservedEncoding(&ir.Status); err != nil || observed != EncodingDenseV1 {
		t.Fatalf("decoded object is not a valid DenseV1 status: encoding=%q err=%v", observed, err)
	}
}

func TestDecoderZeroValueFailsClosedOnColumnar(t *testing.T) {
	ir := columnarFixture(t, readerRows())
	before := ir.DeepCopy()
	_, err := Decoder{}.Decode(ir)
	if err == nil {
		t.Fatal("zero-value decoder decoded ColumnarV2 without a bound")
	}
	if reason, ok := ErrorReasonOf(err); !ok || reason != ErrorReasonCardinalityLimit {
		t.Fatalf("error reason = %q (%v), want %q", reason, err, ErrorReasonCardinalityLimit)
	}
	if !equality.Semantic.DeepEqual(ir, before) {
		t.Fatal("failed decode modified the object")
	}
}

func TestDecoderMalformedColumnarLeavesObjectUntouched(t *testing.T) {
	ir := columnarFixture(t, readerRows())
	ir.Status.InstanceStatusColumns.Members = "1-0"
	before := ir.DeepCopy()
	_, err := NewDecoder(10).Decode(ir)
	if err == nil {
		t.Fatal("malformed payload decoded")
	}
	if _, ok := ErrorReasonOf(err); !ok {
		t.Fatalf("malformed payload error is not a catalogued codec error: %v", err)
	}
	if !equality.Semantic.DeepEqual(ir, before) {
		t.Fatal("failed decode modified the object")
	}
}

func TestDecoderOfAndNewReader(t *testing.T) {
	if NewReader(nil, NewDecoder(5)) != nil {
		t.Fatal("NewReader(nil) must stay nil so missing-reader callers keep their behavior")
	}
	c := fake.NewClientBuilder().WithScheme(readerTestScheme(t)).Build()
	if got := DecoderOf(c); got.MaxDecodedInstances() != 0 {
		t.Fatalf("plain reader carries bound %d, want 0", got.MaxDecodedInstances())
	}
	wrapped := NewReader(c, NewDecoder(7))
	if got := DecoderOf(wrapped); got.MaxDecodedInstances() != 7 {
		t.Fatalf("wrapped reader carries bound %d, want 7", got.MaxDecodedInstances())
	}
}

func TestGetDecodedColumnarAndDenseAgree(t *testing.T) {
	ctx := context.Background()
	rows := readerRows()
	dense := denseFixture(rows)
	columnar := columnarFixture(t, rows)
	columnar.Name = "model-decoder"
	c := fake.NewClientBuilder().WithScheme(readerTestScheme(t)).
		WithObjects(dense, columnar).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()
	reads := NewReader(c, NewDecoder(uint64(len(rows))))

	gotDense := &v1beta1.InferenceReplica{}
	encoding, err := GetDecoded(ctx, reads, client.ObjectKeyFromObject(dense), gotDense)
	if err != nil || encoding != EncodingDenseV1 {
		t.Fatalf("dense GetDecoded: encoding=%q err=%v", encoding, err)
	}
	gotColumnar := &v1beta1.InferenceReplica{}
	encoding, err = GetDecoded(ctx, reads, client.ObjectKeyFromObject(columnar), gotColumnar)
	if err != nil || encoding != EncodingColumnarV2 {
		t.Fatalf("columnar GetDecoded: encoding=%q err=%v", encoding, err)
	}
	if !equality.Semantic.DeepEqual(gotDense.Status.InstanceStatuses, gotColumnar.Status.InstanceStatuses) {
		t.Fatalf("rows differ by stored encoding:\n dense: %#v\n columnar: %#v", gotDense.Status.InstanceStatuses, gotColumnar.Status.InstanceStatuses)
	}
	if gotColumnar.Status.InstanceStatusColumns != nil || gotColumnar.Status.InstanceStatusEncoding != nil {
		t.Fatal("GetDecoded left the ColumnarV2 representation on the object")
	}
}

func TestGetDecodedKeepsAPIErrorIdentity(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(readerTestScheme(t)).Build()
	_, err := GetDecoded(context.Background(), NewReader(c, NewDecoder(1)), client.ObjectKey{Namespace: "default", Name: "absent"}, &v1beta1.InferenceReplica{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("missing object error = %v, want NotFound", err)
	}
}

func TestGetDecodedUnboundedReaderFailsClosedOnColumnar(t *testing.T) {
	columnar := columnarFixture(t, readerRows())
	c := fake.NewClientBuilder().WithScheme(readerTestScheme(t)).WithObjects(columnar).Build()
	got := &v1beta1.InferenceReplica{}
	_, err := GetDecoded(context.Background(), c, client.ObjectKeyFromObject(columnar), got)
	var target *codecError
	if !errors.As(err, &target) {
		t.Fatalf("unbounded reader must fail closed with a codec error, got %v", err)
	}
}
