package inferencereplica

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// TestReconcilerReadsDecodeColumnarV2Identically drives every reader-side
// decision seam of the reconciler against the same logical rows stored as
// DenseV1 and as ColumnarV2.
func TestReconcilerReadsDecodeColumnarV2Identically(t *testing.T) {
	dense := decodeFixtureIR()
	rDense, _ := newReconciler(t, dense)
	rColumnar, _ := newReconciler(t, columnarTwin(t, dense))
	rColumnar.InstanceStatusDecoder = irstatus.NewDecoder(8)
	key := client.ObjectKeyFromObject(dense)

	want, got := decisionsFor(t, rDense, key), decisionsFor(t, rColumnar, key)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reader decisions differ by stored encoding:\n dense:    %+v\n columnar: %+v", want, got)
	}
	if !want.anyFailed || want.ready.Reason != ReasonInstanceFailed || want.stalled.Status != metav1.ConditionTrue || len(want.liveRevisions) != 2 {
		t.Fatalf("fixture does not exercise the decision seams as intended: %+v", want)
	}
}

func TestReconcileFailsClosedOnUndecodableColumnarV2(t *testing.T) {
	ctx := context.Background()
	base := decodeFixtureIR()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(base)}

	t.Run("malformed payload", func(t *testing.T) {
		malformed := columnarTwin(t, base)
		malformed.Status.InstanceStatusColumns.Members = "1-0"
		r, c := newReconciler(t, malformed)
		r.InstanceStatusDecoder = irstatus.NewDecoder(8)
		recorder := record.NewFakeRecorder(32)
		r.Recorder = recorder

		_, err := r.Reconcile(ctx, req)
		if reason, ok := irstatus.ErrorReasonOf(err); !ok || reason != irstatus.ErrorReasonRangeOrder {
			t.Fatalf("malformed payload must fail closed with a codec reason, got %v", err)
		}
		select {
		case event := <-recorder.Events:
			if !strings.Contains(event, EventReasonInstanceStatusDecodeFailed) || !strings.Contains(event, string(irstatus.ErrorReasonRangeOrder)) {
				t.Fatalf("decode failure event = %q, want reason %s", event, EventReasonInstanceStatusDecodeFailed)
			}
		default:
			t.Fatal("decode failure must emit a Warning event")
		}
		pods := &corev1.PodList{}
		if err := c.List(ctx, pods, client.InNamespace(base.Namespace)); err != nil || len(pods.Items) != 0 {
			t.Fatalf("no effect may follow a decode failure: pods=%d err=%v", len(pods.Items), err)
		}
		stored := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, req.NamespacedName, stored); err != nil {
			t.Fatalf("get stored IR: %v", err)
		}
		if !equality.Semantic.DeepEqual(stored.Status, malformed.Status) {
			t.Fatalf("a failed decode must leave the stored status untouched")
		}
	})

	t.Run("no bound configured", func(t *testing.T) {
		r, _ := newReconciler(t, columnarTwin(t, base))
		_, err := r.Reconcile(ctx, req)
		if reason, ok := irstatus.ErrorReasonOf(err); !ok || reason != irstatus.ErrorReasonCardinalityLimit {
			t.Fatalf("a DenseV1-target manager without a bound must fail closed on ColumnarV2, got %v", err)
		}
	})
}

// TestReadersPairTheDecodeBoundWithTheirClients pins which client each
// reader wraps and that both carry the configured bound: the reconcile
// entry read is served from the informer cache, every conflict retry from
// the authoritative reader, and neither may decode past the bound.
func TestReadersPairTheDecodeBoundWithTheirClients(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	cachedOnly := baselineIR("cached-engine", "default", 1)
	liveOnly := baselineIR("live-engine", "default", 1)
	r := &Reconciler{
		Client:                fake.NewClientBuilder().WithScheme(scheme).WithObjects(cachedOnly).Build(),
		APIReader:             fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveOnly).Build(),
		InstanceStatusDecoder: irstatus.NewDecoder(3),
	}
	for _, tc := range []struct {
		name            string
		reader          client.Reader
		present, absent client.ObjectKey
	}{
		{"cachedReader", r.cachedReader(), client.ObjectKeyFromObject(cachedOnly), client.ObjectKeyFromObject(liveOnly)},
		{"liveReader", r.liveReader(), client.ObjectKeyFromObject(liveOnly), client.ObjectKeyFromObject(cachedOnly)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := irstatus.DecoderOf(tc.reader).MaxDecodedInstances(); got != 3 {
				t.Fatalf("decode bound = %d, want the reconciler's 3", got)
			}
			ir := &v1beta1.InferenceReplica{}
			if _, err := irstatus.GetDecoded(ctx, tc.reader, tc.present, ir); err != nil {
				t.Fatalf("read through own client: %v", err)
			}
			if _, err := irstatus.GetDecoded(ctx, tc.reader, tc.absent, ir); !apierrors.IsNotFound(err) {
				t.Fatalf("%s must read only its own client; got %v for the other client's object", tc.name, err)
			}
		})
	}
}

// TestReadersDecodeUnderTheConfiguredBound drives a ColumnarV2 object with
// four rows through both readers: a bound that admits the rows yields the
// dense twin's rows, a bound below them fails closed with the cardinality
// reason, and no bound fails closed the same way.
func TestReadersDecodeUnderTheConfiguredBound(t *testing.T) {
	ctx := context.Background()
	dense := fixtureIRWithRows("llama-engine", uniformFixtureRows(4))
	columnar := columnarTwin(t, dense)
	key := client.ObjectKeyFromObject(dense)
	for _, tc := range []struct {
		name       string
		bound      uint64
		wantReason irstatus.ErrorReason
	}{
		{"bound admits the rows", 4, ""},
		{"bound below the rows fails closed", 3, irstatus.ErrorReasonCardinalityLimit},
		{"no bound fails closed", 0, irstatus.ErrorReasonCardinalityLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newReconciler(t, columnar.DeepCopy())
			r.InstanceStatusDecoder = irstatus.NewDecoder(tc.bound)
			for name, reader := range map[string]client.Reader{"cached": r.cachedReader(), "live": r.liveReader()} {
				ir := &v1beta1.InferenceReplica{}
				encoding, err := irstatus.GetDecoded(ctx, reader, key, ir)
				if tc.wantReason != "" {
					if reason, ok := irstatus.ErrorReasonOf(err); !ok || reason != tc.wantReason {
						t.Fatalf("%s reader: want codec reason %s, got %v", name, tc.wantReason, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s reader: %v", name, err)
				}
				if encoding != irstatus.EncodingColumnarV2 {
					t.Fatalf("%s reader: stored encoding = %s, want %s", name, encoding, irstatus.EncodingColumnarV2)
				}
				if !equality.Semantic.DeepEqual(ir.Status.InstanceStatuses, dense.Status.InstanceStatuses) {
					t.Fatalf("%s reader: decoded rows differ from the dense twin", name)
				}
			}
		})
	}
}

// TestInstanceStatusDecodeError classifies the entry read's failure: a
// codec failure is counted, reported as a Warning naming the fixed-catalog
// reason and returned wrapped so the reconcile fails closed; any other error
// passes through untouched with no event and no count.
func TestInstanceStatusDecodeError(t *testing.T) {
	const codecErrors = "ome_omenative_ir_status_codec_errors_total"
	ir := baselineIR("llama-engine", "default", 1)
	malformed := columnarTwin(t, decodeFixtureIR())
	malformed.Status.InstanceStatusColumns.Members = "1-0"
	_, codecErr := irstatus.NewDecoder(8).Decode(malformed)
	if reason, ok := irstatus.ErrorReasonOf(codecErr); !ok || reason != irstatus.ErrorReasonRangeOrder {
		t.Fatalf("fixture must produce a range-order codec error, got %v", codecErr)
	}
	reasonLabel := map[string]string{"reason": string(irstatus.ErrorReasonRangeOrder)}

	t.Run("codec failure is counted, reported and wrapped", func(t *testing.T) {
		recorder := record.NewFakeRecorder(4)
		r := &Reconciler{Recorder: recorder}
		before := irStatusMetric(t, codecErrors, reasonLabel)
		got := r.instanceStatusDecodeError(ir, codecErr)
		if !errors.Is(got, codecErr) {
			t.Fatalf("returned error must wrap the codec error, got %v", got)
		}
		if reason, ok := irstatus.ErrorReasonOf(got); !ok || reason != irstatus.ErrorReasonRangeOrder {
			t.Fatalf("codec reason must survive wrapping, got %v", got)
		}
		if !strings.Contains(got.Error(), "default/llama-engine") {
			t.Fatalf("error must name the object, got %q", got.Error())
		}
		if after := irStatusMetric(t, codecErrors, reasonLabel); after != before+1 {
			t.Fatalf("codec error counter = %v, want %v", after, before+1)
		}
		select {
		case event := <-recorder.Events:
			if !strings.HasPrefix(event, corev1.EventTypeWarning) || !strings.Contains(event, EventReasonInstanceStatusDecodeFailed) ||
				!strings.Contains(event, string(irstatus.ErrorReasonRangeOrder)) {
				t.Fatalf("event = %q, want a Warning %s naming %s", event, EventReasonInstanceStatusDecodeFailed, irstatus.ErrorReasonRangeOrder)
			}
		default:
			t.Fatal("a codec failure must emit a Warning event")
		}
	})

	t.Run("nil recorder still fails closed", func(t *testing.T) {
		r := &Reconciler{}
		got := r.instanceStatusDecodeError(ir, codecErr)
		if reason, ok := irstatus.ErrorReasonOf(got); !ok || reason != irstatus.ErrorReasonRangeOrder {
			t.Fatalf("without a recorder the codec error must still be returned wrapped, got %v", got)
		}
	})

	t.Run("a non-codec error passes through untouched", func(t *testing.T) {
		recorder := record.NewFakeRecorder(4)
		r := &Reconciler{Recorder: recorder}
		before := irStatusMetric(t, codecErrors, reasonLabel)
		in := apierrors.NewNotFound(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferencereplicas"}, ir.Name)
		if got := r.instanceStatusDecodeError(ir, in); got != in {
			t.Fatalf("a fetch error must keep its identity, got %v", got)
		}
		if after := irStatusMetric(t, codecErrors, reasonLabel); after != before {
			t.Fatalf("a fetch error must not count as a codec failure: %v -> %v", before, after)
		}
		select {
		case event := <-recorder.Events:
			t.Fatalf("a fetch error must emit no event, got %q", event)
		default:
		}
	})
}
