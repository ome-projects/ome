package inferencereplica

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
)

// columnarTwin stores the same logical rows as ir in the ColumnarV2
// representation, exactly as a ColumnarV2-target writer would persist them.
func columnarTwin(t *testing.T, ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	t.Helper()
	twin := ir.DeepCopy()
	columns, err := irstatus.EncodeColumns(twin.Status.InstanceStatuses, uint64(len(twin.Status.InstanceStatuses)))
	if err != nil {
		t.Fatalf("encode columns: %v", err)
	}
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	twin.Status.InstanceStatuses = nil
	twin.Status.InstanceStatusEncoding = &encoding
	twin.Status.InstanceStatusColumns = columns
	return twin
}

// decodeFixtureIR is a two-replica IR with one Ready Instance on a full
// revision name and one Failed Instance carrying a failure record, the mix
// the reader-side decisions branch on.
func decodeFixtureIR() *v1beta1.InferenceReplica {
	ir := baselineIR("llama-engine", "default", 2)
	now := metav1.NewTime(metav1.Now().Truncate(1e9))
	ir.Status.Replicas = 2
	ir.Status.ReadyReplicas = 1
	ir.Status.CurrentRevision = "llama-engine-aaaaaaaa"
	ir.Status.UpdateRevision = "llama-engine-bbbbbbbb"
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "llama-engine-aaaaaaaa", PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true, ReadySince: &now},
		{Index: 1, Incarnation: 2, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: "llama-engine-aaaaaaaa", TargetRevision: "llama-engine-bbbbbbbb", PodCount: 1,
			LastFailure: &v1beta1.InstanceTermination{PodName: "llama-engine-1", ContainerName: "ome-container", Reason: "Error", Time: now}},
	}
	return ir
}

// readerDecisions collects every decision the InferenceReplica reconciler
// derives from decoded rows without performing an effect.
type readerDecisions struct {
	observed       any
	anyFailed      bool
	ready          metav1.Condition
	stalled        metav1.Condition
	liveRevisions  []string
	stagedAtPartit bool
}

func decisionsFor(t *testing.T, r *Reconciler, key client.ObjectKey) readerDecisions {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(context.Background(), r.cachedReader(), key, ir); err != nil {
		t.Fatalf("GetDecoded: %v", err)
	}
	partition := int32(1)
	ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: &partition}
	ready := computeReadyCondition(&ir.Status, ir.Spec.Replicas, ir.Spec.Lifecycle, ir.Spec.Pacing)
	stalled := computeRolloutStalledCondition(&ir.Status)
	ready.LastTransitionTime, stalled.LastTransitionTime = metav1.Time{}, metav1.Time{}
	live := revision.CollectLiveRevisionNames(ir.Status.CurrentRevision, ir.Status.UpdateRevision, observedFromIR(ir).InstanceStatuses)
	sort.Strings(live)
	return readerDecisions{
		observed:       observedFromIR(ir),
		anyFailed:      hasFailedInstance(ir.Status.InstanceStatuses),
		ready:          ready,
		stalled:        stalled,
		liveRevisions:  live,
		stagedAtPartit: stagedAtPartition(&ir.Status, ir.Spec.Pacing),
	}
}

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

func TestReconcileDenseV1IsUnchangedByTheDecoder(t *testing.T) {
	ctx := context.Background()
	ir := decodeFixtureIR()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}
	plain, cPlain := newReconciler(t, ir.DeepCopy())
	bounded, cBounded := newReconciler(t, ir.DeepCopy())
	bounded.InstanceStatusDecoder = irstatus.NewDecoder(8)

	resPlain, errPlain := plain.Reconcile(ctx, req)
	resBounded, errBounded := bounded.Reconcile(ctx, req)
	if resPlain != resBounded || (errPlain == nil) != (errBounded == nil) {
		t.Fatalf("a configured bound must not change DenseV1 reconciliation: %+v/%v vs %+v/%v", resPlain, errPlain, resBounded, errBounded)
	}
	storedPlain, storedBounded := &v1beta1.InferenceReplica{}, &v1beta1.InferenceReplica{}
	if err := cPlain.Get(ctx, req.NamespacedName, storedPlain); err != nil {
		t.Fatal(err)
	}
	if err := cBounded.Get(ctx, req.NamespacedName, storedBounded); err != nil {
		t.Fatal(err)
	}
	storedPlain.Status.Conditions, storedBounded.Status.Conditions = nil, nil
	if !equality.Semantic.DeepEqual(storedPlain.Status, storedBounded.Status) {
		t.Fatalf("published DenseV1 status differs with a configured bound:\n plain:   %+v\n bounded: %+v", storedPlain.Status, storedBounded.Status)
	}
}

// TestReconcileMetadataWriteKeepsDecodedRows pins the in-memory object after
// the entry pass's finalizer write: the API response carries the stored
// representation, so a ColumnarV2 object must be decoded again before the
// lifecycle observes it. A ColumnarV2-stored object under the ColumnarV2
// target must plan exactly what its DenseV1 twin plans; observing an empty
// row set would recreate every Instance.
func TestReconcileMetadataWriteKeepsDecodedRows(t *testing.T) {
	ctx := context.Background()
	dense := fixtureIRWithRows("llama-engine", uniformFixtureRows(64))
	if len(dense.Finalizers) != 0 {
		t.Fatal("fixture must start without the teardown finalizer")
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dense)}

	rDense, cDense := newReconciler(t, dense.DeepCopy())
	denseResult, err := rDense.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("dense reconcile: %v", err)
	}
	rColumnar, cColumnar := newColumnarReconciler(t, columnarTwin(t, dense))
	columnarResult, err := rColumnar.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("columnar reconcile: %v", err)
	}
	if columnarResult != denseResult {
		t.Fatalf("results differ by stored encoding: dense %+v, columnar %+v", denseResult, columnarResult)
	}

	rowsOf := func(r *Reconciler) ([]v1beta1.OMENativeInstanceStatus, []string) {
		ir := &v1beta1.InferenceReplica{}
		if _, err := irstatus.GetDecoded(ctx, r.cachedReader(), req.NamespacedName, ir); err != nil {
			t.Fatalf("decoded read: %v", err)
		}
		return ir.Status.InstanceStatuses, ir.Finalizers
	}
	denseRows, denseFinalizers := rowsOf(rDense)
	columnarRows, columnarFinalizers := rowsOf(rColumnar)
	if len(denseFinalizers) != 1 || !reflect.DeepEqual(denseFinalizers, columnarFinalizers) {
		t.Fatalf("finalizer write differs: dense %v, columnar %v", denseFinalizers, columnarFinalizers)
	}
	if len(columnarRows) != len(denseRows) {
		t.Fatalf("row count differs: dense %d, columnar %d", len(denseRows), len(columnarRows))
	}
	for i := range denseRows {
		want, got := denseRows[i], columnarRows[i]
		if want.Index != got.Index || want.Phase != got.Phase || (want.Operation == nil) != (got.Operation == nil) ||
			(want.Operation != nil && want.Operation.Type != got.Operation.Type) {
			t.Fatalf("lifecycle decision differs at row %d: dense phase %s op %+v, columnar phase %s op %+v", i, want.Phase, want.Operation, got.Phase, got.Operation)
		}
	}
	if got, want := len(listPods(t, cColumnar, dense.Namespace)), len(listPods(t, cDense, dense.Namespace)); got != want {
		t.Fatalf("Pod effect differs: dense %d, columnar %d", want, got)
	}
}
