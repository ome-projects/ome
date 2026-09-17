package inferencereplica

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
)

var conversionLabels = map[string]string{"from": obsmetrics.IRStatusEncodingColumnarV2, "to": obsmetrics.IRStatusEncodingDenseV1}

func conversionEvents(recorder *capturingRecorder) []capturedEvent {
	var events []capturedEvent
	for _, event := range recorder.events {
		if event.reason == EventReasonInstanceStatusConverted {
			events = append(events, event)
		}
	}
	return events
}

func listPods(t *testing.T, c client.Client, namespace string) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return pods.Items
}

// TestReconcileConvertsStoredColumnarV2ToDenseV1 pins the transition gate: an
// object stored as ColumnarV2 is rewritten as DenseV1 with identical logical
// rows and no Pod effect on that pass, and the converted object then
// reconciles exactly like its dense twin.
func TestReconcileConvertsStoredColumnarV2ToDenseV1(t *testing.T) {
	ctx := context.Background()
	dense := decodeFixtureIR()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dense)}

	rDense, cDense := newReconciler(t, dense.DeepCopy())
	rDense.InstanceStatusDecoder = irstatus.NewDecoder(8)
	denseResult, err := rDense.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("dense reconcile: %v", err)
	}
	densePods := listPods(t, cDense, dense.Namespace)

	r, c := newReconciler(t, columnarTwin(t, dense))
	r.InstanceStatusDecoder = irstatus.NewDecoder(8)
	recorder := &capturingRecorder{}
	r.Recorder = recorder
	conversionsBefore := irStatusMetric(t, "ome_omenative_ir_status_conversions_total", conversionLabels)

	result, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("conversion reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("conversion pass must requeue immediately, got %+v", result)
	}
	if pods := listPods(t, c, dense.Namespace); len(pods) != 0 {
		t.Fatalf("conversion pass must have no Pod effect, got %d Pods", len(pods))
	}
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, req.NamespacedName, stored); err != nil {
		t.Fatalf("get stored IR: %v", err)
	}
	if stored.Status.InstanceStatusEncoding != nil || stored.Status.InstanceStatusColumns != nil {
		t.Fatalf("converted object still carries the ColumnarV2 representation: %+v", stored.Status)
	}
	if !equality.Semantic.DeepEqual(stored.Status.InstanceStatuses, dense.Status.InstanceStatuses) {
		t.Fatalf("converted rows differ from the logical rows:\n got:  %+v\n want: %+v", stored.Status.InstanceStatuses, dense.Status.InstanceStatuses)
	}
	if stored.Status.Replicas != dense.Status.Replicas || stored.Status.CurrentRevision != dense.Status.CurrentRevision || stored.Status.UpdateRevision != dense.Status.UpdateRevision {
		t.Fatalf("conversion changed status outside the per-Instance representation: %+v", stored.Status)
	}
	if got := irStatusMetric(t, "ome_omenative_ir_status_conversions_total", conversionLabels) - conversionsBefore; got != 1 {
		t.Fatalf("conversions delta = %v, want 1", got)
	}
	events := conversionEvents(recorder)
	if len(events) != 1 || events[0].kind != corev1.EventTypeNormal {
		t.Fatalf("conversion must emit exactly one Normal %s event, got %+v", EventReasonInstanceStatusConverted, recorder.events)
	}
	if _, onIR := events[0].object.(*v1beta1.InferenceReplica); !onIR {
		t.Fatalf("conversion event must be recorded on the InferenceReplica, got %T", events[0].object)
	}

	// The converged object takes the fast path and reconciles like its
	// dense twin: same result, same Pod effect, no further conversion.
	nextResult, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("post-conversion reconcile: %v", err)
	}
	if nextResult != denseResult {
		t.Fatalf("post-conversion result %+v differs from the dense twin %+v", nextResult, denseResult)
	}
	if got, want := len(listPods(t, c, dense.Namespace)), len(densePods); got != want {
		t.Fatalf("post-conversion Pod effect = %d Pods, dense twin = %d", got, want)
	}
	if got := irStatusMetric(t, "ome_omenative_ir_status_conversions_total", conversionLabels) - conversionsBefore; got != 1 {
		t.Fatalf("a converged object must not convert again, conversions delta = %v", got)
	}
	if len(conversionEvents(recorder)) != 1 {
		t.Fatalf("a converged object must not emit another conversion event: %+v", recorder.events)
	}
}

// TestReconcileDenseV1PerformsNoConversionWrite pins the fast path: a
// DenseV1 object under the DenseV1 target is never rewritten for its
// representation.
func TestReconcileDenseV1PerformsNoConversionWrite(t *testing.T) {
	ctx := context.Background()
	dense := decodeFixtureIR()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dense)}
	r, _ := newReconciler(t, dense)
	r.InstanceStatusDecoder = irstatus.NewDecoder(8)
	recorder := &capturingRecorder{}
	r.Recorder = recorder
	conversionsBefore := irStatusMetric(t, "ome_omenative_ir_status_conversions_total", conversionLabels)

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := irStatusMetric(t, "ome_omenative_ir_status_conversions_total", conversionLabels) - conversionsBefore; got != 0 {
		t.Fatalf("DenseV1 object recorded %v conversions", got)
	}
	if events := conversionEvents(recorder); len(events) != 0 {
		t.Fatalf("DenseV1 object emitted conversion events: %+v", events)
	}
}

// TestConversionRetriesFromFreshReadOnConflict pins the conversion under a
// 409: the retry re-reads the live object through the decoded boundary and
// rebuilds the DenseV1 candidate from it, so a change that landed between
// the attempts survives the conversion.
func TestConversionRetriesFromFreshReadOnConflict(t *testing.T) {
	ctx := context.Background()
	dense := decodeFixtureIR()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dense)}

	var writes int
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			writes++
			if writes == 1 {
				stored := &v1beta1.InferenceReplica{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), stored); err != nil {
					return err
				}
				stored.Status.Replicas = 9
				if err := c.SubResource(sub).Update(ctx, stored); err != nil {
					return err
				}
				return apierrors.NewConflict(schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"}, obj.GetName(), errors.New("the object has been modified"))
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(columnarTwin(t, dense)).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(funcs).
		Build()
	live := &countingReader{Reader: c}
	r, _ := newReconciler(t)
	r.Client = c
	r.APIReader = live
	r.InstanceStatusDecoder = irstatus.NewDecoder(8)

	before := map[string]float64{}
	for _, result := range []string{obsmetrics.IRStatusWriteAttempt, obsmetrics.IRStatusWriteConflict, obsmetrics.IRStatusWriteCommitted} {
		before[result] = irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(result))
	}
	conversionsBefore := irStatusMetric(t, "ome_omenative_ir_status_conversions_total", conversionLabels)

	result, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("conversion reconcile: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("conversion pass must requeue, got %+v", result)
	}
	if writes != 2 || live.gets != 2 {
		t.Fatalf("writes = %d, live reads = %d; want one conflict, one fresh live read, one commit", writes, live.gets)
	}
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, req.NamespacedName, stored); err != nil {
		t.Fatalf("get stored IR: %v", err)
	}
	if stored.Status.InstanceStatusEncoding != nil || stored.Status.InstanceStatusColumns != nil || stored.Status.Replicas != 9 {
		t.Fatalf("retry must convert the fresh object and keep the concurrent change: %+v", stored.Status)
	}
	if !equality.Semantic.DeepEqual(stored.Status.InstanceStatuses, dense.Status.InstanceStatuses) {
		t.Fatalf("converted rows differ from the logical rows:\n got:  %+v\n want: %+v", stored.Status.InstanceStatuses, dense.Status.InstanceStatuses)
	}
	for result, want := range map[string]float64{obsmetrics.IRStatusWriteAttempt: 2, obsmetrics.IRStatusWriteConflict: 1, obsmetrics.IRStatusWriteCommitted: 1} {
		if got := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(result)) - before[result]; got != want {
			t.Fatalf("%s delta = %v, want %v", result, got, want)
		}
	}
	if got := irStatusMetric(t, "ome_omenative_ir_status_conversions_total", conversionLabels) - conversionsBefore; got != 1 {
		t.Fatalf("conversions delta = %v, want 1", got)
	}
}

// TestConversionSkipsObjectsDeletedOrConvergedBeforeTheLiveRead covers the
// gate's no-write outcomes: the object vanished, or another writer already
// converted it, between the entry read and the live read.
func TestConversionSkipsObjectsDeletedOrConvergedBeforeTheLiveRead(t *testing.T) {
	ctx := context.Background()
	dense := decodeFixtureIR()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dense)}

	t.Run("deleted", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(testScheme(t)).
			WithObjects(columnarTwin(t, dense)).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
		liveEmpty := fake.NewClientBuilder().WithScheme(testScheme(t)).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
		r, _ := newReconciler(t)
		r.Client, r.APIReader = cached, liveEmpty
		r.InstanceStatusDecoder = irstatus.NewDecoder(8)
		result, err := r.Reconcile(ctx, req)
		if err != nil || result != (ctrl.Result{}) {
			t.Fatalf("a deleted object must end the pass quietly, got %+v / %v", result, err)
		}
	})

	t.Run("already converged", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(testScheme(t)).
			WithObjects(columnarTwin(t, dense)).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
		liveDense := fake.NewClientBuilder().WithScheme(testScheme(t)).
			WithObjects(dense.DeepCopy()).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
		r, _ := newReconciler(t)
		r.Client, r.APIReader = cached, liveDense
		r.InstanceStatusDecoder = irstatus.NewDecoder(8)
		conversionsBefore := irStatusMetric(t, "ome_omenative_ir_status_conversions_total", conversionLabels)
		result, err := r.Reconcile(ctx, req)
		if err != nil || !result.Requeue {
			t.Fatalf("a converged live object must requeue without writing, got %+v / %v", result, err)
		}
		if got := irStatusMetric(t, "ome_omenative_ir_status_conversions_total", conversionLabels) - conversionsBefore; got != 0 {
			t.Fatalf("no conversion may be counted for a converged object, delta = %v", got)
		}
	})
}

// TestConversionWriterRefusesDenseV1Source pins the defensive invariants of
// the two writer entry points: a conversion of a DenseV1 source and a logical
// mutation of a ColumnarV2 source are both refused before any request.
func TestConversionWriterRefusesDenseV1Source(t *testing.T) {
	ctx := context.Background()
	dense := decodeFixtureIR()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(dense).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	key := client.ObjectKeyFromObject(dense)
	before := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, before); err != nil {
		t.Fatalf("get IR: %v", err)
	}

	if err := convertInferenceReplicaStatus(ctx, testStatusWriter(c), before.DeepCopy(), irstatus.EncodingDenseV1); !errors.Is(err, ErrDenseV1StatusConversion) {
		t.Fatalf("conversion of a DenseV1 source must be refused, got %v", err)
	}
	if err := updateInferenceReplicaStatus(ctx, testStatusWriter(c), before.DeepCopy(), irstatus.EncodingColumnarV2); !errors.Is(err, ErrColumnarV2StatusWrite) {
		t.Fatalf("logical mutation of a ColumnarV2 source must be refused, got %v", err)
	}
	after := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, after); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if after.ResourceVersion != before.ResourceVersion {
		t.Fatal("a refused write must not reach the API server")
	}
}
