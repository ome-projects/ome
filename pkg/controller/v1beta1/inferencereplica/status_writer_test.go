package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
)

// capturingRecorder records every event with the object it was attached to.
type capturingRecorder struct {
	events []capturedEvent
}

type capturedEvent struct {
	object  k8sruntime.Object
	kind    string
	reason  string
	message string
}

func (r *capturingRecorder) Event(object k8sruntime.Object, eventtype, reason, message string) {
	r.events = append(r.events, capturedEvent{object: object, kind: eventtype, reason: reason, message: message})
}

func (r *capturingRecorder) Eventf(object k8sruntime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	r.Event(object, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (r *capturingRecorder) AnnotatedEventf(object k8sruntime.Object, _ map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	r.Eventf(object, eventtype, reason, messageFmt, args...)
}

// irStatusMetric reads one sample of a write-boundary metric from the
// controller-runtime registry; zero when the series does not exist.
func irStatusMetric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if !metricLabelsMatch(metric, labels) {
				continue
			}
			if metric.Counter != nil {
				return metric.Counter.GetValue()
			}
			if metric.Gauge != nil {
				return metric.Gauge.GetValue()
			}
		}
	}
	return 0
}

func writeResultLabels(result string) map[string]string {
	return map[string]string{"encoding": obsmetrics.IRStatusEncodingDenseV1, "result": result}
}

var transientInstanceObservationFields = map[string]struct{}{
	"ReadyPodCount":     {},
	"ScheduledPodCount": {},
	"NodesOccupied":     {},
}

// testStatusWriter is the persistence boundary over a bare fake client under
// the DenseV1 target: no row bound (every object under test is DenseV1) and
// no recorder.
func testStatusWriter(c client.Client) statusWriter {
	return statusWriter{Client: c, target: irstatus.EncodingDenseV1}
}

// columnarStatusWriter is the persistence boundary under the ColumnarV2
// target with a row bound large enough for every fixture in this package.
func columnarStatusWriter(c client.Client) statusWriter {
	return statusWriter{Client: c, decoder: irstatus.NewDecoder(testColumnarBound), target: irstatus.EncodingColumnarV2}
}

// testColumnarBound is the ColumnarV2 decode bound the ColumnarV2-target
// fixtures in this package run under; it exceeds the largest fixture row
// count so the bound never decides a selection here.
const testColumnarBound = 8192

func TestClearPodDerivedInstanceObservations(t *testing.T) {
	original := populatedInstanceStatus()
	assertEveryRetainedInstanceFieldPopulated(t, original)

	ir := &v1beta1.InferenceReplica{
		Status: v1beta1.InferenceReplicaStatus{
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{original},
		},
	}
	want := original.DeepCopy()
	want.ReadyPodCount = 0
	want.ScheduledPodCount = 0
	want.NodesOccupied = nil

	irstatus.ClearPodDerivedObservations(ir.Status.InstanceStatuses)
	if got := ir.Status.InstanceStatuses[0]; !reflect.DeepEqual(got, *want) {
		t.Fatalf("cleared status differs outside the three transient fields:\n got: %#v\nwant: %#v", got, *want)
	}

	once := ir.DeepCopy()
	irstatus.ClearPodDerivedObservations(ir.Status.InstanceStatuses)
	if !reflect.DeepEqual(ir, once) {
		t.Fatalf("second clear changed status:\n got: %#v\nwant: %#v", ir.Status, once.Status)
	}
}

func TestUpdateInferenceReplicaStatusPersistsCompactedStatus(t *testing.T) {
	ctx := context.Background()
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "model-engine", Namespace: "default"},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: 3,
			Replicas:           1,
			InstanceStatuses:   []v1beta1.OMENativeInstanceStatus{populatedInstanceStatus()},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()

	live := &v1beta1.InferenceReplica{}
	key := client.ObjectKeyFromObject(ir)
	if err := c.Get(ctx, key, live); err != nil {
		t.Fatalf("get InferenceReplica: %v", err)
	}
	want := live.DeepCopy()
	want.Status.InstanceStatuses[0].ReadyPodCount = 0
	want.Status.InstanceStatuses[0].ScheduledPodCount = 0
	want.Status.InstanceStatuses[0].NodesOccupied = nil

	if err := updateInferenceReplicaStatus(ctx, testStatusWriter(c), live, irstatus.EncodingDenseV1); err != nil {
		t.Fatalf("update status: %v", err)
	}
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, stored); err != nil {
		t.Fatalf("get persisted InferenceReplica: %v", err)
	}
	if !reflect.DeepEqual(stored.Status, want.Status) {
		t.Fatalf("persisted status differs outside the three transient fields:\n got: %#v\nwant: %#v", stored.Status, want.Status)
	}
}

// TestUpdateInferenceReplicaStatusRefusesColumnarV2 pins the DenseV1-target
// boundary: an object read from ColumnarV2, or a write copy still carrying
// the ColumnarV2 marker, is refused with a typed error and nothing reaches
// the API server.
func TestUpdateInferenceReplicaStatusRefusesColumnarV2(t *testing.T) {
	ctx := context.Background()
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "model-engine", Namespace: "default"},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas:         1,
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{populatedInstanceStatus()},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()
	key := client.ObjectKeyFromObject(ir)
	before := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, before); err != nil {
		t.Fatalf("get InferenceReplica: %v", err)
	}

	decoded := before.DeepCopy()
	decoded.Status.Replicas = 2
	err := updateInferenceReplicaStatus(ctx, testStatusWriter(c), decoded, irstatus.EncodingColumnarV2)
	if !errors.Is(err, ErrColumnarV2StatusWrite) {
		t.Fatalf("ColumnarV2 source must be refused with ErrColumnarV2StatusWrite, got %v", err)
	}
	if !strings.Contains(err.Error(), "default/model-engine") || !strings.Contains(err.Error(), `target "DenseV1"`) {
		t.Fatalf("refusal must name the object and the configured target: %v", err)
	}

	marked := before.DeepCopy()
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	marked.Status.InstanceStatusEncoding = &encoding
	marked.Status.InstanceStatusColumns = &v1beta1.InstanceStatusColumns{Members: "0"}
	marked.Status.InstanceStatuses = nil
	if err := updateInferenceReplicaStatus(ctx, testStatusWriter(c), marked, irstatus.EncodingDenseV1); !errors.Is(err, ErrColumnarV2StatusWrite) {
		t.Fatalf("an object carrying the ColumnarV2 marker must be refused, got %v", err)
	}

	after := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, after); err != nil {
		t.Fatalf("get InferenceReplica: %v", err)
	}
	if !reflect.DeepEqual(after.Status, before.Status) || after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("a refused write must not reach the API server:\n before: %#v\n after: %#v", before.Status, after.Status)
	}
}

// TestUpdateInferenceReplicaStatusDecodesCommittedObject pins the post-write
// boundary: after a successful update the caller's object is the decoded
// API-confirmed object, so mirrors and OnCommit callbacks observe exactly
// what a fresh decoded read returns, and the bytes gauge carries the size of
// the status that was written.
func TestUpdateInferenceReplicaStatusDecodesCommittedObject(t *testing.T) {
	ctx := context.Background()
	ir := baselineIR("model-engine", "default", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{populatedInstanceStatus()}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()
	key := client.ObjectKeyFromObject(ir)
	live := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(ctx, c, key, live); err != nil {
		t.Fatalf("get InferenceReplica: %v", err)
	}
	live.Status.Replicas = 3

	attemptsBefore := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(obsmetrics.IRStatusWriteAttempt))
	committedBefore := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(obsmetrics.IRStatusWriteCommitted))
	if err := updateInferenceReplicaStatus(ctx, testStatusWriter(c), live, irstatus.EncodingDenseV1); err != nil {
		t.Fatalf("update status: %v", err)
	}

	fresh := &v1beta1.InferenceReplica{}
	source, err := irstatus.GetDecoded(ctx, c, key, fresh)
	if err != nil || source != irstatus.EncodingDenseV1 {
		t.Fatalf("re-read: source %q err %v", source, err)
	}
	if !reflect.DeepEqual(live.Status, fresh.Status) || live.ResourceVersion != fresh.ResourceVersion {
		t.Fatalf("object after the write differs from a fresh decoded read:\n got: %#v (rv %s)\nwant: %#v (rv %s)", live.Status, live.ResourceVersion, fresh.Status, fresh.ResourceVersion)
	}
	if live.Status.Replicas != 3 || live.Status.InstanceStatuses[0].ReadyPodCount != 0 || live.Status.InstanceStatuses[0].NodesOccupied != nil {
		t.Fatalf("committed status must carry the mutation and no Pod-derived fields: %#v", live.Status)
	}

	written, err := json.Marshal(&fresh.Status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", map[string]string{
		"namespace": "default", "name": "model-engine", "component": string(v1beta1.EngineComponent), "encoding": obsmetrics.IRStatusEncodingDenseV1,
	})
	if int(gauge) != len(written) {
		t.Fatalf("status bytes gauge = %v, want %d (the serialized dense status)", gauge, len(written))
	}
	if got := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(obsmetrics.IRStatusWriteAttempt)) - attemptsBefore; got != 1 {
		t.Fatalf("attempt delta = %v, want 1", got)
	}
	if got := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(obsmetrics.IRStatusWriteCommitted)) - committedBefore; got != 1 {
		t.Fatalf("committed delta = %v, want 1", got)
	}
}

// TestStatusWriteRetryRebuildsCandidateFromFreshRead pins the conflict path:
// the first update is rejected with a 409 after an out-of-band change landed,
// and the retry must re-read the live object, decode it, and rebuild the
// write candidate from it, so the commit carries both the out-of-band change
// and the intended mutation.
func TestStatusWriteRetryRebuildsCandidateFromFreshRead(t *testing.T) {
	ctx := context.Background()
	ir := baselineIR("model-engine", "default", 1)
	row := populatedInstanceStatus()
	row.Index = 0
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{row}

	var writes int
	var attemptedReplicas []int32
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			assertCompactInferenceReplicaWrite(t, obj)
			writes++
			attemptedReplicas = append(attemptedReplicas, obj.(*v1beta1.InferenceReplica).Status.Replicas)
			if writes == 1 {
				// A concurrent writer lands first; the attempt loses on a
				// stale resourceVersion.
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
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(funcs).
		Build()
	live := &countingReader{Reader: c}

	before := map[string]float64{}
	for _, result := range []string{obsmetrics.IRStatusWriteAttempt, obsmetrics.IRStatusWriteConflict, obsmetrics.IRStatusWriteCommitted} {
		before[result] = irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(result))
	}

	err := buildWriteAggregateCondition(testStatusWriter(c), irstatus.NewReader(live, irstatus.Decoder{}), ir)(ctx, metav1.Condition{
		Type: InferenceReplicaConditionReady, Status: metav1.ConditionTrue, Reason: ReasonAllInstancesReady, Message: "ready",
	})
	if err != nil {
		t.Fatalf("write aggregate condition: %v", err)
	}
	if writes != 2 || live.gets != 2 {
		t.Fatalf("writes = %d, live reads = %d; want one conflict, one fresh read, one commit", writes, live.gets)
	}
	if !reflect.DeepEqual(attemptedReplicas, []int32{0, 9}) {
		t.Fatalf("attempted replicas = %v; the retry must rebuild its candidate from the fresh read", attemptedReplicas)
	}

	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get stored IR: %v", err)
	}
	if stored.Status.Replicas != 9 || len(stored.Status.Conditions) != 1 || stored.Status.Conditions[0].Type != InferenceReplicaConditionReady {
		t.Fatalf("committed status lost the out-of-band change or the mutation: %#v", stored.Status)
	}
	if stored.Status.InstanceStatuses[0].ReadyPodCount != 0 || stored.Status.InstanceStatuses[0].ScheduledPodCount != 0 || stored.Status.InstanceStatuses[0].NodesOccupied != nil {
		t.Fatalf("retry candidate persisted Pod-derived fields: %#v", stored.Status.InstanceStatuses[0])
	}
	for result, want := range map[string]float64{obsmetrics.IRStatusWriteAttempt: 2, obsmetrics.IRStatusWriteConflict: 1, obsmetrics.IRStatusWriteCommitted: 1} {
		if got := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(result)) - before[result]; got != want {
			t.Fatalf("%s delta = %v, want %v", result, got, want)
		}
	}
}

// TestUpdateInferenceReplicaStatusSizeRejection pins the too-large path: the
// write is recorded as rejected, a Warning names the encoded size on the
// parent InferenceService (or the IR when no parent resolves), the error is
// returned unchanged, and nothing is dropped or compacted.
func TestUpdateInferenceReplicaStatusSizeRejection(t *testing.T) {
	tooLarge := apierrors.NewRequestEntityTooLargeError("limit is 1572864")
	internalTooLarge := apierrors.NewInternalError(errors.New("etcdserver: request is too large"))
	unavailable := apierrors.NewServiceUnavailable("apiserver restarting")

	cases := map[string]struct {
		injected   error
		withParent bool
		wantResult string
		wantTarget string
	}{
		"413 with parent":               {injected: tooLarge, withParent: true, wantResult: obsmetrics.IRStatusWriteRejected, wantTarget: "InferenceService"},
		"413 without parent":            {injected: tooLarge, withParent: false, wantResult: obsmetrics.IRStatusWriteRejected, wantTarget: "InferenceReplica"},
		"storage too-large as Internal": {injected: internalTooLarge, withParent: true, wantResult: obsmetrics.IRStatusWriteRejected, wantTarget: "InferenceService"},
		"other error":                   {injected: unavailable, withParent: true, wantResult: obsmetrics.IRStatusWriteError, wantTarget: ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ir := baselineIR("model-engine", "default", 1)
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{populatedInstanceStatus()}
			objs := []client.Object{ir}
			if tc.withParent {
				objs = append(objs, &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: ir.Spec.ParentRef.Name, Namespace: ir.Namespace}})
			}
			c := fake.NewClientBuilder().
				WithScheme(testScheme(t)).
				WithObjects(objs...).
				WithStatusSubresource(&v1beta1.InferenceReplica{}).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
						return tc.injected
					},
				}).
				Build()
			key := client.ObjectKeyFromObject(ir)
			before := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, key, before); err != nil {
				t.Fatalf("get InferenceReplica: %v", err)
			}
			live := before.DeepCopy()
			live.Status.Replicas = 2
			recorder := &capturingRecorder{}
			writer := statusWriter{Client: c, target: irstatus.EncodingDenseV1, recorder: recorder}
			resultBefore := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(tc.wantResult))

			err := updateInferenceReplicaStatus(ctx, writer, live, irstatus.EncodingDenseV1)
			if !errors.Is(err, tc.injected) {
				t.Fatalf("error must be returned unchanged, got %v", err)
			}
			if got := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeResultLabels(tc.wantResult)) - resultBefore; got != 1 {
				t.Fatalf("%s delta = %v, want 1", tc.wantResult, got)
			}
			after := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, key, after); err != nil {
				t.Fatalf("get InferenceReplica: %v", err)
			}
			if !reflect.DeepEqual(after.Status, before.Status) {
				t.Fatalf("a rejected write must leave the stored status unchanged")
			}
			if tc.wantTarget == "" {
				if len(recorder.events) != 0 {
					t.Fatalf("no event expected for a non-size failure, got %+v", recorder.events)
				}
				return
			}
			if len(recorder.events) != 1 {
				t.Fatalf("events = %+v, want exactly one size Warning", recorder.events)
			}
			event := recorder.events[0]
			var gotTarget string
			switch event.object.(type) {
			case *v1beta1.InferenceService:
				gotTarget = "InferenceService"
			case *v1beta1.InferenceReplica:
				gotTarget = "InferenceReplica"
			}
			if event.kind != corev1.EventTypeWarning || event.reason != EventReasonStatusSizeExceeded || gotTarget != tc.wantTarget {
				t.Fatalf("event = %+v, want a %s Warning on the %s", event, EventReasonStatusSizeExceeded, tc.wantTarget)
			}
			written, err := json.Marshal(&live.Status)
			if err != nil {
				t.Fatalf("marshal status: %v", err)
			}
			if !strings.Contains(event.message, fmt.Sprintf("%d bytes", len(written))) || !strings.Contains(event.message, "default/model-engine") {
				t.Fatalf("event message must name the object and the encoded size %d: %q", len(written), event.message)
			}
		})
	}
}

func TestInferenceReplicaStatusUpdatesUseWriterBoundary(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	dir := filepath.Dir(thisFile)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	// Three typed entry points share one raw write: logical mutations enter
	// through the mutation writer, the transition gate through the
	// conversion writer, the operator-side break-glass repair through the
	// repair entry, and only the persist function touches Status().
	const (
		writer     = "updateInferenceReplicaStatus"
		conversion = "convertInferenceReplicaStatus"
		repair     = "RepairInstanceStatus"
		persist    = "persistInferenceReplicaStatus"
	)
	// persistCallers is the approval list of functions that may call the
	// single raw write; the repair entry is the one classified break-glass
	// writer, and it is registered with no reconciler.
	persistCallers := map[string]string{
		writer:     "status_writer.go",
		conversion: "status_writer.go",
		repair:     "status_repair.go",
	}
	var writerCalls, conversionCalls, statusAccesses, statusSubresources, rawUpdates []string
	persistCalls := map[string][]string{}
	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", entry.Name(), parseErr)
		}
		for _, decl := range file.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, isCall := node.(*ast.CallExpr)
				if !isCall {
					return true
				}
				site := fmt.Sprintf("%s:%d (%s)", entry.Name(), fset.Position(call.Pos()).Line, fn.Name.Name)
				if ident, isIdent := call.Fun.(*ast.Ident); isIdent {
					switch ident.Name {
					case writer:
						writerCalls = append(writerCalls, site)
					case conversion:
						conversionCalls = append(conversionCalls, site)
					case persist:
						persistCalls[fn.Name.Name] = append(persistCalls[fn.Name.Name], entry.Name())
					}
				}
				if selectorCallNamed(call, "Status") {
					statusAccesses = append(statusAccesses, site)
				}
				if statusSubresourceCall(call) {
					statusSubresources = append(statusSubresources, site)
				}
				if rawStatusUpdate(call) {
					rawUpdates = append(rawUpdates, site)
				}
				return true
			})
		}
	}

	if len(writerCalls) != 12 {
		t.Fatalf("production status writes through %s = %d, want 12: %v", writer, len(writerCalls), writerCalls)
	}
	if len(conversionCalls) != 1 || !strings.Contains(conversionCalls[0], "status_transition.go") {
		t.Fatalf("conversion writes through %s must come from the transition gate alone: %v", conversion, conversionCalls)
	}
	for caller, file := range persistCallers {
		if files := persistCalls[caller]; len(files) != 1 || files[0] != file {
			t.Fatalf("%s must call %s exactly once from %s, got %v", caller, persist, file, files)
		}
	}
	for caller, files := range persistCalls {
		if _, approved := persistCallers[caller]; !approved {
			t.Fatalf("%s calls %s from %v; only the approved entry points may reach the single writer: %v", caller, persist, files, persistCallers)
		}
	}
	if len(statusAccesses) != 1 || !strings.Contains(statusAccesses[0], "status_writer.go") || !strings.Contains(statusAccesses[0], "("+persist+")") {
		t.Fatalf("raw Status() access must exist only in %s: %v", persist, statusAccesses)
	}
	if len(rawUpdates) != 1 || rawUpdates[0] != statusAccesses[0] {
		t.Fatalf("raw Status().Update must exist only in %s: %v", persist, rawUpdates)
	}
	if len(statusSubresources) != 0 {
		t.Fatalf("SubResource(\"status\") bypasses %s: %v", persist, statusSubresources)
	}

	// The package-local checks above are syntactic; the repo-wide sweep is
	// type-aware and covers every client shape.
	assertInferenceReplicaStatusWritesUseSingleWriter(t)
}

func selectorCallNamed(call *ast.CallExpr, name string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == name
}

func rawStatusUpdate(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Update" {
		return false
	}
	statusCall, ok := selector.X.(*ast.CallExpr)
	return ok && selectorCallNamed(statusCall, "Status")
}

func statusSubresourceCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "SubResource" || len(call.Args) != 1 {
		return false
	}
	literal, ok := call.Args[0].(*ast.BasicLit)
	return ok && literal.Kind == token.STRING && literal.Value == `"status"`
}

func assertEveryRetainedInstanceFieldPopulated(t *testing.T, status v1beta1.OMENativeInstanceStatus) {
	t.Helper()
	typeOfStatus := reflect.TypeOf(status)
	valueOfStatus := reflect.ValueOf(status)
	for i := 0; i < typeOfStatus.NumField(); i++ {
		field := typeOfStatus.Field(i)
		if _, transient := transientInstanceObservationFields[field.Name]; transient {
			continue
		}
		if valueOfStatus.Field(i).IsZero() {
			t.Fatalf("retained fixture field %s must be populated", field.Name)
		}
	}
}

func populatedInstanceStatus() v1beta1.OMENativeInstanceStatus {
	now := metav1.NewTime(time.Date(2026, time.August, 17, 12, 0, 0, 0, time.UTC))
	surgeIndex := int32(9)
	exitCode := int32(137)
	return v1beta1.OMENativeInstanceStatus{
		Index:             7,
		Incarnation:       2,
		Phase:             v1beta1.OMENativeInstanceUpdating,
		RunningRevision:   "revision-a",
		TargetRevision:    "revision-b",
		PodCount:          8,
		ReadyPodCount:     7,
		ServingPodCount:   6,
		AvailablePodCount: 5,
		ScheduledPodCount: 8,
		Admitted:          true,
		NodesOccupied:     []string{"node-a", "node-b"},
		Conditions:        []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 3, LastTransitionTime: now, Reason: "PodsReady", Message: "all pods are ready"}},
		ActiveOrdinal:     1,
		ReadySince:        &now,
		Operation:         &v1beta1.InstanceOperation{ID: "operation-a", Type: v1beta1.InstanceOperationMigrate, Step: "WaitReady", StartedAt: now, LastProgressAt: now, Deadline: now, RetryCount: 1, TargetRevision: "revision-b", Reason: "placement", SurgeIndex: &surgeIndex, FromNode: "node-a", HintTargetNodes: []string{"node-b"}, RequestUUID: "request-a"},
		LastFailure:       &v1beta1.InstanceTermination{PodName: "model-engine-7", ContainerName: "server", Reason: "OOMKilled", ExitCode: &exitCode, Message: "container terminated", Time: now},
	}
}
