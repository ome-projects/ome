package inferencereplica

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/tools/go/packages"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

var transientInstanceObservationFields = map[string]struct{}{
	"ReadyPodCount":     {},
	"ScheduledPodCount": {},
	"NodesOccupied":     {},
}

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
		Announced:         []string{"GangSplitRisk@operation-a"},
	}
}

// The executable consumer inventory for InferenceReplica per-Instance status.
//
// Three type-aware sweeps over every production package (pkg, cmd, internal,
// scheduler; generated files and tests excluded) pin the per-Instance status
// reader and writer boundaries:
//
//   - field reads: every use of InferenceReplicaStatus.InstanceStatuses,
//     InstanceStatusColumns, and InstanceStatusEncoding outside the codec
//     package must be listed below with the classification that makes it
//     safe;
//   - fetch sites: every Get or List of an InferenceReplica must be the
//     decoded accessor or a listed pass-through that inspects no rows;
//   - status writes: every InferenceReplica status write must be the single
//     writer.
//
// The field-read sweep also covers every file under tests/, test files
// included: integration specs read rows only through the shared helper that
// decodes either stored representation, so a spec cannot silently observe an
// empty dense list on a ColumnarV2 object. The qualification suites that
// inspect the stored representation on purpose are classified as such.
//
// A new site anywhere fails until it is classified here; a stale entry fails
// so the table never drifts from the code. An import-graph check keeps the
// shared test-fixture package out of every production package, which is what
// makes its classification below true.

const (
	omeAPIPackagePath  = "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	codecPackagePath   = "sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	fixturePackagePath = codecPackagePath + "/irstatustest"
	decodedAccessor    = "GetDecoded"
	singleWriter       = "persistInferenceReplicaStatus"

	projectorPackagePath               = "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	controllerRuntimeClientPackagePath = "sigs.k8s.io/controller-runtime/pkg/client"
)

var representationFields = map[string]struct{}{
	"InstanceStatuses":       {},
	"InstanceStatusColumns":  {},
	"InstanceStatusEncoding": {},
}

// Classifications name why a site outside the boundary is allowed.
const (
	// decodedObjectRows: the function consumes rows of an object that the
	// decoded accessor rewrote into the dense logical shape before the
	// function ran, or that the single writer decoded after its commit.
	decodedObjectRows = "logical rows of an object decoded at the boundary"
	// inMemoryMirror: the function copies committed rows onto the decoded
	// in-memory object so later work in the same pass observes them.
	inMemoryMirror = "in-memory mirror of committed rows onto the decoded object"
	// passThroughSpecMetadata: the fetch serves a spec- or metadata-only
	// path and never inspects the per-Instance representation.
	passThroughSpecMetadata = "pass-through: spec/metadata only"
	// passThroughTopLevelStatus: the fetch serves a reader of top-level
	// status fields only (revisions, counters, conditions, traffic).
	passThroughTopLevelStatus = "pass-through: top-level status only"
	// decodedBoundary: the fetch is the decoded accessor itself.
	decodedBoundary = "decoded-accessor boundary"
	// administrativeCensus: the operator preflight's paginated list; every
	// object is classified through the codec (ObservedEncoding, DecodeStatus)
	// and no row is consumed by a decision.
	administrativeCensus = "administrative paginated census classified through the codec"
	// testFixtureBuilder: the function builds logical statuses for the codec
	// and qualification suites to measure; its package is test support that
	// no production package imports (TestIRStatusFixturePackageImportInventory).
	testFixtureBuilder = "test-fixture builder constructing logical statuses; imported only by tests"
	// breakGlassRepair: the operator-side repair installs an independently
	// validated replacement on a copy of the raw live object and writes it
	// through the single writer; the stored payload is never decoded.
	breakGlassRepair = "break-glass repair through the single writer"
	// breakGlassRepairRead: the repair's raw live read supplies the
	// resourceVersion precondition and the status outside the per-Instance
	// representation; the stored payload is replaced, not consumed.
	breakGlassRepairRead = "pass-through: break-glass repair live read (resourceVersion and unrelated status)"
	// rawReaderOutsideManager: a reader outside the manager (the kubectl-ome
	// CLI) fetches the object raw and consumes the stored dense rows
	// directly, so a ColumnarV2 object presents no rows to it until it reads
	// through the decoded accessor.
	rawReaderOutsideManager = "raw reader outside the manager: stored dense rows only; a ColumnarV2 object presents no rows"
)

type accessCounts struct {
	reads      int
	writes     int
	readWrites int
}

type fieldUse struct {
	file     string
	function string
	field    string
}

type approvedFieldUse struct {
	counts accessCounts
	reason string
}

type fetchSite struct {
	file     string
	function string
	method   string
}

type approvedFetch struct {
	count  int
	reason string
}

func TestInferenceReplicaStatusReadInventory(t *testing.T) {
	approved := map[fieldUse]approvedFieldUse{}
	approve := func(file, function string, counts accessCounts, reason string, fields ...string) {
		for _, field := range fields {
			approved[fieldUse{file: file, function: function, field: field}] = approvedFieldUse{counts: counts, reason: reason}
		}
	}
	read := func(n int) accessCounts { return accessCounts{reads: n} }
	rows := "InstanceStatuses"

	// InferenceReplica reconciler: every function below runs on an object
	// fetched through irstatus.GetDecoded (see the fetch inventory). Counts
	// are reads / writes / read-writes of the field selector.
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "observedFromIR", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "buildMutateInstance", accessCounts{reads: 4, writes: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "buildApplyInstanceMutationsWithRetryBlockFromReader", accessCounts{reads: 7, writes: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "instanceMutationPostconditionsHold", read(3), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "replaceInstanceStatuses", accessCounts{writes: 1}, inMemoryMirror, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "mirrorInstanceStatuses", accessCounts{reads: 4, writes: 1, readWrites: 2}, inMemoryMirror, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "buildPromoteCurrentRevision", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "buildRemoveInstance", accessCounts{reads: 1, writes: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "Reconciler.aggregateAndWriteStatus", accessCounts{reads: 3, readWrites: 7}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "Reconciler.reconcileHeldDeadlines", accessCounts{reads: 3, readWrites: 2}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "mirrorInstanceCounters", accessCounts{reads: 1, readWrites: 1}, inMemoryMirror, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "stagedAtPartition", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "computeRolloutStalledCondition", read(2), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "computeDrainOverdueCondition", read(2), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "computeReadyCondition", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/reconciler.go", "Reconciler.Reconcile", read(2), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/reconciler.go", "Reconciler.reconcileRelocationDirectives", accessCounts{reads: 3, readWrites: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/reconciler.go", "Reconciler.stampAutoRelocationSuccess", accessCounts{reads: 1, readWrites: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/reset_instances.go", "Reconciler.resetInstances", accessCounts{reads: 2, readWrites: 2}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/retention.go", "Reconciler.sweepRevisions", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status_transition.go", "Reconciler.convertStoredRepresentation", read(1), decodedObjectRows, rows)

	// Break-glass repair: the only writer entry point registered with no
	// reconciler. It installs validated replacement rows on a deep copy of
	// the raw live object and clears the marker and columns on that copy.
	approve("pkg/controller/v1beta1/inferencereplica/status_repair.go", "RepairInstanceStatus", accessCounts{writes: 1}, breakGlassRepair, rows, "InstanceStatusEncoding", "InstanceStatusColumns")
	approve("pkg/controller/v1beta1/inferencereplica/status_repair.go", "validateRepairReplacement", accessCounts{writes: 1}, breakGlassRepair, rows)

	// Remote readers: coordination and placement consume the object that
	// irprojector.DecodedComponentIR(Status) returned.
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/pairing.go", "GateContext.CheckPairing", accessCounts{reads: 1, readWrites: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/ratio.go", "GateContext.CheckRatio", read(3), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/ratio.go", "GateContext.CheckSurge", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/sequential_gate.go", "observeSequentialComponentsForGate", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/reconciler.go", "buildComponentObservation", read(2), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/placement/admission.go", "admittedReplicaCount", read(2), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/placement/admission.go", "componentHasAdmittedInstance", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/placement/failed.go", "IsTerminallyFailed", read(1), decodedObjectRows, rows)

	// The kubectl-ome CLI still reads stored dense rows directly. Alfred has
	// no direct representation-field readers: its bounded adapter calls the
	// shared codec and returns independent logical rows without changing the
	// raw captured object.
	approve("pkg/cli/instancecollection/collect.go", "CollectRelated", read(2), rawReaderOutsideManager, rows)
	approve("pkg/cli/instancecollection/collect.go", "boundedReplicaCopy", accessCounts{reads: 2, writes: 1, readWrites: 2}, rawReaderOutsideManager, rows)
	approve("pkg/cli/instanceprojection/project.go", "Project", read(8), rawReaderOutsideManager, rows)
	approve("pkg/cli/instanceprojection/project.go", "validAggregateStatus", read(2), rawReaderOutsideManager, rows)
	approve("pkg/cli/instancestatusprojection/project.go", "EventTargets", accessCounts{reads: 2, readWrites: 1}, rawReaderOutsideManager, rows)
	approve("pkg/cli/instancestatusprojection/project.go", "findRawRow", accessCounts{reads: 2, readWrites: 1}, rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/held_release_source.go", "validateHeldReplicaIdentity", read(1), rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/migration_evidence.go", "CollectMigrationEvidence", accessCounts{reads: 2, readWrites: 1}, rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/replicas.go", "inspectReplica", read(2), rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/replicas.go", "replicaPayloadBounded", read(1), rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/scale_evidence.go", "scaleLifecycleWork", read(1), rawReaderOutsideManager, rows)

	// Shared test fixtures: the builder writes dense rows into a status it
	// constructs; nothing outside tests links it.
	approve("pkg/controller/v1beta1/irstatus/irstatustest/fixtures.go", "LogicalStatus", accessCounts{writes: 1}, testFixtureBuilder, rows)

	inv := loadStatusInventory(t)
	actual := map[fieldUse]accessCounts{}
	inv.eachProductionFile(func(pkg *packages.Package, file *ast.File, relative string) {
		if pkg.PkgPath == codecPackagePath {
			return
		}
		collectRepresentationFieldUses(pkg, file, relative, actual)
	})
	loadTestInventory(t).eachTestFile(func(pkg *packages.Package, file *ast.File, relative string) {
		collectRepresentationFieldUses(pkg, file, relative, actual)
	})

	for use, got := range actual {
		want, ok := approved[use]
		if !ok {
			t.Errorf("unclassified read of %s in %s:%s: %+v", use.field, use.file, use.function, got)
			continue
		}
		if got != want.counts {
			t.Errorf("access count changed for %s in %s:%s: got %+v, want %+v (%s)", use.field, use.file, use.function, got, want.counts, want.reason)
		}
	}
	for use, want := range approved {
		if _, ok := actual[use]; !ok {
			t.Errorf("stale approval for %s in %s:%s (%s)", use.field, use.file, use.function, want.reason)
		}
	}
}

func TestInferenceReplicaFetchInventory(t *testing.T) {
	approved := map[fetchSite]approvedFetch{}
	approve := func(file, function, method string, count int, reason string) {
		approved[fetchSite{file: file, function: function, method: method}] = approvedFetch{count: count, reason: reason}
	}

	approve("pkg/controller/v1beta1/irstatus/reader.go", decodedAccessor, "Get", 1, decodedBoundary)
	approve("pkg/controller/v1beta1/irstatus/transitionpreflight/preflight.go", "checkReplicas", "List", 1, administrativeCensus)
	approve("pkg/controller/v1beta1/irstatus/statusrepair/repair.go", "Run", "Get", 1, breakGlassRepairRead)

	// InferenceReplica reconciler pass-through reads.
	approve("pkg/controller/v1beta1/inferencereplica/reconciler.go", "Reconciler.Reconcile", "Get", 1, passThroughSpecMetadata+" (finalizer add rejected: re-read DeletionTimestamp)")
	approve("pkg/controller/v1beta1/inferencereplica/teardown.go", "Reconciler.removeTeardownFinalizer", "Get", 1, passThroughSpecMetadata+" (finalizer removal)")
	approve("pkg/controller/v1beta1/inferencereplica/release_held.go", "Reconciler.consumeReleaseHeldRequest", "Get", 1, passThroughSpecMetadata+" (annotation consumption)")
	approve("pkg/controller/v1beta1/inferencereplica/reset_instances.go", "Reconciler.consumeResetInstancesRequest", "Get", 1, passThroughSpecMetadata+" (annotation consumption)")

	// Replay harness pass-through reads.
	approve("pkg/controller/v1beta1/workload/replay/driver.go", "driver.bumpGeneration", "Get", 1, passThroughSpecMetadata+" (replay owner generation bump)")

	// ISVC-side raw accessors and their callers.
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector/irstatus_read.go", "ComponentIR", "Get", 1, passThroughTopLevelStatus+" (raw accessor for callers that inspect no rows)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector/status.go", "aggregateOneComponent", "Get", 1, passThroughTopLevelStatus+" (counters, revisions, RolloutHold, Conditions mirrored onto the ISVC)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector/projector.go", "EnsureInferenceReplica", "Get", 1, passThroughSpecMetadata+" (spec projection)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/dispatch.go", "observeCanaryRevisions", "Get", 1, passThroughTopLevelStatus+" (revision pointers)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/dispatch.go", "reconcileRollbackSignal", "Get", 1, passThroughTopLevelStatus+" (revision pointers and observedGeneration)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun/observe.go", "observeGroupTargets", "Get", 1, passThroughTopLevelStatus+" (revision pointers and replica counters)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/pdb/cutover.go", "OMENativeCutoverReady", "Get", 1, passThroughTopLevelStatus+" (ready and available counters)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler/dispatch.go", "controlledByVerifiedModeBridge", "Get", 1, passThroughSpecMetadata+" (ownership: UID, labels, parentRef)")

	// Generated client-go informer: a raw list/watch cache that consumes no
	// rows; row-consuming code reads through the decoded accessor, never
	// through this cache.
	approve("pkg/client/informers/externalversions/ome/v1beta1/inferencereplica.go", "NewFilteredInferenceReplicaInformer", "List", 2, passThroughSpecMetadata+" (generated informer list/watch)")

	// Alfred preserves the raw object; logical-row consumers use its bounded
	// codec adapter. Dispatch reconciliation reads only migration records.
	approve("pkg/alfred/engine/dispatch_reconcile.go", "Dispatcher.reconcileDispatch", "Get", 1, passThroughTopLevelStatus+" (migration records)")
	approve("pkg/alfred/snapshot/builder.go", "Build", "List", 1, "raw capture; per-instance rows decoded through Alfred's bounded codec adapter")
	// CLI readers below still consume stored dense rows directly.
	approve("pkg/cli/cmd/get/registry.go", "<package>", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/cmd/get/registry.go", "<package>", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/cmd/scale/collect.go", "collect", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/instancecollection/collect.go", "CollectRelated", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/migrationcollection/collect.go", "Collect", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/migrationhistorycollection/collect.go", "collectReplicas", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/held_release_source.go", "CollectHeldReleaseEvidence", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/held_release_source.go", "CollectHeldReleaseEvidence", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/migration_evidence.go", "RecheckMigration", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/replicas.go", "collectReplicaEvidence", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/scale_pinned.go", "CollectScalePinnedTargets", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/scale_pinned.go", "ScaleEvidence.Revalidate", "Get", 1, rawReaderOutsideManager)

	inv := loadStatusInventory(t)
	actual := map[fetchSite]int{}
	inv.eachProductionFile(func(pkg *packages.Package, file *ast.File, relative string) {
		collectInferenceReplicaFetches(pkg, file, relative, actual)
	})

	for site, got := range actual {
		want, ok := approved[site]
		if !ok {
			t.Errorf("unclassified InferenceReplica %s in %s:%s (%d): route it through irstatus.GetDecoded or classify it as pass-through", site.method, site.file, site.function, got)
			continue
		}
		if got != want.count {
			t.Errorf("fetch count changed for %s in %s:%s: got %d, want %d (%s)", site.method, site.file, site.function, got, want.count, want.reason)
		}
	}
	for site, want := range approved {
		if _, ok := actual[site]; !ok {
			t.Errorf("stale fetch approval for %s in %s:%s (%s)", site.method, site.file, site.function, want.reason)
		}
	}
}

// A decoded read carries its row decoder on the reader it receives. A bare
// client.Client carries the zero Decoder and fails closed on every ColumnarV2
// object, so a decoded accessor may receive only the codec's Reader or a
// client.Reader parameter that a caller filled under this same rule.
func TestInferenceReplicaDecodedReadsCarryADecoder(t *testing.T) {
	decodedAccessors := map[string]map[string]bool{
		codecPackagePath:     {decodedAccessor: true},
		projectorPackagePath: {"DecodedComponentIR": true, "DecodedComponentIRStatus": true},
	}
	inv := loadStatusInventory(t)
	calls := 0
	inv.eachProductionFile(func(pkg *packages.Package, file *ast.File, relative string) {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			fn, ok := pkg.TypesInfo.Uses[selector.Sel].(*types.Func)
			if !ok || fn.Pkg() == nil || !decodedAccessors[fn.Pkg().Path()][fn.Name()] || len(call.Args) < 2 {
				return true
			}
			calls++
			typ := pkg.TypesInfo.TypeOf(call.Args[1])
			if typ == nil || readerCarriesDecoder(typ) {
				return true
			}
			t.Errorf("%s:%s passes a %s to %s: wrap it with irstatus.NewReader so the read carries the row decoder",
				relative, enclosingFunction(file, call.Pos()), types.TypeString(typ, nil), fn.Name())
			return true
		})
	})
	if calls == 0 {
		t.Fatal("no decoded-accessor call found; the rule would pass vacuously")
	}
}

// readerCarriesDecoder accepts the codec's Reader, which carries a Decoder by
// construction, and the client.Reader interface, which reaches a decoded
// accessor only as a parameter whose caller is checked by the same rule.
func readerCarriesDecoder(typ types.Type) bool {
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	switch named.Obj().Pkg().Path() {
	case codecPackagePath:
		return named.Obj().Name() == "Reader"
	case controllerRuntimeClientPackagePath:
		return named.Obj().Name() == "Reader"
	}
	return false
}

// The shared fixture package writes the dense representation directly, so its
// read-inventory classification holds only while no production package links
// it. The inventory loads non-test files only, so any importer found here is
// production code; the fixture package itself must be among the loaded
// packages so a rename cannot make the check pass vacuously.
func TestIRStatusFixturePackageImportInventory(t *testing.T) {
	inv := loadStatusInventory(t)
	loaded := false
	var importers []string
	for _, pkg := range inv.pkgs {
		if pkg.PkgPath == fixturePackagePath {
			loaded = true
			continue
		}
		if _, ok := pkg.Imports[fixturePackagePath]; ok {
			importers = append(importers, pkg.PkgPath)
		}
	}
	if !loaded {
		t.Fatalf("fixture package %s is not among the loaded production packages; update fixturePackagePath", fixturePackagePath)
	}
	sort.Strings(importers)
	if len(importers) != 0 {
		t.Fatalf("production packages import the test-only fixture package %s: %v", fixturePackagePath, importers)
	}
}

// assertInferenceReplicaStatusWritesUseSingleWriter is the repo-wide,
// type-aware half of the single-writer contract: every status-subresource
// write of an InferenceReplica, through any client shape, must be the single
// writer.
func assertInferenceReplicaStatusWritesUseSingleWriter(t *testing.T) {
	t.Helper()
	inv := loadStatusInventory(t)
	var sites []string
	inv.eachProductionFile(func(pkg *packages.Package, file *ast.File, relative string) {
		for _, site := range collectInferenceReplicaStatusWrites(pkg, file, relative) {
			if site.function == singleWriter && strings.HasSuffix(site.file, "inferencereplica/status_writer.go") {
				continue
			}
			sites = append(sites, fmt.Sprintf("%s:%s (%s)", site.file, site.function, site.method))
		}
	})
	sort.Strings(sites)
	if len(sites) != 0 {
		t.Fatalf("InferenceReplica status writes outside %s: %v", singleWriter, sites)
	}
}

// --- inventory mechanics ---

type statusInventory struct {
	repoRoot string
	pkgs     []*packages.Package
}

var (
	inventoryOnce sync.Once
	inventoryPkgs []*packages.Package
	inventoryRoot string
	inventoryErr  error
)

func loadStatusInventory(t *testing.T) *statusInventory {
	t.Helper()
	inventoryOnce.Do(func() {
		inventoryRoot = inventoryRepositoryRoot()
		if inventoryRoot == "" {
			inventoryErr = fmt.Errorf("resolve repository root")
			return
		}
		// Every production root of the main module; a root that is its own
		// module (scheduler) cannot be loaded from here and cannot import
		// the main module's API types.
		var patterns []string
		for _, root := range []string{"pkg", "cmd", "internal", "scheduler"} {
			if _, err := os.Stat(filepath.Join(inventoryRoot, root)); err != nil {
				continue
			}
			if _, err := os.Stat(filepath.Join(inventoryRoot, root, "go.mod")); err == nil {
				continue
			}
			patterns = append(patterns, "./"+root+"/...")
		}
		cfg := &packages.Config{
			Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
			Dir:   inventoryRoot,
			Tests: false,
		}
		inventoryPkgs, inventoryErr = packages.Load(cfg, patterns...)
	})
	if inventoryErr != nil {
		t.Fatalf("load production packages: %v", inventoryErr)
	}
	for _, pkg := range inventoryPkgs {
		if len(pkg.Errors) == 0 {
			continue
		}
		if _, usesAPI := pkg.Imports[omeAPIPackagePath]; usesAPI {
			t.Fatalf("package %s did not type-check: %v", pkg.PkgPath, pkg.Errors[0])
		}
		t.Logf("skipping %s (does not import the OME API and did not load: %v)", pkg.PkgPath, pkg.Errors[0])
	}
	return &statusInventory{repoRoot: inventoryRoot, pkgs: inventoryPkgs}
}

func inventoryRepositoryRoot() string {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	for directory := filepath.Dir(source); ; directory = filepath.Dir(directory) {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		if parent := filepath.Dir(directory); parent == directory {
			return ""
		}
	}
}

func (inv *statusInventory) eachProductionFile(visit func(pkg *packages.Package, file *ast.File, relative string)) {
	for _, pkg := range inv.pkgs {
		if len(pkg.Errors) > 0 || pkg.TypesInfo == nil || pkg.Fset == nil {
			continue
		}
		for _, file := range pkg.Syntax {
			path := pkg.Fset.Position(file.Pos()).Filename
			base := filepath.Base(path)
			if strings.HasPrefix(base, "zz_generated.") || base == "openapi_generated.go" || strings.HasSuffix(base, "_test.go") {
				continue
			}
			relative, err := filepath.Rel(inv.repoRoot, path)
			if err != nil || strings.HasPrefix(relative, "..") {
				continue
			}
			visit(pkg, file, filepath.ToSlash(relative))
		}
	}
}

var (
	testInventoryOnce sync.Once
	testInventoryPkgs []*packages.Package
	testInventoryRoot string
	testInventoryErr  error
)

// loadTestInventory loads every package under tests/ together with its test
// files, which is where the integration specs live.
func loadTestInventory(t *testing.T) *statusInventory {
	t.Helper()
	testInventoryOnce.Do(func() {
		testInventoryRoot = inventoryRepositoryRoot()
		if testInventoryRoot == "" {
			testInventoryErr = fmt.Errorf("resolve repository root")
			return
		}
		if _, err := os.Stat(filepath.Join(testInventoryRoot, "tests")); err != nil {
			testInventoryErr = fmt.Errorf("locate tests root: %w", err)
			return
		}
		cfg := &packages.Config{
			Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
			Dir:   testInventoryRoot,
			Tests: true,
		}
		testInventoryPkgs, testInventoryErr = packages.Load(cfg, "./tests/...")
	})
	if testInventoryErr != nil {
		t.Fatalf("load test packages: %v", testInventoryErr)
	}
	for _, pkg := range testInventoryPkgs {
		if len(pkg.Errors) == 0 {
			continue
		}
		if _, usesAPI := pkg.Imports[omeAPIPackagePath]; usesAPI {
			t.Fatalf("package %s did not type-check: %v", pkg.PkgPath, pkg.Errors[0])
		}
		t.Logf("skipping %s (does not import the OME API and did not load: %v)", pkg.PkgPath, pkg.Errors[0])
	}
	return &statusInventory{repoRoot: testInventoryRoot, pkgs: testInventoryPkgs}
}

// eachTestFile visits every repository file of the loaded test packages once,
// test files included. A package's test variants share its non-test files, so
// files are deduplicated by path.
func (inv *statusInventory) eachTestFile(visit func(pkg *packages.Package, file *ast.File, relative string)) {
	seen := map[string]struct{}{}
	for _, pkg := range inv.pkgs {
		if len(pkg.Errors) > 0 || pkg.TypesInfo == nil || pkg.Fset == nil {
			continue
		}
		for _, file := range pkg.Syntax {
			path := pkg.Fset.Position(file.Pos()).Filename
			if _, visited := seen[path]; visited {
				continue
			}
			seen[path] = struct{}{}
			relative, err := filepath.Rel(inv.repoRoot, path)
			if err != nil || strings.HasPrefix(relative, "..") {
				continue
			}
			visit(pkg, file, filepath.ToSlash(relative))
		}
	}
}

// isOMEAPIType reports whether typ (after pointer dereference) is the named
// type name from the OME API package.
func isOMEAPIType(typ types.Type, name string) bool {
	for {
		pointer, ok := typ.(*types.Pointer)
		if !ok {
			break
		}
		typ = pointer.Elem()
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == omeAPIPackagePath && named.Obj().Name() == name
}

func enclosingFunction(file *ast.File, pos token.Pos) string {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || pos < function.Pos() || pos > function.End() {
			continue
		}
		if function.Recv == nil || len(function.Recv.List) == 0 {
			return function.Name.Name
		}
		receiver := function.Recv.List[0].Type
		if star, ok := receiver.(*ast.StarExpr); ok {
			receiver = star.X
		}
		if ident, ok := receiver.(*ast.Ident); ok {
			return ident.Name + "." + function.Name.Name
		}
		return function.Name.Name
	}
	return "<package>"
}

func parentMap(file *ast.File) map[ast.Node]ast.Node {
	parents := make(map[ast.Node]ast.Node)
	stack := make([]ast.Node, 0, 16)
	ast.Inspect(file, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		if len(stack) > 0 {
			parents[node] = stack[len(stack)-1]
		}
		stack = append(stack, node)
		return true
	})
	return parents
}

func selectorAccessKind(selector ast.Node, parents map[ast.Node]ast.Node) string {
	child := selector
	for parent := parents[child]; parent != nil; child, parent = parent, parents[parent] {
		switch node := parent.(type) {
		case *ast.AssignStmt:
			for _, left := range node.Lhs {
				if left != child {
					continue
				}
				if child != selector || (node.Tok != token.ASSIGN && node.Tok != token.DEFINE) {
					return "read-write"
				}
				return "write"
			}
			return "read"
		case *ast.IncDecStmt:
			return "read-write"
		case *ast.RangeStmt:
			if node.Key == child || node.Value == child {
				return "write"
			}
		case *ast.UnaryExpr:
			if node.Op == token.AND && node.X == child {
				return "read-write"
			}
		case *ast.FuncDecl, *ast.FuncLit:
			return "read"
		}
	}
	return "read"
}

func collectRepresentationFieldUses(pkg *packages.Package, file *ast.File, relative string, actual map[fieldUse]accessCounts) {
	parents := parentMap(file)
	record := func(pos token.Pos, field, kind string) {
		use := fieldUse{file: relative, function: enclosingFunction(file, pos), field: field}
		counts := actual[use]
		switch kind {
		case "write":
			counts.writes++
		case "read-write":
			counts.readWrites++
		default:
			counts.reads++
		}
		actual[use] = counts
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.SelectorExpr:
			if _, tracked := representationFields[n.Sel.Name]; !tracked {
				return true
			}
			selection, ok := pkg.TypesInfo.Selections[n]
			if !ok || selection.Kind() != types.FieldVal || !isOMEAPIType(selection.Recv(), "InferenceReplicaStatus") {
				return true
			}
			record(n.Pos(), n.Sel.Name, selectorAccessKind(n, parents))
		case *ast.CompositeLit:
			if !isOMEAPIType(pkg.TypesInfo.TypeOf(n), "InferenceReplicaStatus") {
				return true
			}
			for _, element := range n.Elts {
				keyed, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := keyed.Key.(*ast.Ident)
				if !ok {
					continue
				}
				if _, tracked := representationFields[key.Name]; tracked {
					record(key.Pos(), key.Name, "write")
				}
			}
		}
		return true
	})
}

// inferenceReplicaFetch reports whether call fetches an InferenceReplica:
// a Get or List whose object argument, or whose first result, is the
// InferenceReplica or InferenceReplicaList type.
func inferenceReplicaFetch(pkg *packages.Package, call *ast.CallExpr) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (selector.Sel.Name != "Get" && selector.Sel.Name != "List") {
		return "", false
	}
	for _, argument := range call.Args {
		typ := pkg.TypesInfo.TypeOf(argument)
		if typ != nil && (isOMEAPIType(typ, "InferenceReplica") || isOMEAPIType(typ, "InferenceReplicaList")) {
			return selector.Sel.Name, true
		}
	}
	if signature, ok := pkg.TypesInfo.TypeOf(call.Fun).(*types.Signature); ok && signature.Results().Len() > 0 {
		first := signature.Results().At(0).Type()
		if isOMEAPIType(first, "InferenceReplica") || isOMEAPIType(first, "InferenceReplicaList") {
			return selector.Sel.Name, true
		}
	}
	return "", false
}

func collectInferenceReplicaFetches(pkg *packages.Package, file *ast.File, relative string, actual map[fetchSite]int) {
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, fetch := inferenceReplicaFetch(pkg, call)
		if !fetch {
			return true
		}
		actual[fetchSite{file: relative, function: enclosingFunction(file, call.Pos()), method: method}]++
		return true
	})
}

// collectInferenceReplicaStatusWrites finds status-subresource writes of an
// InferenceReplica through every client shape: controller-runtime
// Status()/SubResource("status") writers, generated typed clients, and
// dynamic clients.
func collectInferenceReplicaStatusWrites(pkg *packages.Package, file *ast.File, relative string) []fetchSite {
	var sites []fetchSite
	record := func(pos token.Pos, method string) {
		sites = append(sites, fetchSite{file: relative, function: enclosingFunction(file, pos), method: method})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		method := selector.Sel.Name
		switch method {
		case "Update", "Patch", "Apply", "Create":
			if statusWriterReceiver(selector.X) && callTouchesInferenceReplica(pkg, call) {
				record(call.Pos(), "Status()."+method)
			}
		case "UpdateStatus", "ApplyStatus":
			if callTouchesInferenceReplica(pkg, call) || dynamicResourceReceiver(pkg, selector.X) {
				record(call.Pos(), method)
			}
		}
		return true
	})
	return sites
}

// statusWriterReceiver reports whether expr is a Status() call or a
// SubResource("status") call.
func statusWriterReceiver(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch selector.Sel.Name {
	case "Status":
		return len(call.Args) == 0
	case "SubResource":
		if len(call.Args) != 1 {
			return false
		}
		literal, ok := call.Args[0].(*ast.BasicLit)
		return ok && literal.Kind == token.STRING && literal.Value == `"status"`
	}
	return false
}

func callTouchesInferenceReplica(pkg *packages.Package, call *ast.CallExpr) bool {
	for _, argument := range call.Args {
		if typ := pkg.TypesInfo.TypeOf(argument); typ != nil && isOMEAPIType(typ, "InferenceReplica") {
			return true
		}
	}
	if signature, ok := pkg.TypesInfo.TypeOf(call.Fun).(*types.Signature); ok && signature.Results().Len() > 0 {
		return isOMEAPIType(signature.Results().At(0).Type(), "InferenceReplica")
	}
	return false
}

func dynamicResourceReceiver(pkg *packages.Package, expr ast.Expr) bool {
	typ := pkg.TypesInfo.TypeOf(expr)
	if typ == nil {
		return false
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == "k8s.io/client-go/dynamic"
}

// massFailureFixtureRows mirrors the shape of the codec package's
// mass-failure fixture: 4,000 single-pod rows after a widespread failure,
// 2,812 Ready, 814 Restarting with an in-flight Restart, 374 Failed with a
// preserved Restart, every row carrying a lastFailure record of about 130
// bytes as stored, and one row on a second revision. Phases are spread over
// the index space by a fixed stride so the index sets fragment.
func massFailureFixtureRows() []v1beta1.OMENativeInstanceStatus {
	const (
		rowCount        = 4000
		readyCount      = 2812
		restartingCount = 814
		stride          = 1103
	)
	rows := make([]v1beta1.OMENativeInstanceStatus, rowCount)
	for i := range rows {
		row := v1beta1.OMENativeInstanceStatus{
			Index:           int32(i),
			Incarnation:     1,
			RunningRevision: selectionFixtureRevision,
			LastFailure: &v1beta1.InstanceTermination{
				Reason:  "DeadlineExceeded",
				Message: "DeadlineExceeded: Restart/Drain exceeded InstanceReadyTimeout",
				Time:    selectionFixtureTime,
			},
		}
		operation := &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("2f6b6c8e-0000-4000-8000-%012d", i),
			Type:           v1beta1.InstanceOperationRestart,
			StartedAt:      selectionFixtureTime,
			LastProgressAt: selectionFixtureTime,
			Deadline:       metav1.NewTime(selectionFixtureTime.Add(30 * time.Minute)),
			TargetRevision: selectionFixtureRevision,
			Reason:         "PodFailed",
		}
		switch rank := (i * stride) % rowCount; {
		case rank < readyCount:
			row.Phase = v1beta1.OMENativeInstanceReady
			row.Incarnation = 2
			row.PodCount, row.ServingPodCount, row.AvailablePodCount = 1, 1, 1
			row.Admitted = true
		case rank < readyCount+restartingCount:
			row.Phase = v1beta1.OMENativeInstanceRestarting
			operation.Step = "WaitReady"
			row.Operation = operation
		default:
			row.Phase = v1beta1.OMENativeInstanceFailed
			operation.Step = "Recreate"
			operation.RetryCount = 3
			operation.Reason = "RestartBudgetExhausted"
			row.Operation = operation
		}
		rows[i] = row
	}
	rows[rowCount-1].RunningRevision = selectionFixtureOtherRevision
	return rows
}

func bytesGaugeLabels(ir *v1beta1.InferenceReplica, encoding string) map[string]string {
	return map[string]string{"namespace": ir.Namespace, "name": ir.Name, "component": string(ir.Spec.Component), "encoding": encoding}
}

func otherEncodingLabel(encoding string) string {
	if encoding == obsmetrics.IRStatusEncodingDenseV1 {
		return obsmetrics.IRStatusEncodingColumnarV2
	}
	return obsmetrics.IRStatusEncodingDenseV1
}

// TestStatusWriterSelectsRepresentationByTarget drives a logical mutation
// through the writer under each target for the uniform and mass-failure
// fixtures and a two-row object: a DenseV1 target always stores the dense
// list; a ColumnarV2 target stores columns only when they are strictly
// smaller and the dense list otherwise. The stored object carries exactly
// one representation, decodes back to the same rows, and the bytes gauge and
// write counters carry the encoding that was written.
func TestStatusWriterSelectsRepresentationByTarget(t *testing.T) {
	cases := []struct {
		name   string
		target irstatus.Encoding
		rows   []v1beta1.OMENativeInstanceStatus
		want   irstatus.Encoding
	}{
		{name: "DenseV1 target, uniform", target: irstatus.EncodingDenseV1, rows: uniformFixtureRows(500), want: irstatus.EncodingDenseV1},
		{name: "DenseV1 target, mass failure", target: irstatus.EncodingDenseV1, rows: massFailureFixtureRows(), want: irstatus.EncodingDenseV1},
		{name: "ColumnarV2 target, uniform", target: irstatus.EncodingColumnarV2, rows: uniformFixtureRows(500), want: irstatus.EncodingColumnarV2},
		{name: "ColumnarV2 target, mass failure", target: irstatus.EncodingColumnarV2, rows: massFailureFixtureRows(), want: irstatus.EncodingColumnarV2},
		{name: "ColumnarV2 target, two rows fall back to DenseV1", target: irstatus.EncodingColumnarV2, rows: decodeFixtureIR().Status.InstanceStatuses, want: irstatus.EncodingDenseV1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ir := fixtureIRWithRows("model-engine", tc.rows)
			c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
			key := client.ObjectKeyFromObject(ir)
			writer := statusWriter{Client: c, decoder: irstatus.NewDecoder(testColumnarBound), target: tc.target}
			wantLabel, otherLabel := encodingLabel(tc.want), otherEncodingLabel(encodingLabel(tc.want))
			before := map[string]float64{}
			for _, label := range []string{wantLabel, otherLabel} {
				for _, result := range []string{obsmetrics.IRStatusWriteAttempt, obsmetrics.IRStatusWriteCommitted} {
					before[label+result] = irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeLabels(label, result))
				}
			}

			live := &v1beta1.InferenceReplica{}
			if _, err := irstatus.GetDecoded(ctx, irstatus.NewReader(c, writer.decoder), key, live); err != nil {
				t.Fatalf("get InferenceReplica: %v", err)
			}
			live.Status.ObservedGeneration = 7
			wantRows := make([]v1beta1.OMENativeInstanceStatus, len(tc.rows))
			for i := range tc.rows {
				wantRows[i] = *tc.rows[i].DeepCopy()
			}
			irstatus.ClearPodDerivedObservations(wantRows)

			if err := updateInferenceReplicaStatus(ctx, writer, live, irstatus.EncodingDenseV1); err != nil {
				t.Fatalf("update status: %v", err)
			}

			stored, got := storedEncoding(t, c, key)
			if got != tc.want {
				t.Fatalf("stored encoding = %s, want %s", got, tc.want)
			}
			if stored.Status.ObservedGeneration != 7 {
				t.Fatalf("logical mutation was not persisted: %+v", stored.Status)
			}
			switch tc.want {
			case irstatus.EncodingColumnarV2:
				if len(stored.Status.InstanceStatuses) != 0 || stored.Status.InstanceStatusColumns == nil {
					t.Fatalf("a ColumnarV2 object must carry columns and no dense rows")
				}
			case irstatus.EncodingDenseV1:
				if stored.Status.InstanceStatusEncoding != nil || stored.Status.InstanceStatusColumns != nil {
					t.Fatalf("a DenseV1 object must carry neither marker nor columns")
				}
			}
			decoded := &v1beta1.InferenceReplica{}
			source, err := irstatus.GetDecoded(ctx, irstatus.NewReader(c, writer.decoder), key, decoded)
			if err != nil || source != tc.want {
				t.Fatalf("decoded read: source %q err %v", source, err)
			}
			if !equality.Semantic.DeepEqual(decoded.Status.InstanceStatuses, wantRows) {
				t.Fatalf("rows read back after a %s write differ from the logical rows", tc.want)
			}
			if !equality.Semantic.DeepEqual(live.Status, decoded.Status) {
				t.Fatalf("the object after the write must equal a fresh decoded read")
			}

			written, err := json.Marshal(&stored.Status)
			if err != nil {
				t.Fatalf("marshal stored status: %v", err)
			}
			if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, wantLabel)); int(gauge) != len(written) {
				t.Fatalf("bytes gauge (%s) = %v, want the stored status size %d", wantLabel, gauge, len(written))
			}
			if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, otherLabel)); gauge != 0 {
				t.Fatalf("bytes gauge for the unselected encoding %s must be absent, got %v", otherLabel, gauge)
			}
			for _, result := range []string{obsmetrics.IRStatusWriteAttempt, obsmetrics.IRStatusWriteCommitted} {
				if got := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeLabels(wantLabel, result)) - before[wantLabel+result]; got != 1 {
					t.Fatalf("%s/%s delta = %v, want 1", wantLabel, result, got)
				}
				if got := irStatusMetric(t, "ome_omenative_ir_status_writes_total", writeLabels(otherLabel, result)) - before[otherLabel+result]; got != 0 {
					t.Fatalf("%s/%s delta = %v, want 0", otherLabel, result, got)
				}
			}
			t.Logf("%s: stored %s, %d bytes", tc.name, got, len(written))
		})
	}
}

// TestStatusWriterExactTieSelectsDenseV1 finds an exact size tie between the
// two candidates and proves the writer stores the dense list for it.
func TestStatusWriterExactTieSelectsDenseV1(t *testing.T) {
	ctx := context.Background()
	var tieRows []v1beta1.OMENativeInstanceStatus
search:
	for rowCount := 1; rowCount <= 6; rowCount++ {
		for shared := 0; shared <= 1; shared++ {
			for length := 0; length <= 200; length++ {
				rows := uniformFixtureRows(rowCount)
				for i := range rows {
					revision := strings.Repeat("r", length)
					if shared == 0 {
						revision = fmt.Sprintf("%s%d", revision, i)
					}
					rows[i].RunningRevision = revision
					rows[i].Admitted = i%2 == 0
				}
				selection, err := irstatus.SelectCandidate(&v1beta1.InferenceReplicaStatus{InstanceStatuses: rows}, testColumnarBound)
				if err != nil {
					t.Fatalf("SelectCandidate: %v", err)
				}
				if selection.ColumnarV2 != nil && selection.ColumnarV2.Size == selection.DenseV1.Size {
					tieRows = rows
					break search
				}
			}
		}
	}
	if tieRows == nil {
		t.Fatal("no exact tie constructed")
	}

	ir := fixtureIRWithRows("model-engine", tieRows)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	key := client.ObjectKeyFromObject(ir)
	live := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(ctx, c, key, live); err != nil {
		t.Fatalf("get InferenceReplica: %v", err)
	}
	live.Status.ObservedGeneration = 2
	if err := updateInferenceReplicaStatus(ctx, columnarStatusWriter(c), live, irstatus.EncodingDenseV1); err != nil {
		t.Fatalf("update status: %v", err)
	}
	if _, got := storedEncoding(t, c, key); got != irstatus.EncodingDenseV1 {
		t.Fatalf("an exact tie stored %s, want DenseV1", got)
	}
	if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, obsmetrics.IRStatusEncodingColumnarV2)); gauge != 0 {
		t.Fatalf("a tie must not publish a ColumnarV2 bytes series, got %v", gauge)
	}
}

// TestStatusWriterSwitchesGaugeSeriesWithTheSelectedEncoding pins the gauge
// on a representation switch driven by a logical mutation: after columns are
// stored, a mutation that makes every row unique selects the dense list and
// the ColumnarV2 series is deleted.
func TestStatusWriterSwitchesGaugeSeriesWithTheSelectedEncoding(t *testing.T) {
	ctx := context.Background()
	ir := fixtureIRWithRows("model-engine", uniformFixtureRows(64))
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	key := client.ObjectKeyFromObject(ir)
	writer := columnarStatusWriter(c)
	reader := irstatus.NewReader(c, writer.decoder)

	live := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(ctx, reader, key, live); err != nil {
		t.Fatalf("get InferenceReplica: %v", err)
	}
	if err := updateInferenceReplicaStatus(ctx, writer, live, irstatus.EncodingDenseV1); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if _, got := storedEncoding(t, c, key); got != irstatus.EncodingColumnarV2 {
		t.Fatalf("64 uniform rows stored %s, want ColumnarV2", got)
	}
	if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, obsmetrics.IRStatusEncodingColumnarV2)); gauge == 0 {
		t.Fatal("ColumnarV2 bytes series must exist after a ColumnarV2 write")
	}

	// A scale-down to two rows is a logical mutation for which the dense
	// list is the smaller candidate, so the same writer switches back.
	fresh := &v1beta1.InferenceReplica{}
	source, err := irstatus.GetDecoded(ctx, reader, key, fresh)
	if err != nil || source != irstatus.EncodingColumnarV2 {
		t.Fatalf("re-read: source %q err %v", source, err)
	}
	fresh.Status.InstanceStatuses = fresh.Status.InstanceStatuses[:2]
	fresh.Status.Replicas = 2
	if err := updateInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
		t.Fatalf("second write: %v", err)
	}
	stored, got := storedEncoding(t, c, key)
	if got != irstatus.EncodingDenseV1 || len(stored.Status.InstanceStatuses) != 2 {
		t.Fatalf("two rows stored as %s with %d rows, want DenseV1 with 2", got, len(stored.Status.InstanceStatuses))
	}
	written, err := json.Marshal(&stored.Status)
	if err != nil {
		t.Fatalf("marshal stored status: %v", err)
	}
	if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, obsmetrics.IRStatusEncodingDenseV1)); int(gauge) != len(written) {
		t.Fatalf("DenseV1 bytes gauge = %v, want %d", gauge, len(written))
	}
	if gauge := irStatusMetric(t, "ome_omenative_ir_status_bytes", bytesGaugeLabels(ir, obsmetrics.IRStatusEncodingColumnarV2)); gauge != 0 {
		t.Fatalf("ColumnarV2 bytes series must be deleted after the switch, got %v", gauge)
	}
}

// TestStatusWriterRefusesUnsetTarget pins that a writer without a configured
// target refuses every write before any request instead of assuming DenseV1.
func TestStatusWriterRefusesUnsetTarget(t *testing.T) {
	ctx := context.Background()
	ir := decodeFixtureIR()
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	key := client.ObjectKeyFromObject(ir)
	before := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, before); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	unset := statusWriter{Client: c}
	if err := updateInferenceReplicaStatus(ctx, unset, before.DeepCopy(), irstatus.EncodingDenseV1); !errors.Is(err, ErrInstanceStatusTargetUnset) {
		t.Fatalf("logical mutation without a target must be refused with ErrInstanceStatusTargetUnset, got %v", err)
	}
	if err := convertInferenceReplicaStatus(ctx, unset, before.DeepCopy(), irstatus.EncodingColumnarV2); !errors.Is(err, ErrInstanceStatusTargetUnset) {
		t.Fatalf("conversion without a target must be refused with ErrInstanceStatusTargetUnset, got %v", err)
	}
	after := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, key, after); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if after.ResourceVersion != before.ResourceVersion {
		t.Fatal("a refused write must not reach the API server")
	}
}

const (
	measurementCurrentRevision = "example-engine-6d8f7b9c"
	measurementUpdateRevision  = "example-engine-a3e129d4"
	measurementLargeMessageLen = 1024
)

var measurementInstanceCounts = []int32{0, 1, 100, 1000, 2000, 5000}

var (
	measurementEncoderOnce sync.Once
	measurementEncoder     k8sruntime.Encoder
	measurementEncoderErr  error
)

type statusSizeShape string

const (
	shapeSteady           statusSizeShape = "steady"
	shapeCreating         statusSizeShape = "creating"
	shapeUpdatingInPlace  statusSizeShape = "updating-in-place"
	shapeUpdatingSurge    statusSizeShape = "updating-surge"
	shapeDeleting         statusSizeShape = "deleting"
	shapeKueueGated       statusSizeShape = "kueue-gated"
	shapeFailureRealistic statusSizeShape = "failure-realistic"
	shapeFailureLarge     statusSizeShape = "failure-large"
	shapeMigrationHeavy   statusSizeShape = "migration-heavy"
)

type statusSizeFixture struct {
	name            string
	shape           statusSizeShape
	podsPerInstance int32
}

var statusSizeFixtures = []statusSizeFixture{
	{name: "steady-singleton", shape: shapeSteady, podsPerInstance: 1},
	{name: "steady-gang-8", shape: shapeSteady, podsPerInstance: 8},
	{name: "creating-operation", shape: shapeCreating, podsPerInstance: 8},
	{name: "updating-in-place", shape: shapeUpdatingInPlace, podsPerInstance: 8},
	{name: "updating-surge", shape: shapeUpdatingSurge, podsPerInstance: 1},
	{name: "deleting-wave", shape: shapeDeleting, podsPerInstance: 8},
	{name: "kueue-gated", shape: shapeKueueGated, podsPerInstance: 8},
	{name: "failure-realistic", shape: shapeFailureRealistic, podsPerInstance: 8},
	{name: "failure-large", shape: shapeFailureLarge, podsPerInstance: 8},
	{name: "migration-heavy", shape: shapeMigrationHeavy, podsPerInstance: 8},
}

type statusSizeMeasurement struct {
	fixture                string
	instances              int32
	observedRequestBytes   int
	persistedRequestBytes  int
	observedStatusBytes    int
	persistedStatusBytes   int
	instanceRowsBytes      int
	readyPodCountBytes     int
	scheduledPodCountBytes int
	nodesOccupiedBytes     int
	podObservationBytes    int
	operationBytes         int
	lastFailureBytes       int
	migrationsBytes        int
	reductionBytes         int
}

func TestInferenceReplicaStatusSizeCompactionDropsExactlyThreeFields(t *testing.T) {
	observed := newStatusSizeIR(statusSizeFixtures[1], 1)
	observed.Status.InstanceStatuses[0] = fullyPopulatedInstanceStatus(0)
	original := observed.DeepCopy()

	persisted := observed.DeepCopy()
	irstatus.ClearPodDerivedObservations(persisted.Status.InstanceStatuses)
	want := observed.DeepCopy()
	for i := range want.Status.InstanceStatuses {
		want.Status.InstanceStatuses[i].ReadyPodCount = 0
		want.Status.InstanceStatuses[i].ScheduledPodCount = 0
		want.Status.InstanceStatuses[i].NodesOccupied = nil
	}

	if diff := cmp.Diff(want, persisted); diff != "" {
		t.Fatalf("status compaction changed fields outside the three Pod-derived observations (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(original, observed); diff != "" {
		t.Fatalf("status compaction mutated its source fixture (-want +got):\n%s", diff)
	}
}

func TestInferenceReplicaStatusSizeNormalizedFixtures(t *testing.T) {
	measurements := collectStatusSizeMeasurements(t)
	actual := renderStatusSizeCSV(t, measurements)
	expected, err := os.ReadFile("testdata/status_size_v1.csv")
	if err != nil {
		t.Fatalf("read status-size golden: %v\nactual report:\n%s", err, actual)
	}
	if diff := cmp.Diff(string(expected), actual); diff != "" {
		t.Fatalf("status-size report changed; review the normalized payload delta before updating the golden (-want +got):\n%s", diff)
	}
}

func TestInferenceReplicaStatusSizeNormalizedSteadyStateAcceptance(t *testing.T) {
	const (
		instances                   = int32(5000)
		maxNormalizedPersistedBytes = 825_000
	)
	tests := []struct {
		fixture             statusSizeFixture
		minReductionBytes   int
		minReductionPercent float64
	}{
		{fixture: statusSizeFixtures[0], minReductionBytes: 400_000, minReductionPercent: 30},
		{fixture: statusSizeFixtures[1], minReductionBytes: 1_250_000, minReductionPercent: 50},
	}
	for _, tt := range tests {
		t.Run(tt.fixture.name, func(t *testing.T) {
			measurement := measureStatusSize(t, tt.fixture, instances)
			if measurement.persistedRequestBytes > maxNormalizedPersistedBytes {
				t.Fatalf("normalized persisted request = %d bytes, want at most %d", measurement.persistedRequestBytes, maxNormalizedPersistedBytes)
			}
			if measurement.reductionBytes < tt.minReductionBytes {
				t.Fatalf("request reduction = %d bytes, want at least %d", measurement.reductionBytes, tt.minReductionBytes)
			}
			percent := float64(measurement.reductionBytes) * 100 / float64(measurement.observedRequestBytes)
			if percent < tt.minReductionPercent {
				t.Fatalf("request reduction = %.2f%%, want at least %.2f%%", percent, tt.minReductionPercent)
			}
		})
	}
}

func BenchmarkInferenceReplicaStatusNormalizedJSON(b *testing.B) {
	fixtures := []statusSizeFixture{
		statusSizeFixtures[0],
		statusSizeFixtures[1],
		statusSizeFixtures[7],
		statusSizeFixtures[9],
	}
	for _, fixture := range fixtures {
		observed := newStatusSizeIR(fixture, 2000)
		persisted := observed.DeepCopy()
		irstatus.ClearPodDerivedObservations(persisted.Status.InstanceStatuses)
		for _, state := range []struct {
			name   string
			object *v1beta1.InferenceReplica
		}{
			{name: "Observed", object: observed},
			{name: "Persisted", object: persisted},
		} {
			body, err := marshalStatusUpdateBody(state.object)
			if err != nil {
				b.Fatalf("marshal benchmark fixture: %v", err)
			}
			b.Run(fixture.name+"/"+state.name+"/marshal", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(body)))
				for range b.N {
					if _, err := marshalStatusUpdateBody(state.object); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fixture.name+"/"+state.name+"/unmarshal", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(body)))
				for range b.N {
					decoded := &v1beta1.InferenceReplica{}
					if err := json.Unmarshal(body, decoded); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func collectStatusSizeMeasurements(t *testing.T) []statusSizeMeasurement {
	t.Helper()
	measurements := make([]statusSizeMeasurement, 0, len(statusSizeFixtures)*len(measurementInstanceCounts))
	for _, fixture := range statusSizeFixtures {
		for _, instances := range measurementInstanceCounts {
			measurements = append(measurements, measureStatusSize(t, fixture, instances))
		}
	}
	return measurements
}

func measureStatusSize(t *testing.T, fixture statusSizeFixture, instances int32) statusSizeMeasurement {
	t.Helper()
	observed := newStatusSizeIR(fixture, instances)
	persisted := observed.DeepCopy()
	irstatus.ClearPodDerivedObservations(persisted.Status.InstanceStatuses)
	if len(observed.ManagedFields) != 0 || len(persisted.ManagedFields) != 0 {
		t.Fatal("normalized fixtures must omit API-server-generated managedFields")
	}

	observedRequest := mustMarshalStatusUpdateBody(t, observed)
	persistedRequest := mustMarshalStatusUpdateBody(t, persisted)
	observedStatus := mustMarshalStatus(t, &observed.Status)
	persistedStatus := mustMarshalStatus(t, &persisted.Status)
	rows, err := json.Marshal(observed.Status.InstanceStatuses)
	if err != nil {
		t.Fatalf("marshal InstanceStatuses: %v", err)
	}
	readyBytes, scheduledBytes, nodesBytes, observationBytes := podObservationRequestBytes(t, observed)
	reductionBytes := len(observedRequest) - len(persistedRequest)
	if observationBytes != readyBytes+scheduledBytes+nodesBytes {
		t.Fatalf("Pod-derived observation marginal bytes = %d, want exact field sum %d", observationBytes, readyBytes+scheduledBytes+nodesBytes)
	}
	if reductionBytes != observationBytes {
		t.Fatalf("persisted request reduction = %d bytes, want exact three-field reduction %d", reductionBytes, observationBytes)
	}

	measurement := statusSizeMeasurement{
		fixture:                fixture.name,
		instances:              instances,
		observedRequestBytes:   len(observedRequest),
		persistedRequestBytes:  len(persistedRequest),
		observedStatusBytes:    len(observedStatus),
		persistedStatusBytes:   len(persistedStatus),
		instanceRowsBytes:      len(rows),
		readyPodCountBytes:     readyBytes,
		scheduledPodCountBytes: scheduledBytes,
		nodesOccupiedBytes:     nodesBytes,
		podObservationBytes:    observationBytes,
		operationBytes:         marginalRequestBytes(t, observed, clearOperations),
		lastFailureBytes:       marginalRequestBytes(t, observed, clearLastFailures),
		migrationsBytes:        marginalRequestBytes(t, observed, clearMigrations),
		reductionBytes:         reductionBytes,
	}
	return measurement
}

func renderStatusSizeCSV(t *testing.T, measurements []statusSizeMeasurement) string {
	t.Helper()
	var out bytes.Buffer
	w := csv.NewWriter(&out)
	header := []string{
		"fixture",
		"instances",
		"normalized_observed_request_bytes",
		"normalized_persisted_request_bytes",
		"observed_status_json_bytes",
		"persisted_status_json_bytes",
		"instance_rows_total_bytes",
		"ready_pod_count_marginal_bytes",
		"scheduled_pod_count_marginal_bytes",
		"nodes_occupied_marginal_bytes",
		"pod_observation_marginal_bytes",
		"operation_marginal_bytes",
		"last_failure_marginal_bytes",
		"top_level_migrations_marginal_bytes",
		"persisted_reduction_bytes",
		"persisted_reduction_percent",
	}
	if err := w.Write(header); err != nil {
		t.Fatalf("write CSV header: %v", err)
	}
	for _, m := range measurements {
		percent := float64(0)
		if m.observedRequestBytes > 0 {
			percent = float64(m.reductionBytes) * 100 / float64(m.observedRequestBytes)
		}
		record := []string{
			m.fixture,
			strconv.FormatInt(int64(m.instances), 10),
			strconv.Itoa(m.observedRequestBytes),
			strconv.Itoa(m.persistedRequestBytes),
			strconv.Itoa(m.observedStatusBytes),
			strconv.Itoa(m.persistedStatusBytes),
			strconv.Itoa(m.instanceRowsBytes),
			strconv.Itoa(m.readyPodCountBytes),
			strconv.Itoa(m.scheduledPodCountBytes),
			strconv.Itoa(m.nodesOccupiedBytes),
			strconv.Itoa(m.podObservationBytes),
			strconv.Itoa(m.operationBytes),
			strconv.Itoa(m.lastFailureBytes),
			strconv.Itoa(m.migrationsBytes),
			strconv.Itoa(m.reductionBytes),
			fmt.Sprintf("%.2f", percent),
		}
		if err := w.Write(record); err != nil {
			t.Fatalf("write CSV record: %v", err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatalf("flush CSV: %v", err)
	}
	return out.String()
}

func newStatusSizeIR(fixture statusSizeFixture, instances int32) *v1beta1.InferenceReplica {
	created := measurementTime()
	maxUnavailable := intstr.FromString("10%")
	replicas := instances
	ir := &v1beta1.InferenceReplica{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       "InferenceReplica",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "example-engine",
			Namespace:         "default",
			UID:               k8stypes.UID("example-engine-uid"),
			ResourceVersion:   "123456",
			Generation:        7,
			CreationTimestamp: created,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "example",
				constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			Finalizers: []string{TeardownFinalizer},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       "example",
				UID:        k8stypes.UID("example-isvc-uid"),
				Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: "example"},
			Component: v1beta1.EngineComponent,
			Replicas:  &replicas,
			Runners:   measurementRunners(fixture.podsPerInstance),
			Pacing: &v1beta1.InferenceReplicaPacing{
				Partition:      ptr.To(int32(0)),
				MaxUnavailable: &maxUnavailable,
			},
			MinReadySeconds: 10,
		},
	}
	if fixture.podsPerInstance > 1 {
		topologyKey := corev1.LabelTopologyZone
		ir.Spec.TopologyKey = &topologyKey
	}

	ir.Status = measurementStatus(fixture, instances)
	return ir
}

func measurementRunners(podsPerInstance int32) []v1beta1.Runner {
	template := func(role v1beta1.RunnerName) corev1.PodTemplateSpec {
		return corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				constants.OMEComponentLabel: string(v1beta1.EngineComponent),
				"ome.io/runner":             string(role),
			}},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  constants.MainContainerName,
					Image: "example.com/ome/model-server:v1",
					Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8000}},
					Env:   []corev1.EnvVar{{Name: "MODEL_ID", Value: "example/model"}},
				}},
			},
		}
	}
	if podsPerInstance == 1 {
		return []v1beta1.Runner{{
			Name:     v1beta1.RunnerNameDefault,
			Size:     1,
			Template: template(v1beta1.RunnerNameDefault),
		}}
	}
	return []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: template(v1beta1.RunnerNameLeader)},
		{Name: v1beta1.RunnerNameWorker, Size: podsPerInstance - 1, Template: template(v1beta1.RunnerNameWorker)},
	}
}

func measurementStatus(fixture statusSizeFixture, instances int32) v1beta1.InferenceReplicaStatus {
	updateRevision := measurementCurrentRevision
	if fixture.shape == shapeCreating || fixture.shape == shapeUpdatingInPlace || fixture.shape == shapeUpdatingSurge {
		updateRevision = measurementUpdateRevision
	}
	status := v1beta1.InferenceReplicaStatus{
		ObservedGeneration: 7,
		CurrentRevision:    measurementCurrentRevision,
		UpdateRevision:     updateRevision,
		CollisionCount:     ptr.To(int32(1)),
		LabelSelector: fmt.Sprintf("%s=example,%s=%s,%s=%s",
			constants.InferenceServicePodLabelKey,
			constants.OMEComponentLabel, v1beta1.EngineComponent,
			query.LabelManagedBy, query.ManagedByOMENative),
		InstanceStatuses: make([]v1beta1.OMENativeInstanceStatus, 0, instances),
	}
	for index := int32(0); index < instances; index++ {
		status.InstanceStatuses = append(status.InstanceStatuses, measurementInstanceStatus(fixture, index, instances))
	}
	if fixture.shape == shapeFailureRealistic || fixture.shape == shapeFailureLarge {
		firstFailure := measurementTime()
		lastFailure := metav1.NewTime(firstFailure.Add(5 * time.Minute))
		nextRetry := metav1.NewTime(lastFailure.Add(10 * time.Minute))
		status.RetryBlocks = []v1beta1.RetryBlock{{
			TargetRevision:  measurementCurrentRevision,
			State:           v1beta1.RetryBlockBackoff,
			AttemptsStarted: 2,
			NextRetryAt:     &nextRetry,
			FirstFailureAt:  &firstFailure,
			LastFailureAt:   &lastFailure,
			Reason:          "container-restart",
		}}
	}
	if fixture.shape == shapeMigrationHeavy {
		status.Migrations = measurementMigrations(instances)
	}
	measurementAggregateStatus(&status, fixture.podsPerInstance)
	return status
}

func measurementInstanceStatus(fixture statusSizeFixture, index, instances int32) v1beta1.OMENativeInstanceStatus {
	status := v1beta1.OMENativeInstanceStatus{
		Index:           index,
		Incarnation:     1,
		RunningRevision: measurementCurrentRevision,
	}
	switch fixture.shape {
	case shapeSteady:
		setMeasurementPodObservation(&status, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, true)
		status.Phase = v1beta1.OMENativeInstanceReady
	case shapeCreating:
		status.Phase = v1beta1.OMENativeInstanceCreating
		status.RunningRevision = ""
		status.TargetRevision = measurementUpdateRevision
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationCreate, "CreatePods", measurementUpdateRevision, false)
	case shapeUpdatingInPlace:
		status.Phase = v1beta1.OMENativeInstanceUpdating
		status.TargetRevision = measurementUpdateRevision
		setMeasurementPodObservation(&status, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance-1, fixture.podsPerInstance-1, fixture.podsPerInstance, true)
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationUpdate, "WaitReady", measurementUpdateRevision, false)
	case shapeUpdatingSurge:
		status.Phase = v1beta1.OMENativeInstanceUpdating
		status.TargetRevision = measurementUpdateRevision
		setMeasurementPodObservation(&status, 2, 2, 1, 1, 2, true)
		status.ActiveOrdinal = index % 2
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationUpdate, "DrainOld", measurementUpdateRevision, false)
	case shapeDeleting:
		status.Phase = v1beta1.OMENativeInstanceDeleting
		setMeasurementPodObservation(&status, fixture.podsPerInstance, fixture.podsPerInstance, 0, 0, fixture.podsPerInstance, true)
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationDelete, "Drain", measurementCurrentRevision, false)
	case shapeKueueGated:
		status.Phase = v1beta1.OMENativeInstanceCreating
		status.PodCount = fixture.podsPerInstance
		status.TargetRevision = measurementCurrentRevision
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationCreate, "WaitReady", measurementCurrentRevision, true)
	case shapeFailureRealistic, shapeFailureLarge:
		status.Phase = v1beta1.OMENativeInstanceRestarting
		setMeasurementPodObservation(&status, fixture.podsPerInstance-1, fixture.podsPerInstance-2, fixture.podsPerInstance-2, fixture.podsPerInstance-2, fixture.podsPerInstance-1, true)
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationRestart, "DeletePods", measurementCurrentRevision, false)
		status.LastFailure = measurementFailure(index, fixture.shape == shapeFailureLarge)
	case shapeMigrationHeavy:
		status.Phase = v1beta1.OMENativeInstanceMigrating
		setMeasurementPodObservation(&status, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, true)
		operation := measurementOperation(index, v1beta1.InstanceOperationMigrate, "WaitReady", measurementCurrentRevision, false)
		operation.SurgeIndex = ptr.To(instances + index)
		operation.FromNode = measurementNodes(index, 1)[0]
		operation.HintTargetNodes = []string{"target-node-a", "target-node-b"}
		operation.RequestUUID = fmt.Sprintf("migration-%04d", index)
		status.Operation = operation
	default:
		panic(fmt.Sprintf("unsupported measurement shape %q", fixture.shape))
	}
	return status
}

func setMeasurementPodObservation(status *v1beta1.OMENativeInstanceStatus, podCount, ready, serving, available, scheduled int32, admitted bool) {
	status.PodCount = podCount
	status.ReadyPodCount = ready
	status.ServingPodCount = serving
	status.AvailablePodCount = available
	status.ScheduledPodCount = scheduled
	status.Admitted = admitted
	status.NodesOccupied = measurementNodes(status.Index, scheduled)
}

func measurementOperation(index int32, operationType v1beta1.InstanceOperationType, step, targetRevision string, parked bool) *v1beta1.InstanceOperation {
	started := measurementTime()
	progress := metav1.NewTime(started.Add(2 * time.Minute))
	deadline := metav1.NewTime(started.Add(30 * time.Minute))
	if parked {
		deadline = metav1.Time{}
	}
	return &v1beta1.InstanceOperation{
		ID:             fmt.Sprintf("%s-%04d", strings.ToLower(string(operationType)), index),
		Type:           operationType,
		Step:           step,
		StartedAt:      started,
		LastProgressAt: progress,
		Deadline:       deadline,
		TargetRevision: targetRevision,
		Reason:         "measurement-fixture",
	}
}

func measurementFailure(index int32, large bool) *v1beta1.InstanceTermination {
	exitCode := int32(137)
	message := "container exited after exceeding its memory limit"
	if large {
		message = strings.Repeat("x", measurementLargeMessageLen)
	}
	return &v1beta1.InstanceTermination{
		PodName:       fmt.Sprintf("example-engine-%04d-worker-6", index),
		ContainerName: constants.MainContainerName,
		Reason:        "OOMKilled",
		ExitCode:      &exitCode,
		Message:       message,
		Time:          measurementTime(),
	}
}

func measurementMigrations(instances int32) []v1beta1.MigrationStatus {
	started := measurementTime()
	deadline := metav1.NewTime(started.Add(time.Hour))
	phases := []v1beta1.MigrationPhase{
		v1beta1.MigrationPhaseAccepted,
		v1beta1.MigrationPhaseSurgePending,
		v1beta1.MigrationPhaseSurgeReady,
		v1beta1.MigrationPhaseDraining,
	}
	migrations := make([]v1beta1.MigrationStatus, 0, instances)
	for index := int32(0); index < instances; index++ {
		phase := phases[int(index)%len(phases)]
		migration := v1beta1.MigrationStatus{
			RequestUUID:     fmt.Sprintf("migration-%04d", index),
			Trigger:         v1beta1.MigrationTriggerManual,
			SourceInstance:  index,
			FromNode:        measurementNodes(index, 1)[0],
			HintTargetNodes: []string{"target-node-a", "target-node-b"},
			Phase:           phase,
			Attempt:         1,
			Reason:          "node-maintenance",
			Message:         "replacement is progressing",
			StartedAt:       started,
			Deadline:        deadline,
		}
		if phase != v1beta1.MigrationPhaseAccepted {
			migration.SurgeInstance = ptr.To(instances + index)
			allocated := metav1.NewTime(started.Add(time.Minute))
			migration.AllocatedAt = &allocated
		}
		migrations = append(migrations, migration)
	}
	return migrations
}

func measurementAggregateStatus(status *v1beta1.InferenceReplicaStatus, expectedPods int32) {
	status.Replicas = int32(len(status.InstanceStatuses))
	for _, instance := range status.InstanceStatuses {
		ready := instance.PodCount >= expectedPods && instance.ReadyPodCount >= expectedPods
		serving := instance.PodCount >= expectedPods && instance.ServingPodCount >= expectedPods
		available := instance.PodCount >= expectedPods && instance.AvailablePodCount >= expectedPods
		updated := instance.RunningRevision == status.UpdateRevision
		if ready {
			status.ReadyReplicas++
		}
		if serving {
			status.ServingReplicas++
		}
		if available {
			status.AvailableReplicas++
		}
		if updated {
			status.UpdatedReplicas++
			if ready {
				status.UpdatedReadyReplicas++
			}
		}
	}
	readyCondition := metav1.ConditionFalse
	reason := "InstancesNotReady"
	if status.ReadyReplicas == status.Replicas {
		readyCondition = metav1.ConditionTrue
		reason = "AllInstancesReady"
	}
	status.Conditions = []metav1.Condition{{
		Type:               "Ready",
		Status:             readyCondition,
		ObservedGeneration: status.ObservedGeneration,
		LastTransitionTime: measurementTime(),
		Reason:             reason,
		Message:            "measurement fixture aggregate",
	}}
}

func measurementNodes(index, count int32) []string {
	if count <= 0 {
		return nil
	}
	nodes := make([]string, 0, count)
	for ordinal := int32(0); ordinal < count; ordinal++ {
		nodes = append(nodes, fmt.Sprintf("worker-%04d-%02d.example", index, ordinal))
	}
	return nodes
}

func measurementTime() metav1.Time {
	return metav1.NewTime(time.Date(2026, time.August, 17, 12, 0, 0, 0, time.UTC))
}

func clearReadyPodCount(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].ReadyPodCount = 0
	}
}

func clearScheduledPodCount(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].ScheduledPodCount = 0
	}
}

func clearNodesOccupied(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].NodesOccupied = nil
	}
}

func clearOperations(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].Operation = nil
	}
}

func clearLastFailures(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].LastFailure = nil
	}
}

func clearMigrations(ir *v1beta1.InferenceReplica) {
	ir.Status.Migrations = nil
}

func marginalRequestBytes(t *testing.T, ir *v1beta1.InferenceReplica, clear func(*v1beta1.InferenceReplica)) int {
	t.Helper()
	before := len(mustMarshalStatusUpdateBody(t, ir))
	without := ir.DeepCopy()
	clear(without)
	after := len(mustMarshalStatusUpdateBody(t, without))
	if after > before {
		t.Fatalf("clearing a status field grew the request from %d to %d bytes", before, after)
	}
	return before - after
}

func podObservationRequestBytes(t *testing.T, ir *v1beta1.InferenceReplica) (ready, scheduled, nodes, combined int) {
	t.Helper()
	projected := ir.DeepCopy()
	before := len(mustMarshalStatusUpdateBody(t, projected))
	initial := before

	clearReadyPodCount(projected)
	after := len(mustMarshalStatusUpdateBody(t, projected))
	ready = before - after
	before = after

	clearScheduledPodCount(projected)
	after = len(mustMarshalStatusUpdateBody(t, projected))
	scheduled = before - after
	before = after

	clearNodesOccupied(projected)
	after = len(mustMarshalStatusUpdateBody(t, projected))
	nodes = before - after
	combined = initial - after
	return ready, scheduled, nodes, combined
}

func mustMarshalStatusUpdateBody(t *testing.T, ir *v1beta1.InferenceReplica) []byte {
	t.Helper()
	body, err := marshalStatusUpdateBody(ir)
	if err != nil {
		t.Fatalf("marshal status update body: %v", err)
	}
	return body
}

func marshalStatusUpdateBody(ir *v1beta1.InferenceReplica) ([]byte, error) {
	encoder, err := statusUpdateEncoder()
	if err != nil {
		return nil, err
	}
	return k8sruntime.Encode(encoder, ir)
}

func statusUpdateEncoder() (k8sruntime.Encoder, error) {
	measurementEncoderOnce.Do(func() {
		scheme := k8sruntime.NewScheme()
		if err := v1beta1.AddToScheme(scheme); err != nil {
			measurementEncoderErr = fmt.Errorf("register OME API: %w", err)
			return
		}
		codecs := serializer.NewCodecFactory(scheme)
		negotiator := k8sruntime.NewClientNegotiator(
			serializer.WithoutConversionCodecFactory{CodecFactory: codecs},
			v1beta1.SchemeGroupVersion,
		)
		measurementEncoder, measurementEncoderErr = negotiator.Encoder(k8sruntime.ContentTypeJSON, nil)
	})
	return measurementEncoder, measurementEncoderErr
}

func mustMarshalStatus(t *testing.T, status *v1beta1.InferenceReplicaStatus) []byte {
	t.Helper()
	body, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	return body
}
