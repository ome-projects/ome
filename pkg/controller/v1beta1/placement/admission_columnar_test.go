package placement

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func columnarScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return scheme
}

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

// placementIR is irWithInstances with the lifecycle phase every controller
// row carries, so the rows are eligible for the ColumnarV2 representation.
func placementIR(component v1beta1.ComponentType, phase v1beta1.OMENativeInstancePhase, admitted ...bool) *v1beta1.InferenceReplica {
	ir := irWithInstances(component, admitted...)
	for i := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[i].Phase = phase
	}
	return ir
}

type placementDecisions struct {
	anyAdmitted      bool
	allAdmitted      bool
	admittedReplicas int32
	terminallyFailed bool
	engineRows       []v1beta1.OMENativeInstanceStatus
	decoderRows      []v1beta1.OMENativeInstanceStatus
}

func placementDecisionsFor(t *testing.T, reads client.Reader, isvc *v1beta1.InferenceService) placementDecisions {
	t.Helper()
	statuses, err := componentIRStatuses(context.Background(), reads, isvc)
	if err != nil {
		t.Fatalf("componentIRStatuses: %v", err)
	}
	return placementDecisions{
		anyAdmitted:      AnyInstanceAdmitted(statuses),
		allAdmitted:      AllComponentsAdmitted(isvc, statuses),
		admittedReplicas: splitAdmittedReplicas(declaredComponents(isvc), statuses),
		terminallyFailed: IsTerminallyFailed(isvc, statuses),
		engineRows:       statuses[v1beta1.EngineComponent].InstanceStatuses,
		decoderRows:      statuses[v1beta1.DecoderComponent].InstanceStatuses,
	}
}

// TestPlacementPredicatesDecodeColumnarV2Identically drives the placement
// predicates through componentIRStatuses against the same logical rows stored
// as DenseV1 and as ColumnarV2, through the production decoded accessor.
func TestPlacementPredicatesDecodeColumnarV2Identically(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc"}}
	declareComponent(isvc, v1beta1.EngineComponent)
	declareComponent(isvc, v1beta1.DecoderComponent)

	engine := placementIR(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, true, true, false)
	decoder := placementIR(v1beta1.DecoderComponent, v1beta1.OMENativeInstanceReady, true, false)
	decoder.Status.InstanceStatuses[1].Phase = v1beta1.OMENativeInstanceFailed

	dense := fake.NewClientBuilder().WithScheme(columnarScheme(t)).WithObjects(engine, decoder).Build()
	columnar := fake.NewClientBuilder().WithScheme(columnarScheme(t)).
		WithObjects(columnarTwin(t, engine), columnarTwin(t, decoder)).Build()
	reconciler := &Reconciler{InstanceStatusDecoder: irstatus.NewDecoder(8)}

	want := placementDecisionsFor(t, reconciler.instanceStatusReader(dense), isvc)
	got := placementDecisionsFor(t, reconciler.instanceStatusReader(columnar), isvc)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("placement decisions differ by stored encoding:\n dense:    %+v\n columnar: %+v", want, got)
	}
	if !want.anyAdmitted || !want.allAdmitted || want.admittedReplicas != 1 || !want.terminallyFailed {
		t.Fatalf("fixture does not exercise the predicates as intended: %+v", want)
	}
}

func TestPlacementPredicatesFailClosedOnUndecodableColumnarV2(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc"}}
	declareComponent(isvc, v1beta1.EngineComponent)
	engine := columnarTwin(t, placementIR(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, true, true))

	// A bound is configured but the stored payload is malformed.
	malformed := engine.DeepCopy()
	malformed.Status.InstanceStatusColumns.Members = "0,0"
	c := fake.NewClientBuilder().WithScheme(columnarScheme(t)).WithObjects(malformed).Build()
	bounded := &Reconciler{InstanceStatusDecoder: irstatus.NewDecoder(8)}
	if _, err := componentIRStatuses(context.Background(), bounded.instanceStatusReader(c), isvc); err == nil {
		t.Fatal("malformed ColumnarV2 payload must be a read error, not an empty status")
	} else if _, ok := irstatus.ErrorReasonOf(err); !ok {
		t.Fatalf("malformed payload error must carry a codec reason: %v", err)
	}

	// No bound configured: a valid payload still fails closed.
	c = fake.NewClientBuilder().WithScheme(columnarScheme(t)).WithObjects(engine).Build()
	unbounded := &Reconciler{}
	_, err := componentIRStatuses(context.Background(), unbounded.instanceStatusReader(c), isvc)
	if reason, ok := irstatus.ErrorReasonOf(err); !ok || reason != irstatus.ErrorReasonCardinalityLimit {
		t.Fatalf("unbounded decoder must fail closed with %s, got %v", irstatus.ErrorReasonCardinalityLimit, err)
	}
}
