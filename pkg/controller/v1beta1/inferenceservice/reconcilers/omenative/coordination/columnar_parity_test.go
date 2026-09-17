package coordination

import (
	"context"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// columnarTwin stores the same logical rows as ir in the ColumnarV2
// representation, exactly as a ColumnarV2-target writer would persist them.
func columnarTwin(t *testing.T, ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	t.Helper()
	twin := ir.DeepCopy()
	columns, err := irstatus.EncodeColumns(twin.Status.InstanceStatuses, uint64(len(twin.Status.InstanceStatuses)))
	if err != nil {
		t.Fatalf("encode columns for %s: %v", ir.Name, err)
	}
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	twin.Status.InstanceStatuses = nil
	twin.Status.InstanceStatusEncoding = &encoding
	twin.Status.InstanceStatusColumns = columns
	return twin
}

// columnarClientFrom rebuilds a fake client whose InferenceReplicas are the
// ColumnarV2 twins of those in dense, keeping every other object. The
// returned reader carries a decoder bound large enough for the fixtures.
func columnarClientFrom(t *testing.T, dense client.Client, extra ...client.Object) client.Reader {
	t.Helper()
	ctx := context.Background()
	irs := &v1beta1.InferenceReplicaList{}
	if err := dense.List(ctx, irs); err != nil {
		t.Fatalf("list InferenceReplicas: %v", err)
	}
	objs := append([]client.Object{}, extra...)
	for i := range irs.Items {
		twin := columnarTwin(t, &irs.Items[i])
		twin.ResourceVersion = ""
		objs = append(objs, twin)
	}
	isvcs := &v1beta1.InferenceServiceList{}
	if err := dense.List(ctx, isvcs); err == nil {
		for i := range isvcs.Items {
			isvc := isvcs.Items[i].DeepCopy()
			isvc.ResourceVersion = ""
			objs = append(objs, isvc)
		}
	}
	c := fake.NewClientBuilder().WithScheme(dense.Scheme()).WithObjects(objs...).Build()
	return irstatus.NewReader(c, irstatus.NewDecoder(64))
}

func withPhase(phase v1beta1.OMENativeInstancePhase, insts []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
	for i := range insts {
		insts[i].Phase = phase
	}
	return insts
}

type gateVerdict struct {
	allowed bool
	reason  string
}

// TestCoordinationGatesDecodeColumnarV2Identically runs the row-consuming
// coordination readers through their production decoded accessors against
// the same logical rows stored as DenseV1 and as ColumnarV2.
func TestCoordinationGatesDecodeColumnarV2Identically(t *testing.T) {
	ctx := context.Background()

	t.Run("pairing gate", func(t *testing.T) {
		isvc := pairingISVC("proto-b")
		engine := pairingIR("prod", "llama", v1beta1.EngineComponent,
			withPhase(v1beta1.OMENativeInstanceReady, servingInstances("llama", v1beta1.EngineComponent, "ea", "eb")))
		decoder := pairingIR("prod", "llama", v1beta1.DecoderComponent,
			withPhase(v1beta1.OMENativeInstanceReady, servingInstances("llama", v1beta1.DecoderComponent, "da")))
		crs := []client.Object{
			pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
			pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
			pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
		}
		dense := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(append([]client.Object{engine, decoder}, crs...)...).Build()
		columnar := columnarClientFrom(t, dense, crs...)

		verdict := func(reads client.Reader) gateVerdict {
			allowed, reason := ResolveGateContext(ctx, reads, isvc, v1beta1.EngineComponent).CheckPairing(workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
			return gateVerdict{allowed, reason}
		}
		want, got := verdict(dense), verdict(columnar)
		if want != got {
			t.Fatalf("pairing verdict differs by stored encoding:\n dense:    %+v\n columnar: %+v", want, got)
		}
		if want.allowed {
			t.Fatalf("fixture must exercise a denial, got %+v", want)
		}
	})

	t.Run("sequential gate", func(t *testing.T) {
		const name = "seq"
		isvc := mkSequentialFixture(name, 0)
		engine := seqIR(name, v1beta1.EngineComponent, "OLD", "NEW", 2, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: revName(name, v1beta1.EngineComponent, "OLD")},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revName(name, v1beta1.EngineComponent, "OLD")},
		})
		decoder := seqIR(name, v1beta1.DecoderComponent, "OLD", "NEW", 2, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: revName(name, v1beta1.DecoderComponent, "OLD")},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revName(name, v1beta1.DecoderComponent, "OLD")},
		})
		dense := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(engine, decoder).Build()
		columnar := columnarClientFrom(t, dense)

		for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
			verdict := func(reads client.Reader) gateVerdict {
				allowed, reason := CheckSequentialGate(ctx, reads, isvc, component)
				return gateVerdict{allowed, reason}
			}
			if want, got := verdict(dense), verdict(columnar); want != got {
				t.Fatalf("%s sequential verdict differs by stored encoding:\n dense:    %+v\n columnar: %+v", component, want, got)
			}
		}
	})

	t.Run("surge gate", func(t *testing.T) {
		maxSurge := intstr.FromInt(1)
		isvc := mkSurgeFixture(&maxSurge)
		insts := []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
			{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating, Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "WaitReady", SurgeIndex: new(int32)}},
		}
		dense := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{v1beta1.EngineComponent: insts})
		columnar := columnarClientFrom(t, dense)

		verdict := func(reads client.Reader) gateVerdict {
			allowed, reason := ResolveGateContext(ctx, reads, isvc, v1beta1.EngineComponent).CheckSurge(1)
			return gateVerdict{allowed, reason}
		}
		if want, got := verdict(dense), verdict(columnar); want != got {
			t.Fatalf("surge verdict differs by stored encoding:\n dense:    %+v\n columnar: %+v", want, got)
		}
	})

	t.Run("group observation", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "isvc", Namespace: "default"}}
		engine := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: "isvc-engine", Namespace: "default"},
			Status: v1beta1.InferenceReplicaStatus{
				Replicas:        3,
				ReadyReplicas:   2,
				CurrentRevision: "isvc-engine-aaaaaaaa",
				UpdateRevision:  "isvc-engine-bbbbbbbb",
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
					{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "isvc-engine-bbbbbbbb", Incarnation: 1},
					{Index: 1, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: "isvc-engine-aaaaaaaa", Incarnation: 2, LastFailure: &v1beta1.InstanceTermination{PodName: "isvc-engine-1", Reason: "Error"}},
					{Index: 2, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "isvc-engine-aaaaaaaa", Incarnation: 1},
				},
			},
		}
		decoder := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: "isvc-decoder", Namespace: "default"},
			Status: v1beta1.InferenceReplicaStatus{
				Replicas:         1,
				InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}},
			},
		}
		g := ResolvedGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Policy:     v1beta1.CoordinationPolicyBlueGreen,
		}
		dense := testClient(engine, decoder)
		columnar := columnarClientFrom(t, dense)
		pods := map[v1beta1.ComponentType]map[string]int32{}

		want, err := buildGroupObservation(ctx, dense, isvc, g, pods)
		if err != nil {
			t.Fatalf("dense observation: %v", err)
		}
		got, err := buildGroupObservation(ctx, columnar, isvc, g, pods)
		if err != nil {
			t.Fatalf("columnar observation: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("group observation differs by stored encoding:\n dense:    %+v\n columnar: %+v", want, got)
		}
		if !want.Components[v1beta1.EngineComponent].Failed {
			t.Fatalf("fixture must exercise the per-Instance failure rollup: %+v", want.Components[v1beta1.EngineComponent])
		}
	})
}

// TestCoordinationGatesFailClosedOnUndecodableColumnarV2 pins that a
// malformed or unbounded ColumnarV2 payload denies every row-consuming gate
// with a read failure instead of an empty observation.
func TestCoordinationGatesFailClosedOnUndecodableColumnarV2(t *testing.T) {
	ctx := context.Background()
	const name = "seq"
	isvc := mkSequentialFixture(name, 0)
	engine := seqIR(name, v1beta1.EngineComponent, "OLD", "NEW", 1, []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revName(name, v1beta1.EngineComponent, "OLD")},
	})
	decoder := seqIR(name, v1beta1.DecoderComponent, "OLD", "NEW", 1, []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revName(name, v1beta1.DecoderComponent, "OLD")},
	})
	malformedEngine := columnarTwin(t, engine)
	malformedEngine.Status.InstanceStatusColumns.Members = "0-0"
	malformed := irstatus.NewReader(
		fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(malformedEngine, columnarTwin(t, decoder)).Build(),
		irstatus.NewDecoder(64))
	unbounded := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(columnarTwin(t, engine), columnarTwin(t, decoder)).Build()

	for _, tc := range []struct {
		name  string
		reads client.Reader
		want  string
	}{
		{name: "malformed payload", reads: malformed, want: irstatus.ErrorReasonRangeOrder.Message()},
		{name: "no bound configured", reads: unbounded, want: irstatus.ErrorReasonCardinalityLimit.Message()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allowed, reason := CheckSequentialGate(ctx, tc.reads, isvc, v1beta1.DecoderComponent)
			if allowed || !strings.Contains(reason, tc.want) {
				t.Fatalf("sequential gate must deny with the codec reason %q, got allowed=%v reason=%q", tc.want, allowed, reason)
			}
			g := ResolvedGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Policy: v1beta1.CoordinationPolicyBlueGreen}
			if _, err := buildGroupObservation(ctx, tc.reads, isvc, g, map[v1beta1.ComponentType]map[string]int32{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("group observation must fail with the codec reason %q, got %v", tc.want, err)
			}
		})
	}
}
