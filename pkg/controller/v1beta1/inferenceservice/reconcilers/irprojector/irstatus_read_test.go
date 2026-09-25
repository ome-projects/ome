package irprojector

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestComponentIRStatus_ReturnsAuthoritativeStatus(t *testing.T) {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: InferenceReplicaName("svc", v1beta1.EngineComponent)},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas: 3, ReadyReplicas: 2,
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).Build()

	got, err := ComponentIRStatus(context.Background(), c, "ns", "svc", v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got == nil || got.Replicas != 3 || len(got.InstanceStatuses) != 1 {
		t.Fatalf("want authoritative IR status (3 replicas, 1 instance); got %+v", got)
	}
}

func TestComponentIRStatus_MissingIRReturnsNil(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	got, err := ComponentIRStatus(context.Background(), c, "ns", "svc", v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("missing IR must not error; got %v", err)
	}
	if got != nil {
		t.Fatalf("missing IR must return nil status; got %+v", got)
	}
}

func TestComponentIRStatus_NilReaderReturnsNil(t *testing.T) {
	// A nil reader must degrade to "no authoritative status" (nil, nil)
	// rather than panicking a reconcile.
	got, err := ComponentIRStatus(context.Background(), nil, "ns", "svc", v1beta1.EngineComponent)
	if err != nil || got != nil {
		t.Fatalf("nil reader must return (nil, nil); got (%+v, %v)", got, err)
	}
}

// erroringReader fails every Get with a non-NotFound error.
type erroringReader struct {
	client.Reader
}

func (erroringReader) Get(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return errors.New("simulated apiserver failure")
}

func TestComponentIRStatus_ReadErrorPropagatesWrapped(t *testing.T) {
	// Only NotFound maps to (nil, nil). Any other read failure must
	// surface as a wrapped error so gate callers can fail closed instead
	// of mistaking a flaky read for "no observation yet".
	got, err := ComponentIRStatus(context.Background(), erroringReader{}, "ns", "svc", v1beta1.EngineComponent)
	if err == nil || got != nil {
		t.Fatalf("read error must propagate with nil status; got (%+v, %v)", got, err)
	}
	if !strings.Contains(err.Error(), "get InferenceReplica ns/") || !strings.Contains(err.Error(), "simulated apiserver failure") {
		t.Errorf("error should wrap the IR key and cause: %v", err)
	}
}

func TestComponentIRPartition_ReadsProjectedSpec(t *testing.T) {
	// The partition comes from the projected IR spec — the merged
	// ISVC↔runtime lifecycle — so a value the operator only set on the
	// ServingRuntime is still visible here.
	partition := int32(2)
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: InferenceReplicaName("svc", v1beta1.EngineComponent)},
		Spec: v1beta1.InferenceReplicaSpec{
			Lifecycle: &v1beta1.LifecycleSpec{
				UpdateStrategy: &v1beta1.UpdateStrategy{
					RollingUpdate: &v1beta1.RollingUpdate{Partition: &partition},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).Build()

	got, err := ComponentIRPartition(context.Background(), c, "ns", "svc", v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != 2 {
		t.Fatalf("partition: got %d want 2", got)
	}
}

func TestComponentIRPartition_UnsetLifecycleIsZero(t *testing.T) {
	// Every level of the lifecycle chain may be nil; all resolve to the
	// API-defined partition 0 ("update every Instance").
	for name, lc := range map[string]*v1beta1.LifecycleSpec{
		"nil lifecycle":      nil,
		"nil updateStrategy": {},
		"nil rollingUpdate":  {UpdateStrategy: &v1beta1.UpdateStrategy{}},
		"nil partition":      {UpdateStrategy: &v1beta1.UpdateStrategy{RollingUpdate: &v1beta1.RollingUpdate{}}},
	} {
		ir := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: InferenceReplicaName("svc", v1beta1.EngineComponent)},
			Spec:       v1beta1.InferenceReplicaSpec{Lifecycle: lc},
		}
		c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).Build()
		got, err := ComponentIRPartition(context.Background(), c, "ns", "svc", v1beta1.EngineComponent)
		if err != nil || got != 0 {
			t.Errorf("%s: got (%d, %v) want (0, nil)", name, got, err)
		}
	}
}

func TestComponentIRPartition_MissingIRIsZero(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	got, err := ComponentIRPartition(context.Background(), c, "ns", "svc", v1beta1.EngineComponent)
	if err != nil || got != 0 {
		t.Fatalf("missing IR must return (0, nil); got (%d, %v)", got, err)
	}
}

func TestComponentIRPartition_NilReaderIsZero(t *testing.T) {
	got, err := ComponentIRPartition(context.Background(), nil, "ns", "svc", v1beta1.EngineComponent)
	if err != nil || got != 0 {
		t.Fatalf("nil reader must return (0, nil); got (%d, %v)", got, err)
	}
}

func TestComponentIRPartition_ReadErrorPropagatesWrapped(t *testing.T) {
	// A transient read failure must not read as "no partition" — the
	// coordination callers fail closed on it.
	got, err := ComponentIRPartition(context.Background(), erroringReader{}, "ns", "svc", v1beta1.EngineComponent)
	if err == nil {
		t.Fatalf("read error must propagate; got (%d, nil)", got)
	}
	if !strings.Contains(err.Error(), "get InferenceReplica ns/") || !strings.Contains(err.Error(), "simulated apiserver failure") {
		t.Errorf("error should wrap the IR key and cause: %v", err)
	}
}

// TestEffectivePartition_PacingWinsOverLifecycle pins the source order every
// partition reader shares with the engine: the projected pacing partition
// wins when set — including an explicit 0, which releases every Instance
// over a user partition — and only a nil pacing partition defers to the
// user's lifecycle rollingUpdate partition.
func TestEffectivePartition_PacingWinsOverLifecycle(t *testing.T) {
	p := func(v int32) *int32 { return &v }
	lifecycle := &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
		RollingUpdate: &v1beta1.RollingUpdate{Partition: p(1)}}}
	cases := map[string]struct {
		lifecycle *v1beta1.LifecycleSpec
		pacing    *v1beta1.InferenceReplicaPacing
		want      *int32
	}{
		"neither":                         {nil, nil, nil},
		"lifecycle only":                  {lifecycle, nil, p(1)},
		"pacing only":                     {nil, &v1beta1.InferenceReplicaPacing{Partition: p(2)}, p(2)},
		"pacing wins":                     {lifecycle, &v1beta1.InferenceReplicaPacing{Partition: p(2)}, p(2)},
		"pacing 0 releases over user":     {lifecycle, &v1beta1.InferenceReplicaPacing{Partition: p(0)}, p(0)},
		"pacing without partition defers": {lifecycle, &v1beta1.InferenceReplicaPacing{}, p(1)},
	}
	for name, tc := range cases {
		got := EffectivePartition(tc.lifecycle, tc.pacing)
		switch {
		case got == nil && tc.want == nil:
		case got == nil || tc.want == nil || *got != *tc.want:
			t.Errorf("%s: EffectivePartition = %v, want %v", name, got, tc.want)
		}
	}
}

// TestComponentIRPartition_ReadsPacingFirst pins that the coordination-side
// reader sees the same number the engine holds at: a projected canary
// partition on spec.pacing is reported even when the user's lifecycle
// carries a different partition.
func TestComponentIRPartition_ReadsPacingFirst(t *testing.T) {
	user, canary := int32(1), int32(3)
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: InferenceReplicaName("svc", v1beta1.EngineComponent)},
		Spec: v1beta1.InferenceReplicaSpec{
			Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
				RollingUpdate: &v1beta1.RollingUpdate{Partition: &user}}},
			Pacing: &v1beta1.InferenceReplicaPacing{Partition: &canary},
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).Build()
	got, err := ComponentIRPartition(context.Background(), c, "ns", "svc", v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != 3 {
		t.Fatalf("partition: got %d want 3 (the projected pacing partition)", got)
	}
}
