package workload_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Headless Service tests for BuildHeadlessService + reconcileHeadlessService.
// The workload-side helpers consume a PerComponentServiceSpec directly,
// so each test constructs the spec inline against the expected outputs.
// No adapter glue, no owner-CRD imports.

// trueP is the controller-true pointer the OwnerReference fixture
// stamps. Local helper so tests don't need to import pointer.Bool.
func trueP() *bool { t := true; return &t }

// fixtureSpec is the canonical PerComponentServiceSpec the tests
// reconcile against. Mirrors what the ISVC-side
// omenative.BuildHeadlessServiceSpec adapter produces for
// (isvc=llama-70b, component=engine) so the workload tests assert
// the same per-Component shape as the legacy omenative tests did
// before the move.
func fixtureSpec() workloadtypes.PerComponentServiceSpec {
	selector := map[string]string{
		"ome.io/inferenceservice": "llama-70b",
		"component":               "engine",
		"ome.io/managed-by":       "OMENative",
	}
	return workloadtypes.PerComponentServiceSpec{
		Name:      "llama-70b-engine-headless",
		Namespace: "prod",
		Selector:  selector,
		Labels:    selector,
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "ome.io/v1beta1",
			Kind:       "InferenceService",
			Name:       "llama-70b",
			UID:        types.UID("llama-70b-uid"),
			Controller: trueP(),
		}},
	}
}

// ---------------------------------------------------------------------
// BuildHeadlessService — pure-compute shape assertions.
// ---------------------------------------------------------------------

func TestBuildHeadlessService_RejectsEmptyName(t *testing.T) {
	spec := fixtureSpec()
	spec.Name = ""
	if _, err := workload.BuildHeadlessService(spec); err == nil {
		t.Fatal("expected error for empty Name")
	}
}

func TestBuildHeadlessService_RejectsEmptyNamespace(t *testing.T) {
	spec := fixtureSpec()
	spec.Namespace = ""
	if _, err := workload.BuildHeadlessService(spec); err == nil {
		t.Fatal("expected error for empty Namespace")
	}
}

func TestBuildHeadlessService_HappyPath(t *testing.T) {
	spec := fixtureSpec()
	svc, err := workload.BuildHeadlessService(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if svc.Name != "llama-70b-engine-headless" {
		t.Errorf("Name: got %q want llama-70b-engine-headless", svc.Name)
	}
	if svc.Namespace != "prod" {
		t.Errorf("Namespace: got %q want prod", svc.Namespace)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("ClusterIP: got %q want %q (none)", svc.Spec.ClusterIP, corev1.ClusterIPNone)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("PublishNotReadyAddresses: want true")
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Type: got %q want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 0 {
		t.Errorf("Ports: got %d want 0 (headless DNS-only service)", len(svc.Spec.Ports))
	}
}

func TestBuildHeadlessService_SelectorAndLabels(t *testing.T) {
	spec := workloadtypes.PerComponentServiceSpec{
		Name:      "llama-70b-decoder-headless",
		Namespace: "prod",
		Selector: map[string]string{
			"ome.io/inferenceservice": "llama-70b",
			"component":               "decoder",
			"ome.io/managed-by":       "OMENative",
		},
		Labels: map[string]string{
			"ome.io/inferenceservice": "llama-70b",
			"component":               "decoder",
			"ome.io/managed-by":       "OMENative",
		},
	}
	svc, err := workload.BuildHeadlessService(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff := cmp.Diff(spec.Selector, svc.Spec.Selector); diff != "" {
		t.Errorf("Selector (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(spec.Labels, svc.Labels); diff != "" {
		t.Errorf("Labels (-want +got):\n%s", diff)
	}
}

func TestBuildHeadlessService_OwnerReference(t *testing.T) {
	spec := fixtureSpec()
	svc, err := workload.BuildHeadlessService(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(svc.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(svc.OwnerReferences))
	}
	ref := svc.OwnerReferences[0]
	if ref.Name != "llama-70b" || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef: got Kind=%q Name=%q want InferenceService/llama-70b", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("OwnerRef.Controller: want true, got %v", ref.Controller)
	}
}

// ---------------------------------------------------------------------
// ReconcileHeadlessService — wired-through CreateOrUpdate behavior
// against the controller-runtime fake client.
// ---------------------------------------------------------------------

// newServiceClient builds a fake client wired with the corev1.Service
// scheme. Owner-CRD types aren't required because the spec carries the
// OwnerReference directly (no GVK resolution from a typed owner
// happens at this layer; that's the adapter's job).
func newServiceClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
}

func TestReconcileHeadlessService_FreshCreate(t *testing.T) {
	c := newServiceClient(t)
	spec := fixtureSpec()

	if err := workload.ReconcileHeadlessService(context.Background(), c, spec); err != nil {
		t.Fatalf("first call: %v", err)
	}

	got := &corev1.Service{}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine-headless"}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatalf("get service after reconcile: %v", err)
	}

	if got.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("ClusterIP mismatch: got=%q want=%q", got.Spec.ClusterIP, corev1.ClusterIPNone)
	}
	if !got.Spec.PublishNotReadyAddresses {
		t.Errorf("PublishNotReadyAddresses should be true")
	}
	if got.Spec.Selector["ome.io/managed-by"] != "OMENative" {
		t.Errorf("managed-by selector missing or wrong: %v", got.Spec.Selector)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].UID != spec.OwnerReferences[0].UID {
		t.Errorf("owner reference not stamped: %v", got.OwnerReferences)
	}
}

func TestReconcileHeadlessService_Idempotent(t *testing.T) {
	c := newServiceClient(t)
	spec := fixtureSpec()

	if err := workload.ReconcileHeadlessService(context.Background(), c, spec); err != nil {
		t.Fatalf("first call: %v", err)
	}
	first := &corev1.Service{}
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine-headless"}
	if err := c.Get(context.Background(), key, first); err != nil {
		t.Fatalf("get after first reconcile: %v", err)
	}
	firstRV := first.ResourceVersion

	// Second call must not bump ResourceVersion — there's nothing to
	// update because the desired shape already matches.
	if err := workload.ReconcileHeadlessService(context.Background(), c, spec); err != nil {
		t.Fatalf("second call: %v", err)
	}
	second := &corev1.Service{}
	if err := c.Get(context.Background(), key, second); err != nil {
		t.Fatalf("get after second reconcile: %v", err)
	}
	if second.ResourceVersion != firstRV {
		t.Errorf("Service was updated on second idempotent reconcile (RV %s -> %s)", firstRV, second.ResourceVersion)
	}
}

func TestReconcileHeadlessService_DriftCorrection(t *testing.T) {
	c := newServiceClient(t)
	spec := fixtureSpec()

	if err := workload.ReconcileHeadlessService(context.Background(), c, spec); err != nil {
		t.Fatalf("seed reconcile: %v", err)
	}

	// Simulate an operator flipping PublishNotReadyAddresses to false
	// and overwriting the Selector. The next reconcile must fix both.
	key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine-headless"}
	svc := &corev1.Service{}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Fatalf("get before mutation: %v", err)
	}
	svc.Spec.PublishNotReadyAddresses = false
	svc.Spec.Selector = map[string]string{"hacked": "true"}
	if err := c.Update(context.Background(), svc); err != nil {
		t.Fatalf("mutate service: %v", err)
	}

	if err := workload.ReconcileHeadlessService(context.Background(), c, spec); err != nil {
		t.Fatalf("drift-correcting reconcile: %v", err)
	}

	fixed := &corev1.Service{}
	if err := c.Get(context.Background(), key, fixed); err != nil {
		t.Fatalf("get after drift correction: %v", err)
	}
	if !fixed.Spec.PublishNotReadyAddresses {
		t.Errorf("drift not corrected: PublishNotReadyAddresses still false")
	}
	if fixed.Spec.Selector["ome.io/managed-by"] != "OMENative" {
		t.Errorf("drift not corrected: selector still %v", fixed.Spec.Selector)
	}
}

// TestReconcileHeadlessService_KeepsClusterIPNoneAcrossPasses pins the
// post-create ClusterIP contract for the headless flavor: the apiserver
// preserves Spec.ClusterIP=None across the controller's idempotent
// reconciles. The reconciler's source-code-side immutability guard
// (only stamp ClusterIP when CreationTimestamp.IsZero()) is what keeps
// the field from being re-sent to the apiserver as a no-op patch on a
// real cluster; the fake client used here doesn't enforce the
// immutability check, so this test asserts the observable end-state
// (ClusterIP stays None) rather than introspecting the patch payload.
//
// envtest is the right harness for the true round-trip
// "Service.ClusterIP is immutable after create" check; that's covered
// by the integration suite in tests/integration/.
func TestReconcileHeadlessService_KeepsClusterIPNoneAcrossPasses(t *testing.T) {
	c := newServiceClient(t)
	spec := fixtureSpec()

	for i := 0; i < 3; i++ {
		if err := workload.ReconcileHeadlessService(context.Background(), c, spec); err != nil {
			t.Fatalf("reconcile pass %d: %v", i, err)
		}
		got := &corev1.Service{}
		key := client.ObjectKey{Namespace: "prod", Name: "llama-70b-engine-headless"}
		if err := c.Get(context.Background(), key, got); err != nil {
			t.Fatalf("get after pass %d: %v", i, err)
		}
		if got.Spec.ClusterIP != corev1.ClusterIPNone {
			t.Errorf("pass %d: ClusterIP drifted: got %q want %q", i, got.Spec.ClusterIP, corev1.ClusterIPNone)
		}
	}
}

func TestReconcileHeadlessService_NilClientRejected(t *testing.T) {
	if err := workload.ReconcileHeadlessService(context.Background(), nil, fixtureSpec()); err == nil {
		t.Fatalf("expected error on nil client")
	}
}

func TestReconcileHeadlessService_EmptySpecRejected(t *testing.T) {
	c := newServiceClient(t)
	if err := workload.ReconcileHeadlessService(context.Background(), c, workloadtypes.PerComponentServiceSpec{}); err == nil {
		t.Fatalf("expected error on empty spec")
	}
}
