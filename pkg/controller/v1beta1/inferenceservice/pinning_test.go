package inferenceservice

import (
	"context"
	"encoding/json"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	knapis "knative.dev/pkg/apis"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// stubRuntimeSelector overrides GetRuntime only; the embedded nil
// Selector panics on any other call, which no pinning path makes.
type stubRuntimeSelector struct {
	runtimeselector.Selector
	spec *v1beta1.ServingRuntimeSpec
	err  error
}

func (s *stubRuntimeSelector) GetRuntime(_ context.Context, _, _, _ string) (*v1beta1.ServingRuntimeSpec, bool, error) {
	return s.spec, false, s.err
}

// pinnedRevisionObj serializes spec into a ControllerRevision named
// revName in the OME namespace, labeled as a revision of runtimeName.
func pinnedRevisionObj(t *testing.T, revName, runtimeName string, spec *v1beta1.ServingRuntimeSpec) *appsv1.ControllerRevision {
	t.Helper()
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal pinned spec: %v", err)
	}
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      revName,
			Namespace: constants.OMENamespace,
			Labels: map[string]string{
				constants.RuntimeRevisionOfLabelKey: runtimeName,
			},
		},
		Data: runtime.RawExtension{Raw: raw},
	}
}

func pinningReconciler(t *testing.T, selector runtimeselector.Selector, objs ...*appsv1.ControllerRevision) *InferenceServiceReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1 to scheme: %v", err)
	}
	builder := ctrlclientfake.NewClientBuilder().WithScheme(scheme)
	for _, o := range objs {
		builder = builder.WithObjects(o)
	}
	return &InferenceServiceReconciler{
		Client:          builder.Build(),
		RuntimeSelector: selector,
	}
}

func pinnedISVC(runtimeName string) *v1beta1.InferenceService {
	autoSync := false
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "prod"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: runtimeName, AutoSync: &autoSync},
		},
	}
}

func findCondition(isvc *v1beta1.InferenceService, condType string) *knapis.Condition {
	for i := range isvc.Status.Conditions {
		if string(isvc.Status.Conditions[i].Type) == condType {
			return &isvc.Status.Conditions[i]
		}
	}
	return nil
}

// TestResolvePinnedRuntime_SourceDeleted_ServesPin pins the deleted-source
// case: a NotFound on the live runtime must not fail the reconcile while
// the pinned revision is intact. The pin keeps serving, with drift
// surfaced as a condition.
func TestResolvePinnedRuntime_SourceDeleted_ServesPin(t *testing.T) {
	const runtimeName = "vllm-rt"
	const revName = "csr-vllm-rt-abc123"

	rev := pinnedRevisionObj(t, revName, runtimeName, &v1beta1.ServingRuntimeSpec{})
	r := pinningReconciler(t, &stubRuntimeSelector{
		err: &runtimeselector.RuntimeNotFoundError{RuntimeName: runtimeName, Namespace: "prod"},
	}, rev)

	isvc := pinnedISVC(runtimeName)
	isvc.Status.PinnedRevisionName = revName

	pin, err := r.resolvePinnedRuntime(context.Background(), isvc)
	if err != nil {
		t.Fatalf("resolvePinnedRuntime returned error %v, want the pin to keep serving", err)
	}
	if pin.skipChildren {
		t.Error("skipChildren = true, want children reconciled from the pinned spec")
	}
	if pin.spec == nil {
		t.Fatal("pin.spec is nil, want the decoded pinned revision spec")
	}
	cond := findCondition(isvc, constants.RuntimeDriftedConditionType)
	if cond == nil {
		t.Fatal("RuntimeDrifted condition not set")
	}
	if cond.Status != v1.ConditionTrue || cond.Reason != "SourceRuntimeMissing" {
		t.Errorf("RuntimeDrifted = %s/%s, want True/SourceRuntimeMissing", cond.Status, cond.Reason)
	}
}

// TestResolvePinnedRuntime_SourceDeleted_ExplicitRevision covers the
// explicit spec.runtime.revision path: the named revision resolves, so
// a deleted source runtime must not block it either.
func TestResolvePinnedRuntime_SourceDeleted_ExplicitRevision(t *testing.T) {
	const runtimeName = "vllm-rt"
	const revName = "csr-vllm-rt-def456"

	rev := pinnedRevisionObj(t, revName, runtimeName, &v1beta1.ServingRuntimeSpec{})
	r := pinningReconciler(t, &stubRuntimeSelector{
		err: &runtimeselector.RuntimeNotFoundError{RuntimeName: runtimeName, Namespace: "prod"},
	}, rev)

	isvc := pinnedISVC(runtimeName)
	pin := revName
	isvc.Spec.Runtime.Revision = &pin

	res, err := r.resolvePinnedRuntime(context.Background(), isvc)
	if err != nil {
		t.Fatalf("resolvePinnedRuntime returned error %v, want the explicit pin to serve", err)
	}
	if res.skipChildren || res.spec == nil {
		t.Errorf("got skipChildren=%v spec=%v, want a served pin", res.skipChildren, res.spec)
	}
	if isvc.Status.PinnedRevisionName != revName {
		t.Errorf("PinnedRevisionName = %q, want %q", isvc.Status.PinnedRevisionName, revName)
	}
	cond := findCondition(isvc, constants.RuntimeDriftedConditionType)
	if cond == nil || cond.Reason != "SourceRuntimeMissing" {
		t.Errorf("RuntimeDrifted condition = %+v, want Reason=SourceRuntimeMissing", cond)
	}
}

// TestResolvePinnedRuntime_SourceDeleted_NoPin: with no pin and no live
// source there is nothing to serve from — the error must propagate.
func TestResolvePinnedRuntime_SourceDeleted_NoPin(t *testing.T) {
	const runtimeName = "vllm-rt"
	r := pinningReconciler(t, &stubRuntimeSelector{
		err: &runtimeselector.RuntimeNotFoundError{RuntimeName: runtimeName, Namespace: "prod"},
	})

	if _, err := r.resolvePinnedRuntime(context.Background(), pinnedISVC(runtimeName)); err == nil {
		t.Fatal("resolvePinnedRuntime succeeded with no pin and no live runtime, want error")
	}
}

// TestResolvePinnedRuntime_LiveError_Propagates: only NotFound is
// tolerated — transient live-resolution failures still error so the
// reconcile retries.
func TestResolvePinnedRuntime_LiveError_Propagates(t *testing.T) {
	const runtimeName = "vllm-rt"
	const revName = "csr-vllm-rt-abc123"

	rev := pinnedRevisionObj(t, revName, runtimeName, &v1beta1.ServingRuntimeSpec{})
	r := pinningReconciler(t, &stubRuntimeSelector{
		err: &runtimeselector.ConfigurationError{Component: "fetcher", Message: "boom"},
	}, rev)

	isvc := pinnedISVC(runtimeName)
	isvc.Status.PinnedRevisionName = revName

	if _, err := r.resolvePinnedRuntime(context.Background(), isvc); err == nil {
		t.Fatal("resolvePinnedRuntime swallowed a non-NotFound live-resolution error")
	}
}
