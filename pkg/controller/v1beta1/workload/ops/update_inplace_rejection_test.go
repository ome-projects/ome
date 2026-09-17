package ops

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestUpdateInPlace_InvalidPatch_DisposesAndHoldsRevision: when the
// apiserver refuses the in-place patch as Invalid, the target revision
// cannot be applied to a live pod at all — the same permanent, workload-
// caused answer a rejected create gets. The attempt is disposed
// (Operation cleared, Phase=Failed) and the revision is held for retry,
// instead of the patch being retried on blind backoff until the
// operation deadline reports a misleading timeout.
func TestUpdateInPlace_InvalidPatch_DisposesAndHoldsRevision(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	spec := legacyTargetSpecImage("example.com/app:v2")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "example.com/app:v1")

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, v1beta1.AddToScheme, discoveryv1.AddToScheme, appsv1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("build scheme: %v", err)
		}
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir, pod).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if p, ok := obj.(*corev1.Pod); ok {
					return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, p.Name, field.ErrorList{
						field.Invalid(field.NewPath("spec", "containers").Index(0).Child("image"), "example.com/app:v2", "must be a valid image reference"),
					})
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	legacySeedRunningRevisionWithMeta(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("example.com/app:v1"), nil)
	target := legacyEnsureTargetCR(t, c, isvc, spec)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = target.Name
	var writes []struct {
		rev   string
		block workload.RetryBlock
	}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			writes = append(writes, struct {
				rev   string
				block workload.RetryBlock
			}{rev: rev, block: b})
		}
		return nil
	}
	markNotReady := false
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: &markNotReady})

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target, spec)
	if err != nil {
		t.Fatalf("Update: %v (a permanent rejection is disposed, not returned)", err)
	}
	if done {
		t.Fatal("Update reported done on a rejected patch")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Fatalf("instance 0: got phase=%q op=%+v want Failed with the operation cleared", s.Phase, s.Operation)
	}
	if s.LastFailure == nil || s.LastFailure.Reason != workload.RejectionReasonInvalidPodSpec {
		t.Fatalf("LastFailure: got %+v want reason=%s", s.LastFailure, workload.RejectionReasonInvalidPodSpec)
	}
	if len(writes) != 1 || writes[0].rev != target.Name {
		t.Fatalf("RetryBlock writes: got %+v want one for %s", writes, target.Name)
	}
}
