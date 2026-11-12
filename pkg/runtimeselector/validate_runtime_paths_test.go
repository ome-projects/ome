package runtimeselector

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// ValidateRuntime is the gate for user-pinned runtimes: every error class it
// returns drives a distinct controller/webhook decision (hard reject vs
// advisory vs park), so each path is pinned individually.
func TestValidateRuntimeErrorPaths(t *testing.T) {
	fakeClient := fakeclient.New(t)
	selector := New(fakeClient)
	ctx := context.Background()

	model := &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "safetensors"}}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}}

	t.Run("missing runtime returns RuntimeNotFoundError naming runtime and namespace", func(t *testing.T) {
		err := selector.ValidateRuntime(ctx, "rt-absent", model, isvc)
		assert.Error(t, err)
		assert.True(t, IsRuntimeNotFoundError(err))
		assert.Contains(t, err.Error(), "rt-absent")
		assert.Contains(t, err.Error(), "default")
	})

	t.Run("disabled cluster runtime returns RuntimeDisabledError flagged cluster-scoped", func(t *testing.T) {
		csr := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{Name: "rt-cluster-disabled"},
			Spec:       v1beta1.ServingRuntimeSpec{Disabled: ptr(true)},
		}
		assert.NoError(t, fakeClient.Create(ctx, csr))

		err := selector.ValidateRuntime(ctx, "rt-cluster-disabled", model, isvc)
		assert.Error(t, err)
		var disabledErr *RuntimeDisabledError
		assert.True(t, errors.As(err, &disabledErr))
		assert.True(t, disabledErr.IsCluster)
		assert.Contains(t, err.Error(), "cluster-scoped")
	})

	t.Run("format mismatch returns RuntimeCompatibilityError with the mismatch reason", func(t *testing.T) {
		sr := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{Name: "rt-wrong-format", Namespace: "default"},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{ModelFormat: &v1beta1.ModelFormat{Name: "tensorrt"}},
				},
			},
		}
		assert.NoError(t, fakeClient.Create(ctx, sr))

		err := selector.ValidateRuntime(ctx, "rt-wrong-format", model, isvc)
		assert.Error(t, err)
		assert.True(t, IsRuntimeCompatibilityError(err))
		assert.Contains(t, err.Error(), "not in supported formats")
	})

	t.Run("size range excluding the model returns RuntimeCompatibilityError", func(t *testing.T) {
		sr := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{Name: "rt-too-small", Namespace: "default"},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{ModelFormat: &v1beta1.ModelFormat{Name: "safetensors"}},
				},
				ModelSizeRange: &v1beta1.ModelSizeRangeSpec{Min: ptr("1B"), Max: ptr("13B")},
			},
		}
		assert.NoError(t, fakeClient.Create(ctx, sr))

		bigModel := &v1beta1.BaseModelSpec{
			ModelFormat:        v1beta1.ModelFormat{Name: "safetensors"},
			ModelParameterSize: ptr("70B"),
		}
		err := selector.ValidateRuntime(ctx, "rt-too-small", bigModel, isvc)
		assert.Error(t, err)
		assert.True(t, IsRuntimeCompatibilityError(err))
		assert.Contains(t, err.Error(), "outside supported range")
	})

	t.Run("compatible runtime passes", func(t *testing.T) {
		sr := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{Name: "rt-good", Namespace: "default"},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{ModelFormat: &v1beta1.ModelFormat{Name: "safetensors"}},
				},
			},
		}
		assert.NoError(t, fakeClient.Create(ctx, sr))
		assert.NoError(t, selector.ValidateRuntime(ctx, "rt-good", model, isvc))
	})

	t.Run("declared ClusterServingRuntime kind validates the cluster runtime on a name collision", func(t *testing.T) {
		// Namespaced runtime is compatible; the same-named cluster runtime is
		// not. Only a cluster-scoped lookup can produce the mismatch error.
		sr := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{Name: "rt-collide", Namespace: "default"},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{ModelFormat: &v1beta1.ModelFormat{Name: "safetensors"}},
				},
			},
		}
		csr := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{Name: "rt-collide"},
			Spec: v1beta1.ServingRuntimeSpec{
				SupportedModelFormats: []v1beta1.SupportedModelFormat{
					{ModelFormat: &v1beta1.ModelFormat{Name: "tensorrt"}},
				},
			},
		}
		assert.NoError(t, fakeClient.Create(ctx, sr))
		assert.NoError(t, fakeClient.Create(ctx, csr))

		clusterKind := KindClusterServingRuntime
		declaringISVC := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
			Spec: v1beta1.InferenceServiceSpec{
				Runtime: &v1beta1.ServingRuntimeRef{Name: "rt-collide", Kind: &clusterKind},
			},
		}
		err := selector.ValidateRuntime(ctx, "rt-collide", model, declaringISVC)
		assert.Error(t, err)
		assert.True(t, IsRuntimeCompatibilityError(err),
			"declared kind must route validation to the cluster runtime, not the namespaced one")

		// Without a declared kind the namespaced runtime wins and validates.
		assert.NoError(t, selector.ValidateRuntime(ctx, "rt-collide", model, isvc))
	})
}

// Error strings are operator-facing (kubectl events, conditions, logs);
// pin the load-bearing fragments so message regressions surface in CI.
func TestErrorMessageRendering(t *testing.T) {
	t.Run("NoRuntimeFoundError lists excluded runtimes with their reasons", func(t *testing.T) {
		err := &NoRuntimeFoundError{
			ModelName:          "example-model",
			ModelFormat:        "safetensors",
			Namespace:          "team-a",
			TotalRuntimes:      2,
			NamespacedRuntimes: 1,
			ClusterRuntimes:    1,
			ExcludedRuntimes: map[string]error{
				"rt-one": errors.New("model format 'mt:onnx' not in supported formats"),
			},
		}
		msg := err.Error()
		assert.Contains(t, msg, "no runtime found to support model example-model")
		assert.Contains(t, msg, "namespace team-a")
		assert.Contains(t, msg, "Checked 2 runtimes (1 namespace-scoped, 1 cluster-scoped)")
		assert.Contains(t, msg, "Excluded runtimes: rt-one")
		assert.Contains(t, msg, "not in supported formats")
	})

	t.Run("NoRuntimeFoundError with zero runtimes omits the checked-count clause", func(t *testing.T) {
		err := &NoRuntimeFoundError{ModelName: "m", ModelFormat: "f", Namespace: "ns"}
		assert.NotContains(t, err.Error(), "Checked")
		assert.NotContains(t, err.Error(), "Excluded")
	})

	t.Run("RuntimeNotFoundError names both scopes searched", func(t *testing.T) {
		err := &RuntimeNotFoundError{RuntimeName: "rt-x", Namespace: "team-a"}
		assert.Contains(t, err.Error(), "rt-x")
		assert.Contains(t, err.Error(), "namespace team-a")
		assert.Contains(t, err.Error(), "cluster scope")
	})

	t.Run("RuntimeNotFoundError for a ServingRuntime-kind lookup names only the namespace", func(t *testing.T) {
		err := &RuntimeNotFoundError{RuntimeName: "rt-x", Namespace: "team-a", Kind: KindServingRuntime}
		assert.Contains(t, err.Error(), "ServingRuntime rt-x")
		assert.Contains(t, err.Error(), "namespace team-a")
		assert.NotContains(t, err.Error(), "cluster scope")
	})

	t.Run("RuntimeDisabledError renders the runtime scope", func(t *testing.T) {
		assert.Contains(t, (&RuntimeDisabledError{RuntimeName: "rt-a"}).Error(), "namespace-scoped runtime rt-a is disabled")
		assert.Contains(t, (&RuntimeDisabledError{RuntimeName: "rt-b", IsCluster: true}).Error(), "cluster-scoped runtime rt-b is disabled")
	})

	t.Run("RuntimeCompatibilityError wraps and renders its detailed cause", func(t *testing.T) {
		cause := errors.New("architecture mismatch")
		err := &RuntimeCompatibilityError{
			RuntimeName:   "rt-c",
			ModelName:     "m",
			Reason:        "declared formats do not match",
			DetailedError: cause,
		}
		assert.Contains(t, err.Error(), "declared formats do not match")
		assert.Contains(t, err.Error(), "architecture mismatch")
		assert.Equal(t, cause, errors.Unwrap(err))
	})

	t.Run("ModelValidationError and ConfigurationError render their subjects", func(t *testing.T) {
		mv := &ModelValidationError{Field: "modelFormat.name", Message: "model format name is required"}
		assert.Contains(t, mv.Error(), "modelFormat.name")

		ce := &ConfigurationError{Component: "scorer", Message: "weights must be positive"}
		assert.Contains(t, ce.Error(), "configuration error in scorer")
	})
}
