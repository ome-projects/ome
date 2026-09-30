package v1beta1testing

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ServingRuntimeWrapper carries a (namespaced) ServingRuntime plus
// chained setters.
type ServingRuntimeWrapper struct {
	v1beta1.ServingRuntime
}

// MakeServingRuntime returns a ServingRuntimeWrapper with the
// minimum-viable scaffold: one container in the pod spec (vendors
// rarely accept an empty container list) and an empty supported-
// formats slice.
func MakeServingRuntime(name, namespace string) *ServingRuntimeWrapper {
	return &ServingRuntimeWrapper{
		ServingRuntime: v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: "ghcr.io/test/runtime:test",
					}},
				},
			},
		},
	}
}

func (w *ServingRuntimeWrapper) Obj() *v1beta1.ServingRuntime { return &w.ServingRuntime }

func (w *ServingRuntimeWrapper) Clone() *ServingRuntimeWrapper {
	return &ServingRuntimeWrapper{ServingRuntime: *w.ServingRuntime.DeepCopy()}
}

// SupportsModelFormat appends an entry to spec.SupportedModelFormats.
// autoSelect controls whether the runtime is eligible for the
// runtime-selector's automatic-pick path (false = selector skips this
// runtime, must be referenced by name).
func (w *ServingRuntimeWrapper) SupportsModelFormat(formatName string, autoSelect bool, priority int32) *ServingRuntimeWrapper {
	auto := autoSelect
	prio := priority
	w.Spec.SupportedModelFormats = append(w.Spec.SupportedModelFormats, v1beta1.SupportedModelFormat{
		Name:           formatName,
		ModelFormat:    &v1beta1.ModelFormat{Name: formatName},
		ModelFramework: defaultModelFramework(),
		AutoSelect:     &auto,
		Priority:       &prio,
	})
	return w
}

// SupportsArchitecture appends a SupportedModelFormat keyed on the
// model's architecture (e.g. "LlamaForCausalLM"). The runtime
// selector matches on this when an InferenceService's referenced
// BaseModel has spec.modelArchitecture set.
func (w *ServingRuntimeWrapper) SupportsArchitecture(arch string, autoSelect bool, priority int32) *ServingRuntimeWrapper {
	auto := autoSelect
	prio := priority
	a := arch
	w.Spec.SupportedModelFormats = append(w.Spec.SupportedModelFormats, v1beta1.SupportedModelFormat{
		Name:              arch,
		ModelFormat:       &v1beta1.ModelFormat{Name: arch},
		ModelFramework:    defaultModelFramework(),
		ModelArchitecture: &a,
		AutoSelect:        &auto,
		Priority:          &prio,
	})
	return w
}

// Disabled toggles spec.Disabled (filters the runtime from auto-
// select even if SupportsModelFormat declared autoSelect=true).
func (w *ServingRuntimeWrapper) Disabled(d bool) *ServingRuntimeWrapper {
	val := d
	w.Spec.Disabled = &val
	return w
}

// Container appends to spec.Containers (the inline ServingRuntimePodSpec).
func (w *ServingRuntimeWrapper) Container(c corev1.Container) *ServingRuntimeWrapper {
	w.Spec.Containers = append(w.Spec.Containers, c)
	return w
}

// init registers a CleanupFunc that strips finalizers from any
// ServingRuntime in the namespace. ServingRuntime has no finalizer
// today but stripping any future ones prevents namespace-deletion
// hangs if the controller starts adding them.
func init() {
	RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
		srs := &v1beta1.ServingRuntimeList{}
		if err := c.List(ctx, srs, client.InNamespace(namespace)); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		for i := range srs.Items {
			sr := &srs.Items[i]
			if len(sr.Finalizers) == 0 {
				continue
			}
			sr.Finalizers = nil
			if err := c.Update(ctx, sr); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		return nil
	})
}
