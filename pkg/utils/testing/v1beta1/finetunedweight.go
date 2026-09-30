package v1beta1testing

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// FineTunedWeightWrapper wraps a FineTunedWeight for chained setters.
// FineTunedWeight is cluster-scoped, so the builder takes only Name —
// no namespace argument.
type FineTunedWeightWrapper struct {
	v1beta1.FineTunedWeight
}

// MakeFineTunedWeight returns a FineTunedWeightWrapper with a minimal
// valid spec scaffold. Defaults that satisfy the CRD's required-field
// validation:
//   - Storage{} is initialized so StorageURI / etc. can chain without
//     nil-checking.
//   - HyperParameters is seeded with an empty `{}` JSON object —
//     marked required by the CRD, so a nil RawExtension is rejected.
//   - TrainingJobRef.Name is given a placeholder value — the parent
//     field is `omitempty` but the struct is a value type, so Go
//     serializes it even when otherwise empty, and the CRD then
//     enforces .name as required. Tests don't use this field.
func MakeFineTunedWeight(name string) *FineTunedWeightWrapper {
	placeholder := "ftw-test-training-job"
	return &FineTunedWeightWrapper{
		FineTunedWeight: v1beta1.FineTunedWeight{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: v1beta1.FineTunedWeightSpec{
				Storage:         &v1beta1.StorageSpec{},
				HyperParameters: runtime.RawExtension{Raw: []byte(`{}`)},
				TrainingJobRef:  v1beta1.ObjectReference{Name: &placeholder},
			},
		},
	}
}

// Obj returns the underlying API type pointer.
func (w *FineTunedWeightWrapper) Obj() *v1beta1.FineTunedWeight { return &w.FineTunedWeight }

// Clone deep-copies the wrapped FTW for variations across subtests.
func (w *FineTunedWeightWrapper) Clone() *FineTunedWeightWrapper {
	return &FineTunedWeightWrapper{FineTunedWeight: *w.FineTunedWeight.DeepCopy()}
}

// StorageURI sets spec.Storage.StorageUri.
func (w *FineTunedWeightWrapper) StorageURI(uri string) *FineTunedWeightWrapper {
	if w.Spec.Storage == nil {
		w.Spec.Storage = &v1beta1.StorageSpec{}
	}
	u := uri
	w.Spec.Storage.StorageUri = &u
	return w
}

// ModelType sets spec.ModelType (e.g. "LoRA", "Adapter", "Tfew").
func (w *FineTunedWeightWrapper) ModelType(t string) *FineTunedWeightWrapper {
	val := t
	w.Spec.ModelType = &val
	return w
}

// BaseModelRef sets spec.BaseModelRef. Pass an empty namespace to
// reference a ClusterBaseModel; pass a non-empty namespace to
// reference a namespaced BaseModel in that namespace.
func (w *FineTunedWeightWrapper) BaseModelRef(name, namespace string) *FineTunedWeightWrapper {
	n := name
	w.Spec.BaseModelRef = v1beta1.ObjectReference{Name: &n}
	if namespace != "" {
		ns := namespace
		w.Spec.BaseModelRef.Namespace = &ns
	}
	return w
}
