// Package v1beta1testing exposes builders for OME's v1beta1 CRDs in
// the chained-setter / .Obj() pattern Kueue popularized:
//
//	bm := v1beta1testing.MakeBaseModel("llama-7b", "models").
//	    StorageURI("pvc://my-pvc/models/llama").
//	    Obj()
//
// Builders set sane defaults (initialized maps, valid storage shapes
// where required) so callers don't trip nil-deref panics or apiserver
// validation errors. Each builder package's init() also registers a
// finalizer-stripping cleanup with this package's cleanup registry so
// per-test namespace deletion works without a central walker.
//
// Importable from production tooling as well as tests; nothing in
// this package depends on the test framework.
package v1beta1testing

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// BaseModelWrapper carries a BaseModel plus chained setters. Callers
// finish with .Obj() to extract the underlying *v1beta1.BaseModel.
type BaseModelWrapper struct {
	v1beta1.BaseModel
}

// MakeBaseModel returns a BaseModelWrapper with a minimal valid spec
// scaffold. Storage{} is initialized so StorageURI / etc. can chain
// without nil-checking.
func MakeBaseModel(name, namespace string) *BaseModelWrapper {
	return &BaseModelWrapper{
		BaseModel: v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{},
			},
		},
	}
}

// Obj returns the underlying API type pointer.
func (w *BaseModelWrapper) Obj() *v1beta1.BaseModel { return &w.BaseModel }

// Clone returns a deep copy wrapped, useful when the same baseline
// needs minor variations across subtests.
func (w *BaseModelWrapper) Clone() *BaseModelWrapper {
	return &BaseModelWrapper{BaseModel: *w.BaseModel.DeepCopy()}
}

// StorageURI sets spec.Storage.StorageUri.
func (w *BaseModelWrapper) StorageURI(uri string) *BaseModelWrapper {
	if w.Spec.Storage == nil {
		w.Spec.Storage = &v1beta1.StorageSpec{}
	}
	u := uri
	w.Spec.Storage.StorageUri = &u
	return w
}

// Distribution sets spec.Distribution.
func (w *BaseModelWrapper) Distribution(d v1beta1.Distribution) *BaseModelWrapper {
	dist := d
	w.Spec.Distribution = &dist
	return w
}

// Disabled marks the BaseModel disabled (consumed by ISVC controller).
func (w *BaseModelWrapper) Disabled(d bool) *BaseModelWrapper {
	val := d
	w.Spec.Disabled = &val
	return w
}

// Label adds a label to the BaseModel object metadata.
func (w *BaseModelWrapper) Label(k, v string) *BaseModelWrapper {
	if w.Labels == nil {
		w.Labels = map[string]string{}
	}
	w.Labels[k] = v
	return w
}

// ModelFormat sets spec.ModelFormat.Name. Required for the ISVC
// runtime selector (validateModelSpecification rejects models with
// no format name).
func (w *BaseModelWrapper) ModelFormat(name string) *BaseModelWrapper {
	w.Spec.ModelFormat = v1beta1.ModelFormat{Name: name}
	return w
}

// ModelArchitecture sets spec.ModelArchitecture (e.g. "LlamaForCausalLM").
func (w *BaseModelWrapper) ModelArchitecture(arch string) *BaseModelWrapper {
	a := arch
	w.Spec.ModelArchitecture = &a
	return w
}

// ModelType sets spec.ModelType (e.g. "llama").
func (w *BaseModelWrapper) ModelType(t string) *BaseModelWrapper {
	val := t
	w.Spec.ModelType = &val
	return w
}

// ModelFramework sets spec.ModelFramework.Name (e.g. "Transformers").
func (w *BaseModelWrapper) ModelFramework(name string) *BaseModelWrapper {
	w.Spec.ModelFramework = &v1beta1.ModelFrameworkSpec{Name: name}
	return w
}

// init registers a CleanupFunc that strips the BaseModel finalizer
// from any BaseModel in the target namespace. Without this, a test
// namespace with a finalizer-bearing BaseModel cannot be deleted.
func init() {
	RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
		bms := &v1beta1.BaseModelList{}
		if err := c.List(ctx, bms, client.InNamespace(namespace)); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		for i := range bms.Items {
			bm := &bms.Items[i]
			if !controllerutil.ContainsFinalizer(bm, constants.BaseModelFinalizer) {
				continue
			}
			controllerutil.RemoveFinalizer(bm, constants.BaseModelFinalizer)
			err := c.Update(ctx, bm)
			switch {
			case err == nil:
				// fall through
			case apierrors.IsNotFound(err), apierrors.IsConflict(err), apierrors.IsInvalid(err):
				// Benign race: the controller cleared the finalizer
				// concurrently with our cleanup → IsNotFound/Conflict,
				// or apiserver UID-precondition rejection (StorageError
				// surfaces as IsInvalid). Namespace cleanup shouldn't
				// fail on these.
				continue
			default:
				return err
			}
		}
		return nil
	})
}
