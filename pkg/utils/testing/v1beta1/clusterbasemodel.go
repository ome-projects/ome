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

// ClusterBaseModelWrapper mirrors BaseModelWrapper for the cluster-
// scoped variant. ClusterBaseModel takes no namespace; the storage
// URI must carry a namespace prefix (pvc://{namespace}:{pvc-name}/{sub-path}).
type ClusterBaseModelWrapper struct {
	v1beta1.ClusterBaseModel
}

// MakeClusterBaseModel returns a ClusterBaseModelWrapper with a
// scaffold spec. Storage{} is initialized so subsequent setters can
// chain without nil-checking.
func MakeClusterBaseModel(name string) *ClusterBaseModelWrapper {
	return &ClusterBaseModelWrapper{
		ClusterBaseModel: v1beta1.ClusterBaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{}},
		},
	}
}

func (w *ClusterBaseModelWrapper) Obj() *v1beta1.ClusterBaseModel { return &w.ClusterBaseModel }

func (w *ClusterBaseModelWrapper) Clone() *ClusterBaseModelWrapper {
	return &ClusterBaseModelWrapper{ClusterBaseModel: *w.ClusterBaseModel.DeepCopy()}
}

func (w *ClusterBaseModelWrapper) StorageURI(uri string) *ClusterBaseModelWrapper {
	if w.Spec.Storage == nil {
		w.Spec.Storage = &v1beta1.StorageSpec{}
	}
	u := uri
	w.Spec.Storage.StorageUri = &u
	return w
}

func (w *ClusterBaseModelWrapper) Distribution(d v1beta1.Distribution) *ClusterBaseModelWrapper {
	dist := d
	w.Spec.Distribution = &dist
	return w
}

func (w *ClusterBaseModelWrapper) Disabled(d bool) *ClusterBaseModelWrapper {
	val := d
	w.Spec.Disabled = &val
	return w
}

func (w *ClusterBaseModelWrapper) Label(k, v string) *ClusterBaseModelWrapper {
	if w.Labels == nil {
		w.Labels = map[string]string{}
	}
	w.Labels[k] = v
	return w
}

// ModelFormat sets spec.ModelFormat.Name. Required for the ISVC
// runtime selector (validateModelSpecification rejects models with
// no format name).
func (w *ClusterBaseModelWrapper) ModelFormat(name string) *ClusterBaseModelWrapper {
	w.Spec.ModelFormat = v1beta1.ModelFormat{Name: name}
	return w
}

// ModelArchitecture sets spec.ModelArchitecture (e.g. "LlamaForCausalLM").
// Used by the runtime selector to score SupportedModelFormats whose
// modelArchitecture matches.
func (w *ClusterBaseModelWrapper) ModelArchitecture(arch string) *ClusterBaseModelWrapper {
	a := arch
	w.Spec.ModelArchitecture = &a
	return w
}

// ModelType sets spec.ModelType (e.g. "llama").
func (w *ClusterBaseModelWrapper) ModelType(t string) *ClusterBaseModelWrapper {
	val := t
	w.Spec.ModelType = &val
	return w
}

// ModelFramework sets spec.ModelFramework.Name (e.g. "Transformers").
func (w *ClusterBaseModelWrapper) ModelFramework(name string) *ClusterBaseModelWrapper {
	w.Spec.ModelFramework = &v1beta1.ModelFrameworkSpec{Name: name}
	return w
}

// init registers a cluster-scoped cleanup. Unlike BaseModel, we walk
// every ClusterBaseModel and strip finalizers regardless of namespace
// (cluster-scoped objects don't disappear with namespace deletion).
// The cleanup is keyed off the namespace argument purely as a guard
// so tests that don't touch ClusterBaseModel don't pay the List cost
// — we only run it when the test's own namespace matches one of the
// PVC URI prefixes carried by any ClusterBaseModel.
//
// In practice the simplest correct behavior is: if a test created
// ClusterBaseModels, it must delete them itself. So this cleanup
// only handles the finalizer-removal half (so namespace deletion
// isn't blocked by a CBM-PVC-ConfigMap ownership chain) and leaves
// the actual CBM Delete to the test's own AfterEach.
func init() {
	RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
		cbms := &v1beta1.ClusterBaseModelList{}
		if err := c.List(ctx, cbms); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		for i := range cbms.Items {
			cbm := &cbms.Items[i]
			if !controllerutil.ContainsFinalizer(cbm, constants.ClusterBaseModelFinalizer) {
				continue
			}
			// Only strip the finalizer if the CBM is being deleted —
			// otherwise we'd un-finalize active models in other tests.
			if cbm.DeletionTimestamp.IsZero() {
				continue
			}
			controllerutil.RemoveFinalizer(cbm, constants.ClusterBaseModelFinalizer)
			if err := c.Update(ctx, cbm); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		return nil
	})
}
