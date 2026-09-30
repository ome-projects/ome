package v1beta1testing

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// InferenceServiceWrapper carries an InferenceService plus chained
// setters. Finishes with .Obj() to extract the underlying API type.
type InferenceServiceWrapper struct {
	v1beta1.InferenceService
}

// MakeInferenceService returns an InferenceServiceWrapper with an empty
// spec scaffold. The Model / Runtime / Engine / Decoder / Annotations
// setters chain to fill in what each test cares about.
func MakeInferenceService(name, namespace string) *InferenceServiceWrapper {
	return &InferenceServiceWrapper{
		InferenceService: v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec:       v1beta1.InferenceServiceSpec{},
		},
	}
}

// Obj returns the underlying API type pointer.
func (w *InferenceServiceWrapper) Obj() *v1beta1.InferenceService { return &w.InferenceService }

// Clone returns a deep copy wrapped, useful when the same baseline
// needs minor variations across subtests.
func (w *InferenceServiceWrapper) Clone() *InferenceServiceWrapper {
	return &InferenceServiceWrapper{InferenceService: *w.InferenceService.DeepCopy()}
}

// Model sets spec.Model.Name. Defaults Kind to ClusterBaseModel and
// APIGroup to ome.io to match the CRD's defaults.
func (w *InferenceServiceWrapper) Model(name string) *InferenceServiceWrapper {
	kind := "ClusterBaseModel"
	apiGroup := "ome.io"
	w.Spec.Model = &v1beta1.ModelRef{
		Name:     name,
		Kind:     &kind,
		APIGroup: &apiGroup,
	}
	return w
}

// NamespacedModel sets spec.Model with Kind=BaseModel (the namespaced
// variant). Use when the test wants a same-namespace BaseModel
// reference rather than the default cluster-scoped one.
func (w *InferenceServiceWrapper) NamespacedModel(name string) *InferenceServiceWrapper {
	kind := "BaseModel"
	apiGroup := "ome.io"
	w.Spec.Model = &v1beta1.ModelRef{
		Name:     name,
		Kind:     &kind,
		APIGroup: &apiGroup,
	}
	return w
}

// FineTunedWeightModel sets spec.Model with Kind=FineTunedWeight, the
// shape used when an ISVC directly references a FineTunedWeight
// instead of a BaseModel/ClusterBaseModel. The modelconfig reconciler
// resolves the FTW's BaseModelRef to find the underlying base model.
func (w *InferenceServiceWrapper) FineTunedWeightModel(name string) *InferenceServiceWrapper {
	kind := "FineTunedWeight"
	apiGroup := "ome.io"
	w.Spec.Model = &v1beta1.ModelRef{
		Name:     name,
		Kind:     &kind,
		APIGroup: &apiGroup,
	}
	return w
}

// WithFineTunedWeights sets spec.Model.FineTunedWeights. Used when the
// primary ModelRef is a BaseModel/ClusterBaseModel that should ALSO
// load one or more fine-tuned weights on top. Per the controller, only
// the FIRST entry is consumed today (modelconfig_reconciler.go:316).
func (w *InferenceServiceWrapper) WithFineTunedWeights(names ...string) *InferenceServiceWrapper {
	if w.Spec.Model == nil {
		w.Spec.Model = &v1beta1.ModelRef{}
	}
	w.Spec.Model.FineTunedWeights = append([]string{}, names...)
	return w
}

// Runtime sets spec.Runtime.Name. Defaults Kind to ClusterServingRuntime.
func (w *InferenceServiceWrapper) Runtime(name string) *InferenceServiceWrapper {
	kind := "ClusterServingRuntime"
	apiGroup := "ome.io"
	w.Spec.Runtime = &v1beta1.ServingRuntimeRef{
		Name:     name,
		Kind:     &kind,
		APIGroup: &apiGroup,
	}
	return w
}

// AutoSync sets spec.Runtime.AutoSync. Requires Runtime() to have
// been called first.
func (w *InferenceServiceWrapper) AutoSync(v bool) *InferenceServiceWrapper {
	w.Spec.Runtime.AutoSync = &v
	return w
}

// RuntimeRevision sets spec.Runtime.Revision (explicit pin).
// Requires Runtime() to have been called first.
func (w *InferenceServiceWrapper) RuntimeRevision(name string) *InferenceServiceWrapper {
	w.Spec.Runtime.Revision = &name
	return w
}

// NamespacedRuntime sets spec.Runtime.{Name,Kind} where Kind is
// ServingRuntime. Used by pinning tests to exercise the
// r-<ns>-<runtime>-<hash> naming branch.
func (w *InferenceServiceWrapper) NamespacedRuntime(name string) *InferenceServiceWrapper {
	kind := "ServingRuntime"
	apiGroup := "ome.io"
	w.Spec.Runtime = &v1beta1.ServingRuntimeRef{
		Name:     name,
		Kind:     &kind,
		APIGroup: &apiGroup,
	}
	return w
}

// Engine sets spec.Engine.
func (w *InferenceServiceWrapper) Engine(spec *v1beta1.EngineSpec) *InferenceServiceWrapper {
	w.Spec.Engine = spec
	return w
}

// Decoder sets spec.Decoder. Presence triggers PD-disaggregated mode
// detection in the controller.
func (w *InferenceServiceWrapper) Decoder(spec *v1beta1.DecoderSpec) *InferenceServiceWrapper {
	w.Spec.Decoder = spec
	return w
}

// Router sets spec.Router. The router is the third optional component
// alongside Engine + Decoder; when set it produces its own child
// resource (Deployment/Service) per the chosen mode.
func (w *InferenceServiceWrapper) Router(spec *v1beta1.RouterSpec) *InferenceServiceWrapper {
	w.Spec.Router = spec
	return w
}

// DeploymentMode sets the ome.io/deploymentMode annotation, which the
// controller reads to override the cluster default. Pass
// constants.RawDeployment / constants.MultiNode / constants.PDDisaggregated
// / constants.OMENative / constants.VirtualDeployment.
func (w *InferenceServiceWrapper) DeploymentMode(mode constants.DeploymentModeType) *InferenceServiceWrapper {
	if w.Annotations == nil {
		w.Annotations = map[string]string{}
	}
	w.Annotations[constants.DeploymentMode] = string(mode)
	return w
}

// Annotation adds a single annotation. Useful for the various ome.io/*
// flags the controller reads (ingress-disable-creation, etc.).
func (w *InferenceServiceWrapper) Annotation(k, v string) *InferenceServiceWrapper {
	if w.Annotations == nil {
		w.Annotations = map[string]string{}
	}
	w.Annotations[k] = v
	return w
}

// Label adds a single label.
func (w *InferenceServiceWrapper) Label(k, v string) *InferenceServiceWrapper {
	if w.Labels == nil {
		w.Labels = map[string]string{}
	}
	w.Labels[k] = v
	return w
}

// Traffic sets spec.traffic (typed core).
func (w *InferenceServiceWrapper) Traffic(t *v1beta1.TrafficSpec) *InferenceServiceWrapper {
	w.Spec.Traffic = t
	return w
}

// init registers a CleanupFunc that strips the ISVC finalizer from
// any InferenceService in the namespace. Without this, the namespace
// (and the test) hangs waiting on the ISVC to clear its finalizer.
func init() {
	RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
		isvcs := &v1beta1.InferenceServiceList{}
		if err := c.List(ctx, isvcs, client.InNamespace(namespace)); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		for i := range isvcs.Items {
			isvc := &isvcs.Items[i]
			// Strip ALL finalizers — the ISVC finalizer is a literal
			// "inferenceservice.finalizers" string in the controller, but
			// future commits may add more. Stripping all is safe in tests
			// where we control the resources.
			if len(isvc.Finalizers) == 0 {
				continue
			}
			isvc.Finalizers = nil
			err := c.Update(ctx, isvc)
			switch {
			case err == nil:
				// fall through
			case apierrors.IsNotFound(err), apierrors.IsConflict(err), apierrors.IsInvalid(err):
				// Benign race: the controller already cleared the
				// finalizer (IsNotFound/IsConflict) or the apiserver
				// rejected the UID-precondition because the object
				// was just deleted (StorageError surfaces as
				// IsInvalid). Namespace cleanup shouldn't fail on these.
				continue
			default:
				return err
			}
		}
		return nil
	})
}
