package utils

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"

	goerrors "github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// GetBaseModel retrieves a BaseModel or ClusterBaseModel by name.
// It first tries to find a namespace-scoped BaseModel, then falls back to a cluster-scoped ClusterBaseModel.
// Returns the model spec, metadata, and any error encountered.
func GetBaseModel(cl client.Client, name string, namespace string) (*v1beta1.BaseModelSpec, *metav1.ObjectMeta, error) {
	spec, meta, _, err := GetBaseModelWithStatus(cl, name, namespace)
	return spec, meta, err
}

// GetBaseModelWithStatus retrieves a BaseModel or ClusterBaseModel by name and
// returns its spec, metadata, and status.
func GetBaseModelWithStatus(cl client.Client, name string, namespace string) (*v1beta1.BaseModelSpec, *metav1.ObjectMeta, *v1beta1.ModelStatusSpec, error) {
	return getModelReferenceWithStatus(cl, &v1beta1.ModelRef{Name: name}, namespace)
}

// ModelReferenceKind validates the primary Model reference. Empty kind retains
// legacy namespaced-first lookup; explicit kinds never fall back across scopes.
func ModelReferenceKind(ref *v1beta1.ModelRef) (string, error) {
	if ref == nil {
		return "", nil
	}
	if ref.APIGroup != nil && *ref.APIGroup != "" && *ref.APIGroup != v1beta1.SchemeGroupVersion.Group {
		return "", fmt.Errorf("unsupported Model API group %q", *ref.APIGroup)
	}
	if ref.Kind == nil || *ref.Kind == "" {
		return "", nil
	}
	if *ref.Kind != "BaseModel" && *ref.Kind != "ClusterBaseModel" {
		return "", fmt.Errorf("unsupported Model kind %q", *ref.Kind)
	}
	return *ref.Kind, nil
}

func getModelReferenceWithStatus(cl client.Client, ref *v1beta1.ModelRef, namespace string) (*v1beta1.BaseModelSpec, *metav1.ObjectMeta, *v1beta1.ModelStatusSpec, error) {
	kind, err := ModelReferenceKind(ref)
	if err != nil {
		return nil, nil, nil, err
	}
	if kind != "ClusterBaseModel" {
		baseModel := &v1beta1.BaseModel{}
		err := cl.Get(context.TODO(), client.ObjectKey{Name: ref.Name, Namespace: namespace}, baseModel)
		if err == nil {
			return &baseModel.Spec, &baseModel.ObjectMeta, &baseModel.Status, nil
		} else if kind == "BaseModel" || !errors.IsNotFound(err) {
			return nil, nil, nil, err
		}
	}
	clusterBaseModel := &v1beta1.ClusterBaseModel{}
	err = cl.Get(context.TODO(), client.ObjectKey{Name: ref.Name}, clusterBaseModel)
	if err == nil {
		return &clusterBaseModel.Spec, &clusterBaseModel.ObjectMeta, &clusterBaseModel.Status, nil
	} else if !errors.IsNotFound(err) {
		return nil, nil, nil, err
	}
	return nil, nil, nil, goerrors.New("No BaseModel or ClusterBaseModel with the name: " + ref.Name)
}

// GetFineTunedWeight Get the fine-tuned weight from the given fine-tuned weight name.
func GetFineTunedWeight(cl client.Client, name string) (*v1beta1.FineTunedWeight, error) {
	fineTunedWeight := &v1beta1.FineTunedWeight{}
	err := cl.Get(context.TODO(), client.ObjectKey{Name: name}, fineTunedWeight)
	if err == nil {
		return fineTunedWeight, nil
	} else if !errors.IsNotFound(err) {
		return nil, err
	}
	return nil, goerrors.New("No FineTunedWeight with the name: " + name)
}

// ReconcileBaseModel retrieves and validates the base model for an InferenceService
func ReconcileBaseModel(cl client.Client, isvc *v1beta1.InferenceService) (*v1beta1.BaseModelSpec, *metav1.ObjectMeta, error) {
	spec, meta, _, err := ReconcileBaseModelWithStatus(cl, isvc)
	return spec, meta, err
}

// ReconcileBaseModelWithStatus retrieves and validates the base model for an
// InferenceService and also returns the model status for readiness checks.
// Returns (nil, nil, nil, nil) when the ISVC has no model reference — the
// lean path where the operator specifies runtime directly and skips
// model-parsing/auto-selection.
func ReconcileBaseModelWithStatus(cl client.Client, isvc *v1beta1.InferenceService) (*v1beta1.BaseModelSpec, *metav1.ObjectMeta, *v1beta1.ModelStatusSpec, error) {
	if isvc.Spec.Model == nil || isvc.Spec.Model.Name == "" {
		return nil, nil, nil, nil
	}

	baseModel, baseModelMeta, baseModelStatus, err := getModelReferenceWithStatus(cl, isvc.Spec.Model, isvc.Namespace)
	if err != nil {
		return nil, nil, nil, err
	}

	if baseModel.Disabled != nil && *baseModel.Disabled {
		return nil, nil, nil, fmt.Errorf("specified base model %s is disabled", isvc.Spec.Model.Name)
	}

	return baseModel, baseModelMeta, baseModelStatus, nil
}

// IsShardedBaseModel reports whether a model uses sharded distribution.
func IsShardedBaseModel(model *v1beta1.BaseModelSpec) bool {
	return model != nil && model.Distribution != nil && *model.Distribution == v1beta1.DistributionSharded
}

// ShardedBaseModelReady reports whether the status for a sharded BaseModel is
// ready for an InferenceService consumer to reconcile runtime resources.
func ShardedBaseModelReady(status *v1beta1.ModelStatusSpec, generation int64) (bool, string) {
	if status == nil {
		return false, "model status is not available"
	}
	readyCondition := meta.FindStatusCondition(status.Conditions, v1beta1.ModelConditionReady)
	if readyCondition == nil {
		return false, "model Ready condition is not available"
	}
	if readyCondition.Status != metav1.ConditionTrue {
		return false, readyCondition.Message
	}
	if readyCondition.ObservedGeneration != generation {
		return false, "model Ready condition has not observed the latest generation"
	}
	return true, readyCondition.Message
}

// MergeRuntimeSpecs merges the runtime and isvc specs to get final engine, decoder, and router specs
func MergeRuntimeSpecs(isvc *v1beta1.InferenceService, runtime *v1beta1.ServingRuntimeSpec, log logr.Logger) (*v1beta1.EngineSpec, *v1beta1.DecoderSpec, *v1beta1.RouterSpec, error) {
	var runtimeEngine *v1beta1.EngineSpec
	var runtimeDecoder *v1beta1.DecoderSpec
	var runtimeRouter *v1beta1.RouterSpec

	// Extract runtime specs if available
	if runtime != nil {
		runtimeEngine = runtime.EngineConfig
		runtimeDecoder = runtime.DecoderConfig
		runtimeRouter = runtime.RouterConfig
	}

	// Merge engine specs
	mergedEngine, err := MergeEngineSpec(runtimeEngine, isvc.Spec.Engine)
	if err != nil {
		return nil, nil, nil, goerrors.Wrap(err, "failed to merge engine specs")
	}

	// Merge decoder specs
	mergedDecoder, err := MergeDecoderSpec(runtimeDecoder, isvc.Spec.Decoder)
	if err != nil {
		return nil, nil, nil, goerrors.Wrap(err, "failed to merge decoder specs")
	}

	// Merge router specs
	mergedRouter, err := MergeRouterSpec(isvc.Spec.Router, runtimeRouter)
	if err != nil {
		return nil, nil, nil, goerrors.Wrap(err, "failed to merge router specs")
	}

	// The renderers read only the merged component specs, so this is the
	// one seam where the runtime's top-level schedulerName can reach a
	// pod. Applied after the component merges so every more specific
	// level (component / leader / worker, from either side) keeps
	// precedence.
	MergeSchedulerName(runtime, mergedEngine, mergedDecoder, mergedRouter)

	return mergedEngine, mergedDecoder, mergedRouter, nil
}
