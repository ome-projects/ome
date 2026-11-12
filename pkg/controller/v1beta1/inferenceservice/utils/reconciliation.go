package utils

import (
	"context"
	"fmt"
	"strings"

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
	baseModel := &v1beta1.BaseModel{}
	err := cl.Get(context.TODO(), client.ObjectKey{Name: name, Namespace: namespace}, baseModel)
	if err == nil {
		return &baseModel.Spec, &baseModel.ObjectMeta, &baseModel.Status, nil
	} else if !errors.IsNotFound(err) {
		return nil, nil, nil, err
	}
	clusterBaseModel := &v1beta1.ClusterBaseModel{}
	err = cl.Get(context.TODO(), client.ObjectKey{Name: name}, clusterBaseModel)
	if err == nil {
		return &clusterBaseModel.Spec, &clusterBaseModel.ObjectMeta, &clusterBaseModel.Status, nil
	} else if !errors.IsNotFound(err) {
		return nil, nil, nil, err
	}
	return nil, nil, nil, goerrors.New("No BaseModel or ClusterBaseModel with the name: " + name)
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

// ReconcileBaseModelWithStatus retrieves and validates the base model for an
// InferenceService and also returns the model status for readiness checks.
// Returns (nil, nil, nil, nil) when the ISVC has no model reference — the
// lean path where the operator specifies runtime directly and skips
// model-parsing/auto-selection.
func ReconcileBaseModelWithStatus(cl client.Client, isvc *v1beta1.InferenceService) (*v1beta1.BaseModelSpec, *metav1.ObjectMeta, *v1beta1.ModelStatusSpec, error) {
	if isvc.Spec.Model == nil || isvc.Spec.Model.Name == "" {
		return nil, nil, nil, nil
	}

	baseModel, baseModelMeta, baseModelStatus, err := GetBaseModelWithStatus(cl, isvc.Spec.Model.Name, isvc.Namespace)
	if err != nil {
		return nil, nil, nil, err
	}

	if baseModel.Disabled != nil && *baseModel.Disabled {
		return nil, nil, nil, &ModelDisabledError{Name: isvc.Spec.Model.Name}
	}

	return baseModel, baseModelMeta, baseModelStatus, nil
}

// ModelDisabledError reports a model that resolved but is disabled and so
// cannot be served.
type ModelDisabledError struct{ Name string }

func (e *ModelDisabledError) Error() string {
	return fmt.Sprintf("specified base model %s is disabled", e.Name)
}

// IsModelDisabledError reports whether err is a disabled-model rejection.
func IsModelDisabledError(err error) bool {
	var disabled *ModelDisabledError
	return goerrors.As(err, &disabled)
}

// IsModelNotFoundError reports whether err is the model lookup's not-found
// result: neither a BaseModel in the namespace nor a ClusterBaseModel carries
// the name.
func IsModelNotFoundError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "No BaseModel or ClusterBaseModel with the name:")
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

// MergeRuntimeSpecs merges the runtime's engine, decoder and router pieces
// with the service's blocks, for callers that name those three roles.
// Callers that name the three roles link against this signature. Code that
// walks every role uses MergeRuntimeRoleSpecs.
func MergeRuntimeSpecs(isvc *v1beta1.InferenceService, runtime *v1beta1.ServingRuntimeSpec, log logr.Logger) (*v1beta1.EngineSpec, *v1beta1.DecoderSpec, *v1beta1.RouterSpec, error) {
	merged, err := MergeRuntimeRoleSpecs(isvc, runtime, log)
	if err != nil {
		return nil, nil, nil, err
	}
	return merged.Engine, merged.Decoder, merged.Router, nil
}

// MergeRuntimeRoleSpecs merges the runtime's pieces with the service's role
// blocks into the final spec of every role the service declares; a piece
// alone declares nothing.
func MergeRuntimeRoleSpecs(isvc *v1beta1.InferenceService, runtime *v1beta1.ServingRuntimeSpec, log logr.Logger) (v1beta1.RoleSpecs, error) {
	pieces := v1beta1.RuntimeRoles(runtime)
	var merged v1beta1.RoleSpecs
	var err error

	if merged.Engine, err = MergeEngineSpec(pieces.Engine, isvc.Spec.Engine); err != nil {
		return v1beta1.RoleSpecs{}, goerrors.Wrap(err, "failed to merge engine specs")
	}
	if merged.Decoder, err = MergeDecoderSpec(pieces.Decoder, isvc.Spec.Decoder); err != nil {
		return v1beta1.RoleSpecs{}, goerrors.Wrap(err, "failed to merge decoder specs")
	}
	if merged.Encoder, err = MergeEncoderSpec(pieces.Encoder, isvc.Spec.Encoder); err != nil {
		return v1beta1.RoleSpecs{}, goerrors.Wrap(err, "failed to merge encoder specs")
	}
	if merged.Router, err = MergeRouterSpec(isvc.Spec.Router, pieces.Router); err != nil {
		return v1beta1.RoleSpecs{}, goerrors.Wrap(err, "failed to merge router specs")
	}

	// The renderers read only the merged component specs, so this is the
	// one seam where the runtime's top-level schedulerName can reach a
	// pod. Applied after the component merges so every more specific
	// level (component / leader / worker, from either side) keeps
	// precedence.
	MergeSchedulerName(runtime, merged)

	return merged, nil
}
