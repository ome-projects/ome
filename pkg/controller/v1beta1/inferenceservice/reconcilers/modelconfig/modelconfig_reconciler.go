package multimodelconfig

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// fineTunedWeightKind is the Spec.Model.Kind that names a FineTunedWeight
// instead of a base model.
const fineTunedWeightKind = "FineTunedWeight"

// ModelSpecInfo holds the extracted storage and metadata for a model or fine-tuned weight.
type ModelSpecInfo struct {
	StorageURI            *string           `json:"storageUri,omitempty"`
	Path                  *string           `json:"path,omitempty"`
	SchemaPath            *string           `json:"schemaPath,omitempty"`
	Parameters            map[string]string `json:"parameters,omitempty"`
	StorageKey            *string           `json:"storageKey,omitempty"`
	ModelFormatName       *string           `json:"modelFormatName,omitempty"`
	ModelFormatVersion    *string           `json:"modelFormatVersion,omitempty"`
	ModelType             *string           `json:"modelType,omitempty"`
	ModelFrameworkName    *string           `json:"modelFrameworkName,omitempty"`
	ModelFrameworkVersion *string           `json:"modelFrameworkVersion,omitempty"`
}

// ModelConfigEntry defines the structure for a single model's configuration within the ConfigMap.
// This structure will be part of an array in the ConfigMap's data to support multi-model configurations in the future.
type ModelConfigEntry struct {
	ModelName           string         `json:"modelName"` // e.g., "primary" or the actual model name from ModelRef
	ModelSpec           ModelSpecInfo  `json:"modelSpec"`
	FineTunedWeightSpec *ModelSpecInfo `json:"fineTunedWeightSpec,omitempty"`
}

type ConfigMapReconciler struct {
	client client.Client
	scheme *runtime.Scheme
}

func NewModelConfigReconciler(client client.Client, _ kubernetes.Interface, scheme *runtime.Scheme) *ConfigMapReconciler {
	return &ConfigMapReconciler{
		client: client,
		scheme: scheme,
	}
}

func (c *ConfigMapReconciler) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	ctxLog := logf.FromContext(ctx)

	// If no model is specified, we don't need a model config ConfigMap.
	if isvc.Spec.Model == nil {
		// TODO: Consider deleting an existing ConfigMap if the model spec is removed.
		ctxLog.V(1).Info("No model specified in InferenceService, skipping ModelConfig reconciliation", "InferenceService", isvc.Name)
		return ctrl.Result{}, nil
	}

	modelConfigName := constants.ModelConfigName(isvc.Name)

	model, err := c.resolveModelRef(ctx, isvc)
	if err != nil {
		ctxLog.Error(err, "Failed to generate ModelConfigEntry")
		return ctrl.Result{}, err
	}

	// For now, we are managing a single model entry based on InferenceService.Spec.Model
	// In the future, this could be expanded to manage multiple models in the ConfigMap.
	desiredEntries := []ModelConfigEntry{entryFrom(model)}
	desiredData, err := json.Marshal(desiredEntries)
	if err != nil {
		ctxLog.Error(err, "Failed to marshal desired ModelConfig data")
		return ctrl.Result{}, err
	}

	// Retrieve the ConfigMap
	existingCm := &corev1.ConfigMap{}
	err = c.client.Get(ctx, types.NamespacedName{Name: modelConfigName, Namespace: isvc.Namespace}, existingCm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// ConfigMap is not found, create a new one
			ctxLog.Info("Creating new ModelConfig", "name", modelConfigName, "InferenceService", isvc.Name)
			return c.createModelConfig(ctx, isvc, modelConfigName, desiredData)
		}
		// Unexpected error while retrieving ConfigMap
		ctxLog.Error(err, "Failed to get ConfigMap", "configmap", modelConfigName)
		return ctrl.Result{}, err
	}

	// ConfigMap exists, check if update is needed
	// Unmarshal existing data to compare semantically with desired state (array of ModelConfigEntry)
	var existingEntries []ModelConfigEntry
	existingDataJSON, ok := existingCm.Data[constants.ModelConfigKey]
	if ok && existingDataJSON != "" {
		if errUnmarshal := json.Unmarshal([]byte(existingDataJSON), &existingEntries); errUnmarshal != nil {
			ctxLog.Error(errUnmarshal, "Failed to unmarshal existing ModelConfig data, will overwrite", "name", modelConfigName)
			ok = false // Treat as if data is not there or invalid
		}
	}

	if !ok || !equality.Semantic.DeepEqual(existingEntries, desiredEntries) {
		ctxLog.Info("ModelConfig exists but needs update", "name", modelConfigName)
		updatedCm := existingCm.DeepCopy()
		if updatedCm.Data == nil {
			updatedCm.Data = make(map[string]string)
		}
		updatedCm.Data[constants.ModelConfigKey] = string(desiredData)

		err = c.client.Update(ctx, updatedCm)
		if err != nil {
			ctxLog.Error(err, "Failed to update ConfigMap", "configmap", modelConfigName)
			return ctrl.Result{}, err
		}

		ctxLog.Info("Successfully updated ModelConfig", "name", modelConfigName)
	} else {
		ctxLog.V(1).Info("ModelConfig already exists and is up-to-date", "configmap", modelConfigName)
	}

	return ctrl.Result{}, nil
}

func (c *ConfigMapReconciler) createModelConfig(ctx context.Context, isvc *v1beta1.InferenceService, modelConfigName string, jsonData []byte) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	newModelConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      modelConfigName,
			Namespace: isvc.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
			},
		},
		Data: map[string]string{
			constants.ModelConfigKey: string(jsonData),
		},
	}

	if err := controllerutil.SetControllerReference(isvc, newModelConfig, c.scheme); err != nil {
		log.Error(err, "Failed to set controller reference for modelConfig", "configmap", modelConfigName)
		return ctrl.Result{}, err
	}

	if err := c.client.Create(ctx, newModelConfig); err != nil {
		log.Error(err, "Failed to create ConfigMap", "configmap", modelConfigName)
		return ctrl.Result{}, err
	}

	log.Info("Successfully created modelConfig", "configmap", modelConfigName)
	return ctrl.Result{}, nil
}

// resolvedModel is what Spec.Model names once the referenced objects are read:
// the base model the entry describes, under the name the entry carries, and the
// fine-tuned weight applied to it, nil when the reference names none.
type resolvedModel struct {
	name   string
	base   *v1beta1.BaseModelSpec
	weight *v1beta1.FineTunedWeightSpec
}

// resolveModelRef reads the objects Spec.Model names. A reference of kind
// FineTunedWeight resolves to that weight and the base model it was trained
// on; any other reference resolves to that base model and the first weight
// listed under it.
func (c *ConfigMapReconciler) resolveModelRef(ctx context.Context, isvc *v1beta1.InferenceService) (resolvedModel, error) {
	ref := isvc.Spec.Model
	if modelRefKind(ref) == fineTunedWeightKind {
		return c.resolveFineTunedWeightRef(ctx, ref)
	}
	return c.resolveBaseModelRef(ctx, ref, isvc.Namespace)
}

// resolveFineTunedWeightRef resolves a reference of kind FineTunedWeight. The
// entry is named after the base model the weight names: a BaseModelRef with a
// namespace names a BaseModel there, one without names a ClusterBaseModel.
func (c *ConfigMapReconciler) resolveFineTunedWeightRef(ctx context.Context, ref *v1beta1.ModelRef) (resolvedModel, error) {
	logf.FromContext(ctx).V(1).Info("InferenceService references a FineTunedWeight directly", "ftwName", ref.Name)
	weight, err := c.getFineTunedWeightSpec(ctx, ref.Name)
	if err != nil {
		return resolvedModel{}, fmt.Errorf("failed to get referenced fine tuned weight spec for %s: %w", ref.Name, err)
	}
	if weight.BaseModelRef.Name == nil || *weight.BaseModelRef.Name == "" {
		return resolvedModel{}, fmt.Errorf("FineTunedWeight %s has no BaseModelRef.Name specified", ref.Name)
	}
	baseName := *weight.BaseModelRef.Name
	baseRef := &v1beta1.ModelRef{Name: baseName, Kind: ptr.To(constants.ClusterBaseModel)}
	baseNamespace := ""
	if weight.BaseModelRef.Namespace != nil && *weight.BaseModelRef.Namespace != "" {
		baseRef.Kind = ptr.To(constants.BaseModel)
		baseNamespace = *weight.BaseModelRef.Namespace
	}
	base, err := c.getBaseModelSpec(ctx, baseRef, baseNamespace)
	if err != nil {
		return resolvedModel{}, fmt.Errorf("failed to get base model spec for %s (referenced by FTW %s): %w", baseName, ref.Name, err)
	}
	return resolvedModel{name: baseName, base: base, weight: weight}, nil
}

// resolveBaseModelRef resolves a reference to a BaseModel or ClusterBaseModel,
// with the first fine-tuned weight listed under it when there is one.
func (c *ConfigMapReconciler) resolveBaseModelRef(ctx context.Context, ref *v1beta1.ModelRef, isvcNamespace string) (resolvedModel, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("InferenceService references a BaseModel/ClusterBaseModel directly", "modelName", ref.Name, "kind", modelRefKind(ref))
	base, err := c.getBaseModelSpec(ctx, ref, isvcNamespace)
	if err != nil {
		return resolvedModel{}, fmt.Errorf("failed to get base model spec for %s: %w", ref.Name, err)
	}
	model := resolvedModel{name: ref.Name, base: base}
	if len(ref.FineTunedWeights) == 0 || ref.FineTunedWeights[0] == "" {
		return model, nil
	}
	weightName := ref.FineTunedWeights[0]
	log.V(1).Info("Processing FineTunedWeight specified in ModelRef.FineTunedWeights", "ftwName", weightName)
	model.weight, err = c.getFineTunedWeightSpec(ctx, weightName)
	if err != nil {
		return resolvedModel{}, fmt.Errorf("failed to get fine tuned weight spec %s (listed in ModelRef.FineTunedWeights): %w", weightName, err)
	}
	return model, nil
}

// modelRefKind is the reference's Kind, empty when unset.
func modelRefKind(ref *v1beta1.ModelRef) string {
	if ref.Kind == nil {
		return ""
	}
	return *ref.Kind
}

// getBaseModelSpec reads the base model a reference names: a BaseModel in the
// given namespace when the Kind says so, else a ClusterBaseModel.
func (c *ConfigMapReconciler) getBaseModelSpec(ctx context.Context, modelRef *v1beta1.ModelRef, isvcNamespace string) (*v1beta1.BaseModelSpec, error) {
	log := logf.FromContext(ctx)
	if modelRefKind(modelRef) == constants.BaseModel {
		baseModel := &v1beta1.BaseModel{}
		if err := c.client.Get(ctx, types.NamespacedName{Name: modelRef.Name, Namespace: isvcNamespace}, baseModel); err != nil {
			log.Error(err, "Failed to get referenced BaseModel", "name", modelRef.Name, "namespace", isvcNamespace)
			return nil, err
		}
		return &baseModel.Spec, nil
	}
	clusterBaseModel := &v1beta1.ClusterBaseModel{}
	if err := c.client.Get(ctx, types.NamespacedName{Name: modelRef.Name}, clusterBaseModel); err != nil {
		log.Error(err, "Failed to get referenced ClusterBaseModel", "name", modelRef.Name)
		return nil, err
	}
	return &clusterBaseModel.Spec, nil
}

// getFineTunedWeightSpec reads a FineTunedWeight, which is cluster-scoped.
func (c *ConfigMapReconciler) getFineTunedWeightSpec(ctx context.Context, name string) (*v1beta1.FineTunedWeightSpec, error) {
	ftw := &v1beta1.FineTunedWeight{}
	if err := c.client.Get(ctx, types.NamespacedName{Name: name}, ftw); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to get FineTunedWeight", "name", name)
		return nil, err
	}
	return &ftw.Spec, nil
}

// entryFrom is the ConfigMap entry for a resolved model.
func entryFrom(model resolvedModel) ModelConfigEntry {
	entry := ModelConfigEntry{ModelName: model.name}
	populateBaseModelDetails(&entry.ModelSpec, model.base)
	if model.weight != nil {
		entry.FineTunedWeightSpec = &ModelSpecInfo{}
		populateFineTunedWeightDetails(entry.FineTunedWeightSpec, model.weight)
	}
	return entry
}

func populateStorageFields(target *ModelSpecInfo, sourceStorage *v1beta1.StorageSpec) {
	if sourceStorage == nil {
		return
	}
	target.StorageURI = sourceStorage.StorageUri
	target.Path = sourceStorage.Path
	target.SchemaPath = sourceStorage.SchemaPath
	if sourceStorage.Parameters != nil {
		target.Parameters = *sourceStorage.Parameters
	}
	target.StorageKey = sourceStorage.StorageKey
}

func populateBaseModelDetails(target *ModelSpecInfo, bmSpec *v1beta1.BaseModelSpec) {
	populateStorageFields(target, bmSpec.Storage)
	target.ModelFormatName = &bmSpec.ModelFormat.Name
	if bmSpec.ModelFormat.Version != nil {
		target.ModelFormatVersion = bmSpec.ModelFormat.Version
	}
	if bmSpec.ModelFramework != nil {
		target.ModelFrameworkName = &bmSpec.ModelFramework.Name
		if bmSpec.ModelFramework.Version != nil {
			target.ModelFrameworkVersion = bmSpec.ModelFramework.Version
		}
	}
	target.ModelType = bmSpec.ModelType
}

func populateFineTunedWeightDetails(target *ModelSpecInfo, ftwSpec *v1beta1.FineTunedWeightSpec) {
	populateStorageFields(target, ftwSpec.Storage)
	target.ModelType = ftwSpec.ModelType
}
