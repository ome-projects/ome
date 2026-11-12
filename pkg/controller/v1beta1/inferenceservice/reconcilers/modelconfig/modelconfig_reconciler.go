package multimodelconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
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

// ModelConfigEntry is one model's configuration in the ConfigMap, whose data
// holds a list of entries. A service names one model, so its list has one.
type ModelConfigEntry struct {
	ModelName           string         `json:"modelName"`
	ModelSpec           ModelSpecInfo  `json:"modelSpec"`
	FineTunedWeightSpec *ModelSpecInfo `json:"fineTunedWeightSpec,omitempty"`
}

// modelConfig is the content the ConfigMap of one service carries: the entries
// and their JSON encoding, which is what the ConfigMap stores and the stored
// entries are compared against.
type modelConfig struct {
	entries []ModelConfigEntry
	data    []byte
}

// ModelConfigNotAdoptedReason is the reason of the event a service records
// when the ConfigMap under its model ConfigMap name is controlled by
// something else, so the service leaves it as it is.
const ModelConfigNotAdoptedReason = "ModelConfigNotAdopted"

type ConfigMapReconciler struct {
	client   client.Client
	scheme   *runtime.Scheme
	recorder record.EventRecorder
}

func NewModelConfigReconciler(client client.Client, scheme *runtime.Scheme, recorder record.EventRecorder) *ConfigMapReconciler {
	return &ConfigMapReconciler{
		client:   client,
		scheme:   scheme,
		recorder: recorder,
	}
}

// Reconcile keeps the model ConfigMap of a service that names a model in step
// with the model it names: the ConfigMap is created when absent, adopted when
// found, and rewritten when its entries differ from the desired ones; one
// another controller holds stays as it is. A service controls one model
// ConfigMap, the one under its current name: any other it controls goes, and
// a service that names no model loses them all.
func (c *ConfigMapReconciler) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	ctxLog := logf.FromContext(ctx)
	modelConfigName := constants.ModelConfigName(isvc.Name)
	// The ConfigMap follows spec.model: a service that names no model has
	// none, so one written for a model the service named earlier is removed.
	if isvc.Spec.Model == nil {
		if err := c.deleteModelConfig(ctx, isvc, modelConfigName); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, c.deleteModelConfigsUnderOtherNames(ctx, isvc, modelConfigName)
	}
	desired, err := c.desiredModelConfig(ctx, isvc)
	if err != nil {
		return ctrl.Result{}, err
	}
	existing := &corev1.ConfigMap{}
	err = c.client.Get(ctx, types.NamespacedName{Name: modelConfigName, Namespace: isvc.Namespace}, existing)
	switch {
	case apierrors.IsNotFound(err):
		ctxLog.Info("Creating new ModelConfig", "name", modelConfigName, "InferenceService", isvc.Name)
		if err := c.createModelConfig(ctx, isvc, modelConfigName, desired.data); err != nil {
			return ctrl.Result{}, err
		}
	case err != nil:
		ctxLog.Error(err, "Failed to get ConfigMap", "configmap", modelConfigName)
		return ctrl.Result{}, err
	default:
		if err := c.updateModelConfig(ctx, isvc, existing, desired); err != nil {
			return ctrl.Result{}, err
		}
	}
	// The ConfigMap under the current name is settled before any other goes.
	return ctrl.Result{}, c.deleteModelConfigsUnderOtherNames(ctx, isvc, modelConfigName)
}

// deleteModelConfigsUnderOtherNames removes the model ConfigMaps this service
// controls under a name other than the one it writes today. A service
// controls one model ConfigMap, so a change of the name rule leaves neither a
// copy nor a leftover. The candidates are found by the service's label; one
// another owner controls, or one without the models key, is not this
// service's model ConfigMap and stays.
func (c *ConfigMapReconciler) deleteModelConfigsUnderOtherNames(ctx context.Context, isvc *v1beta1.InferenceService, currentName string) error {
	log := logf.FromContext(ctx)
	labelled := &corev1.ConfigMapList{}
	if err := c.client.List(ctx, labelled, client.InNamespace(isvc.Namespace), client.MatchingLabels{constants.InferenceServicePodLabelKey: isvc.Name}); err != nil {
		log.Error(err, "Failed to list the ConfigMaps of the InferenceService", "InferenceService", isvc.Name)
		return err
	}
	for i := range labelled.Items {
		configMap := &labelled.Items[i]
		_, holdsModels := configMap.Data[constants.ModelConfigKey]
		if configMap.Name == currentName || !holdsModels || !metav1.IsControlledBy(configMap, isvc) {
			continue
		}
		err := c.client.Delete(ctx, configMap, client.Preconditions{UID: &configMap.UID})
		if err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to delete ConfigMap", "configmap", configMap.Name)
			return err
		}
		log.Info("Deleted a model ConfigMap under another name", "configmap", configMap.Name, "current", currentName, "InferenceService", isvc.Name)
	}
	return nil
}

// desiredModelConfig reads the model the service names and renders its entry.
func (c *ConfigMapReconciler) desiredModelConfig(ctx context.Context, isvc *v1beta1.InferenceService) (modelConfig, error) {
	ctxLog := logf.FromContext(ctx)
	model, err := c.resolveModelRef(ctx, isvc)
	if err != nil {
		ctxLog.Error(err, "Failed to generate ModelConfigEntry")
		return modelConfig{}, err
	}
	entries := []ModelConfigEntry{entryFrom(model)}
	data, err := json.Marshal(entries)
	if err != nil {
		ctxLog.Error(err, "Failed to marshal desired ModelConfig data")
		return modelConfig{}, err
	}
	return modelConfig{entries: entries, data: data}, nil
}

// storedEntries decodes the entries a ConfigMap holds. A missing key, an empty
// value or one that does not decode reads as no stored entries, so the next
// write replaces it.
func storedEntries(ctx context.Context, configMap *corev1.ConfigMap) ([]ModelConfigEntry, bool) {
	raw, ok := configMap.Data[constants.ModelConfigKey]
	if !ok || raw == "" {
		return nil, false
	}
	var entries []ModelConfigEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to unmarshal existing ModelConfig data, will overwrite", "name", configMap.Name)
		return nil, false
	}
	return entries, true
}

// updateModelConfig brings a ConfigMap found under the service's model
// ConfigMap name in step with the service: the service becomes its
// controller and the entries are rewritten when they differ from the desired
// ones; its other keys stay. Nothing is written when nothing differs, and
// nothing when another controller holds the ConfigMap.
func (c *ConfigMapReconciler) updateModelConfig(ctx context.Context, isvc *v1beta1.InferenceService, existing *corev1.ConfigMap, desired modelConfig) error {
	ctxLog := logf.FromContext(ctx)
	updated := existing.DeepCopy()
	if adopted, err := c.adoptModelConfig(ctx, isvc, updated); err != nil || !adopted {
		return err
	}
	if stored, ok := storedEntries(ctx, existing); !ok || !equality.Semantic.DeepEqual(stored, desired.entries) {
		if updated.Data == nil {
			updated.Data = make(map[string]string)
		}
		updated.Data[constants.ModelConfigKey] = string(desired.data)
	}
	if equality.Semantic.DeepEqual(updated, existing) {
		ctxLog.V(1).Info("ModelConfig already exists and is up-to-date", "configmap", existing.Name)
		return nil
	}
	ctxLog.Info("ModelConfig exists but needs update", "name", existing.Name)
	if err := c.client.Update(ctx, updated); err != nil {
		ctxLog.Error(err, "Failed to update ConfigMap", "configmap", existing.Name)
		return err
	}
	ctxLog.Info("Successfully updated ModelConfig", "name", existing.Name)
	return nil
}

// ownModelConfig makes the service the ConfigMap's controller and labels the
// ConfigMap with the service's name, the shape every model ConfigMap has. It
// fails with an AlreadyOwnedError when another controller holds the ConfigMap.
func (c *ConfigMapReconciler) ownModelConfig(isvc *v1beta1.InferenceService, configMap *corev1.ConfigMap) error {
	if err := controllerutil.SetControllerReference(isvc, configMap, c.scheme); err != nil {
		return err
	}
	if configMap.Labels == nil {
		configMap.Labels = map[string]string{}
	}
	configMap.Labels[constants.InferenceServicePodLabelKey] = isvc.Name
	return nil
}

// adoptModelConfig takes a ConfigMap found under the service's model
// ConfigMap name. One with no controller, or controlled by an earlier
// service of this name, becomes this service's and the call reports true.
// One another controller holds stays as it is and the call reports false:
// the service's event names that controller, and the pass goes on without
// a model ConfigMap until the operator frees or removes that one.
func (c *ConfigMapReconciler) adoptModelConfig(ctx context.Context, isvc *v1beta1.InferenceService, configMap *corev1.ConfigMap) (bool, error) {
	err := c.ownModelConfig(isvc, configMap)
	var owned *controllerutil.AlreadyOwnedError
	if !errors.As(err, &owned) {
		return err == nil, err
	}
	controller := owned.Owner.Kind + " " + owned.Owner.Name
	c.recorder.Eventf(isvc, corev1.EventTypeWarning, ModelConfigNotAdoptedReason,
		"OME did not take over ConfigMap %s: %s controls it. The service runs without a model ConfigMap until that ConfigMap is deleted or its controller reference removed.", configMap.Name, controller)
	logf.FromContext(ctx).Info("ModelConfig is controlled by another owner and stays", "configmap", configMap.Name, "controller", controller, "InferenceService", isvc.Name)
	return false, nil
}

// createModelConfig creates the ConfigMap, labelled with the service's name and
// owned by the service, so it is removed with it.
func (c *ConfigMapReconciler) createModelConfig(ctx context.Context, isvc *v1beta1.InferenceService, modelConfigName string, jsonData []byte) error {
	log := logf.FromContext(ctx)
	newModelConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: modelConfigName, Namespace: isvc.Namespace},
		Data:       map[string]string{constants.ModelConfigKey: string(jsonData)},
	}
	if err := c.ownModelConfig(isvc, newModelConfig); err != nil {
		log.Error(err, "Failed to set controller reference for modelConfig", "configmap", modelConfigName)
		return err
	}
	if err := c.client.Create(ctx, newModelConfig); err != nil {
		log.Error(err, "Failed to create ConfigMap", "configmap", modelConfigName)
		return err
	}
	log.Info("Successfully created modelConfig", "configmap", modelConfigName)
	return nil
}

// deleteModelConfig removes the service's model ConfigMap when it exists and
// this service controls it. A ConfigMap of that name under another owner, or
// under none, is not this service's to delete and stays.
func (c *ConfigMapReconciler) deleteModelConfig(ctx context.Context, isvc *v1beta1.InferenceService, modelConfigName string) error {
	log := logf.FromContext(ctx)
	existing := &corev1.ConfigMap{}
	err := c.client.Get(ctx, types.NamespacedName{Name: modelConfigName, Namespace: isvc.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		log.Error(err, "Failed to get ConfigMap", "configmap", modelConfigName)
		return err
	}
	if !metav1.IsControlledBy(existing, isvc) {
		log.V(1).Info("ModelConfig is not controlled by this InferenceService and stays", "configmap", modelConfigName, "InferenceService", isvc.Name)
		return nil
	}
	// The precondition ties the delete to the object that was read, so a
	// ConfigMap created under the same name since then is left alone.
	err = c.client.Delete(ctx, existing, client.Preconditions{UID: &existing.UID})
	if err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "Failed to delete ConfigMap", "configmap", modelConfigName)
		return err
	}
	log.Info("Deleted ModelConfig of an InferenceService without a model", "configmap", modelConfigName, "InferenceService", isvc.Name)
	return nil
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
