package render

import (
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils"
)

// ProcessBaseAnnotations adds the annotations every component carries: the
// fine-tuned weight and its strategy, merged-weight serving, the base model's
// name, vendor and format, and the runtime's name.
func ProcessBaseAnnotations(p *Piece, isvc *v1beta1.InferenceService, annotations map[string]string) (map[string]string, error) {
	if err := addFineTunedAnnotations(p, isvc, annotations); err != nil {
		return nil, err
	}
	if p.FineTunedServingWithMergedWeights {
		p.Log.V(1).Info("Fine-tuned serving with merged weights", "namespace", isvc.Namespace)
		annotations[constants.FTServingWithMergedWeightsAnnotationKey] = "true"
		annotations[constants.ModelInitInjectionKey] = "false"
	} else if p.BaseModelMeta != nil {
		// Model init reads the decryption key and secret from pod annotations.
		if keyName, ok := p.BaseModelMeta.Annotations[constants.BaseModelDecryptionKeyName]; ok {
			annotations[constants.BaseModelDecryptionKeyName] = keyName
		}
		if secretName, ok := p.BaseModelMeta.Annotations[constants.BaseModelDecryptionSecretName]; ok {
			annotations[constants.BaseModelDecryptionSecretName] = secretName
		}
	}
	addBaseModelAnnotations(p, annotations)
	if p.RuntimeName != "" {
		annotations[constants.ServingRuntimeKeyName] = p.RuntimeName
	}
	return annotations, nil
}

// addFineTunedAnnotations names the fine-tuned weight the adapter injects and
// the strategy its hyper-parameters declare; a weight without a strategy is
// refused. Only a fine-tuned serving with at least one weight carries them.
func addFineTunedAnnotations(p *Piece, isvc *v1beta1.InferenceService, annotations map[string]string) error {
	if !p.FineTunedServing || len(p.FineTunedWeights) == 0 {
		return nil
	}
	annotations[constants.FineTunedAdapterInjectionKey] = p.FineTunedWeights[0].Name
	strategy, set, err := fineTunedStrategy(p, isvc)
	if err != nil {
		return err
	}
	if !set {
		return fmt.Errorf("hyper-parameter %q not set on FineTunedWeight %s", constants.StrategyConfigKey, p.FineTunedWeights[0].Name)
	}
	annotations[constants.FineTunedWeightFTStrategyKey] = strategy
	return nil
}

// addBaseModelAnnotations names the base model, its vendor, its format and
// the format's version when the piece carries a model.
func addBaseModelAnnotations(p *Piece, annotations map[string]string) {
	if p.BaseModel == nil || p.BaseModelMeta == nil {
		return
	}
	annotations[constants.BaseModelName] = p.BaseModelMeta.Name
	if p.BaseModel.Vendor != nil {
		annotations[constants.BaseModelVendorAnnotationKey] = *p.BaseModel.Vendor
	}
	annotations[constants.BaseModelFormat] = p.BaseModel.ModelFormat.Name
	if p.BaseModel.ModelFormat.Version != nil {
		annotations[constants.BaseModelFormatVersion] = *p.BaseModel.ModelFormat.Version
	}
}

// fineTunedStrategy reads the strategy hyper-parameter of the first
// fine-tuned weight: its value and whether the weight sets it. Hyper-parameters
// that do not parse, or a strategy that is not a string, are an error.
func fineTunedStrategy(p *Piece, isvc *v1beta1.InferenceService) (string, bool, error) {
	weight := p.FineTunedWeights[0]
	value, err := isvcutils.GetValueFromRawExtension(weight.Spec.HyperParameters, constants.StrategyConfigKey)
	if err != nil {
		p.Log.Error(err, "Error getting hyper-parameter strategy from FineTunedWeight", "FineTunedWeight", weight.Name, "namespace", isvc.Namespace)
		return "", false, err
	}
	if value == nil {
		return "", false, nil
	}
	strategy, ok := value.(string)
	if !ok {
		return "", false, fmt.Errorf("hyper-parameter %q on FineTunedWeight %s must be a string, got %T", constants.StrategyConfigKey, weight.Name, value)
	}
	return strategy, true, nil
}

// ProcessBaseLabels adds the labels every component carries: the service,
// component and runtime names, the fine-tuned serving flag, the base model's
// name, size, kind and vendor, and the fine-tuned weight's strategy.
func ProcessBaseLabels(p *Piece, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, labels map[string]string) (map[string]string, error) {
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[constants.InferenceServicePodLabelKey] = isvc.Name
	labels[constants.OMEComponentLabel] = string(componentType)
	labels[constants.ServingRuntimeLabelKey] = p.RuntimeName
	labels[constants.FTServingLabelKey] = strconv.FormatBool(p.FineTunedServing)
	addBaseModelLabels(p, labels)
	if err := addFineTunedLabels(p, isvc, labels); err != nil {
		return nil, err
	}
	return labels, nil
}

// addBaseModelLabels names the base model, its size category (SMALL unless
// the model's annotation says otherwise), its kind and its vendor.
func addBaseModelLabels(p *Piece, labels map[string]string) {
	if p.BaseModelMeta != nil {
		labels[constants.InferenceServiceBaseModelNameLabelKey] = p.BaseModelMeta.Name
		labels[constants.InferenceServiceBaseModelSizeLabelKey] = baseModelCategory(p.BaseModelMeta)
		labels[constants.BaseModelTypeLabelKey] = string(constants.ServingBaseModel)
	}
	if p.BaseModel != nil && p.BaseModel.Vendor != nil {
		labels[constants.BaseModelVendorLabelKey] = *p.BaseModel.Vendor
	}
}

// baseModelCategory is the size category the model's annotation declares,
// SMALL when it declares none.
func baseModelCategory(meta *metav1.ObjectMeta) string {
	if category, ok := meta.Annotations[constants.ModelCategoryAnnotation]; ok {
		return category
	}
	return "SMALL"
}

// addFineTunedLabels records the fine-tuned weight's strategy (empty when its
// hyper-parameters name none) and whether the weights are served merged. Only
// a fine-tuned serving with at least one weight carries them.
func addFineTunedLabels(p *Piece, isvc *v1beta1.InferenceService, labels map[string]string) error {
	if !p.FineTunedServing || len(p.FineTunedWeights) == 0 {
		return nil
	}
	strategy, _, err := fineTunedStrategy(p, isvc)
	if err != nil {
		return err
	}
	labels[constants.FineTunedWeightFTStrategyLabelKey] = strategy
	labels[constants.FTServingWithMergedWeightsLabelKey] = strconv.FormatBool(p.FineTunedServingWithMergedWeights)
	return nil
}

// ReconcileComponentObjectMeta builds the common ObjectMeta block
// (Name, Namespace, Annotations, Labels) shared by engine / decoder /
// router. componentName is `ComponentName(isvc, componentType)`; the caller
// passes it so one build serves every role. The annotation / label maps are the
// per-Component merge (ISVC + componentExt.Annotations /
// componentExt.Labels) already performed by the caller.
//
// On annotation-build failure the returned ObjectMeta carries Name +
// Namespace only; on label-build failure it carries Name + Namespace +
// Annotations, so callers see exactly the metadata built before the
// failure.
func ReconcileComponentObjectMeta(
	p *Piece,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	componentName string,
	componentAnnotations map[string]string,
	componentLabels map[string]string,
) (metav1.ObjectMeta, error) {
	annotations, err := ProcessComponentAnnotations(p, isvc, componentAnnotations)
	if err != nil {
		return metav1.ObjectMeta{
			Name:      componentName,
			Namespace: isvc.Namespace,
		}, err
	}

	labels, err := ProcessComponentLabels(p, isvc, componentType, componentLabels)
	if err != nil {
		return metav1.ObjectMeta{
			Name:        componentName,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
		}, err
	}

	return metav1.ObjectMeta{
		Name:        componentName,
		Namespace:   isvc.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}, nil
}

// ProcessComponentAnnotations performs the per-Component annotation
// build: filter the ISVC-level annotations against the disallowed
// list, union them with the Component-level annotations, then hand
// off to ProcessBaseAnnotations for the FT / BaseModel / runtime
// annotations the base layer adds. The result is the component's object
// metadata, which its Service and the other objects built from it carry
// whole; the pod template takes only the keys AnnotationReachesPods
// admits (TemplateObjectMeta).
func ProcessComponentAnnotations(
	p *Piece,
	isvc *v1beta1.InferenceService,
	componentAnnotations map[string]string,
) (map[string]string, error) {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	mergedAnnotations := annotations
	if componentAnnotations != nil {
		mergedAnnotations = utils.Union(annotations, componentAnnotations)
	}
	delete(mergedAnnotations, constants.InferenceServiceInPlaceImageTransitionAnnotationKey)

	return ProcessBaseAnnotations(p, isvc, mergedAnnotations)
}

// ProcessComponentLabels performs the per-Component label build:
// union the ISVC-level labels with the Component-level labels, then
// hand off to ProcessBaseLabels for the FT / BaseModel / runtime
// labels the base layer adds.
func ProcessComponentLabels(
	p *Piece,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	componentLabels map[string]string,
) (map[string]string, error) {
	// Union always copies: ProcessBaseLabels mutates the map it is
	// handed, and aliasing isvc.Labels would leak one component's
	// stamps into the next component's build.
	mergedLabels := utils.Union(isvc.Labels, componentLabels)

	return ProcessBaseLabels(p, isvc, componentType, mergedLabels)
}
