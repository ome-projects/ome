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

// ProcessBaseAnnotations processes common annotations
func ProcessBaseAnnotations(p *Piece, isvc *v1beta1.InferenceService, annotations map[string]string) (map[string]string, error) {
	// Add fine-tuned weight annotations if applicable
	if p.FineTunedServing && len(p.FineTunedWeights) > 0 {
		// Inject ft adapter for single/non-stacked fine-tuned weight downloading
		annotations[constants.FineTunedAdapterInjectionKey] = p.FineTunedWeights[0].Name

		// Add fine-tuned weight ft strategy
		fineTunedWeightFTStrategy, err := isvcutils.GetValueFromRawExtension(p.FineTunedWeights[0].Spec.HyperParameters, constants.StrategyConfigKey)
		if err != nil {
			p.Log.Error(err, "Error getting hyper-parameter strategy from FineTunedWeight", "FineTunedWeight", p.FineTunedWeights[0].Name, "namespace", isvc.Namespace)
			return nil, err
		}
		if fineTunedWeightFTStrategy == nil {
			return nil, fmt.Errorf("hyper-parameter %q not set on FineTunedWeight %s", constants.StrategyConfigKey, p.FineTunedWeights[0].Name)
		}
		strategy, ok := fineTunedWeightFTStrategy.(string)
		if !ok {
			return nil, fmt.Errorf("hyper-parameter %q on FineTunedWeight %s must be a string, got %T", constants.StrategyConfigKey, p.FineTunedWeights[0].Name, fineTunedWeightFTStrategy)
		}
		annotations[constants.FineTunedWeightFTStrategyKey] = strategy
	}

	if p.FineTunedServingWithMergedWeights {
		p.Log.V(1).Info("Fine-tuned serving with merged weights", "namespace", isvc.Namespace)
		annotations[constants.FTServingWithMergedWeightsAnnotationKey] = "true"
	}

	// Add base model specific annotations
	if p.BaseModel != nil && p.BaseModelMeta != nil {
		annotations[constants.BaseModelName] = p.BaseModelMeta.Name
		if p.BaseModel.Vendor != nil {
			annotations[constants.BaseModelVendorAnnotationKey] = *p.BaseModel.Vendor
		}
		annotations[constants.BaseModelFormat] = p.BaseModel.ModelFormat.Name
		if p.BaseModel.ModelFormat.Version != nil {
			annotations[constants.BaseModelFormatVersion] = *p.BaseModel.ModelFormat.Version
		}
	}

	if p.RuntimeName != "" {
		annotations[constants.ServingRuntimeKeyName] = p.RuntimeName
	}

	return annotations, nil
}

// ProcessBaseLabels processes common labels
func ProcessBaseLabels(p *Piece, isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType, labels map[string]string) (map[string]string, error) {
	baseModelCategory := "SMALL"
	if p.BaseModelMeta != nil {
		if category, ok := p.BaseModelMeta.Annotations[constants.ModelCategoryAnnotation]; ok {
			baseModelCategory = category
		}
	}

	baseLabels := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(componentType),
		constants.ServingRuntimeLabelKey:      p.RuntimeName,
		constants.FTServingLabelKey:           strconv.FormatBool(p.FineTunedServing),
	}

	// Merge with provided labels
	if labels == nil {
		labels = make(map[string]string)
	}
	for k, v := range baseLabels {
		labels[k] = v
	}

	if p.BaseModelMeta != nil {
		labels[constants.InferenceServiceBaseModelNameLabelKey] = p.BaseModelMeta.Name
		labels[constants.InferenceServiceBaseModelSizeLabelKey] = baseModelCategory
		labels[constants.BaseModelTypeLabelKey] = string(constants.ServingBaseModel)
	}

	if p.BaseModel != nil && p.BaseModel.Vendor != nil {
		labels[constants.BaseModelVendorLabelKey] = *p.BaseModel.Vendor
	}

	// Add fine-tuned serving related labels
	if p.FineTunedServing && len(p.FineTunedWeights) > 0 {
		ftStrategyParameter, err := isvcutils.GetValueFromRawExtension(p.FineTunedWeights[0].Spec.HyperParameters, constants.StrategyConfigKey)
		if err != nil {
			p.Log.Error(err, "Error getting hyper-parameter strategy from FineTunedWeight", "FineTunedWeight", p.FineTunedWeights[0].Name, "namespace", isvc.Namespace)
			return nil, err
		}

		fineTunedWeightFTStrategy := ""
		if ftStrategyParameter != nil {
			s, ok := ftStrategyParameter.(string)
			if !ok {
				return nil, fmt.Errorf("hyper-parameter %q on FineTunedWeight %s must be a string, got %T", constants.StrategyConfigKey, p.FineTunedWeights[0].Name, ftStrategyParameter)
			}
			fineTunedWeightFTStrategy = s
		}
		labels[constants.FineTunedWeightFTStrategyLabelKey] = fineTunedWeightFTStrategy

		labels[constants.FTServingWithMergedWeightsLabelKey] = strconv.FormatBool(p.FineTunedServingWithMergedWeights)
	}

	return labels, nil
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
// annotations the base layer adds.
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
