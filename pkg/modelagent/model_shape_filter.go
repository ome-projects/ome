package modelagent

import (
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils"
)

// Scout and recovery select the same model type and shape-specific file subset.
func newModelShapeFilter(spec v1beta1.BaseModelSpec, shapeAlias string) *TensorRTLLMShapeFilter {
	modelType := string(constants.ServingBaseModel)
	if value, present := spec.AdditionalMetadata["type"]; present {
		modelType = value
	}
	return &TensorRTLLMShapeFilter{
		IsTensorrtLLMModel: spec.ModelFormat.Name == constants.TensorRTLLM,
		ShapeAlias:         shapeAlias, ModelType: modelType,
	}
}

func modelNodeShapeAlias(node *corev1.Node) (string, error) {
	instanceType, present := node.Labels[constants.NodeInstanceShapeLabel]
	if !present {
		instanceType = node.Labels[constants.DeprecatedNodeInstanceShapeLabel]
	}
	return utils.GetInstanceTypeShortName(instanceType)
}
