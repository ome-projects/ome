package utils

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// GetInferenceServiceModel keeps primary-model resolution consistent across
// admission and reconciliation. Ungated services historically preferred the
// namespaced model, even when the API defaulted kind to ClusterBaseModel.
// Gate presence opts into scope-aware resolution; gate validity is checked
// separately so pending admission uses the same model as admitted workloads.
func GetInferenceServiceModel(ctx context.Context, reader client.Reader, isvc *v1beta1.InferenceService) (*v1beta1.BaseModelSpec, *metav1.ObjectMeta, *v1beta1.ModelStatusSpec, error) {
	if isvc.Spec.Model == nil || isvc.Spec.Model.Name == "" {
		return nil, nil, nil, fmt.Errorf("model reference is required")
	}
	ref := isvc.Spec.Model
	if _, gated := isvc.Annotations[constants.ArtifactStartupGateAnnotation]; !gated {
		return GetBaseModelWithStatus(reader, ref.Name, isvc.Namespace)
	}
	return GetReferencedModel(ctx, reader, isvc.Namespace, ref.Name, ref.Kind, ref.APIGroup)
}

// GetReferencedModel honors explicit scope; legacy unqualified references prefer
// a namespaced model and fall back to the cluster model.
func GetReferencedModel(ctx context.Context, reader client.Reader, namespace, name string, kind, group *string) (*v1beta1.BaseModelSpec, *metav1.ObjectMeta, *v1beta1.ModelStatusSpec, error) {
	if group != nil && *group != "" && *group != v1beta1.SchemeGroupVersion.Group {
		return nil, nil, nil, fmt.Errorf("unsupported model API group %q", *group)
	}
	if kind == nil || *kind == "" {
		return GetBaseModelWithStatus(reader, name, namespace)
	}
	switch *kind {
	case "BaseModel":
		model := &v1beta1.BaseModel{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, model); err != nil {
			return nil, nil, nil, err
		}
		return &model.Spec, &model.ObjectMeta, &model.Status, nil
	case "ClusterBaseModel":
		model := &v1beta1.ClusterBaseModel{}
		if err := reader.Get(ctx, client.ObjectKey{Name: name}, model); err != nil {
			return nil, nil, nil, err
		}
		return &model.Spec, &model.ObjectMeta, &model.Status, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported model kind %q", *kind)
	}
}

// ValidateArtifactStartupGate only reads intent. MP owns admission and request CAS.
func ValidateArtifactStartupGate(ctx context.Context, reader client.Reader, isvc *v1beta1.InferenceService) error {
	gate, present := isvc.Annotations[constants.ArtifactStartupGateAnnotation]
	if !present {
		return nil
	}
	if gate != constants.ArtifactStartupGateAdmitted {
		return fmt.Errorf("artifact startup gate is %q", gate)
	}
	if isvc.Spec.Model == nil || isvc.Spec.Model.Name == "" {
		return fmt.Errorf("artifact startup gate requires a model reference")
	}
	_, metadata, _, err := GetInferenceServiceModel(ctx, reader, isvc)
	if err != nil {
		return err
	}
	return ValidateArtifactBinding(isvc, metadata)
}

// ValidateArtifactBinding rejects a gate bound to a different incarnation or request.
func ValidateArtifactBinding(isvc *v1beta1.InferenceService, metadata *metav1.ObjectMeta) error {
	gate, present := isvc.Annotations[constants.ArtifactStartupGateAnnotation]
	if !present {
		return nil
	}
	if gate != constants.ArtifactStartupGateAdmitted {
		return fmt.Errorf("artifact startup gate is %q", gate)
	}
	if metadata == nil || metadata.UID == "" || string(metadata.UID) != isvc.Annotations[constants.ArtifactModelUIDAnnotation] {
		return fmt.Errorf("artifact startup gate model UID does not match the referenced model")
	}
	request := isvc.Annotations[constants.ModelArtifactRehydrationIDAnnotation]
	if len(validation.IsValidLabelValue(request)) != 0 || request != metadata.Annotations[constants.ModelArtifactRehydrationIDAnnotation] {
		return fmt.Errorf("artifact startup gate request does not match the current model request")
	}
	marker := isvc.Annotations[constants.ArtifactRehydrationGenerationAnnotation]
	if request == "" && marker != "" && !strings.HasPrefix(marker, "completed:") {
		return fmt.Errorf("artifact restoration requires a request binding")
	}
	if !metadata.DeletionTimestamp.IsZero() || metadata.Annotations[constants.ModelArtifactResidencyAnnotation] == constants.ModelArtifactResidencyEvicted {
		return fmt.Errorf("artifact startup gate model is evicted or deleting")
	}
	_, err := constants.ArtifactReadyLabelKey(metadata.UID)
	return err
}
