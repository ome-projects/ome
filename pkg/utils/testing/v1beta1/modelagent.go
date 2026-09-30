package v1beta1testing

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// SimulatePVCAgentReady writes the per-PVC status ConfigMap that the
// metadata-extraction Job would normally produce, mimicking a
// successful extraction. envtest has no kubelet so the actual Job
// pod cannot run; tests stand in by writing the ConfigMap directly.
//
// cfg is the ModelConfig the agent would have parsed from
// /model/config.json. Pass nil for a minimal Ready entry with no
// model metadata fields.
//
// Idempotent: if the ConfigMap already exists this is a no-op (the
// test's own loop will overwrite as needed).
func SimulatePVCAgentReady(ctx context.Context, c client.Client,
	modelName, modelNamespace string, isClusterScoped bool, cfg *shared.ModelConfig) error {

	cmName := constants.GetPVCMetadataConfigMapName(modelName, modelNamespace, isClusterScoped)
	modelKey := constants.GetModelConfigMapKey(modelNamespace, modelName, isClusterScoped)

	entry := shared.ModelEntry{
		Name:   modelName,
		Status: shared.ModelStatusReady,
		Config: cfg,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal ModelEntry: %w", err)
	}

	scope := "namespaced"
	if isClusterScoped {
		scope = "cluster"
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: constants.OMENamespace,
			Labels: map[string]string{
				constants.PVCStorageConfigMapLabel:  "true",
				constants.PVCMetadataModelNameLabel: modelName,
				constants.PVCMetadataScopeLabel:     scope,
			},
		},
		Data: map[string]string{modelKey: string(data)},
	}
	if err := c.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create per-PVC ConfigMap %q: %w", cmName, err)
	}
	return nil
}

// SimulatePVCAgentFailed writes a Failed-status per-PVC ConfigMap
// with an error annotation, driving the BaseModel reconciler's
// agent-failure path.
func SimulatePVCAgentFailed(ctx context.Context, c client.Client,
	modelName, modelNamespace string, isClusterScoped bool, errMsg string) error {

	cmName := constants.GetPVCMetadataConfigMapName(modelName, modelNamespace, isClusterScoped)
	modelKey := constants.GetModelConfigMapKey(modelNamespace, modelName, isClusterScoped)

	entry := shared.ModelEntry{Name: modelName, Status: shared.ModelStatusFailed}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal ModelEntry: %w", err)
	}

	scope := "namespaced"
	if isClusterScoped {
		scope = "cluster"
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: constants.OMENamespace,
			Labels: map[string]string{
				constants.PVCStorageConfigMapLabel:  "true",
				constants.PVCMetadataModelNameLabel: modelName,
				constants.PVCMetadataScopeLabel:     scope,
			},
			Annotations: map[string]string{
				constants.PVCMetadataLastErrorAnnotation: errMsg,
			},
		},
		Data: map[string]string{modelKey: string(data)},
	}
	if err := c.Create(ctx, cm); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create per-PVC Failed ConfigMap %q: %w", cmName, err)
	}
	return nil
}
