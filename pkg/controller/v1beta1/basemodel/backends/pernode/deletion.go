package pernode

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// HandleModelDeletion waits for model-agent deletion acknowledgements, cleaning
// up status ConfigMaps for confirmed-absent Nodes before dropping the finalizer.
func HandleModelDeletion(ctx context.Context, kubeClient client.Client, nodeReader client.Reader, obj client.Object, finalizer string) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	if controllerutil.ContainsFinalizer(obj, finalizer) {
		// Before removing the finalizer, make sure all entries are cleared from ConfigMaps
		// This prevents orphaned models when nodes are down or agents aren't running

		// Determine model name, namespace and if it's cluster-scoped
		modelName := obj.GetName()
		var modelNamespace string
		var isClusterScope bool

		// Set namespace and scope based on object type
		switch typedObj := obj.(type) {
		case *v1beta1.BaseModel:
			modelNamespace = typedObj.Namespace
			isClusterScope = false
		case *v1beta1.ClusterBaseModel:
			modelNamespace = ""
			isClusterScope = true
		default:
			log.Error(fmt.Errorf("unknown model type"), "Invalid model type for deletion handler")
			return ctrl.Result{}, fmt.Errorf("unknown model type for deletion")
		}

		// Get the model's ConfigMap key
		modelKey := constants.GetModelConfigMapKey(modelNamespace, modelName, isClusterScope)

		// List all ConfigMaps with model status label in the ome namespace
		configMaps := &corev1.ConfigMapList{}
		listOpts := []client.ListOption{
			client.InNamespace(constants.OMENamespace),
			client.MatchingLabels{constants.ModelStatusConfigMapLabel: "true"},
		}

		if err := kubeClient.List(ctx, configMaps, listOpts...); err != nil {
			log.Error(err, "Failed to list ConfigMaps during model deletion")
			return ctrl.Result{RequeueAfter: time.Second * 10}, err
		}

		// Check if any ConfigMap still has an entry for this model that is not marked as deleted
		var modelsNotDeleted []string
		nodesWithModel := 0

		for i := range configMaps.Items {
			configMap := &configMaps.Items[i]
			// Check if the model exists in this ConfigMap
			data, exists := configMap.Data[modelKey]
			if !exists {
				continue
			}
			// A completed agent acknowledgement does not require Node availability.
			var modelEntry shared.ModelEntry
			if err := json.Unmarshal([]byte(data), &modelEntry); err == nil && modelEntry.Status == shared.ModelStatusDeleted {
				nodesWithModel++
				continue
			}

			orphaned, err := cleanupOrphanedNodeConfigMap(ctx, kubeClient, nodeReader, configMap)
			if err != nil {
				return ctrl.Result{}, err
			}
			if orphaned {
				continue
			}
			nodesWithModel++
			// Unacknowledged or malformed entries still block deletion on live Nodes.
			modelsNotDeleted = append(modelsNotDeleted, configMap.Name)
		}

		modelInfo := modelName
		if !isClusterScope {
			modelInfo = modelNamespace + "/" + modelName
		}

		// If models are still present in ConfigMaps and not deleted, requeue
		if len(modelsNotDeleted) > 0 {
			log.Info("Waiting for model to be cleared from ConfigMaps",
				"model", modelInfo,
				"nodesWithModel", nodesWithModel,
				"nodesNotDeleted", len(modelsNotDeleted),
				"nodes", modelsNotDeleted)

			// Requeue to check again later
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}

		log.Info("All model entries have been cleared or marked as deleted", "model", modelInfo)

		// All entries are either cleared or marked for deletion, safe to remove finalizer
		controllerutil.RemoveFinalizer(obj, finalizer)
		if err := kubeClient.Update(ctx, obj); err != nil {
			log.Error(err, "Failed to remove finalizer", "model", modelInfo)
			return ctrl.Result{}, err
		}
		log.Info("Finalizer removed, deletion complete", "model", modelInfo)
	}
	return ctrl.Result{}, nil
}
