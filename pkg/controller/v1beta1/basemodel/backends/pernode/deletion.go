package pernode

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// HandleModelDeletion waits for model-agent deletion acknowledgements, cleaning
// up status ConfigMaps for confirmed-absent Nodes before dropping the finalizer.
func HandleModelDeletion(ctx context.Context, kubeClient client.Client, nodeReader client.Reader, obj client.Object, isClusterScoped bool, finalizer string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(obj, finalizer) {
		return ctrl.Result{}, nil
	}
	log := ctrl.LoggerFrom(ctx)
	var namespace string
	if !isClusterScoped {
		namespace = obj.GetNamespace()
	}
	model := modelDisplayName(namespace, obj.GetName(), isClusterScoped)

	entries, _, err := listNodeEntries(ctx, kubeClient, constants.GetModelConfigMapKey(namespace, obj.GetName(), isClusterScoped))
	if err != nil {
		log.Error(err, "Failed to list ConfigMaps during model deletion")
		return ctrl.Result{RequeueAfter: time.Second * 10}, err
	}

	// Every node that still records the model holds the finalizer until its
	// agent acknowledges the deletion or the Node itself is confirmed absent.
	var nodesNotDeleted []string
	nodesWithModel := 0
	for _, node := range entries {
		// A completed agent acknowledgement does not require Node availability.
		if node.decodeErr == nil && node.entry.Status == shared.ModelStatusDeleted {
			nodesWithModel++
			continue
		}
		orphaned, err := cleanupOrphanedNodeConfigMap(ctx, kubeClient, nodeReader, node.configMap)
		if err != nil {
			return ctrl.Result{}, err
		}
		if orphaned {
			continue
		}
		nodesWithModel++
		// Unacknowledged or malformed entries still block deletion on live Nodes.
		nodesNotDeleted = append(nodesNotDeleted, node.configMap.Name)
	}

	if len(nodesNotDeleted) > 0 {
		log.Info("Waiting for model to be cleared from ConfigMaps",
			"model", model,
			"nodesWithModel", nodesWithModel,
			"nodesNotDeleted", len(nodesNotDeleted),
			"nodes", nodesNotDeleted)
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}
	log.Info("All model entries have been cleared or marked as deleted", "model", model)

	controllerutil.RemoveFinalizer(obj, finalizer)
	if err := kubeClient.Update(ctx, obj); err != nil {
		// Repeat the deletion checks from fresh state after concurrent writes.
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		log.Error(err, "Failed to remove finalizer", "model", model)
		return ctrl.Result{}, err
	}
	log.Info("Finalizer removed, deletion complete", "model", model)
	return ctrl.Result{}, nil
}
