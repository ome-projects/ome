package pernode

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// processModelStatus folds every node's entry for this model into the
// nodesReady and nodesFailed lists handed to statusUpdateFunc. Entries that
// carry agent-supplied Config pass through specUpdateFunc first.
func processModelStatus(ctx context.Context, kubeClient client.Client, nodeReader client.Reader, log logr.Logger, namespace, name string, isClusterScope bool,
	specUpdateFunc func(context.Context, *shared.ModelConfig) error,
	statusUpdateFunc func(context.Context, []string, []string) error) error {

	log = log.WithValues("model", modelDisplayName(namespace, name, isClusterScope))
	modelKey := constants.GetModelConfigMapKey(namespace, name, isClusterScope)
	entries, listed, err := listNodeEntries(ctx, kubeClient, modelKey)
	if err != nil {
		log.Error(err, "Failed to list ConfigMaps")
		return fmt.Errorf("failed to list ConfigMaps: %w", err)
	}

	log.Info("Processing model status from ConfigMaps", "configMapsTotal", listed)

	var validNodes, readyNodes, failedNodes int
	var nodesReady, nodesFailed, specUpdateErrors []string
	for _, node := range entries {
		orphaned, err := cleanupOrphanedNodeConfigMap(ctx, kubeClient, nodeReader, node.configMap)
		if err != nil {
			return err
		}
		if orphaned {
			continue
		}
		validNodes++

		nodeName := node.configMap.Name
		if node.decodeErr != nil {
			log.Error(node.decodeErr, "Failed to parse model entry", "node", nodeName, "key", modelKey)
			continue
		}
		entry := node.entry
		log.V(1).Info("Processing model entry", "node", nodeName, "status", entry.Status, "hasConfig", entry.Config != nil, "hasProgress", entry.Progress != nil)

		if entry.Config != nil {
			if err := specUpdateFunc(ctx, entry.Config); err != nil {
				log.Error(err, "Failed to update model spec", "node", nodeName)
				specUpdateErrors = append(specUpdateErrors, nodeName)
			}
		}

		switch entry.Status {
		case shared.ModelStatusReady:
			nodesReady = addToSlice(nodesReady, nodeName)
			readyNodes++
		case shared.ModelStatusFailed:
			nodesFailed = addToSlice(nodesFailed, nodeName)
			failedNodes++
		case shared.ModelStatusUpdating, shared.ModelStatusDeleted:
		default:
			log.V(1).Info("Unknown model status", "node", nodeName, "status", entry.Status)
		}
	}

	slices.Sort(nodesReady)
	slices.Sort(nodesFailed)

	log.Info("Model status summary",
		"readyNodes", readyNodes,
		"failedNodes", failedNodes,
		"totalProcessed", listed,
		"validNodes", validNodes)

	if len(specUpdateErrors) > 0 {
		log.Info("Some nodes failed spec updates", "failedNodes", specUpdateErrors)
	}

	return statusUpdateFunc(ctx, nodesReady, nodesFailed)
}

func updateModelSpecWithConfig(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, spec *v1beta1.BaseModelSpec, config *shared.ModelConfig, modelType string) error {
	if updated := shared.UpdateSpecWithConfig(spec, config); updated {
		if err := kubeClient.Update(ctx, obj); err != nil {
			return fmt.Errorf("failed to update %s spec: %w", modelType, err)
		}
		log.Info(fmt.Sprintf("Updated %s spec with configuration data", modelType),
			"name", obj.GetName(), "namespace", obj.GetNamespace())
	}
	return nil
}

func addToSlice(s []string, item string) []string {
	for _, existing := range s {
		if existing == item {
			return s
		}
	}
	return append(s, item)
}

func CalculateLifecycleState(nodesReady, nodesFailed []string) v1beta1.LifeCycleState {
	if len(nodesReady) > 0 {
		return v1beta1.LifeCycleStateReady
	} else if len(nodesFailed) > 0 {
		return v1beta1.LifeCycleStateFailed
	}
	return v1beta1.LifeCycleStateInTransit
}

func updateModelStatusWithRetry(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, nodesReady, nodesFailed []string, modelType string) error {
	updateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		_, status, err := shared.ModelSpecAndStatus(obj)
		if err != nil {
			return err
		}

		newState := CalculateLifecycleState(nodesReady, nodesFailed)
		if slices.Equal(status.NodesReady, nodesReady) &&
			slices.Equal(status.NodesFailed, nodesFailed) &&
			status.State == newState {
			return nil
		}

		status.NodesReady = nodesReady
		status.NodesFailed = nodesFailed
		status.State = newState
		shared.StampObservedReconcile(obj, status)

		if err := client.Status().Update(ctx, obj); err != nil {
			return err
		}
		log.Info(fmt.Sprintf("Updated %s status", modelType),
			"nodesReady", len(nodesReady),
			"nodesFailed", len(nodesFailed),
			"state", newState)
		return nil
	}

	return shared.RetryUpdate(ctx, kubeClient, log, obj, "status", updateFunc)
}

func retrySpecUpdate(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, config *shared.ModelConfig, updateFunc func(context.Context, client.Client, client.Object, *shared.ModelConfig) error) error {
	wrappedUpdateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		return updateFunc(ctx, client, obj, config)
	}
	return shared.RetryUpdate(ctx, kubeClient, log, obj, "spec", wrappedUpdateFunc)
}
