package pernode

import (
	"context"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// ReconcileStatusFromConfigMaps drives the per-node ConfigMap
// handshake: list per-node model-status ConfigMaps, parse this model's
// entries, and reflect into Status.NodesReady/NodesFailed plus
// LifeCycleState. Sharded + PVC backends bypass this path entirely.
func ReconcileStatusFromConfigMaps(ctx context.Context, c client.Client, nodeReader client.Reader, log logr.Logger, obj client.Object, isClusterScoped bool, kind string) error {
	var namespace string
	if !isClusterScoped {
		namespace = obj.GetNamespace()
	}
	specUpdate := func(ctx context.Context, config *shared.ModelConfig) error {
		return retrySpecUpdate(ctx, c, log, obj, config, func(ctx context.Context, kubeClient client.Client, latest client.Object, config *shared.ModelConfig) error {
			latestSpec, _, err := shared.ModelSpecAndStatus(latest)
			if err != nil {
				return err
			}
			return updateModelSpecWithConfig(ctx, kubeClient, log, latest, latestSpec, config, kind)
		})
	}
	statusUpdate := func(ctx context.Context, nodesReady, nodesFailed []string) error {
		return updateModelStatusWithRetry(ctx, c, log, obj, nodesReady, nodesFailed, kind)
	}
	return processModelStatus(ctx, c, nodeReader, log, namespace, obj.GetName(), isClusterScoped, specUpdate, statusUpdate)
}
