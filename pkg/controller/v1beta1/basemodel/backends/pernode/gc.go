package pernode

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// cleanupOrphanedNodeConfigMap removes status metadata for an absent Node.
// nodeReader must bypass the cache: a cache miss alone cannot justify deletion.
// orphaned means the Node was absent and deletion was accepted (or the ConfigMap
// was already gone); it does not mean other ConfigMap finalizers have completed.
func cleanupOrphanedNodeConfigMap(ctx context.Context, kubeClient client.Client, nodeReader client.Reader, configMap *corev1.ConfigMap) (orphaned bool, err error) {
	if !IsModelStatusConfigMap(configMap) {
		return false, fmt.Errorf("refusing orphan cleanup of non-model-status ConfigMap %s", client.ObjectKeyFromObject(configMap))
	}
	// Keep healthy-node reconciliation cache-only. A stale cached presence can
	// delay cleanup, but a cached absence must never authorize deletion.
	nodeKey := types.NamespacedName{Name: configMap.Name}
	if err := kubeClient.Get(ctx, nodeKey, &corev1.Node{}); err == nil {
		return false, nil
	} else if !errors.IsNotFound(err) {
		return false, fmt.Errorf("check cached Node %s for orphan cleanup: %w", configMap.Name, err)
	}
	if nodeReader == nil {
		return false, fmt.Errorf("orphan cleanup requires an uncached Node reader")
	}
	if err := nodeReader.Get(ctx, nodeKey, &corev1.Node{}); err == nil {
		return false, nil
	} else if !errors.IsNotFound(err) {
		return false, fmt.Errorf("check Node %s for orphan cleanup: %w", configMap.Name, err)
	}

	// Fence against an agent updating the status, or replacing the ConfigMap,
	// after it was listed. Node identity is name-based, so these preconditions
	// cannot fence a same-name Node recreated between the lookup and deletion.
	uid, resourceVersion := configMap.UID, configMap.ResourceVersion
	if uid == "" || resourceVersion == "" {
		return false, fmt.Errorf("orphan ConfigMap %s has no UID or resourceVersion", client.ObjectKeyFromObject(configMap))
	}
	err = kubeClient.Delete(ctx, configMap, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion})
	if err != nil && !errors.IsNotFound(err) {
		return false, fmt.Errorf("delete orphan ConfigMap %s: %w", client.ObjectKeyFromObject(configMap), err)
	}
	if err == nil {
		ctrl.LoggerFrom(ctx).Info("Requested orphaned node ConfigMap cleanup", "configMap", client.ObjectKeyFromObject(configMap))
	}
	return true, nil
}
