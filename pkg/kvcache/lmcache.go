// Package kvcache defines the node-local connection shared by the pool controller and vLLM.
package kvcache

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// Consumer and server Pods have distinct labels so that the Service only routes
	// to servers and the pool finalizer only waits for consumers.
	PoolUIDLabel       = "ome.io/kvcachepool-uid"
	ServerPoolUIDLabel = "ome.io/kvcachepool-server-uid"
	ConnectionKey      = "kv-transfer-config.json"
	ConnectorClass     = "LMCacheMPConnector"
	TransferPort       = 5555
)

func ServerName(uid types.UID) string { return "ome-kv-" + string(uid) }

// ConnectionJSON uses a namespace-qualified Service name so it also works in
// clusters with a custom DNS suffix.
func ConnectionJSON(uid types.UID, namespace string) string {
	return fmt.Sprintf(`{"kv_connector":%q,"kv_role":"kv_both","kv_connector_extra_config":{"lmcache.mp.host":%q,"lmcache.mp.port":"%d"}}`,
		ConnectorClass, "tcp://"+ServerName(uid)+"."+namespace, TransferPort)
}

func Connection(cm *corev1.ConfigMap, uid types.UID) (string, error) {
	if cm.Name != ServerName(uid)+"-connection" || cm.Labels[ServerPoolUIDLabel] != string(uid) || len(cm.OwnerReferences) != 0 {
		return "", fmt.Errorf("ConfigMap %q does not belong to this KVCachePool", cm.Name)
	}
	raw := cm.Data[ConnectionKey]
	if raw != ConnectionJSON(uid, cm.Namespace) {
		return "", fmt.Errorf("ConfigMap %q does not describe the pool's node-local LMCache endpoint", cm.Name)
	}
	return raw, nil
}
