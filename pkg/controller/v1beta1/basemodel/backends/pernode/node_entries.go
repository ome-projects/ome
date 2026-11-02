package pernode

import (
	"context"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// modelDisplayName is the model as log lines name it: the bare name for a
// ClusterBaseModel, namespace/name for a BaseModel.
func modelDisplayName(namespace, name string, clusterScoped bool) string {
	if clusterScoped {
		return name
	}
	return namespace + "/" + name
}

// nodeEntry is one node's record of a model: the status ConfigMap the agent
// on that node writes, and the model's entry decoded from it. decodeErr is
// set when the stored data does not parse.
type nodeEntry struct {
	configMap *corev1.ConfigMap
	entry     shared.ModelEntry
	decodeErr error
}

// listNodeEntries lists the per-node model-status ConfigMaps in the operator
// namespace and decodes the entry stored under key from each ConfigMap that
// carries one. It returns the entries in list order and the number of
// ConfigMaps listed.
func listNodeEntries(ctx context.Context, c client.Client, key string) ([]nodeEntry, int, error) {
	configMaps := &corev1.ConfigMapList{}
	listOpts := []client.ListOption{
		client.InNamespace(constants.OMENamespace),
		client.MatchingLabels{constants.ModelStatusConfigMapLabel: "true"},
	}
	if err := c.List(ctx, configMaps, listOpts...); err != nil {
		return nil, 0, err
	}
	var entries []nodeEntry
	for i := range configMaps.Items {
		configMap := &configMaps.Items[i]
		data, ok := configMap.Data[key]
		if !ok {
			continue
		}
		node := nodeEntry{configMap: configMap}
		node.decodeErr = json.Unmarshal([]byte(data), &node.entry)
		entries = append(entries, node)
	}
	return entries, len(configMaps.Items), nil
}
