package pernode

import (
	"context"
	"encoding/json"

	"github.com/go-logr/logr"
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

// readAndCleanupModelReports decodes reports, removes orphan ConfigMaps, and
// excludes malformed entries and reports belonging to another Model instance.
func readAndCleanupModelReports(ctx context.Context, c client.Client, nodeReader client.Reader, log logr.Logger, obj client.Object, isClusterScoped bool) ([]nodeEntry, error) {
	key := constants.GetModelConfigMapKey(obj.GetNamespace(), obj.GetName(), isClusterScoped)
	reports, _, err := listNodeEntries(ctx, c, key)
	if err != nil {
		return nil, err
	}
	valid := reports[:0]
	for _, report := range reports {
		orphaned, err := cleanupOrphanedNodeConfigMap(ctx, c, nodeReader, report.configMap)
		if err != nil {
			return nil, err
		}
		if orphaned {
			continue
		}
		if report.decodeErr != nil {
			log.Error(report.decodeErr, "Ignoring malformed model report", "node", report.configMap.Name, "key", key)
			continue
		}
		if report.entry.ModelUID != "" && report.entry.ModelUID != obj.GetUID() {
			continue
		}
		valid = append(valid, report)
	}
	return valid, nil
}
