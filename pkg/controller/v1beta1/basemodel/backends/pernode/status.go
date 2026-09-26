package pernode

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
	"sigs.k8s.io/ome/pkg/utils"
)

type modelStatusSnapshot struct {
	ready, failed, evicted []string
	configs                []*shared.ModelConfig
	evicting               bool
}

// collectModelStatus validates reports against current placement participants.
// A report's R is its last successful acknowledgement, not an
// attempt ID: non-Ready observations cannot acknowledge the current request.
func collectModelStatus(ctx context.Context, c client.Client, nodeReader client.Reader, log logr.Logger, obj client.Object, isClusterScoped bool) (modelStatusSnapshot, error) {
	var snapshot modelStatusSnapshot
	spec, _, err := shared.ModelSpecAndStatus(obj)
	if err != nil {
		return snapshot, err
	}
	request := obj.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation]
	nodes := map[string]corev1.Node{}
	if request != "" {
		if nodeReader == nil {
			return snapshot, fmt.Errorf("restoration status requires a current Node reader")
		}
		list := &corev1.NodeList{}
		if err := nodeReader.List(ctx, list); err != nil {
			return snapshot, fmt.Errorf("list restoration Nodes: %w", err)
		}
		for _, node := range list.Items {
			if utils.ModelMatchesNodePlacement(spec.Storage, &node) {
				nodes[node.Name] = node
			}
		}
	}
	configMaps := &corev1.ConfigMapList{}
	if err := c.List(ctx, configMaps, client.InNamespace(constants.OMENamespace), client.MatchingLabels{constants.ModelStatusConfigMapLabel: "true"}); err != nil {
		return snapshot, fmt.Errorf("list model ConfigMaps: %w", err)
	}
	modelKey := constants.GetModelConfigMapKey(obj.GetNamespace(), obj.GetName(), isClusterScoped)
	for _, cm := range configMaps.Items {
		data, exists := cm.Data[modelKey]
		if !exists {
			continue
		}
		orphaned, err := cleanupOrphanedNodeConfigMap(ctx, c, nodeReader, &cm)
		if err != nil {
			return snapshot, err
		}
		if orphaned {
			continue
		}
		var entry shared.ModelEntry
		if err := json.Unmarshal([]byte(data), &entry); err != nil {
			log.Error(err, "Ignoring malformed model report", "node", cm.Name, "key", modelKey)
			continue
		}
		if entry.ModelUID != "" && entry.ModelUID != obj.GetUID() {
			continue
		}
		currentAcknowledgement := true
		if request != "" {
			node, eligible := nodes[cm.Name]
			if !eligible || obj.GetUID() == "" || node.UID == "" || entry.ModelUID != obj.GetUID() || entry.NodeUID != node.UID {
				continue
			}
			currentAcknowledgement = entry.Status == shared.ModelStatusReady && entry.ArtifactRehydrationID == request
		}
		if currentAcknowledgement && entry.Config != nil {
			snapshot.configs = append(snapshot.configs, entry.Config)
		}
		switch entry.Status {
		case shared.ModelStatusReady:
			if currentAcknowledgement {
				snapshot.ready = addToSlice(snapshot.ready, cm.Name)
			}
		case shared.ModelStatusFailed:
			snapshot.failed = addToSlice(snapshot.failed, cm.Name)
		case shared.ModelStatusEvicted:
			snapshot.evicted = addToSlice(snapshot.evicted, cm.Name)
		case shared.ModelStatusEvicting:
			snapshot.evicting = true
		}
	}
	slices.Sort(snapshot.ready)
	slices.Sort(snapshot.failed)
	slices.Sort(snapshot.evicted)
	return snapshot, nil
}

func (s modelStatusSnapshot) lifecycleState() v1beta1.LifeCycleState {
	if len(s.ready) > 0 {
		return v1beta1.LifeCycleStateReady
	}
	if s.evicting {
		return v1beta1.LifeCycleStateInTransit
	}
	if len(s.evicted) > 0 {
		return v1beta1.LifeCycleStateEvicted
	}
	return CalculateLifecycleState(s.ready, s.failed)
}

func CalculateLifecycleState(nodesReady, nodesFailed []string) v1beta1.LifeCycleState {
	if len(nodesReady) > 0 {
		return v1beta1.LifeCycleStateReady
	}
	if len(nodesFailed) > 0 {
		return v1beta1.LifeCycleStateFailed
	}
	return v1beta1.LifeCycleStateInTransit
}

func addToSlice(s []string, item string) []string {
	if !slices.Contains(s, item) {
		return append(s, item)
	}
	return s
}
