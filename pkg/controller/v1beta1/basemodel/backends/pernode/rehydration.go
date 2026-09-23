package pernode

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

type rehydrationSnapshot struct {
	ready, failed, evicted []string
	configs                []*shared.ModelConfig
	inProgress, complete   bool
}

func readRehydrationSnapshot(ctx context.Context, reader client.Reader, obj client.Object, request string) (rehydrationSnapshot, error) {
	result := rehydrationSnapshot{complete: true}
	if reader == nil {
		return result, fmt.Errorf("authoritative reader is required to observe rehydration")
	}
	readyKey, err := constants.ArtifactReadyLabelKey(obj.GetUID())
	if err != nil {
		return result, err
	}
	_, cluster := obj.(*v1beta1.ClusterBaseModel)
	label := constants.GetBaseModelLabel(obj.GetNamespace(), obj.GetName())
	if cluster {
		label = constants.GetClusterBaseModelLabel(obj.GetName())
	}
	key := constants.GetModelConfigMapKey(obj.GetNamespace(), obj.GetName(), cluster)
	spec, _, err := shared.ModelSpecAndStatus(obj)
	if err != nil {
		return result, err
	}
	nodes := &corev1.NodeList{}
	if err := reader.List(ctx, nodes); err != nil {
		return result, fmt.Errorf("list rehydration participants: %w", err)
	}
	participants := 0
	for _, node := range nodes.Items {
		matches, explicit, err := matchesArtifactPlacement(spec.Storage, &node)
		if err != nil {
			return result, err
		}
		_, labeled := node.Labels[label]
		if !matches || (!explicit && !labeled) {
			continue
		}
		participants++
		cm := &corev1.ConfigMap{}
		err = reader.Get(ctx, client.ObjectKey{Namespace: constants.OMENamespace, Name: node.Name}, cm)
		if err != nil && !apierrors.IsNotFound(err) {
			return result, fmt.Errorf("read report for node %s: %w", node.Name, err)
		}
		var entry shared.ModelEntry
		if err != nil || !IsModelStatusConfigMap(cm) || node.UID == "" || cm.Annotations[constants.ModelArtifactNodeUIDAnnotation] != string(node.UID) || json.Unmarshal([]byte(cm.Data[key]), &entry) != nil || entry.ModelUID != obj.GetUID() {
			result.complete = false
			result.inProgress = true
			continue
		}
		switch entry.Status {
		case shared.ModelStatusReady:
			if entry.ArtifactRehydrationID == request && node.Labels[label] == "Ready" && node.Labels[readyKey] == request {
				result.ready = append(result.ready, node.Name)
				if entry.Config != nil {
					result.configs = append(result.configs, entry.Config)
				}
			} else {
				result.complete = false
				result.inProgress = true
			}
		case shared.ModelStatusEvicted:
			result.evicted = append(result.evicted, node.Name)
			result.complete = false
		case shared.ModelStatusFailed:
			result.failed = append(result.failed, node.Name)
			result.complete = false
		default:
			result.complete = false
			result.inProgress = true
		}
	}
	result.complete = result.complete && participants > 0
	slices.Sort(result.ready)
	slices.Sort(result.failed)
	slices.Sort(result.evicted)
	return result, nil
}

func matchesArtifactPlacement(storage *v1beta1.StorageSpec, node *corev1.Node) (bool, bool, error) {
	if storage == nil {
		return true, false, nil
	}
	explicit := len(storage.NodeSelector) > 0
	if !labels.SelectorFromSet(storage.NodeSelector).Matches(labels.Set(node.Labels)) {
		return false, explicit, nil
	}
	if storage.NodeAffinity == nil || storage.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true, explicit, nil
	}
	terms := storage.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for _, term := range terms {
		// Kubernetes empty node selector terms match no nodes.
		if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
			continue
		}
		matched := true
		for _, expr := range term.MatchExpressions {
			op := map[corev1.NodeSelectorOperator]selection.Operator{corev1.NodeSelectorOpIn: selection.In, corev1.NodeSelectorOpNotIn: selection.NotIn, corev1.NodeSelectorOpExists: selection.Exists, corev1.NodeSelectorOpDoesNotExist: selection.DoesNotExist, corev1.NodeSelectorOpGt: selection.GreaterThan, corev1.NodeSelectorOpLt: selection.LessThan}[expr.Operator]
			req, err := labels.NewRequirement(expr.Key, op, expr.Values)
			if err != nil {
				return false, true, err
			}
			matched = matched && req.Matches(labels.Set(node.Labels))
		}
		for _, expr := range term.MatchFields {
			if expr.Key != "metadata.name" {
				return false, true, fmt.Errorf("unsupported node placement field %q", expr.Key)
			}
			in := slices.Contains(expr.Values, node.Name)
			matched = matched && ((expr.Operator == corev1.NodeSelectorOpIn && in) || (expr.Operator == corev1.NodeSelectorOpNotIn && !in))
		}
		if matched {
			return true, true, nil
		}
	}
	return false, true, nil
}
