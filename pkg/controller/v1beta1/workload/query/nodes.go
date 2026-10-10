package query

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeUnschedulableField is the Node field a cordon sets. The apiserver
// serves it as a Node field selector and RegisterNodeUnschedulableIndex
// makes the cache serve the same selector, so CordonedNodes issues one
// List against either reader. The name is the Kubernetes API's own field
// path.
const NodeUnschedulableField = "spec.unschedulable"

// NodeUnschedulableIndexExtractor is the cache IndexerFunc for
// NodeUnschedulableField. Every Node is indexed under "true" or "false",
// the values the apiserver's field selector compares against.
func NodeUnschedulableIndexExtractor(obj client.Object) []string {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}
	return []string{strconv.FormatBool(node.Spec.Unschedulable)}
}

// RegisterNodeUnschedulableIndex installs the NodeUnschedulableField index
// on the supplied indexer (mgr.GetFieldIndexer()). Call once during manager
// setup, before Start. The index lives on the cluster-wide Node informer,
// which registering it opens when no other controller has.
func RegisterNodeUnschedulableIndex(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &corev1.Node{}, NodeUnschedulableField, NodeUnschedulableIndexExtractor)
}

// CordonedNodes returns the names of the nodes marked unschedulable, nil
// when there are none. A reader without the index (an index-less cache or
// fake client) is answered by listing every Node and filtering, which
// returns the same set. The names are only read, so the cache hands the
// objects over without copying them.
func CordonedNodes(ctx context.Context, reader client.Reader) (map[string]struct{}, error) {
	nodes := &corev1.NodeList{}
	err := reader.List(ctx, nodes, client.MatchingFields{NodeUnschedulableField: "true"}, client.UnsafeDisableDeepCopy)
	if indexUnavailable(err) {
		nodes = &corev1.NodeList{}
		err = reader.List(ctx, nodes, client.UnsafeDisableDeepCopy)
	}
	if err != nil {
		return nil, err
	}
	var out map[string]struct{}
	for i := range nodes.Items {
		if !nodes.Items[i].Spec.Unschedulable {
			continue
		}
		if out == nil {
			out = map[string]struct{}{}
		}
		out[nodes.Items[i].Name] = struct{}{}
	}
	return out, nil
}

// CordonedInstances returns the indices of the Instances with a pod still
// running on one of the cordoned nodes, nil when there are none. A pod
// already terminating, or in a terminal phase, holds nothing on its node,
// and an unbound pod sits on no node, so neither counts.
func CordonedInstances(podsByInstance map[int32][]*corev1.Pod, cordoned map[string]struct{}) map[int32]struct{} {
	if len(cordoned) == 0 {
		return nil
	}
	var out map[int32]struct{}
	for idx, pods := range podsByInstance {
		for _, pod := range pods {
			if !podHoldsCordonedNode(pod, cordoned) {
				continue
			}
			if out == nil {
				out = map[int32]struct{}{}
			}
			out[idx] = struct{}{}
			break
		}
	}
	return out
}

func podHoldsCordonedNode(pod *corev1.Pod, cordoned map[string]struct{}) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" {
		return false
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}
	_, ok := cordoned[pod.Spec.NodeName]
	return ok
}
