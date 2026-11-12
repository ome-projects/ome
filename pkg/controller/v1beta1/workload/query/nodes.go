package query

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
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

// NodeLookup answers whether a Node exists from the two Node readers: the
// informer cache first, which costs no request, and the live reader for a
// Node the cache does not hold, so a Node the cache has not received yet
// is never read as missing. Each name is read once per lookup, and a
// lookup lives for one pass. Not safe for concurrent use.
type NodeLookup struct {
	readers workload.NodeReaders
	exists  map[string]bool
}

// NewNodeLookup builds the lookup over the two Node readers. A nil live
// reader leaves the cache's answer as the last word.
func NewNodeLookup(readers workload.NodeReaders) *NodeLookup {
	return &NodeLookup{readers: readers}
}

// NodeExists reports whether the cluster holds a Node called name. The
// cached read skips the deep copy: only the answer is kept, never the
// object. A read error other than NotFound is returned, and the caller
// leaves the pods on that node alone this pass.
func (l *NodeLookup) NodeExists(ctx context.Context, name string) (bool, error) {
	if known, ok := l.exists[name]; ok {
		return known, nil
	}
	exists, err := nodeHeldBy(ctx, l.readers.Cache, name, client.UnsafeDisableDeepCopy)
	if err != nil {
		return false, err
	}
	if !exists && l.readers.Live != nil {
		exists, err = nodeHeldBy(ctx, l.readers.Live, name)
		if err != nil {
			return false, err
		}
	}
	if l.exists == nil {
		l.exists = map[string]bool{}
	}
	l.exists[name] = exists
	return exists, nil
}

// nodeHeldBy asks one reader for the Node, turning NotFound into false.
func nodeHeldBy(ctx context.Context, reader client.Reader, name string, opts ...client.GetOption) (bool, error) {
	err := reader.Get(ctx, client.ObjectKey{Name: name}, &corev1.Node{}, opts...)
	switch {
	case err == nil:
		return true, nil
	case apierrors.IsNotFound(err):
		return false, nil
	}
	return false, fmt.Errorf("read node %s: %w", name, err)
}

// PodNodeNameField is the Pod field a binding sets. The apiserver serves
// it as a Pod field selector and RegisterPodNodeNameIndex makes the cache
// serve the same selector, so PodsOnNode issues one List against either
// reader. The name is the Kubernetes API's own field path.
const PodNodeNameField = "spec.nodeName"

// PodNodeNameIndexExtractor is the cache IndexerFunc for PodNodeNameField:
// a bound pod is indexed under its node's name, an unbound one not at all.
func PodNodeNameIndexExtractor(obj client.Object) []string {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod.Spec.NodeName == "" {
		return nil
	}
	return []string{pod.Spec.NodeName}
}

// RegisterPodNodeNameIndex installs the PodNodeNameField index on the
// supplied indexer (mgr.GetFieldIndexer()). Call once during manager
// setup, before Start.
func RegisterPodNodeNameIndex(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &corev1.Pod{}, PodNodeNameField, PodNodeNameIndexExtractor)
}

// PodsOnNode returns the OMENative pods bound to the named node, from any
// namespace. A reader without the index is answered by listing the
// managed pods and filtering, which returns the same set.
func PodsOnNode(ctx context.Context, reader client.Reader, node string) ([]*corev1.Pod, error) {
	pods := &corev1.PodList{}
	managed := client.MatchingLabels{LabelManagedBy: ManagedByOMENative}
	err := reader.List(ctx, pods, managed, client.MatchingFields{PodNodeNameField: node})
	if indexUnavailable(err) {
		pods = &corev1.PodList{}
		err = reader.List(ctx, pods, managed)
	}
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		if pods.Items[i].Spec.NodeName == node {
			out = append(out, &pods.Items[i])
		}
	}
	return out, nil
}

// SplitPodsOnMissingNodes tells the pods still present apart from the pods
// bound to a Node the cluster no longer holds. Such a pod runs nowhere: no
// kubelet reports it, no kubelet will remove it, and the pod garbage
// collector takes it only on its own schedule. The engine reads it as gone
// at once and removes the object itself.
//
// Two kinds of pod stay present whatever their node: an unbound pod has
// no Node to lose, and a terminal pod is one every presence decision
// already leaves out, which its owning operation removes. A pod
// another controller's finalizer keeps in the API is gone all the same:
// it runs nowhere, and the object leaves once that controller lets go.
// present keeps the input order and is the input slice itself when
// nothing is gone; gone is in name order, so re-reading the same pods
// yields the same records. A nil nodes reads every pod as present.
func SplitPodsOnMissingNodes(ctx context.Context, nodes workload.NodePresence, pods []*corev1.Pod) (present, gone []*corev1.Pod, err error) {
	if nodes == nil {
		return pods, nil, nil
	}
	var lost map[*corev1.Pod]struct{}
	for _, pod := range pods {
		missing, err := boundToMissingNode(ctx, nodes, pod)
		if err != nil {
			return nil, nil, err
		}
		if !missing {
			continue
		}
		if lost == nil {
			lost = map[*corev1.Pod]struct{}{}
		}
		lost[pod] = struct{}{}
		gone = append(gone, pod)
	}
	if len(gone) == 0 {
		return pods, nil, nil
	}
	present = make([]*corev1.Pod, 0, len(pods)-len(gone))
	for _, pod := range pods {
		if _, missing := lost[pod]; !missing {
			present = append(present, pod)
		}
	}
	sort.Slice(gone, func(i, j int) bool { return gone[i].Name < gone[j].Name })
	return present, gone, nil
}

// boundToMissingNode reports whether pod is bound to a Node nodes does not
// hold, and is a pod the engine may read as gone for it.
func boundToMissingNode(ctx context.Context, nodes workload.NodePresence, pod *corev1.Pod) (bool, error) {
	if pod == nil || pod.Spec.NodeName == "" || IsTerminalPod(pod) {
		return false, nil
	}
	exists, err := nodes.NodeExists(ctx, pod.Spec.NodeName)
	if err != nil {
		return false, err
	}
	return !exists, nil
}
