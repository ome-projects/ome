package sliceprovision

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// unavailableHosts returns, sorted, why each node of slice name cannot take
// any of the pods. Only a node's health counts, as the scheduler judges it: a
// cordoned node by the unschedulable taint, a node that is not ready by the
// not-ready or unreachable taint its Ready condition maps to. A pod may
// tolerate each of them.
func unavailableHosts(ctx context.Context, nodes client.Reader, sliceLabel, name string, pods []*corev1.PodSpec) ([]string, error) {
	list := &corev1.NodeList{}
	if err := nodes.List(ctx, list, client.MatchingLabels{sliceLabel: name}); err != nil {
		return nil, fmt.Errorf("list the nodes of slice %s: %w", name, err)
	}
	var out []string
	for i := range list.Items {
		if why := unavailable(ctx, &list.Items[i], pods); why != "" {
			out = append(out, fmt.Sprintf("node %s is %s", list.Items[i].Name, why))
		}
	}
	sort.Strings(out)
	return out, nil
}

// unavailable is why none of the pods can be scheduled on node, or empty when
// one can.
func unavailable(ctx context.Context, node *corev1.Node, pods []*corev1.PodSpec) string {
	if node.Spec.Unschedulable && !tolerated(ctx, pods, corev1.TaintNodeUnschedulable) {
		return "cordoned"
	}
	switch readyStatus(node) {
	case corev1.ConditionTrue:
		return ""
	case corev1.ConditionUnknown:
		if !tolerated(ctx, pods, corev1.TaintNodeUnreachable) {
			return "unreachable"
		}
	default:
		if !tolerated(ctx, pods, corev1.TaintNodeNotReady) {
			return "not ready"
		}
	}
	return ""
}

// readyStatus is the status of node's Ready condition, empty when it has none.
func readyStatus(node *corev1.Node) corev1.ConditionStatus {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status
		}
	}
	return ""
}

// tolerated reports whether any of the pods tolerates the NoSchedule taint
// key. The taint carries no value, so a numeric comparison never matches it.
func tolerated(ctx context.Context, pods []*corev1.PodSpec, key string) bool {
	taint := &corev1.Taint{Key: key, Effect: corev1.TaintEffectNoSchedule}
	for _, spec := range pods {
		if corev1helpers.TolerationsTolerateTaint(logf.FromContext(ctx), spec.Tolerations, taint, false) {
			return true
		}
	}
	return false
}
