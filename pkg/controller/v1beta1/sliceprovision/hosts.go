package sliceprovision

import (
	"context"
	"fmt"
	"slices"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	resourcehelper "k8s.io/component-helpers/resource"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// hostReport is why the nodes of a slice cannot take any of its pods,
// each list sorted. unhealthy holds the nodes the scheduler refuses for their
// health. tooSmall holds the healthy nodes where the pods do not fit even
// next to node-wide pods alone: DaemonSet pods and static pods run on every
// matching node, so another slice's nodes would not fit them either. short
// holds the healthy nodes where other pods leave too little of a resource the
// pods request, which another slice may avoid. found is how many nodes the
// slice has.
type hostReport struct {
	unhealthy []string
	tooSmall  []string
	short     []string
	found     int
}

// reasons are all of the report's reasons, unhealthy nodes first.
func (v hostReport) reasons() []string {
	return slices.Concat(v.unhealthy, v.tooSmall, v.short)
}

// unavailableHosts checks each node of slice name as the scheduler would for
// any of the pods. A node is unhealthy when it is cordoned, by the
// unschedulable taint, or not ready, by the not-ready or unreachable taint
// its Ready condition maps to; a pod may tolerate each of them. A healthy
// node is too small when its node-wide pods alone leave too little of a
// resource the pods request, and short when the other pods bound to it do. bound must select pods by the node they
// are bound to; it is read only for a healthy node when the pods request a
// resource other than chip, which the slice's own pods hold by construction.
func unavailableHosts(ctx context.Context, nodes, bound client.Reader, sliceLabel, name string, chip corev1.ResourceName, pods []*corev1.PodSpec) (hostReport, error) {
	list := &corev1.NodeList{}
	if err := nodes.List(ctx, list, client.MatchingLabels{sliceLabel: name}); err != nil {
		return hostReport{}, fmt.Errorf("list the nodes of slice %s: %w", name, err)
	}
	requests := podRequests(pods, chip)
	v := hostReport{found: len(list.Items)}
	for i := range list.Items {
		node := &list.Items[i]
		if why := unavailable(ctx, node, pods); why != "" {
			v.unhealthy = append(v.unhealthy, fmt.Sprintf("node %s is %s", node.Name, why))
			continue
		}
		if len(requests) == 0 {
			continue
		}
		free, err := unrequested(ctx, bound, sliceLabel, name, node)
		if err != nil {
			return hostReport{}, err
		}
		if why := shortfall(free.nodeWide, requests, "left next to its DaemonSet and static pods"); why != "" {
			v.tooSmall = append(v.tooSmall, fmt.Sprintf("node %s has %s", node.Name, why))
		} else if why := shortfall(free.all, requests, "left unrequested"); why != "" {
			v.short = append(v.short, fmt.Sprintf("node %s has %s", node.Name, why))
		}
	}
	sort.Strings(v.unhealthy)
	sort.Strings(v.tooSmall)
	sort.Strings(v.short)
	return v, nil
}

// podRequests are the resource requests of each pod as the scheduler sums
// them once the API server has defaulted each missing request to its limit,
// without chip. It is empty when no pod requests anything else.
func podRequests(pods []*corev1.PodSpec, chip corev1.ResourceName) []corev1.ResourceList {
	var out []corev1.ResourceList
	nonEmpty := false
	for _, spec := range pods {
		pod := &corev1.Pod{Spec: *spec.DeepCopy()}
		requestLimits(pod)
		req := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
		delete(req, chip)
		for r, q := range req {
			if q.IsZero() {
				delete(req, r)
			}
		}
		nonEmpty = nonEmpty || len(req) > 0
		out = append(out, req)
	}
	if !nonEmpty {
		return nil
	}
	return out
}

// requestLimits gives each container a request for every resource it limits
// without requesting, as the API server does when it creates a pod.
func requestLimits(pod *corev1.Pod) {
	for _, containers := range [][]corev1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for i := range containers {
			res := &containers[i].Resources
			for r, q := range res.Limits {
				if _, ok := res.Requests[r]; ok {
					continue
				}
				if res.Requests == nil {
					res.Requests = corev1.ResourceList{}
				}
				res.Requests[r] = q.DeepCopy()
			}
		}
	}
}

// leftover is what a node has left of each allocatable resource: all once
// the requests of every pod bound to it are taken out, nodeWide once only
// those of its node-wide pods are.
type leftover struct {
	all      corev1.ResourceList
	nodeWide corev1.ResourceList
}

// unrequested is what node has left of each allocatable resource once the
// requests of the pods bound to it are taken out, as the scheduler counts
// it. Pods confined to slice name are left out: the slice is not held, so
// any such pod is on its way out and frees its share for the pods placed
// next.
func unrequested(ctx context.Context, bound client.Reader, sliceLabel, name string, node *corev1.Node) (leftover, error) {
	if bound == nil {
		return leftover{}, fmt.Errorf("read the pods on node %s of slice %s: no source of bound pods", node.Name, name)
	}
	pods := &corev1.PodList{}
	if err := bound.List(ctx, pods, client.MatchingFields{podNodeNameField: node.Name}); err != nil {
		return leftover{}, fmt.Errorf("list the pods on node %s of slice %s: %w", node.Name, name, err)
	}
	left := leftover{all: node.Status.Allocatable.DeepCopy(), nodeWide: node.Status.Allocatable.DeepCopy()}
	if left.all == nil {
		left.all, left.nodeWide = corev1.ResourceList{}, corev1.ResourceList{}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if query.IsTerminalPod(pod) || pod.Spec.NodeSelector[sliceLabel] == name {
			continue
		}
		wide := nodeWide(pod)
		for r, q := range resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{}) {
			subtract(left.all, r, q)
			if wide {
				subtract(left.nodeWide, r, q)
			}
		}
	}
	return left, nil
}

// nodeWide reports whether pod runs on every node it matches rather than on
// nodes a scheduler chose: a DaemonSet's pod, or a static pod, which the
// kubelet mirrors under its Node.
func nodeWide(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Controller != nil && *ref.Controller && (ref.Kind == "DaemonSet" || ref.Kind == "Node") {
			return true
		}
	}
	_, mirrored := pod.Annotations[corev1.MirrorPodAnnotationKey]
	return mirrored
}

func subtract(list corev1.ResourceList, r corev1.ResourceName, q resource.Quantity) {
	left := list[r]
	left.Sub(q)
	list[r] = left
}

// shortfall is why free cannot take any of the requests, worded to follow
// "node X has" with left saying what free is left of, or empty when one of
// them fits. The reason given is the first pod's first resource in name order
// that does not fit.
func shortfall(free corev1.ResourceList, requests []corev1.ResourceList, left string) string {
	why := ""
	for _, req := range requests {
		short := ""
		names := make([]corev1.ResourceName, 0, len(req))
		for r := range req {
			names = append(names, r)
		}
		slices.Sort(names)
		for _, r := range names {
			want, have := req[r], free[r]
			if want.Cmp(have) > 0 {
				short = fmt.Sprintf("%s of %s %s, but a pod requests %s", quantity(r, have), r, left, quantity(r, want))
				break
			}
		}
		if short == "" {
			return ""
		}
		if why == "" {
			why = short
		}
	}
	return why
}

// quantity formats q for a message: bytes in GiB, anything else as written.
func quantity(r corev1.ResourceName, q resource.Quantity) string {
	switch r {
	case corev1.ResourceMemory, corev1.ResourceEphemeralStorage:
		return fmt.Sprintf("%.1fGi", float64(q.Value())/(1<<30))
	}
	return q.String()
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
