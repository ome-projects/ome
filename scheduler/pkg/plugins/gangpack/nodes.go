package gangpack

import (
	"slices"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	resourcehelper "k8s.io/component-helpers/resource"
	v1helper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// The scheduler bakes in no workload conventions and no fabric values. Gang
// membership + size come from the standard scheduler-plugins PodGroup; the domain
// label is declared per-workload on the PodGroup (see gang.go); and "is a node
// free for this gang" is inferred from the GANG POD'S OWN resource requests — the
// pod already declares what it needs, so the scheduler is never told an
// accelerator resource name.

// domainOf returns the node's domain — the value of the given topology label — or
// "" when the node has no such label (not part of any domain).
func domainOf(node *v1.Node, topologyKey string) string {
	if node == nil {
		return ""
	}
	return node.Labels[topologyKey]
}

// nodeFitsPod mirrors the resource and hard node-placement constraints that can
// invalidate a domain choice before the framework's regular filters run: the
// node must fit every requested resource and one more pod beside what it already
// runs, gang members assumed on it included.
func nodeFitsPod(ni framework.NodeInfo, pod *v1.Pod) bool {
	if ni == nil || ni.Node() == nil || ni.GetAllocatable() == nil || ni.GetRequested() == nil || pod == nil {
		return false
	}
	node := ni.Node()
	if !nodeSchedulable(node, pod) {
		return false
	}
	if pod.Spec.NodeName != "" && pod.Spec.NodeName != node.Name {
		return false
	}
	match, err := nodeaffinity.GetRequiredNodeAffinity(pod).Match(node)
	if err != nil || !match {
		return false
	}
	req := schedulerframework.NewResource(resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{}))
	allocatable, requested := ni.GetAllocatable(), ni.GetRequested()
	if allocatable.GetMilliCPU()-requested.GetMilliCPU() < req.GetMilliCPU() ||
		allocatable.GetMemory()-requested.GetMemory() < req.GetMemory() ||
		allocatable.GetEphemeralStorage()-requested.GetEphemeralStorage() < req.GetEphemeralStorage() ||
		allocatable.GetAllowedPodNumber() <= len(ni.GetPods()) {
		return false
	}
	for name, quantity := range req.ScalarResources {
		if allocatable.GetScalarResources()[name]-requested.GetScalarResources()[name] < quantity {
			return false
		}
	}
	return true
}

// boundGangPlacement is the result of scanning the node snapshot for pods
// already placed (assumed or bound) that share the pod's PodGroup: how many
// were found and the domain they sit in (the domain of the last such node
// seen; a healthy gang is single-domain). This is how the plugin recovers a
// gang whose in-memory pin was lost (scheduler restart / leader failover):
// the truth of where a gang lives is where its members already are. The pod
// being scheduled is not yet in any node's pod list, so it never counts
// itself.
type boundGangPlacement struct {
	domain string
	count  int
	split  bool
	// placed holds the gang members found in the snapshot, by name.
	placed map[string]*v1.Pod
}

// inspectBoundGangMembers also detects an already-split gang. Silently adopting
// one arbitrary domain would undercount the other domains and make the split
// worse after scheduler failover.
func inspectBoundGangMembers(nodeInfos []framework.NodeInfo, pod *v1.Pod, topologyKey string) boundGangPlacement {
	ns, pgName, ok := podGroupNameOf(pod)
	if !ok {
		return boundGangPlacement{}
	}
	result := boundGangPlacement{placed: make(map[string]*v1.Pod)}
	for _, info := range nodeInfos {
		n := info.Node()
		if n == nil {
			continue
		}
		dom := domainOf(n, topologyKey)
		for _, pi := range info.GetPods() {
			mp := pi.GetPod()
			if mp != nil && mp.Namespace == ns && mp.Labels[podGroupLabel] == pgName {
				result.placed[mp.Name] = mp
				if dom == "" {
					result.split = true
					result.count++
					continue
				}
				if result.domain != "" && result.domain != dom {
					result.split = true
				}
				result.domain = dom
				result.count++
			}
		}
	}
	return result
}

// placedInDomain counts a gang's members (namespace + pod-group name) occupying
// nodes of the given domain in the snapshot. Unlike inspectBoundGangMembers it matches a
// gang by its resolved key rather than a live pod object, so the reservation
// reconciler can count a pinned gang's real footprint without a pod in hand.
func placedInDomain(nodeInfos []framework.NodeInfo, namespace, pgName, topologyKey, domain string) int {
	count := 0
	for _, info := range nodeInfos {
		n := info.Node()
		if n == nil || domainOf(n, topologyKey) != domain {
			continue
		}
		for _, pi := range info.GetPods() {
			mp := pi.GetPod()
			if mp != nil && mp.Namespace == namespace && mp.Labels[podGroupLabel] == pgName {
				count++
			}
		}
	}
	return count
}

// nodeSchedulable reports whether the gang pod could actually be placed on this
// node right now — beyond having the accelerator free, the node must be
// schedulable: not cordoned (spec.unschedulable), and its NoSchedule/NoExecute
// taints tolerated by the pod. Without this check, freeByDomain would count a
// cordoned or tainted node as free, so Choose could pin a domain whose nodes the
// framework's own Filter (NodeUnschedulable / TaintToleration) then rejects —
// wedging the gang there (pinStale keeps the pin because the domain still looks
// "free") while genuinely schedulable domains sit unused.
func nodeSchedulable(node *v1.Node, pod *v1.Pod) bool {
	if node == nil || node.Spec.Unschedulable {
		return false
	}
	_, untolerated := v1helper.FindMatchingUntoleratedTaint(klog.Background(), node.Spec.Taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
		return t.Effect == v1.TaintEffectNoSchedule || t.Effect == v1.TaintEffectNoExecute
	}, false)
	return !untolerated
}

// freeByDomain is the homogeneous convenience projection used by focused tests
// and tracing. Planning uses feasibleByDomain with every remaining member.
func freeByDomain(nodeInfos []framework.NodeInfo, topologyKey string, pod *v1.Pod) topology.FreeByDomain {
	out := topology.FreeByDomain{}
	for _, info := range nodeInfos {
		dom := domainOf(info.Node(), topologyKey)
		if dom == "" {
			continue
		}
		out[dom] += 0 // register the domain even if all nodes are occupied
		if nodeFitsPod(info, pod) {
			out[dom]++
		}
	}
	return out
}

// feasibleByDomain returns a best-fit score only for domains that can place all
// remaining gang templates at once. Feasibility is a bipartite matching problem:
// templates may have different requests, selectors, affinities, and tolerations,
// so aggregate free-node counts are insufficient.
func feasibleByDomain(nodeInfos []framework.NodeInfo, topologyKey string, pods []*v1.Pod) topology.FreeByDomain {
	return feasibleAtLeastByDomain(nodeInfos, topologyKey, pods, len(pods))
}

// feasibleAtLeastByDomain is the minMember-aware form of feasibleByDomain. The
// first pod is the current scheduling cycle and must fit; among surplus siblings,
// any subset large enough to reach need may supply the rest of the gang. A
// domain's value is the room its nodes offer the gang, counted in members, or
// zero when the gang cannot form there.
func feasibleAtLeastByDomain(nodeInfos []framework.NodeInfo, topologyKey string, pods []*v1.Pod, need int) topology.FreeByDomain {
	byDomain := make(map[string][]framework.NodeInfo)
	for _, info := range nodeInfos {
		if domain := domainOf(info.Node(), topologyKey); domain != "" {
			byDomain[domain] = append(byDomain[domain], info)
		}
	}
	out := make(topology.FreeByDomain, len(byDomain))
	for domain, nodes := range byDomain {
		room := newGangRoom(nodes, pods, need)
		if room.currentCanJoin(need) {
			out[domain] = room.total()
		} else {
			out[domain] = 0
		}
	}
	return out
}

// gangRoom is what one domain's nodes offer the gang's remaining members: which
// members fit each node on its own, and how many members each node can hold at
// once. Room is counted the way placement fills a node, so several members may
// share one; a required anti-affinity between members keeps them one per node.
type gangRoom struct {
	pods  []*v1.Pod
	nodes []framework.NodeInfo
	// fits[node][pod] reports that the member fits the node on its own.
	fits [][]bool
	// room[node] is how many members the node can hold at once, capped at the
	// members still to place.
	room []int
}

func newGangRoom(nodes []framework.NodeInfo, pods []*v1.Pod, need int) *gangRoom {
	r := &gangRoom{pods: pods, nodes: nodes, fits: make([][]bool, len(nodes)), room: make([]int, len(nodes))}
	perNode := need
	if membersMustStayApart(pods) {
		perNode = 1
	}
	for n, node := range nodes {
		r.fits[n] = make([]bool, len(pods))
		largest := &schedulerframework.Resource{}
		fitting := 0
		for p, pod := range pods {
			if !nodeFitsPod(node, pod) {
				continue
			}
			r.fits[n][p] = true
			fitting++
			largest.SetMaxResource(resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{}))
		}
		if fitting > 0 {
			r.room[n] = max(1, min(perNode, copiesThatFit(node, largest)))
		}
	}
	return r
}

// copiesThatFit is how many pods of the given request the node can take beside
// its current load; a dimension the request leaves at zero does not bound it.
func copiesThatFit(ni framework.NodeInfo, request *schedulerframework.Resource) int {
	allocatable, requested := ni.GetAllocatable(), ni.GetRequested()
	copies := allocatable.GetAllowedPodNumber() - len(ni.GetPods())
	copies = boundCopies(copies, allocatable.GetMilliCPU()-requested.GetMilliCPU(), request.MilliCPU)
	copies = boundCopies(copies, allocatable.GetMemory()-requested.GetMemory(), request.Memory)
	copies = boundCopies(copies, allocatable.GetEphemeralStorage()-requested.GetEphemeralStorage(), request.EphemeralStorage)
	for name, quantity := range request.ScalarResources {
		copies = boundCopies(copies, allocatable.GetScalarResources()[name]-requested.GetScalarResources()[name], quantity)
	}
	return copies
}

func boundCopies(copies int, free, perCopy int64) int {
	if perCopy <= 0 || copies <= 0 {
		return copies
	}
	if free < perCopy*int64(copies) {
		return int(max(free, 0) / perCopy)
	}
	return copies
}

// membersMustStayApart reports whether a required anti-affinity on any member
// matches a member of the gang, so no two members may share a node.
func membersMustStayApart(pods []*v1.Pod) bool {
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		terms, err := framework.GetAffinityTerms(pod, framework.GetPodAntiAffinityTerms(pod.Spec.Affinity))
		if err != nil {
			return true
		}
		for i := range terms {
			for _, member := range pods {
				if member != nil && terms[i].Matches(member, nil) {
					return true
				}
			}
		}
	}
	return false
}

// total is the domain's room in members, best-fit's measure of how full it is.
func (r *gangRoom) total() int {
	total := 0
	for _, room := range r.room {
		total += room
	}
	return total
}

// currentCanJoin requires the current member (pods[0]) to take one slot and
// proves that need-1 surplus members fit in the slots left beside it.
func (r *gangRoom) currentCanJoin(need int) bool {
	if need <= 0 || len(r.pods) < need {
		return false
	}
	for n := range r.nodes {
		if r.fits[n][0] && r.room[n] > 0 && r.restFitsBeside(n, need-1) {
			return true
		}
	}
	return false
}

// candidates returns the nodes the current member may take while count surplus
// members still fit beside it. Proving that a domain has room is insufficient:
// placing a flexible member on the only node usable by a constrained sibling
// would destroy that fit.
func (r *gangRoom) candidates(count int) sets.Set[string] {
	out := sets.New[string]()
	for n, node := range r.nodes {
		if r.fits[n][0] && r.room[n] > 0 && r.restFitsBeside(n, count) {
			out.Insert(node.Node().Name)
		}
	}
	return out
}

// restFitsBeside reports whether count surplus members fit once the current
// member has taken one slot on node.
func (r *gangRoom) restFitsBeside(node, count int) bool {
	left := slices.Clone(r.room)
	left[node]--
	return r.maxMatching(left, count) >= count
}

// maxMatching returns how many surplus members (pods[1:]) can hold a slot at
// once within room, stopping at limit: a bipartite matching in which a node is
// matched as many times as it has room, grown by augmenting paths.
func (r *gangRoom) maxMatching(room []int, limit int) int {
	if limit <= 0 {
		return 0
	}
	assigned := make([][]int, len(r.nodes))
	var place func(pod int, seen []bool) bool
	place = func(pod int, seen []bool) bool {
		for node := range r.nodes {
			if seen[node] || room[node] <= 0 || !r.fits[node][pod] {
				continue
			}
			seen[node] = true
			if len(assigned[node]) < room[node] {
				assigned[node] = append(assigned[node], pod)
				return true
			}
			for i, other := range assigned[node] {
				if place(other, seen) {
					assigned[node][i] = pod
					return true
				}
			}
		}
		return false
	}
	matched := 0
	for pod := 1; pod < len(r.pods) && matched < limit; pod++ {
		if place(pod, make([]bool, len(r.nodes))) {
			matched++
		}
	}
	return matched
}
