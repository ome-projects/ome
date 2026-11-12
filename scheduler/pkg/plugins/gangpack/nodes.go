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

// memberNeed is what one gang member asks of a node, read from the pod once
// per scheduling cycle rather than once per node it is tried against: its
// requests as the resource the fit compares, and its required node affinity.
type memberNeed struct {
	pod      *v1.Pod
	resource *schedulerframework.Resource
	affinity nodeaffinity.RequiredNodeAffinity
}

func needOf(pod *v1.Pod) memberNeed {
	if pod == nil {
		return memberNeed{}
	}
	requests := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
	return memberNeed{pod: pod, resource: schedulerframework.NewResource(requests), affinity: nodeaffinity.GetRequiredNodeAffinity(pod)}
}

// fits reports whether node, the node behind ni, has room for the member
// beside what it already runs, gang members assumed on it included: the
// member may land on the node at all, and what the node has left covers
// each of its requests.
func (m memberNeed) fits(ni framework.NodeInfo, node *v1.Node) bool {
	if !m.readable(ni, node) {
		return false
	}
	return m.mayLandOn(node) && m.roomLeftOn(ni)
}

// readable reports whether there is a member, a node and both of the entry's
// totals to compare; anything missing is no fit rather than a crash.
func (m memberNeed) readable(ni framework.NodeInfo, node *v1.Node) bool {
	return ni != nil && node != nil && ni.GetAllocatable() != nil && ni.GetRequested() != nil && m.pod != nil
}

// mayLandOn reports whether the node takes the member at all, room aside:
// the node is schedulable for it, a member bound by name is bound to this
// node, and its required node affinity matches.
func (m memberNeed) mayLandOn(node *v1.Node) bool {
	if !nodeSchedulable(node, m.pod) {
		return false
	}
	if m.pod.Spec.NodeName != "" && m.pod.Spec.NodeName != node.Name {
		return false
	}
	match, err := m.affinity.Match(node)
	return err == nil && match
}

// roomLeftOn reports whether what the node has left beside its current load
// covers each of the member's requests and one more pod slot.
func (m memberNeed) roomLeftOn(ni framework.NodeInfo) bool {
	allocatable, requested := ni.GetAllocatable(), ni.GetRequested()
	if allocatable.GetMilliCPU()-requested.GetMilliCPU() < m.resource.GetMilliCPU() ||
		allocatable.GetMemory()-requested.GetMemory() < m.resource.GetMemory() ||
		allocatable.GetEphemeralStorage()-requested.GetEphemeralStorage() < m.resource.GetEphemeralStorage() ||
		allocatable.GetAllowedPodNumber() <= len(ni.GetPods()) {
		return false
	}
	return m.scalarsLeftOn(allocatable, requested)
}

// scalarsLeftOn reports whether each accelerator or other scalar resource the
// member asks for is left on the node in the quantity asked.
func (m memberNeed) scalarsLeftOn(allocatable, requested framework.Resource) bool {
	for name, quantity := range m.resource.ScalarResources {
		if allocatable.GetScalarResources()[name]-requested.GetScalarResources()[name] < quantity {
			return false
		}
	}
	return true
}

// boundGangPlacement is what the node snapshot holds of a gang: the members
// already placed (assumed or bound) that share the pod's PodGroup, how many
// there are and the domain they sit in (the domain of the last such node
// seen; a healthy gang is single-domain). This is how the plugin recovers a
// gang whose in-memory assignment was lost (scheduler restart / leader failover):
// the truth of where a gang lives is where its members already are. The pod
// being scheduled is not yet in any node's pod list, so it never counts
// itself. A gang already split over domains is detected too: silently adopting
// one arbitrary domain would undercount the other domains and make the split
// worse after scheduler failover.
type boundGangPlacement struct {
	domain string
	count  int
	split  bool
	// placed holds the gang members found in the snapshot, by name.
	placed map[string]*v1.Pod
}

// addMembersOn records the gang's members placed on one node of the given
// domain ("" for a node outside every domain).
func (b *boundGangPlacement) addMembersOn(info framework.NodeInfo, domain, namespace, pgName string) {
	for _, pi := range info.GetPods() {
		mp := pi.GetPod()
		if mp == nil || mp.Namespace != namespace || mp.Labels[podGroupLabel] != pgName {
			continue
		}
		b.placed[mp.Name] = mp
		b.count++
		if domain == "" {
			b.split = true
			continue
		}
		if b.domain != "" && b.domain != domain {
			b.split = true
		}
		b.domain = domain
	}
}

// domainNode is one snapshot entry as a scheduling cycle reads it: the entry
// and the node behind it, read once per cycle.
type domainNode struct {
	info framework.NodeInfo
	node *v1.Node
}

// everyDomain is the scope of a snapshot view that groups every domain.
const everyDomain = ""

// snapshotView is what one pass over the node snapshot gives a gang member's
// cycle: the gang's members already placed, and the nodes the member may plan
// on, grouped by domain in snapshot order. scope is the one domain grouped
// for a member that follows an assignment, or everyDomain for a member that
// chooses one.
type snapshotView struct {
	placed   boundGangPlacement
	scope    string
	byDomain map[string][]domainNode
}

// scanSnapshot walks the snapshot once. It finds the gang's members already
// placed on any node, in a domain or outside every one, and groups the nodes
// of the domain scope names, or of every domain when scope is everyDomain.
// A pod that is not a gang member has no placed members.
func scanSnapshot(nodes []framework.NodeInfo, pod *v1.Pod, topologyKey, scope string) snapshotView {
	namespace, pgName, member := podGroupNameOf(pod)
	view := snapshotView{scope: scope, byDomain: make(map[string][]domainNode)}
	if member {
		view.placed.placed = make(map[string]*v1.Pod)
	}
	for _, info := range nodes {
		if info == nil {
			continue
		}
		node := info.Node()
		if node == nil {
			continue
		}
		domain := domainOf(node, topologyKey)
		if member {
			view.placed.addMembersOn(info, domain, namespace, pgName)
		}
		if domain != "" && (scope == everyDomain || domain == scope) {
			view.byDomain[domain] = append(view.byDomain[domain], domainNode{info: info, node: node})
		}
	}
	return view
}

// planning returns the nodes a member may plan on, grouped by domain: the one
// domain named, or every domain the view holds when domain is everyDomain.
func (v snapshotView) planning(domain string) map[string][]domainNode {
	if domain == everyDomain || domain == v.scope {
		return v.byDomain
	}
	return map[string][]domainNode{domain: v.byDomain[domain]}
}

// names returns the node names of one domain, in snapshot order.
func (v snapshotView) names(domain string) []string {
	return nodeNames(v.byDomain[domain])
}

// namesByDomain returns the node names of every domain the view holds, as the
// assignment store reads them.
func (v snapshotView) namesByDomain() map[string][]string {
	out := make(map[string][]string, len(v.byDomain))
	for domain, nodes := range v.byDomain {
		out[domain] = nodeNames(nodes)
	}
	return out
}

func nodeNames(nodes []domainNode) []string {
	out := make([]string, 0, len(nodes))
	for _, dn := range nodes {
		out = append(out, dn.node.Name)
	}
	return out
}

// nodeSchedulable reports whether the gang pod could actually be placed on this
// node right now — beyond having the accelerator free, the node must be
// schedulable: not cordoned (spec.unschedulable), and its NoSchedule/NoExecute
// taints tolerated by the pod. Without this check, freeByDomain would count a
// cordoned or tainted node as free, so Choose could take a domain whose nodes the
// framework's own Filter (NodeUnschedulable / TaintToleration) then rejects —
// holding the gang there forever (assignmentStale keeps the assignment because
// the domain still looks "free") while genuinely schedulable domains sit unused.
func nodeSchedulable(node *v1.Node, pod *v1.Pod) bool {
	if node == nil || node.Spec.Unschedulable {
		return false
	}
	_, untolerated := v1helper.FindMatchingUntoleratedTaint(klog.Background(), node.Spec.Taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
		return t.Effect == v1.TaintEffectNoSchedule || t.Effect == v1.TaintEffectNoExecute
	}, false)
	return !untolerated
}

// freeByDomain counts, per domain, the candidate nodes the pod fits. PreScore
// reads it to rank a standalone pod's domains by how full they are.
func freeByDomain(nodeInfos []framework.NodeInfo, topologyKey string, pod *v1.Pod) topology.FreeByDomain {
	out := topology.FreeByDomain{}
	need := needOf(pod)
	for _, info := range nodeInfos {
		node := info.Node()
		dom := domainOf(node, topologyKey)
		if dom == "" {
			continue
		}
		out[dom] += 0 // register the domain even if all nodes are occupied
		if need.fits(info, node) {
			out[dom]++
		}
	}
	return out
}

// roomByDomain reads, per domain, the room its nodes offer the gang's
// remaining members, counted in members, or zero when the gang cannot form there.
// Members may differ in requests, selectors, affinities and tolerations, so the
// reading matches members to nodes instead of counting free nodes. The first
// member is the current scheduling cycle and must fit; among surplus siblings,
// any subset large enough to reach need may supply the rest of the gang.
func roomByDomain(byDomain map[string][]domainNode, members gangMembers, need int) topology.FreeByDomain {
	out := make(topology.FreeByDomain, len(byDomain))
	for domain, nodes := range byDomain {
		room := newGangRoom(nodes, members, need)
		if room.currentCanJoin(need) {
			out[domain] = room.total()
		} else {
			out[domain] = 0
		}
	}
	return out
}

// candidateNodes restricts the current member to the domain's nodes that leave
// room for the need-1 remaining members it brings along.
func candidateNodes(nodes []domainNode, members gangMembers, need int) sets.Set[string] {
	if len(members.needs) == 0 {
		return sets.New[string]()
	}
	return newGangRoom(nodes, members, need).candidates(need - 1)
}

// gangMembers is a gang's remaining members as the room reading needs them,
// read once per cycle and reused for every domain: each member's need, and
// whether a required anti-affinity between members keeps them one per node.
type gangMembers struct {
	needs []memberNeed
	apart bool
}

func membersOf(pods []*v1.Pod) gangMembers {
	needs := make([]memberNeed, len(pods))
	for i, pod := range pods {
		needs[i] = needOf(pod)
	}
	return gangMembers{needs: needs, apart: membersMustStayApart(pods)}
}

// gangRoom is what one domain's nodes offer the gang's remaining members: which
// members fit each node on its own, and how many members each node can hold at
// once. Room is counted the way placement fills a node, so several members may
// share one; a required anti-affinity between members keeps them one per node.
type gangRoom struct {
	members gangMembers
	nodes   []domainNode
	// fits holds one row per node, a flag per member: whether the member fits
	// the node on its own (read through fit).
	fits []bool
	// room[node] is how many members the node can hold at once, capped at the
	// members still to place.
	room []int
}

func newGangRoom(nodes []domainNode, members gangMembers, need int) *gangRoom {
	width := len(members.needs)
	r := &gangRoom{members: members, nodes: nodes, fits: make([]bool, len(nodes)*width), room: make([]int, len(nodes))}
	perNode := need
	if members.apart {
		perNode = 1
	}
	// largest is the biggest member among those that fit the node at hand; one
	// value serves every node, its scalar map cleared between them.
	var largest schedulerframework.Resource
	for n, dn := range nodes {
		if dn.info == nil || dn.node == nil {
			continue
		}
		largest.MilliCPU, largest.Memory, largest.EphemeralStorage = 0, 0, 0
		clear(largest.ScalarResources)
		row := r.fits[n*width : (n+1)*width]
		fitting := 0
		for p, member := range members.needs {
			if !member.fits(dn.info, dn.node) {
				continue
			}
			row[p] = true
			fitting++
			raiseTo(&largest, member.resource)
		}
		if fitting > 0 {
			r.room[n] = max(1, min(perNode, copiesThatFit(dn.info, &largest)))
		}
	}
	return r
}

// fit reports whether the member fits the node on its own.
func (r *gangRoom) fit(node, member int) bool {
	return r.fits[node*len(r.members.needs)+member]
}

// raiseTo raises every dimension of largest that copiesThatFit reads to the
// member's request, so largest is the biggest member among those that fit.
func raiseTo(largest, request *schedulerframework.Resource) {
	largest.MilliCPU = max(largest.MilliCPU, request.MilliCPU)
	largest.Memory = max(largest.Memory, request.Memory)
	largest.EphemeralStorage = max(largest.EphemeralStorage, request.EphemeralStorage)
	for name, quantity := range request.ScalarResources {
		largest.SetScalar(name, max(largest.ScalarResources[name], quantity))
	}
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
	if need <= 0 || len(r.members.needs) < need {
		return false
	}
	for n := range r.nodes {
		if r.fit(n, 0) && r.room[n] > 0 && r.restFitsBeside(n, need-1) {
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
	for n, dn := range r.nodes {
		if r.fit(n, 0) && r.room[n] > 0 && r.restFitsBeside(n, count) {
			out.Insert(dn.node.Name)
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
// matched as many times as it has room, grown by augmenting paths. seen marks
// the nodes one augmenting path has visited; every path starts with it clear.
func (r *gangRoom) maxMatching(room []int, limit int) int {
	if limit <= 0 {
		return 0
	}
	assigned := make([][]int, len(r.nodes))
	seen := make([]bool, len(r.nodes))
	var place func(pod int, seen []bool) bool
	place = func(pod int, seen []bool) bool {
		for node := range r.nodes {
			if seen[node] || room[node] <= 0 || !r.fit(node, pod) {
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
	for pod := 1; pod < len(r.members.needs) && matched < limit; pod++ {
		clear(seen)
		if place(pod, seen) {
			matched++
		}
	}
	return matched
}
