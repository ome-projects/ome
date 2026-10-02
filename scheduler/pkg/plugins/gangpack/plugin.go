// Package gangpack is the OME placement plugin: topology-aware gang placement and
// bin-packing. PreFilter identifies whole-gang-feasible domains and normally pins
// one; hard-spread gangs defer the pin until Reserve sees the node that survived
// the framework filters. The core (domain accounting, best-fit, pins) lives in the pure
// topology/placement packages; this package wires the live scheduler state onto
// them.
package gangpack

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// Name is the plugin name referenced from the scheduler ConfigMap.
const Name = "OMEGangPack"

// GangPack is the OME placement plugin.
type GangPack struct {
	handle     framework.Handle
	gcInterval time.Duration
	// topologyKey is the configured fallback domain label for PodGroups that do
	// not declare a per-group override.
	topologyKey string
	// podGroupTopologyKeyAnnotation names the configured PodGroup annotation
	// whose value is a node topology label. Keeping it configurable makes the
	// scheduling plugin independent of any workload controller's API prefix.
	podGroupTopologyKeyAnnotation string
	// unsupportedPlacementGroupLabel optionally names a producer's partner-gang
	// label. Until multi-gang reservations exist, its presence fails closed.
	unsupportedPlacementGroupLabel string
	// standaloneDomainPacking enables the domain-level bin-packing Score for
	// unpinned (non-gang) whole-node pods. Gang members are unaffected (their
	// domain is already pinned). See score.go.
	standaloneDomainPacking bool
	// pins records, per placement group, the domain its gangs are committed to.
	pins *placement.Pins
	// pgReader resolves a pod's PodGroup facts (minMember + declared topology
	// key). A missing reader fails closed for labeled gang pods.
	pgReader podGroupReader
	// podLister enumerates live gang pods for the pin garbage collector. nil in
	// unit tests that don't exercise GC.
	podLister gangPodLister
	// attemptByPod associates framework waiting pods with the commitment that
	// admitted them. It prevents stale waiters from satisfying or unwinding a
	// newer retry of the same PodGroup.
	attemptMu    sync.RWMutex
	attemptByPod map[string]uint64
	// reservationBlocked holds ordinary pods rejected only because a forming gang
	// owns their candidate domain. They are explicitly activated when that
	// in-memory reservation drains; no Kubernetes object event represents it.
	blockedMu          sync.Mutex
	reservationBlocked map[string]*v1.Pod
	// templatesIncomplete records gangs that parked a member because the live
	// member set was still short of minMember. An unscheduled sibling's creation
	// is not a queue event, so the first PreFilter that sees the set complete
	// activates the gang once and clears the record.
	templatesMu         sync.Mutex
	templatesIncomplete map[string]bool
	// failedDomains remembers domains whose candidates all failed another Filter.
	// The next attempt excludes them until every feasible domain has been tried.
	failedMu      sync.Mutex
	failedDomains map[string]sets.Set[placement.Domain]
}

// Interface assertions.
var (
	_ framework.PreFilterPlugin   = &GangPack{}
	_ framework.FilterPlugin      = &GangPack{}
	_ framework.PostFilterPlugin  = &GangPack{}
	_ framework.PermitPlugin      = &GangPack{}
	_ framework.ReservePlugin     = &GangPack{}
	_ framework.PostBindPlugin    = &GangPack{}
	_ framework.EnqueueExtensions = &GangPack{}
)

// New is the plugin factory registered via app.WithPlugin. It stands up the
// PodGroup informer from the scheduler's shared kubeconfig (so PreFilter can
// resolve gang facts), wires a gang-pod lister from the scheduler's shared
// informer, and starts the pin garbage collector.
func New(ctx context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	args, err := decodeArgs(obj)
	if err != nil {
		return nil, err
	}
	reader, err := newInformerReader(ctx, h.KubeConfig(), args.PodGroupTopologyKeyAnnotation,
		args.UnsupportedPlacementGroupLabel, args.defaultPermitTimeout(), args.podGroupSyncTimeout())
	if err != nil {
		return nil, err
	}
	podLister, err := newInformerPodLister(h)
	if err != nil {
		return nil, err
	}
	g := &GangPack{
		handle:                         h,
		gcInterval:                     args.gcInterval(),
		topologyKey:                    args.TopologyKey,
		podGroupTopologyKeyAnnotation:  args.PodGroupTopologyKeyAnnotation,
		unsupportedPlacementGroupLabel: args.UnsupportedPlacementGroupLabel,
		standaloneDomainPacking:        args.standaloneDomainPackingEnabled(),
		pins:                           placement.New(),
		pgReader:                       reader,
		podLister:                      podLister,
	}
	go g.runPinGC(ctx)
	return g, nil
}

// Name returns the plugin's registered name.
func (g *GangPack) Name() string { return Name }

// PreFilter plans the gang's domain. Most gangs best-fit and pin immediately.
// For an unplaced gang whose current anchor carries a hard topology spread
// constraint, it exposes the union of every whole-gang-feasible domain instead;
// the framework's regular filters apply the constraint and Reserve pins the
// domain of the selected node. Standalone pods continue so Filter can protect
// forming-gang reservations; labeled gang pods fail closed when their PodGroup
// cannot be resolved. Unschedulable when a real gang fits in no domain.
func (g *GangPack) PreFilter(_ context.Context, state framework.CycleState, pod *v1.Pod, nodes []framework.NodeInfo) (*framework.PreFilterResult, *framework.Status) {
	ns, name, isMember := podGroupNameOf(pod)
	if !isMember {
		// Return Success rather than Skip so Filter can enforce outstanding gang
		// reservations against standalone pods handled by this scheduler.
		return nil, framework.NewStatus(framework.Success)
	}
	if g.pgReader == nil {
		return nil, framework.NewStatus(framework.Unschedulable, "PodGroup reader is unavailable")
	}
	minMember, topologyKey, timeout, uid, found := g.pgReader.get(ns, name)
	if !found {
		g.releaseStaleGang(ns + "/" + name)
		// The pod is labelled into a PodGroup the reader can't resolve yet — most
		// often informer lag right after the PodGroup was created (pod and group
		// created together). Skipping here would let a lone member bind and break
		// all-or-nothing, so hold the pod instead; it requeues automatically when
		// the PodGroup appears (a registered enqueue event). Plain Unschedulable so
		// the requeue hints stay in effect.
		return nil, framework.NewStatus(framework.Unschedulable, "PodGroup "+ns+"/"+name+" not resolvable yet")
	}
	if minMember <= 0 {
		return nil, framework.NewStatus(framework.Unschedulable,
			"PodGroup "+ns+"/"+name+" must declare a positive minMember")
	}
	// A producer may declare a per-gang override on its PodGroup, or a cluster
	// may configure one topology key for every gang. The latter keeps PodGroup
	// producers entirely unaware of this plugin's metadata contract.
	if topologyKey == "" {
		topologyKey = g.topologyKey
	}
	if topologyKey == "" {
		if g.podGroupTopologyKeyAnnotation == "" {
			return nil, framework.NewStatus(framework.Unschedulable,
				"scheduler must configure topologyKey or podGroupTopologyKeyAnnotation for gang pods")
		}
		return nil, framework.NewStatus(framework.Unschedulable,
			"PodGroup "+ns+"/"+name+" must declare "+g.podGroupTopologyKeyAnnotation+" or the scheduler must configure topologyKey")
	}
	if g.unsupportedPlacementGroupLabel != "" {
		if _, present := pod.Labels[g.unsupportedPlacementGroupLabel]; present {
			return nil, framework.NewStatus(framework.Unschedulable,
				"partner placement groups are not supported")
		}
	}
	if r, ok := g.pgReader.(interface {
		placementGroup(namespace, name string) (string, bool)
	}); ok && g.unsupportedPlacementGroupLabel != "" {
		if _, present := r.placementGroup(ns, name); present {
			return nil, framework.NewStatus(framework.Unschedulable,
				"partner placement groups are not supported")
		}
	}
	if timeout <= 0 {
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"PodGroup "+ns+"/"+name+" must set scheduleTimeoutSeconds or the scheduler must configure defaultPermitTimeoutSeconds")
	}
	gang := gangInfo{key: ns + "/" + name, uid: uid, minMember: minMember, topologyKey: topologyKey, timeout: timeout}
	return g.pinGang(state, nodes, gang, pod)
}

// pinGang is the decision core, split out so it is unit-testable with an explicit
// node slice (no Handle): best-fit a domain for the gang, record the pin, and
// return the domain's nodes as the downstream candidate set.
//
// Only the first member of a gang runs all-domain best-fit. Later members still
// make one snapshot pass to detect split placement and assumed siblings, but
// resource matching is bounded to the committed domain.
//
// When the in-memory pin is absent but the gang already has members placed in the
// cluster (scheduler restart / leader failover lost the pin), the gang's domain
// is adopted from where those members already sit rather than best-fit afresh —
// re-planning a live gang into a different domain would strand its bound members.
func (g *GangPack) pinGang(state framework.CycleState, nodes []framework.NodeInfo, gang gangInfo, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	pinnedDomain, commitment, _, pinned, ownerMatch := g.pins.GetOwnedBy(gang.key, gang.uid)
	domain := pinnedDomain.Name
	if pinned && !ownerMatch {
		hadWaiters := g.hasWaitingAttempt(gang.key, commitment)
		oldGang := gang
		oldGang.uid = g.pins.Owners()[gang.key]
		g.releaseAttempt(&pinState{
			domain: domain, topologyKey: pinnedDomain.TopologyKey,
			gang: oldGang, commitment: commitment,
		}, nil, false)
		g.clearFailedDomains(oldGang)
		if hadWaiters {
			return nil, framework.NewStatus(framework.Unschedulable,
				"released commitment owned by a previous PodGroup instance "+gang.key)
		}
		pinned = false
	}
	if pinned && pinnedDomain.TopologyKey != "" && pinnedDomain.TopologyKey != gang.topologyKey {
		hadWaiters := g.hasWaitingAttempt(gang.key, commitment)
		g.releaseAttempt(&pinState{
			domain: domain, topologyKey: pinnedDomain.TopologyKey,
			gang: gang, commitment: commitment,
		}, nil, false)
		gangPinTotal.WithLabelValues("topology_replan").Inc()
		if hadWaiters {
			return nil, framework.NewStatus(framework.Unschedulable,
				"released commitment after topology key changed for "+gang.key)
		}
		pinned = false
	}

	// One snapshot pass establishes bound/assumed placement and the identities that
	// gangTemplates must exclude. Waiting Permit pods are assumed here, so this is
	// also the authoritative view for remaining-template selection.
	placement := inspectBoundGangMembers(nodes, pod, gang.topologyKey)
	if placement.split {
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"gang "+gang.key+" already has members in multiple topology domains")
	}
	templates, need, status := g.gangTemplates(gang, pod, placement)
	if status != nil {
		return nil, status
	}
	// OME puts hard spreading on one anchor (normally the leader). If an
	// unconstrained sibling ran first it could commit the gang before that anchor's
	// PodTopologySpread filter had a say. Let the anchor choose from all feasible
	// domains; Permit activation will wake this member after the anchor is placed.
	if sibling, blocked := hardSpreadAnchorBlocked(pod, templates[1:]); blocked {
		klog.V(4).InfoS("gangpack.pinGang.spread_anchor_wait", "gang", gang.key, "sibling", klog.KObj(sibling))
		return nil, framework.NewStatus(framework.Unschedulable,
			"waiting for gang spread anchor "+sibling.Namespace+"/"+sibling.Name+" to be placed")
	}
	// A required affinity to a sibling that is neither bound nor assumed fails on
	// every node, so planning now would only fail Filter and record the chosen
	// domain as failed. Yield without a pin; the sibling's own Permit activation
	// retries this member once it is placed.
	if sibling, blocked := siblingAffinityBlocked(pod, templates[1:], placement.placed); blocked {
		klog.V(4).InfoS("gangpack.pinGang.sibling_wait", "gang", gang.key, "sibling", klog.KObj(sibling))
		return nil, framework.NewStatus(framework.Unschedulable,
			"waiting for gang sibling "+sibling.Namespace+"/"+sibling.Name+" to be placed")
	}

	// Existing commitments only need matching inside their pinned domain. An
	// adopted gang is similarly constrained to the domain of its bound members.
	// All-domain matching is reserved for an unplaced gang choosing a new domain.
	planningNodes := nodes
	if pinned {
		planningNodes = nodeInfosInDomain(nodes, gang.topologyKey, domain)
	} else if placement.count > 0 {
		planningNodes = nodeInfosInDomain(nodes, gang.topologyKey, placement.domain)
	}
	free := feasibleAtLeastByDomain(planningNodes, gang.topologyKey, templates, need)

	// Trace the pin decision at V(4): the domain choice, the pinStale inputs, and
	// the narrowed candidate set are otherwise invisible, and they are exactly what
	// is needed to debug a gang that won't place (e.g. pinned to a domain whose
	// nodes are all filtered out while other domains sit free). Off at the default
	// verbosity, so it costs nothing in normal operation.
	if klog.V(4).Enabled() {
		klog.V(4).InfoS("gangpack.pinGang.enter", "gang", gang.key, "minMember", gang.minMember,
			"topologyKey", gang.topologyKey, "pinned", pinned, "pinnedDomain", domain,
			"nodesInPinnedDomain", len(planningNodes),
			"boundDomain", placement.domain, "boundCount", placement.count, "freePinnedDomain", free[domain], "freeByDomain", fmt.Sprintf("%v", free))
	}

	// A stale pin must be dropped and re-planned, else the gang wedges on it
	// forever. Releasing also returns the pin's leaked capacity reservation. Only
	// safe when no member has landed in the domain yet (see pinStale).
	if pinned && pinStale(len(planningNodes), placement.count, free, need, domain) {
		g.pins.ReleaseIf(gang.key, commitment, nil)
		gangPinTotal.WithLabelValues("stale_replan").Inc()
		pinnedGroups.Set(float64(g.pins.Len()))
		pinned = false
		planningNodes = nodes
		free = feasibleAtLeastByDomain(planningNodes, gang.topologyKey, templates, need)
		klog.V(4).InfoS("gangpack.pinGang.stale_replan", "gang", gang.key, "releasedDomain", domain)
	}
	if pinned {
		if placement.count > 0 && free[domain] < need {
			return nil, framework.NewStatus(framework.Unschedulable,
				"pinned domain "+domain+" cannot fit all remaining gang members "+gang.key)
		}
	}
	if !pinned && placement.count == 0 && hasHardTopologySpread(pod) {
		return g.deferDomainPin(state, nodes, gang, templates, free, need)
	}

	if !pinned {
		d, id, status := g.planDomain(nodes, gang, placement, free, need)
		if status != nil {
			klog.V(4).InfoS("gangpack.pinGang.no_fit", "gang", gang.key, "status", status.Message())
			return nil, status
		}
		domain = d
		commitment = id
	}
	domainNodes := nodeInfosInDomain(nodes, gang.topologyKey, domain)
	candidates := matchingCandidateNodesForNeed(domainNodes, gang.topologyKey, domain, templates, need)
	if len(candidates) == 0 {
		return nil, framework.NewStatus(framework.Unschedulable,
			"pinned domain "+domain+" has no node feasible for gang member "+gang.key)
	}
	writePin(state, domain, gang, commitment)
	klog.V(4).InfoS("gangpack.pinGang.result", "gang", gang.key, "domain", domain, "narrowedNodes", candidates.Len())
	return &framework.PreFilterResult{NodeNames: candidates}, nil
}

// hasHardTopologySpread identifies constraints that participate in Filter. Soft
// ScheduleAnyway constraints affect only scoring and do not need domain-choice
// deferral (OME's packing profile intentionally disables that built-in score).
func hasHardTopologySpread(pod *v1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, constraint := range pod.Spec.TopologySpreadConstraints {
		if constraint.WhenUnsatisfiable == v1.DoNotSchedule {
			return true
		}
	}
	return false
}

func hardSpreadAnchorBlocked(current *v1.Pod, unplaced []*v1.Pod) (*v1.Pod, bool) {
	if hasHardTopologySpread(current) {
		return nil, false
	}
	for _, sibling := range unplaced {
		if sibling != nil && !samePodIdentity(sibling, current) && hasHardTopologySpread(sibling) {
			return sibling, true
		}
	}
	return nil, false
}

// deferDomainPin returns candidate nodes from every domain that can fit the
// entire gang. PodTopologySpread and the other framework filters can therefore
// veto nodes or whole spread domains before GangPack makes a reservation. The
// scheduler's selected node determines the final gang domain in Reserve.
func (g *GangPack) deferDomainPin(state framework.CycleState, nodes []framework.NodeInfo, gang gangInfo, templates []*v1.Pod, free topology.FreeByDomain, need int) (*framework.PreFilterResult, *framework.Status) {
	nodesByDomain := nodeNamesByDomain(nodes, gang.topologyKey)
	available := g.pins.WithoutReservedDomains(gang.topologyKey, free, nodesByDomain)
	choice, hadFailed := g.withoutFailedDomains(gang, available)
	if _, fits := topology.BestFit(choice, need); !fits && hadFailed {
		g.clearFailedDomains(gang)
		choice = available
	}
	if _, fits := topology.BestFit(choice, need); !fits {
		gangPinTotal.WithLabelValues("no_fit").Inc()
		return nil, framework.NewStatus(framework.Unschedulable, "no domain has room for gang "+gang.key)
	}

	candidates := sets.New[string]()
	plannedFree := make(topology.FreeByDomain)
	plannedNodes := make(map[string][]string)
	for domain, capacity := range choice {
		if capacity < need {
			continue
		}
		domainNodes := nodeInfosInDomain(nodes, gang.topologyKey, domain)
		domainCandidates := matchingCandidateNodesForNeed(domainNodes, gang.topologyKey, domain, templates, need)
		if domainCandidates.Len() == 0 {
			continue
		}
		candidates.Insert(domainCandidates.UnsortedList()...)
		plannedFree[domain] = capacity
		plannedNodes[domain] = append([]string(nil), nodesByDomain[domain]...)
	}
	if candidates.Len() == 0 {
		gangPinTotal.WithLabelValues("no_fit").Inc()
		return nil, framework.NewStatus(framework.Unschedulable, "no domain has candidate nodes for gang "+gang.key)
	}
	writeDeferredPin(state, &deferredPinState{gang: gang, need: need, free: plannedFree, nodesByDomain: plannedNodes})
	klog.V(4).InfoS("gangpack.pinGang.deferred", "gang", gang.key, "topologyKey", gang.topologyKey,
		"domains", fmt.Sprintf("%v", plannedFree), "candidateNodes", candidates.Len())
	return &framework.PreFilterResult{NodeNames: candidates}, nil
}

// planDomain commits an unpinned gang to a domain: it adopts the domain of any
// already-placed members (failover / lost pin), else best-fits a fresh one. It
// records the pin and the placement metric. status is non-nil (Unschedulable)
// only when the gang fits in no domain.
func (g *GangPack) planDomain(nodes []framework.NodeInfo, gang gangInfo, bound boundGangPlacement, free topology.FreeByDomain, need int) (string, uint64, *framework.Status) {
	if bound.split {
		return "", 0, framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"gang "+gang.key+" already has members in multiple topology domains")
	}
	if d, count := bound.domain, bound.count; count > 0 {
		// Rebuild from ground truth: the gang lives where its members already are.
		remaining := gang.minMember - count
		id := g.pins.SetReservationForOwnerOnNodes(gang.key, gang.uid, gang.topologyKey, d, remaining,
			nodeNamesInDomain(nodes, gang.topologyKey, d))
		gangPinTotal.WithLabelValues("adopted").Inc()
		pinnedGroups.Set(float64(g.pins.Len()))
		if free[d] < need {
			return "", 0, framework.NewStatus(framework.Unschedulable,
				"adopted domain "+d+" cannot fit remaining gang members "+gang.key)
		}
		return d, id, nil
	}
	choice, hadFailed := g.withoutFailedDomains(gang, free)
	d, id, fits := g.pins.ChooseForOwnerInTopologyOnNodes(gang.key, gang.uid, gang.topologyKey, choice,
		nodeNamesByDomain(nodes, gang.topologyKey), need)
	if !fits && hadFailed {
		g.clearFailedDomains(gang)
		d, id, fits = g.pins.ChooseForOwnerInTopologyOnNodes(gang.key, gang.uid, gang.topologyKey, free,
			nodeNamesByDomain(nodes, gang.topologyKey), need)
	}
	if !fits {
		gangPinTotal.WithLabelValues("no_fit").Inc()
		return "", 0, framework.NewStatus(framework.Unschedulable, "no domain has room for gang "+gang.key)
	}
	gangPinTotal.WithLabelValues("pinned").Inc()
	pinnedGroups.Set(float64(g.pins.Len()))
	return d, id, nil
}

// PreFilterExtensions returns nil: the plugin does not incrementally re-evaluate
// on pod add/remove during preemption.
func (g *GangPack) PreFilterExtensions() framework.PreFilterExtensions { return nil }

// Filter enforces the gang's pinned domain: once PreFilter has recorded a pin in
// CycleState, only nodes in that domain pass; every other node is rejected as
// UnschedulableAndUnresolvable (a wrong-domain node cannot be made right by
// preempting on it, so the scheduler must not attempt preemption there). When no
// pin is recorded — the pod is not a pinned gang member — Filter imposes no
// constraint and admits the node.
func (g *GangPack) Filter(_ context.Context, state framework.CycleState, pod *v1.Pod, nodeInfo framework.NodeInfo) *framework.Status {
	pin := readPin(state)
	if pin == nil {
		if g.pins == nil {
			return nil
		}
		if nodeInfo != nil && nodeInfo.Node() != nil && g.pins.BlocksNode(nodeInfo.Node().Name, nodeInfo.Node().Labels) {
			g.rememberReservationBlocked(pod)
			return framework.NewStatus(framework.UnschedulableAndUnresolvable,
				"node is reserved for a forming gang")
		}
		return nil
	}
	nodeDomain := domainOf(nodeInfo.Node(), pin.topologyKey)
	if nodeDomain != pin.domain {
		// Trace at V(4): the pin (domain + topology key) Filter actually reads from
		// CycleState, versus the candidate node's domain. Pairs with pinGang's trace
		// to catch a PreFilter->Filter pin mismatch (a pinned domain's own nodes
		// getting rejected here while it looks free in PreFilter).
		klog.V(4).InfoS("gangpack.Filter.reject", "node", nodeInfo.Node().GetName(),
			"nodeDomain", nodeDomain, "pinDomain", pin.domain, "pinTopologyKey", pin.topologyKey)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"node not in gang's pinned domain "+pin.domain)
	}
	return nil
}

// pinStale reports whether a gang's existing pin must be dropped and re-planned.
// Two cases wedge a gang that keeps its pin, and both are safe to re-plan because
// no member has landed in the pinned domain yet:
//
//   - the domain vanished — every node drained/scaled away, or the PodGroup's
//     topology key changed so the old domain value matches no node. Reusing it
//     narrows every candidate away.
//   - the domain filled — another gang won the race for it after this gang pinned,
//     so it can no longer fit this gang's minMember. Without a re-plan the gang
//     retries the full domain forever (Filter rejects every occupied node) while
//     other domains sit free, and its Choose reservation leaks until re-plan
//     releases it.
//
// A domain that already holds any of this gang's members is NEVER stale: those
// members are bound (or assumed) there, so re-planning would strand them. Such a
// gang keeps its pin and waits for capacity in its own domain. The full-domain
// projection is only paid while a gang is unplaced (a transient formation window);
// once a member lands, the early return keeps steady-state PreFilter cheap.
func pinStale(nodesInPinnedDomain, boundCount int, free topology.FreeByDomain, need int, domain string) bool {
	if nodesInPinnedDomain == 0 {
		return true // domain vanished
	}
	if boundCount > 0 {
		return false // has members placed here already; never strand them
	}
	return free[domain] < need
}

func nodeInfosInDomain(nodes []framework.NodeInfo, topologyKey, domain string) []framework.NodeInfo {
	out := make([]framework.NodeInfo, 0)
	for _, node := range nodes {
		if node != nil && domainOf(node.Node(), topologyKey) == domain {
			out = append(out, node)
		}
	}
	return out
}

// matchingCandidateNodes restricts the current member to nodes that leave a
// complete matching for every remaining heterogeneous member. Proving that a
// domain has some matching is insufficient: placing a flexible member on the
// only node usable by a constrained sibling would destroy that matching.
func matchingCandidateNodes(nodes []framework.NodeInfo, topologyKey, domain string, templates []*v1.Pod) sets.Set[string] {
	return matchingCandidateNodesForNeed(nodes, topologyKey, domain, templates, len(templates))
}

func matchingCandidateNodesForNeed(nodes []framework.NodeInfo, topologyKey, domain string, templates []*v1.Pod, need int) sets.Set[string] {
	out := sets.New[string]()
	if len(templates) == 0 {
		return out
	}
	current, remaining := templates[0], templates[1:]
	domainNodes := make([]framework.NodeInfo, 0)
	for _, node := range nodes {
		if domainOf(node.Node(), topologyKey) == domain {
			domainNodes = append(domainNodes, node)
		}
	}
	for candidateIndex, candidate := range domainNodes {
		if !nodeFitsPod(candidate, current) {
			continue
		}
		left := make([]framework.NodeInfo, 0, len(domainNodes)-1)
		left = append(left, domainNodes[:candidateIndex]...)
		left = append(left, domainNodes[candidateIndex+1:]...)
		if maxGangMatching(remaining, left, need-1) >= need-1 {
			out.Insert(candidate.Node().Name)
		}
	}
	return out
}

func nodeNamesByDomain(nodes []framework.NodeInfo, topologyKey string) map[string][]string {
	out := make(map[string][]string)
	for _, node := range nodes {
		if node == nil || node.Node() == nil {
			continue
		}
		if domain := domainOf(node.Node(), topologyKey); domain != "" {
			out[domain] = append(out[domain], node.Node().Name)
		}
	}
	return out
}

func nodeNamesInDomain(nodes []framework.NodeInfo, topologyKey, domain string) []string {
	return nodeNamesByDomain(nodes, topologyKey)[domain]
}

// nodesInDomain returns the names of nodes whose domain matches — the candidate
// set PreFilter narrows to.
func nodesInDomain(nodes []framework.NodeInfo, topologyKey, domain string) sets.Set[string] {
	out := sets.New[string]()
	for _, ni := range nodes {
		n := ni.Node()
		if n != nil && domainOf(n, topologyKey) == domain {
			out.Insert(n.Name)
		}
	}
	return out
}

// gangTemplates returns the concrete remaining members used for simultaneous
// domain feasibility. Production plugins always have the informer-backed member
// lister; the homogeneous fallback keeps the decision core independently usable
// in unit tests.
func (g *GangPack) gangTemplates(gang gangInfo, current *v1.Pod, placement boundGangPlacement) ([]*v1.Pod, int, *framework.Status) {
	need := gang.minMember - placement.count
	if need < 1 {
		need = 1
	}
	lister, ok := g.podLister.(gangMemberLister)
	if !ok {
		out := make([]*v1.Pod, need)
		for i := range out {
			out[i] = current
		}
		return out, need, nil
	}
	ns, name, _ := splitGangKey(gang.key)
	members, err := lister.gangPods(ns, name)
	if err != nil {
		return nil, 0, framework.AsStatus(err)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	out := []*v1.Pod{current}
	for _, member := range members {
		if member == nil || member.Name == current.Name || member.Spec.NodeName != "" || placement.placed[member.Name] != nil ||
			member.DeletionTimestamp != nil || member.Status.Phase == v1.PodSucceeded || member.Status.Phase == v1.PodFailed {
			continue
		}
		out = append(out, member)
	}
	if len(out) < need {
		g.markTemplatesIncomplete(gang)
		return nil, 0, framework.NewStatus(framework.Unschedulable,
			"waiting for all PodGroup member templates for "+gang.key)
	}
	// Members parked on the incomplete set are woken here, once per completion.
	// The record is consumed only after the activation has actually run.
	if g.hasTemplatesIncomplete(gang) && g.activateGangMembers(gang, activationTriggerTemplatesComplete, current) {
		g.clearTemplatesIncomplete(gang)
		klog.V(4).InfoS("gangpack.templates.complete", "gang", gang.key, "members", len(out), "need", need)
	}
	return out, need, nil
}
