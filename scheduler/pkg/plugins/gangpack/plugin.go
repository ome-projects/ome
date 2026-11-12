// Package gangpack is the OME placement plugin: topology-aware gang placement and
// bin-packing. PreFilter chooses a gang's domain and records the assignment;
// Filter enforces it. The core (domain accounting, best-fit, the assignment
// store) lives in the pure topology/placement packages; this package wires the
// live scheduler state onto them.
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
	// whole-node pods outside a gang. Gang members are unaffected (their domain
	// is already assigned). See score.go.
	standaloneDomainPacking bool
	// assignments records, per placement group, the domain its gang is
	// committed to.
	assignments *placement.DomainAssignments
	// pgReader resolves a pod's PodGroup into its gang facts. A missing reader
	// fails closed for labeled gang pods.
	pgReader podGroupReader
	// podLister reads the live gang pods: the keys the garbage collector keeps
	// and the members a gang still has to place. nil in unit tests of the
	// decision core.
	podLister gangPodLister
	// attemptByPod associates framework waiting pods with the commitment that
	// admitted them. It prevents stale waiters from satisfying or unwinding a
	// newer retry of the same PodGroup.
	attemptMu    sync.RWMutex
	attemptByPod map[string]uint64
	// reservationBlocked holds pods rejected only because a forming gang owns the
	// domain they would fit: ordinary pods turned away by Filter, and gang members
	// whose planning found room only in reserved domains. They are explicitly
	// activated when a reservation drains or is released; no Kubernetes object
	// event represents either transition.
	blockedMu          sync.Mutex
	reservationBlocked map[string]*v1.Pod
	// templatesIncomplete records gangs that held a member back because the live
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
// resolve gang facts, and so a PodGroup change requeues the gang's members once
// that informer holds it), wires a gang-pod lister from the scheduler's shared
// informer, registers the gate's handler for a member replaced under its name
// on that informer, and starts the assignment garbage collector.
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
		assignments:                    placement.New(),
		pgReader:                       reader,
		podLister:                      podLister,
	}
	if err := reader.onChange(g.podGroupChanged); err != nil {
		return nil, err
	}
	if err := g.watchReplacedMembers(h.SharedInformerFactory().Core().V1().Pods().Informer()); err != nil {
		return nil, err
	}
	go g.runAssignmentGC(ctx)
	return g, nil
}

// Name returns the plugin's registered name.
func (g *GangPack) Name() string { return Name }

// PreFilter chooses the gang's domain and records the assignment. For a
// resolvable gang it best-fits a domain over the live node snapshot, records the
// assignment (which Filter enforces), and narrows downstream candidates to that
// domain's nodes. Standalone pods continue so Filter can protect forming-gang
// reservations; labeled gang pods fail closed when their PodGroup cannot be
// resolved. Unschedulable when a real gang fits in no domain.
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
	key := ns + "/" + name
	facts, found := g.pgReader.get(ns, name)
	if !found {
		g.releaseStaleGang(key)
		// The pod is labelled into a PodGroup the reader can't resolve yet — most
		// often informer lag right after the PodGroup was created (pod and group
		// created together). Skipping here would let a lone member bind and break
		// all-or-nothing, so hold the pod instead; it requeues automatically when
		// the PodGroup appears (a registered enqueue event). Plain Unschedulable so
		// the requeue hints stay in effect.
		return nil, framework.NewStatus(framework.Unschedulable, "PodGroup "+key+" not resolvable yet")
	}
	if facts.minMember <= 0 {
		return nil, framework.NewStatus(framework.Unschedulable,
			"PodGroup "+key+" must declare a positive minMember")
	}
	// A producer may declare a per-gang override on its PodGroup, or a cluster
	// may configure one topology key for every gang. The latter keeps PodGroup
	// producers entirely unaware of this plugin's metadata contract.
	gang, ok := facts.gang(key, g.topologyKey)
	if !ok {
		if g.podGroupTopologyKeyAnnotation == "" {
			return nil, framework.NewStatus(framework.Unschedulable,
				"scheduler must configure topologyKey or podGroupTopologyKeyAnnotation for gang pods")
		}
		return nil, framework.NewStatus(framework.Unschedulable,
			"PodGroup "+key+" must declare "+g.podGroupTopologyKeyAnnotation+" or the scheduler must configure topologyKey")
	}
	if g.unsupportedPlacementGroupLabel != "" {
		if _, present := pod.Labels[g.unsupportedPlacementGroupLabel]; present || facts.inPlacementGroup {
			return nil, framework.NewStatus(framework.Unschedulable,
				"partner placement groups are not supported")
		}
	}
	if gang.timeout <= 0 {
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"PodGroup "+key+" must set scheduleTimeoutSeconds or the scheduler must configure defaultPermitTimeoutSeconds")
	}
	return g.assignGangDomain(state, nodes, gang, pod)
}

// gangCycle is one PreFilter pass over a gang member: what the snapshot says
// about the gang and the domain it ends up committed to. The steps of
// assignGangDomain fill it in order: the members already placed and the
// templates still to place, the room per domain over the nodes the gang may
// plan on, then the assignment.
type gangCycle struct {
	gang gangInfo
	pod  *v1.Pod
	// nodes is the whole snapshot; view is the one pass over it: the members
	// already placed, and the nodes grouped by domain, read once. A member that
	// holds an assignment reads its domain alone; a member choosing one reads
	// every domain.
	nodes []framework.NodeInfo
	view  snapshotView
	// placed is what the snapshot holds of the gang: the members already bound or
	// assumed, and the domain they sit in.
	placed boundGangPlacement
	// need is how many of the gang's unplaced members must still land together;
	// members is what each of them asks of a node, read once for every domain.
	need    int
	members gangMembers
	// planning is the part of the view the gang may still plan on: its assigned
	// domain, else the domain of its bound members, else every domain;
	// planningNodes counts its nodes (the whole snapshot when planning
	// everywhere). free is the room each planning domain offers the remaining
	// members.
	planning      map[string][]domainNode
	planningNodes int
	free          topology.FreeByDomain
	// assignment is the domain the gang is committed to, nil until chosen.
	assignment *assignmentState
}

// assignGangDomain is the decision core, split out so it is unit-testable with
// an explicit node slice (no Handle). It commits the gang to one domain and
// returns that domain's nodes as the downstream candidate set, in four steps:
// drop an assignment the gang may not keep, read what the snapshot says about
// the gang, follow the assignment the gang already holds or choose a domain,
// then narrow the candidates to the nodes of that domain.
//
// Only the first member of a gang runs all-domain best-fit. Later members still
// make one snapshot pass to detect split placement and assumed siblings, but
// resource matching is bounded to the committed domain.
//
// When the in-memory assignment is absent but the gang already has members
// placed in the cluster (scheduler restart / leader failover lost it), the
// gang's domain is adopted from where those members already sit rather than
// best-fit afresh: re-planning a live gang into a different domain would leave its
// bound members without the rest of the gang.
func (g *GangPack) assignGangDomain(state framework.CycleState, nodes []framework.NodeInfo, gang gangInfo, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	assignment, status := g.currentAssignment(gang)
	if status != nil {
		return nil, status
	}
	cycle, status := g.readGangCycle(nodes, gang, pod, assignment)
	if status != nil {
		return nil, status
	}
	cycle.trace()
	if cycle.assignment != nil {
		if status := g.followAssignment(cycle); status != nil {
			return nil, status
		}
	}
	if cycle.assignment == nil {
		if status := g.chooseDomain(cycle); status != nil {
			klog.V(4).InfoS("gangpack.assignGangDomain.no_fit", "gang", gang.key, "status", status.Message())
			return nil, status
		}
	}
	return g.narrowToDomain(state, cycle)
}

// currentAssignment returns the assignment the gang may keep, or nil when it
// holds none. An assignment owned by a previous PodGroup of the same name, or
// made under a topology key the PodGroup no longer declares, is released
// first, together with its waiting members; when there were any, the gang
// retries later so a new attempt never forms beside them.
func (g *GangPack) currentAssignment(gang gangInfo) (*assignmentState, *framework.Status) {
	domain, commitment, found, ownerMatch := g.assignments.GetOwnedBy(gang.key, gang.uid)
	if !found {
		return nil, nil
	}
	if !ownerMatch {
		hadWaiters := g.hasWaitingAttempt(gang.key, commitment)
		oldGang := gang
		oldGang.uid = g.assignments.Owners()[gang.key]
		g.releaseAttempt(&assignmentState{
			domain: domain.Name, topologyKey: domain.TopologyKey,
			gang: oldGang, commitment: commitment,
		}, nil, false)
		g.clearFailedDomains(oldGang)
		if hadWaiters {
			return nil, framework.NewStatus(framework.Unschedulable,
				"released commitment owned by a previous PodGroup instance "+gang.key)
		}
		return nil, nil
	}
	if domain.TopologyKey != "" && domain.TopologyKey != gang.topologyKey {
		hadWaiters := g.hasWaitingAttempt(gang.key, commitment)
		g.releaseAttempt(&assignmentState{
			domain: domain.Name, topologyKey: domain.TopologyKey,
			gang: gang, commitment: commitment,
		}, nil, false)
		gangAssignmentTotal.WithLabelValues("topology_replan").Inc()
		if hadWaiters {
			return nil, framework.NewStatus(framework.Unschedulable,
				"released commitment after topology key changed for "+gang.key)
		}
		return nil, nil
	}
	return &assignmentState{domain: domain.Name, topologyKey: gang.topologyKey, gang: gang, commitment: commitment}, nil
}

// readGangCycle reads what the snapshot says about the gang: the members already
// placed (a gang split over domains cannot complete), the member templates
// still to place, and the room per domain over the nodes the gang may plan on.
// A member whose required affinity names an unplaced sibling waits instead of
// planning: every node would fail Filter and the chosen domain would be
// recorded as failed. The sibling's own Permit activation retries this member
// once it is placed.
func (g *GangPack) readGangCycle(nodes []framework.NodeInfo, gang gangInfo, pod *v1.Pod, assignment *assignmentState) (*gangCycle, *framework.Status) {
	cycle := &gangCycle{gang: gang, pod: pod, nodes: nodes, assignment: assignment}
	// One snapshot pass establishes bound/assumed placement and the identities that
	// gangTemplates must exclude, and groups the nodes the room is read over.
	// Waiting Permit pods are assumed here, so this is also the authoritative
	// view for remaining-template selection.
	scope := everyDomain
	if assignment != nil {
		scope = assignment.domain
	}
	cycle.view = scanSnapshot(nodes, pod, gang.topologyKey, scope)
	cycle.placed = cycle.view.placed
	// The OME controller reads this reason and the domain-cannot-fit reasons
	// below to end, sooner, the attempt of a gang no capacity elsewhere can
	// free (gangDomainFullPhrases in pkg/controller/v1beta1/workload/types).
	// Keep their wording in step with that list.
	if cycle.placed.split {
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"gang "+gang.key+" already has members in multiple topology domains")
	}
	templates, need, status := g.gangTemplates(gang, pod, cycle.placed)
	if status != nil {
		return nil, status
	}
	cycle.need, cycle.members = need, membersOf(templates)
	if sibling, blocked := siblingAffinityBlocked(pod, templates[1:], cycle.placed.placed); blocked {
		klog.V(4).InfoS("gangpack.assignGangDomain.sibling_wait", "gang", gang.key, "sibling", klog.KObj(sibling))
		return nil, framework.NewStatus(framework.Unschedulable,
			"waiting for gang sibling "+sibling.Namespace+"/"+sibling.Name+" to be placed")
	}
	cycle.measureRoom()
	return cycle, nil
}

// measureRoom bounds the planning nodes and measures the room per domain. A
// gang with an assignment matches only inside its domain; a gang adopted from
// bound members only inside theirs; all-domain matching is reserved for an
// unplaced gang choosing a new domain. A view read for one domain is read
// again over every domain when the gang plans outside it: it comes to choose,
// or its placed members sit in another domain than the one it was assigned.
func (c *gangCycle) measureRoom() {
	domain := everyDomain
	switch {
	case c.assignment != nil:
		domain = c.assignment.domain
	case c.placed.count > 0:
		domain = c.placed.domain
	}
	if c.view.scope != everyDomain && domain != c.view.scope {
		c.view = scanSnapshot(c.nodes, c.pod, c.gang.topologyKey, everyDomain)
	}
	c.planning = c.view.planning(domain)
	c.planningNodes = len(c.nodes)
	if domain != everyDomain {
		c.planningNodes = len(c.planning[domain])
	}
	c.free = roomByDomain(c.planning, c.members, c.need)
}

// trace writes the planning inputs at V(4): the assignment, the room per domain
// and the narrowed candidate set are otherwise invisible, and they are exactly
// what a gang that won't place needs (an assigned domain whose nodes are all
// filtered out while other domains sit free). Off at the default verbosity, so
// it costs nothing in normal operation.
func (c *gangCycle) trace() {
	if !klog.V(4).Enabled() {
		return
	}
	domain := ""
	if c.assignment != nil {
		domain = c.assignment.domain
	}
	klog.V(4).InfoS("gangpack.assignGangDomain.enter", "gang", c.gang.key, "minMember", c.gang.minMember,
		"topologyKey", c.gang.topologyKey, "assigned", c.assignment != nil, "assignedDomain", domain,
		"nodesInAssignedDomain", c.planningNodes,
		"boundDomain", c.placed.domain, "boundCount", c.placed.count, "freeAssignedDomain", c.free[domain], "freeByDomain", fmt.Sprintf("%v", c.free))
}

// followAssignment keeps the gang in the domain it is committed to. An
// assignment the gang can no longer use is dropped and the gang plans again:
// the domain vanished, or it filled before any member landed there (see
// assignmentStale). A domain that already holds a member is kept even while it
// cannot fit the rest; the member waits for room there. The assignment inherits
// the planner's answer on whether any other domain could take the gang, so a
// veto here is judged the same way.
func (g *GangPack) followAssignment(c *gangCycle) *framework.Status {
	gang, domain := c.gang, c.assignment.domain
	if assignmentStale(c.planningNodes, c.placed.count, c.free, c.need, domain) {
		// Releasing also returns the assignment's leaked capacity reservation,
		// which may be what kept other pods out of the domain, so they are
		// requeued as on any other release.
		if g.assignments.Release(gang.key, c.assignment.commitment, nil) {
			g.activateReservationBlocked()
		}
		gangAssignmentTotal.WithLabelValues("stale_replan").Inc()
		assignedGroups.Set(float64(g.assignments.Len()))
		c.assignment = nil
		c.measureRoom()
		klog.V(4).InfoS("gangpack.assignGangDomain.stale_replan", "gang", gang.key, "releasedDomain", domain)
		return nil
	}
	if c.placed.count > 0 && c.free[domain] < c.need {
		return framework.NewStatus(framework.Unschedulable,
			"pinned domain "+domain+" cannot fit all remaining gang members "+gang.key)
	}
	c.assignment.soleFit = g.assignments.SoleFit(gang.key, c.assignment.commitment)
	return nil
}

// chooseDomain commits a gang without an assignment to a domain: it adopts the
// domain of any already-placed members (failover lost the assignment), else
// best-fits a fresh one. It records the assignment and the placement metric.
// The status is Unschedulable only when the gang fits in no domain. The
// assignment's soleFit reports that no other domain free of reservations could
// take the gang, so a veto in this one has nothing left to re-plan into.
//
// Room the snapshot has but Choose withholds belongs to another gang's
// outstanding reservation. That reservation draining or unwinding produces no
// cluster event, so the pod is remembered and requeued with the reservation's
// other blocked pods; a gang short of capacity everywhere is left to the
// registered pod and node events.
func (g *GangPack) chooseDomain(c *gangCycle) *framework.Status {
	gang := c.gang
	if d, count := c.placed.domain, c.placed.count; count > 0 {
		// Rebuild from ground truth: the gang lives where its members already are.
		remaining := gang.minMember - count
		id := g.assignments.Set(gang.placementGang(), d, remaining, c.view.names(d))
		gangAssignmentTotal.WithLabelValues("adopted").Inc()
		assignedGroups.Set(float64(g.assignments.Len()))
		if c.free[d] < c.need {
			return framework.NewStatus(framework.Unschedulable,
				"adopted domain "+d+" cannot fit remaining gang members "+gang.key)
		}
		c.assignment = &assignmentState{domain: d, topologyKey: gang.topologyKey, gang: gang, commitment: id, soleFit: true}
		return nil
	}
	choice, hadFailed := g.withoutFailedDomains(gang, c.free)
	names := c.view.namesByDomain()
	d, id, fits := g.assignments.Choose(gang.placementGang(), choice, names, c.need)
	if !fits && hadFailed {
		g.clearFailedDomains(gang)
		d, id, fits = g.assignments.Choose(gang.placementGang(), c.free, names, c.need)
	}
	if !fits {
		return g.noDomainFits(c)
	}
	gangAssignmentTotal.WithLabelValues("pinned").Inc()
	assignedGroups.Set(float64(g.assignments.Len()))
	c.assignment = &assignmentState{domain: d, topologyKey: gang.topologyKey, gang: gang, commitment: id,
		soleFit: g.assignments.SoleFit(gang.key, id)}
	return nil
}

// noDomainFits is the status for a gang Choose could not place. A domain with
// room that another forming gang has reserved is told apart from a plain
// capacity shortfall: the pod is remembered so the reservation's release
// requeues it.
func (g *GangPack) noDomainFits(c *gangCycle) *framework.Status {
	if _, room := topology.BestFit(c.free, c.need); room {
		g.rememberReservationBlocked(c.pod)
		gangAssignmentTotal.WithLabelValues("reserved").Inc()
		return framework.NewStatus(framework.Unschedulable,
			"no domain has room for gang "+c.gang.key+": every fitting domain is reserved for a forming gang")
	}
	gangAssignmentTotal.WithLabelValues("no_fit").Inc()
	return framework.NewStatus(framework.Unschedulable, "no domain has room for gang "+c.gang.key)
}

// narrowToDomain records the assignment for this cycle and returns the nodes of
// its domain that leave room for the members this one brings along; Filter
// enforces the domain from the recorded state.
func (g *GangPack) narrowToDomain(state framework.CycleState, c *gangCycle) (*framework.PreFilterResult, *framework.Status) {
	gang, domain := c.gang, c.assignment.domain
	candidates := candidateNodes(c.view.byDomain[domain], c.members, c.need)
	if len(candidates) == 0 {
		return nil, framework.NewStatus(framework.Unschedulable,
			"pinned domain "+domain+" has no node feasible for gang member "+gang.key)
	}
	writeAssignment(state, domain, gang, c.assignment.commitment)
	if c.assignment.soleFit {
		markSoleFit(state)
	}
	klog.V(4).InfoS("gangpack.assignGangDomain.result", "gang", gang.key, "domain", domain, "narrowedNodes", candidates.Len())
	return &framework.PreFilterResult{NodeNames: candidates}, nil
}

// PreFilterExtensions returns nil: the plugin does not incrementally re-evaluate
// on pod add/remove during preemption.
func (g *GangPack) PreFilterExtensions() framework.PreFilterExtensions { return nil }

// Filter enforces the gang's assigned domain: once PreFilter has recorded an
// assignment in CycleState, only nodes in that domain pass; every other node is
// rejected as UnschedulableAndUnresolvable (a wrong-domain node cannot be made
// right by preempting on it, so the scheduler must not attempt preemption
// there). When no assignment is recorded — the pod is not an assigned gang
// member — Filter imposes no constraint and admits the node.
func (g *GangPack) Filter(_ context.Context, state framework.CycleState, pod *v1.Pod, nodeInfo framework.NodeInfo) *framework.Status {
	assignment := readAssignment(state)
	if assignment == nil {
		if g.assignments == nil {
			return nil
		}
		if nodeInfo != nil && nodeInfo.Node() != nil && g.assignments.BlocksNode(nodeInfo.Node().Name, nodeInfo.Node().Labels) {
			g.rememberReservationBlocked(pod)
			return framework.NewStatus(framework.UnschedulableAndUnresolvable,
				"node is reserved for a forming gang")
		}
		return nil
	}
	nodeDomain := domainOf(nodeInfo.Node(), assignment.topologyKey)
	if nodeDomain != assignment.domain {
		// Trace at V(4): the assignment (domain + topology key) Filter reads from
		// CycleState, versus the candidate node's domain. Pairs with the
		// assignGangDomain trace to catch a PreFilter->Filter mismatch (an
		// assigned domain's own nodes rejected here while it looks free in
		// PreFilter).
		klog.V(4).InfoS("gangpack.Filter.reject", "node", nodeInfo.Node().GetName(),
			"nodeDomain", nodeDomain, "assignedDomain", assignment.domain, "assignedTopologyKey", assignment.topologyKey)
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"node not in gang's pinned domain "+assignment.domain)
	}
	return nil
}

// assignmentStale reports whether a gang's existing assignment must be dropped
// and re-planned. Two cases hold a gang that keeps its assignment forever, and
// both are safe to re-plan because no member has landed in the domain yet:
//
//   - the domain vanished — every node drained/scaled away, or the PodGroup's
//     topology key changed so the old domain value matches no node. Reusing it
//     narrows every candidate away.
//   - the domain filled — another gang won the race for it after this gang
//     committed, so it can no longer fit this gang's minMember. Without a
//     re-plan the gang retries the full domain forever (Filter rejects every
//     occupied node) while other domains sit free, and its Choose reservation
//     leaks until re-plan releases it.
//
// A domain that already holds any of this gang's members is NEVER stale: those
// members are bound (or assumed) there, so re-planning would split the gang. Such
// a gang keeps its assignment and waits for capacity in its own domain. The
// full-domain projection is only paid while a gang is unplaced (a transient
// formation window); once a member lands, the early return keeps steady-state
// PreFilter cheap.
func assignmentStale(nodesInAssignedDomain, boundCount int, free topology.FreeByDomain, need int, domain string) bool {
	if nodesInAssignedDomain == 0 {
		return true // domain vanished
	}
	if boundCount > 0 {
		return false // has members placed here already; never split the gang
	}
	return free[domain] < need
}

// gangTemplates returns the concrete remaining members used for simultaneous
// domain feasibility: the current member and the siblings still to place, in
// name order, and how many of them the gang needs. A set short of that need
// holds the member and is recorded. Without a pod lister (unit tests of the
// decision core) every remaining member is a copy of the current pod.
func (g *GangPack) gangTemplates(gang gangInfo, current *v1.Pod, placement boundGangPlacement) ([]*v1.Pod, int, *framework.Status) {
	need := membersStillNeeded(gang, placement)
	if g.podLister == nil {
		return copiesOf(current, need), need, nil
	}
	templates, err := g.membersStillToPlace(gang, current, placement)
	if err != nil {
		return nil, 0, framework.AsStatus(err)
	}
	if len(templates) < need {
		g.markTemplatesIncomplete(gang)
		return nil, 0, framework.NewStatus(framework.Unschedulable,
			"waiting for all PodGroup member templates for "+gang.key)
	}
	g.requeueMembersHeldOnTheShortSet(gang, current, len(templates), need)
	return templates, need, nil
}

// membersStillNeeded is how many members the gang needs beside those the
// snapshot already holds; the current member always counts, so at least one.
func membersStillNeeded(gang gangInfo, placement boundGangPlacement) int {
	return max(1, gang.minMember-placement.count)
}

// copiesOf returns count copies of the current pod, one per member still to place.
func copiesOf(current *v1.Pod, count int) []*v1.Pod {
	out := make([]*v1.Pod, count)
	for i := range out {
		out[i] = current
	}
	return out
}

// membersStillToPlace lists the gang's live members from the pod lister: the
// current member first, then the siblings still to place in name order.
func (g *GangPack) membersStillToPlace(gang gangInfo, current *v1.Pod, placement boundGangPlacement) ([]*v1.Pod, error) {
	ns, name, _ := splitGangKey(gang.key)
	members, err := g.podLister.gangPods(ns, name)
	if err != nil {
		return nil, err
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	out := []*v1.Pod{current}
	for _, member := range members {
		if siblingStillToPlace(member, current, placement) {
			out = append(out, member)
		}
	}
	return out, nil
}

// siblingStillToPlace reports whether a listed member is a sibling the gang
// still has to place: not the current member, not on a node, not held by the
// snapshot, not being deleted and not finished.
func siblingStillToPlace(member, current *v1.Pod, placement boundGangPlacement) bool {
	return member != nil && member.Name != current.Name && member.Spec.NodeName == "" && placement.placed[member.Name] == nil &&
		member.DeletionTimestamp == nil && member.Status.Phase != v1.PodSucceeded && member.Status.Phase != v1.PodFailed
}

// requeueMembersHeldOnTheShortSet requeues the members held back while the
// set was short, once per completion. The record is consumed only after the
// activation has run; the current member is in flight and left out of it.
func (g *GangPack) requeueMembersHeldOnTheShortSet(gang gangInfo, current *v1.Pod, listed, need int) {
	if g.hasTemplatesIncomplete(gang) && g.activateGangMembers(gang, activationTriggerTemplatesComplete, current) {
		g.clearTemplatesIncomplete(gang)
		klog.V(4).InfoS("gangpack.templates.complete", "gang", gang.key, "members", listed, "need", need)
	}
}
