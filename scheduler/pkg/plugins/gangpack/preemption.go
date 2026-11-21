package gangpack

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	resourcehelper "k8s.io/component-helpers/resource"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
	apipod "k8s.io/kubernetes/pkg/api/v1/pod"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"

	"sigs.k8s.io/ome/scheduler/pkg/preemption"
)

// Gang preemption. When a gang fits no topology domain for lack of room, the
// plugin may free one domain by evicting lower-priority work there: every pod
// of a lower-priority gang, never part of one, and lower-priority pods that
// belong to no gang. It reserves the domain for the waiting gang before the
// first eviction, so nothing else takes the room while the evicted pods shut
// down. Which units leave is the pure function preemption.Choose; this
// file reads its inputs from the scheduler snapshot and carries out its answer.

// noRoomStateKey marks a cycle in which PreFilter found the gang fits no
// domain for lack of room. PostFilter preempts only for such a cycle: any
// other rejection (an incomplete member set, an unresolved PodGroup, a
// domain reserved by a forming gang) is not cured by evicting anyone.
const noRoomStateKey framework.StateKey = "gangpack.noRoom"

// noRoomState is what PreFilter read about a gang that fits no domain: the
// snapshot nodes, the nodes grouped by domain, and the members still to place.
type noRoomState struct {
	gang    gangInfo
	nodes   []framework.NodeInfo
	view    snapshotView
	members gangMembers
	need    int
}

// Clone satisfies framework.StateData. The state is read-only after PreFilter
// writes it, so the copy shares its slices and maps.
func (s *noRoomState) Clone() framework.StateData {
	c := *s
	return &c
}

func readNoRoom(state framework.CycleState) *noRoomState {
	return cycleStateValue[noRoomState](state, noRoomStateKey)
}

// Results recorded under gang_preemption_total.
const (
	// preemptionEvicted: lower-priority pods were evicted and the domain is held.
	preemptionEvicted = "evicted"
	// preemptionHeld: the domain fits once pods already shutting down are
	// gone, so it is held without evicting anyone new.
	preemptionHeld = "held"
	// preemptionInFlightLimit: the configured number of preemptions is
	// already in flight.
	preemptionInFlightLimit = "in_flight_limit"
	// preemptionNoDomain: no domain fits the gang even with every
	// lower-priority unit gone.
	preemptionNoDomain = "no_domain"
)

// refusal is why a cycle did not preempt: a result to count, empty when the
// case is routine, and a message for the pod's scheduling condition.
type refusal struct {
	result  string
	message string
}

// candidate is one unit that may leave a domain, with the pods that leave
// with it.
type candidate struct {
	unit preemption.Unit
	// gang is the unit's gang key, or empty for a pod in no gang.
	gang string
	// pods are the unit's pods on nodes in the snapshot.
	pods []*v1.Pod
	// eligible is false when one of its pods is not lower priority than the
	// preemptor, or its gang still has room reserved: a forming gang, or one
	// waiting for a domain preemption is freeing.
	eligible bool
}

// preemptionPlan is one decision: the gang, the domain held for it, and the
// units evicted from the domain.
type preemptionPlan struct {
	gang   gangInfo
	domain string
	evict  []*candidate
}

// preempt frees one domain for the pod's gang when preemption is configured
// and PreFilter found no domain with room. A successful preemption returns
// Success with no nomination: the gang waits for the evicted pods to go, held
// by the domain reservation, and no later PostFilter plugin evicts more.
func (g *GangPack) preempt(ctx context.Context, state framework.CycleState, pod *v1.Pod) (*framework.PostFilterResult, *framework.Status) {
	if g.maxPreemptionsInFlight <= 0 {
		return nil, framework.NewStatus(framework.Unschedulable)
	}
	plan, refused := g.planPreemption(state, pod)
	if plan == nil {
		if refused.result != "" {
			gangPreemptionTotal.WithLabelValues(refused.result).Inc()
		}
		if refused.message == "" {
			return nil, framework.NewStatus(framework.Unschedulable)
		}
		return nil, framework.NewStatus(framework.Unschedulable, refused.message)
	}
	assignedGroups.Set(float64(g.assignments.Len()))
	if len(plan.evict) == 0 {
		gangPreemptionTotal.WithLabelValues(preemptionHeld).Inc()
		return noNomination(), framework.NewStatus(framework.Success,
			"holding domain "+plan.domain+" for gang "+plan.gang.key+" while pods there shut down")
	}
	evicted := g.evict(ctx, pod, plan)
	gangPreemptionTotal.WithLabelValues(preemptionEvicted).Inc()
	gangPreemptionEvictedPods.Add(float64(evicted))
	klog.FromContext(ctx).V(2).Info("gangpack.preempt", "gang", plan.gang.key, "domain", plan.domain,
		"evictedUnits", unitKeys(plan.evict), "evictedPods", evicted)
	return noNomination(), framework.NewStatus(framework.Success,
		fmt.Sprintf("evicted %d lower-priority pods (%s) to free domain %s for gang %s",
			evicted, strings.Join(unitKeys(plan.evict), ", "), plan.domain, plan.gang.key))
}

func noNomination() *framework.PostFilterResult {
	return &framework.PostFilterResult{NominatingInfo: &framework.NominatingInfo{NominatingMode: framework.ModeNoop}}
}

// planPreemption chooses the domain and the units to evict and holds the domain for
// the gang. It returns no plan, with the reason, when the cycle is not one
// preemption can help or the safety rules forbid it.
func (g *GangPack) planPreemption(state framework.CycleState, pod *v1.Pod) (*preemptionPlan, refusal) {
	room := readNoRoom(state)
	if room == nil || g.assignments == nil || g.handle == nil {
		return nil, refusal{}
	}
	gang := room.gang
	if pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == v1.PreemptNever {
		return nil, refusal{message: "gang " + gang.key + " does not preempt: its preemption policy is Never"}
	}
	if _, _, _, held := g.assignments.GetOwned(gang.key); held {
		return nil, refusal{message: "gang " + gang.key + " already holds a domain"}
	}
	if g.assignments.Holding() >= g.maxPreemptionsInFlight {
		return nil, refusal{result: preemptionInFlightLimit,
			message: fmt.Sprintf("gang %s waits: %d preemptions are already in flight", gang.key, g.maxPreemptionsInFlight)}
	}
	units := candidateUnits(room.nodes, pod, g.assignments.Reserving)
	choice, ok := preemption.Choose(g.preemptionDomains(room, units), fitsAfterEviction(room, units))
	if !ok {
		return nil, refusal{result: preemptionNoDomain,
			message: "no domain can be freed for gang " + gang.key + " by evicting lower-priority pods"}
	}
	plan := &preemptionPlan{gang: gang, domain: choice.Domain}
	for _, u := range choice.Evict {
		plan.evict = append(plan.evict, units[u.Key])
	}
	until := g.clock().Add(g.holdDuration(plan, room))
	if _, ok := g.assignments.Hold(gang.placementGang(), choice.Domain, room.need, room.view.names(choice.Domain), until); !ok {
		return nil, refusal{message: "domain " + choice.Domain + " was reserved by another gang"}
	}
	return plan, refusal{}
}

// unitKeyOf names the unit a pod leaves with: its gang, or the pod alone. The
// two forms cannot collide. gangKey is empty for a pod in no gang.
func unitKeyOf(pod *v1.Pod) (key, gangKey string) {
	if ns, name, ok := podGroupNameOf(pod); ok {
		gangKey = ns + "/" + name
		return "gang " + gangKey, gangKey
	}
	return "pod " + pod.Namespace + "/" + pod.Name, ""
}

// candidateUnits groups every pod on a node in the snapshot, except the
// preemptor's own gang and pods already shutting down, into the units they
// would leave with. A unit is eligible only when every one of its pods ranks
// below the preemptor and its gang reserves no room.
func candidateUnits(nodes []framework.NodeInfo, preemptor *v1.Pod, reserving func(gang string) bool) map[string]*candidate {
	priority := corev1helpers.PodPriority(preemptor)
	units := make(map[string]*candidate)
	for _, info := range nodes {
		if info == nil {
			continue
		}
		for _, pi := range info.GetPods() {
			pod := pi.GetPod()
			if pod == nil || pod.DeletionTimestamp != nil || samePodGroup(pod, preemptor) {
				continue
			}
			key, gangKey := unitKeyOf(pod)
			u := units[key]
			if u == nil {
				u = &candidate{unit: preemption.Unit{Key: key, Priority: math.MinInt32}, gang: gangKey}
				units[key] = u
			}
			u.pods = append(u.pods, pod)
			u.unit.Pods++
			u.unit.Priority = max(u.unit.Priority, corev1helpers.PodPriority(pod))
			u.unit.Accelerators += extendedRequests(pod)
		}
	}
	for _, u := range units {
		u.eligible = u.unit.Priority < priority && (u.gang == "" || !reserving(u.gang))
	}
	return units
}

// extendedRequests sums the pod's requests of extended resources, such as
// accelerators, in their own units.
func extendedRequests(pod *v1.Pod) int64 {
	var total int64
	for name, quantity := range resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{}) {
		if v1helper.IsExtendedResourceName(name) {
			total += quantity.Value()
		}
	}
	return total
}

// preemptionDomains lists, in name order, every domain no other gang has
// reserved, with the eligible units that have a pod on one of its nodes. A
// domain with no eligible unit and no pod shutting down is left out: nothing
// can leave it, and PreFilter already found it has no room.
func (g *GangPack) preemptionDomains(room *noRoomState, units map[string]*candidate) []preemption.Domain {
	names := make([]string, 0, len(room.view.byDomain))
	for name := range room.view.byDomain {
		names = append(names, name)
	}
	sort.Strings(names)
	domains := make([]preemption.Domain, 0, len(names))
	for _, name := range names {
		if g.assignments.Blocked(room.gang.topologyKey, name, room.view.names(name)) {
			continue
		}
		if domain, leaving := domainUnits(name, room.view.byDomain[name], units); leaving || len(domain.Units) > 0 {
			domains = append(domains, domain)
		}
	}
	return domains
}

// domainUnits lists the eligible units with a pod on one of the domain's
// nodes, each once, and reports whether any pod there is shutting down.
func domainUnits(name string, nodes []domainNode, units map[string]*candidate) (preemption.Domain, bool) {
	seen := sets.New[string]()
	domain := preemption.Domain{Name: name}
	leaving := false
	for _, dn := range nodes {
		for _, pi := range dn.info.GetPods() {
			pod := pi.GetPod()
			if pod == nil {
				continue
			}
			if pod.DeletionTimestamp != nil {
				leaving = true
				continue
			}
			key, _ := unitKeyOf(pod)
			if u := units[key]; u != nil && u.eligible && !seen.Has(key) {
				seen.Insert(key)
				domain.Units = append(domain.Units, u.unit)
			}
		}
	}
	return domain, leaving
}

// fitsAfterEviction answers preemption.Choose: whether the gang's remaining
// members fit a domain once the given units and every pod already shutting
// down there are gone. It measures a copy of each node, so the snapshot is
// never changed.
func fitsAfterEviction(room *noRoomState, units map[string]*candidate) preemption.FitsFunc {
	return func(domain string, gone []preemption.Unit) bool {
		goneKeys := sets.New[string]()
		for _, u := range gone {
			goneKeys.Insert(u.Key)
		}
		nodes := room.view.byDomain[domain]
		trial := make([]domainNode, 0, len(nodes))
		for _, dn := range nodes {
			info := dn.info.Snapshot()
			for _, pi := range dn.info.GetPods() {
				pod := pi.GetPod()
				if pod == nil {
					continue
				}
				if key, _ := unitKeyOf(pod); pod.DeletionTimestamp != nil || goneKeys.Has(key) {
					_ = info.RemovePod(klog.Background(), pod)
				}
			}
			trial = append(trial, domainNode{info: info, node: dn.node})
		}
		return newGangRoom(trial, room.members, room.need).currentCanJoin(room.need)
	}
}

// holdDuration is how long the domain stays reserved for the gang: until the
// slowest pod leaving the domain is due to be gone, then the gang's own wait
// at the gate. Both come from the workloads, so no constant is involved.
func (g *GangPack) holdDuration(plan *preemptionPlan, room *noRoomState) time.Duration {
	now := g.clock()
	var shutdown time.Duration
	for _, u := range plan.evict {
		for _, pod := range u.pods {
			if s := pod.Spec.TerminationGracePeriodSeconds; s != nil {
				shutdown = max(shutdown, time.Duration(*s)*time.Second)
			}
		}
	}
	for _, dn := range room.view.byDomain[plan.domain] {
		for _, pi := range dn.info.GetPods() {
			if pod := pi.GetPod(); pod != nil && pod.DeletionTimestamp != nil {
				shutdown = max(shutdown, pod.DeletionTimestamp.Sub(now))
			}
		}
	}
	return shutdown + plan.gang.timeout
}

// evict removes every pod of each evicted unit, in parallel, and returns how
// many it removed. An evicted gang's members not yet on a node go too, so no
// part of the gang is left behind. A pod waiting at a Permit gate is rejected
// there; any other pod is marked with the scheduler's disruption condition,
// then deleted under its UID, so a pod recreated under the same name is never
// deleted in its place.
func (g *GangPack) evict(ctx context.Context, preemptor *v1.Pod, plan *preemptionPlan) int {
	pods := g.podsToEvict(plan)
	var (
		mu      sync.Mutex
		evicted int
		wg      sync.WaitGroup
	)
	for _, pod := range pods {
		wg.Add(1)
		go func(pod *v1.Pod) {
			defer wg.Done()
			if g.evictPod(ctx, preemptor, pod, plan) {
				mu.Lock()
				evicted++
				mu.Unlock()
			}
		}(pod)
	}
	wg.Wait()
	return evicted
}

// podsToEvict lists every pod of the plan's evicted units once: the pods on
// nodes, and the live members of each evicted gang.
func (g *GangPack) podsToEvict(plan *preemptionPlan) []*v1.Pod {
	seen := sets.New[string]()
	var out []*v1.Pod
	add := func(pod *v1.Pod) {
		if pod == nil || pod.DeletionTimestamp != nil {
			return
		}
		if key := podAttemptKey(pod); !seen.Has(key) {
			seen.Insert(key)
			out = append(out, pod)
		}
	}
	for _, u := range plan.evict {
		for _, pod := range u.pods {
			add(pod)
		}
		if u.gang == "" || g.podLister == nil {
			continue
		}
		if ns, name, ok := splitGangKey(u.gang); ok {
			if members, err := g.podLister.gangPods(ns, name); err == nil {
				for _, pod := range members {
					add(pod)
				}
			}
		}
	}
	return out
}

// evictPod removes one pod and reports whether it is gone or going.
func (g *GangPack) evictPod(ctx context.Context, preemptor, pod *v1.Pod, plan *preemptionPlan) bool {
	logger := klog.FromContext(ctx)
	if wp := g.handle.GetWaitingPod(pod.UID); wp != nil {
		wp.Reject(Name, "preempted by gang "+plan.gang.key)
		return true
	}
	cs := g.handle.ClientSet()
	condition := &v1.PodCondition{
		Type:               v1.DisruptionTarget,
		ObservedGeneration: apipod.CalculatePodConditionObservedGeneration(&pod.Status, pod.Generation, v1.DisruptionTarget),
		Status:             v1.ConditionTrue,
		Reason:             v1.PodReasonPreemptionByScheduler,
		Message:            fmt.Sprintf("%s: preempting to free domain %s for gang %s", preemptor.Spec.SchedulerName, plan.domain, plan.gang.key),
	}
	status := pod.Status.DeepCopy()
	if apipod.UpdatePodCondition(status, condition) {
		if err := schedutil.PatchPodStatus(ctx, cs, pod.Name, pod.Namespace, &pod.Status, status); err != nil {
			if apierrors.IsNotFound(err) {
				return true
			}
			logger.Error(err, "Could not mark a pod for eviction by preemption", "pod", klog.KObj(pod), "gang", plan.gang.key)
			return false
		}
	}
	uid := pod.UID
	err := cs.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "Could not delete a pod evicted by preemption", "pod", klog.KObj(pod), "gang", plan.gang.key)
		return false
	}
	if recorder := g.handle.EventRecorder(); recorder != nil {
		recorder.Eventf(pod, preemptor, v1.EventTypeNormal, "Preempted", "Preempting",
			"Preempted by gang %s to free domain %s", plan.gang.key, plan.domain)
	}
	return true
}

func unitKeys(units []*candidate) []string {
	out := make([]string, 0, len(units))
	for _, u := range units {
		out = append(out, u.unit.Key)
	}
	return out
}

// waitForHeldDomain keeps the domain preemption is freeing for the gang while
// the evicted pods shut down: the gang has no member placed, the domain still
// has nodes, and it cannot fit the gang yet. Once the hold's time is up the
// gang plans like any other, so a domain that never empties is not held
// forever.
func (g *GangPack) waitForHeldDomain(c *gangCycle) (*framework.Status, bool) {
	until, id, held := g.assignments.HeldUntil(c.gang.key)
	domain := c.assignment.domain
	if !held || id != c.assignment.commitment || c.placed.count > 0 || c.planningNodes == 0 || c.free[domain] >= c.need {
		return nil, false
	}
	if !g.clock().Before(until) {
		gangPreemptionTotal.WithLabelValues(preemptionHoldExpired).Inc()
		return nil, false
	}
	return framework.NewStatus(framework.Unschedulable,
		"waiting for evicted pods to leave domain "+domain+", held for gang "+c.gang.key), true
}

// preemptionHoldExpired: a held domain still could not fit its gang when the
// hold's time was up, and the gang plans again.
const preemptionHoldExpired = "hold_expired"

// releaseExpiredHolds drops each hold whose time is up while no member of its
// gang waits at the gate, so a gang that stops retrying does not keep a
// domain closed. A gang whose members wait at the gate is left to the gate's
// own timeout.
func (g *GangPack) releaseExpiredHolds() {
	now := g.clock()
	for group, owner := range g.assignments.Owners() {
		until, id, held := g.assignments.HeldUntil(group)
		if !held || now.Before(until) || g.hasWaitingAttempt(group, id) || !g.assignments.Reserving(group) {
			continue
		}
		domain, _, _, _ := g.assignments.GetOwned(group)
		assignment := &assignmentState{domain: domain.Name, topologyKey: domain.TopologyKey,
			gang: gangInfo{key: group, uid: owner}, commitment: id}
		if released, _ := g.releaseAttempt(assignment, nil, false); released {
			gangPreemptionTotal.WithLabelValues(preemptionHoldExpired).Inc()
		}
	}
}

// clock reads the plugin's clock.
func (g *GangPack) clock() time.Time {
	if g.now != nil {
		return g.now()
	}
	return time.Now()
}
