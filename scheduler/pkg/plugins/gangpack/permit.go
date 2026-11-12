package gangpack

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
)

// sameGang reports whether a pod belongs to the given gang key (namespace/name).
// The key is compared in place, half by half: the unwind's scan calls this
// once per waiting pod, and joining the pod's halves would allocate each time.
func sameGang(pod *v1.Pod, gangKey string) bool {
	ns, name, ok := podGroupNameOf(pod)
	if !ok || len(gangKey) != len(ns)+1+len(name) {
		return false
	}
	return gangKey[:len(ns)] == ns && gangKey[len(ns)] == '/' && gangKey[len(ns)+1:] == name
}

// Permit is the all-or-nothing gang gate. Each gang member waits here instead of
// binding immediately; when the arriving member makes the count reach minMember,
// every waiting sibling is released together and this member proceeds. Members
// wait up to the gang's timeout (its PodGroup ScheduleTimeoutSeconds); on timeout
// the framework rejects the pod and Unreserve unwinds the rest. A pod
// that is not an assigned gang member is not gated.
// Every entry at the gate belongs to a live pod: a member deleted while it waits
// is rejected on the deletion itself, by the scheduler when the deletion arrives
// alone and by rejectReplacedWaiter when the informer merges it with a recreation
// under the same name, so the gate never holds the gang on a pod that is gone.
func (g *GangPack) Permit(_ context.Context, state framework.CycleState, pod *v1.Pod, _ string) (*framework.Status, time.Duration) {
	assignment := readAssignment(state)
	if assignment == nil {
		if _, _, member := podGroupNameOf(pod); member {
			return framework.NewStatus(framework.Unschedulable,
				"labeled gang pod reached Permit without validated PreFilter state"), 0
		}
		return framework.NewStatus(framework.Success), 0
	}
	gang := assignment.gang
	if gang.timeout <= 0 {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"gang permit timeout is not configured"), 0
	}
	g.rememberAttempt(pod, assignment.commitment)
	members, err := g.readGateMembers(gang, pod)
	if err != nil {
		return framework.AsStatus(err), 0
	}
	// Siblings already at the gate are placed and need no retry, and this member
	// is in flight. Requeuing either only sets up an immediate retry of a gate
	// rejection that should wait until something changes.
	g.activate(activationTriggerPermit, members.toRequeue)

	// Reserve has already converted this member's claim into assumed occupancy.
	// The commitment reaches zero exactly when minMember slots, including any
	// members adopted as already bound, have arrived. Waiting pods are assumed in
	// the scheduler snapshot, so combining a snapshot count with the waiting set
	// would count them twice and open the gate early.
	if !g.assignments.Committed(gang.key, assignment.commitment) {
		gangGateTotal.WithLabelValues("wait").Inc()
		return framework.NewStatus(framework.Wait), gang.timeout
	}

	// The gang is complete: release every waiting sibling of this attempt; this
	// member proceeds.
	for _, sibling := range members.waiting {
		if g.attemptFor(sibling.GetPod()) == assignment.commitment {
			sibling.Allow(Name)
		}
	}
	gangGateTotal.WithLabelValues("admit").Inc()
	return framework.NewStatus(framework.Success), 0
}

// gateMembers is a gang as the gate reads it on one decision, from one list of
// the gang's live pods: the siblings waiting at the gate, found by UID, and the
// pods an activation requeues, keyed as Activate takes them.
type gateMembers struct {
	waiting   []framework.WaitingPod
	toRequeue map[string]*v1.Pod
}

// readGateMembers lists the gang's live pods once and sorts them for the gate,
// so a decision costs one lookup per member whatever the size of the waiting
// set, and a pod recreated under a new UID is read under the UID the informer holds.
func (g *GangPack) readGateMembers(gang gangInfo, pod *v1.Pod) (gateMembers, error) {
	// Without the list the gate cannot tell the siblings apart; the error fails
	// the member's cycle and unwinds the attempt.
	if g.podLister == nil || g.handle == nil {
		return gateMembers{}, errors.New("gang pod lister or scheduler handle is unavailable")
	}
	ns, name, ok := splitGangKey(gang.key)
	if !ok {
		return gateMembers{}, fmt.Errorf("malformed gang key %q", gang.key)
	}
	pods, err := g.podLister.gangPods(ns, name)
	if err != nil {
		return gateMembers{}, err
	}
	members := gateMembers{
		waiting:   make([]framework.WaitingPod, 0, len(pods)),
		toRequeue: make(map[string]*v1.Pod, len(pods)),
	}
	for _, member := range pods {
		if member == nil || samePodIdentity(member, pod) {
			continue
		}
		// A waiting pod is a sibling only while it still carries the gang's label.
		if sibling := g.handle.GetWaitingPod(member.UID); sibling != nil && sameGang(sibling.GetPod(), gang.key) {
			members.waiting = append(members.waiting, sibling)
			continue
		}
		members.toRequeue[member.Namespace+"/"+member.Name] = member
	}
	return members, nil
}

// watchReplacedMembers registers rejectReplacedWaiter on the pod informer the
// gang-pod lister reads, the store the scheduler queues a recreated pod from.
func (g *GangPack) watchReplacedMembers(informer cache.SharedIndexInformer) error {
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{UpdateFunc: g.rejectReplacedWaiter})
	return err
}

// rejectReplacedWaiter is the plugin's update handler on the scheduler's pod
// informer. A watch relist can merge a member's deletion and its recreation
// under the same name into one update whose old and new pods differ by UID
// only. The scheduler then queues the new pod and leaves the old pod's entry at
// the gate, keyed by its UID, until the gate's timeout. Rejecting the entry
// here starts the unwind a deletion delivered on its own starts: the attempt is
// released and the gang retries with its live members at once.
func (g *GangPack) rejectReplacedWaiter(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok || g.handle == nil {
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if !ok || newPod.UID == oldPod.UID {
		return
	}
	// Only a gang member can be at this gate; another plugin's waiting pods are
	// not this plugin's to reject.
	if _, _, member := podGroupNameOf(oldPod); !member {
		return
	}
	g.handle.RejectWaitingPod(oldPod.UID)
}

// Activation triggers, recorded as the gang_activation_total label value.
const (
	// activationTriggerPermit: a member reached Permit. An assumed-but-unbound
	// member produces no cluster event, yet waiting siblings whose Filter depends
	// on it (a worker with required affinity to its leader) must retry now.
	activationTriggerPermit = "permit"
	// activationTriggerTemplatesComplete: the live member set reached
	// minMember. An unscheduled pod's creation is not a queue move event, so a
	// member held back on the incomplete set would otherwise wait for the
	// periodic unschedulable flush.
	activationTriggerTemplatesComplete = "templates_complete"
	// activationTriggerReservationReleased: a forming gang's domain reservation
	// drained or was released. The pods it kept out of the domain, ordinary or
	// gang members, would otherwise wait for the periodic unschedulable flush.
	activationTriggerReservationReleased = "reservation_released"
	// activationTriggerPodGroupChange: the plugin's own PodGroup informer stored
	// a new or edited PodGroup. The scheduler requeues members on a separate
	// PodGroup watch that can run ahead of this informer; a member retried
	// against the older PodGroup waits again with no later event to requeue it.
	activationTriggerPodGroupChange = "podgroup_change"
	// activationTriggerDomainFailed: an assigned member was vetoed by another
	// Filter in a domain its gang had not failed before while another domain is
	// untried.
	// The in-flight activation requeues it after its normal backoff to plan there.
	activationTriggerDomainFailed = "domain_failed"
)

// podGroupChanged requeues a gang's live members once the plugin's PodGroup
// informer holds a new or edited PodGroup. PreFilter reads this informer, so a
// retry it triggers sees the change whichever PodGroup watch delivered first.
func (g *GangPack) podGroupChanged(namespace, name string) {
	g.activateGangMembers(gangInfo{key: namespace + "/" + name}, activationTriggerPodGroupChange)
}

// activateGangMembers force-activates every live member of the gang. Gang
// progress is plugin-internal state with no cluster event behind it, so each
// transition that can unblock a waiting sibling must be turned into queue
// activity explicitly; the templates_complete trigger is a gang-wide
// transition, the gate requeues the members it read itself (see Permit), a
// reservation release requeues only the pods it kept out (see
// activateReservationBlocked) and a domain veto only the vetoed member (see
// PostFilter).
// Activation bypasses backoff, so callers must fire it on a real transition,
// not on every failed attempt. except lists the members that need no requeue,
// such as the in-flight member observing the transition. The result reports
// whether the activation ran (lister and handle available), so a caller can
// consume a one-shot record only after the requeue happened.
func (g *GangPack) activateGangMembers(gang gangInfo, trigger string, except ...*v1.Pod) bool {
	if g.podLister == nil || g.handle == nil {
		return false
	}
	ns, name, ok := splitGangKey(gang.key)
	if !ok {
		return false
	}
	pods, err := g.podLister.gangPods(ns, name)
	if err != nil {
		return false
	}
	activate := make(map[string]*v1.Pod, len(pods))
	for _, pod := range pods {
		if pod == nil || slices.ContainsFunc(except, func(e *v1.Pod) bool { return samePodIdentity(pod, e) }) {
			continue
		}
		activate[pod.Namespace+"/"+pod.Name] = pod
	}
	g.activate(trigger, activate)
	return true
}

// activate requeues pods under trigger and counts the activation; an empty set
// requeues nothing and counts nothing.
func (g *GangPack) activate(trigger string, pods map[string]*v1.Pod) {
	if len(pods) == 0 {
		return
	}
	gangActivationTotal.WithLabelValues(trigger).Inc()
	g.handle.Activate(klog.Background(), pods)
}

// markTemplatesIncomplete records that a member of the gang was held back
// because the live member set is short of minMember.
func (g *GangPack) markTemplatesIncomplete(gang gangInfo) {
	g.templatesMu.Lock()
	if g.templatesIncomplete == nil {
		g.templatesIncomplete = make(map[string]bool)
	}
	g.templatesIncomplete[gang.key] = true
	g.templatesMu.Unlock()
}

// hasTemplatesIncomplete reports whether the gang has an incomplete-set record.
func (g *GangPack) hasTemplatesIncomplete(gang gangInfo) bool {
	g.templatesMu.Lock()
	defer g.templatesMu.Unlock()
	return g.templatesIncomplete[gang.key]
}

// clearTemplatesIncomplete consumes the gang's incomplete-set record. It reports
// true exactly once per incomplete-to-complete transition, so the caller's
// activation cannot repeat while the set stays complete.
func (g *GangPack) clearTemplatesIncomplete(gang gangInfo) bool {
	g.templatesMu.Lock()
	defer g.templatesMu.Unlock()
	if !g.templatesIncomplete[gang.key] {
		return false
	}
	delete(g.templatesIncomplete, gang.key)
	return true
}

// pruneTemplatesIncomplete drops records for gangs with no live pods, so a gang
// deleted while still incomplete does not retain its entry. Liveness is checked
// per record under the lock: a member marking its gang concurrently waits for
// the lock, so a record for a gang that has just gained a pod is never dropped.
func (g *GangPack) pruneTemplatesIncomplete() {
	if g.podLister == nil {
		return
	}
	g.templatesMu.Lock()
	defer g.templatesMu.Unlock()
	for key := range g.templatesIncomplete {
		ns, name, ok := splitGangKey(key)
		if !ok {
			delete(g.templatesIncomplete, key)
			continue
		}
		if pods, err := g.podLister.gangPods(ns, name); err == nil && len(pods) == 0 {
			delete(g.templatesIncomplete, key)
		}
	}
}
