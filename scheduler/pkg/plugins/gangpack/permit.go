package gangpack

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
)

// sameGang reports whether a pod belongs to the given gang key (namespace/name).
func sameGang(pod *v1.Pod, gangKey string) bool {
	ns, name, ok := podGroupNameOf(pod)
	return ok && ns+"/"+name == gangKey
}

// Permit is the all-or-nothing gang gate. Each gang member waits here instead of
// binding immediately; when the arriving member makes the count reach minMember,
// every waiting sibling is released together and this member proceeds. Members
// wait up to the gang's timeout (its PodGroup ScheduleTimeoutSeconds); on timeout
// the framework rejects the pod and Unreserve unwinds the rest. A pod
// that is not a domain-pinned gang member is not gated.
func (g *GangPack) Permit(_ context.Context, state framework.CycleState, pod *v1.Pod, _ string) (*framework.Status, time.Duration) {
	pin := readPin(state)
	if pin == nil {
		if _, _, member := podGroupNameOf(pod); member {
			return framework.NewStatus(framework.Unschedulable,
				"labeled gang pod reached Permit without validated PreFilter state"), 0
		}
		return framework.NewStatus(framework.Success), 0
	}
	gang := pin.gang
	if gang.timeout <= 0 {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"gang permit timeout is not configured"), 0
	}
	g.rememberAttempt(pod, pin.commitment)
	g.activateGangMembers(gang, activationTriggerPermit, nil)

	// Reserve has already converted this member's claim into assumed occupancy.
	// The commitment reaches zero exactly when minMember slots, including any
	// members adopted as already bound, have arrived. Waiting pods are assumed in
	// the scheduler snapshot, so combining a snapshot count with the waiting set
	// would count them twice and open the gate early.
	if !g.pins.CommittedIf(gang.key, pin.commitment) {
		gangGateTotal.WithLabelValues("wait").Inc()
		return framework.NewStatus(framework.Wait), gang.timeout
	}

	// The gang is complete: release every waiting sibling; this member proceeds.
	g.handle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
		if sameGang(wp.GetPod(), gang.key) && g.attemptFor(wp.GetPod()) == pin.commitment {
			wp.Allow(Name)
		}
	})
	gangGateTotal.WithLabelValues("admit").Inc()
	return framework.NewStatus(framework.Success), 0
}

// Activation triggers, recorded as the gang_activation_total label value.
const (
	// activationTriggerPermit: a member reached Permit. An assumed-but-unbound
	// member produces no cluster event, yet siblings whose Filter depends on it
	// (a worker with required affinity to its leader) must retry now.
	activationTriggerPermit = "permit"
	// activationTriggerTemplatesComplete: the live member set reached
	// minMember. An unscheduled pod's creation is not a queue move event, so a
	// member parked on the incomplete set would otherwise wait for the
	// periodic unschedulable flush.
	activationTriggerTemplatesComplete = "templates_complete"
	// activationTriggerReservationReleased: a forming gang's domain reservation
	// drained or was released. The pods it kept out of the domain, ordinary or
	// gang members, would otherwise wait for the periodic unschedulable flush.
	activationTriggerReservationReleased = "reservation_released"
	// activationTriggerPodGroupChange: the plugin's own PodGroup informer stored
	// a new or edited PodGroup. The scheduler requeues members on a separate
	// PodGroup watch that can run ahead of this informer; a member retried
	// against the older PodGroup parks again with no later event to wake it.
	activationTriggerPodGroupChange = "podgroup_change"
	// activationTriggerDomainFailed: a pinned member was vetoed by another Filter
	// in a domain its gang had not failed before while another domain is untried.
	// The in-flight activation requeues it after its normal backoff to plan there.
	activationTriggerDomainFailed = "domain_failed"
)

// podGroupChanged wakes a gang's live members once the plugin's PodGroup
// informer holds a new or edited PodGroup. PreFilter reads this informer, so a
// retry it triggers sees the change whichever PodGroup watch delivered first.
func (g *GangPack) podGroupChanged(namespace, name string) {
	g.activateGangMembers(gangInfo{key: namespace + "/" + name}, activationTriggerPodGroupChange, nil)
}

// activateGangMembers force-activates every live member of the gang. Gang
// progress is plugin-internal state with no cluster event behind it, so each
// transition that can unblock a parked sibling must be turned into queue
// activity explicitly; the permit and templates_complete triggers are the
// gang-wide transitions, while a reservation release wakes only the pods it
// parked (see activateReservationBlocked) and a domain veto only the vetoed
// member (see PostFilter).
// Activation bypasses backoff, so callers must fire it on a real transition,
// not on every failed attempt. except, when non-nil, is the in-flight member
// observing the transition; it is already being scheduled and needs no wake-up.
// The result reports whether the activation ran (lister and handle available),
// so a caller can consume a one-shot record only after the wake-up happened.
func (g *GangPack) activateGangMembers(gang gangInfo, trigger string, except *v1.Pod) bool {
	lister, ok := g.podLister.(gangMemberLister)
	if !ok || g.handle == nil {
		return false
	}
	ns, name, ok := splitGangKey(gang.key)
	if !ok {
		return false
	}
	pods, err := lister.gangPods(ns, name)
	if err != nil {
		return false
	}
	activate := make(map[string]*v1.Pod, len(pods))
	for _, pod := range pods {
		if pod == nil || samePodIdentity(pod, except) {
			continue
		}
		activate[pod.Namespace+"/"+pod.Name] = pod
	}
	if len(activate) == 0 {
		return true
	}
	gangActivationTotal.WithLabelValues(trigger).Inc()
	g.handle.Activate(klog.Background(), activate)
	return true
}

// markTemplatesIncomplete records that a member of the gang was parked because
// the live member set is short of minMember.
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
	lister, ok := g.podLister.(gangMemberLister)
	if !ok {
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
		if pods, err := lister.gangPods(ns, name); err == nil && len(pods) == 0 {
			delete(g.templatesIncomplete, key)
		}
	}
}
