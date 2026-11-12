package reading

import (
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Context is what a reading needs besides the pods: the pass's clock, the
// two windows the Component configures, the rotation set of the routed
// Service and the pairing picture of the peer Component.
type Context struct {
	// Now is the pass's clock reading; every age is measured against it.
	Now time.Time
	// AvailabilityWindow is how long a pod must hold PodReady before it is
	// Available (the Component's minReadySeconds). Zero makes Available the
	// same as Serving.
	AvailabilityWindow time.Duration
	// UnreadyGrace is how long a pod may fail its readiness before it is out
	// of service (the Component's stuck-pod grace). Zero means no pod is
	// ever read as unready past the grace.
	UnreadyGrace time.Duration
	// Rotation is the set of pod names the routed Service publishes for new
	// traffic. Nil means no pod is Routed.
	Rotation Rotation
	// Pairing is the peer Component's serving picture for the Pairable level.
	Pairing Pairing
}

// Rotation is the set of pod names a routed Service currently sends new
// traffic to.
type Rotation map[string]struct{}

// publishes reports whether the routed Service sends new traffic to the
// named pod.
func (r Rotation) publishes(podName string) bool {
	_, ok := r[podName]
	return ok
}

// Pairing is what the Pairable level needs from the peer Component.
type Pairing struct {
	// TransitionInFlight reports that a pairing-protocol change is under
	// way, so a serving pod must have a peer of a cohort it pairs with.
	TransitionInFlight bool
	// PeerServingByProtocol counts the peer Component's serving pods by
	// their pairing-protocol label; the empty key is the cohort that pairs
	// with anything.
	PeerServingByProtocol map[string]int32
}

// PodReading is one pod read at every level. The levels build on Live: a
// pod that is being deleted or is terminal reads false at every other
// level, whatever its conditions still say.
type PodReading struct {
	// Name is the pod's name.
	Name string
	// Protocol is the pod's pairing-protocol label; empty pairs with anything.
	Protocol string
	// RevisionHash is the pod's revision-hash label, naming its routed Service.
	RevisionHash string
	// Runner is the pod's runner label (default, leader or worker).
	Runner string
	// Worker reports a gang worker: never a member of the routed Service.
	Worker bool
	// Terminal reports phase Failed or Succeeded: the kubelet runs nothing in
	// the pod again, and it occupies its name until it is deleted.
	Terminal bool
	// Terminating reports a deletion timestamp: the pod is on its way out.
	Terminating bool
	// SurgeDraining reports the serving gate held by the surge drain: the
	// source of a surge, out of rotation ahead of its deletion.
	SurgeDraining bool

	// Live: the pod exists, is not being deleted and is not terminal.
	Live bool
	// RuntimeReady: ContainersReady is True, so every container passes its
	// readiness probe.
	RuntimeReady bool
	// Serving: PodReady and the serving gate are both True, so the pod is
	// eligible for its Service.
	Serving bool
	// Routed: the pod is routable (not a worker, and it carries a revision
	// hash) and the routed Service publishes it for new traffic. It is what
	// the data plane sees and can lag Serving either way.
	Routed bool
	// Available: Serving, and PodReady held for the availability window.
	Available bool
	// Pairable: Serving, and able to pair with a serving pod of the peer
	// Component while a pairing-protocol transition is in flight.
	Pairable bool

	// HeldIn names the terminal kubelet waiting reason a container of the
	// pod is held in; empty when none is.
	HeldIn string
	// UnreadyPastGrace: the pod served, then failed its readiness for at
	// least the unready grace.
	UnreadyPastGrace bool
	// ContainersUnreadyPastGrace: ContainersReady has been False for at
	// least the unready grace, whatever the serving gate reads.
	ContainersUnreadyPastGrace bool
}

// OutOfService reports whether the pod provably serves nothing and will not
// on its own: held in a terminal kubelet waiting reason, or served and
// then unready past the grace. A pod still coming up reads false.
func (p PodReading) OutOfService() bool {
	return p.HeldIn != "" || p.UnreadyPastGrace
}

// Counts is how many pods of the Instance reach each level. Pods counts
// every pod handed to Read, dead or alive.
type Counts struct {
	Pods         int32
	Live         int32
	RuntimeReady int32
	Serving      int32
	Routed       int32
	Available    int32
	Pairable     int32
}

func (c *Counts) add(p PodReading) {
	c.Pods++
	for _, level := range []struct {
		holds bool
		n     *int32
	}{
		{p.Live, &c.Live},
		{p.RuntimeReady, &c.RuntimeReady},
		{p.Serving, &c.Serving},
		{p.Routed, &c.Routed},
		{p.Available, &c.Available},
		{p.Pairable, &c.Pairable},
	} {
		if level.holds {
			*level.n++
		}
	}
}

// InstanceReading is one Instance's pods read once, with the counts per
// level and the verdicts the passes act on.
//
// Every verdict reads the pod set by one membership rule. A pod being
// deleted is not a member: its conditions outlive its place in any
// Service. A terminal pod (Failed or Succeeded) is a dead member: it never
// serves and never counts toward the desired count, so a verdict that
// needs a healthy set reads false on it, and a verdict that asks whether
// anything serves reads it as a member that serves nothing. Workers are
// never routed, so the rotation verdicts read the routable members alone.
type InstanceReading struct {
	// Desired is the pod count the Instance is expected to run.
	Desired int32
	// Pods are the readings, in the order the pods were handed to Read.
	Pods []PodReading
	// Counts are the per-level counts over Pods.
	Counts Counts
}

// Read reads one Instance's pods against ctx. A nil pod is skipped.
func Read(pods []*corev1.Pod, desired int32, ctx Context) InstanceReading {
	r := InstanceReading{Desired: desired, Pods: make([]PodReading, 0, len(pods))}
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		p := ReadPod(pod, ctx)
		r.Pods = append(r.Pods, p)
		r.Counts.add(p)
	}
	return r
}

// ReadPod reads one pod against ctx.
func ReadPod(pod *corev1.Pod, ctx Context) PodReading {
	p := PodReading{
		Name:          pod.Name,
		Protocol:      pod.Labels[query.LabelPairingProtocol],
		RevisionHash:  pod.Labels[query.LabelRevisionHash],
		Runner:        pod.Labels[query.LabelRunner],
		Worker:        !query.RoutedRunner(pod),
		Terminal:      query.IsTerminalPod(pod),
		Terminating:   pod.DeletionTimestamp != nil,
		SurgeDraining: podreadiness.HeldNotServingBy(pod, podreadiness.WriterUpdateSurgeDrain),
	}
	p.Live = !p.Terminal && !p.Terminating
	if !p.Live {
		return p
	}
	p.RuntimeReady = podreadiness.IsContainersReady(pod)
	p.Serving = podreadiness.ReadyAndServing(pod)
	p.Routed = ctx.Rotation.publishes(pod.Name) && !p.Worker && p.RevisionHash != ""
	p.Available = p.Serving && heldReadyForWindow(pod, ctx.AvailabilityWindow, ctx.Now)
	p.Pairable = p.Serving && (!ctx.Pairing.TransitionInFlight || p.Protocol == "" ||
		pairsWithPeer(ctx.Pairing.PeerServingByProtocol, p.Protocol))
	p.HeldIn = heldIn(pod)
	p.UnreadyPastGrace = unreadyPastGrace(pod, ctx.Now, ctx.UnreadyGrace)
	p.ContainersUnreadyPastGrace = containersUnreadyPastGrace(pod, ctx.Now, ctx.UnreadyGrace)
	return p
}

// FullyServing reports whether the Instance is fully in rotation: at least
// the desired number of live pods, every one of them Serving. A terminal
// pod makes it false. A source the surge drain already holds out of
// rotation ahead of deleting it is on its way out and is left out, like a
// pod being deleted. A desired count of zero expects nothing and never
// reads as serving.
func (r InstanceReading) FullyServing() bool {
	if r.Desired <= 0 {
		return false
	}
	staying := r.staying()
	return allPods(staying, func(p PodReading) bool { return !p.Terminal && p.Serving }) &&
		int32(len(staying)) >= r.Desired
}

// ServesNothing reports whether a non-empty Instance provably serves
// nothing: no live routable pod is Serving, and every live routable pod is
// out of service (OutOfService). A pod still coming up, or unready inside
// the grace, may yet serve and reads false. Workers are ignored; a terminal
// pod is a member that serves nothing.
func (r InstanceReading) ServesNothing() bool {
	m := r.members()
	return m.count > 0 && allPods(m.routable, func(p PodReading) bool { return !p.Serving && p.OutOfService() })
}

// OutOfRotation reports whether a non-empty Instance has no live routable
// pod Serving right now, whatever the reason and for however long. Workers
// are ignored; a terminal pod is a member that serves nothing.
func (r InstanceReading) OutOfRotation() bool {
	m := r.members()
	return m.count > 0 && allPods(m.routable, func(p PodReading) bool { return !p.Serving })
}

// ContainersUnreadyPastGrace reports whether the Instance runs but fails
// its readiness: every live routable pod has had ContainersReady False for
// at least the grace, whatever its serving gate reads, so a set a pass has
// already drained still reads unready past the grace. There must be at least one such pod;
// workers are ignored and a terminal pod is neither unready past the grace
// nor serving.
func (r InstanceReading) ContainersUnreadyPastGrace() bool {
	m := r.members()
	return m.count > 0 && len(m.routable) > 0 &&
		allPods(m.routable, func(p PodReading) bool { return p.ContainersUnreadyPastGrace })
}

// UnavailableOnHeldMember reports whether the Instance is short of its
// desired Serving count because a member is held out of service: fewer
// live pods are Serving than desired, and a live pod that is not Serving
// is OutOfService. A member merely coming up, or unready inside the
// grace, is not held.
func (r InstanceReading) UnavailableOnHeldMember() bool {
	held := slices.ContainsFunc(r.Pods, func(p PodReading) bool { return p.Live && !p.Serving && p.OutOfService() })
	return held && r.Counts.Serving < r.Desired
}

// membership is the one membership rule the rotation readings share: every
// pod not being deleted is a member, and the members that are neither
// terminal nor workers are the routable ones, the only pods that can
// serve.
type membership struct {
	count    int
	routable []PodReading
}

// members reads the Instance's pods by the membership rule.
func (r InstanceReading) members() membership {
	var m membership
	for _, p := range r.Pods {
		if p.Terminating {
			continue
		}
		m.count++
		if !p.Terminal && !p.Worker {
			m.routable = append(m.routable, p)
		}
	}
	return m
}

// staying are the pods that are neither being deleted nor held out of
// rotation by the surge drain ahead of their deletion: the pods a full
// rotation is read over.
func (r InstanceReading) staying() []PodReading {
	var out []PodReading
	for _, p := range r.Pods {
		if !p.Terminating && !p.SurgeDraining {
			out = append(out, p)
		}
	}
	return out
}

// allPods reports whether every pod holds the predicate; true of none.
func allPods(pods []PodReading, holds func(PodReading) bool) bool {
	return !slices.ContainsFunc(pods, func(p PodReading) bool { return !holds(p) })
}

// PairableServing is how many pods are Serving and Pairable.
func (r InstanceReading) PairableServing() int32 {
	return r.Counts.Pairable
}

// pairsWithPeer reports whether a pod of cohort proto can pair with some
// serving pod of the peer: equal protocols, or either side empty.
func pairsWithPeer(peerServing map[string]int32, proto string) bool {
	for p, n := range peerServing {
		if n > 0 && (p == "" || proto == "" || p == proto) {
			return true
		}
	}
	return false
}

// heldReadyForWindow reports whether the pod's PodReady condition has been
// True for at least window (the Deployment minReadySeconds rule). A window
// of zero or less asks nothing. A True condition with no transition time
// cannot prove its age and reads false.
func heldReadyForWindow(pod *corev1.Pod, window time.Duration, now time.Time) bool {
	if window <= 0 {
		return true
	}
	since, known := conditionSince(pod, corev1.PodReady, corev1.ConditionTrue)
	return known && !now.Before(since.Add(window))
}

// heldIn names the terminal kubelet waiting reason a container of the
// pod, regular or init, is held in (types.IsTerminalWaiting); empty when
// none is.
func heldIn(pod *corev1.Pod) string {
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if types.IsTerminalWaiting(cs.State.Waiting) {
				return cs.State.Waiting.Reason
			}
		}
	}
	return ""
}

// unreadyPastGrace reports whether a Running pod served and then failed its
// readiness for at least grace: the serving gate written at promotion is
// still True while PodReady is False and ContainersReady has been False
// since a known time. A withdrawn PodReady alone is the node-loss shape,
// not this one. No grace means no such window.
func unreadyPastGrace(pod *corev1.Pod, now time.Time, grace time.Duration) bool {
	if grace <= 0 || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if !podreadiness.IsServing(pod) || podreadiness.IsPodReady(pod) {
		return false
	}
	return containersUnreadySince(pod, now, grace)
}

// containersUnreadyPastGrace reports whether a Running pod's ContainersReady
// has been False for at least grace, whatever the serving gate reads. No
// grace means no such window.
func containersUnreadyPastGrace(pod *corev1.Pod, now time.Time, grace time.Duration) bool {
	if grace <= 0 || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	return containersUnreadySince(pod, now, grace)
}

// containersUnreadySince reports whether ContainersReady is False with a
// transition time at least grace ago.
func containersUnreadySince(pod *corev1.Pod, now time.Time, grace time.Duration) bool {
	since, known := conditionSince(pod, corev1.ContainersReady, corev1.ConditionFalse)
	return known && now.Sub(since) >= grace
}

// conditionSince is when the pod's condition of the given type last moved
// to status. known is false when the pod has no such condition, it reads
// another status, or its transition time is unset, so its age cannot be
// proven.
func conditionSince(pod *corev1.Pod, t corev1.PodConditionType, status corev1.ConditionStatus) (since time.Time, known bool) {
	i := slices.IndexFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool { return c.Type == t })
	if i < 0 || pod.Status.Conditions[i].Status != status || pod.Status.Conditions[i].LastTransitionTime.IsZero() {
		return time.Time{}, false
	}
	return pod.Status.Conditions[i].LastTransitionTime.Time, true
}
