package gangpack

import (
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
)

// Reserve drains one whole node from the gang's domain reservation: this member
// has now been assigned a real node (PreFilter assigned the domain and reserved
// the gang's full capacity there; Filter kept it in-domain), so it becomes live
// occupancy in the snapshot and its reserved slot is handed to the snapshot via
// Place. That transfer is what stops two gangs racing into one domain — the
// reservation holds the not-yet-placed capacity, and draining it as members
// land avoids double-counting against the snapshot. A pod that is not an
// assigned gang member reserves nothing.
func (g *GangPack) Reserve(_ context.Context, state framework.CycleState, _ *v1.Pod, _ string) *framework.Status {
	if assignment := readAssignment(state); assignment != nil {
		_, drained := g.assignments.Place(assignment.gang.key, assignment.commitment)
		if drained {
			g.clearFailedDomains(assignment.gang)
			g.activateReservationBlocked()
		}
	}
	return nil
}

// Unreserve unwinds a gang when one of its members fails to schedule — a Permit
// timeout, or a bind failure after the gate opened. It does two things:
//
//   - releases the domain assignment, so the gang's retry re-runs best-fit
//     instead of reusing a stale commitment (the old domain may no longer be the best, or
//     may be why the member failed);
//   - rejects every sibling still waiting at the gate, so a half-formed gang
//     unwinds together in one shot rather than each member waiting out its own
//     timeout.
//
// It is idempotent: rejected siblings are themselves unreserved, re-entering here,
// but Release and Reject are both safe to repeat. A pod that is not a gang member
// is left alone.
//
// The gang key comes from the pod's OWN pod-group label, not a PodGroup lookup, so
// the assignment and reservation are still released even if the PodGroup was
// deleted mid-flight (which is when Unreserve tends to fire) — otherwise they
// would leak.
func (g *GangPack) Unreserve(_ context.Context, state framework.CycleState, pod *v1.Pod, _ string) {
	assignment := readAssignment(state)
	if assignment == nil {
		return
	}
	g.releaseAttempt(assignment, pod, false)
}

// releaseAttempt drops the attempt's commitment. It reports whether this call
// released it and, with failedDomain, whether the assigned domain was new to
// the gang's failed-domain memory.
func (g *GangPack) releaseAttempt(assignment *assignmentState, pod *v1.Pod, failedDomain bool) (released, newFailedDomain bool) {
	key := assignment.gang.key
	token := assignment.commitment
	// Count the unwind once per gang: only the first member's Unreserve actually
	// releases the matching attempt. Rejected siblings from an older attempt cannot
	// release or reject a newer retry because the token must still own the
	// assignment.
	released = g.assignments.Release(key, token, func() {
		if failedDomain {
			newFailedDomain = g.markFailedDomain(assignment.gang, placement.Domain{TopologyKey: assignment.topologyKey, Name: assignment.domain})
		}
		if g.handle != nil {
			g.handle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
				if sameGang(wp.GetPod(), key) && g.attemptFor(wp.GetPod()) == token {
					wp.Reject(Name, "gang "+key+" unwound after a member failed to schedule")
				}
			})
		}
	})
	if released {
		gangUnwindTotal.Inc()
		assignedGroups.Set(float64(g.assignments.Len()))
		g.activateReservationBlocked()
	}
	g.forgetAttempt(pod, token)
	return released, newFailedDomain
}

func (g *GangPack) hasWaitingAttempt(key string, token uint64) bool {
	if g.handle == nil {
		return false
	}
	found := false
	g.handle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
		if sameGang(wp.GetPod(), key) && g.attemptFor(wp.GetPod()) == token {
			found = true
		}
	})
	return found
}

// releaseStaleGang removes a name-keyed commitment when its PodGroup is absent.
// The immutable UID check in currentAssignment handles the corresponding
// recreate case.
func (g *GangPack) releaseStaleGang(key string) {
	if g.assignments == nil {
		return
	}
	_, token, _, found := g.assignments.GetOwned(key)
	if !found {
		return
	}
	g.releaseAttempt(&assignmentState{gang: gangInfo{key: key}, commitment: token}, nil, false)
	g.failedMu.Lock()
	for failedKey := range g.failedDomains {
		if strings.HasPrefix(failedKey, key+"\x00") {
			delete(g.failedDomains, failedKey)
		}
	}
	g.failedMu.Unlock()
}

func podAttemptKey(pod *v1.Pod) string {
	if pod == nil {
		return ""
	}
	if pod.UID != "" {
		return string(pod.UID)
	}
	if pod.Name != "" {
		return pod.Namespace + "/" + pod.Name
	}
	// Unit-test pods may omit API identity; the same object is retained by the
	// fake WaitingPod, so pointer identity is sufficient there.
	return fmt.Sprintf("%p", pod)
}

func (g *GangPack) rememberAttempt(pod *v1.Pod, token uint64) {
	key := podAttemptKey(pod)
	if key == "" {
		return
	}
	g.attemptMu.Lock()
	if g.attemptByPod == nil {
		g.attemptByPod = make(map[string]uint64)
	}
	g.attemptByPod[key] = token
	g.attemptMu.Unlock()
}

func (g *GangPack) attemptFor(pod *v1.Pod) uint64 {
	key := podAttemptKey(pod)
	g.attemptMu.RLock()
	token := g.attemptByPod[key]
	g.attemptMu.RUnlock()
	return token
}

func (g *GangPack) forgetAttempt(pod *v1.Pod, token uint64) {
	key := podAttemptKey(pod)
	g.attemptMu.Lock()
	if g.attemptByPod[key] == token {
		delete(g.attemptByPod, key)
	}
	g.attemptMu.Unlock()
}

func (g *GangPack) rememberReservationBlocked(pod *v1.Pod) {
	key := podAttemptKey(pod)
	if key == "" {
		return
	}
	g.blockedMu.Lock()
	if g.reservationBlocked == nil {
		g.reservationBlocked = make(map[string]*v1.Pod)
	}
	g.reservationBlocked[key] = pod
	g.blockedMu.Unlock()
}

func (g *GangPack) forgetReservationBlocked(pod *v1.Pod) {
	g.blockedMu.Lock()
	delete(g.reservationBlocked, podAttemptKey(pod))
	g.blockedMu.Unlock()
}

// activateReservationBlocked requeues every pod a reservation kept out, once. A
// pod that still finds its domain reserved re-registers on its next attempt, so
// each activation corresponds to one reservation draining or being released.
func (g *GangPack) activateReservationBlocked() {
	if g.handle == nil {
		return
	}
	g.blockedMu.Lock()
	pods := g.reservationBlocked
	g.reservationBlocked = make(map[string]*v1.Pod)
	g.blockedMu.Unlock()
	if len(pods) > 0 {
		gangActivationTotal.WithLabelValues(activationTriggerReservationReleased).Inc()
		g.handle.Activate(klog.Background(), pods)
	}
}

// PostBind clears successful-cycle bookkeeping. Failed binding cycles take the
// Unreserve path instead.
func (g *GangPack) PostBind(_ context.Context, state framework.CycleState, pod *v1.Pod, _ string) {
	if assignment := readAssignment(state); assignment != nil {
		g.forgetAttempt(pod, assignment.commitment)
	}
	g.forgetReservationBlocked(pod)
}
