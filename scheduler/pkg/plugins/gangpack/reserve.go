package gangpack

import (
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// Reserve commits a deferred hard-spread plan to the selected node's domain, if
// PreFilter did not pin it already, then drains one whole node from the gang's
// reservation. The member becomes live occupancy in the snapshot and its
// reserved slot is handed off via Place. That hand-off is what stops two gangs
// racing into one domain — the reservation holds the not-yet-placed capacity,
// and draining it as members land avoids double-counting against the snapshot. A
// pod that is not a pinned or deferred gang member reserves nothing.
func (g *GangPack) Reserve(_ context.Context, state framework.CycleState, _ *v1.Pod, nodeName string) *framework.Status {
	pin := readPin(state)
	if pin == nil {
		plan := readDeferredPin(state)
		if plan == nil {
			return nil
		}
		domain := ""
		for candidateDomain, names := range plan.nodesByDomain {
			for _, name := range names {
				if name == nodeName {
					domain = candidateDomain
					break
				}
			}
			if domain != "" {
				break
			}
		}
		if domain == "" || plan.free[domain] < plan.need {
			return framework.NewStatus(framework.Error, "selected node is outside the deferred gang plan")
		}
		selectedFree := topology.FreeByDomain{domain: plan.free[domain]}
		selectedNodes := map[string][]string{domain: plan.nodesByDomain[domain]}
		chosen, commitment, ok := g.pins.ChooseForOwnerInTopologyOnNodes(plan.gang.key, plan.gang.uid,
			plan.gang.topologyKey, selectedFree, selectedNodes, plan.need)
		if !ok || chosen != domain {
			return framework.NewStatus(framework.Error, "selected gang domain became unavailable before Reserve")
		}
		writePin(state, domain, plan.gang, commitment)
		pin = readPin(state)
		gangPinTotal.WithLabelValues("pinned_after_filter").Inc()
		pinnedGroups.Set(float64(g.pins.Len()))
	}

	_, drained := g.pins.PlaceIf(pin.gang.key, pin.commitment)
	if drained {
		g.clearFailedDomains(pin.gang)
		g.activateReservationBlocked()
	}
	return nil
}

// Unreserve unwinds a gang when one of its members fails to schedule — a Permit
// timeout, or a bind failure after the gate opened. It does two things:
//
//   - releases the domain pin, so the gang's retry re-runs best-fit instead of
//     reusing a stale commitment (the old domain may no longer be the best, or
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
// the pin and reservation are still released even if the PodGroup was deleted
// mid-flight (which is when Unreserve tends to fire) — otherwise they would leak.
func (g *GangPack) Unreserve(_ context.Context, state framework.CycleState, pod *v1.Pod, _ string) {
	pin := readPin(state)
	if pin == nil {
		return
	}
	g.releaseAttempt(pin, pod, false)
}

func (g *GangPack) releaseAttempt(pin *pinState, pod *v1.Pod, failedDomain bool) bool {
	key := pin.gang.key
	token := pin.commitment
	// Count the unwind once per gang: only the first member's Unreserve actually
	// releases the matching attempt. Rejected siblings from an older attempt cannot
	// release or reject a newer retry because the token must still own the pin.
	released := g.pins.ReleaseIf(key, token, func() {
		if failedDomain {
			g.markFailedDomain(pin.gang, placement.Domain{TopologyKey: pin.topologyKey, Name: pin.domain})
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
		pinnedGroups.Set(float64(g.pins.Len()))
		g.activateReservationBlocked()
	}
	g.forgetAttempt(pod, token)
	return released
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
// The immutable UID check in pinGang handles the corresponding recreate case.
func (g *GangPack) releaseStaleGang(key string) {
	if g.pins == nil {
		return
	}
	_, token, _, found := g.pins.GetOwned(key)
	if !found {
		return
	}
	g.releaseAttempt(&pinState{gang: gangInfo{key: key}, commitment: token}, nil, false)
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

func (g *GangPack) activateReservationBlocked() {
	if g.handle == nil {
		return
	}
	g.blockedMu.Lock()
	pods := g.reservationBlocked
	g.reservationBlocked = make(map[string]*v1.Pod)
	g.blockedMu.Unlock()
	if len(pods) > 0 {
		g.handle.Activate(klog.Background(), pods)
	}
}

// PostBind clears successful-cycle bookkeeping. Failed binding cycles take the
// Unreserve path instead.
func (g *GangPack) PostBind(_ context.Context, state framework.CycleState, pod *v1.Pod, _ string) {
	if pin := readPin(state); pin != nil {
		g.forgetAttempt(pod, pin.commitment)
	}
	g.forgetReservationBlocked(pod)
}
