package gangpack

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

func TestReserve_PodWithoutGangState_Succeeds(t *testing.T) {
	g := &GangPack{}
	if s := g.Reserve(context.Background(), nil, &v1.Pod{}, "n1"); !s.IsSuccess() {
		t.Fatalf("Reserve = %v, want Success", s)
	}
}

// TestReserve_EachMemberLands_DrainsOneSlotUntilDomainFrees: reserving a gang member (it just got a real node)
// drains one whole node from its domain reservation, so the capacity another gang
// sees frees up in step — the switch from reserved to placed.
func TestReserve_EachMemberLands_DrainsOneSlotUntilDomainFrees(t *testing.T) {
	g := &GangPack{
		assignments: placement.New(),
		pgReader:    fakeReader{"team/pf": {min: 2, topo: "clique"}},
	}
	// PreFilter would have assigned pf into domain d over 2 free nodes, reserving 2.
	choose(g.assignments, "team/pf", topology.FreeByDomain{"d": 2}, 2)
	_, token, _, _ := g.assignments.GetOwned("team/pf")
	// A second gang can't fit: d is fully reserved (2 raw - 2 reserved = 0).
	if _, ok := choose(g.assignments, "team/other", topology.FreeByDomain{"d": 2}, 1); ok {
		t.Fatal("precondition: d should be fully reserved before Reserve")
	}

	state := newCycleState()
	writeAssignment(state, "d", gangInfo{key: "team/pf", minMember: 2, topologyKey: "clique", timeout: time.Minute}, token)
	// One pf member lands: its heterogeneous claim still owns the domain.
	if s := g.Reserve(context.Background(), state, gangPod("team", "pf"), "node1"); !s.IsSuccess() {
		t.Fatalf("Reserve = %v, want Success", s)
	}
	if _, ok := choose(g.assignments, "team/other", topology.FreeByDomain{"d": 2}, 1); ok {
		t.Fatal("another gang must remain blocked while any reservation is outstanding")
	}
	if s := g.Reserve(context.Background(), state, gangPod("team", "pf"), "node2"); !s.IsSuccess() {
		t.Fatalf("second Reserve = %v, want Success", s)
	}
	if d, ok := choose(g.assignments, "team/other", topology.FreeByDomain{"d": 2}, 1); !ok || d != "d" {
		t.Fatalf("after reservation drains, other Choose = %q,%v want d,true", d, ok)
	}
}

// TestUnreserve_OneMember_ReleasesAssignmentAndRejectsOwnWaitingSiblings: unreserving one member releases the domain assignment and
// rejects the siblings still waiting at the gate (but not pods of other gangs).
func TestUnreserve_OneMember_ReleasesAssignmentAndRejectsOwnWaitingSiblings(t *testing.T) {
	sibling := &fakeWaitingPod{pod: gangPod("team", "pf")}
	other := &fakeWaitingPod{pod: gangPod("team", "decode")}
	h := &fakeHandle{waiting: []framework.WaitingPod{sibling, other}}
	g := &GangPack{
		handle:      h,
		assignments: placement.New(),
		pgReader:    fakeReader{"team/pf": {min: 3, topo: "clique"}},
	}
	g.assignments.Set(placement.Gang{Group: "team/pf"}, "d1", 0, nil)
	_, token, _, _ := g.assignments.GetOwned("team/pf")
	g.rememberAttempt(sibling.pod, token)
	state := newCycleState()
	writeAssignment(state, "d1", gangInfo{key: "team/pf", topologyKey: "clique"}, token)

	g.Unreserve(context.Background(), state, gangPod("team", "pf"), "node1")

	if _, assigned := assignedDomain(g.assignments, "team/pf"); assigned {
		t.Fatal("assignment should be released so the retry re-plans")
	}
	if !sibling.rejected {
		t.Fatal("waiting sibling of the same gang should be rejected")
	}
	if other.rejected {
		t.Fatal("a pod of a different gang must not be rejected")
	}
}

// TestUnreserve_PodWithoutGangLabel_RejectsNobody: a pod with no pod-group label is not a gang member,
// so Unreserve does nothing.
func TestUnreserve_PodWithoutGangLabel_RejectsNobody(t *testing.T) {
	victim := &fakeWaitingPod{pod: gangPod("team", "pf")}
	h := &fakeHandle{waiting: []framework.WaitingPod{victim}}
	g := &GangPack{handle: h, assignments: placement.New(), pgReader: fakeReader{}}

	g.Unreserve(context.Background(), nil, gangPod("team", ""), "node1") // no label

	if victim.rejected {
		t.Fatal("non-gang Unreserve must not reject anyone")
	}
}

// TestUnreserve_PodGroupDeleted_StillReleasesAssignment: if the PodGroup was deleted mid-flight
// (the reader can no longer resolve it), Unreserve must STILL release the assignment and
// reservation — the key comes from the pod's own label, not a PodGroup lookup —
// so nothing leaks.
func TestUnreserve_PodGroupDeleted_StillReleasesAssignment(t *testing.T) {
	g := &GangPack{
		handle:      &fakeHandle{},
		assignments: placement.New(),
		pgReader:    fakeReader{}, // empty: the PodGroup is gone
	}
	g.assignments.Set(placement.Gang{Group: "team/pf"}, "d1", 0, nil) // assigned before the PodGroup vanished
	_, token, _, _ := g.assignments.GetOwned("team/pf")
	state := newCycleState()
	writeAssignment(state, "d1", gangInfo{key: "team/pf", topologyKey: "clique"}, token)

	g.Unreserve(context.Background(), state, gangPod("team", "pf"), "node1")

	if _, assigned := assignedDomain(g.assignments, "team/pf"); assigned {
		t.Fatal("assignment must be released even though the PodGroup is gone (else it leaks)")
	}
}

func TestUnreserve_FromOlderAttempt_LeavesRetryAssignmentAndWaiter(t *testing.T) {
	assignments := placement.New()
	_, oldToken, ok := assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey},
		topology.FreeByDomain{"a": 1}, map[string][]string{"a": {"a1"}}, 1)
	if !ok || !assignments.Release("team/pf", oldToken, nil) {
		t.Fatal("failed to establish and release old attempt")
	}
	_, newToken, ok := assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey},
		topology.FreeByDomain{"b": 1}, map[string][]string{"b": {"b1"}}, 1)
	if !ok {
		t.Fatal("failed to establish retry")
	}
	retryWaiter := &fakeWaitingPod{pod: gangPod("team", "pf")}
	h := &fakeHandle{waiting: []framework.WaitingPod{retryWaiter}}
	g := &GangPack{handle: h, assignments: assignments}
	g.rememberAttempt(retryWaiter.pod, newToken)
	oldState := newCycleState()
	writeAssignment(oldState, "a", gangInfo{key: "team/pf", topologyKey: testKey}, oldToken)

	g.Unreserve(context.Background(), oldState, gangPod("team", "pf"), "a1")

	if domain, _, _, assigned := assignments.GetOwned("team/pf"); !assigned || domain.Name != "b" {
		t.Fatalf("retry assignment = %+v,%v, want b,true", domain, assigned)
	}
	if retryWaiter.rejected {
		t.Fatal("stale Unreserve rejected a waiter owned by the retry")
	}
}

func TestReserve_ReservationDrains_ActivatesStandalonePodItBlocked(t *testing.T) {
	assignments := placement.New()
	_, token, ok := assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey},
		topology.FreeByDomain{"a": 1}, map[string][]string{"a": {"a1"}}, 1)
	if !ok {
		t.Fatal("failed to reserve domain")
	}
	h := &fakeHandle{}
	g := &GangPack{handle: h, assignments: assignments}
	standalone := gpuPod("4")
	standalone.Namespace, standalone.Name = "team", "standalone"
	standalone.UID = "standalone-uid"
	if status := g.Filter(context.Background(), newCycleState(), standalone,
		nodeInfo(gpuNode("a1", "a", "4"))); status.Code() != framework.UnschedulableAndUnresolvable {
		t.Fatalf("Filter = %v, want reservation rejection", status)
	}
	state := newCycleState()
	writeAssignment(state, "a", gangInfo{key: "team/pf", topologyKey: testKey}, token)
	g.Reserve(context.Background(), state, gangPod("team", "pf"), "a1")
	if h.activated[string(standalone.UID)] != standalone {
		t.Fatalf("activated = %v, want blocked standalone pod", h.activated)
	}
}

func TestPostBind_MemberBound_ClearsAttemptKeepsAssignment(t *testing.T) {
	assignments := placement.New()
	_, token, ok := assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey},
		topology.FreeByDomain{"a": 2}, map[string][]string{"a": {"a1", "a2"}}, 2)
	if !ok {
		t.Fatal("failed to establish gang assignment")
	}
	pod := gangPod("team", "pf")
	pod.Name, pod.UID = "member-0", "member-0-uid"
	g := &GangPack{assignments: assignments}
	g.rememberAttempt(pod, token)
	state := newCycleState()
	writeAssignment(state, "a", gangInfo{key: "team/pf", topologyKey: testKey}, token)

	g.PostBind(context.Background(), state, pod, "a1")

	if domain, currentToken, _, assigned := assignments.GetOwned("team/pf"); !assigned || domain.Name != "a" || currentToken != token {
		t.Fatalf("PostBind changed gang assignment: domain=%+v token=%d assigned=%v", domain, currentToken, assigned)
	}
	if got := g.attemptFor(pod); got != 0 {
		t.Fatalf("PostBind left pod attempt token %d", got)
	}
}
