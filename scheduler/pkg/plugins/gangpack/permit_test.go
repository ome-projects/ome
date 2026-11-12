package gangpack

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

func TestPermit_CommittedGang_AdmitsWithoutSnapshotRead(t *testing.T) {
	assignments := placement.New()
	id := assignments.Set(placement.Gang{Group: "team/pf"}, "a", 0, nil)
	member := namedMember("member-3")
	g := &GangPack{
		handle:      &fakeHandle{},
		assignments: assignments,
		pgReader:    fakeReader{"team/pf": {min: 3, topo: testKey}},
		podLister:   fakeGangPodLister{pods: []*v1.Pod{member}},
	}

	state := newCycleState()
	writeAssignment(state, "a", gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey, timeout: time.Minute}, id)
	status, _ := g.Permit(context.Background(), state, member, "a3")
	if status.IsWait() {
		t.Fatal("gate never admits: committed gang with 2 bound siblings + arriving member = 3 should admit, not wait")
	}
	if !status.IsSuccess() {
		t.Fatalf("Permit = %v, want Success", status)
	}
}

func TestPermit_WaitingSiblingAlsoAssumed_CountsItOnce(t *testing.T) {
	assignments := placement.New()
	_, token, ok := assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey},
		topology.FreeByDomain{"a": 4}, map[string][]string{"a": {"a1", "a2", "a3", "a4"}}, 4)
	if !ok {
		t.Fatal("failed to reserve gang")
	}
	// Two members have reached Reserve. Both are already assumed in the snapshot,
	// and one is also in the waiting set; only two of four members have arrived.
	assignments.Place("team/pf", token)
	assignments.Place("team/pf", token)
	waiterPod, current := namedMember("waiter"), namedMember("current")
	waiter := &fakeWaitingPod{pod: waiterPod}
	g := &GangPack{assignments: assignments, podLister: fakeGangPodLister{pods: []*v1.Pod{waiterPod, current}}, handle: &fakeHandle{
		waiting: []framework.WaitingPod{waiter},
		snapshot: []framework.NodeInfo{
			nodeInfo(gpuNode("a1", "a", "4"), waiterPod),
			nodeInfo(gpuNode("a2", "a", "4"), current),
		},
	}}
	g.rememberAttempt(waiterPod, token)
	state := newCycleState()
	writeAssignment(state, "a", gangInfo{key: "team/pf", minMember: 4, topologyKey: testKey, timeout: time.Minute}, token)
	status, _ := g.Permit(context.Background(), state, current, "a2")
	if !status.IsWait() {
		t.Fatalf("Permit = %v, want Wait with only two committed arrivals", status)
	}
	if waiter.allowed {
		t.Fatal("waiting sibling was released before the commitment reached minMember")
	}
}

// TestPermit_FormingGangWithBoundSiblings_WaitsWithoutScanningBound: a still-forming gang (remaining>0)
// holds until its reservation drains, regardless of snapshot contents.
func TestPermit_FormingGangWithBoundSiblings_WaitsWithoutScanningBound(t *testing.T) {
	assignments := placement.New()
	_, id, _ := assignments.Choose(placement.Gang{Group: "team/pf"}, topology.FreeByDomain{"a": 5}, nil, 3) // assigned, remaining==3, not committed
	// A snapshot that WOULD satisfy the gate if (wrongly) counted.
	snapshot := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gangPod("team", "pf")),
		nodeInfo(gpuNode("a2", "a", "4"), gangPod("team", "pf")),
	}
	member := namedMember("member-3")
	g := &GangPack{
		handle:      &fakeHandle{snapshot: snapshot},
		assignments: assignments,
		pgReader:    fakeReader{"team/pf": {min: 3, topo: testKey}},
		podLister:   fakeGangPodLister{pods: []*v1.Pod{member}},
	}
	state := newCycleState()
	writeAssignment(state, "a", gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey, timeout: time.Minute}, id)
	status, _ := g.Permit(context.Background(), state, member, "a3")
	if !status.IsWait() {
		t.Fatalf("forming gang (remaining>0) must wait, not count bound siblings; got %v", status)
	}
}

// TestGateDecision: the gang releases when the members already waiting plus the
// arriving one reach minMember — not before.
func TestSameGang_MatchingOrOtherNamespaceGroupOrNoLabel_MatchesOnlySameKey(t *testing.T) {
	if !sameGang(gangPod("team", "pf"), "team/pf") {
		t.Fatal("pod in team/pf should match gang key team/pf")
	}
	if sameGang(gangPod("team", "pf"), "team/decode") {
		t.Fatal("pod in team/pf must not match a different gang")
	}
	if sameGang(gangPod("other", "pf"), "team/pf") {
		t.Fatal("namespace must be part of the gang key")
	}
	if sameGang(gangPod("team", ""), "team/pf") {
		t.Fatal("non-gang pod matches nothing")
	}
}

func TestPermit_LabelledPodWithoutCycleAssignment_IsUnschedulable(t *testing.T) {
	g := &GangPack{}
	status, _ := g.Permit(context.Background(), newCycleState(), gangPod("team", "pf"), "n")
	if status.Code() != framework.Unschedulable {
		t.Fatalf("Permit = %v, want Unschedulable", status)
	}
}

func TestPermit_WaiterFromOlderAttempt_DoesNotCountOrRelease(t *testing.T) {
	assignments := placement.New()
	_, oldToken, ok := assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey},
		topology.FreeByDomain{"a": 2}, map[string][]string{"a": {"a1", "a2"}}, 2)
	if !ok || !assignments.Release("team/pf", oldToken, nil) {
		t.Fatal("failed to establish old attempt")
	}
	_, newToken, ok := assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey},
		topology.FreeByDomain{"b": 2}, map[string][]string{"b": {"b1", "b2"}}, 2)
	if !ok {
		t.Fatal("failed to establish retry")
	}
	oldWaiter := &fakeWaitingPod{pod: namedMember("old-waiter")}
	current := namedMember("current")
	h := &fakeHandle{waiting: []framework.WaitingPod{oldWaiter}}
	g := &GangPack{handle: h, assignments: assignments, podLister: fakeGangPodLister{pods: []*v1.Pod{oldWaiter.pod, current}}}
	g.rememberAttempt(oldWaiter.pod, oldToken)
	state := newCycleState()
	writeAssignment(state, "b", gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey, timeout: time.Minute}, newToken)

	status, _ := g.Permit(context.Background(), state, current, "b1")
	if !status.IsWait() {
		t.Fatalf("Permit = %v, want Wait; old-attempt waiter must not complete retry", status)
	}
	if oldWaiter.allowed {
		t.Fatal("old-attempt waiter was allowed by retry")
	}
}

// TestPermit_GangComplete_ReleasesTheSiblingsTheInformerLists: the member that
// completes the gang releases the siblings the informer lists for the gang and
// finds waiting at the gate under their UIDs: a sibling recreated under a new
// UID is listed under it, and a sibling that joined the gang after its
// reservation is listed too. A waiting entry of a pod the informer no longer
// lists is a deleted pod, which leaves the gate on its deletion (see Permit), so
// it is left alone, as is a waiter of another gang. Nobody is requeued: every
// listed member is in flight or at the gate.
func TestPermit_GangComplete_ReleasesTheSiblingsTheInformerLists(t *testing.T) {
	assignments := placement.New()
	_, token, ok := assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey},
		topology.FreeByDomain{"a": 3}, map[string][]string{"a": {"a1", "a2", "a3"}}, 3)
	if !ok {
		t.Fatal("failed to reserve gang")
	}
	for range 3 {
		assignments.Place("team/pf", token)
	}
	leader, recreated, joined := namedMember("leader"), namedMember("worker-1"), namedMember("worker-2")
	deleted := namedMember("worker-1")
	deleted.UID = "worker-1-before-recreation"
	otherGang := namedMember("other-0")
	otherGang.Labels[podGroupLabel] = "other"
	recreatedWait, joinedWait := &fakeWaitingPod{pod: recreated}, &fakeWaitingPod{pod: joined}
	deletedWait, otherWait := &fakeWaitingPod{pod: deleted}, &fakeWaitingPod{pod: otherGang}
	h := &fakeHandle{waiting: []framework.WaitingPod{recreatedWait, joinedWait, deletedWait, otherWait}}
	g := &GangPack{handle: h, assignments: assignments,
		podLister: fakeGangPodLister{pods: []*v1.Pod{leader, recreated, joined, otherGang}}}
	for _, pod := range []*v1.Pod{recreated, joined, deleted, otherGang} {
		g.rememberAttempt(pod, token)
	}
	state := newCycleState()
	writeAssignment(state, "a", gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey, timeout: time.Minute}, token)

	status, _ := g.Permit(context.Background(), state, leader, "a1")
	if !status.IsSuccess() {
		t.Fatalf("Permit of the completing member = %v, want Success", status)
	}
	if !recreatedWait.allowed || !joinedWait.allowed {
		t.Fatalf("released: recreated %v, joined %v; want both siblings the informer lists released", recreatedWait.allowed, joinedWait.allowed)
	}
	if deletedWait.allowed {
		t.Fatal("a waiting entry the informer no longer lists was released")
	}
	if otherWait.allowed {
		t.Fatal("a waiter of another gang was released")
	}
	if h.activateCalls != 0 {
		t.Fatalf("activate calls = %d, want 0 with every listed member in flight or at the gate", h.activateCalls)
	}
}

// TestPermit_GangPodsUnreadable_FailsTheCycle: the gate tells a gang's waiting
// siblings from the members to requeue by the informer's list of its pods. A
// gate that cannot read the list fails the member's cycle with the error, so
// Unreserve unwinds the attempt and the gang retries, instead of the gate
// holding the member with its siblings unread.
func TestPermit_GangPodsUnreadable_FailsTheCycle(t *testing.T) {
	assignments := placement.New()
	_, token, ok := assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey},
		topology.FreeByDomain{"a": 2}, map[string][]string{"a": {"a1", "a2"}}, 2)
	if !ok {
		t.Fatal("failed to reserve gang")
	}
	g := &GangPack{handle: &fakeHandle{}, assignments: assignments,
		podLister: fakeGangPodLister{err: errors.New("pod-group index is missing")}}
	state := newCycleState()
	writeAssignment(state, "a", gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey, timeout: time.Minute}, token)

	status, _ := g.Permit(context.Background(), state, namedMember("leader"), "a1")
	if status.Code() != framework.Error {
		t.Fatalf("Permit = %v, want an Error status carrying the lister's error", status)
	}
}
