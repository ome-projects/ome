package gangpack

import (
	"context"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

func TestPostFilter_EveryCandidateFailedBeforeReserve_ReleasesAndRejectsSiblings(t *testing.T) {
	sibling := &fakeWaitingPod{pod: gangPod("team", "pf")}
	g := &GangPack{handle: &fakeHandle{waiting: []framework.WaitingPod{sibling}}, assignments: placement.New()}
	_, id, _ := g.assignments.Choose(placement.Gang{Group: "team/pf", TopologyKey: testKey}, topology.FreeByDomain{"a": 2}, nil, 2)
	g.rememberAttempt(sibling.pod, id)
	state := newCycleState()
	writeAssignment(state, "a", gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey, timeout: time.Minute}, id)

	_, status := g.PostFilter(context.Background(), state, gangPod("team", "pf"), nil)
	if status.Code() != framework.Unschedulable {
		t.Fatalf("PostFilter = %v, want Unschedulable", status)
	}
	if _, assigned := assignedDomain(g.assignments, "team/pf"); assigned {
		t.Fatal("assignment leaked after every candidate failed before Reserve")
	}
	if !sibling.rejected {
		t.Fatal("waiting sibling must unwind with the failed member")
	}
}

func TestPostFilter_DomainFailed_NextAssignmentSkipsIt(t *testing.T) {
	g := &GangPack{handle: &fakeHandle{}, assignments: placement.New()}
	gang := gangInfo{key: "team/pf", uid: "uid-1", minMember: 1, topologyKey: testKey, timeout: time.Minute}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")),
	}
	first := newCycleState()
	_, status := g.assignGangDomain(first, nodes, gang, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() || readAssignment(first).domain != "a" {
		t.Fatalf("first assignment = %+v, %v, want domain a", readAssignment(first), status)
	}
	g.PostFilter(context.Background(), first, gangPod("team", "pf"), nil)

	retry := newCycleState()
	_, status = g.assignGangDomain(retry, nodes, gang, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() || readAssignment(retry).domain != "b" {
		t.Fatalf("retry assignment = %+v, %v, want domain b after a failed", readAssignment(retry), status)
	}
}

func TestPostFilter_PartialReserveThenSecondFailure_KeepsBothDomainsFailed(t *testing.T) {
	g := &GangPack{handle: &fakeHandle{}, assignments: placement.New()}
	gang := gangInfo{key: "team/pf", uid: "uid-1", minMember: 2, topologyKey: testKey, timeout: time.Minute}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")), nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")), nodeInfo(gpuNode("b2", "b", "4")),
	}
	first := newCycleState()
	g.assignGangDomain(first, nodes, gang, gangGPUPod("team", "pf", "4"))
	if readAssignment(first).domain != "a" {
		t.Fatalf("first domain = %q, want a", readAssignment(first).domain)
	}
	g.PostFilter(context.Background(), first, gangPod("team", "pf"), nil)

	second := newCycleState()
	g.assignGangDomain(second, nodes, gang, gangGPUPod("team", "pf", "4"))
	if readAssignment(second).domain != "b" {
		t.Fatalf("second domain = %q, want b", readAssignment(second).domain)
	}
	g.Reserve(context.Background(), second, gangPod("team", "pf"), "b1")
	filtered, _ := g.withoutFailedDomains(gang, topology.FreeByDomain{"a": 2, "b": 2})
	if _, present := filtered["a"]; present {
		t.Fatal("partial Reserve cleared failed domain a before the commitment drained")
	}
	g.PostFilter(context.Background(), second, gangPod("team", "pf"), nil)
	filtered, _ = g.withoutFailedDomains(gang, topology.FreeByDomain{"a": 2, "b": 2})
	if len(filtered) != 0 {
		t.Fatalf("failed domains after a then b = %v, want both excluded", filtered)
	}
}

// An assigned member another Filter vetoes is activated when the release records
// a domain new to its gang: no event covers that retry, and a worker held back by
// its affinity to the unplaced leader cannot plan the next domain for it.
func TestPostFilter_AssignedMemberVetoed_ActivatesItToPlanNextDomain(t *testing.T) {
	ctx := context.Background()
	leader, worker := leaderMember("pf-leader"), workerMember("pf-worker")
	for _, p := range []*v1.Pod{leader, worker} {
		p.Spec.Containers[0].Resources.Requests[gpu] = resource.MustParse("1")
	}
	h := &fakeHandle{}
	g := &GangPack{
		handle:      h,
		assignments: placement.New(),
		pgReader:    fakeReader{"team/pf": {min: 2, topo: testKey, to: time.Minute}},
		podLister:   fakeGangPodLister{pods: []*v1.Pod{leader, worker}},
	}
	// Two single-node domains, each with room for the whole gang: best-fit ties
	// and takes the lowest name first.
	nodes := []framework.NodeInfo{nodeInfo(gpuNode("x", "x", "4")), nodeInfo(gpuNode("y", "y", "4"))}

	first := newCycleState()
	if _, st := g.PreFilter(ctx, first, leader, nodes); !st.IsSuccess() || readAssignment(first).domain != "x" {
		t.Fatalf("leader PreFilter = %+v, %v, want an assignment in domain x", readAssignment(first), st)
	}
	before := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerDomainFailed))
	if _, st := g.PostFilter(ctx, first, leader, nil); st.Code() != framework.Unschedulable {
		t.Fatalf("PostFilter = %v, want Unschedulable", st)
	}
	if _, assigned := assignedDomain(g.assignments, "team/pf"); assigned {
		t.Fatal("assignment kept after every candidate in domain x was filtered")
	}
	if h.activateCalls != 1 || h.activated["team/pf-leader"] == nil {
		t.Fatalf("activations after the veto = %d %v, want exactly one, for the vetoed leader", h.activateCalls, h.activated)
	}
	if d := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerDomainFailed)) - before; d != 1 {
		t.Fatalf("domain_failed activation counter delta = %v, want 1", d)
	}

	// The worker's required affinity targets a leader that is neither bound nor
	// assumed: it yields without an assignment and requeues nobody.
	workerState := newCycleState()
	if _, st := g.PreFilter(ctx, workerState, worker, nodes); st.Code() != framework.Unschedulable || !strings.Contains(st.Message(), "waiting for gang sibling") {
		t.Fatalf("worker PreFilter = %v, want Unschedulable waiting for its sibling", st)
	}
	if assignment := readAssignment(workerState); assignment != nil {
		t.Fatalf("worker assigned %+v, want no assignment while its leader is unplaced", assignment)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activations after the worker yielded = %d, want still 1", h.activateCalls)
	}

	// The retry the activation produces skips the failed domain and wakes nobody.
	retry := newCycleState()
	if _, st := g.PreFilter(ctx, retry, leader, nodes); !st.IsSuccess() || readAssignment(retry).domain != "y" {
		t.Fatalf("leader retry = %+v, %v, want an assignment in domain y", readAssignment(retry), st)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activations after the re-plan = %d, want still 1", h.activateCalls)
	}
}

// A gang held to one domain by a bound member is never activated for a veto
// there: no re-plan can move it, so the member waits for cluster events
// instead of retrying every backoff.
func TestPostFilter_VetoInDomainHeldByBoundMember_ActivatesNobody(t *testing.T) {
	ctx := context.Background()
	h := &fakeHandle{}
	g := &GangPack{handle: h, assignments: placement.New()}
	gang := gangInfo{key: "team/pf", uid: "uid-1", minMember: 2, topologyKey: testKey, timeout: time.Minute}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gangPod("team", "pf")), // a member already bound in domain a
		nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")),
	}
	veto := func() {
		t.Helper()
		state := newCycleState()
		if _, st := g.assignGangDomain(state, nodes, gang, gangGPUPod("team", "pf", "4")); !st.IsSuccess() || readAssignment(state).domain != "a" {
			t.Fatalf("assignment = %+v, %v, want the bound member's domain a", readAssignment(state), st)
		}
		if _, st := g.PostFilter(ctx, state, gangGPUPod("team", "pf", "4"), nil); st.Code() != framework.Unschedulable {
			t.Fatalf("PostFilter = %v, want Unschedulable", st)
		}
	}
	veto()
	veto()
	if h.activateCalls != 0 {
		t.Fatalf("activations after two vetoes in the imposed domain a = %d, want 0", h.activateCalls)
	}
}

// Each veto activates the member while a domain is untried; the veto of the
// last fitting domain leaves it held, and an attempt after the lap wraps (an event
// or the periodic flush) starts the sequence again.
func TestPostFilter_VetoesAcrossDomains_ActivateOncePerUntriedDomain(t *testing.T) {
	ctx := context.Background()
	h := &fakeHandle{}
	g := &GangPack{handle: h, assignments: placement.New()}
	gang := gangInfo{key: "team/pf", uid: "uid-1", minMember: 1, topologyKey: testKey, timeout: time.Minute}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("x", "x", "4")), nodeInfo(gpuNode("y", "y", "4")), nodeInfo(gpuNode("z", "z", "4")),
	}
	member := func() *v1.Pod {
		p := gangGPUPod("team", "pf", "4")
		p.Name = "pf-0"
		return p
	}
	for _, step := range []struct {
		domain      string
		activations int
	}{
		{"x", 1}, {"y", 2}, {"z", 2}, // z is the last untried domain
		{"x", 3}, // the lap wrapped: x is untried again
	} {
		state := newCycleState()
		if _, st := g.assignGangDomain(state, nodes, gang, member()); !st.IsSuccess() || readAssignment(state).domain != step.domain {
			t.Fatalf("assignment = %+v, %v, want domain %s", readAssignment(state), st, step.domain)
		}
		if _, st := g.PostFilter(ctx, state, member(), nil); st.Code() != framework.Unschedulable {
			t.Fatalf("PostFilter in %s = %v, want Unschedulable", step.domain, st)
		}
		if h.activateCalls != step.activations {
			t.Fatalf("activations after the veto in %s = %d, want %d", step.domain, h.activateCalls, step.activations)
		}
	}
}
