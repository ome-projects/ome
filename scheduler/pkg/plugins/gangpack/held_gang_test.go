package gangpack

import (
	"context"
	"sort"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// heldGang wires a 2-member gang whose worker carries no constraint of its
// own, over one node per name, each with room for one member.
func heldGang(nodeNames ...string) (*GangPack, *fakeHandle, []framework.NodeInfo, *v1.Pod, *v1.Pod) {
	leader, worker := leaderMember("leader"), namedMember("worker")
	h := &fakeHandle{}
	g := &GangPack{
		handle:      h,
		assignments: placement.New(),
		pgReader:    fakeReader{"team/pf": {min: 2, topo: testKey, to: time.Minute}},
		podLister:   fakeGangPodLister{pods: []*v1.Pod{leader, worker}},
	}
	nodes := make([]framework.NodeInfo, 0, len(nodeNames))
	for _, name := range nodeNames {
		nodes = append(nodes, nodeInfo(gpuNode(name, name[:1], "4")))
	}
	return g, h, nodes, leader, worker
}

// wokenPods lists the pods a fake handle was asked to activate, by key.
func wokenPods(h *fakeHandle) []string {
	keys := make([]string, 0, len(h.activated))
	for key := range h.activated {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// withAssumed returns the snapshot with pod assumed on the named node.
func withAssumed(nodes []framework.NodeInfo, nodeName string, pod *v1.Pod) []framework.NodeInfo {
	out := make([]framework.NodeInfo, 0, len(nodes))
	for _, ni := range nodes {
		if ni.Node().Name == nodeName {
			out = append(out, nodeInfo(ni.Node(), pod))
			continue
		}
		out = append(out, nodeInfo(ni.Node()))
	}
	return out
}

// heldRound drives one round of a gang held Pending: the worker plans the
// domain, is assumed on its first node and waits at the gate; the leader
// follows the assignment, another Filter vetoes every candidate there, and PostFilter
// unwinds the gang; the rejected worker is unreserved. It returns the domain
// the round used.
func heldRound(t *testing.T, g *GangPack, h *fakeHandle, nodes []framework.NodeInfo, leader, worker *v1.Pod) string {
	t.Helper()
	ctx := context.Background()
	workerState := newCycleState()
	if _, st := g.PreFilter(ctx, workerState, worker, nodes); !st.IsSuccess() {
		t.Fatalf("worker PreFilter = %v, want Success", st)
	}
	domain := readAssignment(workerState).domain
	node := domain + "1"
	g.Reserve(ctx, workerState, worker, node)
	waiting := &fakeWaitingPod{pod: worker}
	h.waiting = []framework.WaitingPod{waiting}
	if st, _ := g.Permit(ctx, workerState, worker, node); !st.IsWait() {
		t.Fatalf("worker Permit = %v, want Wait", st)
	}

	leaderState := newCycleState()
	if _, st := g.PreFilter(ctx, leaderState, leader, withAssumed(nodes, node, worker)); !st.IsSuccess() || readAssignment(leaderState).domain != domain {
		t.Fatalf("leader PreFilter = %+v, %v, want the assigned domain %s", readAssignment(leaderState), st, domain)
	}
	if _, st := g.PostFilter(ctx, leaderState, leader, nil); st.Code() != framework.Unschedulable {
		t.Fatalf("leader PostFilter = %v, want Unschedulable", st)
	}
	if !waiting.rejected {
		t.Fatal("the gate-waiting worker was not rejected when its leader was vetoed")
	}
	g.Unreserve(ctx, workerState, worker, node)
	h.waiting = nil
	return domain
}

// TestActivation_GangHeldOverRounds_OnePerRoundForTheLeaderOnly: the worker takes the only domain with room and
// waits at the gate; the leader follows the assignment and is vetoed there, which
// unwinds the gang. Nothing changes between rounds. A wake-up bypasses backoff
// and retries its target at once, so the gate must not wake the member that
// entered it, and a veto in a domain no other could stand in for must wake
// nobody: the one wake-up per round is the gate waking the parked leader.
func TestActivation_GangHeldOverRounds_OnePerRoundForTheLeaderOnly(t *testing.T) {
	g, h, nodes, leader, worker := heldGang("a1", "a2")
	for round := 1; round <= 3; round++ {
		if d := heldRound(t, g, h, nodes, leader, worker); d != "a" {
			t.Fatalf("round %d used domain %s, want a", round, d)
		}
		if _, woken := h.activated["team/worker"]; woken {
			t.Fatalf("round %d: the worker was woken by its own gate entry", round)
		}
		if h.activateCalls != round {
			t.Fatalf("round %d: %d wake-ups so far, want %d (the gate waking the leader once per round)", round, h.activateCalls, round)
		}
	}
}

// TestPostFilter_FollowerVetoed_ActivatesReplanOnlyIfAnotherDomainFits: a member vetoed in the
// domain a sibling took is requeued to plan again exactly when the planner saw
// another domain with room for the gang. With a second free domain the leader
// is woken and its retry plans there; with that domain held by another
// forming gang, or absent, the veto wakes nobody.
func TestPostFilter_FollowerVetoed_ActivatesReplanOnlyIfAnotherDomainFits(t *testing.T) {
	ctx := context.Background()

	t.Run("another domain fits", func(t *testing.T) {
		g, h, nodes, leader, worker := heldGang("x1", "x2", "y1", "y2")
		if d := heldRound(t, g, h, nodes, leader, worker); d != "x" {
			t.Fatalf("first round used domain %s, want the best-fit tie x", d)
		}
		// The gate woke the leader once; the veto woke it once more to re-plan.
		if h.activateCalls != 2 || h.activated["team/leader"] == nil {
			t.Fatalf("wake-ups after the veto = %d %v, want 2, both for the leader", h.activateCalls, wokenPods(h))
		}
		if _, woken := h.activated["team/worker"]; woken {
			t.Fatalf("woken pods = %v, want the gate-waiting worker left out", wokenPods(h))
		}
		retry := newCycleState()
		if _, st := g.PreFilter(ctx, retry, leader, nodes); !st.IsSuccess() || readAssignment(retry).domain != "y" {
			t.Fatalf("leader retry = %+v, %v, want an assignment in the untried domain y", readAssignment(retry), st)
		}
	})

	t.Run("other domain reserved by a forming gang", func(t *testing.T) {
		g, h, nodes, leader, worker := heldGang("x1", "x2", "y1", "y2")
		if _, _, ok := g.assignments.Choose(placement.Gang{Group: "team/other", TopologyKey: testKey}, topology.FreeByDomain{"y": 2}, map[string][]string{"y": {"y1", "y2"}}, 2); !ok {
			t.Fatal("failed to reserve domain y for another gang")
		}
		if d := heldRound(t, g, h, nodes, leader, worker); d != "x" {
			t.Fatalf("round used domain %s, want x", d)
		}
		if h.activateCalls != 1 {
			t.Fatalf("wake-ups after the veto = %d %v, want 1 (the gate waking the leader, the veto waking nobody)", h.activateCalls, wokenPods(h))
		}
	})
}
