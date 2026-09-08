package gangpack

import (
	"context"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// roleLabel marks a member's role so a worker's required affinity term can
// target the leader alone.
const roleLabel = "testing.example/role"

// twoMemberGang wires a plugin for a 2-member gang whose live member set is
// served by a swappable fake lister, with two free nodes in one domain.
func twoMemberGang(members ...*v1.Pod) (*GangPack, *fakeHandle, []framework.NodeInfo) {
	h := &fakeHandle{}
	g := &GangPack{
		handle:    h,
		pins:      placement.New(),
		pgReader:  fakeReader{"team/pf": {min: 2, topo: testKey, to: time.Minute}},
		podLister: fakeGangPodLister{pods: members},
	}
	nodes := []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "4")), nodeInfo(gpuNode("a2", "a", "4"))}
	return g, h, nodes
}

func namedMember(name string) *v1.Pod {
	p := gangGPUPod("team", "pf", "4")
	p.Name = name
	return p
}

func leaderMember(name string) *v1.Pod {
	p := namedMember(name)
	p.Labels[roleLabel] = "leader"
	return p
}

// workerMember carries a REQUIRED podAffinity term that only the gang's leader
// can satisfy.
func workerMember(name string) *v1.Pod {
	p := namedMember(name)
	p.Labels[roleLabel] = "worker"
	p.Spec.Affinity = requiredAffinityTo(map[string]string{podGroupLabel: "pf", roleLabel: "leader"})
	return p
}

func requiredAffinityTo(matchLabels map[string]string) *v1.Affinity {
	return &v1.Affinity{PodAffinity: &v1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{
			TopologyKey:   testKey,
			LabelSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
		}},
	}}
}

func TestUnconstrainedMemberWaitsForHardSpreadAnchor(t *testing.T) {
	leader, worker := namedMember("leader"), namedMember("worker")
	leader.Spec.TopologySpreadConstraints = []v1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "topology.example/cube",
		WhenUnsatisfiable: v1.DoNotSchedule,
	}}
	g, _, nodes := twoMemberGang(leader, worker)

	workerState := newCycleState()
	_, status := g.PreFilter(context.Background(), workerState, worker, nodes)
	if status.Code() != framework.Unschedulable || !strings.Contains(status.Message(), "waiting for gang spread anchor") {
		t.Fatalf("worker PreFilter = %v, want wait for constrained leader", status)
	}
	if pin := readPin(workerState); pin != nil {
		t.Fatalf("worker wrote premature pin %+v", pin)
	}

	leaderState := newCycleState()
	result, status := g.PreFilter(context.Background(), leaderState, leader, nodes)
	if !status.IsSuccess() || result == nil || result.NodeNames.Len() != 2 {
		t.Fatalf("leader PreFilter = %v, candidates=%v, want deferred two-node plan", status, result)
	}
	if readDeferredPin(leaderState) == nil {
		t.Fatal("hard-spread leader must defer its pin until Reserve")
	}
}

// TestTemplatesCompleteActivatesParkedMembersOnce: a member parked because the
// live set was short of minMember is activated by the first PreFilter that sees
// the set complete, and only by that one. The observing member is in flight and
// is left out of the activation. Later PreFilters of the same complete gang
// activate nobody.
func TestTemplatesCompleteActivatesParkedMembersOnce(t *testing.T) {
	ctx := context.Background()
	leader, worker := namedMember("leader"), namedMember("worker")
	g, h, nodes := twoMemberGang(leader)

	// Leader alone: parked on the incomplete set; nothing to wake yet.
	if _, st := g.PreFilter(ctx, newCycleState(), leader, nodes); st.Code() != framework.Unschedulable {
		t.Fatalf("lone leader PreFilter = %v, want Unschedulable (templates incomplete)", st)
	}
	if h.activateCalls != 0 {
		t.Fatalf("activate calls after lone leader = %d, want 0", h.activateCalls)
	}

	// Worker arrives; its PreFilter is the first to observe the full set.
	g.podLister = fakeGangPodLister{pods: []*v1.Pod{leader, worker}}
	before := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerTemplatesComplete))
	if _, st := g.PreFilter(ctx, newCycleState(), worker, nodes); !st.IsSuccess() {
		t.Fatalf("worker PreFilter = %v, want Success", st)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls after set completed = %d, want exactly 1", h.activateCalls)
	}
	if _, ok := h.activated["team/leader"]; !ok {
		t.Fatalf("activated = %v, want the parked leader included", h.activated)
	}
	if _, ok := h.activated["team/worker"]; ok {
		t.Fatalf("activated = %v, want the in-flight observing worker left out", h.activated)
	}
	if d := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerTemplatesComplete)) - before; d != 1 {
		t.Fatalf("templates_complete activation counter delta = %v, want 1", d)
	}

	// The set stays complete: further PreFilters of either member must not
	// re-activate (activation bypasses backoff, so repeats would spin).
	for _, pod := range []*v1.Pod{leader, worker, leader} {
		if _, st := g.PreFilter(ctx, newCycleState(), pod, nodes); !st.IsSuccess() {
			t.Fatalf("PreFilter of %s on complete gang = %v, want Success", pod.Name, st)
		}
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls after repeated complete PreFilters = %d, want still 1", h.activateCalls)
	}
}

// TestTemplatesIncompleteAgainRearmsActivation: once the set is complete the
// wake-up is spent; a member leaving (set incomplete again) re-arms it, and the
// next completion fires exactly one more activation.
func TestTemplatesIncompleteAgainRearmsActivation(t *testing.T) {
	ctx := context.Background()
	leader, worker := namedMember("leader"), namedMember("worker")
	g, h, nodes := twoMemberGang(leader, worker)

	// Complete from the start: nobody was ever parked, so nothing fires.
	if _, st := g.PreFilter(ctx, newCycleState(), worker, nodes); !st.IsSuccess() {
		t.Fatalf("worker PreFilter = %v, want Success", st)
	}
	if h.activateCalls != 0 {
		t.Fatalf("activate calls on a never-parked gang = %d, want 0", h.activateCalls)
	}

	// Worker gone: the leader parks again.
	g.podLister = fakeGangPodLister{pods: []*v1.Pod{leader}}
	if _, st := g.PreFilter(ctx, newCycleState(), leader, nodes); st.Code() != framework.Unschedulable {
		t.Fatalf("leader PreFilter after worker left = %v, want Unschedulable", st)
	}
	if h.activateCalls != 0 {
		t.Fatalf("activate calls while incomplete = %d, want 0", h.activateCalls)
	}

	// Replacement arrives: one activation for this new completion.
	replacement := namedMember("worker-2")
	g.podLister = fakeGangPodLister{pods: []*v1.Pod{leader, replacement}}
	if _, st := g.PreFilter(ctx, newCycleState(), replacement, nodes); !st.IsSuccess() {
		t.Fatalf("replacement PreFilter = %v, want Success", st)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls after re-completion = %d, want 1", h.activateCalls)
	}
	if _, st := g.PreFilter(ctx, newCycleState(), leader, nodes); !st.IsSuccess() {
		t.Fatalf("leader PreFilter after re-completion = %v, want Success", st)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls after a further complete PreFilter = %d, want still 1", h.activateCalls)
	}
}

// TestCapacityFailuresDoNotActivateSiblings: members that keep failing for lack
// of capacity, whether in PreFilter (no domain fits) or after PreFilter (every
// candidate rejected, unwound by PostFilter), never activate each other. The
// wake-up is tied to the set-completion transition alone, so two failing
// members cannot ping-pong through the backoff-bypassing Activate.
func TestCapacityFailuresDoNotActivateSiblings(t *testing.T) {
	ctx := context.Background()
	leader, worker := namedMember("leader"), namedMember("worker")

	t.Run("no domain fits", func(t *testing.T) {
		g, h, nodes := twoMemberGang(leader, worker)
		nodes = nodes[:1] // one node cannot hold a 2-member gang
		for range 2 {
			for _, pod := range []*v1.Pod{leader, worker} {
				_, st := g.PreFilter(ctx, newCycleState(), pod, nodes)
				if st.Code() != framework.Unschedulable || !strings.Contains(st.Message(), "no domain has room") {
					t.Fatalf("PreFilter of %s = %v, want Unschedulable for lack of room", pod.Name, st)
				}
			}
		}
		if h.activateCalls != 0 {
			t.Fatalf("activate calls across repeated no-fit failures = %d, want 0", h.activateCalls)
		}
		if g.clearTemplatesIncomplete(gangInfo{key: "team/pf"}) {
			t.Fatal("a gang that was never short of members must hold no incomplete-set record")
		}
	})

	t.Run("all candidates filtered", func(t *testing.T) {
		g, h, nodes := twoMemberGang(leader, worker)
		for range 2 {
			for _, pod := range []*v1.Pod{leader, worker} {
				state := newCycleState()
				if _, st := g.PreFilter(ctx, state, pod, nodes); !st.IsSuccess() {
					t.Fatalf("PreFilter of %s = %v, want Success", pod.Name, st)
				}
				if _, st := g.PostFilter(ctx, state, pod, nil); st.Code() != framework.Unschedulable {
					t.Fatalf("PostFilter of %s = %v, want Unschedulable", pod.Name, st)
				}
			}
		}
		if h.activateCalls != 0 {
			t.Fatalf("activate calls across repeated post-PreFilter failures = %d, want 0", h.activateCalls)
		}
	})
}

// TestTemplatesCompleteActivationIsOneShotUnderNoFit: the completion wake-up
// fires once even when the completing member's own attempt then finds no room,
// and the members' subsequent no-fit failures do not fire it again.
func TestTemplatesCompleteActivationIsOneShotUnderNoFit(t *testing.T) {
	ctx := context.Background()
	leader, worker := namedMember("leader"), namedMember("worker")
	g, h, nodes := twoMemberGang(leader)

	if _, st := g.PreFilter(ctx, newCycleState(), leader, nodes); st.Code() != framework.Unschedulable {
		t.Fatalf("lone leader PreFilter = %v, want Unschedulable (templates incomplete)", st)
	}

	// The cluster shrinks to one node; the arriving worker completes the set
	// but the gang no longer fits anywhere.
	nodes = nodes[:1]
	g.podLister = fakeGangPodLister{pods: []*v1.Pod{leader, worker}}
	_, st := g.PreFilter(ctx, newCycleState(), worker, nodes)
	if st.Code() != framework.Unschedulable || !strings.Contains(st.Message(), "no domain has room") {
		t.Fatalf("worker PreFilter = %v, want Unschedulable for lack of room", st)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls after completion under no-fit = %d, want 1", h.activateCalls)
	}

	for range 3 {
		for _, pod := range []*v1.Pod{leader, worker} {
			_, st := g.PreFilter(ctx, newCycleState(), pod, nodes)
			if st.Code() != framework.Unschedulable || !strings.Contains(st.Message(), "no domain has room") {
				t.Fatalf("PreFilter of %s = %v, want Unschedulable for lack of room", pod.Name, st)
			}
		}
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls after repeated no-fit failures = %d, want still 1", h.activateCalls)
	}
	if g.clearTemplatesIncomplete(gangInfo{key: "team/pf"}) {
		t.Fatal("incomplete-set record must be consumed by the completion wake-up")
	}
}

// TestAffinityToUnplacedSiblingYieldsWithoutPinning: a member whose required
// affinity can only be satisfied by a sibling that is neither bound nor assumed
// yields in PreFilter, without planning a domain: no pin is written, no domain
// is recorded as failed, and the completion wake-up still fires exactly once.
// The yield does not depend on the wake-up record, and ends as soon as the
// sibling is placed.
func TestAffinityToUnplacedSiblingYieldsWithoutPinning(t *testing.T) {
	ctx := context.Background()
	leader, worker := leaderMember("leader"), workerMember("worker")
	g, h, nodes := twoMemberGang(leader)
	gang := gangInfo{key: "team/pf", topologyKey: testKey}

	if _, st := g.PreFilter(ctx, newCycleState(), leader, nodes); st.Code() != framework.Unschedulable {
		t.Fatalf("lone leader PreFilter = %v, want Unschedulable (templates incomplete)", st)
	}

	g.podLister = fakeGangPodLister{pods: []*v1.Pod{leader, worker}}
	for attempt := range 2 {
		state := newCycleState()
		_, st := g.PreFilter(ctx, state, worker, nodes)
		if st.Code() != framework.Unschedulable || !strings.Contains(st.Message(), "waiting for gang sibling team/leader to be placed") {
			t.Fatalf("worker PreFilter attempt %d = %v, want Unschedulable waiting for the leader", attempt, st)
		}
		if readPin(state) != nil {
			t.Fatalf("attempt %d wrote a cycle pin %+v while yielding", attempt, readPin(state))
		}
		if _, pinned := g.pins.Get("team/pf"); pinned {
			t.Fatalf("attempt %d pinned the gang while yielding", attempt)
		}
		if _, hadFailed := g.withoutFailedDomains(gang, topology.FreeByDomain{"a": 2}); hadFailed {
			t.Fatalf("attempt %d recorded a failed domain while yielding", attempt)
		}
		// One activation from the completion transition on the first attempt;
		// the yield itself never activates, so a second yield adds nothing.
		if h.activateCalls != 1 {
			t.Fatalf("activate calls after yield attempt %d = %d, want 1", attempt, h.activateCalls)
		}
	}
	if _, ok := h.activated["team/leader"]; !ok {
		t.Fatalf("activated = %v, want the parked leader included", h.activated)
	}
	if _, ok := h.activated["team/worker"]; ok {
		t.Fatalf("activated = %v, want the in-flight worker left out", h.activated)
	}

	// The leader is assumed in domain a: the worker's term is now satisfiable
	// there, so it plans, adopts the leader's domain, and narrows to the free node.
	assumed := []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "4"), leader), nodeInfo(gpuNode("a2", "a", "4"))}
	state := newCycleState()
	result, st := g.PreFilter(ctx, state, worker, assumed)
	if !st.IsSuccess() {
		t.Fatalf("worker PreFilter with leader assumed = %v, want Success", st)
	}
	if pin := readPin(state); pin == nil || pin.domain != "a" {
		t.Fatalf("pin = %+v, want the leader's domain a", pin)
	}
	if result == nil || !result.NodeNames.Has("a2") || result.NodeNames.Has("a1") {
		t.Fatalf("candidates = %v, want the node the leader does not occupy", result)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls after the worker proceeded = %d, want still 1", h.activateCalls)
	}
}

// TestAffinityYieldScope: only a required term that some unplaced sibling of the
// same gang satisfies causes a yield. A term the member satisfies itself may pass
// the framework's first-pod rule, and a term aimed at pods outside the gang is
// not this plugin's concern; both plan and pin as before.
func TestAffinityYieldScope(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		labels map[string]string
	}{
		{name: "self-matching term", labels: map[string]string{podGroupLabel: "pf"}},
		{name: "term outside the gang", labels: map[string]string{"app": "cache"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leader, worker := leaderMember("leader"), namedMember("worker")
			worker.Spec.Affinity = requiredAffinityTo(tc.labels)
			g, _, nodes := twoMemberGang(leader, worker)
			state := newCycleState()
			if _, st := g.PreFilter(ctx, state, worker, nodes); !st.IsSuccess() {
				t.Fatalf("worker PreFilter = %v, want Success (no sibling wait)", st)
			}
			if pin := readPin(state); pin == nil || pin.domain != "a" {
				t.Fatalf("pin = %+v, want domain a", pin)
			}
		})
	}
}

// TestPermitActivatesGangMembers: every member reaching Permit activates the
// gang's live members, itself included, and counts it under the permit trigger.
// A sibling whose Filter depends on this member being assumed has no cluster
// event to react to otherwise.
func TestPermitActivatesGangMembers(t *testing.T) {
	pins := placement.New()
	_, token, ok := pins.ChooseInTopologyOnNodes("team/pf", testKey,
		topology.FreeByDomain{"a": 2}, map[string][]string{"a": {"a1", "a2"}}, 2)
	if !ok {
		t.Fatal("failed to reserve gang")
	}
	leader, worker := namedMember("leader"), namedMember("worker")
	h := &fakeHandle{}
	g := &GangPack{handle: h, pins: pins, podLister: fakeGangPodLister{pods: []*v1.Pod{leader, worker}}}
	state := newCycleState()
	writePin(state, "a", gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey, timeout: time.Minute}, token)

	before := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerPermit))
	status, _ := g.Permit(context.Background(), state, leader, "a1")
	if !status.IsWait() {
		t.Fatalf("Permit = %v, want Wait for the incomplete gang", status)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls from Permit = %d, want 1", h.activateCalls)
	}
	for _, key := range []string{"team/leader", "team/worker"} {
		if _, ok := h.activated[key]; !ok {
			t.Fatalf("activated = %v, want %s included", h.activated, key)
		}
	}
	if d := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerPermit)) - before; d != 1 {
		t.Fatalf("permit activation counter delta = %v, want 1", d)
	}
}

// TestGCPrunesTemplatesIncomplete: the incomplete-set record of a gang whose
// pods are all gone is dropped by the pin garbage collector; a live gang's
// record survives.
func TestGCPrunesTemplatesIncomplete(t *testing.T) {
	g := &GangPack{pins: placement.New(), podLister: fakeGangPodLister{pods: []*v1.Pod{gangPod("team", "live")}}}
	g.markTemplatesIncomplete(gangInfo{key: "team/live"})
	g.markTemplatesIncomplete(gangInfo{key: "team/gone"})

	g.gcPins()

	if !g.clearTemplatesIncomplete(gangInfo{key: "team/live"}) {
		t.Fatal("record for a gang with live pods must survive GC")
	}
	if g.clearTemplatesIncomplete(gangInfo{key: "team/gone"}) {
		t.Fatal("record for a gang with no live pods must be pruned by GC")
	}
}
