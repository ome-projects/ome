package gangpack

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// TestPinGang is the PreFilter decision core (node slice in, pin + narrowed set
// out — no fake Handle): best-fit a domain for the gang, record the pin, and
// narrow candidates to that domain's nodes. A gang that fits nowhere is
// Unschedulable.
func TestPinGang(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("n1", "a", "4")), nodeInfo(gpuNode("n2", "a", "4")), // a: 2 free
		nodeInfo(gpuNode("n3", "b", "4")), nodeInfo(gpuNode("n4", "b", "4")), nodeInfo(gpuNode("n5", "b", "4")), // b: 3 free
	}
	pod := gpuPod("4")
	st := newCycleState()

	res, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, pod)
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success", status)
	}
	// Best-fit: a (2 free) is fuller than b (3) and still fits a gang of 2.
	if res == nil || !res.NodeNames.Equal(sets.New("n1", "n2")) {
		t.Fatalf("narrowed to %v, want {n1,n2}", res.NodeNames)
	}
	if pin := readPin(st); pin == nil || pin.domain != "a" || pin.topologyKey != testKey {
		t.Fatalf("pin = %+v, want domain a", pin)
	}

	// A gang larger than any domain cannot be placed.
	_, s2 := g.pinGang(newCycleState(), nodes, gangInfo{key: "team/big", minMember: 10, topologyKey: testKey}, pod)
	if s2.IsSuccess() || s2.Code() != framework.Unschedulable {
		t.Fatalf("oversized gang status = %v, want Unschedulable", s2)
	}
}

// TestPinGangDefersHardSpreadDomainChoice proves that GangPack does not commit
// an unplaced gang to one rack before PodTopologySpread has run. PreFilter
// exposes every rack that can hold the complete gang; Reserve then commits to
// the rack of the node selected from the framework-filtered candidates.
func TestPinGangDefersHardSpreadDomainChoice(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")), nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")), nodeInfo(gpuNode("b2", "b", "4")), nodeInfo(gpuNode("b3", "b", "4")),
	}
	pod := gpuPod("4")
	pod.Spec.TopologySpreadConstraints = []v1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: v1.DoNotSchedule,
	}}
	state := newCycleState()
	gang := gangInfo{key: "team/spread", uid: "uid-1", minMember: 2, topologyKey: testKey}

	result, status := g.pinGang(state, nodes, gang, pod)
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success", status)
	}
	if result == nil || !result.NodeNames.Equal(sets.New("a1", "a2", "b1", "b2", "b3")) {
		t.Fatalf("deferred candidates = %v, want every whole-gang-feasible domain", result.NodeNames)
	}
	if pin := readPin(state); pin != nil {
		t.Fatalf("PreFilter pin = %+v, want nil until framework filters select a node", pin)
	}
	if plan := readDeferredPin(state); plan == nil || plan.need != 2 || len(plan.free) != 2 {
		t.Fatalf("deferred plan = %+v, want two feasible domains for two members", plan)
	}

	// Model PodTopologySpread filtering rack a away by selecting b1. Reserve must
	// commit the gang to b even though ordinary GangPack best-fit preferred a.
	if status := g.Reserve(context.Background(), state, pod, "b1"); !status.IsSuccess() {
		t.Fatalf("Reserve = %v, want Success", status)
	}
	if domain, ok := g.pins.Get("team/spread"); !ok || domain != "b" {
		t.Fatalf("reserved domain = %q,%v, want b,true", domain, ok)
	}
	if pin := readPin(state); pin == nil || pin.domain != "b" || pin.commitment == 0 {
		t.Fatalf("Reserve pin = %+v, want owned domain b pin", pin)
	}
	reservations := g.pins.Reservations()
	if len(reservations) != 1 || reservations[0].Remaining != 1 {
		t.Fatalf("post-Reserve reservation = %+v, want one remaining member", reservations)
	}
}

func TestDeferredSpreadPlanExcludesReservedDomain(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")), nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")), nodeInfo(gpuNode("b2", "b", "4")),
	}
	if _, _, ok := g.pins.ChooseInTopologyOnNodes("team/forming", testKey,
		topology.FreeByDomain{"a": 2}, map[string][]string{"a": {"a1", "a2"}}, 2); !ok {
		t.Fatal("reserve domain a for forming gang")
	}
	pod := gpuPod("4")
	pod.Spec.TopologySpreadConstraints = []v1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       testKey,
		WhenUnsatisfiable: v1.DoNotSchedule,
	}}
	state := newCycleState()

	result, status := g.pinGang(state, nodes,
		gangInfo{key: "team/spread", uid: "uid-1", minMember: 2, topologyKey: testKey}, pod)
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success", status)
	}
	if result == nil || !result.NodeNames.Equal(sets.New("b1", "b2")) {
		t.Fatalf("deferred candidates = %v, want only unreserved domain b", result.NodeNames)
	}
}

// TestPinGangAdoptsBoundMembers: with the in-memory pin lost (restart) but a gang
// member already placed in a domain, pinGang adopts that domain instead of
// best-fitting fresh — even when a fresh best-fit would fail. Here domain "a" has
// one member already bound and one free node (2 nodes total), so a fresh best-fit
// for a gang of 3 fits nowhere; adoption pins "a" so the remaining members can
// still land with their sibling.
func TestPinGangAdoptsBoundMembers(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	placed := gangPod("team", "pf") // a member already bound in domain a
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), placed),
		nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("a3", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")),
	}
	st := newCycleState()

	res, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey}, gangPod("team", "pf"))
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success (adopt bound member's domain)", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "a" {
		t.Fatalf("adopted pin = %q,%v, want a,true", d, ok)
	}
	if g.pins.Committed("team/pf") {
		t.Fatal("partial adopted gang must retain reservations for missing members")
	}
	reservations := g.pins.Reservations()
	if len(reservations) != 1 || reservations[0].Remaining != 2 {
		t.Fatalf("adopted reservation = %+v, want 2 remaining members", reservations)
	}
	if pin := readPin(st); pin == nil || pin.domain != "a" {
		t.Fatalf("cycle pin = %+v, want domain a", pin)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("a1", "a2", "a3")) {
		t.Fatalf("narrowed to %v, want {a1,a2,a3}", res.NodeNames)
	}
}

// TestPinGangReplansStalePin: a pin to a domain that no longer has any node (its
// nodes drained/scaled away, or the topology key changed so the old domain value
// matches nothing) is dropped and re-planned, instead of narrowing every
// candidate away and wedging the gang forever.
func TestPinGangReplansStalePin(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	g.pins.Set("team/pf", "gone") // pinned to a domain that no longer exists
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	st := newCycleState()

	res, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gpuPod("4"))
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success (re-plan off the stale domain)", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned pin = %q,%v, want b,true", d, ok)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("b1", "b2")) {
		t.Fatalf("narrowed to %v, want {b1,b2}", res.NodeNames)
	}
}

func TestPinGangDoesNotReuseRecreatedPodGroupPin(t *testing.T) {
	waitingPod := gangPod("team", "pf")
	waitingPod.Name = "old-waiter"
	waiter := &fakeWaitingPod{pod: waitingPod}
	g := &GangPack{pins: placement.New(), handle: &fakeHandle{waiting: []framework.WaitingPod{waiter}}}
	_, oldToken, ok := g.pins.ChooseForOwnerInTopologyOnNodes("team/pf", "old-uid", testKey,
		topology.FreeByDomain{"old": 1}, map[string][]string{"old": {"old-1"}}, 1)
	if !ok || oldToken == 0 {
		t.Fatal("failed to establish old PodGroup commitment")
	}
	g.rememberAttempt(waitingPod, oldToken)
	nodes := []framework.NodeInfo{nodeInfo(gpuNode("new-1", "new", "4"))}
	state := newCycleState()
	_, status := g.pinGang(state, nodes, gangInfo{
		key: "team/pf", uid: "new-uid", minMember: 1, topologyKey: testKey,
	}, gangGPUPod("team", "pf", "4"))
	if status.Code() != framework.Unschedulable {
		t.Fatalf("first recreated PodGroup cycle = %v, want stale-attempt unwind", status)
	}
	if !waiter.rejected {
		t.Fatal("old PodGroup waiter was not rejected before the recreated group retried")
	}
	state = newCycleState()
	_, status = g.pinGang(state, nodes, gangInfo{
		key: "team/pf", uid: "new-uid", minMember: 1, topologyKey: testKey,
	}, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() || readPin(state).domain != "new" {
		t.Fatalf("recreated PodGroup pin = %+v, %v, want new domain", readPin(state), status)
	}
	_, token, _, found, ownerMatch := g.pins.GetOwnedBy("team/pf", "new-uid")
	if !found || !ownerMatch || token == oldToken {
		t.Fatalf("new commitment = token %d found=%v ownerMatch=%v; old token=%d", token, found, ownerMatch, oldToken)
	}
}

// TestPinGangReplansOnFullDomain: a gang pinned (with a capacity reservation) to a
// domain that then FILLS — another gang won the race for it before this gang placed
// any member — must drop the pin, re-plan onto a free domain, AND release the
// reservation it leaked on the full domain. Without this the gang retries the full
// domain forever while free domains sit idle, and the leaked reservation
// permanently under-counts that domain for every other gang.
func TestPinGangReplansOnFullDomain(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	// pf pinned to "a" with a reservation, exactly as a first-member Choose would
	// when "a" and "b" both looked free.
	if d, ok := g.pins.Choose("team/pf", topology.FreeByDomain{"a": 2, "b": 2}, 2); !ok || d != "a" {
		t.Fatalf("precondition: Choose = %q,%v, want a,true", d, ok)
	}
	// Snapshot now: "a" is full (its node occupied by another gang's pod), "b" free.
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gpuPod("4")), // occupied by a non-pf pod
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	st := newCycleState()

	res, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success (re-plan off the full domain)", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned pin = %q,%v, want b,true", d, ok)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("b1", "b2")) {
		t.Fatalf("narrowed to %v, want {b1,b2}", res.NodeNames)
	}
	// The reservation pf leaked on "a" must be gone: a fresh gang can pin "a" once
	// it frees. If the reservation leaked, "a"'s effective free would be 0 here.
	if d, ok := g.pins.Choose("team/other", topology.FreeByDomain{"a": 2}, 2); !ok || d != "a" {
		t.Fatalf("reservation on 'a' leaked after re-plan: other gang Choose = %q,%v, want a,true", d, ok)
	}
}

// TestPinGangReplansOffCordonedDomain: a gang pinned to a domain whose nodes have
// become unschedulable (cordoned) — capacity intact but the framework's Filter
// would reject them — must re-plan onto a schedulable domain, not wedge there.
// freeByDomain excludes unschedulable nodes, so pinStale sees the pinned domain
// as unusable and drops it.
func TestPinGangReplansOffCordonedDomain(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	g.pins.Set("team/pf", "a") // pinned to a
	nodes := []framework.NodeInfo{
		nodeInfo(cordoned(gpuNode("a1", "a", "4"))), // a: cordoned (has gpu, unschedulable)
		nodeInfo(cordoned(gpuNode("a2", "a", "4"))),
		nodeInfo(gpuNode("b1", "b", "4")), // b: schedulable
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	_, status := g.pinGang(newCycleState(), nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() {
		t.Fatalf("pinGang status = %v, want Success (re-plan off the cordoned domain)", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned pin = %q,%v, want b,true", d, ok)
	}
}

// TestPinGangKeepsPinWhenMemberBound: a pinned domain that is full but already
// holds one of the gang's own members is NOT stale — re-planning would strand that
// member. The gang keeps its pin and waits for capacity in its own domain.
func TestPinGangKeepsPinWhenMemberBound(t *testing.T) {
	g := &GangPack{pins: placement.New()}
	g.pins.Set("team/pf", "a")
	// Domain "a": one node holds a pf member (bound), a second node is full with
	// another gang's pod — so "a" cannot fit a fresh member, but pf lives here.
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gangPod("team", "pf")), // pf's own bound member
		nodeInfo(gpuNode("a2", "a", "4"), gpuPod("4")),           // occupied by another gang
		nodeInfo(gpuNode("b1", "b", "4")),                        // free elsewhere
	}
	st := newCycleState()

	_, status := g.pinGang(st, nodes, gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if status.Code() != framework.Unschedulable {
		t.Fatalf("pinGang status = %v, want Unschedulable while pinned domain cannot complete", status)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "a" {
		t.Fatalf("pin = %q,%v, want a,true (must not re-plan away from a bound member)", d, ok)
	}
}

// TestPreFilterClassifiesPods verifies standalone pods continue to Filter for the
// reservation barrier, while every labeled but unresolved/invalid pod fails closed.
func TestPreFilterClassifiesPods(t *testing.T) {
	ctx := context.Background()

	nilReader := &GangPack{pins: placement.New()} // pgReader nil -> inert
	if _, st := nilReader.PreFilter(ctx, newCycleState(), gangPod("team", "x"), nil); st.Code() != framework.Unschedulable {
		t.Fatalf("nil-reader PreFilter = %v, want Unschedulable", st)
	}

	// No pod-group label -> Success, keeping Filter active for reservation checks.
	nonMember := &GangPack{pins: placement.New(), pgReader: fakeReader{}}
	res, st := nonMember.PreFilter(ctx, newCycleState(), gangPod("team", ""), nil)
	if res != nil || !st.IsSuccess() {
		t.Fatalf("non-member PreFilter = %v,%v, want nil,Success", res, st)
	}

	// PodGroup present but declares no topology key -> fail closed.
	noTopo := &GangPack{pins: placement.New(), pgReader: fakeReader{"team/x": {min: 3, topo: ""}}}
	res, st = noTopo.PreFilter(ctx, newCycleState(), gangPod("team", "x"), nil)
	if res != nil || st.Code() != framework.Unschedulable {
		t.Fatalf("no-topology-key PreFilter = %v,%v, want nil,Unschedulable", res, st)
	}
}

// TestPreFilterHoldsWhenPodGroupMissing: a pod that IS a gang member (carries the
// pod-group label) but whose PodGroup the reader can't resolve yet — the common
// informer-lag race when a workload's pods and PodGroup are created together — must
// be held (Unschedulable), NOT skipped. Skipping would let a lone member bind and
// break all-or-nothing; holding requeues it until the PodGroup appears.
func TestPreFilterHoldsWhenPodGroupMissing(t *testing.T) {
	g := &GangPack{pins: placement.New(), pgReader: fakeReader{}} // reader present, group absent
	res, st := g.PreFilter(context.Background(), newCycleState(), gangPod("team", "pf"), nil)
	if res != nil {
		t.Fatalf("PreFilter result = %v, want nil", res)
	}
	if st.Code() != framework.Unschedulable {
		t.Fatalf("PreFilter for uncached PodGroup = %v, want Unschedulable (hold, not Skip)", st)
	}
}

func TestPreFilterUsesConfiguredTopologyWhenPodGroupHasNoOverride(t *testing.T) {
	pod := gangGPUPod("team", "pf", "4")
	g := &GangPack{
		handle:      &fakeHandle{snapshot: []framework.NodeInfo{nodeInfo(gpuNode("n1", "a", "4"))}},
		topologyKey: testKey,
		pins:        placement.New(),
		pgReader:    fakeReader{"team/pf": {min: 1, to: time.Minute}},
	}
	state := newCycleState()
	result, status := g.PreFilter(context.Background(), state, pod, g.handle.(*fakeHandle).snapshot)
	if !status.IsSuccess() {
		t.Fatalf("PreFilter = %v, want configured topology fallback to succeed", status)
	}
	if result == nil || !result.NodeNames.Equal(sets.New("n1")) {
		t.Fatalf("nodes = %v, want n1", result)
	}
	if pin := readPin(state); pin == nil || pin.topologyKey != testKey {
		t.Fatalf("pin = %+v, want configured topology key", pin)
	}
}

func TestPreFilterHoldsInvalidGangConfiguration(t *testing.T) {
	cases := []struct {
		name   string
		facts  fakeReader
		mutate func(*v1.Pod)
	}{
		{name: "non-positive minMember", facts: fakeReader{"team/pf": {min: 0, topo: testKey}}},
		{name: "missing topology key", facts: fakeReader{"team/pf": {min: 2}}},
		{name: "unsupported partner placement group", facts: fakeReader{"team/pf": {min: 2, topo: testKey, to: time.Minute}}, mutate: func(p *v1.Pod) {
			p.Labels[placementGroupLabel] = "service"
		}},
		{name: "unsupported empty partner placement group", facts: fakeReader{"team/pf": {min: 2, topo: testKey, to: time.Minute}}, mutate: func(p *v1.Pod) {
			p.Labels[placementGroupLabel] = ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := gangPod("team", "pf")
			if tc.mutate != nil {
				tc.mutate(pod)
			}
			g := &GangPack{
				pins:                           placement.New(),
				pgReader:                       tc.facts,
				podGroupTopologyKeyAnnotation:  topologyKeyAnnotation,
				unsupportedPlacementGroupLabel: placementGroupLabel,
			}
			_, status := g.PreFilter(context.Background(), newCycleState(), pod, nil)
			if status.Code() != framework.Unschedulable {
				t.Fatalf("PreFilter = %v, want Unschedulable", status)
			}
			if _, pinned := g.pins.Get("team/pf"); pinned {
				t.Fatal("invalid gang must never be pinned")
			}
		})
	}
}

func TestPreFilterRejectsPlacementGroupOnPodGroup(t *testing.T) {
	for _, value := range []string{"service", ""} {
		t.Run("value="+value, func(t *testing.T) {
			g := &GangPack{
				pins:                           placement.New(),
				podGroupTopologyKeyAnnotation:  topologyKeyAnnotation,
				unsupportedPlacementGroupLabel: placementGroupLabel,
				pgReader: fakePlacementReader{
					fakeReader: fakeReader{"team/pf": {min: 2, topo: testKey, to: time.Minute}},
					group:      value,
				},
			}
			_, status := g.PreFilter(context.Background(), newCycleState(), gangPod("team", "pf"), nil)
			if status.Code() != framework.Unschedulable {
				t.Fatalf("PreFilter = %v, want explicit unsupported placement-group rejection", status)
			}
		})
	}
}

func TestPinGangUsesAllHeterogeneousMemberTemplates(t *testing.T) {
	current := gangGPUPod("team", "pf", "4")
	current.Name = "small"
	large := gangGPUPod("team", "pf", "8")
	large.Name = "large"
	g := &GangPack{
		pins:      placement.New(),
		podLister: fakeGangPodLister{pods: []*v1.Pod{current, large}},
	}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")), nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "8")), nodeInfo(gpuNode("b2", "b", "8")),
	}
	result, status := g.pinGang(newCycleState(), nodes,
		gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, current)
	if !status.IsSuccess() {
		t.Fatalf("pinGang = %v, want Success", status)
	}
	if domain, _ := g.pins.Get("team/pf"); domain != "b" {
		t.Fatalf("pin = %q, want b (only domain fitting the large sibling)", domain)
	}
	if !result.NodeNames.Equal(sets.New("b1", "b2")) {
		t.Fatalf("candidates = %v, want b nodes", result.NodeNames)
	}
}

func TestPinGangPreservesMatchingForConstrainedSibling(t *testing.T) {
	current := gangGPUPod("team", "pf", "4")
	current.Name = "flexible"
	constrained := gangGPUPod("team", "pf", "4")
	constrained.Name = "constrained"
	constrained.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": "n1"}
	n1, n2 := gpuNode("n1", "a", "4"), gpuNode("n2", "a", "4")
	n1.Labels["kubernetes.io/hostname"] = "n1"
	n2.Labels["kubernetes.io/hostname"] = "n2"
	g := &GangPack{pins: placement.New(), podLister: fakeGangPodLister{pods: []*v1.Pod{current, constrained}}}
	result, status := g.pinGang(newCycleState(), []framework.NodeInfo{nodeInfo(n1), nodeInfo(n2)},
		gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, current)
	if !status.IsSuccess() {
		t.Fatalf("pinGang = %v, want Success", status)
	}
	if !result.NodeNames.Equal(sets.New("n2")) {
		t.Fatalf("flexible candidates = %v, want only n2 so n1 remains for constrained sibling", result.NodeNames)
	}
}

func TestPinGangFailsClosedWhenBoundMembersAreSplit(t *testing.T) {
	member := func() *v1.Pod {
		p := gangGPUPod("team", "pf", "4")
		return p
	}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "8"), member()),
		nodeInfo(gpuNode("b1", "b", "8"), member()),
		nodeInfo(gpuNode("a2", "a", "8")),
	}
	g := &GangPack{pins: placement.New()}
	_, status := g.pinGang(newCycleState(), nodes,
		gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if status.Code() != framework.UnschedulableAndUnresolvable {
		t.Fatalf("split gang status = %v, want UnschedulableAndUnresolvable", status)
	}
	if _, pinned := g.pins.Get("team/pf"); pinned {
		t.Fatal("split gang must not be adopted into an arbitrary domain")
	}
}

func TestPinGangWaitsForMissingMemberTemplates(t *testing.T) {
	current := gangGPUPod("team", "pf", "4")
	current.Name = "only"
	g := &GangPack{pins: placement.New(), podLister: fakeGangPodLister{pods: []*v1.Pod{current}}}
	_, status := g.pinGang(newCycleState(), []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")), nodeInfo(gpuNode("a2", "a", "4")),
	}, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, current)
	if status.Code() != framework.Unschedulable {
		t.Fatalf("pinGang = %v, want Unschedulable until all templates are visible", status)
	}
}
