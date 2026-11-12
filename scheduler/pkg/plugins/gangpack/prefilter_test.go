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

// TestAssignGangDomain_TwoDomainsFit_BestFitNarrowsAndOversizedFails is the PreFilter decision core (node slice in, assignment + narrowed
// set out — no fake Handle): best-fit a domain for the gang, record the assignment, and
// narrow candidates to that domain's nodes. A gang that fits nowhere is
// Unschedulable.
func TestAssignGangDomain_TwoDomainsFit_BestFitNarrowsAndOversizedFails(t *testing.T) {
	g := &GangPack{assignments: placement.New()}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("n1", "a", "4")), nodeInfo(gpuNode("n2", "a", "4")), // a: 2 free
		nodeInfo(gpuNode("n3", "b", "4")), nodeInfo(gpuNode("n4", "b", "4")), nodeInfo(gpuNode("n5", "b", "4")), // b: 3 free
	}
	pod := gpuPod("4")
	st := newCycleState()

	res, status := g.assignGangDomain(st, nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, pod)
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain status = %v, want Success", status)
	}
	// Best-fit: a (2 free) is fuller than b (3) and still fits a gang of 2.
	if res == nil || !res.NodeNames.Equal(sets.New("n1", "n2")) {
		t.Fatalf("narrowed to %v, want {n1,n2}", res.NodeNames)
	}
	if assignment := readAssignment(st); assignment == nil || assignment.domain != "a" || assignment.topologyKey != testKey {
		t.Fatalf("assignment = %+v, want domain a", assignment)
	}

	// A gang larger than any domain cannot be placed.
	_, s2 := g.assignGangDomain(newCycleState(), nodes, gangInfo{key: "team/big", minMember: 10, topologyKey: testKey}, pod)
	if s2.IsSuccess() || s2.Code() != framework.Unschedulable {
		t.Fatalf("oversized gang status = %v, want Unschedulable", s2)
	}
}

// TestAssignGangDomain_MemberAlreadyBound_AdoptsItsDomain: with the in-memory assignment lost (restart) but a gang
// member already placed in a domain, assignGangDomain adopts that domain instead of
// best-fitting fresh — even when a fresh best-fit would fail. Here domain "a" has
// one member already bound and one free node (2 nodes total), so a fresh best-fit
// for a gang of 3 fits nowhere; adoption takes "a" so the remaining members can
// still land with their sibling.
func TestAssignGangDomain_MemberAlreadyBound_AdoptsItsDomain(t *testing.T) {
	g := &GangPack{assignments: placement.New()}
	placed := gangPod("team", "pf") // a member already bound in domain a
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), placed),
		nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("a3", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")),
	}
	st := newCycleState()

	res, status := g.assignGangDomain(st, nodes, gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey}, gangPod("team", "pf"))
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain status = %v, want Success (adopt bound member's domain)", status)
	}
	if d, ok := assignedDomain(g.assignments, "team/pf"); !ok || d != "a" {
		t.Fatalf("adopted assignment = %q,%v, want a,true", d, ok)
	}
	_, id, remaining, _ := g.assignments.GetOwned("team/pf")
	if g.assignments.Committed("team/pf", id) {
		t.Fatal("partial adopted gang must retain reservations for missing members")
	}
	if remaining != 2 {
		t.Fatalf("adopted reservation = %d remaining, want 2 missing members", remaining)
	}
	if assignment := readAssignment(st); assignment == nil || assignment.domain != "a" {
		t.Fatalf("cycle assignment = %+v, want domain a", assignment)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("a1", "a2", "a3")) {
		t.Fatalf("narrowed to %v, want {a1,a2,a3}", res.NodeNames)
	}
}

// TestAssignGangDomain_AssignedDomainHasNoNodes_ReplansElsewhere: an assignment to a domain that no longer has any node (its
// nodes drained/scaled away, or the topology key changed so the old domain value
// matches nothing) is dropped and re-planned, instead of narrowing every
// candidate away and leaving the gang unplaceable forever.
func TestAssignGangDomain_AssignedDomainHasNoNodes_ReplansElsewhere(t *testing.T) {
	g := &GangPack{assignments: placement.New()}
	g.assignments.Set(placement.Gang{Group: "team/pf"}, "gone", 0, nil) // assigned to a domain that no longer exists
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	st := newCycleState()

	res, status := g.assignGangDomain(st, nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gpuPod("4"))
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain status = %v, want Success (re-plan off the stale domain)", status)
	}
	if d, ok := assignedDomain(g.assignments, "team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned assignment = %q,%v, want b,true", d, ok)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("b1", "b2")) {
		t.Fatalf("narrowed to %v, want {b1,b2}", res.NodeNames)
	}
}

// TestAssignGangDomain_StaleAssignmentWithMembersPlacedElsewhere_AdoptsTheirDomain:
// an assignment to a domain with no node left is dropped, and a gang whose
// member already sits in another domain adopts that domain with its free nodes:
// the room is read over them, the candidates are its nodes, and the adopted
// commitment reserves them by name.
func TestAssignGangDomain_StaleAssignmentWithMembersPlacedElsewhere_AdoptsTheirDomain(t *testing.T) {
	g := &GangPack{assignments: placement.New()}
	g.assignments.Set(placement.Gang{Group: "team/pf"}, "gone", 0, nil) // assigned to a domain that no longer exists
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("b1", "b", "4"), gangPod("team", "pf")), // a member already placed in b
		nodeInfo(gpuNode("b2", "b", "4")),
		nodeInfo(gpuNode("b3", "b", "4")),
	}
	st := newCycleState()

	res, status := g.assignGangDomain(st, nodes, gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey}, gangPod("team", "pf"))
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain status = %v, want Success (adopt the placed member's domain)", status)
	}
	if d, ok := assignedDomain(g.assignments, "team/pf"); !ok || d != "b" {
		t.Fatalf("adopted assignment = %q,%v, want b,true", d, ok)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("b1", "b2", "b3")) {
		t.Fatalf("narrowed to %v, want {b1,b2,b3}", res.NodeNames)
	}
	if _, _, remaining, _ := g.assignments.GetOwned("team/pf"); remaining != 2 {
		t.Fatalf("adopted reservation = %d remaining, want 2 missing members", remaining)
	}
	if !g.assignments.BlocksNode("b2", nil) {
		t.Fatal("the adopted commitment must reserve the domain's nodes by name")
	}
}

func TestAssignGangDomain_PodGroupRecreated_UnwindsOldAttemptThenAssignsNew(t *testing.T) {
	waitingPod := gangPod("team", "pf")
	waitingPod.Name = "old-waiter"
	waiter := &fakeWaitingPod{pod: waitingPod}
	g := &GangPack{assignments: placement.New(), handle: &fakeHandle{waiting: []framework.WaitingPod{waiter}}}
	_, oldToken, ok := g.assignments.Choose(placement.Gang{Group: "team/pf", Owner: "old-uid", TopologyKey: testKey},
		topology.FreeByDomain{"old": 1}, map[string][]string{"old": {"old-1"}}, 1)
	if !ok || oldToken == 0 {
		t.Fatal("failed to establish old PodGroup commitment")
	}
	g.rememberAttempt(waitingPod, oldToken)
	nodes := []framework.NodeInfo{nodeInfo(gpuNode("new-1", "new", "4"))}
	state := newCycleState()
	_, status := g.assignGangDomain(state, nodes, gangInfo{
		key: "team/pf", uid: "new-uid", minMember: 1, topologyKey: testKey,
	}, gangGPUPod("team", "pf", "4"))
	if status.Code() != framework.Unschedulable {
		t.Fatalf("first recreated PodGroup cycle = %v, want stale-attempt unwind", status)
	}
	if !waiter.rejected {
		t.Fatal("old PodGroup waiter was not rejected before the recreated group retried")
	}
	state = newCycleState()
	_, status = g.assignGangDomain(state, nodes, gangInfo{
		key: "team/pf", uid: "new-uid", minMember: 1, topologyKey: testKey,
	}, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() || readAssignment(state).domain != "new" {
		t.Fatalf("recreated PodGroup assignment = %+v, %v, want new domain", readAssignment(state), status)
	}
	_, token, found, ownerMatch := g.assignments.GetOwnedBy("team/pf", "new-uid")
	if !found || !ownerMatch || token == oldToken {
		t.Fatalf("new commitment = token %d found=%v ownerMatch=%v; old token=%d", token, found, ownerMatch, oldToken)
	}
}

// TestAssignGangDomain_AssignedDomainFilledBeforePlacement_ReplansAndReleases: a gang assigned (with a capacity reservation) to a
// domain that then FILLS — another gang won the race for it before this gang placed
// any member — must drop the assignment, re-plan onto a free domain, AND release the
// reservation it leaked on the full domain. Without this the gang retries the full
// domain forever while free domains sit idle, and the leaked reservation
// permanently under-counts that domain for every other gang.
func TestAssignGangDomain_AssignedDomainFilledBeforePlacement_ReplansAndReleases(t *testing.T) {
	g := &GangPack{assignments: placement.New()}
	// pf assigned to "a" with a reservation, exactly as a first-member Choose would
	// when "a" and "b" both looked free.
	if d, ok := choose(g.assignments, "team/pf", topology.FreeByDomain{"a": 2, "b": 2}, 2); !ok || d != "a" {
		t.Fatalf("precondition: Choose = %q,%v, want a,true", d, ok)
	}
	// Snapshot now: "a" is full (its node occupied by another gang's pod), "b" free.
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gpuPod("4")), // occupied by a non-pf pod
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	st := newCycleState()

	res, status := g.assignGangDomain(st, nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain status = %v, want Success (re-plan off the full domain)", status)
	}
	if d, ok := assignedDomain(g.assignments, "team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned assignment = %q,%v, want b,true", d, ok)
	}
	if res == nil || !res.NodeNames.Equal(sets.New("b1", "b2")) {
		t.Fatalf("narrowed to %v, want {b1,b2}", res.NodeNames)
	}
	// The reservation pf leaked on "a" must be gone: a fresh gang can take "a" once
	// it frees. If the reservation leaked, "a"'s effective free would be 0 here.
	if d, ok := choose(g.assignments, "team/other", topology.FreeByDomain{"a": 2}, 2); !ok || d != "a" {
		t.Fatalf("reservation on 'a' leaked after re-plan: other gang Choose = %q,%v, want a,true", d, ok)
	}
}

// TestAssignGangDomain_AssignedDomainCordoned_ReplansToSchedulableDomain: a gang assigned to a domain whose nodes have
// become unschedulable (cordoned) — capacity intact but the framework's Filter
// would reject them — must re-plan onto a schedulable domain, not wait there forever.
// freeByDomain excludes unschedulable nodes, so assignmentStale sees the assigned domain
// as unusable and drops it.
func TestAssignGangDomain_AssignedDomainCordoned_ReplansToSchedulableDomain(t *testing.T) {
	g := &GangPack{assignments: placement.New()}
	g.assignments.Set(placement.Gang{Group: "team/pf"}, "a", 0, nil) // assigned to a
	nodes := []framework.NodeInfo{
		nodeInfo(cordoned(gpuNode("a1", "a", "4"))), // a: cordoned (has gpu, unschedulable)
		nodeInfo(cordoned(gpuNode("a2", "a", "4"))),
		nodeInfo(gpuNode("b1", "b", "4")), // b: schedulable
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	_, status := g.assignGangDomain(newCycleState(), nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain status = %v, want Success (re-plan off the cordoned domain)", status)
	}
	if d, ok := assignedDomain(g.assignments, "team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned assignment = %q,%v, want b,true", d, ok)
	}
}

// TestAssignGangDomain_FullDomainHoldsOwnMember_KeepsAssignmentAndWaits: an assigned domain that is full but already
// holds one of the gang's own members is NOT stale — re-planning would split the
// gang. The gang keeps its assignment and waits for capacity in its own domain.
func TestAssignGangDomain_FullDomainHoldsOwnMember_KeepsAssignmentAndWaits(t *testing.T) {
	g := &GangPack{assignments: placement.New()}
	g.assignments.Set(placement.Gang{Group: "team/pf"}, "a", 0, nil)
	// Domain "a": one node holds a pf member (bound), a second node is full with
	// another gang's pod — so "a" cannot fit a fresh member, but pf lives here.
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gangPod("team", "pf")), // pf's own bound member
		nodeInfo(gpuNode("a2", "a", "4"), gpuPod("4")),           // occupied by another gang
		nodeInfo(gpuNode("b1", "b", "4")),                        // free elsewhere
	}
	st := newCycleState()

	_, status := g.assignGangDomain(st, nodes, gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if status.Code() != framework.Unschedulable {
		t.Fatalf("assignGangDomain status = %v, want Unschedulable while assigned domain cannot complete", status)
	}
	if d, ok := assignedDomain(g.assignments, "team/pf"); !ok || d != "a" {
		t.Fatalf("assignment = %q,%v, want a,true (must not re-plan away from a bound member)", d, ok)
	}
}

// TestPreFilter_NoReaderNoLabelOrNoTopologyKey_OnlyUnlabelledPasses verifies standalone pods continue to Filter for the
// reservation barrier, while every labeled but unresolved/invalid pod fails closed.
func TestPreFilter_NoReaderNoLabelOrNoTopologyKey_OnlyUnlabelledPasses(t *testing.T) {
	ctx := context.Background()

	nilReader := &GangPack{assignments: placement.New()} // pgReader nil -> inert
	if _, st := nilReader.PreFilter(ctx, newCycleState(), gangPod("team", "x"), nil); st.Code() != framework.Unschedulable {
		t.Fatalf("nil-reader PreFilter = %v, want Unschedulable", st)
	}

	// No pod-group label -> Success, keeping Filter active for reservation checks.
	nonMember := &GangPack{assignments: placement.New(), pgReader: fakeReader{}}
	res, st := nonMember.PreFilter(ctx, newCycleState(), gangPod("team", ""), nil)
	if res != nil || !st.IsSuccess() {
		t.Fatalf("non-member PreFilter = %v,%v, want nil,Success", res, st)
	}

	// PodGroup present but declares no topology key -> fail closed.
	noTopo := &GangPack{assignments: placement.New(), pgReader: fakeReader{"team/x": {min: 3, topo: ""}}}
	res, st = noTopo.PreFilter(ctx, newCycleState(), gangPod("team", "x"), nil)
	if res != nil || st.Code() != framework.Unschedulable {
		t.Fatalf("no-topology-key PreFilter = %v,%v, want nil,Unschedulable", res, st)
	}
}

// TestPreFilter_PodGroupNotYetReadable_IsUnschedulableNotSkipped: a pod that IS a gang member (carries the
// pod-group label) but whose PodGroup the reader can't resolve yet — the common
// informer-lag race when a workload's pods and PodGroup are created together — must
// be held (Unschedulable), NOT skipped. Skipping would let a lone member bind and
// break all-or-nothing; holding requeues it until the PodGroup appears.
func TestPreFilter_PodGroupNotYetReadable_IsUnschedulableNotSkipped(t *testing.T) {
	g := &GangPack{assignments: placement.New(), pgReader: fakeReader{}} // reader present, group absent
	res, st := g.PreFilter(context.Background(), newCycleState(), gangPod("team", "pf"), nil)
	if res != nil {
		t.Fatalf("PreFilter result = %v, want nil", res)
	}
	if st.Code() != framework.Unschedulable {
		t.Fatalf("PreFilter for uncached PodGroup = %v, want Unschedulable (hold, not Skip)", st)
	}
}

func TestPreFilter_PodGroupWithoutTopologyKey_UsesConfiguredKey(t *testing.T) {
	pod := gangGPUPod("team", "pf", "4")
	g := &GangPack{
		handle:      &fakeHandle{snapshot: []framework.NodeInfo{nodeInfo(gpuNode("n1", "a", "4"))}},
		topologyKey: testKey,
		assignments: placement.New(),
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
	if assignment := readAssignment(state); assignment == nil || assignment.topologyKey != testKey {
		t.Fatalf("assignment = %+v, want configured topology key", assignment)
	}
}

func TestPreFilter_InvalidGangConfiguration_IsUnschedulableAndUnassigned(t *testing.T) {
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
				assignments:                    placement.New(),
				pgReader:                       tc.facts,
				podGroupTopologyKeyAnnotation:  topologyKeyAnnotation,
				unsupportedPlacementGroupLabel: placementGroupLabel,
			}
			_, status := g.PreFilter(context.Background(), newCycleState(), pod, nil)
			if status.Code() != framework.Unschedulable {
				t.Fatalf("PreFilter = %v, want Unschedulable", status)
			}
			if _, assigned := assignedDomain(g.assignments, "team/pf"); assigned {
				t.Fatal("invalid gang must never be assigned")
			}
		})
	}
}

func TestPreFilter_PodGroupCarriesPlacementGroup_IsUnschedulable(t *testing.T) {
	g := &GangPack{
		assignments:                    placement.New(),
		podGroupTopologyKeyAnnotation:  topologyKeyAnnotation,
		unsupportedPlacementGroupLabel: placementGroupLabel,
		pgReader:                       fakeReader{"team/pf": {min: 2, topo: testKey, to: time.Minute, inGroup: true}},
	}
	_, status := g.PreFilter(context.Background(), newCycleState(), gangPod("team", "pf"), nil)
	if status.Code() != framework.Unschedulable {
		t.Fatalf("PreFilter = %v, want explicit unsupported placement-group rejection", status)
	}
}

func TestAssignGangDomain_SiblingNeedsLargerNode_PicksDomainFittingEveryMember(t *testing.T) {
	current := gangGPUPod("team", "pf", "4")
	current.Name = "small"
	large := gangGPUPod("team", "pf", "8")
	large.Name = "large"
	g := &GangPack{
		assignments: placement.New(),
		podLister:   fakeGangPodLister{pods: []*v1.Pod{current, large}},
	}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")), nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "8")), nodeInfo(gpuNode("b2", "b", "8")),
	}
	result, status := g.assignGangDomain(newCycleState(), nodes,
		gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, current)
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain = %v, want Success", status)
	}
	if domain, _ := assignedDomain(g.assignments, "team/pf"); domain != "b" {
		t.Fatalf("assignment = %q, want b (only domain fitting the large sibling)", domain)
	}
	if !result.NodeNames.Equal(sets.New("b1", "b2")) {
		t.Fatalf("candidates = %v, want b nodes", result.NodeNames)
	}
}

func TestAssignGangDomain_SiblingSelectsOneNode_LeavesThatNodeForIt(t *testing.T) {
	current := gangGPUPod("team", "pf", "4")
	current.Name = "flexible"
	constrained := gangGPUPod("team", "pf", "4")
	constrained.Name = "constrained"
	constrained.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": "n1"}
	n1, n2 := gpuNode("n1", "a", "4"), gpuNode("n2", "a", "4")
	n1.Labels["kubernetes.io/hostname"] = "n1"
	n2.Labels["kubernetes.io/hostname"] = "n2"
	g := &GangPack{assignments: placement.New(), podLister: fakeGangPodLister{pods: []*v1.Pod{current, constrained}}}
	result, status := g.assignGangDomain(newCycleState(), []framework.NodeInfo{nodeInfo(n1), nodeInfo(n2)},
		gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, current)
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain = %v, want Success", status)
	}
	if !result.NodeNames.Equal(sets.New("n2")) {
		t.Fatalf("flexible candidates = %v, want only n2 so n1 remains for constrained sibling", result.NodeNames)
	}
}

func TestAssignGangDomain_BoundMembersInTwoDomains_UnresolvableAndUnassigned(t *testing.T) {
	member := func() *v1.Pod {
		p := gangGPUPod("team", "pf", "4")
		return p
	}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "8"), member()),
		nodeInfo(gpuNode("b1", "b", "8"), member()),
		nodeInfo(gpuNode("a2", "a", "8")),
	}
	g := &GangPack{assignments: placement.New()}
	_, status := g.assignGangDomain(newCycleState(), nodes,
		gangInfo{key: "team/pf", minMember: 3, topologyKey: testKey}, gangGPUPod("team", "pf", "4"))
	if status.Code() != framework.UnschedulableAndUnresolvable {
		t.Fatalf("split gang status = %v, want UnschedulableAndUnresolvable", status)
	}
	if _, assigned := assignedDomain(g.assignments, "team/pf"); assigned {
		t.Fatal("split gang must not be adopted into an arbitrary domain")
	}
}

func TestAssignGangDomain_MemberTemplatesMissing_IsUnschedulable(t *testing.T) {
	current := gangGPUPod("team", "pf", "4")
	current.Name = "only"
	g := &GangPack{assignments: placement.New(), podLister: fakeGangPodLister{pods: []*v1.Pod{current}}}
	_, status := g.assignGangDomain(newCycleState(), []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")), nodeInfo(gpuNode("a2", "a", "4")),
	}, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, current)
	if status.Code() != framework.Unschedulable {
		t.Fatalf("assignGangDomain = %v, want Unschedulable until all templates are visible", status)
	}
}

// TestAssignGangDomain_OnlyRoomIsTwoSlotsOnOneNode_SteersBothMembersThere: in a packed pool the only room is
// the two slots a two-member gang vacated on one node. The rebuilt gang takes
// that domain and both members are steered to that node, the worker once the
// leader is assumed there.
func TestAssignGangDomain_OnlyRoomIsTwoSlotsOnOneNode_SteersBothMembersThere(t *testing.T) {
	leader := gangGPUPod("team", "pf", "1")
	leader.Name = "leader"
	worker := gangGPUPod("team", "pf", "1")
	worker.Name = "worker"
	g := &GangPack{assignments: placement.New(), podLister: fakeGangPodLister{pods: []*v1.Pod{leader, worker}}}
	gang := gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}
	packed := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "2")),              // the vacated room: two slots on one node
		nodeInfo(gpuNode("a2", "a", "1"), gpuPod("1")), // full
		nodeInfo(gpuNode("b1", "b", "1"), gpuPod("1")), // full
	}

	result, status := g.assignGangDomain(newCycleState(), packed, gang, leader)
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain(leader) = %v, want Success: a1 has room for both members", status)
	}
	if !result.NodeNames.Equal(sets.New("a1")) {
		t.Fatalf("leader candidates = %v, want a1 only", result.NodeNames)
	}
	if domain, _ := assignedDomain(g.assignments, "team/pf"); domain != "a" {
		t.Fatalf("assignment = %q, want a", domain)
	}

	assumedLeader := leader.DeepCopy()
	assumedLeader.Spec.NodeName = "a1"
	packed[0] = nodeInfo(gpuNode("a1", "a", "2"), assumedLeader)
	result, status = g.assignGangDomain(newCycleState(), packed, gang, worker)
	if !status.IsSuccess() {
		t.Fatalf("assignGangDomain(worker) = %v, want Success beside the assumed leader", status)
	}
	if !result.NodeNames.Equal(sets.New("a1")) {
		t.Fatalf("worker candidates = %v, want a1 only", result.NodeNames)
	}
}
