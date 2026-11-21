package gangpack

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
)

// A gang is one leader pod and its workers, and it serves only when every one
// of them runs. The tests below ask one question of the plugin: when the
// domain a gang was planned into can no longer take the rest of the gang, how
// many of its members are bound to a node? The stories about replaced members
// ask a second one: which nodes does the plugin keep from other pods for the
// gang, and what ends that? Each story runs one gang through PreFilter,
// Filter, Reserve, Permit and Unreserve on the stand-ins of
// permit_fleet_test.go, with whole-node pods on two domains that each have
// room for exactly one such gang.

// domainFullReason is how the plugin refuses a member whose gang is kept in a
// domain that cannot take the members still to place. The controller reads
// these words to end the attempt sooner.
const domainFullReason = "cannot fit all remaining gang members"

// gangShape is one gang size and where a story changes it: first members are
// placed before room is lost, kept members stay when the others are replaced.
type gangShape struct{ size, first, kept int }

// gangShapes are three gang sizes: a leader with one, three or seven
// workers.
var gangShapes = []gangShape{{size: 2, first: 1, kept: 1}, {size: 4, first: 2, kept: 2}, {size: 8, first: 3, kept: 3}}

// gangStory is one gang on a fleet of two domains. names holds the node names
// of each domain; gone holds the nodes that left the cluster.
type gangStory struct {
	*permitFleet
	size  int
	names [2][]string
	gone  map[string]bool
}

// newGangStory builds the fleet and fills every node of both domains but
// size of them with a pod of another workload.
func newGangStory(t *testing.T, size int) *gangStory {
	t.Helper()
	s := &gangStory{permitFleet: newPermitFleet(t, fleetShape{domains: 2, gangSize: size, gangs: 1}), size: size, gone: map[string]bool{}}
	for d := range s.names {
		for n := 0; n < fleetDomainNodes; n++ {
			name := s.snapshot[d*fleetDomainNodes+n].Node().Name
			s.names[d] = append(s.names[d], name)
			if n >= size {
				s.land(name)
			}
		}
	}
	return s
}

// land binds a whole-node pod that another scheduler placed to the node.
func (s *gangStory) land(node string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: fleetNamespace, Name: "other-" + node, UID: types.UID("other-" + node)},
		Spec: v1.PodSpec{Containers: []v1.Container{{Name: "main", Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{gpu: resource.MustParse("4")},
		}}}},
	}
	s.infos[node].AddPod(pod)
	return pod
}

// depart removes a pod land put on a node, so the node has room again.
func (s *gangStory) depart(t *testing.T, node string, pod *v1.Pod) {
	t.Helper()
	if err := s.infos[node].RemovePod(klog.Background(), pod); err != nil {
		t.Fatalf("removing %s from %s: %v", pod.Name, node, err)
	}
}

// leave takes the node out of the cluster: the next cycle no longer sees it.
func (s *gangStory) leave(node string) {
	s.gone[node] = true
	kept := s.snapshot[:0]
	for _, info := range s.snapshot {
		if info.Node().Name != node {
			kept = append(kept, info)
		}
	}
	s.snapshot = kept
}

// freeNode is the first node of the domain that is in the cluster and holds
// no pod.
func (s *gangStory) freeNode(t *testing.T, domain int) string {
	t.Helper()
	for _, name := range s.names[domain] {
		if !s.gone[name] && len(s.infos[name].GetPods()) == 0 {
			return name
		}
	}
	t.Fatalf("domain %d has no free node", domain)
	return ""
}

// gangDomain is the domain the plugin holds the gang in.
func (s *gangStory) gangDomain(t *testing.T) int {
	t.Helper()
	held, _, _, ok := s.plugin.assignments.GetOwned(fleetGangKey(0))
	for d := range s.names {
		if ok && held.Name == fleetDomainName(d) {
			return d
		}
	}
	t.Fatalf("the gang is held in no domain of the fleet (%+v, %v)", held, ok)
	return -1
}

// reserved is how many members the plugin still holds room for in the gang's
// domain, and whether it holds the gang in a domain at all.
func (s *gangStory) reserved() (members int, held bool) {
	_, _, members, held = s.plugin.assignments.GetOwned(fleetGangKey(0))
	return members, held
}

// schedule runs the cycle of each listed member and fails unless every one
// ends as want.
func (s *gangStory) schedule(t *testing.T, want cycleOutcome, members ...int) {
	t.Helper()
	for _, m := range members {
		if got := s.scheduleOne(s.member(0, m)); got != want {
			t.Fatalf("member %d ended its cycle in %d, want %d (0 no node, 1 at the gate, 2 bound)", m, got, want)
		}
	}
}

// scheduleWhole runs every member's cycle in order: all but the last wait at
// the gate, and the last one opens it.
func (s *gangStory) scheduleWhole(t *testing.T) {
	t.Helper()
	s.schedule(t, cycleWaiting, memberRange(0, s.size-1)...)
	s.schedule(t, cycleBound, s.size-1)
	s.wantPlaced(t, s.size, 0)
}

// refused runs PreFilter for the member and fails unless the plugin refuses it
// with the reason the controller reads.
func (s *gangStory) refused(t *testing.T, member int) {
	t.Helper()
	s.refusedWith(t, member, domainFullReason)
}

// noPlanFullReason is how the plugin refuses a member of a gang it holds no
// plan for, when the domain of the gang's placed members cannot take the
// members still to place. The controller reads these words as well.
const noPlanFullReason = "cannot fit remaining gang members"

// refusedWith runs PreFilter for the member and fails unless the plugin
// refuses it with the reason.
func (s *gangStory) refusedWith(t *testing.T, member int, reason string) {
	t.Helper()
	_, status := s.plugin.PreFilter(context.Background(), newCycleState(), s.member(0, member), s.snapshot)
	if status.IsSuccess() || !strings.Contains(status.Message(), reason) {
		t.Fatalf("member %d: PreFilter = %v, want a refusal that says %q", member, status, reason)
	}
}

// reservedReason is how the plugin's Filter turns a pod outside the gang away
// from a node the gang's plan keeps for its members.
const reservedReason = "node is reserved for a forming gang"

// outsider is a whole-node pod outside every gang that this scheduler places.
func outsider() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: fleetNamespace, Name: "outsider", UID: "outsider"},
		Spec: v1.PodSpec{Containers: []v1.Container{{Name: "main", Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{gpu: resource.MustParse("4")},
		}}}},
	}
}

// freeNodes lists the nodes of the domain that are in the cluster and hold no
// pod.
func (s *gangStory) freeNodes(domain int) []string {
	var free []string
	for _, name := range s.names[domain] {
		if !s.gone[name] && len(s.infos[name].GetPods()) == 0 {
			free = append(free, name)
		}
	}
	return free
}

// wantNoRoomHeld fails unless the plugin holds no room for the gang: its plan
// keeps no member's place, and Filter lets a pod outside the gang take every
// free node of the domain, of which there must be one at least.
func (s *gangStory) wantNoRoomHeld(t *testing.T, domain int) {
	t.Helper()
	if members, _ := s.reserved(); members != 0 {
		t.Fatalf("the plugin holds room for %d member(s) of a gang it refuses, want none", members)
	}
	free := s.freeNodes(domain)
	if len(free) == 0 {
		t.Fatalf("domain %d has no free node to ask about", domain)
	}
	for _, name := range free {
		if status := s.plugin.Filter(context.Background(), newCycleState(), outsider(), s.entries[name]); !status.IsSuccess() {
			t.Fatalf("free node %s is closed to a pod outside the gang: %q", name, status.Message())
		}
	}
}

// wantRoomHeld fails unless the plugin holds room for that many members and
// Filter turns a pod outside the gang away from every free node of the
// domain, with the words an operator reads on that pod.
func (s *gangStory) wantRoomHeld(t *testing.T, domain, members int) {
	t.Helper()
	if got, held := s.reserved(); !held || got != members {
		t.Fatalf("room held for %d member(s) (held %v), want %d", got, held, members)
	}
	for _, name := range s.freeNodes(domain) {
		status := s.plugin.Filter(context.Background(), newCycleState(), outsider(), s.entries[name])
		if status.IsSuccess() || !strings.Contains(status.Message(), reservedReason) {
			t.Fatalf("free node %s: Filter = %v for a pod outside the gang, want a refusal that says %q", name, status, reservedReason)
		}
	}
}

// toGate runs the member's cycle, fails unless the gate holds the member, and
// returns how long the gate tells the framework to hold it.
func (s *gangStory) toGate(t *testing.T, member int) time.Duration {
	t.Helper()
	pod := s.member(0, member)
	state, node := s.reserveOne(pod)
	if node == "" {
		t.Fatalf("member %d found no node", member)
	}
	status, wait := s.plugin.Permit(context.Background(), state, pod, node)
	if !status.IsWait() {
		t.Fatalf("member %d: Permit = %v, want the gate to hold it", member, status)
	}
	s.handle.waiting[pod.UID] = &fleetWaitingPod{pod: pod, state: state, node: node}
	return wait
}

// gangTimeout is the timeout the gang's PodGroup carries.
func (s *gangStory) gangTimeout() time.Duration {
	return s.plugin.pgReader.(fleetReader)[fleetGangKey(0)].timeout
}

// restart is a scheduler restart: the plugin starts with an empty memory, and
// every member that waited at the gate is an unplaced pod again. Bound
// members stay on their nodes.
func (s *gangStory) restart() {
	for uid, waiting := range s.handle.waiting {
		s.unassume(waiting.pod)
		delete(s.handle.waiting, uid)
	}
	s.handle = &fleetHandle{reads: &s.reads, waiting: map[types.UID]*fleetWaitingPod{}}
	s.plugin = &GangPack{
		handle: s.handle, topologyKey: testKey, standaloneDomainPacking: true,
		assignments: placement.New(), pgReader: s.plugin.pgReader, podLister: s.lister(),
	}
}

// wantPlaced fails unless bound members are bound to a node and waiting
// members wait at the gate.
func (s *gangStory) wantPlaced(t *testing.T, bound, waiting int) {
	t.Helper()
	if len(s.bound) != bound || len(s.handle.waiting) != waiting {
		t.Fatalf("%d member(s) bound and %d at the gate, want %d bound and %d at the gate", len(s.bound), len(s.handle.waiting), bound, waiting)
	}
}

// gateTimesOut ends the wait of one member at the gate the way the framework
// does when the gang's timeout passes, and finishes the cycles of every member
// that rejection sends back.
func (s *gangStory) gateTimesOut(t *testing.T) {
	t.Helper()
	var first *fleetWaitingPod
	for _, waiting := range s.handle.waiting {
		if first == nil || waiting.pod.Name < first.pod.Name {
			first = waiting
		}
	}
	if first == nil {
		t.Fatal("no member waits at the gate")
	}
	first.Reject(Name, "the gang's timeout passed")
	for rounds := 0; s.anyReleased(); rounds++ {
		if rounds > s.size {
			t.Fatalf("the gate still holds answered members after %d rounds", rounds)
		}
		s.finishReleased()
	}
}

// anyReleased reports whether the gate has answered a member whose cycle is
// not finished yet.
func (s *gangStory) anyReleased() bool {
	for _, waiting := range s.handle.waiting {
		if waiting.allowed || waiting.rejected {
			return true
		}
	}
	return false
}

// replace deletes the listed members and creates them again under their
// names: each leaves its node, and a pod with a new UID takes its place in
// the gang.
func (s *gangStory) replace(members ...int) {
	key := fleetGangKey(0)
	for _, m := range members {
		old := s.byGang[key][m]
		s.unassume(old)
		delete(s.bound, old.UID)
		created := old.DeepCopy()
		created.UID = types.UID(fmt.Sprintf("%s-again", old.UID))
		created.DeletionTimestamp = nil
		s.byGang[key][m] = created
		for i, pod := range s.pods {
			if pod == old {
				s.pods[i] = created
			}
		}
	}
}

// collect runs the plugin's periodic release of gangs with no pod left, at a
// moment when every pod of the gang is being deleted.
func (s *gangStory) collect() {
	now := metav1.Now()
	for _, pod := range s.byGang[fleetGangKey(0)] {
		pod.DeletionTimestamp = &now
	}
	s.plugin.gcAssignments()
}

// memberRange lists the member indexes from, from+1, ... up to but not
// including to.
func memberRange(from, to int) []int {
	out := make([]int, 0, to-from)
	for m := from; m < to; m++ {
		out = append(out, m)
	}
	return out
}

// roomLoss is one way the domain of a forming gang loses a free node.
type roomLoss struct {
	name string
	lose func(t *testing.T, s *gangStory, domain int)
}

// roomLosses are the losses the stories run. Neither is this scheduler's own
// doing, so the plugin learns of each from the node list of the next cycle.
var roomLosses = []roomLoss{
	{"a pod of another scheduler lands", func(t *testing.T, s *gangStory, domain int) { s.land(s.freeNode(t, domain)) }},
	{"a node leaves the cluster", func(t *testing.T, s *gangStory, domain int) { s.leave(s.freeNode(t, domain)) }},
}

// formsThenLosesRoom takes a gang with no member placed through the loss of
// room. Its first members are placed and wait at the gate, then the gang's
// domain loses a free node the rest need. The plugin refuses the next member,
// binds nobody and keeps the room it still holds for the gang. When the gang's
// timeout passes, every waiting member goes back and the gang is held in no
// domain. Planned again, the gang binds whole in the other domain, which had
// room all along.
func (s *gangStory) formsThenLosesRoom(t *testing.T, shape gangShape, loss roomLoss) {
	s.schedule(t, cycleWaiting, memberRange(0, shape.first)...)
	first := s.gangDomain(t)
	loss.lose(t, s, first)

	s.refused(t, shape.first)
	s.wantPlaced(t, 0, shape.first)
	if members, held := s.reserved(); !held || members != shape.size-shape.first {
		t.Fatalf("room held for %d member(s) (held %v), want %d", members, held, shape.size-shape.first)
	}

	s.gateTimesOut(t)
	s.wantPlaced(t, 0, 0)
	if _, held := s.reserved(); held {
		t.Fatal("the gang is still held in its domain after its timeout passed")
	}

	s.scheduleWhole(t)
	if second := s.gangDomain(t); second == first {
		t.Fatalf("the gang bound in domain %d again, want the domain with room for all of it", second)
	}
}

// TestGangFormingInADomainThatLosesRoom: a new gang whose domain loses room
// after its first members were placed binds nobody there. It binds whole in
// the other domain once its timeout has passed (see formsThenLosesRoom).
func TestGangFormingInADomainThatLosesRoom(t *testing.T) {
	for _, shape := range gangShapes {
		for _, loss := range roomLosses {
			t.Run(fmt.Sprintf("gang of %d/%s", shape.size, loss.name), func(t *testing.T) {
				newGangStory(t, shape.size).formsThenLosesRoom(t, shape, loss)
			})
		}
	}
}

// TestGangWhoseLastMembersAreHeldBack: the first members of a new gang wait at
// the gate and the others never reach the scheduler. The plugin binds nobody
// and keeps every node of the domain from other pods. When the gang's timeout
// passes, the waiting members go back and the domain is open again.
func TestGangWhoseLastMembersAreHeldBack(t *testing.T) {
	for _, shape := range gangShapes {
		t.Run(fmt.Sprintf("gang of %d", shape.size), func(t *testing.T) {
			s := newGangStory(t, shape.size)
			s.schedule(t, cycleWaiting, memberRange(0, shape.first)...)
			free := s.freeNode(t, s.gangDomain(t))
			node := s.infos[free].Node()
			if !s.plugin.assignments.BlocksNode(node.Name, node.Labels) {
				t.Fatalf("node %s of the gang's domain is open to other pods while the gang forms", free)
			}

			s.gateTimesOut(t)
			s.wantPlaced(t, 0, 0)
			if _, held := s.reserved(); held || s.plugin.assignments.BlocksNode(node.Name, node.Labels) {
				t.Fatalf("the gang still holds its domain after its timeout passed (held %v)", held)
			}
		})
	}
}

// TestGangRebuiltUnderItsPodGroup: a gang binds whole, then every pod of it is
// deleted and created again under the same PodGroup, as a restart of the whole
// gang does. The plugin's plan for the gang says every member landed, so it
// holds no room, yet no member is placed any more. The plugin drops that plan
// and plans the rebuilt gang as a new one: nobody is bound before all of it
// has a node, whatever its domain loses meanwhile (see formsThenLosesRoom).
func TestGangRebuiltUnderItsPodGroup(t *testing.T) {
	for _, shape := range gangShapes {
		for _, loss := range roomLosses {
			t.Run(fmt.Sprintf("gang of %d/%s", shape.size, loss.name), func(t *testing.T) {
				s := newGangStory(t, shape.size)
				s.scheduleWhole(t)
				s.replace(memberRange(0, shape.size)...)
				s.formsThenLosesRoom(t, shape, loss)
			})
		}
	}
}

// TestGangRebuiltAfterTheCollectorRan: the same rebuild, but the plugin's
// periodic release ran while every pod of the gang was being deleted. The
// rebuilt gang is planned as a new one: its first members wait at the gate
// and nobody is bound.
func TestGangRebuiltAfterTheCollectorRan(t *testing.T) {
	for _, shape := range gangShapes {
		t.Run(fmt.Sprintf("gang of %d", shape.size), func(t *testing.T) {
			s := newGangStory(t, shape.size)
			s.scheduleWhole(t)
			s.collect()
			s.replace(memberRange(0, shape.size)...)

			s.schedule(t, cycleWaiting, memberRange(0, shape.first)...)
			s.wantPlaced(t, 0, shape.first)
			if members, held := s.reserved(); !held || members != shape.size-shape.first {
				t.Fatalf("room held for %d member(s) (held %v), want %d", members, held, shape.size-shape.first)
			}
		})
	}
}

// TestGangWhoseMembersAreReplaced: a gang binds whole, then several of its
// members are deleted and created again while the others keep running, as
// happens when kubelets refuse pods the scheduler has just bound.
//
// The plugin's plan says every member landed, so the plugin drops it and plans
// the gang again in the domain of the members it kept. The first replacement
// waits at the gate, with room held for the other replacements. A pod of
// another scheduler then takes a free node: the plugin refuses the next
// replacement and binds none. When the node is free again the replacements
// are placed one by one, and they bind together once the last has a node.
func TestGangWhoseMembersAreReplaced(t *testing.T) {
	for _, shape := range gangShapes {
		if shape.size-shape.kept < 2 {
			continue
		}
		t.Run(fmt.Sprintf("gang of %d keeps %d", shape.size, shape.kept), func(t *testing.T) {
			s := newGangStory(t, shape.size)
			s.scheduleWhole(t)
			domain := s.gangDomain(t)
			s.replace(memberRange(shape.kept, shape.size)...)

			s.schedule(t, cycleWaiting, shape.kept)
			s.wantPlaced(t, shape.kept, 1)
			if members, held := s.reserved(); !held || members != shape.size-shape.kept-1 {
				t.Fatalf("room held for %d member(s) (held %v), want %d", members, held, shape.size-shape.kept-1)
			}

			node := s.freeNode(t, domain)
			other := s.land(node)
			s.refused(t, shape.kept+1)
			s.wantPlaced(t, shape.kept, 1)

			s.depart(t, node, other)
			s.schedule(t, cycleWaiting, memberRange(shape.kept+1, shape.size-1)...)
			s.schedule(t, cycleBound, shape.size-1)
			s.wantPlaced(t, shape.size, 0)
		})
	}
}

// TestGangThatLosesAKeptMemberWhileReplacementsWait: several members of a
// bound gang are replaced and the first replacement waits at the gate. One of
// the members the gang kept is then deleted and created again. The plan
// counted that member as placed when it was made, so it holds room for the
// replacements only. The gate opens when the last of them has a node: all but
// one member are bound while that one is not placed. It then binds alone, the
// way a gang short of one member takes it back.
func TestGangThatLosesAKeptMemberWhileReplacementsWait(t *testing.T) {
	for _, shape := range gangShapes {
		if shape.size-shape.kept < 2 {
			continue
		}
		t.Run(fmt.Sprintf("gang of %d keeps %d", shape.size, shape.kept), func(t *testing.T) {
			s := newGangStory(t, shape.size)
			s.scheduleWhole(t)
			s.replace(memberRange(shape.kept, shape.size)...)
			s.schedule(t, cycleWaiting, shape.kept)

			s.replace(0)
			s.schedule(t, cycleWaiting, memberRange(shape.kept+1, shape.size-1)...)
			s.schedule(t, cycleBound, shape.size-1)
			s.wantPlaced(t, shape.size-1, 0)

			s.schedule(t, cycleBound, 0)
			s.wantPlaced(t, shape.size, 0)
		})
	}
}

// TestGangShortOfOneMemberTakesItBack: one member of a bound gang is deleted
// and created again. The gang is whole as soon as that member lands, so the
// plugin binds it at once: it does not wait at the gate, and the gang keeps
// the plan it has.
func TestGangShortOfOneMemberTakesItBack(t *testing.T) {
	for _, shape := range gangShapes {
		t.Run(fmt.Sprintf("gang of %d", shape.size), func(t *testing.T) {
			s := newGangStory(t, shape.size)
			s.scheduleWhole(t)
			_, plan, _, _ := s.plugin.assignments.GetOwned(fleetGangKey(0))
			s.replace(shape.size - 1)

			s.schedule(t, cycleBound, shape.size-1)
			s.wantPlaced(t, shape.size, 0)
			if _, now, _, _ := s.plugin.assignments.GetOwned(fleetGangKey(0)); now != plan {
				t.Fatalf("the gang was planned again for one member: plan %d, then %d", plan, now)
			}
		})
	}
}

// eachGangWithMembersReplaced runs the story on every gang shape that can
// lose two members or more while others stay. The story starts from a gang
// that bound whole and then had all but its kept members deleted and created
// again, and it gets the domain the gang sits in.
func eachGangWithMembersReplaced(t *testing.T, story func(t *testing.T, s *gangStory, shape gangShape, domain int)) {
	for _, shape := range gangShapes {
		if shape.size-shape.kept < 2 {
			continue
		}
		t.Run(fmt.Sprintf("gang of %d keeps %d", shape.size, shape.kept), func(t *testing.T) {
			s := newGangStory(t, shape.size)
			s.scheduleWhole(t)
			domain := s.gangDomain(t)
			s.replace(memberRange(shape.kept, shape.size)...)
			story(t, s, shape, domain)
		})
	}
}

// TestGangThatCannotPlaceItsReplacementsHoldsNoRoom: several members of a
// bound gang are replaced, and a pod of another scheduler takes one of the
// nodes they left. The domain has room for all the replacements but one. The
// plugin refuses each replacement and holds no room for them: every free node
// of the domain stays open to a pod outside the gang. That is so while the
// plugin still has the gang's plan, after its periodic release ran, and after
// a scheduler restart, when it has no plan for the gang. Once the node is
// free again the replacements wait at the gate and bind together.
func TestGangThatCannotPlaceItsReplacementsHoldsNoRoom(t *testing.T) {
	eachGangWithMembersReplaced(t, func(t *testing.T, s *gangStory, shape gangShape, domain int) {
		node := s.freeNode(t, domain)
		other := s.land(node)

		s.refused(t, shape.kept)
		s.wantNoRoomHeld(t, domain)
		s.plugin.gcAssignments()
		s.refused(t, shape.kept+1)
		s.wantNoRoomHeld(t, domain)
		if _, held := s.reserved(); !held {
			t.Fatal("the plugin dropped the plan of a gang whose replacements it refuses")
		}

		s.restart()
		s.refusedWith(t, shape.kept, noPlanFullReason)
		s.wantNoRoomHeld(t, domain)

		s.depart(t, node, other)
		s.schedule(t, cycleWaiting, memberRange(shape.kept, shape.size-1)...)
		s.schedule(t, cycleBound, shape.size-1)
		s.wantPlaced(t, shape.size, 0)
	})
}

// TestGangWhoseReplacementWaitsHoldsRoomUntilItsTimeout: several members of a
// bound gang are replaced and the domain has room for all of them. The first
// replacement waits at the gate for the gang's timeout, the one its PodGroup
// carries. While it waits the plugin holds room for the other replacements
// and keeps every free node of the domain from pods outside the gang. A pod of
// another scheduler then takes a free node, and the plugin refuses the next
// replacement. When the timeout passes, the waiting replacement goes back and
// the plugin holds nothing: the gang cannot place its replacements, so every
// free node of the domain is open again and stays open.
func TestGangWhoseReplacementWaitsHoldsRoomUntilItsTimeout(t *testing.T) {
	eachGangWithMembersReplaced(t, func(t *testing.T, s *gangStory, shape gangShape, domain int) {
		others := shape.size - shape.kept - 1
		if wait := s.toGate(t, shape.kept); wait != s.gangTimeout() {
			t.Fatalf("the gate holds the replacement for %s, want the gang's timeout of %s", wait, s.gangTimeout())
		}
		s.wantRoomHeld(t, domain, others)

		s.land(s.freeNode(t, domain))
		s.refused(t, shape.kept+1)
		s.wantPlaced(t, shape.kept, 1)
		s.wantRoomHeld(t, domain, others)

		s.gateTimesOut(t)
		s.wantPlaced(t, shape.kept, 0)
		s.wantNoRoomHeld(t, domain)
		s.refusedWith(t, shape.kept, noPlanFullReason)
		s.wantNoRoomHeld(t, domain)
	})
}

// TestGangRebuiltAfterItsReplacementsCouldNotFit: several members of a bound
// gang are replaced and cannot all be placed, so the plugin refuses them and
// holds no room. The whole gang is then deleted and created again, and its
// domain has room for all of it. The plugin plans the rebuilt gang as a new
// one: its members wait at the gate until the last has a node, and they bind
// together.
func TestGangRebuiltAfterItsReplacementsCouldNotFit(t *testing.T) {
	eachGangWithMembersReplaced(t, func(t *testing.T, s *gangStory, shape gangShape, domain int) {
		node := s.freeNode(t, domain)
		other := s.land(node)
		s.refused(t, shape.kept)

		s.depart(t, node, other)
		s.replace(memberRange(0, shape.size)...)
		s.scheduleWhole(t)
	})
}

// TestGangPlannedAgainRequeuesOnlyItsOwnMembers: a forming gang holds a
// domain, and its reservation has turned a pod outside every gang away. A
// second gang that bound whole is then rebuilt under its PodGroup. The plan
// the plugin drops for the rebuilt gang held no room, so the pod that waits
// on the first gang's reservation is not requeued. The pass requeues members
// of the rebuilt gang and nobody else.
func TestGangPlannedAgainRequeuesOnlyItsOwnMembers(t *testing.T) {
	s := &gangStory{permitFleet: newPermitFleet(t, fleetShape{domains: 2, gangSize: 2, gangs: 2}), size: 2, gone: map[string]bool{}}
	s.scheduleWhole(t)
	if got := s.scheduleOne(s.member(1, 0)); got != cycleWaiting {
		t.Fatalf("the first member of the forming gang ended its cycle in %d, want it at the gate", got)
	}
	turnedAway := false
	for _, entry := range s.snapshot {
		status := s.plugin.Filter(context.Background(), newCycleState(), outsider(), entry)
		turnedAway = turnedAway || strings.Contains(status.Message(), reservedReason)
	}
	if !turnedAway {
		t.Fatal("the forming gang's reservation turned the pod outside every gang away from no node")
	}

	s.replace(0, 1)
	s.handle.activated = nil
	s.schedule(t, cycleWaiting, 0)
	for _, pod := range s.handle.activated {
		if pod.Labels[podGroupLabel] != fleetGangName(0) {
			t.Fatalf("the pass of the rebuilt gang requeued %s, a pod its plan never kept out", pod.Name)
		}
	}
}
