package gangpack

import (
	"errors"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// fakeGangPodLister stands in for the informer-backed live-gang lister.
type fakeGangPodLister struct {
	keys map[string]bool
	pods []*v1.Pod
	err  error
}

func (f fakeGangPodLister) liveGangKeys() (map[string]bool, error) { return f.keys, f.err }
func (f fakeGangPodLister) gangPods(namespace, name string) ([]*v1.Pod, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []*v1.Pod
	for _, pod := range f.pods {
		ns, pg, ok := podGroupNameOf(pod)
		if ok && ns == namespace && pg == name {
			out = append(out, pod)
		}
	}
	return out, nil
}

// TestGC_GangWithNoLivePods_ReleasesItsAssignment: gcAssignments releases the
// assignments of gangs with no live pods and keeps those that still have some.
func TestGC_GangWithNoLivePods_ReleasesItsAssignment(t *testing.T) {
	assignments := placement.New()
	assignments.Set(placement.Gang{Group: "ns/live"}, "a", 0, nil)
	assignments.Set(placement.Gang{Group: "ns/dead"}, "b", 0, nil)
	g := &GangPack{assignments: assignments, podLister: fakeGangPodLister{keys: map[string]bool{"ns/live": true}}}

	g.gcAssignments()

	if _, ok := assignedDomain(assignments, "ns/dead"); ok {
		t.Fatal("assignment for a gang with no live pods should be GC'd")
	}
	if _, ok := assignedDomain(assignments, "ns/live"); !ok {
		t.Fatal("assignment for a gang that still has pods must be kept")
	}
}

// TestGC_LiveGangWithNoMemberPlaced_KeepsFullReservation: a gang assigned to an empty domain with zero
// members placed is still legitimately forming — its full reservation is what
// stops a second gang racing into the same domain. Reconcile sets remaining to
// minMember-placed = minMember, i.e. it leaves the reservation intact. This is the
// deliberate boundary: reconcile reclaims only the reservation NOT backed by real
// remaining need; a still-forming gang keeps its whole claim.
func TestGC_LiveGangWithNoMemberPlaced_KeepsFullReservation(t *testing.T) {
	assignments := placement.New()
	if d, ok := choose(assignments, "team/pf", topology.FreeByDomain{"a": 3}, 3); !ok || d != "a" {
		t.Fatalf("precondition Choose = %q,%v want a,true", d, ok)
	}

	// Snapshot: "a" has 3 nodes, none holding a pf member (A never placed).
	snapshot := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")),
		nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("a3", "a", "4")),
	}
	g := &GangPack{
		assignments: assignments,
		handle:      &fakeHandle{snapshot: snapshot},
		pgReader:    fakeReader{"team/pf": {min: 3, topo: testKey}},
		podLister:   fakeGangPodLister{keys: map[string]bool{"team/pf": true}}, // still live
	}

	g.gcAssignments()

	// A justifies its full reservation (0 placed of 3), so a second gang must not be
	// able to take A's domain — A may yet form there.
	if _, ok := choose(assignments, "team/other", topology.FreeByDomain{"a": 3}, 3); ok {
		t.Fatal("with zero members placed, A's reservation is legitimate and must be preserved")
	}
}

// TestGCDoesNotReadOrRewriteLiveReservations: lifecycle callbacks own
// reservation accounting. GC only reaps dead gang keys and must not consult the
// framework snapshot from its background goroutine.
func TestGC_LiveGangWithClaims_LeavesReservationsUntouched(t *testing.T) {
	assignments := placement.New()
	// Gang A took "a" reserving 3, but only its actual placement matters now.
	choose(assignments, "team/pf", topology.FreeByDomain{"a": 5}, 3)

	g := &GangPack{
		assignments: assignments,
		podLister:   fakeGangPodLister{keys: map[string]bool{"team/pf": true}},
	}

	g.gcAssignments()

	// GC preserves all three claims; Reserve is the only path that converts them
	// into assumed-pod occupancy.
	if _, ok := choose(assignments, "team/other", topology.FreeByDomain{"a": 3}, 2); ok {
		t.Fatal("other gang must remain blocked while A has one outstanding member")
	}
	if _, _, remaining, _ := assignments.GetOwned("team/pf"); assignments.Len() != 1 || remaining != 3 {
		t.Fatalf("reservations = %d groups, %d remaining, want one group with three untouched claims", assignments.Len(), remaining)
	}
}

// TestGC_NoPodGroupReader_KeepsAssignmentWithoutPanic / snapshot: the reconcile is inert when its
// inputs are missing, so a plugin without a wired reader never panics.
func TestGC_NoPodGroupReader_KeepsAssignmentWithoutPanic(t *testing.T) {
	assignments := placement.New()
	choose(assignments, "team/pf", topology.FreeByDomain{"a": 3}, 3)
	g := &GangPack{assignments: assignments, podLister: fakeGangPodLister{keys: map[string]bool{"team/pf": true}}}
	g.gcAssignments() // pgReader nil -> reconcile no-op, no panic
	if _, ok := assignedDomain(assignments, "team/pf"); !ok {
		t.Fatal("assignment must survive a no-op reconcile")
	}
}

// TestGC_ListerError_ReleasesNothing: a transient lister error must NOT release
// anything (a partial view could reap live gangs) — the next tick retries.
func TestGC_ListerError_ReleasesNothing(t *testing.T) {
	assignments := placement.New()
	assignments.Set(placement.Gang{Group: "ns/x"}, "a", 0, nil)
	g := &GangPack{assignments: assignments, podLister: fakeGangPodLister{err: errors.New("cache not synced")}}

	g.gcAssignments()

	if _, ok := assignedDomain(assignments, "ns/x"); !ok {
		t.Fatal("must not release assignments on a lister error")
	}
}

// TestLiveGangKeys_MixedPods_DedupesMembersAndSkipsUnlabelled: the informer lister maps pods to gang keys, deduping members
// and ignoring pods with no pod-group label.
func TestLiveGangKeys_MixedPods_DedupesMembersAndSkipsUnlabelled(t *testing.T) {
	l := &informerPodLister{
		list: func() ([]*v1.Pod, error) {
			return []*v1.Pod{
				gangPod("team", "pf"),     // member
				gangPod("team", "pf"),     // sibling -> same key
				gangPod("team", "decode"), // other gang
				gangPod("team", ""),       // no label -> ignored
			}, nil
		},
	}
	keys, err := l.liveGangKeys()
	if err != nil {
		t.Fatalf("liveGangKeys: %v", err)
	}
	if len(keys) != 2 || !keys["team/pf"] || !keys["team/decode"] {
		t.Fatalf("keys = %v, want {team/pf, team/decode}", keys)
	}
}

func TestLiveGangKeys_TerminalOrDeletingPods_LeavesThemOut(t *testing.T) {
	deleting := gangPod("team", "deleting")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	succeeded := gangPod("team", "succeeded")
	succeeded.Status.Phase = v1.PodSucceeded
	failed := gangPod("team", "failed")
	failed.Status.Phase = v1.PodFailed
	l := &informerPodLister{list: func() ([]*v1.Pod, error) {
		return []*v1.Pod{deleting, succeeded, failed, gangPod("team", "live")}, nil
	}}
	keys, err := l.liveGangKeys()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || !keys["team/live"] {
		t.Fatalf("live keys = %v, want only team/live", keys)
	}
}

func TestGC_PodGroupRecreatedWithNewUID_ReleasesOldAssignment(t *testing.T) {
	assignments := placement.New()
	if _, _, ok := assignments.Choose(placement.Gang{Group: "team/pf", Owner: "old-uid", TopologyKey: testKey},
		topology.FreeByDomain{"a": 1}, map[string][]string{"a": {"a1"}}, 1); !ok {
		t.Fatal("failed to establish old owner")
	}
	g := &GangPack{
		assignments: assignments,
		podLister:   fakeGangPodLister{keys: map[string]bool{"team/pf": true}},
		pgReader:    fakeReader{"team/pf": {min: 1, topo: testKey, uid: "new-uid"}},
	}
	g.gcAssignments()
	if _, ok := assignedDomain(assignments, "team/pf"); ok {
		t.Fatal("same-name assignment from old PodGroup UID survived GC")
	}
}

func TestGC_GangWithNoPodsHasPermitWaiter_RejectsTheWaiter(t *testing.T) {
	assignments := placement.New()
	_, token, ok := assignments.Choose(placement.Gang{Group: "team/pf", Owner: "pg-uid", TopologyKey: testKey},
		topology.FreeByDomain{"a": 1}, map[string][]string{"a": {"a1"}}, 1)
	if !ok {
		t.Fatal("failed to establish commitment")
	}
	pod := gangPod("team", "pf")
	pod.Name = "waiter"
	waiter := &fakeWaitingPod{pod: pod}
	g := &GangPack{
		assignments: assignments, handle: &fakeHandle{waiting: []framework.WaitingPod{waiter}},
		podLister: fakeGangPodLister{keys: map[string]bool{}},
	}
	g.rememberAttempt(pod, token)
	g.gcAssignments()
	if !waiter.rejected {
		t.Fatal("GC released terminal gang assignment without rejecting its Permit waiter")
	}
}
