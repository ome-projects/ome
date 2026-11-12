package gangpack

import (
	"errors"
	"slices"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kube-scheduler/framework"
)

// templateGang is the gang team/pf wanting minMember members.
func templateGang(minMember int) gangInfo {
	return gangInfo{key: "team/pf", minMember: minMember, topologyKey: testKey}
}

// placedMembers is what the snapshot holds of the gang: count members
// already on nodes, the named ones found there by name.
func placedMembers(count int, names ...string) boundGangPlacement {
	placed := make(map[string]*v1.Pod, len(names))
	for _, name := range names {
		placed[name] = namedMember(name)
	}
	return boundGangPlacement{count: count, placed: placed}
}

func templateNames(templates []*v1.Pod) []string {
	names := make([]string, 0, len(templates))
	for _, template := range templates {
		names = append(names, template.Name)
	}
	return names
}

// TestGangTemplatesWithoutAListerCopyTheCurrentMember holds that without a
// pod lister every member still to place is a copy of the current one, and
// that the current member always counts, so at least one is needed.
func TestGangTemplatesWithoutAListerCopyTheCurrentMember(t *testing.T) {
	current := namedMember("current")
	for _, tt := range []struct {
		name      string
		minMember int
		placed    int
		wantNeed  int
	}{
		{name: "three members still to place", minMember: 4, placed: 1, wantNeed: 3},
		{name: "every member placed but the current one", minMember: 3, placed: 2, wantNeed: 1},
		{name: "as many members placed as the gang wants", minMember: 2, placed: 2, wantNeed: 1},
		{name: "more members placed than the gang wants", minMember: 2, placed: 5, wantNeed: 1},
		{name: "a gang that wants no member", minMember: 0, placed: 0, wantNeed: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := &GangPack{}
			templates, need, status := g.gangTemplates(templateGang(tt.minMember), current, placedMembers(tt.placed))
			if status != nil {
				t.Fatalf("status = %v, want none", status)
			}
			if need != tt.wantNeed || len(templates) != tt.wantNeed {
				t.Fatalf("need = %d, templates = %d, want %d of each", need, len(templates), tt.wantNeed)
			}
			for i, template := range templates {
				if template != current {
					t.Fatalf("template %d = %v, want the current member", i, template)
				}
			}
		})
	}
}

// TestGangTemplatesListTheMembersStillToPlace holds which listed members are
// templates: the current member first, then its siblings in name order,
// without a sibling that is bound to a node, that the snapshot already holds,
// that is being deleted or that has finished; and how many the gang needs
// beside the members already placed.
func TestGangTemplatesListTheMembersStillToPlace(t *testing.T) {
	current := namedMember("current")
	a, b, c := namedMember("a"), namedMember("b"), namedMember("c")
	bound := namedMember("bound")
	bound.Spec.NodeName = "n1"
	leaving := namedMember("leaving")
	leaving.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	finished := namedMember("finished")
	finished.Status.Phase = v1.PodSucceeded
	failed := namedMember("failed")
	failed.Status.Phase = v1.PodFailed
	running := namedMember("running")
	running.Status.Phase = v1.PodRunning
	listedAgain := namedMember("current")
	listedAgain.UID = "another-uid"
	for _, tt := range []struct {
		name      string
		minMember int
		placed    boundGangPlacement
		members   []*v1.Pod
		wantNames []string
		wantNeed  int
	}{
		{name: "the current member first, the siblings in name order", minMember: 4, placed: placedMembers(0),
			members: []*v1.Pod{current, c, a, b}, wantNames: []string{"current", "a", "b", "c"}, wantNeed: 4},
		{name: "a sibling the snapshot holds is left out and lowers the need", minMember: 3, placed: placedMembers(1, "a"),
			members: []*v1.Pod{current, a, b, c}, wantNames: []string{"current", "b", "c"}, wantNeed: 2},
		{name: "a sibling bound to a node is left out", minMember: 2, placed: placedMembers(0),
			members: []*v1.Pod{current, bound, a}, wantNames: []string{"current", "a"}, wantNeed: 2},
		{name: "a sibling being deleted is left out", minMember: 2, placed: placedMembers(0),
			members: []*v1.Pod{current, leaving, a}, wantNames: []string{"current", "a"}, wantNeed: 2},
		{name: "a sibling that has finished is left out", minMember: 2, placed: placedMembers(0),
			members: []*v1.Pod{current, finished, failed, a}, wantNames: []string{"current", "a"}, wantNeed: 2},
		{name: "a running sibling without a node is kept", minMember: 2, placed: placedMembers(0),
			members: []*v1.Pod{running, current}, wantNames: []string{"current", "running"}, wantNeed: 2},
		{name: "the current member listed again is not a second template", minMember: 2, placed: placedMembers(0),
			members: []*v1.Pod{current, listedAgain, a}, wantNames: []string{"current", "a"}, wantNeed: 2},
		{name: "more siblings than the gang needs are all kept", minMember: 2, placed: placedMembers(0),
			members: []*v1.Pod{current, a, b}, wantNames: []string{"current", "a", "b"}, wantNeed: 2},
		{name: "exactly the members the gang needs", minMember: 3, placed: placedMembers(0),
			members: []*v1.Pod{current, a, b}, wantNames: []string{"current", "a", "b"}, wantNeed: 3},
		{name: "members placed beyond the gang's wish leave one to place", minMember: 2, placed: placedMembers(3),
			members: []*v1.Pod{current, a}, wantNames: []string{"current", "a"}, wantNeed: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := &GangPack{podLister: fakeGangPodLister{pods: tt.members}}
			templates, need, status := g.gangTemplates(templateGang(tt.minMember), current, tt.placed)
			if status != nil {
				t.Fatalf("status = %v, want none", status)
			}
			if got := templateNames(templates); !slices.Equal(got, tt.wantNames) {
				t.Fatalf("templates = %v, want %v", got, tt.wantNames)
			}
			if need != tt.wantNeed {
				t.Fatalf("need = %d, want %d", need, tt.wantNeed)
			}
			if g.hasTemplatesIncomplete(templateGang(tt.minMember)) {
				t.Fatal("a complete set left an incomplete-set record")
			}
		})
	}
}

// TestGangTemplatesHoldAMemberWhoseSetIsShort holds that a member whose
// listed set is short of the need waits with one message, returns no
// templates, and leaves the record that the set was short.
func TestGangTemplatesHoldAMemberWhoseSetIsShort(t *testing.T) {
	current, a := namedMember("current"), namedMember("a")
	for _, tt := range []struct {
		name    string
		gang    gangInfo
		members []*v1.Pod
		want    string
	}{
		{name: "one sibling short", gang: templateGang(3), members: []*v1.Pod{current, a},
			want: "waiting for all PodGroup member templates for team/pf"},
		{name: "a lone member of a pair", gang: templateGang(2), members: []*v1.Pod{current},
			want: "waiting for all PodGroup member templates for team/pf"},
		{name: "a gang key without a namespace lists no siblings", gang: gangInfo{key: "pf", minMember: 2, topologyKey: testKey},
			members: []*v1.Pod{current, a}, want: "waiting for all PodGroup member templates for pf"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := &GangPack{podLister: fakeGangPodLister{pods: tt.members}}
			templates, need, status := g.gangTemplates(tt.gang, current, placedMembers(0))
			if templates != nil || need != 0 {
				t.Fatalf("templates = %v, need = %d, want none and 0", templateNames(templates), need)
			}
			if status.Code() != framework.Unschedulable || status.Message() != tt.want {
				t.Fatalf("status = %v, want Unschedulable %q", status, tt.want)
			}
			if !g.hasTemplatesIncomplete(tt.gang) {
				t.Fatal("the short set left no incomplete-set record")
			}
		})
	}
}

// TestGangTemplatesFailTheCycleWhenTheListFails holds that a lister error
// fails the cycle with that error and records nothing.
func TestGangTemplatesFailTheCycleWhenTheListFails(t *testing.T) {
	g := &GangPack{podLister: fakeGangPodLister{err: errors.New("pod-group index is missing")}}
	templates, need, status := g.gangTemplates(templateGang(2), namedMember("current"), placedMembers(0))
	if templates != nil || need != 0 {
		t.Fatalf("templates = %v, need = %d, want none and 0", templateNames(templates), need)
	}
	if status.Code() != framework.Error || status.Message() != "pod-group index is missing" {
		t.Fatalf("status = %v, want Error with the lister's text", status)
	}
	if g.hasTemplatesIncomplete(templateGang(2)) {
		t.Fatal("a failed list left an incomplete-set record")
	}
}

// TestGangTemplatesRequeueTheMembersHeldOnAShortSetOnce holds that the first
// complete set after a short one requeues the gang's other members, counts
// one activation and consumes the record; a later complete set requeues
// nobody, and a plugin without a scheduler handle keeps the record.
func TestGangTemplatesRequeueTheMembersHeldOnAShortSetOnce(t *testing.T) {
	current, held := namedMember("current"), namedMember("held")
	gang := templateGang(2)
	h := &fakeHandle{}
	g := &GangPack{handle: h, podLister: fakeGangPodLister{pods: []*v1.Pod{current}}}
	if _, _, status := g.gangTemplates(gang, current, placedMembers(0)); status.Code() != framework.Unschedulable {
		t.Fatalf("status of the short set = %v, want Unschedulable", status)
	}
	before := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerTemplatesComplete))

	g.podLister = fakeGangPodLister{pods: []*v1.Pod{current, held}}
	templates, need, status := g.gangTemplates(gang, current, placedMembers(0))
	if status != nil || need != 2 || !slices.Equal(templateNames(templates), []string{"current", "held"}) {
		t.Fatalf("complete set = %v, %d, %v; want current and held, 2, no status", templateNames(templates), need, status)
	}
	if h.activateCalls != 1 || len(h.activated) != 1 || h.activated["team/held"] == nil {
		t.Fatalf("activations = %d of %v, want one of team/held alone", h.activateCalls, h.activated)
	}
	if d := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerTemplatesComplete)) - before; d != 1 {
		t.Fatalf("activation counter moved by %v, want 1", d)
	}
	if g.hasTemplatesIncomplete(gang) {
		t.Fatal("the record outlived the requeue")
	}

	if _, _, status := g.gangTemplates(gang, current, placedMembers(0)); status != nil || h.activateCalls != 1 {
		t.Fatalf("a later complete set: status = %v, activations = %d; want none and still 1", status, h.activateCalls)
	}

	fresh := &GangPack{handle: &fakeHandle{}, podLister: fakeGangPodLister{pods: []*v1.Pod{current, held}}}
	if _, _, status := fresh.gangTemplates(gang, current, placedMembers(0)); status != nil || fresh.handle.(*fakeHandle).activateCalls != 0 {
		t.Fatalf("a complete set with no record: status = %v, activations = %d; want none and 0", status, fresh.handle.(*fakeHandle).activateCalls)
	}

	noHandle := &GangPack{podLister: fakeGangPodLister{pods: []*v1.Pod{current}}}
	noHandle.gangTemplates(gang, current, placedMembers(0))
	noHandle.podLister = fakeGangPodLister{pods: []*v1.Pod{current, held}}
	if templates, _, status := noHandle.gangTemplates(gang, current, placedMembers(0)); status != nil || len(templates) != 2 {
		t.Fatalf("without a handle = %v, %v; want two templates and no status", templateNames(templates), status)
	}
	if !noHandle.hasTemplatesIncomplete(gang) {
		t.Fatal("without a handle the record was consumed with nobody requeued")
	}
}
