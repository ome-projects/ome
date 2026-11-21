package gangpack

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/events"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

const (
	lowPriority    int32 = 10
	mediumPriority int32 = 50
	highPriority   int32 = 100
)

// runningPod is a pod bound to node at the given priority that takes the
// node's whole accelerator allocation; pg names its gang, or "" for none.
func runningPod(name, pg, node string, priority int32) *v1.Pod {
	p := gpuPod("4")
	p.Name, p.Namespace, p.UID = name, "team", types.UID("uid-"+name)
	p.Spec.NodeName = node
	p.Spec.Priority = &priority
	if pg != "" {
		p.Labels = map[string]string{podGroupLabel: pg}
	}
	return p
}

// preemptionFixture is a worked example of gang preemption: domain a holds the
// low-priority gangs a (2 pods) and b (2 pods); domain b holds the
// medium-priority gang c (4 pods); every node has room for one pod. The
// high-priority gang h of 4 pods fits neither domain.
type preemptionFixture struct {
	g       *GangPack
	h       *fakeHandle
	client  *fake.Clientset
	nodes   []framework.NodeInfo
	running []*v1.Pod
	member  *v1.Pod
	clock   time.Time
}

func newPreemptionFixture(t *testing.T, preemptorPriority int32, maxInFlight int) *preemptionFixture {
	t.Helper()
	f := &preemptionFixture{clock: time.Unix(10_000, 0)}
	layout := []struct{ node, domain, pod, pg string }{
		{"a1", "a", "a-0", "a"}, {"a2", "a", "a-1", "a"}, {"a3", "a", "b-0", "b"}, {"a4", "a", "b-1", "b"},
		{"b1", "b", "c-0", "c"}, {"b2", "b", "c-1", "c"}, {"b3", "b", "c-2", "c"}, {"b4", "b", "c-3", "c"},
	}
	var objects []runtime.Object
	for _, l := range layout {
		priority := lowPriority
		if l.pg == "c" {
			priority = mediumPriority
		}
		pod := runningPod(l.pod, l.pg, l.node, priority)
		f.running = append(f.running, pod)
		objects = append(objects, pod)
		f.nodes = append(f.nodes, nodeInfo(gpuNode(l.node, l.domain, "4"), pod))
	}
	f.client = fake.NewClientset(objects...)
	f.h = &fakeHandle{snapshot: f.nodes, clientSet: f.client, recorder: events.NewFakeRecorder(100)}
	f.g = &GangPack{
		handle:      f.h,
		assignments: placement.New(),
		pgReader: fakeReader{
			"team/h": {min: 4, topo: testKey, to: time.Minute, uid: "uid-pg-h"},
			"team/a": {min: 2, topo: testKey, to: time.Minute, uid: "uid-pg-a"},
			"team/b": {min: 2, topo: testKey, to: time.Minute, uid: "uid-pg-b"},
			"team/c": {min: 4, topo: testKey, to: time.Minute, uid: "uid-pg-c"},
		},
		maxPreemptionsInFlight: maxInFlight,
		now:                    func() time.Time { return f.clock },
	}
	f.member = gangGPUPod("team", "h", "4")
	f.member.Name, f.member.UID = "h-0", "uid-h-0"
	f.member.Spec.Priority = &preemptorPriority
	return f
}

// cycle runs PreFilter then, on a rejection, PostFilter for the member over the
// fixture's snapshot, as the scheduler does.
func (f *preemptionFixture) cycle(t *testing.T) (*framework.Status, *framework.PostFilterResult, *framework.Status) {
	t.Helper()
	f.h.snapshot = f.nodes
	st := newCycleState()
	_, pre := f.g.PreFilter(context.Background(), st, f.member, f.nodes)
	if pre.IsSuccess() {
		return pre, nil, nil
	}
	res, post := f.g.PostFilter(context.Background(), st, f.member, nil)
	return pre, res, post
}

// remaining lists the names of the pods the API still holds.
func (f *preemptionFixture) remaining(t *testing.T) []string {
	t.Helper()
	pods, err := f.client.CoreV1().Pods("team").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	var names []string
	for _, p := range pods.Items {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	return names
}

// TestPreempt_FreesTheDomainThatEvictsTheLowestPriority: the gang fits no domain,
// so PostFilter evicts both low-priority gangs of domain a, whole, and holds
// the domain; the medium-priority gang in domain b is untouched. The domain's
// hold lasts the gang's gate wait, since the evicted pods declare no grace period.
func TestPreempt_FreesTheDomainThatEvictsTheLowestPriority(t *testing.T) {
	f := newPreemptionFixture(t, highPriority, 1)
	pre, res, post := f.cycle(t)
	if pre.Code() != framework.Unschedulable || !strings.Contains(pre.Message(), "no domain has room") {
		t.Fatalf("PreFilter = %v, want Unschedulable with no domain has room", pre)
	}
	if !post.IsSuccess() || res == nil || res.Mode() != framework.ModeNoop {
		t.Fatalf("PostFilter = %v %+v, want Success with no nomination", post, res)
	}
	if !strings.Contains(post.Message(), "gang team/a, gang team/b") || !strings.Contains(post.Message(), "domain a") {
		t.Fatalf("PostFilter message %q does not name the evicted gangs and the domain", post.Message())
	}
	if got, want := strings.Join(f.remaining(t), ","), "c-0,c-1,c-2,c-3"; got != want {
		t.Fatalf("pods left = %s, want %s", got, want)
	}
	until, _, held := f.g.assignments.HeldUntil("team/h")
	domain, _, remaining, _ := f.g.assignments.GetOwned("team/h")
	if !held || domain.Name != "a" || remaining != 4 || !until.Equal(f.clock.Add(time.Minute)) {
		t.Fatalf("hold = %v domain %s remaining %d until %v, want domain a, 4 reserved, until %v",
			held, domain.Name, remaining, until, f.clock.Add(time.Minute))
	}
}

// TestPreempt_HeldGangWaitsWithoutEvictingAgainThenPlacesInTheDomain: while the
// evicted pods are still in the snapshot, every retry waits on the held domain and
// evicts nothing more; once they are gone, the gang plans into that domain.
func TestPreempt_HeldGangWaitsWithoutEvictingAgainThenPlacesInTheDomain(t *testing.T) {
	f := newPreemptionFixture(t, highPriority, 1)
	f.cycle(t)
	before := f.remaining(t)

	pre, _, post := f.cycle(t)
	if pre.Code() != framework.Unschedulable || !strings.Contains(pre.Message(), "waiting for evicted pods to leave domain a") {
		t.Fatalf("retry PreFilter = %v, want a wait on the held domain", pre)
	}
	if post.IsSuccess() || strings.Join(f.remaining(t), ",") != strings.Join(before, ",") {
		t.Fatalf("retry PostFilter = %v and pods %v, want no further eviction", post, f.remaining(t))
	}

	// The evicted pods leave the snapshot: domain a is empty.
	for i := range f.nodes[:4] {
		f.nodes[i] = nodeInfo(gpuNode(f.nodes[i].Node().Name, "a", "4"))
	}
	pre, _, _ = f.cycle(t)
	if !pre.IsSuccess() {
		t.Fatalf("PreFilter after the evicted pods left = %v, want Success", pre)
	}
}

// TestPreempt_DoesNotEvictEqualOrHigherPriority: a gang that outranks nothing
// in any domain evicts nothing and holds nothing.
func TestPreempt_DoesNotEvictEqualOrHigherPriority(t *testing.T) {
	f := newPreemptionFixture(t, lowPriority, 1)
	_, _, post := f.cycle(t)
	if post.Code() != framework.Unschedulable || !strings.Contains(post.Message(), "no domain can be freed") {
		t.Fatalf("PostFilter = %v, want Unschedulable: no domain can be freed", post)
	}
	if len(f.remaining(t)) != 8 || f.g.assignments.Len() != 0 {
		t.Fatalf("pods %v, assignments %d: want all 8 pods and no hold", f.remaining(t), f.g.assignments.Len())
	}
}

// TestPreempt_OffWithoutConfiguration: with no preemption block the plugin
// never preempts: the gang waits, and nothing is evicted.
func TestPreempt_OffWithoutConfiguration(t *testing.T) {
	f := newPreemptionFixture(t, highPriority, 0)
	_, res, post := f.cycle(t)
	if post.Code() != framework.Unschedulable || post.Message() != "" || res != nil {
		t.Fatalf("PostFilter = %v %+v, want a bare Unschedulable", post, res)
	}
	if len(f.remaining(t)) != 8 {
		t.Fatalf("pods %v, want all 8", f.remaining(t))
	}
}

func TestPreempt_PolicyNeverEvictsNothing(t *testing.T) {
	f := newPreemptionFixture(t, highPriority, 1)
	never := v1.PreemptNever
	f.member.Spec.PreemptionPolicy = &never
	_, _, post := f.cycle(t)
	if post.IsSuccess() || !strings.Contains(post.Message(), "preemption policy is Never") || len(f.remaining(t)) != 8 {
		t.Fatalf("PostFilter = %v, pods %v: want no preemption", post, f.remaining(t))
	}
}

// TestPreempt_RespectsTheInFlightLimit: another gang already waits for a
// domain preemption is freeing, and the limit is one.
func TestPreempt_RespectsTheInFlightLimit(t *testing.T) {
	f := newPreemptionFixture(t, highPriority, 1)
	if _, ok := f.g.assignments.Hold(placement.Gang{Group: "team/other", TopologyKey: testKey}, "z", 1, []string{"z1"}, f.clock.Add(time.Hour)); !ok {
		t.Fatal("setup: Hold refused")
	}
	_, _, post := f.cycle(t)
	if post.IsSuccess() || !strings.Contains(post.Message(), "1 preemptions are already in flight") || len(f.remaining(t)) != 8 {
		t.Fatalf("PostFilter = %v, pods %v: want the limit to stop preemption", post, f.remaining(t))
	}
}

// TestPreempt_RoomReservedByAFormingGangIsNotPreemption: domain c is empty but
// a forming gang has reserved it. The gang waits for that reservation; evicting
// anyone would not help, so nothing is evicted.
func TestPreempt_RoomReservedByAFormingGangIsNotPreemption(t *testing.T) {
	f := newPreemptionFixture(t, highPriority, 1)
	var cNodes []string
	for _, name := range []string{"c1", "c2", "c3", "c4"} {
		f.nodes = append(f.nodes, nodeInfo(gpuNode(name, "c", "4")))
		cNodes = append(cNodes, name)
	}
	if _, _, ok := f.g.assignments.Choose(placement.Gang{Group: "team/forming", TopologyKey: testKey},
		topology.FreeByDomain{"c": 4}, map[string][]string{"c": cNodes}, 4); !ok {
		t.Fatal("setup: forming gang found no domain")
	}
	pre, _, post := f.cycle(t)
	if !strings.Contains(pre.Message(), "reserved for a forming gang") || post.IsSuccess() || len(f.remaining(t)) != 8 {
		t.Fatalf("PreFilter %v, PostFilter %v, pods %v: want a wait on the reservation, no eviction", pre, post, f.remaining(t))
	}
}

// TestPreempt_AfterTheHoldExpiresPodsStillShuttingDownCountAsLeaving: the
// evicted pods are still shutting down when the hold's time is up. The gang plans
// again, finds no room, and holds the same domain without evicting anyone new.
func TestPreempt_AfterTheHoldExpiresPodsStillShuttingDownCountAsLeaving(t *testing.T) {
	f := newPreemptionFixture(t, highPriority, 1)
	f.cycle(t)
	deleting := metav1.NewTime(f.clock.Add(30 * time.Second))
	for i := range f.nodes[:4] {
		pod := f.running[i].DeepCopy()
		pod.DeletionTimestamp = &deleting
		f.nodes[i] = nodeInfo(gpuNode(f.nodes[i].Node().Name, "a", "4"), pod)
	}
	f.clock = f.clock.Add(2 * time.Minute)

	_, res, post := f.cycle(t)
	if !post.IsSuccess() || res == nil || !strings.Contains(post.Message(), "holding domain a") {
		t.Fatalf("PostFilter = %v, want domain a held again with no new eviction", post)
	}
	until, _, held := f.g.assignments.HeldUntil("team/h")
	// The slowest pod is due to be gone 30s after the original clock, which is
	// already past; the hold then lasts the gang's gate wait.
	if !held || !until.Equal(f.clock.Add(time.Minute)) {
		t.Fatalf("hold = %v until %v, want until %v", held, until, f.clock.Add(time.Minute))
	}
	if got, want := strings.Join(f.remaining(t), ","), "c-0,c-1,c-2,c-3"; got != want {
		t.Fatalf("pods left = %s, want %s", got, want)
	}
}

// TestReleaseExpiredHolds_DropsAHoldNoMemberWaitsOn: the collector releases a
// hold whose time is up when no member of the gang waits at the gate, and
// leaves a hold that is still running.
func TestReleaseExpiredHolds_DropsAHoldNoMemberWaitsOn(t *testing.T) {
	f := newPreemptionFixture(t, highPriority, 2)
	expired := placement.Gang{Group: "team/expired", Owner: "u1", TopologyKey: testKey}
	running := placement.Gang{Group: "team/running", Owner: "u2", TopologyKey: testKey}
	if _, ok := f.g.assignments.Hold(expired, "x", 1, []string{"x1"}, f.clock.Add(-time.Second)); !ok {
		t.Fatal("setup: Hold refused")
	}
	if _, ok := f.g.assignments.Hold(running, "y", 1, []string{"y1"}, f.clock.Add(time.Second)); !ok {
		t.Fatal("setup: Hold refused")
	}
	f.g.releaseExpiredHolds()
	if _, _, held := f.g.assignments.HeldUntil("team/expired"); held {
		t.Fatal("an expired hold survived the collector")
	}
	if _, _, held := f.g.assignments.HeldUntil("team/running"); !held {
		t.Fatal("the collector released a hold whose time is not up")
	}
}

// TestCandidateUnits_GangsLeaveWholeAndOnlyBelowThePreemptor: a gang is one
// unit with every pod it has on a node; a gang with any pod at or above the
// preemptor's priority, or with room still reserved, is not eligible; pods
// already shutting down and the preemptor's own gang are not candidates.
func TestCandidateUnits_GangsLeaveWholeAndOnlyBelowThePreemptor(t *testing.T) {
	deleting := metav1.NewTime(time.Unix(1, 0))
	leaving := runningPod("leaving", "", "n5", lowPriority)
	leaving.DeletionTimestamp = &deleting
	mixed := runningPod("m-1", "m", "n4", highPriority)
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("n1", "a", "8"), runningPod("a-0", "a", "n1", lowPriority), runningPod("solo", "", "n1", lowPriority)),
		nodeInfo(gpuNode("n2", "b", "8"), runningPod("a-1", "a", "n2", lowPriority), runningPod("f-0", "f", "n2", lowPriority)),
		nodeInfo(gpuNode("n3", "a", "8"), runningPod("m-0", "m", "n3", lowPriority), runningPod("own", "h", "n3", lowPriority)),
		nodeInfo(gpuNode("n4", "a", "8"), mixed),
		nodeInfo(gpuNode("n5", "a", "8"), leaving),
	}
	preemptor := gangGPUPod("team", "h", "4")
	p := mediumPriority
	preemptor.Spec.Priority = &p
	units := candidateUnits(nodes, preemptor, func(gang string) bool { return gang == "team/f" })

	want := map[string]struct {
		pods     int
		eligible bool
	}{
		"gang team/a":   {2, true},
		"pod team/solo": {1, true},
		"gang team/f":   {1, false},
		"gang team/m":   {2, false},
	}
	if len(units) != len(want) {
		var got []string
		for k := range units {
			got = append(got, k)
		}
		t.Fatalf("units %v, want %d units", got, len(want))
	}
	for key, w := range want {
		u := units[key]
		if u == nil || u.unit.Pods != w.pods || u.eligible != w.eligible {
			t.Errorf("unit %s = %+v, want %d pods, eligible %v", key, u, w.pods, w.eligible)
		}
	}
	if units["gang team/a"].unit.Accelerators != 8 {
		t.Errorf("gang team/a accelerators = %d, want 8", units["gang team/a"].unit.Accelerators)
	}
}
