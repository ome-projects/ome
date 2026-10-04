package gangpack

import (
	"context"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// gangMember builds a member of the named gang requesting req GPUs, with a
// stable identity so activations can be checked by UID.
func gangMember(pg, name, req string) *v1.Pod {
	p := gangGPUPod("team", pg, req)
	p.Name = name
	p.UID = types.UID(name + "-uid")
	return p
}

// twoGangsOneDomain wires two 2-member gangs against a single domain of two
// nodes, each node roomy enough for one member of each gang. Whichever gang
// plans first holds the whole domain until its reservation drains or unwinds.
func twoGangsOneDomain() (g *GangPack, h *fakeHandle, nodes []framework.NodeInfo, first, second [2]*v1.Pod) {
	first = [2]*v1.Pod{gangMember("first", "first-0", "4"), gangMember("first", "first-1", "4")}
	second = [2]*v1.Pod{gangMember("second", "second-0", "4"), gangMember("second", "second-1", "4")}
	h = &fakeHandle{}
	g = &GangPack{
		handle: h,
		pins:   placement.New(),
		pgReader: fakeReader{
			"team/first":  {min: 2, topo: testKey, to: time.Minute},
			"team/second": {min: 2, topo: testKey, to: time.Minute},
		},
		podLister: fakeGangPodLister{pods: []*v1.Pod{first[0], first[1], second[0], second[1]}},
	}
	nodes = []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "8")), nodeInfo(gpuNode("a2", "a", "8"))}
	return g, h, nodes, first, second
}

// rejectForReservedDomain runs PreFilter for every member and requires each to
// be parked because the domain that has room for it is reserved.
func rejectForReservedDomain(t *testing.T, g *GangPack, nodes []framework.NodeInfo, members [2]*v1.Pod) {
	t.Helper()
	for _, member := range members {
		_, st := g.PreFilter(context.Background(), newCycleState(), member, nodes)
		if st.Code() != framework.Unschedulable || !strings.Contains(st.Message(), "no domain has room") {
			t.Fatalf("%s PreFilter = %v, want Unschedulable for lack of room", member.Name, st)
		}
	}
}

// TestReservationDrainWakesGangHeldOutOfReservedDomain: a gang whose only
// fitting domain is held by another gang's outstanding reservation is parked in
// PreFilter. No cluster event follows the holder's last member landing, so the
// plugin itself must wake the parked members when the reservation drains; their
// retry then plans into the domain.
func TestReservationDrainWakesGangHeldOutOfReservedDomain(t *testing.T) {
	ctx := context.Background()
	g, h, nodes, first, second := twoGangsOneDomain()

	// The first gang's leader pins the domain and reserves both member slots.
	leaderState := newCycleState()
	if _, st := g.PreFilter(ctx, leaderState, first[0], nodes); !st.IsSuccess() {
		t.Fatalf("first gang leader PreFilter = %v, want Success", st)
	}

	// The second gang finds room in the snapshot, but every domain with room is
	// reserved: both members park.
	reservedBefore := counterValue(t, gangPinTotal.WithLabelValues("reserved"))
	noFitBefore := counterValue(t, gangPinTotal.WithLabelValues("no_fit"))
	rejectForReservedDomain(t, g, nodes, second)
	if h.activateCalls != 0 {
		t.Fatalf("activate calls while the domain is reserved = %d, want 0", h.activateCalls)
	}

	// The leader lands: one slot stays reserved, so the parked gang stays parked.
	if st := g.Reserve(ctx, leaderState, first[0], "a1"); !st.IsSuccess() {
		t.Fatalf("first gang leader Reserve = %v, want Success", st)
	}
	if h.activateCalls != 0 {
		t.Fatalf("activate calls with a slot still reserved = %d, want 0", h.activateCalls)
	}

	// The worker lands and drains the reservation: exactly the parked members
	// are woken, in one activation.
	assumed := []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "8"), first[0]), nodeInfo(gpuNode("a2", "a", "8"))}
	workerState := newCycleState()
	if _, st := g.PreFilter(ctx, workerState, first[1], assumed); !st.IsSuccess() {
		t.Fatalf("first gang worker PreFilter = %v, want Success", st)
	}
	activationsBefore := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerReservationReleased))
	if st := g.Reserve(ctx, workerState, first[1], "a2"); !st.IsSuccess() {
		t.Fatalf("first gang worker Reserve = %v, want Success", st)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls after the reservation drained = %d, want 1", h.activateCalls)
	}
	for _, member := range second {
		if h.activated[string(member.UID)] != member {
			t.Fatalf("activated = %v, want %s included", h.activated, member.Name)
		}
	}
	if len(h.activated) != len(second) {
		t.Fatalf("activated = %v, want only the parked members", h.activated)
	}
	if d := counterValue(t, gangActivationTotal.WithLabelValues(activationTriggerReservationReleased)) - activationsBefore; d != 1 {
		t.Fatalf("reservation_released activation counter delta = %v, want 1", d)
	}
	// The rejections were accounted as reserved, not as a capacity no-fit.
	if d := counterValue(t, gangPinTotal.WithLabelValues("reserved")) - reservedBefore; d != 2 {
		t.Fatalf("reserved pin counter delta = %v, want 2", d)
	}
	if d := counterValue(t, gangPinTotal.WithLabelValues("no_fit")) - noFitBefore; d != 0 {
		t.Fatalf("no_fit pin counter delta = %v, want 0 for a reserved domain", d)
	}

	// The retry plans into the domain: the first gang's members are ordinary
	// occupancy now, and the nodes still have room for the second gang.
	settled := []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "8"), first[0]), nodeInfo(gpuNode("a2", "a", "8"), first[1])}
	state := newCycleState()
	if _, st := g.PreFilter(ctx, state, second[0], settled); !st.IsSuccess() {
		t.Fatalf("second gang PreFilter after the drain = %v, want Success", st)
	}
	if pin := readPin(state); pin == nil || pin.domain != "a" {
		t.Fatalf("pin = %+v, want domain a", pin)
	}

	// A wake-up is consumed by the activation: a later drain of some other
	// reservation does not activate the same members again.
	if st := g.Reserve(ctx, state, second[0], "a1"); !st.IsSuccess() {
		t.Fatalf("second gang leader Reserve = %v, want Success", st)
	}
	if h.activateCalls != 1 {
		t.Fatalf("activate calls after a drain with nobody parked = %d, want still 1", h.activateCalls)
	}
}

// TestReservationUnwindWakesGangHeldOutOfReservedDomain: the holder never
// completes and its attempt is unwound, so the reservation is released rather
// than drained. The parked gang is woken by the release and its retry plans
// into the domain.
func TestReservationUnwindWakesGangHeldOutOfReservedDomain(t *testing.T) {
	ctx := context.Background()
	g, h, nodes, first, second := twoGangsOneDomain()

	leaderState := newCycleState()
	if _, st := g.PreFilter(ctx, leaderState, first[0], nodes); !st.IsSuccess() {
		t.Fatalf("first gang leader PreFilter = %v, want Success", st)
	}
	rejectForReservedDomain(t, g, nodes, second)

	g.Unreserve(ctx, leaderState, first[0], "a1")

	if h.activateCalls != 1 {
		t.Fatalf("activate calls after the reservation was released = %d, want 1", h.activateCalls)
	}
	for _, member := range second {
		if h.activated[string(member.UID)] != member {
			t.Fatalf("activated = %v, want %s included", h.activated, member.Name)
		}
	}
	state := newCycleState()
	if _, st := g.PreFilter(ctx, state, second[0], nodes); !st.IsSuccess() {
		t.Fatalf("second gang PreFilter after the release = %v, want Success", st)
	}
	if pin := readPin(state); pin == nil || pin.domain != "a" {
		t.Fatalf("pin = %+v, want domain a", pin)
	}
}

// TestCapacityNoFitIsNotWokenByReservationDrain: a gang that fits no domain on
// capacity alone is not waiting on any reservation, so a reservation draining
// elsewhere does not activate it. Capacity changes reach it through the
// registered pod and node events instead.
func TestCapacityNoFitIsNotWokenByReservationDrain(t *testing.T) {
	ctx := context.Background()
	solo := gangMember("solo", "solo-0", "4")
	pair := [2]*v1.Pod{gangMember("pair", "pair-0", "4"), gangMember("pair", "pair-1", "4")}
	h := &fakeHandle{}
	g := &GangPack{
		handle: h,
		pins:   placement.New(),
		pgReader: fakeReader{
			"team/solo": {min: 1, topo: testKey, to: time.Minute},
			"team/pair": {min: 2, topo: testKey, to: time.Minute},
		},
		podLister: fakeGangPodLister{pods: []*v1.Pod{solo, pair[0], pair[1]}},
	}
	// Two single-node domains: the pair can never fit, reservation or not.
	nodes := []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "4")), nodeInfo(gpuNode("b1", "b", "4"))}

	soloState := newCycleState()
	if _, st := g.PreFilter(ctx, soloState, solo, nodes); !st.IsSuccess() {
		t.Fatalf("solo PreFilter = %v, want Success", st)
	}
	noFitBefore := counterValue(t, gangPinTotal.WithLabelValues("no_fit"))
	reservedBefore := counterValue(t, gangPinTotal.WithLabelValues("reserved"))
	rejectForReservedDomain(t, g, nodes, pair)
	if d := counterValue(t, gangPinTotal.WithLabelValues("no_fit")) - noFitBefore; d != 2 {
		t.Fatalf("no_fit pin counter delta = %v, want 2", d)
	}
	if d := counterValue(t, gangPinTotal.WithLabelValues("reserved")) - reservedBefore; d != 0 {
		t.Fatalf("reserved pin counter delta = %v, want 0 for a capacity no-fit", d)
	}

	if st := g.Reserve(ctx, soloState, solo, "b1"); !st.IsSuccess() {
		t.Fatalf("solo Reserve = %v, want Success", st)
	}
	if h.activateCalls != 0 {
		t.Fatalf("activate calls after an unrelated reservation drained = %d, want 0", h.activateCalls)
	}
}

// TestStaleReplanWakesPodsHeldOutOfReleasedDomain: dropping a stale pin also
// releases the reservation that kept other pods out of that domain, so the
// pods parked on it are woken like on any other release.
func TestStaleReplanWakesPodsHeldOutOfReleasedDomain(t *testing.T) {
	ctx := context.Background()
	h := &fakeHandle{}
	g := &GangPack{handle: h, pins: placement.New()}
	if d, _, ok := g.pins.ChooseInTopologyOnNodes("team/pf", testKey,
		topology.FreeByDomain{"a": 2, "b": 2}, map[string][]string{"a": {"a1", "a2"}, "b": {"b1", "b2"}}, 2); !ok || d != "a" {
		t.Fatalf("precondition: Choose = %q,%v, want a,true", d, ok)
	}
	standalone := gpuPod("4")
	standalone.Namespace, standalone.Name, standalone.UID = "team", "standalone", "standalone-uid"
	if st := g.Filter(ctx, newCycleState(), standalone, nodeInfo(gpuNode("a1", "a", "4"))); st.Code() != framework.UnschedulableAndUnresolvable {
		t.Fatalf("Filter on the reserved node = %v, want reservation rejection", st)
	}

	// Domain a filled under the pin before any member landed: the pin is stale,
	// the gang re-plans onto b, and a's reservation goes with the old pin.
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gpuPod("4")),
		nodeInfo(gpuNode("a2", "a", "4"), gpuPod("4")),
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	if _, st := g.pinGang(newCycleState(), nodes, gangInfo{key: "team/pf", minMember: 2, topologyKey: testKey}, gangGPUPod("team", "pf", "4")); !st.IsSuccess() {
		t.Fatalf("pinGang = %v, want Success after re-planning off the stale domain", st)
	}
	if d, ok := g.pins.Get("team/pf"); !ok || d != "b" {
		t.Fatalf("re-planned pin = %q,%v, want b,true", d, ok)
	}
	if h.activated[string(standalone.UID)] != standalone {
		t.Fatalf("activated = %v, want the pod parked on the released domain", h.activated)
	}
}
