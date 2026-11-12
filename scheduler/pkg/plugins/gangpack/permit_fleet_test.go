package gangpack

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/testing/allocbudget"
)

// The cost of the gang scheduler at fleet size: one scheduling cycle of one
// gang member (PreFilter, Filter, PreScore, Reserve, Permit), the eight cycles
// of one whole gang, against a cluster of 1,008 to 4,500 nodes in domains of
// 18, with 50 to 500 gangs of 2, 4 or 8 pending at once, and the drain of the
// whole pending set. Everything runs in process against this file's stand-ins
// for the framework: a handle that holds the waiting pods and the activations,
// a pod lister with the informer's index, and node snapshots that count every
// read of a node. Durations are readings for the benchmarks; allocations and
// reads are the same on every machine and are held to the goldens under
// testdata. The reads held are the node reads and the lists of a gang's pods
// of every operation, and at the gate the scans of the waiting set, the
// waiting pods they hand to the plugin and the lookups of a waiting pod by
// UID, so a gate decision whose cost grows with the waiting set shows.

var updateFleetGoldens = flag.Bool("update", false, "re-record testdata/permit_fleet_allocations.json and testdata/permit_fleet_reads.json from this run")

// fleetDomainNodes is the nodes per domain: one GPU-fabric clique of 18 nodes.
const fleetDomainNodes = 18

const (
	fleetNamespace = "team-a"
	// fleetClusterID is the cluster half of a clique label value, which the
	// GPU operator writes as <cluster uuid>.<clique id>.
	fleetClusterID = "8c2f6e1a-4b7d-4c3e-9f2a-1d5e7b9c0a3f"
	// fleetFlushLimit bounds the periodic requeues of a drain: a pending set
	// that needs more is not draining.
	fleetFlushLimit = 50
)

// fleetShape is one pending set on one fleet: domains of fleetDomainNodes
// nodes each, and gangs of gangSize whole-node pods pending at once.
type fleetShape struct {
	domains  int
	gangSize int
	gangs    int
}

func (s fleetShape) nodes() int { return s.domains * fleetDomainNodes }

func (s fleetShape) String() string {
	return fmt.Sprintf("nodes=%d/gang=%d/pending=%d", s.nodes(), s.gangSize, s.gangs)
}

// fleetShapes are the measured fleets. Two gangs of 8 fit one domain, so the
// 1,008-node fleet holds 112 of them, the 3,996-node fleet 444 and the
// 4,500-node fleet 500 exactly; the pending sets stop at those counts so every
// gang can land.
var fleetShapes = []fleetShape{
	{domains: 56, gangSize: 2, gangs: 500},
	{domains: 56, gangSize: 4, gangs: 200},
	{domains: 56, gangSize: 8, gangs: 50},
	{domains: 56, gangSize: 8, gangs: 100},
	{domains: 222, gangSize: 2, gangs: 500},
	{domains: 222, gangSize: 4, gangs: 500},
	{domains: 222, gangSize: 8, gangs: 200},
	{domains: 222, gangSize: 8, gangs: 400},
	{domains: 250, gangSize: 8, gangs: 500},
}

// fleetReads counts what the plugin asked the framework's stand-ins for.
type fleetReads struct {
	nodes          int64 // calls of NodeInfo.Node()
	waitingScans   int64 // calls of IterateOverWaitingPods
	waitingPods    int64 // waiting pods handed to its callback
	waitingLookups int64 // calls of GetWaitingPod
	gangLists      int64 // lists of one gang's pods
	activations    int64 // calls of Activate
	activatedPods  int64 // pods handed to Activate
}

func (r fleetReads) since(start fleetReads) fleetReads {
	return fleetReads{
		nodes:          r.nodes - start.nodes,
		waitingScans:   r.waitingScans - start.waitingScans,
		waitingPods:    r.waitingPods - start.waitingPods,
		waitingLookups: r.waitingLookups - start.waitingLookups,
		gangLists:      r.gangLists - start.gangLists,
		activations:    r.activations - start.activations,
		activatedPods:  r.activatedPods - start.activatedPods,
	}
}

// countingNodeInfo is one snapshot entry that counts every read of its node.
type countingNodeInfo struct {
	framework.NodeInfo
	reads *fleetReads
}

func (n *countingNodeInfo) Node() *v1.Node {
	n.reads.nodes++
	return n.NodeInfo.Node()
}

// fleetWaitingPod is a pod at the gate, with the cycle state the framework
// would keep for its binding cycle.
type fleetWaitingPod struct {
	pod      *v1.Pod
	state    framework.CycleState
	node     string
	allowed  bool
	rejected bool
}

func (w *fleetWaitingPod) GetPod() *v1.Pod             { return w.pod }
func (w *fleetWaitingPod) GetPendingPlugins() []string { return []string{Name} }
func (w *fleetWaitingPod) Allow(string)                { w.allowed = true }
func (w *fleetWaitingPod) Reject(string, string)       { w.rejected = true }

// fleetHandle is the framework handle the plugin sees: the waiting pods keyed
// by UID as the framework keeps them, and the pods the plugin activated.
type fleetHandle struct {
	framework.Handle
	reads     *fleetReads
	waiting   map[types.UID]*fleetWaitingPod
	activated []*v1.Pod
}

func (h *fleetHandle) IterateOverWaitingPods(f func(framework.WaitingPod)) {
	h.reads.waitingScans++
	for _, wp := range h.waiting {
		h.reads.waitingPods++
		f(wp)
	}
}

func (h *fleetHandle) GetWaitingPod(uid types.UID) framework.WaitingPod {
	h.reads.waitingLookups++
	if wp, ok := h.waiting[uid]; ok {
		return wp
	}
	return nil
}

// Activate records the pods in name order, so a drain runs the same way
// every time whatever order the plugin's map iterates in.
func (h *fleetHandle) Activate(_ klog.Logger, pods map[string]*v1.Pod) {
	h.reads.activations++
	h.reads.activatedPods += int64(len(pods))
	keys := make([]string, 0, len(pods))
	for key := range pods {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		h.activated = append(h.activated, pods[key])
	}
}

// fleetReader resolves every pending gang's PodGroup.
type fleetReader map[string]gangFacts

func (r fleetReader) get(namespace, name string) (gangFacts, bool) {
	facts, ok := r[namespace+"/"+name]
	return facts, ok
}

func fleetDomainName(d int) string { return fmt.Sprintf("%s.%d", fleetClusterID, d) }
func fleetGangName(g int) string   { return fmt.Sprintf("chat-model-engine-%d", g) }
func fleetGangKey(g int) string    { return fleetNamespace + "/" + fleetGangName(g) }

// fleetNode is node i of domain d: four GPUs, the clique label and the labels
// every node carries.
func fleetNode(i, d int) *v1.Node {
	name := fmt.Sprintf("gpu-node-%04d", i)
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("node-" + name), Labels: map[string]string{
			testKey:                            fleetDomainName(d),
			"kubernetes.io/hostname":           name,
			"kubernetes.io/arch":               "amd64",
			"kubernetes.io/os":                 "linux",
			"topology.kubernetes.io/zone":      "zone-a",
			"topology.kubernetes.io/region":    "region-a",
			"node.kubernetes.io/instance-type": "gpu-4x",
			"nvidia.com/gpu.count":             "4",
			"nvidia.com/gpu.present":           "true",
		}},
		Status: v1.NodeStatus{Allocatable: v1.ResourceList{
			gpu:                         resource.MustParse("4"),
			v1.ResourceCPU:              resource.MustParse("128"),
			v1.ResourceMemory:           resource.MustParse("1Ti"),
			v1.ResourceEphemeralStorage: resource.MustParse("1Ti"),
			v1.ResourcePods:             resource.MustParse("110"),
		}},
	}
}

// fleetPod is member m of gang g: a whole-node engine pod with the identity a
// serving pod carries.
func fleetPod(g, m int) *v1.Pod {
	gang := fleetGangName(g)
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: fleetNamespace,
			Name:      fmt.Sprintf("%s-%d", gang, m),
			UID:       types.UID(fmt.Sprintf("pod-%d-%d", g, m)),
			Labels:    map[string]string{podGroupLabel: gang, "app": "chat-model", "ome.io/component": "engine"},
		},
		Spec: v1.PodSpec{Containers: []v1.Container{{Name: "engine", Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{gpu: resource.MustParse("4"), v1.ResourceCPU: resource.MustParse("32"), v1.ResourceMemory: resource.MustParse("256Gi")},
			Limits:   v1.ResourceList{gpu: resource.MustParse("4")},
		}}}},
	}
}

// permitFleet is one fleet, its pending gangs, the plugin over them, and the
// driver that runs scheduling cycles the way the framework does.
type permitFleet struct {
	tb     testing.TB
	shape  fleetShape
	reads  fleetReads
	plugin *GangPack
	handle *fleetHandle
	// snapshot is what PreFilter sees: every node, counted. entries holds the
	// same counted entries by node name, infos the uncounted node behind each
	// for the driver's own bookkeeping.
	snapshot []framework.NodeInfo
	entries  map[string]framework.NodeInfo
	infos    map[string]*schedulerframework.NodeInfo
	pods     []*v1.Pod
	byGang   map[string][]*v1.Pod
	// assumed is the node each pod sits on once the driver assumed it there;
	// bound marks the pods whose gang the gate admitted.
	assumed map[types.UID]string
	bound   map[types.UID]bool
	queue   []*v1.Pod
	queued  map[types.UID]bool
	// cycles, unschedulable and flushes describe a drain: cycles run, cycles
	// that found no node, and requeues of the whole pending set.
	cycles, unschedulable, flushes int
}

func newPermitFleet(tb testing.TB, shape fleetShape) *permitFleet {
	tb.Helper()
	f := &permitFleet{
		tb: tb, shape: shape,
		entries: map[string]framework.NodeInfo{}, infos: map[string]*schedulerframework.NodeInfo{},
		byGang: map[string][]*v1.Pod{}, assumed: map[types.UID]string{}, bound: map[types.UID]bool{}, queued: map[types.UID]bool{},
	}
	for d := 0; d < shape.domains; d++ {
		for n := 0; n < fleetDomainNodes; n++ {
			node := fleetNode(d*fleetDomainNodes+n, d)
			info := schedulerframework.NewNodeInfo()
			info.SetNode(node)
			entry := &countingNodeInfo{NodeInfo: info, reads: &f.reads}
			f.infos[node.Name] = info
			f.entries[node.Name] = entry
			f.snapshot = append(f.snapshot, entry)
		}
	}
	reader := fleetReader{}
	for g := 0; g < shape.gangs; g++ {
		reader[fleetGangKey(g)] = gangFacts{minMember: shape.gangSize, topologyKey: testKey, timeout: time.Minute, uid: fmt.Sprintf("pg-%d", g)}
		for m := 0; m < shape.gangSize; m++ {
			pod := fleetPod(g, m)
			f.pods = append(f.pods, pod)
			f.byGang[fleetGangKey(g)] = append(f.byGang[fleetGangKey(g)], pod)
		}
	}
	f.handle = &fleetHandle{reads: &f.reads, waiting: map[types.UID]*fleetWaitingPod{}}
	f.plugin = &GangPack{
		handle: f.handle, topologyKey: testKey, standaloneDomainPacking: true,
		assignments: placement.New(), pgReader: reader, podLister: f.lister(),
	}
	return f
}

// lister is the gang pod lister over the informer's index: one list per gang,
// a fresh slice per call as the indexer returns one.
func (f *permitFleet) lister() *informerPodLister {
	return &informerPodLister{
		list: func() ([]*v1.Pod, error) { return f.pods, nil },
		listGroup: func(namespace, name string) ([]*v1.Pod, error) {
			f.reads.gangLists++
			members := f.byGang[namespace+"/"+name]
			out := make([]*v1.Pod, len(members))
			copy(out, members)
			return out, nil
		},
	}
}

func (f *permitFleet) member(g, m int) *v1.Pod { return f.byGang[fleetGangKey(g)][m] }

// cycleOutcome is how one scheduling cycle of a pod ended.
type cycleOutcome int

const (
	// cycleUnschedulable: PreFilter or Filter left the pod no node.
	cycleUnschedulable cycleOutcome = iota
	// cycleWaiting: the gate holds the pod, assumed on its node.
	cycleWaiting
	// cycleBound: the gate admitted the gang; the pod and the siblings it
	// released are bound.
	cycleBound
)

// reserveOne runs a pod's cycle up to Reserve the way the framework does:
// PreFilter over the whole snapshot, Filter over the candidates it named,
// PreScore (which the plugin skips for a gang member), the first candidate
// with room, then Reserve with the pod assumed on that node. The node is ""
// when the pod found none.
func (f *permitFleet) reserveOne(pod *v1.Pod) (framework.CycleState, string) {
	f.cycles++
	state := newCycleState()
	result, status := f.plugin.PreFilter(context.Background(), state, pod, f.snapshot)
	if !status.IsSuccess() {
		f.unschedulable++
		return state, ""
	}
	node := f.pickNode(state, pod, result)
	if node == "" {
		f.tb.Fatalf("%s: PreFilter named %d candidates and Filter or room rejected every one", pod.Name, len(result.NodeNames))
	}
	if status := f.plugin.PreScore(context.Background(), state, pod, nil); !status.IsSkip() {
		f.tb.Fatalf("%s: PreScore for a gang member = %v, want Skip", pod.Name, status)
	}
	f.infos[node].AddPod(pod)
	f.assumed[pod.UID] = node
	f.plugin.Reserve(context.Background(), state, pod, node)
	return state, node
}

// candidates are the nodes a PreFilter result names, in name order.
func (f *permitFleet) candidates(result *framework.PreFilterResult) []string {
	if result.AllNodes() {
		f.tb.Fatalf("PreFilter of a gang member narrowed nothing")
	}
	names := result.NodeNames.UnsortedList()
	sort.Strings(names)
	return names
}

// pickNode is the first candidate Filter admits that has room for the pod,
// which the framework's own resource filter would decide beside this plugin.
func (f *permitFleet) pickNode(state framework.CycleState, pod *v1.Pod, result *framework.PreFilterResult) string {
	need := needOf(pod)
	for _, name := range f.candidates(result) {
		if status := f.plugin.Filter(context.Background(), state, pod, f.entries[name]); !status.IsSuccess() {
			continue
		}
		if info := f.infos[name]; need.fits(info, info.Node()) {
			return name
		}
	}
	return ""
}

// permitOne runs Permit for a reserved pod and applies what it decided: a
// pod the gate holds back joins the waiting set; an admitted pod is bound with the
// siblings the gate released; anything else is unreserved.
func (f *permitFleet) permitOne(state framework.CycleState, pod *v1.Pod, node string) cycleOutcome {
	status, _ := f.plugin.Permit(context.Background(), state, pod, node)
	switch {
	case status.IsWait():
		f.handle.waiting[pod.UID] = &fleetWaitingPod{pod: pod, state: state, node: node}
		return cycleWaiting
	case status.IsSuccess():
		f.bind(state, pod, node)
		f.finishReleased()
		return cycleBound
	default:
		f.plugin.Unreserve(context.Background(), state, pod, node)
		f.unassume(pod)
		f.unschedulable++
		return cycleUnschedulable
	}
}

func (f *permitFleet) scheduleOne(pod *v1.Pod) cycleOutcome {
	state, node := f.reserveOne(pod)
	if node == "" {
		return cycleUnschedulable
	}
	return f.permitOne(state, pod, node)
}

func (f *permitFleet) bind(state framework.CycleState, pod *v1.Pod, node string) {
	f.plugin.PostBind(context.Background(), state, pod, node)
	f.bound[pod.UID] = true
}

func (f *permitFleet) unassume(pod *v1.Pod) {
	if node, ok := f.assumed[pod.UID]; ok {
		if err := f.infos[node].RemovePod(klog.Background(), pod); err != nil {
			f.tb.Fatalf("removing %s from %s: %v", pod.Name, node, err)
		}
		delete(f.assumed, pod.UID)
	}
}

// unscheduleGang returns a bound gang to the pending set: its pods leave their
// nodes and the bound set, and its commitment is released, so the gang can run
// its eight cycles again on the same fleet.
func (f *permitFleet) unscheduleGang(g int) {
	key := fleetGangKey(g)
	if _, id, _, ok := f.plugin.assignments.GetOwned(key); ok {
		f.plugin.assignments.Release(key, id, nil)
	}
	for m := 0; m < f.shape.gangSize; m++ {
		pod := f.member(g, m)
		f.unassume(pod)
		delete(f.bound, pod.UID)
	}
}

// finishReleased ends the binding cycles of the waiting pods the gate
// released or rejected, as the framework does once Permit answers them.
func (f *permitFleet) finishReleased() {
	for uid, wp := range f.handle.waiting {
		switch {
		case wp.allowed:
			delete(f.handle.waiting, uid)
			f.bind(wp.state, wp.pod, wp.node)
		case wp.rejected:
			delete(f.handle.waiting, uid)
			f.plugin.Unreserve(context.Background(), wp.state, wp.pod, wp.node)
			f.unassume(wp.pod)
		}
	}
}

// form runs every member but the last of the first n gangs to the gate, so n
// gangs are forming: each holds its domain, and its members wait.
func (f *permitFleet) form(n int) {
	f.tb.Helper()
	for g := 0; g < n; g++ {
		for m := 0; m < f.shape.gangSize-1; m++ {
			if outcome := f.scheduleOne(f.member(g, m)); outcome != cycleWaiting {
				f.tb.Fatalf("forming gang %d: member %d ended in %d, want the gate to hold it", g, m, outcome)
			}
		}
	}
}

// interleaved is the pending set with the first member of every gang first,
// then every second member, and so on: the order that keeps the most gangs
// forming at once. gangMajor is the set gang by gang.
func (f *permitFleet) interleaved() []*v1.Pod {
	out := make([]*v1.Pod, 0, len(f.pods))
	for m := 0; m < f.shape.gangSize; m++ {
		for g := 0; g < f.shape.gangs; g++ {
			out = append(out, f.member(g, m))
		}
	}
	return out
}

func (f *permitFleet) gangMajor() []*v1.Pod { return append([]*v1.Pod(nil), f.pods...) }

// drain schedules the pending set in the given order until every gang is
// bound. A pod the plugin activates re-enters the queue at its end, as the
// framework moves it to the active queue; when the queue is empty with pods
// still pending, every one of them re-enters, as the framework's periodic
// requeue of the unschedulable pool does. A set that is not drained after
// fleetFlushLimit such requeues fails.
func (f *permitFleet) drain(order []*v1.Pod) {
	f.tb.Helper()
	f.queue = f.queue[:0]
	for _, pod := range order {
		f.enqueue(pod)
	}
	for len(f.bound) < len(f.pods) {
		if len(f.queue) == 0 {
			f.flushes++
			if f.flushes > fleetFlushLimit {
				f.tb.Fatalf("%s: %d of %d pods bound after %d requeues of the pending set", f.shape, len(f.bound), len(f.pods), f.flushes)
			}
			for _, pod := range f.pods {
				f.enqueue(pod)
			}
			continue
		}
		pod := f.queue[0]
		f.queue = f.queue[1:]
		f.queued[pod.UID] = false
		if f.bound[pod.UID] || f.handle.waiting[pod.UID] != nil {
			continue
		}
		f.scheduleOne(pod)
		for _, activated := range f.handle.activated {
			f.enqueue(activated)
		}
		f.handle.activated = f.handle.activated[:0]
	}
}

// enqueue adds a pod to the active queue unless it is bound, at the gate or
// already queued.
func (f *permitFleet) enqueue(pod *v1.Pod) {
	if f.bound[pod.UID] || f.handle.waiting[pod.UID] != nil || f.queued[pod.UID] {
		return
	}
	f.queued[pod.UID] = true
	f.queue = append(f.queue, pod)
}

// checkDrained holds the drained fleet to the gang rules: every pod bound, no
// node holding two, every gang whole in one domain.
func (f *permitFleet) checkDrained() {
	f.tb.Helper()
	if len(f.bound) != len(f.pods) {
		f.tb.Fatalf("%s: %d of %d pods bound", f.shape, len(f.bound), len(f.pods))
	}
	nodes := map[string]types.UID{}
	for g := 0; g < f.shape.gangs; g++ {
		domain := ""
		for m := 0; m < f.shape.gangSize; m++ {
			pod := f.member(g, m)
			node := f.assumed[pod.UID]
			if other, taken := nodes[node]; taken {
				f.tb.Fatalf("%s: node %s holds %s and %s", f.shape, node, other, pod.UID)
			}
			nodes[node] = pod.UID
			if d := f.infos[node].Node().Labels[testKey]; domain == "" {
				domain = d
			} else if d != domain {
				f.tb.Fatalf("%s: gang %d sits in domains %s and %s", f.shape, g, domain, d)
			}
		}
	}
}

// fleetOp is one measured operation on a fleet with gangs forming: the name
// the benchmarks and the goldens share, the operation, which leaves the fleet
// as it found it so it can run again, and whether it decides at the gate, so
// its waiting-set scans, waiting-pod reads and lookups are held as well as
// its node reads and gang lists.
type fleetOp struct {
	name string
	op   func()
	gate bool
}

// fleetOps builds a fleet in which every domain but one holds a forming gang,
// its members waiting at the gate, and names six operations on it: the
// PreFilter of a gang's first member (which chooses a domain and gives it
// back), the PreFilter of a gang's last member (which follows its domain),
// the Filter calls over the candidates that PreFilter named, the Permit of a
// waiting member (the gate holds it), the Permit of the last member of a
// gang (the gate opens and releases its siblings), and the whole gang: every
// member of a fresh gang through its cycle, member by member, until the gate
// opens, after which the gang is returned to the pending set.
func fleetOps(tb testing.TB, shape fleetShape) (*permitFleet, []fleetOp) {
	tb.Helper()
	f := newPermitFleet(tb, shape)
	forming := min(shape.gangs-1, shape.domains-1)
	if forming < 3 {
		tb.Fatalf("%s: %d forming gangs, need three and a fresh one", shape, forming)
	}
	f.form(forming)
	fresh, follower, waiter, admit := forming, forming-1, forming-2, forming-3
	last := shape.gangSize - 1
	ctx := context.Background()

	admitPod := f.member(admit, last)
	admitState, admitNode := f.reserveOne(admitPod)
	if admitNode == "" {
		tb.Fatalf("%s: the last member of gang %d found no node", shape, admit)
	}
	followerPod := f.member(follower, last)
	followerState := newCycleState()
	result, status := f.plugin.PreFilter(ctx, followerState, followerPod, f.snapshot)
	if !status.IsSuccess() {
		tb.Fatalf("%s: PreFilter of the last member of gang %d = %v", shape, follower, status)
	}
	followerCandidates := f.candidates(result)
	waitingPod := f.member(waiter, last-1)
	waiting := f.handle.waiting[waitingPod.UID]
	if waiting == nil {
		tb.Fatalf("%s: member %d of gang %d is not at the gate", shape, last-1, waiter)
	}
	freshPod := f.member(fresh, 0)

	return f, []fleetOp{
		{name: "prefilter-first", op: func() {
			state := newCycleState()
			if _, status := f.plugin.PreFilter(ctx, state, freshPod, f.snapshot); !status.IsSuccess() {
				tb.Fatalf("PreFilter of a first member = %v", status)
			}
			assignment := readAssignment(state)
			f.plugin.assignments.Release(assignment.gang.key, assignment.commitment, nil)
		}},
		{name: "prefilter-follower", op: func() {
			if _, status := f.plugin.PreFilter(ctx, newCycleState(), followerPod, f.snapshot); !status.IsSuccess() {
				tb.Fatalf("PreFilter of a last member = %v", status)
			}
		}},
		{name: "filter", op: func() {
			for _, name := range followerCandidates {
				if status := f.plugin.Filter(ctx, followerState, followerPod, f.entries[name]); !status.IsSuccess() {
					tb.Fatalf("Filter of candidate %s = %v", name, status)
				}
			}
		}},
		{name: "permit-wait", gate: true, op: func() {
			f.handle.activated = f.handle.activated[:0]
			if status, _ := f.plugin.Permit(ctx, waiting.state, waitingPod, waiting.node); !status.IsWait() {
				tb.Fatalf("Permit of a waiting member = %v, want Wait", status)
			}
		}},
		{name: "permit-admit", gate: true, op: func() {
			f.handle.activated = f.handle.activated[:0]
			if status, _ := f.plugin.Permit(ctx, admitState, admitPod, admitNode); !status.IsSuccess() {
				tb.Fatalf("Permit of a gang's last member = %v, want Success", status)
			}
		}},
		{name: "gang", gate: true, op: func() {
			f.handle.activated = f.handle.activated[:0]
			for m := 0; m < shape.gangSize; m++ {
				want := cycleWaiting
				if m == last {
					want = cycleBound
				}
				if outcome := f.scheduleOne(f.member(fresh, m)); outcome != want {
					tb.Fatalf("member %d of a whole gang ended in %d, want %d", m, outcome, want)
				}
			}
			f.unscheduleGang(fresh)
		}},
	}
}

// readsOf runs op once and returns what it read.
func (f *permitFleet) readsOf(op func()) fleetReads {
	start := f.reads
	op()
	return f.reads.since(start)
}

// fleetOrders are the two orders a pending set is drained in.
var fleetOrders = []string{"interleaved", "gang-major"}

func (f *permitFleet) ordered(order string) []*v1.Pod {
	if order == "gang-major" {
		return f.gangMajor()
	}
	return f.interleaved()
}

// heapCounter reads the heap's running totals, so a benchmark can report
// what one gang or one pod of a whole run allocated.
type heapCounter struct{ stats runtime.MemStats }

func (c *heapCounter) read() (allocations, bytes uint64) {
	runtime.ReadMemStats(&c.stats)
	return c.stats.Mallocs, c.stats.TotalAlloc
}

// BenchmarkPermitFleet is the cost of each operation of a gang member's cycle
// per fleet shape, with the node and waiting-pod reads one call makes; for the
// whole gang, the allocations and bytes per pod beside those per gang.
func BenchmarkPermitFleet(b *testing.B) {
	for _, shape := range fleetShapes {
		f, ops := fleetOps(b, shape)
		for _, op := range ops {
			b.Run(shape.String()+"/"+op.name, func(b *testing.B) {
				reads := f.readsOf(op.op)
				b.ReportAllocs()
				var heap heapCounter
				allocations, bytes := heap.read()
				for b.Loop() {
					op.op()
				}
				b.ReportMetric(float64(reads.nodes), "node-reads/op")
				b.ReportMetric(float64(reads.gangLists), "gang-lists/op")
				b.ReportMetric(float64(reads.waitingScans), "waiting-scans/op")
				b.ReportMetric(float64(reads.waitingPods), "waiting-pod-reads/op")
				b.ReportMetric(float64(reads.waitingLookups), "waiting-lookups/op")
				if op.name == "gang" {
					allocationsAfter, bytesAfter := heap.read()
					pods := float64(b.N * shape.gangSize)
					b.ReportMetric(float64(allocationsAfter-allocations)/pods, "allocs/pod")
					b.ReportMetric(float64(bytesAfter-bytes)/pods, "B/pod")
				}
			})
		}
	}
}

// BenchmarkPermitFleetDrain is the time to drain a whole pending set, with
// the cycles, reads, allocations and bytes per gang it took. One drain is
// seconds to minutes, so run it with -benchtime=1x.
func BenchmarkPermitFleetDrain(b *testing.B) {
	for _, shape := range fleetShapes {
		for _, order := range fleetOrders {
			b.Run(shape.String()+"/"+order, func(b *testing.B) {
				b.ReportAllocs()
				var reads fleetReads
				var heap heapCounter
				var allocations, bytes uint64
				cycles := 0
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					f := newPermitFleet(b, shape)
					pods := f.ordered(order)
					allocationsBefore, bytesBefore := heap.read()
					b.StartTimer()
					f.drain(pods)
					b.StopTimer()
					allocationsAfter, bytesAfter := heap.read()
					allocations += allocationsAfter - allocationsBefore
					bytes += bytesAfter - bytesBefore
					b.StartTimer()
					reads, cycles = f.reads, f.cycles
				}
				gangs := float64(shape.gangs)
				b.ReportMetric(float64(cycles)/gangs, "cycles/gang")
				b.ReportMetric(float64(reads.nodes)/gangs, "node-reads/gang")
				b.ReportMetric(float64(reads.gangLists)/gangs, "gang-lists/gang")
				b.ReportMetric(float64(reads.waitingScans)/gangs, "waiting-scans/gang")
				b.ReportMetric(float64(reads.waitingPods)/gangs, "waiting-pod-reads/gang")
				b.ReportMetric(float64(reads.waitingLookups)/gangs, "waiting-lookups/gang")
				b.ReportMetric(float64(allocations)/float64(b.N)/gangs, "allocs/gang")
				b.ReportMetric(float64(bytes)/float64(b.N)/gangs, "B/gang")
			})
		}
	}
}

// fleetLockShapes are the shapes the unit step holds to the goldens: 504-node
// fleets with the same per-node and per-waiting-pod structure as the measured
// ones, so a read or an allocation added per element shows, in the time the
// race detector allows.
var fleetLockShapes = []fleetShape{
	{domains: 28, gangSize: 2, gangs: 100},
	{domains: 28, gangSize: 4, gangs: 100},
	{domains: 28, gangSize: 8, gangs: 56},
}

// fleetDrainShapes are the shapes whose whole drain runs in the unit step:
// more gangs pending than domains, so the retries every gang completion
// causes are part of the count.
var fleetDrainShapes = []fleetShape{
	{domains: 12, gangSize: 8, gangs: 24},
}

// TestPermitFleet_CycleCostStaysWithinBudget holds, per lock shape and
// operation (the whole gang among them), the allocations to
// testdata/permit_fleet_allocations.json and the node reads and gang lists,
// with the gate's waiting-set scans, waiting-pod reads and waiting-pod
// lookups, to testdata/permit_fleet_reads.json.
func TestPermitFleet_CycleCostStaysWithinBudget(t *testing.T) {
	allocations := allocbudget.Open(t, filepath.Join("testdata", "permit_fleet_allocations.json"), *updateFleetGoldens)
	defer allocations.Close()
	reads := allocbudget.Open(t, filepath.Join("testdata", "permit_fleet_reads.json"), *updateFleetGoldens)
	defer reads.Close()
	for _, shape := range fleetLockShapes {
		f, ops := fleetOps(t, shape)
		for _, op := range ops {
			name := shape.String() + "/" + op.name
			allocations.Check(name, op.op)
			read := f.readsOf(op.op)
			reads.CheckCount(name+"/node-reads", read.nodes)
			reads.CheckCount(name+"/gang-lists", read.gangLists)
			if op.gate {
				reads.CheckCount(name+"/waiting-set-scans", read.waitingScans)
				reads.CheckCount(name+"/waiting-pod-reads", read.waitingPods)
				reads.CheckCount(name+"/waiting-pod-lookups", read.waitingLookups)
			}
		}
	}
}

// TestPermitFleet_PendingSetDrains drains each drain shape in both orders,
// holds the result to the gang rules (every pod bound, one per node, every
// gang whole in one domain) and the cycles, requeues, reads, lists, scans,
// lookups and activations it took to testdata/permit_fleet_drain.json.
func TestPermitFleet_PendingSetDrains(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "permit_fleet_drain.json"), *updateFleetGoldens)
	defer golden.Close()
	for _, shape := range fleetDrainShapes {
		for _, order := range fleetOrders {
			f := newPermitFleet(t, shape)
			start := time.Now()
			f.drain(f.ordered(order))
			f.checkDrained()
			t.Logf("%s %s: %d cycles (%.1f per gang), %d found no node, %d requeues of the set, per gang %d node reads, %d gang lists, %d waiting-set scans over %d waiting pods and %d lookups, %v",
				shape, order, f.cycles, float64(f.cycles)/float64(shape.gangs), f.unschedulable, f.flushes,
				f.reads.nodes/int64(shape.gangs), f.reads.gangLists/int64(shape.gangs), f.reads.waitingScans/int64(shape.gangs),
				f.reads.waitingPods/int64(shape.gangs), f.reads.waitingLookups/int64(shape.gangs), time.Since(start).Round(time.Millisecond))
			name := shape.String() + "/" + order
			golden.CheckCount(name+"/cycles", int64(f.cycles))
			golden.CheckCount(name+"/requeues", int64(f.flushes))
			golden.CheckCount(name+"/node-reads", f.reads.nodes)
			golden.CheckCount(name+"/gang-lists", f.reads.gangLists)
			golden.CheckCount(name+"/waiting-set-scans", f.reads.waitingScans)
			golden.CheckCount(name+"/waiting-pod-reads", f.reads.waitingPods)
			golden.CheckCount(name+"/waiting-pod-lookups", f.reads.waitingLookups)
			golden.CheckCount(name+"/activations", f.reads.activations)
		}
	}
}
