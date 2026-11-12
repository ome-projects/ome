package gang

import (
	"context"
	"flag"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podgroup"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updatePodGroupPassGoldens = flag.Bool("update-podgroup-pass", false, "re-record testdata/podgroup_pass_allocations.json from this run")

// podGroupPassShape is one gang Component at rest: gangs Instances, each a
// leader and workers workers, every pod running and carrying its gang's
// PodGroup name.
type podGroupPassShape struct {
	name           string
	gangs, workers int
}

// podGroupPassShapes are the budget's shapes: a service of 160 gangs of
// eight; 500 such gangs, the 4,000 pods the other pass budgets measure;
// and the largest reference gang service, 1,725 gangs of a leader
// and one worker.
func podGroupPassShapes() []podGroupPassShape {
	return []podGroupPassShape{
		{name: "160x8", gangs: 160, workers: 7},
		{name: "500x8", gangs: 500, workers: 7},
		{name: "1725x2", gangs: 1725, workers: 1},
	}
}

// podGroupPassWorlds are the two clusters the pass runs in: one with the
// PodGroup API, where every gang's group exists and matches the plan, and
// one without it, where the pass writes nothing and keeps the key the
// pods carry.
func podGroupPassWorlds() []struct {
	name      string
	available bool
} {
	return []struct {
		name      string
		available bool
	}{
		{name: "podgroup-api", available: true},
		{name: "no-podgroup-api", available: false},
	}
}

const (
	// podGroupPassOwner names the service and its namespace.
	podGroupPassOwner = "fleet"
	// podGroupPassTopologyKey is the topology key the Component is
	// configured with and every worker's affinity term carries.
	podGroupPassTopologyKey = "topology.example.com/rack"
	// podGroupPassScheduler is a gang-aware scheduler name, so the pass has
	// no scheduler warning to consider.
	podGroupPassScheduler = "gang-scheduler"
)

// cacheStandIn answers every Pod list and every PodGroup list from the
// objects held in memory, one deep copy per object read, the way the
// informer cache answers the controller: a PodGroup list keeps the groups
// in the namespace asked for and, when the list names the controller-UID
// index, those the index holds under that value. Every other call reaches
// the client underneath. podCopies counts the pods handed out as copies,
// the cache's cost of every list the pass asks for.
type cacheStandIn struct {
	client.Client
	pods       []corev1.Pod
	groups     []schedulingv1alpha1.PodGroup
	podLists   int
	podCopies  int
	groupLists int
}

func (c *cacheStandIn) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	switch typed := list.(type) {
	case *corev1.PodList:
		c.podLists++
		c.podCopies += len(c.pods)
		typed.Items = make([]corev1.Pod, len(c.pods))
		for i := range c.pods {
			c.pods[i].DeepCopyInto(&typed.Items[i])
		}
		return nil
	case *schedulingv1alpha1.PodGroupList:
		c.groupLists++
		options := client.ListOptions{}
		options.ApplyOptions(opts)
		typed.Items = typed.Items[:0]
		for i := range c.groups {
			if groupSelected(&c.groups[i], &options) {
				typed.Items = append(typed.Items, *c.groups[i].DeepCopy())
			}
		}
		return nil
	default:
		return c.Client.List(ctx, list, opts...)
	}
}

// groupSelected says whether a cached group is in the namespace the list
// asks for and carries the controller UID its field selector names, as the
// manager's cache reads the controller-UID index.
func groupSelected(group *schedulingv1alpha1.PodGroup, options *client.ListOptions) bool {
	if options.Namespace != "" && group.Namespace != options.Namespace {
		return false
	}
	if options.FieldSelector == nil {
		return true
	}
	for _, req := range options.FieldSelector.Requirements() {
		if req.Field != PodGroupControllerUIDIndexField {
			return false
		}
		if !slices.Contains(PodGroupControllerUIDIndexExtractor(group), req.Value) {
			return false
		}
	}
	return true
}

// podGroupPassFixture is one gang Component at rest behind two readers, the
// cache and the apiserver, each holding every pod and every group, with
// the counts of the pod lists and the PodGroup lists each served and of
// the writes the pass made.
type podGroupPassFixture struct {
	deps  workloadtypes.Deps
	input workloadtypes.ReconcileInput
	plan  workloadtypes.ComponentPlan
	state PodGroupReconcileState
	// cached answers the lists; cacheStore under it takes the writes.
	cached     *cacheStandIn
	cacheStore client.WithWatch
	live       client.WithWatch
	// conditions takes the Component conditions the pass writes.
	conditions     *conditionStore
	livePodLists   *fakeclient.Calls
	liveGroupLists *fakeclient.Calls
	writes         *fakeclient.Calls
}

// newPodGroupPassFixture builds the Component's pods, its plan, and in the
// world with the PodGroup API one group per gang as the pass itself would
// write it, held by both readers. The pass is given no inventory, so it
// reads the owner's groups itself, as a pass over a Component at rest
// does, and the read is part of what the golden measures.
func newPodGroupPassFixture(tb testing.TB, shape podGroupPassShape, available bool) *podGroupPassFixture {
	tb.Helper()
	return newPodGroupPassFixtureOf(tb, podGroupPassOwner, shape, available)
}

// newPodGroupPassFixtureOf is newPodGroupPassFixture for a service of the
// given name in the fixture's namespace: a name long enough that the
// label limit hashes its gangs' PodGroup names is one the pass must still
// group the pods under.
func newPodGroupPassFixtureOf(tb testing.TB, ownerName string, shape podGroupPassShape, available bool) *podGroupPassFixture {
	tb.Helper()
	owner := newOwner(podGroupPassOwner, ownerName)
	indexes := make([]int32, 0, shape.gangs)
	rows := make([]workloadtypes.InstanceStatus, 0, shape.gangs)
	for i := 0; i < shape.gangs; i++ {
		indexes = append(indexes, int32(i))
		rows = append(rows, workloadtypes.InstanceStatus{Index: int32(i), Phase: workloadtypes.InstancePhaseReady})
	}
	plan := planFor(workloadtypes.ComponentEngine, indexes, true, int32(shape.workers), 5*time.Minute)
	plan.TopologyKey = podGroupPassTopologyKey
	input, conditions := inputWithConditionStore(owner, true, int32(shape.workers), available, podGroupPassScheduler)
	input.ObservedState.InstanceStatuses = rows
	input.Gangs = workloadtypes.NewGangObservations()

	pods := gangPodsAtRest(owner, plan, shape)
	liveObjects := make([]client.Object, 0, len(pods))
	for i := range pods {
		liveObjects = append(liveObjects, &pods[i])
	}
	var groups []schedulingv1alpha1.PodGroup
	var groupObjects []client.Object
	if available {
		for _, inst := range plan.Instances {
			group, err := podgroup.BuildPodGroup(owner, testOwnerGVK, owner.GetName(), plan, input.UnschedulableGrace, inst)
			if err != nil {
				tb.Fatalf("BuildPodGroup: %v", err)
			}
			groups = append(groups, *group)
			groupObjects = append(groupObjects, group)
		}
	}
	writes := fakeclient.CountingWrites()
	cacheStore := fakeclient.New(tb, fakeclient.WithTypes(schedulingv1alpha1.AddToScheme),
		fakeclient.WithObjects(groupObjects...), writes)
	cached := &cacheStandIn{Client: cacheStore, pods: pods, groups: groups}
	livePodLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&corev1.PodList{})
	liveGroupLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	live := fakeclient.New(tb, fakeclient.WithTypes(schedulingv1alpha1.AddToScheme),
		fakeclient.WithObjects(append(liveObjects, groupObjects...)...), livePodLists, liveGroupLists)

	return &podGroupPassFixture{
		deps:           workloadtypes.Deps{Client: cached, APIReader: live},
		input:          input,
		plan:           plan,
		cached:         cached,
		cacheStore:     cacheStore,
		live:           live,
		conditions:     conditions,
		livePodLists:   livePodLists,
		liveGroupLists: liveGroupLists,
		writes:         writes,
	}
}

// removeThePodGroupAPI takes the PodGroup API out of the cluster under the
// Component: the cache has shown every group deleted, as the apiserver's
// own cleanup deletes them when the kind goes, and from now on the
// apiserver refuses every PodGroup list and write.
func (f *podGroupPassFixture) removeThePodGroupAPI(tb testing.TB) {
	tb.Helper()
	f.cached.groups = nil
	f.writes = fakeclient.CountingWrites()
	f.cached.Client = fakeclient.Wrap(tb, f.cacheStore, f.writes,
		fakeclient.FailingWrites(fakeclient.EveryCall, noPodGroupAPI()).OnKind(&schedulingv1alpha1.PodGroup{}))
	f.liveGroupLists = fakeclient.CountingVerb(fakeclient.List).OnKind(&schedulingv1alpha1.PodGroupList{})
	f.deps.APIReader = fakeclient.Wrap(tb, f.live, f.liveGroupLists,
		fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall, noPodGroupAPI()).OnKind(&schedulingv1alpha1.PodGroupList{}))
}

// gangPodsAtRest renders every gang's leader and workers the way the
// create pass renders them: the OMENative labels, the gang's PodGroup
// name, and on each worker the affinity term that follows the leader on
// the Component's topology key.
func gangPodsAtRest(owner client.Object, plan workloadtypes.ComponentPlan, shape podGroupPassShape) []corev1.Pod {
	pods := make([]corev1.Pod, 0, shape.gangs*(shape.workers+1))
	for _, inst := range plan.Instances {
		name := query.PodGroupName(owner.GetName(), plan.Component, inst.Index)
		pods = append(pods, *topologyLeaderPod(owner.GetNamespace(), owner.GetName(), plan.Component, inst.Index, name))
		for ordinal := 0; ordinal < shape.workers; ordinal++ {
			worker := topologyWorkerPod(owner.GetNamespace(), owner.GetName(), plan.Component, inst.Index, name, podGroupPassTopologyKey)
			worker.Name = query.PodName(owner.GetName(), plan.Component, inst.Index, workloadtypes.RunnerWorker, int32(ordinal))
			worker.Labels[query.LabelPodOrdinal] = strconv.Itoa(ordinal)
			pods = append(pods, *worker)
		}
	}
	return pods
}

// run is one PodGroup pass over the Component at rest.
func (f *podGroupPassFixture) run(tb testing.TB) map[int32]string {
	tb.Helper()
	topology, err := EnsurePodGroupsWithState(context.Background(), f.deps, f.input, f.plan, f.state)
	if err != nil {
		tb.Fatalf("EnsurePodGroupsWithState: %v", err)
	}
	return topology
}

// requireAtRest holds what the golden cannot express as a count: the pass
// wrote nothing, and every gang keeps the key its pods carry.
func (f *podGroupPassFixture) requireAtRest(tb testing.TB, topology map[int32]string) {
	tb.Helper()
	if n := f.writes.Count(); n != 0 {
		tb.Errorf("a pass over a Component at rest wrote %d times; want none", n)
	}
	for _, inst := range f.plan.Instances {
		if key := topology[inst.Index]; key != podGroupPassTopologyKey {
			tb.Fatalf("gang %d: topology key %q, want the pods' %q", inst.Index, key, podGroupPassTopologyKey)
		}
	}
}

// passReads are the lists the two readers served over some passes: the
// pods and the PodGroups each of the apiserver and the cache listed, and
// the pods the cache handed out as copies.
type passReads struct {
	livePods, cachedPods, liveGroups, cachedGroups int
	cachedPodCopies                                int
}

// reads are the lists served so far.
func (f *podGroupPassFixture) reads() passReads {
	return passReads{
		livePods: f.livePodLists.Count(), cachedPods: f.cached.podLists,
		liveGroups: f.liveGroupLists.Count(), cachedGroups: f.cached.groupLists,
		cachedPodCopies: f.cached.podCopies,
	}
}

// since is the lists served after before.
func (r passReads) since(before passReads) passReads {
	return passReads{
		livePods: r.livePods - before.livePods, cachedPods: r.cachedPods - before.cachedPods,
		liveGroups: r.liveGroups - before.liveGroups, cachedGroups: r.cachedGroups - before.cachedGroups,
		cachedPodCopies: r.cachedPodCopies - before.cachedPodCopies,
	}
}

// TestPodGroupPassAtRestStaysWithinBudget holds the PodGroup pass over a
// gang Component at rest to its golden: its allocations and its bytes at
// 10 percent, and exactly the pod lists and the PodGroup lists the
// apiserver served and the cache served and the pods the cache copied for
// the pass; beside the golden, no write and every gang on its pods' key.
func TestPodGroupPassAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "podgroup_pass_allocations.json"), *updatePodGroupPassGoldens)
	defer golden.Close()
	for _, world := range podGroupPassWorlds() {
		for _, shape := range podGroupPassShapes() {
			f := newPodGroupPassFixture(t, shape, world.available)
			name := world.name + "/" + shape.name
			golden.Check(name+"/allocations", func() { f.run(t) })
			golden.CheckBytes(name+"/bytes", func() { f.run(t) })
			before := f.reads()
			f.requireAtRest(t, f.run(t))
			reads := f.reads().since(before)
			golden.CheckCount(name+"/pods-listed-live", int64(reads.livePods))
			golden.CheckCount(name+"/pods-listed-cached", int64(reads.cachedPods))
			golden.CheckCount(name+"/pods-copied-cached", int64(reads.cachedPodCopies))
			golden.CheckCount(name+"/podgroups-listed-live", int64(reads.liveGroups))
			golden.CheckCount(name+"/podgroups-listed-cached", int64(reads.cachedGroups))
		}
	}
}

// TestPodGroupPassAfterTheAPIIsRemovedKeepsEveryGangOnItsPodsKey: a gang
// Component at rest with the PodGroup API, 160 gangs of eight and 500, and
// the API is removed from the cluster. The pass finds every name free in
// the cache, asks the apiserver once, is told there is no PodGroup API,
// and goes on as on a cluster without it: no write,
// GangSchedulingUnavailable=True, and every gang on the key its pods carry.
// Each reader serves one list of the pods, the cache copying every pod,
// and one of the groups.
func TestPodGroupPassAfterTheAPIIsRemovedKeepsEveryGangOnItsPodsKey(t *testing.T) {
	for _, shape := range podGroupPassShapes() {
		t.Run(shape.name, func(t *testing.T) {
			f := newPodGroupPassFixture(t, shape, true)
			f.requireAtRest(t, f.run(t))
			f.removeThePodGroupAPI(t)
			before := f.reads()
			f.requireAtRest(t, f.run(t))
			assertCondition(t, f.conditions, metav1.ConditionTrue, string(workloadtypes.ReasonPodGroupCRDNotInstalled))
			want := passReads{livePods: 1, cachedPods: 1, liveGroups: 1, cachedGroups: 1, cachedPodCopies: shape.gangs * (shape.workers + 1)}
			if reads := f.reads().since(before); reads != want {
				t.Errorf("lists served after the API's removal: %+v, want %+v", reads, want)
			}
		})
	}
}

// BenchmarkPodGroupPassAtRest is the wall time and the allocations of one
// PodGroup pass over a gang Component at rest, with the pod lists and the
// PodGroup lists each reader served per pass and the pods the cache copied.
func BenchmarkPodGroupPassAtRest(b *testing.B) {
	for _, world := range podGroupPassWorlds() {
		for _, shape := range podGroupPassShapes() {
			b.Run(world.name+"/"+shape.name, func(b *testing.B) {
				f := newPodGroupPassFixture(b, shape, world.available)
				before := f.reads()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					f.run(b)
				}
				b.StopTimer()
				reads := f.reads().since(before)
				perOp := func(n int) float64 { return float64(n) / float64(b.N) }
				b.ReportMetric(perOp(reads.livePods), "live-pod-lists/op")
				b.ReportMetric(perOp(reads.cachedPods), "cached-pod-lists/op")
				b.ReportMetric(perOp(reads.cachedPodCopies), "cached-pod-copies/op")
				b.ReportMetric(perOp(reads.liveGroups), "live-podgroup-lists/op")
				b.ReportMetric(perOp(reads.cachedGroups), "cached-podgroup-lists/op")
			})
		}
	}
}
