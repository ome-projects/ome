package gang

import (
	"context"
	"flag"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateGangPassRecordsGoldens = flag.Bool("update-gang-pass-records", false, "re-record testdata/gang_pass_records_allocations.json from this run")

// cachedPodList is the Component's pods as the cache lists them for a
// pass, before the pass groups them.
func (f *podGroupPassFixture) cachedPodList(tb testing.TB) []*corev1.Pod {
	tb.Helper()
	pods, err := query.ListOMENativePodsByName(context.Background(), f.deps.Client, f.input.Key.Namespace,
		f.input.Key.OwnerName, f.plan.Component, true)
	if err != nil {
		tb.Fatalf("list the Component's pods: %v", err)
	}
	return pods
}

// buildRecords is the pass's own work per gang over one listed pod list:
// the pods grouped under the PodGroup name each carries, and the write
// record of every planned gang over them, carrying the gang's name and
// its pods.
func (p *podGroupPass) buildRecords(pods []*corev1.Pod) {
	p.groupPods(pods)
	for _, inst := range p.gangs {
		p.writeOf(inst)
	}
}

// gangRecord is what one planned gang's write record carries: the gang's
// PodGroup name and the names of its pods, in the order the list had them.
type gangRecord struct {
	name string
	pods []string
}

// recordOf reads one planned gang's write record.
func (p *podGroupPass) recordOf(inst *workloadtypes.InstancePlan) gangRecord {
	write := p.writeOf(inst)
	rec := gangRecord{name: write.name}
	for _, pod := range write.pods {
		rec.pods = append(rec.pods, pod.Name)
	}
	return rec
}

// podsNamed are the names of the pods of the list that carry the PodGroup
// name, in list order.
func podsNamed(pods []*corev1.Pod, name string) []string {
	var names []string
	for _, pod := range pods {
		if pod != nil && pod.Labels[query.LabelPodGroup] == name {
			names = append(names, pod.Name)
		}
	}
	return names
}

// requireRecord holds one gang's record to what it must carry, and that
// the pass hands out one record per gang however often it is asked.
func requireRecord(tb testing.TB, pass *podGroupPass, inst *workloadtypes.InstancePlan, want gangRecord) {
	tb.Helper()
	got := pass.recordOf(inst)
	if got.name != want.name || !slices.Equal(got.pods, want.pods) {
		tb.Fatalf("gang %d: record name %q pods %v, want name %q pods %v", inst.Index, got.name, got.pods, want.name, want.pods)
	}
	first := pass.writeOf(inst)
	if again := pass.writeOf(inst); again != first {
		tb.Fatalf("gang %d: asked twice, the pass hands out two records; want one per gang and pod list", inst.Index)
	}
}

// requireRecordsAtRest holds what the golden cannot express as a count:
// every planned gang's record carries the gang's deterministic name and
// every pod of the gang, in list order, and asked again the pass hands out
// the same record.
func (f *podGroupPassFixture) requireRecordsAtRest(tb testing.TB, pass *podGroupPass, pods []*corev1.Pod) {
	tb.Helper()
	for _, inst := range pass.gangs {
		want := gangRecord{name: query.PodGroupName(f.input.Key.OwnerName, f.plan.Component, inst.Index)}
		want.pods = podsNamed(pods, want.name)
		if len(want.pods) != int(inst.TotalPods()) {
			tb.Fatalf("gang %d: the list carries %d of its %d pods", inst.Index, len(want.pods), inst.TotalPods())
		}
		requireRecord(tb, pass, inst, want)
	}
}

// surgeWriteAtRest is the surge callback's write of one gang of a Component
// at rest: the owner's groups brought as the apiserver lists them and the
// Component's pods brought with the input, so the callback lists nothing,
// reads the name from the inventory and, the group being the plan's,
// writes nothing. The gang is the plan's last.
func (f *podGroupPassFixture) surgeWriteAtRest(tb testing.TB) (write func() string, inst workloadtypes.InstancePlan) {
	tb.Helper()
	inventory, err := ObservePodGroups(context.Background(), f.live, f.input.OwnerObject)
	if err != nil {
		tb.Fatalf("ObservePodGroups: %v", err)
	}
	f.input.AuthoritativePods = &workloadtypes.ComponentPodSnapshot{OwnerUID: f.input.OwnerObject.GetUID(), Pods: f.cachedPodList(tb)}
	callback := EnsureSurgePodGroupWithState(f.deps, PodGroupReconcileState{Inventory: inventory})
	inst = f.plan.Instances[len(f.plan.Instances)-1]
	return func() string {
		key, err := callback(context.Background(), f.input, f.plan, inst)
		if err != nil {
			tb.Fatalf("surge write of gang %d: %v", inst.Index, err)
		}
		return key
	}, inst
}

// setupOf is the pass's setup over the plan, before anything is read: the
// planned gangs, how each is found by its index, and the keys to publish.
func (f *podGroupPassFixture) setupOf() *podGroupPass {
	return newPodGroupPass(f.deps, &f.input, f.plan, f.state.Inventory)
}

// podUnder is a listed pod carrying one PodGroup name.
func podUnder(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelPodGroup: name}}}
}

// requireSetup holds what the setup's golden cannot express as a count:
// the pass lists the plan's multi-pod entries in plan order, and a pod
// carrying a planned gang's name is placed at that gang's position, the
// first when the plan lists the index twice.
func requireSetup(tb testing.TB, pass *podGroupPass, plan workloadtypes.ComponentPlan, owner string) {
	tb.Helper()
	var want []int32
	for _, inst := range plan.Instances {
		if inst.TotalPods() > 1 {
			want = append(want, inst.Index)
		}
	}
	if len(pass.gangs) != len(want) {
		tb.Fatalf("the setup lists %d gangs, want the plan's %d", len(pass.gangs), len(want))
	}
	firstPos := map[int32]int{}
	for pos, inst := range pass.gangs {
		if inst.Index != want[pos] {
			tb.Fatalf("gang at position %d is Instance %d, want %d in plan order", pos, inst.Index, want[pos])
		}
		if _, listed := firstPos[inst.Index]; !listed {
			firstPos[inst.Index] = pos
		}
		name := query.PodGroupName(owner, plan.Component, inst.Index)
		gotName, gotPos, planned := pass.gangOf(podUnder(name))
		if !planned || gotPos != firstPos[inst.Index] || gotName != name {
			tb.Fatalf("a pod under %s is placed at position %d planned=%t, want position %d of a planned gang", name, gotPos, planned, firstPos[inst.Index])
		}
	}
}

// checkSetupAtRest holds the pass's setup over the plan to the golden: the
// allocations and the bytes of listing the planned gangs and of how each
// is found by its index, and beside it what the golden cannot express.
func checkSetupAtRest(t *testing.T, golden *allocbudget.File, shape podGroupPassShape) {
	t.Helper()
	f := newPodGroupPassFixture(t, shape, false)
	golden.Check(shape.name+"/setup/allocations", func() { f.setupOf() })
	golden.CheckBytes(shape.name+"/setup/bytes", func() { f.setupOf() })
	requireSetup(t, f.setupOf(), f.plan, f.input.Key.OwnerName)
}

// checkRecordsAtRest holds the pass's own work per gang over one listed pod
// list to the golden, and beside it what the golden cannot express.
func checkRecordsAtRest(t *testing.T, golden *allocbudget.File, shape podGroupPassShape) {
	t.Helper()
	f := newPodGroupPassFixture(t, shape, false)
	pods := f.cachedPodList(t)
	pass := newPodGroupPass(f.deps, &f.input, f.plan, f.state.Inventory)
	golden.Check(shape.name+"/records/allocations", func() { pass.buildRecords(pods) })
	golden.CheckBytes(shape.name+"/records/bytes", func() { pass.buildRecords(pods) })
	pass.buildRecords(pods)
	f.requireRecordsAtRest(t, pass, pods)
	if n := f.writes.Count(); n != 0 {
		t.Fatalf("%s: building the records wrote %d times; want none", shape.name, n)
	}
}

// checkSurgeWriteAtRest holds the surge callback's write of one gang at
// rest to the golden, and beside it that the write answers the plan's key,
// records the gang's reading, lists no pod and writes nothing.
func checkSurgeWriteAtRest(t *testing.T, golden *allocbudget.File, shape podGroupPassShape) {
	t.Helper()
	f := newPodGroupPassFixture(t, shape, true)
	write, inst := f.surgeWriteAtRest(t)
	golden.Check(shape.name+"/surge-write/allocations", func() { write() })
	golden.CheckBytes(shape.name+"/surge-write/bytes", func() { write() })
	if key := write(); key != podGroupPassTopologyKey {
		t.Fatalf("%s: the surge write of gang %d answers key %q; want the plan's %q", shape.name, inst.Index, key, podGroupPassTopologyKey)
	}
	if obs := f.input.Gangs.For(inst.Index); obs.Name != f.groupNameOf(inst.Index) {
		t.Fatalf("%s: the surge write recorded the gang's reading as %+v; want its group %s", shape.name, obs, f.groupNameOf(inst.Index))
	}
	if n := f.writes.Count(); n != 0 {
		t.Fatalf("%s: the surge write of a gang at rest wrote %d times; want none", shape.name, n)
	}
	if n := f.livePodLists.Count(); n != 0 {
		t.Fatalf("%s: the surge write listed the pods %d times; want none, the input brought them", shape.name, n)
	}
}

// TestGangPassRecordsAtRestStayWithinBudget holds the pass's own work per
// gang to its golden: per shape, the allocations and the bytes of the
// pass's setup over the plan, of grouping the Component's listed pods
// under their gangs and building every planned gang's write record over
// them, and of the surge callback's write of one gang at rest; beside the
// golden, the setup lists the plan's gangs in plan order and places a pod
// at its gang's position, every record carries the gang's deterministic
// name and every pod of the gang in list order, the surge write answers
// the plan's key, and nothing is written.
func TestGangPassRecordsAtRestStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "gang_pass_records_allocations.json"), *updateGangPassRecordsGoldens)
	defer golden.Close()
	for _, shape := range podGroupPassShapes() {
		checkSetupAtRest(t, golden, shape)
		checkRecordsAtRest(t, golden, shape)
		checkSurgeWriteAtRest(t, golden, shape)
	}
}

// planOrderCase is one picture of a plan's entries: their indexes in plan
// order, which of them are gangs, and the indexes no planned gang has.
type planOrderCase struct {
	name    string
	indexes []int32
	// singles are the entries of the plan that are single-pod Instances.
	singles []int32
	// unplanned are indexes a pod may carry that no planned gang has.
	unplanned []int32
}

// planOrderCases are the pictures: the dense ascending plan a pass at rest
// sees, a plan with gaps and out of index order, a plan with an index far
// above its count, a plan listing one index twice, and a plan with a
// single-pod entry among the gangs.
func planOrderCases() []planOrderCase {
	return []planOrderCase{
		{name: "dense and ascending", indexes: []int32{0, 1, 2, 3}, unplanned: []int32{4, 7}},
		{name: "gaps, out of index order", indexes: []int32{9, 0, 4}, unplanned: []int32{1, 2, 3, 5, 10}},
		{name: "an index far above the count", indexes: []int32{0, 1, 1<<31 - 1}, unplanned: []int32{2, 1<<31 - 2}},
		{name: "an index listed twice", indexes: []int32{2, 5, 2}, unplanned: []int32{0, 1}},
		{name: "a single-pod entry among the gangs", indexes: []int32{0, 1, 2}, singles: []int32{1}, unplanned: []int32{1, 3}},
	}
}

// TestGangPassFindsEachPlannedGangByItsIndexInAnyPlanOrder: whatever the
// order of the plan's entries and the gaps between their indexes, the pass
// lists the plan's gangs in plan order, places a pod under a planned
// gang's name at that gang's position, an index listed twice at the first,
// and treats a pod under any other index as no planned gang's.
func TestGangPassFindsEachPlannedGangByItsIndexInAnyPlanOrder(t *testing.T) {
	for _, tc := range planOrderCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newPodGroupPassFixture(t, decisionShape, false)
			f.plan = planFor(f.plan.Component, tc.indexes, true, 2, f.plan.InstanceReadyTimeout)
			for i := range f.plan.Instances {
				if slices.Contains(tc.singles, f.plan.Instances[i].Index) {
					f.plan.Instances[i].Runners = []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}
				}
			}
			pass := f.setupOf()
			requireSetup(t, pass, f.plan, f.input.Key.OwnerName)
			for _, index := range tc.unplanned {
				if _, _, planned := pass.gangOf(podUnder(f.groupNameOf(index))); planned {
					t.Fatalf("a pod under index %d, which no planned gang has, is placed on a planned gang", index)
				}
			}
		})
	}
}

// BenchmarkGangPassRecords is the wall time and the allocations of the
// pass's own work per gang over a listed pod list.
func BenchmarkGangPassRecords(b *testing.B) {
	for _, shape := range podGroupPassShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newPodGroupPassFixture(b, shape, false)
			pods := f.cachedPodList(b)
			pass := newPodGroupPass(f.deps, &f.input, f.plan, f.state.Inventory)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				pass.buildRecords(pods)
			}
		})
	}
}

// longOwner is a service name long enough that its gangs' PodGroup names
// pass the label limit and are hashed, so the name a pod carries is not
// the plain owner-component-index form.
var longOwner = strings.Repeat("a", 56)

// podOf names one pod of a gang: the leader, or the worker of one ordinal.
func (f *podGroupPassFixture) podOf(index int32, runner string, ordinal int32) string {
	return query.PodName(f.input.Key.OwnerName, f.plan.Component, index, runner, ordinal)
}

// groupNameOf is the deterministic PodGroup name of one gang of the
// fixture's service.
func (f *podGroupPassFixture) groupNameOf(index int32) string {
	return query.PodGroupName(f.input.Key.OwnerName, f.plan.Component, index)
}

// listed finds one pod of the list by name.
func listed(tb testing.TB, pods []*corev1.Pod, name string) *corev1.Pod {
	tb.Helper()
	for _, pod := range pods {
		if pod != nil && pod.Name == name {
			return pod
		}
	}
	tb.Fatalf("the list has no pod %s", name)
	return nil
}

// without is the list without the named pod.
func without(tb testing.TB, pods []*corev1.Pod, name string) []*corev1.Pod {
	tb.Helper()
	listed(tb, pods, name)
	return slices.DeleteFunc(slices.Clone(pods), func(pod *corev1.Pod) bool { return pod != nil && pod.Name == name })
}

// gangRecordCase is one picture of the Component's listed pods and the
// record the pass builds for one gang on it: the gang's name and its pods.
type gangRecordCase struct {
	name string
	// owner is the service's name; the fixture's when empty.
	owner string
	// change edits the listed pods and the plan, and returns the list the
	// pass groups.
	change func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod
	// gang is the Instance whose record is read; decisionGang when zero.
	gang int32
	want func(t *testing.T, f *podGroupPassFixture) gangRecord
}

// recordOfLeaderAndWorkers is the record of the gang the pictures change
// with every pod of it, in the order the fixture lists them.
func recordOfLeaderAndWorkers(_ *testing.T, f *podGroupPassFixture) gangRecord {
	return gangRecord{name: f.groupNameOf(decisionGang), pods: []string{
		f.podOf(decisionGang, workloadtypes.RunnerLeader, 0), f.podOf(decisionGang, workloadtypes.RunnerWorker, 0), f.podOf(decisionGang, workloadtypes.RunnerWorker, 1)}}
}

// recordOfLeaderAndFirstWorker is that record without the last worker.
func recordOfLeaderAndFirstWorker(_ *testing.T, f *podGroupPassFixture) gangRecord {
	return gangRecord{name: f.groupNameOf(decisionGang), pods: []string{
		f.podOf(decisionGang, workloadtypes.RunnerLeader, 0), f.podOf(decisionGang, workloadtypes.RunnerWorker, 0)}}
}

// relabelLastWorker is the change that gives the gang's last worker
// another PodGroup name.
func relabelLastWorker(name func(f *podGroupPassFixture) string) func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
	return func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
		listed(t, pods, f.podOf(decisionGang, workloadtypes.RunnerWorker, 1)).Labels[query.LabelPodGroup] = name(f)
		return pods
	}
}

// listOrderCases are the pictures of the list itself: a gang's pods stand
// in its record in the order the list had them, a pod listed twice twice,
// a nil entry and a pod of another gang between them left out.
func listOrderCases() []gangRecordCase {
	const gang = decisionGang
	return []gangRecordCase{
		{name: "every pod of the gang, in the order listed",
			change: func(_ *testing.T, _ *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod { return pods },
			want:   recordOfLeaderAndWorkers},
		{name: "the gang's pods listed workers first",
			change: func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				leader := f.podOf(gang, workloadtypes.RunnerLeader, 0)
				return append(without(t, pods, leader), listed(t, pods, leader))
			},
			want: func(_ *testing.T, f *podGroupPassFixture) gangRecord {
				return gangRecord{name: f.groupNameOf(gang), pods: []string{
					f.podOf(gang, workloadtypes.RunnerWorker, 0), f.podOf(gang, workloadtypes.RunnerWorker, 1), f.podOf(gang, workloadtypes.RunnerLeader, 0)}}
			}},
		{name: "the gang's pods listed between the other gang's",
			change: func(_ *testing.T, _ *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				half := len(pods) / 2
				var interleaved []*corev1.Pod
				for i := 0; i < half; i++ {
					interleaved = append(interleaved, pods[i], pods[half+i])
				}
				return interleaved
			},
			want: recordOfLeaderAndWorkers},
		{name: "a worker missing from the list",
			change: func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				return without(t, pods, f.podOf(gang, workloadtypes.RunnerWorker, 1))
			},
			want: recordOfLeaderAndFirstWorker},
		{name: "a pod listed twice",
			change: func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				return append(pods, listed(t, pods, f.podOf(gang, workloadtypes.RunnerWorker, 0)))
			},
			want: func(t *testing.T, f *podGroupPassFixture) gangRecord {
				rec := recordOfLeaderAndWorkers(t, f)
				rec.pods = append(rec.pods, f.podOf(gang, workloadtypes.RunnerWorker, 0))
				return rec
			}},
		{name: "a nil entry in the list",
			change: func(_ *testing.T, _ *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				return slices.Insert(pods, len(pods)/2, nil)
			},
			want: recordOfLeaderAndWorkers},
	}
}

// otherNameCases are the pictures of a pod's labels: a pod is the gang's
// when it carries the gang's name, whatever its index label says, and a
// pod carrying no name or a name no planned gang has is nobody's.
func otherNameCases() []gangRecordCase {
	const gang = decisionGang
	return []gangRecordCase{
		{name: "a pod with no PodGroup label",
			change: func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				delete(listed(t, pods, f.podOf(gang, workloadtypes.RunnerWorker, 1)).Labels, query.LabelPodGroup)
				return pods
			},
			want: recordOfLeaderAndFirstWorker},
		{name: "a pod carrying the gang's name under another Instance's index label",
			change: func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				listed(t, pods, f.podOf(gang, workloadtypes.RunnerWorker, 1)).Labels[query.LabelInstanceIdx] = "5"
				return pods
			},
			want: recordOfLeaderAndWorkers},
		{name: "a pod carrying the gang's name with no index label",
			change: func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				delete(listed(t, pods, f.podOf(gang, workloadtypes.RunnerWorker, 1)).Labels, query.LabelInstanceIdx)
				return pods
			},
			want: recordOfLeaderAndWorkers},
		{name: "a pod whose name is an Instance the plan does not list",
			change: func(t *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				pod := listed(t, pods, f.podOf(gang, workloadtypes.RunnerWorker, 1))
				pod.Labels[query.LabelPodGroup] = f.groupNameOf(7)
				pod.Labels[query.LabelInstanceIdx] = "7"
				return pods
			},
			want: recordOfLeaderAndFirstWorker},
		{name: "a pod whose name spells the index with a leading zero",
			change: relabelLastWorker(func(f *podGroupPassFixture) string { return strings.TrimSuffix(f.groupNameOf(gang), "1") + "01" }),
			want:   recordOfLeaderAndFirstWorker},
		{name: "a pod whose name spells the index with a sign",
			change: relabelLastWorker(func(f *podGroupPassFixture) string { return strings.TrimSuffix(f.groupNameOf(gang), "1") + "+1" }),
			want:   recordOfLeaderAndFirstWorker},
		{name: "a pod whose name is another service's",
			change: relabelLastWorker(func(f *podGroupPassFixture) string { return query.PodGroupName("other", f.plan.Component, gang) }),
			want:   recordOfLeaderAndFirstWorker},
		{name: "a pod whose name is another Component's",
			change: relabelLastWorker(func(f *podGroupPassFixture) string {
				return query.PodGroupName(f.input.Key.OwnerName, workloadtypes.ComponentDecoder, gang)
			}),
			want: recordOfLeaderAndFirstWorker},
		{name: "a pod whose name is another Component's of the same length (router)",
			change: relabelLastWorker(func(f *podGroupPassFixture) string {
				return query.PodGroupName(f.input.Key.OwnerName, workloadtypes.ComponentRouter, gang)
			}),
			want: recordOfLeaderAndFirstWorker},
		{name: "a pod whose name has the gang's name as a prefix",
			change: relabelLastWorker(func(f *podGroupPassFixture) string { return f.groupNameOf(gang) + "0" }),
			want:   recordOfLeaderAndFirstWorker},
	}
}

// gangNameCases are the pictures of the gang's own name: a gang with no
// pod listed has its name and no pods, a gang above index 255 and a
// service whose names the label limit hashes keep their names.
func gangNameCases() []gangRecordCase {
	const gang = decisionGang
	return []gangRecordCase{
		{name: "a gang with no pod listed",
			change: func(_ *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				name := f.groupNameOf(gang)
				return slices.DeleteFunc(pods, func(pod *corev1.Pod) bool { return pod.Labels[query.LabelPodGroup] == name })
			},
			want: func(_ *testing.T, f *podGroupPassFixture) gangRecord { return gangRecord{name: f.groupNameOf(gang)} }},
		{name: "a gang whose index is above 255",
			change: func(_ *testing.T, f *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod {
				old := f.groupNameOf(gang)
				f.plan.Instances[1].Index = 300
				for _, pod := range pods {
					if pod.Labels[query.LabelPodGroup] == old {
						pod.Labels[query.LabelPodGroup] = f.groupNameOf(300)
						pod.Labels[query.LabelInstanceIdx] = "300"
					}
				}
				return pods
			},
			gang: 300,
			want: func(t *testing.T, f *podGroupPassFixture) gangRecord {
				rec := recordOfLeaderAndWorkers(t, f)
				rec.name = f.groupNameOf(300)
				return rec
			}},
		{name: "a service whose names the label limit hashes",
			owner:  longOwner,
			change: func(_ *testing.T, _ *podGroupPassFixture, pods []*corev1.Pod) []*corev1.Pod { return pods },
			want: func(t *testing.T, f *podGroupPassFixture) gangRecord {
				rec := recordOfLeaderAndWorkers(t, f)
				if plain := f.input.Key.OwnerName + "-" + string(f.plan.Component) + "-1"; rec.name == plain || len(rec.name) != validation.LabelValueMaxLength {
					t.Fatalf("the picture's name %q is not hashed", rec.name)
				}
				return rec
			}},
	}
}

// gangRecordCases are every picture.
func gangRecordCases() []gangRecordCase {
	return slices.Concat(listOrderCases(), otherNameCases(), gangNameCases())
}

// TestGangPassRecords_OnEveryPicture holds, for each picture of the
// Component's listed pods, the record the pass builds for the gang the
// picture changes, and that every other planned gang's record is whole
// and in list order.
func TestGangPassRecords_OnEveryPicture(t *testing.T) {
	for _, tc := range gangRecordCases() {
		t.Run(tc.name, func(t *testing.T) {
			owner := tc.owner
			if owner == "" {
				owner = podGroupPassOwner
			}
			f := newPodGroupPassFixtureOf(t, owner, decisionShape, false)
			gang := tc.gang
			if gang == 0 {
				gang = decisionGang
			}
			pods := tc.change(t, f, f.cachedPodList(t))
			pass := newPodGroupPass(f.deps, &f.input, f.plan, f.state.Inventory)
			pass.groupPods(pods)
			for _, inst := range pass.gangs {
				if inst.Index == gang {
					requireRecord(t, pass, inst, tc.want(t, f))
					continue
				}
				requireRecord(t, pass, inst, gangRecord{name: f.groupNameOf(inst.Index), pods: podsNamed(pods, f.groupNameOf(inst.Index))})
			}
			if n := f.writes.Count(); n != 0 {
				t.Fatalf("building the records wrote %d times; want none", n)
			}
		})
	}
}
