package gang

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// groupingByName is the grouping of a pod list under the PodGroup name
// each pod carries, one append per pod, a nil pod and a pod with no name
// left out: the pods a planned gang's record must carry are the ones under
// the gang's deterministic name here, in this order.
func groupingByName(pods []*corev1.Pod) map[string][]*corev1.Pod {
	byName := map[string][]*corev1.Pod{}
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		name := pod.Labels[query.LabelPodGroup]
		if name == "" {
			continue
		}
		byName[name] = append(byName[name], pod)
	}
	return byName
}

// recordDraw is one random Component and pod list: the service and its
// Component, the plan with its gangs and single-pod Instances, and the
// listed pods, built from a pool of PodGroup names a pod may carry.
type recordDraw struct {
	owner     string
	component workloadtypes.ComponentType
	plan      workloadtypes.ComponentPlan
	pods      []*corev1.Pod
	// counts of the pictures drawn, for the log.
	gangPods, strays, nils, repeats int
}

// drawRecordCase draws one Component and pod list. The names a pod may
// carry: a planned gang's (plain, or hashed when the service's name is
// long), a planned single-pod Instance's, an Instance the plan does not
// list, the gang's index with a leading zero or a sign, another service's
// or Component's name, the gang's name with more after it, a 63-character
// name of another shape, no name; and the list may carry a nil entry or
// the same pod twice. A pod's index label may disagree with its name.
func drawRecordCase(r *rand.Rand) recordDraw {
	owners := []string{"fleet", "a", longOwner, strings.Repeat("b", 50)}
	components := []workloadtypes.ComponentType{workloadtypes.ComponentEngine, workloadtypes.ComponentDecoder}
	indexPool := []int32{0, 1, 2, 3, 4, 5, 7, 9, 10, 99, 255, 256, 300, 1725, 3999, 65535, 1<<31 - 1}
	d := recordDraw{owner: owners[r.IntN(len(owners))], component: components[r.IntN(len(components))]}
	r.Shuffle(len(indexPool), func(i, j int) { indexPool[i], indexPool[j] = indexPool[j], indexPool[i] })
	gangs := indexPool[:1+r.IntN(6)]
	singles := indexPool[len(gangs) : len(gangs)+r.IntN(3)]
	d.plan = planFor(d.component, gangs, true, int32(1+r.IntN(7)), 0)
	for _, index := range singles {
		d.plan.Instances = append(d.plan.Instances, workloadtypes.InstancePlan{Index: index, Incarnation: 1,
			Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}})
	}
	r.Shuffle(len(d.plan.Instances), func(i, j int) { d.plan.Instances[i], d.plan.Instances[j] = d.plan.Instances[j], d.plan.Instances[i] })
	nameOf := func(index int32) string { return query.PodGroupName(d.owner, d.component, index) }
	for i, n := 0, r.IntN(15); i < n; i++ {
		gang := gangs[r.IntN(len(gangs))]
		var name string
		switch r.IntN(14) {
		case 0, 1, 2, 3, 4:
			name = nameOf(gang)
			d.gangPods++
		case 5:
			if len(singles) > 0 {
				name = nameOf(singles[r.IntN(len(singles))])
			} else {
				name = nameOf(gang + 1000)
			}
		case 6:
			name = nameOf(gang + 1000)
		case 7:
			name = fmt.Sprintf("%s-%s-0%d", d.owner, d.component, gang)
		case 8:
			name = fmt.Sprintf("%s-%s-+%d", d.owner, d.component, gang)
		case 9:
			name = query.PodGroupName("other", d.component, gang)
		case 10:
			name = query.PodGroupName(d.owner, workloadtypes.ComponentRouter, gang)
		case 11:
			name = nameOf(gang) + []string{"0", "-0", "x"}[r.IntN(3)]
		case 12:
			name = strings.Repeat("h", 63)
		case 13:
			if len(d.pods) > 0 && r.IntN(2) == 0 {
				d.pods = append(d.pods, d.pods[r.IntN(len(d.pods))])
				d.repeats++
			} else {
				d.pods = append(d.pods, nil)
				d.nils++
			}
			continue
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", i), Labels: map[string]string{}}}
		if r.IntN(8) != 0 {
			pod.Labels[query.LabelPodGroup] = name
		}
		switch r.IntN(4) {
		case 0:
			pod.Labels[query.LabelInstanceIdx] = fmt.Sprint(gang)
		case 1:
			pod.Labels[query.LabelInstanceIdx] = fmt.Sprint(gangs[r.IntN(len(gangs))])
		case 2:
			pod.Labels[query.LabelInstanceIdx] = "x"
		}
		if pod.Labels[query.LabelPodGroup] != "" && pod.Labels[query.LabelPodGroup] != nameOf(gang) {
			d.strays++
		}
		d.pods = append(d.pods, pod)
	}
	return d
}

// TestGroupPods_AgreesWithTheGroupingByNameOnRandomLists draws 300,000
// Components and pod lists and holds every planned gang's record equal to
// the grouping by the PodGroup label looked up under the gang's
// deterministic name: the same name, and the same pods, pointer for
// pointer, in list order; asked twice, the same record; every record over
// the pass's one set of readings and the plan's own entry for the gang.
func TestGroupPods_AgreesWithTheGroupingByNameOnRandomLists(t *testing.T) {
	r := rand.New(rand.NewPCG(26, 1725))
	const draws = 300_000
	var gangsChecked, gangsWithPods, gangPods, strays, nils, repeats, hashed int
	for draw := 0; draw < draws; draw++ {
		d := drawRecordCase(r)
		input := workloadtypes.ReconcileInput{Key: workloadtypes.Key{Namespace: "prod", Component: d.component, OwnerName: d.owner}}
		pass := newPodGroupPass(workloadtypes.Deps{}, &input, d.plan, nil)
		pass.groupPods(d.pods)
		byName := groupingByName(d.pods)
		for i, inst := range pass.gangs {
			name := query.PodGroupName(d.owner, d.component, inst.Index)
			got := pass.writeOf(inst)
			if got.gangPassShared != &pass.gangPassShared || got.inst != pass.gangs[i] {
				t.Fatalf("draw %d: gang %d's record reads readings %p and entry %p, want the pass's %p and %p", draw, inst.Index, got.gangPassShared, got.inst, &pass.gangPassShared, pass.gangs[i])
			}
			if !isPlanEntry(d.plan, got.inst) {
				t.Fatalf("draw %d: gang %d's record reads a copy of its entry, want the plan's own", draw, inst.Index)
			}
			if got.name != name {
				t.Fatalf("draw %d: gang %d's record is named %q, want %q", draw, inst.Index, got.name, name)
			}
			if want := byName[name]; !slices.Equal(got.pods, want) {
				t.Fatalf("draw %d: gang %d (%s) carries %d pods, want the %d under its name in list order", draw, inst.Index, name, len(got.pods), len(want))
			}
			if pass.writeOf(inst) != got {
				t.Fatalf("draw %d: gang %d asked twice gets two records", draw, inst.Index)
			}
			if got.inst.Index != inst.Index {
				t.Fatalf("draw %d: gang %d's record is Instance %d's", draw, inst.Index, got.inst.Index)
			}
			gangsChecked++
			if len(got.pods) > 0 {
				gangsWithPods++
			}
			if len(name) == 63 && name != fmt.Sprintf("%s-%s-%d", d.owner, d.component, inst.Index) {
				hashed++
			}
		}
		gangPods += d.gangPods
		strays += d.strays
		nils += d.nils
		repeats += d.repeats
	}
	t.Logf("%d draws: %d gangs checked, %d with pods, %d pods on a planned gang's name, %d on another name, %d nil entries, %d pods listed twice, %d hashed names",
		draws, gangsChecked, gangsWithPods, gangPods, strays, nils, repeats, hashed)
	if gangsWithPods == 0 || strays == 0 || nils == 0 || repeats == 0 || hashed == 0 {
		t.Fatalf("the draws left a picture out")
	}
}

// isPlanEntry reports whether inst is one of the plan's own entries, not a
// copy of one.
func isPlanEntry(plan workloadtypes.ComponentPlan, inst *workloadtypes.InstancePlan) bool {
	for i := range plan.Instances {
		if &plan.Instances[i] == inst {
			return true
		}
	}
	return false
}

// TestGroupPods_EveryRecordReadsThePassesReadings: the records of a pass,
// a planned gang's and the one an Instance the plan does not list gets,
// read the clients, the input, the plan and the inventory from the pass's
// one copy, so what the pass reads into them later, the apiserver's
// inventory before a write, every record sees.
func TestGroupPods_EveryRecordReadsThePassesReadings(t *testing.T) {
	f := newPodGroupPassFixture(t, decisionShape, true)
	pass := newPodGroupPass(f.deps, &f.input, f.plan, nil)
	pass.groupPods(f.cachedPodList(t))
	unplanned := workloadtypes.InstancePlan{Index: 7, Runners: f.plan.Instances[0].Runners}
	records := []*gangWrite{pass.writeOf(pass.gangs[0]), pass.writeOf(pass.gangs[1]), pass.writeOf(&unplanned)}
	if _, err := pass.readInventory(context.Background()); err != nil {
		t.Fatalf("readInventory: %v", err)
	}
	for i, record := range records {
		if record.gangPassShared != &pass.gangPassShared {
			t.Fatalf("record %d reads its own copy of the pass's readings", i)
		}
		if record.inventory != pass.inventory || record.inventory == nil {
			t.Fatalf("record %d reads inventory %p; want the one the pass read, %p", i, record.inventory, pass.inventory)
		}
		if record.deps.Client != f.deps.Client || record.input != &f.input || record.plan.Component != f.plan.Component {
			t.Fatalf("record %d reads other clients, input or plan than the pass's", i)
		}
	}
	if records[2].inst.Index != 7 || records[2].inst == pass.gangs[0] || records[2].inst == pass.gangs[1] {
		t.Fatalf("the record of an Instance the plan does not list reads entry %+v", records[2].inst)
	}
}

// TestGroupPods_ABucketGrownByACallerLeavesTheOthersWhole: every gang's
// pods are cut from one array, so a caller that appends to one gang's
// record must reallocate it and reach no other gang's pods.
func TestGroupPods_ABucketGrownByACallerLeavesTheOthersWhole(t *testing.T) {
	f := newPodGroupPassFixture(t, decisionShape, false)
	pods := f.cachedPodList(t)
	pass := newPodGroupPass(f.deps, &f.input, f.plan, f.state.Inventory)
	pass.groupPods(pods)
	first, second := pass.writeOf(pass.gangs[0]), pass.writeOf(pass.gangs[1])
	before := slices.Clone(second.pods)
	grown := append(first.pods, first.pods[0], first.pods[0], first.pods[0])
	if !slices.Equal(second.pods, before) {
		t.Fatalf("growing gang %d's pods by three changed gang %d's: %v", first.inst.Index, second.inst.Index, pass.recordOf(pass.gangs[1]).pods)
	}
	if len(grown) != len(first.pods)+3 || cap(first.pods) != len(first.pods) {
		t.Fatalf("gang %d's record holds %d pods with room for %d; want room for exactly its own", first.inst.Index, len(first.pods), cap(first.pods))
	}
}
