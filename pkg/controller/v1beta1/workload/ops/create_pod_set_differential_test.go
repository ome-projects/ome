package ops

import (
	"math/rand/v2"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The pod set is read against the pods' names as they stand; the form it
// replaces built every name of the set and compared the pods' names with
// them through an index. The reference below is that form, kept beside the
// reading, and the test holds the two to the same answers over random
// pictures of an Instance and its pods.

// builtPodNames is the reference reading: every name of the set built, as
// expectedPodNamesForInstance built it.
func builtPodNames(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) []podTarget {
	total := inst.TotalPods()
	if total <= 0 {
		return nil
	}
	targets := make([]podTarget, 0, total)
	for _, runner := range inst.Runners {
		if runner.Size == 1 {
			ordinal := activeOrdinalForInstance(input, inst.Index)
			targets = append(targets, podTarget{Name: query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, ordinal), Runner: runner, Ordinal: ordinal})
			continue
		}
		for o := int32(0); o < runner.Size; o++ {
			targets = append(targets, podTarget{Name: query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, o), Runner: runner, Ordinal: o})
		}
	}
	return targets
}

// missingByBuiltNames is the reference diff: the built names indexed against
// the live pods' names.
func missingByBuiltNames(desired []podTarget, existing []*corev1.Pod) []podTarget {
	existingByName := query.IndexPodsByName(query.ExcludeTerminalPods(existing))
	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}
	return missing
}

// podsAtBuiltNames is the reference subset: the pods whose name is among the
// built names.
func podsAtBuiltNames(existing []*corev1.Pod, targets []podTarget) []*corev1.Pod {
	names := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		names[t.Name] = struct{}{}
	}
	out := make([]*corev1.Pod, 0, len(existing))
	for _, pod := range existing {
		if pod == nil {
			continue
		}
		if _, ok := names[pod.Name]; ok {
			out = append(out, pod)
		}
	}
	return out
}

var (
	podSetIndexes = []int32{0, 1, 7, 10, 11, 70, 100, 2147483647}
	podSetRunners = []string{workload.RunnerDefault, workload.RunnerLeader, workload.RunnerWorker, "workers", "worker-2"}
	podSetOwners  = []string{createNamesOwner, createNamesOwner + "x", "llama", "svc-engine"}
)

// randomPodSetPicture draws an Instance (its index, its runners of random
// names and sizes, among them a size of none, of 70 and of 300, which sends
// the set past its marks, and the ordinal its row records) and a pod list
// around it:
// the set's own pods, pods named for a neighbouring Instance, another
// runner, ordinal or spelling, another service or Component, terminal pods,
// nil entries and duplicates.
func randomPodSetPicture(rng *rand.Rand) (workload.ReconcileInput, workload.ComponentPlan, workload.InstancePlan, []*corev1.Pod) {
	index := podSetIndexes[rng.IntN(len(podSetIndexes))]
	inst := workload.InstancePlan{Index: index, Incarnation: 1}
	for n := rng.IntN(4); n > 0; n-- {
		size := int32(rng.IntN(5))
		switch rng.IntN(40) {
		case 0:
			size = -2
		case 1:
			size = 70
		case 2:
			size = 300
		}
		inst.Runners = append(inst.Runners, workload.RunnerPlan{Name: podSetRunners[rng.IntN(len(podSetRunners))], Size: size})
	}
	var rows []workload.InstanceStatus
	if rng.IntN(3) > 0 {
		row := readyRow(index, createFinishRevision)
		row.ActiveOrdinal = int32(rng.IntN(3))
		rows = append(rows, row)
	}
	input := workload.ReconcileInput{Key: workload.Key{Namespace: createNamesNamespace, OwnerName: createNamesOwner, Component: workload.ComponentEngine},
		ObservedState: workload.WorkloadObservedState{InstanceStatuses: rows}}
	input.ObservedState.IndexRows()
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, Replicas: 1, Instances: []workload.InstancePlan{inst}}
	own := builtPodNames(input, plan, inst)
	var pods []*corev1.Pod
	for n := rng.IntN(8); n > 0; n-- {
		var name string
		switch kind := rng.IntN(10); {
		case kind < 4 && len(own) > 0:
			name = own[rng.IntN(len(own))].Name
		case kind == 4:
			name = podSetName(podSetIndexes[rng.IntN(len(podSetIndexes))], podSetRunners[rng.IntN(len(podSetRunners))], []int32{0, 1, 2, 3, 4, 69, 70, 299, 300, -1}[rng.IntN(10)])
		case kind == 5:
			name = query.PodName(podSetOwners[rng.IntN(len(podSetOwners))], []workload.ComponentType{workload.ComponentEngine, workload.ComponentDecoder}[rng.IntN(2)], index, podSetRunners[rng.IntN(len(podSetRunners))], int32(rng.IntN(3)))
		case kind == 6 && len(own) > 0:
			name = own[rng.IntN(len(own))].Name + []string{"0", "-x", "x", ""}[rng.IntN(4)]
		case kind == 7:
			pods = append(pods, nil)
			continue
		default:
			name = podSetName(index, podSetRunners[rng.IntN(len(podSetRunners))], int32(rng.IntN(3)))
		}
		pod := podNamed(index, name)
		switch rng.IntN(6) {
		case 0:
			pod.Status.Phase = corev1.PodFailed
		case 1:
			pod.Status.Phase = corev1.PodSucceeded
		}
		pods = append(pods, pod)
	}
	return input, plan, inst, pods
}

// TestPodSetNames_AgreesWithTheBuiltNamesOnRandomPictures holds, over
// random pictures, that the set reads as the built names read: the same
// targets built, the same pods short, the same pods at the set's names and
// the same answer on every pod's name.
func TestPodSetNames_AgreesWithTheBuiltNamesOnRandomPictures(t *testing.T) {
	rng := rand.New(rand.NewPCG(30, 1107))
	draws, short, held, named := 100000, 0, 0, 0
	for i := 0; i < draws; i++ {
		input, plan, inst, pods := randomPodSetPicture(rng)
		set := podSetNamesOf(input, plan, inst)
		built := builtPodNames(input, plan, inst)
		if got := set.targets(); !reflect.DeepEqual(got, built) {
			t.Fatalf("draw %d: the set builds %+v, the reference %+v", i, got, built)
		}
		if got := expectedPodNamesForInstance(input, plan, inst); !reflect.DeepEqual(got, built) {
			t.Fatalf("draw %d: expectedPodNamesForInstance builds %+v, the reference %+v", i, got, built)
		}
		missing, wantMissing := missingPodTargets(set, pods), missingByBuiltNames(built, pods)
		if !reflect.DeepEqual(missing, wantMissing) {
			t.Fatalf("draw %d: the set is short of %+v, the reference of %+v (pods %v)", i, missing, wantMissing, podNamesOrNil(pods))
		}
		short += len(missing)
		at, wantAt := podsAtTargets(pods, set), podsAtBuiltNames(pods, built)
		if !samePods(at, wantAt) {
			t.Fatalf("draw %d: the pods at the set's names are %v, the reference's %v", i, podNamesOrNil(at), podNamesOrNil(wantAt))
		}
		held += len(at)
		for _, pod := range pods {
			if pod == nil {
				continue
			}
			if got, want := set.holds(pod.Name), builtNamesHold(built, pod.Name); got != want {
				t.Fatalf("draw %d: the set holds %q: %t, the built names: %t", i, pod.Name, got, want)
			}
			named++
		}
	}
	t.Logf("%d draws: %d pods short, %d pods at the set's names, %d names read", draws, short, held, named)
	if short == 0 || held == 0 {
		t.Fatalf("the draws are one-sided: %d short, %d at the set's names", short, held)
	}
}

func builtNamesHold(targets []podTarget, name string) bool {
	for _, target := range targets {
		if target.Name == name {
			return true
		}
	}
	return false
}

func samePods(got, want []*corev1.Pod) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func podNamesOrNil(pods []*corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		if pod == nil {
			names = append(names, "<nil>")
			continue
		}
		names = append(names, pod.Name)
	}
	return names
}
