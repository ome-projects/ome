package ops

import (
	"context"
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	fixture "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The Create pass reads every planned Instance's pod set before it decides
// anything: the names the plan entry asks for at the attempt's revision,
// one per runner and ordinal, with a single-pod runner at the ordinal the
// row records, and which of those names no live pod holds. A Failed row is
// then read on the pods at those names alone. The pictures below say which
// names are short and which pods a Failed row is read on, pod list by pod
// list.

// podSetPicture is one Instance as the Create pass reads its pod set: the
// plan entry's runners, the row (its ordinal, its phase) and the pods
// bucketed under the Instance.
type podSetPicture struct {
	name    string
	index   int32
	runners []workload.RunnerPlan
	row     *workload.InstanceStatus
	pods    []*corev1.Pod
	// widths is the worker count each revision recorded; an attempt at a
	// revision of another width than the plan's asks for that width.
	widths map[string]int32
	// missing is every name no live pod holds, as runner/ordinal=name, in
	// runner and ordinal order.
	missing []string
	// podSet is, for a Failed row, the pods the row is read on, by name in
	// list order; nil for any other row.
	podSet []string
}

var (
	podSetSingle = []workload.RunnerPlan{{Name: workload.RunnerDefault, Size: 1}}
	podSetPair   = []workload.RunnerPlan{{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: 1}}
	podSetEight  = []workload.RunnerPlan{{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: 7}}
	podSetWide   = []workload.RunnerPlan{{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: 3}}
)

// readyRowOf is a row Ready on the fixture's revision.
func readyRowOf(index int32) *workload.InstanceStatus {
	row := readyRow(index, createFinishRevision)
	return &row
}

// podSetName is the name of one pod of the fixture's service and Component.
func podSetName(index int32, runner string, ordinal int32) string {
	return query.PodName(createNamesOwner, workload.ComponentEngine, index, runner, ordinal)
}

// podNamed is a serving pod bucketed under index with the given name,
// whatever the name says.
func podNamed(index int32, name string, changes ...func(*corev1.Pod)) *corev1.Pod {
	pod := servingPodOf(workload.ComponentEngine, index, workload.RunnerDefault, 0)
	pod.Name = name
	for _, change := range changes {
		change(pod)
	}
	return pod
}

// podAt is the pod of index at runner and ordinal, as the renderer names it.
func podAt(index int32, runner string, ordinal int32, changes ...func(*corev1.Pod)) *corev1.Pod {
	return podNamed(index, podSetName(index, runner, ordinal), changes...)
}

func succeeded(pod *corev1.Pod) { pod.Status.Phase = corev1.PodSucceeded }

// rowAtOrdinal is a Ready row whose single-pod runner serves at ordinal.
func rowAtOrdinal(index, ordinal int32) *workload.InstanceStatus {
	row := readyRow(index, createFinishRevision)
	row.ActiveOrdinal = ordinal
	return &row
}

// failedRow is a Failed row claiming no verb, the row the pass reads on the
// pods at its names.
func failedRow(index int32) *workload.InstanceStatus {
	return &workload.InstanceStatus{Index: index, Incarnation: 1, Phase: workload.InstancePhaseFailed, Admitted: true,
		LastFailure: &workload.InstanceTermination{Reason: "Error", PodName: podSetName(index, workload.RunnerDefault, 0), Time: metav1.NewTime(createFinishNow.Add(-10 * time.Minute))}}
}

func short(runner string, ordinal int32, index int32) string {
	return targetName(runner, ordinal, podSetName(index, runner, ordinal))
}

func gangOfEight(index int32) []*corev1.Pod {
	pods := []*corev1.Pod{podAt(index, workload.RunnerLeader, 0)}
	for ordinal := int32(0); ordinal < 7; ordinal++ {
		pods = append(pods, podAt(index, workload.RunnerWorker, ordinal))
	}
	return pods
}

func podSetPicturesOfOnePod() []podSetPicture {
	return []podSetPicture{
		{name: "a pod at its name", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podAt(7, workload.RunnerDefault, 0)}, missing: []string{}},
		{name: "no pod", index: 7, runners: podSetSingle, missing: []string{short(workload.RunnerDefault, 0, 7)}},
		{name: "a pod in the Failed phase holds the name and nothing else", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podAt(7, workload.RunnerDefault, 0, fixture.Failed)}, missing: []string{short(workload.RunnerDefault, 0, 7)}},
		{name: "a pod in the Succeeded phase holds the name and nothing else", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podAt(7, workload.RunnerDefault, 0, succeeded)}, missing: []string{short(workload.RunnerDefault, 0, 7)}},
		{name: "a terminal pod and a live pod at one name", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podAt(7, workload.RunnerDefault, 0, fixture.Failed), podAt(7, workload.RunnerDefault, 0)}, missing: []string{}},
		{name: "a nil entry beside the pod", index: 7, runners: podSetSingle, pods: []*corev1.Pod{nil, podAt(7, workload.RunnerDefault, 0)}, missing: []string{}},
		{name: "the row serves at ordinal 1 and the pod stands at 0", index: 7, runners: podSetSingle, row: rowAtOrdinal(7, 1), pods: []*corev1.Pod{podAt(7, workload.RunnerDefault, 0)}, missing: []string{short(workload.RunnerDefault, 1, 7)}},
		{name: "the row serves at ordinal 1 and the pod stands at 1", index: 7, runners: podSetSingle, row: rowAtOrdinal(7, 1), pods: []*corev1.Pod{podAt(7, workload.RunnerDefault, 1)}, missing: []string{}},
		{name: "a pod named for Instance 70 bucketed under Instance 7", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podNamed(7, podSetName(70, workload.RunnerDefault, 0))}, missing: []string{short(workload.RunnerDefault, 0, 7)}},
		{name: "a pod named for Instance 1 bucketed under Instance 10", index: 10, runners: podSetSingle, pods: []*corev1.Pod{podNamed(10, podSetName(1, workload.RunnerDefault, 0))}, missing: []string{short(workload.RunnerDefault, 0, 10)}},
		{name: "a pod named for another service", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podNamed(7, query.PodName(createNamesOwner+"x", workload.ComponentEngine, 7, workload.RunnerDefault, 0))}, missing: []string{short(workload.RunnerDefault, 0, 7)}},
		{name: "a pod named for another Component", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podNamed(7, query.PodName(createNamesOwner, workload.ComponentDecoder, 7, workload.RunnerDefault, 0))}, missing: []string{short(workload.RunnerDefault, 0, 7)}},
		{name: "a pod whose ordinal is spelled 00", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podNamed(7, podSetName(7, workload.RunnerDefault, 0)+"0")}, missing: []string{short(workload.RunnerDefault, 0, 7)}},
		{name: "a pod whose name carries a suffix", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podNamed(7, podSetName(7, workload.RunnerDefault, 0)+"-x")}, missing: []string{short(workload.RunnerDefault, 0, 7)}},
		{name: "a pod whose name is cut short", index: 7, runners: podSetSingle, pods: []*corev1.Pod{podNamed(7, createNamesOwner+"-engine-7-default-")}, missing: []string{short(workload.RunnerDefault, 0, 7)}},
	}
}

func podSetPicturesOfAGang() []podSetPicture {
	eightShortOfWorkerThree := gangOfEight(3)
	eightShortOfWorkerThree = append(eightShortOfWorkerThree[:4], eightShortOfWorkerThree[5:]...)
	workersOfAnotherRunner := []*corev1.Pod{podAt(3, workload.RunnerLeader, 0)}
	for ordinal := int32(0); ordinal < 7; ordinal++ {
		workersOfAnotherRunner = append(workersOfAnotherRunner, podNamed(3, podSetName(3, workload.RunnerWorker+"s", ordinal)))
	}
	allWorkersShort := make([]string, 0, 7)
	for ordinal := int32(0); ordinal < 7; ordinal++ {
		allWorkersShort = append(allWorkersShort, short(workload.RunnerWorker, ordinal, 3))
	}
	return []podSetPicture{
		{name: "a gang of eight, every pod present", index: 3, runners: podSetEight, pods: gangOfEight(3), missing: []string{}},
		{name: "a gang of eight short of its fourth worker", index: 3, runners: podSetEight, pods: eightShortOfWorkerThree, missing: []string{short(workload.RunnerWorker, 3, 3)}},
		{name: "a gang of eight whose workers are named for another runner", index: 3, runners: podSetEight, pods: workersOfAnotherRunner, missing: allWorkersShort},
		{name: "a gang of eight with a worker past its size", index: 3, runners: podSetEight, pods: append(gangOfEight(3), podAt(3, workload.RunnerWorker, 7)), missing: []string{}},
		{name: "a gang of eight whose row serves at ordinal 1 keeps its workers at 0 to 6", index: 3, runners: podSetEight, row: rowAtOrdinal(3, 1), pods: gangOfEight(3), missing: []string{short(workload.RunnerLeader, 1, 3)}},
		{name: "a gang of two in reverse list order", index: 3, runners: podSetPair, pods: []*corev1.Pod{podAt(3, workload.RunnerWorker, 0), podAt(3, workload.RunnerLeader, 0)}, missing: []string{}},
		{name: "a gang refilled beside live pods keeps the width its revision recorded", index: 3, runners: podSetWide, row: readyRowOf(3), widths: map[string]int32{createFinishRevision: 1},
			pods: []*corev1.Pod{podAt(3, workload.RunnerLeader, 0), podAt(3, workload.RunnerWorker, 0)}, missing: []string{}},
		{name: "a gang short of a worker at the width its revision recorded", index: 3, runners: podSetWide, row: readyRowOf(3), widths: map[string]int32{createFinishRevision: 1},
			pods: []*corev1.Pod{podAt(3, workload.RunnerLeader, 0)}, missing: []string{short(workload.RunnerWorker, 0, 3)}},
		{name: "a gang whose revision recorded the plan's width asks for the plan's workers", index: 3, runners: podSetWide, row: readyRowOf(3), widths: map[string]int32{createFinishRevision: 3},
			pods: []*corev1.Pod{podAt(3, workload.RunnerLeader, 0)}, missing: []string{short(workload.RunnerWorker, 0, 3), short(workload.RunnerWorker, 1, 3), short(workload.RunnerWorker, 2, 3)}},
		{name: "an Instance with no runners", index: 3, missing: []string{}},
		{name: "a runner of size zero", index: 3, runners: []workload.RunnerPlan{{Name: workload.RunnerWorker, Size: 0}}, missing: []string{}},
	}
}

func podSetPicturesOfAFailedRow() []podSetPicture {
	return []podSetPicture{
		{name: "a Failed row is read on the pods at its names, in list order", index: 7, runners: podSetPair, row: failedRow(7),
			pods:    []*corev1.Pod{podAt(7, workload.RunnerWorker, 0), podAt(7, workload.RunnerLeader, 1), nil, podNamed(7, podSetName(70, workload.RunnerLeader, 0)), podAt(7, workload.RunnerLeader, 0)},
			missing: []string{}, podSet: []string{podSetName(7, workload.RunnerWorker, 0), podSetName(7, workload.RunnerLeader, 0)}},
		{name: "a Failed row serving at ordinal 1 is read on the pod at 1", index: 7, runners: podSetSingle, row: failedRowAtOrdinal(7, 1),
			pods:    []*corev1.Pod{podAt(7, workload.RunnerDefault, 0), podAt(7, workload.RunnerDefault, 1)},
			missing: []string{}, podSet: []string{podSetName(7, workload.RunnerDefault, 1)}},
		{name: "a Failed row with no pod at its names is read on none", index: 7, runners: podSetSingle, row: failedRow(7),
			pods:    []*corev1.Pod{podNamed(7, podSetName(70, workload.RunnerDefault, 0))},
			missing: []string{short(workload.RunnerDefault, 0, 7)}, podSet: []string{}},
	}
}

func failedRowAtOrdinal(index, ordinal int32) *workload.InstanceStatus {
	row := failedRow(index)
	row.ActiveOrdinal = ordinal
	return row
}

// TestCreatePodSetReading_OnEveryPicture holds, for each picture, the
// names the pass finds no live pod at, at the width the attempt's revision
// recorded, and, for a Failed row, the pods the row is read on.
func TestCreatePodSetReading_OnEveryPicture(t *testing.T) {
	pictures := append(append(podSetPicturesOfOnePod(), podSetPicturesOfAGang()...), podSetPicturesOfAFailedRow()...)
	for _, pic := range pictures {
		t.Run(pic.name, func(t *testing.T) {
			missing, podSet := runPodSetPicture(t, pic)
			if !reflect.DeepEqual(missing, pic.missing) {
				t.Errorf("short of %v, want %v", missing, pic.missing)
			}
			if !reflect.DeepEqual(podSet, pic.podSet) {
				t.Errorf("a Failed row read on %v, want %v", podSet, pic.podSet)
			}
		})
	}
}

// runPodSetPicture reads the picture's Instance as the pass reads it: the
// pod set first and, for a Failed row, the pods the row is read on.
func runPodSetPicture(t *testing.T, pic podSetPicture) (missing, podSet []string) {
	t.Helper()
	ctx := context.Background()
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, Replicas: 1, InstanceReadyTimeout: 30 * time.Minute,
		Instances: []workload.InstancePlan{{Index: pic.index, Incarnation: 1, Runners: pic.runners}}, WorkerSizeByRevision: pic.widths}
	var rows []workload.InstanceStatus
	if pic.row != nil {
		rows = []workload.InstanceStatus{*pic.row}
	}
	writer := &finishWriter{rows: rowsByIndex(rows)}
	input := finishPassInput(rows, writer)
	deps := workload.Deps{Recorder: record.NewFakeRecorder(16), Expectations: workload.NewExpectations()}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: createFinishRevision, Namespace: createNamesNamespace}}
	byInstance := map[int32][]*corev1.Pod{pic.index: pic.pods}
	pass, err := newCreatePass(ctx, deps, input, plan, target, keepEveryIndex, byInstance)
	if err != nil {
		t.Fatalf("newCreatePass: %v", err)
	}
	c := createCandidate{pass: pass, planned: plan.Instances[0], index: pic.index, observed: input.ObservedState.Instance(pic.index), existing: pic.pods}
	if next, err := c.readPodSet(ctx); !next || err != nil {
		t.Fatalf("readPodSet: next=%t err=%v", next, err)
	}
	missing = targetNames(c.missing)
	if workload.StateOf(c.observed) != workload.StateFailed {
		return missing, nil
	}
	c.adoptFailedRow(ctx)
	podSet = make([]string, 0, len(c.podSet))
	for _, pod := range c.podSet {
		podSet = append(podSet, pod.Name)
	}
	return missing, podSet
}
