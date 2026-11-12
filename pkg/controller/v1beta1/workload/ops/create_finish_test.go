package ops

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	fixture "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The finish of a Create pass: for every Instance holding the pods its
// plan entry asks for, the pass writes the serving gate on any pod that
// lost it, and once the whole set is PodReady past its availability window
// it hands the writer one Ready mutation per Instance, batched in plan
// order with the neighbours that answer alike on whether the write changes
// the row. The pictures below say what reaches the writer, what the writer
// holds afterwards, which gates are written, which events and retry record
// prunes follow, and what the pass asks for next.

const (
	finishRevisionA = createFinishRevision
	finishRevisionB = "llama-70b-engine-" + fixture.RevisionHashB
)

var finishGang = []workload.RunnerPlan{{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: 1}}

// finishRowView is what a picture reads off a row the writer holds.
type finishRowView struct {
	phase                 workload.InstancePhase
	running, target       string
	operation, readySince bool
	readySinceAtTheClock  bool
}

func viewOf(row workload.InstanceStatus) finishRowView {
	return finishRowView{
		phase: row.Phase, running: row.RunningRevision, target: row.TargetRevision,
		operation: row.Operation != nil, readySince: row.ReadySince != nil,
		readySinceAtTheClock: row.ReadySince != nil && row.ReadySince.Time.Equal(createFinishNow),
	}
}

// finishPicture is one Component as the Create pass meets it: the rows the
// pass observed, each Instance's pods, the roll's target and partition, the
// availability window, and the rows the writer itself holds when a picture
// wants them to differ from the pass's copy.
type finishPicture struct {
	name       string
	runners    []workload.RunnerPlan
	rows       []workload.InstanceStatus
	pods       map[int32][]*corev1.Pod
	target     string
	partition  *int32
	minReady   int32
	writerRows map[int32]workload.InstanceStatus
	want       finishWant
}

// finishWant is what one pass does with the picture.
type finishWant struct {
	batches [][]finishMutationRecord
	rows    map[int32]finishRowView
	gates   []string
	events  []string
	prunes  []string
	result  ctrl.Result
	err     string
}

// finishOutcome is what one pass did.
type finishOutcome struct {
	batches [][]finishMutationRecord
	rows    map[int32]finishRowView
	gates   []string
	events  []string
	prunes  []string
	result  ctrl.Result
	err     string
}

func creatingRow(index int32, rev string) workload.InstanceStatus {
	started := metav1.NewTime(createFinishNow.Add(-5 * time.Minute))
	return workload.InstanceStatus{Index: index, Incarnation: 1, Phase: workload.InstancePhaseCreating, TargetRevision: rev, Admitted: true,
		Operation: &workload.InstanceOperation{ID: fmt.Sprintf("create-%d", index), Type: workload.InstanceOperationCreate, Step: "CreatePods",
			StartedAt: started, LastProgressAt: started, Deadline: metav1.NewTime(started.Add(30 * time.Minute)), TargetRevision: rev}}
}

func withTargetField(row workload.InstanceStatus, rev string) workload.InstanceStatus {
	row.TargetRevision = rev
	return row
}

// podOf is one pod of a single-pod Instance serving on rev's hash, with
// the given changes.
func podOf(index int32, hash string, changes ...func(*corev1.Pod)) *corev1.Pod {
	return gangPodOf(index, workload.RunnerDefault, hash, changes...)
}

func gangPodOf(index int32, runner, hash string, changes ...func(*corev1.Pod)) *corev1.Pod {
	pod := servingPodOf(workload.ComponentEngine, index, runner, 0)
	pod.Labels[query.LabelRevisionHash] = hash
	for _, change := range changes {
		change(pod)
	}
	return pod
}

// gateLost takes the serving gate away, as a drain or an operator's hold
// leaves a pod, with the kubelet's PodReady folded from it.
func gateLost(pod *corev1.Pod) {
	fixture.SetCondition(pod, podreadiness.ConditionType, corev1.ConditionFalse, createFinishNow.Add(-time.Minute))
	fixture.SetCondition(pod, corev1.PodReady, corev1.ConditionFalse, createFinishNow.Add(-time.Minute))
}

// readyFor makes the pod PodReady since ago before the pass's clock.
func readyFor(ago time.Duration) func(*corev1.Pod) {
	return func(pod *corev1.Pod) {
		fixture.SetCondition(pod, corev1.PodReady, corev1.ConditionTrue, createFinishNow.Add(-ago))
	}
}

func unchanged(index int32) finishMutationRecord {
	return finishMutationRecord{index: index, unchangedOnCopy: true}
}
func changed(index int32) finishMutationRecord {
	return finishMutationRecord{index: index, changedTheRow: true}
}

func readyView(rev string) finishRowView {
	return finishRowView{phase: workload.InstancePhaseReady, running: rev, readySince: true}
}

func promotedView(rev string) finishRowView {
	return finishRowView{phase: workload.InstancePhaseReady, running: rev, readySince: true, readySinceAtTheClock: true}
}

func cadence() ctrl.Result { return ctrl.Result{RequeueAfter: 10 * time.Second} }

func finishPicturesAtRest() []finishPicture {
	one := int32(1)
	return []finishPicture{
		{name: "a settled Instance", rows: []workload.InstanceStatus{readyRow(0, finishRevisionA)}, pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}}, target: finishRevisionA,
			want: finishWant{batches: [][]finishMutationRecord{{unchanged(0)}}, rows: map[int32]finishRowView{0: readyView(finishRevisionA)}}},
		{name: "a settled gang", runners: finishGang, rows: []workload.InstanceStatus{readyRow(0, finishRevisionA)},
			pods: map[int32][]*corev1.Pod{0: {gangPodOf(0, workload.RunnerLeader, fixture.RevisionHashA), gangPodOf(0, workload.RunnerWorker, fixture.RevisionHashA)}}, target: finishRevisionA,
			want: finishWant{batches: [][]finishMutationRecord{{unchanged(0)}}, rows: map[int32]finishRowView{0: readyView(finishRevisionA)}}},
		{name: "a settled Instance on the revision behind the roll", rows: []workload.InstanceStatus{readyRow(0, finishRevisionA)}, pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}}, target: finishRevisionB,
			want: finishWant{batches: [][]finishMutationRecord{{unchanged(0)}}, rows: map[int32]finishRowView{0: readyView(finishRevisionA)}}},
		{name: "a settled Instance the partition holds behind the roll", rows: []workload.InstanceStatus{readyRow(0, finishRevisionA)}, pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}}, target: finishRevisionB, partition: &one,
			want: finishWant{batches: [][]finishMutationRecord{{unchanged(0)}}, rows: map[int32]finishRowView{0: readyView(finishRevisionA)}}},
		{name: "two settled Instances on two revisions share a batch", rows: []workload.InstanceStatus{readyRow(0, finishRevisionA), readyRow(1, finishRevisionB)},
			pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}, 1: {podOf(1, fixture.RevisionHashB)}}, target: finishRevisionB,
			want: finishWant{batches: [][]finishMutationRecord{{unchanged(0), unchanged(1)}}, rows: map[int32]finishRowView{0: readyView(finishRevisionA), 1: readyView(finishRevisionB)}}},
		{name: "pods naming no revision leave a settled row as it is", rows: []workload.InstanceStatus{readyRow(0, finishRevisionA)}, pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA, fixture.NoRevision)}}, target: finishRevisionA,
			want: finishWant{rows: map[int32]finishRowView{0: readyView(finishRevisionA)}}},
	}
}

func finishPicturesOfAPromote() []finishPicture {
	return []finishPicture{
		{name: "an attempt whose set serves promotes", rows: []workload.InstanceStatus{creatingRow(0, finishRevisionA)}, pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}}, target: finishRevisionA,
			want: finishWant{batches: [][]finishMutationRecord{{changed(0)}}, rows: map[int32]finishRowView{0: promotedView(finishRevisionA)}, events: []string{"InstanceReady"}, prunes: []string{finishRevisionA}}},
		{name: "a settled row still naming a target is written once", rows: []workload.InstanceStatus{withTargetField(readyRow(0, finishRevisionA), finishRevisionA)}, pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}}, target: finishRevisionA,
			want: finishWant{batches: [][]finishMutationRecord{{changed(0)}}, rows: map[int32]finishRowView{0: readyView(finishRevisionA)}}},
		{name: "finishes commit in plan order, a changed one alone", rows: []workload.InstanceStatus{readyRow(0, finishRevisionA), creatingRow(1, finishRevisionA), readyRow(2, finishRevisionA)},
			pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}, 1: {podOf(1, fixture.RevisionHashA)}, 2: {podOf(2, fixture.RevisionHashA)}}, target: finishRevisionA,
			want: finishWant{batches: [][]finishMutationRecord{{unchanged(0)}, {changed(1)}, {unchanged(2)}},
				rows: map[int32]finishRowView{0: readyView(finishRevisionA), 1: promotedView(finishRevisionA), 2: readyView(finishRevisionA)}, events: []string{"InstanceReady"}, prunes: []string{finishRevisionA}}},
		{name: "adjacent unchanged finishes share a batch", rows: []workload.InstanceStatus{readyRow(0, finishRevisionA), readyRow(1, finishRevisionA), creatingRow(2, finishRevisionA)},
			pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}, 1: {podOf(1, fixture.RevisionHashA)}, 2: {podOf(2, fixture.RevisionHashA)}}, target: finishRevisionA,
			want: finishWant{batches: [][]finishMutationRecord{{unchanged(0), unchanged(1)}, {changed(2)}},
				rows: map[int32]finishRowView{0: readyView(finishRevisionA), 1: readyView(finishRevisionA), 2: promotedView(finishRevisionA)}, events: []string{"InstanceReady"}, prunes: []string{finishRevisionA}}},
		{name: "a finish unchanged on the copy still promotes the row the writer holds", rows: []workload.InstanceStatus{readyRow(0, finishRevisionA), readyRow(1, finishRevisionA)},
			pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA)}, 1: {podOf(1, fixture.RevisionHashA)}}, target: finishRevisionA,
			writerRows: map[int32]workload.InstanceStatus{0: creatingRow(0, finishRevisionA), 1: readyRow(1, finishRevisionA)},
			want: finishWant{batches: [][]finishMutationRecord{{{index: 0, unchangedOnCopy: true, changedTheRow: true}, unchanged(1)}},
				rows: map[int32]finishRowView{0: promotedView(finishRevisionA), 1: readyView(finishRevisionA)}}},
	}
}

func finishPicturesHeldBelowTheBar() []finishPicture {
	return []finishPicture{
		{name: "a serving pod that lost its gate gets it back", rows: []workload.InstanceStatus{readyRow(0, finishRevisionA)}, pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA, gateLost)}}, target: finishRevisionA,
			want: finishWant{rows: map[int32]finishRowView{0: readyView(finishRevisionA)}, gates: []string{query.PodName(createNamesOwner, workload.ComponentEngine, 0, workload.RunnerDefault, 0)}, result: cadence()}},
		{name: "a gang member that lost its gate gets it back alone", runners: finishGang, rows: []workload.InstanceStatus{readyRow(0, finishRevisionA)},
			pods: map[int32][]*corev1.Pod{0: {gangPodOf(0, workload.RunnerLeader, fixture.RevisionHashA), gangPodOf(0, workload.RunnerWorker, fixture.RevisionHashA, gateLost)}}, target: finishRevisionA,
			want: finishWant{rows: map[int32]finishRowView{0: readyView(finishRevisionA)}, gates: []string{query.PodName(createNamesOwner, workload.ComponentEngine, 0, workload.RunnerWorker, 0)}, result: cadence()}},
		{name: "a set inside its availability window waits for the window", rows: []workload.InstanceStatus{creatingRow(0, finishRevisionA)}, pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA, readyFor(10*time.Second))}}, target: finishRevisionA, minReady: 30,
			want: finishWant{rows: map[int32]finishRowView{0: viewOf(creatingRow(0, finishRevisionA))}, result: ctrl.Result{RequeueAfter: 20 * time.Second}}},
		{name: "pods naming no revision hold an open attempt and report it", rows: []workload.InstanceStatus{creatingRow(0, finishRevisionA)}, pods: map[int32][]*corev1.Pod{0: {podOf(0, fixture.RevisionHashA, fixture.NoRevision)}}, target: finishRevisionA,
			want: finishWant{rows: map[int32]finishRowView{0: viewOf(creatingRow(0, finishRevisionA))}, result: cadence(), err: "instance 0 cannot become Ready from its pod labels"}},
	}
}

// TestCreateFinishes_OnEveryPicture holds what one Create pass does with
// each picture's finishes: the batches that reach the writer in plan order
// with each mutation's answer, the rows the writer holds afterwards, the
// gates written, the events and prunes that follow, and the requeue.
func TestCreateFinishes_OnEveryPicture(t *testing.T) {
	pictures := append(append(finishPicturesAtRest(), finishPicturesOfAPromote()...), finishPicturesHeldBelowTheBar()...)
	for _, pic := range pictures {
		t.Run(pic.name, func(t *testing.T) {
			got := runFinishPicture(t, pic)
			want := finishOutcome(pic.want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("the pass did\n  %+v\nwant\n  %+v", got, want)
			}
		})
	}
}

// runFinishPicture runs one Create pass over the picture, the pods already
// listed and bucketed as the pass buckets them and held by a fake apiserver
// for the gate writes, under the replay store's rule for the writer.
func runFinishPicture(t *testing.T, pic finishPicture) finishOutcome {
	t.Helper()
	ctx := context.Background()
	runners := pic.runners
	if runners == nil {
		runners = []workload.RunnerPlan{{Name: workload.RunnerDefault, Size: 1}}
	}
	instances := len(pic.rows)
	for index := range pic.pods {
		if int(index) >= instances {
			instances = int(index) + 1
		}
	}
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, Replicas: int32(instances), InstanceReadyTimeout: 30 * time.Minute, MinReadySeconds: pic.minReady,
		UpdateStrategy: workload.UpdateStrategy{Type: workload.UpdateStrategyRecreatePod, RollingUpdate: &workload.RollingUpdate{Partition: pic.partition}}}
	for i := 0; i < instances; i++ {
		plan.Instances = append(plan.Instances, workload.InstancePlan{Index: int32(i), Incarnation: 1, Runners: runners})
	}
	writerRows := pic.writerRows
	if writerRows == nil {
		writerRows = rowsByIndex(pic.rows)
	}
	writer := &finishWriter{rows: writerRows, appliesUnchanged: true, recording: true}
	input := finishPassInput(pic.rows, writer)
	var objects []client.Object
	for _, pods := range pic.pods {
		for _, pod := range pods {
			objects = append(objects, pod.DeepCopy())
		}
	}
	apiserver := fakeclient.New(t, fakeclient.WithObjects(objects...), fakeclient.WithStatusSubresource(&corev1.Pod{}))
	recorder := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: apiserver, Recorder: recorder, Expectations: workload.NewExpectations()}
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: pic.target, Namespace: createNamesNamespace}}
	res, err := createFilteredBatched(ctx, deps, input, plan, target, keepEveryIndex, pic.pods)
	out := finishOutcome{batches: writer.batches, rows: map[int32]finishRowView{}, prunes: writer.prunes, result: res}
	if err != nil {
		out.err = err.Error()
	}
	if pic.want.err != "" && strings.Contains(out.err, pic.want.err) {
		out.err = pic.want.err
	}
	for index, row := range writer.rows {
		out.rows[index] = viewOf(row)
	}
	out.gates = gatesWritten(t, ctx, apiserver, pic.pods)
	out.events = eventReasons(recorder)
	return out
}

// gatesWritten names the pods whose serving gate the pass wrote True.
func gatesWritten(t *testing.T, ctx context.Context, c client.Client, pods map[int32][]*corev1.Pod) []string {
	t.Helper()
	var written []string
	for _, list := range pods {
		for _, pod := range list {
			live := &corev1.Pod{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(pod), live); err != nil {
				t.Fatalf("read pod %s: %v", pod.Name, err)
			}
			if podreadiness.IsServing(live) && !podreadiness.IsServing(pod) {
				written = append(written, pod.Name)
			}
		}
	}
	sort.Strings(written)
	return written
}

func eventReasons(recorder *record.FakeRecorder) []string {
	var reasons []string
	for {
		select {
		case event := <-recorder.Events:
			if fields := strings.Fields(event); len(fields) > 1 {
				reasons = append(reasons, fields[1])
			}
		default:
			return reasons
		}
	}
}
