package ops

import (
	"context"
	"errors"
	"flag"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	fixture "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateCreateFinishGoldens = flag.Bool("update-create-finishes", false, "re-record testdata/create_finish_allocations.json from this run")

// createFinishRevision is the one revision every Instance of the fixture
// runs; its pods carry the hash the fixture's serving pod carries.
const createFinishRevision = "llama-70b-engine-" + fixture.RevisionHashA

// createFinishNow is the pass's clock: an hour after every pod became Ready.
var createFinishNow = fixture.Epoch

// createFinishShapes are the budget's shapes: 1,000 and 4,000 single-pod
// Instances, 160 gangs of a leader and seven workers, and 1,725 gangs of a
// leader and one worker.
func createFinishShapes() []createNamesShape {
	single := []workload.RunnerPlan{{Name: workload.RunnerDefault, Size: 1}}
	return append([]createNamesShape{{name: "1000x1", instances: 1000, runners: single}}, createNamesShapes()...)
}

// keepEveryIndex is the admission of the whole Create pass: every planned
// Instance is the pass's to build or finish.
func keepEveryIndex(int32, []*corev1.Pod) bool { return true }

// createFinishFixture is one Component at rest as the Create pass meets
// it: every Instance Ready on the spec revision, every pod of every
// Instance serving on that revision and past its availability window, the
// pods bucketed by Instance as the pass buckets them, and a status writer
// that answers a batch whose every finish the pass found unchanged on its
// copy as the controller's writer does, with no read and no write.
type createFinishFixture struct {
	deps       workload.Deps
	input      workload.ReconcileInput
	plan       workload.ComponentPlan
	target     *appsv1.ControllerRevision
	byInstance map[int32][]*corev1.Pod
	writer     *finishWriter
	recorder   *record.FakeRecorder
}

func newCreateFinishFixture(shape createNamesShape) *createFinishFixture {
	component := workload.ComponentEngine
	plan := workload.ComponentPlan{
		Component:            component,
		Replicas:             int32(shape.instances),
		Instances:            make([]workload.InstancePlan, shape.instances),
		InstanceReadyTimeout: 30 * time.Minute,
	}
	rows := make([]workload.InstanceStatus, shape.instances)
	byInstance := make(map[int32][]*corev1.Pod, shape.instances)
	for i := range plan.Instances {
		index := int32(i)
		plan.Instances[i] = workload.InstancePlan{Index: index, Incarnation: 1, Runners: shape.runners}
		rows[i] = readyRow(index, createFinishRevision)
		for _, runner := range shape.runners {
			for ordinal := int32(0); ordinal < runner.Size; ordinal++ {
				byInstance[index] = append(byInstance[index], servingPodOf(component, index, runner.Name, ordinal))
			}
		}
	}
	writer := &finishWriter{rows: rowsByIndex(rows)}
	recorder := record.NewFakeRecorder(16)
	f := &createFinishFixture{
		deps:       workload.Deps{Recorder: recorder, Expectations: workload.NewExpectations()},
		input:      finishPassInput(rows, writer),
		plan:       plan,
		target:     &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: createFinishRevision, Namespace: createNamesNamespace}},
		byInstance: byInstance,
		writer:     writer,
		recorder:   recorder,
	}
	return f
}

// readyRow is the row of an Instance Ready on rev for an hour, as the
// controller leaves it after a promote.
func readyRow(index int32, rev string) workload.InstanceStatus {
	readySince := metav1.NewTime(createFinishNow.Add(-time.Hour))
	return workload.InstanceStatus{Index: index, Incarnation: 1, Phase: workload.InstancePhaseReady,
		RunningRevision: rev, Admitted: true, ReadySince: &readySince}
}

// servingPodOf is one pod of an Instance as the renderer names and labels
// it, serving on the fixture's revision since an hour before the pass.
func servingPodOf(component workload.ComponentType, index int32, runner string, ordinal int32) *corev1.Pod {
	pod := fixture.ServingPod(query.PodName(createNamesOwner, component, index, runner, ordinal))
	pod.Namespace = createNamesNamespace
	pod.UID = k8stypes.UID("uid-" + pod.Name)
	for key, value := range fixture.PodLabels(createNamesOwner, component, index, runner, 1, ordinal) {
		pod.Labels[key] = value
	}
	return pod
}

func rowsByIndex(rows []workload.InstanceStatus) map[int32]workload.InstanceStatus {
	byIndex := make(map[int32]workload.InstanceStatus, len(rows))
	for _, row := range rows {
		byIndex[row.Index] = row
	}
	return byIndex
}

// finishPassInput is the engine's input to one Create pass over rows: the
// clock, the operator's cadence and force-delete policy, the per-pass
// records the engine's entry gives a pass, and writer behind every status
// seam the pass writes through.
func finishPassInput(rows []workload.InstanceStatus, writer *finishWriter) workload.ReconcileInput {
	owner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: createNamesOwner, Namespace: createNamesNamespace, UID: "owner-uid"}}
	input := workload.ReconcileInput{
		OwnerObject:   owner,
		Key:           workload.Key{Namespace: createNamesNamespace, OwnerName: createNamesOwner, Component: workload.ComponentEngine},
		DesiredSpec:   workload.WorkloadDesiredSpec{PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "serving:v1"}}}},
		ObservedState: workload.WorkloadObservedState{InstanceStatuses: rows},
		ForceDelete:   &workload.ForceDeletePolicy{OverdueSlack: 2 * time.Minute, NodeUnreachableThreshold: 5 * time.Minute},
		Clock:         clocktesting.NewFakeClock(createFinishNow),
		Requeue:       workload.RequeueIntervals{Operation: 10 * time.Second, Gate: 10 * time.Second},
		Pacing:        &workload.APIPacing{},
		PromoteWindow: &workload.PromoteWindow{},
		PassWake:      &workload.PassWake{},
		Swept:         &workload.SweptPods{},
		Held:          &workload.HeldInstances{},
	}
	input.ApplyInstanceMutationsWithRetryBlock = writer.apply
	input.ApplyInstanceMutations = func(ctx context.Context, muts []workload.InstanceMutation) error {
		return writer.apply(ctx, muts, "", nil)
	}
	input.MutateRetryBlock = writer.pruneRetryBlock
	input.ObservedState.IndexRows()
	return input
}

// finishMutationRecord is what the writer saw of one mutation: the Instance,
// whether the pass found it unchanged on its copy, and whether applying it
// to the writer's own row changed that row.
type finishMutationRecord struct {
	index           int32
	unchangedOnCopy bool
	changedTheRow   bool
}

// finishWriter is the status writer behind the Create pass in these tests.
// Under the controller's rule a batch whose every mutation the pass found
// unchanged on its copy is answered with no read and no write; under the
// replay store's rule (appliesUnchanged) every mutation is applied to the
// writer's own rows all the same. A committed batch moves the rows and
// calls each committed mutation's callback as the controller's writer does.
// When recording, every batch is kept, mutation by mutation.
type finishWriter struct {
	rows             map[int32]workload.InstanceStatus
	appliesUnchanged bool
	recording        bool
	batches          [][]finishMutationRecord
	unchangedBatches int
	writes           int
	prunes           []string
}

func (w *finishWriter) apply(_ context.Context, muts []workload.InstanceMutation, _ string, mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
	if mutateRetryBlock != nil {
		return errors.New("the finish writer takes no retry record with a batch")
	}
	var records []finishMutationRecord
	if w.recording {
		records = make([]finishMutationRecord, 0, len(muts))
	}
	if everyFinishUnchanged(muts) && !w.appliesUnchanged {
		w.unchangedBatches++
		for i := range muts {
			records = w.record(records, muts[i], false)
		}
		w.batches = appendRecorded(w.batches, records, w.recording)
		return nil
	}
	type commit struct {
		callback          func(previous, current *workload.InstanceStatus)
		previous, current *workload.InstanceStatus
	}
	next := make(map[int32]workload.InstanceStatus, len(w.rows))
	for index, row := range w.rows {
		next[index] = row
	}
	var commits []commit
	for i := range muts {
		if muts[i].Remove || muts[i].Mutate == nil {
			return errors.New("the Create pass commits no removal and no empty mutation")
		}
		row, found := next[muts[i].Index]
		if !found {
			row = workload.InstanceStatus{Index: muts[i].Index}
		}
		if muts[i].Precondition != nil && !muts[i].Precondition(&row) {
			records = w.record(records, muts[i], false)
			continue
		}
		var previous *workload.InstanceStatus
		if found {
			before := row
			previous = &before
		}
		if !muts[i].Mutate(&row) {
			records = w.record(records, muts[i], false)
			continue
		}
		next[muts[i].Index] = row
		current := row
		commits = append(commits, commit{callback: muts[i].OnCommit, previous: previous, current: &current})
		records = w.record(records, muts[i], true)
	}
	w.batches = appendRecorded(w.batches, records, w.recording)
	if len(commits) == 0 {
		return nil
	}
	w.rows = next
	w.writes++
	for _, c := range commits {
		if c.callback != nil {
			c.callback(c.previous, c.current)
		}
	}
	return nil
}

func (w *finishWriter) record(records []finishMutationRecord, m workload.InstanceMutation, changed bool) []finishMutationRecord {
	if !w.recording {
		return records
	}
	return append(records, finishMutationRecord{index: m.Index, unchangedOnCopy: m.UnchangedOnCopy, changedTheRow: changed})
}

func appendRecorded(batches [][]finishMutationRecord, records []finishMutationRecord, recording bool) [][]finishMutationRecord {
	if !recording {
		return batches
	}
	return append(batches, records)
}

// pruneRetryBlock is the retry record seam: the pass asks it to drop the
// record of a revision an Instance converged on, and the writer keeps the
// revisions asked for.
func (w *finishWriter) pruneRetryBlock(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
	block := workload.RetryBlock{TargetRevision: rev}
	mutate(&block)
	w.prunes = append(w.prunes, rev)
	return nil
}

func everyFinishUnchanged(muts []workload.InstanceMutation) bool {
	for i := range muts {
		if !muts[i].UnchangedOnCopy {
			return false
		}
	}
	return true
}

// decide is the pass's decision over every planned Instance, as
// createFilteredBatched makes it before committing anything.
func (f *createFinishFixture) decide(tb testing.TB) *createPass {
	tb.Helper()
	ctx := context.Background()
	pass, err := newCreatePass(ctx, f.deps, f.input, f.plan, f.target, keepEveryIndex, f.byInstance)
	if err != nil {
		tb.Fatalf("newCreatePass: %v", err)
	}
	for _, planned := range f.plan.Instances {
		if err := pass.decide(ctx, planned); err != nil {
			tb.Fatalf("decide instance %d: %v", planned.Index, err)
		}
	}
	return pass
}

// commit commits what the pass decided, group by group, and returns the
// pass's one requeue.
func (f *createFinishFixture) commit(tb testing.TB, pass *createPass) ctrl.Result {
	tb.Helper()
	ownerPresent, err := pass.commitActions(context.Background())
	if err != nil || !ownerPresent {
		tb.Fatalf("commitActions: ownerPresent=%t err=%v", ownerPresent, err)
	}
	return pass.requeue()
}

// BenchmarkCreateFinishesAtRest is the cost of deciding and committing the
// finish of every Instance of a Component at rest, as every Create pass
// does, with the pods already listed and bucketed.
func BenchmarkCreateFinishesAtRest(b *testing.B) {
	for _, shape := range createFinishShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newCreateFinishFixture(shape)
			b.ReportAllocs()
			for b.Loop() {
				f.commit(b, f.decide(b))
			}
		})
	}
}

// TestCreateFinishesAtRestStayWithinBudget holds, per shape, the
// allocations and the bytes of deciding the finish of every Instance of a
// Component at rest and of committing those finishes, to the recorded
// golden. Beside the golden: every Instance's finish is a promote found
// unchanged on the pass's copy, the one batch handed to the writer names
// every Instance once in plan order with every mutation marked unchanged,
// the writer reads and writes nothing, no event is recorded, and the pass
// asks for no requeue.
func TestCreateFinishesAtRestStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "create_finish_allocations.json"), *updateCreateFinishGoldens)
	defer golden.Close()
	for _, shape := range createFinishShapes() {
		f := newCreateFinishFixture(shape)
		f.holdFinishesAtRest(t, shape)
		golden.Check(shape.name+"/decide/allocations", func() { f.decide(t) })
		golden.CheckBytes(shape.name+"/decide/bytes", func() { f.decide(t) })
		golden.Check(shape.name+"/pass/allocations", func() { f.commit(t, f.decide(t)) })
		golden.CheckBytes(shape.name+"/pass/bytes", func() { f.commit(t, f.decide(t)) })
	}
}

// holdFinishesAtRest holds one pass over the fixture to what a pass at rest
// does: a finish per Instance, unchanged on the copy, one unchanged batch
// in plan order, nothing written, no event, no requeue.
func (f *createFinishFixture) holdFinishesAtRest(t *testing.T, shape createNamesShape) {
	t.Helper()
	f.writer.recording = true
	defer func() { f.writer.recording = false; f.writer.batches = nil }()
	pass := f.decide(t)
	if len(pass.actions) != shape.instances || !pass.allReady {
		t.Fatalf("%s: %d actions decided for %d Instances, allReady=%t", shape.name, len(pass.actions), shape.instances, pass.allReady)
	}
	podsPerInstance := int(f.plan.Instances[0].TotalPods())
	for i, action := range pass.actions {
		planned := f.plan.Instances[i]
		ready := action.ready
		if action.start != nil || ready == nil || ready.instance.Index != planned.Index {
			t.Fatalf("%s: action %d is %+v, want the finish of Instance %d", shape.name, i, action, planned.Index)
		}
		if !ready.promote || !ready.wasReady || ready.statusChanges || !ready.statusMutation.UnchangedOnCopy ||
			ready.statusMutation.Index != planned.Index || ready.pruneRevision != "" || len(ready.existing) != podsPerInstance {
			t.Errorf("%s: finish of Instance %d: promote=%t wasReady=%t statusChanges=%t unchangedOnCopy=%t mutation.index=%d prune=%q pods=%d; want a promote found unchanged on the copy over %d pods",
				shape.name, planned.Index, ready.promote, ready.wasReady, ready.statusChanges, ready.statusMutation.UnchangedOnCopy, ready.statusMutation.Index, ready.pruneRevision, len(ready.existing), podsPerInstance)
		}
	}
	if res := f.commit(t, pass); res != (ctrl.Result{}) {
		t.Errorf("%s: a pass at rest asked for %+v, want no requeue", shape.name, res)
	}
	if f.writer.writes != 0 || f.writer.unchangedBatches != 1 || len(f.writer.batches) != 1 || len(f.writer.prunes) != 0 {
		t.Fatalf("%s: writes=%d unchangedBatches=%d batches=%d prunes=%v; want one unchanged batch and nothing written", shape.name, f.writer.writes, f.writer.unchangedBatches, len(f.writer.batches), f.writer.prunes)
	}
	for i, record := range f.writer.batches[0] {
		if want := f.plan.Instances[i].Index; record.index != want || !record.unchangedOnCopy || record.changedTheRow {
			t.Errorf("%s: batch entry %d is %+v, want Instance %d unchanged", shape.name, i, record, want)
		}
	}
	select {
	case event := <-f.recorder.Events:
		t.Errorf("%s: a pass at rest recorded %q, want no event", shape.name, event)
	default:
	}
}
