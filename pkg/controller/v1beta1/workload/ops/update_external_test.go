package ops_test

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	alienRunningRevision = "llama-70b-engine-aaaaaaaa"
	alienPodRevision     = "llama-70b-engine-bbbbbbbb"
)

// alienFixture is a Ready Instance whose pod carries a third revision —
// neither the Instance's running one nor the current roll target.
func alienFixture(t *testing.T, serving bool) (*v1beta1.InferenceService, client.Client, *corev1.Pod, *appsv1.ControllerRevision, *record.FakeRecorder) {
	t.Helper()
	return alienRowFixture(t, serving, v1beta1.OMENativeInstanceReady, nil)
}

// alienRowFixture is alienFixture with the row's phase and operation
// chosen: a Ready row, a Failed row, or a parked attempt whose set sits on
// a withdrawn revision.
func alienRowFixture(t *testing.T, serving bool, phase v1beta1.OMENativeInstancePhase, op *v1beta1.InstanceOperation) (*v1beta1.InferenceService, client.Client, *corev1.Pod, *appsv1.ControllerRevision, *record.FakeRecorder) {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           phase,
		RunningRevision: alienRunningRevision,
		Operation:       op,
	})
	pod := podForInstance(isvc, 0, true /* ready */, serving)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(alienPodRevision)
	c := newFakeClient(t, isvc, ir, pod)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-cccccccc", Namespace: "prod"},
	}
	return isvc, c, pod, target, record.NewFakeRecorder(8)
}

func TestCleanupWreckage_ServingAlienLeavesRotationBeforeItIsDeleted(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, true /* serving */)
	plan := buildPlanSinglePodEngine(1)
	sweep := func() {
		t.Helper()
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		live := &corev1.Pod{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
			t.Fatalf("re-read alien: %v", err)
		}
		if _, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
			input, plan, plan.Instances[0], target, []*corev1.Pod{live}); err != nil {
			t.Fatalf("CleanupWreckage: %v", err)
		}
	}

	sweep()
	afterFirst := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), afterFirst); err != nil {
		t.Fatalf("the alien was deleted in the pass that unrouted it: %v", err)
	}
	if podreadiness.IsServing(afterFirst) {
		t.Fatalf("the alien is still in rotation after the first pass")
	}

	sweep()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err == nil {
		t.Errorf("the alien survived the pass after it left rotation")
	}
}

// A pod the pass's list shows on a revision the Instance neither runs nor
// rolls to, and that the apiserver holds on the running revision, is no
// leftover: an
// in-place roll moved it there under the same name after the list was
// taken. The cleanup reads the pod live, leaves it in rotation and deletes
// nothing, whether the list shows the pod serving or out of rotation.
func TestCleanupOfLeftoverPods_APodListedBehindItsInPlacePatchIsLeftAlone(t *testing.T) {
	for _, listedServing := range []bool{true, false} {
		isvc, c, pod, target, events := alienFixture(t, true /* serving */)
		listed := pod.DeepCopy()
		if !listedServing {
			listed.Status.Conditions = podForInstance(isvc, 0, true /* ready */, false /* serving */).Status.Conditions
		}
		live := &corev1.Pod{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
			t.Fatalf("read the pod: %v", err)
		}
		live.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(alienRunningRevision)
		if err := c.Update(context.Background(), live); err != nil {
			t.Fatalf("move the pod onto the running revision: %v", err)
		}
		plan := buildPlanSinglePodEngine(1)
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		done, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
			input, plan, plan.Instances[0], target, []*corev1.Pod{listed})
		if err != nil || !done {
			t.Fatalf("listed serving=%v: done=%v err=%v, want nothing left to clean", listedServing, done, err)
		}
		after := &corev1.Pod{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), after); err != nil {
			t.Fatalf("listed serving=%v: the pod was deleted: %v", listedServing, err)
		}
		if !podreadiness.IsServing(after) {
			t.Fatalf("listed serving=%v: the pod was taken out of rotation", listedServing)
		}
		if len(events.Events) != 0 {
			t.Fatalf("listed serving=%v: the cleanup recorded %q", listedServing, <-events.Events)
		}
	}
}

// A pod deleted and made again under its own name since the pass listed it
// is another pod, one the pass has not read. The cleanup leaves it in
// rotation and deletes nothing, even while the new pod carries a revision
// the Instance neither runs nor rolls to: the next pass decides on the pod
// it lists.
func TestCleanupOfLeftoverPods_APodMadeAgainUnderItsNameIsLeftToTheNextPass(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, true /* serving */)
	listed := pod.DeepCopy()
	listed.UID = "the-pod-the-pass-listed"
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	done, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{listed})
	if err != nil || !done {
		t.Fatalf("done=%v err=%v, want nothing left to clean in this pass", done, err)
	}
	after := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), after); err != nil {
		t.Fatalf("the pod made again under the name was deleted: %v", err)
	}
	if !podreadiness.IsServing(after) {
		t.Fatalf("the pod made again under the name was taken out of rotation")
	}
	if len(events.Events) != 0 {
		t.Fatalf("the cleanup recorded %q", <-events.Events)
	}
}

// A leftover pod the pass's list shows terminating is on its way out, and
// the cleanup only waits for it. It reads no pod from the apiserver for it,
// writes nothing, and reports the Instance not clean yet.
func TestCleanupOfLeftoverPods_APodListedTerminatingIsNotReadAgain(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, true /* serving */)
	listed := pod.DeepCopy()
	deleted := metav1.Now()
	listed.DeletionTimestamp = &deleted
	reads := &podReadCounter{Reader: c}
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	done, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, APIReader: reads, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{listed})
	if err != nil || done {
		t.Fatalf("done=%v err=%v, want the cleanup to wait for the terminating pod", done, err)
	}
	if reads.pods != 0 {
		t.Fatalf("the cleanup read %d pod(s) from the apiserver for a pod its list shows terminating; want none", reads.pods)
	}
	after := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), after); err != nil || !podreadiness.IsServing(after) {
		t.Fatalf("the cleanup wrote to the pod it only waits for: err=%v", err)
	}
	if len(events.Events) != 0 {
		t.Fatalf("the cleanup recorded %q", <-events.Events)
	}
}

// podReadCounter counts the pods read through it.
type podReadCounter struct {
	client.Reader
	pods int
}

func (r *podReadCounter) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		r.pods++
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestCleanupWreckage_VanishedAlienIsNotAnError(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, true /* serving */)
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatalf("delete the alien out from under the sweep: %v", err)
	}
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	if _, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{pod}); err != nil {
		t.Fatalf("a pod that vanished mid-sweep must not fail the pass: %v", err)
	}
}

func TestCleanupWreckage_UnroutedAlienIsDeletedImmediately(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, false /* serving */)
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	if _, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{pod}); err != nil {
		t.Fatalf("CleanupWreckage: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err == nil {
		t.Errorf("an alien that was never in rotation has nothing to wait for; it must go in this pass")
	}
}

// A superseded-revision pod the cleanup has already deleted is still its
// wreckage while it terminates: the row stays the cleanup's, which
// neither deletes nor expects the pod a second time and reports the work
// unfinished until the object is gone.
func TestCleanupWreckage_TerminatingAlienKeepsTheRowUntilItIsGone(t *testing.T) {
	isvc, c, pod, target, events := alienFixture(t, false /* serving */)
	plan := buildPlanSinglePodEngine(1)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	row := input.ObservedState.Instance(0)
	if !ops.EvaluateWreckage(row, target, []*corev1.Pod{pod}) {
		t.Fatalf("an alien pod is wreckage before the cleanup deletes it")
	}
	// Deleted behind a finalizer, so the fake client keeps the object
	// with a deletionTimestamp, as a live List does before the apiserver
	// collects it.
	pod.Finalizers = append(pod.Finalizers, "ome.io/test-hold")
	if err := c.Update(context.Background(), pod); err != nil {
		t.Fatalf("pin the alien with a finalizer: %v", err)
	}
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatalf("delete the alien: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), pod); err != nil || pod.DeletionTimestamp == nil {
		t.Fatalf("the alien must be terminating; err=%v deletionTimestamp=%v", err, pod.DeletionTimestamp)
	}
	if !ops.EvaluateWreckage(row, target, []*corev1.Pod{pod}) {
		t.Fatalf("a terminating alien is still the cleanup's wreckage; the row must stay its until the pod is gone")
	}
	done, err := ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
		input, plan, plan.Instances[0], target, []*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("CleanupWreckage: %v", err)
	}
	if done {
		t.Fatalf("a terminating alien is unfinished work; got done=true")
	}
	if !workload.DefaultExpectations.Satisfied(isvc.Namespace, isvc.Name, workload.ComponentEngine, 0) {
		t.Fatalf("a pod already terminating must not be expected or deleted a second time")
	}
	select {
	case event := <-events.Events:
		t.Fatalf("no delete is issued for a pod already terminating; got event %q", event)
	default:
	}
}

// rejectPodDeletes wraps c so every pod delete is refused with err,
// counting the attempts.
func rejectPodDeletes(t *testing.T, c client.Client, err error, attempts *int) client.Client {
	t.Helper()
	base, ok := c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", c)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				*attempts++
				return err
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
}

// TestCleanupWreckage_RejectedWriteDisposesRowOnce: the wreckage sweep's
// two pod writes, the unroute patch that takes a superseded-revision pod
// out of rotation and the delete that follows, are issued for a Ready, a
// Failed and a parked row with no attempt in flight. A permanent rejection
// of either ends the row as a rejected create does: Failed, the operation
// cleared, the rejection on LastFailure, the revision in force held, one
// Warning. The next sweep issues nothing while the ladder denies.
func TestCleanupWreckage_RejectedWriteDisposesRowOnce(t *testing.T) {
	parked := &v1beta1.InstanceOperation{ID: "update-0", Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepParked, TargetRevision: alienPodRevision}
	for _, tc := range []struct {
		name  string
		phase v1beta1.OMENativeInstancePhase
		op    *v1beta1.InstanceOperation
		// A serving alien is unrouted first, so the patch is the rejected
		// write; one out of rotation goes straight to the delete.
		serving bool
	}{
		{name: "Ready row, unroute patch", phase: v1beta1.OMENativeInstanceReady, serving: true},
		{name: "Ready row, delete", phase: v1beta1.OMENativeInstanceReady},
		{name: "Failed row, delete", phase: v1beta1.OMENativeInstanceFailed},
		{name: "parked attempt, delete", phase: v1beta1.OMENativeInstanceUpdating, op: parked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc, base, pod, target, events := alienRowFixture(t, tc.serving, tc.phase, tc.op)
			attempts := 0
			var c client.Client
			if tc.serving {
				c = rejectPodStatusPatches(t, base, invalidPodError(pod.Name), &attempts)
			} else {
				c = rejectPodDeletes(t, base, invalidPodError(pod.Name), &attempts)
			}
			plan := buildPlanSinglePodEngine(1)
			blocks := map[string]workload.RetryBlock{}
			sweep := func() (bool, error) {
				t.Helper()
				input := buildTestInput(isvc, c, workload.ComponentEngine)
				input.ObservedState.UpdateRevision = target.Name
				for _, b := range blocks {
					input.ObservedState.RetryBlocks = append(input.ObservedState.RetryBlocks, b)
				}
				input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
					b, found := blocks[rev]
					if !found {
						b = workload.RetryBlock{TargetRevision: rev}
					}
					switch mutate(&b) {
					case workload.RetryBlockPersist:
						blocks[rev] = b
					case workload.RetryBlockRemove:
						delete(blocks, rev)
					}
					return nil
				}
				live := &corev1.Pod{}
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
					t.Fatalf("re-read alien: %v", err)
				}
				return ops.CleanupWreckage(context.Background(), workload.Deps{Client: c, Recorder: events},
					input, plan, plan.Instances[0], target, []*corev1.Pod{live})
			}

			if _, err := sweep(); err != nil {
				t.Fatalf("CleanupWreckage: %v (a permanent rejection is disposed, not returned)", err)
			}
			if attempts != 1 {
				t.Fatalf("rejected writes: got %d want 1", attempts)
			}
			rows := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)
			if len(rows) != 1 || rows[0].Phase != v1beta1.OMENativeInstanceFailed || rows[0].Operation != nil {
				t.Fatalf("row after the rejection: got %+v want Failed with no operation", rows)
			}
			if rows[0].LastFailure == nil || rows[0].LastFailure.Reason != workload.RejectionReasonInvalidPodSpec {
				t.Fatalf("LastFailure: got %+v want reason %s", rows[0].LastFailure, workload.RejectionReasonInvalidPodSpec)
			}
			if b, held := blocks[target.Name]; !held || b.State != workload.RetryBlockHeld {
				t.Fatalf("RetryBlock for %s: got %+v want Held (the revision in force is blamed)", target.Name, blocks)
			}
			if n := countRejectionEvents(rejectionEvents(events), workload.EventReasonInstanceRejected); n != 1 {
				t.Fatalf("InstanceRejected events after the rejection: got %d want 1", n)
			}

			done, err := sweep()
			if err != nil {
				t.Fatalf("CleanupWreckage on the disposed row: %v", err)
			}
			if !done {
				t.Fatalf("a row the apiserver refused is left as it stands; got done=false")
			}
			if attempts != 1 {
				t.Fatalf("the rejected write was issued again: %d attempts", attempts)
			}
			if n := countRejectionEvents(rejectionEvents(events), workload.EventReasonInstanceRejected); n != 0 {
				t.Fatalf("InstanceRejected events on the sweep after: got %d want 0", n)
			}
		})
	}
}
