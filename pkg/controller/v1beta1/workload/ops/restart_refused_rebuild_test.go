package ops_test

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// refusedRebuild is a gang Instance (a leader and a worker) that served
// the revision it records for three hours and then lost its worker. The
// repair opens, drains and deletes the leader, and the next pass issues
// the rebuild creates. With rolled, a roll to a second revision is open,
// so the rebuild follows that target instead of the revision the Instance
// ran.
type refusedRebuild struct {
	t      *testing.T
	isvc   *v1beta1.InferenceService
	c      client.Client
	plan   workload.ComponentPlan
	served *appsv1.ControllerRevision
	target *appsv1.ControllerRevision
	clock  *clocktesting.FakeClock
	spec   *corev1.PodSpec
	worker *corev1.PodSpec
	// blocks are the retry records persisted across passes.
	blocks []workload.RetryBlock
	reason string
}

func newRefusedRebuild(t *testing.T, rolled bool) *refusedRebuild {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	c := newFakeClient(t, isvc, instanceIR(isvc, workload.ComponentEngine))
	f := &refusedRebuild{
		t:      t,
		isvc:   isvc,
		c:      c,
		plan:   buildPlanGangEngine(workload.RestartPolicyRecreateInstance),
		clock:  clocktesting.NewFakeClock(time.Now().Truncate(time.Second)),
		spec:   &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime:v1"}}},
		worker: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime-worker:v1"}}},
	}
	f.served = mintEngineRevision(t, c, isvc, f.spec, f.worker)
	f.target = f.served
	if rolled {
		f.spec = &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime:v2"}}}
		f.worker = &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/runtime-worker:v2"}}}
		f.target = mintEngineRevision(t, c, isvc, f.spec, f.worker)
	}

	// The gang promoted on the served revision three hours ago.
	readySince := metav1.NewTime(f.clock.Now().Add(-3 * time.Hour))
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: irName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
		RunningRevision: f.served.Name, ReadySince: &readySince, PodCount: 2,
	}}
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("write the promoted row: %v", err)
	}
	// The worker is lost; the leader still serves.
	leader := gangPod(isvc, 0, "leader", 0, 1, true, true)
	leader.UID = k8stypes.UID("leader-served")
	leader.Labels[query.LabelRevisionHash] = query.RevisionOf(f.served).Hash()
	if err := c.Create(context.Background(), leader); err != nil {
		t.Fatalf("seed the surviving leader: %v", err)
	}
	return f
}

// input is a fresh pass input over the rows and the retry records as
// persisted, the fake clock, and a three-attempt retry schedule starting
// at one minute.
func (f *refusedRebuild) input() workload.ReconcileInput {
	input := buildTestInput(f.isvc, f.c, workload.ComponentEngine)
	input.DesiredSpec.PodSpec, input.DesiredSpec.WorkerPodSpec = f.spec, f.worker
	input.ObservedState.UpdateRevision = f.target.Name
	input.Clock = f.clock
	input.UpdateRetryPolicy = &workload.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 10 * time.Minute, Multiplier: 2}
	input.PassWake = &workload.PassWake{}
	input.ObservedState.RetryBlocks = append([]workload.RetryBlock(nil), f.blocks...)
	retryBlockStore(&input, &f.blocks)
	if row := input.ObservedState.Instance(0); row != nil {
		f.plan.Instances[0].Incarnation = row.Incarnation
	}
	return input
}

// charged names the revisions that hold a retry record.
func (f *refusedRebuild) charged() []string {
	names := make([]string, 0, len(f.blocks))
	for _, b := range f.blocks {
		names = append(names, b.TargetRevision)
	}
	return names
}

func (f *refusedRebuild) row() *v1beta1.OMENativeInstanceStatus {
	return findInstanceStatusOnIR(f.c, f.isvc, workload.ComponentEngine, 0)
}

func (f *refusedRebuild) pods() []corev1.Pod {
	f.t.Helper()
	pods := &corev1.PodList{}
	if err := f.c.List(context.Background(), pods, client.InNamespace(f.isvc.Namespace)); err != nil {
		f.t.Fatalf("list pods: %v", err)
	}
	return pods.Items
}

// restart runs one restart pass with the client given.
func (f *refusedRebuild) restart(c client.Client, recorder record.EventRecorder) (workload.ReconcileInput, error) {
	f.t.Helper()
	input := f.input()
	workload.DefaultExpectations.Forget(f.isvc.Namespace, f.isvc.Name, workload.ComponentEngine, 0)
	_, err := ops.Restart(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, f.plan, f.plan.Instances[0], f.target, f.reason)
	return input, err
}

// driveToRebuild opens the repair on the lost worker and runs it until the
// old leader is gone, which is the pass before the rebuild creates.
func (f *refusedRebuild) driveToRebuild() {
	f.t.Helper()
	input := f.input()
	needs, reason, err := ops.DetectRestartTrigger(context.Background(), workload.Deps{Client: f.c}, input, f.plan, f.plan.Instances[0])
	if err != nil {
		f.t.Fatalf("DetectRestartTrigger: %v", err)
	}
	if !needs || !strings.Contains(reason, "below desired") {
		f.t.Fatalf("trigger = (%t, %q), want the repair of the gang short of its worker", needs, reason)
	}
	f.reason = reason
	for pass := 0; pass < 4 && len(f.pods()) > 0; pass++ {
		if _, err := f.restart(f.c, nil); err != nil {
			f.t.Fatalf("pass %d: Restart: %v", pass, err)
		}
	}
	row := f.row()
	if len(f.pods()) != 0 || row == nil || row.Phase != v1beta1.OMENativeInstanceRestarting || row.Incarnation != 2 {
		f.t.Fatalf("row = %+v with %d pod(s), want the repair open at incarnation=2 with the old gang gone", row, len(f.pods()))
	}
}

// refusePodCreates wraps c so every pod create is refused with err.
func refusePodCreates(t *testing.T, c client.Client, err error) client.Client {
	t.Helper()
	base, ok := c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", c)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return err
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
}

// invalidPodSpec is a validating admission answer: the apiserver refuses
// the pod object itself.
func invalidPodSpec() error {
	return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "engine-pod", nil)
}

// recordedEvents drains the fake recorder.
func recordedEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// TestRestart_RefusedRebuildOfTheRevisionTheInstanceRan_RetriesOnTheRepairsSchedule:
// a gang (a leader and a worker) served version 1 for hours and lost its
// worker; the repair drained the leader, and a validating webhook refuses
// the first rebuild create as an invalid pod spec. Version 1 served, so
// the refusal is the cluster's answer and not the revision's: no retry
// block is written for version 1, the repair keeps its attempt at Failed
// with the refusal as its failure, the Create pass leaves the row alone,
// and the repair issues the create again when its own schedule comes due
// (one minute after the refusal under the test's schedule), naming the
// refusal in the reason the new attempt opens with.
func TestRestart_RefusedRebuildOfTheRevisionTheInstanceRan_RetriesOnTheRepairsSchedule(t *testing.T) {
	f := newRefusedRebuild(t, false)
	f.driveToRebuild()

	rec := record.NewFakeRecorder(32)
	if _, err := f.restart(refusePodCreates(t, f.c, invalidPodSpec()), rec); err != nil {
		t.Fatalf("Restart: %v (a classified refusal is read, not returned)", err)
	}
	refusedAt := f.clock.Now()
	if len(f.blocks) != 0 {
		t.Fatalf("retry blocks charged = %v, want none: the revision served, so the refusal is not its failure", f.charged())
	}
	row := f.row()
	if row == nil || row.Phase != v1beta1.OMENativeInstanceFailed ||
		row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationRestart || row.Operation.RetryCount != 0 {
		t.Fatalf("row = %+v, want Failed with the repair's attempt kept", row)
	}
	if !row.Operation.Deadline.IsZero() {
		t.Fatalf("deadline = %v on the Failed row, want none: the repair's next attempt writes its own", row.Operation.Deadline)
	}
	if row.LastFailure == nil || row.LastFailure.Reason != workload.RejectionReasonInvalidPodSpec || row.LastFailure.PodName != "" ||
		!strings.Contains(row.LastFailure.Message, "is invalid") {
		t.Fatalf("lastFailure = %+v, want the apiserver's refusal naming no pod", row.LastFailure)
	}
	events := recordedEvents(rec)
	if len(events) != 1 || !strings.Contains(events[0], "InstanceRejected") || !strings.Contains(events[0], "the repair retries on its own schedule") {
		t.Fatalf("events = %q, want one InstanceRejected saying the repair retries on its own schedule", events)
	}

	// Before the schedule comes due, no pass rebuilds the row: the Create
	// pass leaves it, and the restart trigger deposits the remaining wait.
	input := f.input()
	if _, err := ops.Create(context.Background(), workload.Deps{Client: f.c}, input, f.plan, f.target); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if pods := f.pods(); len(pods) != 0 {
		t.Fatalf("the Create pass built %d pod(s) under a repair that waits for its own retry", len(pods))
	}
	if again := f.row(); again.Operation == nil || again.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("row = %+v, want the repair's attempt still kept after the Create pass", again)
	}
	input = f.input()
	waiting := input.ObservedState.Instance(0)
	if workload.RepairWaitsOnWorkload(waiting) {
		t.Fatalf("the refusal must not read as a cause the kubelet retries in place: no pod exists")
	}
	if at, owed := ops.RepairRetryAt(input, waiting); !owed || !at.Equal(refusedAt.Add(time.Minute)) {
		t.Fatalf("RepairRetryAt = (%v, %t), want one minute after the refusal at %v", at, owed, refusedAt)
	}
	if needs, reason := ops.DetectRestartTriggerWithPods(input, f.plan, f.plan.Instances[0], nil); needs {
		t.Fatalf("restart trigger = (%t, %q) right after the refusal, want the repair to wait for its schedule", needs, reason)
	}
	if got := input.PassWake.Pending(); got != time.Minute {
		t.Fatalf("pass wait = %v, want the minute left on the repair's schedule", got)
	}

	// The schedule comes due: the repair starts again, says what refused
	// the last attempt, and the create goes through this time.
	f.clock.Step(time.Minute)
	input = f.input()
	needs, reason := ops.DetectRestartTriggerWithPods(input, f.plan, f.plan.Instances[0], nil)
	if !needs || !strings.Contains(reason, "(attempt 1) after") || !strings.Contains(reason, "is invalid") {
		t.Fatalf("restart trigger = (%t, %q), want the second attempt naming the refusal", needs, reason)
	}
	f.reason = reason
	if _, err := f.restart(f.c, nil); err != nil {
		t.Fatalf("Restart (second attempt): %v", err)
	}
	row = f.row()
	if row == nil || row.Phase != v1beta1.OMENativeInstanceRestarting || row.Incarnation != 3 ||
		row.Operation == nil || row.Operation.RetryCount != 1 || row.RunningRevision != f.served.Name {
		t.Fatalf("row = %+v, want the repair's second attempt open at incarnation=3 on the served revision", row)
	}
	pods := f.pods()
	if len(pods) != 2 {
		t.Fatalf("pods = %d, want the rebuilt leader and worker", len(pods))
	}
	for _, pod := range pods {
		if pod.Labels["ome.io/instance-incarnation"] != "3" || pod.Labels[query.LabelRevisionHash] != query.RevisionOf(f.served).Hash() {
			t.Errorf("pod %s labels = %v, want incarnation=3 on the served revision", pod.Name, pod.Labels)
		}
	}
	if len(f.blocks) != 0 {
		t.Fatalf("retry blocks charged = %v, want none through the second attempt", f.charged())
	}
}

// TestRestart_RefusedRebuildFollowingTheRollTarget_ChargesTheTarget: the
// same gang loses its worker while a roll to version 2 is open, so the
// repair's rebuild follows the roll target. That create is the push's
// first at version 2, and a refusal of it charges version 2's retry block
// and ends the attempt as any push's refused create does; the block then
// paces the next try.
func TestRestart_RefusedRebuildFollowingTheRollTarget_ChargesTheTarget(t *testing.T) {
	f := newRefusedRebuild(t, true)
	f.driveToRebuild()

	rec := record.NewFakeRecorder(32)
	if _, err := f.restart(refusePodCreates(t, f.c, invalidPodSpec()), rec); err != nil {
		t.Fatalf("Restart: %v (a classified refusal is read, not returned)", err)
	}
	if len(f.blocks) != 1 || f.blocks[0].TargetRevision != f.target.Name || f.blocks[0].AttemptsStarted != 1 {
		t.Fatalf("retry blocks charged = %+v, want the roll target %s once", f.blocks, f.target.Name)
	}
	row := f.row()
	if row == nil || row.Phase != v1beta1.OMENativeInstanceFailed || row.Operation != nil ||
		row.LastFailure == nil || row.LastFailure.Reason != workload.RejectionReasonInvalidPodSpec {
		t.Fatalf("row = %+v, want Failed with the attempt ended on the refusal", row)
	}
	events := recordedEvents(rec)
	if len(events) != 1 || !strings.Contains(events[0], "held for retry") {
		t.Fatalf("events = %q, want one InstanceRejected holding the roll target", events)
	}
}
