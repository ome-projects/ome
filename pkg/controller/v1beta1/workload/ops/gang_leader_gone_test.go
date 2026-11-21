package ops

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// leaderGoneBound is the grace the tests set on the replica's lifecycle.
const leaderGoneBound = 30 * time.Second

// memberPod is one pod of Instance 0 as a pass lists it: its runner, the
// count of the set it was built with, and a termination grace of 480 s in
// its spec.
func memberPod(name, runner string, built int64) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "prod",
			UID:       k8stypes.UID(name + "-uid"),
			Labels: map[string]string{
				query.LabelRunner:              runner,
				query.LabelInstanceIdx:         "0",
				query.LabelInstanceIncarnation: strconv.FormatInt(built, 10),
			},
		},
		Spec: corev1.PodSpec{
			NodeName:                      "node-a",
			TerminationGracePeriodSeconds: ptrInt64(480),
			Containers:                    []corev1.Container{{Name: "main", Image: "test:v1"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// deletedWithGrace shows pod as the API server shows a pod deleted at fdNow
// with grace seconds to stop.
func deletedWithGrace(pod *corev1.Pod, grace int64) *corev1.Pod {
	return endingIn(pod, grace, time.Duration(grace)*time.Second)
}

// endingIn shows pod as a pod deleted with grace seconds to stop, of which
// left remain at fdNow.
func endingIn(pod *corev1.Pod, grace int64, left time.Duration) *corev1.Pod {
	at := metav1.NewTime(fdNow.Add(left))
	pod.DeletionTimestamp = &at
	pod.DeletionGracePeriodSeconds = &grace
	return pod
}

// threeWorkers are the workers of a gang built once, each deleted with its
// own 480 s.
func threeWorkers() []*corev1.Pod {
	return []*corev1.Pod{
		deletedWithGrace(memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1), 480),
		deletedWithGrace(memberPod("svc-engine-0-worker-1", workload.RunnerWorker, 1), 480),
		deletedWithGrace(memberPod("svc-engine-0-worker-2", workload.RunnerWorker, 1), 480),
	}
}

// leaderGoneInput is a pass input at fdNow whose replica sets
// lifecycle.leaderGoneGracePeriod to bound.
func leaderGoneInput(bound time.Duration) workload.ReconcileInput {
	input := fdInput(fdISVC("svc"), nil)
	input.DesiredSpec.Lifecycle.LeaderGoneGracePeriod = &metav1.Duration{Duration: bound}
	return input
}

// storedCopies are the pods as the fake client can hold them.
func storedCopies(pods ...*corev1.Pod) []client.Object {
	objs := make([]client.Object, 0, len(pods))
	for _, pod := range pods {
		objs = append(objs, fdStoredCopy(pod))
	}
	return objs
}

// TestLeaderGoneGraceApplies holds the rule one pod at a time: a worker
// that is still terminating with more than the bound left, whose leader has
// gone, takes the bound. Every other pod keeps what it has.
func TestLeaderGoneGraceApplies(t *testing.T) {
	const bound = int64(30)
	worker := func() *corev1.Pod {
		return deletedWithGrace(memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1), 480)
	}
	leader := func(built int64) *corev1.Pod {
		return memberPod("svc-engine-0-leader-0", workload.RunnerLeader, built)
	}
	stopped := func(pod *corev1.Pod, phase corev1.PodPhase) *corev1.Pod {
		pod.Status.Phase = phase
		return pod
	}
	unreadable := func(pod *corev1.Pod) *corev1.Pod {
		delete(pod.Labels, query.LabelInstanceIncarnation)
		return pod
	}
	changed := func(change func(*corev1.Pod)) *corev1.Pod {
		pod := worker()
		change(pod)
		return pod
	}
	cases := []struct {
		name   string
		pod    *corev1.Pod
		others []*corev1.Pod
		want   bool
	}{
		{name: "a worker whose leader pod is gone", pod: worker(), want: true},
		{name: "a worker whose leader still runs", pod: worker(), others: []*corev1.Pod{leader(1)}},
		{name: "a worker whose leader is terminating and still runs", pod: worker(), others: []*corev1.Pod{deletedWithGrace(leader(1), 480)}},
		{name: "a worker whose leader still runs beside a worker that has stopped", pod: worker(), others: []*corev1.Pod{deletedWithGrace(leader(1), 480), stopped(deletedWithGrace(memberPod("svc-engine-0-worker-2", workload.RunnerWorker, 1), 480), corev1.PodFailed)}},
		{name: "a worker whose leader was never deleted beside a worker that has succeeded", pod: worker(), others: []*corev1.Pod{leader(1), stopped(deletedWithGrace(memberPod("svc-engine-0-worker-2", workload.RunnerWorker, 1), 480), corev1.PodSucceeded)}},
		{name: "a worker whose leader runs with a set that cannot be read", pod: worker(), others: []*corev1.Pod{unreadable(deletedWithGrace(leader(1), 480))}},
		{name: "a worker whose leader pod remains, failed, with every container stopped", pod: worker(), others: []*corev1.Pod{stopped(deletedWithGrace(leader(1), 480), corev1.PodFailed)}, want: true},
		{name: "a worker whose leader pod remains, succeeded, with every container stopped", pod: worker(), others: []*corev1.Pod{stopped(leader(1), corev1.PodSucceeded)}, want: true},
		{name: "a worker of the old set beside the leader of the set that replaces it", pod: worker(), others: []*corev1.Pod{leader(2)}, want: true},
		{name: "a worker of the old set beside its own leader and the next one", pod: worker(), others: []*corev1.Pod{leader(2), leader(1)}},
		{name: "a worker nobody deleted", pod: memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1)},
		{name: "a worker whose grace equals the bound", pod: deletedWithGrace(memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1), 30)},
		{name: "a worker whose grace is under the bound", pod: deletedWithGrace(memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1), 10)},
		{name: "a worker with 31 s of its 480 s left", pod: endingIn(memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1), 480, 31*time.Second), want: true},
		{name: "a worker with 30 s of its 480 s left", pod: endingIn(memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1), 480, 30*time.Second)},
		{name: "a worker past the end of its 480 s", pod: endingIn(memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1), 480, -time.Minute)},
		{name: "a worker that shows no grace", pod: changed(func(p *corev1.Pod) { p.DeletionGracePeriodSeconds = nil })},
		{name: "a worker observed with no UID", pod: changed(func(p *corev1.Pod) { p.UID = "" })},
		{name: "a worker whose set cannot be read", pod: changed(func(p *corev1.Pod) { delete(p.Labels, query.LabelInstanceIncarnation) })},
		{name: "a worker that has itself stopped", pod: stopped(worker(), corev1.PodFailed)},
		{name: "the leader itself", pod: deletedWithGrace(leader(1), 480)},
		{name: "the leader itself, failed, with every container stopped", pod: stopped(deletedWithGrace(leader(1), 480), corev1.PodFailed)},
		{name: "the pod of a single-pod Instance", pod: deletedWithGrace(memberPod("svc-engine-0-default-0", workload.RunnerDefault, 1), 480)},
		{name: "a pod with no runner label", pod: changed(func(p *corev1.Pod) { delete(p.Labels, query.LabelRunner) })},
		{name: "no pod", pod: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pods := append([]*corev1.Pod{tc.pod}, tc.others...)
			if got := leaderGoneGraceApplies(tc.pod, pods, bound, fdNow); got != tc.want {
				t.Fatalf("leaderGoneGraceApplies = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestShortenGraceAfterLeaderGone_GivesEachWorkerTheBoundOnce is the
// plain case: a gang of one leader and three workers is deleted with a
// grace of 480 s each, the leader's pod is gone, and the workers are still
// terminating. Each worker gets one more delete with 30 s under its UID,
// one event names the three, and no delete is added to the expectations.
// A later pass, on pods that show the 30 s, writes nothing.
func TestShortenGraceAfterLeaderGone_GivesEachWorkerTheBoundOnce(t *testing.T) {
	workers := threeWorkers()
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, storedCopies(workers...)...)
	rec := record.NewFakeRecorder(8)
	deps := workload.Deps{Client: c, Recorder: rec, Expectations: workload.NewExpectations()}
	input := leaderGoneInput(leaderGoneBound)

	if err := ShortenGraceAfterLeaderGone(context.Background(), deps, input, map[int32][]*corev1.Pod{0: workers}); err != nil {
		t.Fatalf("ShortenGraceAfterLeaderGone: %v", err)
	}
	if len(deletes) != 3 {
		t.Fatalf("deletes = %+v, want one per worker", deletes)
	}
	for i, d := range deletes {
		if d.name != workers[i].Name {
			t.Errorf("delete %d names %s, want %s: the workers go in name order", i, d.name, workers[i].Name)
		}
		if d.grace == nil || *d.grace != 30 {
			t.Errorf("%s: grace = %v, want 30", d.name, d.grace)
		}
		if d.uid == nil || *d.uid != workers[i].UID {
			t.Errorf("%s: the delete must carry the pod's UID, got %v", d.name, d.uid)
		}
	}
	events := fdDrainEvents(rec)
	if len(events) != 1 {
		t.Fatalf("events = %v, want one for the Instance", events)
	}
	for _, want := range []string{
		"Normal " + string(workload.EventReasonLeaderGoneGraceShortened),
		"component=engine instance=0",
		"the leader pod of this gang is gone, and 3 pods are still terminating: svc-engine-0-worker-0, svc-engine-0-worker-1, svc-engine-0-worker-2.",
		"OME shortened their termination grace from 480s to 30s (lifecycle.leaderGoneGracePeriod)",
		"because a pod of a gang cannot finish its work without the leader",
		"Each kubelet counts the 30s from the moment it learns of them and then sends SIGKILL.",
	} {
		if !strings.Contains(events[0], want) {
			t.Errorf("the event must say %q, got %q", want, events[0])
		}
	}
	if !deps.ExpectationsCache().Satisfied("prod", "svc", workload.ComponentEngine, 0) {
		t.Errorf("the step must add no expected delete: the first delete of each pod already expects its removal")
	}

	deletes = nil
	shown := threeWorkers()
	for _, pod := range shown {
		endingIn(pod, 30, 25*time.Second)
	}
	if err := ShortenGraceAfterLeaderGone(context.Background(), deps, input, map[int32][]*corev1.Pod{0: shown}); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if len(deletes) != 0 || len(fdDrainEvents(rec)) != 0 {
		t.Fatalf("a pod that shows the bound gets no further delete and no event: deletes=%+v", deletes)
	}
}

// TestShortenGraceAfterLeaderGone_LeavesEveryOtherPodAlone runs the step
// over pictures in which it has nothing to do, and reads that it writes
// nothing and says nothing.
func TestShortenGraceAfterLeaderGone_LeavesEveryOtherPodAlone(t *testing.T) {
	leader := func() *corev1.Pod { return memberPod("svc-engine-0-leader-0", workload.RunnerLeader, 1) }
	each := func(pods []*corev1.Pod, change func(*corev1.Pod)) []*corev1.Pod {
		for _, pod := range pods {
			change(pod)
		}
		return pods
	}
	swept := &workload.SweptPods{}
	swept.Record(k8stypes.UID("svc-engine-0-worker-0-uid"))
	cases := []struct {
		name  string
		bound time.Duration
		pods  []*corev1.Pod
		swept *workload.SweptPods
	}{
		{name: "the field at zero reads as unset", bound: 0, pods: threeWorkers()},
		{name: "a negative field reads as unset", bound: -time.Second, pods: threeWorkers()},
		{name: "the leader still runs", bound: leaderGoneBound, pods: append(threeWorkers(), leader())},
		{name: "the leader is terminating and still runs", bound: leaderGoneBound, pods: append(threeWorkers(), deletedWithGrace(leader(), 480))},
		{name: "the workers were deleted with the bound already", bound: leaderGoneBound, pods: each(threeWorkers(), func(p *corev1.Pod) { deletedWithGrace(p, 30) })},
		{name: "the workers' own 480 s end in 20 s", bound: leaderGoneBound, pods: each(threeWorkers(), func(p *corev1.Pod) { endingIn(p, 480, 20*time.Second) })},
		{name: "a single-pod Instance", bound: leaderGoneBound, pods: []*corev1.Pod{deletedWithGrace(memberPod("svc-engine-0-default-0", workload.RunnerDefault, 1), 480)}},
		{name: "a leader deleted with no worker left", bound: leaderGoneBound, pods: []*corev1.Pod{deletedWithGrace(leader(), 480)}},
		{name: "a gang nobody deleted, short of its leader", bound: leaderGoneBound, pods: []*corev1.Pod{memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1)}},
		{name: "a worker the force delete of this pass removed", bound: leaderGoneBound, pods: threeWorkers()[:1], swept: swept},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var deletes []recordedDeleteOpts
			funcs := fdDeleteRecorder(&deletes)
			c := fdFakeClient(t, &funcs, storedCopies(tc.pods...)...)
			rec := record.NewFakeRecorder(8)
			input := leaderGoneInput(tc.bound)
			input.Swept = tc.swept
			if err := ShortenGraceAfterLeaderGone(context.Background(), workload.Deps{Client: c, Recorder: rec}, input, map[int32][]*corev1.Pod{0: tc.pods}); err != nil {
				t.Fatalf("ShortenGraceAfterLeaderGone: %v", err)
			}
			if len(deletes) != 0 {
				t.Fatalf("deletes = %+v, want none", deletes)
			}
			if events := fdDrainEvents(rec); len(events) != 0 {
				t.Fatalf("events = %v, want none", events)
			}
		})
	}
}

// TestShortenGraceAfterLeaderGone_RoundsTheBoundUp: the API takes whole
// seconds, and a bound of 1.5 s must not become 1 s.
func TestShortenGraceAfterLeaderGone_RoundsTheBoundUp(t *testing.T) {
	workers := threeWorkers()[:1]
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, storedCopies(workers...)...)
	if err := ShortenGraceAfterLeaderGone(context.Background(), workload.Deps{Client: c}, leaderGoneInput(1500*time.Millisecond), map[int32][]*corev1.Pod{0: workers}); err != nil {
		t.Fatalf("ShortenGraceAfterLeaderGone: %v", err)
	}
	if len(deletes) != 1 || deletes[0].grace == nil || *deletes[0].grace != 2 {
		t.Fatalf("deletes = %+v, want one with a grace of 2", deletes)
	}
}

// TestShortenGraceAfterLeaderGone_ReadsEachInstanceAndEachSetApart: the
// worker of Instance 0 keeps its grace while its leader runs. The two
// workers of Instance 1, deleted with 480 s and 120 s, take the bound. So
// does the old worker of Instance 2, whose name the new set waits for
// beside the new set's leader.
func TestShortenGraceAfterLeaderGone_ReadsEachInstanceAndEachSetApart(t *testing.T) {
	member := func(idx int32, runner string, ordinal int, built int64) *corev1.Pod {
		pod := memberPod("svc-engine-"+strconv.Itoa(int(idx))+"-"+runner+"-"+strconv.Itoa(ordinal), runner, built)
		pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(int(idx))
		return pod
	}
	byInstance := map[int32][]*corev1.Pod{
		0: {member(0, workload.RunnerLeader, 0, 1), deletedWithGrace(member(0, workload.RunnerWorker, 0, 1), 480)},
		1: {deletedWithGrace(member(1, workload.RunnerWorker, 1, 1), 120), deletedWithGrace(member(1, workload.RunnerWorker, 0, 1), 480)},
		2: {member(2, workload.RunnerLeader, 0, 2), deletedWithGrace(member(2, workload.RunnerWorker, 0, 1), 480)},
	}
	var all []*corev1.Pod
	for _, pods := range byInstance {
		all = append(all, pods...)
	}
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, storedCopies(all...)...)
	rec := record.NewFakeRecorder(8)
	if err := ShortenGraceAfterLeaderGone(context.Background(), workload.Deps{Client: c, Recorder: rec}, leaderGoneInput(leaderGoneBound), byInstance); err != nil {
		t.Fatalf("ShortenGraceAfterLeaderGone: %v", err)
	}
	var names []string
	for _, d := range deletes {
		names = append(names, d.name)
	}
	if got, want := strings.Join(names, " "), "svc-engine-1-worker-0 svc-engine-1-worker-1 svc-engine-2-worker-0"; got != want {
		t.Fatalf("shortened %q, want %q: by Instance index, then by name", got, want)
	}
	events := fdDrainEvents(rec)
	if len(events) != 2 {
		t.Fatalf("events = %v, want one for Instance 1 and one for Instance 2", events)
	}
	for _, want := range []string{"instance=1", "2 pods are still terminating: svc-engine-1-worker-0, svc-engine-1-worker-1.", "OME shortened their termination grace from up to 480s to 30s"} {
		if !strings.Contains(events[0], want) {
			t.Errorf("the event of Instance 1 must say %q, got %q", want, events[0])
		}
	}
	for _, want := range []string{"instance=2", "1 pod is still terminating: svc-engine-2-worker-0.", "OME shortened its termination grace from 480s to 30s"} {
		if !strings.Contains(events[1], want) {
			t.Errorf("the event of Instance 2 must say %q, got %q", want, events[1])
		}
	}
}

// TestShortenGraceAfterLeaderGone_APodGoneOrReplacedIsNotAnError: a worker
// that left between the pass's read and the delete, and one replaced at its
// name, are done already. The step neither fails on them nor names them.
func TestShortenGraceAfterLeaderGone_APodGoneOrReplacedIsNotAnError(t *testing.T) {
	workers := threeWorkers()
	pods := schema.GroupResource{Resource: "pods"}
	var deletes []recordedDeleteOpts
	recorder := fdDeleteRecorder(&deletes)
	funcs := interceptor.Funcs{Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		switch obj.GetName() {
		case workers[0].Name:
			return apierrors.NewNotFound(pods, obj.GetName())
		case workers[1].Name:
			return apierrors.NewConflict(pods, obj.GetName(), errors.New("the UID in the precondition does not match"))
		}
		return recorder.Delete(ctx, cl, obj, opts...)
	}}
	c := fdFakeClient(t, &funcs, storedCopies(workers...)...)
	rec := record.NewFakeRecorder(8)
	if err := ShortenGraceAfterLeaderGone(context.Background(), workload.Deps{Client: c, Recorder: rec}, leaderGoneInput(leaderGoneBound), map[int32][]*corev1.Pod{0: workers}); err != nil {
		t.Fatalf("a pod already gone or replaced must not fail the step: %v", err)
	}
	events := fdDrainEvents(rec)
	if len(deletes) != 1 || len(events) != 1 || !strings.Contains(events[0], "1 pod is still terminating: svc-engine-0-worker-2.") {
		t.Fatalf("deletes=%+v events=%v, want the third worker alone shortened and named", deletes, events)
	}
}

// TestShortenGraceAfterLeaderGone_OneFailedDeleteDoesNotStopTheOthers: the
// API server refuses the first worker's delete. The step reports the
// refusal, and the other two workers still take the bound.
func TestShortenGraceAfterLeaderGone_OneFailedDeleteDoesNotStopTheOthers(t *testing.T) {
	workers := threeWorkers()
	var deletes []recordedDeleteOpts
	recorder := fdDeleteRecorder(&deletes)
	funcs := interceptor.Funcs{Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if obj.GetName() == workers[0].Name {
			return apierrors.NewServiceUnavailable("the API server is busy")
		}
		return recorder.Delete(ctx, cl, obj, opts...)
	}}
	c := fdFakeClient(t, &funcs, storedCopies(workers...)...)
	rec := record.NewFakeRecorder(8)
	err := ShortenGraceAfterLeaderGone(context.Background(), workload.Deps{Client: c, Recorder: rec}, leaderGoneInput(leaderGoneBound), map[int32][]*corev1.Pod{0: workers})
	if err == nil || !strings.Contains(err.Error(), workers[0].Name) {
		t.Fatalf("err = %v, want the refused delete of %s reported", err, workers[0].Name)
	}
	events := fdDrainEvents(rec)
	if len(deletes) != 2 || len(events) != 1 || !strings.Contains(events[0], "2 pods are still terminating: svc-engine-0-worker-1, svc-engine-0-worker-2.") {
		t.Fatalf("deletes=%+v events=%v, want the other two workers shortened and named", deletes, events)
	}
}

// TestShortenGraceAfterLeaderGone_AnUnsetFieldLeavesEveryPodItsGrace: a
// replica that does not set lifecycle.leaderGoneGracePeriod gets no delete
// and no event, whatever its pods show.
func TestShortenGraceAfterLeaderGone_AnUnsetFieldLeavesEveryPodItsGrace(t *testing.T) {
	pods := threeWorkers()
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, storedCopies(pods...)...)
	rec := record.NewFakeRecorder(8)
	input := fdInput(fdISVC("svc"), nil)
	if input.DesiredSpec.Lifecycle.LeaderGoneGracePeriod != nil {
		t.Fatalf("this test needs an input whose replica leaves the field unset")
	}
	if err := ShortenGraceAfterLeaderGone(context.Background(), workload.Deps{Client: c, Recorder: rec}, input, map[int32][]*corev1.Pod{0: pods}); err != nil {
		t.Fatalf("ShortenGraceAfterLeaderGone: %v", err)
	}
	if events := fdDrainEvents(rec); len(deletes) != 0 || len(events) != 0 {
		t.Fatalf("deletes = %+v, events = %v, want none of either", deletes, events)
	}
}

// TestShortenGraceAfterLeaderGone_ABoundUnderOneSecondNamesOneSecond: a
// bound of 400 ms names a grace of 1 s. A grace of 0 would remove the pod
// before its kubelet has stopped the containers.
func TestShortenGraceAfterLeaderGone_ABoundUnderOneSecondNamesOneSecond(t *testing.T) {
	workers := threeWorkers()
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, storedCopies(workers...)...)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(8)}
	if err := ShortenGraceAfterLeaderGone(context.Background(), deps, leaderGoneInput(400*time.Millisecond), map[int32][]*corev1.Pod{0: workers}); err != nil {
		t.Fatalf("ShortenGraceAfterLeaderGone: %v", err)
	}
	if len(deletes) != 3 {
		t.Fatalf("%d deletes, want one for each of the 3 workers", len(deletes))
	}
	for _, d := range deletes {
		if d.grace == nil || *d.grace != 1 {
			t.Errorf("the delete of %s does not name a grace of 1 s", d.name)
		}
	}
}

// runLeaderGoneStep runs the step with a bound of 30 s over pods as the
// pods of Instance 0, with the same pods in the fake client. It returns the
// deletes the step sent and the events it recorded.
func runLeaderGoneStep(t *testing.T, pods []*corev1.Pod) ([]recordedDeleteOpts, []string) {
	t.Helper()
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, storedCopies(pods...)...)
	rec := record.NewFakeRecorder(8)
	if err := ShortenGraceAfterLeaderGone(context.Background(), workload.Deps{Client: c, Recorder: rec}, leaderGoneInput(leaderGoneBound), map[int32][]*corev1.Pod{0: pods}); err != nil {
		t.Fatalf("ShortenGraceAfterLeaderGone: %v", err)
	}
	return deletes, fdDrainEvents(rec)
}

// checkEventSays fails the test for each text the event lacks.
func checkEventSays(t *testing.T, event string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(event, want) {
			t.Errorf("the event must say %q, got %q", want, event)
		}
	}
}

// TestShortenGraceAfterLeaderGone_ALeaderGoneLaterThanTheBound: the workers
// were deleted 60 s ago with 480 s each, and only now is the leader's pod
// gone. Each worker still gets its one delete with 30 s. The API server
// counts them from the first delete, so it leaves each worker 1 s, and the
// event says that and does not promise 30 s. Workers whose first delete is
// exactly 30 s old still take the 30 s.
func TestShortenGraceAfterLeaderGone_ALeaderGoneLaterThanTheBound(t *testing.T) {
	cases := []struct {
		name string
		// left is what remains of each worker's 480 s at the pass.
		left time.Duration
		want []string
	}{
		{name: "the first delete is 60 s old", left: 420 * time.Second, want: []string{
			"OME asked for a termination grace of 30s (lifecycle.leaderGoneGracePeriod) in place of 480s, because a pod of a gang cannot finish its work without the leader.",
			"The pods were deleted more than 30s ago, so the API server leaves them a grace of 1s and each kubelet sends SIGKILL about 2s from now.",
		}},
		{name: "the first delete is exactly 30 s old", left: 450 * time.Second, want: []string{
			"OME shortened their termination grace from 480s to 30s (lifecycle.leaderGoneGracePeriod)",
			"Each kubelet counts the 30s from the moment it learns of them and then sends SIGKILL.",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workers := threeWorkers()
			for _, pod := range workers {
				endingIn(pod, 480, tc.left)
			}
			deletes, events := runLeaderGoneStep(t, workers)
			if len(deletes) != 3 || len(events) != 1 {
				t.Fatalf("deletes = %+v, events = %v, want three deletes and one event", deletes, events)
			}
			for _, d := range deletes {
				if d.grace == nil || *d.grace != 30 {
					t.Errorf("the delete of %s does not name the bound of 30 s", d.name)
				}
			}
			checkEventSays(t, events[0], tc.want...)
		})
	}
}

// TestShortenGraceAfterLeaderGone_OneEventForEachOutcome: worker 0 was
// deleted 60 s ago and workers 1 and 2 were deleted 5 s ago, and the
// leader's pod is gone. All three get the delete with 30 s. One event names
// the two that take the 30 s, and one names the worker the API server
// leaves 1 s.
func TestShortenGraceAfterLeaderGone_OneEventForEachOutcome(t *testing.T) {
	workers := threeWorkers()
	endingIn(workers[0], 480, 420*time.Second)
	endingIn(workers[1], 480, 475*time.Second)
	endingIn(workers[2], 480, 475*time.Second)
	deletes, events := runLeaderGoneStep(t, workers)
	if len(deletes) != 3 || len(events) != 2 {
		t.Fatalf("deletes = %+v, events = %v, want three deletes and two events", deletes, events)
	}
	checkEventSays(t, events[0],
		"2 pods are still terminating: svc-engine-0-worker-1, svc-engine-0-worker-2.",
		"OME shortened their termination grace from 480s to 30s (lifecycle.leaderGoneGracePeriod)")
	checkEventSays(t, events[1],
		"1 pod is still terminating: svc-engine-0-worker-0.",
		"OME asked for a termination grace of 30s (lifecycle.leaderGoneGracePeriod) in place of 480s",
		"The pod was deleted more than 30s ago, so the API server leaves it a grace of 1s and its kubelet sends SIGKILL about 2s from now.")
}

// TestShortenGraceAfterLeaderGone_APassOnAnOlderPodListWritesNothing: the
// pass holds the workers as they read before an earlier pass shortened
// them, each with 480 s. The API server already shows 30 s on two of them
// and another pod at the name of the third, so the step sends no delete and
// records no event.
func TestShortenGraceAfterLeaderGone_APassOnAnOlderPodListWritesNothing(t *testing.T) {
	listed := threeWorkers()
	shown := threeWorkers()
	for _, pod := range shown[:2] {
		endingIn(pod, 30, 25*time.Second)
	}
	shown[2].UID = "another-pod-at-the-same-name"
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	c := fdFakeClient(t, &funcs, storedCopies(shown...)...)
	rec := record.NewFakeRecorder(8)
	if err := ShortenGraceAfterLeaderGone(context.Background(), workload.Deps{Client: c, Recorder: rec}, leaderGoneInput(leaderGoneBound), map[int32][]*corev1.Pod{0: listed}); err != nil {
		t.Fatalf("ShortenGraceAfterLeaderGone: %v", err)
	}
	if events := fdDrainEvents(rec); len(deletes) != 0 || len(events) != 0 {
		t.Fatalf("deletes = %+v, events = %v, want none of either", deletes, events)
	}
}

// TestShortenGraceAfterLeaderGone_AFailedReadIsReported: the API server does
// not answer the read of the first worker. The step reports that and sends
// the worker no delete, and the other two workers still take the bound.
func TestShortenGraceAfterLeaderGone_AFailedReadIsReported(t *testing.T) {
	workers := threeWorkers()
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	funcs.Get = func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if key.Name == workers[0].Name {
			return apierrors.NewServiceUnavailable("the API server is busy")
		}
		return cl.Get(ctx, key, obj, opts...)
	}
	c := fdFakeClient(t, &funcs, storedCopies(workers...)...)
	err := ShortenGraceAfterLeaderGone(context.Background(), workload.Deps{Client: c, Recorder: record.NewFakeRecorder(8)}, leaderGoneInput(leaderGoneBound), map[int32][]*corev1.Pod{0: workers})
	if err == nil || !strings.Contains(err.Error(), workers[0].Name) {
		t.Fatalf("err = %v, want the failed read of %s reported", err, workers[0].Name)
	}
	if len(deletes) != 2 {
		t.Fatalf("deletes = %+v, want one for each of the other two workers", deletes)
	}
}
