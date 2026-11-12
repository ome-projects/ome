package ops_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// heldRowPriorRevision is the revision the held Instance of these pictures
// ran before the push, the one its row still records.
const heldRowPriorRevision = "llama-70b-engine-priorrev"

// heldRowWorld is the picture every test of this file starts from: a roll
// to a new revision with Instance 1 Ready on it, and Instance 0 Failed with
// no operation on the revision it ran before, a rebuild of it having run
// past its deadline, with its pod running, its containers ready and no
// serving gate. With held set, the roll's partition holds Instance 0 on
// that revision; without it the roll may take the Instance.
type heldRowWorld struct {
	t      *testing.T
	isvc   *v1beta1.InferenceService
	c      client.Client
	target *appsv1.ControllerRevision
	plan   workload.ComponentPlan
	held   bool
	blocks []workload.RetryBlock
	// keptTarget, when set, is the target the held row still records: the
	// shape of a surge source whose attempt toward the roll failed.
	keptTarget string
}

func newHeldRowWorld(t *testing.T, held bool, pods ...*corev1.Pod) *heldRowWorld {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-" + testRevisionHash, Namespace: "prod"},
	}
	ir := seedInstanceStatuses(isvc,
		v1beta1.OMENativeInstanceStatus{
			Index: 0, Incarnation: 1,
			Phase:           v1beta1.OMENativeInstanceFailed,
			RunningRevision: heldRowPriorRevision,
			LastFailure:     &v1beta1.InstanceTermination{PodName: "llama-70b-engine-0-default-0", Reason: "DeadlineExceeded", Time: metav1.Now()},
		},
		v1beta1.OMENativeInstanceStatus{
			Index: 1, Incarnation: 1,
			Phase:           v1beta1.OMENativeInstanceReady,
			RunningRevision: target.Name,
		},
	)
	objs := []client.Object{isvc, ir, promotablePodForInstance(isvc, 1)}
	for _, pod := range pods {
		objs = append(objs, pod)
	}
	return &heldRowWorld{t: t, isvc: isvc, c: newFakeClient(t, objs...), target: target, plan: buildPlanSinglePodEngine(2), held: held}
}

// heldRowPod is Instance 0's pod on the revision the row records, running
// with its containers ready and no serving gate.
func heldRowPod(isvc *v1beta1.InferenceService, revision string) *corev1.Pod {
	pod := podForInstance(isvc, 0, true /* containers ready */, false /* serving */)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(revision)
	return pod
}

func (w *heldRowWorld) input() workload.ReconcileInput {
	input := buildTestInput(w.isvc, w.c, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = w.target.Name
	input.ObservedState.RetryBlocks = w.blocks
	if w.keptTarget != "" {
		input.ObservedState.Instance(0).TargetRevision = w.keptTarget
	}
	if w.held {
		input.DesiredSpec.Pacing = &workload.WorkloadPacing{Partition: ptr.To[int32](1)}
	}
	return input
}

// pass runs the Create pass at the fresh scope, or the full one.
func (w *heldRowWorld) pass(fresh bool) {
	w.t.Helper()
	var err error
	if fresh {
		_, err = ops.CreateFreshIndices(context.Background(), workload.Deps{Client: w.c}, w.input(), w.plan, w.target)
	} else {
		_, err = ops.Create(context.Background(), workload.Deps{Client: w.c}, w.input(), w.plan, w.target)
	}
	if err != nil {
		w.t.Fatalf("Create pass: %v", err)
	}
}

func (w *heldRowWorld) podServing(name string) bool {
	w.t.Helper()
	got := &corev1.Pod{}
	if err := w.c.Get(context.Background(), client.ObjectKey{Namespace: w.isvc.Namespace, Name: name}, got); err != nil {
		w.t.Fatalf("get pod %s: %v", name, err)
	}
	return podreadiness.IsServing(got)
}

func (w *heldRowWorld) row(idx int32) *v1beta1.OMENativeInstanceStatus {
	return findInstanceStatusOnIR(w.c, w.isvc, workload.ComponentEngine, idx)
}

// requireReproved walks the held re-prove through two passes at the given
// scope: the first writes the serving gate on the running pod and leaves
// the row Failed for the kubelet to fold the gate into PodReady; the
// second, once it has, returns the row to Ready on the revision it
// records, and leaves the Ready peer on the target as it was.
func (w *heldRowWorld) requireReproved(fresh bool) {
	w.t.Helper()
	pod := heldRowPod(w.isvc, heldRowPriorRevision)
	w.pass(fresh)
	if !w.podServing(pod.Name) {
		w.t.Fatalf("the held row's running pod must get the serving gate")
	}
	if s := w.row(0); s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		w.t.Fatalf("the row waits Failed for the kubelet to fold the gate into PodReady, got %+v", s)
	}
	makeNewPodReady(w.t, w.c, w.isvc.Namespace, pod.Name, 1)
	w.pass(fresh)
	if s := w.row(0); s == nil || s.Phase != v1beta1.OMENativeInstanceReady || s.Operation != nil || s.RunningRevision != heldRowPriorRevision {
		w.t.Fatalf("the held row must return to Ready on the revision it runs, got %+v", s)
	}
	if peer := w.row(1); peer == nil || peer.Phase != v1beta1.OMENativeInstanceReady || peer.Operation != nil || peer.RunningRevision != w.target.Name {
		w.t.Fatalf("the Ready peer on the target must be left as it was, got %+v", peer)
	}
}

// A Failed row the roll's partition holds, off the target and with no
// operation, is the fresh scope's as a held podless row is: the roll never
// takes a held Instance, so nothing else returns it to service. Once every
// pod of its set runs at the revision it records, the pass writes the
// serving gate, and promotes the row Ready on that revision when the
// kubelet folds the gate into PodReady; the Ready peer on the target is
// left as it was.
func TestCreateFreshIndices_ReprovesAFailedRowTheRollHoldsOnceItsSetRuns(t *testing.T) {
	w := newHeldRowWorld(t, true)
	w.c.Create(context.Background(), heldRowPod(w.isvc, heldRowPriorRevision)) //nolint:errcheck // the fake client refuses nothing here
	w.requireReproved(true)
}

// The full Create pass, the scope a pass with no update under way runs,
// proves the same held row on the revision it records.
func TestCreate_ReprovesAFailedRowTheRollHoldsOnceItsSetRuns(t *testing.T) {
	w := newHeldRowWorld(t, true)
	w.c.Create(context.Background(), heldRowPod(w.isvc, heldRowPriorRevision)) //nolint:errcheck // the fake client refuses nothing here
	w.requireReproved(false)
}

// The same Failed row with no partition holding it stays the roll's: the
// roll may take it this pass, and a second owner would race the Update it
// writes. The fresh scope leaves the row and its pod untouched, pass after
// pass.
func TestCreateFreshIndices_LeavesAFailedRowTheRollMayTakeToTheRoll(t *testing.T) {
	w := newHeldRowWorld(t, false)
	pod := heldRowPod(w.isvc, heldRowPriorRevision)
	w.c.Create(context.Background(), pod) //nolint:errcheck // the fake client refuses nothing here
	for pass := 0; pass < 2; pass++ {
		w.pass(true)
		if w.podServing(pod.Name) {
			t.Fatalf("pass %d: a Failed row the roll may take is the roll's; the Create pass must not gate its pod", pass)
		}
		if s := w.row(0); s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil || s.RunningRevision != heldRowPriorRevision {
			t.Fatalf("pass %d: the row must stay Failed with no operation for the roll, got %+v", pass, s)
		}
	}
}

// A held row is proved on the revision it records only when that
// revision's retry record admits a start: a backoff still running or a Held
// record holds the gate write back, and a backoff that is due admits it.
func TestCreateFreshIndices_HeldReproveWaitsForTheRecordOfItsRevision(t *testing.T) {
	cases := []struct {
		name  string
		block workload.RetryBlock
		gated bool
	}{
		{"a backoff still running holds the gate back", workload.RetryBlock{TargetRevision: heldRowPriorRevision, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: ptr.To(metav1.NewTime(time.Now().Add(time.Hour)))}, false},
		{"a Held record holds the gate back", workload.RetryBlock{TargetRevision: heldRowPriorRevision, State: workload.RetryBlockHeld, AttemptsStarted: 3}, false},
		{"a backoff that is due admits the gate", workload.RetryBlock{TargetRevision: heldRowPriorRevision, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: ptr.To(metav1.NewTime(time.Now().Add(-time.Minute)))}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newHeldRowWorld(t, true)
			w.blocks = []workload.RetryBlock{tc.block}
			pod := heldRowPod(w.isvc, heldRowPriorRevision)
			w.c.Create(context.Background(), pod) //nolint:errcheck // the fake client refuses nothing here
			w.pass(true)
			if got := w.podServing(pod.Name); got != tc.gated {
				t.Fatalf("serving gate written: got %v want %v", got, tc.gated)
			}
			if s := w.row(0); s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
				t.Fatalf("the row stays Failed until the kubelet folds the gate into PodReady, got %+v", s)
			}
		})
	}
}

// A held row whose pod at its active ordinal carries another revision than
// the one the row records is not proved: such a pod is the replacement of
// an attempt the roll made toward its target, which the partition keeps
// from starting again, and the row keeps the failure that names it.
// TestCreateFreshIndices_HeldRowKeepingATargetTowardTheRollIsTheRolls: a
// held Failed row that still records a target toward the roll is a surge
// source whose attempt failed with its replacement standing. The roll
// holds on it under the RetryBlock gate and names the exits, so neither
// scope of the Create pass gates its serving source or moves the row.
func TestCreateFreshIndices_HeldRowKeepingATargetTowardTheRollIsTheRolls(t *testing.T) {
	w := newHeldRowWorld(t, true)
	w.keptTarget = w.target.Name
	pod := heldRowPod(w.isvc, heldRowPriorRevision)
	w.c.Create(context.Background(), pod) //nolint:errcheck // the fake client refuses nothing here
	w.pass(true)
	w.pass(false)
	if w.podServing(pod.Name) {
		t.Fatalf("a held row keeping a target toward the roll is the roll's; its pod must not be gated")
	}
	if s := w.row(0); s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil || s.RunningRevision != heldRowPriorRevision {
		t.Fatalf("the row must stay Failed with no operation, got %+v", s)
	}
}

func TestCreateFreshIndices_HeldRowWithAPodOfAnotherRevisionIsNotProved(t *testing.T) {
	w := newHeldRowWorld(t, true)
	pod := heldRowPod(w.isvc, w.target.Name)
	w.c.Create(context.Background(), pod) //nolint:errcheck // the fake client refuses nothing here
	w.pass(true)
	if w.podServing(pod.Name) {
		t.Fatalf("a pod of another revision than the row records must not be gated")
	}
	if s := w.row(0); s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil || s.RunningRevision != heldRowPriorRevision {
		t.Fatalf("the row must stay Failed with no operation, got %+v", s)
	}
}

// heldGangWorld is the gang picture: two gangs of a leader and a worker,
// gang 1 Ready on the roll target and gang 0 Failed with no operation on the
// revision it ran before, held there by the roll's partition. The pods the
// test seeds for gang 0 carry that revision.
type heldGangWorld struct {
	t      *testing.T
	isvc   *v1beta1.InferenceService
	c      client.Client
	target *appsv1.ControllerRevision
	plan   workload.ComponentPlan
}

func newHeldGangWorld(t *testing.T, restart workload.RestartPolicy, gangZero func(*v1beta1.InferenceService) []*corev1.Pod) *heldGangWorld {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-" + testRevisionHash, Namespace: "prod"},
	}
	ir := seedInstanceStatuses(isvc,
		v1beta1.OMENativeInstanceStatus{
			Index: 0, Incarnation: 1,
			Phase:           v1beta1.OMENativeInstanceFailed,
			RunningRevision: heldRowPriorRevision,
			LastFailure:     &v1beta1.InstanceTermination{PodName: "llama-70b-engine-0-leader-0", Reason: "DeadlineExceeded", Time: metav1.Now()},
		},
		v1beta1.OMENativeInstanceStatus{
			Index: 1, Incarnation: 1,
			Phase:           v1beta1.OMENativeInstanceReady,
			RunningRevision: target.Name,
		},
	)
	// A refill renders the template of the revision the gang records, so
	// the stored revision is seeded for the pass to read.
	prior := storedRevision(t, isvc, heldRowPriorRevision, fixtureTemplate(), fixtureTemplate())
	objs := []client.Object{isvc, ir, prior,
		gangPod(isvc, 1, "leader", 0, 1, true, true), gangPod(isvc, 1, "worker", 0, 1, true, true)}
	for _, pod := range gangZero(isvc) {
		pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(heldRowPriorRevision)
		objs = append(objs, pod)
	}
	plan := workload.ComponentPlan{
		Component:            workload.ComponentEngine,
		Replicas:             2,
		RestartPolicy:        restart,
		InstanceReadyTimeout: 30 * time.Minute,
		Instances: []workload.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}},
		},
	}
	return &heldGangWorld{t: t, isvc: isvc, c: newFakeClient(t, objs...), target: target, plan: plan}
}

func (w *heldGangWorld) pass() {
	w.t.Helper()
	input := buildTestInput(w.isvc, w.c, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = w.target.Name
	input.DesiredSpec.Pacing = &workload.WorkloadPacing{Partition: ptr.To[int32](1)}
	if _, err := ops.CreateFreshIndices(context.Background(), workload.Deps{Client: w.c}, input, w.plan, w.target); err != nil {
		w.t.Fatalf("CreateFreshIndices: %v", err)
	}
}

func (w *heldGangWorld) pods(idx int32) []corev1.Pod {
	w.t.Helper()
	list := &corev1.PodList{}
	if err := w.c.List(context.Background(), list, client.InNamespace(w.isvc.Namespace)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	out := make([]corev1.Pod, 0, 2)
	for _, pod := range list.Items {
		if pod.Labels[query.LabelInstanceIdx] == strconv.Itoa(int(idx)) {
			out = append(out, pod)
		}
	}
	return out
}

func (w *heldGangWorld) row(idx int32) *v1beta1.OMENativeInstanceStatus {
	return findInstanceStatusOnIR(w.c, w.isvc, workload.ComponentEngine, idx)
}

// A held gang whose every member runs again at the revision it records is
// proved the way a single pod is: the gate on both pods first, then Ready
// on that revision once the kubelet folds the gate into PodReady.
func TestCreateFreshIndices_ReprovesAHeldGangOnceEveryMemberRuns(t *testing.T) {
	w := newHeldGangWorld(t, workload.RestartPolicyNone, func(isvc *v1beta1.InferenceService) []*corev1.Pod {
		return []*corev1.Pod{gangPod(isvc, 0, "leader", 0, 1, true, false), gangPod(isvc, 0, "worker", 0, 1, true, false)}
	})
	w.pass()
	for _, pod := range w.pods(0) {
		if !podreadiness.IsServing(&pod) {
			t.Fatalf("every member of the held gang must get the serving gate; %s has none", pod.Name)
		}
		makeNewPodReady(t, w.c, w.isvc.Namespace, pod.Name, 1)
	}
	if s := w.row(0); s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Fatalf("the row waits Failed for the kubelet to fold the gate into PodReady, got %+v", s)
	}
	w.pass()
	if s := w.row(0); s == nil || s.Phase != v1beta1.OMENativeInstanceReady || s.Operation != nil || s.RunningRevision != heldRowPriorRevision {
		t.Fatalf("the held gang must return to Ready on the revision it runs, got %+v", s)
	}
}

// A held gang short of a member under restart policy None is refilled by
// the fresh scope at the revision it records: the pass opens a Create for
// the gap and the new member carries that revision, not the roll target.
func TestCreateFreshIndices_RefillsAHeldGangShortOfAMemberOnItsRevision(t *testing.T) {
	w := newHeldGangWorld(t, workload.RestartPolicyNone, func(isvc *v1beta1.InferenceService) []*corev1.Pod {
		return []*corev1.Pod{gangPod(isvc, 0, "leader", 0, 1, true, true)}
	})
	w.pass()
	s := w.row(0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationCreate || s.Operation.TargetRevision != heldRowPriorRevision {
		t.Fatalf("the fresh scope must open a Create for the gap at the revision the held gang records, got %+v", s)
	}
	// The attempt is recorded ahead of its first create; the pass that
	// reads the record back creates the member.
	w.pass()
	pods := w.pods(0)
	if len(pods) != 2 {
		t.Fatalf("the missing member must be created; got %d pod(s) of gang 0; row %+v", len(pods), w.row(0))
	}
	for _, pod := range pods {
		if got := pod.Labels[query.LabelRevisionHash]; got != query.RevisionHashFromControllerRevisionName(heldRowPriorRevision) {
			t.Fatalf("%s must carry the revision the held gang records, got hash %q", pod.Name, got)
		}
	}
}

// Under RecreateInstanceOnPodRestart a gang short of a member is the restart
// pass's, which rebuilds it whole; the Create pass defers to it and writes
// nothing for the held gang.
func TestCreateFreshIndices_LeavesAHeldGangShortOfAMemberToTheRestartPassUnderRecreateOnRestart(t *testing.T) {
	w := newHeldGangWorld(t, workload.RestartPolicyRecreateInstance, func(isvc *v1beta1.InferenceService) []*corev1.Pod {
		return []*corev1.Pod{gangPod(isvc, 0, "leader", 0, 1, true, true)}
	})
	w.pass()
	if s := w.row(0); s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Fatalf("the Create pass must leave the short gang to the restart pass, got %+v", s)
	}
	if pods := w.pods(0); len(pods) != 1 {
		t.Fatalf("the Create pass must create no member of a gang the restart pass rebuilds; got %d pod(s)", len(pods))
	}
}
