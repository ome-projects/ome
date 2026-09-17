package workload_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
)

// Terminal-pod recovery: a pod the kubelet rejects at admission (phase
// Failed, no container ever started) still occupies its stable name. The
// owning operation must delete it and recreate the target within the same
// attempt, paced by the update retry ladder and bounded by the attempt's
// deadline; a gang recycles as a whole.
//
// The harness is the closed reconcile loop of corrective_recovery_test.go
// with a kubelet that rejects admissions on demand: fake client + fake
// clock + IR-backed InstanceStatus round-trip + in-memory RetryBlocks.

const (
	admissionRejectReason = "UnexpectedAdmissionError"
	// admissionStep is the loop's clock advance per pass — the op requeue
	// interval, so pacing can be observed at that resolution.
	admissionStep = 5 * time.Second
)

// admission records one pod object the kubelet saw for the first time.
type admission struct {
	name        string
	incarnation string
	at          time.Time
	rejected    bool
	// op is the Instance's operation that issued the pod (nil when none).
	op *workload.InstanceOperation
}

type admissionHarness struct {
	t    *testing.T
	ctx  context.Context
	c    client.Client
	isvc *v1beta1.InferenceService
	clk  *clocktesting.FakeClock

	desired workload.WorkloadDesiredSpec
	target  *appsv1.ControllerRevision
	policy  *workload.RetryPolicy

	// reject decides whether the kubelet rejects the n-th pod object
	// (1-based) it sees under a name.
	reject func(name string, n int) bool
	seen   map[string]int

	admissions      []admission
	currentRevision string
	blocks          []workload.RetryBlock
	phases          map[workload.InstancePhase]int
	opTypes         map[workload.InstanceOperationType]int
}

func newAdmissionHarness(t *testing.T, multiPod bool, reject func(name string, n int) bool) *admissionHarness {
	t.Helper()
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: recoveryOwner, Namespace: recoveryNS, UID: "uid-1",
	}}
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc).Build()
	h := &admissionHarness{
		t:    t,
		ctx:  context.Background(),
		c:    c,
		isvc: isvc,
		// Status timestamps round-trip at second precision.
		clk:     clocktesting.NewFakeClock(time.Now().Truncate(time.Second)),
		policy:  &workload.RetryPolicy{MaxAttempts: 3, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2},
		reject:  reject,
		seen:    map[string]int{},
		phases:  map[workload.InstancePhase]int{},
		opTypes: map[workload.InstanceOperationType]int{},
	}
	spec := recoveryPodSpec(goodImage)
	var workerSpec *corev1.PodSpec
	if multiPod {
		workerSpec = spec.DeepCopy()
	}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		h.ctx, h.c, h.c, h.isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		revision.Key{Namespace: recoveryNS, Name: recoveryOwner + "-" + string(workload.ComponentEngine)},
		spec, workerSpec, nil, nil, h.isvc.UID,
	)
	if err != nil {
		t.Fatalf("EnsureControllerRevision: %v", err)
	}
	h.target = cr
	h.desired = workload.WorkloadDesiredSpec{Replicas: 1, PodSpec: spec}
	if multiPod {
		h.desired.MultiPod = true
		h.desired.WorkerPodSpec = workerSpec
		h.desired.Runners = []workload.Runner{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	} else {
		h.desired.Runners = []workload.Runner{{Name: "default", Size: 1}}
	}
	return h
}

func (h *admissionHarness) irKey() types.NamespacedName {
	return types.NamespacedName{Namespace: recoveryNS, Name: recoveryOwner + "-" + string(workload.ComponentEngine)}
}

func (h *admissionHarness) irStatuses() []workload.InstanceStatus {
	h.t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := h.c.Get(h.ctx, h.irKey(), ir); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		h.t.Fatalf("get IR: %v", err)
	}
	out := make([]workload.InstanceStatus, 0, len(ir.Status.InstanceStatuses))
	for _, s := range ir.Status.InstanceStatuses {
		out = append(out, v1beta1convert.InstanceStatusToWorkload(s))
	}
	return out
}

// status returns the single Instance's status, or nil before it exists.
func (h *admissionHarness) status() *workload.InstanceStatus {
	for _, s := range h.irStatuses() {
		if s.Index == 0 {
			return &s
		}
	}
	return nil
}

func (h *admissionHarness) removeInstance() func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		ir := &v1beta1.InferenceReplica{}
		if err := h.c.Get(ctx, h.irKey(), ir); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index != idx {
				continue
			}
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses[:i], ir.Status.InstanceStatuses[i+1:]...)
			return true, h.c.Status().Update(ctx, ir)
		}
		return false, nil
	}
}

func (h *admissionHarness) buildInput() workload.ReconcileInput {
	in := workload.ReconcileInput{
		OwnerObject: h.isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: h.isvc,
		Key: workload.Key{
			Namespace: recoveryNS,
			Component: workload.ComponentEngine,
			OwnerName: recoveryOwner,
		},
		DesiredSpec: h.desired,
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: h.irStatuses(),
			RetryBlocks:      append([]workload.RetryBlock(nil), h.blocks...),
			CurrentRevision:  h.currentRevision,
			UpdateRevision:   h.target.Name,
		},
		MutateInstance:    roundTripMutateInstance(h.c, h.isvc, workload.ComponentEngine),
		RemoveInstance:    h.removeInstance(),
		UpdateRetryPolicy: h.policy,
		StuckPodGrace:     30 * time.Second,
		Clock:             h.clk,
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
			pos := -1
			b := workload.RetryBlock{TargetRevision: rev}
			for i := range h.blocks {
				if h.blocks[i].TargetRevision == rev {
					pos, b = i, h.blocks[i]
					break
				}
			}
			switch mutate(&b) {
			case workload.RetryBlockPersist:
				if pos == -1 {
					h.blocks = append(h.blocks, b)
				} else {
					h.blocks[pos] = b
				}
			case workload.RetryBlockRemove:
				if pos != -1 {
					h.blocks = append(h.blocks[:pos], h.blocks[pos+1:]...)
				}
			}
			return nil
		},
	}
	stubInputCallbacks(&in)
	return in
}

// kubelet decides admission for every pod object it sees for the first
// time: a rejected pod parks in phase Failed with no container started; an
// admitted pod runs, becomes ContainersReady, and PodReady once the
// controller's serving gate is True. Terminal pods are never touched again.
func (h *admissionHarness) kubelet() {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("kubelet list: %v", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || query.IsTerminalPod(pod) {
			continue
		}
		if pod.Status.Phase == "" {
			pod.CreationTimestamp = metav1.NewTime(h.clk.Now())
			if err := h.c.Update(h.ctx, pod); err != nil {
				h.t.Fatalf("kubelet stamp creation %s: %v", pod.Name, err)
			}
			h.seen[pod.Name]++
			rejected := h.reject != nil && h.reject(pod.Name, h.seen[pod.Name])
			var op *workload.InstanceOperation
			if s := h.status(); s != nil {
				op = s.Operation
			}
			h.admissions = append(h.admissions, admission{
				name:        pod.Name,
				incarnation: pod.Labels[query.LabelInstanceIncarnation],
				at:          h.clk.Now(),
				rejected:    rejected,
				op:          op,
			})
			if rejected {
				pod.Status.Phase = corev1.PodFailed
				pod.Status.Reason = admissionRejectReason
				pod.Status.Message = "Pod was rejected: node resources unavailable"
				pod.Status.ContainerStatuses = nil
				setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
				setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
				if err := h.c.Status().Update(h.ctx, pod); err != nil {
					h.t.Fatalf("kubelet reject %s: %v", pod.Name, err)
				}
				continue
			}
		}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  pod.Spec.Containers[0].Name,
			Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, h.clk.Now())
		ready := corev1.ConditionFalse
		for _, cond := range pod.Status.Conditions {
			if cond.Type == query.ServingConditionType && cond.Status == corev1.ConditionTrue {
				ready = corev1.ConditionTrue
			}
		}
		setPodCondition(pod, corev1.PodReady, ready, h.clk.Now())
		if err := h.c.Status().Update(h.ctx, pod); err != nil {
			h.t.Fatalf("kubelet update %s: %v", pod.Name, err)
		}
	}
}

func (h *admissionHarness) pods() []corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	return pods.Items
}

// step runs one pass: kubelet, fresh expectations (watch caught up),
// Reconcile, bookkeeping, then the clock advances by the pass's requeue
// (at least admissionStep).
func (h *admissionHarness) step() {
	h.t.Helper()
	h.kubelet()
	deps := workload.Deps{Client: h.c, APIReader: h.c, Expectations: workload.NewExpectations(), Clock: h.clk}
	in := h.buildInput()
	plan, err := workload.BuildPlan(workload.ComponentEngine, h.desired, in.ObservedState)
	if err != nil {
		h.t.Fatalf("BuildPlan: %v", err)
	}
	res, err := workload.Reconcile(h.ctx, deps, in, plan, h.target)
	if err != nil {
		h.t.Fatalf("Reconcile: %v", err)
	}
	if workload.RolloutComplete(h.irStatuses(), h.target.Name) {
		h.currentRevision = h.target.Name
	}
	h.observe()
	advance := admissionStep
	if res.RequeueAfter > advance {
		advance = res.RequeueAfter
	}
	h.clk.Step(advance)
}

// observe records the phases and operations seen after a pass and checks
// the per-pass invariants: a pod created this pass was issued by an
// attempt that has not passed its deadline, and a recycling operation
// (Create or Restart) never creates a replacement while a terminal pod of
// the same Instance is still standing. A SurgeThenDrain Update is exempt
// from the second check: its replacement legitimately runs at the other
// ordinal next to the pod it replaces.
func (h *admissionHarness) observe() {
	h.t.Helper()
	s := h.status()
	if s != nil {
		h.phases[s.Phase]++
		if s.Operation != nil {
			h.opTypes[s.Operation.Type]++
		}
	}
	fresh, terminal := 0, 0
	pods := h.pods()
	for i := range pods {
		switch {
		case query.IsTerminalPod(&pods[i]):
			terminal++
		case pods[i].Status.Phase == "":
			fresh++
		}
	}
	if fresh > 0 {
		if s == nil || s.Operation == nil {
			h.t.Fatalf("a pod was created with no operation owning the Instance: %+v", s)
		}
		if h.clk.Now().After(s.Operation.Deadline.Time) {
			h.t.Fatalf("a pod was created at %v, after the attempt's deadline %v", h.clk.Now(), s.Operation.Deadline.Time)
		}
	}
	recycling := s != nil && s.Operation != nil &&
		(s.Operation.Type == workload.InstanceOperationCreate || s.Operation.Type == workload.InstanceOperationRestart)
	if recycling && fresh > 0 && terminal > 0 {
		h.dump("mixed fresh and terminal pods")
		h.t.Fatalf("%s created a replacement next to %d terminal pod(s) of the same Instance", s.Operation.Type, terminal)
	}
}

func (h *admissionHarness) run(max int, pred func() bool) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		if pred() {
			return true
		}
		h.step()
	}
	return pred()
}

// converged reports Phase=Ready on the target with no operation and every
// pod live and ContainersReady.
func (h *admissionHarness) converged() bool {
	s := h.status()
	if s == nil || s.Phase != workload.InstancePhaseReady || s.RunningRevision != h.target.Name || s.Operation != nil {
		return false
	}
	pods := h.pods()
	if len(pods) == 0 {
		return false
	}
	for i := range pods {
		if query.IsTerminalPod(&pods[i]) || workload.CountReadyPods([]*corev1.Pod{&pods[i]}) == 0 {
			return false
		}
	}
	return true
}

func (h *admissionHarness) dump(label string) {
	h.t.Logf("--- %s at %v ---", label, h.clk.Now())
	if s := h.status(); s != nil {
		op := "nil"
		if s.Operation != nil {
			op = fmt.Sprintf("{type=%s step=%s retries=%d deadline=%v}", s.Operation.Type, s.Operation.Step, s.Operation.RetryCount, s.Operation.Deadline.Time)
		}
		h.t.Logf("  instance %d: phase=%s incarnation=%d op=%s", s.Index, s.Phase, s.Incarnation, op)
	}
	for _, p := range h.pods() {
		h.t.Logf("  pod %s phase=%s incarnation=%s", p.Name, p.Status.Phase, p.Labels[query.LabelInstanceIncarnation])
	}
	for _, a := range h.admissions {
		id := "-"
		if a.op != nil {
			id = fmt.Sprintf("%s#%d", a.op.ID, a.op.RetryCount)
		}
		h.t.Logf("  admission %s at %v rejected=%v op=%s", a.name, a.at, a.rejected, id)
	}
}

// (a) A single-pod Instance whose pod dies at admission is repaired within
// the same Create attempt: the dead pod is deleted, the target recreated,
// and the Instance reaches Ready when the replacement does.
func TestTerminalPodRecovery_SinglePod_RecycledWithinAttempt(t *testing.T) {
	h := newAdmissionHarness(t, false, func(_ string, n int) bool { return n == 1 })
	if !h.run(40, h.converged) {
		h.dump("single-pod recycle wedged")
		t.Fatalf("Instance never reached Ready after its pod died at admission")
	}
	if len(h.admissions) != 2 || !h.admissions[0].rejected || h.admissions[1].rejected || h.admissions[0].name != h.admissions[1].name {
		h.dump("admissions")
		t.Fatalf("expected one rejected admission followed by one admitted replacement of the same name, got %d admissions", len(h.admissions))
	}
	first, second := h.admissions[0].op, h.admissions[1].op
	if first == nil || second == nil || first.ID != second.ID || first.Type != workload.InstanceOperationCreate {
		h.dump("attempts")
		t.Fatalf("the replacement must be issued by the original Create attempt, got %+v then %+v", first, second)
	}
	if second.RetryCount != 1 {
		t.Fatalf("the replacement must follow one recorded recycle, got RetryCount=%d", second.RetryCount)
	}
	if h.phases[workload.InstancePhaseFailed] > 0 {
		t.Fatalf("the attempt must not escalate to Failed while it is repairing itself")
	}
	if s := h.status(); s.LastFailure == nil || s.LastFailure.Reason != admissionRejectReason {
		t.Fatalf("LastFailure must preserve the admission failure, got %+v", s.LastFailure)
	}
}

// (b) A gang whose every member dies at admission is deleted and recreated
// together, by Create at the same incarnation, and reaches Ready.
func TestTerminalPodRecovery_Gang_AllMembersRecycledTogether(t *testing.T) {
	h := newAdmissionHarness(t, true, func(_ string, n int) bool { return n == 1 })
	if !h.run(40, h.converged) {
		h.dump("gang recycle wedged")
		t.Fatalf("gang never reached Ready after both members died at admission")
	}
	if len(h.admissions) != 4 {
		h.dump("admissions")
		t.Fatalf("expected two rejected admissions and two admitted replacements, got %d", len(h.admissions))
	}
	if !h.admissions[2].at.Equal(h.admissions[3].at) {
		t.Fatalf("replacements must be created together, got %v and %v", h.admissions[2].at, h.admissions[3].at)
	}
	if h.opTypes[workload.InstanceOperationRestart] > 0 {
		t.Fatalf("total loss is Create's to rebuild; no Restart may run")
	}
	if s := h.status(); s.Incarnation != 1 {
		t.Fatalf("a whole-gang recycle by Create keeps the incarnation, got %d", s.Incarnation)
	}
	for _, p := range h.pods() {
		if p.Labels[query.LabelInstanceIncarnation] != "1" {
			t.Fatalf("pod %s carries incarnation %q, want 1", p.Name, p.Labels[query.LabelInstanceIncarnation])
		}
	}
}

// (d) Repeated rejections space recreates by the retry ladder within one
// attempt, and no recreate outlives the attempt's deadline; the expired
// attempt is escalated and disposed, and only a different operation may
// touch the Instance afterwards.
func TestTerminalPodRecovery_RecreatesFollowLadderWithinDeadline(t *testing.T) {
	h := newAdmissionHarness(t, false, func(string, int) bool { return true })
	h.desired.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 4 * time.Minute}
	for i := 0; i < 60; i++ {
		h.step()
	}

	// Group admissions by attempt, in order of first appearance.
	var attempts [][]admission
	byID := map[string]int{}
	for _, a := range h.admissions {
		if a.op == nil {
			h.dump("admission without an attempt")
			t.Fatalf("admission of %s at %v was not issued by an attempt", a.name, a.at)
		}
		pos, ok := byID[a.op.ID]
		if !ok {
			pos = len(attempts)
			byID[a.op.ID] = pos
			attempts = append(attempts, nil)
		}
		attempts[pos] = append(attempts[pos], a)
	}
	if len(attempts) < 2 {
		h.dump("attempts")
		t.Fatalf("expected the expired attempt to be disposed and a fresh one started, got %d attempt(s)", len(attempts))
	}
	first := attempts[0]
	if len(first) < 4 {
		h.dump("first attempt")
		t.Fatalf("expected several paced recreates within the first attempt, got %d admission(s)", len(first))
	}
	for i := range first {
		if a := first[i]; a.at.After(a.op.Deadline.Time) {
			t.Fatalf("recreate %d at %v outlived the attempt's deadline %v", i, a.at, a.op.Deadline.Time)
		}
		if first[i].op.RetryCount != int32(i) {
			t.Fatalf("recreate %d must follow %d recorded recycle(s), got RetryCount=%d", i, i, first[i].op.RetryCount)
		}
	}
	// The first recycle is immediate; each later one waits the ladder delay
	// measured from the previous recycle.
	if gap := first[1].at.Sub(first[0].at); gap > 2*admissionStep {
		t.Fatalf("first recycle must be immediate, got %v", gap)
	}
	for i := 1; i+1 < len(first); i++ {
		want := h.policy.NextRetryDelay(int32(i))
		gap := first[i+1].at.Sub(first[i].at)
		if gap < want || gap > want+admissionStep {
			h.dump("ladder")
			t.Fatalf("recreate %d -> %d spaced %v, want the ladder delay %v", i, i+1, gap, want)
		}
	}
	if h.phases[workload.InstancePhaseFailed] == 0 {
		t.Fatalf("the expired attempt must escalate to Failed at its deadline")
	}
	if second := attempts[1]; !second[0].at.After(first[0].op.Deadline.Time) {
		t.Fatalf("the fresh attempt's first recreate at %v must follow the expired attempt's deadline %v", second[0].at, first[0].op.Deadline.Time)
	}
}

// admissionsByIncarnation counts admitted-or-rejected pod objects per
// incarnation label.
func (h *admissionHarness) admissionsByIncarnation() map[string]int {
	out := map[string]int{}
	for _, a := range h.admissions {
		out[a.incarnation]++
	}
	return out
}

// requireNoGapFill fails when a pod was created at an incarnation older
// than one already seen: a gang gap filled next to survivors instead of a
// whole-gang rebuild at a bumped incarnation.
func (h *admissionHarness) requireNoGapFill() {
	h.t.Helper()
	newest := int64(0)
	for _, a := range h.admissions {
		inc, err := strconv.ParseInt(a.incarnation, 10, 64)
		if err != nil {
			h.t.Fatalf("pod %s carries an unreadable incarnation label %q: %v", a.name, a.incarnation, err)
		}
		if inc < newest {
			h.dump("gap fill")
			h.t.Fatalf("pod %s was created at incarnation %d after incarnation %d existed: a gap fill next to survivors", a.name, inc, newest)
		}
		newest = inc
	}
}

// (c) A gang with one dead member and one live member is rebuilt as a
// whole by Restart: incarnation bump, the survivor drained and deleted,
// both members recreated together — never a single-member gap fill next
// to the survivor.
func TestTerminalPodRecovery_Gang_SurvivorRebuiltAsWhole(t *testing.T) {
	leader := query.PodName(recoveryOwner, workload.ComponentEngine, 0, "leader", 0)
	worker := query.PodName(recoveryOwner, workload.ComponentEngine, 0, "worker", 0)
	h := newAdmissionHarness(t, true, func(name string, n int) bool { return name == leader && n == 1 })
	if !h.run(40, h.converged) {
		h.dump("gang with survivor wedged")
		t.Fatalf("gang never reached Ready after one member died at admission")
	}
	if h.opTypes[workload.InstanceOperationRestart] == 0 {
		t.Fatalf("a gang with a survivor must be rebuilt by Restart, not gap-filled by Create")
	}
	if s := h.status(); s.Incarnation != 2 {
		t.Fatalf("the rebuild must bump the incarnation, got %d", s.Incarnation)
	}
	h.requireNoGapFill()
	if byInc := h.admissionsByIncarnation(); byInc["1"] != 2 || byInc["2"] != 2 || len(h.admissions) != 4 {
		h.dump("admissions")
		t.Fatalf("expected the initial pair at incarnation 1 and one whole-gang recreate at incarnation 2, got %v", byInc)
	}
	seenWorker := 0
	for _, a := range h.admissions {
		if a.name == worker {
			seenWorker++
		}
	}
	if seenWorker != 2 {
		t.Fatalf("the surviving worker must be drained and recreated with the gang, got %d worker admissions", seenWorker)
	}
	for _, p := range h.pods() {
		if p.Labels[query.LabelInstanceIncarnation] != "2" {
			t.Fatalf("pod %s still runs at incarnation %q after the rebuild", p.Name, p.Labels[query.LabelInstanceIncarnation])
		}
	}
}

// (c, spent attempt) When the dead member keeps dying, the Restart attempt
// expires to Failed with its operation preserved; that spent attempt must
// re-arm into a new whole-gang Restart (another incarnation bump, survivor
// drained again) instead of wedging or letting Create gap-fill.
func TestTerminalPodRecovery_Gang_SpentRestartReArms(t *testing.T) {
	leader := query.PodName(recoveryOwner, workload.ComponentEngine, 0, "leader", 0)
	worker := query.PodName(recoveryOwner, workload.ComponentEngine, 0, "worker", 0)
	h := newAdmissionHarness(t, true, func(name string, n int) bool { return name == leader && n <= 5 })
	h.desired.Lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: 2 * time.Minute}
	if !h.run(80, h.converged) {
		h.dump("spent restart wedged")
		t.Fatalf("gang never reached Ready after its Restart attempt expired")
	}
	if h.phases[workload.InstancePhaseFailed] == 0 {
		t.Fatalf("the first Restart attempt must expire to Failed before re-arming")
	}
	s := h.status()
	if s.Incarnation < 3 {
		t.Fatalf("the re-armed Restart must bump the incarnation again, got %d", s.Incarnation)
	}
	h.requireNoGapFill()
	seenWorker := 0
	for _, a := range h.admissions {
		if a.name == worker {
			seenWorker++
		}
	}
	if seenWorker != int(s.Incarnation) {
		h.dump("worker admissions")
		t.Fatalf("every rebuild must drain and recreate the survivor: %d worker admissions across %d incarnations", seenWorker, s.Incarnation)
	}
}
