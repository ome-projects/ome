package workload_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
)

// Corrective-edit recovery: after a rollout toward a broken revision
// exhausts its retries (Held), a corrective spec edit — roll-forward or
// roll-back — must reconcile to convergence without operator action.
//
// These tests drive the REAL exhaustion flow — bad target revision →
// stuck pods → escalation → disposition/abandon → Backoff → retry →
// Held — through workload.Reconcile against a fake client with a
// simulated kubelet and a fake clock, then apply a corrective edit and
// assert the loop converges with zero manual intervention. The acceptance
// matrix covers:
//
//   - direction: roll-forward (corrective NEW revision) and roll-back
//     (target returns to the source's exact RunningRevision — zero
//     revision distance, so the update trigger correctly never fires
//     and recovery must arrive via Plan's wreckage scan);
//   - topology: single-pod (surge-ordinal wreckage) and gang
//     (GangSurgeTarget marker + replacement-gang wreckage);
//   - timing: the settled at-Held state and the dirty mid-wreckage
//     state (corrective edit lands while the source is
//     Failed-with-Operation and the crashed attempt's pods are live).
//
// A partial initial Create retarget exercises the same recovery loop before
// any serving source exists.
//
// The serving source must stay in rotation throughout every recovery:
// cleanup only removes superseded-revision debris, never the healthy
// pod (asserted per step via runWithInvariant).

const (
	recoveryNS    = "prod"
	recoveryOwner = "llama-70b"

	goodImage  = "registry.test/serving:v1"
	badImage   = "registry.test/serving:broken"
	fixedImage = "registry.test/serving:v2"
)

// recoveryHarness is the closed reconcile loop: fake client + simulated
// kubelet + fake clock + in-memory RetryBlock store + IR-backed
// InstanceStatus round-trip.
type recoveryHarness struct {
	t    *testing.T
	ctx  context.Context
	c    client.Client
	isvc *v1beta1.InferenceService
	clk  *clocktesting.FakeClock

	multiPod  bool
	replicas  int32
	lifecycle workload.Lifecycle
	desired   workload.WorkloadDesiredSpec
	target    *appsv1.ControllerRevision

	// mutateErr rejects every InstanceStatus write for the listed
	// indices — the API server refusing that Instance's status update
	// on every pass — while other indices round-trip normally.
	mutateErr map[int32]error

	blocks          []workload.RetryBlock
	currentRevision string
	heldWarnings    []string

	revV1    *appsv1.ControllerRevision
	revBad   *appsv1.ControllerRevision
	revFixed *appsv1.ControllerRevision
}

func recoveryPodSpec(image string) *corev1.PodSpec {
	return &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
}

// newRecoveryHarness builds the harness plus the three ControllerRevisions
// (v1 good, bad, fixed) minted through the real revision machinery so
// names, hashes, and payloads match production exactly.
func newRecoveryHarness(t *testing.T, multiPod bool) *recoveryHarness {
	t.Helper()
	scheme := makeScheme(t)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: recoveryOwner, Namespace: recoveryNS, UID: "uid-1",
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc).Build()

	h := &recoveryHarness{
		t:        t,
		ctx:      context.Background(),
		c:        c,
		isvc:     isvc,
		clk:      clocktesting.NewFakeClock(time.Now()),
		multiPod: multiPod,
		replicas: 1,
	}
	h.revV1 = h.ensureRevision(recoveryPodSpec(goodImage))
	h.revBad = h.ensureRevision(recoveryPodSpec(badImage))
	h.revFixed = h.ensureRevision(recoveryPodSpec(fixedImage))
	h.setTarget(h.revV1, goodImage)
	h.currentRevision = ""
	return h
}

func (h *recoveryHarness) revisionKey() revision.Key {
	return revision.Key{
		Namespace: recoveryNS,
		Name:      recoveryOwner + "-" + string(workload.ComponentEngine),
	}
}

func (h *recoveryHarness) ensureRevision(spec *corev1.PodSpec) *appsv1.ControllerRevision {
	h.t.Helper()
	var workerSpec *corev1.PodSpec
	if h.multiPod {
		workerSpec = spec.DeepCopy()
	}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(
		h.ctx, h.c, h.c, h.isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		h.revisionKey(), spec, workerSpec, nil, nil, h.isvc.UID,
	)
	if err != nil {
		h.t.Fatalf("EnsureControllerRevision: %v", err)
	}
	return cr
}

// setTarget points the loop at (revision, image) — the harness analogue
// of the ISVC/runtime edit that re-renders the desired pod spec and
// republishes the target ControllerRevision.
func (h *recoveryHarness) setTarget(cr *appsv1.ControllerRevision, image string) {
	h.target = cr
	spec := recoveryPodSpec(image)
	h.desired = workload.WorkloadDesiredSpec{
		Replicas:  h.replicas,
		PodSpec:   spec,
		Lifecycle: h.lifecycle,
	}
	if h.multiPod {
		h.desired.MultiPod = true
		h.desired.WorkerPodSpec = spec.DeepCopy()
		h.desired.Runners = []workload.Runner{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	} else {
		h.desired.Runners = []workload.Runner{{Name: "default", Size: 1}}
	}
}

// kubelet simulates node-side convergence for every live pod: the bad
// image parks in ImagePullBackOff forever; every other image becomes
// ContainersReady, and PodReady once the controller's serving gate is
// True (kubelet ANDs readiness gates into PodReady).
func (h *recoveryHarness) kubelet() {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("kubelet list: %v", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.CreationTimestamp.IsZero() {
			// PodStuckPullFailure measures stuck age from CreationTimestamp
			// against the fake clock; stamp it deterministically.
			pod.CreationTimestamp = metav1.NewTime(h.clk.Now())
			if err := h.c.Update(h.ctx, pod); err != nil {
				h.t.Fatalf("kubelet stamp creation %s: %v", pod.Name, err)
			}
		}
		if pod.Spec.Containers[0].Image == badImage {
			pod.Status.Phase = corev1.PodPending
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: pod.Spec.Containers[0].Name,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason:  "ImagePullBackOff",
					Message: "Back-off pulling image " + badImage,
				}},
			}}
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, h.clk.Now())
			setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, h.clk.Now())
		} else {
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
		}
		// Pod status is a built-in subresource on the fake client — a
		// plain Update silently drops status changes.
		if err := h.c.Status().Update(h.ctx, pod); err != nil {
			h.t.Fatalf("kubelet update %s: %v", pod.Name, err)
		}
	}
}

func setPodCondition(pod *corev1.Pod, condType corev1.PodConditionType, status corev1.ConditionStatus, now time.Time) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == condType {
			pod.Status.Conditions[i].Status = status
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type: condType, Status: status, LastTransitionTime: metav1.NewTime(now),
	})
}

func (h *recoveryHarness) irKey() types.NamespacedName {
	return types.NamespacedName{Namespace: recoveryNS, Name: recoveryOwner + "-" + string(workload.ComponentEngine)}
}

func (h *recoveryHarness) irStatuses() []workload.InstanceStatus {
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

func (h *recoveryHarness) removeInstance() func(ctx context.Context, idx int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		ir := &v1beta1.InferenceReplica{}
		if err := h.c.Get(ctx, h.irKey(), ir); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		pos := -1
		for i, s := range ir.Status.InstanceStatuses {
			if s.Index == idx {
				pos = i
				break
			}
		}
		if pos == -1 {
			return false, nil
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses[:pos], ir.Status.InstanceStatuses[pos+1:]...)
		if err := h.c.Status().Update(ctx, ir); err != nil {
			return false, err
		}
		return true, nil
	}
}

// mutateInstance is the harness's status-write seam: the IR round-trip
// for every index, except those mutateErr rejects.
func (h *recoveryHarness) mutateInstance() func(context.Context, int32, func(*workload.InstanceStatus) bool) error {
	delegate := roundTripMutateInstance(h.c, h.isvc, workload.ComponentEngine)
	return func(ctx context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
		if err := h.mutateErr[idx]; err != nil {
			return err
		}
		return delegate(ctx, idx, mutate)
	}
}

func (h *recoveryHarness) buildInput() workload.ReconcileInput {
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
		MutateInstance:    h.mutateInstance(),
		RemoveInstance:    h.removeInstance(),
		UpdateRetryPolicy: &workload.RetryPolicy{MaxAttempts: 2, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2},
		StuckPodGrace:     30 * time.Second,
		Clock:             h.clk,
		WarnRetryHeld: func(rev string, attempts int32, reason string) {
			h.heldWarnings = append(h.heldWarnings, fmt.Sprintf("%s attempts=%d %s", rev, attempts, reason))
		},
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

// step runs one reconcile: kubelet convergence, fresh expectations
// (watch caught up), Reconcile, aggregate CurrentRevision, clock
// advance (the op's own requeue interval, or a 10s baseline). A
// Reconcile error fails the test.
func (h *recoveryHarness) step() {
	h.t.Helper()
	if _, err := h.stepResult(); err != nil {
		h.t.Fatalf("Reconcile: %v", err)
	}
}

// stepResult is step returning the pass's result and error, for tests
// that expect a pass to fail.
func (h *recoveryHarness) stepResult() (ctrl.Result, error) {
	h.t.Helper()
	h.kubelet()
	deps := workload.Deps{Client: h.c, APIReader: h.c, Expectations: workload.NewExpectations()}
	in := h.buildInput()
	plan, err := workload.BuildPlan(workload.ComponentEngine, h.desired, in.ObservedState)
	if err != nil {
		h.t.Fatalf("BuildPlan: %v", err)
	}
	res, err := workload.Reconcile(h.ctx, deps, in, plan, h.target)
	if workload.RolloutComplete(h.irStatuses(), h.target.Name) {
		h.currentRevision = h.target.Name
	}
	// A 30s floor keeps stuck-pod grace and Backoff windows crossing in
	// a handful of passes, and lets an 80-pass wedge run outlive the 30m
	// Operation deadline — proving the deadline backstop is also inert.
	advance := 30 * time.Second
	if res.RequeueAfter > advance {
		advance = res.RequeueAfter
	}
	h.clk.Step(advance)
	return res, err
}

// run steps until pred returns true, up to max iterations. Returns
// whether pred was ever satisfied.
func (h *recoveryHarness) run(max int, pred func() bool) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		if pred() {
			return true
		}
		h.step()
	}
	return pred()
}

// runWithInvariant is run with a per-step invariant check: inv is
// evaluated after every step (and before the first), so a transient
// violation mid-recovery fails the test even if the end state is fine.
func (h *recoveryHarness) runWithInvariant(max int, pred func() bool, inv func()) bool {
	h.t.Helper()
	for i := 0; i < max; i++ {
		inv()
		if pred() {
			return true
		}
		h.step()
	}
	inv()
	return pred()
}

// servingPods returns the live pods currently carrying the serving
// gate (in LB rotation).
func (h *recoveryHarness) servingPods() []*corev1.Pod {
	var out []*corev1.Pod
	for _, p := range h.livePods() {
		if podreadiness.IsServing(p) {
			out = append(out, p)
		}
	}
	return out
}

func (h *recoveryHarness) findBlock(rev string) *workload.RetryBlock {
	for i := range h.blocks {
		if h.blocks[i].TargetRevision == rev {
			return &h.blocks[i]
		}
	}
	return nil
}

func (h *recoveryHarness) livePods() []*corev1.Pod {
	h.t.Helper()
	pods := &corev1.PodList{}
	if err := h.c.List(h.ctx, pods, client.InNamespace(recoveryNS)); err != nil {
		h.t.Fatalf("list pods: %v", err)
	}
	out := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			out = append(out, &pods.Items[i])
		}
	}
	return out
}

func (h *recoveryHarness) badPods() []*corev1.Pod {
	var out []*corev1.Pod
	for _, p := range h.livePods() {
		if p.Spec.Containers[0].Image == badImage {
			out = append(out, p)
		}
	}
	return out
}

// converged reports full convergence on rev: exactly one InstanceStatus,
// Phase=Ready, RunningRevision=rev, no Operation, and zero bad-image
// pods left anywhere (wreckage cleaned).
func (h *recoveryHarness) converged(rev string) bool {
	sts := h.irStatuses()
	if len(sts) != 1 {
		return false
	}
	s := sts[0]
	if s.Phase != workload.InstancePhaseReady || s.RunningRevision != rev || s.Operation != nil {
		return false
	}
	return len(h.badPods()) == 0
}

func (h *recoveryHarness) dumpState(label string) {
	h.t.Logf("--- %s ---", label)
	for _, s := range h.irStatuses() {
		op := "nil"
		if s.Operation != nil {
			op = fmt.Sprintf("{type=%s step=%s target=%s surgeIdx=%v}", s.Operation.Type, s.Operation.Step, s.Operation.TargetRevision, s.Operation.SurgeIndex)
		}
		h.t.Logf("  instance %d: phase=%s running=%s target=%s op=%s", s.Index, s.Phase, s.RunningRevision, s.TargetRevision, op)
	}
	for _, b := range h.blocks {
		h.t.Logf("  block %s: state=%s attempts=%d", b.TargetRevision, b.State, b.AttemptsStarted)
	}
	for _, p := range h.livePods() {
		h.t.Logf("  pod %s image=%s rev=%s", p.Name, p.Spec.Containers[0].Image, p.Labels[query.LabelRevisionHash])
	}
}

// driveToReadyOnV1 converges the initial create on the good revision.
func (h *recoveryHarness) driveToReadyOnV1() {
	h.t.Helper()
	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("initial create")
		h.t.Fatalf("initial create never converged on v1")
	}
}

// driveToHeld publishes the bad revision and drives the real exhaustion
// flow until retryBlocks[bad]=Held.
func (h *recoveryHarness) driveToHeld() {
	h.t.Helper()
	h.setTarget(h.revBad, badImage)
	held := func() bool {
		b := h.findBlock(h.revBad.Name)
		return b != nil && b.State == workload.RetryBlockHeld
	}
	if !h.run(120, held) {
		h.dumpState("exhaustion")
		h.t.Fatalf("retry exhaustion never reached Held for %s", h.revBad.Name)
	}
	if len(h.heldWarnings) != 1 || !strings.HasPrefix(h.heldWarnings[0], h.revBad.Name) {
		h.t.Fatalf("WarnRetryHeld: got %v, want exactly one warning for %s", h.heldWarnings, h.revBad.Name)
	}
}

// settle runs a few extra passes so the post-Held state reaches its
// steady shape (in-flight abandon/dispose passes complete; nothing new
// starts because the Held gate denies the bad target).
func (h *recoveryHarness) settle(n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		h.step()
	}
}

func TestCorrectiveRecovery_Gang_InitialCreateRetarget(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.setTarget(h.revBad, badImage)
	h.step()

	initial := h.livePods()
	if len(initial) != 2 {
		t.Fatalf("initial Create: got %d pods want leader and worker", len(initial))
	}
	removedWorker := false
	for _, pod := range initial {
		if pod.Labels[query.LabelRunner] != "worker" {
			continue
		}
		if err := h.c.Delete(h.ctx, pod); err != nil {
			t.Fatalf("delete worker %s: %v", pod.Name, err)
		}
		removedWorker = true
		break
	}
	if !removedWorker {
		t.Fatal("initial Create did not produce a worker pod")
	}

	h.setTarget(h.revFixed, fixedImage)
	converged := h.runWithInvariant(80,
		func() bool { return h.converged(h.revFixed.Name) },
		func() {
			revisionsByInstance := map[string]map[string]struct{}{}
			for _, pod := range h.livePods() {
				index := pod.Labels[query.LabelInstanceIdx]
				if revisionsByInstance[index] == nil {
					revisionsByInstance[index] = map[string]struct{}{}
				}
				if hash := pod.Labels[query.LabelRevisionHash]; hash != "" {
					revisionsByInstance[index][hash] = struct{}{}
				}
			}
			for index, revisions := range revisionsByInstance {
				if len(revisions) > 1 {
					t.Fatalf("instance %s contains pods from multiple revisions: %v; statuses=%+v", index, revisions, h.irStatuses())
				}
			}
		})
	if !converged {
		h.dumpState("initial Create retarget wedged")
		t.Fatalf("gang initial Create did not converge after a corrective edit")
	}
}

// ---------------------------------------------------------------------------
// Roll-forward: corrective NEW revision.
// ---------------------------------------------------------------------------

// TestCorrectiveRecovery_SinglePod_RollForward reproduces the prod
// release shape for a single-pod instance: Held + Phase=Failed + the
// bad-revision pod still occupying the surge ordinal, then a corrective
// NEW revision. The rollout must converge on the corrective revision
// with zero manual intervention: reclassifyByRevisionHash
// (ops/update_surge.go) routes the dead pod — labeled a third revision,
// neither RunningRevision nor the pinned target — into the drain set,
// the stale-slot eviction clears the surge ordinal, and the fresh surge
// drives to the fixed revision. The serving source must
// stay in rotation throughout: the recovery is SurgeThenDrain, so
// capacity never drops to zero while the wreckage is cleaned.
func TestCorrectiveRecovery_SinglePod_RollForward(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (single-pod)")

	h.setTarget(h.revFixed, fixedImage)
	converged := h.runWithInvariant(80,
		func() bool { return h.converged(h.revFixed.Name) },
		func() {
			if len(h.servingPods()) == 0 {
				t.Fatalf("no pod serving mid-recovery: the cleanup pulled the healthy source out of rotation before the corrective surge was ready")
			}
		})
	if !converged {
		h.dumpState("roll-forward wedged (single-pod)")
		t.Fatalf("single-pod roll-forward after Held did not converge")
	}
}

// TestCorrectiveRecovery_Gang_RollForward pins gang recovery from the
// settled at-Held state (which the abandon path leaves CLEAN: source
// Ready on v1, marker removed, surge pods deleted).
func TestCorrectiveRecovery_Gang_RollForward(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (gang)")

	h.setTarget(h.revFixed, fixedImage)
	if !h.run(80, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("roll-forward wedged")
		t.Fatalf("gang roll-forward after Held did not converge")
	}
}

// TestCorrectiveRecovery_Gang_RollForward_MidWreckage pins the dirty
// timing: the corrective revision lands while the source is
// Failed-with-Operation toward the bad revision, the GangSurgeTarget
// marker still exists, and the crashed surge gang's pods are still
// live. The Failed continuation must dispatch, abandon the stale surge,
// and re-surge toward the corrective revision.
func TestCorrectiveRecovery_Gang_RollForward_MidWreckage(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()

	h.setTarget(h.revBad, badImage)
	if !h.run(60, func() bool { return h.gangMidWreckage() }) {
		h.dumpState("exhaustion")
		t.Fatalf("never reached the mid-wreckage state (source Failed-with-op + marker + live bad pods)")
	}
	h.dumpState("mid-wreckage (gang)")

	h.setTarget(h.revFixed, fixedImage)
	if !h.run(80, func() bool { return h.converged(h.revFixed.Name) }) {
		h.dumpState("roll-forward wedged")
		t.Fatalf("gang mid-wreckage roll-forward did not converge")
	}
}

// gangMidWreckage reports the dirty gang state: source instance
// Phase=Failed with a preserved gang-surge Update Operation, the surge
// marker present, and the bad gang's pods live.
func (h *recoveryHarness) gangMidWreckage() bool {
	var source, marker *workload.InstanceStatus
	sts := h.irStatuses()
	for i := range sts {
		s := &sts[i]
		if s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate && s.Operation.SurgeIndex != nil {
			source = s
		}
		if s.Operation != nil && s.Operation.Step == workload.UpdateStepGangSurgeTarget {
			marker = s
		}
	}
	return source != nil && source.Phase == workload.InstancePhaseFailed &&
		marker != nil && len(h.badPods()) > 0
}

// ---------------------------------------------------------------------------
// Roll-back: corrective target == the exact prior revision — zero
// revision distance, so the update trigger never fires and recovery is
// owned by the wreckage-cleanup path.
// ---------------------------------------------------------------------------

// TestCorrectiveRecovery_SinglePod_RollBack reproduces the undo path
// from the settled at-Held state: the operator reverts the spec so the
// target revision equals the instance's RunningRevision. Stock
// Deployments recover here for free; OMENative must too.
// The update trigger stays revision-diff-keyed and correctly declines
// (pinned below) — recovery arrives via Plan's wreckage scan
// (UpdateItem.CleanupOnly -> ops.CleanupWreckage), which deletes the
// dead surge pod without ever touching the serving source: the
// original v1 pod stays in rotation through the whole undo.
func TestCorrectiveRecovery_SinglePod_RollBack(t *testing.T) {
	h := newRecoveryHarness(t, false)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (single-pod)")

	// The healthy v1 pod that must keep serving through the undo.
	serving := h.servingPods()
	if len(serving) != 1 || serving[0].Spec.Containers[0].Image != goodImage {
		t.Fatalf("precondition: exactly the good v1 pod serving at Held, got %d serving", len(serving))
	}
	originalPod := serving[0].Name

	// Pin the trigger contract the fix deliberately preserves: the pure
	// evaluation still declines (RunningRevision == target fast-path) —
	// cleanup is Plan's wreckage scan, not a widened trigger.
	h.setTarget(h.revV1, goodImage)
	in := h.buildInput()
	plan, err := workload.BuildPlan(workload.ComponentEngine, h.desired, in.ObservedState)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	dec := workloadops.EvaluateUpdateTrigger(in, plan.Instances[0], h.target, h.desired.PodSpec, nil)
	if dec.Trigger || dec.AdoptRevision || dec.RetryAfter != 0 {
		t.Fatalf("expected the update trigger to keep declining (RunningRevision==target fast-path), got %+v", dec)
	}

	converged := h.runWithInvariant(60,
		func() bool { return h.converged(h.revV1.Name) },
		func() {
			for _, p := range h.servingPods() {
				if p.Name != originalPod {
					t.Fatalf("pod %s entered rotation during the undo; only the original %s may serve", p.Name, originalPod)
				}
			}
			if len(h.servingPods()) == 0 {
				t.Fatalf("the original pod left rotation during the undo (capacity outage)")
			}
		})
	if !converged {
		h.dumpState("roll-back wedged (single-pod)")
		t.Fatalf("single-pod roll-back after Held did not converge")
	}
	if got := h.servingPods(); len(got) != 1 || got[0].Name != originalPod {
		t.Fatalf("the original pod must still be the serving pod after the undo, got %v", got)
	}
}

// TestCorrectiveRecovery_Gang_RollBack_MidWreckage is the gang variant
// of the undo path at the dirty timing: the roll-back lands while the
// source is Failed-with-Operation toward the bad revision, the
// GangSurgeTarget marker is present, and the crashed replacement gang's
// pods are live. Convergence must be automatic: either the
// Failed continuation's abandon tears the wreckage down, or — once the
// source is re-adopted Ready and its operation cleared — the orphaned
// marker falls out of the plan's pinned set (marker-liveness invariant,
// plan.go updateSurgeInFlightIndices) and the scale-down pass reaps the
// dead gang.
func TestCorrectiveRecovery_Gang_RollBack_MidWreckage(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()

	h.setTarget(h.revBad, badImage)
	if !h.run(60, func() bool { return h.gangMidWreckage() }) {
		h.dumpState("exhaustion")
		t.Fatalf("never reached the mid-wreckage state")
	}
	h.dumpState("mid-wreckage (gang)")

	h.setTarget(h.revV1, goodImage)
	if !h.run(80, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("roll-back wedged (gang)")
		t.Fatalf("gang mid-wreckage roll-back did not converge")
	}
}

// TestCorrectiveRecovery_Gang_RollBack_PostHeld documents that the gang
// roll-back from the SETTLED at-Held state trivially converges on
// current main: the final abandon left the source Ready on v1 with no
// wreckage, so a target equal to v1 needs no work. The gang undo path
// is only broken at the dirty (mid-wreckage) timing.
func TestCorrectiveRecovery_Gang_RollBack_PostHeld(t *testing.T) {
	h := newRecoveryHarness(t, true)
	h.driveToReadyOnV1()
	h.driveToHeld()
	h.settle(5)
	h.dumpState("at Held (gang)")

	h.setTarget(h.revV1, goodImage)
	if !h.run(30, func() bool { return h.converged(h.revV1.Name) }) {
		h.dumpState("post-Held roll-back")
		t.Fatalf("gang roll-back from the clean at-Held state must converge trivially")
	}
}
