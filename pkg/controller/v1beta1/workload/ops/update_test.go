package ops

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Tests for the per-Instance Update state machine and its chooser /
// trigger / per-revision helpers.

// TestUpdate_NilClient pins the nil-client guard. Without it the
// dispatcher would panic in production when the controller-runtime
// client hasn't been wired yet.
func TestUpdate_NilClient(t *testing.T) {
	legacyResetExpectations(t)
	_, err := Update(context.Background(), workload.Deps{}, workload.ReconcileInput{},
		workload.ComponentPlan{Component: workload.ComponentEngine},
		workload.InstancePlan{Index: 0, Incarnation: 1},
		&appsv1.ControllerRevision{}, &corev1.PodSpec{})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

// TestUpdate_InPlace_RechecksServingBeforePatch pins the in-place
// pre-patch race contract: a concurrent writer (another controller,
// a pod-mutating webhook re-injecting a sidecar) could flip
// serving=True between the drain check passing and the image patch
// firing. The fix re-reads each pod immediately before patching and,
// on serving=True, re-marks NotServing and requeues without patching.
//
// Race simulation: a Get interceptor flips serving=True on the pod
// ONLY during the pre-patch re-read; the listPodsForInstance call
// uses the original snapshot, and the drain check reads
// EndpointSlices, not the pod — so the flip is observed exclusively
// by the pre-patch Get.
func TestUpdate_InPlace_RechecksServingBeforePatch(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(true),
			},
		},
	}
	// Pod: image v1, ContainersReady, NOT serving (Step 2 will skip it).
	pod := legacyPodAtIncarnation(isvc, 0, 1, true /* ready */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	target := legacyTargetSpecImage("llama:v2")
	// Slice claims the pod is drained — Step 3 drain check passes.
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, false /* drained */)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = discoveryv1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)

	// Get interceptor: when Step 4 re-reads the pod, hand it back with
	// serving=True. Only fires for *corev1.Pod Gets matching pod.Name.
	flipServingOnGet := func(ctx context.Context, cli client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if err := cli.Get(ctx, key, obj, opts...); err != nil {
			return err
		}
		if p, ok := obj.(*corev1.Pod); ok && p.Name == pod.Name {
			// Strip any existing serving condition and append a True
			// one — simulates a concurrent writer who removed all keys.
			out := make([]corev1.PodCondition, 0, len(p.Status.Conditions))
			for _, c := range p.Status.Conditions {
				if c.Type == "ome.io/serving" {
					continue
				}
				out = append(out, c)
			}
			out = append(out, corev1.PodCondition{
				Type:   "ome.io/serving",
				Status: corev1.ConditionTrue,
			})
			p.Status.Conditions = out
		}
		return nil
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir, pod, slice).
		WithInterceptorFuncs(interceptor.Funcs{Get: flipServingOnGet}).
		Build()

	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after detecting serving=True race")
	}

	// Image must NOT have been patched (recheck bailed before patch).
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got.Spec.Containers[0].Image != "llama:v1" {
		t.Errorf("image should still be v1 (no patch on serving pod); got %q", got.Spec.Containers[0].Image)
	}
}

// TestUpdate_InPlaceEligibleImageOnly_PatchesPodImage pins the basic
// happy-path in-place rollout: an image-only diff under
// InPlaceIfPossible advances by patching the pod's container image.
func TestUpdate_InPlaceEligibleImageOnly_PatchesPodImage(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(true),
			},
		},
	}
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, false /* drained */)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	for pass := 1; pass <= 2; pass++ {
		done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
		if err != nil {
			t.Fatalf("Update pass %d: %v", pass, err)
		}
		if done {
			t.Fatalf("pass %d returned done=true before kubelet rolled containers", pass)
		}
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got.Spec.Containers[0].Image != "llama:v2" {
		t.Errorf("image: got %q want llama:v2", got.Spec.Containers[0].Image)
	}
}

// TestUpdate_InPlace_RestampsRevisionHashLabel pins in-place revision-hash
// restamping: an in-place rollout must restamp the pod's
// ome.io/revision-hash label to the target revision. Without it the rolled
// pod keeps its old label, so per-revision Service routing / drain /
// stuck-pod detection (all keyed on the label) mis-classify it as the
// previous revision.
func TestUpdate_InPlace_RestampsRevisionHashLabel(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(true),
			},
		},
	}
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	// Pod starts on the legacy synthetic hash; the in-place roll must move it.
	if pod.Labels[query.LabelRevisionHash] != testRevisionHashLegacy {
		t.Fatalf("precondition: pod label got %q want %q", pod.Labels[query.LabelRevisionHash], testRevisionHashLegacy)
	}
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, false /* drained */)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	for pass := 1; pass <= 2; pass++ {
		if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target); err != nil {
			t.Fatalf("Update pass %d: %v", pass, err)
		}
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	wantHash := query.RevisionHashFromControllerRevisionName(tcr.Name)
	if wantHash == "" {
		t.Fatalf("target CR %q yielded empty revision hash", tcr.Name)
	}
	if got.Labels[query.LabelRevisionHash] != wantHash {
		t.Errorf("revision-hash label: got %q want %q (in-place must restamp)", got.Labels[query.LabelRevisionHash], wantHash)
	}
}

// TestUpdate_InPlaceConverges_MarksReadyWithRunningRevision pins the
// in-place terminator: when the pod is already on the target image,
// runtime-ready, and NOT yet serving (because a previous pass drained it),
// Update flips serving=True and then holds at the promote bar until kubelet
// folds that gate into PodReady; the pass that observes PodReady stamps
// Phase=Ready with the new RunningRevision.
func TestUpdate_InPlaceConverges_MarksReadyWithRunningRevision(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepInPlace,
		},
	}
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(true),
			},
		},
	}
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, true /* ready */, false /* not serving yet */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	// Kubelet has finished rolling the container — runtime image matches
	// spec image. Without this status entry the podRuntimeImagesMatch
	// gate would hold the rollout in the "patched spec, not yet rolled"
	// state.
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", Image: "llama:v2"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	legacyStampPodRevisionHash(t, c, pod, tcr.Name)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: the gate was only just written, so the pod is not PodReady yet")
	}

	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if !podreadiness.IsServing(got) {
		t.Errorf("serving should have been flipped True after in-place convergence")
	}

	legacyFoldServingIntoPodReady(t, c, pod, time.Now())
	input = legacyTestInput(isvc, c, workload.ComponentEngine)
	done, err = Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update after PodReady: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true: the pod is PodReady on the target image")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("Phase: got %q want Ready", s.Phase)
	}
	if s.RunningRevision != tcr.Name {
		t.Errorf("RunningRevision: got %q want %q", s.RunningRevision, tcr.Name)
	}
	if s.TargetRevision != "" {
		t.Errorf("TargetRevision should be cleared, got %q", s.TargetRevision)
	}
	if s.Operation != nil {
		t.Errorf("Operation should be nil, got %+v", s.Operation)
	}
}

// TestUpdate_InPlaceWaitsForRuntimeImageRoll pins the runtime-truth
// gate: spec.image == target but kubelet has not yet rolled the
// container — pod.Status.ContainerStatuses still reflects the old
// image. Update must NOT flip serving / mark Ready: doing so would
// expose stale runtime to traffic on the new revision pointer.
func TestUpdate_InPlaceWaitsForRuntimeImageRoll(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepInPlace,
		},
	}
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(true),
			},
		},
	}
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, true, false)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	// Container hasn't been rolled yet: runtime still on v1 even though
	// spec was just patched to v2.
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: runtime image hasn't rolled yet")
	}

	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if podreadiness.IsServing(got) {
		t.Errorf("serving must NOT flip True while runtime image still lags spec")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.RunningRevision == tcr.Name {
		t.Errorf("RunningRevision must NOT advance to target until runtime rolls")
	}
}

// TestUpdate_InPlace_InjectedSidecar_Converges is the end-to-end
// livelock guard: a pod already on the target image but carrying
// a webhook-injected sidecar (spec + status) absent from the target
// must converge to Ready rather than redeclaring an image mismatch
// every pass, patching nothing, and requeuing forever with the pod
// drained.
func TestUpdate_InPlace_InjectedSidecar_Converges(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepInPlace,
		},
	}
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(true),
			},
		},
	}
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, true, false)
	pod.Spec.Containers = []corev1.Container{
		{Name: "main", Image: "llama:v2"},
		{Name: "istio-proxy", Image: "istio/proxyv2:1.20"},
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "main", Image: "llama:v2"},
		{Name: "istio-proxy", Image: "istio/proxyv2:1.20"},
	}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	legacyStampPodRevisionHash(t, c, pod, tcr.Name)

	if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target); err != nil {
		t.Fatalf("Update: %v", err)
	}
	legacyFoldServingIntoPodReady(t, c, pod, time.Now())
	input = legacyTestInput(isvc, c, workload.ComponentEngine)
	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update after PodReady: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true: pod is on the target image; the sidecar must not block convergence")
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if !podreadiness.IsServing(got) {
		t.Errorf("serving must flip True after convergence")
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady || s.RunningRevision != tcr.Name {
		t.Errorf("status: got (phase=%q, rev=%q) want (Ready, %q)", s.Phase, s.RunningRevision, tcr.Name)
	}
}

// TestUpdate_InPlaceIfPossible_FallsThroughToRecreateOnBigDiff pins
// the chooser's "fall through" path: a non-image-only diff under
// InPlaceIfPossible routes to recreate (bumps Incarnation, etc.)
// rather than erroring.
func TestUpdate_InPlaceIfPossible_FallsThroughToRecreateOnBigDiff(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
		},
	}
	pod := legacyPodAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.Containers = []corev1.Container{
		{Name: "main", Image: "llama:v1", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}},
	}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true /* still Ready */)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	// Recorded running spec carries the env var; the target drops it.
	runningSpec := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v1", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}},
	}}
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, runningSpec)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: recreate path drains then deletes across multiple passes")
	}

	// Recreate path should have bumped Incarnation.
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (recreate bumped it)", s.Incarnation)
	}
}

// TestUpdate_InPlaceOnly_RejectsBigDiff pins the InPlaceOnly safety
// contract: a non-image diff produces a hard error rather than
// silently routing to recreate.
func TestUpdate_InPlaceOnly_RejectsBigDiff(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceOnly,
		},
	}
	pod := legacyPodAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.Containers = []corev1.Container{
		{Name: "main", Image: "llama:v1", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}},
	}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	runningSpec := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v1", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}},
	}}
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, runningSpec)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceOnly, nil)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	_, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err == nil {
		t.Fatal("expected error: InPlaceOnly strategy rejects non-image diff")
	}
}

// TestUpdate_StrategyChangeMidRollout_RecreateStillBumpsIncarnation pins
// the recreate idempotency cross-state: in-place wrote
// Phase=Updating+Operation{Update,InPlace,target=tcr} on a prior pass;
// then the operator flips UpdateStrategy to RecreatePod for the SAME
// target revision. The recreate path must still bump Incarnation — its
// idempotency guard requires Operation.Step=Drain, so the in-place
// state doesn't short-circuit the bump.
func TestUpdate_StrategyChangeMidRollout_RecreateStillBumpsIncarnation(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	// Pre-stamp the in-place state at the same target.
	ir2 := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: legacyIRName(isvc, workload.ComponentEngine)}, ir2); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	ir2.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	ir2.Status.InstanceStatuses[0].TargetRevision = tcr.Name
	ir2.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		Type:           v1beta1.InstanceOperationUpdate,
		Step:           workload.UpdateStepInPlace,
		TargetRevision: tcr.Name,
	}
	if err := c.Status().Update(context.Background(), ir2); err != nil {
		t.Fatalf("seed in-place status: %v", err)
	}

	// Now the strategy flips to RecreatePod.
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyRecreatePod,
		},
	}
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: recreate is multi-pass")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (recreate must bump even though in-place wrote first)", s.Incarnation)
	}
	if s.Operation == nil || s.Operation.Step != workload.UpdateStepDrain {
		t.Errorf("Operation.Step: got %+v want Step=Drain", s.Operation)
	}
}

// TestUpdate_NoRunningRevision_ForcesRecreate pins the safety contract
// when an Instance has no RunningRevision recorded (legacy or
// first-time update post-Create). chooseUpdateMode treats nil running
// spec as ineligible; even an image-only diff under InPlaceIfPossible
// must fall through to recreate so the rollout starts from a known
// recorded baseline.
func TestUpdate_NoRunningRevision_ForcesRecreate(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
		},
	}
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	// NOTE: deliberately not calling legacySeedRunningRevision — Instance
	// has no recorded baseline.
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: recreate is multi-pass")
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (recreate path bumped)", s.Incarnation)
	}
}

// TestUpdate_RecreatePod_BumpsIncarnationAndDrainsOld pins the
// explicit-recreate first-pass behavior: stamps Phase=Updating,
// bumps Incarnation, drains old pods.
func TestUpdate_RecreatePod_BumpsIncarnationAndDrainsOld(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyRecreatePod,
		},
	}
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: recreate is multi-pass")
	}

	// RecreatePod strategy is explicit recreate — Incarnation bumped to 2.
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2", s.Incarnation)
	}
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", s.Phase)
	}
	if s.TargetRevision != tcr.Name {
		t.Errorf("TargetRevision: got %q want %q", s.TargetRevision, tcr.Name)
	}
}

// TestUpdate_MultiPodGangSurges pins that an Instance with >1 desired pod
// under SurgeThenDrain now routes to the gang surge (gangSurgeUpdate),
// NOT recreate. The distinguishing signal is the source Incarnation: a
// recreate bumps it (1→2, same index), whereas a gang surge leaves the
// source untouched and brings up a replacement at a fresh index. The
// source is stamped Phase=Updating with an in-flight surge Operation.
func TestUpdate_MultiPodGangSurges(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategySurgeThenDrain,
		},
	}

	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	// Multi-pod plan (TotalPods()=2) under SurgeThenDrain → gang surge.
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	if plan.UpdateStrategy.Type != workload.UpdateStrategySurgeThenDrain {
		t.Fatalf("test setup: expected SurgeThenDrain strategy, got %q", plan.UpdateStrategy.Type)
	}
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: gang surge is multi-pass")
	}

	// Gang surge stamps the SOURCE Phase=Updating but must NOT bump its
	// Incarnation (that would be a recreate). The replacement gang comes
	// up at a fresh index instead.
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("Phase: got %q want Updating", s.Phase)
	}
	if s.Incarnation != 1 {
		t.Errorf("Incarnation: got %d want 1 (gang surge must NOT bump like recreate)", s.Incarnation)
	}
}

// TestGangSurgeUpdate_StampsReplacementAtNewIndex pins the novel gang
// surge behavior: the first pass stamps the source for a surge and
// creates a SECOND InstanceStatus at a fresh index (the replacement
// gang) — unlike single-pod surge, which stays at the same index and
// toggles ActiveOrdinal. The source stays Phase=Updating without an
// Incarnation bump (a recreate would bump it).
func TestGangSurgeUpdate_StampsReplacementAtNewIndex(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategySurgeThenDrain},
	}
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir, pod)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	done, err := gangSurgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("gangSurgeUpdate pass 1: %v", err)
	}
	if done {
		t.Fatalf("expected done=false on the surge-stamp pass")
	}

	statuses := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)
	if len(statuses) != 2 {
		t.Fatalf("expected 2 instance statuses (source + replacement at a new index), got %d: %+v", len(statuses), statuses)
	}
	var sawSource, sawReplacement bool
	for _, s := range statuses {
		if s.Index == 0 {
			sawSource = true
			if s.Phase != v1beta1.OMENativeInstanceUpdating {
				t.Errorf("source phase: got %q want Updating", s.Phase)
			}
			if s.Incarnation != 1 {
				t.Errorf("source incarnation: got %d want 1 (no recreate bump)", s.Incarnation)
			}
		} else {
			sawReplacement = true // a fresh index != 0
		}
	}
	if !sawSource || !sawReplacement {
		t.Fatalf("want source(0) + a fresh replacement index; got %+v", statuses)
	}
}

// TestChooseUpdateMode_MultiPodSurges is the chooser's per-mode
// contract: multi-pod SurgeThenDrain now resolves to updateModeSurge —
// gang surge (per-gang index allocation) is implemented, so the
// previous Surge→Recreate fallback is gone. In-place modes still fall
// back to recreate for gangs (see the InPlace test). Tested separately
// from the higher-level Update path because chooseUpdateMode is the
// single source of truth every Update call routes through.
func TestChooseUpdateMode_MultiPodSurges(t *testing.T) {
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v1"}}}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2"}}}

	mode, err := chooseUpdateModeForInstance(workload.UpdateStrategySurgeThenDrain, running, target, true /* multiPod */)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != updateModeSurge {
		t.Errorf("multi-pod surge: got mode %v want surge", mode)
	}

	// Sanity: single-pod path keeps surge mode.
	mode, err = chooseUpdateModeForInstance(workload.UpdateStrategySurgeThenDrain, running, target, false /* multiPod */)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != updateModeSurge {
		t.Errorf("single-pod surge: got mode %v want surge", mode)
	}
}

// TestChooseUpdateMode_MultiPodInPlaceFallsBackToRecreate pins that
// in-place modes route to recreate for multi-pod Components.
// inPlaceEligible compares only the leader's PodSpec, so an in-place
// patch on a worker-only spec change would leave the workers on the
// old image — split-brain.
func TestChooseUpdateMode_MultiPodInPlaceFallsBackToRecreate(t *testing.T) {
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v1"}}}
	imageOnlyTarget := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2"}}}
	envTarget := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2", Env: []corev1.EnvVar{{Name: "X"}}}}}

	// InPlaceIfPossible — eligible diff still goes recreate when multi-pod.
	mode, err := chooseUpdateModeForInstance(workload.UpdateStrategyInPlaceIfPossible, running, imageOnlyTarget, true)
	if err != nil {
		t.Fatalf("InPlaceIfPossible multipod eligible: %v", err)
	}
	if mode != updateModeRecreate {
		t.Errorf("InPlaceIfPossible multipod: got %v want recreate", mode)
	}

	// InPlaceOnly + eligible — still recreate for multi-pod, no error.
	mode, err = chooseUpdateModeForInstance(workload.UpdateStrategyInPlaceOnly, running, imageOnlyTarget, true)
	if err != nil {
		t.Fatalf("InPlaceOnly multipod eligible: %v", err)
	}
	if mode != updateModeRecreate {
		t.Errorf("InPlaceOnly multipod eligible: got %v want recreate", mode)
	}

	// InPlaceOnly + ineligible — single-pod errors, multi-pod recreates.
	if _, err := chooseUpdateModeForInstance(workload.UpdateStrategyInPlaceOnly, running, envTarget, false); err == nil {
		t.Errorf("single-pod InPlaceOnly ineligible: want error")
	}
	mode, err = chooseUpdateModeForInstance(workload.UpdateStrategyInPlaceOnly, running, envTarget, true)
	if err != nil {
		t.Fatalf("multi-pod InPlaceOnly ineligible: want recreate, got error: %v", err)
	}
	if mode != updateModeRecreate {
		t.Errorf("multi-pod InPlaceOnly ineligible: got %v want recreate", mode)
	}

	// Single-pod eligible path still in-places (the multi-pod override is
	// scoped — single-pod keeps full in-place semantics).
	mode, err = chooseUpdateModeForInstance(workload.UpdateStrategyInPlaceIfPossible, running, imageOnlyTarget, false)
	if err != nil {
		t.Fatalf("single-pod InPlaceIfPossible: %v", err)
	}
	if mode != updateModeInPlace {
		t.Errorf("single-pod InPlaceIfPossible: got %v want in-place", mode)
	}
}

// TestInPlaceEligible_OnlyImageDiff confirms image-only diff is
// eligible for in-place rollout.
func TestInPlaceEligible_OnlyImageDiff(t *testing.T) {
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}}}
	if !inPlaceEligible(running, target) {
		t.Errorf("image-only diff should be in-place eligible")
	}
}

// TestInPlaceEligible_InitContainerImageDiffRejected pins that
// init-container image diffs route to recreate. Init containers run
// once at pod creation; kubelet has no machinery to re-run them after
// an image patch.
func TestInPlaceEligible_InitContainerImageDiffRejected(t *testing.T) {
	running := &corev1.PodSpec{
		InitContainers: []corev1.Container{{Name: "init", Image: "init:v1"}},
		Containers:     []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	target := &corev1.PodSpec{
		InitContainers: []corev1.Container{{Name: "init", Image: "init:v2"}},
		Containers:     []corev1.Container{{Name: "main", Image: "llama:v1"}},
	}
	if inPlaceEligible(running, target) {
		t.Errorf("init-container image diff must force recreate (kubelet cannot re-run inits in place)")
	}
}

// TestInPlaceEligible_NonImageDiffRejected covers each non-image diff
// kind: env, command, volumes.
func TestInPlaceEligible_NonImageDiffRejected(t *testing.T) {
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}

	envDiff := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v2", Env: []corev1.EnvVar{{Name: "X", Value: "1"}}}},
	}
	if inPlaceEligible(running, envDiff) {
		t.Errorf("env diff should NOT be in-place eligible")
	}

	cmdDiff := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v2", Command: []string{"go"}}},
	}
	if inPlaceEligible(running, cmdDiff) {
		t.Errorf("command diff should NOT be in-place eligible")
	}

	volDiff := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}},
		Volumes:    []corev1.Volume{{Name: "extra"}},
	}
	if inPlaceEligible(running, volDiff) {
		t.Errorf("volume diff should NOT be in-place eligible")
	}
}

// TestInPlaceEligible_NilRunningOrTargetRejected pins that nil inputs
// are treated as ineligible. A nil running spec means "no recorded
// baseline" — force recreate.
func TestInPlaceEligible_NilRunningOrTargetRejected(t *testing.T) {
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}}}
	if inPlaceEligible(nil, target) {
		t.Errorf("nil running should not be claimed eligible")
	}
	running := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}}}
	if inPlaceEligible(running, nil) {
		t.Errorf("nil target should not be claimed eligible")
	}
}

// TestChooseUpdateMode is the chooser's full decision-matrix coverage
// across every (strategy, eligibility) pair. The bare chooser is
// pod-count-agnostic; multi-pod overrides live in
// TestChooseUpdateMode_MultiPod*.
func TestChooseUpdateMode(t *testing.T) {
	runningSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v1"}}}
	imageOnlyTarget := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2"}}}
	envTarget := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "v2", Env: []corev1.EnvVar{{Name: "X"}}}}}

	tests := []struct {
		name     string
		strategy workload.UpdateStrategyType
		running  *corev1.PodSpec
		target   *corev1.PodSpec
		wantMode updateMode
		wantErr  bool
	}{
		{"RecreatePod always recreates", workload.UpdateStrategyRecreatePod, runningSpec, imageOnlyTarget, updateModeRecreate, false},
		{"InPlaceIfPossible + eligible = in-place", workload.UpdateStrategyInPlaceIfPossible, runningSpec, imageOnlyTarget, updateModeInPlace, false},
		{"InPlaceIfPossible + ineligible = recreate", workload.UpdateStrategyInPlaceIfPossible, runningSpec, envTarget, updateModeRecreate, false},
		{"InPlaceOnly + eligible = in-place", workload.UpdateStrategyInPlaceOnly, runningSpec, imageOnlyTarget, updateModeInPlace, false},
		{"InPlaceOnly + ineligible = error", workload.UpdateStrategyInPlaceOnly, runningSpec, envTarget, 0, true},
		{"empty strategy defaults to surge (matches SurgeThenDrain default)", "", runningSpec, imageOnlyTarget, updateModeSurge, false},
		{"SurgeThenDrain routes to surge mode", workload.UpdateStrategySurgeThenDrain, runningSpec, imageOnlyTarget, updateModeSurge, false},
		{"unknown strategy = error", workload.UpdateStrategyType("Bogus"), runningSpec, imageOnlyTarget, 0, true},
		{"nil running forces recreate even on image-only diff (no baseline)", workload.UpdateStrategyInPlaceIfPossible, nil, imageOnlyTarget, updateModeRecreate, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := chooseUpdateMode(tt.strategy, tt.running, tt.target)
			if tt.wantErr {
				if err == nil {
					t.Errorf("want error, got mode=%v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantMode {
				t.Errorf("mode: got %v want %v", got, tt.wantMode)
			}
		})
	}
}

// TestDetectUpdateTrigger_PhaseUpdatingAlwaysTrue: an Instance already
// in Phase=Updating must keep returning true so the dispatcher resumes
// the in-flight Update on the next pass.
func TestDetectUpdateTrigger_PhaseUpdatingAlwaysTrue(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	target := legacyTargetSpecImage("llama:v1")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("Phase=Updating should keep returning true")
	}
}

// TestDetectUpdateTrigger_PhaseMigratingSuppressed: a source mid-
// migration must not start Update — Migrate owns the lifecycle until
// it terminates.
func TestDetectUpdateTrigger_PhaseMigratingSuppressed(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceMigrating
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	target := legacyTargetSpecImage("llama:v2")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Phase=Migrating must suppress Update — Migrate owns the lifecycle")
	}
}

// TestDetectUpdateTrigger_OperationMigrateSuppressed: surge mid-create
// carries Operation.Type=Migrate while Phase=Creating. The explicit
// Operation-type guard catches the post-promote race where a just-
// promoted surge briefly looks like a stale-revision Instance.
func TestDetectUpdateTrigger_OperationMigrateSuppressed(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		Type: v1beta1.InstanceOperationMigrate,
		Step: "CreatePods",
	}
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	target := legacyTargetSpecImage("llama:v2")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Operation.Type=Migrate must suppress Update")
	}
}

// TestDetectUpdateTrigger_FailedMigrateRowStaysClaimed: a pair row that
// escalated keeps its Migrate operation while it reads Failed, and the
// record still owns its end. The trigger reads the claim, not the owner,
// so the corrective-revision path that re-drives a plain Failed row does
// not start an Update over the migration; the wreckage scan leaves the
// row alone for the same reason.
func TestDetectUpdateTrigger_FailedMigrateRowStaysClaimed(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	st := ir.Status.InstanceStatuses
	st[0].Phase = v1beta1.OMENativeInstanceFailed
	st[0].RunningRevision = "llama-70b-engine-oldrev"
	st[0].Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationMigrate, Step: "CreatePods"}
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	target := legacyTargetSpecImage("llama:v2")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("a Failed row with a Migrate operation is the record's; no Update may start on it")
	}
	failed := &workload.InstanceStatus{
		Index: 0, Phase: workload.InstancePhaseFailed, RunningRevision: "llama-70b-engine-oldrev",
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationMigrate, Step: "CreatePods"},
	}
	if EvaluateWreckage(failed, tcr, nil) {
		t.Errorf("the wreckage scan must leave a migrate-claimed row alone")
	}
}

// TestDetectUpdateTrigger_GangSurgeTargetMarkerSuppressed: a gang
// surge-target marker (Op{Update, Step=GangSurgeTarget}) is driven by its
// SOURCE instance's gangSurgeUpdate, not as an independent target. Even
// when its running revision differs from the current target (a corrective
// edit moved the target mid-surge), the trigger must be suppressed — else
// the marker is mis-driven as a fresh surge source and corrupts the
// rollout (gang route).
func TestDetectUpdateTrigger_GangSurgeTargetMarkerSuppressed(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		Type: v1beta1.InstanceOperationUpdate,
		Step: "GangSurgeTarget",
	}
	// Marker still runs the old surge revision while the target moved on.
	ir.Status.InstanceStatuses[0].RunningRevision = "llama-70b-engine-oldrev"
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	target := legacyTargetSpecImage("llama:v2")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("gang surge-target marker must suppress Update — the source owns its lifecycle")
	}
}

// TestDetectUpdateTrigger_RunningRevisionMatchesTarget: Status's
// RunningRevision matches the target CR name — no update.
func TestDetectUpdateTrigger_RunningRevisionMatchesTarget(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	target := legacyTargetSpecImage("llama:v1")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	ir.Status.InstanceStatuses[0].RunningRevision = tcr.Name
	// Re-store with the running revision pinned.
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("RunningRevision == target should NOT trigger update")
	}
}

// TestDetectUpdateTrigger_PhaseFailedRetriggersOnMismatch: a Failed Instance (e.g.
// escalated after a bad rollout) must re-trigger toward a NEW target — otherwise a
// corrective revision never rolls and the Instance is wedged forever. Failed is
// treated like Ready for the revision comparison.
func TestDetectUpdateTrigger_PhaseFailedRetriggersOnMismatch(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	target := legacyTargetSpecImage("llama:v2")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	st := ir.Status.InstanceStatuses
	st[0].Phase = v1beta1.OMENativeInstanceFailed
	st[0].RunningRevision = "llama-70b-engine-oldrev" // != tcr.Name (the corrective target)
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("Phase=Failed with RunningRevision != target must re-trigger update (recovery)")
	}
}

// TestDetectUpdateTrigger_PodOnOtherRevisionTriggersUpdate: no
// RunningRevision recorded and the observed pod carries a different
// revision's hash — must trigger the ordinary roll.
func TestDetectUpdateTrigger_PodOnOtherRevisionTriggersUpdate(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	c := legacyNewFakeClient(t, isvc, ir, pod)
	priorCR := legacyEnsureTargetCR(t, c, isvc, legacyTargetSpecImage("llama:v1"))
	legacyStampPodRevisionHash(t, c, pod, priorCR.Name)
	tcr := legacyEnsureTargetCR(t, c, isvc, legacyTargetSpecImage("llama:v2"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("a pod on another revision should trigger update")
	}
}

// TestDetectUpdateTrigger_UnlabelledPodTriggersUpdate: a pod carrying no
// revision-hash label cannot be proven on the target, so it is rolled
// rather than adopted.
func TestDetectUpdateTrigger_UnlabelledPodTriggersUpdate(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	delete(pod.Labels, query.LabelRevisionHash)
	c := legacyNewFakeClient(t, isvc, ir, pod)
	tcr := legacyEnsureTargetCR(t, c, isvc, legacyTargetSpecImage("llama:v1"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("an unlabelled pod must not be adopted; it should trigger update")
	}
}

// TestDetectUpdateTrigger_TargetRevisionPodsBackfillRunningRevision:
// Status has no RunningRevision but the pods carry the target
// revision's hash. DetectUpdateTrigger must NOT trigger an update, and
// as a side effect should backfill RunningRevision so future reconciles
// take the cheap fast-path.
func TestDetectUpdateTrigger_TargetRevisionPodsBackfillRunningRevision(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	target := legacyTargetSpecImage("llama:v1")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	c := legacyNewFakeClient(t, isvc, ir, pod)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	legacyStampPodRevisionHash(t, c, pod, tcr.Name)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("pods on the target revision must NOT trigger update")
	}

	// Backfill should have happened.
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.RunningRevision != tcr.Name {
		t.Errorf("RunningRevision: got %q want %q (backfill)", s.RunningRevision, tcr.Name)
	}
}

// TestDetectUpdateTrigger_PhaseCreatingNotInterruptible: Phase=Creating
// blocks the Update trigger — Create owns the lifecycle until Ready.
func TestDetectUpdateTrigger_PhaseCreatingNotInterruptible(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceCreating
	c := legacyNewFakeClient(t, isvc, ir)
	target := legacyTargetSpecImage("llama:v2")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Phase=Creating must not be interruptible by Update")
	}
}

// TestPatchInstanceStatusReadyOnRevision_SkipsWriteWhenIdempotent pins
// the no-op contract: re-invoking with the same target on an
// already-ready Instance does NOT bump the ResourceVersion.
func TestPatchInstanceStatusReadyOnRevision_SkipsWriteWhenIdempotent(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision = "rev-abc"
	c := legacyNewFakeClient(t, isvc, ir)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	// Already in the target shape — write should be a no-op.
	if err := status.StampReadyOnRevision(context.Background(), input, 0, "rev-abc"); err != nil {
		t.Fatalf("patch: %v", err)
	}
}

// TestDrainServiceForPod_UsesPerRevisionRoutedService pins the per-
// revision *routed* Service name derivation. The headless Service
// sets PublishNotReadyAddresses=true, which makes kube-proxy publish
// Ready=true on its EndpointSlice regardless of the pod's actual
// Ready — so drain.IsPodDrained against the headless Service waits
// forever. The per-revision routed Service (created by the coordination
// layer with PublishNotReadyAddresses=false) reflects the controller-
// owned ome.io/serving gate correctly.
func TestDrainServiceForPod_UsesPerRevisionRoutedService(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "llama-70b-engine-0-default-0",
		Namespace: "prod",
		Labels:    map[string]string{"ome.io/revision-hash": "abcd1234"},
	}}
	input := workload.ReconcileInput{Key: workload.Key{OwnerName: "llama-70b", Component: workload.ComponentEngine}}
	plan := workload.ComponentPlan{Component: workload.ComponentEngine}
	got := drainServiceForPod(input, plan, pod)
	if got != "llama-70b-engine-rev-abcd1234" {
		t.Errorf("got %q want llama-70b-engine-rev-abcd1234", got)
	}
}

// TestDrainServiceForPod_EmptyWhenLabelMissing: pods that predate the
// revision-hash label return empty — caller skips the drain rather
// than blocking on a service it cannot identify.
func TestDrainServiceForPod_EmptyWhenLabelMissing(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-0-default-0", Namespace: "prod"}}
	input := workload.ReconcileInput{Key: workload.Key{OwnerName: "llama-70b", Component: workload.ComponentEngine}}
	plan := workload.ComponentPlan{Component: workload.ComponentEngine}
	if got := drainServiceForPod(input, plan, pod); got != "" {
		t.Errorf("got %q want empty", got)
	}
}

// TestUpdate_InPlaceAnnotationMutationRequiresFreshObservation verifies that
// an annotation-only rollout remains Updating for the pass that patches pod
// metadata. Ready promotion uses a fresh observation of the metadata and
// revision label; an alternate runtime image alias does not imply an image
// transition when the running and target revisions name the same image.
func TestUpdate_InPlaceAnnotationMutationRequiresFreshObservation(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(false),
			},
		},
	}
	spec := legacyTargetSpecImage("nginxinc/nginx-unprivileged:1.27-alpine")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "nginxinc/nginx-unprivileged:1.27-alpine")
	pod.Annotations = map[string]string{"release": "one"}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		Image: "mirror.example.com/library/nginx-unprivileged:1.27-alpine",
	}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevisionWithMeta(t, c, isvc, workload.ComponentEngine, 0, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"release": "one"}})
	target := legacyEnsureTargetCRWithMeta(t, c, isvc, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"release": "two"}})
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(false)})

	done, err := Update(context.Background(), legacyTestDeps(c), legacyTestInput(isvc, c, workload.ComponentEngine), plan, plan.Instances[0], target, spec)
	if err != nil {
		t.Fatalf("first Update: %v", err)
	}
	if done {
		t.Fatal("first Update returned done=true after issuing metadata patches")
	}

	gotPod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), gotPod); err != nil {
		t.Fatalf("get pod after first Update: %v", err)
	}
	if got := gotPod.Annotations["release"]; got != "two" {
		t.Errorf("release annotation: got %q want %q", got, "two")
	}
	wantHash := query.RevisionHashFromControllerRevisionName(target.Name)
	if got := gotPod.Labels[query.LabelRevisionHash]; got != wantHash {
		t.Errorf("revision hash: got %q want %q", got, wantHash)
	}
	firstStatus := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if firstStatus.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Errorf("phase after metadata patch: got %q want Updating", firstStatus.Phase)
	}
	if firstStatus.RunningRevision == target.Name {
		t.Errorf("RunningRevision advanced before a fresh pod observation")
	}

	done, err = Update(context.Background(), legacyTestDeps(c), legacyTestInput(isvc, c, workload.ComponentEngine), plan, plan.Instances[0], target, spec)
	if err != nil {
		t.Fatalf("second Update: %v", err)
	}
	if !done {
		t.Fatal("second Update returned done=false after observing converged metadata")
	}
	secondStatus := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if secondStatus.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("phase after fresh observation: got %q want Ready", secondStatus.Phase)
	}
	if secondStatus.RunningRevision != target.Name {
		t.Errorf("RunningRevision: got %q want %q", secondStatus.RunningRevision, target.Name)
	}
}

// TestUpdate_InPlaceAnnotationMutationRestoresServingOnFreshObservation
// verifies that a drained pod remains out of rotation during the metadata
// mutation pass and returns to service on the converged pass.
func TestUpdate_InPlaceAnnotationMutationRestoresServingOnFreshObservation(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(true),
			},
		},
	}
	spec := legacyTargetSpecImage("llama:v1")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Annotations = map[string]string{"release": "one"}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", Image: "llama:v1"}}
	serviceName := query.PerRevisionServiceName(isvc.Name, workload.ComponentEngine, testRevisionHashLegacy)
	slice := legacySliceWithEndpoint(isvc.Namespace, "engine-revision", serviceName, pod, false)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	legacySeedRunningRevisionWithMeta(t, c, isvc, workload.ComponentEngine, 0, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"release": "one"}})
	target := legacyEnsureTargetCRWithMeta(t, c, isvc, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"release": "two"}})
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})

	done, err := Update(context.Background(), legacyTestDeps(c), legacyTestInput(isvc, c, workload.ComponentEngine), plan, plan.Instances[0], target, spec)
	if err != nil {
		t.Fatalf("first Update: %v", err)
	}
	if done {
		t.Fatal("first Update returned done=true after issuing metadata patches")
	}
	gotPod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), gotPod); err != nil {
		t.Fatalf("get pod after first Update: %v", err)
	}
	if podreadiness.IsServing(gotPod) {
		t.Error("pod returned to service in the metadata mutation pass")
	}

	done, err = Update(context.Background(), legacyTestDeps(c), legacyTestInput(isvc, c, workload.ComponentEngine), plan, plan.Instances[0], target, spec)
	if err != nil {
		t.Fatalf("second Update: %v", err)
	}
	if !done {
		t.Fatal("second Update returned done=false after observing converged metadata")
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), gotPod); err != nil {
		t.Fatalf("get pod after second Update: %v", err)
	}
	if !podreadiness.IsServing(gotPod) {
		t.Error("pod did not return to service after fresh convergence observation")
	}
	status := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if status.Phase != v1beta1.OMENativeInstanceReady || status.RunningRevision != target.Name {
		t.Errorf("status after convergence: phase=%q runningRevision=%q", status.Phase, status.RunningRevision)
	}
}

// TestUpdate_InPlaceUpdate_PropagatesAnnotationsToPod pins annotation
// propagation: after an in-place pass, the pod's metadata.annotations
// contains BOTH the previously-authored value AND the new annotation
// added in the target spec. Without the propagation, the new annotation
// would land on the ControllerRevision but never reach the pod.
func TestUpdate_InPlaceUpdate_PropagatesAnnotationsToPod(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			// markNotReady=false so we don't need to wire EndpointSlice
			// drain fixtures — annotation propagation is orthogonal.
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(false),
			},
		},
	}
	// Pod carries the previous-revision annotation set, plus a foreign
	// annotation injected by a hypothetical webhook. The webhook
	// annotation MUST survive.
	spec := legacyTargetSpecImage("llama:v1")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Annotations = map[string]string{
		"a":                 "1",
		"linkerd.io/inject": "enabled", // webhook-set, foreign
	}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	// Running revision encodes the OMENative-owned annotation set {a:1}.
	legacySeedRunningRevisionWithMeta(t, c, isvc, workload.ComponentEngine, 0, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1"}})
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(false)})
	// Target revision: identical spec, new annotation set {a:1, b:2}.
	target := legacyEnsureTargetCRWithMeta(t, c, isvc, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1", "b": "2"}})

	_, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target, spec)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if v := got.Annotations["a"]; v != "1" {
		t.Errorf("annotation 'a': got %q want \"1\" (preserved across in-place)", v)
	}
	if v := got.Annotations["b"]; v != "2" {
		t.Errorf("annotation 'b': got %q want \"2\" — new spec annotation never reached pod", v)
	}
	if v := got.Annotations["linkerd.io/inject"]; v != "enabled" {
		t.Errorf("foreign annotation linkerd.io/inject: got %q want \"enabled\" (must NOT be clobbered by in-place patch)", v)
	}
}

// TestUpdate_InPlaceUpdate_RemovesAnnotationDroppedFromSpec pins the
// removal half of the annotation-propagation design choice. When the operator removes a
// key from spec.{component}.annotations that the previous revision
// authored, the in-place patch removes it from the pod too. The
// scoping rule — "only delete keys the previous revision once
// authored" — protects foreign annotations from being collateral
// damage on every spec edit.
func TestUpdate_InPlaceUpdate_RemovesAnnotationDroppedFromSpec(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(false),
			},
		},
	}
	spec := legacyTargetSpecImage("llama:v1")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Annotations = map[string]string{
		"a":                 "1",
		"b":                 "2",
		"linkerd.io/inject": "enabled",
	}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	// Previous revision authored {a, b}.
	legacySeedRunningRevisionWithMeta(t, c, isvc, workload.ComponentEngine, 0, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1", "b": "2"}})
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(false)})
	// Target revision: user removed 'b' from spec.
	target := legacyEnsureTargetCRWithMeta(t, c, isvc, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1"}})

	if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target, spec); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if v := got.Annotations["a"]; v != "1" {
		t.Errorf("annotation 'a': got %q want \"1\" (kept across in-place)", v)
	}
	if _, present := got.Annotations["b"]; present {
		t.Errorf("annotation 'b' still on pod (got %q); user removed it from spec — in-place should drop it", got.Annotations["b"])
	}
	if v := got.Annotations["linkerd.io/inject"]; v != "enabled" {
		t.Errorf("foreign annotation linkerd.io/inject: got %q want \"enabled\" (must NOT be deleted)", v)
	}
}

// TestUpdate_InPlaceUpdate_LifecycleAnnotationFilterStillWorks pins
// the existing TemplateMeta lifecycle filter behavior at the workload
// layer: lifecycle annotations the operator writes on the ISVC must
// NOT participate in the revision hash and must NOT leak onto the
// pod via the in-place annotation patch (since the CR's PodMeta never
// carries them in the first place).
func TestUpdate_InPlaceUpdate_LifecycleAnnotationFilterStillWorks(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Annotations = map[string]string{
		"ome.io/rollout-paused": "true",
	}
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &v1beta1.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: legacyBoolPtr(false),
			},
		},
	}
	spec := legacyTargetSpecImage("llama:v1")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Annotations = map[string]string{"a": "1"}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	// Running revision's PodMeta has {a:1}; the lifecycle annotation
	// "ome.io/rollout-paused" is intentionally NOT included here
	// because the production reconciler calls revision.TemplateMeta
	// which strips it. Simulate the same in the fixture.
	legacySeedRunningRevisionWithMeta(t, c, isvc, workload.ComponentEngine, 0, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1"}})
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(false)})
	// Target adds {b:2}; lifecycle annotation is again stripped from PodMeta.
	target := legacyEnsureTargetCRWithMeta(t, c, isvc, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"a": "1", "b": "2"}})

	if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target, spec); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if v := got.Annotations["b"]; v != "2" {
		t.Errorf("annotation 'b': got %q want \"2\" (in-place propagation regressed)", v)
	}
	if _, present := got.Annotations["ome.io/rollout-paused"]; present {
		t.Errorf("lifecycle annotation 'ome.io/rollout-paused' leaked onto pod (got %q); the TemplateMeta filter must keep it out of the CR's PodMeta and therefore off the pod", got.Annotations["ome.io/rollout-paused"])
	}
}

// Keep imports live in case a helper above grows / shrinks: fmt is
// reachable through the inline format strings in test assertions; query
// is reachable through legacy_test_helpers; metav1 is reachable through
// the multiple metav1.NewTime / metav1.ObjectMeta usages above.
var (
	_ = fmt.Sprint
	_ = query.LabelInstanceIdx
)

// seedRunningRevisionOnStatus sets InstanceStatus[idx].RunningRevision on the
// InferenceReplica (the source of truth) so legacyTestInput projects it into
// ObservedState (where recreateRevisionCause reads the "from" revision).
func seedRunningRevisionOnStatus(c client.Client, isvc *v1beta1.InferenceService, idx int32, rev string) {
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: legacyIRName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		return
	}
	for i := range ir.Status.InstanceStatuses {
		if ir.Status.InstanceStatuses[i].Index == idx {
			ir.Status.InstanceStatuses[i].RunningRevision = rev
		}
	}
	_ = c.Status().Update(context.Background(), ir)
}

// A revision-roll recreate must record a "revision <from> -> <to>" cause
// on Operation.Reason so the roll is distinguishable in status from a
// pod-failure Restart (which records a termination reason). This is the
// debuggability surface for "why did this gang recreate".
func TestRecreateUpdate_StampsRevisionCauseOnOperationReason(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	seedRunningRevisionOnStatus(c, isvc, 0, "llama-70b-engine-oldrev")

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target); err != nil {
		t.Fatalf("Update: %v", err)
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil {
		t.Fatalf("Operation: nil, want recreate Update operation")
	}
	if s.Operation.Type != v1beta1.InstanceOperationUpdate {
		t.Errorf("Operation.Type: got %q want Update", s.Operation.Type)
	}
	want := "revision llama-70b-engine-oldrev -> " + tcr.Name
	if s.Operation.Reason != want {
		t.Errorf("Operation.Reason: got %q want %q", s.Operation.Reason, want)
	}
}

// The recreate's first-pass event must name the from->to revision so an
// operator watching `kubectl describe` sees the rollout cause even after
// the old pods are drained.
func TestRecreateUpdate_EmitsFromToRevisionEvent(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	target := legacyTargetSpecImage("llama:v2")
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, true)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	seedRunningRevisionOnStatus(c, isvc, 0, "llama-70b-engine-oldrev")

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	rec := record.NewFakeRecorder(16)
	deps := legacyTestDeps(c)
	deps.Recorder = rec

	if _, err := Update(context.Background(), deps, input, plan, plan.Instances[0], tcr, target); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got := drainEvents(rec)
	if !anyContains(got, string(workload.EventReasonRecreateUpdateStarted)) {
		t.Fatalf("no RecreateUpdateStarted event; got %v", got)
	}
	if !anyContains(got, "revision llama-70b-engine-oldrev -> "+tcr.Name) {
		t.Errorf("recreate event must name the from->to revision; got %v", got)
	}
}

// drainEvents pulls all buffered events off a FakeRecorder without blocking.
func drainEvents(rec *record.FakeRecorder) []string {
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

func anyContains(events []string, sub string) bool {
	for _, e := range events {
		if strings.Contains(e, sub) {
			return true
		}
	}
	return false
}

// RetryBlock gate tests: DetectUpdateTrigger consults the
// persisted RetryBlock for the CURRENT target revision before firing a
// FRESH trigger. Held / RetryInProgress / not-yet-due Backoff deny; a
// due Backoff allows WITHOUT flipping state — the RetryInProgress flip
// happens at attempt-stamp time (status.RetryBlockAttemptStarted), after
// the dispatcher's budget/coordination gates admit the start. A block
// for a DIFFERENT target revision is a different RetrySubject and never
// gates, and a Failed-continuation (teardown/abandon of a failed
// candidate) is exempt.

// retryBlockCall records one MutateRetryBlock invocation.
type retryBlockCall struct {
	rev         string
	disposition workload.RetryBlockDisposition
	block       workload.RetryBlock
}

// retryGateFixture builds the steady-state from which an Update trigger
// fires absent a RetryBlock: Instance 0 Ready on an OLD revision while
// the target CR captures a different image. MutateRetryBlock is a
// recorder that snapshots (rev, disposition, mutated block).
func retryGateFixture(t *testing.T, t0 time.Time) (*workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *[]retryBlockCall, client.Client, *v1beta1.InferenceService) {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	c := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, c, isvc, legacyTargetSpecImage("llama:v2"))
	// Ready on a NON-target revision → the fast-path fires absent a block.
	ir.Status.InstanceStatuses[0].RunningRevision = "llama-70b-engine-oldrev"
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(t0)
	calls := &[]retryBlockCall{}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		var b workload.RetryBlock
		if existing := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev); existing != nil {
			b = *existing
		} else {
			b = workload.RetryBlock{TargetRevision: rev}
		}
		d := mutate(&b)
		*calls = append(*calls, retryBlockCall{rev: rev, disposition: d, block: b})
		return nil
	}
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)
	return &input, plan, tcr, calls, c, isvc
}

// TestDetectUpdate_RetryBlockHeld_Denies: a Held block for the current
// target denies the trigger with no wake-up and no writes of any kind.
func TestDetectUpdate_RetryBlockHeld_Denies(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc := retryGateFixture(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockHeld},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Held block for the current target must deny the trigger")
	}
	if retryAfter != 0 {
		t.Errorf("Held is not time-bounded: retryAfter got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock must not be called on Held denial: %d calls", len(*calls))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("instance status mutated on denial: phase got %q want %q", s.Phase, v1beta1.OMENativeInstanceReady)
	}
}

// TestDetectUpdate_RetryBlockBackoffNotDue_DeniesWithRequeue: a Backoff
// block whose NextRetryAt is in the future denies AND reports exactly
// when to re-evaluate (fake clock → exact remaining interval).
func TestDetectUpdate_RetryBlockBackoffNotDue_DeniesWithRequeue(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	next := metav1.NewTime(t0.Add(37 * time.Second))
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, NextRetryAt: &next},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("not-yet-due Backoff must deny the trigger")
	}
	if retryAfter != 37*time.Second {
		t.Errorf("retryAfter: got %v want exactly 37s", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock must not be called before NextRetryAt: %d calls", len(*calls))
	}
}

// TestDetectUpdate_RetryBlockDue_AllowsWithoutFlip: a due Backoff block
// lets the trigger fire but the GATE records nothing — the
// RetryInProgress flip belongs to attempt-stamp time, after the
// dispatcher budgets admit the start.
func TestDetectUpdate_RetryBlockDue_AllowsWithoutFlip(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	next := metav1.NewTime(t0.Add(-1 * time.Second))
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &next},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("due Backoff must allow the trigger")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0 on allowed fire", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("the gate must not flip state (attempt-stamp time owns the flip): %d calls", len(*calls))
	}
}

// TestDetectUpdate_RetryBlockDueBoundaryExact: at exactly NextRetryAt
// the block is due — now.Before(next) is false at equality.
func TestDetectUpdate_RetryBlockDueBoundaryExact(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	next := metav1.NewTime(t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, NextRetryAt: &next},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("block at exactly NextRetryAt is due — trigger must fire")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0 at the due boundary", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("gate must not write at the due boundary: %d calls", len(*calls))
	}
}

// TestDetectUpdate_RetryBlockInProgress_DeniesSecondAttempt: while an
// authorized attempt is in flight the gate denies any further fresh
// trigger — exactly-one-attempt semantics.
func TestDetectUpdate_RetryBlockInProgress_DeniesSecondAttempt(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockRetryInProgress, AttemptsStarted: 1},
	}
	// A LIVE authorization: some Instance carries an in-flight Update
	// Operation at the target revision.
	input.ObservedState.InstanceStatuses = append(input.ObservedState.InstanceStatuses, workload.InstanceStatus{
		Index: 7, Phase: workload.InstancePhaseUpdating,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, TargetRevision: tcr.Name},
	})

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("RetryInProgress with a live in-flight attempt must deny a second attempt")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock must not be called on RetryInProgress denial: %d calls", len(*calls))
	}
}

// TestDetectUpdate_RetryBlockInProgressLeaked_SelfHeals: RetryInProgress
// with NO in-flight Update attempt at the revision (superseded surge,
// scale-down, crash) is a leaked authorization — the gate treats it as
// due so a later rollback to that revision is not silently denied
// forever.
func TestDetectUpdate_RetryBlockInProgressLeaked_SelfHeals(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockRetryInProgress, AttemptsStarted: 1},
	}
	// No instance carries an in-flight Update Operation at tcr.Name.

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("leaked RetryInProgress (no in-flight attempt) must self-heal and allow the trigger")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("gate must not write on self-heal (stamp re-confirms): %d calls", len(*calls))
	}
}

// TestDetectUpdate_NewRevisionPassesGate: a block for a DIFFERENT
// (older) target revision never gates the new target: a corrective
// revision must roll even when the prior revision Held.
func TestDetectUpdate_NewRevisionPassesGate(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: "some-OTHER-rev", State: workload.RetryBlockHeld},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("a Held block for a different revision must NOT gate the new target")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock must not be called for an unrelated block: %d calls", len(*calls))
	}
}

// TestDetectUpdate_FailedContinuationPassesGate: Phase=Failed with an
// in-flight Update Operation is a CONTINUATION (teardown/abandon of the
// failed candidate) and must proceed regardless of the block — a Held
// block must not freeze the candidate gang mid-teardown. Mirrors the dispatcher's startingFresh carve-out.
func TestDetectUpdate_FailedContinuationPassesGate(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, _ := retryGateFixture(t, t0)
	input.ObservedState.InstanceStatuses[0].Phase = workload.InstancePhaseFailed
	input.ObservedState.InstanceStatuses[0].Operation = &workload.InstanceOperation{
		ID:             "update-0-1",
		Type:           workload.InstanceOperationUpdate,
		Step:           workload.UpdateStepSurge,
		TargetRevision: tcr.Name,
	}
	input.ObservedState.RetryBlocks = []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockHeld},
	}

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("Failed-continuation must pass the gate even when Held")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 0 {
		t.Errorf("gate must not write on the continuation exemption: %d calls", len(*calls))
	}
}

// TestPatchSurgingForUpdate_FlipsBackoffOnAttemptStart: the attempt-
// stamp helper owns the RetryInProgress flip — an existing Backoff
// block flips (exactly one Persist) when the fresh Update Operation is
// stamped, and NOTHING is recorded when no block exists (a fresh start
// with no prior failure needs no block).
func TestPatchSurgingForUpdate_FlipsBackoffOnAttemptStart(t *testing.T) {
	t.Run("existing Backoff block flips to RetryInProgress", func(t *testing.T) {
		t0 := time.Now()
		input, _, tcr, calls, _, _ := retryGateFixture(t, t0)
		next := metav1.NewTime(t0.Add(-1 * time.Second))
		input.ObservedState.RetryBlocks = []workload.RetryBlock{
			{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &next},
		}

		if err := status.StampSurging(context.Background(), *input, 0, tcr.Name, workload.UpdateStrategySurgeThenDrain, 30*time.Minute); err != nil {
			t.Fatalf("stamp: %v", err)
		}
		if len(*calls) != 1 {
			t.Fatalf("MutateRetryBlock calls: got %d want exactly 1", len(*calls))
		}
		call := (*calls)[0]
		if call.rev != tcr.Name {
			t.Errorf("mutate rev: got %q want %q", call.rev, tcr.Name)
		}
		if call.disposition != workload.RetryBlockPersist {
			t.Errorf("disposition: got %v want Persist", call.disposition)
		}
		if call.block.State != workload.RetryBlockRetryInProgress {
			t.Errorf("mutated state: got %q want %q", call.block.State, workload.RetryBlockRetryInProgress)
		}
	})

	t.Run("no block records nothing", func(t *testing.T) {
		t0 := time.Now()
		input, _, tcr, calls, _, _ := retryGateFixture(t, t0)

		if err := status.StampSurging(context.Background(), *input, 0, tcr.Name, workload.UpdateStrategySurgeThenDrain, 30*time.Minute); err != nil {
			t.Fatalf("stamp: %v", err)
		}
		for _, call := range *calls {
			if call.disposition != workload.RetryBlockUnchanged {
				t.Errorf("no-block start must persist nothing: disposition got %v want Unchanged", call.disposition)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// RetryBlock writer tests: recordUpdateFailureInRetryBlock counts
// attempts per WAVE — an existing Backoff block means this wave
// already recorded, so only the evidence refreshes. Policy nil (unconfigured)
// or exhausted → Held + WarnRetryHeld exactly once at the transition.
// These drive workload-caused waves (the charged arm); the uncharged,
// environment-caused arm is pinned in the types package and the gang
// abandon tests.
// ---------------------------------------------------------------------------

// retryHeldWarning records one WarnRetryHeld invocation.
type retryHeldWarning struct {
	rev      string
	attempts int32
	reason   string
}

// retryWriterInput builds the minimal ReconcileInput the writer needs: a
// fake clock, the recording MutateRetryBlock closure backed by
// ObservedState.RetryBlocks, an optional policy, and a WarnRetryHeld
// recorder. Same recording-closure pattern as retryGateFixture, without
// the fake-client scaffolding the pure writer doesn't touch.
func retryWriterInput(t0 time.Time, existing []workload.RetryBlock, policy *workload.RetryPolicy) (*workload.ReconcileInput, *[]retryBlockCall, *[]retryHeldWarning) {
	input := &workload.ReconcileInput{
		Clock:             clocktesting.NewFakeClock(t0),
		UpdateRetryPolicy: policy,
	}
	input.ObservedState.RetryBlocks = existing
	calls := &[]retryBlockCall{}
	warns := &[]retryHeldWarning{}
	input.WarnRetryHeld = func(rev string, attempts int32, reason string) {
		*warns = append(*warns, retryHeldWarning{rev: rev, attempts: attempts, reason: reason})
	}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		var b workload.RetryBlock
		if existing := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev); existing != nil {
			b = *existing
		} else {
			b = workload.RetryBlock{TargetRevision: rev}
		}
		d := mutate(&b)
		*calls = append(*calls, retryBlockCall{rev: rev, disposition: d, block: b})
		return nil
	}
	return input, calls, warns
}

// retryTestPolicy is the canonical test policy: 3 attempts, 1m initial
// delay, 30m cap, multiplier 2.
func retryTestPolicy() *workload.RetryPolicy {
	return &workload.RetryPolicy{MaxAttempts: 3, InitialDelay: time.Minute, MaxDelay: 30 * time.Minute, Multiplier: 2}
}

// TestRecordUpdateFailure_FirstFailureBacksOff: (a) first same-target
// failure creates the block — AttemptsStarted=1, Backoff, persisted
// NextRetryAt = now + InitialDelay, both failure timestamps stamped.
func TestRecordUpdateFailure_FirstFailureBacksOff(t *testing.T) {
	t0 := time.Now()
	input, calls, warns := retryWriterInput(t0, nil, retryTestPolicy())

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "ImagePullBackOff", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0]
	if b.rev != "rev-bad" || b.disposition != workload.RetryBlockPersist {
		t.Errorf("call: got (rev=%q, disposition=%v) want (rev-bad, Persist)", b.rev, b.disposition)
	}
	if b.block.State != workload.RetryBlockBackoff {
		t.Errorf("state: got %q want Backoff", b.block.State)
	}
	if b.block.AttemptsStarted != 1 {
		t.Errorf("AttemptsStarted: got %d want 1", b.block.AttemptsStarted)
	}
	if b.block.NextRetryAt == nil || !b.block.NextRetryAt.Time.Equal(t0.Add(time.Minute)) {
		t.Errorf("NextRetryAt: got %v want %v", b.block.NextRetryAt, t0.Add(time.Minute))
	}
	if b.block.FirstFailureAt == nil || !b.block.FirstFailureAt.Time.Equal(t0) {
		t.Errorf("FirstFailureAt: got %v want %v", b.block.FirstFailureAt, t0)
	}
	if b.block.LastFailureAt == nil || !b.block.LastFailureAt.Time.Equal(t0) {
		t.Errorf("LastFailureAt: got %v want %v", b.block.LastFailureAt, t0)
	}
	if b.block.Reason != "ImagePullBackOff" {
		t.Errorf("Reason: got %q want ImagePullBackOff", b.block.Reason)
	}
	if len(*warns) != 0 {
		t.Errorf("WarnRetryHeld: got %d calls want 0 (attempts remain)", len(*warns))
	}
}

// TestRecordUpdateFailure_SecondWaveCounts: (b) the authorized retry
// (block RetryInProgress) failed — counts as a new wave: AttemptsStarted
// 1→2, back to Backoff with NextRetryAt = now + InitialDelay*Multiplier,
// FirstFailureAt preserved.
func TestRecordUpdateFailure_SecondWaveCounts(t *testing.T) {
	t0 := time.Now()
	first := metav1.NewTime(t0.Add(-10 * time.Minute))
	input, calls, warns := retryWriterInput(t0, []workload.RetryBlock{{
		TargetRevision:  "rev-bad",
		State:           workload.RetryBlockRetryInProgress,
		AttemptsStarted: 1,
		FirstFailureAt:  &first,
		Reason:          "old evidence",
	}}, retryTestPolicy())

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "still ImagePullBackOff", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0].block
	if b.AttemptsStarted != 2 {
		t.Errorf("AttemptsStarted: got %d want 2 (RetryInProgress failure counts the wave)", b.AttemptsStarted)
	}
	if b.State != workload.RetryBlockBackoff {
		t.Errorf("state: got %q want Backoff", b.State)
	}
	if b.NextRetryAt == nil || !b.NextRetryAt.Time.Equal(t0.Add(2*time.Minute)) {
		t.Errorf("NextRetryAt: got %v want %v (1m * 2^1)", b.NextRetryAt, t0.Add(2*time.Minute))
	}
	if b.FirstFailureAt == nil || !b.FirstFailureAt.Time.Equal(first.Time) {
		t.Errorf("FirstFailureAt: got %v want preserved %v", b.FirstFailureAt, first.Time)
	}
	if b.Reason != "still ImagePullBackOff" {
		t.Errorf("Reason: got %q want refreshed", b.Reason)
	}
	if len(*warns) != 0 {
		t.Errorf("WarnRetryHeld: got %d calls want 0", len(*warns))
	}
}

// TestRecordUpdateFailure_SameWaveRefreshOnly: (c) a sibling instance's
// failure in the SAME wave finds the block already Backoff — evidence
// refresh only: no increment, no NextRetryAt recompute.
func TestRecordUpdateFailure_SameWaveRefreshOnly(t *testing.T) {
	t0 := time.Now()
	next := metav1.NewTime(t0.Add(-30 * time.Second)) // set by the wave's first failure
	first := metav1.NewTime(t0.Add(-90 * time.Second))
	input, calls, warns := retryWriterInput(t0, []workload.RetryBlock{{
		TargetRevision:  "rev-bad",
		State:           workload.RetryBlockBackoff,
		AttemptsStarted: 1,
		NextRetryAt:     &next,
		FirstFailureAt:  &first,
		Reason:          "first instance failed",
	}}, retryTestPolicy())

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "second instance failed", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0]
	if b.disposition != workload.RetryBlockPersist {
		t.Errorf("disposition: got %v want Persist (evidence refresh)", b.disposition)
	}
	if b.block.AttemptsStarted != 1 {
		t.Errorf("AttemptsStarted: got %d want 1 (same wave — no increment)", b.block.AttemptsStarted)
	}
	if b.block.NextRetryAt == nil || !b.block.NextRetryAt.Time.Equal(next.Time) {
		t.Errorf("NextRetryAt: got %v want unchanged %v", b.block.NextRetryAt, next.Time)
	}
	if b.block.Reason != "second instance failed" {
		t.Errorf("Reason: got %q want refreshed", b.block.Reason)
	}
	if b.block.LastFailureAt == nil || !b.block.LastFailureAt.Time.Equal(t0) {
		t.Errorf("LastFailureAt: got %v want refreshed to %v", b.block.LastFailureAt, t0)
	}
	if len(*warns) != 0 {
		t.Errorf("WarnRetryHeld: got %d calls want 0", len(*warns))
	}
}

// TestRecordUpdateFailure_ExhaustionHolds: (d) the third counted wave
// exhausts MaxAttempts=3 → Held, NextRetryAt cleared, WarnRetryHeld
// exactly once with attempts=3. A later failure against the Held block
// refreshes evidence only — no second warning, no increment.
func TestRecordUpdateFailure_ExhaustionHolds(t *testing.T) {
	t0 := time.Now()
	input, calls, warns := retryWriterInput(t0, []workload.RetryBlock{{
		TargetRevision:  "rev-bad",
		State:           workload.RetryBlockRetryInProgress,
		AttemptsStarted: 2,
	}}, retryTestPolicy())

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "third strike", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0].block
	if b.State != workload.RetryBlockHeld {
		t.Errorf("state: got %q want Held (3 >= MaxAttempts)", b.State)
	}
	if b.AttemptsStarted != 3 {
		t.Errorf("AttemptsStarted: got %d want 3", b.AttemptsStarted)
	}
	if b.NextRetryAt != nil {
		t.Errorf("NextRetryAt: got %v want nil (Held has no time bound)", b.NextRetryAt)
	}
	if len(*warns) != 1 {
		t.Fatalf("WarnRetryHeld: got %d calls want exactly 1 (at the Held transition)", len(*warns))
	}
	if w := (*warns)[0]; w.rev != "rev-bad" || w.attempts != 3 || w.reason != "third strike" {
		t.Errorf("warning: got %+v want {rev-bad 3 third strike}", w)
	}

	// A subsequent failure against the persisted Held block: refresh only.
	input.ObservedState.RetryBlocks = []workload.RetryBlock{b}
	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "post-hold noise", true); err != nil {
		t.Fatalf("record on Held: %v", err)
	}
	held := (*calls)[1].block
	if held.State != workload.RetryBlockHeld || held.AttemptsStarted != 3 {
		t.Errorf("Held refresh: got (state=%q, attempts=%d) want (Held, 3)", held.State, held.AttemptsStarted)
	}
	if held.Reason != "post-hold noise" {
		t.Errorf("Held refresh Reason: got %q want refreshed", held.Reason)
	}
	if len(*warns) != 1 {
		t.Errorf("WarnRetryHeld after Held refresh: got %d calls want still 1", len(*warns))
	}
}

// TestRecordUpdateFailure_NilPolicyHoldsFirstFailure: (e) unconfigured
// policy fails safe — Held on the FIRST failure, never Backoff.
func TestRecordUpdateFailure_NilPolicyHoldsFirstFailure(t *testing.T) {
	t0 := time.Now()
	input, calls, warns := retryWriterInput(t0, nil, nil)

	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "rev-bad", "no policy configured", true); err != nil {
		t.Fatalf("record: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	b := (*calls)[0].block
	if b.State != workload.RetryBlockHeld {
		t.Errorf("state: got %q want Held (nil policy is always exhausted)", b.State)
	}
	if b.AttemptsStarted != 1 {
		t.Errorf("AttemptsStarted: got %d want 1", b.AttemptsStarted)
	}
	if b.NextRetryAt != nil {
		t.Errorf("NextRetryAt: got %v want nil", b.NextRetryAt)
	}
	if len(*warns) != 1 || (*warns)[0].attempts != 1 {
		t.Errorf("WarnRetryHeld: got %+v want exactly one call with attempts=1", *warns)
	}
}

// TestRecordUpdateFailure_UnwiredNoOp: (f) nil MutateRetryBlock (adapter
// opted out) and empty targetRev are both silent no-ops — no panic.
func TestRecordUpdateFailure_UnwiredNoOp(t *testing.T) {
	t0 := time.Now()

	unwired := &workload.ReconcileInput{Clock: clocktesting.NewFakeClock(t0)}
	if err := recordUpdateFailureInRetryBlock(context.Background(), *unwired, "rev-bad", "x", true); err != nil {
		t.Fatalf("nil closure must no-op: %v", err)
	}

	input, calls, _ := retryWriterInput(t0, nil, retryTestPolicy())
	if err := recordUpdateFailureInRetryBlock(context.Background(), *input, "", "x", true); err != nil {
		t.Fatalf("empty targetRev must no-op: %v", err)
	}
	if len(*calls) != 0 {
		t.Errorf("empty targetRev: got %d MutateRetryBlock calls want 0", len(*calls))
	}
}

// TestInstanceFailureReason pins the call-site evidence extraction:
// LastFailure.Message wins, ShortString covers message-less stuck-pod
// escalations, and the fallback covers a Failed instance with no
// recorded termination.
func TestInstanceFailureReason(t *testing.T) {
	if got := instanceFailureReason(nil, "fallback"); got != "fallback" {
		t.Errorf("nil status: got %q want fallback", got)
	}
	s := &workload.InstanceStatus{}
	if got := instanceFailureReason(s, "fallback"); got != "fallback" {
		t.Errorf("nil LastFailure: got %q want fallback", got)
	}
	s.LastFailure = &workload.InstanceTermination{PodName: "p-0", Reason: "ImagePullBackOff"}
	if got := instanceFailureReason(s, "fallback"); got != "pod p-0 stuck (ImagePullBackOff)" {
		t.Errorf("ShortString path: got %q", got)
	}
	s.LastFailure.Message = "DeadlineExceeded: Update/Surge exceeded InstanceReadyTimeout"
	if got := instanceFailureReason(s, "fallback"); got != s.LastFailure.Message {
		t.Errorf("Message path: got %q want %q", got, s.LastFailure.Message)
	}
}

// TestInstanceFailureWorkloadCaused pins the call-site cause attribution:
// only a LastFailure whose Reason is in the workload-caused set charges
// the ladder; an elapsed deadline, an ambiguous kubelet reason, and
// missing evidence do not.
func TestInstanceFailureWorkloadCaused(t *testing.T) {
	if instanceFailureWorkloadCaused(nil) {
		t.Error("nil status must not be workload-caused")
	}
	s := &workload.InstanceStatus{}
	if instanceFailureWorkloadCaused(s) {
		t.Error("nil LastFailure must not be workload-caused")
	}
	for reason, want := range map[string]bool{
		"ImagePullBackOff":           true,
		"ErrImagePull":               true,
		"InvalidImageName":           true,
		"CreateContainerConfigError": true,
		"DeadlineExceeded":           false,
		"CrashLoopBackOff":           false,
		"RunContainerError":          false,
		"":                           false,
	} {
		s.LastFailure = &workload.InstanceTermination{PodName: "p-0", Reason: reason}
		if got := instanceFailureWorkloadCaused(s); got != want {
			t.Errorf("reason %q: got %v want %v", reason, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Success prune: the promote helpers remove the promoted revision's block.
// ---------------------------------------------------------------------------

// TestPatchReadyOnRevision_PrunesBlock: promoting Ready on rev removes
// that rev's block (disposition Remove observed).
func TestPatchReadyOnRevision_PrunesBlock(t *testing.T) {
	t0 := time.Now()
	input, _, tcr, calls, _, _ := retryGateFixture(t, t0)

	if err := status.StampReadyOnRevision(context.Background(), *input, 0, tcr.Name); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	if c := (*calls)[0]; c.rev != tcr.Name || c.disposition != workload.RetryBlockRemove {
		t.Errorf("prune: got (rev=%q, disposition=%v) want (%q, Remove)", c.rev, c.disposition, tcr.Name)
	}
}

// disposedBackfillFixture builds the deadline-disposed shape the
// empty-RunningRevision backfill can encounter: Instance 0 is
// Phase=Failed with NO Operation and NO RunningRevision, a Backoff
// RetryBlock for the target revision is already due, and the wedged
// attempt's pods are still present carrying the target revision's hash.
// podReady controls whether those pods carry ContainersReady. Returns
// the pod separately so the caller threads it through instancePods.
func disposedBackfillFixture(t *testing.T, t0 time.Time, podReady bool) (*workload.ReconcileInput, workload.ComponentPlan, *appsv1.ControllerRevision, *[]retryBlockCall, client.Client, *v1beta1.InferenceService, *corev1.Pod) {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	// Deadline disposition left: Failed, Operation cleared, no
	// RunningRevision ever stamped (initial create never converged).
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceFailed
	c := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, c, isvc, legacyTargetSpecImage("llama:v2"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(t0)
	due := metav1.NewTime(t0.Add(-1 * time.Second))
	calls := recordRetryBlockCalls(&input, []workload.RetryBlock{
		{TargetRevision: tcr.Name, State: workload.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &due},
	})
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	// The wedged attempt's pod: labelled with the target revision (a bad
	// image ref always carries the hash of its own bad revision).
	pod := legacyPodForInstance(isvc, 0, podReady, false)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(tcr.Name)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	if !podReady {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  "main",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
		}}
	}
	return &input, plan, tcr, calls, c, isvc, pod
}

// TestDetectUpdate_BackfillRefusesWedgedPods: the empty-RunningRevision
// backfill must NOT stamp Ready / prune the target's RetryBlock off a
// revision-match alone. A deadline-disposed create (Failed, no Operation,
// pods present in a kubelet waiting reason) carries the very revision
// that wedged it; stamping Ready here would prune the block and defuse
// the retry machinery while nothing is actually serving. The row is not
// left alone either: refusing the stamp hands it back to the ordinary
// roll, which is the retry its RetryBlock paces.
func TestDetectUpdate_BackfillRefusesWedgedPods(t *testing.T) {
	for _, reason := range []string{"ImagePullBackOff", "CrashLoopBackOff"} {
		t.Run(reason, func(t *testing.T) {
			t0 := time.Now()
			input, plan, tcr, calls, c, isvc, pod := disposedBackfillFixture(t, t0, false /* podReady */)
			pod.Status.ContainerStatuses[0].State.Waiting.Reason = reason

			trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, []*corev1.Pod{pod})
			if err != nil {
				t.Fatalf("detect: %v", err)
			}
			if !trigger {
				t.Errorf("a wedged pod on the target revision must take the ordinary retry roll")
			}
			if retryAfter != 0 {
				t.Errorf("retryAfter: got %v want 0 (the block is due)", retryAfter)
			}
			if len(*calls) != 0 {
				t.Errorf("MutateRetryBlock calls: got %d want 0 (detect stamps nothing; the flip belongs to attempt-stamp time)", len(*calls))
			}
			s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
			if s.Phase != v1beta1.OMENativeInstanceFailed {
				t.Errorf("phase: got %q want still Failed (no Ready stamp without proof)", s.Phase)
			}
			if s.RunningRevision != "" {
				t.Errorf("RunningRevision: got %q want empty (no backfill without proof)", s.RunningRevision)
			}
		})
	}
}

// TestDetectUpdate_WedgedPodsReachHeld: the retry ladder is reachable
// end to end from the roll a wedged on-target row keeps. Each pass
// re-triggers, the attempt stamp flips the block, the failure charges
// the ladder — and the count reaches Held, after which the gate denies
// and the churn stops. An on-target row left alone instead of rolled
// opens no further attempt, so the block would sit at Backoff forever.
func TestDetectUpdate_WedgedPodsReachHeld(t *testing.T) {
	const maxAttempts = 3
	t0 := time.Now()
	input, plan, tcr, _, c, isvc, pod := disposedBackfillFixture(t, t0, false /* podReady */)
	plan.UpdateStrategy.Type = workload.UpdateStrategyRecreatePod
	clock := clocktesting.NewFakeClock(t0)
	input.Clock = clock
	policy := &workload.RetryPolicy{
		MaxAttempts:  maxAttempts,
		InitialDelay: 5 * time.Second,
		MaxDelay:     20 * time.Second,
		Multiplier:   2,
	}
	input.UpdateRetryPolicy = policy
	// Start from the first failure wave, with the ladder persisted back
	// into the observed state so each pass reads what the last one wrote.
	input.ObservedState.RetryBlocks = nil
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if found := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev); found != nil {
			b = *found
		}
		if mutate(&b) == workload.RetryBlockRemove {
			input.ObservedState.RetryBlocks = nil
			return nil
		}
		input.ObservedState.RetryBlocks = []workload.RetryBlock{b}
		return nil
	}

	ctx := context.Background()
	for attempt := int32(1); attempt <= maxAttempts; attempt++ {
		trigger, _, err := DetectUpdateTriggerWithPods(ctx, legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, []*corev1.Pod{pod})
		if err != nil {
			t.Fatalf("detect (attempt %d): %v", attempt, err)
		}
		if !trigger {
			t.Fatalf("attempt %d: the wedged row must re-trigger so the ladder can advance", attempt)
		}
		if _, err := status.StampRecreating(ctx, *input, 0, tcr.Name,
			recreateRevisionCause(*input, 0, tcr.Name), workload.UpdateStrategyRecreatePod, time.Hour); err != nil {
			t.Fatalf("stamp attempt %d: %v", attempt, err)
		}
		// The attempt wedges on the same bad image: the disposition charges
		// the revision's ladder and disposes the row back to Failed with the
		// Operation cleared.
		if err := recordUpdateFailureInRetryBlock(ctx, *input, tcr.Name, "ImagePullBackOff", true /* workloadCaused */); err != nil {
			t.Fatalf("record failure %d: %v", attempt, err)
		}
		if err := input.MutateInstance(ctx, 0, func(s *workload.InstanceStatus) bool {
			s.Phase = workload.InstancePhaseFailed
			s.Operation = nil
			return true
		}); err != nil {
			t.Fatalf("dispose attempt %d: %v", attempt, err)
		}
		input.ObservedState.InstanceStatuses = legacyInstanceStatuses(c, isvc, workload.ComponentEngine)

		b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, tcr.Name)
		if b == nil {
			t.Fatalf("attempt %d: no RetryBlock recorded", attempt)
		}
		if b.AttemptsStarted != attempt {
			t.Fatalf("attempt %d: AttemptsStarted got %d want %d", attempt, b.AttemptsStarted, attempt)
		}
		// Wait out the rung so the next pass is not denied by the backoff.
		clock.Step(policy.MaxDelay + time.Second)
	}

	b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, tcr.Name)
	if b.State != workload.RetryBlockHeld {
		t.Fatalf("after %d charged attempts: state got %q want Held", maxAttempts, b.State)
	}
	if b.NextRetryAt != nil {
		t.Errorf("Held has no time bound: NextRetryAt got %v want nil", b.NextRetryAt)
	}
	trigger, retryAfter, err := DetectUpdateTriggerWithPods(ctx, legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, []*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("detect after Held: %v", err)
	}
	if trigger {
		t.Errorf("Held must stop the churn: no further attempt may be opened")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter after Held: got %v want 0 (Held has no time bound)", retryAfter)
	}
}

// TestDetectUpdate_StuckPodOnTargetRelocates: a node-scoped fault parks
// the pod Running-but-unready with no kubelet waiting reason, and the
// deadline disposition answers it with a terminal AutoRecover directive
// — recorded as the instance's node exclusion, executed by the ordinary
// rebuild. That rebuild is the roll this trigger opens: an on-target row
// left alone is never recreated, so the pod keeps its node and its UID.
func TestDetectUpdate_StuckPodOnTargetRelocates(t *testing.T) {
	const suspectNode = "node-suspect"
	t0 := time.Now()
	input, plan, tcr, _, c, isvc, pod := disposedBackfillFixture(t, t0, false /* podReady */)
	// A node-scoped fault leaves the container Running: no waiting reason,
	// just never ContainersReady.
	pod.Status.ContainerStatuses = nil
	pod.Status.Phase = corev1.PodRunning
	pod.Spec.NodeName = suspectNode
	pod.Labels[query.LabelInstanceIncarnation] = "1"
	plan.UpdateStrategy.Type = workload.UpdateStrategyRecreatePod
	plan.Instances[0].ExcludedNodes = []string{suspectNode}

	ctx := context.Background()
	trigger, _, err := DetectUpdateTriggerWithPods(ctx, legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, []*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Fatalf("a stuck pod on the target revision must take the roll the relocation rides on")
	}

	if err := c.Create(ctx, pod); err != nil {
		t.Fatalf("seed stuck pod: %v", err)
	}
	deps := legacyTestDeps(c)
	if _, err := recreateUpdate(ctx, deps, *input, plan, plan.Instances[0], tcr, []*corev1.Pod{pod}); err != nil {
		t.Fatalf("recreate (drain pass): %v", err)
	}
	live := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), live); !apierrors.IsNotFound(err) {
		t.Fatalf("stuck pod must be deleted by the relocation rebuild: get err=%v", err)
	}

	// Next pass: the index is rebuilt at the bumped incarnation, steered
	// off the recorded node by the exclusion overlay.
	input.ObservedState.InstanceStatuses = legacyInstanceStatuses(c, isvc, workload.ComponentEngine)
	legacyResetExpectations(t)
	if _, err := recreateUpdate(ctx, deps, *input, plan, plan.Instances[0], tcr, nil); err != nil {
		t.Fatalf("recreate (rebuild pass): %v", err)
	}
	rebuilt := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), rebuilt); err != nil {
		t.Fatalf("rebuilt pod: %v", err)
	}
	if got := rebuilt.Labels[query.LabelInstanceIncarnation]; got != "2" {
		t.Errorf("rebuilt pod incarnation: got %q want \"2\" (the stuck pod was replaced, not reused)", got)
	}
	if got := hostnameNotInValues(rebuilt); len(got) != 1 || got[0] != suspectNode {
		t.Errorf("rebuilt pod hostname NotIn values: got %v want [%s]", got, suspectNode)
	}
}

// TestDetectUpdate_BackfillAdoptsRuntimeReadyPods: the backfill's
// legitimate purpose survives the guard — pods genuinely running the
// target (runtime-ready) with a lost/never-written status record are
// adopted: Ready stamped, RunningRevision backfilled, and the target's
// block pruned (success at rev is real proof here).
func TestDetectUpdate_BackfillAdoptsRuntimeReadyPods(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc, pod := disposedBackfillFixture(t, t0, true /* podReady */)

	trigger, retryAfter, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, []*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("runtime-ready pods on the target revision must not trigger an update")
	}
	if retryAfter != 0 {
		t.Errorf("retryAfter: got %v want 0", retryAfter)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1 (success-prune)", len(*calls))
	}
	if call := (*calls)[0]; call.rev != tcr.Name || call.disposition != workload.RetryBlockRemove {
		t.Errorf("prune: got (rev=%q, disposition=%v) want (%q, Remove)", call.rev, call.disposition, tcr.Name)
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady {
		t.Errorf("phase: got %q want Ready (legitimate adoption)", s.Phase)
	}
	if s.RunningRevision != tcr.Name {
		t.Errorf("RunningRevision: got %q want %q (backfilled)", s.RunningRevision, tcr.Name)
	}
}

// TestDetectUpdate_BackfillAdoptsEngineRenderedPods: the adoption path
// has to recognise the pods the engine itself writes. The renderer adds
// hostname, subdomain and the serving readiness gate on top of the
// desired template, so the pod under test is produced by the renderer
// rather than hand-built — a comparison that cannot see past those
// additions leaves this path unreachable in production.
func TestDetectUpdate_BackfillAdoptsEngineRenderedPods(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc, _ := disposedBackfillFixture(t, t0, true /* podReady */)

	inst := plan.Instances[0]
	rendered, err := testRenderWithRevision(isvc, legacyTargetSpecImage("llama:v2"), nil, plan, inst,
		inst.Runners[0], 0, query.RevisionHashFromControllerRevisionName(tcr.Name))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	rendered.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(t0),
	}}

	trigger, _, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, inst, tcr, []*corev1.Pod{rendered})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Fatalf("an engine-rendered pod on the target revision must be adopted, not re-surged")
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1 (success-prune)", len(*calls))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady || s.RunningRevision != tcr.Name {
		t.Errorf("adoption stamp: got (phase=%q, runningRevision=%q) want (%q, %q)",
			s.Phase, s.RunningRevision, v1beta1.OMENativeInstanceReady, tcr.Name)
	}
}

// TestDetectUpdate_BackfillRollsPodSetWithAForeignRevision: adoption is
// all-or-nothing over the Instance's pod set. One member on another
// revision means the Instance is not on the target, so the row takes the
// ordinary roll and nothing is stamped.
func TestDetectUpdate_BackfillRollsPodSetWithAForeignRevision(t *testing.T) {
	t0 := time.Now()
	input, plan, tcr, calls, c, isvc, onTarget := disposedBackfillFixture(t, t0, true /* podReady */)

	foreign := onTarget.DeepCopy()
	foreign.Name = onTarget.Name + "-peer"
	foreign.Labels[query.LabelRevisionHash] = "0badf00d"

	trigger, _, err := DetectUpdateTriggerWithPods(context.Background(), legacyTestDeps(c), *input, plan, plan.Instances[0], tcr, []*corev1.Pod{onTarget, foreign})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("a pod set with a member on another revision must take the ordinary roll")
	}
	if len(*calls) != 0 {
		t.Errorf("MutateRetryBlock calls: got %d want 0 (nothing adopted, nothing pruned)", len(*calls))
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed || s.RunningRevision != "" {
		t.Errorf("row mutated: got (phase=%q, runningRevision=%q) want (%q, empty)",
			s.Phase, s.RunningRevision, v1beta1.OMENativeInstanceFailed)
	}
}

// TestPatchReadyOnRevisionWithOrdinal_PrunesBlock: the surge-promote
// variant prunes likewise.
func TestPatchReadyOnRevisionWithOrdinal_PrunesBlock(t *testing.T) {
	t0 := time.Now()
	input, _, tcr, calls, _, _ := retryGateFixture(t, t0)

	if err := status.StampReadyAtOrdinal(context.Background(), *input, 0, tcr.Name, 1); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("MutateRetryBlock calls: got %d want 1", len(*calls))
	}
	if c := (*calls)[0]; c.rev != tcr.Name || c.disposition != workload.RetryBlockRemove {
		t.Errorf("prune: got (rev=%q, disposition=%v) want (%q, Remove)", c.rev, c.disposition, tcr.Name)
	}
}

// The update strategy an attempt runs under is fixed when its operation
// opens. UpdateStrategy is not part of the revision payload, so editing it
// retargets nothing: the attempt in flight keeps its mechanism and the edit
// is picked up by the next attempt this Instance is admitted for. The tests
// below drive each in-flight step with the strategy already flipped and
// assert the attempt does not change mechanism.

// TestUpdateWithPods_InPlacePatchIgnoresAStrategyEdit: a patch already
// applied to a live pod cannot be taken back, so dispatching the recreate
// machine over it would tear down a pod that is already converging — and
// bump its Incarnation out from under the patch.
func TestUpdateWithPods_InPlacePatchIgnoresAStrategyEdit(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	ir.Status.InstanceStatuses[0].Operation.Strategy = string(v1beta1.UpdateStrategyInPlaceIfPossible)
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, false /* not ready */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	// The attempt is already converging on the target, so the in-place stamp
	// recognizes its own state and the attempt's identity is observable.
	live := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), live); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	live.Status.InstanceStatuses[0].TargetRevision = tcr.Name
	live.Status.InstanceStatuses[0].Operation.TargetRevision = tcr.Name
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("seed the attempt's target: %v", err)
	}

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	// The desired strategy now says recreate; the pinned one still says
	// in place.
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	if _, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{pod}); err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != workload.UpdateStepInPlace {
		t.Fatalf("Operation: got %+v, want the patch still on Step=%s", s.Operation, workload.UpdateStepInPlace)
	}
	if s.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (the same attempt)", s.Operation.ID, op.ID)
	}
	if s.Operation.Strategy != string(v1beta1.UpdateStrategyInPlaceIfPossible) {
		t.Errorf("Operation.Strategy: got %q, want the pin the attempt opened with", s.Operation.Strategy)
	}
	if s.Incarnation != 1 {
		t.Errorf("Incarnation: got %d want 1 — a recreate ran over the in-flight patch", s.Incarnation)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
		t.Errorf("the patched pod is gone (%v): the edit flipped the attempt to recreate", err)
	}
}

// TestUpdateWithPods_FailedRowPicksUpTheEditedStrategy: a Failed row holds
// no attempt, so there is nothing to keep on a mechanism. Editing the
// strategy is the operator's rescue lever out of a mode that cannot make
// progress, and the next admitted attempt has to honour it.
func TestUpdateWithPods_FailedRowPicksUpTheEditedStrategy(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceFailed,
		Operation: &v1beta1.InstanceOperation{
			ID:             "update-0-1",
			Type:           v1beta1.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			Strategy:       string(v1beta1.UpdateStrategySurgeThenDrain),
			StartedAt:      metav1.NewTime(time.Now().Add(-time.Hour)),
			LastProgressAt: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
	}
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	if _, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{pod}); err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil {
		t.Fatalf("no attempt was admitted off the Failed row")
	}
	if s.Operation.Strategy != string(v1beta1.UpdateStrategyRecreatePod) {
		t.Errorf("Operation.Strategy: got %q, want the edited %q",
			s.Operation.Strategy, v1beta1.UpdateStrategyRecreatePod)
	}
	if s.Operation.Step != workload.UpdateStepDrain {
		t.Errorf("Operation.Step: got %q, want the recreate's %q", s.Operation.Step, workload.UpdateStepDrain)
	}
}

// TestUpdateWithPods_RecreateDrainIgnoresAStrategyEdit: past the Incarnation
// bump the old materialization is already being torn down. Handing the row to
// the in-place patcher now would patch a pod that is on its way out and leave
// the recreate's replacement unbuilt.
func TestUpdateWithPods_RecreateDrainIgnoresAStrategyEdit(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 2)
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	live := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), live); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	live.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceUpdating
	live.Status.InstanceStatuses[0].Incarnation = 2
	live.Status.InstanceStatuses[0].TargetRevision = tcr.Name
	live.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		ID:             "update-0-1",
		Type:           v1beta1.InstanceOperationUpdate,
		Step:           workload.UpdateStepDrain,
		TargetRevision: tcr.Name,
		Strategy:       string(v1beta1.UpdateStrategyRecreatePod),
		StartedAt:      metav1.NewTime(time.Now().Add(-time.Minute)),
		LastProgressAt: metav1.NewTime(time.Now().Add(-time.Minute)),
		Deadline:       metav1.NewTime(time.Now().Add(29 * time.Minute)),
	}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("seed the in-flight recreate: %v", err)
	}

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	if _, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{pod}); err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != workload.UpdateStepDrain {
		t.Fatalf("Operation: got %+v, want the recreate still on Step=%s", s.Operation, workload.UpdateStepDrain)
	}
	if s.Operation.ID != "update-0-1" {
		t.Errorf("Operation.ID: got %q want %q (the same attempt)", s.Operation.ID, "update-0-1")
	}
	if s.Operation.Strategy != string(v1beta1.UpdateStrategyRecreatePod) {
		t.Errorf("Operation.Strategy: got %q, want the pin the attempt opened with", s.Operation.Strategy)
	}
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err == nil {
		if got := fresh.Spec.Containers[0].Image; got != "llama:v1" {
			t.Errorf("the draining pod was patched to %q: the edit flipped the attempt to in place", got)
		}
	}
}

// The replacement gang's marker index is driven entirely by the source's
// gang surge. These tests pin the three consequences that show up inside
// the ops package: the update strategy the pair runs under is the one
// pinned when it started, the restart trigger does not treat the marker
// as a row of its own, and the promote is what hands the index back to
// the ordinary repair path.

// gangMarkerRevision is the replacement gang's target.
const gangMarkerRevision = "llama-70b-engine-gangtgt1"

// TestEffectiveUpdateStrategy_GangSurgePairStaysOnThePinnedStrategy: the
// strategy is pinned on the operation when the attempt starts, so a
// strategy edit mid-surge does not switch the pair into another mode
// halfway through. The gang surge stays in control and finishes under
// SurgeThenDrain; the edit reaches the pair at its next admitted
// attempt.
func TestEffectiveUpdateStrategy_GangSurgePairStaysOnThePinnedStrategy(t *testing.T) {
	surgeIndex := int32(2)
	source := &workload.InstanceStatus{
		Index:           0,
		Incarnation:     1,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "llama-70b-engine-priorrev",
		TargetRevision:  gangMarkerRevision,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			Strategy:       workload.UpdateStrategySurgeThenDrain,
			SurgeIndex:     &surgeIndex,
			TargetRevision: gangMarkerRevision,
		},
	}
	for _, edited := range []workload.UpdateStrategyType{
		workload.UpdateStrategyRecreatePod,
		workload.UpdateStrategyInPlaceOnly,
	} {
		t.Run(string(edited), func(t *testing.T) {
			if got := effectiveUpdateStrategy(source, edited); got != workload.UpdateStrategySurgeThenDrain {
				t.Errorf("effective strategy: got %q want the pinned SurgeThenDrain", got)
			}
		})
	}

	// Counter-case: with no attempt pinning it, the edit takes effect at
	// once — so the pin above is the operation's doing, not a constant.
	idle := &workload.InstanceStatus{Index: 0, Phase: workload.InstancePhaseReady}
	if got := effectiveUpdateStrategy(idle, workload.UpdateStrategyRecreatePod); got != workload.UpdateStrategyRecreatePod {
		t.Errorf("idle row effective strategy: got %q want the edited RecreatePod", got)
	}
}
