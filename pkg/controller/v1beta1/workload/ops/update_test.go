package ops

import (
	"context"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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
// runtime-ready, and NOT yet serving (because a previous pass drained
// it), Update flips serving=True and stamps Phase=Ready with the new
// RunningRevision.
func TestUpdate_InPlaceConverges_MarksReadyWithRunningRevision(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: updateStepInPlace,
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
	if !done {
		t.Fatalf("expected done=true: pod is ready on target image")
	}

	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if !podreadiness.IsServing(got) {
		t.Errorf("serving should have been flipped True after in-place convergence")
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
			Type: v1beta1.InstanceOperationUpdate, Step: updateStepInPlace,
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

// TestPodImagesMatch_InjectedSidecarIgnored pins the container-subset
// compare: live containers absent from the target spec (istio/linkerd
// style injections) are not OMENative-owned and must not block the
// match — counting them would fail podImagesMatch and podRuntimeImagesMatch
// forever, livelocking the in-place update with the pod held drained.
// A TARGET container missing from the pod still fails both.
func TestPodImagesMatch_InjectedSidecarIgnored(t *testing.T) {
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}}}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "main", Image: "llama:v2"},
			{Name: "istio-proxy", Image: "istio/proxyv2:1.20"},
		}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "main", Image: "llama:v2"},
			{Name: "istio-proxy", Image: "istio/proxyv2:1.20"},
		}},
	}
	if !podImagesMatch(pod, target) {
		t.Errorf("podImagesMatch: injected sidecar must be ignored")
	}
	if !podRuntimeImagesMatch(pod, target) {
		t.Errorf("podRuntimeImagesMatch: injected sidecar status must be ignored")
	}
	stale := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "llama:v3"}}}
	if podImagesMatch(pod, stale) {
		t.Errorf("podImagesMatch: diverged target image must still mismatch")
	}
	foreign := &corev1.PodSpec{Containers: []corev1.Container{{Name: "other", Image: "x:v1"}}}
	if podImagesMatch(pod, foreign) {
		t.Errorf("podImagesMatch: target container missing from pod must mismatch")
	}
	if podRuntimeImagesMatch(pod, foreign) {
		t.Errorf("podRuntimeImagesMatch: target container without a status must mismatch")
	}
}

// TestPatchPodImages_ReportsIssued pins the issued-return contract: a
// no-diff patch must report issued=false (the caller only requeues for
// a kubelet roll when a patch actually went out), a real diff true.
func TestPatchPodImages_ReportsIssued(t *testing.T) {
	isvc, _ := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	pod := legacyPodAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, pod)
	stored := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	pod = stored

	issued, err := patchPodImages(context.Background(), c, pod, &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v1"}},
	})
	if err != nil {
		t.Fatalf("patchPodImages (no diff): %v", err)
	}
	if issued {
		t.Errorf("issued: got true want false when nothing needs patching")
	}

	issued, err = patchPodImages(context.Background(), c, pod, &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}},
	})
	if err != nil {
		t.Fatalf("patchPodImages (diff): %v", err)
	}
	if !issued {
		t.Errorf("issued: got false want true for a real image diff")
	}
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got.Spec.Containers[0].Image != "llama:v2" {
		t.Errorf("pod image: got %q want llama:v2", got.Spec.Containers[0].Image)
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
			Type: v1beta1.InstanceOperationUpdate, Step: updateStepInPlace,
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

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("Update: %v", err)
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
		Step:           updateStepInPlace,
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
	if s.Operation == nil || s.Operation.Step != updateStepDrain {
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

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
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

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
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

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Operation.Type=Migrate must suppress Update")
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

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
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

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
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

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("Phase=Failed with RunningRevision != target must re-trigger update (recovery)")
	}
}

// TestDetectUpdateTrigger_PodImageDiffersTriggersUpdate: no
// RunningRevision recorded, observed pod has image v1 but target spec
// is v2 — must trigger.
func TestDetectUpdateTrigger_PodImageDiffersTriggersUpdate(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	c := legacyNewFakeClient(t, isvc, ir, pod)
	target := legacyTargetSpecImage("llama:v2")
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !trigger {
		t.Errorf("pod image mismatch should trigger update")
	}
}

// TestDetectUpdateTrigger_LegacyMatchedPodsBackfillRunningRevision:
// Status has no RunningRevision, pod already runs the target image.
// DetectUpdateTrigger must NOT trigger an update, and as a side effect
// should backfill RunningRevision so future reconciles take the cheap
// fast-path.
func TestDetectUpdateTrigger_LegacyMatchedPodsBackfillRunningRevision(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	target := legacyTargetSpecImage("llama:v1")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	c := legacyNewFakeClient(t, isvc, ir, pod)
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategySurgeThenDrain, nil)

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("matched legacy pods must NOT trigger update")
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

	trigger, _, err := DetectUpdateTrigger(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if trigger {
		t.Errorf("Phase=Creating must not be interruptible by Update")
	}
}

// TestPodImagesMatch pins the spec-image equality check.
func TestPodImagesMatch(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v2"},
		{Name: "sidecar", Image: "metrics:v1"},
	}}}
	matching := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v2"},
		{Name: "sidecar", Image: "metrics:v1"},
	}}
	mismatching := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "llama:v3"},
		{Name: "sidecar", Image: "metrics:v1"},
	}}
	if !podImagesMatch(pod, matching) {
		t.Errorf("matching images should report true")
	}
	if podImagesMatch(pod, mismatching) {
		t.Errorf("differing images should report false")
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
	if err := patchInstanceStatusReadyOnRevision(context.Background(), input, 0, "rev-abc"); err != nil {
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

// TestCanonicalImage_NormalizesDockerHubReferences covers the qualified
// forms container runtimes report for short Docker Hub references.
func TestCanonicalImage_NormalizesDockerHubReferences(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"implicit library tag", "fake-serving:v1", "fake-serving:v1"},
		{"runtime-stamped library tag", "docker.io/library/fake-serving:v1", "fake-serving:v1"},
		{"explicit registry round-trip", "ghcr.io/foo/bar:v1", "ghcr.io/foo/bar:v1"},
		{"runtime-stamped namespaced tag", "docker.io/myuser/myimg:v1", "myuser/myimg:v1"},
		{"empty string", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := canonicalImage(tc.in); got != tc.want {
				t.Errorf("canonicalImage(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestPodRuntimeImagesMatch_RegistryNormalization: `fake-serving:v1`
// (spec) and `docker.io/library/fake-serving:v1` (runtime) MUST
// compare equal so in-place updates against KIND or any cluster whose
// runtime fully-qualifies implicit Docker Hub references converge.
func TestPodRuntimeImagesMatch_RegistryNormalization(t *testing.T) {
	target := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "fake-serving:v2"},
	}}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "fake-serving:v2"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "main", Image: "docker.io/library/fake-serving:v2"},
		}},
	}
	if !podRuntimeImagesMatch(pod, target) {
		t.Errorf("expected match between spec %q and runtime %q after canonicalImage normalization",
			target.Containers[0].Image, pod.Status.ContainerStatuses[0].Image)
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

// TestAnnotationsDiff_Cases exercises the helper directly to pin each
// branch of the (add | update | delete | leave-alone) decision matrix.
// These cases compose into the per-pod patches but are easier to reason
// about in isolation than via the full Update fixture.
func TestAnnotationsDiff_Cases(t *testing.T) {
	cases := []struct {
		name                string
		pod, previous, want map[string]string
		expect              map[string]any
	}{
		{
			name:     "no diff: nothing to patch",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{},
		},
		{
			name:     "add new key from target",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "1", "b": "2"},
			expect:   map[string]any{"b": "2"},
		},
		{
			name:     "update existing target key with new value",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "2"},
			expect:   map[string]any{"a": "2"},
		},
		{
			name:     "delete key the user removed (previous owned it)",
			pod:      map[string]string{"a": "1", "b": "2"},
			previous: map[string]string{"a": "1", "b": "2"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{"b": nil},
		},
		{
			name:     "leave foreign key alone (in pod, not in previous or target)",
			pod:      map[string]string{"a": "1", "linkerd.io/inject": "enabled"},
			previous: map[string]string{"a": "1"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{},
		},
		{
			name:     "delete only if pod actually has the key (no spurious null patch)",
			pod:      map[string]string{"a": "1"},
			previous: map[string]string{"a": "1", "b": "2"},
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{},
		},
		{
			name:     "nil-as-empty across all inputs",
			pod:      nil,
			previous: nil,
			want:     nil,
			expect:   map[string]any{},
		},
		{
			name:     "first revision: previous nil, target adds key, pod empty",
			pod:      nil,
			previous: nil,
			want:     map[string]string{"a": "1"},
			expect:   map[string]any{"a": "1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := annotationsDiff(tc.pod, tc.previous, tc.want)
			if len(got) != len(tc.expect) {
				t.Fatalf("diff length: got %d (%+v) want %d (%+v)", len(got), got, len(tc.expect), tc.expect)
			}
			for k, want := range tc.expect {
				gv, ok := got[k]
				if !ok {
					t.Errorf("missing key %q in diff", k)
					continue
				}
				if want == nil && gv != nil {
					t.Errorf("key %q: got %+v want nil", k, gv)
				}
				if want != nil && gv != want {
					t.Errorf("key %q: got %+v want %+v", k, gv, want)
				}
			}
		})
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
