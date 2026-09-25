package ops

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// inPlaceRollWithoutPods seeds an Instance whose in-place roll is in
// flight — Phase=Updating, Op{Update, InPlace} at targetRev with its own
// id and deadline — and whose only pod has been lost.
func inPlaceRollWithoutPods(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica, *v1beta1.InstanceOperation) {
	t.Helper()
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	op := &v1beta1.InstanceOperation{
		ID:             "update-0-1",
		Type:           v1beta1.InstanceOperationUpdate,
		Step:           workload.UpdateStepInPlace,
		StartedAt:      metav1.NewTime(time.Now().Add(-time.Minute)),
		LastProgressAt: metav1.NewTime(time.Now().Add(-time.Minute)),
		Deadline:       metav1.NewTime(time.Now().Add(29 * time.Minute)),
	}
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation:   op.DeepCopy(),
	}
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyInPlaceIfPossible},
	}
	return isvc, ir, op
}

// TestUpdateWithPods_InPlaceRollLosingItsOnlyPodReResolvesToRecreate:
// an in-place step has no repair for an empty pod set — the update pass
// owns the index so no create reaches it, and the restart trigger
// declines below Ready — so the roll would poll nothing until its
// deadline disposed it. Losing the pod re-resolves the roll to recreate
// in the same pass: the step becomes Drain, the replacement is rendered
// at the target revision, and the attempt keeps its identity and its
// deadline, so this is a continuation rather than a new attempt.
func TestUpdateWithPods_InPlaceRollLosingItsOnlyPodReResolvesToRecreate(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	var blocked []workload.RetryBlockDisposition
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		blocked = append(blocked, mutate(&workload.RetryBlock{TargetRevision: rev}))
		return nil
	}
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	done, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target, nil)
	if err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	if done {
		t.Fatalf("the roll must stay in flight while the replacement comes up")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Fatalf("Phase: got %q want Updating (no Failed on a re-resolution)", s.Phase)
	}
	if s.Operation == nil || s.Operation.Step != workload.UpdateStepDrain {
		t.Fatalf("Operation: got %+v want Step=%s", s.Operation, workload.UpdateStepDrain)
	}
	if s.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (same attempt)", s.Operation.ID, op.ID)
	}
	if s.Operation.Deadline.Unix() != op.Deadline.Unix() {
		t.Errorf("Operation.Deadline: got %v want %v (same attempt)", s.Operation.Deadline, op.Deadline)
	}
	if s.Incarnation <= 1 {
		t.Errorf("Incarnation: got %d want a bump (the recreate's new materialization)", s.Incarnation)
	}
	// The pin follows the mechanism the attempt moved to: later passes read
	// it instead of the live plan, so leaving it on in-place would send the
	// next one back to the patch this pod loss just ruled out.
	if s.Operation.Strategy != string(v1beta1.UpdateStrategyRecreatePod) {
		t.Errorf("Operation.Strategy: got %q want %q (the mechanism it re-resolved to)",
			s.Operation.Strategy, v1beta1.UpdateStrategyRecreatePod)
	}
	for _, d := range blocked {
		if d == workload.RetryBlockPersist {
			t.Errorf("a re-resolution must charge no RetryBlock")
		}
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected the replacement to be rendered, got %d pods", len(pods.Items))
	}
	replacement := pods.Items[0]
	if got := replacement.Labels[query.LabelRevisionHash]; got != query.RevisionHashFromControllerRevisionName(tcr.Name) {
		t.Errorf("replacement revision hash: got %q want the target's", got)
	}
	if got := replacement.Labels[query.LabelInstanceIncarnation]; got != "2" {
		t.Errorf("replacement incarnation label: got %q want 2", got)
	}
}

// TestUpdateWithPods_InPlaceRollKeepsPatchingWhileItsPodLives pins the
// negative: the re-resolution is keyed on the pod set being empty, so a
// roll whose pod is merely not ready yet stays in place.
func TestUpdateWithPods_InPlaceRollKeepsPatchingWhileItsPodLives(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, _ := inPlaceRollWithoutPods(t)
	target := legacyTargetSpecImage("llama:v2")
	pod := legacyPodAtIncarnation(isvc, 0, 1, false /* not ready */, false /* not serving */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

	if _, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{pod}); err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != workload.UpdateStepInPlace {
		t.Fatalf("Operation: got %+v want Step=%s", s.Operation, workload.UpdateStepInPlace)
	}
	if s.Incarnation != 1 {
		t.Errorf("Incarnation: got %d want 1 (in-place keeps the materialization)", s.Incarnation)
	}
}

// TestUpdateWithPods_InPlaceRollWaitsForItsOwnExpectations: an empty pod
// set is evidence of loss only once this Instance's own creates and
// deletes have been observed. Before that it can be a read that predates
// them, and converting the roll on one would bump the Incarnation out
// from under a pod that does exist.
func TestUpdateWithPods_InPlaceRollWaitsForItsOwnExpectations(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}
	workload.DefaultExpectations.ExpectCreates(input.Key.Namespace, input.Key.OwnerName, workload.ComponentEngine, 0, 1)

	done, err := UpdateWithPods(context.Background(), deps, input, plan, plan.Instances[0], tcr, target, nil)
	if err != nil {
		t.Fatalf("UpdateWithPods: %v", err)
	}
	if done {
		t.Fatalf("the roll must stay in flight while its own writes are unobserved")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != workload.UpdateStepInPlace {
		t.Fatalf("Operation: got %+v want Step=%s (no conversion on an unsettled read)", s.Operation, workload.UpdateStepInPlace)
	}
	if s.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (same attempt)", s.Operation.ID, op.ID)
	}
	if s.Incarnation != 1 {
		t.Errorf("Incarnation: got %d want 1 (nothing has been re-materialized)", s.Incarnation)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Errorf("pods: got %d want none rendered", len(pods.Items))
	}
}

// TestUpdateWithPods_InPlaceRollReResolvedOnceStaysOneAttempt: the
// update mode is re-resolved every pass, so the pass after a
// re-resolution can pick in-place again over the replacement pod. That
// is a step move inside an attempt that never ended — it must not stamp
// a new operation id, hand the row a second full deadline window, or
// announce itself again.
func TestUpdateWithPods_InPlaceRollReResolvedOnceStaysOneAttempt(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	target := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	recorder := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: recorder}

	if _, err := UpdateWithPods(context.Background(), deps, legacyTestInput(isvc, c, workload.ComponentEngine),
		plan, plan.Instances[0], tcr, target, nil); err != nil {
		t.Fatalf("UpdateWithPods (first pass): %v", err)
	}
	firstPassEvents := len(recorder.Events)

	// Second pass: the replacement the re-resolution rendered is now the
	// Instance's pod set, and the mode resolves back to in-place.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected the replacement to be rendered, got %d pods", len(pods.Items))
	}
	legacyResetExpectations(t)
	next := legacyTestInput(isvc, c, workload.ComponentEngine)
	if _, err := UpdateWithPods(context.Background(), deps, next, plan, plan.Instances[0], tcr, target,
		[]*corev1.Pod{&pods.Items[0]}); err != nil {
		t.Fatalf("UpdateWithPods (second pass): %v", err)
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil {
		t.Fatalf("Operation: got none want the attempt still in flight")
	}
	if s.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (one attempt, two mechanisms)", s.Operation.ID, op.ID)
	}
	if s.Operation.Deadline.Unix() != op.Deadline.Unix() {
		t.Errorf("Operation.Deadline: got %v want %v (no second window)", s.Operation.Deadline, op.Deadline)
	}
	if s.Operation.StartedAt.Unix() != op.StartedAt.Unix() {
		t.Errorf("Operation.StartedAt: got %v want %v (the attempt did not restart)", s.Operation.StartedAt, op.StartedAt)
	}
	if len(recorder.Events) != firstPassEvents {
		t.Errorf("events: got %d want %d; a step move announces nothing", len(recorder.Events), firstPassEvents)
	}
}

func TestUpdateInPlaceMetadataRollRepairsImageDriftBeforePromotion(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	spec := legacyTargetSpecImage("example.com/app:v1")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "example.com/app:drift")
	pod.Annotations = map[string]string{"release": "one"}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "main", Image: "example.com/app:drift", ContainerID: "containerd://drift",
	}}
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevisionWithMeta(t, c, isvc, workload.ComponentEngine, 0, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"release": "one"}})
	target := legacyEnsureTargetCRWithMeta(t, c, isvc, spec,
		&metav1.ObjectMeta{Annotations: map[string]string{"release": "two"}})
	markNotReady := false
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: &markNotReady})
	run := func(pass string) bool {
		t.Helper()
		done, err := Update(context.Background(), legacyTestDeps(c), legacyTestInput(isvc, c, workload.ComponentEngine), plan, plan.Instances[0], target, spec)
		if err != nil {
			t.Fatalf("%s Update: %v", pass, err)
		}
		return done
	}
	getPod := func() *corev1.Pod {
		t.Helper()
		got := &corev1.Pod{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
			t.Fatalf("get pod: %v", err)
		}
		return got
	}

	if run("record image transition") {
		t.Fatal("marker pass returned done=true")
	}
	marked := getPod()
	transition, present, valid := inPlaceImageTransitionFromPod(marked)
	if !present || !valid || transition.TargetImages["main"] != "example.com/app:v1" {
		t.Fatalf("transition marker: %+v present=%v valid=%v", transition, present, valid)
	}
	if marked.Spec.Containers[0].Image != "example.com/app:drift" {
		t.Fatal("image changed before the write-ahead marker was observed")
	}

	if run("patch image") {
		t.Fatal("image patch pass returned done=true")
	}
	patched := getPod()
	if patched.Spec.Containers[0].Image != "example.com/app:v1" {
		t.Fatalf("patched image = %q, want example.com/app:v1", patched.Spec.Containers[0].Image)
	}
	if patched.Status.ContainerStatuses[0].Image != "example.com/app:drift" {
		t.Fatal("fake kubelet unexpectedly advanced runtime status")
	}

	if run("stale runtime") {
		t.Fatal("stale ContainersReady and old runtime image promoted the Instance")
	}
	waiting := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if waiting.Phase != v1beta1.OMENativeInstanceUpdating || waiting.RunningRevision == target.Name {
		t.Fatalf("status advanced before runtime confirmation: %+v", waiting)
	}

	patched = getPod()
	patched.Status.ContainerStatuses[0].Image = "example.com/app:v1"
	patched.Status.ContainerStatuses[0].ContainerID = "containerd://target"
	if err := c.Status().Update(context.Background(), patched); err != nil {
		t.Fatalf("advance runtime status: %v", err)
	}
	if run("clear runtime proof") {
		t.Fatal("marker removal pass returned done=true")
	}
	cleared, present, valid := inPlaceImageTransitionFromPod(getPod())
	if present || valid || cleared != nil {
		t.Fatalf("transition remained after runtime proof: %+v present=%v valid=%v", cleared, present, valid)
	}

	if !run("promote") {
		t.Fatal("confirmed transition did not promote")
	}
	final := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if final.Phase != v1beta1.OMENativeInstanceReady || final.RunningRevision != target.Name || final.Operation != nil {
		t.Fatalf("final status: %+v", final)
	}
}

func TestInPlaceImageTransitionRetargetsUnconfirmedImages(t *testing.T) {
	isvc, _ := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	pod := legacyPodAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "example.com/app:v1"}}
	pod.Annotations = map[string]string{inPlaceImageTransitionAnnotation: `{"targetImages":{"main":"example.com/app:v2"}}`}
	c := legacyNewFakeClient(t, pod)
	stored := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	target := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example.com/app:v3"}}}
	ready, err := ensureInPlaceImageTransition(context.Background(), c, stored, target, map[string]string{"main": "example.com/app:v3"})
	if err != nil || ready {
		t.Fatalf("retarget marker: ready=%v err=%v", ready, err)
	}
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get retargeted pod: %v", err)
	}
	transition, present, valid := inPlaceImageTransitionFromPod(got)
	if !present || !valid || transition.TargetImages["main"] != "example.com/app:v3" {
		t.Fatalf("retargeted transition: %+v present=%v valid=%v", transition, present, valid)
	}
}

func TestInPlaceImageTransitionInvalidMarkersFailClosed(t *testing.T) {
	target := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "main", Image: "example.com/app:v2"},
		{Name: "helper", Image: "example.com/helper:v1"},
	}}
	tests := []struct {
		name string
		raw  string
	}{
		{name: "malformed JSON", raw: "{"},
		{name: "empty target set", raw: `{"targetImages":{}}`},
		{name: "removed container", raw: `{"targetImages":{"removed":"example.com/removed:v1"}}`},
		{name: "stale target value", raw: `{"targetImages":{"main":"example.com/app:v1"}}`},
		{name: "forged confirmed field", raw: `{"targetImages":{"main":"example.com/app:v2"},"confirmed":true}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc, _ := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
			pod := legacyPodAtIncarnation(isvc, 0, 1, true, true)
			pod.Spec = *target.DeepCopy()
			pod.Annotations = map[string]string{inPlaceImageTransitionAnnotation: test.raw}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{Name: "main", Image: "mirror.example.com/app:v2"},
				{Name: "helper", Image: "mirror.example.com/helper:v1"},
			}
			c := legacyNewFakeClient(t, pod)
			stored := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err != nil {
				t.Fatalf("get pod: %v", err)
			}

			ready, err := ensureInPlaceImageTransition(context.Background(), c, stored, target, nil)
			if err != nil {
				t.Fatalf("ensure transition: %v", err)
			}
			if test.name == "forged confirmed field" && !ready {
				t.Fatal("unknown confirmed field changed an otherwise current pending marker")
			}
			if test.name != "forged confirmed field" && ready {
				t.Fatal("invalid or stale marker was accepted without a corrective write")
			}

			got := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
				t.Fatalf("get ensured pod: %v", err)
			}
			transition, present, valid := inPlaceImageTransitionFromPod(got)
			if !present || !valid || !inPlaceImageTransitionMatchesTarget(transition, target) {
				t.Fatalf("ensured transition: %+v present=%v valid=%v", transition, present, valid)
			}
			if inPlaceImageTransitionRuntimeMatches(got, transition) {
				t.Fatal("runtime aliases satisfied a pending exact-image proof")
			}
		})
	}
}

func TestRemoveInPlaceImageTransitionRejectsStaleResourceVersion(t *testing.T) {
	isvc, _ := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	pod := legacyPodAtIncarnation(isvc, 0, 1, true, true)
	pod.Annotations = map[string]string{inPlaceImageTransitionAnnotation: `{"targetImages":{"main":"example.com/app:v2"}}`}
	c := legacyNewFakeClient(t, pod)

	stale := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), stale); err != nil {
		t.Fatalf("get stale pod: %v", err)
	}
	concurrent := stale.DeepCopy()
	concurrent.Annotations["example.com/concurrent"] = "write"
	if err := c.Update(context.Background(), concurrent); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	if err := removeInPlaceImageTransition(context.Background(), c, stale); !apierrors.IsConflict(err) {
		t.Fatalf("removeInPlaceImageTransition error = %v, want conflict", err)
	}
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if _, present := got.Annotations[inPlaceImageTransitionAnnotation]; !present {
		t.Fatal("conflicted removal deleted the transition marker")
	}
	if got.Annotations["example.com/concurrent"] != "write" {
		t.Fatal("conflicted removal lost a concurrent annotation")
	}
}

type stalePodListClient struct {
	client.Client
	pod *corev1.Pod
}

func (c *stalePodListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	pods, ok := list.(*corev1.PodList)
	if !ok {
		return c.Client.List(ctx, list, opts...)
	}
	pods.Items = []corev1.Pod{*c.pod.DeepCopy()}
	return nil
}

func TestUpdateInPlaceUsesLivePodForOptimisticImagePatch(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "llama:v1")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", Image: "llama:v1"}}
	live := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, live, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	targetSpec := legacyTargetSpecImage("llama:v2")
	target := legacyEnsureTargetCR(t, live, isvc, targetSpec)

	stale := &corev1.Pod{}
	if err := live.Get(context.Background(), client.ObjectKeyFromObject(pod), stale); err != nil {
		t.Fatalf("get stale pod: %v", err)
	}
	concurrent := stale.DeepCopy()
	concurrent.Annotations = map[string]string{"example.com/concurrent": "write"}
	if err := live.Update(context.Background(), concurrent); err != nil {
		t.Fatalf("concurrent pod update: %v", err)
	}

	cached := &stalePodListClient{Client: live, pod: stale}
	deps := legacyTestDeps(cached)
	deps.APIReader = live
	input := legacyTestInput(isvc, cached, workload.ComponentEngine)
	markNotReady := false
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: &markNotReady})

	for pass := 1; pass <= 2; pass++ {
		if _, err := Update(context.Background(), deps, input, plan, plan.Instances[0], target, targetSpec); err != nil {
			t.Fatalf("Update pass %d: %v", pass, err)
		}
	}
	got := &corev1.Pod{}
	if err := live.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get patched pod: %v", err)
	}
	if got.Spec.Containers[0].Image != "llama:v2" {
		t.Fatalf("image = %q, want llama:v2", got.Spec.Containers[0].Image)
	}
	if got.Annotations["example.com/concurrent"] != "write" {
		t.Fatal("concurrent annotation was lost")
	}
}

// TestUpdateInPlace_InvalidPatch_DisposesAndHoldsRevision: when the
// apiserver refuses the in-place patch as Invalid, the target revision
// cannot be applied to a live pod at all — the same permanent, workload-
// caused answer a rejected create gets. The attempt is disposed
// (Operation cleared, Phase=Failed) and the revision is held for retry,
// instead of the patch being retried on blind backoff until the
// operation deadline reports a misleading timeout.
func TestUpdateInPlace_InvalidPatch_DisposesAndHoldsRevision(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	spec := legacyTargetSpecImage("example.com/app:v2")
	pod := legacyRunningPodAtRevision(isvc, 0, 1, "example.com/app:v1")

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, v1beta1.AddToScheme, discoveryv1.AddToScheme, appsv1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("build scheme: %v", err)
		}
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, ir, pod).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if p, ok := obj.(*corev1.Pod); ok {
					return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, p.Name, field.ErrorList{
						field.Invalid(field.NewPath("spec", "containers").Index(0).Child("image"), "example.com/app:v2", "must be a valid image reference"),
					})
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	legacySeedRunningRevisionWithMeta(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("example.com/app:v1"), nil)
	target := legacyEnsureTargetCR(t, c, isvc, spec)

	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = target.Name
	var writes []struct {
		rev   string
		block workload.RetryBlock
	}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		if d := mutate(&b); d != workload.RetryBlockUnchanged {
			writes = append(writes, struct {
				rev   string
				block workload.RetryBlock
			}{rev: rev, block: b})
		}
		return nil
	}
	markNotReady := false
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: &markNotReady})

	done, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], target, spec)
	if err != nil {
		t.Fatalf("Update: %v (a permanent rejection is disposed, not returned)", err)
	}
	if done {
		t.Fatal("Update reported done on a rejected patch")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Fatalf("instance 0: got phase=%q op=%+v want Failed with the operation cleared", s.Phase, s.Operation)
	}
	if s.LastFailure == nil || s.LastFailure.Reason != workload.RejectionReasonInvalidPodSpec {
		t.Fatalf("LastFailure: got %+v want reason=%s", s.LastFailure, workload.RejectionReasonInvalidPodSpec)
	}
	if len(writes) != 1 || writes[0].rev != target.Name {
		t.Fatalf("RetryBlock writes: got %+v want one for %s", writes, target.Name)
	}
}

// The in-place writers: each patch is issued only when the pod actually
// owes the change, and each carries the optimistic precondition that
// makes a concurrent mutation lose rather than silently win.

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

func TestPatchPodImagesRejectsStaleResourceVersion(t *testing.T) {
	isvc, _ := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	pod := legacyPodAtIncarnation(isvc, 0, 1, true, true)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v1"}}
	c := legacyNewFakeClient(t, pod)

	stale := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), stale); err != nil {
		t.Fatalf("get stale pod: %v", err)
	}
	concurrent := stale.DeepCopy()
	concurrent.Annotations = map[string]string{"example.com/concurrent": "write"}
	if err := c.Update(context.Background(), concurrent); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	issued, err := patchPodImages(context.Background(), c, stale, &corev1.PodSpec{
		Containers: []corev1.Container{{Name: "main", Image: "llama:v2"}},
	})
	if !apierrors.IsConflict(err) {
		t.Fatalf("patchPodImages error = %v, want conflict", err)
	}
	if issued {
		t.Fatal("conflicted image patch reported issued=true")
	}
}

// Tests for the minReadySeconds pacing gate: each strategy holds its
// budget-releasing step until the new pods have been Ready for the window.

// convergedInPlaceFixture seeds an in-place update whose only remaining step
// is promotion: the pod is serving, already relabeled to the target revision,
// runs the target image, and the target revision's routed Service still
// lists it as ready, so any re-drain would park the pass at the drain check.
type convergedInPlaceFixture struct {
	isvc   *v1beta1.InferenceService
	client client.Client
	pod    *corev1.Pod
	target *corev1.PodSpec
	tcr    *appsv1.ControllerRevision
	plan   workload.ComponentPlan
}

func newConvergedInPlaceFixture(t *testing.T) convergedInPlaceFixture {
	t.Helper()
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
	pod := legacyPodAtIncarnation(isvc, 0, 1, true /* ready */, false /* drained */)
	pod.Spec.Containers = []corev1.Container{{Name: "main", Image: "llama:v2"}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", Image: "llama:v2"}}
	podReadyAt(pod, minReadyWindowStart.Add(-5*time.Second))
	c := legacyNewFakeClient(t, isvc, ir, pod)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, target)
	legacyStampPodRevisionHash(t, c, pod, tcr.Name)
	targetService := query.PerRevisionServiceName(isvc.Name, workload.ComponentEngine, query.RevisionHashFromControllerRevisionName(tcr.Name))
	if err := c.Create(context.Background(), legacySliceWithEndpoint(isvc.Namespace, "engine-target", targetService, pod, true)); err != nil {
		t.Fatalf("seed target EndpointSlice: %v", err)
	}
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})
	return convergedInPlaceFixture{isvc: isvc, client: c, pod: pod, target: target, tcr: tcr, plan: plan}
}

func (f convergedInPlaceFixture) update(t *testing.T, clk *clocktesting.FakeClock) bool {
	t.Helper()
	input := legacyTestInput(f.isvc, f.client, workload.ComponentEngine)
	input.Clock = clk
	done, err := Update(context.Background(), legacyTestDeps(f.client), input, f.plan, f.plan.Instances[0], f.tcr, f.target)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	return done
}

func (f convergedInPlaceFixture) podServing(t *testing.T) bool {
	t.Helper()
	got := &corev1.Pod{}
	if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	return podreadiness.IsServing(got)
}

// TestInPlaceUpdate_PromotionWaitsForMinReadySeconds: a converged in-place
// pod is returned to rotation and then held at Phase=Updating until it has
// been Ready for the window. The waiting passes must not drain the pod
// again — an EndpointSlice that still lists it as ready would then park the
// rollout at the drain check forever — so the pod stays serving throughout.
func TestInPlaceUpdate_PromotionWaitsForMinReadySeconds(t *testing.T) {
	f := newConvergedInPlaceFixture(t)
	f.plan.MinReadySeconds = 20
	clk := clocktesting.NewFakeClock(minReadyWindowStart)

	if f.update(t, clk) {
		t.Fatalf("expected done=false while the patched pod is inside the minReadySeconds window")
	}
	if !f.podServing(t) {
		t.Fatalf("converged pod must return to rotation before the window starts counting")
	}
	s := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Fatalf("promoted inside the window: phase=%q", s.Phase)
	}

	clk.SetTime(minReadyWindowStart.Add(15 * time.Second))
	if !f.update(t, clk) {
		t.Fatalf("expected done=true once the patched pod became Available (a re-drain would park at the drain check)")
	}
	if !f.podServing(t) {
		t.Fatalf("pod must stay in rotation across the wait")
	}
	s = legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady || s.RunningRevision != f.tcr.Name || s.Operation != nil {
		t.Fatalf("promotion after window: phase=%q runningRevision=%q operation=%+v", s.Phase, s.RunningRevision, s.Operation)
	}
}

// TestInPlaceUpdate_ZeroWindowPromotionRetryDoesNotRedrain: with no window,
// a pass that finds the pod already relabeled to the target revision and
// serving (promotion did not persist on an earlier pass) promotes it as-is.
// Re-draining a converged pod would take capacity out of rotation for
// nothing and, with the routed Service still listing it, never observe it
// drained.
func TestInPlaceUpdate_ZeroWindowPromotionRetryDoesNotRedrain(t *testing.T) {
	f := newConvergedInPlaceFixture(t)
	clk := clocktesting.NewFakeClock(minReadyWindowStart)

	if !f.update(t, clk) {
		t.Fatalf("expected done=true: a converged, serving pod promotes without a window")
	}
	if !f.podServing(t) {
		t.Fatalf("converged pod must not be drained on the promotion pass")
	}
	s := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady || s.RunningRevision != f.tcr.Name || s.Operation != nil {
		t.Fatalf("promotion: phase=%q runningRevision=%q operation=%+v", s.Phase, s.RunningRevision, s.Operation)
	}
}

// TestInPlaceUpdate_PromotionKeepsTheRuntimeImageCheck: the in-place roll
// carries one requirement on top of the shared promote bar — every container
// whose image the roll changed must report the target image at runtime. A pod
// that is PodReady with no window left still waits while its runtime reports
// the image it was rolled off.
func TestInPlaceUpdate_PromotionKeepsTheRuntimeImageCheck(t *testing.T) {
	f := newConvergedInPlaceFixture(t)
	clk := clocktesting.NewFakeClock(minReadyWindowStart)

	setRuntimeImage := func(image string) {
		t.Helper()
		live := &corev1.Pod{}
		if err := f.client.Get(context.Background(), client.ObjectKeyFromObject(f.pod), live); err != nil {
			t.Fatalf("get converged pod: %v", err)
		}
		live.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", Image: image}}
		if err := f.client.Status().Update(context.Background(), live); err != nil {
			t.Fatalf("set runtime image %q: %v", image, err)
		}
	}

	setRuntimeImage("llama:v1")
	if f.update(t, clk) {
		t.Fatal("promoted while the runtime still reported the pre-roll image")
	}
	s := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceUpdating {
		t.Fatalf("row left Update on a stale runtime image: phase=%q", s.Phase)
	}

	setRuntimeImage("llama:v2")
	if !f.update(t, clk) {
		t.Fatal("expected the promote once the runtime reported the target image")
	}
	s = legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceReady || s.RunningRevision != f.tcr.Name {
		t.Fatalf("promote: phase=%q runningRevision=%q", s.Phase, s.RunningRevision)
	}
}

// TestUpdate_InPlacePausedCompletesThePatch: an in-place patch is one
// step, so a pause taken while it runs does not interrupt it — the
// images reach the target rather than the Instance being left with a
// half-applied revision.
func TestUpdate_InPlacePausedCompletesThePatch(t *testing.T) {
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
	slice := legacySliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-headless", pod, false)
	c := legacyNewFakeClient(t, isvc, ir, pod, slice)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	input := legacyTestInput(isvc, c, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible,
		&workload.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: legacyBoolPtr(true)})
	plan.Paused = true
	tcr := legacyEnsureTargetCR(t, c, isvc, target)

	for pass := 1; pass <= 2; pass++ {
		if _, err := Update(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr, target); err != nil {
			t.Fatalf("Update pass %d under pause: %v", pass, err)
		}
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got.Spec.Containers[0].Image != "llama:v2" {
		t.Errorf("image under pause: got %q want llama:v2 (the patch step must finish)", got.Spec.Containers[0].Image)
	}
}

// The two non-surge update mechanisms — the in-place image patch and the
// recreate roll — share one shape: a single step converging on the target
// revision, with everything else that can happen to the row while it runs
// being either inert or a plain wait. These tests pin that half of the
// contract: what an observation or an operator edit does NOT move.

// TestUpdateInPlace_PodObservationsThatDecideNothing: the in-place step
// promotes on one thing only — every changed image confirmed from
// container status on a pod that is PodReady past its availability
// window. Everything else the kubelet can report about that pod either
// is the patch taking effect or is a wait: none of them ends the attempt,
// bumps the Incarnation, or moves the step off the patch. The roll is
// bounded by its operation deadline, not by any of these readings.
func TestUpdateInPlace_PodObservationsThatDecideNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		observe func(t *testing.T, f *nonSurgeFixture) *corev1.Pod
	}{
		{
			name: "a container restarting under the patch",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name: "main", Image: "llama:v1", RestartCount: 3,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				}}
				return pod
			},
		},
		{
			name: "the pod wedged Terminating from outside",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				return terminatingPod(t, f.c, f.pod)
			},
		},
		{
			name: "the node hosting the pod gone",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Spec.NodeName = "node-that-no-longer-exists"
				return pod
			},
		},
		{
			name: "the pod evicted",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Status.Phase = corev1.PodFailed
				pod.Status.Reason = "Evicted"
				return pod
			},
		},
		{
			name: "the pod in a terminal Failed phase",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Status.Phase = corev1.PodFailed
				return pod
			},
		},
		{
			name: "the pod in a terminal Succeeded phase",
			observe: func(t *testing.T, f *nonSurgeFixture) *corev1.Pod {
				pod := f.pod.DeepCopy()
				pod.Status.Phase = corev1.PodSucceeded
				return pod
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := inPlaceInFlightFixture(t)
			pod := tc.observe(t, f)
			got := f.run(t, []*corev1.Pod{pod}, nil)

			if got.done {
				t.Errorf("done: got true want false (none of these is the promote signal)")
			}
			if got.phase != v1beta1.OMENativeInstanceUpdating {
				t.Errorf("Phase: got %q want Updating", got.phase)
			}
			if got.step != workload.UpdateStepInPlace {
				t.Errorf("Operation.Step: got %q want %q", got.step, workload.UpdateStepInPlace)
			}
			if got.incarnation != 1 {
				t.Errorf("Incarnation: got %d want 1 (in-place keeps the materialization)", got.incarnation)
			}
			if got.failed {
				t.Errorf("LastFailure: got one want none; only the operation deadline ends this roll")
			}
			if got.blocks != 0 {
				t.Errorf("RetryBlock writes: got %d want none", got.blocks)
			}
		})
	}
}

// TestUpdateInPlace_OperatorConfigChangesDoNotChangeThePatch: every knob
// below is re-read per reconcile, and each one has its reader somewhere
// else — the admission gate, the scale-down pacing, the PodGroup build,
// the migration admission, the end-of-pass prune, the wake-up cadence. A
// patch already converging reads none of them, so editing any of them
// leaves the row and its pod exactly as the unedited pass does.
func TestUpdateInPlace_OperatorConfigChangesDoNotChangeThePatch(t *testing.T) {
	baseline := inPlaceInFlightFixture(t)
	want := baseline.run(t, []*corev1.Pod{baseline.pod}, nil)

	for _, tc := range configChangeCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := inPlaceInFlightFixture(t)
			got := f.run(t, []*corev1.Pod{f.pod}, tc.tweak)
			if got != want {
				t.Errorf("the %s edit moved the roll: got %+v want the unedited %+v", tc.name, got, want)
			}
		})
	}
}

// TestUpdateWithPods_InPlaceRollLosingItsPodTwiceReDerivesTheSameRecreate:
// the re-resolution is a step restamp on one operation, not a new
// attempt, so re-observing the same empty pod set commits nothing
// further. The second pass lands on the recreate, which owns the empty
// set from there and rebuilds under the same name at the same
// Incarnation.
func TestUpdateWithPods_InPlaceRollLosingItsPodTwiceReDerivesTheSameRecreate(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, op := inPlaceRollWithoutPods(t)
	targetSpec := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, targetSpec)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	recorder := record.NewFakeRecorder(32)
	deps := workload.Deps{Client: c, Recorder: recorder}

	// The boundary pass: the loss is observed and the step is restamped
	// to the recreate, which renders the replacement.
	if _, err := UpdateWithPods(context.Background(), deps, legacyTestInput(isvc, c, workload.ComponentEngine),
		plan, plan.Instances[0], tcr, targetSpec, nil); err != nil {
		t.Fatalf("UpdateWithPods (boundary): %v", err)
	}
	first := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if first.Operation == nil || first.Operation.Step != workload.UpdateStepDrain {
		t.Fatalf("boundary pass: got %+v want the step restamped to %s", first.Operation, workload.UpdateStepDrain)
	}
	events := len(recorder.Events)

	// Delete what it rendered and hand the next pass the same empty set.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	for i := range pods.Items {
		if err := c.Delete(context.Background(), &pods.Items[i]); err != nil {
			t.Fatalf("delete the replacement: %v", err)
		}
	}
	legacyResetExpectations(t)
	if _, err := UpdateWithPods(context.Background(), deps, legacyTestInput(isvc, c, workload.ComponentEngine),
		plan, plan.Instances[0], tcr, targetSpec, nil); err != nil {
		t.Fatalf("UpdateWithPods (after): %v", err)
	}

	second := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if second.Operation == nil || second.Operation.Step != workload.UpdateStepDrain {
		t.Fatalf("after pass: got %+v want the recreate still owning the row", second.Operation)
	}
	if second.Operation.ID != op.ID {
		t.Errorf("Operation.ID: got %q want %q (one attempt across both losses)", second.Operation.ID, op.ID)
	}
	if second.Incarnation != first.Incarnation {
		t.Errorf("Incarnation: got %d want %d (a re-observed loss re-materializes nothing new)",
			second.Incarnation, first.Incarnation)
	}
	if len(recorder.Events) != events {
		t.Errorf("events: got %d want %d; the re-derived write announces nothing", len(recorder.Events), events)
	}
}

// TestUpdateWithPods_InPlaceReadinessAndRetargetAfterThePodLoss: once the
// pod loss has moved the row to the recreate, the in-place promote is not
// what finishes the roll — the readiness of a pod the patch was aimed at
// cannot commit it, and a further revision move is decided by the
// recreate's own stamp rather than by the patch's.
func TestUpdateWithPods_InPlaceReadinessAndRetargetAfterThePodLoss(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir, _ := inPlaceRollWithoutPods(t)
	targetSpec := legacyTargetSpecImage("llama:v2")
	c := legacyNewFakeClient(t, isvc, ir)
	legacySeedRunningRevision(t, c, isvc, workload.ComponentEngine, 0, legacyTargetSpecImage("llama:v1"))
	tcr := legacyEnsureTargetCR(t, c, isvc, targetSpec)
	plan := legacyComponentPlan(workload.UpdateStrategyInPlaceIfPossible, nil)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(32)}

	// Boundary: the empty set re-resolves the roll and renders the
	// replacement. The pod that reported ready is gone, so nothing it
	// said can promote the patch being retired.
	done, err := UpdateWithPods(context.Background(), deps, legacyTestInput(isvc, c, workload.ComponentEngine),
		plan, plan.Instances[0], tcr, targetSpec, nil)
	if err != nil {
		t.Fatalf("UpdateWithPods (boundary): %v", err)
	}
	if done {
		t.Fatal("the re-resolution pass must not promote")
	}
	bumped := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0].Incarnation

	// After: the row is on the recreate. The replacement reporting
	// ContainersReady is judged by the recreate's own bar, and a second
	// revision move falls through the recreate stamp's idempotency guard
	// — another Incarnation bump, not an in-place retarget.
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected one replacement, got %d", len(pods.Items))
	}
	replacement := &pods.Items[0]
	replacement.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
	}
	if err := c.Status().Update(context.Background(), replacement); err != nil {
		t.Fatalf("mark the replacement ContainersReady: %v", err)
	}
	nextSpec := legacyTargetSpecImage("llama:v3")
	nextCR := legacyEnsureTargetCR(t, c, isvc, nextSpec)

	legacyResetExpectations(t)
	next := legacyTestInput(isvc, c, workload.ComponentEngine)
	next.ObservedState.UpdateRevision = nextCR.Name
	done, err = UpdateWithPods(context.Background(), deps, next, plan, plan.Instances[0], nextCR, nextSpec,
		[]*corev1.Pod{replacement})
	if err != nil {
		t.Fatalf("UpdateWithPods (after): %v", err)
	}
	if done {
		t.Fatal("a retarget must not complete the roll")
	}

	s := legacyInstanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.Step != workload.UpdateStepDrain {
		t.Fatalf("Operation: got %+v want the recreate still owning the row", s.Operation)
	}
	if s.Operation.TargetRevision != nextCR.Name {
		t.Errorf("Operation.TargetRevision: got %q want %q", s.Operation.TargetRevision, nextCR.Name)
	}
	if s.Incarnation <= bumped {
		t.Errorf("Incarnation: got %d want a further bump past %d (the recreate restarts Phase A)", s.Incarnation, bumped)
	}
}
