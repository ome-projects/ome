package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	v1beta1testing "sigs.k8s.io/ome/pkg/utils/testing/v1beta1"
)

const (
	refsNamespace    = "team-a"
	refsReplicaName  = "pool-a"
	refsModelName    = "model-a"
	refsRuntimeName  = "runtime-a"
	refsClassName    = "accelerator-a"
	refsRuntimeImage = "example.com/serving:v1"
)

// refsCatalog is what a refs replica renders against: a cluster model on a
// PVC and a cluster runtime whose engine piece carries the runner and whose
// pod spec carries a node selector. The runtime declares an accelerator class
// the catalog does not hold: a replica selects no class, so none is read, and
// the runtime's pod spec places the pods.
func refsCatalog() (*v1beta1.ClusterBaseModel, *v1beta1.ClusterServingRuntime) {
	model := v1beta1testing.MakeClusterBaseModel(refsModelName).
		StorageURI("pvc://models/model-a").ModelFormat("safetensors").ModelFramework("Transformers").Obj()
	rt := v1beta1testing.MakeClusterServingRuntime(refsRuntimeName).
		SupportsModelFormat("safetensors", true, 10).
		EngineRunner(corev1.Container{Name: "ome-container", Image: refsRuntimeImage}).Obj()
	rt.Spec.ServingRuntimePodSpec.NodeSelector = map[string]string{"accelerator": "type-a"}
	rt.Spec.AcceleratorRequirements = &v1beta1.AcceleratorRequirements{AcceleratorClasses: []string{refsClassName}}
	return model, rt
}

// refsReplica is a standalone engine replica naming the catalog's model and
// runtime.
func refsReplica() *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: refsReplicaName, Namespace: refsNamespace, UID: types.UID(refsReplicaName + "-uid"), Generation: 1,
			Labels: map[string]string{"team": "a"},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			Component:  v1beta1.EngineComponent,
			Replicas:   ptr.To(int32(1)),
			ModelRef:   &v1beta1.ModelRef{Name: refsModelName},
			RuntimeRef: &v1beta1.ServingRuntimeRef{Name: refsRuntimeName},
		},
	}
}

// renderingReconciler is newReconciler with what rendering from refs needs:
// the operator ConfigMap behind a fake clientset, the runtime selector over
// the fake client, and a recorder.
func renderingReconciler(t *testing.T, objs ...client.Object) (*Reconciler, client.Client, *record.FakeRecorder) {
	t.Helper()
	r, c := newReconciler(t, objs...)
	r.Clientset = kubefake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace,
	}})
	r.RuntimeSelector = runtimeselector.New(c)
	rec := record.NewFakeRecorder(16)
	r.Recorder = rec
	return r, c, rec
}

// blockedRender asserts err is a *renderBlocked with the reason and returns it.
func blockedRender(t *testing.T, err error, reason string) *renderBlocked {
	t.Helper()
	var blocked *renderBlocked
	if !errors.As(err, &blocked) {
		t.Fatalf("error = %v, want a *renderBlocked with reason %s", err, reason)
	}
	if blocked.Reason != reason {
		t.Fatalf("blocked reason = %s (%s), want %s", blocked.Reason, blocked.Message, reason)
	}
	if blocked.Message == "" {
		t.Fatalf("blocked %s carries no message", reason)
	}
	return blocked
}

func envValue(container corev1.Container, name string) (string, bool) {
	for _, env := range container.Env {
		if env.Name == name {
			return env.Value, true
		}
	}
	return "", false
}

func TestRenderFromRefs_RendersTheRuntimePieceForTheModel(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	r, c, rec := renderingReconciler(t, model, rt, ir)

	runners, err := r.renderFromRefs(context.Background(), ir)
	if err != nil {
		t.Fatalf("renderFromRefs: %v", err)
	}
	if len(runners) != 1 || runners[0].Name != v1beta1.RunnerNameDefault || runners[0].Size != 1 {
		t.Fatalf("runners = %+v, want one default runner of size 1", runners)
	}
	if events := drainEvents(rec); len(events) != 0 {
		t.Errorf("events = %v, want none for a runtime that declares the model", events)
	}
	pod := runners[0].Template.Spec
	if len(pod.Containers) != 1 || pod.Containers[0].Image != refsRuntimeImage {
		t.Fatalf("containers = %+v, want the runtime's runner image %s", pod.Containers, refsRuntimeImage)
	}
	if path, ok := envValue(pod.Containers[0], constants.ModelPathEnvVarKey); !ok || path == "" {
		t.Errorf("env %s = %q (%v), want the model path", constants.ModelPathEnvVarKey, path, ok)
	}
	if pod.NodeSelector["accelerator"] != "type-a" {
		t.Errorf("nodeSelector = %v, want the runtime pod spec's node selector", pod.NodeSelector)
	}
	labels := runners[0].Template.Labels
	if labels[constants.InferenceServicePodLabelKey] != refsReplicaName || labels[constants.OMEComponentLabel] != string(v1beta1.EngineComponent) {
		t.Errorf("template labels = %v, want the replica's name prefix and component", labels)
	}
	if labels["team"] != "a" {
		t.Errorf("template labels = %v, want the replica's own labels carried over", labels)
	}

	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Spec.Runners) != 0 || len(ir.Spec.Runners) != 0 || stored.Spec.ModelRef == nil || stored.Spec.RuntimeRef == nil {
		t.Errorf("the rendered runners reached the replica spec: stored=%+v in-memory=%+v", stored.Spec.Runners, ir.Spec.Runners)
	}
}

func TestRenderFromRefs_SelectsTheRuntimeForTheModel(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	ir.Spec.RuntimeRef = nil
	r, _, _ := renderingReconciler(t, model, rt, ir)

	runners, err := r.renderFromRefs(context.Background(), ir)
	if err != nil {
		t.Fatalf("renderFromRefs: %v", err)
	}
	if len(runners) != 1 || runners[0].Template.Spec.Containers[0].Image != refsRuntimeImage {
		t.Fatalf("runners = %+v, want the selected runtime's runner", runners)
	}
}

func TestRenderFromRefs_RendersARuntimeWithoutAModel(t *testing.T) {
	_, rt := refsCatalog()
	ir := refsReplica()
	ir.Spec.ModelRef = nil
	r, _, _ := renderingReconciler(t, rt, ir)

	runners, err := r.renderFromRefs(context.Background(), ir)
	if err != nil {
		t.Fatalf("renderFromRefs: %v", err)
	}
	if len(runners) != 1 || runners[0].Template.Spec.Containers[0].Image != refsRuntimeImage {
		t.Fatalf("runners = %+v, want the runtime's runner", runners)
	}
	if _, ok := envValue(runners[0].Template.Spec.Containers[0], constants.ModelPathEnvVarKey); ok {
		t.Errorf("a replica without a model rendered %s", constants.ModelPathEnvVarKey)
	}
}

func TestRenderFromRefs_ReportsTheRuntimePieceMissing(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	ir.Spec.Component = v1beta1.DecoderComponent
	r, _, _ := renderingReconciler(t, model, rt, ir)

	_, err := r.renderFromRefs(context.Background(), ir)
	blocked := blockedRender(t, err, ReasonRuntimePieceMissing)
	if want := `ClusterServingRuntime "runtime-a" has no decoder piece`; !strings.Contains(blocked.Message, want) {
		t.Errorf("message = %q, want it to contain %q", blocked.Message, want)
	}
}

func TestRenderFromRefs_ReportsTheRuntimeNotFound(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	ir.Spec.RuntimeRef.Name = "runtime-b"
	r, _, _ := renderingReconciler(t, model, rt, ir)

	_, err := r.renderFromRefs(context.Background(), ir)
	blockedRender(t, err, ReasonRuntimeNotFound)
}

func TestRenderFromRefs_ReportsTheModelNotFound(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	ir.Spec.ModelRef.Name = "model-b"
	r, _, _ := renderingReconciler(t, model, rt, ir)

	_, err := r.renderFromRefs(context.Background(), ir)
	blockedRender(t, err, ReasonModelNotFound)
}

func TestRenderFromRefs_ReportsALoadingShardedModel(t *testing.T) {
	model, rt := refsCatalog()
	model.Spec.Distribution = ptr.To(v1beta1.DistributionSharded)
	ir := refsReplica()
	r, _, _ := renderingReconciler(t, model, rt, ir)

	_, err := r.renderFromRefs(context.Background(), ir)
	blocked := blockedRender(t, err, ReasonModelNotFound)
	if !strings.Contains(blocked.Message, "not ready") {
		t.Errorf("message = %q, want it to say the model is not ready", blocked.Message)
	}
}

func TestRenderFromRefs_ReportsADisabledModel(t *testing.T) {
	model, rt := refsCatalog()
	model.Spec.Disabled = ptr.To(true)
	ir := refsReplica()
	r, _, _ := renderingReconciler(t, model, rt, ir)

	_, err := r.renderFromRefs(context.Background(), ir)
	blockedRender(t, err, ReasonModelDisabled)
}

func TestRenderFromRefs_ReportsADisabledRuntime(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withModel bool
	}{
		{name: "named with a model", withModel: true},
		{name: "named without a model", withModel: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model, rt := refsCatalog()
			rt.Spec.Disabled = ptr.To(true)
			ir := refsReplica()
			if !tc.withModel {
				ir.Spec.ModelRef = nil
			}
			r, _, _ := renderingReconciler(t, model, rt, ir)

			_, err := r.renderFromRefs(context.Background(), ir)
			blockedRender(t, err, ReasonRuntimeDisabled)
		})
	}
}

func TestRenderFromRefs_ReportsNoRuntimeForTheModel(t *testing.T) {
	model, rt := refsCatalog()
	rt.Spec.SupportedModelFormats = nil
	ir := refsReplica()
	ir.Spec.RuntimeRef = nil
	r, _, _ := renderingReconciler(t, model, rt, ir)

	_, err := r.renderFromRefs(context.Background(), ir)
	blockedRender(t, err, ReasonRuntimeSelectionFailed)
}

func TestRenderFromRefs_WarnsWhenTheNamedRuntimeDoesNotDeclareTheModel(t *testing.T) {
	model, rt := refsCatalog()
	rt.Spec.SupportedModelFormats = []v1beta1.SupportedModelFormat{{Name: "other", ModelFormat: &v1beta1.ModelFormat{Name: "other"}, AutoSelect: ptr.To(true)}}
	ir := refsReplica()
	r, _, rec := renderingReconciler(t, model, rt, ir)

	runners, err := r.renderFromRefs(context.Background(), ir)
	if err != nil {
		t.Fatalf("renderFromRefs: %v", err)
	}
	if len(runners) != 1 {
		t.Fatalf("runners = %+v, want the named runtime rendered despite the declared-format mismatch", runners)
	}
	events := drainEvents(rec)
	if len(events) != 1 || !strings.Contains(events[0], EventReasonRuntimeCompatibilityAdvisory) {
		t.Errorf("events = %v, want one %s warning", events, EventReasonRuntimeCompatibilityAdvisory)
	}
}

func TestRenderFromRefs_ReportsAnUnconfiguredController(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	r, _, _ := renderingReconciler(t, model, rt, ir)
	r.RuntimeSelector = nil

	_, err := r.renderFromRefs(context.Background(), ir)
	blocked := blockedRender(t, err, ReasonRenderFailed)
	if !strings.Contains(blocked.Message, "not configured") {
		t.Errorf("message = %q, want it to say rendering is not configured", blocked.Message)
	}
}

func TestRenderFromRefs_ReportsAMissingTemplateSource(t *testing.T) {
	ir := refsReplica()
	ir.Spec.ModelRef, ir.Spec.RuntimeRef = nil, nil
	r, _ := newReconciler(t, ir)

	_, err := r.renderFromRefs(context.Background(), ir)
	blockedRender(t, err, ReasonTemplateSourceMissing)
}

func TestRenderFromRefs_LeavesARunnersReplicaAlone(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	r, _ := newReconciler(t, ir)

	runners, err := r.renderFromRefs(context.Background(), ir)
	if err != nil || runners != nil {
		t.Fatalf("renderFromRefs(runners replica) = %v, %v; want nil, nil", runners, err)
	}
}

// A refs replica runs the rendered templates: the reconcile creates its pods
// from the runtime's runner and its revision, and stores no runners.
func TestReconcile_RefsReplicaRunsTheRenderedTemplates(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	r, c, _ := renderingReconciler(t, model, rt, ir)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Spec.Containers[0].Image != refsRuntimeImage {
		t.Fatalf("pods = %v, want one pod running %s", podNames(pods.Items), refsRuntimeImage)
	}
	if pods.Items[0].Labels[constants.InferenceServicePodLabelKey] != refsReplicaName {
		t.Errorf("pod labels = %v, want the replica's name prefix", pods.Items[0].Labels)
	}
	revisions := &appsv1.ControllerRevisionList{}
	if err := c.List(context.Background(), revisions, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(revisions.Items) != 1 {
		t.Fatalf("revisions = %d, want the rendered template's revision", len(revisions.Items))
	}
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Spec.Runners) != 0 {
		t.Errorf("stored runners = %+v, want none", stored.Spec.Runners)
	}
	if stored.Status.UpdateRevision != revisions.Items[0].Name {
		t.Errorf("updateRevision = %q, want %q", stored.Status.UpdateRevision, revisions.Items[0].Name)
	}
}

// A blocked render publishes Ready=False with the reason and a Warning event,
// returns without error or requeue, and creates nothing. The event is not
// repeated while the block stands; once the runtime exists the replica
// renders.
func TestReconcile_BlockedRenderPublishesReadyFalse(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	r, c, rec := renderingReconciler(t, model, ir)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	result, err := r.Reconcile(context.Background(), req)
	if err != nil || result != (ctrl.Result{}) {
		t.Fatalf("reconcile = %+v, %v; want an empty result without error", result, err)
	}
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), req.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(stored.Status.Conditions, InferenceReplicaConditionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonRuntimeNotFound || cond.ObservedGeneration != ir.Generation || cond.Message == "" {
		t.Fatalf("Ready condition = %+v, want False/%s observed at generation %d", cond, ReasonRuntimeNotFound, ir.Generation)
	}
	if events := drainEvents(rec); len(events) != 1 || !strings.Contains(events[0], ReasonRuntimeNotFound) {
		t.Errorf("events = %v, want one %s warning", events, ReasonRuntimeNotFound)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatal(err)
	}
	revisions := &appsv1.ControllerRevisionList{}
	if err := c.List(context.Background(), revisions, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 || len(revisions.Items) != 0 {
		t.Fatalf("a blocked render created %d pods and %d revisions", len(pods.Items), len(revisions.Items))
	}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if events := drainEvents(rec); len(events) != 0 {
		t.Errorf("events on an unchanged block = %v, want none", events)
	}

	if err := c.Create(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile after the runtime appeared: %v", err)
	}
	if err := c.List(context.Background(), pods, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods after the runtime appeared = %v, want one", podNames(pods.Items))
	}
	if err := c.Get(context.Background(), req.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if cond := apimeta.FindStatusCondition(stored.Status.Conditions, InferenceReplicaConditionReady); cond == nil || cond.Reason == ReasonRuntimeNotFound {
		t.Errorf("Ready condition after the runtime appeared = %+v, want the block cleared", cond)
	}
}

// A stored replica with neither runners nor refs publishes Ready=False and
// creates nothing.
func TestReconcile_NoTemplateSourcePublishesReadyFalse(t *testing.T) {
	ir := refsReplica()
	ir.Spec.ModelRef, ir.Spec.RuntimeRef = nil, nil
	r, c := newReconciler(t, ir)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	result, err := r.Reconcile(context.Background(), req)
	if err != nil || result != (ctrl.Result{}) {
		t.Fatalf("reconcile = %+v, %v; want an empty result without error", result, err)
	}
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), req.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(stored.Status.Conditions, InferenceReplicaConditionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonTemplateSourceMissing {
		t.Fatalf("Ready condition = %+v, want False/%s", cond, ReasonTemplateSourceMissing)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("a replica without a template source created %d pods", len(pods.Items))
	}
}

// A refs replica tears down without rendering: its pods are deleted through
// the delete pipeline even when the runtime and model it named are gone.
func TestTeardown_RefsReplicaDeletesItsPodsWithoutRendering(t *testing.T) {
	ir := refsReplica()
	now := metav1.NewTime(time.Now())
	ir.DeletionTimestamp = &now
	ir.Finalizers = []string{TeardownFinalizer}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady}}
	pod := podForIR(ir, 0, "default", 0, false, false)
	pod.Finalizers = []string{"test.ome.io/hold"}
	r, c, _ := renderingReconciler(t, ir, pod)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	// The first pass admits the teardown, the second dispatches the deletes.
	for pass := 0; pass < 2; pass++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("teardown reconcile (pass %d): %v", pass, err)
		}
	}
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatal(err)
	}
	if got.DeletionTimestamp == nil {
		t.Fatal("teardown of a refs replica left its pod in place")
	}
}

// A refs replica's revision follows the runtime: an edited runner image
// produces a new revision on the next pass without a generation change.
func TestReconcile_RefsReplicaFollowsARuntimeEdit(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	r, c, _ := renderingReconciler(t, model, rt, ir)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	first := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), req.NamespacedName, first); err != nil {
		t.Fatal(err)
	}
	live := &v1beta1.ClusterServingRuntime{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(rt), live); err != nil {
		t.Fatal(err)
	}
	live.Spec.EngineConfig.Runner.Container.Image = "example.com/serving:v2"
	if err := c.Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile after the runtime edit: %v", err)
	}
	second := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), req.NamespacedName, second); err != nil {
		t.Fatal(err)
	}
	if first.Status.UpdateRevision == "" || second.Status.UpdateRevision == first.Status.UpdateRevision {
		t.Fatalf("updateRevision before/after the runtime edit = %q/%q, want a new revision", first.Status.UpdateRevision, second.Status.UpdateRevision)
	}
}

// A refs replica's own object annotations are inherited pod metadata excluded
// from the revision hash, as an InferenceService's are: an annotation edit
// mints no revision, while a runtime image edit still does.
func TestReconcile_RefsReplicaAnnotationEditKeepsItsRevision(t *testing.T) {
	model, rt := refsCatalog()
	ir := refsReplica()
	ir.Annotations = map[string]string{"example.com/owner": "team-a"}
	r, c, _ := renderingReconciler(t, model, rt, ir)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}
	updateRevision := func(step string) string {
		t.Helper()
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("reconcile %s: %v", step, err)
		}
		stored := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), req.NamespacedName, stored); err != nil {
			t.Fatal(err)
		}
		if stored.Status.UpdateRevision == "" {
			t.Fatalf("updateRevision %s is empty", step)
		}
		return stored.Status.UpdateRevision
	}
	revisionCount := func() int {
		t.Helper()
		revisions := &appsv1.ControllerRevisionList{}
		if err := c.List(context.Background(), revisions, client.InNamespace(ir.Namespace)); err != nil {
			t.Fatal(err)
		}
		return len(revisions.Items)
	}

	initial := updateRevision("with the initial annotation")

	live := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), req.NamespacedName, live); err != nil {
		t.Fatal(err)
	}
	live.Annotations["example.com/owner"] = "team-b"
	live.Annotations["example.com/tier"] = "gold"
	if err := c.Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	if got := updateRevision("after the annotation edit"); got != initial || revisionCount() != 1 {
		t.Fatalf("updateRevision after the annotation edit = %q with %d revisions, want %q kept as the only revision", got, revisionCount(), initial)
	}

	liveRuntime := &v1beta1.ClusterServingRuntime{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(rt), liveRuntime); err != nil {
		t.Fatal(err)
	}
	liveRuntime.Spec.EngineConfig.Runner.Container.Image = "example.com/serving:v2"
	if err := c.Update(context.Background(), liveRuntime); err != nil {
		t.Fatal(err)
	}
	if got := updateRevision("after the runtime image edit"); got == initial || revisionCount() != 2 {
		t.Fatalf("updateRevision after the runtime image edit = %q with %d revisions, want a second revision", got, revisionCount())
	}
}

const (
	parityGPU = "example.com/gpu"
	// parityScopeUID scopes both revision hashes: the projected replica scopes
	// its revisions by the InferenceService UID and a standalone replica by
	// its own, so the hashes are comparable only under one UID.
	parityScopeUID = types.UID("scope-uid")
)

// parityCatalog is what both forms render against: a cluster model on a PVC
// and a cluster runtime whose engine runner requests two accelerators and
// whose pod spec carries a node selector and a required affinity term.
func parityCatalog() (*v1beta1.ClusterBaseModel, *v1beta1.ClusterServingRuntime) {
	model := v1beta1testing.MakeClusterBaseModel(refsModelName).
		StorageURI("pvc://models/model-a").
		ModelFormat("safetensors").ModelType("llama").
		ModelArchitecture("LlamaForCausalLM").ModelFramework("Transformers").Obj()
	gpus := corev1.ResourceList{parityGPU: resource.MustParse("2")}
	rt := v1beta1testing.MakeClusterServingRuntime(refsRuntimeName).
		SupportsModelFormat("safetensors", true, 10).
		WithLastFormatArchitecture("LlamaForCausalLM").
		EngineRunner(corev1.Container{
			Name:      "ome-container",
			Image:     refsRuntimeImage,
			Ports:     []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			Resources: corev1.ResourceRequirements{Requests: gpus.DeepCopy(), Limits: gpus},
		})
	rt.Spec.ServingRuntimePodSpec.NodeSelector = map[string]string{"pool": "runtime", "tier": "x"}
	rt.Spec.ServingRuntimePodSpec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "accelerator-zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"zone-a"}}}}}}}}
	return model, rt.Obj()
}

// parityService is an OMENative InferenceService whose engine is the runtime
// piece alone; it names the model and the runtime, and carries a label and an
// annotation of its own.
func parityService() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: refsReplicaName, Namespace: refsNamespace, UID: "service-uid", Generation: 1,
			Labels:      map[string]string{"team": "a"},
			Annotations: map[string]string{"example.com/owner": "team-a"},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Model:          &v1beta1.ModelRef{Name: refsModelName, Kind: ptr.To("ClusterBaseModel")},
			Runtime:        &v1beta1.ServingRuntimeRef{Name: refsRuntimeName, Kind: ptr.To("ClusterServingRuntime")},
			DeploymentMode: ptr.To(constants.OMENative),
			Engine:         &v1beta1.EngineSpec{},
		},
	}
}

// parityReplica is the standalone engine replica that describes the
// service's workload: the same name and namespace (the pods of both carry
// the name as their service label), the same labels and annotations, and
// the service's refs.
func parityReplica(svc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: svc.Name, Namespace: svc.Namespace, UID: types.UID(svc.Name + "-uid"), Generation: svc.Generation,
			Labels: maps.Clone(svc.Labels), Annotations: maps.Clone(svc.Annotations),
		},
		Spec: v1beta1.InferenceReplicaSpec{
			Component:  v1beta1.EngineComponent,
			Replicas:   ptr.To(int32(1)),
			ModelRef:   svc.Spec.Model.DeepCopy(),
			RuntimeRef: svc.Spec.Runtime.DeepCopy(),
		},
	}
}

// parityReconciler is a reconciler over the catalog whose operator ConfigMap
// names the accelerator resource; both forms read their configuration from
// it and resolve their refs through its runtime selector.
func parityReconciler(t *testing.T) *Reconciler {
	t.Helper()
	model, rt := parityCatalog()
	r, c := newReconciler(t, model, rt)
	r.Clientset = kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data:       map[string]string{controllerconfig.AcceleratorResourcesConfigName: fmt.Sprintf("[%q]", parityGPU)},
	})
	r.RuntimeSelector = runtimeselector.New(c)
	return r
}

// renderInline renders the service's engine the way the InferenceService
// controller does (resolve, prepare every declared role, assemble the engine
// piece, render, project) and returns the replica the projector wrote.
func renderInline(t *testing.T, ctx context.Context, r *Reconciler, svc *v1beta1.InferenceService) *v1beta1.InferenceReplica {
	t.Helper()
	cfg, err := controllerconfig.NewInferenceServicesConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		t.Fatal(err)
	}
	deploy, err := controllerconfig.NewDeployConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		t.Fatal(err)
	}
	svc = svc.DeepCopy()
	// The InferenceService controller wires an accelerator class selector;
	// this service names no class, so it picks none.
	in := render.Inputs{Service: svc, Client: r.Client, Runtimes: r.RuntimeSelector, Accelerators: acceleratorclassselector.New(r.Client), Config: cfg, Log: r.Log}
	res, err := render.Resolve(ctx, in)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := render.Prepare(res, svc.Spec.DeploymentMode, deploy); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	piece, _, err := res.Piece(ctx, in, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("engine piece: %v", err)
	}
	rendered, err := render.RenderEngine(ctx, piece, svc, res.Specs.Engine)
	if err != nil {
		t.Fatalf("render engine: %v", err)
	}
	ir, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
		ISVC: svc, Component: v1beta1.EngineComponent, ComponentExt: &res.Specs.Engine.ComponentExtensionSpec,
		ObjectMeta: rendered.ObjectMeta, PodSpec: rendered.Primary, WorkerPodSpec: rendered.Worker, WorkerSize: rendered.WorkerSize,
		MultiPod: rendered.MultiPod, TopologyKey: res.Specs.Engine.TopologyKey, TopologySpread: res.Specs.Engine.TopologySpread, TopologySpreadKey: res.Specs.Engine.TopologySpreadKey,
		Client: r.Client,
	})
	if err != nil {
		t.Fatalf("project engine: %v", err)
	}
	return ir
}

// controllerHash is the revision hash the replica controller derives for a
// replica over the given runners, under the parity scope UID and with no
// collision count: the projected replica's stamped exclusions and a refs
// replica's own annotation keys are applied as the controller applies them.
// Each hash is computed on a fresh Reconciler so the memo cannot mask a
// drift between the two forms.
func controllerHash(t *testing.T, ir *v1beta1.InferenceReplica, runners []v1beta1.Runner) string {
	t.Helper()
	input := workloadtypes.ReconcileInput{DesiredSpec: desiredFromIR(ir, runners)}
	hash, _, err := (&Reconciler{}).revisionHash(ir, input, nil, parityScopeUID)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// The same model, runtime and configuration render to the same runners
// whether an InferenceService projects them onto its replica or a standalone
// replica renders them from its refs, and the replica
// controller derives the same revision hash from either under one scope UID:
// the owner's annotation stays off the pod template on both (the render keeps
// a service key no Component declares off the pods) and is listed as outside
// the hash on both, through the record the projector writes on one and the
// refs rule on the other.
func TestInlineAndRefsRenderingAgree(t *testing.T) {
	ctx := context.Background()
	r := parityReconciler(t)
	svc := parityService()
	replica := parityReplica(svc)

	projected := renderInline(t, ctx, r, svc)
	refsRunners, err := r.renderFromRefs(ctx, replica)
	if err != nil {
		t.Fatalf("render from refs: %v", err)
	}

	if diff := cmp.Diff(mustJSON(t, projected.Spec.Runners), mustJSON(t, refsRunners)); diff != "" {
		t.Fatalf("inline and refs runners differ (-inline +refs):\n%s", diff)
	}
	// The fixture exercises the metadata both forms derive from their owner:
	// the service label from the shared name reaches the template, the
	// owner's own annotation does not.
	template := projected.Spec.Runners[0].Template
	if template.Labels[constants.InferenceServicePodLabelKey] != svc.Name {
		t.Fatalf("template labels = %v, want %s=%s", template.Labels, constants.InferenceServicePodLabelKey, svc.Name)
	}
	if value, ok := template.Annotations["example.com/owner"]; ok {
		t.Fatalf("template annotations = %v; the owner's example.com/owner annotation must stay off the pods, got %q", template.Annotations, value)
	}

	if stamped := projected.Annotations[constants.RevisionExcludedAnnotationKeysAnnotationKey]; stamped != "example.com/owner" {
		t.Fatalf("projected replica's excluded annotation keys = %q, want the owner's example.com/owner annotation", stamped)
	}
	if _, stamped := replica.Annotations[constants.RevisionExcludedAnnotationKeysAnnotationKey]; stamped {
		t.Fatalf("refs replica carries %s; its exclusions are derived in memory", constants.RevisionExcludedAnnotationKeysAnnotationKey)
	}
	inlineHash := controllerHash(t, projected, projected.Spec.Runners)
	refsHash := controllerHash(t, replica, refsRunners)
	if inlineHash != refsHash {
		t.Fatalf("revision hash over identical runners: inline %s != refs %s", inlineHash, refsHash)
	}
}

// coordinatedISVC builds a coordination-enabled, multi-component ISVC: a
// single BlueGreen group binding engine + decoder. Used to exercise the
// peer-env injection hook end-to-end.
func coordinatedISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "prod"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine:  &v1beta1.EngineSpec{},
			Decoder: &v1beta1.DecoderSpec{},
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{
							v1beta1.EngineComponent,
							v1beta1.DecoderComponent,
						},
						BlueGreen: &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	}
}

// decoupledISVC builds a PD ISVC whose engine and decoder are in SEPARATE
// single-Component blueGreen groups — the v2 spelling of "roll engine, then
// decoder, NOT as a coupled unit." They roll independently, but they are still
// SERVING peers (a PD engine needs the decoder's address regardless of how the
// rollout groups them), so peer-env must still be injected.
func decoupledISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "prod"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine:  &v1beta1.EngineSpec{},
			Decoder: &v1beta1.DecoderSpec{},
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
					{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
				},
			},
		},
	}
}

// renderedPod mimics what workload/ops.Render produces: a pod stamped
// with the canonical component label (constants.OMEComponentLabel, whose
// value is "component") for the given component.
func renderedPod(component v1beta1.ComponentType) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				constants.OMEComponentLabel: string(component),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main"},
				{Name: "sidecar"},
			},
		},
	}
}

// pairedWithDecoder is a resolver that pairs every engine revision with
// one fixed decoder revision and records the pod hash it was asked about.
func pairedWithDecoder(decoderHash string, asked *[]string) coordination.PeerRevisionFunc {
	return func(peer v1beta1.ComponentType, podRevisionHash string) string {
		*asked = append(*asked, podRevisionHash)
		if peer == v1beta1.DecoderComponent {
			return decoderHash
		}
		return ""
	}
}

// TestISVCRenderHook_InjectsPeerEnvForCoordinatedComponent pins that the
// hook recovers the component from constants.OMEComponentLabel
// ("component"); reading any other key yields "", an empty peer lookup,
// and no InjectPeerEnv call. This drives a rendered engine pod through the
// live hook and asserts both decoder peer endpoints land on every
// container: the generic one, and the per-revision one naming the decoder
// revision the resolver paired this pod with — NOT the pod's own hash
// (each Component hashes its own template, so llama-decoder-rev-<engineHash>
// never exists).
func TestISVCRenderHook_InjectsPeerEnvForCoordinatedComponent(t *testing.T) {
	isvc := coordinatedISVC()
	var asked []string
	hook := ISVCRenderHook(isvc, "cluster.local", pairedWithDecoder("dec456", &asked))
	if hook == nil {
		t.Fatal("ISVCRenderHook returned nil for a coordination-enabled ISVC")
	}

	pod := renderedPod(v1beta1.EngineComponent)
	hook(pod, "runner-0", 0, "eng123")

	for _, ctr := range pod.Spec.Containers {
		if got, _ := envValue(ctr, "OME_DECODER_ENDPOINT"); got != "llama-decoder.prod.svc.cluster.local" {
			t.Errorf("container %q: OME_DECODER_ENDPOINT = %q, want llama-decoder.prod.svc.cluster.local",
				ctr.Name, got)
		}
		want := coordination.PerRevisionServiceName("llama", v1beta1.DecoderComponent, "dec456") + ".prod.svc.cluster.local"
		if got, _ := envValue(ctr, "OME_DECODER_REVISION_ENDPOINT"); got != want {
			t.Errorf("container %q: OME_DECODER_REVISION_ENDPOINT = %q, want the paired decoder revision Service %q",
				ctr.Name, got, want)
		}
		// The engine never names itself as a peer.
		if _, found := envValue(ctr, "OME_ENGINE_ENDPOINT"); found {
			t.Errorf("container %q: pod should not carry its own component as a peer", ctr.Name)
		}
	}
	for _, h := range asked {
		if h != "eng123" {
			t.Errorf("resolver must be asked about the rendered pod's own revision hash, got %q", h)
		}
	}
	if len(asked) == 0 {
		t.Error("resolver was never consulted")
	}
}

// TestISVCRenderHook_PeerAddressesCarryTheConfiguredClusterDomain: the hook
// renders every peer address under the cluster domain it is given, and the
// namespaced short name when there is none; it never writes a suffix of its
// own.
func TestISVCRenderHook_PeerAddressesCarryTheConfiguredClusterDomain(t *testing.T) {
	for _, tc := range []struct{ clusterDomain, generic, revision string }{
		{"example.org", "llama-decoder.prod.svc.example.org", "llama-decoder-rev-dec456.prod.svc.example.org"},
		{"", "llama-decoder.prod", "llama-decoder-rev-dec456.prod"},
	} {
		var asked []string
		hook := ISVCRenderHook(coordinatedISVC(), tc.clusterDomain, pairedWithDecoder("dec456", &asked))
		pod := renderedPod(v1beta1.EngineComponent)
		hook(pod, "runner-0", 0, "eng123")
		for _, ctr := range pod.Spec.Containers {
			if got, _ := envValue(ctr, "OME_DECODER_ENDPOINT"); got != tc.generic {
				t.Errorf("clusterDomain %q, container %q: OME_DECODER_ENDPOINT = %q, want %q", tc.clusterDomain, ctr.Name, got, tc.generic)
			}
			if got, _ := envValue(ctr, "OME_DECODER_REVISION_ENDPOINT"); got != tc.revision {
				t.Errorf("clusterDomain %q, container %q: OME_DECODER_REVISION_ENDPOINT = %q, want %q", tc.clusterDomain, ctr.Name, got, tc.revision)
			}
		}
	}
}

// TestISVCRenderHook_NoResolverEmitsGenericOnly pins the degraded shape: with
// no resolver the hook must inject only the revision-agnostic endpoint, never
// a per-revision DNS name derived from the pod's own hash.
func TestISVCRenderHook_NoResolverEmitsGenericOnly(t *testing.T) {
	hook := ISVCRenderHook(coordinatedISVC(), "cluster.local", nil)
	pod := renderedPod(v1beta1.EngineComponent)
	hook(pod, "runner-0", 0, "eng123")
	for _, ctr := range pod.Spec.Containers {
		if got, _ := envValue(ctr, "OME_DECODER_ENDPOINT"); got != "llama-decoder.prod.svc.cluster.local" {
			t.Errorf("container %q: OME_DECODER_ENDPOINT = %q", ctr.Name, got)
		}
		if got, _ := envValue(ctr, "OME_DECODER_REVISION_ENDPOINT"); got != "" {
			t.Errorf("container %q: OME_DECODER_REVISION_ENDPOINT = %q, want it absent without a resolver", ctr.Name, got)
		}
	}
}

// TestISVCRenderHook_NilForNonCoordinatedISVC confirms the hook is a
// no-op (nil) when the ISVC declares no rollout coordination, so
// single-component / non-coordinated boxes are unaffected by the wiring.
func TestISVCRenderHook_NilForNonCoordinatedISVC(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "solo", Namespace: "prod"}}
	if ISVCRenderHook(isvc, "cluster.local", nil) != nil {
		t.Error("ISVCRenderHook should return nil when the ISVC has no rolloutCoordination")
	}
	if ISVCRenderHook(nil, "cluster.local", nil) != nil {
		t.Error("ISVCRenderHook(nil) should return nil")
	}
}

// TestISVCRenderHook_EmptyHashStillPairs confirms the empty-hash render
// path still asks the resolver, which pairs it as a target-revision pod.
func TestISVCRenderHook_EmptyHashStillPairs(t *testing.T) {
	var asked []string
	hook := ISVCRenderHook(coordinatedISVC(), "cluster.local", pairedWithDecoder("dec456", &asked))
	pod := renderedPod(v1beta1.EngineComponent)
	hook(pod, "runner-0", 0, "")

	ctr := pod.Spec.Containers[0]
	if got, _ := envValue(ctr, "OME_DECODER_ENDPOINT"); got != "llama-decoder.prod.svc.cluster.local" {
		t.Errorf("generic endpoint missing/wrong: %q", got)
	}
	if got, _ := envValue(ctr, "OME_DECODER_REVISION_ENDPOINT"); got != "llama-decoder-rev-dec456.prod.svc.cluster.local" {
		t.Errorf("revision endpoint = %q, want the paired decoder revision", got)
	}
}

// TestISVCRenderHook_SeparateGroupsStillServingPeers verifies that a PD pair
// placed in SEPARATE rollout groups (rolled one-at-a-time) is STILL wired as
// serving peers. Peer endpoints reflect serving topology — the ISVC's declared
// Components — not rollout grouping, so the engine still gets the decoder's
// endpoint regardless of how the rollout sequences them.
func TestISVCRenderHook_SeparateGroupsStillServingPeers(t *testing.T) {
	var asked []string
	hook := ISVCRenderHook(decoupledISVC(), "cluster.local", pairedWithDecoder("dec456", &asked))
	if hook == nil {
		t.Fatal("hook nil for an ISVC with rollout groups")
	}
	pod := renderedPod(v1beta1.EngineComponent)
	hook(pod, "runner-0", 0, "eng123")
	for _, ctr := range pod.Spec.Containers {
		if got, _ := envValue(ctr, "OME_DECODER_ENDPOINT"); got != "llama-decoder.prod.svc.cluster.local" {
			t.Errorf("container %q: OME_DECODER_ENDPOINT = %q, want the decoder serving peer (rollout grouping must not matter)", ctr.Name, got)
		}
		if got, _ := envValue(ctr, "OME_DECODER_REVISION_ENDPOINT"); got != "llama-decoder-rev-dec456.prod.svc.cluster.local" {
			t.Errorf("container %q: OME_DECODER_REVISION_ENDPOINT = %q, want the paired decoder revision", ctr.Name, got)
		}
	}
}
