package ops

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// testISVCGVK is the GroupVersionKind for InferenceService owner refs
// (duplicated from omenative/core to keep workload/ops free of the
// dispatch-side helpers).
var testISVCGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// testKey produces the ISVC-form workload.Key the render path consumes:
// Namespace + OwnerName from the ISVC, Component cast across the type
// boundary, SelectorLabels = the OMENative pod-selector trio.
func testKey(isvc *v1beta1.InferenceService, component workload.ComponentType) workload.Key {
	return workload.Key{
		Namespace: isvc.Namespace,
		Component: component,
		OwnerName: isvc.Name,
		SelectorLabels: map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// testRender wraps Render with the (isvc, podSpec, plan, inst, runner,
// ordinal) signature test bodies use directly, deriving the
// owner/key/hook arguments from the ISVC under test.
func testRender(isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32) (*corev1.Pod, error) {
	if isvc == nil {
		// Tests rely on Render rejecting a nil owner — pass nil
		// through to exercise that branch.
		return Render(nil, testISVCGVK, workload.Key{}, podSpec, plan, inst, runner, ordinal, nil)
	}
	return Render(isvc, testISVCGVK, testKey(isvc, plan.Component), podSpec, plan, inst, runner, ordinal, nil)
}

// testRenderWithRevision wraps RenderWithRevision with the (isvc,
// podSpec, podTemplateMeta, plan, inst, runner, ordinal, revisionHash)
// signature test bodies use directly.
func testRenderWithRevision(isvc *v1beta1.InferenceService, podSpec *corev1.PodSpec, meta *metav1.ObjectMeta, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32, revisionHash string) (*corev1.Pod, error) {
	if isvc == nil {
		return RenderWithRevision(nil, testISVCGVK, workload.Key{}, podSpec, meta, plan, inst, runner, ordinal, revisionHash, nil)
	}
	return RenderWithRevision(isvc, testISVCGVK, testKey(isvc, plan.Component), podSpec, meta, plan, inst, runner, ordinal, revisionHash, nil)
}

func singlePodPlan() (workload.ComponentPlan, workload.InstancePlan, workload.RunnerPlan) {
	runner := workload.RunnerPlan{Name: "default", Size: 1}
	inst := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{runner}}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  2,
		Instances: []workload.InstancePlan{inst},
	}
	return plan, inst, runner
}

// multiPodPlan returns a 2-Runner Instance shape: 1 leader + workerSize
// workers. Used by the multi-pod render tests.
func multiPodPlan(workerSize int32) (workload.ComponentPlan, workload.InstancePlan, []workload.RunnerPlan) {
	runners := []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: workerSize},
	}
	inst := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: runners}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  1,
		Instances: []workload.InstancePlan{inst},
	}
	return plan, inst, runners
}

// multiPodPlanWithTopologyKey is multiPodPlan plus the resolved gang
// topologyKey the injector reads to co-locate the gang's workers onto their
// Instance's leader.
func multiPodPlanWithTopologyKey(workerSize int32, topologyKey string) (workload.ComponentPlan, workload.InstancePlan, []workload.RunnerPlan) {
	plan, inst, runners := multiPodPlan(workerSize)
	plan.TopologyKey = topologyKey
	return plan, inst, runners
}

func basicISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b",
			Namespace: "prod",
			UID:       types.UID("isvc-uid-1"),
		},
	}
}

func basicPodSpec() *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "runner", Image: "llama:v1"},
		},
	}
}

func TestRender_NilInputsRejected(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	if _, err := testRender(nil, basicPodSpec(), plan, inst, runner, 0); err == nil {
		t.Fatal("expected error for nil ISVC")
	}
	if _, err := testRender(basicISVC(), nil, plan, inst, runner, 0); err == nil {
		t.Fatal("expected error for nil PodSpec")
	}
}

func TestRender_StableNameAndDNS(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pod.Name != "llama-70b-engine-0-default-0" {
		t.Errorf("Name: got %q want llama-70b-engine-0-default-0", pod.Name)
	}
	if pod.Namespace != "prod" {
		t.Errorf("Namespace: got %q want prod", pod.Namespace)
	}
	if pod.Spec.Hostname != "llama-70b-engine-0-default-0" {
		t.Errorf("Hostname: got %q", pod.Spec.Hostname)
	}
	if pod.Spec.Subdomain != "llama-70b-engine-headless" {
		t.Errorf("Subdomain: got %q want llama-70b-engine-headless", pod.Spec.Subdomain)
	}
}

func TestRender_Labels(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           "engine",
		query.LabelInstanceIdx:                "0",
		query.LabelInstanceIncarnation:        "1",
		query.LabelRunner:                     "default",
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelPodOrdinal:                 "0",
	}
	if diff := cmp.Diff(want, pod.Labels); diff != "" {
		t.Fatalf("labels mismatch (-want +got):\n%s", diff)
	}
}

// TestRender_SinglePod_OmitsPodGroupLabel pins that single-pod
// Instances do NOT carry the scheduler-plugins pod-group label. The
// gang-scheduling contract is clear: no PodGroup, no gang — and stamping
// the label without a matching PodGroup would put the scheduler-plugins
// coscheduler into a wait-for-non-existent-gang state.
func TestRender_SinglePod_OmitsPodGroupLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, present := pod.Labels[query.LabelPodGroup]; present {
		t.Errorf("%s must be absent on single-pod Instance, got %q", query.LabelPodGroup, got)
	}
}

// TestRender_MultiPod_StampsPodGroupLabel pins that every pod in a
// multi-pod Instance carries the scheduler-plugins pod-group label
// (`scheduling.x-k8s.io/pod-group=<isvc>-<comp>-<idx>`). The leader
// and every worker must agree on the same PodGroup name so the
// coscheduler treats them as one gang.
func TestRender_MultiPod_StampsPodGroupLabel(t *testing.T) {
	plan, inst, runners := multiPodPlan(2)
	leader := runners[0]
	worker := runners[1]

	leaderPod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, leader, 0)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	worker0, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker 0: %v", err)
	}
	worker1, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, 1)
	if err != nil {
		t.Fatalf("worker 1: %v", err)
	}

	const wantPodGroup = "llama-70b-engine-0"
	for _, p := range []*corev1.Pod{leaderPod, worker0, worker1} {
		got, present := p.Labels[query.LabelPodGroup]
		if !present {
			t.Errorf("%s: pod %s missing %s label, labels=%+v",
				p.Name, p.Name, query.LabelPodGroup, p.Labels)
			continue
		}
		if got != wantPodGroup {
			t.Errorf("%s %s: got %q want %q", p.Name, query.LabelPodGroup, got, wantPodGroup)
		}
	}
}

// TestRender_MultiPod_PodGroupLabelPerInstance pins that the
// pod-group label is per-Instance: Instance 0's pods carry one
// pod-group name, Instance 1's pods carry a different name. Otherwise
// the coscheduler would treat both Instances as one mega-gang and
// hang both until every member of both is schedulable.
func TestRender_MultiPod_PodGroupLabelPerInstance(t *testing.T) {
	runners := []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 1},
	}
	inst0 := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: runners}
	inst1 := workload.InstancePlan{Index: 1, Incarnation: 1, Runners: runners}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  2,
		Instances: []workload.InstancePlan{inst0, inst1},
	}

	inst0Pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst0, runners[1], 0)
	if err != nil {
		t.Fatalf("inst0 worker: %v", err)
	}
	inst1Pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst1, runners[1], 0)
	if err != nil {
		t.Fatalf("inst1 worker: %v", err)
	}

	if inst0Pod.Labels[query.LabelPodGroup] != "llama-70b-engine-0" {
		t.Errorf("inst0 %s: got %q want llama-70b-engine-0",
			query.LabelPodGroup, inst0Pod.Labels[query.LabelPodGroup])
	}
	if inst1Pod.Labels[query.LabelPodGroup] != "llama-70b-engine-1" {
		t.Errorf("inst1 %s: got %q want llama-70b-engine-1",
			query.LabelPodGroup, inst1Pod.Labels[query.LabelPodGroup])
	}
	if inst0Pod.Labels[query.LabelPodGroup] == inst1Pod.Labels[query.LabelPodGroup] {
		t.Errorf("inst0 and inst1 must carry distinct pod-group names; both got %q",
			inst0Pod.Labels[query.LabelPodGroup])
	}
}

func TestRenderWithRevision_StampsRevisionHashLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRenderWithRevision(basicISVC(), basicPodSpec(), nil, plan, inst, runner, 0, "abc12345")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pod.Labels[query.LabelRevisionHash]; got != "abc12345" {
		t.Errorf("%s: got %q want %q", query.LabelRevisionHash, got, "abc12345")
	}
}

func TestRenderWithRevision_EmptyHashOmitsRevisionLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRenderWithRevision(basicISVC(), basicPodSpec(), nil, plan, inst, runner, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, present := pod.Labels[query.LabelRevisionHash]; present {
		t.Errorf("%s should be absent when hash is empty, got: %+v", query.LabelRevisionHash, pod.Labels)
	}
}

func TestRender_DelegatesToRenderWithRevisionEmptyHash(t *testing.T) {
	// Render() is the back-compat entry point and must produce the same
	// pod as testRenderWithRevision(..., "") — i.e., no revision-hash label.
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, present := pod.Labels[query.LabelRevisionHash]; present {
		t.Errorf("Render() must omit %s; got pod labels: %+v", query.LabelRevisionHash, pod.Labels)
	}
}

// TestRenderWithRevision_PropagatesComponentMetaAnnotationsAndLabels pins
// the merge from the upstream-built componentObjectMeta onto the rendered
// pod. Dropping either breaks PD runtimes that need multus IB annotations
// (k8s.v1.cni.cncf.io/networks) to spin up. OME-mandatory labels still
// win on collision so selectors keep working.
func TestRenderWithRevision_PropagatesComponentMetaAnnotationsAndLabels(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	meta := &metav1.ObjectMeta{
		Annotations: map[string]string{
			"k8s.v1.cni.cncf.io/networks": "[{\"name\":\"net0-macvlan\",\"namespace\":\"multus-system\"}]",
			"metrics.example/schema":      "sglang_metrics",
		},
		Labels: map[string]string{
			"team":                                "ml-infra",
			constants.InferenceServicePodLabelKey: "should-be-overwritten-by-OMENative",
		},
	}
	pod, err := testRenderWithRevision(basicISVC(), basicPodSpec(), meta, plan, inst, runner, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pod.Annotations["k8s.v1.cni.cncf.io/networks"]; got == "" {
		t.Errorf("multus networks annotation missing: %+v", pod.Annotations)
	}
	if got := pod.Annotations["metrics.example/schema"]; got != "sglang_metrics" {
		t.Errorf("metrics.example/schema annotation: got %q want %q", got, "sglang_metrics")
	}
	if got := pod.Labels["team"]; got != "ml-infra" {
		t.Errorf("user-declared label dropped: %+v", pod.Labels)
	}
	if got := pod.Labels[constants.InferenceServicePodLabelKey]; got == "should-be-overwritten-by-OMENative" {
		t.Errorf("OMENative selector label was clobbered by user label; got %q (must be the ISVC name)", got)
	}
}

func TestRenderWithRevisionReservesInPlaceImageTransitionAnnotation(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	meta := &metav1.ObjectMeta{Annotations: map[string]string{
		constants.InferenceServiceInPlaceImageTransitionAnnotationKey: "forged",
		"example.com/preserve": "yes",
	}}
	pod, err := testRenderWithRevision(basicISVC(), basicPodSpec(), meta, plan, inst, runner, 0, "")
	if err != nil {
		t.Fatalf("render pod: %v", err)
	}
	if _, found := pod.Annotations[constants.InferenceServiceInPlaceImageTransitionAnnotationKey]; found {
		t.Fatalf("reserved transition annotation reached pod: %#v", pod.Annotations)
	}
	if pod.Annotations["example.com/preserve"] != "yes" {
		t.Fatalf("ordinary annotation was dropped: %#v", pod.Annotations)
	}
}

func TestRender_IncarnationStampedOnLabel(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	inst.Incarnation = 7
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pod.Labels[query.LabelInstanceIncarnation]; got != "7" {
		t.Errorf("%s: got %q want 7", query.LabelInstanceIncarnation, got)
	}
}

func TestRender_OwnerReference(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pod.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(pod.OwnerReferences))
	}
	ref := pod.OwnerReferences[0]
	if ref.Name != "llama-70b" || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef: got Kind=%q Name=%q want InferenceService/llama-70b", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("OwnerRef.Controller: want true, got %v", ref.Controller)
	}
}

func TestRender_AppendsServingReadinessGateWithoutClobberingExisting(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	ps := basicPodSpec()
	ps.ReadinessGates = []corev1.PodReadinessGate{
		{ConditionType: "user.example.com/ready"},
	}
	pod, err := testRender(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pod.Spec.ReadinessGates) != 2 {
		t.Fatalf("ReadinessGates: got %d want 2", len(pod.Spec.ReadinessGates))
	}
	if pod.Spec.ReadinessGates[0].ConditionType != "user.example.com/ready" {
		t.Errorf("user gate clobbered: %+v", pod.Spec.ReadinessGates)
	}
	if pod.Spec.ReadinessGates[1].ConditionType != query.ServingConditionType {
		t.Errorf("ome.io/serving not appended: %+v", pod.Spec.ReadinessGates)
	}
}

func TestRender_DoesNotDuplicateExistingServingGate(t *testing.T) {
	// If the user template already declares ome.io/serving, Render must
	// not append a duplicate — kubelet ANDs all gates and the controller
	// writes only one condition, so a duplicate keeps PodReady=False
	// forever.
	plan, inst, runner := singlePodPlan()
	ps := basicPodSpec()
	ps.ReadinessGates = []corev1.PodReadinessGate{
		{ConditionType: query.ServingConditionType},
	}
	pod, err := testRender(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pod.Spec.ReadinessGates) != 1 {
		t.Fatalf("ReadinessGates duplicated: got %d want 1: %+v", len(pod.Spec.ReadinessGates), pod.Spec.ReadinessGates)
	}
	if pod.Spec.ReadinessGates[0].ConditionType != query.ServingConditionType {
		t.Errorf("preserved gate type wrong: %v", pod.Spec.ReadinessGates[0].ConditionType)
	}
}

func TestRender_OMEEnvVarsInEveryContainer(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Image: "llama:v1"},
			{Name: "sidecar", Image: "metrics:v1"},
		},
	}
	pod, err := testRender(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{
		"OME_INFERENCESERVICE_NAME": "llama-70b",
		"OME_COMPONENT":             "engine",
		"OME_COMPONENT_REPLICAS":    "2",
		"OME_INSTANCE_INDEX":        "0",
		"OME_RUNNER":                "default",
		"OME_RUNNER_SIZE":           "1",
		"OME_RUNNER_INDEX":          "0",
		"OME_INSTANCE_SUBDOMAIN":    "llama-70b-engine-0",
	}
	for ci, c := range pod.Spec.Containers {
		got := envByName(c.Env)
		for k, v := range want {
			if got[k] != v {
				t.Errorf("container[%d].env[%s]: got %q want %q", ci, k, got[k], v)
			}
		}
		// OME_LEADER_ADDRESS only emitted for multi-pod; should not appear here.
		if _, ok := got["OME_LEADER_ADDRESS"]; ok {
			t.Errorf("container[%d].env: OME_LEADER_ADDRESS should not be set for single-pod", ci)
		}
	}
}

// TestRender_MultiPod_LeaderEnvVarsForLeader pins the leader pod's
// OME_RUNNER / OME_RUNNER_SIZE / OME_LEADER_ADDRESS env vars. The leader's
// OME_LEADER_ADDRESS points at itself so leader-side code that reads the
// var to identify the gang head sees the same value as workers.
//
// Address shape: short form <pod-name>.<headless-service>; cluster DNS
// search path resolves it the same as the fully qualified
// <pod>.<headless>.<ns>.svc.<cluster-domain>.
func TestRender_MultiPod_LeaderEnvVarsForLeader(t *testing.T) {
	plan, inst, runners := multiPodPlan(2)
	leader := runners[0]
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, leader, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := envByName(pod.Spec.Containers[0].Env)
	want := map[string]string{
		"OME_RUNNER":             "leader",
		"OME_RUNNER_SIZE":        "1",
		"OME_RUNNER_INDEX":       "0",
		"OME_LEADER_ADDRESS":     "llama-70b-engine-0-leader-0.llama-70b-engine-headless",
		"OME_INSTANCE_INDEX":     "0",
		"OME_COMPONENT":          "engine",
		"OME_INSTANCE_SUBDOMAIN": "llama-70b-engine-0",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("env[%s]: got %q want %q", k, got[k], v)
		}
	}
}

// TestRender_MultiPod_LeaderAddressOnWorker pins that every worker pod
// in a multi-pod Instance gets OME_LEADER_ADDRESS pointing at the leader
// pod of the SAME Instance. Worker's own OME_RUNNER_INDEX is its ordinal
// (NCCL rank position), not the leader's 0.
func TestRender_MultiPod_LeaderAddressOnWorker(t *testing.T) {
	plan, inst, runners := multiPodPlan(2)
	worker := runners[1]

	pod0, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker 0: %v", err)
	}
	pod1, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, 1)
	if err != nil {
		t.Fatalf("worker 1: %v", err)
	}

	for i, pod := range []*corev1.Pod{pod0, pod1} {
		got := envByName(pod.Spec.Containers[0].Env)
		if got["OME_LEADER_ADDRESS"] != "llama-70b-engine-0-leader-0.llama-70b-engine-headless" {
			t.Errorf("worker %d OME_LEADER_ADDRESS: got %q", i, got["OME_LEADER_ADDRESS"])
		}
		if got["OME_RUNNER"] != "worker" {
			t.Errorf("worker %d OME_RUNNER: got %q want worker", i, got["OME_RUNNER"])
		}
		if got["OME_RUNNER_SIZE"] != "2" {
			t.Errorf("worker %d OME_RUNNER_SIZE: got %q want 2", i, got["OME_RUNNER_SIZE"])
		}
	}
	if envByName(pod0.Spec.Containers[0].Env)["OME_RUNNER_INDEX"] != "0" {
		t.Errorf("worker 0 OME_RUNNER_INDEX: got %q want 0", envByName(pod0.Spec.Containers[0].Env)["OME_RUNNER_INDEX"])
	}
	if envByName(pod1.Spec.Containers[0].Env)["OME_RUNNER_INDEX"] != "1" {
		t.Errorf("worker 1 OME_RUNNER_INDEX: got %q want 1", envByName(pod1.Spec.Containers[0].Env)["OME_RUNNER_INDEX"])
	}
}

// TestRender_MultiPod_InstancePodRankAndCount pins the gang-global node-rank
// env. OME_INSTANCE_POD_RANK is the pod's flat rank across the WHOLE Instance
// (leader runner first, then worker runner); OME_INSTANCE_POD_COUNT is the
// gang's total pod count. Multi-node frameworks (sglang --node-rank/--nnodes,
// torchrun) read these directly instead of reconstructing a global rank from
// the per-runner OME_RUNNER_INDEX — which can't serve as a gang rank because
// leader and worker each index from 0 (raw OME_RUNNER_INDEX would collide
// leader-0 with worker-0). Gated on multi-pod, like OME_LEADER_ADDRESS.
func TestRender_MultiPod_InstancePodRankAndCount(t *testing.T) {
	plan, inst, runners := multiPodPlan(3) // 1 leader + 3 workers = 4-node gang
	leader, worker := runners[0], runners[1]

	// Leader is global rank 0; gang size 4.
	lpod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, leader, 0)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	lenv := envByName(lpod.Spec.Containers[0].Env)
	if lenv["OME_INSTANCE_POD_RANK"] != "0" {
		t.Errorf("leader OME_INSTANCE_POD_RANK: got %q want 0", lenv["OME_INSTANCE_POD_RANK"])
	}
	if lenv["OME_INSTANCE_POD_COUNT"] != "4" {
		t.Errorf("leader OME_INSTANCE_POD_COUNT: got %q want 4", lenv["OME_INSTANCE_POD_COUNT"])
	}

	// Workers follow the leader: ordinal 0/1/2 → global rank 1/2/3, count 4.
	for _, w := range []struct {
		ord  int32
		rank string
	}{{0, "1"}, {1, "2"}, {2, "3"}} {
		wpod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, w.ord)
		if err != nil {
			t.Fatalf("worker ord %d: %v", w.ord, err)
		}
		wenv := envByName(wpod.Spec.Containers[0].Env)
		if wenv["OME_INSTANCE_POD_RANK"] != w.rank {
			t.Errorf("worker ord %d OME_INSTANCE_POD_RANK: got %q want %s", w.ord, wenv["OME_INSTANCE_POD_RANK"], w.rank)
		}
		if wenv["OME_INSTANCE_POD_COUNT"] != "4" {
			t.Errorf("worker ord %d OME_INSTANCE_POD_COUNT: got %q want 4", w.ord, wenv["OME_INSTANCE_POD_COUNT"])
		}
	}

	// Single-pod Instances must NOT get the gang-global vars (same multi-pod
	// gate as OME_LEADER_ADDRESS) — runtimes branch on presence to detect multi-node.
	splan, sinst, srunner := singlePodPlan()
	spod, err := testRender(basicISVC(), basicPodSpec(), splan, sinst, srunner, 0)
	if err != nil {
		t.Fatalf("single-pod: %v", err)
	}
	senv := envByName(spod.Spec.Containers[0].Env)
	for _, k := range []string{"OME_INSTANCE_POD_RANK", "OME_INSTANCE_POD_COUNT"} {
		if v, ok := senv[k]; ok {
			t.Errorf("single-pod must not set %s (multi-pod only); got %q", k, v)
		}
	}
}

// TestRender_MultiPod_LeaderAddressPerInstance pins that the leader
// address is per-Instance: Instance 0's workers point at the Instance 0
// leader; Instance 1's workers point at the Instance 1 leader. Cross-talk
// would be a sharding bug.
func TestRender_MultiPod_LeaderAddressPerInstance(t *testing.T) {
	runners := []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 1},
	}
	inst0 := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: runners}
	inst1 := workload.InstancePlan{Index: 1, Incarnation: 1, Runners: runners}
	plan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Replicas:  2,
		Instances: []workload.InstancePlan{inst0, inst1},
	}

	pod0Worker, err := testRender(basicISVC(), basicPodSpec(), plan, inst0, runners[1], 0)
	if err != nil {
		t.Fatalf("inst0 worker: %v", err)
	}
	pod1Worker, err := testRender(basicISVC(), basicPodSpec(), plan, inst1, runners[1], 0)
	if err != nil {
		t.Fatalf("inst1 worker: %v", err)
	}

	got0 := envByName(pod0Worker.Spec.Containers[0].Env)["OME_LEADER_ADDRESS"]
	got1 := envByName(pod1Worker.Spec.Containers[0].Env)["OME_LEADER_ADDRESS"]
	want0 := "llama-70b-engine-0-leader-0.llama-70b-engine-headless"
	want1 := "llama-70b-engine-1-leader-0.llama-70b-engine-headless"
	if got0 != want0 {
		t.Errorf("inst0 worker OME_LEADER_ADDRESS: got %q want %q", got0, want0)
	}
	if got1 != want1 {
		t.Errorf("inst1 worker OME_LEADER_ADDRESS: got %q want %q", got1, want1)
	}
}

// TestRender_SinglePod_OmitsLeaderAddress pins the negative: single-pod
// "default" runner does NOT carry OME_LEADER_ADDRESS. Routers and any
// non-multi-pod Engine/Decoder must not see this variable; runtimes that
// branch on it would interpret its presence as a multi-pod environment.
func TestRender_SinglePod_OmitsLeaderAddress(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := envByName(pod.Spec.Containers[0].Env)
	if _, present := got["OME_LEADER_ADDRESS"]; present {
		t.Errorf("single-pod must not set OME_LEADER_ADDRESS; got %q", got["OME_LEADER_ADDRESS"])
	}
}

// tpuPodSpec is basicPodSpec with a google.com/tpu request on the runner
// container, so the render path detects a TPU pod and injects the multi-host
// slice env.
func tpuPodSpec() *corev1.PodSpec {
	spec := basicPodSpec()
	spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceName(constants.GoogleTPUResourceType): resource.MustParse("4"),
		},
	}
	return spec
}

// TestRender_MultiPod_TPUTopologyEnv pins the native multi-host TPU env OMENative
// supplies in place of GKE's LWS-only webhook: every pod gets the same ordered
// peer host list (leader rank 0, then workers), its own TPU_WORKER_ID = gang
// rank, and the matching :port process-address list. JAX/libtpu reads these to
// build the cross-host device mesh; without them each host comes up as a lone
// single-host slice. Gated on the pod requesting google.com/tpu.
func TestRender_MultiPod_TPUTopologyEnv(t *testing.T) {
	plan, inst, runners := multiPodPlan(1) // 1 leader + 1 worker = 2-host slice
	leader, worker := runners[0], runners[1]

	wantHosts := "llama-70b-engine-0-leader-0.llama-70b-engine-headless," +
		"llama-70b-engine-0-worker-0.llama-70b-engine-headless"
	wantAddrs := "llama-70b-engine-0-leader-0.llama-70b-engine-headless:8476," +
		"llama-70b-engine-0-worker-0.llama-70b-engine-headless:8476"

	lpod, err := testRender(basicISVC(), tpuPodSpec(), plan, inst, leader, 0)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	lenv := envByName(lpod.Spec.Containers[0].Env)
	for k, want := range map[string]string{
		"TPU_WORKER_HOSTNAMES":  wantHosts,
		"TPU_WORKER_ID":         "0",
		"TPU_PROCESS_ADDRESSES": wantAddrs,
		"TPU_PROCESS_PORT":      "8476",
		"TPU_NAME":              "llama-70b-engine-0",
	} {
		if lenv[k] != want {
			t.Errorf("leader %s: got %q want %q", k, lenv[k], want)
		}
	}

	// Worker shares the host list but reports its own gang rank (1).
	wpod, err := testRender(basicISVC(), tpuPodSpec(), plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	wenv := envByName(wpod.Spec.Containers[0].Env)
	if wenv["TPU_WORKER_HOSTNAMES"] != wantHosts {
		t.Errorf("worker TPU_WORKER_HOSTNAMES: got %q want %q", wenv["TPU_WORKER_HOSTNAMES"], wantHosts)
	}
	if wenv["TPU_WORKER_ID"] != "1" {
		t.Errorf("worker TPU_WORKER_ID: got %q want 1", wenv["TPU_WORKER_ID"])
	}
}

// TestRender_TPUTopologyEnv_PrimedPeerHostnamesMatchUncached pins the perf
// optimization that hoists the per-gang peer-host list out of the O(gangsize^2)
// per-pod rebuild: when the gang-render loop primes inst.PeerHostnames once,
// Render must produce byte-identical TPU env to the uncached path — same shared
// host/address list, same per-pod TPU_WORKER_ID.
func TestRender_TPUTopologyEnv_PrimedPeerHostnamesMatchUncached(t *testing.T) {
	plan, inst, runners := multiPodPlan(2) // 1 leader + 2 workers = 3-host slice

	// Prime the cache exactly as createMissingPods does, once per gang.
	primed := inst
	primed.PeerHostnames = buildInstancePeerHostnames(basicISVC().Name, plan.Component, inst)

	// Sanity: priming must not change the computed list — it's the same
	// derivation, just hoisted.
	if got, want := strings.Join(primed.PeerHostnames, ","),
		strings.Join(buildInstancePeerHostnames(basicISVC().Name, plan.Component, inst), ","); got != want {
		t.Fatalf("primed list diverged from derivation: got %q want %q", got, want)
	}

	tpuKeys := []string{"TPU_WORKER_HOSTNAMES", "TPU_WORKER_ID", "TPU_PROCESS_ADDRESSES", "TPU_PROCESS_PORT", "TPU_NAME"}
	// leader(0) + worker ordinals 0..1 → gang ranks 0,1,2.
	cases := []struct {
		runner  workload.RunnerPlan
		ordinal int32
	}{
		{runners[0], 0},
		{runners[1], 0},
		{runners[1], 1},
	}
	for _, c := range cases {
		uncached, err := testRender(basicISVC(), tpuPodSpec(), plan, inst, c.runner, c.ordinal)
		if err != nil {
			t.Fatalf("uncached render %s/%d: %v", c.runner.Name, c.ordinal, err)
		}
		cached, err := testRender(basicISVC(), tpuPodSpec(), plan, primed, c.runner, c.ordinal)
		if err != nil {
			t.Fatalf("cached render %s/%d: %v", c.runner.Name, c.ordinal, err)
		}
		uncachedEnv, ce := envByName(uncached.Spec.Containers[0].Env), envByName(cached.Spec.Containers[0].Env)
		for _, k := range tpuKeys {
			if uncachedEnv[k] != ce[k] {
				t.Errorf("%s/%d %s: cached %q != uncached %q", c.runner.Name, c.ordinal, k, ce[k], uncachedEnv[k])
			}
		}
	}

	// The shared list is constant across the gang, but TPU_WORKER_ID is not.
	wid := func(r workload.RunnerPlan, o int32) string {
		pod, err := testRender(basicISVC(), tpuPodSpec(), plan, primed, r, o)
		if err != nil {
			t.Fatalf("render %s/%d: %v", r.Name, o, err)
		}
		return envByName(pod.Spec.Containers[0].Env)["TPU_WORKER_ID"]
	}
	if got := []string{wid(runners[0], 0), wid(runners[1], 0), wid(runners[1], 1)}; got[0] != "0" || got[1] != "1" || got[2] != "2" {
		t.Errorf("per-pod TPU_WORKER_ID with primed cache: got %v want [0 1 2]", got)
	}
}

// TestRender_TPUTopologyEnv_GatedOnTPUAndMultiPod pins the two negatives: a
// multi-pod pod that does NOT request google.com/tpu gets no TPU env, and a
// single-pod TPU pod gets none either (a lone host needs no peer mesh).
func TestRender_TPUTopologyEnv_GatedOnTPUAndMultiPod(t *testing.T) {
	// Multi-pod but no TPU request -> no TPU env.
	plan, inst, runners := multiPodPlan(1)
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runners[0], 0)
	if err != nil {
		t.Fatalf("non-tpu multi-pod: %v", err)
	}
	if v, ok := envByName(pod.Spec.Containers[0].Env)["TPU_WORKER_HOSTNAMES"]; ok {
		t.Errorf("non-TPU pod must not set TPU_WORKER_HOSTNAMES; got %q", v)
	}

	// Single-pod TPU request -> no multi-host env (single-host slice is correct).
	splan, sinst, srunner := singlePodPlan()
	spod, err := testRender(basicISVC(), tpuPodSpec(), splan, sinst, srunner, 0)
	if err != nil {
		t.Fatalf("single-pod tpu: %v", err)
	}
	if v, ok := envByName(spod.Spec.Containers[0].Env)["TPU_WORKER_HOSTNAMES"]; ok {
		t.Errorf("single-pod TPU must not set TPU_WORKER_HOSTNAMES; got %q", v)
	}
}

// requiredPodAffinity is a small accessor for the rendered pod's required
// podAffinity terms (nil-safe) used by the slice-affinity tests.
func requiredPodAffinity(pod *corev1.Pod) []corev1.PodAffinityTerm {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
		return nil
	}
	return pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

// TestRender_MultiPod_TPUConfiguredGangAffinity pins that an explicit topology
// key drives affinity even when provider-specific selectors are also present.
func TestRender_MultiPod_TPUConfiguredGangAffinity(t *testing.T) {
	plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/domain")
	worker := runners[1]
	spec := tpuPodSpec()
	spec.NodeSelector = map[string]string{"cloud.google.com/gke-tpu-topology": "2x2x1"}

	pod, err := testRender(basicISVC(), spec, plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	terms := requiredPodAffinity(pod)
	if len(terms) != 1 {
		t.Fatalf("worker required podAffinity: got %d terms want 1: %+v", len(terms), terms)
	}
	if got := terms[0].TopologyKey; got != plan.TopologyKey {
		t.Errorf("topologyKey: got %q want %q", got, plan.TopologyKey)
	}
	wantSel := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           "engine",
		query.LabelInstanceIdx:                "0",
		query.LabelRunner:                     "leader",
	}
	if terms[0].LabelSelector == nil {
		t.Fatalf("term has nil LabelSelector: %+v", terms[0])
	}
	if diff := cmp.Diff(wantSel, terms[0].LabelSelector.MatchLabels); diff != "" {
		t.Errorf("leader selector mismatch (-want +got):\n%s", diff)
	}
}

func TestRender_TPUNodeSelectorDoesNotInferTopology(t *testing.T) {
	plan, inst, runners := multiPodPlan(1)
	worker := runners[1]
	spec := tpuPodSpec()
	spec.NodeSelector = map[string]string{"cloud.google.com/gke-tpu-topology": "2x2x1"}

	pod, err := testRender(basicISVC(), spec, plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	terms := requiredPodAffinity(pod)
	if len(terms) != 0 {
		t.Fatalf("provider selector must not infer topology affinity: %+v", terms)
	}
}

// TestRender_TPUGangAffinity_InstanceScoped pins that each Instance's worker
// follows its OWN leader: Instance 1's worker selector carries instance-index=1,
// not 0. Cross-instance co-location would be a sharding bug.
func TestRender_TPUGangAffinity_InstanceScoped(t *testing.T) {
	runners := []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 1},
	}
	inst1 := workload.InstancePlan{Index: 1, Incarnation: 1, Runners: runners}
	plan := workload.ComponentPlan{
		Component:   workload.ComponentEngine,
		Replicas:    2,
		Instances:   []workload.InstancePlan{inst1},
		TopologyKey: "topology.example.com/domain",
	}

	pod, err := testRender(basicISVC(), tpuPodSpec(), plan, inst1, runners[1], 0)
	if err != nil {
		t.Fatalf("inst1 worker: %v", err)
	}
	terms := requiredPodAffinity(pod)
	if len(terms) != 1 {
		t.Fatalf("got %d terms want 1", len(terms))
	}
	if got := terms[0].LabelSelector.MatchLabels[query.LabelInstanceIdx]; got != "1" {
		t.Errorf("selector instance-index: got %q want 1", got)
	}
}

// TestRender_TPUGangAffinity_Negatives pins the remaining gates: leaders and
// single-pod TPU workloads never receive follower affinity, and a multi-pod
// TPU worker without an explicit key receives no inferred affinity.
func TestRender_TPUGangAffinity_Negatives(t *testing.T) {
	plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/domain")
	leaderPod, err := testRender(basicISVC(), tpuPodSpec(), plan, inst, runners[0], 0)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	if terms := requiredPodAffinity(leaderPod); len(terms) != 0 {
		t.Errorf("leader must not get gang affinity; got %+v", terms)
	}

	splan, sinst, srunner := singlePodPlan()
	splan.TopologyKey = plan.TopologyKey
	spod, err := testRender(basicISVC(), tpuPodSpec(), splan, sinst, srunner, 0)
	if err != nil {
		t.Fatalf("single-pod tpu: %v", err)
	}
	if terms := requiredPodAffinity(spod); len(terms) != 0 {
		t.Errorf("single-pod must not get gang affinity; got %+v", terms)
	}

	noKeyPlan, noKeyInst, noKeyRunners := multiPodPlan(1)
	noKeyPod, err := testRender(basicISVC(), tpuPodSpec(), noKeyPlan, noKeyInst, noKeyRunners[1], 0)
	if err != nil {
		t.Fatalf("worker without topology key: %v", err)
	}
	if terms := requiredPodAffinity(noKeyPod); len(terms) != 0 {
		t.Errorf("TPU worker without topology key or selector must get no affinity; got %+v", terms)
	}
}

func TestRender_TPUNoKeyFullRecreateRemainsTopologyFree(t *testing.T) {
	plan, inst, runners := multiPodPlan(1)
	inst.Incarnation = 2
	spec := tpuPodSpec()
	spec.NodeSelector = map[string]string{"cloud.google.com/gke-tpu-topology": "4x4"}

	pod, err := testRender(basicISVC(), spec, plan, inst, runners[1], 0)
	if err != nil {
		t.Fatalf("recreated worker: %v", err)
	}
	terms := requiredPodAffinity(pod)
	if len(terms) != 0 {
		t.Fatalf("full recreate inferred provider topology without an explicit key: %+v", terms)
	}
}

// TestRender_TPUGangAffinity_PreservesUserAffinity pins that the injected term
// is APPENDED — a user-declared podAffinity term survives alongside it.
func TestRender_TPUGangAffinity_PreservesUserAffinity(t *testing.T) {
	plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/domain")
	worker := runners[1]
	ps := tpuPodSpec()
	userTerm := corev1.PodAffinityTerm{
		TopologyKey: "kubernetes.io/hostname",
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "user-thing"},
		},
	}
	ps.Affinity = &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{userTerm},
		},
	}

	pod, err := testRender(basicISVC(), ps, plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	terms := requiredPodAffinity(pod)
	if len(terms) != 2 {
		t.Fatalf("expected user term + injected term = 2, got %d: %+v", len(terms), terms)
	}
	if terms[0].TopologyKey != "kubernetes.io/hostname" {
		t.Errorf("user term clobbered or reordered: %+v", terms[0])
	}
	if terms[1].TopologyKey != plan.TopologyKey {
		t.Errorf("injected term missing/wrong: %+v", terms[1])
	}
}

// TestInjectTPUGangAffinity_Idempotent pins that re-rendering (cache-lag retry)
// does not duplicate the gang-affinity term — mirrors the migration-overlay
// idempotency guard.
func TestInjectTPUGangAffinity_Idempotent(t *testing.T) {
	plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/domain")
	worker := runners[1]
	pod := &corev1.Pod{Spec: *tpuPodSpec()}

	injectGangDomainAffinity(pod, plan, inst, worker, "llama-70b")
	injectGangDomainAffinity(pod, plan, inst, worker, "llama-70b")

	terms := requiredPodAffinity(pod)
	if len(terms) != 1 {
		t.Fatalf("gang-affinity term should appear once after double render, got %d: %+v", len(terms), terms)
	}
}

// TestRender_MultiPod_GangDomainAffinity pins the generic (non-TPU)
// worker→leader gang co-location: a multi-node worker whose component
// carries a resolved topologyKey gets a REQUIRED podAffinity whose
// topologyKey is that key and whose selector targets the SAME Instance's
// leader using the configured topology key.
func TestRender_MultiPod_GangDomainAffinity(t *testing.T) {
	plan, inst, runners := multiPodPlanWithTopologyKey(2, "topology.example.com/domain")
	worker := runners[1]

	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	terms := requiredPodAffinity(pod)
	if len(terms) != 1 {
		t.Fatalf("worker required podAffinity: got %d terms want 1: %+v", len(terms), terms)
	}
	if got := terms[0].TopologyKey; got != "topology.example.com/domain" {
		t.Errorf("topologyKey: got %q want topology.example.com/domain", got)
	}
	wantSel := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           "engine",
		query.LabelInstanceIdx:                "0",
		query.LabelRunner:                     "leader",
	}
	if terms[0].LabelSelector == nil {
		t.Fatalf("term has nil LabelSelector: %+v", terms[0])
	}
	if diff := cmp.Diff(wantSel, terms[0].LabelSelector.MatchLabels); diff != "" {
		t.Errorf("leader selector mismatch (-want +got):\n%s", diff)
	}
}

func TestRender_GangDomainAffinity_PartialCreateUsesHeldInstanceTopology(t *testing.T) {
	const heldTopology = "topology.example.com/old"
	const desiredTopology = "topology.example.com/new"
	plan, source, runners := multiPodPlanWithTopologyKey(1, desiredTopology)
	plan.InstanceTopologyKeys = map[int32]string{source.Index: heldTopology}
	worker := runners[1]

	sourcePod, err := testRender(basicISVC(), basicPodSpec(), plan, source, worker, 0)
	if err != nil {
		t.Fatalf("render missing source worker: %v", err)
	}
	sourceTerms := requiredPodAffinity(sourcePod)
	if len(sourceTerms) != 1 || sourceTerms[0].TopologyKey != heldTopology {
		t.Fatalf("partial source gang must retain topology %q, got %+v", heldTopology, sourceTerms)
	}

	// A fresh surge index has no live override and must use the new Component
	// topology. This is the split that lets an old source finish safely while
	// its replacement rolls forward.
	surge := workload.InstancePlan{Index: 1, Incarnation: 1, Runners: runners}
	surgePod, err := testRender(basicISVC(), basicPodSpec(), plan, surge, worker, 0)
	if err != nil {
		t.Fatalf("render fresh surge worker: %v", err)
	}
	surgeTerms := requiredPodAffinity(surgePod)
	if len(surgeTerms) != 1 || surgeTerms[0].TopologyKey != desiredTopology {
		t.Fatalf("fresh surge gang must use topology %q, got %+v", desiredTopology, surgeTerms)
	}
}

// TestRender_GangDomainAffinity_InstanceScoped pins that each Instance's
// worker follows its OWN leader: the injected selector carries the
// CONCRETE per-Instance index (0 for Instance 0, 1 for Instance 1), not
// a shared key. OME knows the index at render time, so it stamps the
// literal value — keeping the term portable to any k8s version (no
// matchLabelKeys dependency). Cross-instance co-location would be a
// sharding bug.
func TestRender_GangDomainAffinity_InstanceScoped(t *testing.T) {
	runners := []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 1},
	}
	for _, idx := range []int32{0, 1} {
		inst := workload.InstancePlan{Index: idx, Incarnation: 1, Runners: runners}
		plan := workload.ComponentPlan{
			Component:   workload.ComponentEngine,
			Replicas:    2,
			Instances:   []workload.InstancePlan{inst},
			TopologyKey: "topology.example.com/domain",
		}

		pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runners[1], 0)
		if err != nil {
			t.Fatalf("inst%d worker: %v", idx, err)
		}
		terms := requiredPodAffinity(pod)
		if len(terms) != 1 {
			t.Fatalf("inst%d: got %d terms want 1", idx, len(terms))
		}
		want := fmt.Sprintf("%d", idx)
		if got := terms[0].LabelSelector.MatchLabels[query.LabelInstanceIdx]; got != want {
			t.Errorf("inst%d selector instance-index: got %q want %q", idx, got, want)
		}
	}
}

// TestRender_GangDomainAffinity_Negatives pins the gates: leader,
// single-pod (default runner), and router get NO gang-domain affinity
// even when a topologyKey is set — only multi-node workers anchor to a
// leader.
func TestRender_GangDomainAffinity_Negatives(t *testing.T) {
	// Leader gets nothing — it's the domain anchor, not a follower.
	plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/domain")
	leaderPod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runners[0], 0)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	if terms := requiredPodAffinity(leaderPod); len(terms) != 0 {
		t.Errorf("leader must not get gang-domain affinity; got %+v", terms)
	}

	// Single-pod shape: "default" runner, no leader → nothing, even
	// though the plan carries a topologyKey.
	splan, sinst, srunner := singlePodPlan()
	splan.TopologyKey = "topology.example.com/domain"
	spod, err := testRender(basicISVC(), basicPodSpec(), splan, sinst, srunner, 0)
	if err != nil {
		t.Fatalf("single-pod: %v", err)
	}
	if terms := requiredPodAffinity(spod); len(terms) != 0 {
		t.Errorf("single-pod must not get gang-domain affinity; got %+v", terms)
	}

	// Router (single-pod component) with a topologyKey set → nothing.
	rplan := workload.ComponentPlan{
		Component:   workload.ComponentRouter,
		Replicas:    1,
		Instances:   []workload.InstancePlan{sinst},
		TopologyKey: "topology.example.com/domain",
	}
	rpod, err := testRender(basicISVC(), basicPodSpec(), rplan, sinst, srunner, 0)
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	if terms := requiredPodAffinity(rpod); len(terms) != 0 {
		t.Errorf("router must not get gang-domain affinity; got %+v", terms)
	}
}

// TestRender_GangDomainAffinity_NoTopologyKey pins that a multi-node
// worker WITHOUT a resolved topologyKey (the optional field unset) gets
// no injection — the field is opt-in, nil is zero behavior change.
func TestRender_GangDomainAffinity_NoTopologyKey(t *testing.T) {
	plan, inst, runners := multiPodPlan(1) // no TopologyKey
	pod, err := testRender(basicISVC(), basicPodSpec(), plan, inst, runners[1], 0)
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	if terms := requiredPodAffinity(pod); len(terms) != 0 {
		t.Errorf("worker without topologyKey must get no affinity; got %+v", terms)
	}
}

// TestRender_GangDomainAffinity_RespectsUserOverride pins the override
// guard: if the user already hand-wrote a required podAffinity term on
// the SAME topologyKey, OME does NOT inject a duplicate — it defers to
// the user's affinity. A user term on a DIFFERENT key is left intact and
// the OME term is still appended.
func TestRender_GangDomainAffinity_RespectsUserOverride(t *testing.T) {
	// User declared their own term on the gang topologyKey → no dup.
	plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/domain")
	worker := runners[1]
	ps := basicPodSpec()
	userTerm := corev1.PodAffinityTerm{
		TopologyKey: "topology.example.com/domain",
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "user-hand-written"},
		},
	}
	ps.Affinity = &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{userTerm},
		},
	}
	pod, err := testRender(basicISVC(), ps, plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	terms := requiredPodAffinity(pod)
	if len(terms) != 1 {
		t.Fatalf("user already declared the topologyKey; expected no duplicate (1 term), got %d: %+v", len(terms), terms)
	}
	if diff := cmp.Diff(map[string]string{"app": "user-hand-written"}, terms[0].LabelSelector.MatchLabels); diff != "" {
		t.Errorf("user term must be preserved verbatim (-want +got):\n%s", diff)
	}

	// User term on a DIFFERENT key → OME term still appended (preserve
	// user affinity, add the gang term).
	ps2 := basicPodSpec()
	ps2.Affinity = &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey:   "kubernetes.io/hostname",
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}},
			}},
		},
	}
	pod2, err := testRender(basicISVC(), ps2, plan, inst, worker, 0)
	if err != nil {
		t.Fatalf("worker different-key: %v", err)
	}
	terms2 := requiredPodAffinity(pod2)
	if len(terms2) != 2 {
		t.Fatalf("user term on a different key: expected user term + injected term = 2, got %d: %+v", len(terms2), terms2)
	}
	if terms2[0].TopologyKey != "kubernetes.io/hostname" {
		t.Errorf("user term clobbered or reordered: %+v", terms2[0])
	}
	if terms2[1].TopologyKey != "topology.example.com/domain" {
		t.Errorf("injected gang-domain term missing/wrong: %+v", terms2[1])
	}
}

func TestRender_OMEEnvOverridesUserEnvWithSameName(t *testing.T) {
	plan, inst, runner := singlePodPlan()
	ps := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: "main", Image: "llama:v1",
				Env: []corev1.EnvVar{
					{Name: "OME_COMPONENT", Value: "user-supplied-junk"},
					{Name: "USER_KEEP", Value: "kept"},
				},
			},
		},
	}
	pod, err := testRender(basicISVC(), ps, plan, inst, runner, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := envByName(pod.Spec.Containers[0].Env)
	if got["OME_COMPONENT"] != "engine" {
		t.Errorf("OME_COMPONENT: got %q want engine", got["OME_COMPONENT"])
	}
	if got["USER_KEEP"] != "kept" {
		t.Errorf("USER_KEEP: got %q want kept", got["USER_KEEP"])
	}
	// No duplicate OME_COMPONENT
	count := 0
	for _, e := range pod.Spec.Containers[0].Env {
		if e.Name == "OME_COMPONENT" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("OME_COMPONENT appeared %d times, want 1", count)
	}
}

func TestRender_NameVariesWithEachAxis(t *testing.T) {
	tests := []struct {
		name      string
		isvc      string
		component workload.ComponentType
		inst      int32
		runner    string
		ordinal   int32
		want      string
	}{
		{"engine, single-pod default, ord 0", "llama", workload.ComponentEngine, 0, "default", 0, "llama-engine-0-default-0"},
		{"decoder, instance 2", "llama", workload.ComponentDecoder, 2, "default", 0, "llama-decoder-2-default-0"},
		{"router, instance 5", "llama", workload.ComponentRouter, 5, "default", 0, "llama-router-5-default-0"},
		{"engine, leader runner ord 0 (multi-pod shape preview)", "llama", workload.ComponentEngine, 0, "leader", 0, "llama-engine-0-leader-0"},
		{"engine, worker runner ord 2 (multi-pod shape preview)", "llama", workload.ComponentEngine, 0, "worker", 2, "llama-engine-0-worker-2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := query.PodName(tt.isvc, tt.component, tt.inst, tt.runner, tt.ordinal)
			if got != tt.want {
				t.Errorf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestRender_HeadlessServiceName(t *testing.T) {
	if got := query.HeadlessServiceName("llama-70b", workload.ComponentEngine); got != "llama-70b-engine-headless" {
		t.Errorf("got %q want llama-70b-engine-headless", got)
	}
	if got := query.HeadlessServiceName("llama-70b", workload.ComponentDecoder); got != "llama-70b-decoder-headless" {
		t.Errorf("got %q want llama-70b-decoder-headless", got)
	}
}

func envByName(env []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

// envKeys is a small debugging helper; sorted for stable diff messages.
//
//nolint:unused
func envKeys(env []corev1.EnvVar) []string {
	keys := make([]string, 0, len(env))
	for _, e := range env {
		keys = append(keys, e.Name)
	}
	sort.Strings(keys)
	return keys
}

// --- applyMigrationOverlay idempotency ---

func TestApplyMigrationOverlay_IdempotentDoesNotDuplicateRequirements(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{}}
	overlay := &workload.MigrationOverlay{
		FromNode:        "node5",
		HintTargetNodes: []string{"node3", "node7"},
	}
	applyMigrationOverlay(pod, overlay)
	applyMigrationOverlay(pod, overlay)

	req := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if req == nil || len(req.NodeSelectorTerms) != 1 {
		t.Fatalf("expected exactly one term, got %+v", req)
	}
	notInCount := 0
	for _, expr := range req.NodeSelectorTerms[0].MatchExpressions {
		if expr.Key == "kubernetes.io/hostname" && expr.Operator == corev1.NodeSelectorOpNotIn {
			notInCount++
		}
	}
	if notInCount != 1 {
		t.Errorf("NotIn[hostname] requirement should appear once, got %d", notInCount)
	}

	pref := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(pref) != 1 {
		t.Errorf("preferred terms should appear once, got %d: %+v", len(pref), pref)
	}
}

func TestApplyMigrationOverlay_PreferredWeightIsSentinel(t *testing.T) {
	// Weight is a documented sentinel — pinned so future changes are
	// deliberate and don't silently collide with operator preferences.
	pod := &corev1.Pod{Spec: corev1.PodSpec{}}
	applyMigrationOverlay(pod, &workload.MigrationOverlay{HintTargetNodes: []string{"node3"}})
	pref := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(pref) != 1 || pref[0].Weight != overlayPreferredHintWeight {
		t.Errorf("preferred weight: got %+v want sentinel=%d", pref, overlayPreferredHintWeight)
	}
}

// --- wouldOverlayConflictWithNodeAffinity ---

func TestWouldOverlayConflictWithNodeAffinity_DetectsUnsatisfiablePin(t *testing.T) {
	// Source PodSpec required hostname=node5; overlay's NotIn[node5]
	// would collapse the term to "hostname=node5 AND hostname!=node5".
	spec := &corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node5"},
						}},
					}},
				},
			},
		},
	}
	overlay := &workload.MigrationOverlay{FromNode: "node5"}
	if !WouldOverlayConflictWithNodeAffinity(spec, overlay) {
		t.Errorf("hostname=FromNode pin should be detected as conflict")
	}
}

func TestWouldOverlayConflictWithNodeAffinity_HostnameSetWithOtherNodesIsSatisfiable(t *testing.T) {
	// Source required hostname IN {node5, node7}. Overlay's NotIn[node5]
	// excludes node5; node7 remains scheduable. NOT a conflict.
	spec := &corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "kubernetes.io/hostname",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"node5", "node7"},
						}},
					}},
				},
			},
		},
	}
	overlay := &workload.MigrationOverlay{FromNode: "node5"}
	if WouldOverlayConflictWithNodeAffinity(spec, overlay) {
		t.Errorf("multi-value hostname set must not be flagged as conflict")
	}
}

func TestWouldOverlayConflictWithNodeAffinity_OnlyConflictIfEveryTermPins(t *testing.T) {
	// Terms are OR'd. Term 1 pins to node5 (conflict), Term 2 is unconstrained
	// (satisfiable). Overall: still satisfiable, so NOT a conflict.
	spec := &corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node5"},
						}}},
						{MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"us-west-2a"},
						}}},
					},
				},
			},
		},
	}
	overlay := &workload.MigrationOverlay{FromNode: "node5"}
	if WouldOverlayConflictWithNodeAffinity(spec, overlay) {
		t.Errorf("at least one satisfiable term must clear the conflict")
	}
}

func TestWouldOverlayConflictWithNodeAffinity_NilSpecOrAffinityNoConflict(t *testing.T) {
	overlay := &workload.MigrationOverlay{FromNode: "node5"}
	if WouldOverlayConflictWithNodeAffinity(nil, overlay) {
		t.Errorf("nil spec should not conflict")
	}
	if WouldOverlayConflictWithNodeAffinity(&corev1.PodSpec{}, overlay) {
		t.Errorf("empty affinity should not conflict")
	}
}

func TestWouldOverlayConflictWithNodeAffinity_NilOverlayOrEmptyFromNode(t *testing.T) {
	spec := &corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node5"},
						}},
					}},
				},
			},
		},
	}
	if WouldOverlayConflictWithNodeAffinity(spec, nil) {
		t.Errorf("nil overlay must not conflict")
	}
	if WouldOverlayConflictWithNodeAffinity(spec, &workload.MigrationOverlay{FromNode: ""}) {
		t.Errorf("empty FromNode must not conflict")
	}
}

// TestWouldExclusionsConflictWithNodeAffinity_MultiNode pins the
// exclusion-set generalization the deadline disposition uses: a
// hostname In-pin conflicts only once EVERY permitted host is excluded.
func TestWouldExclusionsConflictWithNodeAffinity_MultiNode(t *testing.T) {
	pinned := func(hosts ...string) *corev1.PodSpec {
		return &corev1.PodSpec{
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{{
								Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: hosts,
							}},
						}},
					},
				},
			},
		}
	}
	if WouldExclusionsConflictWithNodeAffinity(pinned("node-a", "node-b"), []string{"node-a"}) {
		t.Errorf("In[a,b] minus {a} still permits b — no conflict")
	}
	if !WouldExclusionsConflictWithNodeAffinity(pinned("node-a", "node-b"), []string{"node-a", "node-b"}) {
		t.Errorf("In[a,b] minus {a,b} permits nothing — conflict expected")
	}
	if WouldExclusionsConflictWithNodeAffinity(pinned("node-a"), nil) {
		t.Errorf("empty exclusion set must not conflict")
	}
	// A term that doesn't pin hostname at all defuses the conflict.
	unpinned := &corev1.PodSpec{
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: "gpu-type", Operator: corev1.NodeSelectorOpIn, Values: []string{"h100"},
						}},
					}},
				},
			},
		},
	}
	if WouldExclusionsConflictWithNodeAffinity(unpinned, []string{"node-a"}) {
		t.Errorf("term without hostname pin must not conflict")
	}
}

// TestRender_ContainerName_FallbackPrecedence pins the precedence walk
// for rendered container names when the upstream merge layer hasn't
// stamped a name. Render is the apply-time chokepoint that talks to the
// apiserver; an empty container Name produces a Pod-create rejection
// (`spec.containers[N].name: Required value`) that deadlocks OMENative
// rollouts.
//
// The precedence under test:
//
//  1. Container name set on the PodSpec we receive → preserved as-is.
//     Upstream merge ran or the user template explicitly named the
//     container; either way we don't overwrite.
//  2. Container name empty AND we have a PodSpec → backfill to the
//     canonical "ome-container". Single source of truth at apply time
//     so the bug surfaces neither at Pod-create nor at runtime.
//  3. Multiple containers with mixed empty/non-empty names → only the
//     empty ones get the default; named siblings are untouched.
//  4. Init containers get the same treatment as regular containers.
//
// Note: cases where upstream merge correctly picks the ServingRuntime's
// engineConfig.runner.name are covered by utils.MergeRuntimeContainers
// tests (since the merge happens upstream of render); this test focuses
// on the render-layer last-line backfill that catches direct callers.
func TestRender_ContainerName_FallbackPrecedence(t *testing.T) {
	plan, inst, runner := singlePodPlan()

	// Case (a): caller set a name explicitly → preserved.
	t.Run("explicit name is preserved", func(t *testing.T) {
		spec := &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "my-runner", Image: "img:v1"}},
		}
		pod, err := testRender(basicISVC(), spec, plan, inst, runner, 0)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if got := pod.Spec.Containers[0].Name; got != "my-runner" {
			t.Errorf("Containers[0].Name: got %q want my-runner", got)
		}
	})

	// Case (b): caller left the container unnamed (e.g., neither ISVC
	// nor runtime stamped one and the merge produced "") → backfill to
	// the canonical default.
	t.Run("empty name falls back to ome-container", func(t *testing.T) {
		spec := &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "", Image: "img:v1"}},
		}
		pod, err := testRender(basicISVC(), spec, plan, inst, runner, 0)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if got := pod.Spec.Containers[0].Name; got != constants.MainContainerName {
			t.Errorf("Containers[0].Name: got %q want %q", got, constants.MainContainerName)
		}
	})

	// Case (c): mixed slice — empty names get the default; named
	// siblings are untouched.
	t.Run("only empty names are backfilled", func(t *testing.T) {
		spec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "sidecar", Image: "sc:v1"},
				{Name: "", Image: "main:v1"},
			},
		}
		pod, err := testRender(basicISVC(), spec, plan, inst, runner, 0)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if got := pod.Spec.Containers[0].Name; got != "sidecar" {
			t.Errorf("Containers[0].Name (sidecar): got %q want sidecar", got)
		}
		if got := pod.Spec.Containers[1].Name; got != constants.MainContainerName {
			t.Errorf("Containers[1].Name (was empty): got %q want %q", got, constants.MainContainerName)
		}
	})

	// Case (d): init containers get the same treatment; an unnamed init
	// container is just as fatal at apply-time as an unnamed regular one.
	t.Run("init containers backfilled too", func(t *testing.T) {
		spec := &corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "", Image: "init:v1"}},
			Containers:     []corev1.Container{{Name: "main", Image: "main:v1"}},
		}
		pod, err := testRender(basicISVC(), spec, plan, inst, runner, 0)
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if got := pod.Spec.InitContainers[0].Name; got != constants.MainContainerName {
			t.Errorf("InitContainers[0].Name: got %q want %q", got, constants.MainContainerName)
		}
	})
}

// TestBackfillEmptyContainerNames pins the helper's nil-and-zero
// semantics directly — Render's wrapper test above exercises the
// "with-Render" path; this one isolates the helper for documentation.
func TestBackfillEmptyContainerNames(t *testing.T) {
	t.Run("nil spec is a no-op", func(t *testing.T) {
		backfillEmptyContainerNames(nil) // must not panic
	})
	t.Run("zero containers is a no-op", func(t *testing.T) {
		spec := &corev1.PodSpec{}
		backfillEmptyContainerNames(spec)
		if len(spec.Containers) != 0 || len(spec.InitContainers) != 0 {
			t.Errorf("must not invent containers; got %+v", spec)
		}
	})
}

// TestRender_TopologySpreadConstraint pins the spread rendering contract:
// the anchor pod (leader / single-pod default) carries one constraint on the
// resolved fault-domain key with the policy-mapped whenUnsatisfiable and a
// selector matching exactly its sibling anchors; workers carry none; the
// co-location key is the fallback fault domain; an operator-authored
// constraint on the same key wins; a single-pod component without an
// explicit spread key renders nothing.
func TestRender_TopologySpreadConstraint(t *testing.T) {
	isvc := basicISVC()

	t.Run("leader gets DoNotSchedule on the spread key", func(t *testing.T) {
		plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/partition")
		plan.TopologySpread = "Required"
		plan.TopologySpreadKey = "topology.example.com/cube"
		leader, err := testRenderWithRevision(isvc, basicPodSpec(), nil, plan, inst, runners[0], 0, "")
		if err != nil {
			t.Fatalf("render leader: %v", err)
		}
		if n := len(leader.Spec.TopologySpreadConstraints); n != 1 {
			t.Fatalf("leader constraints = %d, want 1", n)
		}
		c := leader.Spec.TopologySpreadConstraints[0]
		if c.TopologyKey != "topology.example.com/cube" || c.MaxSkew != 1 || c.WhenUnsatisfiable != corev1.DoNotSchedule {
			t.Fatalf("constraint = %+v, want cube/maxSkew 1/DoNotSchedule", c)
		}
		want := map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(plan.Component),
			query.LabelRunner:                     "leader",
		}
		if c.LabelSelector == nil || !reflect.DeepEqual(c.LabelSelector.MatchLabels, want) {
			t.Fatalf("selector = %+v, want %+v", c.LabelSelector, want)
		}
		// The selector must match the rendered leader itself (its sibling
		// anchors carry the same labels).
		for k, v := range want {
			if leader.Labels[k] != v {
				t.Fatalf("leader label %s=%q, selector wants %q", k, leader.Labels[k], v)
			}
		}

		// Workers carry NO constraint: their required worker→leader
		// affinity makes them unschedulable until the leader is placed,
		// so the TSC-gated leader always decides the fault domain.
		worker, err := testRenderWithRevision(isvc, basicPodSpec(), nil, plan, inst, runners[1], 0, "")
		if err != nil {
			t.Fatalf("render worker: %v", err)
		}
		if len(worker.Spec.TopologySpreadConstraints) != 0 {
			t.Fatalf("worker must carry no spread constraint, got %+v", worker.Spec.TopologySpreadConstraints)
		}
	})

	t.Run("Preferred maps to ScheduleAnyway and falls back to the co-location key", func(t *testing.T) {
		plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/partition")
		plan.TopologySpread = "Preferred"
		leader, err := testRenderWithRevision(isvc, basicPodSpec(), nil, plan, inst, runners[0], 0, "")
		if err != nil {
			t.Fatalf("render leader: %v", err)
		}
		if n := len(leader.Spec.TopologySpreadConstraints); n != 1 {
			t.Fatalf("leader constraints = %d, want 1", n)
		}
		c := leader.Spec.TopologySpreadConstraints[0]
		if c.TopologyKey != "topology.example.com/partition" || c.WhenUnsatisfiable != corev1.ScheduleAnyway {
			t.Fatalf("constraint = %+v, want partition/ScheduleAnyway", c)
		}
	})

	t.Run("single-pod anchor spreads only with an explicit key", func(t *testing.T) {
		plan, inst, runner := singlePodPlan()
		plan.TopologySpread = "Required"
		pod, err := testRenderWithRevision(isvc, basicPodSpec(), nil, plan, inst, runner, 0, "")
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if len(pod.Spec.TopologySpreadConstraints) != 0 {
			t.Fatalf("no co-location key and no spread key must render nothing, got %+v", pod.Spec.TopologySpreadConstraints)
		}

		plan.TopologySpreadKey = "topology.kubernetes.io/zone"
		pod, err = testRenderWithRevision(isvc, basicPodSpec(), nil, plan, inst, runner, 0, "")
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if n := len(pod.Spec.TopologySpreadConstraints); n != 1 {
			t.Fatalf("constraints = %d, want 1", n)
		}
		if got := pod.Spec.TopologySpreadConstraints[0].LabelSelector.MatchLabels[query.LabelRunner]; got != "default" {
			t.Fatalf("selector runner = %q, want default", got)
		}
	})

	t.Run("operator-authored constraint on the same key wins", func(t *testing.T) {
		plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/partition")
		plan.TopologySpread = "Required"
		spec := basicPodSpec()
		spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew: 3, TopologyKey: "topology.example.com/partition",
			WhenUnsatisfiable: corev1.ScheduleAnyway,
		}}
		leader, err := testRenderWithRevision(isvc, spec, nil, plan, inst, runners[0], 0, "")
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if n := len(leader.Spec.TopologySpreadConstraints); n != 1 {
			t.Fatalf("constraints = %d, want the operator's single constraint", n)
		}
		if leader.Spec.TopologySpreadConstraints[0].MaxSkew != 3 {
			t.Fatalf("operator constraint was replaced: %+v", leader.Spec.TopologySpreadConstraints[0])
		}
	})

	t.Run("no policy renders nothing", func(t *testing.T) {
		plan, inst, runners := multiPodPlanWithTopologyKey(1, "topology.example.com/partition")
		leader, err := testRenderWithRevision(isvc, basicPodSpec(), nil, plan, inst, runners[0], 0, "")
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if len(leader.Spec.TopologySpreadConstraints) != 0 {
			t.Fatalf("unset policy must render nothing, got %+v", leader.Spec.TopologySpreadConstraints)
		}
	})
}
