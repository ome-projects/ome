package coordination

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// trafficEndpointsCondition is the status condition that names the wait
// for the endpoints controller, spelled as an operator reads it.
const trafficEndpointsCondition = "TrafficEndpointsReady"

// endpointsListed mirrors the endpoints controller over a fixed set of
// objects: for every revision with a pod among objs it adds the
// EndpointSlice of the revision's routed Service, listing the routed pods
// that report PodReady as Ready endpoints. The controller reads PodReady
// alone, so a pod whose serving gate is off but whose PodReady still
// stands is listed, as it is in a cluster until the kubelet folds the gate.
func endpointsListed(isvc *v1beta1.InferenceService, objs ...runtime.Object) []runtime.Object {
	type revision struct {
		component v1beta1.ComponentType
		hash      string
	}
	var order []revision
	listed := map[revision][]*corev1.Pod{}
	for _, obj := range objs {
		pod, ok := obj.(*corev1.Pod)
		if !ok || pod.Labels[query.LabelRevisionHash] == "" {
			continue
		}
		key := revision{component: v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel]), hash: pod.Labels[query.LabelRevisionHash]}
		if _, seen := listed[key]; !seen {
			order = append(order, key)
			listed[key] = nil
		}
		if query.RoutedRunner(pod) && podReady(pod) {
			listed[key] = append(listed[key], pod)
		}
	}
	out := append([]runtime.Object(nil), objs...)
	for _, key := range order {
		out = append(out, listedEndpoints(isvc, key.component, key.hash, listed[key]...))
	}
	return out
}

func podReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// listedEndpoints is the EndpointSlice the endpoints controller keeps for
// the routed Service of one revision: every pod given is a Ready endpoint.
func listedEndpoints(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, pods ...*corev1.Pod) *discoveryv1.EndpointSlice {
	service := PerRevisionServiceName(prefixFor(isvc, component), component, revisionHash)
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      service + "-abc12",
			Namespace: isvc.Namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: service},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
	}
	for i, pod := range pods {
		ready := true
		slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{
			Addresses:  []string{"10.0.0." + strconv.Itoa(i+1)},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name},
		})
	}
	return slice
}

// instancePod is a Running, PodReady pod of one Instance on one revision.
func instancePod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, idx int, ordinal int) *corev1.Pod {
	pod := buildPod(isvc, component, revisionHash, idx).(*corev1.Pod)
	pod.Name = pod.Name + "-" + strconv.Itoa(ordinal)
	pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(idx)
	return pod
}

// gateOff holds a pod out of rotation under the controller's serving gate,
// the way a surge drain leaves its source before the pod is deleted.
func gateOff(pod *corev1.Pod) *corev1.Pod {
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: query.ServingConditionType, Status: corev1.ConditionFalse})
	return pod
}

func trafficPercent(t *testing.T, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string) int32 {
	t.Helper()
	service := PerRevisionServiceName(prefixFor(isvc, component), component, revisionHash)
	for _, target := range isvc.Status.Components[component].Traffic {
		if target.RevisionName == service {
			return target.Percent
		}
	}
	return 0
}

// TestReconcile_TrafficWaitsForTheReplacementToBeListed: 4 engines roll
// under SurgeThenDrain at maxSurge 1. The replacement of Instance 0 turns
// Ready at 26 s and the engine holds the old pod out of rotation at once;
// the endpoints controller lists the replacement only at 73 s. The split
// must stay on version 1 until the replacement is a Ready endpoint of its
// routed Service, and the status must say what it waits for; at 73 s the
// split moves to 75/25 and the wait clears.
func TestReconcile_TrafficWaitsForTheReplacementToBeListed(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Rollout = singleEngineGroup()
	engine := v1beta1.EngineComponent
	old := make([]*corev1.Pod, 0, 4)
	for idx := 0; idx < 4; idx++ {
		old = append(old, instancePod(isvc, engine, "v1", idx, 0))
	}
	gateOff(old[0])
	replacement := instancePod(isvc, engine, "v2", 0, 1)
	objs := []runtime.Object{listedEndpoints(isvc, engine, "v1", old...), replacement}
	for _, pod := range old {
		objs = append(objs, pod)
	}
	c := testClient(objs...)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reconcile := func(at time.Duration) {
		t.Helper()
		if _, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC: isvc, Client: c, Reader: c, Now: start.Add(at),
			ComponentDeploymentModes: testOMENativeModes(engine),
		}); err != nil {
			t.Fatalf("reconcile at %s: %v", at, err)
		}
	}

	reconcile(26 * time.Second)
	if got := trafficPercent(t, isvc, engine, "v2"); got != 0 {
		t.Fatalf("at 26 s the split sends %d%% to version 2 before the endpoints controller lists its pod; want 0 (traffic=%+v)", got, isvc.Status.Components[engine].Traffic)
	}
	if got := trafficPercent(t, isvc, engine, "v1"); got != 100 {
		t.Fatalf("at 26 s version 1 carries %d%%; want 100", got)
	}
	cond := isvc.Status.GetCondition(apis.ConditionType(trafficEndpointsCondition))
	if cond == nil || cond.Status != corev1.ConditionFalse {
		t.Fatalf("at 26 s the status does not name the wait: %s=%v", trafficEndpointsCondition, cond)
	}
	if !strings.Contains(cond.Message, "waiting for endpoints: 1 pod Ready, 0 listed") {
		t.Fatalf("condition message %q does not say what the split waits for", cond.Message)
	}

	// 73 s: the endpoints controller lists the replacement.
	if err := c.Create(context.Background(), listedEndpoints(isvc, engine, "v2", replacement)); err != nil {
		t.Fatalf("list the replacement: %v", err)
	}
	reconcile(73 * time.Second)
	if got := trafficPercent(t, isvc, engine, "v2"); got != 25 {
		t.Fatalf("at 73 s version 2 carries %d%%; want 25 (traffic=%+v)", got, isvc.Status.Components[engine].Traffic)
	}
	if got := trafficPercent(t, isvc, engine, "v1"); got != 75 {
		t.Fatalf("at 73 s version 1 carries %d%%; want 75", got)
	}
	cond = isvc.Status.GetCondition(apis.ConditionType(trafficEndpointsCondition))
	if cond == nil || cond.Status != corev1.ConditionTrue {
		t.Fatalf("at 73 s the wait has not cleared: %s=%v", trafficEndpointsCondition, cond)
	}
	// The old pod of Instance 0 is still a listed endpoint while its gate is
	// off: it is leaving, and carries no weight.
	var slices discoveryv1.EndpointSliceList
	if err := c.List(context.Background(), &slices, client.InNamespace(isvc.Namespace)); err != nil {
		t.Fatalf("list slices: %v", err)
	}
	if len(slices.Items) != 2 {
		t.Fatalf("expected the two revision slices, got %d", len(slices.Items))
	}
}

// TestReconcile_TrafficCountsNothingBeforeTheFirstEndpoint: a fresh
// revision whose only pod is Ready but not yet a Ready endpoint of its
// routed Service carries no weight, and the status names the wait with
// the pod count.
func TestReconcile_TrafficCountsNothingBeforeTheFirstEndpoint(t *testing.T) {
	isvc := testOMENativeISVC()
	engine := v1beta1.EngineComponent
	served := []*corev1.Pod{instancePod(isvc, engine, "v1", 0, 0), instancePod(isvc, engine, "v1", 1, 0)}
	coming := []*corev1.Pod{instancePod(isvc, engine, "v2", 0, 1), instancePod(isvc, engine, "v2", 1, 1)}
	objs := []runtime.Object{listedEndpoints(isvc, engine, "v1", served...), listedEndpoints(isvc, engine, "v2")}
	for _, pod := range append(served, coming...) {
		objs = append(objs, pod)
	}
	c := testClient(objs...)
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
		ComponentDeploymentModes: testOMENativeModes(engine),
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := trafficPercent(t, isvc, engine, "v2"); got != 0 {
		t.Fatalf("version 2 carries %d%% with an empty slice; want 0", got)
	}
	cond := isvc.Status.GetCondition(apis.ConditionType(trafficEndpointsCondition))
	if cond == nil || cond.Status != corev1.ConditionFalse || !strings.Contains(cond.Message, "waiting for endpoints: 2 pods Ready, 0 listed") {
		t.Fatalf("the status does not name the wait for both pods: %v", cond)
	}
}
