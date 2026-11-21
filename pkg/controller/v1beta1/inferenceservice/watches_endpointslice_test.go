package inferenceservice

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// The traffic table (Status.Components.<c>.Traffic) is written by the
// coordination pass from the pods the routed per-revision Services list as
// Ready endpoints. A pod that serves but is not listed yet carries nothing
// and the status names the wait. The pass that re-reads the list runs when
// an event enqueues the service; the EndpointSlice watch enqueues it
// through endpointSliceToISVC. This file reads that the slices of every
// Service the InferenceService controls enqueue it, and what the table
// does when the listing lands.

// sliceEventService is a service with two prefill Instances, OMENative, the
// per-revision Services the coordination pass creates, their slices, and
// the replica the pass reads the revision pair from.
type sliceEventService struct {
	t    *testing.T
	ctx  context.Context
	c    client.Client
	r    *InferenceServiceReconciler
	isvc *v1beta1.InferenceService
	pods map[string]*corev1.Pod
}

func newSliceEventService(t *testing.T) *sliceEventService {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, discoveryv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: "team-a", UID: types.UID("pool-uid")},
		Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)}},
		}},
	}
	replica := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-engine", Namespace: "team-a"},
		Spec:       v1beta1.InferenceReplicaSpec{Replicas: ptr.To(int32(2))},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas: 2, ReadyReplicas: 2, ServingReplicas: 2,
			CurrentRevision: "pool-engine-v1", UpdateRevision: "pool-engine-v2",
		},
	}
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(replica).Build()
	s := &sliceEventService{t: t, ctx: context.Background(), c: c, r: &InferenceServiceReconciler{Client: c}, isvc: isvc, pods: map[string]*corev1.Pod{}}
	for idx := 0; idx < 2; idx++ {
		s.pods[s.podName(idx)] = s.createPod(idx, "v1")
	}
	return s
}

func (s *sliceEventService) podName(idx int) string { return "pool-engine-" + strconv.Itoa(idx) }

// createPod creates a Running pod of one Instance on a revision, Ready
// with the serving gate on.
func (s *sliceEventService) createPod(idx int, revision string) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: s.podName(idx), Namespace: s.isvc.Namespace,
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: s.isvc.Name,
			constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
			query.LabelManagedBy:                  query.ManagedByOMENative,
			query.LabelRevisionHash:               revision,
			query.LabelInstanceIdx:                strconv.Itoa(idx),
		},
	}}
	if err := s.c.Create(s.ctx, pod); err != nil {
		s.t.Fatalf("create pod %d: %v", idx, err)
	}
	s.setPod(idx, revision, true)
	return pod
}

// setPod writes a pod's revision label and whether it serves.
func (s *sliceEventService) setPod(idx int, revision string, serving bool) {
	key := client.ObjectKey{Namespace: s.isvc.Namespace, Name: s.podName(idx)}
	pod := &corev1.Pod{}
	if err := s.c.Get(s.ctx, key, pod); err != nil {
		s.t.Fatalf("get pod %d: %v", idx, err)
	}
	pod.Labels[query.LabelRevisionHash] = revision
	if err := s.c.Update(s.ctx, pod); err != nil {
		s.t.Fatalf("relabel pod %d: %v", idx, err)
	}
	if err := s.c.Get(s.ctx, key, pod); err != nil {
		s.t.Fatalf("get pod %d: %v", idx, err)
	}
	status := corev1.ConditionFalse
	if serving {
		status = corev1.ConditionTrue
	}
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
		{Type: corev1.PodReady, Status: status},
		{Type: podreadiness.ConditionType, Status: status},
	}}
	if err := s.c.Status().Update(s.ctx, pod); err != nil {
		s.t.Fatalf("write pod %d status: %v", idx, err)
	}
}

func (s *sliceEventService) routedService(revision string) string {
	return coordination.PerRevisionServiceName(s.isvc.Name, v1beta1.EngineComponent, revision)
}

// list writes the EndpointSlice the endpoints controller keeps for one
// revision's routed Service, listing the pods given as Ready endpoints,
// and returns the slice as the watch would see it.
func (s *sliceEventService) list(revision string, idx ...int) *discoveryv1.EndpointSlice {
	service := s.routedService(revision)
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
		Name: service + "-x7k2p", Namespace: s.isvc.Namespace,
		Labels: map[string]string{discoveryv1.LabelServiceName: service},
	}, AddressType: discoveryv1.AddressTypeIPv4}
	for i, idx := range idx {
		slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{
			Addresses:  []string{"10.0.0." + strconv.Itoa(i+1)},
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: s.isvc.Namespace, Name: s.podName(idx)},
		})
	}
	stored := &discoveryv1.EndpointSlice{}
	err := s.c.Get(s.ctx, client.ObjectKeyFromObject(slice), stored)
	switch {
	case err == nil:
		stored.Endpoints = slice.Endpoints
		if err := s.c.Update(s.ctx, stored); err != nil {
			s.t.Fatalf("update slice of %s: %v", revision, err)
		}
		return stored
	default:
		if err := s.c.Create(s.ctx, slice); err != nil {
			s.t.Fatalf("create slice of %s: %v", revision, err)
		}
		return slice
	}
}

// enqueues reports whether a slice event enqueues the service, by the
// controller's own mapper.
func (s *sliceEventService) enqueues(slice *discoveryv1.EndpointSlice) bool {
	for _, req := range s.r.endpointSliceToISVC(s.ctx, slice) {
		if req.Name == s.isvc.Name && req.Namespace == s.isvc.Namespace {
			return true
		}
	}
	return false
}

// pass is one coordination pass, as the service controller runs it.
func (s *sliceEventService) pass() {
	if _, err := coordination.Reconcile(s.ctx, coordination.ReconcileInputs{
		ISVC: s.isvc, Client: s.c, Reader: s.c, Now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ComponentDeploymentModes: map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.OMENative},
		ComponentRunnerPorts:     map[v1beta1.ComponentType][]corev1.ContainerPort{v1beta1.EngineComponent: {{Name: "http", ContainerPort: 8000, Protocol: corev1.ProtocolTCP}}},
	}); err != nil {
		s.t.Fatalf("pass: %v", err)
	}
}

func (s *sliceEventService) table() map[string]int32 {
	out := map[string]int32{}
	for _, target := range s.isvc.Status.Components[v1beta1.EngineComponent].Traffic {
		out[query.RevisionFromName(target.RevisionName).Hash()] = target.Percent
	}
	return out
}

func (s *sliceEventService) wait() string {
	cond := s.isvc.Status.GetCondition(apis.ConditionType(v1beta1.TrafficEndpointsReady))
	if cond == nil || cond.Status != corev1.ConditionFalse {
		return ""
	}
	return cond.Message
}

// TestTrafficTable_TheRoutedListingThatLandsAfterThePassStartsThePass: two
// prefill Instances serve version 1. The operator pushes version 2 in
// place. Instance 0's pod comes back on version 2 and serves; Instance 1's
// old pod is between two exits and serves nothing. The pass the readiness
// enqueued runs before the endpoints controller lists the relabelled pod
// in version 2's routed Service, so it keeps version 1 at 100 and names
// the wait. The listing lands next and enqueues the service, because the
// routed Service's slice belongs to a Service the InferenceService
// controls; the pass it starts moves everything to version 2 and clears
// the wait.
func TestTrafficTable_TheRoutedListingThatLandsAfterThePassStartsThePass(t *testing.T) {
	s := newSliceEventService(t)
	s.list("v1", 0, 1)
	s.pass()
	if got := s.table(); len(got) != 1 || got["v1"] != 100 {
		t.Fatalf("at rest version 1 carries everything, got %v", got)
	}

	// Instance 0 is back on version 2 and serves; its old listing is gone.
	// Instance 1's old pod is between two exits.
	s.setPod(0, "v2", true)
	s.setPod(1, "v1", false)
	s.list("v1")
	s.pass()
	if got := s.table(); len(got) != 1 || got["v1"] != 100 {
		t.Fatalf("before the listing the table stands on version 1, got %v", got)
	}
	if msg := s.wait(); !strings.Contains(msg, "engine revision v2: waiting for endpoints: 1 pod Ready, 0 listed") {
		t.Fatalf("the status must name the wait, got %q", msg)
	}

	// The endpoints controller lists the relabelled pod in version 2's
	// routed Service, and the slice enqueues the service.
	slice := s.list("v2", 0)
	if !s.enqueues(slice) {
		t.Fatalf("the routed per-revision Service's slice must enqueue the service; the mapper enqueued %v", s.r.endpointSliceToISVC(s.ctx, slice))
	}

	// The pass the listing started reads it.
	s.pass()
	if got := s.table(); len(got) != 1 || got["v2"] != 100 {
		t.Fatalf("the pass must move everything to version 2, got %v", got)
	}
	if msg := s.wait(); msg != "" {
		t.Fatalf("the wait must clear, got %q", msg)
	}
}

// TestEndpointSliceToISVC_EveryServiceTheServiceControlsEnqueuesIt: the
// per-Component headless Service, the routed per-revision Service and its
// headless twin all belong to the InferenceService, and a slice of any of
// them enqueues it.
func TestEndpointSliceToISVC_EveryServiceTheServiceControlsEnqueuesIt(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, discoveryv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	owned := func(name string) *corev1.Service {
		return &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "team-a",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: "pool", UID: types.UID("pool-uid"), Controller: ptr.To(true),
			}},
		}}
	}
	r := &InferenceServiceReconciler{Client: fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(
		owned("pool-engine-headless"),
		owned("pool-engine-rev-abc12345"),
		owned("pool-engine-rev-abc12345-headless"),
	).Build()}
	slice := func(service string) *discoveryv1.EndpointSlice {
		return &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
			Name: service + "-x7k2p", Namespace: "team-a",
			Labels: map[string]string{discoveryv1.LabelServiceName: service},
		}}
	}
	for _, tc := range []struct {
		service  string
		enqueues bool
	}{
		{"pool-engine-headless", true},
		{"pool-engine-rev-abc12345", true},
		{"pool-engine-rev-abc12345-headless", true},
	} {
		got := r.endpointSliceToISVC(context.Background(), slice(tc.service))
		if (len(got) == 1 && got[0].Name == "pool") != tc.enqueues {
			t.Errorf("a slice of %s: enqueued %v, want enqueues=%v", tc.service, got, tc.enqueues)
		}
	}
}
