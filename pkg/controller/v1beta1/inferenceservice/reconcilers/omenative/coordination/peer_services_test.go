package coordination

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

func TestRoutingSelectorForRunners(t *testing.T) {
	single := []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	if got := RoutingSelectorForRunners(single); got != (RevisionRoutingSelector{}) {
		t.Errorf("single default Runner: got %+v want the broad selector", got)
	}
	multi := []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1},
		{Name: v1beta1.RunnerNameWorker, Size: 3},
	}
	if got := RoutingSelectorForRunners(multi); !got.LeaderOnly || !got.PodOrdinal {
		t.Errorf("leader/worker Runners: got %+v want leader-only at ordinal 0", got)
	}
}

func TestServingPortsFromRunners(t *testing.T) {
	leaderPorts := []corev1.ContainerPort{{Name: "http", ContainerPort: 8000}}
	workerPorts := []corev1.ContainerPort{{Name: "dist", ContainerPort: 5000}}
	runners := []v1beta1.Runner{
		{Name: v1beta1.RunnerNameWorker, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "w", Ports: workerPorts}},
		}}},
		{Name: v1beta1.RunnerNameLeader, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "l", Ports: leaderPorts}},
		}}},
	}
	got := ServingPortsFromRunners(runners)
	if len(got) != 1 || got[0].Name != "http" {
		t.Errorf("leader template must supply the serving ports, got %+v", got)
	}
	single := []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "main", Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8000}}},
			{Name: "side", Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}}},
		},
	}}}}
	if got := ServingPortsFromRunners(single); len(got) != 2 || got[0].Name != "http" {
		t.Errorf("default template ports in container order, got %+v", got)
	}
	if got := ServingPortsFromRunners(nil); got != nil {
		t.Errorf("no Runners yields no ports, got %+v", got)
	}
}

// TestCreatePerRevisionServicesIfAbsent_CreatesBothThenLeavesThem pins the
// create-before-pods path: both halves of the pair are created for a
// revision with no pods, selected by revision hash only, and a second call
// leaves an existing (drifted) Service exactly as found.
func TestCreatePerRevisionServicesIfAbsent_CreatesBothThenLeavesThem(t *testing.T) {
	isvc := testISVC()
	c := fakeClient()
	routing := RevisionRoutingSelector{LeaderOnly: true, PodOrdinal: true}
	if err := CreatePerRevisionServicesIfAbsent(context.Background(), c, isvc, v1beta1.DecoderComponent, "dec1", routing, runnerPorts()); err != nil {
		t.Fatalf("create: %v", err)
	}
	names := PerRevisionServiceNames(isvc.Name, v1beta1.DecoderComponent, "dec1")
	routingSvc := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: names.RoutingName}, routingSvc); err != nil {
		t.Fatalf("routing Service must exist before any pod: %v", err)
	}
	if routingSvc.Spec.Selector[query.LabelRevisionHash] != "dec1" {
		t.Errorf("routing selector must pin the revision hash, got %v", routingSvc.Spec.Selector)
	}
	if routingSvc.Spec.Selector[query.LabelRunner] != string(v1beta1.RunnerNameLeader) || routingSvc.Spec.Selector[query.LabelPodOrdinal] != "0" {
		t.Errorf("routing selector must honor the leader/ordinal shape, got %v", routingSvc.Spec.Selector)
	}
	if len(routingSvc.Spec.Ports) != 1 || routingSvc.Spec.Ports[0].Port != 8000 {
		t.Errorf("routing Service must publish the http port, got %+v", routingSvc.Spec.Ports)
	}
	headless := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: names.HeadlessName}, headless); err != nil {
		t.Fatalf("headless Service must exist before any pod: %v", err)
	}
	if headless.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("headless Service must be headless, got ClusterIP %q", headless.Spec.ClusterIP)
	}

	// Drift the routing Service; the create-only path must not touch it.
	routingSvc.Spec.Selector = map[string]string{query.LabelRevisionHash: "dec1"}
	if err := c.Update(context.Background(), routingSvc); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := CreatePerRevisionServicesIfAbsent(context.Background(), c, isvc, v1beta1.DecoderComponent, "dec1", routing, runnerPorts()); err != nil {
		t.Fatalf("second create: %v", err)
	}
	after := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: names.RoutingName}, after); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(after.Spec.Selector) != 1 {
		t.Errorf("create-only path must leave an existing Service untouched, got selector %v", after.Spec.Selector)
	}
}

// TestCreatePerRevisionServicesIfAbsent_NoPortSkipsRoutingOnly mirrors the
// ensure path's degrade: no serving port means no routing Service, but the
// headless Service is still created so peer DNS resolves.
func TestCreatePerRevisionServicesIfAbsent_NoPortSkipsRoutingOnly(t *testing.T) {
	isvc := testISVC()
	c := fakeClient()
	if err := CreatePerRevisionServicesIfAbsent(context.Background(), c, isvc, v1beta1.DecoderComponent, "dec1", RevisionRoutingSelector{}, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	names := PerRevisionServiceNames(isvc.Name, v1beta1.DecoderComponent, "dec1")
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: names.RoutingName}, &corev1.Service{}); err == nil {
		t.Error("routing Service must be skipped without a serving port")
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: names.HeadlessName}, &corev1.Service{}); err != nil {
		t.Errorf("headless Service must still be created: %v", err)
	}
}

// TestCreatePerRevisionServicesIfAbsent_NilInputs pins the no-op contract
// for an empty hash and the error for a nil client.
func TestCreatePerRevisionServicesIfAbsent_NilInputs(t *testing.T) {
	if err := CreatePerRevisionServicesIfAbsent(context.Background(), fakeClient(), testISVC(), v1beta1.DecoderComponent, "", RevisionRoutingSelector{}, nil); err != nil {
		t.Errorf("empty hash must be a no-op, got %v", err)
	}
	if err := CreatePerRevisionServicesIfAbsent(context.Background(), nil, testISVC(), v1beta1.DecoderComponent, "h", RevisionRoutingSelector{}, nil); err == nil {
		t.Error("nil client must error")
	}
}
