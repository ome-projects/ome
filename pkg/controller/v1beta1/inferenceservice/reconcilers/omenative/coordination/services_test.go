package coordination

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

func TestPerRevisionServiceName(t *testing.T) {
	got := PerRevisionServiceName("llama", v1beta1.EngineComponent, "abcd1234")
	if got != "llama-engine-rev-abcd1234" {
		t.Errorf("got %q want llama-engine-rev-abcd1234", got)
	}
}

func TestPerRevisionHeadlessServiceName(t *testing.T) {
	got := PerRevisionHeadlessServiceName("llama", v1beta1.EngineComponent, "abcd1234")
	if got != "llama-engine-rev-abcd1234-headless" {
		t.Errorf("got %q want llama-engine-rev-abcd1234-headless", got)
	}
}

func TestBuildPerRevisionRoutingService_Properties(t *testing.T) {
	isvc := testISVC()
	svc, err := BuildPerRevisionRoutingService(isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, runnerPorts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Spec.Type: got %q want ClusterIP", svc.Spec.Type)
	}
	if svc.Spec.ClusterIP == corev1.ClusterIPNone {
		t.Errorf("routing service must not be headless")
	}
	if svc.Spec.PublishNotReadyAddresses {
		t.Errorf("routing service should publish only ready addresses")
	}
	checkSelector(t, svc.Spec.Selector, "llama-70b", "engine", "hash1")
	// Single-pod routing Service must NOT carry the rank-0 filter —
	// SurgeThenDrain alternates pod-naming ordinals.
	if _, ok := svc.Spec.Selector[query.LabelPodOrdinal]; ok {
		t.Errorf("single-pod routing Service selector must NOT include %s; got %v",
			query.LabelPodOrdinal, svc.Spec.Selector)
	}
	checkOwnerRef(t, svc, isvc)
}

// A referenced role's per-revision Services are named with the replica's
// own prefix and select its labels, while the InferenceService still owns
// them; a projected role keeps the service name.
func TestBuildPerRevisionServices_ReferencedRole(t *testing.T) {
	isvc := testISVC()
	isvc.Spec.ReplicaRefs = &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}, Decoder: []string{"pool-d"}}

	routing, err := BuildPerRevisionRoutingService(isvc, v1beta1.DecoderComponent, "abc", RevisionRoutingSelector{}, runnerPorts())
	if err != nil {
		t.Fatalf("routing: %v", err)
	}
	if routing.Name != "pool-d-decoder-rev-abc" {
		t.Errorf("routing name: got %q want pool-d-decoder-rev-abc", routing.Name)
	}
	checkSelector(t, routing.Spec.Selector, "pool-d", "decoder", "abc")
	checkOwnerRef(t, routing, isvc)

	headless, err := BuildPerRevisionHeadlessService(isvc, v1beta1.DecoderComponent, "abc")
	if err != nil {
		t.Fatalf("headless: %v", err)
	}
	if headless.Name != "pool-d-decoder-rev-abc-headless" {
		t.Errorf("headless name: got %q want pool-d-decoder-rev-abc-headless", headless.Name)
	}
	checkSelector(t, headless.Spec.Selector, "pool-d", "decoder", "abc")
	checkOwnerRef(t, headless, isvc)

	projected, err := BuildPerRevisionRoutingService(testISVC(), v1beta1.EngineComponent, "abc", RevisionRoutingSelector{}, runnerPorts())
	if err != nil {
		t.Fatalf("projected routing: %v", err)
	}
	if projected.Name != "llama-70b-engine-rev-abc" {
		t.Errorf("projected routing name: got %q want llama-70b-engine-rev-abc", projected.Name)
	}
	checkSelector(t, projected.Spec.Selector, "llama-70b", "engine", "abc")
}

func TestEnsurePerRevisionServices_ReferencedRole(t *testing.T) {
	c := fakeClient()
	isvc := testISVC()
	isvc.Spec.ReplicaRefs = &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}, Decoder: []string{"pool-d"}}
	out, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.DecoderComponent, "hash1", RevisionRoutingSelector{}, runnerPorts())
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if out.RoutingName != "pool-d-decoder-rev-hash1" || out.HeadlessName != "pool-d-decoder-rev-hash1-headless" {
		t.Errorf("names: got %+v", out)
	}
	for _, name := range []string{out.RoutingName, out.HeadlessName} {
		svc := &corev1.Service{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: name}, svc); err != nil {
			t.Fatalf("service %s missing: %v", name, err)
		}
		checkSelector(t, svc.Spec.Selector, "pool-d", "decoder", "hash1")
		checkOwnerRef(t, svc, isvc)
	}
	if err := GCPerRevisionServices(context.Background(), c, isvc.Namespace, "pool-d", v1beta1.DecoderComponent, "hash1"); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}, &corev1.Service{}); !apierrors.IsNotFound(err) {
		t.Errorf("routing service after gc: err=%v want NotFound", err)
	}
}

func TestBuildPerRevisionRoutingService_LeaderAndOrdinalFilters(t *testing.T) {
	isvc := testISVC()
	svc, err := BuildPerRevisionRoutingService(isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{LeaderOnly: true, PodOrdinal: true}, runnerPorts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Multi-pod routing Service MUST carry the rank-0 selector so
	// customer traffic lands only on the leader pod.
	got, ok := svc.Spec.Selector[query.LabelPodOrdinal]
	if !ok || got != "0" {
		t.Errorf("multi-pod routing Service must have %s=0; got %q (ok=%v); selector=%v",
			query.LabelPodOrdinal, got, ok, svc.Spec.Selector)
	}
	// The other selector keys still hold.
	checkSelector(t, svc.Spec.Selector, "llama-70b", "engine", "hash1")
	// Service Labels (used for catalog/UI) intentionally exclude the
	// pod-ordinal filter — labels describe the Service itself, not
	// the pod membership filter.
	if _, ok := svc.Labels[query.LabelPodOrdinal]; ok {
		t.Errorf("Service Labels must NOT include %s; got %v",
			query.LabelPodOrdinal, svc.Labels)
	}
}

func TestBuildPerRevisionRoutingService_WithoutOrdinalMatchesLeader(t *testing.T) {
	svc, err := BuildPerRevisionRoutingService(testISVC(), v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{LeaderOnly: true}, runnerPorts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := svc.Spec.Selector[query.LabelPodOrdinal]; ok {
		t.Errorf("selector must not require absent %s: %v", query.LabelPodOrdinal, svc.Spec.Selector)
	}

	leader := revisionPodLabels("hash1", string(v1beta1.RunnerNameLeader), "")
	worker := revisionPodLabels("hash1", string(v1beta1.RunnerNameWorker), "")
	if !routingSelectorMatches(svc.Spec.Selector, leader) {
		t.Errorf("leader labels without ordinal must match routing selector: selector=%v labels=%v", svc.Spec.Selector, leader)
	}
	if routingSelectorMatches(svc.Spec.Selector, worker) {
		t.Errorf("worker labels without ordinal must not match routing selector: selector=%v labels=%v", svc.Spec.Selector, worker)
	}
}

func TestBuildPerRevisionRoutingService_WithOrdinalMatchesLeaderOnly(t *testing.T) {
	svc, err := BuildPerRevisionRoutingService(testISVC(), v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{LeaderOnly: true, PodOrdinal: true}, runnerPorts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	leader := revisionPodLabels("hash1", string(v1beta1.RunnerNameLeader), "0")
	worker := revisionPodLabels("hash1", string(v1beta1.RunnerNameWorker), "0")
	if !routingSelectorMatches(svc.Spec.Selector, leader) {
		t.Errorf("leader labels must match routing selector: selector=%v labels=%v", svc.Spec.Selector, leader)
	}
	if routingSelectorMatches(svc.Spec.Selector, worker) {
		t.Errorf("worker labels must not match routing selector: selector=%v labels=%v", svc.Spec.Selector, worker)
	}
}

func TestBuildPerRevisionRoutingService_SinglePodSelectorsStayBroad(t *testing.T) {
	svc, err := BuildPerRevisionRoutingService(testISVC(), v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, runnerPorts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for name, podLabels := range map[string]map[string]string{
		"default runner": revisionPodLabels("hash1", string(v1beta1.RunnerNameDefault), "0"),
		"missing labels": revisionPodLabels("hash1", "", ""),
	} {
		t.Run(name, func(t *testing.T) {
			if !routingSelectorMatches(svc.Spec.Selector, podLabels) {
				t.Errorf("single-pod labels must match broad routing selector: selector=%v labels=%v", svc.Spec.Selector, podLabels)
			}
		})
	}
}

func TestBuildPerRevisionRoutingService_OrdinalWithoutLeaderStaysBroad(t *testing.T) {
	svc, err := BuildPerRevisionRoutingService(testISVC(), v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{PodOrdinal: true}, runnerPorts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := svc.Spec.Selector[query.LabelRunner]; ok {
		t.Errorf("ordinal-only routing selector must not add %s: %v", query.LabelRunner, svc.Spec.Selector)
	}
	if _, ok := svc.Spec.Selector[query.LabelPodOrdinal]; ok {
		t.Errorf("ordinal-only routing selector must not add %s: %v", query.LabelPodOrdinal, svc.Spec.Selector)
	}
}

func TestBuildPerRevisionHeadlessService_Properties(t *testing.T) {
	isvc := testISVC()
	svc, err := BuildPerRevisionHeadlessService(isvc, v1beta1.EngineComponent, "hash1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("headless ClusterIP: got %q want None", svc.Spec.ClusterIP)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Errorf("headless must publish not-ready addresses")
	}
	checkSelector(t, svc.Spec.Selector, "llama-70b", "engine", "hash1")
	// Headless Service NEVER pins the rank-0 selector — workers must
	// stay discoverable for peer DNS during distributed init.
	if _, ok := svc.Spec.Selector[query.LabelPodOrdinal]; ok {
		t.Errorf("headless Service selector must NEVER include %s; got %v",
			query.LabelPodOrdinal, svc.Spec.Selector)
	}
	checkOwnerRef(t, svc, isvc)
}

func TestBuildPerRevisionRoutingService_RejectsNilISVC(t *testing.T) {
	if _, err := BuildPerRevisionRoutingService(nil, v1beta1.EngineComponent, "hash", RevisionRoutingSelector{}, runnerPorts()); err == nil {
		t.Errorf("nil ISVC: want error")
	}
}

func TestBuildPerRevisionRoutingService_RejectsEmptyHash(t *testing.T) {
	if _, err := BuildPerRevisionRoutingService(testISVC(), v1beta1.EngineComponent, "", RevisionRoutingSelector{}, runnerPorts()); err == nil {
		t.Errorf("empty hash: want error")
	}
}

// The routing Service must publish the port the runner actually listens on;
// a Service on any other port black-holes traffic while its EndpointSlice
// still reports ready.
func TestBuildPerRevisionRoutingService_PublishesRunnerServingPort(t *testing.T) {
	cases := []struct {
		name         string
		ports        []corev1.ContainerPort
		wantName     string
		wantPort     int32
		wantTarget   intstr.IntOrString
		wantProtocol corev1.Protocol
	}{
		{
			name: "named http wins over the first declared port",
			ports: []corev1.ContainerPort{
				{Name: "dist", ContainerPort: 5000, Protocol: corev1.ProtocolTCP},
				{Name: "http", ContainerPort: 8000, Protocol: corev1.ProtocolTCP},
				{Name: "metrics", ContainerPort: 9090, Protocol: corev1.ProtocolTCP},
			},
			wantName:     "http",
			wantPort:     8000,
			wantTarget:   intstr.FromString("http"),
			wantProtocol: corev1.ProtocolTCP,
		},
		{
			name:         "no http name falls back to the first declared port",
			ports:        []corev1.ContainerPort{{Name: "serve", ContainerPort: 30000}, {Name: "dist", ContainerPort: 5000}},
			wantName:     "serve",
			wantPort:     30000,
			wantTarget:   intstr.FromString("serve"),
			wantProtocol: corev1.ProtocolTCP,
		},
		{
			name:         "unnamed port targets the number",
			ports:        []corev1.ContainerPort{{ContainerPort: 8000}},
			wantName:     "",
			wantPort:     8000,
			wantTarget:   intstr.FromInt32(8000),
			wantProtocol: corev1.ProtocolTCP,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := BuildPerRevisionRoutingService(testISVC(), v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, tc.ports)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(svc.Spec.Ports) != 1 {
				t.Fatalf("Ports: got %d want 1 (%+v)", len(svc.Spec.Ports), svc.Spec.Ports)
			}
			got := svc.Spec.Ports[0]
			if got.Name != tc.wantName {
				t.Errorf("Ports[0].Name: got %q want %q", got.Name, tc.wantName)
			}
			if got.Port != tc.wantPort {
				t.Errorf("Ports[0].Port: got %d want %d", got.Port, tc.wantPort)
			}
			if got.TargetPort != tc.wantTarget {
				t.Errorf("Ports[0].TargetPort: got %v want %v", got.TargetPort, tc.wantTarget)
			}
			if got.Protocol != tc.wantProtocol {
				t.Errorf("Ports[0].Protocol: got %q want %q", got.Protocol, tc.wantProtocol)
			}
		})
	}
}

// A runner that declares no port yields no routing Service rather than one
// published on an invented number.
func TestBuildPerRevisionRoutingService_NoDeclaredPort(t *testing.T) {
	for _, ports := range [][]corev1.ContainerPort{nil, {}} {
		svc, err := BuildPerRevisionRoutingService(testISVC(), v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, ports)
		if !errors.Is(err, ErrNoServingPort) {
			t.Errorf("ports=%v: got err %v want ErrNoServingPort", ports, err)
		}
		if svc != nil {
			t.Errorf("ports=%v: got service %+v want nil", ports, svc)
		}
	}
}

func TestEnsurePerRevisionServices_Idempotent(t *testing.T) {
	c := fakeClient()
	isvc := testISVC()
	out, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, runnerPorts())
	if err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	if out.RoutingName != "llama-70b-engine-rev-hash1" || out.HeadlessName != "llama-70b-engine-rev-hash1-headless" {
		t.Errorf("names: got %+v", out)
	}
	// Second call must not error and must not double-create.
	out2, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, runnerPorts())
	if err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	if out2 != out {
		t.Errorf("idempotent ensure should produce same names: %+v vs %+v", out, out2)
	}
	// Verify both services are present.
	routing := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}, routing); err != nil {
		t.Errorf("routing service missing: %v", err)
	}
	headless := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.HeadlessName}, headless); err != nil {
		t.Errorf("headless service missing: %v", err)
	}
	if len(routing.Spec.Ports) != 1 || routing.Spec.Ports[0].Port != 8000 {
		t.Errorf("routing ports: got %+v want a single port 8000", routing.Spec.Ports)
	}
	// The headless variant carries no ports — peer DNS resolves pod IPs.
	if len(headless.Spec.Ports) != 0 {
		t.Errorf("headless ports: got %+v want none", headless.Spec.Ports)
	}
}

func TestEnsurePerRevisionServices_LeaderFiltersApplyOnlyToRouting(t *testing.T) {
	c := fakeClient()
	isvc := testISVC()
	out, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{LeaderOnly: true, PodOrdinal: true}, runnerPorts())
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// Routing Service carries the rank-0 filter on its persisted shape.
	routing := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}, routing); err != nil {
		t.Fatalf("routing service missing: %v", err)
	}
	if got, ok := routing.Spec.Selector[query.LabelPodOrdinal]; !ok || got != "0" {
		t.Errorf("multi-pod routing Service selector must have %s=0; got %q (ok=%v)",
			query.LabelPodOrdinal, got, ok)
	}
	// runner=leader is required alongside pod-ordinal=0: ordinals are per
	// runner, so the rank-0 worker also carries pod-ordinal=0; without this the
	// routing Service would admit a worker.
	if got, ok := routing.Spec.Selector[query.LabelRunner]; !ok || got != string(v1beta1.RunnerNameLeader) {
		t.Errorf("multi-pod routing Service selector must have %s=leader; got %q (ok=%v)",
			query.LabelRunner, got, ok)
	}
	// Headless Service stays broad — workers must remain discoverable.
	headless := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.HeadlessName}, headless); err != nil {
		t.Fatalf("headless service missing: %v", err)
	}
	if _, ok := headless.Spec.Selector[query.LabelPodOrdinal]; ok {
		t.Errorf("headless Service selector must NEVER include %s; got %v",
			query.LabelPodOrdinal, headless.Spec.Selector)
	}
	if _, ok := headless.Spec.Selector[query.LabelRunner]; ok {
		t.Errorf("headless Service selector must NEVER include %s; got %v",
			query.LabelRunner, headless.Spec.Selector)
	}
}

func TestEnsurePerRevisionServices_UpdatesBroadSelectorWithoutClearingClusterIP(t *testing.T) {
	ctx := context.Background()
	c := fakeClient()
	isvc := testISVC()
	out, err := EnsurePerRevisionServices(ctx, c, isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, runnerPorts())
	if err != nil {
		t.Fatalf("initial ensure: %v", err)
	}

	routing := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}
	if err := c.Get(ctx, key, routing); err != nil {
		t.Fatalf("get initial routing Service: %v", err)
	}
	routing.Spec.ClusterIP = "10.0.0.10"
	routing.Spec.ClusterIPs = []string{"10.0.0.10"}
	if err := c.Update(ctx, routing); err != nil {
		t.Fatalf("set allocated ClusterIP: %v", err)
	}

	if _, err := EnsurePerRevisionServices(ctx, c, isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{LeaderOnly: true, PodOrdinal: true}, runnerPorts()); err != nil {
		t.Fatalf("leader-only ensure: %v", err)
	}
	if err := c.Get(ctx, key, routing); err != nil {
		t.Fatalf("get updated routing Service: %v", err)
	}
	if routing.Spec.ClusterIP != "10.0.0.10" || len(routing.Spec.ClusterIPs) != 1 || routing.Spec.ClusterIPs[0] != "10.0.0.10" {
		t.Errorf("allocated ClusterIP changed: ClusterIP=%q ClusterIPs=%v", routing.Spec.ClusterIP, routing.Spec.ClusterIPs)
	}
	if routing.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Service type: got %q want %q", routing.Spec.Type, corev1.ServiceTypeClusterIP)
	}
	if got := routing.Spec.Selector[query.LabelRunner]; got != string(v1beta1.RunnerNameLeader) {
		t.Errorf("runner selector: got %q want %q", got, v1beta1.RunnerNameLeader)
	}
	if got := routing.Spec.Selector[query.LabelPodOrdinal]; got != "0" {
		t.Errorf("ordinal selector: got %q want 0", got)
	}
}

func TestEnsurePerRevisionServices_WithoutOrdinalPinsLeaderOnly(t *testing.T) {
	c := fakeClient()
	isvc := testISVC()
	out, err := EnsurePerRevisionServices(
		context.Background(),
		c,
		isvc,
		v1beta1.EngineComponent,
		"hash1",
		RevisionRoutingSelector{LeaderOnly: true},
		runnerPorts(),
	)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	routing := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}, routing); err != nil {
		t.Fatalf("routing service missing: %v", err)
	}
	if _, ok := routing.Spec.Selector[query.LabelPodOrdinal]; ok {
		t.Errorf("routing selector must not require absent %s: %v", query.LabelPodOrdinal, routing.Spec.Selector)
	}
	leader := revisionPodLabels("hash1", string(v1beta1.RunnerNameLeader), "")
	worker := revisionPodLabels("hash1", string(v1beta1.RunnerNameWorker), "")
	if !routingSelectorMatches(routing.Spec.Selector, leader) {
		t.Errorf("leader labels without ordinal must match persisted routing selector: selector=%v labels=%v", routing.Spec.Selector, leader)
	}
	if routingSelectorMatches(routing.Spec.Selector, worker) {
		t.Errorf("worker labels without ordinal must not match persisted routing selector: selector=%v labels=%v", routing.Spec.Selector, worker)
	}
}

func TestEnsurePerRevisionServices_NilClientErrors(t *testing.T) {
	_, err := EnsurePerRevisionServices(context.Background(), nil, testISVC(), v1beta1.EngineComponent, "hash", RevisionRoutingSelector{}, runnerPorts())
	if err == nil {
		t.Errorf("nil client: want error")
	}
}

// With no declared runner port the routing Service is skipped — no invented
// port, no failed reconcile — while the headless Service (peer DNS for
// distributed init, independent of the serving port) is still ensured.
func TestEnsurePerRevisionServices_NoDeclaredPortSkipsRoutingKeepsHeadless(t *testing.T) {
	c := fakeClient()
	isvc := testISVC()
	out, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, nil)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if out.RoutingName != "" {
		t.Errorf("RoutingName: got %q want empty (routing Service skipped)", out.RoutingName)
	}
	routing := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "hash1")}
	if err := c.Get(context.Background(), key, routing); !apierrors.IsNotFound(err) {
		t.Errorf("routing service must not be created: err=%v svc=%+v", err, routing)
	}
	headless := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.HeadlessName}, headless); err != nil {
		t.Errorf("headless service missing: %v", err)
	}
}

func TestGCPerRevisionServices_DeletesBoth(t *testing.T) {
	c := fakeClient()
	isvc := testISVC()
	out, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, runnerPorts())
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := GCPerRevisionServices(context.Background(), c, isvc.Namespace, isvc.Name, v1beta1.EngineComponent, "hash1"); err != nil {
		t.Fatalf("GC: %v", err)
	}
	routing := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}, routing); !apierrors.IsNotFound(err) {
		t.Errorf("routing should be deleted: %v", err)
	}
	headless := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.HeadlessName}, headless); !apierrors.IsNotFound(err) {
		t.Errorf("headless should be deleted: %v", err)
	}
}

func TestGCPerRevisionServices_NotFoundIsNotAnError(t *testing.T) {
	c := fakeClient()
	if err := GCPerRevisionServices(context.Background(), c, "ns", "llama", v1beta1.EngineComponent, "missing"); err != nil {
		t.Errorf("GC of missing service: %v", err)
	}
}

func TestGCPerRevisionServices_NilClientErrors(t *testing.T) {
	if err := GCPerRevisionServices(context.Background(), nil, "ns", "llama", v1beta1.EngineComponent, "hash"); err == nil {
		t.Errorf("nil client: want error")
	}
}

func checkSelector(t *testing.T, sel map[string]string, isvc, component, hash string) {
	t.Helper()
	if sel[constants.InferenceServicePodLabelKey] != isvc {
		t.Errorf("selector inferenceservice: got %q want %q", sel[constants.InferenceServicePodLabelKey], isvc)
	}
	if sel[constants.OMEComponentLabel] != component {
		t.Errorf("selector component: got %q want %q", sel[constants.OMEComponentLabel], component)
	}
	if sel[query.LabelRevisionHash] != hash {
		t.Errorf("selector revision-hash: got %q want %q", sel[query.LabelRevisionHash], hash)
	}
	if sel[query.LabelManagedBy] != query.ManagedByOMENative {
		t.Errorf("selector managed-by: got %q want %q", sel[query.LabelManagedBy], query.ManagedByOMENative)
	}
}

func checkOwnerRef(t *testing.T, svc *corev1.Service, isvc *v1beta1.InferenceService) {
	t.Helper()
	if len(svc.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences: got %d want 1", len(svc.OwnerReferences))
	}
	ref := svc.OwnerReferences[0]
	if ref.Name != isvc.Name || ref.Kind != "InferenceService" {
		t.Errorf("OwnerRef target: got Kind=%q Name=%q", ref.Kind, ref.Name)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("Controller flag: want true, got %v", ref.Controller)
	}
}

func revisionPodLabels(revisionHash, runner, ordinal string) map[string]string {
	podLabels := map[string]string{
		constants.InferenceServicePodLabelKey: "llama-70b",
		constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
		query.LabelRevisionHash:               revisionHash,
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	if runner != "" {
		podLabels[query.LabelRunner] = runner
	}
	if ordinal != "" {
		podLabels[query.LabelPodOrdinal] = ordinal
	}
	return podLabels
}

func routingSelectorMatches(selector, podLabels map[string]string) bool {
	return labels.SelectorFromSet(selector).Matches(labels.Set(podLabels))
}

func testISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-70b",
			Namespace: "prod",
			UID:       types.UID("llama-70b-uid"),
		},
	}
}

// runnerPorts is the port set a typical serving template declares: an `http`
// listener alongside a distributed-init port.
func runnerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
		{Name: "http", ContainerPort: 8000, Protocol: corev1.ProtocolTCP},
		{Name: "dist", ContainerPort: 5000, Protocol: corev1.ProtocolTCP},
	}
}

// testComponentRunnerPorts mirrors what the controller threads in: the
// effective serving ports of every Component the coordination layer may touch.
func testComponentRunnerPorts() map[v1beta1.ComponentType][]corev1.ContainerPort {
	return map[v1beta1.ComponentType][]corev1.ContainerPort{
		v1beta1.EngineComponent:  runnerPorts(),
		v1beta1.DecoderComponent: runnerPorts(),
		v1beta1.RouterComponent:  runnerPorts(),
	}
}

func fakeClient(initObjs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(initObjs...).Build()
}
