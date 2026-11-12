package coordination

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
)

// The appProtocol values the fixtures declare. The builder copies whatever a
// Component declares, so these are test data only.
const (
	declaredH2C = "kubernetes.io/h2c"
	declaredWS  = "kubernetes.io/ws"
)

// namedPort is one TCP container port.
func namedPort(name string, number int32) corev1.ContainerPort {
	return corev1.ContainerPort{Name: name, ContainerPort: number, Protocol: corev1.ProtocolTCP}
}

// multiListenerPorts is an engine template that opens one listener per
// serving process and names none of them `http`.
func multiListenerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{namedPort("engine1", 8001), namedPort("engine2", 8002), namedPort("engine3", 8003), namedPort("engine4", 8004)}
}

// onlyRoutingPort is the one port a routing Service publishes.
func onlyRoutingPort(t *testing.T, svc *corev1.Service) corev1.ServicePort {
	t.Helper()
	if svc == nil || len(svc.Spec.Ports) != 1 {
		t.Fatalf("routing Service must publish exactly one port, got %+v", svc)
	}
	return svc.Spec.Ports[0]
}

// storedRoutingService reads a revision's routing Service from the client.
func storedRoutingService(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, hash string) *corev1.Service {
	t.Helper()
	svc := &corev1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: PerRevisionServiceName(isvc.Name, component, hash)}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Fatalf("%s routing Service for revision %s: %v", component, hash, err)
	}
	return svc
}

// checkAppProtocol compares a port's appProtocol with the one wanted; nil
// means none.
func checkAppProtocol(t *testing.T, what string, got, want *string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s AppProtocol: got %q want %q", what, ptr.Deref(got, ""), ptr.Deref(want, ""))
	}
}

// The routing Service publishes one port and gives it the appProtocol the
// Component declares for that port's own name, by the stable Service's rule:
// no declarations, an empty value or a value under another name leave it
// unset, whatever the other Components declare. Each row also builds the
// stable Service from the same ports and declarations and checks that the
// two Services agree on the port they share.
func TestBuildPerRevisionRoutingService_AppProtocolFollowsThePublishedPortName(t *testing.T) {
	otherName := []corev1.ContainerPort{namedPort("serve", 30000), namedPort("bootstrap", 8998)}
	cases := []struct {
		name      string
		component v1beta1.ComponentType
		ports     []corev1.ContainerPort
		declared  map[string]string
		wantPort  string
		want      *string
	}{
		{name: "declared for the http port", component: v1beta1.RouterComponent, ports: runnerPorts(), declared: map[string]string{"http": declaredH2C}, wantPort: "http", want: ptr.To(declaredH2C)},
		{name: "no declarations", component: v1beta1.RouterComponent, ports: runnerPorts(), wantPort: "http"},
		{name: "an empty map", component: v1beta1.RouterComponent, ports: runnerPorts(), declared: map[string]string{}, wantPort: "http"},
		{name: "an empty value", component: v1beta1.RouterComponent, ports: runnerPorts(), declared: map[string]string{"http": ""}, wantPort: "http"},
		{name: "declared only for a port the Service does not publish", component: v1beta1.RouterComponent, ports: runnerPorts(), declared: map[string]string{"dist": declaredH2C}, wantPort: "http"},
		{name: "an engine with one listener per process declaring http", component: v1beta1.EngineComponent, ports: multiListenerPorts(), declared: map[string]string{"http": declaredH2C}, wantPort: "engine1"},
		{name: "an engine declaring its first listener beside http", component: v1beta1.EngineComponent, ports: multiListenerPorts(), declared: map[string]string{"http": declaredH2C, "engine1": declaredWS}, wantPort: "engine1", want: ptr.To(declaredWS)},
		{name: "a decoder serving on another name declaring http", component: v1beta1.DecoderComponent, ports: otherName, declared: map[string]string{"http": declaredH2C}, wantPort: "serve"},
		{name: "a decoder serving on http declaring nothing", component: v1beta1.DecoderComponent, ports: runnerPorts(), wantPort: "http"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := BuildPerRevisionRoutingService(testISVC(), tc.component, "hash1", RevisionRoutingSelector{}, tc.ports, tc.declared)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var wantNumber int32
			for _, p := range tc.ports {
				if p.Name == tc.wantPort {
					wantNumber = p.ContainerPort
				}
			}
			port := onlyRoutingPort(t, svc)
			if port.Name != tc.wantPort || port.Port != wantNumber || port.TargetPort != intstr.FromString(tc.wantPort) {
				t.Errorf("published port: got %+v want %s %d targeting %s", port, tc.wantPort, wantNumber, tc.wantPort)
			}
			checkAppProtocol(t, "per-revision port "+port.Name, port.AppProtocol, tc.want)

			ext := &v1beta1.ComponentExtensionSpec{ServicePortAppProtocols: tc.declared}
			podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Ports: tc.ports}}}
			stable := service.NewServiceReconciler(nil, nil, metav1.ObjectMeta{Name: "llama-70b-" + string(tc.component), Namespace: "prod"}, ext, podSpec, nil).Service
			for _, stablePort := range stable.Spec.Ports {
				if stablePort.Name != port.Name {
					continue
				}
				if !reflect.DeepEqual(stablePort.AppProtocol, port.AppProtocol) {
					t.Errorf("port %s: the stable Service says %q, the per-revision Service %q", port.Name, ptr.Deref(stablePort.AppProtocol, ""), ptr.Deref(port.AppProtocol, ""))
				}
				return
			}
			t.Errorf("stable Service has no port named %q: %+v", port.Name, stable.Spec.Ports)
		})
	}
}

// The two writers of a revision's routing Service. The ensure path rewrites
// the port on every pass: a Service created before its Component's
// declaration reached it gains the appProtocol in place and loses it again
// when the declaration goes, while the allocated ClusterIP, the selector,
// the type and the port mapping stay as they were. The create-if-absent
// path, which a peer replica runs before the pods exist, publishes a
// declaration it is given and leaves an existing Service as found; the next
// ensure writes the declaration onto it.
func TestPerRevisionRoutingService_AppProtocolWritePaths(t *testing.T) {
	type writer struct {
		name  string
		write func(context.Context, client.Client, *v1beta1.InferenceService, map[string]string) error
	}
	ensure := writer{"ensure", func(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, declared map[string]string) error {
		_, err := EnsurePerRevisionServices(ctx, c, isvc, v1beta1.RouterComponent, "hash1", RevisionRoutingSelector{}, runnerPorts(), declared)
		return err
	}}
	create := writer{"create-if-absent", func(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, declared map[string]string) error {
		return CreatePerRevisionServicesIfAbsent(ctx, c, isvc, v1beta1.RouterComponent, "hash1", RevisionRoutingSelector{}, runnerPorts(), declared)
	}}
	type write struct {
		by       writer
		declared map[string]string
		want     *string
	}
	declared, h2c := map[string]string{"http": declaredH2C}, ptr.To(declaredH2C)
	cases := []struct {
		name   string
		writes []write
	}{
		{name: "the ensure path adds and removes the declaration in place", writes: []write{
			{by: ensure}, {by: ensure, declared: declared, want: h2c}, {by: ensure}}},
		{name: "the create path publishes a declaration it is given", writes: []write{
			{by: create, declared: declared, want: h2c}}},
		{name: "the create path leaves an existing Service as found until an ensure rewrites it", writes: []write{
			{by: create}, {by: create, declared: declared}, {by: ensure, declared: declared, want: h2c}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, c, isvc := context.Background(), fakeClient(), testISVC()
			var first *corev1.Service
			for i, w := range tc.writes {
				if err := w.by.write(ctx, c, isvc, w.declared); err != nil {
					t.Fatalf("write %d (%s): %v", i, w.by.name, err)
				}
				svc := storedRoutingService(t, c, isvc, v1beta1.RouterComponent, "hash1")
				checkAppProtocol(t, fmt.Sprintf("after write %d (%s)", i, w.by.name), onlyRoutingPort(t, svc).AppProtocol, w.want)
				if first == nil {
					first = allocateClusterIP(t, c, svc)
					continue
				}
				checkRoutingServiceKept(t, svc, first)
			}
		})
	}
}

// allocateClusterIP writes the ClusterIP the apiserver would allocate onto a
// stored routing Service and returns it as stored.
func allocateClusterIP(t *testing.T, c client.Client, svc *corev1.Service) *corev1.Service {
	t.Helper()
	svc.Spec.ClusterIP = "10.0.0.10"
	svc.Spec.ClusterIPs = []string{"10.0.0.10"}
	if err := c.Update(context.Background(), svc); err != nil {
		t.Fatalf("set allocated ClusterIP: %v", err)
	}
	return svc
}

// checkRoutingServiceKept fails when a rewrite moved anything but the port's
// appProtocol: the allocated ClusterIP, the selector, the type and the port
// mapping of a routing Service outlive every ensure.
func checkRoutingServiceKept(t *testing.T, got, before *corev1.Service) {
	t.Helper()
	if got.Spec.ClusterIP != before.Spec.ClusterIP || !reflect.DeepEqual(got.Spec.ClusterIPs, before.Spec.ClusterIPs) {
		t.Errorf("allocated ClusterIP changed: ClusterIP=%q ClusterIPs=%v", got.Spec.ClusterIP, got.Spec.ClusterIPs)
	}
	if !reflect.DeepEqual(got.Spec.Selector, before.Spec.Selector) {
		t.Errorf("selector changed: got %v want %v", got.Spec.Selector, before.Spec.Selector)
	}
	if got.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("Service type: got %q want ClusterIP", got.Spec.Type)
	}
	after, prior := onlyRoutingPort(t, got), onlyRoutingPort(t, before)
	after.AppProtocol, prior.AppProtocol = nil, nil
	if !reflect.DeepEqual(after, prior) {
		t.Errorf("port mapping changed: got %+v want %+v", after, prior)
	}
}

// The coordination pass hands each Component its own declarations. The
// router gets h2c on `http`. The engine serves on engine1 and gets the value
// it declared for engine1, not its `http` value. The decoder serves on `http`
// and declares nothing, so it gets nothing although the router declares `http`.
func TestReconcile_RevisionServicesCarryDeclaredAppProtocol(t *testing.T) {
	isvc := testOMENativeISVC()
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	c := testClient(
		buildPod(isvc, v1beta1.RouterComponent, "rhash", 0),
		buildPod(isvc, v1beta1.EngineComponent, "ehash", 1),
		buildPod(isvc, v1beta1.DecoderComponent, "dhash", 2),
	)
	ports := testComponentRunnerPorts()
	ports[v1beta1.EngineComponent] = multiListenerPorts()
	if _, err := Reconcile(context.Background(), ReconcileInputs{
		ISVC:                     isvc,
		Client:                   c,
		Reader:                   c,
		Now:                      time.Now(),
		ComponentDeploymentModes: testOMENativeModes(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent),
		ComponentRunnerPorts:     ports,
		ComponentServicePortAppProtocols: map[v1beta1.ComponentType]map[string]string{
			v1beta1.RouterComponent: {"http": declaredH2C},
			v1beta1.EngineComponent: {"http": declaredH2C, "engine1": declaredWS},
		},
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, tc := range []struct {
		component      v1beta1.ComponentType
		hash, wantPort string
		want           *string
	}{
		{component: v1beta1.RouterComponent, hash: "rhash", wantPort: "http", want: ptr.To(declaredH2C)},
		{component: v1beta1.EngineComponent, hash: "ehash", wantPort: "engine1", want: ptr.To(declaredWS)},
		{component: v1beta1.DecoderComponent, hash: "dhash", wantPort: "http"},
	} {
		port := onlyRoutingPort(t, storedRoutingService(t, c, isvc, tc.component, tc.hash))
		if port.Name != tc.wantPort {
			t.Errorf("%s routing port: got %s want %s", tc.component, port.Name, tc.wantPort)
		}
		checkAppProtocol(t, string(tc.component)+" routing port", port.AppProtocol, tc.want)
	}
}
