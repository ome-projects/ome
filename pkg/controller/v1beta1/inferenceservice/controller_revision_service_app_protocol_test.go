package inferenceservice

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// declaredAppProtocol is the value the fixture runtime declares. The pass
// copies whatever a runtime declares, so this value is only test data.
const declaredAppProtocol = "kubernetes.io/h2c"

// servingContainer is a pod spec of one container with the given ports.
func servingContainer(name string, ports ...v1.ContainerPort) v1beta1.PodSpec {
	return v1beta1.PodSpec{Containers: []v1.Container{{Name: name, Image: name + ":v1", Ports: ports}}}
}

func tcpPort(name string, number int32) v1.ContainerPort {
	return v1.ContainerPort{Name: name, ContainerPort: number, Protocol: v1.ProtocolTCP}
}

// appProtocolRuntime declares an appProtocol for `http` on its router and its
// engine. The router serves on a port named `http`. The engine opens one
// listener per serving process, named engine1 to engine4, so its declaration
// names no port it serves on. The decoder serves on `http` and declares nothing.
func appProtocolRuntime() *v1beta1.ServingRuntimeSpec {
	declared := v1beta1.ComponentExtensionSpec{ServicePortAppProtocols: map[string]string{"http": declaredAppProtocol}}
	return &v1beta1.ServingRuntimeSpec{
		EngineConfig: &v1beta1.EngineSpec{ComponentExtensionSpec: declared, PodSpec: servingContainer("engine",
			tcpPort("engine1", 8001), tcpPort("engine2", 8002), tcpPort("engine3", 8003), tcpPort("engine4", 8004))},
		DecoderConfig: &v1beta1.DecoderSpec{PodSpec: servingContainer("decoder", tcpPort("http", 8080))},
		RouterConfig: &v1beta1.RouterSpec{ComponentExtensionSpec: declared, PodSpec: servingContainer("router",
			tcpPort("http", 30080), tcpPort("metrics", 9090))},
	}
}

// appProtocolService declares an engine and a router and runs both on the
// OMENative engine. Its own router block declares no appProtocol, so the
// value can only come from the runtime through the merged spec.
func appProtocolService() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "service-a", Namespace: "team-a", UID: types.UID("service-a-uid")},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(constants.OMENative),
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "runtime-a"},
			Engine:         &v1beta1.EngineSpec{},
			Router:         &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2)}},
		},
	}
}

// withDecoder adds a decoder that declares nothing of its own.
func withDecoder(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

// withRouterCanary puts the router under a two-step canary group.
func withRouterCanary(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	return isvc
}

// revisionPod is a Running, Ready pod of one revision of a Component.
func revisionPod(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, hash, name string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: isvc.Namespace,
			Name:      name,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               hash,
				query.LabelRunner:                     string(v1beta1.RunnerNameDefault),
				query.LabelPodOrdinal:                 "0",
			},
		},
		Status: v1.PodStatus{
			Phase:      v1.PodRunning,
			Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
		},
	}
}

// runAppProtocolPass runs a pass over isvc, whose runtime was already looked
// up, with a client that holds isvc and objs and an operator config without
// coordination or canary tuning: the runtime step, the spec step and then
// last, the step that writes the revision Services. It returns the client
// those Services were written to.
func runAppProtocolPass(t *testing.T, isvc *v1beta1.InferenceService, last func(*servicePass, context.Context) (bool, error), objs ...client.Object) client.Client {
	t.Helper()
	g := gomega.NewWithT(t)
	c := fakeclient.New(t, fakeclient.WithObjects(append(objs, isvc)...))
	clientset := fake.NewClientset(&v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
	})
	pass := &servicePass{
		reconciler: &InferenceServiceReconciler{Client: c, APIReader: c, Clientset: clientset, Log: logr.Discard(), Recorder: record.NewFakeRecorder(64)},
		isvc:       isvc,
		log:        logr.Discard(),
		in:         render.Inputs{Service: isvc, ModelRead: true, OverlaysRead: true, Runtime: appProtocolRuntime(), RuntimeName: "runtime-a"},
	}
	next, err := runPassSteps(context.Background(), []passStep{
		pass.resolveRuntime,
		pass.prepareComponentSpecs,
		func(ctx context.Context) (bool, error) { return last(pass, ctx) },
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(next).To(gomega.BeTrue())
	return c
}

// routingPort reads the single port of a per-revision routing Service.
func routingPort(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, hash string) v1.ServicePort {
	t.Helper()
	svc := &v1.Service{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: coordination.PerRevisionServiceName(isvc.Name, component, hash)}
	if err := c.Get(context.Background(), key, svc); err != nil {
		t.Fatalf("routing Service %s: %v", key.Name, err)
	}
	if len(svc.Spec.Ports) != 1 {
		t.Fatalf("routing Service %s: got ports %+v, want one", key.Name, svc.Spec.Ports)
	}
	return svc.Spec.Ports[0]
}

// The pass reads the appProtocol a runtime declares from the merged spec and
// hands it to both writers of a revision's routing Service. Under the
// coordination step the router's revision Service speaks the declared
// protocol on `http`; the engine declares `http` too but serves on engine1,
// so its Service carries none; the decoder serves on `http` and declares
// nothing, so the router's declaration does not reach it. Under a router
// canary the dispatch writes the router's Services, and the stable and the
// canary revision both speak the declared protocol.
func TestPassRevisionServicesCarryTheRuntimeAppProtocol(t *testing.T) {
	type revision struct {
		component   v1beta1.ComponentType
		hash, port  string
		appProtocol *string
	}
	cases := []struct {
		name      string
		service   *v1beta1.InferenceService
		step      func(*servicePass, context.Context) (bool, error)
		revisions []revision
	}{
		{
			name:    "the coordination step gives each Component its own declaration",
			service: withDecoder(appProtocolService()),
			step:    (*servicePass).reconcileCoordination,
			revisions: []revision{
				{component: v1beta1.RouterComponent, hash: "rhash", port: "http", appProtocol: ptr.To(declaredAppProtocol)},
				{component: v1beta1.EngineComponent, hash: "ehash", port: "engine1"},
				{component: v1beta1.DecoderComponent, hash: "dhash", port: "http"},
			},
		},
		{
			name:    "a router canary's dispatch gives both router revisions the declaration",
			service: withRouterCanary(appProtocolService()),
			step:    (*servicePass).dispatchCanaryGroups,
			revisions: []revision{
				{component: v1beta1.RouterComponent, hash: "stable0", port: "http", appProtocol: ptr.To(declaredAppProtocol)},
				{component: v1beta1.RouterComponent, hash: "newhash", port: "http", appProtocol: ptr.To(declaredAppProtocol)},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			var pods []client.Object
			index := map[v1beta1.ComponentType]int{}
			for _, rev := range tc.revisions {
				name := fmt.Sprintf("%s-%s-%d", tc.service.Name, rev.component, index[rev.component])
				pods = append(pods, revisionPod(tc.service, rev.component, rev.hash, name))
				index[rev.component]++
			}
			c := runAppProtocolPass(t, tc.service, tc.step, pods...)
			for _, rev := range tc.revisions {
				port := routingPort(t, c, tc.service, rev.component, rev.hash)
				g.Expect(port.Name).To(gomega.Equal(rev.port), "%s revision %s", rev.component, rev.hash)
				g.Expect(port.AppProtocol).To(gomega.Equal(rev.appProtocol), "%s revision %s", rev.component, rev.hash)
			}
		})
	}
}

// readyEndpoints is the EndpointSlice the endpoints controller publishes for
// a revision's routing Service once the pod is a Ready endpoint of it.
func readyEndpoints(pod *v1.Pod, service string) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pod.Namespace,
			Name:      service + "-slice",
			Labels:    map[string]string{discoveryv1.LabelServiceName: service},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.10"},
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
			TargetRef:  &v1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name},
		}},
	}
}

// The gateway reads the protocol from the Service port a route's backendRef
// targets. Once the router revision serves, the coordination step points the
// router's traffic at its revision Service. The router route and the
// top-level route both name that Service as their backend, and that
// backend's port carries the protocol the runtime declared, so the gateway
// speaks it to the router.
func TestPassRouterRouteBackendSpeaksTheRuntimeAppProtocol(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := appProtocolService()
	pod := revisionPod(isvc, v1beta1.RouterComponent, "rhash", "service-a-router-0")
	revisionService := coordination.PerRevisionServiceName(isvc.Name, v1beta1.RouterComponent, "rhash")
	c := runAppProtocolPass(t, isvc, (*servicePass).reconcileCoordination, pod, readyEndpoints(pod, revisionService))
	traffic := isvc.Status.Components[v1beta1.RouterComponent].Traffic
	g.Expect(traffic).To(gomega.HaveLen(1), "premise: the serving revision takes the router's traffic")
	g.Expect(traffic[0].RevisionName).To(gomega.Equal(revisionService))
	port := routingPort(t, c, isvc, v1beta1.RouterComponent, "rhash")
	g.Expect(port.AppProtocol).To(gomega.HaveValue(gomega.Equal(declaredAppProtocol)))

	isvc.Status.SetCondition(v1beta1.RouterReady, &knapis.Condition{Type: v1beta1.RouterReady, Status: v1.ConditionTrue})
	builder := builders.NewHTTPRouteBuilder(c, &controllerconfig.IngressConfig{
		IngressDomain:     "example.com",
		DomainTemplate:    "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
		OmeIngressGateway: "gateway-ns/gateway",
		SharedHostPrefix:  "llm",
	}, &controllerconfig.InferenceServicesConfig{}, services.NewDomainService(), services.NewPathService())
	for _, route := range []string{builders.RouterComponent, builders.TopLevelComponent} {
		built, err := builder.BuildHTTPRoute(context.Background(), isvc, route)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "%s route", route)
		rules := built.(*gatewayapiv1.HTTPRoute).Spec.Rules
		g.Expect(rules).To(gomega.HaveLen(1), "%s route", route)
		g.Expect(rules[0].BackendRefs).To(gomega.HaveLen(1), "%s route", route)
		backend := rules[0].BackendRefs[0]
		g.Expect(string(backend.Name)).To(gomega.Equal(revisionService), "%s route", route)
		g.Expect(backend.Port).NotTo(gomega.BeNil(), "%s route", route)
		g.Expect(port.Port).To(gomega.Equal(int32(*backend.Port)), "the %s route targets the port that carries the protocol", route)
	}
}
