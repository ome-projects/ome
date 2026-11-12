package strategies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// targetServiceRaw is the Service the Kubernetes Ingress pass reads the URL's
// port from: the router's when the service declares a router, else the
// engine's, declaring the given port.
func targetServiceRaw(isvc *v1beta1.InferenceService, port int32) *corev1.Service {
	name := constants.EngineServiceName(isvc.Name)
	if isvc.Spec.Router != nil {
		name = constants.RouterServiceName(isvc.Name)
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: isvc.Namespace},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: port}}},
	}
}

// The test: status.url names the target Service's port, so the pass waits
// for the port instead of writing a compiled-in one. Pass 1: the Ingress is
// written but the Service is absent, so the pass ends with the error,
// IngressReady=False carries it and no URL is written. Pass 2: the Service
// exists but declares no port: the same. Pass 3: the Service declares port
// 8081: the URL and the address name it and IngressReady=True.
func TestKubernetesIngressStrategy_URLWaitsForTheServicePort(t *testing.T) {
	isvc := createTestInferenceServiceRaw("test-isvc", "default")
	setComponentStatusReadyRaw(isvc)
	cl := fakeclient.New(t, fakeclient.WithScheme(fakeclient.Scheme(t)), fakeclient.WithObjects(isvc))
	strategy := NewKubernetesIngressStrategy(interfaces.ReconcilerOptions{
		Client: cl,
		Scheme: cl.Scheme(),
		IngressConfig: &controllerconfig.IngressConfig{
			ClusterDomain:    "cluster.local",
			IngressDomain:    "example.com",
			IngressClassName: ptr.To("nginx"),
			DomainTemplate:   "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			UrlScheme:        "https",
		},
		IsvcConfig: &controllerconfig.InferenceServicesConfig{},
	}, services.NewDomainService(), services.NewPathService())
	ctx := context.Background()

	requireNoURLYet := func(t *testing.T, err error, want string) {
		t.Helper()
		require.ErrorContains(t, err, want, "the pass ends with the error so it is retried")
		require.Nil(t, isvc.Status.URL, "no URL until the port is read")
		require.Nil(t, isvc.Status.Address, "no address until the port is read")
		condition := isvc.Status.GetCondition(v1beta1.IngressReady)
		require.NotNil(t, condition)
		require.Equal(t, corev1.ConditionFalse, condition.Status)
		require.Equal(t, "TargetServicePortNotAvailable", condition.Reason)
		require.Contains(t, condition.Message, want)
	}

	requireNoURLYet(t, strategy.Reconcile(ctx, isvc), "not found")
	require.NoError(t, cl.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, &netv1.Ingress{}), "the Ingress is written while the URL waits")

	service := targetServiceRaw(isvc, 8081)
	service.Spec.Ports = nil
	require.NoError(t, cl.Create(ctx, service))
	requireNoURLYet(t, strategy.Reconcile(ctx, isvc), "declares no port")

	service.Spec.Ports = []corev1.ServicePort{{Port: 8081}}
	require.NoError(t, cl.Update(ctx, service))
	require.NoError(t, strategy.Reconcile(ctx, isvc))
	require.Equal(t, "test-isvc.default.example.com:8081", isvc.Status.URL.Host)
	require.Equal(t, "test-isvc-engine.default.svc.cluster.local:8081", isvc.Status.Address.URL.Host)
	require.Equal(t, corev1.ConditionTrue, isvc.Status.GetCondition(v1beta1.IngressReady).Status)
}
