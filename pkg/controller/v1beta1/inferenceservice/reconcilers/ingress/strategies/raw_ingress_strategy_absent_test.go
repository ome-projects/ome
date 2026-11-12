package strategies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	netv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The test: a pass over a service with no Ingress creates one in the same
// pass that writes status.url, so the URL never names an Ingress that does
// not exist.
func TestKubernetesIngressStrategy_Reconcile_CreatesTheIngressWhenAbsent(t *testing.T) {
	isvc := createTestInferenceServiceRaw("test-isvc", "default")
	setComponentStatusReadyRaw(isvc)
	cl := fakeclient.New(t, fakeclient.WithScheme(fakeclient.Scheme(t)), fakeclient.WithObjects(isvc, targetServiceRaw(isvc, 8080)))
	strategy := NewKubernetesIngressStrategy(interfaces.ReconcilerOptions{
		Client: cl,
		Scheme: cl.Scheme(),
		IngressConfig: &controllerconfig.IngressConfig{
			IngressDomain:    "example.com",
			IngressClassName: ptr.To("nginx"),
			DomainTemplate:   "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			UrlScheme:        "https",
		},
		IsvcConfig: &controllerconfig.InferenceServicesConfig{},
	}, services.NewDomainService(), services.NewPathService())

	require.NoError(t, strategy.Reconcile(context.Background(), isvc))
	require.NotNil(t, isvc.Status.URL)
	ingress := &netv1.Ingress{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, ingress), "the pass that writes status.url also writes the Ingress")
	require.NotEmpty(t, ingress.Spec.Rules)
}
