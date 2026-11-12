package strategies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The test: the service controls every HTTPRoute a pass creates, so the
// routes go when the service goes and the HTTPRoute watch re-enqueues the
// service when the gateway programs them.
func TestGatewayAPIStrategy_Reconcile_RoutesAreControlledByTheService(t *testing.T) {
	isvc := createTestInferenceServiceGateway("test-isvc", "default")
	isvc.UID = types.UID("test-isvc-uid")
	setComponentStatusReadyGateway(isvc)
	cl := fakeclient.New(t, fakeclient.WithScheme(fakeclient.Scheme(t, gatewayapiv1.Install)), fakeclient.WithObjects(isvc))
	strategy := createGatewayAPIStrategyWithClient(t, cl)

	require.NoError(t, strategy.Reconcile(context.Background(), isvc))
	routes := &gatewayapiv1.HTTPRouteList{}
	require.NoError(t, cl.List(context.Background(), routes, client.InNamespace(isvc.Namespace)))
	require.Len(t, routes.Items, 2, "the engine route and the top-level route")
	for i := range routes.Items {
		require.True(t, metav1.IsControlledBy(&routes.Items[i], isvc), "HTTPRoute %s is controlled by the service", routes.Items[i].Name)
	}
}
