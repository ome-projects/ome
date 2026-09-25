package endpoint_test

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/endpoint"
)

// TestEndpointPublisher_NoControllerNameCollision guards the control-plane crash
// where the placement controller and the endpoint publisher — both
// For(&InferenceService{}) — auto-derived the same name "inferenceservice" and
// controller-runtime rejected the duplicate. Register a stand-in under the
// placement name plus the real publisher on one manager; assert the names differ
// and there's no collision. A non-connecting rest.Config suffices — setup only
// registers controllers; nothing dials the apiserver until mgr.Start.
func TestEndpointPublisher_NoControllerNameCollision(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(&rest.Config{Host: "http://127.0.0.1:1"}, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"}, // disable metrics server
	})
	require.NoError(t, err)

	// Stand-in for the placement controller (its real name, also For(ISVC)).
	noop := reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
		return reconcile.Result{}, nil
	})
	require.NoError(t,
		ctrl.NewControllerManagedBy(mgr).
			Named(placement.PlacementControllerName).
			For(&v1beta1.InferenceService{}).
			Complete(noop),
		"stand-in placement controller")

	// Names must differ (the fix).
	require.NotEqual(t, placement.PlacementControllerName, endpoint.PlacementEndpointControllerName)

	// The endpoint publisher must register under a DISTINCT name.
	require.NoError(t,
		(&endpoint.Reconciler{
			Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Log: logr.Discard(),
		}).SetupWithManager(mgr),
		"endpoint publisher must not collide with the 'inferenceservice' controller name")
}
