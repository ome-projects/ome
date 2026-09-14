package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
	placementendpoint "sigs.k8s.io/ome/pkg/controller/v1beta1/placement/endpoint"
	placementrouting "sigs.k8s.io/ome/pkg/controller/v1beta1/placement/routing"
	workloadcluster "sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

// mcWiringFromJSON exercises the production config path: it seeds the
// inferenceservice-config ConfigMap with the given "multicluster" block, loads
// it via controllerconfig.NewMultiClusterConfig (as cmd/manager does), and
// resolves the wiring. An empty mcJSON omits the block entirely.
func mcWiringFromJSON(t *testing.T, mcJSON string) mcWiring {
	t.Helper()
	data := map[string]string{}
	if mcJSON != "" {
		data[controllerconfig.MultiClusterConfigName] = mcJSON
	}
	clientset := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: data,
	})
	mc, err := controllerconfig.NewMultiClusterConfig(clientset)
	require.NoError(t, err)
	return resolveMCWiring(mc)
}

// TestResolveMCWiringFullConfig asserts every ConfigMap field lands on the right
// wiring field — the mapping that would otherwise only exist (untested) in the
// cmd/manager wiring block, where a cross-wire would silently mis-tune the
// control plane.
func TestResolveMCWiringFullConfig(t *testing.T) {
	w := mcWiringFromJSON(t, `{
		"workloadCluster": {
			"clientQPS": 50, "clientBurst": 100, "perCallTimeout": "15s", "cacheEnabled": true,
			"healthInterval": "30s", "connectionGrace": "90s", "eventsBatchPeriod": "200ms",
			"establishInitial": "2m", "establishMax": "20m", "reconnectRetryMax": "5m",
			"funnelResyncInterval": "250ms", "funnelBufferSize": 64
		},
		"placement": {
			"requeueInterval": "45s", "gcInterval": "10m", "maxConcurrentReconciles": 8,
			"fanoutTimeout": "20s", "winnerLostGrace": "2m", "statusBatchPeriod": "500ms",
			"statusSafetyRequeue": "7m", "dispatcherMode": "Incremental", "dispatcherStepSize": 4,
			"dispatcherRoundTimeout": "30s"
		},
		"endpoint": {
			"globalHostTemplate": "{{.Name}}.global", "globalGateway": "ome/gw",
			"routeNamespace": "ome-routes", "backendPort": 443,
			"gatewayBackend": {
				"rewriteHostname": true,
				"tls": {"enabled": true, "wellKnownCACertificates": "System"},
				"endpointSlices": {"enabled": true, "addressRefreshInterval": "1m"}
			}
		},
		"routing": { "enabled": true }
	}`)

	// WorkloadCluster transport.
	assert.Equal(t, float32(50), w.clientTuning.QPS)
	assert.Equal(t, 100, w.clientTuning.Burst)
	assert.Equal(t, 15*time.Second, w.clientTuning.PerCallTimeout)
	assert.True(t, w.cacheEnabled)
	assert.Equal(t, 30*time.Second, w.healthInterval)
	assert.Equal(t, 90*time.Second, w.connectionGrace)
	assert.Equal(t, 200*time.Millisecond, w.eventsBatchPeriod)
	assert.Equal(t, 2*time.Minute, w.reconnectBackoff.EstablishInitial)
	assert.Equal(t, 20*time.Minute, w.reconnectBackoff.EstablishMax)
	assert.Equal(t, 5*time.Minute, w.reconnectBackoff.RetryMax)
	assert.Equal(t, 250*time.Millisecond, w.funnelResyncInterval)
	assert.Equal(t, 64, w.funnelBufferSize)

	// Placement.
	assert.Equal(t, 45*time.Second, w.requeue)
	assert.Equal(t, 10*time.Minute, w.gcInterval)
	assert.Equal(t, 8, w.maxConcurrent)
	assert.Equal(t, 20*time.Second, w.placeTimeout)
	assert.Equal(t, 2*time.Minute, w.winnerLostGrace)
	assert.Equal(t, 500*time.Millisecond, w.statusBatchPeriod)
	assert.Equal(t, 7*time.Minute, w.statusSafetyRequeue) // cache on => configured backstop
	assert.Equal(t, placement.DispatcherMode("Incremental"), w.dispatcherMode)
	assert.Equal(t, 4, w.dispatcherStepSize)
	assert.Equal(t, 30*time.Second, w.dispatcherRoundTimeout)

	// Endpoint.
	assert.Equal(t, "{{.Name}}.global", w.endpoint.GlobalHostTemplate)
	assert.Equal(t, "ome/gw", w.endpoint.GlobalGateway)
	assert.Equal(t, "ome-routes", w.endpoint.RouteNamespace)
	assert.Equal(t, int32(443), w.endpoint.BackendPort)
	assert.True(t, w.endpoint.GatewayBackend.RewriteHostname)
	assert.True(t, w.endpoint.GatewayBackend.TLS.Enabled)
	assert.Equal(t, "System", w.endpoint.GatewayBackend.TLS.WellKnownCACertificates)
	assert.True(t, w.endpoint.GatewayBackend.EndpointSlices.Enabled)
	assert.Equal(t, time.Minute, w.endpoint.GatewayBackend.EndpointSlices.AddressRefreshInterval)

	// Routing.
	assert.True(t, w.routing.Enabled)
}

// TestResolveMCWiringSafetyRequeueDependsOnCache pins the one non-trivial bit of
// logic in the mapping: the status-convergence backstop is the configured
// statusSafetyRequeue when the cache/funnel is on (events drive freshness), but
// folds to the poll cadence (requeueInterval) when it's off (no event source).
func TestResolveMCWiringSafetyRequeueDependsOnCache(t *testing.T) {
	off := mcWiringFromJSON(t, `{"placement": {"requeueInterval": "30s", "statusSafetyRequeue": "10m"}}`)
	assert.False(t, off.cacheEnabled)
	assert.Equal(t, 30*time.Second, off.statusSafetyRequeue, "cache off => backstop is the poll cadence")

	on := mcWiringFromJSON(t, `{
		"workloadCluster": {"cacheEnabled": true},
		"placement": {"requeueInterval": "30s", "statusSafetyRequeue": "10m"}
	}`)
	assert.True(t, on.cacheEnabled)
	assert.Equal(t, 10*time.Minute, on.statusSafetyRequeue, "cache on => backstop is the configured statusSafetyRequeue")
}

// TestResolveMCWiringAbsentBlockIsZero confirms an omitted "multicluster" block
// yields an all-zero wiring, so every consuming option applies its own
// in-package default (no magic literals in cmd/manager).
func TestResolveMCWiringAbsentBlockIsZero(t *testing.T) {
	w := mcWiringFromJSON(t, "")

	assert.Equal(t, float32(0), w.clientTuning.QPS)
	assert.Equal(t, 0, w.clientTuning.Burst)
	assert.Equal(t, time.Duration(0), w.clientTuning.PerCallTimeout)
	assert.False(t, w.cacheEnabled)
	assert.Equal(t, time.Duration(0), w.healthInterval)
	assert.Equal(t, time.Duration(0), w.connectionGrace)
	assert.Equal(t, workloadcluster.ReconnectBackoffConfig{}, w.reconnectBackoff)
	assert.Equal(t, time.Duration(0), w.requeue)
	assert.Equal(t, time.Duration(0), w.gcInterval)
	assert.Equal(t, time.Duration(0), w.statusSafetyRequeue)
	assert.Equal(t, 0, w.maxConcurrent)
	assert.Equal(t, 0, w.funnelBufferSize)
	assert.Equal(t, placement.DispatcherMode(""), w.dispatcherMode)
	assert.Equal(t, placementendpoint.Config{}, w.endpoint)
	assert.Equal(t, placementrouting.Config{}, w.routing)
}

// TestResolveMCWiringMissingConfigMapErrors confirms the loader (and thus
// cmd/manager startup) fails fast when the ConfigMap is absent, rather than
// silently running with an empty config.
func TestResolveMCWiringMissingConfigMapErrors(t *testing.T) {
	_, err := controllerconfig.NewMultiClusterConfig(fake.NewSimpleClientset())
	assert.Error(t, err)
}

// The operator-configured LocalQueue must reach the placement reconciler; if it
// stops being mapped, deriveds carry no queue label and Kueue never admits them
// on a fleet that gates admission on a LocalQueue.
func TestResolveMCWiringCarriesLocalQueue(t *testing.T) {
	w := mcWiringFromJSON(t, `{"placement":{"localQueue":"gpu-queue"}}`)
	assert.Equal(t, "gpu-queue", w.localQueue)

	assert.Empty(t, mcWiringFromJSON(t, `{"placement":{}}`).localQueue,
		"absent knob stays empty so the per-ISVC annotation is the only queue source")
}

// The dispatcher falls back to AllAtOnce for anything it does not recognize, so
// a mis-cased mode would silently select a different fan-out breadth than the
// operator asked for. Startup must reject it instead.
func TestValidateDispatcherMode(t *testing.T) {
	for _, ok := range []placement.DispatcherMode{"", placement.DispatcherModeAllAtOnce, placement.DispatcherModeIncremental} {
		assert.NoError(t, validateDispatcherMode(ok), "mode %q must be accepted", ok)
	}
	for _, bad := range []placement.DispatcherMode{"incremental", "allatonce", "Bogus"} {
		err := validateDispatcherMode(bad)
		require.Error(t, err, "mode %q must be rejected", bad)
		assert.Contains(t, err.Error(), "dispatcherMode")
	}
}
