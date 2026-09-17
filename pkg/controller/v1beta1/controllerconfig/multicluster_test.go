package controllerconfig

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestNewMultiClusterConfig(t *testing.T) {
	tests := []struct {
		name          string
		configMapData map[string]string
		expectedError bool
		validate      func(*testing.T, *MultiClusterConfig)
	}{
		{
			name:          "missing configmap",
			configMapData: nil,
			expectedError: true,
		},
		{
			name:          "absent multicluster block yields zero config",
			configMapData: map[string]string{},
			validate: func(t *testing.T, cfg *MultiClusterConfig) {
				// Every knob zero/empty: each consumer applies its own default.
				assert.False(t, cfg.WorkloadCluster.CacheEnabled)
				assert.Equal(t, time.Duration(0), cfg.WorkloadCluster.HealthIntervalDuration())
				assert.Equal(t, time.Duration(0), cfg.Placement.RequeueIntervalDuration())
				assert.Equal(t, 0, cfg.Placement.MaxConcurrentReconciles)
				assert.Equal(t, "", cfg.Placement.DispatcherMode)
				assert.Equal(t, "", cfg.Endpoint.GlobalGateway)
				assert.Equal(t, "", cfg.Routing.Publisher.Name)
			},
		},
		{
			name: "full block parses every field",
			configMapData: map[string]string{
				MultiClusterConfigName: `{
					"workloadCluster": {
						"clientQPS": 50,
						"clientBurst": 100,
						"perCallTimeout": "15s",
						"cacheEnabled": true,
						"healthInterval": "30s",
						"connectionGrace": "90s",
						"eventsBatchPeriod": "200ms",
						"establishInitial": "2m",
						"establishMax": "20m",
						"reconnectRetryMax": "5m",
						"funnelResyncInterval": "250ms",
						"funnelBufferSize": 64
					},
					"placement": {
						"requeueInterval": "45s",
						"gcInterval": "10m",
						"maxConcurrentReconciles": 8,
						"fanoutTimeout": "20s",
						"winnerLostGrace": "2m",
						"statusBatchPeriod": "500ms",
						"statusSafetyRequeue": "5m",
						"dispatcherMode": "Incremental",
						"dispatcherStepSize": 4,
						"dispatcherRoundTimeout": "30s"
					},
					"endpoint": {
						"globalHostTemplate": "{{.Name}}.{{.Namespace}}.global.example",
						"globalGateway": "ome/global-gateway",
						"routeNamespace": "ome-routes",
						"backendPort": 8080
					},
					"routing": {
						"publisher": {
							"name": "test-publisher",
							"options": {"key": "value"}
						}
					}
				}`,
			},
			validate: func(t *testing.T, cfg *MultiClusterConfig) {
				wc := cfg.WorkloadCluster
				assert.Equal(t, 50.0, wc.ClientQPS)
				assert.Equal(t, 100, wc.ClientBurst)
				assert.Equal(t, 15*time.Second, wc.PerCallTimeoutDuration())
				assert.True(t, wc.CacheEnabled)
				assert.Equal(t, 30*time.Second, wc.HealthIntervalDuration())
				assert.Equal(t, 90*time.Second, wc.ConnectionGraceDuration())
				assert.Equal(t, 200*time.Millisecond, wc.EventsBatchPeriodDuration())
				assert.Equal(t, 2*time.Minute, wc.EstablishInitialDuration())
				assert.Equal(t, 20*time.Minute, wc.EstablishMaxDuration())
				assert.Equal(t, 5*time.Minute, wc.ReconnectRetryMaxDuration())
				assert.Equal(t, 250*time.Millisecond, wc.FunnelResyncIntervalDuration())
				assert.Equal(t, 64, wc.FunnelBufferSize)

				pl := cfg.Placement
				assert.Equal(t, 45*time.Second, pl.RequeueIntervalDuration())
				assert.Equal(t, 10*time.Minute, pl.GCIntervalDuration())
				assert.Equal(t, 8, pl.MaxConcurrentReconciles)
				assert.Equal(t, 20*time.Second, pl.FanoutTimeoutDuration())
				assert.Equal(t, 2*time.Minute, pl.WinnerLostGraceDuration())
				assert.Equal(t, 500*time.Millisecond, pl.StatusBatchPeriodDuration())
				assert.Equal(t, 5*time.Minute, pl.StatusSafetyRequeueDuration())
				assert.Equal(t, "Incremental", pl.DispatcherMode)
				assert.Equal(t, 4, pl.DispatcherStepSize)
				assert.Equal(t, 30*time.Second, pl.DispatcherRoundTimeoutDuration())

				ep := cfg.Endpoint
				assert.Equal(t, "{{.Name}}.{{.Namespace}}.global.example", ep.GlobalHostTemplate)
				assert.Equal(t, "ome/global-gateway", ep.GlobalGateway)
				assert.Equal(t, "ome-routes", ep.RouteNamespace)
				assert.Equal(t, 8080, ep.BackendPort)

				assert.Equal(t, "test-publisher", cfg.Routing.Publisher.Name)
				assert.Equal(t, map[string]string{"key": "value"}, cfg.Routing.Publisher.Options)
			},
		},
		{
			name: "empty and malformed durations resolve to zero",
			configMapData: map[string]string{
				MultiClusterConfigName: `{
					"workloadCluster": {"healthInterval": "", "connectionGrace": "not-a-duration"},
					"placement": {"requeueInterval": "0s"}
				}`,
			},
			validate: func(t *testing.T, cfg *MultiClusterConfig) {
				assert.Equal(t, time.Duration(0), cfg.WorkloadCluster.HealthIntervalDuration())
				assert.Equal(t, time.Duration(0), cfg.WorkloadCluster.ConnectionGraceDuration())
				// non-positive parses but is treated as "use the package default".
				assert.Equal(t, time.Duration(0), cfg.Placement.RequeueIntervalDuration())
			},
		},
		{
			name: "invalid json errors",
			configMapData: map[string]string{
				MultiClusterConfigName: `{"workloadCluster": {`,
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			if tt.configMapData != nil {
				_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(context.TODO(), &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      constants.InferenceServiceConfigMapName,
						Namespace: constants.OMENamespace,
					},
					Data: tt.configMapData,
				}, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			cfg, err := NewMultiClusterConfig(clientset)
			if tt.expectedError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cfg)
			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

// A stated-but-unparsable duration must fail at startup rather than read as
// "not set": the forgiving accessors would discard the operator's intended
// value for the life of the process, with the built-in default silently
// standing in.
func TestMultiClusterConfig_ValidateRejectsMalformedKnobs(t *testing.T) {
	cases := []struct {
		name string
		cfg  MultiClusterConfig
		want string
	}{
		{
			name: "duration missing a unit",
			cfg:  MultiClusterConfig{WorkloadCluster: WorkloadClusterConfig{HealthInterval: "30"}},
			want: "workloadCluster.healthInterval",
		},
		{
			name: "non-positive duration",
			cfg:  MultiClusterConfig{Placement: PlacementConfig{GCInterval: "0s"}},
			want: "placement.gcInterval",
		},
		{
			name: "gateway configured without a backend port",
			cfg:  MultiClusterConfig{Endpoint: EndpointConfig{GlobalGateway: "infra/global-gw"}},
			want: "endpoint.backendPort",
		},
		{
			name: "port out of range",
			cfg:  MultiClusterConfig{Endpoint: EndpointConfig{BackendPort: 99999}},
			want: "endpoint.backendPort",
		},
		{
			name: "gateway backend missing refresh interval",
			cfg: MultiClusterConfig{Endpoint: EndpointConfig{
				GlobalGateway: "infra/global-gw", BackendPort: 443,
				GatewayBackend: EndpointGatewayBackendConfig{
					EndpointSlices: EndpointGatewayBackendEndpointSliceConfig{Enabled: true},
				},
			}},
			want: "endpoint.gatewayBackend.endpointSlices.addressRefreshInterval",
		},
		{
			name: "gateway backend missing trust roots",
			cfg: MultiClusterConfig{Endpoint: EndpointConfig{
				GlobalGateway: "infra/global-gw", BackendPort: 443,
				GatewayBackend: EndpointGatewayBackendConfig{
					TLS: EndpointGatewayBackendTLSConfig{Enabled: true},
				},
			}},
			want: "endpoint.gatewayBackend.tls.wellKnownCACertificates",
		},
		{
			name: "endpoint slice settings ignored while disabled",
			cfg: MultiClusterConfig{Endpoint: EndpointConfig{
				GatewayBackend: EndpointGatewayBackendConfig{
					EndpointSlices: EndpointGatewayBackendEndpointSliceConfig{AddressRefreshInterval: "1m"},
				},
			}},
			want: "endpoint.gatewayBackend.endpointSlices.enabled",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// An absent block and every well-formed knob must pass, so validation never
// blocks a legitimate config (or the graceful-degradation path).
func TestMultiClusterConfig_ValidateAcceptsEmptyAndWellFormed(t *testing.T) {
	require.NoError(t, MultiClusterConfig{}.Validate())
	// Endpoint publishing fully off (no gateway, no port) stays valid: that is
	// the graceful-degradation default, not a half-finished config.
	require.NoError(t, MultiClusterConfig{Endpoint: EndpointConfig{}}.Validate())
	require.NoError(t, MultiClusterConfig{
		WorkloadCluster: WorkloadClusterConfig{HealthInterval: "30s", EstablishMax: "10m"},
		Placement:       PlacementConfig{GCInterval: "5m", LocalQueue: "gpu-queue"},
		Endpoint: EndpointConfig{
			GlobalGateway: "infra/global-gw",
			BackendPort:   443,
			GatewayBackend: EndpointGatewayBackendConfig{
				RewriteHostname: true,
				TLS: EndpointGatewayBackendTLSConfig{
					Enabled:                 true,
					WellKnownCACertificates: "System",
				},
				EndpointSlices: EndpointGatewayBackendEndpointSliceConfig{
					Enabled:                true,
					AddressRefreshInterval: "1m",
				},
			},
		},
	}.Validate())
	for _, gatewayBackend := range []EndpointGatewayBackendConfig{
		{RewriteHostname: true},
		{TLS: EndpointGatewayBackendTLSConfig{Enabled: true, WellKnownCACertificates: "System"}},
		{EndpointSlices: EndpointGatewayBackendEndpointSliceConfig{Enabled: true, AddressRefreshInterval: "1m"}},
	} {
		require.NoError(t, MultiClusterConfig{Endpoint: EndpointConfig{
			GlobalGateway:  "infra/global-gw",
			BackendPort:    443,
			GatewayBackend: gatewayBackend,
		}}.Validate(), "gateway backend features can be enabled independently")
	}
}

// Loading stays forgiving by contract: a malformed knob must NOT fail the load
// (the accessor reads it as zero). Validate is the separate, explicit gate the
// composition root applies.
func TestNewMultiClusterConfig_LoadStaysForgiving_ValidateGates(t *testing.T) {
	cs := fake.NewSimpleClientset(&v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data:       map[string]string{MultiClusterConfigName: `{"workloadCluster":{"healthInterval":"1 m"}}`},
	})
	cfg, err := NewMultiClusterConfig(cs)
	require.NoError(t, err, "load must not reject knob values")
	assert.Equal(t, time.Duration(0), cfg.WorkloadCluster.HealthIntervalDuration())

	err = cfg.Validate()
	require.Error(t, err, "validation must catch what loading tolerates")
	assert.Contains(t, err.Error(), "workloadCluster.healthInterval")
}
