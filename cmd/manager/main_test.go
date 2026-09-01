package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	volcano "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

func TestGetOptions(t *testing.T) {
	// Save original command line arguments and restore them after the test
	oldArgs := os.Args
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	}()

	tests := []struct {
		name     string
		args     []string
		expected Options
	}{
		{
			name: "default options",
			args: []string{"cmd"},
			expected: Options{
				metricsAddr:             ":8080",
				webhookPort:             9443,
				enableLeaderElection:    false,
				enableWebhook:           false,
				probeAddr:               ":8081",
				leaderElectionNamespace: LeaderElectionNamespace,
				zapOpts:                 zap.Options{},
			},
		},
		{
			name: "custom options",
			args: []string{
				"cmd",
				"--metrics-bind-address=:9090",
				"--webhook-port=8443",
				"--leader-elect=true",
				"--webhook=true",
				"--health-probe-addr=:9091",
				"--leader-election-namespace=custom-namespace",
			},
			expected: Options{
				metricsAddr:             ":9090",
				webhookPort:             8443,
				enableLeaderElection:    true,
				enableWebhook:           true,
				probeAddr:               ":9091",
				leaderElectionNamespace: "custom-namespace",
				zapOpts:                 zap.Options{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset flags before each test
			flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
			os.Args = tt.args

			options := GetOptions()
			assert.Equal(t, tt.expected.metricsAddr, options.metricsAddr)
			assert.Equal(t, tt.expected.webhookPort, options.webhookPort)
			assert.Equal(t, tt.expected.enableLeaderElection, options.enableLeaderElection)
			assert.Equal(t, tt.expected.enableWebhook, options.enableWebhook)
			assert.Equal(t, tt.expected.probeAddr, options.probeAddr)
			assert.Equal(t, tt.expected.leaderElectionNamespace, options.leaderElectionNamespace)
		})
	}
}

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	assert.Equal(t, ":8080", opts.metricsAddr)
	assert.Equal(t, 9443, opts.webhookPort)
	assert.False(t, opts.enableLeaderElection)
	assert.False(t, opts.enableWebhook)
	assert.Equal(t, ":8081", opts.probeAddr)
	assert.Equal(t, LeaderElectionNamespace, opts.leaderElectionNamespace)
	// Multi-cluster topology/identity/security flag defaults (the tunables moved
	// to the ConfigMap; these five stay flags).
	assert.False(t, opts.enableMultiCluster)
	assert.Equal(t, "", opts.multiClusterRole)
	assert.False(t, opts.allowExecCredentials)
	assert.Equal(t, "aws,gke-gcloud-auth-plugin,kubelogin", opts.execCredentialAllowedCmds)
	assert.Equal(t, "", opts.placementControlPlaneID)
	// No in-code default: the chart is the source of truth, and zero leaves
	// controller-runtime's own default in place.
	assert.Zero(t, opts.kubeAPIQPS)
	assert.Zero(t, opts.kubeAPIBurst)
}

func TestGetOptionsKubeAPIRateLimits(t *testing.T) {
	oldArgs := os.Args
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	}()

	flag.CommandLine = flag.NewFlagSet("cmd", flag.ExitOnError)
	os.Args = []string{"cmd", "--kube-api-qps=50", "--kube-api-burst=100"}

	o := GetOptions()
	assert.Equal(t, 50.0, o.kubeAPIQPS)
	assert.Equal(t, 100, o.kubeAPIBurst)
}

func TestApplyKubeAPIRateLimits(t *testing.T) {
	cfg := &rest.Config{}
	applyKubeAPIRateLimits(cfg, 50, 100)
	assert.EqualValues(t, 50, cfg.QPS)
	assert.Equal(t, 100, cfg.Burst)

	// Zero must not clobber a value already on the config — that would silently
	// drop the caller below controller-runtime's default.
	unset := &rest.Config{QPS: 20, Burst: 30}
	applyKubeAPIRateLimits(unset, 0, 0)
	assert.EqualValues(t, 20, unset.QPS)
	assert.Equal(t, 30, unset.Burst)

	// The two limits are independent.
	qpsOnly := &rest.Config{QPS: 20, Burst: 30}
	applyKubeAPIRateLimits(qpsOnly, 50, 0)
	assert.EqualValues(t, 50, qpsOnly.QPS)
	assert.Equal(t, 30, qpsOnly.Burst)
}

// TestGetOptionsMultiCluster asserts the five retained multi-cluster flags parse
// and bind onto Options (the tunables are ConfigMap-driven and tested separately).
func TestGetOptionsMultiCluster(t *testing.T) {
	oldArgs := os.Args
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	}()

	flag.CommandLine = flag.NewFlagSet("cmd", flag.ExitOnError)
	os.Args = []string{
		"cmd",
		"--enable-multicluster",
		"--multicluster-role=control-plane",
		"--allow-exec-credentials",
		"--exec-credential-allowed-commands=aws,foo",
		"--placement-control-plane-id=cp-1",
	}

	o := GetOptions()
	assert.True(t, o.enableMultiCluster)
	assert.Equal(t, "control-plane", o.multiClusterRole)
	assert.True(t, o.allowExecCredentials)
	assert.Equal(t, "aws,foo", o.execCredentialAllowedCmds)
	assert.Equal(t, "cp-1", o.placementControlPlaneID)
}

// Mock for testing CRD availability
type mockCRDChecker struct {
	available bool
	err       error
}

func (m *mockCRDChecker) IsCrdAvailable(config *rest.Config, groupVersion, kind string) (bool, error) {
	return m.available, m.err
}

func TestSetupLogger(t *testing.T) {
	options := DefaultOptions()
	logger := zap.New(zap.UseFlagOptions(&options.zapOpts))
	ctrl.SetLogger(logger)
	assert.NotNil(t, ctrl.Log)
}

func TestLeaderElectionConfiguration(t *testing.T) {
	tests := []struct {
		name                  string
		enableLeaderElection  bool
		leaderElectionNS      string
		expectedLockName      string
		expectedLockNamespace string
	}{
		{
			name:                  "leader election disabled",
			enableLeaderElection:  false,
			leaderElectionNS:      LeaderElectionNamespace,
			expectedLockName:      LeaderLockName,
			expectedLockNamespace: LeaderElectionNamespace,
		},
		{
			name:                  "leader election enabled custom namespace",
			enableLeaderElection:  true,
			leaderElectionNS:      "custom-namespace",
			expectedLockName:      LeaderLockName,
			expectedLockNamespace: "custom-namespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := Options{
				enableLeaderElection:    tt.enableLeaderElection,
				leaderElectionNamespace: tt.leaderElectionNS,
			}
			assert.Equal(t, tt.expectedLockName, LeaderLockName)
			assert.Equal(t, tt.expectedLockNamespace, opts.leaderElectionNamespace)
		})
	}
}

func TestInferenceReplicaCapabilityRegistration(t *testing.T) {
	tests := []struct {
		name                  string
		enableController      bool
		isControlPlane        bool
		enableLeaderElection  bool
		wantCapabilityPublish bool
	}{
		{name: "controller disabled", isControlPlane: false, enableLeaderElection: true},
		{name: "control plane", enableController: true, isControlPlane: true, enableLeaderElection: true},
		{name: "leader election disabled", enableController: true, isControlPlane: false},
		{name: "local elected manager", enableController: true, isControlPlane: false, enableLeaderElection: true, wantCapabilityPublish: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := shouldRegisterInferenceReplicaCapability(test.enableController, test.isControlPlane, test.enableLeaderElection)
			assert.Equal(t, test.wantCapabilityPublish, got)
		})
	}
}

func TestPodIdentity(t *testing.T) {
	t.Run("downward API name", func(t *testing.T) {
		t.Setenv("POD_NAME", "ome-controller-manager-7")
		got, err := podIdentity()
		require.NoError(t, err)
		assert.Equal(t, "ome-controller-manager-7", got)
	})

	t.Run("hostname fallback", func(t *testing.T) {
		t.Setenv("POD_NAME", "")
		want, err := os.Hostname()
		require.NoError(t, err)
		got, err := podIdentity()
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})
}

func TestHealthProbeConfiguration(t *testing.T) {
	opts := DefaultOptions()
	assert.Equal(t, ":8081", opts.probeAddr, "Default health probe address should be :8081")

	customOpts := Options{
		probeAddr: ":9091",
	}
	assert.Equal(t, ":9091", customOpts.probeAddr, "Custom health probe address should be set correctly")
}

func TestManagerProbeChecker(t *testing.T) {
	t.Run("webhook disabled", func(t *testing.T) {
		checker := managerProbeChecker(false, func() webhook.Server {
			t.Fatal("webhook server getter was called")
			return nil
		})
		require.NoError(t, checker(nil))
	})

	t.Run("webhook enabled", func(t *testing.T) {
		want := errors.New("webhook probe")
		calls := 0
		checker := managerProbeChecker(true, func() webhook.Server {
			calls++
			return webhookServerWithChecker{checker: func(*http.Request) error { return want }}
		})
		require.Equal(t, 1, calls)
		require.ErrorIs(t, checker(nil), want)
	})
}

type webhookServerWithChecker struct {
	webhook.Server
	checker healthz.Checker
}

func (s webhookServerWithChecker) StartedChecker() healthz.Checker {
	return s.checker
}

func TestWebhookConfiguration(t *testing.T) {
	opts := DefaultOptions()
	assert.Equal(t, 9443, opts.webhookPort, "Default webhook port should be 9443")
	assert.False(t, opts.enableWebhook, "Webhook should be disabled by default")

	customOpts := Options{
		webhookPort:   8443,
		enableWebhook: true,
	}
	assert.Equal(t, 8443, customOpts.webhookPort, "Custom webhook port should be set correctly")
	assert.True(t, customOpts.enableWebhook, "Webhook should be enabled")
}

func TestMetricsConfiguration(t *testing.T) {
	opts := DefaultOptions()
	assert.Equal(t, ":8080", opts.metricsAddr, "Default metrics address should be :8080")

	customOpts := Options{
		metricsAddr: ":9090",
	}
	assert.Equal(t, ":9090", customOpts.metricsAddr, "Custom metrics address should be set correctly")
}

// TestManagerSetup tests the manager configuration
func TestManagerSetup(t *testing.T) {
	tests := []struct {
		name          string
		opts          Options
		expectedError bool
		setupMockFunc func()
		cleanupFunc   func()
	}{
		{
			name: "valid configuration",
			opts: Options{
				metricsAddr:             "127.0.0.1:0",
				probeAddr:               "127.0.0.1:0",
				webhookPort:             18443,
				leaderElectionNamespace: LeaderElectionNamespace,
			},
			expectedError: false,
		},
		{
			name: "custom metrics port",
			opts: Options{
				metricsAddr:             "127.0.0.1:0",
				probeAddr:               "127.0.0.1:0",
				webhookPort:             19443,
				leaderElectionNamespace: LeaderElectionNamespace,
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupMockFunc != nil {
				tt.setupMockFunc()
			}
			if tt.cleanupFunc != nil {
				defer tt.cleanupFunc()
			}

			cfg := &rest.Config{
				Host: "http://localhost:8080",
			}

			mgr, err := manager.New(cfg, manager.Options{
				Metrics: metricsserver.Options{
					BindAddress: tt.opts.metricsAddr},
				WebhookServer: webhook.NewServer(webhook.Options{
					Port: tt.opts.webhookPort}),
				LeaderElection:          tt.opts.enableLeaderElection,
				LeaderElectionID:        LeaderLockName,
				LeaderElectionNamespace: tt.opts.leaderElectionNamespace,
				HealthProbeBindAddress:  tt.opts.probeAddr,
			})

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, mgr)
			}
		})
	}
}

// createMockConfigMap creates a mock ConfigMap for testing
func createMockConfigMap() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inferenceservice-config",
			Namespace: "ome",
		},
		Data: map[string]string{
			"deploy": `{
				"defaultDeploymentMode": "RawDeployment"
			}`,
			"ingress": `{
				"ingressGateway": "test-gateway",
				"ingressService": "test-service"
			}`,
		},
	}
}

func TestLoadPodBatchSizes(t *testing.T) {
	tests := []struct {
		name          string
		configMapData map[string]string
		omitConfigMap bool
		wantScaleUp   int32
		wantScaleDown int32
		wantInterval  time.Duration
		wantError     string
	}{
		{
			name: "positive configured values",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleUpPodBatchSize":37,"scaleDownPodBatchSize":41,"scaleDownRequeueInterval":"7s"}`,
			},
			wantScaleUp:   37,
			wantScaleDown: 41,
			wantInterval:  7 * time.Second,
		},
		{
			name: "scale-up field does not configure scale-down",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleUpPodBatchSize":37}`,
			},
			wantScaleUp: 37,
		},
		{
			name: "scale-down field does not configure scale-up",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownPodBatchSize":41}`,
			},
			wantScaleDown: 41,
		},
		{
			name: "requeue interval does not configure either batch size",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownRequeueInterval":"11s"}`,
			},
			wantInterval: 11 * time.Second,
		},
		{
			name:          "absent lifecycle key preserves compatibility behavior",
			configMapData: map[string]string{},
		},
		{
			name: "absent field preserves compatibility behavior",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{}`,
			},
		},
		{
			name:          "missing ConfigMap is rejected",
			omitConfigMap: true,
			wantError:     `configmaps "inferenceservice-config" not found`,
		},
		{
			name: "malformed lifecycle JSON is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{not-json`,
			},
			wantError: "unable to parse lifecycle config json",
		},
		{
			name: "zero scale-up is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleUpPodBatchSize":0}`,
			},
			wantError: "must be > 0, got 0",
		},
		{
			name: "negative scale-up is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleUpPodBatchSize":-1}`,
			},
			wantError: "must be > 0, got -1",
		},
		{
			name: "zero scale-down is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownPodBatchSize":0}`,
			},
			wantError: "must be > 0, got 0",
		},
		{
			name: "negative scale-down is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownPodBatchSize":-1}`,
			},
			wantError: "must be > 0, got -1",
		},
		{
			name: "malformed scale-down type is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownPodBatchSize":"many"}`,
			},
			wantError: "cannot unmarshal string",
		},
		{
			name: "malformed requeue interval is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownRequeueInterval":"many"}`,
			},
			wantError: "lifecycle.scaleDownRequeueInterval",
		},
		{
			name: "explicit empty requeue interval is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownRequeueInterval":""}`,
			},
			wantError: "lifecycle.scaleDownRequeueInterval",
		},
		{
			name: "zero requeue interval is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownRequeueInterval":"0s"}`,
			},
			wantError: "must be > 0, got 0s",
		},
		{
			name: "negative requeue interval is rejected",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownRequeueInterval":"-1s"}`,
			},
			wantError: "must be > 0, got -1s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			if !tt.omitConfigMap {
				_, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(
					context.Background(),
					&corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      constants.InferenceServiceConfigMapName,
							Namespace: constants.OMENamespace,
						},
						Data: tt.configMapData,
					},
					metav1.CreateOptions{},
				)
				require.NoError(t, err)
			}

			before := len(clientset.Actions())
			got, err := loadPodBatchSizes(clientset)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				assert.Equal(t, controllerconfig.PodBatchSizes{}, got)
			} else {
				require.NoError(t, err)
				if tt.wantScaleUp == 0 {
					assert.Nil(t, got.ScaleUp)
				} else {
					require.NotNil(t, got.ScaleUp)
					assert.Equal(t, tt.wantScaleUp, *got.ScaleUp)
				}
				if tt.wantScaleDown == 0 {
					assert.Nil(t, got.ScaleDown)
				} else {
					require.NotNil(t, got.ScaleDown)
					assert.Equal(t, tt.wantScaleDown, *got.ScaleDown)
				}
				assert.Equal(t, tt.wantInterval, got.ScaleDownRequeueInterval)
			}

			getCount := 0
			for _, action := range clientset.Actions()[before:] {
				if action.GetVerb() == "get" && action.GetResource().Resource == "configmaps" {
					getCount++
				}
			}
			assert.Equal(t, 1, getCount, "scale settings must share one ConfigMap GET")
		})
	}
}

// TestDeployConfigSetup tests the setup of deployment configuration
func TestDeployConfigSetup(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func() (kubernetes.Interface, error)
		shouldError bool
	}{
		{
			name: "successful config setup",
			setupFunc: func() (kubernetes.Interface, error) {
				client := fake.NewSimpleClientset()
				// Create the required ConfigMap
				_, err := client.CoreV1().ConfigMaps("ome").Create(context.Background(), createMockConfigMap(), metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return client, nil
			},
			shouldError: false,
		},
		{
			name: "config setup failure",
			setupFunc: func() (kubernetes.Interface, error) {
				return nil, errors.New("failed to create clientset")
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset, err := tt.setupFunc()
			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			deployConfig, err := controllerconfig.NewDeployConfig(clientset)
			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, deployConfig)
			}
		})
	}
}

// TestIngressConfigSetup tests the setup of ingress configuration
func TestIngressConfigSetup(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func() (kubernetes.Interface, error)
		shouldError bool
	}{
		{
			name: "successful ingress config setup",
			setupFunc: func() (kubernetes.Interface, error) {
				client := fake.NewSimpleClientset()
				// Create the required ConfigMap
				_, err := client.CoreV1().ConfigMaps("ome").Create(context.Background(), createMockConfigMap(), metav1.CreateOptions{})
				if err != nil {
					return nil, err
				}
				return client, nil
			},
			shouldError: false,
		},
		{
			name: "ingress config setup failure",
			setupFunc: func() (kubernetes.Interface, error) {
				return nil, errors.New("failed to create clientset")
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset, err := tt.setupFunc()
			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			ingressConfig, err := controllerconfig.NewIngressConfig(clientset)
			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ingressConfig)
			}
		})
	}
}

// TestCRDSetup tests the setup of various CRDs
func TestCRDSetup(t *testing.T) {
	tests := []struct {
		name        string
		crdType     string
		available   bool
		setupError  error
		shouldError bool
	}{
		{
			name:        "Volcano CRD available",
			crdType:     "Volcano",
			available:   true,
			setupError:  nil,
			shouldError: false,
		},
		{
			name:        "Error checking CRD",
			crdType:     "Volcano",
			available:   false,
			setupError:  errors.New("failed to check CRD"),
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockChecker := &mockCRDChecker{
				available: tt.available,
				err:       tt.setupError,
			}

			cfg := &rest.Config{}
			var err error

			switch tt.crdType {
			case "Volcano":
				_, err = mockChecker.IsCrdAvailable(cfg, volcano.SchemeGroupVersion.String(), constants.VolcanoQueueKind)
			}

			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
