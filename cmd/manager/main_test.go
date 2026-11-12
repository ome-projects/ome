package main

import (
	"context"
	"errors"
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	volcano "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/webhook/admission/splitplan"
)

// withArgs gives a test its own flag set and arguments for GetOptions and
// restores the process's parsed command line afterwards: the test runner
// reads that command line again before every repeated run (-count).
func withArgs(t *testing.T, args ...string) {
	t.Helper()
	oldArgs, oldFlags := os.Args, flag.CommandLine
	flag.CommandLine = flag.NewFlagSet(args[0], flag.ExitOnError)
	os.Args = args
	t.Cleanup(func() {
		os.Args = oldArgs
		flag.CommandLine = oldFlags
	})
}

func TestGetOptions(t *testing.T) {
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
			withArgs(t, tt.args...)

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
	assert.Empty(t, opts.pprofAddr, "profiling stays off unless the chart asks for it")
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
	assert.Zero(t, opts.leaderElectionTiming.LeaseDuration)
	assert.Zero(t, opts.leaderElectionTiming.RenewDeadline)
	assert.Zero(t, opts.leaderElectionTiming.RetryPeriod)
}

func TestGetOptionsLeaderElectionTiming(t *testing.T) {
	withArgs(t, "cmd",
		"--leader-elect-lease-duration=60s",
		"--leader-elect-renew-deadline=40s",
		"--leader-elect-retry-period=8s",
	)

	o := GetOptions()
	assert.Equal(t, 60*time.Second, o.leaderElectionTiming.LeaseDuration)
	assert.Equal(t, 40*time.Second, o.leaderElectionTiming.RenewDeadline)
	assert.Equal(t, 8*time.Second, o.leaderElectionTiming.RetryPeriod)
	assert.NoError(t, o.leaderElectionTiming.Validate())
}

func TestManagerOptionsLeaderElectionReleaseOnCancel(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "the manager option is off without the flag", args: []string{"cmd", "--leader-elect"}},
		{name: "the flag turns the manager option on", args: []string{"cmd", "--leader-elect", "--leader-elect-release-on-cancel"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withArgs(t, tt.args...)

			opts := (&startup{options: GetOptions()}).managerOptions(cache.Options{})
			t.Cleanup(opts.EventBroadcaster.Shutdown) //nolint:staticcheck // managerOptions starts the broadcaster the manager would own
			assert.Equal(t, tt.want, opts.LeaderElectionReleaseOnCancel)
		})
	}
}

func TestValidateLeaderElectionTimingWithReleaseOnCancel(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{
			name: "start-up goes on when the lease outlasts a request timeout, retry and twice renew",
			args: []string{"cmd", "--leader-elect", "--leader-elect-release-on-cancel",
				"--leader-elect-lease-duration=120s", "--leader-elect-renew-deadline=40s", "--leader-elect-retry-period=8s"},
			want: true,
		},
		{
			name: "start-up stops when the lease does not outlast a request timeout, retry and twice renew",
			args: []string{"cmd", "--leader-elect", "--leader-elect-release-on-cancel",
				"--leader-elect-lease-duration=60s", "--leader-elect-renew-deadline=40s", "--leader-elect-retry-period=8s"},
		},
		{
			name: "start-up stops when the timing flags are unset",
			args: []string{"cmd", "--leader-elect", "--leader-elect-release-on-cancel"},
		},
		{
			name: "start-up goes on without the flag at a timing the flag would refuse",
			args: []string{"cmd", "--leader-elect",
				"--leader-elect-lease-duration=60s", "--leader-elect-renew-deadline=40s", "--leader-elect-retry-period=8s"},
			want: true,
		},
		{
			name: "start-up goes on with leader election off, the release flag set and no timing",
			args: []string{"cmd", "--leader-elect-release-on-cancel"},
			want: true,
		},
		{
			name: "start-up goes on with leader election off at a timing the release would refuse",
			args: []string{"cmd", "--leader-elect-release-on-cancel",
				"--leader-elect-lease-duration=60s", "--leader-elect-renew-deadline=40s", "--leader-elect-retry-period=8s"},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withArgs(t, tt.args...)

			assert.Equal(t, tt.want, (&startup{options: GetOptions()}).validateLeaderElectionTiming())
		})
	}
}

func TestGetOptionsKubeAPIRateLimits(t *testing.T) {
	withArgs(t, "cmd", "--kube-api-qps=50", "--kube-api-burst=100")

	o := GetOptions()
	assert.Equal(t, 50.0, o.kubeAPIQPS)
	assert.Equal(t, 100, o.kubeAPIBurst)
}

func TestGetOptionsPprofBindAddress(t *testing.T) {
	withArgs(t, "cmd", "--pprof-bind-address=127.0.0.1:6060")

	o := GetOptions()
	assert.Equal(t, "127.0.0.1:6060", o.pprofAddr)
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
	withArgs(t,
		"cmd",
		"--enable-multicluster",
		"--multicluster-role=control-plane",
		"--allow-exec-credentials",
		"--exec-credential-allowed-commands=aws,foo",
		"--placement-control-plane-id=cp-1",
	)

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

func TestDeclaredWebhooksAreRegistered(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	registered := map[string]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if path, isLit := call.Args[0].(*ast.BasicLit); isLit && selector.Sel.Name == "Register" && isWebhookServer(selector.X) {
			registered[strings.Trim(path.Value, `"`)] = true
		}
		if named, isNamed := call.Args[0].(*ast.SelectorExpr); isNamed && selector.Sel.Name == "Register" && isWebhookServer(selector.X) {
			if path, known := namedWebhookPaths[exprName(named)]; known {
				registered[path] = true
			}
		}
		if selector.Sel.Name == "NewWebhookManagedBy" && len(call.Args) == 2 {
			// controller-runtime derives the validator path from the registered v1beta1 type.
			if object, ok := call.Args[1].(*ast.UnaryExpr); ok {
				if literal, ok := object.X.(*ast.CompositeLit); ok {
					if kind, ok := literal.Type.(*ast.SelectorExpr); ok {
						registered["/validate-ome-io-v1beta1-"+strings.ToLower(kind.Sel.Name)] = true
					}
				}
			}
		}
		return true
	})

	manifests, err := filepath.Glob(filepath.Join("..", "..", "charts", "ome-resources", "templates", "ome-controller", "webhooks", "*.yaml"))
	require.NoError(t, err)
	manifests = append(manifests, filepath.Join("..", "..", "config", "webhook", "manifests.yaml"))
	declared := 0
	for _, manifest := range manifests {
		data, err := os.ReadFile(manifest)
		require.NoError(t, err)
		for _, line := range strings.Split(string(data), "\n") {
			if path, ok := strings.CutPrefix(strings.TrimSpace(line), "path: "); ok {
				declared++
				assert.True(t, registered[path], "%s declares %s but cmd/manager/main.go does not register it", manifest, path)
			}
		}
	}
	require.NotZero(t, declared, "no webhook paths found in the manifests")
}

// namedWebhookPaths resolves the path constants main.go registers by name
// instead of by string literal.
var namedWebhookPaths = map[string]string{
	"splitplan.ValidatorPath": splitplan.ValidatorPath,
}

// exprName renders a package-qualified selector such as splitplan.ValidatorPath.
func exprName(expr *ast.SelectorExpr) string {
	if pkg, ok := expr.X.(*ast.Ident); ok {
		return pkg.Name + "." + expr.Sel.Name
	}
	return expr.Sel.Name
}

// isWebhookServer reports whether expr is the webhook server a registration
// goes to: the hookServer variable or the manager's GetWebhookServer call.
func isWebhookServer(expr ast.Expr) bool {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name == "hookServer"
	case *ast.CallExpr:
		selector, ok := x.Fun.(*ast.SelectorExpr)
		return ok && selector.Sel.Name == "GetWebhookServer"
	}
	return false
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
		wantScaleDown intstr.IntOrString
		wantInterval  time.Duration
		wantError     string
	}{
		{
			name: "positive configured values",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleUpPodBatchSize":37,"scaleDownPodBatchSize":41,"scaleDownRequeueInterval":"7s"}`,
			},
			wantScaleUp:   37,
			wantScaleDown: intstr.FromInt32(41),
			wantInterval:  7 * time.Second,
		},
		{
			name: "percentage configured at manager startup",
			configMapData: map[string]string{
				controllerconfig.LifecycleConfigName: `{"scaleDownPodBatchSize":"10%"}`,
			},
			wantScaleDown: intstr.FromString("10%"),
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
			wantScaleDown: intstr.FromInt32(41),
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
			wantError: "lifecycle.scaleDownPodBatchSize",
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
				if tt.wantScaleDown == (intstr.IntOrString{}) {
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

func TestRunStartupSteps_StopsAtTheFirstStepThatDoesNotGoOn(t *testing.T) {
	var ran []string
	step := func(name string, next bool) startupStep {
		return func() bool {
			ran = append(ran, name)
			return next
		}
	}
	run := func(steps ...startupStep) (bool, string) {
		ran = nil
		return runStartupSteps(steps), strings.Join(ran, ",")
	}

	if next, order := run(step("first", true), step("second", true)); !next || order != "first,second" {
		t.Fatalf("every step went on: next=%v ran=%q, want next=true ran=first,second", next, order)
	}
	if next, order := run(step("first", true), step("stops", false), step("never", true)); next || order != "first,stops" {
		t.Fatalf("a step stopped start-up: next=%v ran=%q, want next=false ran=first,stops", next, order)
	}
	if next, order := run(); !next || order != "" {
		t.Fatalf("no steps: next=%v ran=%q, want next=true and nothing run", next, order)
	}

	stops := func() bool {
		ran = append(ran, "stops")
		return stop(errors.New("the apiserver refused the connection"), "Failed to create Kubernetes client set")
	}
	if next, order := run(step("first", true), stops, step("never", true)); next || order != "first,stops" {
		t.Fatalf("a step stopped start-up through stop: next=%v ran=%q, want next=false ran=first,stops", next, order)
	}
}
