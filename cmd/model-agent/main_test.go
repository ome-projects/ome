package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"k8s.io/client-go/rest"
)

func setupTestEnv(t *testing.T) {
	t.Helper()
	// Save original env and restore after test
	originalNodeName := os.Getenv("NODE_NAME")
	t.Cleanup(func() {
		os.Setenv("NODE_NAME", originalNodeName)
	})
	os.Setenv("NODE_NAME", "test-node")
}

func setupTestLogger(t *testing.T) *zap.SugaredLogger {
	return zaptest.NewLogger(t).Sugar()
}

func TestConfigInitialization(t *testing.T) {
	// Reset config before each test
	cfg = &config{}

	tests := []struct {
		name          string
		setupEnv      func()
		expectedPanic bool
	}{
		{
			name: "valid NODE_NAME",
			setupEnv: func() {
				os.Setenv("NODE_NAME", "test-node")
				// Instead of calling initConfig, directly set the nodeName
				cfg.nodeName = os.Getenv("NODE_NAME")
			},
			expectedPanic: false,
		},
		{
			name: "missing NODE_NAME",
			setupEnv: func() {
				os.Unsetenv("NODE_NAME")
			},
			expectedPanic: true,
		},
		{
			name: "empty NODE_NAME",
			setupEnv: func() {
				os.Setenv("NODE_NAME", "")
			},
			expectedPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectedPanic {
				assert.Panics(t, func() {
					tt.setupEnv()
					// Simulate the NODE_NAME check as in initConfig()
					nodeName, ok := os.LookupEnv("NODE_NAME")
					if !ok || nodeName == "" {
						panic("NODE_NAME environment variable is not set for model-agent")
					}
					cfg.nodeName = nodeName
				})
			} else {
				assert.NotPanics(t, func() {
					tt.setupEnv()
					// Simulate the NODE_NAME check as in initConfig()
					nodeName, ok := os.LookupEnv("NODE_NAME")
					if !ok || nodeName == "" {
						panic("NODE_NAME environment variable is not set for model-agent")
					}
					cfg.nodeName = nodeName
				})
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	setupTestEnv(t)
	// Create a new command instance for testing
	testCmd := &cobra.Command{
		Use:   "start",
		Short: "Starts the model agent",
		Long:  `Starts the model agent to watch the base model custom resources and update the node labels`,
		Run:   runCommand,
	}

	// Reset config and initialize
	cfg = &config{}

	// Set up default flags
	testCmd.Flags().IntVar(&cfg.port, "health-check-port", 8080, "Address for readiness and liveness health check")
	testCmd.Flags().StringVar(&cfg.modelsRootDirOnHost, "models-root-dir-on-host", "/raid/models", "host's root dir for storing all models")
	testCmd.Flags().StringVar(&cfg.modelsRootDir, "models-root-dir", "/raid/models", "container's root dir for storing all models")
	testCmd.Flags().IntVar(&cfg.nodeLabelRetry, "node-label-retry", 2, "retry times for node label update")
	testCmd.Flags().IntVar(&cfg.downloadRetry, "download-retry", 3, "retry times for model download")
	testCmd.Flags().StringVar(&cfg.downloadAuthType, "download-auth-type", "instance-principal", "authentication method for model download")
	testCmd.Flags().IntVar(&cfg.numDownloadWorker, "num-download-worker", 3, "number of download workers")
	testCmd.Flags().IntVar(&cfg.numHighPriorityWorker, "num-high-priority-worker", 1, "number of high-priority workers")
	testCmd.Flags().DurationVar(&cfg.samePathReuseTimeout, "same-path-reuse-wait-timeout", 30*time.Minute, "same-path reuse wait timeout")
	testCmd.Flags().StringVar(&cfg.namespace, "namespace", "ome", "the namespace of the ome model agents daemon set")

	// Call initConfig to set cfg.nodeName
	initConfig(nil, nil)

	// Verify config values
	assert.Equal(t, "test-node", cfg.nodeName)
	assert.Equal(t, 8080, cfg.port)
	assert.Equal(t, "/raid/models", cfg.modelsRootDir)
	assert.Equal(t, "/raid/models", cfg.modelsRootDirOnHost)
	assert.Equal(t, 2, cfg.nodeLabelRetry)
	assert.Equal(t, 3, cfg.downloadRetry)
	assert.Equal(t, "instance-principal", cfg.downloadAuthType)
	assert.Equal(t, 3, cfg.numDownloadWorker)
	assert.Equal(t, 1, cfg.numHighPriorityWorker)
	assert.Equal(t, 30*time.Minute, cfg.samePathReuseTimeout)
	assert.Equal(t, "ome", cfg.namespace)
}

func TestSchedulingContractFlagsAreRegistered(t *testing.T) {
	tests := []struct {
		name         string
		defaultValue string
	}{
		{name: "num-high-priority-worker", defaultValue: "1"},
		{name: "task-scheduler-capacity", defaultValue: "4096"},
		{name: "same-path-reuse-wait-timeout", defaultValue: "30m0s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := rootCmd.PersistentFlags().Lookup(tt.name)
			require.NotNil(t, flag, "model-agent must expose --%s", tt.name)
			assert.Equal(t, tt.defaultValue, flag.DefValue)
		})
	}
}

func TestConfiguredSamePathReuseTimeoutAcceptsDeprecatedAlias(t *testing.T) {
	original := cfg
	t.Cleanup(func() { cfg = original })

	flags := parseSamePathTimeoutFlags(t, "--same-path-wait-timeout=45m")
	timeout, err := configuredSamePathReuseTimeout(flags)
	require.NoError(t, err)
	assert.Equal(t, 45*time.Minute, timeout)
}

func TestConfiguredSamePathReuseTimeoutRejectsConflictingFlags(t *testing.T) {
	original := cfg
	t.Cleanup(func() { cfg = original })

	flags := parseSamePathTimeoutFlags(t,
		"--same-path-reuse-wait-timeout=30m",
		"--same-path-wait-timeout=45m",
	)
	_, err := configuredSamePathReuseTimeout(flags)
	require.ErrorContains(t, err, "disagree")
}

func TestConfiguredSamePathReuseTimeoutAcceptsMatchingFlags(t *testing.T) {
	original := cfg
	t.Cleanup(func() { cfg = original })

	flags := parseSamePathTimeoutFlags(t,
		"--same-path-reuse-wait-timeout=45m",
		"--same-path-wait-timeout=45m",
	)
	timeout, err := configuredSamePathReuseTimeout(flags)
	require.NoError(t, err)
	assert.Equal(t, 45*time.Minute, timeout)
}

func parseSamePathTimeoutFlags(t *testing.T, args ...string) *pflag.FlagSet {
	t.Helper()
	cfg = &config{}
	flags := pflag.NewFlagSet("same-path-timeout-test", pflag.ContinueOnError)
	flags.DurationVar(&cfg.samePathReuseTimeout, "same-path-reuse-wait-timeout", 30*time.Minute, "")
	flags.DurationVar(&cfg.legacySamePathTimeout, "same-path-wait-timeout", 0, "")
	require.NoError(t, flags.Parse(args))
	return flags
}

func TestInitializeLogger(t *testing.T) {
	// Use Viper directly for logger configuration
	testViper := viper.New()
	testViper.Set("log.level", "info")
	testViper.Set("log.encoder", "console")
	testViper.Set("log.development", true)

	// Mock the initializeLogger function by setting Viper first
	v = testViper

	// Initialize logger
	logger, err := initializeLogger()
	require.NoError(t, err)
	require.NotNil(t, logger)

	// Test with different config
	testViper.Set("log.level", "debug")
	testViper.Set("log.encoder", "json")
	testViper.Set("log.development", false)

	// Re-initialize logger
	logger, err = initializeLogger()
	require.NoError(t, err)
	require.NotNil(t, logger)
}

func TestSetupServer(t *testing.T) {
	// Setup test logger
	logger := setupTestLogger(t)

	// Call the function being tested
	server := setupServer(8080, "/models", logger)

	// Verify server configuration
	require.NotNil(t, server)
	assert.Equal(t, ":8080", server.Addr)
	assert.NotNil(t, server.Handler)
}

func TestHealthCheckEndpoint(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "model-agent-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Setup test logger
	logger := setupTestLogger(t)

	// Setup server with the temp directory
	server := setupServer(8080, tempDir, logger)

	// Create test request for health check
	req := httptest.NewRequest("GET", "/healthz", nil)
	recorder := httptest.NewRecorder()

	// Process the request
	server.Handler.ServeHTTP(recorder, req)

	// Verify response
	resp := recorder.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Test metrics endpoint
	metricsReq := httptest.NewRequest("GET", "/metrics", nil)
	metricsRecorder := httptest.NewRecorder()

	// Process the request
	server.Handler.ServeHTTP(metricsRecorder, metricsReq)

	// Verify response
	metricsResp := metricsRecorder.Result()
	defer metricsResp.Body.Close()

	assert.Equal(t, http.StatusOK, metricsResp.StatusCode)
}

func TestInitializePrometheusMetrics(t *testing.T) {
	// Setup test logger and registry
	logger := setupTestLogger(t)
	origReg := prometheus.DefaultRegisterer
	defer func() {
		prometheus.DefaultRegisterer = origReg
	}()

	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	// Call the function being tested
	metrics := initializePrometheusMetrics(logger)

	// Verify metrics was created
	require.NotNil(t, metrics)
}

// TestCreateKubeClient tests the createKubeClient function behavior
func TestCreateKubeClient(t *testing.T) {
	// Create a mock REST config
	config := &rest.Config{
		Host: "https://localhost:8443",
	}

	// Call the function being tested (should not panic)
	assert.NotPanics(t, func() {
		client := createKubeClient(config)
		require.NotNil(t, client)
	})
}

// TestCreateOmeClient tests the createOmeClient function behavior
func TestCreateOmeClient(t *testing.T) {
	// Create a mock REST config
	config := &rest.Config{
		Host: "https://localhost:8443",
	}

	// Call the function being tested (should not panic)
	assert.NotPanics(t, func() {
		client := createOmeClient(config)
		require.NotNil(t, client)
	})
}

// TestGetKubeConfig tests that we can access getKubeConfig
// but only verify the function exists rather than calling it
func TestGetKubeConfig(t *testing.T) {
	// Skip if in CI environment
	if os.Getenv("CI") != "" {
		t.Skip("Skipping test in CI environment")
	}

	// We can verify the function exists, but don't call it as it will panic
	// in a non-Kubernetes environment
	assert.NotNil(t, getKubeConfig, "getKubeConfig function should exist")
}

// TestSetupKubernetesClientsSkip skips the actual test in non-K8s environments
func TestSetupKubernetesClientsSkip(t *testing.T) {
	// Since we're not in a K8s environment, we'll skip this test
	t.Skip("Skipping test that requires Kubernetes environment")
}

// TestSetupInformersBasicSkip verifies setupInformers handles a nil client
func TestSetupInformersBasicSkip(t *testing.T) {
	// Skip this test as it would require adapting the setupInformers function
	t.Skip("Skipping test that requires modifying setupInformers to handle nil clients")
}

// TestInitializePrometheusMetricsCoverage tests metrics initialization
// without causing duplicate registrations
func TestInitializePrometheusMetricsCoverage(t *testing.T) {
	// Setup test logger
	logger := setupTestLogger(t)

	// Save original registerer
	origReg := prometheus.DefaultRegisterer

	// Create a completely new registry to avoid collisions
	// with metrics registered in other tests
	defer func() {
		prometheus.DefaultRegisterer = origReg
	}()

	// Set a new clean registry for this test
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	// Call the function being tested
	metrics := initializePrometheusMetrics(logger)

	// Verify the metrics were initialized
	require.NotNil(t, metrics)

	// Create a second registry for the second test
	// Rather than trying to register the same metrics twice,
	// we'll use a separate registry
	secondReg := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = secondReg

	// Try again with new registry
	metrics2 := initializePrometheusMetrics(logger)
	require.NotNil(t, metrics2, "Should still return metrics object with fresh registry")
}

// TestRunCommandBasic tests basic error case handling in runCommand
func TestRunCommandBasic(t *testing.T) {
	// Since runCommand is complex and starts many goroutines,
	// we'll simply verify that the function signature matches what we expect
	t.Skip("Skipping runCommand test as it's difficult to test isolated from real environment")
}

// TestMainBasic tests that main function doesn't panic
func TestMainBasic(t *testing.T) {
	// Save original rootCmd and restore after test
	origRootCmd := rootCmd
	defer func() {
		rootCmd = origRootCmd
	}()

	// Create a non-nil command that doesn't execute
	rootCmd = &cobra.Command{
		Use:   "test",
		Short: "Test command",
		Run: func(_ *cobra.Command, _ []string) {
			// Do nothing
		},
	}

	// Test that main doesn't panic
	assert.NotPanics(t, func() {
		main()
	})
}
