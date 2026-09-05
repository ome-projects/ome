package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	kubeapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"

	omev1beta1client "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omev1beta1informers "sigs.k8s.io/ome/pkg/client/informers/externalversions"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/modelagent"
	"sigs.k8s.io/ome/pkg/modelparser"
	"sigs.k8s.io/ome/pkg/version"
	"sigs.k8s.io/ome/pkg/xet"
)

// config holds all configuration parameters for the model agent
type config struct {
	port                  int
	modelsRootDir         string
	modelsRootDirOnHost   string
	nodeName              string
	nodeLabelRetry        int
	concurrency           int
	multipartConcurrency  int
	downloadRetry         int
	downloadAuthType      string
	numDownloadWorker     int
	numHighPriorityWorker int
	taskSchedulerCapacity int
	samePathReuseTimeout  time.Duration
	legacySamePathTimeout time.Duration
	namespace             string
	logLevel              string
}

// Logger type alias for zap.SugaredLogger
type Logger = zap.SugaredLogger

// Global variables
var (
	rootCmd = &cobra.Command{
		Use:              "model-agent",
		Short:            "Model agent for Open Model Engine",
		Run:              runCommand,
		PersistentPreRun: initConfig,
	}
	cfg = &config{}
	v   = viper.New() // Global viper instance for configuration
)

// init sets up command line flags and binds them to Viper
func init() {
	// Define command-line flags
	rootCmd.PersistentFlags().IntVar(&cfg.port, "port", 8080, "HTTP port for health checks")
	rootCmd.PersistentFlags().StringVar(&cfg.modelsRootDir, "models-root-dir", "/mnt/models", "Root directory for models")
	rootCmd.PersistentFlags().StringVar(&cfg.nodeName, "node-name", "", "Name of the node where agent is running")
	rootCmd.PersistentFlags().IntVar(&cfg.nodeLabelRetry, "node-label-retry", 5, "Number of retries for node labeling")
	rootCmd.PersistentFlags().IntVar(&cfg.downloadRetry, "download-retry", 3, "Number of retries for downloading")
	rootCmd.PersistentFlags().IntVar(&cfg.concurrency, "concurrency", 4, "Number of concurrent download workers per gopher")
	rootCmd.PersistentFlags().IntVar(&cfg.multipartConcurrency, "multipart-concurrency", 4, "Number of concurrent multipart download workers per gopher")
	rootCmd.PersistentFlags().IntVar(&cfg.numDownloadWorker, "num-download-worker", 5, "Number of download workers")
	rootCmd.PersistentFlags().IntVar(&cfg.numHighPriorityWorker, "num-high-priority-worker", 1, "Number of high-priority workers for delete and same-path reuse tasks")
	rootCmd.PersistentFlags().IntVar(&cfg.taskSchedulerCapacity, "task-scheduler-capacity", 4096, "Maximum number of distinct queued model tasks")
	rootCmd.PersistentFlags().DurationVar(&cfg.samePathReuseTimeout, "same-path-reuse-wait-timeout", 30*time.Minute, "Maximum time to wait for another task populating the same local artifact path before resuming the normal download flow")
	rootCmd.PersistentFlags().DurationVar(&cfg.legacySamePathTimeout, "same-path-wait-timeout", 0, "Deprecated alias for --same-path-reuse-wait-timeout")
	_ = rootCmd.PersistentFlags().MarkDeprecated("same-path-wait-timeout", "use --same-path-reuse-wait-timeout")
	rootCmd.PersistentFlags().StringVar(&cfg.namespace, "namespace", "ome", "Kubernetes namespace to use")
	rootCmd.PersistentFlags().StringVar(&cfg.logLevel, "log-level", "info", "Log level (debug, info, warn, error)")

	_ = v.BindPFlags(rootCmd.PersistentFlags())
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()
}

// initConfig validates required environment variables
func initConfig(_ *cobra.Command, _ []string) {
	nodeName, ok := os.LookupEnv("NODE_NAME")
	if !ok {
		panic("NODE_NAME environment variable is not set for model-agent")
	}
	if nodeName == "" {
		panic("NODE_NAME environment variable is empty")
	}
	cfg.nodeName = nodeName
}

// initializeLogger creates and configures a zap logger with the specified settings
func initializeLogger() (*Logger, error) {
	level, err := zapcore.ParseLevel(v.GetString("log-level"))
	if err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", "info", err)
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Development:      false,
		Encoding:         "console",
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	// Use a more human-friendly timestamp format for console encoder
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	if config.Encoding == "console" {
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	logger, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build logger: %w", err)
	}

	return logger.Sugar(), nil
}

// setupServer configures an HTTP server for health checks and metrics
func setupServer(port int, modelsRootDir string, logger *Logger) *http.Server {
	mux := http.NewServeMux()

	// Add health check endpoint
	healthz.InstallPathHandler(mux, "/healthz", modelagent.NewModelAgentHealthCheck(modelsRootDir))

	// Add liveness check
	healthz.InstallLivezHandler(mux, healthz.PingHealthz)

	// Add metrics endpoint
	modelagent.RegisterMetricsHandler(mux)
	logger.Info("Registered Prometheus metrics endpoint at /metrics")

	logger.Infof("Health check server configured with port %d", port)
	logger.Infof("Health check configured for models root dir: %s", modelsRootDir)

	return &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
}

// setupKubernetesClients creates the Kubernetes and OME clients
func setupKubernetesClients() (*kubernetes.Clientset, *omev1beta1client.Clientset, error) {
	cfg := ctrl.GetConfigOrDie()
	kubeClient := createKubeClient(cfg)
	omeClient := createOmeClient(cfg)
	return kubeClient, omeClient, nil
}

// initializePrometheusMetrics sets up Prometheus metrics and registers collectors
func initializePrometheusMetrics(logger *Logger) *modelagent.Metrics {
	// Register Go and process collectors (safely, without panicking if already registered)
	reg := prometheus.DefaultRegisterer

	// Register Go collector
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		// Ignore "already exists" errors, warn about others
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			logger.Warnf("Error registering Go collector: %v", err)
		}
	}

	// Register Process collector
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			logger.Warnf("Error registering Process collector: %v", err)
		}
	}

	// Initialize metrics
	metrics := modelagent.NewMetrics(nil)
	logger.Info("Initialized Prometheus metrics")
	return metrics
}

// setupInformers initializes the Kubernetes informers for watching resources
func setupInformers(omeClient *omev1beta1client.Clientset) (omev1beta1informers.SharedInformerFactory, error) {
	var omeInformerOpts []omev1beta1informers.SharedInformerOption
	omeInformerFactory := omev1beta1informers.NewSharedInformerFactoryWithOptions(omeClient, 0, omeInformerOpts...)
	return omeInformerFactory, nil
}

// initializeComponents creates and initializes all the model agent components
func initializeComponents(
	ctx context.Context,
	kubeClient *kubernetes.Clientset,
	omeClient *omev1beta1client.Clientset,
	omeInformerFactory omev1beta1informers.SharedInformerFactory,
	metrics *modelagent.Metrics,
	gopherTaskChan chan *modelagent.GopherTask,
	logger *Logger,
	flags *pflag.FlagSet,
) (*modelagent.Scout, *modelagent.Gopher, error) {
	// Create node label reconciler for labeling the node based on model status
	nodeLabelReconciler := modelagent.NewNodeLabelReconciler(cfg.nodeName, kubeClient, cfg.nodeLabelRetry, logger)

	// Convert sugared logger back to a regular zap logger to use ForZap
	zapLogger := logger.Desugar()

	// Create a ModelConfigParser instance
	modelConfigParser := modelparser.NewModelConfigParser(omeClient, logger)

	// Create a ConfigMapReconciler instance
	configMapReconciler := modelagent.NewConfigMapReconciler(cfg.nodeName, cfg.namespace, kubeClient, logger)

	// Create a Scout instance
	baseModelInformer := omeInformerFactory.Ome().V1beta1().BaseModels()
	clusterBaseModelInformer := omeInformerFactory.Ome().V1beta1().ClusterBaseModels()

	scout, err := modelagent.NewScout(
		ctx,
		cfg.nodeName,
		baseModelInformer,
		clusterBaseModelInformer,
		omeInformerFactory,
		gopherTaskChan,
		kubeClient,
		logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create scout: %w", err)
	}

	// Add random jitter to prevent thundering herd when multiple agents start
	// This helps avoid hitting rate limits when many agents start simultaneously
	if cfg.nodeName != "" {
		// Use node name hash to create deterministic but distributed start delay
		hash := 0
		for _, c := range cfg.nodeName {
			hash = hash*31 + int(c)
		}
		// Create delay between 0-30 seconds based on node name
		jitterDelay := time.Duration(hash%30) * time.Second
		logger.Infof("Adding %v jitter delay before initializing HF client to prevent API rate limiting", jitterDelay)
		time.Sleep(jitterDelay)
	}

	// Create default Hugging Face hub config
	// Use log-only mode for cleaner logs in production
	xetHubConfig, err := xet.NewConfig(
		xet.WithDefaults(),
		xet.WithViper(v),                          // Apply viper config first to set defaults
		xet.WithLogger(logging.ForZap(zapLogger)), // Then set the logger
		xet.WithEnableProgressReporting(true),     // Enable progress reporting
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create HuggingFace hub config: %w", err)
	}

	logger.Infof("Configured Xet Hugging Face hub client with max concurrent downloads: %d", xetHubConfig.MaxConcurrentDownloads)

	// Create a Gopher instance for downloading models
	samePathReuseTimeout, err := configuredSamePathReuseTimeout(flags)
	if err != nil {
		return nil, nil, err
	}
	gopher, err := modelagent.NewGopher(
		modelConfigParser,
		configMapReconciler,
		xetHubConfig,
		kubeClient, // Pass the Kubernetes client for secret access
		cfg.concurrency,
		cfg.multipartConcurrency,
		cfg.downloadRetry,
		cfg.modelsRootDir,
		gopherTaskChan,
		samePathReuseTimeout,
		nodeLabelReconciler,
		metrics,
		logger,
		baseModelInformer.Lister(),
		clusterBaseModelInformer.Lister(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gopher: %w", err)
	}
	gopher.SetTaskSchedulerCapacity(cfg.taskSchedulerCapacity)

	return scout, gopher, nil
}

func configuredSamePathReuseTimeout(flags *pflag.FlagSet) (time.Duration, error) {
	canonicalSet := flags.Changed("same-path-reuse-wait-timeout")
	legacySet := flags.Changed("same-path-wait-timeout")
	if canonicalSet && legacySet && cfg.samePathReuseTimeout != cfg.legacySamePathTimeout {
		return 0, fmt.Errorf("--same-path-reuse-wait-timeout and deprecated --same-path-wait-timeout disagree")
	}
	if legacySet {
		return cfg.legacySamePathTimeout, nil
	}
	return cfg.samePathReuseTimeout, nil
}

// runCommand is the main entry point executed by Cobra
func runCommand(cmd *cobra.Command, args []string) {
	// Initialize logger
	logger, err := initializeLogger()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	// Log version information
	logger.Infow("Initializing", "gitVersion", version.GitVersion, "gitCommit", version.GitCommit)

	// Log all Viper config at startup for traceability
	logger.Infow("Model Agent configuration (Viper)", "allSettings", v.AllSettings())

	// Setup Kubernetes clients
	kubeClient, omeClient, err := setupKubernetesClients()
	if err != nil {
		logger.Fatalf("Failed to setup Kubernetes clients: %v", err)
	}

	// Setup informers
	omeInformerFactory, err := setupInformers(omeClient)
	if err != nil {
		logger.Fatalf("Failed to setup informers: %v", err)
	}

	// Setup metrics
	metrics := initializePrometheusMetrics(logger)

	// Setup signal handling
	stopCh := kubeapiserver.SetupSignalHandler()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	// Create a download task communication channel
	gopherTaskChan := make(chan *modelagent.GopherTask)

	// Initialize components
	scout, gopher, err := initializeComponents(
		ctx,
		kubeClient,
		omeClient,
		omeInformerFactory,
		metrics,
		gopherTaskChan,
		logger,
		cmd.PersistentFlags(),
	)
	if err != nil {
		logger.Fatalf("Failed to initialize components: %v", err)
	}

	// Set up a health check server
	server := setupServer(cfg.port, cfg.modelsRootDir, logger)
	go func() {
		logger.Infof("Starting health check server on port %d", cfg.port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Errorf("Health check server error: %v", err)
		}
	}()

	// Start gopher (download workers)
	go gopher.Run(stopCh, cfg.numDownloadWorker, cfg.numHighPriorityWorker)

	// Start scout (watchers)
	if err := scout.Run(stopCh); err != nil {
		logger.Fatalf("Error running scout: %v", err)
	}
}

// createKubeClient creates a Kubernetes client from the provided config
func createKubeClient(kubeConfig *rest.Config) *kubernetes.Clientset {
	return kubernetes.NewForConfigOrDie(kubeConfig)
}

// createOmeClient creates an OME client from the provided config
func createOmeClient(kubeConfig *rest.Config) *omev1beta1client.Clientset {
	return omev1beta1client.NewForConfigOrDie(kubeConfig)
}

// getKubeConfig creates and returns a Kubernetes REST config
func getKubeConfig() *rest.Config {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to create in-cluster Kubernetes config: %v", err))
	}
	return config
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
