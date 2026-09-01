package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	zaplog "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"
	schedulerpluginsv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	volcanobatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	volcano "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	v1beta1acceleratorclasscontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/acceleratorclass"
	v1beta1basemodelcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	v1beta1inferencereplicacontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/inferencereplica"
	v1beta1isvccontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	trafficfactory "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/factory"
	v1beta1runtimerevisioncontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/runtimerevision"
	v1beta1servingruntimecontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/servingruntime"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils"
	"sigs.k8s.io/ome/pkg/version"
	basemodelwebhook "sigs.k8s.io/ome/pkg/webhook/admission/basemodel"
	inferencereplicawebhook "sigs.k8s.io/ome/pkg/webhook/admission/inferencereplica"
	"sigs.k8s.io/ome/pkg/webhook/admission/isvc"
	"sigs.k8s.io/ome/pkg/webhook/admission/pod"
	"sigs.k8s.io/ome/pkg/webhook/admission/runtimepreset"
	runtimerevisionwebhook "sigs.k8s.io/ome/pkg/webhook/admission/runtimerevision"
	"sigs.k8s.io/ome/pkg/webhook/admission/servingruntime"
)

const (
	LeaderLockName          = "ome-controller-manager-leader-lock"
	LeaderElectionNamespace = "ome"

	// multiClusterRoleControlPlane is the --multicluster-role value that makes
	// this manager the multi-cluster fan-out control plane: it runs the placement
	// controller and disables the local InferenceService->pods reconcilers.
	multiClusterRoleControlPlane = "control-plane"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
	tlsOpts  []func(*tls.Config)
)

// registerOptionalScheme attempts to register a scheme if its CRD is available
func registerOptionalScheme(cfg *rest.Config, s *runtime.Scheme, groupVersion schema.GroupVersion, kind string, addToScheme func(*runtime.Scheme) error) error {
	found, err := utils.IsCrdAvailable(cfg, groupVersion.String(), kind)
	if err != nil {
		return fmt.Errorf("error checking if %s kind is available: %w", kind, err)
	}
	if found {
		setupLog.Info("Setting up scheme", "groupVersion", groupVersion.String(), "kind", kind)
		if err := addToScheme(s); err != nil {
			return fmt.Errorf("unable to add %s APIs to scheme: %w", kind, err)
		}
	}
	return nil
}

func loadPodBatchSizes(clientset kubernetes.Interface) (controllerconfig.PodBatchSizes, error) {
	return controllerconfig.LoadPodBatchSizes(clientset)
}

func managerProbeChecker(enableWebhook bool, webhookServer func() webhook.Server) healthz.Checker {
	if !enableWebhook {
		return healthz.Ping
	}
	return webhookServer().StartedChecker()
}

func shouldRegisterInferenceReplicaCapability(enableController, isControlPlane, enableLeaderElection bool) bool {
	return enableController && !isControlPlane && enableLeaderElection
}

func newInferenceReplicaCapabilityReadiness(enableController, isControlPlane, enableLeaderElection bool) chan struct{} {
	if !shouldRegisterInferenceReplicaCapability(enableController, isControlPlane, enableLeaderElection) {
		return nil
	}
	return make(chan struct{})
}

func podIdentity() (string, error) {
	if podName := os.Getenv("POD_NAME"); podName != "" {
		return podName, nil
	}
	return os.Hostname()
}

func init() {
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	utilruntime.Must(schedulerpluginsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kueuev1beta2.AddToScheme(scheme))
	// KEDA is a hard dependency, not an
	// optional CRD probe. Register the scheme unconditionally — if KEDA is
	// absent from the cluster the first ScaledObject Create will fail with
	// a clear apiserver error, which is the documented cluster-owner
	// contract.
	utilruntime.Must(kedav1.AddToScheme(scheme))
}

// splitAndTrim splits a comma-separated list, trims spaces, and drops empties.
func splitAndTrim(csv string) []string {
	var out []string
	for _, p := range strings.Split(csv, ",") {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Options defines the program-configurable options that may be passed on the command line.
type Options struct {
	metricsAddr                 string
	secureMetrics               bool
	enableHTTP2                 bool
	webhookPort                 int
	enableLeaderElection        bool
	enableWebhook               bool
	probeAddr                   string
	leaderElectionNamespace     string
	runtimeRevisionRetention    int
	runtimeRevisionGracePeriod  time.Duration
	isvcMaxConcurrentReconciles int
	irMaxConcurrentReconciles   int
	enableInferenceReplicaCtrl  bool
	// Client-side rate limit toward the local apiserver, shared by the manager's
	// cache/client, the direct clientset, and the webhook handlers.
	kubeAPIQPS   float64
	kubeAPIBurst int
	// Multi-cluster topology, identity, and security: these decide which
	// controllers run and the manager's identity, so they are deploy-time flags.
	// The tunables live in the inferenceservice-config ConfigMap
	// (controllerconfig.MultiClusterConfig), loaded at startup.
	enableMultiCluster        bool
	multiClusterRole          string
	allowExecCredentials      bool
	execCredentialAllowedCmds string
	placementControlPlaneID   string
	configCacheTTL            time.Duration
	zapOpts                   zap.Options
}

// DefaultOptions returns the default values for the program options.
func DefaultOptions() Options {
	return Options{
		metricsAddr:                ":8080",
		webhookPort:                9443,
		enableLeaderElection:       false,
		enableWebhook:              false,
		enableHTTP2:                false,
		secureMetrics:              false,
		probeAddr:                  ":8081",
		leaderElectionNamespace:    LeaderElectionNamespace,
		runtimeRevisionRetention:   10,
		runtimeRevisionGracePeriod: 24 * time.Hour,
		// The InferenceReplica controller defaults ON. The reconciler
		// drives the per-Component pipeline via workload.Reconcile when
		// an IR is created directly (the ISVC controller projects IRs
		// via irprojector). The flag exists so operators can disable
		// the controller if a regression lands.
		enableInferenceReplicaCtrl: true,
		enableMultiCluster:         false,
		multiClusterRole:           "",
		allowExecCredentials:       false,
		execCredentialAllowedCmds:  "aws,gke-gcloud-auth-plugin,kubelogin",
		// Short TTL for the inferenceservice-config ConfigMap read on the ISVC
		// reconcile hot path. Collapses the per-pass Deploy/InferenceServices/
		// Ingress/CanaryAnalysis loads onto one apiserver GET per TTL window
		// while staying short enough that a ConfigMap edit applies without a
		// restart. Override via --config-cache-ttl (0 disables caching).
		configCacheTTL: 30 * time.Second,
		zapOpts: zap.Options{
			TimeEncoder: zapcore.RFC3339TimeEncoder,
			ZapOpts:     []zaplog.Option{zaplog.AddCaller()},
		},
	}
}

// GetOptions parses the program flags and returns them as Options.
func GetOptions() Options {
	opts := DefaultOptions()
	flag.StringVar(&opts.metricsAddr, "metrics-bind-address", opts.metricsAddr, "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.BoolVar(&opts.secureMetrics, "metrics-secure", opts.secureMetrics,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.BoolVar(&opts.enableHTTP2, "enable-http2", opts.enableHTTP2,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.IntVar(&opts.webhookPort, "webhook-port", opts.webhookPort, "The port that the webhook server binds to.")
	flag.BoolVar(&opts.enableLeaderElection, "leader-elect", opts.enableLeaderElection,
		"Enable leader election for ome controller manager. "+
			"Enabling this will ensure there is only one active ome controller manager.")
	flag.StringVar(&opts.leaderElectionNamespace, "leader-election-namespace", opts.leaderElectionNamespace, "The namespace in which the leader election configmap will be created.")
	flag.BoolVar(&opts.enableWebhook, "webhook", opts.enableWebhook, "Enable the webhook server.")
	flag.StringVar(&opts.probeAddr, "health-probe-addr", opts.probeAddr, "The address the probe endpoint binds to.")
	flag.IntVar(&opts.runtimeRevisionRetention, "runtime-revision-retention", opts.runtimeRevisionRetention,
		"Max OME-owned ControllerRevisions to keep per source runtime (older ones GC after the grace period if unreferenced).")
	flag.DurationVar(&opts.runtimeRevisionGracePeriod, "runtime-revision-grace-period", opts.runtimeRevisionGracePeriod,
		"How long an OME-owned ControllerRevision stays after first being observed unreferenced + over retention.")
	flag.IntVar(&opts.isvcMaxConcurrentReconciles, "inferenceservice-max-concurrent-reconciles", opts.isvcMaxConcurrentReconciles,
		"Max InferenceService reconciles running in parallel (distinct objects only). No in-code default; "+
			"the chart supplies the value. Zero/unset falls back to controller-runtime's single-worker default.")
	flag.IntVar(&opts.irMaxConcurrentReconciles, "inferencereplica-max-concurrent-reconciles", opts.irMaxConcurrentReconciles,
		"Max InferenceReplica reconciles running in parallel (distinct objects only). No in-code default; "+
			"the chart supplies the value. Zero/unset falls back to controller-runtime's single-worker default.")
	flag.BoolVar(&opts.enableInferenceReplicaCtrl, "enable-inferencereplica-controller", opts.enableInferenceReplicaCtrl,
		"Run the InferenceReplica controller. Defaults true; flip to false to disable if the per-IR lifecycle code ships a regression.")
	flag.Float64Var(&opts.kubeAPIQPS, "kube-api-qps", opts.kubeAPIQPS,
		"Steady-state client-side request rate to the local apiserver, shared by the manager cache/client, the "+
			"direct clientset, and the webhook handlers. No in-code default; the chart supplies the value. "+
			"Zero/unset falls back to controller-runtime's default (20 QPS), which throttles reconcile throughput "+
			"once several controllers run concurrently.")
	flag.IntVar(&opts.kubeAPIBurst, "kube-api-burst", opts.kubeAPIBurst,
		"Token-bucket burst over --kube-api-qps, absorbing the request spikes of a reconcile fan-out or an informer "+
			"resync. No in-code default; the chart supplies the value. Zero/unset falls back to controller-runtime's "+
			"default (30).")
	flag.BoolVar(&opts.enableMultiCluster, "enable-multicluster", opts.enableMultiCluster,
		"Enable the multi-cluster controllers (WorkloadCluster registry). Alpha; default off.")
	flag.StringVar(&opts.multiClusterRole, "multicluster-role", opts.multiClusterRole,
		"Multi-cluster role. \"control-plane\" makes this the fan-out placer: it runs "+
			"the placement controller (which clones InferenceServices onto workload clusters) and "+
			"DISABLES the local InferenceService->pods reconcilers. Empty (default) is a normal "+
			"single-cluster / workload-cluster manager. \"control-plane\" implies --enable-multicluster.")
	flag.BoolVar(&opts.allowExecCredentials, "allow-exec-credentials", opts.allowExecCredentials,
		"Allow WorkloadCluster kubeconfigs to use an exec credential plugin (e.g. aws, gke-gcloud-auth-plugin) "+
			"for short-lived cloud tokens. Default false. The plugin command must also appear in "+
			"--exec-credential-allowed-commands, and its binary must be present in the manager image. "+
			"SECURITY: an exec block runs a command in the controller pod; keep WorkloadCluster kubeconfig Secrets admin-only.")
	flag.StringVar(&opts.execCredentialAllowedCmds, "exec-credential-allowed-commands", opts.execCredentialAllowedCmds,
		"Comma-separated allowlist of exec credential plugin commands permitted when --allow-exec-credentials is set. Entries match the kubeconfig command exactly: a bare name (\"aws\") permits PATH resolution, an absolute path pins one binary, and a path-qualified command is only allowed by that exact absolute path.")
	flag.StringVar(&opts.placementControlPlaneID, "placement-control-plane-id", opts.placementControlPlaneID,
		"identity stamped on every derived InferenceService this control plane creates. "+
			"Required when multiple control planes share a workload cluster so each control plane's GC "+
			"only reaps its OWN deriveds; the chart supplies a per-control-plane value. Empty (default) "+
			"keeps single-control-plane behavior (no identity stamping or GC scoping).")
	flag.DurationVar(&opts.configCacheTTL, "config-cache-ttl", opts.configCacheTTL,
		"TTL for the in-memory cache of the inferenceservice-config ConfigMap on the InferenceService reconcile path. "+
			"Collapses the per-reconcile config loads onto one apiserver GET per window; kept short so ConfigMap edits "+
			"apply without a restart. Set to 0 to disable caching (read the apiserver on every load).")
	opts.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return opts
}

// applyKubeAPIRateLimits sets the client-side rate limit on the rest.Config used
// for every connection to the local apiserver. It must run before the config is
// handed to the clientset, the discovery probes, and the manager, since each
// copies the limits at construction time.
//
// Zero leaves the value untouched so controller-runtime's default applies; the
// two limits are independent, so supplying only one is honored.
func applyKubeAPIRateLimits(cfg *rest.Config, qps float64, burst int) {
	if qps > 0 {
		cfg.QPS = float32(qps)
	}
	if burst > 0 {
		cfg.Burst = burst
	}
}

func main() {
	options := GetOptions()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options.zapOpts)))

	setupLog.Info("Initializing", "gitVersion", version.GitVersion, "gitCommit", version.GitCommit)

	// Get a config to talk to the apiserver
	setupLog.Info("Configuring API client connection")
	cfg := ctrl.GetConfigOrDie()
	applyKubeAPIRateLimits(cfg, options.kubeAPIQPS, options.kubeAPIBurst)
	setupLog.Info("Configured API client rate limits", "qps", cfg.QPS, "burst", cfg.Burst)

	// Setup clientset to directly talk to the api server
	setupLog.Info("Creating Kubernetes client set")
	clientSet, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "Failed to create Kubernetes client set")
		os.Exit(1)
	}

	if !options.enableHTTP2 {
		// if the enable-http2 flag is false (the default), http/2 should be disabled
		// due to its vulnerabilities. More specifically, disabling http/2 will
		// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
		// Rapid Reset CVEs. For more information see:
		// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
		// - https://github.com/advisories/GHSA-4374-p667-p6c8
		tlsOpts = append(tlsOpts, func(c *tls.Config) {
			setupLog.Info("disabling http/2")
			c.NextProtos = []string{"http/1.1"}
		})
	}

	// Create a new Cmd to provide shared dependencies and start components
	setupLog.Info("Initializing controller manager",
		"metricsAddr", options.metricsAddr,
		"webhookPort", options.webhookPort,
		"leaderElection", options.enableLeaderElection)

	// Scope the manager cache to OME-owned pods. With no Cache option,
	// controller-runtime LIST+WATCHes every Pod cluster-wide and holds them
	// in memory before any reconcile runs (gated by WaitForCacheSync) — on a
	// 130k-pod cluster the Pod informer alone is multi-GB and OOMKills the
	// manager regardless of how many InferenceServices exist. Every OME pod
	// (RawDeployment, MultiNode, OMENative/IR, PD engine/decoder/router)
	// carries the ome.io/inferenceservice label key, and every cached pod
	// read filters within that set, so scoping by key-existence hides nothing
	// the controllers read. Pods are the only cluster-scale type; the
	// remaining informers are bounded by the DefaultTransform below.
	omePodReq, err := labels.NewRequirement(constants.InferenceServicePodLabelKey, selection.Exists, nil)
	if err != nil {
		setupLog.Error(err, "Failed to build OME pod cache selector")
		os.Exit(1)
	}
	omePodSelector := labels.NewSelector().Add(*omePodReq)

	mgr, err := manager.New(cfg, manager.Options{
		Scheme: scheme,
		Cache: cache.Options{
			// Strip managedFields from every cached object (Pods, Nodes,
			// EndpointSlices, ConfigMaps, ...): typically a large fraction of
			// each object's serialized size and never read by OME. The
			// built-in transform handles non-metav1 objects safely, and a
			// ByObject entry with only Label set inherits this default.
			DefaultTransform: cache.TransformStripManagedFields(),
			ByObject: map[client.Object]cache.ByObject{
				&v1.Pod{}: {Label: omePodSelector},
			},
		},
		Metrics: metricsserver.Options{
			BindAddress:   options.metricsAddr,
			TLSOpts:       tlsOpts,
			SecureServing: options.secureMetrics,
		},
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    options.webhookPort,
			TLSOpts: tlsOpts,
		}),
		LeaderElection:          options.enableLeaderElection,
		LeaderElectionID:        LeaderLockName,
		LeaderElectionNamespace: options.leaderElectionNamespace,
		HealthProbeBindAddress:  options.probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "Failed to initialize controller manager")
		os.Exit(1)
	}

	// Register the OMENative pod field index so per-Instance pod lookups
	// resolve through the cache index instead of scanning every cached
	// pod — see query.ListOMENativePodsByName.
	if err := query.RegisterOMENativePodIndex(context.Background(), mgr.GetFieldIndexer()); err != nil {
		setupLog.Error(err, "Failed to register OMENative pod field index")
		os.Exit(1)
	}

	// NewDeployConfig is called here only for its startup-time
	// validation side effect — the ISVC reconciler re-fetches the
	// config on every reconcile via its Clientset, so the parsed
	// value is intentionally discarded.
	if _, err := controllerconfig.NewDeployConfig(clientSet); err != nil {
		setupLog.Error(err, "Failed to initialize deployment configuration")
		os.Exit(1)
	}
	podBatchSizes, err := loadPodBatchSizes(clientSet)
	if err != nil {
		setupLog.Error(err, "Failed to initialize lifecycle scale configuration")
		os.Exit(1)
	}
	ingressConfig, err := controllerconfig.NewIngressConfig(clientSet)
	if err != nil {
		setupLog.Error(err, "Failed to initialize ingress configuration")
		os.Exit(1)
	}
	omeAgentConfig, err := controllerconfig.NewOmeAgentConfig(clientSet)
	if err != nil {
		setupLog.Error(err, "Failed to initialize ome-agent configuration")
		os.Exit(1)
	}

	// Register optional schemes based on CRD availability
	setupLog.Info("Registering optional CRD schemes")
	optionalSchemes := []struct {
		groupVersion schema.GroupVersion
		kind         string
		addToScheme  func(*runtime.Scheme) error
	}{
		{lws.SchemeGroupVersion, constants.LWSKind, lws.AddToScheme},
		{volcano.SchemeGroupVersion, constants.VolcanoQueueKind, volcano.AddToScheme},
		{volcanobatch.SchemeGroupVersion, constants.VolcanoJobKind, volcanobatch.AddToScheme},
		{monitoringv1.SchemeGroupVersion, constants.PodMonitorKind, monitoringv1.AddToScheme},
	}

	for _, s := range optionalSchemes {
		if err := registerOptionalScheme(cfg, mgr.GetScheme(), s.groupVersion, s.kind, s.addToScheme); err != nil {
			setupLog.Error(err, "Failed to register optional scheme",
				"groupVersion", s.groupVersion.String(),
				"kind", s.kind)
			os.Exit(1)
		}
	}

	if ingressConfig.EnableGatewayAPI {
		setupLog.Info("Registering Gateway API scheme")
		utilruntime.Must(gatewayapiv1.Install(mgr.GetScheme()))
	}

	// Select the active traffic translator based on
	// installed backend-policy CRDs (Envoy Gateway BackendTrafficPolicy,
	// Istio DestinationRule, else Noop). The selection is process-
	// level — the translator stays stable for the controller's
	// lifetime — so we build it once at startup.
	setupLog.Info("Selecting traffic translator")
	trafficTranslator, err := trafficfactory.New(cfg)
	if err != nil {
		setupLog.Error(err, "Failed to select traffic translator")
		os.Exit(1)
	}
	setupLog.Info("Selected traffic translator", "translator", trafficTranslator.Name())
	trafficReconciler := traffic.NewReconciler(mgr.GetClient(), mgr.GetScheme(), trafficTranslator)

	// Setup Event Broadcaster
	setupLog.Info("Configuring event broadcaster")
	// Multi-cluster role resolution. On the control plane the placement (fan-out)
	// controller owns InferenceServices — it clones them onto workload clusters
	// — so the local ISVC->pods reconcilers must NOT run here.
	isControlPlane := options.multiClusterRole == multiClusterRoleControlPlane
	if options.multiClusterRole != "" && !isControlPlane {
		setupLog.Error(fmt.Errorf("invalid --multicluster-role %q (want %q or empty)", options.multiClusterRole, multiClusterRoleControlPlane), "bad flag")
		os.Exit(1)
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientSet.CoreV1().Events("")})
	if isControlPlane {
		setupLog.Info("control-plane role: local InferenceService reconciler disabled; placement controller owns ISVCs")
	} else {
		setupLog.Info("Setting up InferenceService controller")
		if err = (&v1beta1isvccontroller.InferenceServiceReconciler{
			Client:                  mgr.GetClient(),
			Clientset:               clientSet,
			Log:                     ctrl.Log.WithName("InferenceService"),
			Scheme:                  mgr.GetScheme(),
			Recorder:                eventBroadcaster.NewRecorder(mgr.GetScheme(), v1.EventSource{Component: "v1beta1Controllers"}),
			TrafficReconciler:       trafficReconciler,
			MaxConcurrentReconciles: options.isvcMaxConcurrentReconciles,
			ConfigCacheTTL:          options.configCacheTTL,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create InferenceService controller")
			os.Exit(1)
		}
	}

	// The control plane owns no workloads, models, or runtimes —
	// those live on the workload clusters. Running the model/runtime lifecycle
	// controllers here would reconcile against an empty local catalog (no-op
	// at best, churn at worst), so gate them off under the control-plane role.
	if isControlPlane {
		setupLog.Info("control-plane role: BaseModel/ClusterBaseModel/ServingRuntime/RuntimeRevisionGC controllers disabled")
	} else {
		setupLog.Info("Setting up BaseModel controller")
		if err = (&v1beta1basemodelcontroller.BaseModelReconciler{
			Client:         mgr.GetClient(),
			Log:            ctrl.Log.WithName("BaseModel"),
			Scheme:         mgr.GetScheme(),
			OmeAgentConfig: omeAgentConfig,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create BaseModel controller")
			os.Exit(1)
		}

		setupLog.Info("Setting up ClusterBaseModel controller")
		if err = (&v1beta1basemodelcontroller.ClusterBaseModelReconciler{
			Client:         mgr.GetClient(),
			Log:            ctrl.Log.WithName("ClusterBaseModel"),
			Scheme:         mgr.GetScheme(),
			OmeAgentConfig: omeAgentConfig,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create ClusterBaseModel controller")
			os.Exit(1)
		}

		// Single inheritance reconciler that handles both CSR and
		// SR via one source per GVK. Reconcile branches on req.Namespace.
		servingRuntimeEventBroadcaster := record.NewBroadcaster()
		servingRuntimeEventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientSet.CoreV1().Events("")})
		setupLog.Info("Setting up ServingRuntime inheritance controller")
		if err = (&v1beta1servingruntimecontroller.InheritanceReconciler{
			Client:   mgr.GetClient(),
			Log:      ctrl.Log.WithName("ServingRuntimeInheritance"),
			Scheme:   mgr.GetScheme(),
			Recorder: servingRuntimeEventBroadcaster.NewRecorder(mgr.GetScheme(), v1.EventSource{Component: "v1beta1Controllers"}),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create ServingRuntime inheritance controller")
			os.Exit(1)
		}

		setupLog.Info("Setting up runtime-revision GC controller",
			"retention", options.runtimeRevisionRetention,
			"gracePeriod", options.runtimeRevisionGracePeriod)
		if err = (&v1beta1runtimerevisioncontroller.GCReconciler{
			Client:              mgr.GetClient(),
			Log:                 ctrl.Log.WithName("RuntimeRevisionGC"),
			Scheme:              mgr.GetScheme(),
			OMENamespace:        constants.OMENamespace,
			RetentionPerRuntime: options.runtimeRevisionRetention,
			GracePeriod:         options.runtimeRevisionGracePeriod,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create runtime-revision GC controller")
			os.Exit(1)
		}
	}

	capabilityReadiness := newInferenceReplicaCapabilityReadiness(
		options.enableInferenceReplicaCtrl, isControlPlane, options.enableLeaderElection)
	if options.enableInferenceReplicaCtrl && !isControlPlane {
		setupLog.Info("Setting up InferenceReplica controller")
		if err = (&v1beta1inferencereplicacontroller.Reconciler{
			Client:                   mgr.GetClient(),
			Clientset:                clientSet,
			Log:                      ctrl.Log.WithName("InferenceReplica"),
			APIReader:                mgr.GetAPIReader(),
			Recorder:                 eventBroadcaster.NewRecorder(mgr.GetScheme(), v1.EventSource{Component: "v1beta1Controllers"}),
			MaxConcurrentReconciles:  options.irMaxConcurrentReconciles,
			ConfigCacheTTL:           options.configCacheTTL,
			ScaleUpPodBatchSize:      podBatchSizes.ScaleUp,
			ScaleDownPodBatchSize:    podBatchSizes.ScaleDown,
			ScaleDownRequeueInterval: podBatchSizes.ScaleDownRequeueInterval,
			// The same non-nil channel gates publisher registration below. The
			// controller closes it only when a worker processes the reserved
			// startup sentinel after all event sources have synchronized.
			CapabilityReadiness: capabilityReadiness,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create InferenceReplica controller")
			os.Exit(1)
		}
		if capabilityReadiness != nil {
			holder, holderErr := podIdentity()
			if holderErr != nil {
				setupLog.Error(holderErr, "Failed to determine InferenceReplica capability holder identity")
				os.Exit(1)
			}
			publisher := v1beta1inferencereplicacontroller.NewCapabilityPublisher(
				mgr.GetAPIReader(), mgr.GetClient(), mgr.GetCache(), capabilityReadiness, constants.OMENamespace, holder)
			if err = mgr.Add(publisher); err != nil {
				setupLog.Error(err, "Failed to register InferenceReplica capability publisher")
				os.Exit(1)
			}
		}
	} else if isControlPlane {
		setupLog.Info("control-plane role: InferenceReplica controller disabled")
	} else {
		setupLog.Info("InferenceReplica controller disabled via --enable-inferencereplica-controller=false")
	}

	// Multi-cluster: the WorkloadCluster registry/transport runs when multi-cluster
	// is enabled OR this is the control plane (which requires it). The control plane
	// additionally runs the placement (fan-out) controller, its GC, and the endpoint
	// publisher — see setupMultiCluster.
	if options.enableMultiCluster || isControlPlane {
		if err = setupMultiCluster(mgr, clientSet, options, isControlPlane); err != nil {
			setupLog.Error(err, "Failed to set up multi-cluster")
			os.Exit(1)
		}
	}

	// AcceleratorClass discovery stamps status from the local cluster's Node
	// inventory — meaningless on the control plane (which owns no workload
	// nodes) and its Node watch would add a cluster-wide Node informer there,
	// so it follows the same role gating as the other catalog controllers.
	if isControlPlane {
		setupLog.Info("control-plane role: AcceleratorClass controller disabled")
	} else {
		acceleratorClassEventBroadcaster := record.NewBroadcaster()
		setupLog.Info("Setting up AcceleratorClass controller")
		acceleratorClassEventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientSet.CoreV1().Events("")})
		if err = (&v1beta1acceleratorclasscontroller.AcceleratorClassReconciler{
			Client:   mgr.GetClient(),
			Log:      ctrl.Log.WithName("AcceleratorClass"),
			Scheme:   mgr.GetScheme(),
			Recorder: acceleratorClassEventBroadcaster.NewRecorder(mgr.GetScheme(), v1.EventSource{Component: "v1beta1Controllers"}),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create AcceleratorClass controller")
			os.Exit(1)
		}
	}

	if options.enableWebhook {
		setupLog.Info("Configuring webhook server", "port", options.webhookPort)
		hookServer := mgr.GetWebhookServer()

		setupLog.Info("Registering InferenceService webhook to the webhook server")
		hookServer.Register("/mutate-pods", &webhook.Admission{
			Handler: &pod.Mutator{Client: mgr.GetClient(), Clientset: clientSet, Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering cluster serving runtime validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-clusterservingruntime", &webhook.Admission{
			Handler: &servingruntime.ClusterServingRuntimeValidator{Client: mgr.GetAPIReader(), Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering serving runtime validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-servingruntime", &webhook.Admission{
			Handler: &servingruntime.ServingRuntimeValidator{Client: mgr.GetAPIReader(), Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering serving runtime engine-preset mutator webhooks to the webhook server")
		hookServer.Register("/mutate-ome-io-v1beta1-servingruntime-preset", &webhook.Admission{
			Handler: &runtimepreset.ServingRuntimeMutator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})
		hookServer.Register("/mutate-ome-io-v1beta1-clusterservingruntime-preset", &webhook.Admission{
			Handler: &runtimepreset.ClusterServingRuntimeMutator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering ControllerRevision immutability validator webhook to the webhook server")
		hookServer.Register("/validate-apps-v1-controllerrevision", &webhook.Admission{
			Handler: &runtimerevisionwebhook.ImmutabilityValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering BaseModel validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-basemodel", &webhook.Admission{
			Handler: &basemodelwebhook.BaseModelValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering ClusterBaseModel validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-clusterbasemodel", &webhook.Admission{
			Handler: &basemodelwebhook.ClusterBaseModelValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		setupLog.Info("Registering InferenceReplica validator webhook to the webhook server")
		hookServer.Register("/validate-ome-io-v1beta1-inferencereplica", &webhook.Admission{
			Handler: &inferencereplicawebhook.Validator{Decoder: admission.NewDecoder(mgr.GetScheme())},
		})

		// The InferenceService defaulter/validator runtime-selects
		// against the LOCAL cluster's runtime/model catalog. On the control
		// plane that catalog is empty (runtimes/models live on the workload
		// clusters), so running this webhook here would reject or mis-default
		// every user-authored ISVC. The placement controller derives and fans
		// the ISVC out to workload clusters, where their own webhook validates
		// it against the real catalog. Skip the ISVC webhook on the control plane.
		if isControlPlane {
			setupLog.Info("control-plane role: InferenceService defaulter/validator webhook disabled (runtime selection runs on workload clusters)")
		} else {
			runtimeSelector := runtimeselector.New(mgr.GetClient())

			if err = ctrl.NewWebhookManagedBy(mgr).
				For(&v1beta1.InferenceService{}).
				WithDefaulter(&isvc.InferenceServiceDefaulter{
					Client:    mgr.GetClient(),
					ClientSet: clientSet,
				}).
				WithValidator(&isvc.InferenceServiceValidator{
					Client:          mgr.GetClient(),
					RuntimeSelector: runtimeSelector,
					// Reject ome.io/btp.* (Envoy Gateway) when the active
					// translator is Istio or Noop, and vice versa, at
					// admission. Sourced from the same translator the
					// reconciler uses so the two stay in sync.
					KnownPassthroughPrefixes: trafficTranslator.SupportedPassthroughPrefixes(),
					// Same sourcing for typed spec.traffic fields: the
					// capability set gates the reserved Metadata endpoint
					// override and multi-header hashing at admission.
					SupportedTrafficFields: sets.List(trafficTranslator.SupportedTrafficFields()),
				}).
				Complete(); err != nil {
				setupLog.Error(err, "Failed to create InferenceService webhook", "webhook", "v1beta1")
				os.Exit(1)
			}
		}
	}

	probeChecker := managerProbeChecker(options.enableWebhook, mgr.GetWebhookServer)
	if err := mgr.AddHealthzCheck("healthz", probeChecker); err != nil {
		setupLog.Error(err, "Unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", probeChecker); err != nil {
		setupLog.Error(err, "Unable to set up ready check")
		os.Exit(1)
	}

	// Start the Cmd
	setupLog.Info("Starting manager")
	managerCtx := signals.SetupSignalHandler()
	if err := mgr.Start(managerCtx); err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}
}
