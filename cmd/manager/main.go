package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	zaplog "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	autoscalerpolicycontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/autoscalerpolicy"
	v1beta1basemodelcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel"
	v1beta1benchmarkjobcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/benchmark"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	v1beta1inferencereplicacontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/inferencereplica"
	v1beta1isvccontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	trafficfactory "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic/factory"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/crdskew"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/schemapreflight"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/managercache"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	rolloutpolicycontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/rolloutpolicy"
	v1beta1runtimerevisioncontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/runtimerevision"
	v1beta1servingruntimecontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/servingruntime"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/leaderelection"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/tpuslice/gke"
	"sigs.k8s.io/ome/pkg/utils"
	"sigs.k8s.io/ome/pkg/version"
	autoscalerpolicywebhook "sigs.k8s.io/ome/pkg/webhook/admission/autoscalerpolicy"
	basemodelwebhook "sigs.k8s.io/ome/pkg/webhook/admission/basemodel"
	"sigs.k8s.io/ome/pkg/webhook/admission/benchmark"
	inferencereplicawebhook "sigs.k8s.io/ome/pkg/webhook/admission/inferencereplica"
	"sigs.k8s.io/ome/pkg/webhook/admission/isvc"
	"sigs.k8s.io/ome/pkg/webhook/admission/pod"
	rolloutpolicywebhook "sigs.k8s.io/ome/pkg/webhook/admission/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/webhook/admission/runtimepreset"
	runtimerevisionwebhook "sigs.k8s.io/ome/pkg/webhook/admission/runtimerevision"
	"sigs.k8s.io/ome/pkg/webhook/admission/servingruntime"
	"sigs.k8s.io/ome/pkg/webhook/admission/splitplan"
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

// newEventBroadcaster builds a broadcaster whose spam filter grants every
// object the configured event budget; unconfigured settings leave client-go's
// own defaults in place.
func newEventBroadcaster(settings controllerconfig.EventRecorderSettings) record.EventBroadcaster {
	return record.NewBroadcasterWithCorrelatorOptions(settings.CorrelatorOptions())
}

func managerProbeChecker(enableWebhook bool, webhookServer func() webhook.Server) healthz.Checker {
	if !enableWebhook {
		return healthz.Ping
	}
	return webhookServer().StartedChecker()
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
	webhookOnly             bool
	webhookCertDir          string
	metricsAddr             string
	secureMetrics           bool
	enableHTTP2             bool
	webhookPort             int
	enableLeaderElection    bool
	enableWebhook           bool
	probeAddr               string
	leaderElectionNamespace string
	// pprofAddr serves net/http/pprof for profiling a live manager. Empty
	// disables it; the chart supplies the address.
	pprofAddr string
	// leaderElectionReleaseOnCancel gives up the leader lease at the end of a
	// graceful stop, so a standby takes over without waiting for it to expire.
	leaderElectionReleaseOnCancel bool
	// Timing of the leader election loop. Supplied by the chart, because the
	// margin a leader needs depends on the apiserver latency of the cluster.
	leaderElectionTiming        leaderelection.Timing
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
	// quotaAcceleratorResources names the resources whose presence puts a
	// workload under the quota backend. Deploy-time rather than a ConfigMap
	// key, and distinct from the inferenceservice-config key of a similar
	// name, which sizes PARALLELISM_SIZE.
	quotaAcceleratorResources string
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
	opts.leaderElectionTiming.BindFlags(flag.CommandLine)
	leaderelection.BindReleaseOnCancel(flag.CommandLine, &opts.leaderElectionReleaseOnCancel)
	flag.BoolVar(&opts.enableWebhook, "webhook", opts.enableWebhook, "Enable the webhook server.")
	flag.BoolVar(&opts.webhookOnly, "webhook-only", opts.webhookOnly, "Serve admission webhooks without registering controllers. Requires --webhook and disabled leader election.")
	flag.StringVar(&opts.webhookCertDir, "webhook-cert-dir", opts.webhookCertDir, "Directory containing webhook tls.crt and tls.key; empty uses controller-runtime's certificate directory.")
	flag.StringVar(&opts.probeAddr, "health-probe-addr", opts.probeAddr, "The address the probe endpoint binds to.")
	flag.StringVar(&opts.pprofAddr, "pprof-bind-address", opts.pprofAddr, "The address the pprof endpoint binds to, "+
		"for example 127.0.0.1:6060 to reach it only through kubectl port-forward. Empty or 0 disables it.")
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
	flag.StringVar(&opts.quotaAcceleratorResources, "accelerator-resources", opts.quotaAcceleratorResources,
		"Comma-separated resource names that put a workload under the quota backend, in full, e.g. "+
			"\"google.com/tpu,nvidia.com/gpu\". Only a Component whose pods request one of them carries "+
			"the Kueue queue-name label, so a cpu-only router is never gated. A vendor left off runs "+
			"ungated; keep this in step with ome-quota-manager's flag of the same name. Empty (default) "+
			"governs every Component.")
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

// main parses the flags, sets the logger and runs start-up. A step that
// stops start-up has logged why; the process then exits with code 1.
func main() {
	options := GetOptions()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options.zapOpts)))

	setupLog.Info("Initializing", "gitVersion", version.GitVersion, "gitCommit", version.GitCommit)

	if !(&startup{options: options}).run() {
		os.Exit(1)
	}
}

// startup is one start of the manager: the flags, what start-up read from
// the cluster and the operator configuration, and what it has built so far.
// The steps share the record; a step that stops start-up has logged why.
type startup struct {
	options Options

	cfg       *rest.Config
	clientSet kubernetes.Interface
	// tlsOptions apply to the metrics and webhook servers.
	tlsOptions []func(*tls.Config)
	mgr        manager.Manager

	// omenativeStatusConfig says how InferenceReplica status is stored and how
	// many Instances a reader decodes; instanceStatusDecoder is the one decoder
	// every reader in the process shares.
	omenativeStatusConfig *controllerconfig.OMENativeStatusConfig
	instanceStatusDecoder irstatus.Decoder
	// readySinceForm is where every InferenceReplica status write stores the
	// time each Instance became Ready, as the served schema allows: the
	// readySince columns, or each Instance's entry when the installed CRD
	// lacks the columns.
	readySinceForm irstatus.ReadySinceForm
	// servedDocument is the OpenAPI v3 document the apiserver serves for the
	// manager's API group, read once for the schema preflight and the CRD
	// comparison. shippedSchema is the schema this release ships and
	// crdSchema the picture of the installed CRDs against it, which the
	// service status writer and the CRD watch share.
	servedDocument []byte
	shippedSchema  crdskew.Shipped
	crdSchema      *crdskew.Report
	// eventRecorderSettings is the per-object event budget every recorder in
	// the process shares; eventBroadcaster records for the InferenceService and
	// InferenceReplica controllers.
	eventRecorderSettings controllerconfig.EventRecorderSettings
	eventBroadcaster      record.EventBroadcaster

	deployConfig   *controllerconfig.DeployConfig
	podBatchSizes  controllerconfig.PodBatchSizes
	ingressConfig  *controllerconfig.IngressConfig
	omeAgentConfig *controllerconfig.OmeAgentConfig
	// tpuSliceConfig is nil when nothing in this process provisions or checks
	// TPU slices.
	tpuSliceConfig *controllerconfig.TPUSliceProvisioningConfig

	trafficTranslator traffic.Translator
	trafficReconciler *traffic.Reconciler
	runtimeSelector   runtimeselector.Selector

	// isControlPlane is the --multicluster-role control-plane role: the
	// placement controller owns InferenceServices and clones them onto the
	// workload clusters, so no local InferenceService-to-pods controller runs.
	isControlPlane bool
	// autoscalerPolicyFound and rolloutPolicyFound say whether the chart
	// installed each policy CRD; the probe is the one source of truth for
	// whether the feature exists on this cluster.
	autoscalerPolicyFound bool
	rolloutPolicyFound    bool
	// rolloutMaxPlanBytes caps an inline plan or policy body; 0 is uncapped.
	rolloutMaxPlanBytes int
	// defaultRatioTolerancePercent is the tolerance a group that omits its own
	// rolls with; nil when the operator configured none.
	defaultRatioTolerancePercent *int32
}

// startupStep is one step of start-up. It reports whether start-up goes on;
// a step that stops it has logged why.
type startupStep func() bool

// run takes start-up through its steps in order and stops at the first one
// that does not go on. The list is the whole order of start-up: what is read
// and checked, what is built, which controllers and webhooks register, then
// the manager runs until it is told to stop.
func (s *startup) run() bool {
	if s.options.webhookOnly {
		return runStartupSteps(s.webhookOnlySteps())
	}
	return runStartupSteps([]startupStep{
		s.validateLeaderElectionTiming,
		s.configureAPIClient,
		s.createClientset,
		s.configureInstanceStatusDecoding,
		s.compareInstalledCRDSchemas,
		s.disableHTTP2UnlessEnabled,
		s.createManager,
		s.registerPodIndex,
		s.loadDeployConfig,
		s.loadLifecycleScaleConfig,
		s.loadIngressConfig,
		s.loadOmeAgentConfig,
		s.registerOptionalSchemes,
		s.selectTrafficTranslator,
		s.resolveMultiClusterRole,
		s.probePolicyCRDs,
		s.loadRolloutPlanCap,
		s.loadRatioToleranceDefault,
		s.loadTPUSliceConfig,
		s.startEventBroadcaster,
		s.setupInferenceServiceController,
		s.followCRDSchemaChanges,
		s.addControllerIdentityCheck,
		s.setupPolicyStatusControllers,
		s.setupBaseModelControllers,
		s.setupBenchmarkJobController,
		s.setupServingRuntimeControllers,
		s.buildRuntimeSelector,
		s.setupInferenceReplicaController,
		s.setupMultiClusterControllers,
		s.setupAcceleratorClassController,
		s.registerPodAndRuntimeWebhooks,
		s.registerBenchmarkJobWebhook,
		s.registerModelWebhooks,
		s.registerInferenceReplicaWebhook,
		s.registerPolicyWebhooks,
		s.registerInferenceServiceWebhook,
		s.addHealthChecks,
		s.startManager,
	})
}

// webhookOnlySteps retain admission configuration and handlers without
// registering reconcilers or initializing controller publication clients.
func (s *startup) webhookOnlySteps() []startupStep {
	return []startupStep{
		s.validateWebhookOnlyOptions,
		s.configureAPIClient, s.createClientset, s.configureInstanceStatusDecoding,
		s.disableHTTP2UnlessEnabled, s.createManager, s.registerPodIndex,
		s.loadDeployConfig, s.loadIngressConfig,
		s.registerOptionalSchemes, s.selectTrafficTranslator, s.resolveMultiClusterRole,
		s.probePolicyCRDs, s.loadRolloutPlanCap, s.loadRatioToleranceDefault,
		s.loadTPUSliceConfig, s.buildRuntimeSelector,
		s.registerPodAndRuntimeWebhooks, s.registerModelWebhooks,
		s.registerInferenceReplicaWebhook, s.registerPolicyWebhooks,
		s.registerInferenceServiceWebhook, s.addHealthChecks, s.startManager,
	}
}

func (s *startup) validateWebhookOnlyOptions() bool {
	if !s.options.enableWebhook || s.options.enableLeaderElection {
		return stop(fmt.Errorf("webhook-only mode requires --webhook and disabled leader election"), "Invalid webhook-only options")
	}
	return true
}

// runStartupSteps runs the steps in order and reports whether every one let
// start-up go on. It stops at the first step that does not.
func runStartupSteps(steps []startupStep) bool {
	for _, step := range steps {
		if !step() {
			return false
		}
	}
	return true
}

// stop logs why start-up stops and reports that it does not go on. The log
// line names the step that called it.
func stop(err error, msg string, keysAndValues ...any) bool {
	setupLog.WithCallDepth(1).Error(err, msg, keysAndValues...)
	return false
}

// newRecordingBroadcaster builds a broadcaster on the shared event budget
// that records to the cluster.
func (s *startup) newRecordingBroadcaster() record.EventBroadcaster {
	broadcaster := newEventBroadcaster(s.eventRecorderSettings)
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: s.clientSet.CoreV1().Events("")})
	return broadcaster
}

// controllerRecorder is a recorder on broadcaster for one controller; every
// recorder in the manager reports as the same source component.
func (s *startup) controllerRecorder(broadcaster record.EventBroadcaster) record.EventRecorder {
	return broadcaster.NewRecorder(s.mgr.GetScheme(), v1.EventSource{Component: "v1beta1Controllers"})
}

// admissionDecoder decodes admission requests against the manager's scheme.
func (s *startup) admissionDecoder() admission.Decoder {
	return admission.NewDecoder(s.mgr.GetScheme())
}

// validateLeaderElectionTiming refuses a leader election the elector would
// reject, or one under which a stopping leader's release could let two
// managers act at once (leaderelection.Settings).
func (s *startup) validateLeaderElectionTiming() bool {
	settings := leaderelection.Settings{
		Enabled:         s.options.enableLeaderElection,
		ReleaseOnCancel: s.options.leaderElectionReleaseOnCancel,
		Timing:          s.options.leaderElectionTiming,
	}
	if err := settings.Validate(); err != nil {
		return stop(err, "Invalid leader election timing")
	}
	return true
}

// configureAPIClient builds the REST config for the local apiserver with the
// configured rate limits. The limits go on before the clientset, the
// discovery probes and the manager copy the config.
func (s *startup) configureAPIClient() bool {
	setupLog.Info("Configuring API client connection")
	s.cfg = ctrl.GetConfigOrDie()
	applyKubeAPIRateLimits(s.cfg, s.options.kubeAPIQPS, s.options.kubeAPIBurst)
	setupLog.Info("Configured API client rate limits", "qps", s.cfg.QPS, "burst", s.cfg.Burst)
	return true
}

// createClientset builds the clientset that reads the operator configuration
// and records events.
func (s *startup) createClientset() bool {
	setupLog.Info("Creating Kubernetes client set")
	clientSet, err := kubernetes.NewForConfig(s.cfg)
	if err != nil {
		return stop(err, "Failed to create Kubernetes client set")
	}
	s.clientSet = clientSet
	return true
}

// configureInstanceStatusDecoding reads how InferenceReplica status is
// stored, refuses a cluster whose InferenceReplica CRD cannot store or
// validate the columnar form, and builds the one decoder every reader
// shares. The bound is read once, before the manager exists, so every reader
// decodes under the same bound for the life of the process. A CRD that
// lacks only the readySince columns does not stop the manager: it starts,
// says so in the log and the gauge, and stores each Instance's readySince
// in its entry until a start finds the columns.
func (s *startup) configureInstanceStatusDecoding() bool {
	config, err := controllerconfig.NewOMENativeStatusConfig(s.clientSet)
	if err != nil {
		return stop(err, "Failed to initialize OMENative status configuration")
	}
	document, err := schemapreflight.Document(s.clientSet.Discovery().OpenAPIV3())
	if err != nil {
		return stop(err, "InferenceReplica status schema preflight failed")
	}
	capabilities, err := schemapreflight.VerifyDocument(document)
	if err != nil {
		return stop(err, "InferenceReplica status schema preflight failed")
	}
	s.servedDocument = document
	s.omenativeStatusConfig = config
	s.instanceStatusDecoder = irstatus.NewDecoder(config.DecodeBound())
	s.readySinceForm = capabilities.ReadySinceForm()
	if missing := capabilities.MissingReadySinceColumns; len(missing) > 0 {
		obsmetrics.SetIRStatusReadySinceColumnsMissing(len(missing))
		setupLog.Info("The installed InferenceReplica CRD lacks readySince columns; every status write stores each Instance's Ready time in its own entry, the larger form, until a manager start finds the columns",
			"missingColumns", missing)
	}
	setupLog.Info("Configured InferenceReplica status representation",
		"instanceStatusEncoding", config.InstanceStatusEncoding,
		"maxDecodedInstances", config.DecodeBound(),
		"readySinceForm", s.readySinceForm.String())
	return true
}

// compareInstalledCRDSchemas compares the served schema of every kind in the
// manager's API group with the schema this release ships and keeps the
// picture the service status writer and the CRD watch share. A CRD that
// lacks a field this release knows is named here in the log and, once the
// manager runs, in an event. It does not stop start-up: the manager serves
// with the fields the CRD has, and the operator is told which are dropped.
func (s *startup) compareInstalledCRDSchemas() bool {
	s.crdSchema = &crdskew.Report{}
	shipped, err := crdskew.ShippedSchemas()
	if err != nil {
		setupLog.Error(err, "The installed CRD schemas were not compared with this release's")
		return true
	}
	s.shippedSchema = shipped
	resources, err := utils.GetAvailableResourcesForApi(s.cfg, v1beta1.SchemeGroupVersion.String())
	if err != nil {
		setupLog.Error(err, "The installed CRD schemas were not compared with this release's")
		return true
	}
	if resources == nil {
		setupLog.Info("The installed CRD schemas were not compared with this release's: discovery does not list the manager's API group")
		return true
	}
	findings, err := shipped.CompareServed(s.servedDocument, resources.APIResources)
	if err != nil {
		setupLog.Error(err, "The installed CRD schemas were not compared with this release's")
		return true
	}
	lagging := 0
	for _, finding := range findings {
		s.crdSchema.Set(finding)
		if finding.Lagging() {
			lagging++
			setupLog.Info(finding.Sentence(), "crd", finding.CRD, "missingFields", finding.MissingFields)
		}
	}
	setupLog.Info("Compared the installed CRD schemas with this release's", "kinds", len(findings), "lagging", lagging)
	return true
}

// disableHTTP2UnlessEnabled serves the metrics and webhook endpoints over
// HTTP/1.1 unless --enable-http2 is set, so the servers are not open to the
// HTTP/2 stream cancellation and rapid reset attacks.
func (s *startup) disableHTTP2UnlessEnabled() bool {
	if s.options.enableHTTP2 {
		return true
	}
	s.tlsOptions = append(s.tlsOptions, func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	})
	return true
}

// createManager builds the controller manager. Its cache has the shape
// managercache.Options describes, the one the integration suites build their
// managers from too; its leader lock is LeaderLockName, and the recorders it
// hands out share the per-object event budget, which is read here so every
// recorder built later shares it too.
func (s *startup) createManager() bool {
	setupLog.Info("Initializing controller manager",
		"metricsAddr", s.options.metricsAddr,
		"pprofAddr", s.options.pprofAddr,
		"webhookPort", s.options.webhookPort,
		"leaderElection", s.options.enableLeaderElection, "webhookOnly", s.options.webhookOnly)
	cacheOptions, err := managercache.Options()
	if err != nil {
		return stop(err, "Failed to build OME pod cache selector")
	}
	if s.eventRecorderSettings, err = controllerconfig.LoadEventRecorderSettings(s.clientSet); err != nil {
		return stop(err, "Failed to initialize event recorder configuration")
	}
	mgr, err := manager.New(s.cfg, s.managerOptions(cacheOptions))
	if err != nil {
		return stop(err, "Failed to initialize controller manager")
	}
	s.mgr = mgr
	return true
}

// managerOptions is the manager's configuration around cacheOptions, the
// cache shape of managercache.Options.
func (s *startup) managerOptions(cacheOptions cache.Options) manager.Options {
	opts := manager.Options{
		Scheme: scheme,
		Cache:  cacheOptions,
		Metrics: metricsserver.Options{
			BindAddress:   s.options.metricsAddr,
			TLSOpts:       s.tlsOptions,
			SecureServing: s.options.secureMetrics,
		},
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    s.options.webhookPort,
			CertDir: s.options.webhookCertDir,
			TLSOpts: s.tlsOptions,
		}),
		LeaderElection:                s.options.enableLeaderElection,
		LeaderElectionID:              LeaderLockName,
		LeaderElectionNamespace:       s.options.leaderElectionNamespace,
		LeaderElectionReleaseOnCancel: s.options.leaderElectionReleaseOnCancel,
		HealthProbeBindAddress:        s.options.probeAddr,
		PprofBindAddress:              s.options.pprofAddr,
		EventBroadcaster:              newEventBroadcaster(s.eventRecorderSettings), //nolint:staticcheck // the manager-owned recorders must share the configured per-object event budget
	}
	s.options.leaderElectionTiming.Apply(&opts)
	return opts
}

// registerPodIndex adds the field index through which an Instance's pods are
// found in the cache without a scan of every cached pod.
func (s *startup) registerPodIndex() bool {
	if err := query.RegisterOMENativePodIndex(context.Background(), s.mgr.GetFieldIndexer()); err != nil {
		return stop(err, "Failed to register OMENative pod field index")
	}
	return true
}

// loadDeployConfig validates the deploy block at start-up. The
// InferenceService controller re-reads the block on every pass; the admission
// webhook keeps this copy's replica default, so a change to it lands on the
// next restart.
func (s *startup) loadDeployConfig() bool {
	config, err := controllerconfig.NewDeployConfig(s.clientSet)
	if err != nil {
		return stop(err, "Failed to initialize deployment configuration")
	}
	s.deployConfig = config
	return true
}

// loadLifecycleScaleConfig reads the pod batch sizes the InferenceReplica
// controller scales and repairs with.
func (s *startup) loadLifecycleScaleConfig() bool {
	sizes, err := loadPodBatchSizes(s.clientSet)
	if err != nil {
		return stop(err, "Failed to initialize lifecycle scale configuration")
	}
	s.podBatchSizes = sizes
	return true
}

// loadIngressConfig reads the ingress block, which decides whether the
// Gateway API types register.
func (s *startup) loadIngressConfig() bool {
	config, err := controllerconfig.NewIngressConfig(s.clientSet)
	if err != nil {
		return stop(err, "Failed to initialize ingress configuration")
	}
	s.ingressConfig = config
	return true
}

// loadOmeAgentConfig reads the ome-agent block the model controllers run
// their download pods with.
func (s *startup) loadOmeAgentConfig() bool {
	config, err := controllerconfig.NewOmeAgentConfig(s.clientSet)
	if err != nil {
		return stop(err, "Failed to initialize ome-agent configuration")
	}
	s.omeAgentConfig = config
	return true
}

// registerOptionalSchemes adds the API types of the optional dependencies
// whose CRDs are installed, and the Gateway API types when the ingress
// configuration enables them.
func (s *startup) registerOptionalSchemes() bool {
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
	for _, optional := range optionalSchemes {
		if err := registerOptionalScheme(s.cfg, s.mgr.GetScheme(), optional.groupVersion, optional.kind, optional.addToScheme); err != nil {
			return stop(err, "Failed to register optional scheme",
				"groupVersion", optional.groupVersion.String(),
				"kind", optional.kind)
		}
	}
	if s.ingressConfig.EnableGatewayAPI {
		setupLog.Info("Registering Gateway API scheme")
		utilruntime.Must(gatewayapiv1.Install(s.mgr.GetScheme()))
	}
	return true
}

// selectTrafficTranslator picks the traffic translator from the installed
// backend-policy CRDs (Envoy Gateway BackendTrafficPolicy, Istio
// DestinationRule, else none), once for the life of the process, and builds
// the traffic reconciler on it.
func (s *startup) selectTrafficTranslator() bool {
	setupLog.Info("Selecting traffic translator")
	translator, err := trafficfactory.New(s.cfg, s.ingressConfig.ClusterDomain)
	if err != nil {
		return stop(err, "Failed to select traffic translator")
	}
	setupLog.Info("Selected traffic translator", "translator", translator.Name())
	s.trafficTranslator = translator
	s.trafficReconciler = traffic.NewReconciler(s.mgr.GetClient(), s.mgr.GetScheme(), translator)
	return true
}

// resolveMultiClusterRole reads --multicluster-role: control-plane, or empty
// for a single-cluster or workload-cluster manager. Any other value stops
// start-up.
func (s *startup) resolveMultiClusterRole() bool {
	// The broadcaster this line announces is built once the role, the policy
	// probes and the start-up blocks below are known; the line keeps its
	// place in the start-up log.
	setupLog.Info("Configuring event broadcaster")
	s.isControlPlane = s.options.multiClusterRole == multiClusterRoleControlPlane
	if s.options.multiClusterRole != "" && !s.isControlPlane {
		return stop(fmt.Errorf("invalid --multicluster-role %q (want %q or empty)", s.options.multiClusterRole, multiClusterRoleControlPlane), "bad flag")
	}
	return true
}

// probePolicyCRDs asks the apiserver whether the chart installed the
// AutoscalerPolicy and RolloutPolicy CRDs. Each probe is the one source of
// truth for its feature: the status controller, the reference check and the
// webhook of a policy kind all follow it. Recording a rollout's plan is not
// gated on the RolloutPolicy probe.
func (s *startup) probePolicyCRDs() bool {
	var err error
	if s.autoscalerPolicyFound, err = utils.IsCrdAvailable(s.cfg, v1beta1.SchemeGroupVersion.String(), constants.AutoscalerPolicyKind); err != nil {
		return stop(err, "Failed to probe for the AutoscalerPolicy CRD")
	}
	if s.rolloutPolicyFound, err = utils.IsCrdAvailable(s.cfg, v1beta1.SchemeGroupVersion.String(), constants.RolloutPolicyKind); err != nil {
		return stop(err, "Failed to probe for the RolloutPolicy CRD")
	}
	return true
}

// loadRolloutPlanCap reads the size cap of an inline plan or policy body
// once; a change lands on the next restart. Absent or unreadable config
// leaves the cap at 0, uncapped, never an in-code number.
func (s *startup) loadRolloutPlanCap() bool {
	config, err := controllerconfig.NewRolloutConfig(s.clientSet)
	if err != nil {
		setupLog.Error(err, "Failed to load the rollout operator config; proceeding with no plan-size cap")
		return true
	}
	s.rolloutMaxPlanBytes = config.MaxPinnedPlanBytes
	return true
}

// loadRatioToleranceDefault reads, once, the ratio tolerance a group that
// omits its own rolls with, for the admission drain-feasibility rule. Absent
// or unreadable config leaves it nil: such a group rolls with no drift bound
// and admission does not check it.
func (s *startup) loadRatioToleranceDefault() bool {
	config, err := controllerconfig.NewCoordinationConfig(s.clientSet)
	if err != nil {
		setupLog.Error(err, "Failed to load the coordination operator config; admission checks only explicit ratio tolerances")
		return true
	}
	s.defaultRatioTolerancePercent = config.DefaultRatioTolerancePercent
	return true
}

// loadTPUSliceConfig reads the TPU slice provisioning block when something in
// this process uses it: the InferenceReplica controller provisions slices
// from it and the InferenceReplica webhook checks slice demand against it.
// The control plane runs neither.
func (s *startup) loadTPUSliceConfig() bool {
	if s.isControlPlane || !(s.options.enableInferenceReplicaCtrl || s.options.enableWebhook) {
		return true
	}
	config, err := controllerconfig.NewTPUSliceProvisioningConfig(s.clientSet)
	if err != nil {
		return stop(err, "Failed to initialize TPU slice provisioning configuration")
	}
	s.tpuSliceConfig = config
	return true
}

// startEventBroadcaster builds the broadcaster the InferenceService and
// InferenceReplica controllers record through and starts it recording to the
// cluster.
func (s *startup) startEventBroadcaster() bool {
	s.eventBroadcaster = s.newRecordingBroadcaster()
	return true
}

// setupInferenceServiceController registers the InferenceService controller.
// The control plane does not run it: there the placement controller owns
// InferenceServices and clones them onto the workload clusters.
func (s *startup) setupInferenceServiceController() bool {
	if s.isControlPlane {
		setupLog.Info("control-plane role: local InferenceService reconciler disabled; placement controller owns ISVCs")
		return true
	}
	setupLog.Info("Setting up InferenceService controller")
	// Logged because the consequence is invisible in the objects: a Component
	// requesting none of these resources is projected with no queue-name
	// label, so nothing downstream records that it was released.
	quotaAcceleratorResources := splitAndTrim(s.options.quotaAcceleratorResources)
	if len(quotaAcceleratorResources) > 0 {
		setupLog.Info("Quota backend governs workloads requesting these resources",
			"quotaAcceleratorResources", quotaAcceleratorResources)
	}
	err := (&v1beta1isvccontroller.InferenceServiceReconciler{
		Client:                    s.mgr.GetClient(),
		Clientset:                 s.clientSet,
		Log:                       ctrl.Log.WithName("InferenceService"),
		Scheme:                    s.mgr.GetScheme(),
		Recorder:                  s.controllerRecorder(s.eventBroadcaster),
		InstanceStatusDecoder:     s.instanceStatusDecoder,
		CRDSchema:                 s.crdSchema,
		TrafficReconciler:         s.trafficReconciler,
		MaxConcurrentReconciles:   s.options.isvcMaxConcurrentReconciles,
		ConfigCacheTTL:            s.options.configCacheTTL,
		QuotaAcceleratorResources: quotaAcceleratorResources,
	}).SetupWithManager(s.mgr)
	if err != nil {
		return stop(err, "Failed to create InferenceService controller")
	}
	return true
}

// followCRDSchemaChanges adds the runnable that reports the start-up
// picture of the installed CRDs as events once the manager runs and keeps
// it current as CRDs change. The control plane writes the same objects, so
// it follows too.
func (s *startup) followCRDSchemaChanges() bool {
	if s.shippedSchema == nil {
		return true
	}
	crds, err := apiextensionsclient.NewForConfig(s.cfg)
	if err != nil {
		return stop(err, "Failed to create the CustomResourceDefinition client")
	}
	watcher := &crdskew.Watcher{
		Shipped:   s.shippedSchema,
		Report:    s.crdSchema,
		CRDs:      crds.ApiextensionsV1().CustomResourceDefinitions(),
		Informers: s.mgr.GetCache(),
		Log:       ctrl.Log.WithName("CRDSchema"),
		Changed:   s.tellCRDSchemaFinding(s.controllerRecorder(s.eventBroadcaster)),
	}
	if err := s.mgr.Add(watcher); err != nil {
		return stop(err, "Failed to register the CRD schema watch")
	}
	return true
}

// tellCRDSchemaFinding logs a CRD's picture and records it as an event
// on the manager Pod named after this host, where an operator looks first.
// A process that is not such a Pod, a local run, has the log line alone.
func (s *startup) tellCRDSchemaFinding(recorder record.EventRecorder) func(crdskew.Finding) {
	log := ctrl.Log.WithName("CRDSchema")
	var (
		lookup sync.Once
		pod    *v1.Pod
	)
	return func(finding crdskew.Finding) {
		lookup.Do(func() {
			hostname, err := os.Hostname()
			if err != nil {
				return
			}
			found, err := s.clientSet.CoreV1().Pods(constants.OMENamespace).Get(context.Background(), hostname, metav1.GetOptions{})
			if err != nil {
				log.Info("No manager Pod is named after this host; CRD schema findings stay in the log", "namespace", constants.OMENamespace, "host", hostname)
				return
			}
			pod = found
		})
		log.Info(finding.Sentence(), "crd", finding.CRD, "missingFields", finding.MissingFields)
		if pod == nil {
			return
		}
		if finding.Lagging() {
			recorder.Event(pod, v1.EventTypeWarning, crdskew.ReasonLagging, finding.Sentence())
			return
		}
		recorder.Event(pod, v1.EventTypeNormal, crdskew.ReasonCurrent, finding.Sentence())
	}
}

// addControllerIdentityCheck adds the runnable that, once the manager runs
// and on the leader that projects, reports whether this manager is the
// controller identity the InferenceReplica webhook admits projections from.
// A mismatch, or an identity it cannot verify, is reported and does not stop
// start-up. The control plane projects nothing locally and skips it.
func (s *startup) addControllerIdentityCheck() bool {
	if s.isControlPlane {
		return true
	}
	if err := s.mgr.Add(manager.RunnableFunc(s.reportControllerIdentity)); err != nil {
		return stop(err, "Failed to register the InferenceReplica controller identity check")
	}
	return true
}

// reportControllerIdentity logs the InferenceReplica admission mode and
// whether this manager is the configured controller identity.
func (s *startup) reportControllerIdentity(ctx context.Context) error {
	mode, err := controllerconfig.CheckControllerIdentity(ctx, s.clientSet)
	switch {
	case err != nil && mode == "":
		setupLog.Error(err, "InferenceReplica admission: could not load the controller identity")
	case errors.Is(err, controllerconfig.ErrControllerIdentityUnverified):
		setupLog.Error(err, "InferenceReplica admission: could not verify this manager's identity", "mode", mode)
	case err != nil:
		setupLog.Error(err, "InferenceReplica admission: this manager is not the configured controller identity; its InferenceReplica projections will be denied until inferenceReplica.controllerIdentity names it", "mode", mode)
	default:
		setupLog.Info("InferenceReplica admission mode", "mode", mode)
	}
	return nil
}

// setupPolicyStatusControllers registers the AutoscalerPolicy and
// RolloutPolicy status controllers, each only when its CRD is installed. The
// control plane runs neither.
func (s *startup) setupPolicyStatusControllers() bool {
	if s.isControlPlane {
		return true
	}
	if s.autoscalerPolicyFound {
		setupLog.Info("Setting up AutoscalerPolicy status controller")
		if err := autoscalerpolicycontroller.SetupWithManager(s.mgr, s.clientSet, controllerconfig.NewConfigCache(s.options.configCacheTTL)); err != nil {
			return stop(err, "Failed to create AutoscalerPolicy status controller")
		}
	}
	if s.rolloutPolicyFound {
		setupLog.Info("Setting up RolloutPolicy status controller")
		if err := rolloutpolicycontroller.SetupWithManager(s.mgr, s.clientSet, controllerconfig.NewConfigCache(s.options.configCacheTTL)); err != nil {
			return stop(err, "Failed to create RolloutPolicy status controller")
		}
	}
	return true
}

// setupBaseModelControllers registers the BaseModel and ClusterBaseModel
// controllers. The control plane owns no models or runtimes, those live on
// the workload clusters, so it runs none of the four catalog controllers:
// these two, ServingRuntime inheritance and runtime-revision GC.
func (s *startup) setupBaseModelControllers() bool {
	if s.isControlPlane {
		setupLog.Info("control-plane role: BaseModel/ClusterBaseModel/ServingRuntime/RuntimeRevisionGC controllers disabled")
		return true
	}
	setupLog.Info("Setting up BaseModel controller")
	if err := (&v1beta1basemodelcontroller.BaseModelReconciler{
		Client:         s.mgr.GetClient(),
		Log:            ctrl.Log.WithName("BaseModel"),
		Scheme:         s.mgr.GetScheme(),
		OmeAgentConfig: s.omeAgentConfig,
	}).SetupWithManager(s.mgr); err != nil {
		return stop(err, "Failed to create BaseModel controller")
	}
	setupLog.Info("Setting up ClusterBaseModel controller")
	if err := (&v1beta1basemodelcontroller.ClusterBaseModelReconciler{
		Client:         s.mgr.GetClient(),
		Log:            ctrl.Log.WithName("ClusterBaseModel"),
		Scheme:         s.mgr.GetScheme(),
		OmeAgentConfig: s.omeAgentConfig,
	}).SetupWithManager(s.mgr); err != nil {
		return stop(err, "Failed to create ClusterBaseModel controller")
	}
	return true
}

// setupBenchmarkJobController registers the BenchmarkJob controller. The
// control plane owns no workloads and does not run it.
func (s *startup) setupBenchmarkJobController() bool {
	if s.isControlPlane {
		setupLog.Info("control-plane role: BenchmarkJob controller disabled")
		return true
	}
	setupLog.Info("Setting up BenchmarkJob controller")
	if err := (&v1beta1benchmarkjobcontroller.BenchmarkJobReconciler{
		Client:    s.mgr.GetClient(),
		Clientset: s.clientSet,
		Log:       ctrl.Log.WithName("BenchmarkJob"),
		Scheme:    s.mgr.GetScheme(),
		Recorder:  s.controllerRecorder(s.newRecordingBroadcaster()),
	}).SetupWithManager(s.mgr); err != nil {
		return stop(err, "Failed to create BenchmarkJob controller")
	}
	return true
}

// setupServingRuntimeControllers registers the ServingRuntime inheritance
// controller, one reconciler for ServingRuntime and ClusterServingRuntime
// with one source per kind, and the runtime-revision GC controller. The
// control plane has no local runtime catalog and runs neither.
func (s *startup) setupServingRuntimeControllers() bool {
	if s.isControlPlane {
		return true
	}
	setupLog.Info("Setting up ServingRuntime inheritance controller")
	if err := (&v1beta1servingruntimecontroller.InheritanceReconciler{
		Client:   s.mgr.GetClient(),
		Log:      ctrl.Log.WithName("ServingRuntimeInheritance"),
		Recorder: s.controllerRecorder(s.newRecordingBroadcaster()),
	}).SetupWithManager(s.mgr); err != nil {
		return stop(err, "Failed to create ServingRuntime inheritance controller")
	}
	setupLog.Info("Setting up runtime-revision GC controller",
		"retention", s.options.runtimeRevisionRetention,
		"gracePeriod", s.options.runtimeRevisionGracePeriod)
	if err := (&v1beta1runtimerevisioncontroller.GCReconciler{
		Client:              s.mgr.GetClient(),
		Log:                 ctrl.Log.WithName("RuntimeRevisionGC"),
		OMENamespace:        constants.OMENamespace,
		RetentionPerRuntime: s.options.runtimeRevisionRetention,
		GracePeriod:         s.options.runtimeRevisionGracePeriod,
	}).SetupWithManager(s.mgr); err != nil {
		return stop(err, "Failed to create runtime-revision GC controller")
	}
	return true
}

// buildRuntimeSelector builds the runtime selector the InferenceReplica
// controller renders replicas with and the InferenceService validator selects
// with.
func (s *startup) buildRuntimeSelector() bool {
	config := runtimeselector.NewConfig(s.mgr.GetClient())
	s.runtimeSelector = runtimeselector.NewWithConfig(config)
	return true
}

// setupInferenceReplicaController registers the InferenceReplica controller
// unless --enable-inferencereplica-controller=false turned it off. The
// control plane does not run it.
func (s *startup) setupInferenceReplicaController() bool {
	switch {
	case s.isControlPlane:
		setupLog.Info("control-plane role: InferenceReplica controller disabled")
		return true
	case !s.options.enableInferenceReplicaCtrl:
		setupLog.Info("InferenceReplica controller disabled via --enable-inferencereplica-controller=false")
		return true
	}
	setupLog.Info("Setting up InferenceReplica controller")
	err := (&v1beta1inferencereplicacontroller.Reconciler{
		Client:                   s.mgr.GetClient(),
		Clientset:                s.clientSet,
		Log:                      ctrl.Log.WithName("InferenceReplica"),
		APIReader:                s.mgr.GetAPIReader(),
		RuntimeSelector:          s.runtimeSelector,
		InstanceStatusDecoder:    s.instanceStatusDecoder,
		InstanceStatusTarget:     s.omenativeStatusConfig.InstanceStatusEncoding,
		ReadySinceForm:           s.readySinceForm,
		Recorder:                 s.controllerRecorder(s.eventBroadcaster),
		MaxConcurrentReconciles:  s.options.irMaxConcurrentReconciles,
		ConfigCacheTTL:           s.options.configCacheTTL,
		ScaleUpPodBatchSize:      s.podBatchSizes.ScaleUp,
		ScaleDownPodBatchSize:    s.podBatchSizes.ScaleDown,
		ScaleDownRequeueInterval: s.podBatchSizes.ScaleDownRequeueInterval,
		RepairBatchSize:          s.podBatchSizes.Repair,
		TPUSliceProvisioning:     s.tpuSliceConfig,
	}).SetupWithManager(s.mgr)
	if err != nil {
		return stop(err, "Failed to create InferenceReplica controller")
	}
	return true
}

// setupMultiClusterControllers wires the WorkloadCluster registry when
// multi-cluster is enabled or this is the control plane, which needs it, and
// on the control plane also placement, its GC and the endpoint publishers.
func (s *startup) setupMultiClusterControllers() bool {
	if !s.options.enableMultiCluster && !s.isControlPlane {
		return true
	}
	if err := setupMultiCluster(s.mgr, s.clientSet, s.options, s.isControlPlane, s.instanceStatusDecoder); err != nil {
		return stop(err, "Failed to set up multi-cluster")
	}
	return true
}

// setupAcceleratorClassController registers the AcceleratorClass controller,
// which reads the local Node inventory. The control plane owns no workload
// nodes, and a Node watch there would add a cluster-wide informer for
// nothing, so it does not run it.
func (s *startup) setupAcceleratorClassController() bool {
	if s.isControlPlane {
		setupLog.Info("control-plane role: AcceleratorClass controller disabled")
		return true
	}
	setupLog.Info("Setting up AcceleratorClass controller")
	if err := (&v1beta1acceleratorclasscontroller.AcceleratorClassReconciler{
		Client: s.mgr.GetClient(),
		Log:    ctrl.Log.WithName("AcceleratorClass"),
	}).SetupWithManager(s.mgr); err != nil {
		return stop(err, "Failed to create AcceleratorClass controller")
	}
	return true
}

// registerPodAndRuntimeWebhooks registers, when the webhook runs, the pod
// mutator, the ServingRuntime and ClusterServingRuntime validators, the two
// runtime-preset mutators and the ControllerRevision immutability validator.
func (s *startup) registerPodAndRuntimeWebhooks() bool {
	if !s.options.enableWebhook {
		return true
	}
	setupLog.Info("Configuring webhook server", "port", s.options.webhookPort)
	hookServer, decoder := s.mgr.GetWebhookServer(), s.admissionDecoder()

	setupLog.Info("Registering InferenceService webhook to the webhook server")
	hookServer.Register("/mutate-pods", &webhook.Admission{
		Handler: &pod.Mutator{Client: s.mgr.GetClient(), Clientset: s.clientSet, Decoder: decoder},
	})
	setupLog.Info("Registering cluster serving runtime validator webhook to the webhook server")
	hookServer.Register("/validate-ome-io-v1beta1-clusterservingruntime", &webhook.Admission{
		Handler: &servingruntime.ClusterServingRuntimeValidator{Client: s.mgr.GetAPIReader(), Decoder: decoder},
	})
	setupLog.Info("Registering serving runtime validator webhook to the webhook server")
	hookServer.Register("/validate-ome-io-v1beta1-servingruntime", &webhook.Admission{
		Handler: &servingruntime.ServingRuntimeValidator{Client: s.mgr.GetAPIReader(), Decoder: decoder},
	})
	setupLog.Info("Registering serving runtime engine-preset mutator webhooks to the webhook server")
	hookServer.Register("/mutate-ome-io-v1beta1-servingruntime-preset", &webhook.Admission{
		Handler: &runtimepreset.ServingRuntimeMutator{Decoder: decoder, Clientset: s.clientSet},
	})
	hookServer.Register("/mutate-ome-io-v1beta1-clusterservingruntime-preset", &webhook.Admission{
		Handler: &runtimepreset.ClusterServingRuntimeMutator{Decoder: decoder, Clientset: s.clientSet},
	})
	setupLog.Info("Registering ControllerRevision immutability validator webhook to the webhook server")
	hookServer.Register("/validate-apps-v1-controllerrevision", &webhook.Admission{
		Handler: &runtimerevisionwebhook.ImmutabilityValidator{Decoder: decoder},
	})
	return true
}

// registerBenchmarkJobWebhook registers, when the webhook runs, the
// BenchmarkJob validator.
func (s *startup) registerBenchmarkJobWebhook() bool {
	if !s.options.enableWebhook {
		return true
	}
	hookServer, decoder := s.mgr.GetWebhookServer(), s.admissionDecoder()
	setupLog.Info("Registering benchmark job validator webhook to the webhook server")
	hookServer.Register("/validate-ome-io-v1beta1-benchmarkjob", &webhook.Admission{
		Handler: &benchmark.BenchmarkJobValidator{Client: s.mgr.GetClient(), Decoder: decoder},
	})
	return true
}

// registerModelWebhooks registers, when the webhook runs, the BaseModel and
// ClusterBaseModel validators.
func (s *startup) registerModelWebhooks() bool {
	if !s.options.enableWebhook {
		return true
	}
	hookServer, decoder := s.mgr.GetWebhookServer(), s.admissionDecoder()
	setupLog.Info("Registering BaseModel validator webhook to the webhook server")
	hookServer.Register("/validate-ome-io-v1beta1-basemodel", &webhook.Admission{
		Handler: &basemodelwebhook.BaseModelValidator{Decoder: decoder},
	})
	setupLog.Info("Registering ClusterBaseModel validator webhook to the webhook server")
	hookServer.Register("/validate-ome-io-v1beta1-clusterbasemodel", &webhook.Admission{
		Handler: &basemodelwebhook.ClusterBaseModelValidator{Decoder: decoder},
	})
	return true
}

// registerInferenceReplicaWebhook registers, when the webhook runs, the
// InferenceReplica validator. It checks slice demand only when slices are
// provisioned: the block is configured and the Slice CRD is installed.
func (s *startup) registerInferenceReplicaWebhook() bool {
	if !s.options.enableWebhook {
		return true
	}
	setupLog.Info("Registering InferenceReplica validator webhook to the webhook server")
	validator := &inferencereplicawebhook.Validator{
		Decoder:     s.admissionDecoder(),
		Reader:      s.mgr.GetAPIReader(),
		Clientset:   s.clientSet,
		ConfigCache: controllerconfig.NewConfigCache(s.options.configCacheTTL),
	}
	if s.tpuSliceConfig != nil {
		sliceFound, err := utils.IsCrdAvailable(s.cfg, gke.GroupVersion.String(), gke.Kind)
		if err != nil {
			return stop(err, "Failed to probe for the Slice CRD")
		}
		if sliceFound {
			validator.TPUSliceProvisioning, validator.Nodes = s.tpuSliceConfig, s.mgr.GetAPIReader()
		}
	}
	s.mgr.GetWebhookServer().Register("/validate-ome-io-v1beta1-inferencereplica", &webhook.Admission{Handler: validator})
	return true
}

// registerPolicyWebhooks registers, when the webhook runs, the
// AutoscalerPolicy and RolloutPolicy validators. Each handler is inert until
// the chart installs its ValidatingWebhookConfiguration beside the CRD.
func (s *startup) registerPolicyWebhooks() bool {
	if !s.options.enableWebhook {
		return true
	}
	hookServer, decoder := s.mgr.GetWebhookServer(), s.admissionDecoder()
	setupLog.Info("Registering AutoscalerPolicy validator webhook to the webhook server")
	hookServer.Register("/validate-ome-io-v1beta1-autoscalerpolicy", &webhook.Admission{
		Handler: &autoscalerpolicywebhook.Validator{Client: s.mgr.GetClient(), Decoder: decoder},
	})
	setupLog.Info("Registering RolloutPolicy validator webhook to the webhook server")
	hookServer.Register("/validate-ome-io-v1beta1-rolloutpolicy", &webhook.Admission{
		Handler: &rolloutpolicywebhook.Validator{Client: s.mgr.GetClient(), Decoder: decoder, MaxPlanBytes: s.rolloutMaxPlanBytes},
	})
	return true
}

// registerInferenceServiceWebhook registers, when the webhook runs, the
// InferenceService validator. It selects runtimes against the local catalog,
// which is empty on the control plane, where it would refuse every service;
// the workload clusters' own webhooks validate the copies placement sends
// them, so the control plane skips it. The control plane serves the Split
// plan validator instead, since only it holds the WorkloadCluster
// registrations a Split plan divides replicas across.
func (s *startup) registerInferenceServiceWebhook() bool {
	if !s.options.enableWebhook {
		return true
	}
	if s.isControlPlane {
		setupLog.Info("control-plane role: InferenceService validator webhook disabled (runtime selection runs on workload clusters); registering the Split plan validator")
		s.mgr.GetWebhookServer().Register(splitplan.ValidatorPath,
			admission.WithValidator[*v1beta1.InferenceService](s.mgr.GetScheme(), &splitplan.Validator{Reader: s.mgr.GetAPIReader()}))
		return true
	}
	if err := ctrl.NewWebhookManagedBy(s.mgr, &v1beta1.InferenceService{}).
		WithValidator(s.inferenceServiceValidator()).
		Complete(); err != nil {
		return stop(err, "Failed to create InferenceService webhook", "webhook", "v1beta1")
	}
	return true
}

// inferenceServiceValidator is the InferenceService validator, fed the same
// facts the controllers run with, so admission checks a service as it will
// run.
func (s *startup) inferenceServiceValidator() *isvc.InferenceServiceValidator {
	return &isvc.InferenceServiceValidator{
		Client:          s.mgr.GetClient(),
		Reader:          s.mgr.GetAPIReader(),
		RuntimeSelector: s.runtimeSelector,
		// A policy reference is refused when its CRD is absent, so a reference
		// never does nothing on a cluster without the feature.
		AutoscalerPolicyEnabled: s.autoscalerPolicyFound,
		RolloutPolicyEnabled:    s.rolloutPolicyFound,
		// Inline plans and policy bodies share one size cap.
		RolloutMaxPlanBytes: s.rolloutMaxPlanBytes,
		// The tolerance the rollout resolves an omitted maintainRatio.tolerance
		// to, so the drain-feasibility rule sees the group as it will run.
		DefaultRatioTolerancePercent: s.defaultRatioTolerancePercent,
		// The replica fill the reconciler applies to a Component with no
		// minReplicas, so a member with no replica fields is sized as the
		// rollout will size it.
		DefaultMinReplicas: s.deployConfig.Replicas.Min(),
		// The strategy fill the reconciler applies to a Component with no
		// update strategy, so a member with none is read as the rollout
		// will roll it.
		DefaultUpdateStrategy: s.deployConfig.UpdateStrategy,
		// The passthrough prefixes and the typed traffic fields the active
		// translator supports, so admission refuses what the reconciler could
		// not apply.
		KnownPassthroughPrefixes: s.trafficTranslator.SupportedPassthroughPrefixes(),
		SupportedTrafficFields:   sets.List(s.trafficTranslator.SupportedTrafficFields()),
		// The route provider and its declared capabilities
		// (routeProviderCapabilities) come from the same ingress config the
		// controller renders routes from, so a canary the provider cannot
		// carry is refused here and held there under one declaration.
		IngressConfig: s.ingressConfig,
	}
}

// addHealthChecks registers the liveness and readiness checks. Both report
// the webhook server's started state when the webhook runs, else a ping, so
// a standby replica that serves the webhook reports Ready.
func (s *startup) addHealthChecks() bool {
	probeChecker := managerProbeChecker(s.options.enableWebhook, s.mgr.GetWebhookServer)
	if err := s.mgr.AddHealthzCheck("healthz", probeChecker); err != nil {
		return stop(err, "Unable to set up health check")
	}
	if err := s.mgr.AddReadyzCheck("readyz", probeChecker); err != nil {
		return stop(err, "Unable to set up ready check")
	}
	return true
}

// startManager runs the manager until it is told to stop. The process exits
// as soon as Start returns: with --leader-elect-release-on-cancel the lease
// may already be released by then, so a standby may be leading.
func (s *startup) startManager() bool {
	setupLog.Info("Starting manager")
	managerCtx := signals.SetupSignalHandler()
	if err := s.mgr.Start(managerCtx); err != nil {
		return stop(err, "Failed to start manager")
	}
	return true
}
