// Alfred is the OME GPU cluster caretaker: a leader-elected
// controller that observes the physical GPU layer and recommends corrective
// migrations. Explicit operator compatibility configuration enables guarded
// migration-request annotations handled by the workload-owning controllers.
//
// This binary wires two loops onto a controller-runtime manager:
//   - the observation loop (every replica): snapshot + gauges, read-only;
//   - the decision loop (leader only): policies → optional prediction or guarded
//     dispatch and arbitration → reporter.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/engine"
	"sigs.k8s.io/ome/pkg/alfred/guard"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/observer"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/policy/defrag"
	"sigs.k8s.io/ome/pkg/alfred/policy/nodehealth"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/process"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("alfred-setup")
)

// Leader-election parameters for leader election (mirrors
// cluster-autoscaler's pattern).
const (
	leaderElectionID = "alfred.ome.io"
	leaseDuration    = 15 * time.Second
	renewDeadline    = 10 * time.Second
	retryPeriod      = 2 * time.Second
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
}

func decisionPolicies() []policy.Policy {
	return []policy.Policy{&nodehealth.Policy{}, &defrag.Policy{}}
}

// Options holds the command-line configuration.
type Options struct {
	metricsAddr             string
	probeAddr               string
	enableLeaderElection    bool
	namespace               string
	configMapName           string
	configMapKey            string
	simulationWorkers       string
	simulationTimeout       time.Duration
	migrationAPIVersion     string
	migrationServiceAccount string
	migrationAckTimeout     time.Duration
	migrationFailureBackoff time.Duration
	zapOpts                 zap.Options
}

// DefaultOptions returns the flag defaults. The namespace defaults from the
// POD_NAMESPACE downward-API variable so the deployment needs no explicit
// flag.
func DefaultOptions() Options {
	return Options{
		metricsAddr:             ":8080",
		probeAddr:               ":8081",
		enableLeaderElection:    true,
		namespace:               constants.OMENamespace,
		configMapName:           "alfred-config",
		configMapKey:            "config.yaml",
		simulationTimeout:       10 * time.Second,
		migrationAckTimeout:     2 * time.Minute,
		migrationFailureBackoff: 5 * time.Minute,
	}
}

// GetOptions parses flags into Options.
func GetOptions() Options {
	opts := DefaultOptions()
	flag.StringVar(&opts.metricsAddr, "metrics-bind-address", opts.metricsAddr, "The address the metric endpoint binds to.")
	flag.StringVar(&opts.probeAddr, "health-probe-bind-address", opts.probeAddr, "The address the probe endpoint binds to.")
	flag.BoolVar(&opts.enableLeaderElection, "leader-elect", opts.enableLeaderElection,
		"Enable leader election. Only the leader runs the decision loop; all replicas observe.")
	flag.StringVar(&opts.namespace, "namespace", opts.namespace,
		"Namespace holding Alfred's ConfigMaps and leader-election Lease.")
	flag.StringVar(&opts.configMapName, "config-name", opts.configMapName, "Name of the Alfred configuration ConfigMap.")
	flag.StringVar(&opts.configMapKey, "config-key", opts.configMapKey, "Key inside the ConfigMap holding config.yaml.")
	flag.StringVar(&opts.simulationWorkers, "simulation-workers", "", "Absolute path to the trusted startup worker registry JSON; empty disables prediction.")
	flag.DurationVar(&opts.simulationTimeout, "simulation-timeout", opts.simulationTimeout, "Whole-process timeout per scheduler prediction (positive, at most 1m).")
	flag.StringVar(&opts.migrationAPIVersion, "migration-api-version", "", "Operator-confirmed migration API compatibility (v1); empty disables migration dispatch.")
	flag.StringVar(&opts.migrationServiceAccount, "migration-service-account", "", "Alfred service account name enforced by the migration admission guard; required with migration-api-version.")
	flag.DurationVar(&opts.migrationAckTimeout, "migration-ack-timeout", opts.migrationAckTimeout, "Timeout before marking an unacknowledged request stalled (positive, at most 1h).")
	flag.DurationVar(&opts.migrationFailureBackoff, "migration-failure-backoff", opts.migrationFailureBackoff, "Backoff after migration failure (positive, at most 1h).")
	opts.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return opts
}

func main() {
	opts := GetOptions()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts.zapOpts)))
	if err := validateOptions(opts); err != nil {
		setupLog.Error(err, "invalid startup configuration")
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), manager.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: opts.metricsAddr},
		HealthProbeBindAddress:  opts.probeAddr,
		LeaderElection:          opts.enableLeaderElection,
		LeaderElectionID:        leaderElectionID,
		LeaderElectionNamespace: opts.namespace,
		LeaseDuration:           ptr(leaseDuration),
		RenewDeadline:           ptr(renewDeadline),
		RetryPeriod:             ptr(retryPeriod),
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				// Alfred reads ConfigMaps only in its own namespace
				// (alfred-config, alfred-recommendations); do not
				// cache the rest of the cluster's ConfigMaps.
				&corev1.ConfigMap{}: {
					Namespaces: map[string]cache.Config{opts.namespace: {}},
				},
				// The pod cache is cluster-wide by design (non-OME
				// GPU occupants count against capacity); the
				// transform bounds its memory cost.
				&corev1.Pod{}: {Transform: podCacheTransform},
			},
		},
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, "spec.nodeName",
		func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}); err != nil {
		setupLog.Error(err, "unable to index pods by spec.nodeName")
		os.Exit(1)
	}

	alfredMetrics := metrics.New(nil)
	store := config.NewStore()

	watcher := &config.Watcher{
		Cache:     mgr.GetCache(),
		Namespace: opts.namespace,
		Name:      opts.configMapName,
		Key:       opts.configMapKey,
		Store:     store,
		Log:       ctrl.Log.WithName("alfred-config"),
		Recorder:  mgr.GetEventRecorderFor("alfred"),
		Observer:  alfredMetrics,
	}
	if err := mgr.Add(watcher); err != nil {
		setupLog.Error(err, "unable to add config watcher")
		os.Exit(1)
	}

	observationLoop := &observer.Loop{
		Reader:  mgr.GetClient(),
		Store:   store,
		Metrics: alfredMetrics,
		Log:     ctrl.Log.WithName("alfred-observer"),
		Scorer:  defrag.PublishScores,
	}
	if err := mgr.Add(observationLoop); err != nil {
		setupLog.Error(err, "unable to add observation loop")
		os.Exit(1)
	}

	// The leader decides and, when explicitly configured, submits guarded
	// migration requests through the existing OME migration API.
	earlyTicker := &engine.EarlyTicker{
		Cache: mgr.GetCache(),
		Store: store,
		Log:   ctrl.Log.WithName("alfred-earlytick"),
		C:     make(chan struct{}, 1),
	}
	if err := mgr.Add(earlyTicker); err != nil {
		setupLog.Error(err, "unable to add early ticker")
		os.Exit(1)
	}
	predictions, err := predictionStage(ctx, mgr.GetAPIReader(), opts)
	if err != nil {
		setupLog.Error(err, "unable to configure recommendation simulation")
		os.Exit(1)
	}
	decisionLoop := &engine.DecisionLoop{
		Snapshots:   observationLoop,
		Store:       store,
		Policies:    decisionPolicies(),
		Predictions: predictions,
		Arbiter:     &engine.Arbiter{Ledger: engine.NewLedger()},
		Reporter: &engine.Reporter{
			Client:        mgr.GetClient(),
			DirectReader:  mgr.GetAPIReader(),
			Recorder:      mgr.GetEventRecorderFor("alfred"),
			Metrics:       alfredMetrics,
			Log:           ctrl.Log.WithName("alfred-reporter"),
			Namespace:     opts.namespace,
			ConfigMapName: opts.configMapName,
		},
		Metrics:   alfredMetrics,
		Log:       ctrl.Log.WithName("alfred-decision"),
		EarlyTick: earlyTicker.C,
	}
	if err := configureMigration(opts, mgr.GetAPIReader(), mgr.GetClient(), observationLoop, decisionLoop); err != nil {
		setupLog.Error(err, "unable to configure migration dispatch")
		os.Exit(1)
	}
	if err := mgr.Add(decisionLoop); err != nil {
		setupLog.Error(err, "unable to add decision loop")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// alfred_leader_status: 0 on every replica until this one wins the
	// Lease; only the leader runs the decision loop.
	go func() {
		pod := podIdentity()
		alfredMetrics.LeaderStatus.WithLabelValues(pod).Set(0)
		select {
		case <-mgr.Elected():
			alfredMetrics.LeaderStatus.WithLabelValues(pod).Set(1)
		case <-ctx.Done():
		}
	}()

	setupLog.Info("starting alfred",
		"namespace", opts.namespace, "config", opts.configMapName, "leaderElection", opts.enableLeaderElection)
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running alfred")
		os.Exit(1)
	}
}

func validateOptions(opts Options) error {
	if opts.migrationAPIVersion != "" && opts.migrationAPIVersion != "v1" {
		return fmt.Errorf("migration-api-version must be empty or v1")
	}
	if opts.migrationAckTimeout <= 0 || opts.migrationAckTimeout > time.Hour {
		return fmt.Errorf("migration-ack-timeout must be positive and at most 1h")
	}
	if opts.migrationFailureBackoff <= 0 || opts.migrationFailureBackoff > time.Hour {
		return fmt.Errorf("migration-failure-backoff must be positive and at most 1h")
	}
	if opts.migrationAPIVersion == "" {
		return nil
	}
	if opts.migrationServiceAccount == "" || len(validation.IsDNS1123Subdomain(opts.migrationServiceAccount)) != 0 {
		return fmt.Errorf("migration-service-account must be a nonempty DNS1123 name")
	}
	if !opts.enableLeaderElection {
		return fmt.Errorf("migration dispatch requires leader election")
	}
	if opts.simulationWorkers == "" {
		return fmt.Errorf("migration dispatch requires simulation-workers")
	}
	return nil
}

func configureMigration(opts Options, reader client.Reader, cl client.Client, observations *observer.Loop, decisions *engine.DecisionLoop) error {
	if err := validateOptions(opts); err != nil {
		return err
	}
	if opts.migrationAPIVersion == "" {
		return nil
	}
	if decisions.Predictions == nil || decisions.Predictions.Simulator == nil {
		return fmt.Errorf("migration dispatch requires a successfully loaded simulation worker")
	}
	admissionGuard := &guard.Guard{Reader: reader, Namespace: opts.namespace, ServiceAccount: opts.migrationServiceAccount}
	decisions.Dispatcher = &engine.Dispatcher{
		Reader: reader, Client: cl, Simulator: decisions.Predictions.Simulator,
		Guard: admissionGuard.Check, Namespace: opts.namespace,
		Options: engine.DispatchOptions{APIVersion: opts.migrationAPIVersion,
			AcknowledgementTimeout: opts.migrationAckTimeout, FailureBackoff: opts.migrationFailureBackoff},
		Policies: decisions.Policies, Store: decisions.Store,
	}
	observations.OMENativeExecutor = func(context.Context) snapshot.OMENativeExecutorState {
		return snapshot.OMENativeExecutorState{Available: true, WireVersion: "v1", Reason: "OperatorConfigured"}
	}
	return nil
}

func predictionStage(ctx context.Context, reader client.Reader, opts Options) (*engine.PredictionStage, error) {
	if opts.simulationTimeout <= 0 || opts.simulationTimeout > time.Minute {
		return nil, fmt.Errorf("simulation-timeout must be positive and at most 1m")
	}
	if opts.simulationWorkers == "" {
		return nil, nil
	}
	// Bound the complete startup probe, not just each configured worker.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	registry, err := process.Load(ctx, opts.simulationWorkers, opts.simulationTimeout)
	if err != nil {
		return nil, err
	}
	return &engine.PredictionStage{Reader: reader, Simulator: registry}, nil
}

func ptr[T any](v T) *T { return &v }

func podIdentity() string {
	if pod := os.Getenv("POD_NAME"); pod != "" {
		return pod
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// podCacheTransform strips the pod fields the snapshot never reads before
// they enter the informer cache. Caching every pod in a large cluster is
// Alfred's dominant memory cost; this keeps only scheduling-relevant state:
// labels, node name, node selector, container resources, phase, conditions,
// start/deletion timestamps.
func podCacheTransform(obj interface{}) (interface{}, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return obj, nil
	}
	controllerOwner := metav1.GetControllerOf(pod)
	pod.ManagedFields = nil
	pod.Annotations = nil
	if controllerOwner == nil {
		pod.OwnerReferences = nil
	} else {
		pod.OwnerReferences = []metav1.OwnerReference{*controllerOwner}
	}

	trimContainers := func(containers []corev1.Container) {
		for i := range containers {
			c := &containers[i]
			c.Command = nil
			c.Args = nil
			c.Env = nil
			c.EnvFrom = nil
			c.VolumeMounts = nil
			c.VolumeDevices = nil
			c.Ports = nil
			c.Lifecycle = nil
			c.LivenessProbe = nil
			c.ReadinessProbe = nil
			c.StartupProbe = nil
			c.SecurityContext = nil
		}
	}
	trimContainers(pod.Spec.Containers)
	trimContainers(pod.Spec.InitContainers)
	pod.Spec.EphemeralContainers = nil
	pod.Spec.Volumes = nil
	pod.Spec.ImagePullSecrets = nil
	pod.Spec.Affinity = nil
	pod.Spec.Tolerations = nil

	pod.Status.ContainerStatuses = nil
	pod.Status.InitContainerStatuses = nil
	pod.Status.EphemeralContainerStatuses = nil
	return pod, nil
}
