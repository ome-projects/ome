// Alfred is the OME GPU cluster caretaker (OEP-0008): a leader-elected
// controller that observes the physical GPU layer, recommends corrective
// migrations, and reports arbitration results. Execute mode is configuration
// intent only in this phase: without a Dispatcher, admitted actions in both
// modes remain explicitly withheld.
//
// This binary wires two loops onto a controller-runtime manager:
//   - the observation loop (every replica): snapshot + gauges, read-only;
//   - the decision loop (leader only): policies → arbiter → reporter.
//
// A Dispatcher and its admission guard are added together by a later change.
package main

import (
	"context"
	"flag"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
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
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/observer"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/policy/defrag"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("alfred-setup")
)

// Leader-election parameters per OEP-0008 §Leader election (mirrors
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

// Options holds the command-line configuration.
type Options struct {
	metricsAddr          string
	probeAddr            string
	enableLeaderElection bool
	namespace            string
	configMapName        string
	configMapKey         string
	zapOpts              zap.Options
}

// DefaultOptions returns the flag defaults. The namespace defaults from the
// POD_NAMESPACE downward-API variable so the deployment needs no explicit
// flag.
func DefaultOptions() Options {
	return Options{
		metricsAddr:          ":8080",
		probeAddr:            ":8081",
		enableLeaderElection: true,
		namespace:            constants.OMENamespace,
		configMapName:        "alfred-config",
		configMapKey:         "config.yaml",
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
	opts.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	return opts
}

func main() {
	opts := GetOptions()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts.zapOpts)))

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
		Reader:            mgr.GetClient(),
		Store:             store,
		Metrics:           alfredMetrics,
		Log:               ctrl.Log.WithName("alfred-observer"),
		Scorer:            defrag.PublishScores,
		OMENativeExecutor: newOMENativeExecutor(mgr.GetAPIReader()),
	}
	if err := mgr.Add(observationLoop); err != nil {
		setupLog.Error(err, "unable to add observation loop")
		os.Exit(1)
	}

	// Decision side: leader-only loop over policies → arbiter → reporter.
	// The dispatcher joins in the execute path; until then admitted
	// candidates are withheld and reported as such.
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
	decisionLoop := &engine.DecisionLoop{
		Snapshots: observationLoop,
		Store:     store,
		Policies:  []policy.Policy{&defrag.Policy{}},
		Arbiter:   &engine.Arbiter{Ledger: engine.NewLedger()},
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

	ctx := ctrl.SetupSignalHandler()

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

func ptr[T any](v T) *T { return &v }

func newOMENativeExecutor(reader client.Reader) func(context.Context, *config.Config) snapshot.OMENativeExecutorState {
	return (&observer.OMENativeExecutorReader{Reader: reader}).Read
}

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
