// model-eviction-controller observes model usage and produces periodic dry-run
// eviction plans. It does not submit eviction intent or delete artifacts.
package main

import (
	"flag"
	"fmt"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/modeleviction"
)

func main() {
	if err := run(); err != nil {
		ctrl.Log.Error(err, "Model eviction controller stopped")
		os.Exit(1)
	}
}

func run() error {
	options := modeleviction.DefaultOptions()
	var caFile, probeAddress, electionNamespace string
	flag.StringVar(&options.NodeSelector, "node-selector", "", "Required label selector for model nodes whose storage shares kubelet imageFs")
	flag.StringVar(&options.ModelNamespace, "model-namespace", options.ModelNamespace, "Namespace of node model ConfigMaps")
	flag.DurationVar(&options.Interval, "interval", options.Interval, "Delay between completed dry-run rounds")
	flag.Float64Var(&options.HighWatermark, "high-watermark", options.HighWatermark, "Used capacity fraction that triggers planning")
	flag.Float64Var(&options.LowWatermark, "low-watermark", options.LowWatermark, "Target used capacity fraction")
	flag.IntVar(&options.Workers, "node-concurrency", options.Workers, "Maximum concurrent node capacity and inventory reads")
	flag.DurationVar(&options.RequestTimeout, "request-timeout", options.RequestTimeout, "Timeout for API and kubelet requests, and each history check")
	flag.DurationVar(&options.RoundTimeout, "round-timeout", options.RoundTimeout, "Overall deadline for one planning round")
	flag.DurationVar(&options.MaxSampleAge, "max-sample-age", options.MaxSampleAge, "Maximum age of capacity observations used in a plan")
	flag.StringVar(&caFile, "kubelet-ca-file", "", "CA bundle for kubelet serving certificates; defaults to the ServiceAccount CA")
	flag.StringVar(&probeAddress, "health-probe-bind-address", ":8081", "Health and readiness listen address")
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "ome"
	}
	flag.StringVar(&electionNamespace, "leader-election-namespace", namespace, "Namespace of the controller leader Lease")
	logger := zap.Options{}
	logger.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&logger)))

	// This service runs with its own Kubernetes ServiceAccount. Kubelet access
	// reuses the rotating bearer token but validates kubelet serving certificates.
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load in-cluster credentials: %w", err)
	}
	config.QPS = 10
	config.Burst = 20
	kubeletConfig := rest.CopyConfig(config)
	if caFile != "" {
		kubeletConfig.CAFile = caFile
		kubeletConfig.CAData = nil
	}
	capacity, err := modeleviction.NewKubeletReader(kubeletConfig, options.RequestTimeout, options.MaxSampleAge)
	if err != nil {
		return err
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, v1beta1.AddToScheme, lws.AddToScheme} {
		if err := add(scheme); err != nil {
			return err
		}
	}
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:  scheme,
		Cache:   cache.Options{ReaderFailOnMissingInformer: true, DefaultTransform: cache.TransformStripManagedFields()},
		Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: probeAddress,
		LeaderElection: true, LeaderElectionID: "model-eviction-controller.ome.io", LeaderElectionNamespace: electionNamespace,
	})
	if err != nil {
		return err
	}
	ctx := ctrl.SetupSignalHandler()
	if err := modeleviction.SetupWithManager(ctx, mgr, capacity, options); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	ctrl.Log.Info("Starting model eviction controller", "dryRun", true, "interval", options.Interval, "nodeSelector", options.NodeSelector)
	return mgr.Start(ctx)
}
