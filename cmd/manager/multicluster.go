package main

import (
	"fmt"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
	placementendpoint "sigs.k8s.io/ome/pkg/controller/v1beta1/placement/endpoint"
	placementrouting "sigs.k8s.io/ome/pkg/controller/v1beta1/placement/routing"
	workloadcluster "sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

// mcWiring is the multi-cluster wiring resolved from a MultiClusterConfig: the
// plain values that flow into the WorkloadCluster transport, the placement
// controller, and the endpoint publisher. It is split out of setupMultiCluster
// so the config->values mapping — the part where a mis-wired field would
// silently mis-tune the control plane — is unit-testable without a manager.
//
// Durations are already resolved (the accessors yield 0 on an omitted key, and
// each consuming option then applies its own in-package default).
type mcWiring struct {
	clientTuning      workloadcluster.ClientTuning
	cacheEnabled      bool
	healthInterval    time.Duration
	connectionGrace   time.Duration
	eventsBatchPeriod time.Duration
	reconnectBackoff  workloadcluster.ReconnectBackoffConfig

	// Placement + its status convergence and GC (control plane only).
	requeue                 time.Duration
	gcInterval              time.Duration
	maxConcurrent           int
	placeTimeout            time.Duration
	winnerLostGrace         time.Duration
	observationGrace        time.Duration
	memberReadRetry         placement.MemberReadRetry
	statusBatchPeriod       time.Duration
	statusSafetyRequeue     time.Duration
	dispatcherMode          placement.DispatcherMode
	dispatcherStepSize      int
	dispatcherRoundTimeout  time.Duration
	localQueue              string
	capacity                *placement.CapacityConfig
	memberOperatorNamespace string
	funnelResyncInterval    time.Duration
	funnelBufferSize        int

	endpoint placementendpoint.Config
	routing  placementrouting.Config
}

// trafficMapPublisherMode describes the endpoint controllers selected by the
// routing configuration. The TrafficMap controller always runs so a disabled
// publisher can finalize its existing journals. The legacy endpoint controller
// runs only while the built-in Gateway API publisher is inactive.
type trafficMapPublisherMode struct {
	useBuiltInGateway bool
	trafficMapActive  bool
	useLegacyGateway  bool
}

func resolveTrafficMapPublisherMode(cfg placementrouting.Config) trafficMapPublisherMode {
	useBuiltInGateway := cfg.Publisher.Name == "" ||
		cfg.Publisher.Name == placementendpoint.GatewayAPITrafficMapPublisherName
	return trafficMapPublisherMode{
		useBuiltInGateway: useBuiltInGateway,
		trafficMapActive:  cfg.Enabled,
		useLegacyGateway:  useBuiltInGateway && !cfg.Enabled,
	}
}

func newGatewayTrafficMapPublisher(
	kubeClient client.Client,
	apiReader client.Reader,
	publisherConfig placementrouting.PublisherConfig,
	endpointConfig placementendpoint.Config,
	opts ...placementendpoint.GatewayAPIPublisherOption,
) (*placementrouting.TrafficMapPublisher, error) {
	publisher := placementendpoint.NewGatewayAPITrafficMapPublisher(
		kubeClient,
		apiReader,
		endpointConfig,
		opts...,
	)
	globalOptions, err := publisher.ResolveOptions(publisherConfig.Options, nil)
	if err != nil {
		return nil, err
	}
	return &placementrouting.TrafficMapPublisher{
		Publisher:      publisher,
		GlobalOptions:  globalOptions,
		ResyncInterval: gatewayTrafficMapResyncInterval(publisherConfig.ResyncInterval, endpointConfig),
	}, nil
}

// gatewayTrafficMapResyncInterval keeps direct Gateway address publication at
// least as fresh as its configured refresh cadence while retaining the common
// publisher resync as the collision-retry backstop.
func gatewayTrafficMapResyncInterval(publisherResync time.Duration, endpointConfig placementendpoint.Config) time.Duration {
	addressRefresh := endpointConfig.GatewayBackend.EndpointSlices.AddressRefreshInterval
	if !endpointConfig.GatewayBackend.EndpointSlices.Enabled || addressRefresh <= 0 {
		return publisherResync
	}
	if publisherResync <= 0 || addressRefresh < publisherResync {
		return addressRefresh
	}
	return publisherResync
}

// resolveMCWiring maps a loaded MultiClusterConfig to the wiring values used to
// build the multi-cluster controllers.
func resolveMCWiring(mc *controllerconfig.MultiClusterConfig) mcWiring {
	wc, pl, ep, rt := mc.WorkloadCluster, mc.Placement, mc.Endpoint, mc.Routing

	// Status-convergence backstop: with the cache (and thus the watch funnel) on,
	// events drive freshness and the safety requeue only recovers a missed event;
	// with it off there is no event source, so the poll cadence (requeueInterval)
	// is the backstop.
	safetyRequeue := pl.StatusSafetyRequeue.Duration()
	if !wc.CacheEnabled {
		safetyRequeue = pl.RequeueInterval.Duration()
	}
	var capacityConfig *placement.CapacityConfig
	if pl.Capacity != nil {
		capacityConfig = &placement.CapacityConfig{
			RootName: pl.Capacity.RootName,
			MaxAge:   pl.Capacity.MaxAge.Duration(), StabilityWindow: pl.Capacity.StabilityWindow.Duration(), RefreshInterval: pl.Capacity.RefreshInterval.Duration(),
		}
	}

	return mcWiring{
		clientTuning: workloadcluster.ClientTuning{
			QPS:            float32(wc.ClientQPS),
			Burst:          wc.ClientBurst,
			PerCallTimeout: wc.PerCallTimeout.Duration(),
		},
		cacheEnabled:      wc.CacheEnabled,
		healthInterval:    wc.HealthInterval.Duration(),
		connectionGrace:   wc.ConnectionGrace.Duration(),
		eventsBatchPeriod: wc.EventsBatchPeriod.Duration(),
		reconnectBackoff: workloadcluster.ReconnectBackoffConfig{
			EstablishInitial: wc.EstablishInitial.Duration(),
			EstablishMax:     wc.EstablishMax.Duration(),
			RetryMax:         wc.ReconnectRetryMax.Duration(),
		},
		requeue:          pl.RequeueInterval.Duration(),
		gcInterval:       pl.GCInterval.Duration(),
		maxConcurrent:    pl.MaxConcurrentReconciles,
		placeTimeout:     pl.FanoutTimeout.Duration(),
		winnerLostGrace:  pl.WinnerLostGrace.Duration(),
		observationGrace: pl.ObservationGrace.Duration(),
		memberReadRetry: placement.MemberReadRetry{
			MaxAttempts:    pl.MemberReadRetry.MaxAttempts,
			InitialBackoff: pl.MemberReadRetry.InitialBackoff.Duration(),
			MaxBackoff:     pl.MemberReadRetry.MaxBackoff.Duration(),
		},
		statusBatchPeriod:       pl.StatusBatchPeriod.Duration(),
		statusSafetyRequeue:     safetyRequeue,
		dispatcherMode:          placement.DispatcherMode(pl.DispatcherMode),
		dispatcherStepSize:      pl.DispatcherStepSize,
		dispatcherRoundTimeout:  pl.DispatcherRoundTimeout.Duration(),
		localQueue:              pl.LocalQueue,
		capacity:                capacityConfig,
		memberOperatorNamespace: pl.MemberOperatorNamespace,
		funnelResyncInterval:    wc.FunnelResyncInterval.Duration(),
		funnelBufferSize:        wc.FunnelBufferSize,
		endpoint: placementendpoint.Config{
			GlobalHostTemplate: ep.GlobalHostTemplate,
			GlobalGateway:      ep.GlobalGateway,
			RouteNamespace:     ep.RouteNamespace,
			BackendPort:        int32(ep.BackendPort),
			GatewayBackend: placementendpoint.GatewayBackendConfig{
				RewriteHostname: ep.GatewayBackend.RewriteHostname,
				TLS: placementendpoint.GatewayBackendTLSConfig{
					Enabled:                 ep.GatewayBackend.TLS.Enabled,
					WellKnownCACertificates: ep.GatewayBackend.TLS.WellKnownCACertificates,
				},
				EndpointSlices: placementendpoint.GatewayBackendEndpointSliceConfig{
					Enabled:                ep.GatewayBackend.EndpointSlices.Enabled,
					AddressRefreshInterval: ep.GatewayBackend.EndpointSlices.AddressRefreshInterval.Duration(),
				},
			},
		},
		routing: placementrouting.Config{
			Enabled: rt.Enabled,
			Observer: placementrouting.ObserverConfig{
				MaxConcurrentReconciles: rt.Observer.MaxConcurrentReconciles,
				MaxConcurrentRequests:   rt.Observer.MaxConcurrentRequests,
				MaxResponseBytes:        rt.Observer.MaxResponseBytes,
				MinPeriod:               rt.Observer.MinPeriod.Duration(),
				MaxSamples:              rt.Observer.MaxSamples,
			},
			Publisher: placementrouting.PublisherConfig{
				Name:           rt.Publisher.Name,
				Options:        rt.Publisher.Options,
				ResyncInterval: rt.Publisher.ResyncInterval.Duration(),
			},
			Probe: placementrouting.ProbeConfig{
				Path:             rt.Probe.Path,
				Method:           rt.Probe.Method,
				AcceptStatuses:   rt.Probe.AcceptStatuses,
				GateStatuses:     rt.Probe.GateStatuses,
				Period:           rt.Probe.Period.Duration(),
				Timeout:          rt.Probe.Timeout.Duration(),
				FailureThreshold: rt.Probe.FailureThreshold,
				SuccessThreshold: rt.Probe.SuccessThreshold,
				AllFailedPolicy:  placementrouting.AllFailedPolicy(rt.Probe.AllFailedPolicy),
			},
			Capacity: placementrouting.CapacityConfig{
				Mode:    v1beta1.RoutingCapacityMode(rt.Capacity.Mode),
				Path:    rt.Capacity.Path,
				Method:  rt.Capacity.Method,
				Format:  placementrouting.CapacityFormat(rt.Capacity.Format),
				Options: rt.Capacity.Options,
				Samples: rt.Capacity.Samples,
				Quorum:  rt.Capacity.Quorum,
				Period:  rt.Capacity.Period.Duration(),
				Timeout: rt.Capacity.Timeout.Duration(),
				MaxAge:  rt.Capacity.MaxAge.Duration(),
			},
		},
	}
}

// validateDispatcherMode rejects an unrecognized fan-out policy. The dispatcher
// itself falls back to AllAtOnce for anything it does not recognize, so a typo
// ("incremental" for "Incremental") would otherwise silently select a different
// breadth policy than the operator asked for.
func validateDispatcherMode(mode placement.DispatcherMode) error {
	switch mode {
	case "", placement.DispatcherModeAllAtOnce, placement.DispatcherModeIncremental:
		return nil
	default:
		return fmt.Errorf("invalid multi-cluster configuration: placement.dispatcherMode %q must be %q or %q",
			mode, placement.DispatcherModeAllAtOnce, placement.DispatcherModeIncremental)
	}
}

func newTrafficMapPublisherReconciler(
	kubeClient client.Client,
	apiReader client.Reader,
	publisher *placementrouting.TrafficMapPublisher,
	active bool,
	leaderElectionEnabled bool,
) *placementendpoint.TrafficMapPublisherReconciler {
	requeueAfter := publisher.ResyncInterval
	if !active {
		requeueAfter = 0
	}
	return &placementendpoint.TrafficMapPublisherReconciler{
		Client:                kubeClient,
		APIReader:             apiReader,
		Log:                   ctrl.Log.WithName("controllers").WithName("TrafficMapPublisher"),
		Publisher:             publisher.Publisher,
		GlobalOptions:         publisher.GlobalOptions,
		RequeueAfter:          requeueAfter,
		Active:                active,
		LeaderElectionEnabled: leaderElectionEnabled,
	}
}

// multiClusterSetup is one wiring of the multi-cluster layer onto the
// manager: the flags and the role, the values read from the multicluster
// ConfigMap block, and what the steps have built so far. The steps share the
// record and run in the order steps lists.
type multiClusterSetup struct {
	mgr                   manager.Manager
	clientSet             kubernetes.Interface
	options               Options
	isControlPlane        bool
	instanceStatusDecoder irstatus.Decoder

	wiring mcWiring
	// execPolicy says whether a WorkloadCluster kubeconfig may run an exec
	// credential plugin, and which commands.
	execPolicy workloadcluster.ExecCredentialPolicy
	// clusterManager holds the live client of every connected WorkloadCluster;
	// the placement controllers read the member clusters through it.
	clusterManager *workloadcluster.Manager

	// The control plane's publisher choice and the observers routing uses;
	// prober and capacity stay nil while routing is disabled.
	publisherMode       trafficMapPublisherMode
	trafficMapPublisher *placementrouting.TrafficMapPublisher
	prober              *placementrouting.Prober
	capacity            *placementrouting.CapacityPoller
}

// multiClusterStep is one step of the wiring. A step that fails returns why,
// and the manager does not start.
type multiClusterStep func() error

// setupMultiCluster wires the multi-cluster layer onto mgr: the WorkloadCluster
// registry and transport for every manager that enables multi-cluster, and on
// the control plane the placement controller, its GC, the TrafficMap and
// endpoint publishers and the routing controller, which all read the member
// clusters through the one cluster manager. Tunables come from the
// inferenceservice-config ConfigMap; topology, identity and security come from
// the flags.
func setupMultiCluster(mgr manager.Manager, clientSet kubernetes.Interface, options Options, isControlPlane bool, instanceStatusDecoder irstatus.Decoder) error {
	setup := &multiClusterSetup{
		mgr:                   mgr,
		clientSet:             clientSet,
		options:               options,
		isControlPlane:        isControlPlane,
		instanceStatusDecoder: instanceStatusDecoder,
	}
	return runMultiClusterSteps(setup.steps())
}

// steps lists the wiring in order: the steps every multi-cluster manager runs,
// then the steps only the control plane adds.
func (s *multiClusterSetup) steps() []multiClusterStep {
	steps := []multiClusterStep{
		s.loadConfiguration,
		s.validateResolvedWiring,
		s.addClusterManager,
		s.setupWorkloadClusterController,
	}
	if !s.isControlPlane {
		return steps
	}
	return append(steps,
		s.selectTrafficMapPublisher,
		s.setupPlacementController,
		s.addPlacementGC,
		s.setupTrafficMapPublisherController,
		s.setupPlacementEndpointController,
		s.buildRoutingObservers,
		s.setupPlacementRoutingController,
	)
}

// runMultiClusterSteps runs the steps in order and returns the first failure.
func runMultiClusterSteps(steps []multiClusterStep) error {
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

// loadConfiguration reads the multicluster block of the ConfigMap and maps it
// onto the wiring values. Loading reads an unusable knob as unset, and the
// owning package would then apply its own default, so a malformed block stops
// the manager instead: the control plane must not run with timings the
// operator did not choose.
func (s *multiClusterSetup) loadConfiguration() error {
	mcConfig, err := controllerconfig.NewMultiClusterConfig(s.clientSet)
	if err != nil {
		return fmt.Errorf("load multi-cluster configuration: %w", err)
	}
	if err := mcConfig.Validate(); err != nil {
		return fmt.Errorf("invalid multi-cluster configuration: %w", err)
	}
	s.wiring = resolveMCWiring(mcConfig)
	return nil
}

// validateResolvedWiring checks the dispatcher mode and the routing block
// after the mapping, so the checks see parsed durations. A disabled
// installation may keep staged observer settings without loading their
// optional plugins until routing is enabled again.
func (s *multiClusterSetup) validateResolvedWiring() error {
	if err := validateDispatcherMode(s.wiring.dispatcherMode); err != nil {
		return err
	}
	if err := s.wiring.routing.Validate(); err != nil {
		return fmt.Errorf("invalid multi-cluster configuration: %w", err)
	}
	return nil
}

// addClusterManager builds the WorkloadCluster manager with the credential
// policy, the client tuning and the member cache, and adds it as a runnable:
// it then opens its remote clients and cache informers under the controller
// manager's long-lived context and disconnects them all on shutdown, so no
// watch outlives the process.
func (s *multiClusterSetup) addClusterManager() error {
	s.clusterManager = workloadcluster.NewManager(s.mgr.GetScheme())
	s.execPolicy = workloadcluster.ExecCredentialPolicy{
		Allowed:         s.options.allowExecCredentials,
		AllowedCommands: splitAndTrim(s.options.execCredentialAllowedCmds),
	}
	s.clusterManager.SetExecCredentialPolicy(s.execPolicy)
	s.clusterManager.SetClientTuning(s.wiring.clientTuning)
	if s.wiring.cacheEnabled {
		s.clusterManager.SetCacheOptions(placement.MemberCacheOptions())
	}
	if err := s.mgr.Add(s.clusterManager); err != nil {
		return fmt.Errorf("add WorkloadCluster manager runnable: %w", err)
	}
	return nil
}

// setupWorkloadClusterController registers the controller that connects,
// probes and reconnects every registered WorkloadCluster.
func (s *multiClusterSetup) setupWorkloadClusterController() error {
	setupLog.Info("Setting up WorkloadCluster controller")
	err := (&workloadcluster.Reconciler{
		Client:                s.mgr.GetClient(),
		Scheme:                s.mgr.GetScheme(),
		Log:                   ctrl.Log.WithName("controllers").WithName("WorkloadCluster"),
		Manager:               s.clusterManager,
		ExecPolicy:            s.execPolicy,
		HealthInterval:        s.wiring.healthInterval,
		ConnectionGracePeriod: s.wiring.connectionGrace,
		ProbeTimeout:          s.wiring.clientTuning.PerCallTimeout,
	}).SetupWithManager(s.mgr,
		workloadcluster.WithEventsBatchPeriod(s.wiring.eventsBatchPeriod),
		workloadcluster.WithReconnectBackoff(s.wiring.reconnectBackoff),
	)
	if err != nil {
		return fmt.Errorf("create WorkloadCluster controller: %w", err)
	}
	return nil
}

// selectTrafficMapPublisher builds the publisher the routing configuration
// names. The built-in Gateway API publisher, which an empty name selects,
// installs the Gateway API types on the scheme and resolves backend addresses
// through the cluster manager; any other name comes from the registry.
func (s *multiClusterSetup) selectTrafficMapPublisher() error {
	s.publisherMode = resolveTrafficMapPublisherMode(s.wiring.routing)
	var err error
	if s.publisherMode.useBuiltInGateway {
		utilruntime.Must(gatewayapiv1.Install(s.mgr.GetScheme()))
		s.trafficMapPublisher, err = newGatewayTrafficMapPublisher(
			s.mgr.GetClient(),
			s.mgr.GetAPIReader(),
			s.wiring.routing.Publisher,
			s.wiring.endpoint,
			placementendpoint.WithBackendAddressResolver(
				placementendpoint.NewGatewayAddressResolver(s.clusterManager),
			),
		)
		if err != nil {
			return fmt.Errorf("invalid multi-cluster configuration: configure built-in Gateway API publisher: %w", err)
		}
		return nil
	}
	s.trafficMapPublisher, err = placementrouting.NewTrafficMapPublisher(
		s.wiring.routing.Publisher,
		s.mgr.GetClient(),
		s.mgr.GetAPIReader(),
	)
	if err != nil {
		return fmt.Errorf("invalid multi-cluster configuration: %w", err)
	}
	return nil
}

// setupPlacementController registers the fan-out controller that places an
// InferenceService onto the member clusters and converges its status from
// the derived copies. Derived workloads are Kueue-gated only when they carry
// a queue label, so with no localQueue placement bypasses quota, and the log
// says so.
func (s *multiClusterSetup) setupPlacementController() error {
	setupLog.Info("Setting up multi-cluster placement (fan-out) controller")
	convergeOpts, err := s.statusConvergenceOptions()
	if err != nil {
		return err
	}
	if s.wiring.localQueue == "" {
		setupLog.Info("placement: no localQueue configured; derived workloads will not carry a Kueue queue label and will not be quota-gated")
	}
	err = (&placement.Reconciler{
		Client:                  s.mgr.GetClient(),
		APIReader:               s.mgr.GetAPIReader(),
		InstanceStatusDecoder:   s.instanceStatusDecoder,
		Scheme:                  s.mgr.GetScheme(),
		Log:                     ctrl.Log.WithName("controllers").WithName("Placement"),
		Clusters:                s.clusterManager,
		Requeue:                 s.wiring.requeue,
		ControlPlaneID:          s.options.placementControlPlaneID,
		MaxConcurrentReconciles: s.wiring.maxConcurrent,
		PlaceTimeout:            s.wiring.placeTimeout,
		WinnerLostGracePeriod:   s.wiring.winnerLostGrace,
		ObservationGrace:        s.wiring.observationGrace,
		MemberReadRetry:         s.wiring.memberReadRetry,
		RoutingDisabled:         !s.wiring.routing.IsEnabled(),
		DispatcherMode:          s.wiring.dispatcherMode,
		DispatcherStepSize:      s.wiring.dispatcherStepSize,
		DispatcherRoundTimeout:  s.wiring.dispatcherRoundTimeout,
		LocalQueue:              s.wiring.localQueue,
		Capacity:                s.wiring.capacity,
		MemberOperatorNamespace: s.wiring.memberOperatorNamespace,
	}).SetupWithManager(s.mgr, convergeOpts...)
	if err != nil {
		return fmt.Errorf("create Placement controller: %w", err)
	}
	return nil
}

// statusConvergenceOptions says how a placed service's status follows its
// derived copies. The status batch period and the safety requeue always
// apply. With the member cache on, one watch funnel per funnel configuration
// feeds member events, so a copy's status change re-reconciles its source at
// once and the safety requeue only recovers a missed event. With the cache
// off there is no event source, and the poll cadence, already folded into
// statusSafetyRequeue, is what keeps the status fresh.
func (s *multiClusterSetup) statusConvergenceOptions() ([]placement.ConvergeOption, error) {
	convergeOpts := []placement.ConvergeOption{
		placement.WithStatusBatchPeriod(s.wiring.statusBatchPeriod),
		placement.WithStatusSafetyRequeue(s.wiring.statusSafetyRequeue),
	}
	if !s.wiring.cacheEnabled {
		return convergeOpts, nil
	}
	funnelConfigs := append(placement.StatusFunnelConfigsFor(s.options.placementControlPlaneID), placement.RuntimeFunnelConfigs()...)
	for _, funnelCfg := range funnelConfigs {
		funnelCfg.ResyncInterval = s.wiring.funnelResyncInterval
		funnelCfg.BufferSize = s.wiring.funnelBufferSize
		funnel := workloadcluster.NewStatusFunnel(s.clusterManager, funnelCfg)
		if err := s.mgr.Add(funnel); err != nil {
			return nil, fmt.Errorf("add multi-cluster status funnel runnable: %w", err)
		}
		convergeOpts = append(convergeOpts, placement.WithStatusEvents(funnel.Events()))
	}
	return convergeOpts, nil
}

// addPlacementGC adds the runnable that sweeps the member clusters for
// derived copies this control plane created whose source InferenceService no
// longer exists.
func (s *multiClusterSetup) addPlacementGC() error {
	if err := s.mgr.Add(&placement.GCReconciler{
		APIReader:      s.mgr.GetAPIReader(),
		Log:            ctrl.Log.WithName("controllers").WithName("PlacementGC"),
		Clusters:       s.clusterManager,
		Interval:       s.wiring.gcInterval,
		ControlPlaneID: s.options.placementControlPlaneID,
	}); err != nil {
		return fmt.Errorf("add placement GC runnable: %w", err)
	}
	return nil
}

// setupTrafficMapPublisherController registers the controller that publishes
// every TrafficMap through the selected publisher. It runs whether or not
// routing is enabled: a disabled publisher still finalizes the publications
// it made earlier.
func (s *multiClusterSetup) setupTrafficMapPublisherController() error {
	setupLog.Info("Setting up multi-cluster TrafficMap publisher", "publisher", s.trafficMapPublisher.Publisher.Name(),
		"active", s.publisherMode.trafficMapActive,
		"availableTrafficMapPublishers", placementrouting.RegisteredTrafficMapPublishers())
	err := newTrafficMapPublisherReconciler(
		s.mgr.GetClient(),
		s.mgr.GetAPIReader(),
		s.trafficMapPublisher,
		s.publisherMode.trafficMapActive,
		s.options.enableLeaderElection,
	).SetupWithManager(s.mgr)
	if err != nil {
		return fmt.Errorf("create TrafficMap publisher controller: %w", err)
	}
	return nil
}

// setupPlacementEndpointController registers the endpoint publisher that
// writes one global Gateway API route per placed InferenceService straight
// from its placement. It runs only while the built-in publisher is selected
// and routing is disabled; with routing enabled the TrafficMap publisher
// writes the routes instead.
func (s *multiClusterSetup) setupPlacementEndpointController() error {
	if !s.publisherMode.useLegacyGateway {
		return nil
	}
	endpointPublisher := placementendpoint.NewGatewayAPIPublisher(
		s.mgr.GetClient(),
		s.wiring.endpoint,
		placementendpoint.WithBackendAddressResolver(
			placementendpoint.NewGatewayAddressResolver(s.clusterManager),
		),
		placementendpoint.WithGatewayAPIReader(s.mgr.GetAPIReader()),
	)
	setupLog.Info("Setting up multi-cluster legacy endpoint publisher", "publisher", endpointPublisher.Name())
	err := (&placementendpoint.Reconciler{
		Client:    s.mgr.GetClient(),
		APIReader: s.mgr.GetAPIReader(),
		Log:       ctrl.Log.WithName("controllers").WithName("PlacementEndpoint"),
		Publisher: endpointPublisher,
		Config:    s.wiring.endpoint,
	}).SetupWithManager(s.mgr)
	if err != nil {
		return fmt.Errorf("create PlacementEndpoint controller: %w", err)
	}
	return nil
}

// buildRoutingObservers builds the endpoint prober and the capacity poller
// when routing is enabled. One process-wide executor bounds all probe and
// capacity HTTP work, and its queue takes the same operator-supplied request
// limit, so running and waiting observations are both bounded. Both observers
// reach the same member endpoints with the same credentials, so they share
// one redirect-refusing HTTP client and transport.
func (s *multiClusterSetup) buildRoutingObservers() error {
	if !s.wiring.routing.IsEnabled() {
		return nil
	}
	executor, err := placementrouting.NewObserverExecutor(
		s.wiring.routing.Observer.MaxConcurrentRequests,
		s.wiring.routing.Observer.MaxConcurrentRequests,
	)
	if err != nil {
		return fmt.Errorf("create routing observer executor: %w", err)
	}
	if err := s.mgr.Add(executor); err != nil {
		return fmt.Errorf("add routing observer executor: %w", err)
	}
	observerClient := placementrouting.NewObserverClient()
	s.prober, err = placementrouting.NewProber(
		executor,
		observerClient,
		clock.RealClock{},
		s.wiring.routing.Observer.MaxResponseBytes,
		ctrl.Log.WithName("controllers").WithName("PlacementRoutingProbe"),
	)
	if err != nil {
		return fmt.Errorf("create routing endpoint prober: %w", err)
	}
	s.capacity, err = placementrouting.NewCapacityPoller(
		executor,
		observerClient,
		clock.RealClock{},
		s.wiring.routing.Observer.MaxResponseBytes,
		ctrl.Log.WithName("controllers").WithName("PlacementRoutingCapacity"),
	)
	if err != nil {
		return fmt.Errorf("create routing capacity poller: %w", err)
	}
	return nil
}

// setupPlacementRoutingController registers the controller that projects a
// placement into the capacity-aware TrafficMap the publisher consumes. It
// runs whether or not routing is enabled: when disabled it removes any
// TrafficMap it created earlier, so turning the feature off (a restart, since
// the configuration loads once) leaves no routing table behind. The log names
// the capacity formats this build carries, so a binary missing one shows it
// here rather than at the first poll.
func (s *multiClusterSetup) setupPlacementRoutingController() error {
	setupLog.Info("Setting up multi-cluster TrafficMap routing controller", "enabled", s.wiring.routing.Enabled,
		"capacityFormats", placementrouting.RegisteredCapacityFormats())
	err := (&placementrouting.Reconciler{
		Client:    s.mgr.GetClient(),
		APIReader: s.mgr.GetAPIReader(),
		Log:       ctrl.Log.WithName("controllers").WithName("PlacementRouting"),
		Config:    s.wiring.routing,
		Prober:    s.prober,
		Capacity:  s.capacity,
	}).SetupWithManager(s.mgr)
	if err != nil {
		return fmt.Errorf("create PlacementRouting controller: %w", err)
	}
	return nil
}
