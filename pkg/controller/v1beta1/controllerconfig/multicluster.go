package controllerconfig

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// MultiClusterConfigName is the inferenceservice-config ConfigMap key holding
// the multi-cluster tuning block.
const MultiClusterConfigName = "multicluster"

// +kubebuilder:object:generate=false
// MultiClusterConfig holds operator-level tuning for the multi-cluster fan-out
// layer (the WorkloadCluster connection/transport, the placement controller,
// and the global endpoint publisher). It is loaded once at manager startup, from
// the inferenceservice-config ConfigMap, only when multi-cluster is enabled.
//
// Topology, role, identity, and security stay manager flags, not config: they
// decide which controllers run and the manager's identity, so they are
// deploy-time decisions (a restart), not hot-tunable config.
//
// An absent "multicluster" block reproduces the disabled built-in behavior.
// Optional durations are text on the wire; Duration() reads one as zero when it
// is absent, malformed or not positive, and a zero handed to the
// workloadcluster/placement options makes those packages apply their own
// in-package default. Routing observer limits are different: they bound shared
// process resources and are required explicitly whenever routing is enabled.
//
// A knob that is stated but unusable is a startup error, not a silent fallback:
// Validate gates the loaded config before any controller is wired.
type MultiClusterConfig struct {
	WorkloadCluster WorkloadClusterConfig `json:"workloadCluster,omitempty"`
	Placement       PlacementConfig       `json:"placement,omitempty"`
	Endpoint        EndpointConfig        `json:"endpoint,omitempty"`
	Routing         RoutingConfig         `json:"routing,omitempty"`
}

// +kubebuilder:object:generate=false
// WorkloadClusterConfig tunes the remote-cluster connection and transport layer
// and the cross-cluster status watch funnel.
type WorkloadClusterConfig struct {
	// ClientQPS / ClientBurst are the steady-state request rate and burst to each
	// remote workload-cluster apiserver. Zero leaves the client-go default.
	ClientQPS   float64 `json:"clientQPS,omitempty"`
	ClientBurst int     `json:"clientBurst,omitempty"`
	// PerCallTimeout bounds one remote request. Empty leaves no client-level timeout.
	PerCallTimeout durationValue `json:"perCallTimeout,omitempty"`
	// CacheEnabled serves member InferenceService, ServingRuntime, and
	// ClusterServingRuntime reads from per-cluster informers. The cache includes
	// shared runtimes and foreign services so ownership collisions remain visible.
	// False keeps reads live and disables the member event funnels.
	CacheEnabled bool `json:"cacheEnabled,omitempty"`
	// HealthInterval is the re-probe cadence for an otherwise-idle WorkloadCluster.
	HealthInterval durationValue `json:"healthInterval,omitempty"`
	// ConnectionGrace is how long a previously reachable cluster tolerates transient
	// probe failures before it is flipped to Ready=False and disconnected.
	ConnectionGrace durationValue `json:"connectionGrace,omitempty"`
	// EventsBatchPeriod debounces a rotated-kubeconfig Secret's burst of key updates
	// into one reconcile.
	EventsBatchPeriod durationValue `json:"eventsBatchPeriod,omitempty"`
	// EstablishInitial / EstablishMax bound a single remote watch-establish attempt
	// (the timeout grows from initial to max). ReconnectRetryMax caps the
	// inter-attempt backoff for an unreachable cluster.
	EstablishInitial  durationValue `json:"establishInitial,omitempty"`
	EstablishMax      durationValue `json:"establishMax,omitempty"`
	ReconnectRetryMax durationValue `json:"reconnectRetryMax,omitempty"`
	// FunnelResyncInterval is how often the status funnel reconciles its per-cluster
	// watch set against the connected clusters (how fast a newly-connected cluster
	// is noticed, not the status latency). FunnelBufferSize is the depth of its
	// buffered event channel; a full channel drops events (the safety requeue
	// recovers).
	FunnelResyncInterval durationValue `json:"funnelResyncInterval,omitempty"`
	FunnelBufferSize     int           `json:"funnelBufferSize,omitempty"`
}

// +kubebuilder:object:generate=false
// PlacementConfig tunes the fan-out placement controller, its status
// convergence, and its orphan GC.
type PlacementConfig struct {
	// MemberOperatorNamespace locates member configuration and pinned runtime
	// revisions. Empty holds resolution whenever those inputs are required.
	MemberOperatorNamespace string `json:"memberOperatorNamespace,omitempty"`
	// Capacity opts into hardware-based placement. Nil keeps that mode pending.
	Capacity *PlacementCapacityConfig `json:"capacity,omitempty"`
	// RequeueInterval is the status-refresh poll cadence. With the cache/funnel off
	// it also paces the cross-cluster status re-read.
	RequeueInterval durationValue `json:"requeueInterval,omitempty"`
	// GCInterval is the orphan-sweep cadence for the placement GC runnable.
	GCInterval durationValue `json:"gcInterval,omitempty"`
	// MaxConcurrentReconciles caps placement reconciles in parallel (distinct
	// ISVCs). Zero falls back to controller-runtime's single worker.
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`
	// FanoutTimeout is the per-cluster deadline bounding a single placement read,
	// apply, or delete, so one slow remote cannot block work on healthy peers.
	FanoutTimeout durationValue `json:"fanoutTimeout,omitempty"`
	// WinnerLostGrace is the grace window for transient loss of a sticky winner,
	// including derived absence, membership removal, or admission loss. Empty
	// uses the controller default.
	WinnerLostGrace durationValue `json:"winnerLostGrace,omitempty"`
	// ObservationGrace is how long a home whose reads fail keeps the ready count
	// from its last successful read, measured from the first failed read. Empty
	// withholds that count at the first failed read.
	ObservationGrace durationValue `json:"observationGrace,omitempty"`
	// MemberReadRetry re-reads a member within one reconcile after a transient
	// read failure. Absent reads each member once per reconcile.
	MemberReadRetry PlacementMemberReadRetryConfig `json:"memberReadRetry,omitempty"`
	// StatusBatchPeriod debounces a burst of cross-cluster derived-status events for
	// one ISVC into a single placement reconcile.
	StatusBatchPeriod durationValue `json:"statusBatchPeriod,omitempty"`
	// StatusSafetyRequeue is the steady-state re-read backstop when the funnel is on
	// (events drive freshness; this only recovers a missed event).
	StatusSafetyRequeue durationValue `json:"statusSafetyRequeue,omitempty"`
	// DispatcherMode is the fan-out breadth policy: "AllAtOnce" clones onto every
	// matched candidate at once; "Incremental" probes candidates in batches. Empty
	// or unrecognized means AllAtOnce.
	DispatcherMode string `json:"dispatcherMode,omitempty"`
	// DispatcherStepSize is the candidates the Incremental dispatcher adds per round.
	// Non-positive advances by one.
	DispatcherStepSize int `json:"dispatcherStepSize,omitempty"`
	// DispatcherRoundTimeout is how long an Incremental round waits for a nominated
	// cluster to win before adding the next batch. Non-positive enforces no dwell.
	DispatcherRoundTimeout durationValue `json:"dispatcherRoundTimeout,omitempty"`
	// LocalQueue is the Kueue LocalQueue that a derived workload's pods join on
	// the target cluster when the InferenceService carries no per-ISVC queue
	// annotation. It names a resource the operator created, so it has no in-code
	// default: empty leaves the choice to the placement package.
	LocalQueue string `json:"localQueue,omitempty"`
}

// +kubebuilder:object:generate=false
// PlacementMemberReadRetryConfig bounds how often placement re-reads a member
// after a transient failure. MaxAttempts counts every read, the first one
// included, so zero or one reads once. The wait before each re-read starts at
// InitialBackoff and doubles up to MaxBackoff; both are required once
// MaxAttempts exceeds one, and neither is accepted otherwise.
type PlacementMemberReadRetryConfig struct {
	MaxAttempts    int           `json:"maxAttempts,omitempty"`
	InitialBackoff durationValue `json:"initialBackoff,omitempty"`
	MaxBackoff     durationValue `json:"maxBackoff,omitempty"`
}

// Validate reports a retry block that is stated but unusable.
func (c PlacementMemberReadRetryConfig) Validate() error {
	var errs []error
	if c.MaxAttempts < 0 {
		errs = append(errs, fmt.Errorf("placement.memberReadRetry.maxAttempts: %d must not be negative", c.MaxAttempts))
	}
	if c.MaxAttempts <= 1 {
		if c.InitialBackoff.written() || c.MaxBackoff.written() {
			errs = append(errs, errors.New("placement.memberReadRetry.maxAttempts: must exceed 1 when backoffs are set"))
		}
		return errors.Join(errs...)
	}
	for _, field := range []struct {
		name  string
		value durationValue
	}{
		{"initialBackoff", c.InitialBackoff},
		{"maxBackoff", c.MaxBackoff},
	} {
		if field.value.Duration() == 0 {
			errs = append(errs, fmt.Errorf("placement.memberReadRetry.%s: %q must be a positive duration when maxAttempts exceeds 1", field.name, field.value.text))
		}
	}
	if len(errs) == 0 && c.MaxBackoff.Duration() < c.InitialBackoff.Duration() {
		errs = append(errs, errors.New("placement.memberReadRetry.maxBackoff: must not be shorter than initialBackoff"))
	}
	return errors.Join(errs...)
}

// +kubebuilder:object:generate=false
// PlacementCapacityConfig identifies reports and bounds their acceptance age.
// All fields are required when the block is present; there are no Go defaults.
type PlacementCapacityConfig struct {
	RootName        string        `json:"rootName"`
	MaxAge          durationValue `json:"maxAge"`
	StabilityWindow durationValue `json:"stabilityWindow"`
	RefreshInterval durationValue `json:"refreshInterval"`
}

func (c PlacementCapacityConfig) Validate() error {
	var errs []error
	if strings.TrimSpace(c.RootName) == "" {
		errs = append(errs, errors.New("placement.capacity: rootName is required"))
	}
	for _, field := range []struct {
		name  string
		value durationValue
	}{{"maxAge", c.MaxAge}, {"stabilityWindow", c.StabilityWindow}, {"refreshInterval", c.RefreshInterval}} {
		if field.value.Duration() == 0 {
			errs = append(errs, fmt.Errorf("placement.capacity.%s: must be an explicit positive duration", field.name))
		}
	}
	if len(errs) == 0 && c.RefreshInterval.Duration() >= c.MaxAge.Duration() {
		errs = append(errs, errors.New("placement.capacity.refreshInterval: must be shorter than maxAge"))
	}
	return errors.Join(errs...)
}

// +kubebuilder:object:generate=false
// EndpointConfig tunes the global endpoint publisher (a Gateway API HTTPRoute to
// the placement winner). The host template, gateway, and namespace are
// deployment-identity values with no in-code default: empty disables publishing
// (a no-op), never a baked-in gateway or host.
type EndpointConfig struct {
	// GlobalHostTemplate is the text/template for the global host an ISVC publishes
	// to the winner. Empty means only ISVCs carrying the global-host annotation
	// publish.
	GlobalHostTemplate string `json:"globalHostTemplate,omitempty"`
	// GlobalGateway is the "namespace/name" of the global-traffic Gateway the
	// published HTTPRoute attaches to. Empty makes the publisher a no-op.
	GlobalGateway string `json:"globalGateway,omitempty"`
	// RouteNamespace is the namespace for the published HTTPRoute and backing
	// resources. Empty uses the ISVC's own namespace.
	RouteNamespace string `json:"routeNamespace,omitempty"`
	// BackendPort is the port on the winner cluster's ingress the global host
	// forwards to.
	BackendPort int `json:"backendPort,omitempty"`
	// GatewayBackend configures hostname rewriting, backend TLS, and an optional
	// direct-address fallback while retaining ExternalName as the baseline.
	GatewayBackend EndpointGatewayBackendConfig `json:"gatewayBackend,omitempty"`
}

// +kubebuilder:object:generate=false
// EndpointGatewayBackendConfig configures the standard Gateway API behavior
// used when forwarding from the global Gateway to workload cluster Gateways.
type EndpointGatewayBackendConfig struct {
	// RewriteHostname replaces the forwarded Host header with the selected
	// workload cluster ingress hostname.
	RewriteHostname bool `json:"rewriteHostname,omitempty"`
	// TLS configures backend TLS independently of address resolution.
	TLS EndpointGatewayBackendTLSConfig `json:"tls,omitempty"`
	// EndpointSlices configures the direct-address fallback for environments
	// where ExternalName DNS does not return a routable Gateway address.
	EndpointSlices EndpointGatewayBackendEndpointSliceConfig `json:"endpointSlices,omitempty"`
}

// +kubebuilder:object:generate=false
// EndpointGatewayBackendTLSConfig configures BackendTLSPolicy publication.
type EndpointGatewayBackendTLSConfig struct {
	Enabled bool `json:"enabled,omitempty"`
	// WellKnownCACertificates is the Gateway API trust-root set written to each
	// BackendTLSPolicy. The currently supported standard value is "System".
	WellKnownCACertificates string `json:"wellKnownCACertificates,omitempty"`
}

// +kubebuilder:object:generate=false
// EndpointGatewayBackendEndpointSliceConfig configures direct Gateway address
// publication through EndpointSlices.
type EndpointGatewayBackendEndpointSliceConfig struct {
	Enabled bool `json:"enabled,omitempty"`
	// AddressRefreshInterval is how often child Gateway status addresses are
	// refreshed.
	AddressRefreshInterval durationValue `json:"addressRefreshInterval,omitempty"`
}

// +kubebuilder:object:generate=false
// RoutingConfig tunes the TrafficMap routing controller (control plane only),
// which projects a placed InferenceService into the capacity-aware, health-gated
// TrafficMap a gateway consumes. Generation is off by default: the field exists
// so an operator opts in, never so the control plane silently starts writing a
// routing table.
type RoutingConfig struct {
	// Enabled turns on TrafficMap generation. False (the default) leaves the
	// routing controller a no-op that reaps any TrafficMap it previously created,
	// so enabling or disabling the feature is reversible without stranding objects.
	Enabled bool `json:"enabled,omitempty"`

	// Observer bounds controller concurrency, outbound requests, response size,
	// polling frequency, and retained capacity samples. Every field is required
	// when routing is enabled.
	Observer RoutingObserverConfig `json:"observer,omitempty"`

	// Probe configures the optional active end-to-end health probe. Absent (no
	// path) means no probing and the health gate stays readyReplicas > 0.
	Probe ProbeConfig `json:"probe,omitempty"`

	// Capacity configures the optional endpoint-reported capacity ceiling.
	// Absent (no path) means allocation comes from the control-plane plan alone.
	// Independent of Probe: enabling one never enables the other.
	Capacity CapacityConfig `json:"capacity,omitempty"`

	// Publisher selects an optional compiled-in TrafficMap publisher. Empty
	// retains the Gateway API endpoint publisher. A selected stateful backend
	// must be drained by disabling routing before its name is removed. Options
	// are validated by the selected backend at manager startup.
	Publisher TrafficMapPublisherConfig `json:"publisher,omitempty"`
}

// RoutingObserverConfig sets process-wide limits for routing observations.
//
// +kubebuilder:object:generate=false
type RoutingObserverConfig struct {
	// MaxConcurrentReconciles caps TrafficMap reconciles running in parallel.
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`
	// MaxConcurrentRequests caps in-flight probe and capacity HTTP requests.
	MaxConcurrentRequests int `json:"maxConcurrentRequests,omitempty"`
	// MaxResponseBytes bounds the body read from one observed endpoint.
	MaxResponseBytes int64 `json:"maxResponseBytes,omitempty"`
	// MinPeriod is the shortest permitted probe or capacity cadence.
	MinPeriod durationValue `json:"minPeriod,omitempty"`
	// MaxSamples caps the effective per-home capacity history window.
	MaxSamples int `json:"maxSamples,omitempty"`
}

// TrafficMapPublisherConfig configures the TrafficMap publication backend.
//
// +kubebuilder:object:generate=false
type TrafficMapPublisherConfig struct {
	// Name selects a backend compiled into the manager. Empty or "gatewayapi"
	// uses the built-in Gateway API publisher.
	Name string `json:"name,omitempty"`
	// ResyncInterval is the safety cadence for reconciling publisher state.
	// It is required when routing is enabled and has no binary default.
	ResyncInterval durationValue `json:"resyncInterval,omitempty"`
	// Options are interpreted and validated only by the named publisher.
	Options map[string]string `json:"options,omitempty"`
}

// +kubebuilder:object:generate=false
// CapacityConfig is the operator-supplied configuration for polling each home
// for a replica ceiling or an authoritative routing weight.
type CapacityConfig struct {
	// Mode selects replica ceilings or authoritative reported weights.
	Mode string `json:"mode,omitempty"`

	// Path is the capacity endpoint appended to each home's endpoint. Empty
	// turns the input off.
	Path string `json:"path,omitempty"`

	// Method is the HTTP method for the capacity request.
	Method string `json:"method,omitempty"`

	// Format names the response shape the home answers with. "Report" selects
	// the built-in timestamped capacity object. Other formats come from optional
	// packages compiled into the binary; naming one this build does not carry is
	// a startup error rather than a silent fallback.
	Format string `json:"format,omitempty"`

	// Options carries settings specific to the selected format, interpreted by
	// that format and opaque to the control plane.
	Options map[string]string `json:"options,omitempty"`

	// Samples is how many recent readings the applied ceiling is derived from.
	Samples int `json:"samples,omitempty"`

	// Quorum is how many readings must corroborate a lower value before it is
	// applied -- the ceiling is the Quorum-th smallest, not the smallest, so a
	// single misbehaving reporter cannot set it.
	Quorum int `json:"quorum,omitempty"`

	// Period is how often each home is polled, as a duration string.
	Period durationValue `json:"period,omitempty"`

	// Timeout bounds one capacity request, as a duration string.
	Timeout durationValue `json:"timeout,omitempty"`

	// MaxAge is how old a report's observedAt stamp may be and still be
	// applied, as a duration string. Beyond it the poller falls open to the
	// plan, so a home whose reporter froze releases its ceiling rather than
	// pinning a stale one forever.
	MaxAge durationValue `json:"maxAge,omitempty"`
}

// +kubebuilder:object:generate=false
// ProbeConfig is the operator-supplied configuration for the active end-to-end
// health probe: the control plane periodically requests each home's
// externally-addressable endpoint, so the verdict covers the whole serving path
// rather than pod readiness inside the home.
//
// Every field is required once Path is set, and none has an in-code default. A
// guessed probe path would be a behavioral value baked into the binary, and its
// particular harm is that it looks like it works: a shallow probe against a
// wrong-but-live path returns 200 forever while catching nothing.
type ProbeConfig struct {
	// Path is the request path appended to each home's endpoint. Empty turns
	// probing off — it is the enable switch, not just an unset default.
	Path string `json:"path,omitempty"`

	// Method is the HTTP method for the probe request.
	Method string `json:"method,omitempty"`

	// AcceptStatuses are the response codes counted as a pass.
	AcceptStatuses []int `json:"acceptStatuses,omitempty"`

	// GateStatuses are the response codes counted as a failure. Codes in
	// neither list are inconclusive and move nothing: 429 means alive and
	// overloaded, and 401/403 means the prober's own credentials are wrong.
	GateStatuses []int `json:"gateStatuses,omitempty"`

	// Period is how often each home is probed, as a duration string.
	Period durationValue `json:"period,omitempty"`

	// Timeout bounds one probe request, as a duration string.
	Timeout durationValue `json:"timeout,omitempty"`

	// FailureThreshold is the number of consecutive failures before a home is
	// gated to weight 0.
	FailureThreshold int `json:"failureThreshold,omitempty"`

	// SuccessThreshold is the number of consecutive passes before a gated home
	// is restored.
	SuccessThreshold int `json:"successThreshold,omitempty"`

	// AllFailedPolicy controls whether a conclusive failure from every home
	// preserves traffic or drains every route arm.
	AllFailedPolicy string `json:"allFailedPolicy,omitempty"`
}

// NewMultiClusterConfig loads the "multicluster" block from the
// inferenceservice-config ConfigMap. It is read once at manager startup (the
// multi-cluster wiring is built before any reconcile), so there is no
// ConfigCache-backed variant. An absent block yields a zero-valued config and
// every consumer applies its own in-package default.
func NewMultiClusterConfig(clientset kubernetes.Interface) (*MultiClusterConfig, error) {
	return loadBlock(clientset, parseMultiClusterConfig)
}

func parseMultiClusterConfig(configMap *v1.ConfigMap) (*MultiClusterConfig, error) {
	cfg := &MultiClusterConfig{}
	if err := getComponentConfig(MultiClusterConfigName, configMap, cfg); err != nil {
		return nil, fmt.Errorf("unable to parse multicluster config json: %w", err)
	}
	return cfg, nil
}

// Validate reports knob values that are stated but unusable, for the manager to
// refuse at startup. Loading stays forgiving on purpose — an unparsable duration
// reads as zero so the consuming package applies its own default — but that
// makes a typo like "30" or "1 m" indistinguishable from "not set", silently
// discarding the operator's intended value for the life of the process. The
// composition root calls this so the deploy fails loudly instead.
func (c MultiClusterConfig) Validate() error {
	durations := map[string]durationValue{
		"workloadCluster.perCallTimeout":                                c.WorkloadCluster.PerCallTimeout,
		"workloadCluster.healthInterval":                                c.WorkloadCluster.HealthInterval,
		"workloadCluster.connectionGrace":                               c.WorkloadCluster.ConnectionGrace,
		"workloadCluster.eventsBatchPeriod":                             c.WorkloadCluster.EventsBatchPeriod,
		"workloadCluster.establishInitial":                              c.WorkloadCluster.EstablishInitial,
		"workloadCluster.establishMax":                                  c.WorkloadCluster.EstablishMax,
		"workloadCluster.reconnectRetryMax":                             c.WorkloadCluster.ReconnectRetryMax,
		"workloadCluster.funnelResyncInterval":                          c.WorkloadCluster.FunnelResyncInterval,
		"placement.requeueInterval":                                     c.Placement.RequeueInterval,
		"placement.gcInterval":                                          c.Placement.GCInterval,
		"placement.fanoutTimeout":                                       c.Placement.FanoutTimeout,
		"placement.winnerLostGrace":                                     c.Placement.WinnerLostGrace,
		"placement.observationGrace":                                    c.Placement.ObservationGrace,
		"placement.statusBatchPeriod":                                   c.Placement.StatusBatchPeriod,
		"placement.statusSafetyRequeue":                                 c.Placement.StatusSafetyRequeue,
		"placement.dispatcherRoundTimeout":                              c.Placement.DispatcherRoundTimeout,
		"endpoint.gatewayBackend.endpointSlices.addressRefreshInterval": c.Endpoint.GatewayBackend.EndpointSlices.AddressRefreshInterval,
		"routing.probe.period":                                          c.Routing.Probe.Period,
		"routing.probe.timeout":                                         c.Routing.Probe.Timeout,
		"routing.observer.minPeriod":                                    c.Routing.Observer.MinPeriod,
		"routing.capacity.period":                                       c.Routing.Capacity.Period,
		"routing.capacity.timeout":                                      c.Routing.Capacity.Timeout,
		"routing.capacity.maxAge":                                       c.Routing.Capacity.MaxAge,
	}
	keys := make([]string, 0, len(durations))
	for k := range durations {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var errs []error
	if err := c.Placement.MemberReadRetry.Validate(); err != nil {
		errs = append(errs, err)
	}
	if c.Placement.Capacity != nil {
		if strings.TrimSpace(c.Placement.MemberOperatorNamespace) == "" {
			errs = append(errs, errors.New("placement.memberOperatorNamespace is required for capacity placement"))
		}
		if err := c.Placement.Capacity.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, k := range keys {
		if err := durations[k].validatePositive(k); err != nil {
			errs = append(errs, err)
		}
	}
	publisherResync := c.Routing.Publisher.ResyncInterval
	if !publisherResync.written() {
		if c.Routing.Enabled {
			errs = append(errs, errors.New("routing.publisher.resyncInterval: must be positive when routing is enabled"))
		}
	} else if !publisherResync.wellFormed {
		errs = append(errs, fmt.Errorf(
			"routing.publisher.resyncInterval: %q is not a duration (use forms like \"30s\", \"5m\")",
			publisherResync.text))
	} else if publisherResync.parsed < 0 || (c.Routing.Enabled && publisherResync.parsed == 0) {
		errs = append(errs, fmt.Errorf(
			"routing.publisher.resyncInterval: %q must be positive when routing is enabled and non-negative otherwise",
			publisherResync.text))
	}
	if p := c.Endpoint.BackendPort; p < 0 || p > 65535 {
		errs = append(errs, fmt.Errorf("endpoint.backendPort: %d is not a valid port", p))
	}
	// Naming a gateway states the intent to publish, but the publisher stays off
	// without a routable backend port. Zero stays valid for the fully-disabled
	// endpoint config; paired with a gateway it is a half-finished one that would
	// otherwise publish nothing and report nothing.
	if c.Endpoint.GlobalGateway != "" && c.Endpoint.BackendPort <= 0 {
		errs = append(errs, fmt.Errorf("endpoint.backendPort: must be set when endpoint.globalGateway is configured (%q)", c.Endpoint.GlobalGateway))
	}
	gb := c.Endpoint.GatewayBackend
	if gb.RewriteHostname || gb.TLS.Enabled || gb.EndpointSlices.Enabled {
		if strings.TrimSpace(c.Endpoint.GlobalGateway) == "" {
			errs = append(errs, errors.New("endpoint.globalGateway: must be set when gateway backend features are enabled"))
		}
	}
	if gb.TLS.Enabled {
		if gb.TLS.WellKnownCACertificates != string(gatewayapiv1.WellKnownCACertificatesSystem) {
			errs = append(errs, fmt.Errorf("endpoint.gatewayBackend.tls.wellKnownCACertificates: %q must be %q when enabled", gb.TLS.WellKnownCACertificates, gatewayapiv1.WellKnownCACertificatesSystem))
		}
	} else if gb.TLS.WellKnownCACertificates != "" {
		errs = append(errs, errors.New("endpoint.gatewayBackend.tls.enabled: must be true when TLS settings are supplied"))
	}
	if gb.EndpointSlices.Enabled {
		if strings.TrimSpace(gb.EndpointSlices.AddressRefreshInterval.text) == "" {
			errs = append(errs, errors.New("endpoint.gatewayBackend.endpointSlices.addressRefreshInterval: must be set when enabled"))
		}
	} else if gb.EndpointSlices.AddressRefreshInterval.written() {
		errs = append(errs, errors.New("endpoint.gatewayBackend.endpointSlices.enabled: must be true when EndpointSlice settings are supplied"))
	}
	return errors.Join(errs...)
}
