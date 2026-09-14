package controllerconfig

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

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
// Every field degrades gracefully when omitted. Durations are stored as strings
// and parsed by the *Duration() accessors, which yield 0 on an empty or
// unparsable value; a zero handed to the workloadcluster/placement options
// makes those packages apply their OWN in-package default. So the default for
// each knob stays single-sourced in the package that owns it — never duplicated
// as a literal here — and an absent "multicluster" block reproduces the
// built-in behavior exactly.
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
	PerCallTimeout string `json:"perCallTimeout,omitempty"`
	// CacheEnabled serves cross-cluster derived-InferenceService reads from a
	// per-cluster informer cache instead of live apiserver reads, and is the
	// prerequisite event source for the status watch funnel. False (the default)
	// keeps reads live and the funnel off.
	CacheEnabled bool `json:"cacheEnabled,omitempty"`
	// HealthInterval is the re-probe cadence for an otherwise-idle WorkloadCluster.
	HealthInterval string `json:"healthInterval,omitempty"`
	// ConnectionGrace is how long a previously reachable cluster tolerates transient
	// probe failures before it is flipped to Ready=False and disconnected.
	ConnectionGrace string `json:"connectionGrace,omitempty"`
	// EventsBatchPeriod debounces a rotated-kubeconfig Secret's burst of key updates
	// into one reconcile.
	EventsBatchPeriod string `json:"eventsBatchPeriod,omitempty"`
	// EstablishInitial / EstablishMax bound a single remote watch-establish attempt
	// (the timeout grows from initial to max). ReconnectRetryMax caps the
	// inter-attempt backoff for an unreachable cluster.
	EstablishInitial  string `json:"establishInitial,omitempty"`
	EstablishMax      string `json:"establishMax,omitempty"`
	ReconnectRetryMax string `json:"reconnectRetryMax,omitempty"`
	// FunnelResyncInterval is how often the status funnel reconciles its per-cluster
	// watch set against the connected clusters (how fast a newly-connected cluster
	// is noticed, not the status latency). FunnelBufferSize is the depth of its
	// buffered event channel; a full channel drops events (the safety requeue
	// recovers).
	FunnelResyncInterval string `json:"funnelResyncInterval,omitempty"`
	FunnelBufferSize     int    `json:"funnelBufferSize,omitempty"`
}

// +kubebuilder:object:generate=false
// PlacementConfig tunes the fan-out placement controller, its status
// convergence, and its orphan GC.
type PlacementConfig struct {
	// RequeueInterval is the status-refresh poll cadence. With the cache/funnel off
	// it also paces the cross-cluster status re-read.
	RequeueInterval string `json:"requeueInterval,omitempty"`
	// GCInterval is the orphan-sweep cadence for the placement GC runnable.
	GCInterval string `json:"gcInterval,omitempty"`
	// MaxConcurrentReconciles caps placement reconciles in parallel (distinct
	// ISVCs). Zero falls back to controller-runtime's single worker.
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`
	// FanoutTimeout is the per-cluster deadline bounding a single fan-out apply, so
	// one slow remote cannot block placement to healthy peers.
	FanoutTimeout string `json:"fanoutTimeout,omitempty"`
	// WinnerLostGrace is the grace window held before re-placing when the sticky
	// winner's derived is absent on a still-connected winner. Empty re-places
	// immediately.
	WinnerLostGrace string `json:"winnerLostGrace,omitempty"`
	// StatusBatchPeriod debounces a burst of cross-cluster derived-status events for
	// one ISVC into a single placement reconcile.
	StatusBatchPeriod string `json:"statusBatchPeriod,omitempty"`
	// StatusSafetyRequeue is the steady-state re-read backstop when the funnel is on
	// (events drive freshness; this only recovers a missed event).
	StatusSafetyRequeue string `json:"statusSafetyRequeue,omitempty"`
	// DispatcherMode is the fan-out breadth policy: "AllAtOnce" clones onto every
	// matched candidate at once; "Incremental" probes candidates in batches. Empty
	// or unrecognized means AllAtOnce.
	DispatcherMode string `json:"dispatcherMode,omitempty"`
	// DispatcherStepSize is the candidates the Incremental dispatcher adds per round.
	// Non-positive advances by one.
	DispatcherStepSize int `json:"dispatcherStepSize,omitempty"`
	// DispatcherRoundTimeout is how long an Incremental round waits for a nominated
	// cluster to win before adding the next batch. Non-positive enforces no dwell.
	DispatcherRoundTimeout string `json:"dispatcherRoundTimeout,omitempty"`
	// LocalQueue is the Kueue LocalQueue that a derived workload's pods join on
	// the target cluster when the InferenceService carries no per-ISVC queue
	// annotation. It names a resource the operator created, so it has no in-code
	// default: empty leaves the choice to the placement package.
	LocalQueue string `json:"localQueue,omitempty"`
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
	AddressRefreshInterval string `json:"addressRefreshInterval,omitempty"`
}

// AddressRefreshIntervalDuration returns the parsed refresh interval, or zero
// when absent or malformed. MultiClusterConfig.Validate rejects a malformed or
// missing value when EndpointSlice publication is enabled.
func (c EndpointGatewayBackendEndpointSliceConfig) AddressRefreshIntervalDuration() time.Duration {
	return parseDurationOrZero(c.AddressRefreshInterval)
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

	// Probe configures the optional active end-to-end health probe. Absent (no
	// path) means no probing and the health gate stays readyReplicas > 0.
	Probe ProbeConfig `json:"probe,omitempty"`

	// Capacity configures the optional endpoint-reported capacity ceiling.
	// Absent (no path) means allocation comes from the control-plane plan alone.
	// Independent of Probe: enabling one never enables the other.
	Capacity CapacityConfig `json:"capacity,omitempty"`
}

// +kubebuilder:object:generate=false
// CapacityConfig is the operator-supplied configuration for polling each home
// for what it can currently serve, applied as a ceiling on the planned
// allocation -- never a raise.
type CapacityConfig struct {
	// Path is the capacity endpoint appended to each home's endpoint. Empty
	// turns the input off.
	Path string `json:"path,omitempty"`

	// Method is the HTTP method for the capacity request.
	Method string `json:"method,omitempty"`

	// Format names the response shape the home answers with. Empty means
	// "Report", the built-in servable-count object. Other formats come from
	// optional packages compiled into the binary; naming one this build does
	// not carry is a startup error rather than a silent fallback.
	Format string `json:"format,omitempty"`

	// Options carries settings specific to the selected format, interpreted by
	// that format and opaque to the control plane.
	Options map[string]string `json:"options,omitempty"`

	// Samples is how many recent readings the applied ceiling is derived from.
	// Zero uses the routing package's default.
	Samples int `json:"samples,omitempty"`

	// Quorum is how many readings must corroborate a lower value before it is
	// applied -- the ceiling is the Quorum-th smallest, not the smallest, so a
	// single misbehaving reporter cannot set it. Zero uses the routing
	// package's default.
	Quorum int `json:"quorum,omitempty"`

	// Period is how often each home is polled, as a duration string.
	Period string `json:"period,omitempty"`

	// Timeout bounds one capacity request, as a duration string.
	Timeout string `json:"timeout,omitempty"`

	// MaxAge is how old a report's observedAt stamp may be and still be
	// applied, as a duration string. Beyond it the poller falls open to the
	// plan, so a home whose reporter froze releases its ceiling rather than
	// pinning a stale one forever.
	MaxAge string `json:"maxAge,omitempty"`
}

// PeriodDuration parses Period, yielding 0 on an unparsable value so Validate
// rejects it rather than a default being silently substituted.
func (c CapacityConfig) PeriodDuration() time.Duration {
	d, err := time.ParseDuration(c.Period)
	if err != nil {
		return 0
	}
	return d
}

// TimeoutDuration parses Timeout, with the same rejection-over-substitution
// rule as PeriodDuration.
func (c CapacityConfig) TimeoutDuration() time.Duration {
	d, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return 0
	}
	return d
}

// MaxAgeDuration parses MaxAge, with the same rejection-over-substitution rule.
func (c CapacityConfig) MaxAgeDuration() time.Duration {
	d, err := time.ParseDuration(c.MaxAge)
	if err != nil {
		return 0
	}
	return d
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
	Period string `json:"period,omitempty"`

	// Timeout bounds one probe request, as a duration string.
	Timeout string `json:"timeout,omitempty"`

	// FailureThreshold is the number of consecutive failures before a home is
	// gated to weight 0.
	FailureThreshold int `json:"failureThreshold,omitempty"`

	// SuccessThreshold is the number of consecutive passes before a gated home
	// is restored.
	SuccessThreshold int `json:"successThreshold,omitempty"`
}

// PeriodDuration parses Period. An unparsable value yields 0, which the
// routing config's Validate rejects rather than silently substituting a rate.
func (p ProbeConfig) PeriodDuration() time.Duration {
	d, err := time.ParseDuration(p.Period)
	if err != nil {
		return 0
	}
	return d
}

// TimeoutDuration parses Timeout, with the same rejection-over-substitution
// rule as PeriodDuration.
func (p ProbeConfig) TimeoutDuration() time.Duration {
	d, err := time.ParseDuration(p.Timeout)
	if err != nil {
		return 0
	}
	return d
}

// NewMultiClusterConfig loads the "multicluster" block from the
// inferenceservice-config ConfigMap. It is read once at manager startup (the
// multi-cluster wiring is built before any reconcile), so there is no
// ConfigCache-backed variant. An absent block yields a zero-valued config and
// every consumer applies its own in-package default.
func NewMultiClusterConfig(clientset kubernetes.Interface) (*MultiClusterConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseMultiClusterConfig(configMap)
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
	durations := map[string]string{
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
		"placement.statusBatchPeriod":                                   c.Placement.StatusBatchPeriod,
		"placement.statusSafetyRequeue":                                 c.Placement.StatusSafetyRequeue,
		"placement.dispatcherRoundTimeout":                              c.Placement.DispatcherRoundTimeout,
		"endpoint.gatewayBackend.endpointSlices.addressRefreshInterval": c.Endpoint.GatewayBackend.EndpointSlices.AddressRefreshInterval,
		"routing.probe.period":                                          c.Routing.Probe.Period,
		"routing.probe.timeout":                                         c.Routing.Probe.Timeout,
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
	for _, k := range keys {
		raw := durations[k]
		if raw == "" {
			continue
		}
		d, err := time.ParseDuration(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %q is not a duration (use forms like \"30s\", \"5m\")", k, raw))
			continue
		}
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s: %q must be positive", k, raw))
		}
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
		if strings.TrimSpace(gb.EndpointSlices.AddressRefreshInterval) == "" {
			errs = append(errs, errors.New("endpoint.gatewayBackend.endpointSlices.addressRefreshInterval: must be set when enabled"))
		}
	} else if gb.EndpointSlices.AddressRefreshInterval != "" {
		errs = append(errs, errors.New("endpoint.gatewayBackend.endpointSlices.enabled: must be true when EndpointSlice settings are supplied"))
	}
	return errors.Join(errs...)
}

// PerCallTimeoutDuration returns the parsed PerCallTimeout (0 if absent/unparsable).
func (c WorkloadClusterConfig) PerCallTimeoutDuration() time.Duration {
	return parseDurationOrZero(c.PerCallTimeout)
}

// HealthIntervalDuration returns the parsed HealthInterval (0 if absent/unparsable).
func (c WorkloadClusterConfig) HealthIntervalDuration() time.Duration {
	return parseDurationOrZero(c.HealthInterval)
}

// ConnectionGraceDuration returns the parsed ConnectionGrace (0 if absent/unparsable).
func (c WorkloadClusterConfig) ConnectionGraceDuration() time.Duration {
	return parseDurationOrZero(c.ConnectionGrace)
}

// EventsBatchPeriodDuration returns the parsed EventsBatchPeriod (0 if absent/unparsable).
func (c WorkloadClusterConfig) EventsBatchPeriodDuration() time.Duration {
	return parseDurationOrZero(c.EventsBatchPeriod)
}

// EstablishInitialDuration returns the parsed EstablishInitial (0 if absent/unparsable).
func (c WorkloadClusterConfig) EstablishInitialDuration() time.Duration {
	return parseDurationOrZero(c.EstablishInitial)
}

// EstablishMaxDuration returns the parsed EstablishMax (0 if absent/unparsable).
func (c WorkloadClusterConfig) EstablishMaxDuration() time.Duration {
	return parseDurationOrZero(c.EstablishMax)
}

// ReconnectRetryMaxDuration returns the parsed ReconnectRetryMax (0 if absent/unparsable).
func (c WorkloadClusterConfig) ReconnectRetryMaxDuration() time.Duration {
	return parseDurationOrZero(c.ReconnectRetryMax)
}

// FunnelResyncIntervalDuration returns the parsed FunnelResyncInterval (0 if absent/unparsable).
func (c WorkloadClusterConfig) FunnelResyncIntervalDuration() time.Duration {
	return parseDurationOrZero(c.FunnelResyncInterval)
}

// RequeueIntervalDuration returns the parsed RequeueInterval (0 if absent/unparsable).
func (c PlacementConfig) RequeueIntervalDuration() time.Duration {
	return parseDurationOrZero(c.RequeueInterval)
}

// GCIntervalDuration returns the parsed GCInterval (0 if absent/unparsable).
func (c PlacementConfig) GCIntervalDuration() time.Duration {
	return parseDurationOrZero(c.GCInterval)
}

// FanoutTimeoutDuration returns the parsed FanoutTimeout (0 if absent/unparsable).
func (c PlacementConfig) FanoutTimeoutDuration() time.Duration {
	return parseDurationOrZero(c.FanoutTimeout)
}

// WinnerLostGraceDuration returns the parsed WinnerLostGrace (0 if absent/unparsable).
func (c PlacementConfig) WinnerLostGraceDuration() time.Duration {
	return parseDurationOrZero(c.WinnerLostGrace)
}

// StatusBatchPeriodDuration returns the parsed StatusBatchPeriod (0 if absent/unparsable).
func (c PlacementConfig) StatusBatchPeriodDuration() time.Duration {
	return parseDurationOrZero(c.StatusBatchPeriod)
}

// StatusSafetyRequeueDuration returns the parsed StatusSafetyRequeue (0 if absent/unparsable).
func (c PlacementConfig) StatusSafetyRequeueDuration() time.Duration {
	return parseDurationOrZero(c.StatusSafetyRequeue)
}

// DispatcherRoundTimeoutDuration returns the parsed DispatcherRoundTimeout (0 if absent/unparsable).
func (c PlacementConfig) DispatcherRoundTimeoutDuration() time.Duration {
	return parseDurationOrZero(c.DispatcherRoundTimeout)
}

// parseDurationOrZero parses s, returning 0 when it is empty, malformed, or
// non-positive. Callers hand the zero to a workloadcluster/placement option,
// which then applies its own in-package default — so the fallback stays
// single-sourced in the consuming package, not duplicated here. Only the empty
// case reaches a running manager; Validate rejects the rest at startup.
func parseDurationOrZero(s string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	return 0
}
