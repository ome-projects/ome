package routing

import (
	"fmt"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Config holds the operator-supplied, config-driven inputs for the routing
// controller. TrafficMap generation carries no host/gateway/port values of its
// own — those belong to the downstream publisher — so the knobs here are the
// enable toggle and the optional observed inputs.
//
// +kubebuilder:object:generate=false
type Config struct {
	// Enabled turns on TrafficMap generation. Off by default: the controller is a
	// no-op and reaps any TrafficMap it previously created, so enabling or
	// disabling the feature is reversible without stranding objects.
	Enabled bool

	// Observer bounds the controller and HTTP work shared by the optional probe
	// and capacity inputs. Every limit is required when routing is enabled so an
	// installation cannot accidentally run an unbounded observer.
	Observer ObserverConfig

	// Probe configures end-to-end health checks. With no path, health depends
	// only on the selected capacity mode.
	Probe ProbeConfig

	// Capacity configures endpoint-reported capacity. Zero-valued (no path)
	// means allocation comes from the control-plane plan alone.
	//
	// Independent of Probe: the two fix different failure modes (reachability
	// versus capacity overstatement), cost differently -- a deep probe consumes
	// serving capacity, a capacity poll does not -- and enabling one never
	// enables the other.
	Capacity CapacityConfig

	// Publisher configures the TrafficMap publication backend. An empty name or
	// "gatewayapi" selects the built-in Gateway API publisher.
	Publisher PublisherConfig
}

type configValidationError struct {
	section string
	err     error
}

func (e *configValidationError) Error() string { return e.err.Error() }
func (e *configValidationError) Unwrap() error { return e.err }

// IsEnabled reports whether TrafficMap generation is on. When false the routing
// controller publishes nothing and releases what it already owns.
func (c Config) IsEnabled() bool {
	return c.Enabled
}

// Validate checks the operator-level routing policy before controllers start.
// A disabled installation cannot run either observer, so staged observer and
// format-plugin settings are intentionally inert until routing is enabled.
func (c Config) Validate() error {
	if c.Publisher.ResyncInterval < 0 {
		return &configValidationError{section: "publisher", err: fmt.Errorf(
			"routing.publisher.resyncInterval must not be negative")}
	}
	if !c.Enabled {
		return nil
	}
	if err := c.Observer.Validate(); err != nil {
		return &configValidationError{section: "observer", err: err}
	}
	if err := c.Probe.Validate(); err != nil {
		return &configValidationError{section: "probe", err: err}
	}
	if c.Probe.IsEnabled() {
		if err := c.Observer.probeLimits(c.Probe); err != nil {
			return &configValidationError{section: "probe", err: err}
		}
	}
	if err := c.Capacity.Validate(); err != nil {
		return &configValidationError{section: "capacity", err: err}
	}
	if c.Capacity.IsEnabled() {
		if err := c.Observer.capacityLimits(c.Capacity); err != nil {
			return &configValidationError{section: "capacity", err: err}
		}
	}
	if c.Publisher.ResyncInterval <= 0 {
		return &configValidationError{section: "publisher", err: fmt.Errorf(
			"routing.publisher.resyncInterval must be positive when routing is enabled")}
	}
	return nil
}

// ObserverConfig sets process-wide resource limits for routing observations.
// Values have no in-code defaults; deployment configuration owns the limits.
//
// +kubebuilder:object:generate=false
type ObserverConfig struct {
	// MaxConcurrentReconciles caps TrafficMap reconciles running in parallel.
	MaxConcurrentReconciles int

	// MaxConcurrentRequests caps in-flight probe and capacity HTTP requests.
	MaxConcurrentRequests int

	// MaxResponseBytes bounds the body read from one observed endpoint.
	MaxResponseBytes int64

	// MinPeriod is the shortest permitted probe or capacity polling period.
	MinPeriod time.Duration

	// MaxSamples caps the effective per-cluster capacity history window.
	MaxSamples int
}

// Validate rejects absent or non-positive observer limits.
func (c ObserverConfig) Validate() error {
	if c.MaxConcurrentReconciles <= 0 {
		return fmt.Errorf("routing.observer.maxConcurrentReconciles must be positive when routing is enabled")
	}
	if c.MaxConcurrentRequests <= 0 {
		return fmt.Errorf("routing.observer.maxConcurrentRequests must be positive when routing is enabled")
	}
	if c.MaxResponseBytes <= 0 {
		return fmt.Errorf("routing.observer.maxResponseBytes must be positive when routing is enabled")
	}
	if c.MinPeriod <= 0 {
		return fmt.Errorf("routing.observer.minPeriod must be positive when routing is enabled")
	}
	if c.MaxSamples <= 0 {
		return fmt.Errorf("routing.observer.maxSamples must be positive when routing is enabled")
	}
	return nil
}

// probeLimits rejects an enabled probe that polls faster than the observer's
// minimum period. A limit that is not set bounds nothing.
func (c ObserverConfig) probeLimits(probe ProbeConfig) error {
	if c.MinPeriod > 0 && probe.Period < c.MinPeriod {
		return fmt.Errorf("routing.probe.period (%s) must be at least routing.observer.minPeriod (%s)",
			probe.Period, c.MinPeriod)
	}
	return nil
}

// capacityLimits rejects an enabled capacity source that polls faster than the
// observer's minimum period or keeps a longer window than its maximum. A limit
// that is not set bounds nothing.
func (c ObserverConfig) capacityLimits(capacity CapacityConfig) error {
	if c.MinPeriod > 0 && capacity.Period < c.MinPeriod {
		return fmt.Errorf("routing.capacity.period (%s) must be at least routing.observer.minPeriod (%s)",
			capacity.Period, c.MinPeriod)
	}
	if c.MaxSamples > 0 && capacity.samples() > c.MaxSamples {
		return fmt.Errorf("routing.capacity.samples (%d) must not exceed routing.observer.maxSamples (%d)",
			capacity.samples(), c.MaxSamples)
	}
	return nil
}

// httpPoll is the request a probe or capacity block repeats against each
// cluster: its path, method and cadence. The two blocks check it by the same
// rules under their own config key.
type httpPoll struct {
	key     string
	path    string
	method  string
	period  time.Duration
	timeout time.Duration
}

// validatePath rejects a path that does not begin with "/".
func (h httpPoll) validatePath() error {
	if h.path[0] != '/' {
		return fmt.Errorf("%s.path %q must begin with %q", h.key, h.path, "/")
	}
	return nil
}

// validateMethodSet rejects an empty method: the load a request puts on a
// cluster is always an explicit choice, never a default.
func (h httpPoll) validateMethodSet() error {
	if h.method == "" {
		return fmt.Errorf("%s.method is required when %s.path is set", h.key, h.key)
	}
	return nil
}

// validateMethodAllowed rejects a method other than GET, HEAD or POST.
func (h httpPoll) validateMethodAllowed() error {
	switch h.method {
	case "GET", "HEAD", "POST":
		return nil
	}
	return fmt.Errorf("%s.method %q must be GET, HEAD, or POST", h.key, h.method)
}

// validateCadence rejects a non-positive period or timeout, and a timeout that
// reaches the next request: requests to a slow cluster would overlap and pile
// up, and the real rate would stop matching the configured period.
func (h httpPoll) validateCadence() error {
	if h.period <= 0 {
		return fmt.Errorf("%s.period must be positive when %s.path is set", h.key, h.key)
	}
	if h.timeout <= 0 {
		return fmt.Errorf("%s.timeout must be positive when %s.path is set", h.key, h.key)
	}
	if h.timeout >= h.period {
		return fmt.Errorf("%s.timeout (%s) must be shorter than %s.period (%s)",
			h.key, h.timeout, h.key, h.period)
	}
	return nil
}

// firstError runs the checks in order and returns the first error.
func firstError(checks ...func() error) error {
	for _, check := range checks {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

// PublisherConfig selects one compiled-in TrafficMap publisher backend.
//
// +kubebuilder:object:generate=false
type PublisherConfig struct {
	// Name is the registered publisher name. Empty or "gatewayapi" selects the
	// built-in Gateway API publisher. To disable a stateful publisher safely,
	// leave its name selected while disabling routing so the shared reconciler
	// can withdraw its state.
	Name string

	// ResyncInterval is the safety cadence for reconciling publisher state.
	// Active routing requires a positive interval. Disabled cleanup is driven by
	// object events and permits zero.
	ResyncInterval time.Duration

	// Options are interpreted and validated only by the named publisher.
	Options map[string]string
}

// IsEnabled reports whether a TrafficMap publisher was explicitly selected.
func (c PublisherConfig) IsEnabled() bool {
	return c.Name != ""
}

// ProbeConfig configures the active end-to-end probe of each cluster's
// externally-addressable endpoint — the URL a client uses, so the outcome
// covers the whole serving path rather than pod readiness inside the cluster.
//
// Every field is operator-supplied with no in-code default. An unconfigured
// probe does not run, rather than silently probing a guessed path: a guess is a
// hardcoded behavioral value, and its specific harm is that it appears to work,
// because a shallow probe against a wrong-but-live path returns 200 forever.
//
// +kubebuilder:object:generate=false
type ProbeConfig struct {
	// Path is the request path appended to each entry's endpoint. Empty turns
	// probing off entirely — it is the enable switch, not merely a default.
	//
	// Depth is the whole decision here: a liveness path proves a process is up,
	// while an inference path proves the cluster can actually serve, and only the
	// second catches the failure this feature exists for. The trade is cost —
	// on a disaggregated cluster a synthetic inference request occupies real
	// serving capacity — so the choice is the operator's, per install.
	Path string

	// Method is the HTTP method. Empty is rejected rather than defaulted, so the
	// cost of the probe is always an explicit choice: GET and POST against an
	// inference path are very different loads.
	Method string

	// AcceptStatuses are the response codes counted as a pass. Empty is
	// rejected: "not 200 means down" is too blunt a policy to infer.
	AcceptStatuses []int

	// GateStatuses are the response codes counted as a failure. Any status that
	// is in neither list is inconclusive and leaves the gate untouched.
	//
	// The two lists are explicit and separate because the interesting codes are
	// the ones that belong to neither: 429 means alive and overloaded, so gating
	// it removes capacity exactly when the fleet is hot, and 401/403 means the
	// prober's own credentials are wrong, so failing the cluster hides a
	// control-plane defect and takes traffic down with it.
	GateStatuses []int

	// Period is how often each cluster is probed.
	Period time.Duration

	// Timeout bounds a single probe request. A timeout counts as a failure.
	Timeout time.Duration

	// FailureThreshold is the number of consecutive failures before a cluster is
	// gated. A single failed probe must never move a large traffic share, and
	// the reprogramming churn from flapping would itself be the outage.
	FailureThreshold int

	// SuccessThreshold is the number of consecutive passes before a gated cluster
	// is restored.
	SuccessThreshold int

	// AllFailedPolicy controls what happens after every cluster has independently
	// crossed the probe failure threshold. PreserveTraffic ignores only the probe
	// gates and recomputes from ready capacity; Drain preserves the all-zero result
	// so the publisher removes every route arm. It has no effect while any cluster is
	// passing or has no conclusive outcome.
	AllFailedPolicy AllFailedPolicy
}

// AllFailedPolicy controls whether a complete, conclusive probe failure is
// allowed to remove every route arm.
type AllFailedPolicy string

const (
	AllFailedPolicyPreserveTraffic AllFailedPolicy = "PreserveTraffic"
	AllFailedPolicyDrain           AllFailedPolicy = "Drain"
)

// IsEnabled reports whether probing is configured. Path is the switch: without
// a target there is nothing to probe and the health gate stays readyReplicas.
func (p ProbeConfig) IsEnabled() bool {
	return p.Path != ""
}

// Validate rejects a partially-configured probe. A half-configured probe is
// worse than none: it reads as enabled while behaving arbitrarily, so every
// field is required once Path is set rather than quietly acquiring a default.
func (p ProbeConfig) Validate() error {
	if !p.IsEnabled() {
		return nil
	}
	poll := p.httpPoll()
	return firstError(
		poll.validatePath,
		poll.validateMethodSet,
		poll.validateMethodAllowed,
		p.validateStatusLists,
		poll.validateCadence,
		p.validateThresholds,
		p.validateAllFailedPolicy,
	)
}

func (p ProbeConfig) httpPoll() httpPoll {
	return httpPoll{key: "routing.probe", path: p.Path, method: p.Method, period: p.Period, timeout: p.Timeout}
}

// validateStatusLists requires both status lists, with valid HTTP codes, no
// code in both, and none of 401, 403 and 429 as a gate code.
func (p ProbeConfig) validateStatusLists() error {
	if len(p.AcceptStatuses) == 0 {
		return fmt.Errorf("routing.probe.acceptStatuses is required when routing.probe.path is set")
	}
	if len(p.GateStatuses) == 0 {
		return fmt.Errorf("routing.probe.gateStatuses is required when routing.probe.path is set")
	}
	for _, status := range p.AcceptStatuses {
		if status < 100 || status > 599 {
			return fmt.Errorf("routing.probe.acceptStatuses contains invalid HTTP status %d", status)
		}
	}
	for _, status := range p.GateStatuses {
		if status < 100 || status > 599 {
			return fmt.Errorf("routing.probe.gateStatuses contains invalid HTTP status %d", status)
		}
		if status == 401 || status == 403 || status == 429 {
			return fmt.Errorf("routing.probe.gateStatuses must not contain %d", status)
		}
	}
	for _, s := range p.AcceptStatuses {
		for _, g := range p.GateStatuses {
			if s == g {
				return fmt.Errorf("routing.probe: status %d is in both acceptStatuses and gateStatuses", s)
			}
		}
	}
	return nil
}

// validateThresholds requires a positive failure and success threshold.
func (p ProbeConfig) validateThresholds() error {
	if p.FailureThreshold <= 0 {
		return fmt.Errorf("routing.probe.failureThreshold must be positive when routing.probe.path is set")
	}
	if p.SuccessThreshold <= 0 {
		return fmt.Errorf("routing.probe.successThreshold must be positive when routing.probe.path is set")
	}
	return nil
}

// validateAllFailedPolicy requires one of the two known policies.
func (p ProbeConfig) validateAllFailedPolicy() error {
	switch p.AllFailedPolicy {
	case AllFailedPolicyPreserveTraffic, AllFailedPolicyDrain:
		return nil
	}
	return fmt.Errorf("routing.probe.allFailedPolicy %q must be %q or %q when routing.probe.path is set",
		p.AllFailedPolicy, AllFailedPolicyPreserveTraffic, AllFailedPolicyDrain)
}

// ProbeOutcome classifies one probe response against the configured status policy.
type ProbeOutcome int

const (
	// ProbeOutcomeInconclusive leaves the gate and the failure count untouched. It
	// covers a status in neither list — 429 (alive but overloaded), 401/403 (the
	// prober's credentials, not the cluster's health) — and the probe that could
	// not run at all. That last case is uniform across clusters, so counting it as
	// failure would zero the whole fleet at once.
	ProbeOutcomeInconclusive ProbeOutcome = iota

	// ProbeOutcomePass counts toward restoring a gated cluster.
	ProbeOutcomePass

	// ProbeOutcomeFail counts toward gating a cluster.
	ProbeOutcomeFail
)

// ClassifyStatus maps an HTTP status onto the configured policy.
func (p ProbeConfig) ClassifyStatus(code int) ProbeOutcome {
	for _, s := range p.AcceptStatuses {
		if s == code {
			return ProbeOutcomePass
		}
	}
	for _, s := range p.GateStatuses {
		if s == code {
			return ProbeOutcomeFail
		}
	}
	return ProbeOutcomeInconclusive
}

// CapacityConfig configures polling a cluster for its replica ceiling or weight.
//
// Like the probe, every field is operator-supplied with no in-code default, and
// Path is the enable switch: without a target there is nothing to ask and the
// plan stands.
//
// +kubebuilder:object:generate=false
type CapacityConfig struct {
	// Mode selects whether reports cap replica counts or replace them. A cluster
	// without a usable report keeps its replica count in either mode.
	Mode v1beta1.RoutingCapacityMode

	// Path is the capacity endpoint appended to each cluster's endpoint. Empty
	// turns the input off.
	Path string

	// Method is the HTTP method for the capacity request.
	Method string

	// Format names the response shape the cluster answers with. Other formats come
	// from optional packages compiled into the binary; naming one this build
	// does not carry is a startup error, not a silent fallback.
	Format CapacityFormat

	// Options carries format-specific settings, interpreted by the selected
	// format and opaque here. Keeping them out of this struct is what lets an
	// optional format own its own knobs -- and lets that format be removed
	// without leaving a field behind that nothing reads.
	Options map[string]string

	// Period is how often each cluster is polled.
	Period time.Duration

	// Timeout bounds a single capacity request.
	Timeout time.Duration

	// Samples is how many recent readings the accepted value is derived from.
	//
	// The control plane polls one address per cluster, so behind a load balancer
	// successive polls may sample different reporters. Keeping only the latest
	// is neither min nor max but any-responder: a lucky high reading raises a
	// ceiling that an earlier reading correctly lowered.
	//
	// The window smooths; it does not protect. A longer window delays recovery,
	// because a low reading has to age out before the ceiling rises again.
	// Outlier protection is Quorum's job for a ceiling, and the lower median's in
	// Reported mode.
	Samples int

	// Quorum is how many readings in the window must corroborate a lower value
	// before it is applied: the accepted value is the Quorum-th smallest reading, not
	// the smallest.
	//
	// These are observations, not distinct reporters. One router can supply
	// repeated readings, so the window does not establish reporter consensus.
	//
	// It is also the lag before a genuine drop is believed: Quorum polls.
	//
	// In Reported mode Quorum is only the minimum window size: the accepted
	// value is the lower median of the window, because a report that is too high
	// misroutes traffic as surely as one that is too low.
	Quorum int

	// MaxAge is the exclusive upper age bound for a report's observedAt stamp.
	// Expired samples cannot constrain Ceiling mode or supply Reported weight.
	MaxAge time.Duration
}

// IsEnabled reports whether endpoint-reported capacity is configured.
func (c CapacityConfig) IsEnabled() bool {
	return c.Path != ""
}

// resolvedFormat returns the explicitly configured response format.
func (c CapacityConfig) resolvedFormat() CapacityFormat {
	return c.Format
}

// Validate rejects a partially-configured capacity source, for the same reason
// the probe does: half-configured reads as enabled while behaving arbitrarily.
func (c CapacityConfig) Validate() error {
	if err := c.validateMode(); err != nil {
		return err
	}
	if !c.IsEnabled() {
		return nil
	}
	poll := c.httpPoll()
	return firstError(
		poll.validatePath,
		poll.validateMethodSet,
		c.validateFormatSet,
		poll.validateMethodAllowed,
		poll.validateCadence,
		c.validateWindow,
		c.validateFormat,
	)
}

func (c CapacityConfig) httpPoll() httpPoll {
	return httpPoll{key: "routing.capacity", path: c.Path, method: c.Method, period: c.Period, timeout: c.Timeout}
}

// validateMode requires a known mode, and polling for Reported mode, whether or
// not the source is enabled.
func (c CapacityConfig) validateMode() error {
	if c.Mode != "" && c.Mode != v1beta1.RoutingCapacityModeCeiling && c.Mode != v1beta1.RoutingCapacityModeReported {
		return fmt.Errorf("routing.capacity.mode %q must be Ceiling or Reported", c.Mode)
	}
	if c.Mode == v1beta1.RoutingCapacityModeReported && !c.IsEnabled() {
		return fmt.Errorf("routing.capacity.mode Reported requires capacity polling")
	}
	return nil
}

// validateFormatSet requires a response format once polling is on.
func (c CapacityConfig) validateFormatSet() error {
	if c.Format == "" {
		return fmt.Errorf("routing.capacity.format is required when routing.capacity.path is set")
	}
	return nil
}

// validateWindow requires a positive age bound, window and quorum, a quorum
// the window can reach, and an age bound that outlives the polls the quorum
// needs.
func (c CapacityConfig) validateWindow() error {
	if c.MaxAge <= 0 {
		// Without a bound the staleness guard cannot fire, and a frozen reporter
		// would pin a cluster's ceiling indefinitely.
		return fmt.Errorf("routing.capacity.maxAge must be positive when routing.capacity.path is set")
	}
	if c.Samples <= 0 {
		return fmt.Errorf("routing.capacity.samples must be positive when routing.capacity.path is set")
	}
	if c.Quorum <= 0 {
		return fmt.Errorf("routing.capacity.quorum must be positive when routing.capacity.path is set")
	}
	if q, n := c.quorum(), c.samples(); q > n {
		// A quorum larger than the window can never be reached, so no reading
		// would ever lower a ceiling and the input would silently do nothing.
		return fmt.Errorf("routing.capacity.quorum (%d) must not exceed routing.capacity.samples (%d)", q, n)
	}
	if q := c.quorum(); q > 1 {
		intervals := time.Duration(q)
		wholePeriods := c.MaxAge / c.Period
		if wholePeriods < intervals {
			return fmt.Errorf("routing.capacity.maxAge (%s) must span at least quorum (%d) routing.capacity.period intervals",
				c.MaxAge, q)
		}
	}
	if c.MaxAge < c.Period {
		// Every report would age out before the next poll, so the ceiling would
		// flap between applied and fallen-open on every cycle.
		return fmt.Errorf("routing.capacity.maxAge (%s) must be at least routing.capacity.period (%s)",
			c.MaxAge, c.Period)
	}
	return nil
}

// validateFormat requires a format this build carries and lets the format
// check its own options.
func (c CapacityConfig) validateFormat() error {
	name := c.resolvedFormat()
	plugin, ok := lookupCapacityFormat(name)
	if !ok {
		// Distinguish a typo from a build that simply does not carry the
		// format: silently ignoring the setting would drop the operator's
		// intent and leave the plan in force with no explanation.
		return fmt.Errorf("routing.capacity.format %q is not registered in this build (available: %v)",
			name, RegisteredCapacityFormats())
	}
	if plugin.Validate != nil {
		return plugin.Validate(c)
	}
	return nil
}

// samples is the effective window length.
func (c CapacityConfig) samples() int {
	return c.Samples
}

// quorum is the effective corroboration count.
//
// Deliberately does not clamp to the window. An unreachable quorum is rejected
// by Validate at startup instead, because quietly lowering it would hand the
// operator a different corroboration guarantee than the one they configured --
// and they would have no way to notice.
func (c CapacityConfig) quorum() int {
	return c.Quorum
}
