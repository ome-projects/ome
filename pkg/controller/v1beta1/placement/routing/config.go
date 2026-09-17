package routing

import (
	"fmt"
	"time"
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

	// Probe configures the active end-to-end health probe. Zero-valued (no path)
	// means no probing, and the health gate stays readyReplicas > 0.
	Probe ProbeConfig

	// Capacity configures endpoint-reported capacity. Zero-valued (no path)
	// means allocation comes from the control-plane plan alone.
	//
	// Independent of Probe: the two fix different failure modes (reachability
	// versus capacity overstatement), cost differently -- a deep probe consumes
	// serving capacity, a capacity poll does not -- and enabling one never
	// enables the other.
	Capacity CapacityConfig

	// Publisher selects an optional deployment-specific backend for the existing
	// endpoint publisher. Empty retains the Gateway API backend. Options are
	// opaque here and validated by the selected publisher at manager startup.
	Publisher PublisherConfig
}

// IsEnabled reports whether TrafficMap generation is on. When false the routing
// controller publishes nothing and releases what it already owns.
func (c Config) IsEnabled() bool {
	return c.Enabled
}

// PublisherConfig selects one compiled-in TrafficMap publisher backend.
//
// +kubebuilder:object:generate=false
type PublisherConfig struct {
	// Name is the registered publisher name. Empty retains the Gateway API
	// publisher. To disable a stateful publisher safely, leave its name selected
	// while disabling routing so the shared reconciler can withdraw its state.
	Name string

	// Options are interpreted and validated only by the named publisher.
	Options map[string]string
}

// IsEnabled reports whether a TrafficMap publisher was explicitly selected.
func (c PublisherConfig) IsEnabled() bool {
	return c.Name != ""
}

// ProbeConfig configures the active end-to-end probe of each home's
// externally-addressable endpoint — the URL a client uses, so the verdict
// covers the whole serving path rather than pod readiness inside the home.
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
	// while an inference path proves the home can actually serve, and only the
	// second catches the failure this feature exists for. The trade is cost —
	// on a disaggregated home a synthetic inference request occupies real
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
	// prober's own credentials are wrong, so failing the home hides a
	// control-plane defect and takes traffic down with it.
	GateStatuses []int

	// Period is how often each home is probed.
	Period time.Duration

	// Timeout bounds a single probe request. A timeout counts as a failure.
	Timeout time.Duration

	// FailureThreshold is the number of consecutive failures before a home is
	// gated. A single failed probe must never move a large traffic share, and
	// the reprogramming churn from flapping would itself be the outage.
	FailureThreshold int

	// SuccessThreshold is the number of consecutive passes before a gated home
	// is restored.
	SuccessThreshold int
}

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
	if p.Path[0] != '/' {
		return fmt.Errorf("routing.probe.path %q must begin with %q", p.Path, "/")
	}
	if p.Method == "" {
		return fmt.Errorf("routing.probe.method is required when routing.probe.path is set")
	}
	if len(p.AcceptStatuses) == 0 {
		return fmt.Errorf("routing.probe.acceptStatuses is required when routing.probe.path is set")
	}
	if len(p.GateStatuses) == 0 {
		return fmt.Errorf("routing.probe.gateStatuses is required when routing.probe.path is set")
	}
	for _, s := range p.AcceptStatuses {
		for _, g := range p.GateStatuses {
			if s == g {
				return fmt.Errorf("routing.probe: status %d is in both acceptStatuses and gateStatuses", s)
			}
		}
	}
	if p.Period <= 0 {
		return fmt.Errorf("routing.probe.period must be positive when routing.probe.path is set")
	}
	if p.Timeout <= 0 {
		return fmt.Errorf("routing.probe.timeout must be positive when routing.probe.path is set")
	}
	if p.Timeout >= p.Period {
		// Otherwise a slow home's probes overlap and pile up, and the effective
		// probe rate silently stops matching the configured period.
		return fmt.Errorf("routing.probe.timeout (%s) must be shorter than routing.probe.period (%s)",
			p.Timeout, p.Period)
	}
	if p.FailureThreshold <= 0 {
		return fmt.Errorf("routing.probe.failureThreshold must be positive when routing.probe.path is set")
	}
	if p.SuccessThreshold <= 0 {
		return fmt.Errorf("routing.probe.successThreshold must be positive when routing.probe.path is set")
	}
	return nil
}

// Verdict classifies one probe outcome against the configured status policy.
type Verdict int

const (
	// VerdictInconclusive leaves the gate and the failure count untouched. It
	// covers a status in neither list — 429 (alive but overloaded), 401/403 (the
	// prober's credentials, not the home's health) — and the probe that could
	// not run at all. That last case is uniform across homes, so counting it as
	// failure would zero the whole fleet at once.
	VerdictInconclusive Verdict = iota

	// VerdictPass counts toward restoring a gated home.
	VerdictPass

	// VerdictFail counts toward gating a home.
	VerdictFail
)

// ClassifyStatus maps an HTTP status onto the configured policy.
func (p ProbeConfig) ClassifyStatus(code int) Verdict {
	for _, s := range p.AcceptStatuses {
		if s == code {
			return VerdictPass
		}
	}
	for _, s := range p.GateStatuses {
		if s == code {
			return VerdictFail
		}
	}
	return VerdictInconclusive
}

// CapacityConfig configures polling a home for what it can currently serve,
// applied as a CEILING on the control-plane plan.
//
// Like the probe, every field is operator-supplied with no in-code default, and
// Path is the enable switch: without a target there is nothing to ask and the
// plan stands.
//
// +kubebuilder:object:generate=false
type CapacityConfig struct {
	// Path is the capacity endpoint appended to each home's endpoint. Empty
	// turns the input off.
	Path string

	// Method is the HTTP method for the capacity request.
	Method string

	// Format names the response shape the home answers with. Empty means
	// FormatReport, the built-in shape. Other formats come from optional
	// packages compiled into the binary; naming one this build does not carry
	// is a startup error, not a silent fallback.
	Format CapacityFormat

	// Options carries format-specific settings, interpreted by the selected
	// format and opaque here. Keeping them out of this struct is what lets an
	// optional format own its own knobs -- and lets that format be removed
	// without leaving a field behind that nothing reads.
	Options map[string]string

	// Period is how often each home is polled.
	Period time.Duration

	// Timeout bounds a single capacity request.
	Timeout time.Duration

	// Samples is how many recent readings the applied ceiling is derived from.
	// Zero uses DefaultCapacitySamples.
	//
	// The control plane polls one address per home, so behind a load balancer
	// successive polls may sample different reporters. Keeping only the latest
	// is neither min nor max but any-responder: a lucky high reading raises a
	// ceiling that an earlier reading correctly lowered.
	//
	// The window smooths; it does not protect. A longer window delays recovery,
	// because a low reading has to age out before the ceiling rises again.
	// Outlier protection is Quorum's job.
	Samples int

	// Quorum is how many readings in the window must corroborate a lower value
	// before it is applied: the ceiling is the Quorum-th smallest reading, not
	// the smallest. Zero uses DefaultCapacityQuorum.
	//
	// It tolerates Quorum-1 misbehaving reporters. That is the difference
	// between believing a pessimist and believing a bug: one router that has
	// lost its watch reports a near-zero figure, and a strict minimum would take
	// it at face value and hold the home there for the whole window -- with a
	// round-robin balancer and a window at least as long as the replica count,
	// permanently. Requiring corroboration means a reporter can still always
	// lower the ceiling; it just cannot do so alone.
	//
	// It is also the lag before a genuine drop is believed: Quorum polls.
	Quorum int

	// MaxAge is how old a report's observedAt stamp may be and still be
	// applied. Beyond it the poller falls open to the plan, so a home whose
	// reporter froze releases its ceiling instead of holding a stale one
	// forever.
	MaxAge time.Duration
}

// IsEnabled reports whether endpoint-reported capacity is configured.
func (c CapacityConfig) IsEnabled() bool {
	return c.Path != ""
}

// resolvedFormat returns the configured format, treating empty as FormatReport.
func (c CapacityConfig) resolvedFormat() CapacityFormat {
	if c.Format == "" {
		return FormatReport
	}
	return c.Format
}

// Validate rejects a partially-configured capacity source, for the same reason
// the probe does: half-configured reads as enabled while behaving arbitrarily.
func (c CapacityConfig) Validate() error {
	if !c.IsEnabled() {
		return nil
	}
	if c.Path[0] != '/' {
		return fmt.Errorf("routing.capacity.path %q must begin with %q", c.Path, "/")
	}
	if c.Method == "" {
		return fmt.Errorf("routing.capacity.method is required when routing.capacity.path is set")
	}
	if c.Period <= 0 {
		return fmt.Errorf("routing.capacity.period must be positive when routing.capacity.path is set")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("routing.capacity.timeout must be positive when routing.capacity.path is set")
	}
	if c.Timeout >= c.Period {
		return fmt.Errorf("routing.capacity.timeout (%s) must be shorter than routing.capacity.period (%s)",
			c.Timeout, c.Period)
	}
	if c.MaxAge <= 0 {
		// Without a bound the staleness guard cannot fire, and a frozen reporter
		// would pin a home's ceiling indefinitely.
		return fmt.Errorf("routing.capacity.maxAge must be positive when routing.capacity.path is set")
	}
	if c.Samples < 0 {
		return fmt.Errorf("routing.capacity.samples must not be negative")
	}
	if c.Quorum < 0 {
		return fmt.Errorf("routing.capacity.quorum must not be negative")
	}
	if q, n := c.quorum(), c.samples(); q > n {
		// A quorum larger than the window can never be reached, so no reading
		// would ever lower a ceiling and the input would silently do nothing.
		return fmt.Errorf("routing.capacity.quorum (%d) must not exceed routing.capacity.samples (%d)", q, n)
	}
	if c.MaxAge < c.Period {
		// Every report would age out before the next poll, so the ceiling would
		// flap between applied and fallen-open on every cycle.
		return fmt.Errorf("routing.capacity.maxAge (%s) must be at least routing.capacity.period (%s)",
			c.MaxAge, c.Period)
	}
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

// Defaults for the capacity sampling window. They live here, in the package
// that owns the behavior, rather than in the config loader -- the same
// single-sourcing the other control-plane tuning knobs use. These are algorithm
// parameters with a safe universal choice, unlike a probe path or a gateway
// host, which name a specific deployment and so have no default at all.
const (
	// DefaultCapacitySamples smooths over enough polls to see several reporters
	// behind a balancer without making recovery from a low reading slow.
	DefaultCapacitySamples = 10

	// DefaultCapacityQuorum tolerates exactly one misbehaving reporter. Raise
	// it if more than one reporter can plausibly be wrong at the same time.
	DefaultCapacityQuorum = 2
)

// samples is the effective window length.
func (c CapacityConfig) samples() int {
	if c.Samples <= 0 {
		return DefaultCapacitySamples
	}
	return c.Samples
}

// quorum is the effective corroboration count.
//
// Deliberately does not clamp to the window. An unreachable quorum is rejected
// by Validate at startup instead, because quietly lowering it would hand the
// operator a different corroboration guarantee than the one they configured --
// and they would have no way to notice.
func (c CapacityConfig) quorum() int {
	if c.Quorum <= 0 {
		return DefaultCapacityQuorum
	}
	return c.Quorum
}
