package routing

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// CapacityFormat names the response shape a cluster's capacity endpoint answers
// with.
type CapacityFormat string

// FormatReport carries a whole replica count (servable) for Ceiling mode or a
// replica weight that may be fractional (weight) for Reported mode, plus an
// observedAt stamp the cluster sets itself.
const FormatReport CapacityFormat = "Report"

// CapacityFormatPlugin is one response shape the poller can read.
//
// A format is not merely a parse target. Where a number lives is the easy part;
// what it means is what decides whether a ceiling is safe. So a plugin owns
// both: it decodes, and it validates its own configuration — including whether
// the reading carries a usable timestamp, and whether it needs converting into
// servable replicas before it can be compared with the plan.
//
// +kubebuilder:object:generate=false
type CapacityFormatPlugin struct {
	// Decode returns capacity in replicas, whole for Ceiling mode and possibly
	// fractional for Reported mode, or a failure reason.
	// The supplied config, including Options, is a private copy for this call.
	Decode func(body []byte, cfg CapacityConfig) (CapacityReport, string)

	// Validate rejects a configuration this format cannot serve, at startup
	// rather than at the first poll.
	Validate func(cfg CapacityConfig) error
}

// capacityFormatRegistry holds the formats compiled into this binary.
type capacityFormatRegistry struct {
	mu      sync.RWMutex
	plugins map[CapacityFormat]CapacityFormatPlugin
}

var formats = &capacityFormatRegistry{plugins: map[CapacityFormat]CapacityFormatPlugin{}}

// RegisterCapacityFormat adds a response format to the registry. It is intended
// for an init() in an optional package, so a deployment can compile in formats
// this repository does not define.
//
// Registering a duplicate is an error rather than an overwrite: two packages
// silently competing for one name would make the active decoder depend on
// import order.
func RegisterCapacityFormat(name CapacityFormat, p CapacityFormatPlugin) error {
	if name == "" {
		return fmt.Errorf("capacity format name must not be empty")
	}
	if p.Decode == nil {
		return fmt.Errorf("capacity format %q must provide a Decode function", name)
	}
	formats.mu.Lock()
	defer formats.mu.Unlock()
	if _, dup := formats.plugins[name]; dup {
		return fmt.Errorf("capacity format %q is already registered", name)
	}
	formats.plugins[name] = p
	return nil
}

// lookupCapacityFormat returns the plugin for a format name.
func lookupCapacityFormat(name CapacityFormat) (CapacityFormatPlugin, bool) {
	formats.mu.RLock()
	defer formats.mu.RUnlock()
	p, ok := formats.plugins[name]
	return p, ok
}

// RegisteredCapacityFormats lists the formats compiled into this binary, sorted.
// The manager logs it at startup so a build missing an optional format is
// visible immediately rather than at the first poll.
func RegisteredCapacityFormats() []string {
	formats.mu.RLock()
	defer formats.mu.RUnlock()
	names := make([]string, 0, len(formats.plugins))
	for n := range formats.plugins {
		names = append(names, string(n))
	}
	sort.Strings(names)
	return names
}

func init() {
	// The built-in format. Registered here rather than special-cased so that
	// lookup, validation and the "not registered" error path are the same for
	// every format, including this one.
	if err := RegisterCapacityFormat(FormatReport, CapacityFormatPlugin{
		Decode:   decodeReport,
		Validate: validateReport,
	}); err != nil {
		panic(fmt.Sprintf("registering the built-in capacity format: %v", err))
	}
}

// decodeReport reads the built-in shape, which carries its own stamp. Each mode
// decodes only the field it reads, so a value of any type in the other mode's
// field cannot invalidate the report.
func decodeReport(body []byte, cfg CapacityConfig) (CapacityReport, string) {
	if cfg.Mode == v1beta1.RoutingCapacityModeReported {
		var wire struct {
			Weight     *float64  `json:"weight"`
			ObservedAt time.Time `json:"observedAt"`
		}
		if err := json.Unmarshal(body, &wire); err != nil {
			return CapacityReport{}, fmt.Sprintf("capacity response is not a valid report: %v", err)
		}
		if wire.Weight == nil {
			return CapacityReport{}, "capacity report has no weight"
		}
		if wire.ObservedAt.IsZero() {
			return CapacityReport{}, "capacity report has no observedAt stamp"
		}
		return CapacityReport{Weight: wire.Weight, ObservedAt: wire.ObservedAt}, ""
	}
	var wire struct {
		Servable   *int32    `json:"servable"`
		ObservedAt time.Time `json:"observedAt"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return CapacityReport{}, fmt.Sprintf("capacity response is not a valid report: %v", err)
	}
	if wire.Servable == nil {
		return CapacityReport{}, "capacity report has no servable count"
	}
	if wire.ObservedAt.IsZero() {
		// Without a stamp a frozen reporter is indistinguishable from a fresh
		// one, so the staleness guard could never fire.
		return CapacityReport{}, "capacity report has no observedAt stamp"
	}
	return CapacityReport{Servable: wire.Servable, ObservedAt: wire.ObservedAt}, ""
}

// validateReport rejects options this format has no use for. The report already
// states servable units, so a conversion factor would silently scale a number
// that needs no scaling.
func validateReport(cfg CapacityConfig) error {
	if len(cfg.Options) > 0 {
		return fmt.Errorf("routing.capacity.options is not accepted for format %q", FormatReport)
	}
	return nil
}
