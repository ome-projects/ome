package routing

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// CapacityFormat names the response shape a home's capacity endpoint answers
// with.
type CapacityFormat string

// FormatReport is the built-in shape: an absolute servable count plus an
// observedAt stamp the home sets itself. It is the default, the only format
// this repository guarantees, and the one a producer should emit unless it
// needs a different shape.
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
	// Decode turns a response body into a report in servable units, or returns
	// the reason the control-plane plan should stand instead. It must never
	// return a report it is unsure of: a plausible wrong ceiling silently
	// mis-weights a home, where falling open merely keeps the plan.
	Decode func(body []byte, cfg CapacityConfig) (CapacityReport, string)

	// Validate rejects a configuration this format cannot serve, at startup
	// rather than at the first poll.
	Validate func(cfg CapacityConfig) error

	// StampsOwnTime reports whether the producer timestamps its reading. When
	// false the poller stamps at receipt, which makes MaxAge inert for this
	// format: a home wedged while still answering holds its ceiling. Surfaced
	// so an operator can see that trade rather than discover it.
	StampsOwnTime bool
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
		Decode:        decodeReport,
		Validate:      validateReport,
		StampsOwnTime: true,
	}); err != nil {
		panic(fmt.Sprintf("registering the built-in capacity format: %v", err))
	}
}

// decodeReport reads the built-in shape, which carries its own stamp.
func decodeReport(body []byte, _ CapacityConfig) (CapacityReport, string) {
	var report CapacityReport
	if err := json.Unmarshal(body, &report); err != nil {
		return report, fmt.Sprintf("capacity response is not a valid report: %v", err)
	}
	if report.ObservedAt.IsZero() {
		// Without a stamp a frozen reporter is indistinguishable from a fresh
		// one, so the staleness guard could never fire.
		return report, "capacity report has no observedAt stamp"
	}
	return report, ""
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
