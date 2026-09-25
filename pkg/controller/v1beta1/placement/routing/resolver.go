package routing

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

// PublisherOptionsResolver is provided by the operator-selected publisher. It
// combines operator-level and per-InferenceService options and validates the
// result against publisher-specific semantics, including the authority scope
// an InferenceService may target. Both input maps are private copies. The
// returned map is copied again before it enters the effective Config.
type PublisherOptionsResolver func(
	globalOptions map[string]string,
	inlineOptions map[string]string,
) (map[string]string, error)

// ResolvedProbePolicy is one InferenceService's effective endpoint probe
// policy. Enabled requires both the operator-level routing gate and a configured
// effective probe. PolicyDigest identifies every setting that can affect probe
// observations or routing decisions and is empty when Enabled is false.
type ResolvedProbePolicy struct {
	Enabled      bool
	Probe        ProbeConfig
	PolicyDigest string
}

// ResolvedCapacityPolicy is one InferenceService's effective capacity polling
// policy. Enabled requires both the operator-level routing gate and a configured
// effective capacity source. PolicyDigest identifies every setting that can
// affect capacity observations and is empty when Enabled is false.
type ResolvedCapacityPolicy struct {
	Enabled      bool
	Capacity     CapacityConfig
	PolicyDigest string
}

// IsEnabled reports whether this policy should run endpoint probes.
func (p ResolvedProbePolicy) IsEnabled() bool {
	return p.Enabled
}

// IsEnabled reports whether this policy should poll endpoint capacity.
func (p ResolvedCapacityPolicy) IsEnabled() bool {
	return p.Enabled
}

// ResolveProbePolicy resolves only the probe portion of an InferenceService's
// routing policy. Capacity and publisher overrides are independent and do not
// participate in probe resolution.
func ResolveProbePolicy(global Config, spec *v1beta1.InferenceServiceSpec) (ResolvedProbePolicy, error) {
	probe := cloneProbeConfig(global.Probe)
	routingEnabled := global.Enabled
	var inline *v1beta1.RoutingProbeSpec
	if spec != nil && spec.Routing != nil {
		routing := spec.Routing
		if routing.Enabled != nil && !*routing.Enabled {
			routingEnabled = false
		}
		inline = routing.Probe
	}

	if inline != nil {
		if err := validation.ValidateRouting(&v1beta1.InferenceServiceSpec{
			Routing: &v1beta1.RoutingSpec{Probe: inline},
		}); err != nil {
			return ResolvedProbePolicy{}, err
		}
		if inline.Disabled {
			probe = ProbeConfig{}
		} else {
			probe = probeConfigFromSpec(inline)
		}
	}

	if !routingEnabled {
		return ResolvedProbePolicy{Probe: probe}, nil
	}
	if err := probe.Validate(); err != nil {
		if inline != nil {
			return ResolvedProbePolicy{}, fmt.Errorf("spec.%w", err)
		}
		return ResolvedProbePolicy{}, err
	}
	if !probe.IsEnabled() {
		return ResolvedProbePolicy{Probe: probe}, nil
	}
	if global.Observer.MinPeriod > 0 && probe.Period < global.Observer.MinPeriod {
		err := fmt.Errorf("routing.probe.period (%s) must be at least routing.observer.minPeriod (%s)",
			probe.Period, global.Observer.MinPeriod)
		if inline != nil {
			return ResolvedProbePolicy{}, fmt.Errorf("spec.%w", err)
		}
		return ResolvedProbePolicy{}, err
	}

	digest, err := probePolicyDigest(probe)
	if err != nil {
		return ResolvedProbePolicy{}, err
	}
	return ResolvedProbePolicy{
		Enabled:      true,
		Probe:        probe,
		PolicyDigest: digest,
	}, nil
}

type probePolicyDigestPayload struct {
	Version            int             `json:"version"`
	Path               string          `json:"path"`
	Method             string          `json:"method"`
	AcceptStatuses     []int           `json:"acceptStatuses"`
	GateStatuses       []int           `json:"gateStatuses"`
	PeriodNanoseconds  int64           `json:"periodNanoseconds"`
	TimeoutNanoseconds int64           `json:"timeoutNanoseconds"`
	FailureThreshold   int             `json:"failureThreshold"`
	SuccessThreshold   int             `json:"successThreshold"`
	AllFailedPolicy    AllFailedPolicy `json:"allFailedPolicy"`
}

func probePolicyDigest(probe ProbeConfig) (string, error) {
	payload, err := json.Marshal(probePolicyDigestPayload{
		Version:            1,
		Path:               probe.Path,
		Method:             probe.Method,
		AcceptStatuses:     canonicalStatuses(probe.AcceptStatuses),
		GateStatuses:       canonicalStatuses(probe.GateStatuses),
		PeriodNanoseconds:  int64(probe.Period),
		TimeoutNanoseconds: int64(probe.Timeout),
		FailureThreshold:   probe.FailureThreshold,
		SuccessThreshold:   probe.SuccessThreshold,
		AllFailedPolicy:    probe.AllFailedPolicy,
	})
	if err != nil {
		return "", fmt.Errorf("encode probe policy digest: %w", err)
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%x", sum), nil
}

// ResolveCapacityPolicy resolves only the capacity portion of an
// InferenceService's routing policy. Probe and publisher overrides are
// independent and do not participate in capacity resolution.
func ResolveCapacityPolicy(global Config, spec *v1beta1.InferenceServiceSpec) (ResolvedCapacityPolicy, error) {
	capacity := cloneCapacityConfig(global.Capacity)
	routingEnabled := global.Enabled
	var inline *v1beta1.RoutingCapacitySpec
	if spec != nil && spec.Routing != nil {
		routing := spec.Routing
		if routing.Enabled != nil && !*routing.Enabled {
			routingEnabled = false
		}
		inline = routing.Capacity
	}

	if inline != nil {
		if err := validation.ValidateRouting(&v1beta1.InferenceServiceSpec{
			Routing: &v1beta1.RoutingSpec{Capacity: inline},
		}); err != nil {
			return ResolvedCapacityPolicy{}, err
		}
		if inline.Disabled {
			capacity = CapacityConfig{}
		} else {
			capacity = capacityConfigFromSpec(inline)
		}
	}

	if !routingEnabled {
		return ResolvedCapacityPolicy{Capacity: capacity}, nil
	}
	if err := capacity.Validate(); err != nil {
		if inline != nil {
			return ResolvedCapacityPolicy{}, fmt.Errorf("spec.%w", err)
		}
		return ResolvedCapacityPolicy{}, err
	}
	if !capacity.IsEnabled() {
		return ResolvedCapacityPolicy{Capacity: capacity}, nil
	}
	if global.Observer.MinPeriod > 0 && capacity.Period < global.Observer.MinPeriod {
		err := fmt.Errorf("routing.capacity.period (%s) must be at least routing.observer.minPeriod (%s)",
			capacity.Period, global.Observer.MinPeriod)
		if inline != nil {
			return ResolvedCapacityPolicy{}, fmt.Errorf("spec.%w", err)
		}
		return ResolvedCapacityPolicy{}, err
	}
	if global.Observer.MaxSamples > 0 && capacity.samples() > global.Observer.MaxSamples {
		err := fmt.Errorf("routing.capacity.samples (%d) must not exceed routing.observer.maxSamples (%d)",
			capacity.samples(), global.Observer.MaxSamples)
		if inline != nil {
			return ResolvedCapacityPolicy{}, fmt.Errorf("spec.%w", err)
		}
		return ResolvedCapacityPolicy{}, err
	}

	digest, err := capacityPolicyDigest(capacity)
	if err != nil {
		return ResolvedCapacityPolicy{}, err
	}
	return ResolvedCapacityPolicy{
		Enabled:      true,
		Capacity:     capacity,
		PolicyDigest: digest,
	}, nil
}

type capacityPolicyOption struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type capacityPolicyDigestPayload struct {
	Version            int                    `json:"version"`
	Path               string                 `json:"path"`
	Method             string                 `json:"method"`
	Format             CapacityFormat         `json:"format"`
	Options            []capacityPolicyOption `json:"options"`
	PeriodNanoseconds  int64                  `json:"periodNanoseconds"`
	TimeoutNanoseconds int64                  `json:"timeoutNanoseconds"`
	Samples            int                    `json:"samples"`
	Quorum             int                    `json:"quorum"`
	MaxAgeNanoseconds  int64                  `json:"maxAgeNanoseconds"`
}

func capacityPolicyDigest(capacity CapacityConfig) (string, error) {
	payload, err := json.Marshal(capacityPolicyDigestPayload{
		Version:            1,
		Path:               capacity.Path,
		Method:             capacity.Method,
		Format:             capacity.Format,
		Options:            canonicalCapacityOptions(capacity.Options),
		PeriodNanoseconds:  int64(capacity.Period),
		TimeoutNanoseconds: int64(capacity.Timeout),
		Samples:            capacity.Samples,
		Quorum:             capacity.Quorum,
		MaxAgeNanoseconds:  int64(capacity.MaxAge),
	})
	if err != nil {
		return "", fmt.Errorf("encode capacity policy digest: %w", err)
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%x", sum), nil
}

func canonicalCapacityOptions(options map[string]string) []capacityPolicyOption {
	if len(options) == 0 {
		return nil
	}
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	canonical := make([]capacityPolicyOption, 0, len(keys))
	for _, key := range keys {
		canonical = append(canonical, capacityPolicyOption{Key: key, Value: options[key]})
	}
	return canonical
}

func canonicalStatuses(statuses []int) []int {
	if len(statuses) == 0 {
		return nil
	}
	canonical := cloneInts(statuses)
	sort.Ints(canonical)
	n := 1
	for _, status := range canonical[1:] {
		if status != canonical[n-1] {
			canonical[n] = status
			n++
		}
	}
	return canonical[:n]
}

// ResolveConfig combines the operator routing configuration with one
// InferenceService's inline overrides. The returned config shares no mutable
// maps or slices with either input.
//
// The operator-level enabled flag is a hard gate: an InferenceService may opt
// out, but it cannot turn on routing for an installation where it is disabled.
// Probe and capacity blocks replace their global counterparts as a whole.
// Publisher identity stays global; its implementation owns the merge and
// semantic validation of per-InferenceService options.
func ResolveConfig(
	global Config,
	spec *v1beta1.InferenceServiceSpec,
	resolvePublisherOptions PublisherOptionsResolver,
) (Config, error) {
	resolved := cloneConfig(global)
	if err := validation.ValidateRouting(spec); err != nil {
		return Config{}, err
	}
	if spec == nil || spec.Routing == nil {
		if err := resolved.Validate(); err != nil {
			return Config{}, err
		}
		return resolved, nil
	}
	routingSpec := spec.Routing

	if routingSpec.Enabled != nil && !*routingSpec.Enabled {
		resolved.Enabled = false
	}
	if !resolved.Enabled {
		return resolved, nil
	}

	if routingSpec.Probe != nil {
		if routingSpec.Probe.Disabled {
			resolved.Probe = ProbeConfig{}
		} else {
			resolved.Probe = probeConfigFromSpec(routingSpec.Probe)
		}
	}
	if routingSpec.Capacity != nil {
		if routingSpec.Capacity.Disabled {
			resolved.Capacity = CapacityConfig{}
		} else {
			resolved.Capacity = capacityConfigFromSpec(routingSpec.Capacity)
		}
	}
	if err := resolved.Validate(); err != nil {
		var configErr *configValidationError
		if errors.As(err, &configErr) &&
			((configErr.section == "probe" && routingSpec.Probe != nil) ||
				(configErr.section == "capacity" && routingSpec.Capacity != nil)) {
			return Config{}, fmt.Errorf("spec.%w", err)
		}
		return Config{}, err
	}

	if routingSpec.Publisher == nil || len(routingSpec.Publisher.Options) == 0 {
		return resolved, nil
	}
	if resolvePublisherOptions == nil {
		return Config{}, fmt.Errorf(
			"spec.routing.publisher.options cannot be set because the selected publisher does not support per-InferenceService options")
	}
	options, err := resolvePublisherOptions(
		cloneStringMap(global.Publisher.Options),
		cloneStringMap(routingSpec.Publisher.Options),
	)
	if err != nil {
		return Config{}, fmt.Errorf("spec.routing.publisher.options: %w", err)
	}
	resolved.Publisher.Options = cloneStringMap(options)

	return resolved, nil
}

func cloneConfig(in Config) Config {
	out := in
	out.Probe = cloneProbeConfig(in.Probe)
	out.Capacity = cloneCapacityConfig(in.Capacity)
	out.Publisher.Options = cloneStringMap(in.Publisher.Options)
	return out
}

func cloneProbeConfig(in ProbeConfig) ProbeConfig {
	out := in
	out.AcceptStatuses = cloneInts(in.AcceptStatuses)
	out.GateStatuses = cloneInts(in.GateStatuses)
	return out
}

func cloneCapacityConfig(in CapacityConfig) CapacityConfig {
	out := in
	out.Options = cloneStringMap(in.Options)
	return out
}

func probeConfigFromSpec(in *v1beta1.RoutingProbeSpec) ProbeConfig {
	return ProbeConfig{
		Path:             in.Path,
		Method:           in.Method,
		AcceptStatuses:   int32sToInts(in.AcceptStatuses),
		GateStatuses:     int32sToInts(in.GateStatuses),
		Period:           in.Period.Duration,
		Timeout:          in.Timeout.Duration,
		FailureThreshold: int(in.FailureThreshold),
		SuccessThreshold: int(in.SuccessThreshold),
		AllFailedPolicy:  AllFailedPolicy(in.AllFailedPolicy),
	}
}

func capacityConfigFromSpec(in *v1beta1.RoutingCapacitySpec) CapacityConfig {
	return CapacityConfig{
		Path:    in.Path,
		Method:  in.Method,
		Format:  CapacityFormat(in.Format),
		Options: cloneStringMap(in.Options),
		Period:  in.Period.Duration,
		Timeout: in.Timeout.Duration,
		Samples: int(in.Samples),
		Quorum:  int(in.Quorum),
		MaxAge:  in.MaxAge.Duration,
	}
}

func cloneInts(in []int) []int {
	if in == nil {
		return nil
	}
	return append([]int(nil), in...)
}

func int32sToInts(in []int32) []int {
	if in == nil {
		return nil
	}
	out := make([]int, len(in))
	for i, value := range in {
		out[i] = int(value)
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
