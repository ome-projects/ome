package routing

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
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

// policyBlock is one block of the routing policy that an InferenceService may
// replace or clear: the probe block or the capacity block.
type policyBlock interface {
	ProbeConfig | CapacityConfig
	IsEnabled() bool
	Validate() error
}

// effectiveBlock is one InferenceService's effective probe or capacity block.
// Enabled requires the routing gate and a configured block; Digest identifies
// every setting that can change an observation and is empty otherwise.
type effectiveBlock[T policyBlock] struct {
	Enabled bool
	Block   T
	Digest  string
}

// resolveBlock applies one InferenceService's override to the operator's block
// and checks the result. With routing switched off the block is inert and goes
// unchecked. An override replaces the operator's block whole (the zero block
// clears it), and a fault in an override is reported under "spec.". An enabled
// block must also respect the observer's limits before it gets a digest.
func resolveBlock[T policyBlock](
	routingEnabled bool,
	operator T,
	override *T,
	observerLimits func(T) error,
	digest func(T) (string, error),
) (effectiveBlock[T], error) {
	block := operator
	if override != nil {
		block = *override
	}
	if !routingEnabled {
		return effectiveBlock[T]{Block: block}, nil
	}
	if err := block.Validate(); err != nil {
		return effectiveBlock[T]{}, inSpec(err, override != nil)
	}
	if !block.IsEnabled() {
		return effectiveBlock[T]{Block: block}, nil
	}
	if err := observerLimits(block); err != nil {
		return effectiveBlock[T]{}, inSpec(err, override != nil)
	}
	policyDigest, err := digest(block)
	if err != nil {
		return effectiveBlock[T]{}, err
	}
	return effectiveBlock[T]{Enabled: true, Block: block, Digest: policyDigest}, nil
}

// inSpec reports a fault under "spec." when the InferenceService's own block
// caused it, so the message names the object the operator has to fix.
func inSpec(err error, fromSpec bool) error {
	if fromSpec {
		return fmt.Errorf("spec.%w", err)
	}
	return err
}

// routingSpecOf is the InferenceService's routing block, nil when it has none.
func routingSpecOf(spec *v1beta1.InferenceServiceSpec) *v1beta1.RoutingSpec {
	if spec == nil {
		return nil
	}
	return spec.Routing
}

// routingEnabledFor applies the InferenceService's own switch to the
// operator's routing gate: a service may opt out, never in.
func routingEnabledFor(global Config, routing *v1beta1.RoutingSpec) bool {
	if routing != nil && routing.Enabled != nil && !*routing.Enabled {
		return false
	}
	return global.Enabled
}

// probeOverride is what the InferenceService says about the probe block: nil
// when it says nothing, the zero block when it marks the probe Disabled,
// otherwise the inline probe whole.
func probeOverride(routing *v1beta1.RoutingSpec) *ProbeConfig {
	if routing == nil || routing.Probe == nil {
		return nil
	}
	override := ProbeConfig{}
	if !routing.Probe.Disabled {
		override = probeConfigFromSpec(routing.Probe)
	}
	return &override
}

// capacityOverride is what the InferenceService says about the capacity block,
// read by the same rule as probeOverride.
func capacityOverride(routing *v1beta1.RoutingSpec) *CapacityConfig {
	if routing == nil || routing.Capacity == nil {
		return nil
	}
	override := CapacityConfig{}
	if !routing.Capacity.Disabled {
		override = capacityConfigFromSpec(routing.Capacity)
	}
	return &override
}

// blockResolver names what the probe and capacity resolvers do differently:
// the block the InferenceService wrote by itself, the operator's copy of the
// block, the override read from the spec, the observer limit the block must
// respect and its digest. resolve runs the one sequence they share.
type blockResolver[T policyBlock] struct {
	// own is the InferenceService's block by itself, nil when it says nothing.
	own      func(*v1beta1.RoutingSpec) *v1beta1.RoutingSpec
	operator func(Config) T
	override func(*v1beta1.RoutingSpec) *T
	limits   func(ObserverConfig, T) error
	digest   func(T) (string, error)
}

// resolve checks the InferenceService's own block alone, so a fault in the
// other block never fails this one, then applies it to the operator's block.
func (b blockResolver[T]) resolve(global Config, spec *v1beta1.InferenceServiceSpec) (effectiveBlock[T], error) {
	routing := routingSpecOf(spec)
	if own := b.own(routing); own != nil {
		if err := validation.ValidateRouting(&v1beta1.InferenceServiceSpec{Routing: own}); err != nil {
			return effectiveBlock[T]{}, err
		}
	}
	return resolveBlock(
		routingEnabledFor(global, routing),
		b.operator(global),
		b.override(routing),
		func(block T) error { return b.limits(global.Observer, block) },
		b.digest,
	)
}

var probeResolver = blockResolver[ProbeConfig]{
	own: func(routing *v1beta1.RoutingSpec) *v1beta1.RoutingSpec {
		if routing == nil || routing.Probe == nil {
			return nil
		}
		return &v1beta1.RoutingSpec{Probe: routing.Probe}
	},
	operator: func(global Config) ProbeConfig { return cloneProbeConfig(global.Probe) },
	override: probeOverride,
	limits:   ObserverConfig.probeLimits,
	digest:   probePolicyDigest,
}

var capacityResolver = blockResolver[CapacityConfig]{
	own: func(routing *v1beta1.RoutingSpec) *v1beta1.RoutingSpec {
		if routing == nil || routing.Capacity == nil {
			return nil
		}
		return &v1beta1.RoutingSpec{Capacity: routing.Capacity}
	},
	operator: func(global Config) CapacityConfig { return cloneCapacityConfig(global.Capacity) },
	override: capacityOverride,
	limits:   ObserverConfig.capacityLimits,
	digest:   capacityPolicyDigest,
}

// ResolveProbePolicy resolves only the probe portion of an InferenceService's
// routing policy. Capacity and publisher overrides are independent and do not
// participate in probe resolution.
func ResolveProbePolicy(global Config, spec *v1beta1.InferenceServiceSpec) (ResolvedProbePolicy, error) {
	resolved, err := probeResolver.resolve(global, spec)
	if err != nil {
		return ResolvedProbePolicy{}, err
	}
	return ResolvedProbePolicy{Enabled: resolved.Enabled, Probe: resolved.Block, PolicyDigest: resolved.Digest}, nil
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
	resolved, err := capacityResolver.resolve(global, spec)
	if err != nil {
		return ResolvedCapacityPolicy{}, err
	}
	return ResolvedCapacityPolicy{Enabled: resolved.Enabled, Capacity: resolved.Block, PolicyDigest: resolved.Digest}, nil
}

type capacityPolicyOption struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type capacityPolicyDigestPayload struct {
	Mode               v1beta1.RoutingCapacityMode `json:"mode,omitempty"`
	Version            int                         `json:"version"`
	Path               string                      `json:"path"`
	Method             string                      `json:"method"`
	Format             CapacityFormat              `json:"format"`
	Options            []capacityPolicyOption      `json:"options"`
	PeriodNanoseconds  int64                       `json:"periodNanoseconds"`
	TimeoutNanoseconds int64                       `json:"timeoutNanoseconds"`
	Samples            int                         `json:"samples"`
	Quorum             int                         `json:"quorum"`
	MaxAgeNanoseconds  int64                       `json:"maxAgeNanoseconds"`
}

func capacityPolicyDigest(capacity CapacityConfig) (string, error) {
	if capacity.Mode == v1beta1.RoutingCapacityModeCeiling {
		capacity.Mode = ""
	}
	payload, err := json.Marshal(capacityPolicyDigestPayload{
		Mode:               capacity.Mode,
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
	canonical := slices.Clone(statuses)
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
	routing := routingSpecOf(spec)
	if routing == nil {
		if err := resolved.Validate(); err != nil {
			return Config{}, err
		}
		return resolved, nil
	}

	resolved.Enabled = routingEnabledFor(global, routing)
	if !resolved.Enabled {
		return resolved, nil
	}
	if err := applyBlockOverrides(&resolved, routing); err != nil {
		return Config{}, err
	}
	if err := applyPublisherOptions(&resolved, global, routing, resolvePublisherOptions); err != nil {
		return Config{}, err
	}
	return resolved, nil
}

// applyBlockOverrides replaces the operator's probe and capacity blocks with
// the ones the InferenceService wrote and checks the result. A fault in a
// block the service wrote is reported under "spec."; one in a block it
// inherited names the operator's configuration.
func applyBlockOverrides(resolved *Config, routing *v1beta1.RoutingSpec) error {
	probe, capacity := probeOverride(routing), capacityOverride(routing)
	if probe != nil {
		resolved.Probe = *probe
	}
	if capacity != nil {
		resolved.Capacity = *capacity
	}
	err := resolved.Validate()
	if err == nil {
		return nil
	}
	var configErr *configValidationError
	fromSpec := errors.As(err, &configErr) &&
		((configErr.section == "probe" && probe != nil) ||
			(configErr.section == "capacity" && capacity != nil))
	return inSpec(err, fromSpec)
}

// applyPublisherOptions merges the InferenceService's publisher options
// through the publisher's own resolver. A publisher without one refuses
// per-InferenceService options.
func applyPublisherOptions(
	resolved *Config,
	global Config,
	routing *v1beta1.RoutingSpec,
	resolvePublisherOptions PublisherOptionsResolver,
) error {
	if routing.Publisher == nil || len(routing.Publisher.Options) == 0 {
		return nil
	}
	if resolvePublisherOptions == nil {
		return fmt.Errorf(
			"spec.routing.publisher.options cannot be set because the selected publisher does not support per-InferenceService options")
	}
	options, err := resolvePublisherOptions(
		maps.Clone(global.Publisher.Options),
		maps.Clone(routing.Publisher.Options),
	)
	if err != nil {
		return fmt.Errorf("spec.routing.publisher.options: %w", err)
	}
	resolved.Publisher.Options = maps.Clone(options)
	return nil
}

func cloneConfig(in Config) Config {
	out := in
	out.Probe = cloneProbeConfig(in.Probe)
	out.Capacity = cloneCapacityConfig(in.Capacity)
	out.Publisher.Options = maps.Clone(in.Publisher.Options)
	return out
}

func cloneProbeConfig(in ProbeConfig) ProbeConfig {
	out := in
	out.AcceptStatuses = slices.Clone(in.AcceptStatuses)
	out.GateStatuses = slices.Clone(in.GateStatuses)
	return out
}

func cloneCapacityConfig(in CapacityConfig) CapacityConfig {
	out := in
	out.Options = maps.Clone(in.Options)
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
		Mode:    in.Mode,
		Path:    in.Path,
		Method:  in.Method,
		Format:  CapacityFormat(in.Format),
		Options: maps.Clone(in.Options),
		Period:  in.Period.Duration,
		Timeout: in.Timeout.Duration,
		Samples: int(in.Samples),
		Quorum:  int(in.Quorum),
		MaxAge:  in.MaxAge.Duration,
	}
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
