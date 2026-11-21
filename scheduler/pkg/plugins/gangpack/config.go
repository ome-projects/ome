package gangpack

import (
	"fmt"
	"math"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

// Args configures behavior that cannot be inferred from an individual gang.
// TopologyKey is the fallback for PodGroups without a per-gang topology
// annotation.
type Args struct {
	TopologyKey                    string `json:"topologyKey,omitempty"`
	PodGroupTopologyKeyAnnotation  string `json:"podGroupTopologyKeyAnnotation,omitempty"`
	UnsupportedPlacementGroupLabel string `json:"unsupportedPlacementGroupLabel,omitempty"`
	DefaultPermitTimeoutSeconds    *int64 `json:"defaultPermitTimeoutSeconds,omitempty"`
	PodGroupSyncTimeoutSeconds     *int64 `json:"podGroupSyncTimeoutSeconds,omitempty"`
	GCIntervalSeconds              *int64 `json:"gcIntervalSeconds,omitempty"`
	// StandaloneDomainPacking enables the domain-level bin-packing Score for
	// unassigned (non-gang) whole-node pods. Defaults to true when unset, and only
	// takes effect when a pool-wide TopologyKey is configured.
	StandaloneDomainPacking *bool `json:"standaloneDomainPacking,omitempty"`
	// Preemption lets a gang that fits no domain free one by evicting whole
	// lower-priority gangs and pods there. Unset leaves preemption off.
	Preemption *PreemptionArgs `json:"preemption,omitempty"`
}

// PreemptionArgs configures gang preemption.
type PreemptionArgs struct {
	// MaxInFlight caps how many gangs may wait at once for a domain that
	// preemption is freeing. Required, and at least 1.
	MaxInFlight *int32 `json:"maxInFlight,omitempty"`
}

// check reports a preemption setting that cannot be used.
func (p *PreemptionArgs) check() error {
	if p == nil {
		return nil
	}
	if p.MaxInFlight == nil {
		return fmt.Errorf("preemption.maxInFlight must be configured when preemption is set")
	}
	if *p.MaxInFlight < 1 {
		return fmt.Errorf("preemption.maxInFlight must be at least 1")
	}
	return nil
}

// maxPreemptionsInFlight is the configured cap, or zero when preemption is off.
func (a Args) maxPreemptionsInFlight() int {
	if a.Preemption == nil || a.Preemption.MaxInFlight == nil {
		return 0
	}
	return int(*a.Preemption.MaxInFlight)
}

// standaloneDomainPackingEnabled reports the effective toggle: on unless the
// operator explicitly disables it.
func (a Args) standaloneDomainPackingEnabled() bool {
	return a.StandaloneDomainPacking == nil || *a.StandaloneDomainPacking
}

// secondsArgument is one duration argument given in whole seconds. A set
// value must be positive and fit a time.Duration; a required one must be set.
type secondsArgument struct {
	name     string
	value    *int64
	required bool
}

// check is the one rule every seconds argument follows.
func (a secondsArgument) check() error {
	if a.value == nil {
		if a.required {
			return fmt.Errorf("%s must be configured", a.name)
		}
		return nil
	}
	if *a.value <= 0 {
		return fmt.Errorf("%s must be positive", a.name)
	}
	if *a.value > math.MaxInt64/int64(time.Second) {
		return fmt.Errorf("%s is too large", a.name)
	}
	return nil
}

// secondsArguments lists the duration arguments in the order they are checked.
func (a Args) secondsArguments() []secondsArgument {
	return []secondsArgument{
		{name: "defaultPermitTimeoutSeconds", value: a.DefaultPermitTimeoutSeconds},
		{name: "podGroupSyncTimeoutSeconds", value: a.PodGroupSyncTimeoutSeconds},
		{name: "gcIntervalSeconds", value: a.GCIntervalSeconds, required: true},
	}
}

func decodeArgs(obj runtime.Object) (Args, error) {
	var args Args
	if err := frameworkruntime.DecodeInto(obj, &args); err != nil {
		return Args{}, fmt.Errorf("decode %s args: %w", Name, err)
	}
	if args.TopologyKey != "" {
		if errs := validation.IsQualifiedName(args.TopologyKey); len(errs) > 0 {
			return Args{}, fmt.Errorf("topologyKey %q is not a valid node label key: %s", args.TopologyKey, errs[0])
		}
	}
	for name, value := range map[string]string{
		"podGroupTopologyKeyAnnotation":  args.PodGroupTopologyKeyAnnotation,
		"unsupportedPlacementGroupLabel": args.UnsupportedPlacementGroupLabel,
	} {
		if value == "" {
			continue
		}
		if errs := validation.IsQualifiedName(value); len(errs) > 0 {
			return Args{}, fmt.Errorf("%s %q is not a valid metadata key: %s", name, value, errs[0])
		}
	}
	for _, seconds := range args.secondsArguments() {
		if err := seconds.check(); err != nil {
			return Args{}, err
		}
	}
	if err := args.Preemption.check(); err != nil {
		return Args{}, err
	}
	return args, nil
}

func (a Args) podGroupSyncTimeout() time.Duration {
	if a.PodGroupSyncTimeoutSeconds == nil {
		return 0
	}
	return time.Duration(*a.PodGroupSyncTimeoutSeconds) * time.Second
}

func (a Args) gcInterval() time.Duration {
	if a.GCIntervalSeconds == nil {
		return 0
	}
	return time.Duration(*a.GCIntervalSeconds) * time.Second
}

func (a Args) defaultPermitTimeout() time.Duration {
	if a.DefaultPermitTimeoutSeconds == nil {
		return 0
	}
	return time.Duration(*a.DefaultPermitTimeoutSeconds) * time.Second
}
