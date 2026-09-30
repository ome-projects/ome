package controllerconfig

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// RolloutConfigName is the inferenceservice-config ConfigMap key holding the
// operator-level rollout tuning block.
const RolloutConfigName = "rollout"

// +kubebuilder:object:generate=false
// RolloutConfig holds operator-level tuning for rollout runs. There are
// intentionally NO in-code defaults — values are supplied via the
// inferenceservice-config ConfigMap (Helm chart / GitOps); an absent block
// yields zero values and each consumer keeps its own documented fallback.
type RolloutConfig struct {
	// MaxPinnedPlanBytes caps the serialized size of the effective plan
	// pinned into a rollout run's status. Zero means uncapped.
	MaxPinnedPlanBytes int `json:"maxPinnedPlanBytes,omitempty"`

	// DefaultReadyTimeout is the fleet-default readiness deadline applied
	// when a rollout does not set its own. Stored parsed from the ConfigMap's
	// duration string; zero means no configured default (the consumer keeps
	// its documented fallback).
	DefaultReadyTimeout time.Duration `json:"-"`

	// CanaryRequeue is how soon the canary executor re-checks a step in
	// progress: capacity coming up, a timed pause, a drain window. Zero means
	// unconfigured, and the executor then asks for the controller's
	// rate-limited requeue instead of a fixed cadence.
	CanaryRequeue time.Duration `json:"-"`

	// CanaryParkedRequeue is the heartbeat of a parked (Failed) canary, which
	// only an operator or a new target moves. Zero means unconfigured, with
	// the same rate-limited fallback.
	CanaryParkedRequeue time.Duration `json:"-"`
}

// rolloutConfigJSON is the ConfigMap wire shape of the block: durations are
// strings ("15m"), parsed at load.
type rolloutConfigJSON struct {
	MaxPinnedPlanBytes  int    `json:"maxPinnedPlanBytes,omitempty"`
	DefaultReadyTimeout string `json:"defaultReadyTimeout,omitempty"`
	CanaryRequeue       string `json:"canaryRequeue,omitempty"`
	CanaryParkedRequeue string `json:"canaryParkedRequeue,omitempty"`
}

// NewRolloutConfig loads the block once (startup paths).
func NewRolloutConfig(clientset kubernetes.Interface) (*RolloutConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseRolloutConfig(configMap)
}

// NewRolloutConfigCached is the ConfigCache-backed variant of NewRolloutConfig
// used on the reconcile hot path.
func NewRolloutConfigCached(cache *ConfigCache, clientset kubernetes.Interface) (*RolloutConfig, error) {
	configMap, err := cache.get(clientset)
	if err != nil {
		return nil, err
	}
	return parseRolloutConfig(configMap)
}

// parseRolloutConfig parses the block. An empty, malformed, or non-positive
// defaultReadyTimeout degrades to zero — "no configured default" — rather
// than failing the load, mirroring the canaryAnalysis duration handling. A
// negative maxPinnedPlanBytes can never be a sane cap, so it is a config
// error surfaced at load time.
func parseRolloutConfig(configMap *v1.ConfigMap) (*RolloutConfig, error) {
	raw := &rolloutConfigJSON{}
	if err := getComponentConfig(RolloutConfigName, configMap, raw); err != nil {
		return nil, fmt.Errorf("unable to parse rollout config json: %w", err)
	}
	if raw.MaxPinnedPlanBytes < 0 {
		return nil, fmt.Errorf("invalid rollout config, maxPinnedPlanBytes must be >= 0, got %d", raw.MaxPinnedPlanBytes)
	}
	cfg := &RolloutConfig{MaxPinnedPlanBytes: raw.MaxPinnedPlanBytes}
	cfg.DefaultReadyTimeout = positiveDuration(raw.DefaultReadyTimeout)
	cfg.CanaryRequeue = positiveDuration(raw.CanaryRequeue)
	cfg.CanaryParkedRequeue = positiveDuration(raw.CanaryParkedRequeue)
	return cfg, nil
}

// positiveDuration parses a ConfigMap duration string; empty, malformed and
// non-positive values read as zero, the "unconfigured" value every consumer
// of this block treats as "keep the documented fallback".
func positiveDuration(s string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	return 0
}
