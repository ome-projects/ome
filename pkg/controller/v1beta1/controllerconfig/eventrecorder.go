package controllerconfig

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

// EventRecorderConfigName is the inferenceservice ConfigMap key holding the
// event recorder budget.
const EventRecorderConfigName = "eventRecorder"

// EventRecorderConfig is the "eventRecorder" block of the inferenceservice
// ConfigMap: the budget client-go's event spam filter grants the manager per
// object. The filter keeps one token bucket per (source, object, event type)
// and silently drops every event that finds its bucket empty, so this budget
// bounds how many state changes about one object reach the API server in a
// burst and how fast the bucket recovers. Both fields are optional and
// independent; an absent field keeps client-go's own default for that side
// of the bucket. There is intentionally NO in-code default.
//
// +kubebuilder:object:generate=false
type EventRecorderConfig struct {
	// BurstSize is how many events one object may receive back to back
	// before only the refill applies.
	BurstSize *int32 `json:"burstSize,omitempty"`
	// RefillInterval is how long the bucket takes to regain one event, as a
	// duration string ("5s").
	RefillInterval *string `json:"refillInterval,omitempty"`
}

// EventRecorderSettings is the validated budget the manager builds its event
// broadcasters from. A zero field is unconfigured: client-go's correlator
// substitutes its own default for a zero burst or fill rate, so no default
// is invented here.
//
// +kubebuilder:object:generate=false
type EventRecorderSettings struct {
	BurstSize      int
	RefillInterval time.Duration
}

// CorrelatorOptions expresses the budget as client-go correlator options.
// The refill interval becomes the bucket's fill rate in events per second;
// an unconfigured side stays zero so client-go applies its default to it.
func (s EventRecorderSettings) CorrelatorOptions() record.CorrelatorOptions {
	options := record.CorrelatorOptions{BurstSize: s.BurstSize}
	if s.RefillInterval > 0 {
		options.QPS = float32(float64(time.Second) / float64(s.RefillInterval))
	}
	return options
}

// NewEventRecorderConfig loads the eventRecorder block from the
// inferenceservice ConfigMap. An absent or empty key yields (nil, nil):
// unconfigured, never an error and never a fabricated default.
func NewEventRecorderConfig(clientset kubernetes.Interface) (*EventRecorderConfig, error) {
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return parseEventRecorderConfig(configMap)
}

func parseEventRecorderConfig(configMap *v1.ConfigMap) (*EventRecorderConfig, error) {
	data, ok := configMap.Data[EventRecorderConfigName]
	if !ok || strings.TrimSpace(data) == "" {
		return nil, nil
	}
	cfg := &EventRecorderConfig{}
	if err := json.Unmarshal([]byte(data), cfg); err != nil {
		return nil, fmt.Errorf("unable to parse eventRecorder config json: %w", err)
	}
	return cfg, nil
}

// ToBurstSize validates the configured burst. A nil config or absent field is
// unconfigured (zero). An explicit zero or negative burst is invalid so
// manager startup can reject bad configuration instead of silently keeping
// client-go's default.
func (c *EventRecorderConfig) ToBurstSize() (int, error) {
	if c == nil || c.BurstSize == nil {
		return 0, nil
	}
	if *c.BurstSize <= 0 {
		return 0, fmt.Errorf("invalid eventRecorder.burstSize: must be > 0, got %d", *c.BurstSize)
	}
	return int(*c.BurstSize), nil
}

// ToRefillInterval validates and parses the configured refill interval. A
// nil config or absent field is unconfigured (zero). An explicit malformed,
// zero or negative duration is invalid so manager startup can reject it.
func (c *EventRecorderConfig) ToRefillInterval() (time.Duration, error) {
	if c == nil || c.RefillInterval == nil {
		return 0, nil
	}
	interval, err := time.ParseDuration(*c.RefillInterval)
	if err != nil {
		return 0, fmt.Errorf("invalid eventRecorder.refillInterval %q: %w", *c.RefillInterval, err)
	}
	if interval <= 0 {
		return 0, fmt.Errorf("invalid eventRecorder.refillInterval: must be > 0, got %s", interval)
	}
	return interval, nil
}

// LoadEventRecorderSettings reads one ConfigMap snapshot and validates the
// event recorder budget. The manager keeps the result for the lifetime of
// the process: a changed budget takes effect on the next restart.
func LoadEventRecorderSettings(clientset kubernetes.Interface) (EventRecorderSettings, error) {
	cfg, err := NewEventRecorderConfig(clientset)
	if err != nil {
		return EventRecorderSettings{}, fmt.Errorf("load eventRecorder configuration: %w", err)
	}
	burstSize, err := cfg.ToBurstSize()
	if err != nil {
		return EventRecorderSettings{}, fmt.Errorf("validate eventRecorder.burstSize: %w", err)
	}
	refillInterval, err := cfg.ToRefillInterval()
	if err != nil {
		return EventRecorderSettings{}, fmt.Errorf("validate eventRecorder.refillInterval: %w", err)
	}
	return EventRecorderSettings{BurstSize: burstSize, RefillInterval: refillInterval}, nil
}
