// Package config loads and hot-reloads Alfred's configuration from the
// alfred-config ConfigMap (key config.yaml), with schema validation and a
// last-known-good fallback: a fat-fingered edit must never leave the
// caretaker acting on a half-parsed policy (OEP-0008).
package config

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/constants"
)

// Operating modes.
const (
	// ModeRecommendOnly runs policies, arbitration, and reporting but
	// never dispatches — the safe default.
	ModeRecommendOnly = "recommend-only"
	// ModeExecute additionally lets the dispatcher write
	// migration-request annotations.
	ModeExecute = "execute"
)

// Aggressiveness values (a scoring knob, never a safety bypass).
const (
	AggressivenessConservative = "conservative"
	AggressivenessBalanced     = "balanced"
	AggressivenessAggressive   = "aggressive"
)

// EarlyTickNodeConditionChange is the only earlyTickOn trigger defined today.
const EarlyTickNodeConditionChange = "NodeConditionChange"

// SupportedSchemaVersion is the config schema this build understands.
const SupportedSchemaVersion = 1

const defaultCapabilityLeaseNamespace = "ome"

// Config is the full alfred-config schema. Load returns a fully-defaulted
// value: every pointer field is non-nil and every duration/count positive, so
// consumers never re-default.
type Config struct {
	SchemaVersion int    `json:"schemaVersion"`
	Mode          string `json:"mode"`

	DecisionLoopInterval    metav1.Duration `json:"decisionLoopInterval"`
	ObservationLoopInterval metav1.Duration `json:"observationLoopInterval"`
	// EarlyTickOn requests a supplemental fresh decision pass on the named
	// events without moving the regular cadence; empty disables these passes.
	// A running pass is never interrupted.
	EarlyTickOn []string `json:"earlyTickOn"`

	Policies Policies `json:"policies"`

	DefaultMovable                 *bool `json:"defaultMovable"`
	RecentPlacementCooldownMinutes int   `json:"recentPlacementCooldownMinutes"`
	PerWorkloadCooldownMinutes     int   `json:"perWorkloadCooldownMinutes"`
	PerNodeCooldownMinutes         int   `json:"perNodeCooldownMinutes"`

	// RawDeploymentMigrationEnabled is reserved for schema compatibility;
	// RawDeployment recommendations remain advisory regardless of its value.
	RawDeploymentMigrationEnabled *bool `json:"rawDeploymentMigrationEnabled"`
	OMENativeMigrationEnabled     *bool `json:"omenativeMigrationEnabled"`
	LWSRecommendationsEnabled     *bool `json:"lwsRecommendationsEnabled"`

	OMENativeCapabilityLeaseName      string          `json:"omenativeCapabilityLeaseName"`
	OMENativeCapabilityLeaseNamespace string          `json:"omenativeCapabilityLeaseNamespace"`
	OMENativeCapabilityMaxStaleness   metav1.Duration `json:"omenativeCapabilityMaxStaleness"`

	RecommendationsConfigMapEnabled *bool  `json:"recommendationsConfigMapEnabled"`
	RecommendationsConfigMapName    string `json:"recommendationsConfigMapName"`

	MaxInFlightMigrations      int `json:"maxInFlightMigrations"`
	MaxMigrationsPerHour       int `json:"maxMigrationsPerHour"`
	EmergencyPendingAgeMinutes int `json:"emergencyPendingAgeMinutes"`

	// MaintenanceWindows gate defragmentation dispatch (node-health
	// evacuation deliberately overrides them). Empty means no windows —
	// defrag may dispatch at any time.
	MaintenanceWindows []MaintenanceWindow `json:"maintenanceWindows"`

	SpotPolicy SpotPolicy `json:"spotPolicy"`

	AllowCrossTenantOptimization *bool `json:"allowCrossTenantOptimization"`

	LogLevel          string `json:"logLevel"`
	StructuredLogging *bool  `json:"structuredLogging"`
}

// Policies groups the per-policy blocks.
type Policies struct {
	Defragmentation Defragmentation `json:"defragmentation"`
	NodeHealth      NodeHealth      `json:"nodeHealth"`
}

// Defragmentation is Policy #1's block.
type Defragmentation struct {
	Enabled                *bool    `json:"enabled"`
	FragmentationThreshold *float64 `json:"fragmentationThreshold"`
	Aggressiveness         string   `json:"aggressiveness"`
	Scoring                Scoring  `json:"scoring"`
}

// Scoring holds the fragmentation-score knobs (OEP-0008 §Fragmentation
// scoring).
type Scoring struct {
	// SizeLadder is the within-node demand-size ladder, ascending.
	SizeLadder []int `json:"sizeLadder"`
	// DemandBlendLambda blends observed demand with the static prior:
	// 0 = pure observed, 1 = pure prior.
	DemandBlendLambda *float64 `json:"demandBlendLambda"`
	// SizePrior is the static prior over demand sizes, keyed by size.
	SizePrior map[string]float64 `json:"sizePrior"`
	// PendingUrgencyTauMinutes is tau in the pending-pressure term.
	PendingUrgencyTauMinutes int `json:"pendingUrgencyTauMinutes"`
}

// NodeHealth is Policy #2's block.
type NodeHealth struct {
	Enabled        *bool  `json:"enabled"`
	Aggressiveness string `json:"aggressiveness"`
	// TriggerConditions are the node conditions that trigger evacuation;
	// consumed from existing signals, never detected by Alfred.
	TriggerConditions []string `json:"triggerConditions"`
	// SignalOnly emits the remediation signal but never evacuates.
	SignalOnly bool `json:"signalOnly"`
	// HealthCooldownFloorMinutes is the per-workload cooldown floor for
	// NodeUnhealthy candidates (instead of the standard cooldown).
	HealthCooldownFloorMinutes int `json:"healthCooldownFloorMinutes"`
	// NodeSuspicionWindowMinutes keeps evacuated nodes out of target
	// hints even after the condition clears.
	NodeSuspicionWindowMinutes int `json:"nodeSuspicionWindowMinutes"`
}

// SpotPolicy configures spot/preemptible node handling.
type SpotPolicy struct {
	AvoidAsTarget     *bool    `json:"avoidAsTarget"`
	PreferAsSource    *bool    `json:"preferAsSource"`
	PreemptibleLabels []string `json:"preemptibleLabels"`
}

// MaintenanceWindow is a weekly UTC window. Days use three-letter English
// names (Mon..Sun); Start/End are "HH:MM" with Start < End.
type MaintenanceWindow struct {
	Days  []string `json:"days"`
	Start string   `json:"start"`
	End   string   `json:"end"`
}

// Default returns the fully-defaulted configuration: recommend-only, both
// policies enabled, the OEP's conservative safety bounds.
func Default() *Config {
	return defaultConfig(defaultCapabilityLeaseNamespace)
}

func defaultConfig(capabilityLeaseNamespace string) *Config {
	cfg := &Config{SchemaVersion: SupportedSchemaVersion}
	cfg.applyDefaults(capabilityLeaseNamespace)
	return cfg
}

// Load parses and validates raw config.yaml content, returning a
// fully-defaulted Config or an error describing the first violation.
func Load(raw []byte) (*Config, error) {
	return loadWithCapabilityNamespace(raw, defaultCapabilityLeaseNamespace)
}

func loadWithCapabilityNamespace(raw []byte, capabilityLeaseNamespace string) (*Config, error) {
	cfg := &Config{}
	if err := yaml.UnmarshalStrict(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config.yaml: %w", err)
	}
	if err := validateCapabilityOverrides(raw); err != nil {
		return nil, err
	}
	cfg.applyDefaults(capabilityLeaseNamespace)
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// validateCapabilityOverrides distinguishes omitted values, which receive
// defaults, from explicitly empty or non-positive values. Config intentionally
// exposes fully defaulted value fields to its consumers, so that distinction is
// available only while loading the raw document.
func validateCapabilityOverrides(raw []byte) error {
	var overrides struct {
		LeaseName      *string          `json:"omenativeCapabilityLeaseName"`
		LeaseNamespace *string          `json:"omenativeCapabilityLeaseNamespace"`
		MaxStaleness   *metav1.Duration `json:"omenativeCapabilityMaxStaleness"`
	}
	if err := yaml.Unmarshal(raw, &overrides); err != nil {
		return fmt.Errorf("parse config.yaml: %w", err)
	}
	if overrides.LeaseName != nil && *overrides.LeaseName == "" {
		return fmt.Errorf("omenativeCapabilityLeaseName must not be empty")
	}
	if overrides.LeaseNamespace != nil && *overrides.LeaseNamespace == "" {
		return fmt.Errorf("omenativeCapabilityLeaseNamespace must not be empty")
	}
	if overrides.MaxStaleness != nil && overrides.MaxStaleness.Duration <= 0 {
		return fmt.Errorf("omenativeCapabilityMaxStaleness must be >= %s, got %s",
			constants.OMENativeExecutorCapabilityMinStaleness, overrides.MaxStaleness.Duration)
	}
	return nil
}

func boolPtr(b bool) *bool        { return &b }
func floatPtr(f float64) *float64 { return &f }
func minutes(m int) time.Duration { return time.Duration(m) * time.Minute }
func defaultInt(v *int, d int) {
	if *v == 0 {
		*v = d
	}
}
func defaultStr(v *string, d string) {
	if *v == "" {
		*v = d
	}
}

func (c *Config) applyDefaults(capabilityLeaseNamespace string) {
	defaultStr(&c.Mode, ModeRecommendOnly)
	if c.DecisionLoopInterval.Duration == 0 {
		c.DecisionLoopInterval = metav1.Duration{Duration: 5 * time.Minute}
	}
	if c.ObservationLoopInterval.Duration == 0 {
		c.ObservationLoopInterval = metav1.Duration{Duration: 30 * time.Second}
	}
	if c.EarlyTickOn == nil {
		c.EarlyTickOn = []string{EarlyTickNodeConditionChange}
	}

	d := &c.Policies.Defragmentation
	if d.Enabled == nil {
		d.Enabled = boolPtr(true)
	}
	if d.FragmentationThreshold == nil {
		d.FragmentationThreshold = floatPtr(0.25)
	}
	defaultStr(&d.Aggressiveness, AggressivenessBalanced)
	if len(d.Scoring.SizeLadder) == 0 {
		d.Scoring.SizeLadder = []int{1, 2, 4, 8}
	}
	if d.Scoring.DemandBlendLambda == nil {
		d.Scoring.DemandBlendLambda = floatPtr(0.3)
	}
	if len(d.Scoring.SizePrior) == 0 {
		d.Scoring.SizePrior = map[string]float64{"1": 0.1, "2": 0.1, "4": 0.2, "8": 0.6}
	}
	defaultInt(&d.Scoring.PendingUrgencyTauMinutes, 30)

	n := &c.Policies.NodeHealth
	if n.Enabled == nil {
		n.Enabled = boolPtr(true)
	}
	defaultStr(&n.Aggressiveness, AggressivenessBalanced)
	if len(n.TriggerConditions) == 0 {
		n.TriggerConditions = []string{"GpuUnhealthy"}
	}
	defaultInt(&n.HealthCooldownFloorMinutes, 5)
	defaultInt(&n.NodeSuspicionWindowMinutes, 30)

	if c.DefaultMovable == nil {
		c.DefaultMovable = boolPtr(true)
	}
	defaultInt(&c.RecentPlacementCooldownMinutes, 10)
	defaultInt(&c.PerWorkloadCooldownMinutes, 30)
	defaultInt(&c.PerNodeCooldownMinutes, 10)

	if c.RawDeploymentMigrationEnabled == nil {
		c.RawDeploymentMigrationEnabled = boolPtr(false)
	}
	if c.OMENativeMigrationEnabled == nil {
		c.OMENativeMigrationEnabled = boolPtr(true)
	}
	if c.LWSRecommendationsEnabled == nil {
		c.LWSRecommendationsEnabled = boolPtr(true)
	}
	defaultStr(&c.OMENativeCapabilityLeaseName, constants.OMENativeExecutorCapabilityLeaseName)
	defaultStr(&c.OMENativeCapabilityLeaseNamespace, capabilityLeaseNamespace)
	if c.OMENativeCapabilityMaxStaleness.Duration == 0 {
		c.OMENativeCapabilityMaxStaleness = metav1.Duration{
			Duration: constants.OMENativeExecutorCapabilityMinStaleness,
		}
	}
	if c.RecommendationsConfigMapEnabled == nil {
		c.RecommendationsConfigMapEnabled = boolPtr(true)
	}
	defaultStr(&c.RecommendationsConfigMapName, "alfred-recommendations")

	defaultInt(&c.MaxInFlightMigrations, 3)
	defaultInt(&c.MaxMigrationsPerHour, 10)
	defaultInt(&c.EmergencyPendingAgeMinutes, 10)

	if c.SpotPolicy.AvoidAsTarget == nil {
		c.SpotPolicy.AvoidAsTarget = boolPtr(true)
	}
	if c.SpotPolicy.PreferAsSource == nil {
		c.SpotPolicy.PreferAsSource = boolPtr(true)
	}
	if c.SpotPolicy.PreemptibleLabels == nil {
		c.SpotPolicy.PreemptibleLabels = []string{
			"node.kubernetes.io/preemptible",
			"cloud.google.com/gke-preemptible",
		}
	}

	if c.AllowCrossTenantOptimization == nil {
		c.AllowCrossTenantOptimization = boolPtr(true)
	}
	defaultStr(&c.LogLevel, "info")
	if c.StructuredLogging == nil {
		c.StructuredLogging = boolPtr(true)
	}
}

var validDays = map[string]struct{}{
	"Mon": {}, "Tue": {}, "Wed": {}, "Thu": {}, "Fri": {}, "Sat": {}, "Sun": {},
}

var validAggressiveness = map[string]struct{}{
	AggressivenessConservative: {}, AggressivenessBalanced: {}, AggressivenessAggressive: {},
}

func (c *Config) validate() error {
	if c.SchemaVersion != SupportedSchemaVersion {
		return fmt.Errorf("unsupported schemaVersion %d (supported: %d)", c.SchemaVersion, SupportedSchemaVersion)
	}
	if c.Mode != ModeRecommendOnly && c.Mode != ModeExecute {
		return fmt.Errorf("mode must be %q or %q, got %q", ModeRecommendOnly, ModeExecute, c.Mode)
	}
	if c.DecisionLoopInterval.Duration < time.Second {
		return fmt.Errorf("decisionLoopInterval must be >= 1s, got %s", c.DecisionLoopInterval.Duration)
	}
	if c.ObservationLoopInterval.Duration < time.Second {
		return fmt.Errorf("observationLoopInterval must be >= 1s, got %s", c.ObservationLoopInterval.Duration)
	}
	if errs := utilvalidation.IsDNS1123Subdomain(c.OMENativeCapabilityLeaseName); len(errs) != 0 {
		return fmt.Errorf("omenativeCapabilityLeaseName %q invalid: %s", c.OMENativeCapabilityLeaseName, errs[0])
	}
	if err := validateCapabilityLeaseNamespace(c.OMENativeCapabilityLeaseNamespace); err != nil {
		return err
	}
	if c.OMENativeCapabilityMaxStaleness.Duration < constants.OMENativeExecutorCapabilityMinStaleness {
		return fmt.Errorf("omenativeCapabilityMaxStaleness must be >= %s, got %s",
			constants.OMENativeExecutorCapabilityMinStaleness, c.OMENativeCapabilityMaxStaleness.Duration)
	}
	for _, trigger := range c.EarlyTickOn {
		if trigger != EarlyTickNodeConditionChange {
			return fmt.Errorf("unknown earlyTickOn trigger %q", trigger)
		}
	}

	d := &c.Policies.Defragmentation
	if t := *d.FragmentationThreshold; t < 0 || t > 1 {
		return fmt.Errorf("policies.defragmentation.fragmentationThreshold must be in [0, 1], got %v", t)
	}
	if _, ok := validAggressiveness[d.Aggressiveness]; !ok {
		return fmt.Errorf("policies.defragmentation.aggressiveness %q invalid", d.Aggressiveness)
	}
	if l := *d.Scoring.DemandBlendLambda; l < 0 || l > 1 {
		return fmt.Errorf("policies.defragmentation.scoring.demandBlendLambda must be in [0, 1], got %v", l)
	}
	if !sort.IntsAreSorted(d.Scoring.SizeLadder) {
		return fmt.Errorf("policies.defragmentation.scoring.sizeLadder must be ascending, got %v", d.Scoring.SizeLadder)
	}
	ladderSizes := map[int]struct{}{}
	for _, size := range d.Scoring.SizeLadder {
		if size <= 0 {
			return fmt.Errorf("policies.defragmentation.scoring.sizeLadder entries must be positive, got %v", d.Scoring.SizeLadder)
		}
		// IntsAreSorted accepts non-strict ascending, so duplicates need
		// their own check: a repeated size would double-count its Frag
		// term in every blended score.
		if _, dup := ladderSizes[size]; dup {
			return fmt.Errorf("policies.defragmentation.scoring.sizeLadder has duplicate size %d", size)
		}
		ladderSizes[size] = struct{}{}
	}
	var priorSum float64
	for key, weight := range d.Scoring.SizePrior {
		// Canonical whole-string parse: "8x" must not alias size 8, and
		// neither may "08" or "+8" — Atoi accepts those aliases, and two
		// keys collapsing onto one size would leave the surviving weight
		// to map iteration order.
		size, err := strconv.Atoi(key)
		if err != nil || size <= 0 || strconv.Itoa(size) != key {
			return fmt.Errorf("policies.defragmentation.scoring.sizePrior key %q is not a canonical positive size", key)
		}
		// A prior key outside the ladder would be silently dropped by
		// the demand blend, deflating every weight it should have
		// carried; reject it loudly instead.
		if _, ok := ladderSizes[size]; !ok {
			return fmt.Errorf("policies.defragmentation.scoring.sizePrior key %q is not a sizeLadder size %v", key, d.Scoring.SizeLadder)
		}
		if weight < 0 {
			return fmt.Errorf("policies.defragmentation.scoring.sizePrior[%s] must be >= 0, got %v", key, weight)
		}
		priorSum += weight
	}
	if priorSum < 0.99 || priorSum > 1.01 {
		return fmt.Errorf("policies.defragmentation.scoring.sizePrior must sum to 1, got %v", priorSum)
	}

	n := &c.Policies.NodeHealth
	if _, ok := validAggressiveness[n.Aggressiveness]; !ok {
		return fmt.Errorf("policies.nodeHealth.aggressiveness %q invalid", n.Aggressiveness)
	}

	// Zero values were defaulted above, so anything non-positive here is an
	// explicit negative — which would invert cooldown and cap arithmetic.
	positives := []struct {
		name  string
		value int
	}{
		{"recentPlacementCooldownMinutes", c.RecentPlacementCooldownMinutes},
		{"perWorkloadCooldownMinutes", c.PerWorkloadCooldownMinutes},
		{"perNodeCooldownMinutes", c.PerNodeCooldownMinutes},
		{"maxInFlightMigrations", c.MaxInFlightMigrations},
		{"maxMigrationsPerHour", c.MaxMigrationsPerHour},
		{"emergencyPendingAgeMinutes", c.EmergencyPendingAgeMinutes},
		{"policies.defragmentation.scoring.pendingUrgencyTauMinutes", d.Scoring.PendingUrgencyTauMinutes},
		{"policies.nodeHealth.healthCooldownFloorMinutes", n.HealthCooldownFloorMinutes},
		{"policies.nodeHealth.nodeSuspicionWindowMinutes", n.NodeSuspicionWindowMinutes},
	}
	for _, p := range positives {
		if p.value <= 0 {
			return fmt.Errorf("%s must be positive, got %d", p.name, p.value)
		}
	}

	for i, w := range c.MaintenanceWindows {
		if len(w.Days) == 0 {
			return fmt.Errorf("maintenanceWindows[%d].days must not be empty", i)
		}
		for _, day := range w.Days {
			if _, ok := validDays[day]; !ok {
				return fmt.Errorf("maintenanceWindows[%d] has unknown day %q (want Mon..Sun)", i, day)
			}
		}
		for _, hhmm := range []string{w.Start, w.End} {
			if _, err := time.Parse("15:04", hhmm); err != nil {
				return fmt.Errorf("maintenanceWindows[%d] time %q must be HH:MM", i, hhmm)
			}
		}
		start, _ := time.Parse("15:04", w.Start)
		end, _ := time.Parse("15:04", w.End)
		if !start.Before(end) {
			return fmt.Errorf("maintenanceWindows[%d] start %q must be before end %q", i, w.Start, w.End)
		}
	}
	return nil
}

func validateCapabilityLeaseNamespace(namespace string) error {
	if errs := utilvalidation.IsDNS1123Label(namespace); len(errs) != 0 {
		return fmt.Errorf("omenativeCapabilityLeaseNamespace %q invalid: %s", namespace, errs[0])
	}
	return nil
}

// Convenience accessors for duration-typed bounds.

func (c *Config) RecentPlacementCooldown() time.Duration {
	return minutes(c.RecentPlacementCooldownMinutes)
}
func (c *Config) PerWorkloadCooldown() time.Duration { return minutes(c.PerWorkloadCooldownMinutes) }
func (c *Config) PerNodeCooldown() time.Duration     { return minutes(c.PerNodeCooldownMinutes) }
func (c *Config) HealthCooldownFloor() time.Duration {
	return minutes(c.Policies.NodeHealth.HealthCooldownFloorMinutes)
}
func (c *Config) NodeSuspicionWindow() time.Duration {
	return minutes(c.Policies.NodeHealth.NodeSuspicionWindowMinutes)
}
func (c *Config) EmergencyPendingAge() time.Duration { return minutes(c.EmergencyPendingAgeMinutes) }
func (c *Config) PendingUrgencyTau() time.Duration {
	return minutes(c.Policies.Defragmentation.Scoring.PendingUrgencyTauMinutes)
}
