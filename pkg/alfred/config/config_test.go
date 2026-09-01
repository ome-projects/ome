package config

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestDefaultIsSafeAndComplete(t *testing.T) {
	cfg := Default()

	if cfg.Mode != ModeRecommendOnly {
		t.Fatalf("default mode = %q, want recommend-only", cfg.Mode)
	}
	if cfg.DecisionLoopInterval.Duration != 5*time.Minute || cfg.ObservationLoopInterval.Duration != 30*time.Second {
		t.Fatalf("default intervals: %v / %v", cfg.DecisionLoopInterval, cfg.ObservationLoopInterval)
	}
	if cfg.OMENativeCapabilityLeaseName != "ome-inferencereplica-executor" ||
		cfg.OMENativeCapabilityLeaseNamespace != "ome" ||
		cfg.OMENativeCapabilityMaxStaleness.Duration != 30*time.Second {
		t.Fatalf("OMENative capability defaults: %q/%q/%v",
			cfg.OMENativeCapabilityLeaseNamespace, cfg.OMENativeCapabilityLeaseName,
			cfg.OMENativeCapabilityMaxStaleness.Duration)
	}
	if len(cfg.EarlyTickOn) != 1 || cfg.EarlyTickOn[0] != EarlyTickNodeConditionChange {
		t.Fatalf("default earlyTickOn: %v", cfg.EarlyTickOn)
	}

	d := cfg.Policies.Defragmentation
	if !*d.Enabled || *d.FragmentationThreshold != 0.25 || d.Aggressiveness != AggressivenessBalanced {
		t.Fatalf("defrag defaults: %+v", d)
	}
	if got := d.Scoring.SizeLadder; len(got) != 4 || got[0] != 1 || got[3] != 8 {
		t.Fatalf("size ladder: %v", got)
	}
	if *d.Scoring.DemandBlendLambda != 0.3 || d.Scoring.SizePrior["8"] != 0.6 {
		t.Fatalf("scoring defaults: %+v", d.Scoring)
	}

	n := cfg.Policies.NodeHealth
	if !*n.Enabled || n.HealthCooldownFloorMinutes != 5 || n.NodeSuspicionWindowMinutes != 30 {
		t.Fatalf("nodeHealth defaults: %+v", n)
	}
	if len(n.TriggerConditions) != 1 || n.TriggerConditions[0] != "GpuUnhealthy" {
		t.Fatalf("trigger conditions: %v", n.TriggerConditions)
	}

	if cfg.MaxInFlightMigrations != 3 || cfg.MaxMigrationsPerHour != 10 {
		t.Fatalf("caps: %d/%d", cfg.MaxInFlightMigrations, cfg.MaxMigrationsPerHour)
	}
	if cfg.PerWorkloadCooldown() != 30*time.Minute || cfg.PerNodeCooldown() != 10*time.Minute ||
		cfg.RecentPlacementCooldown() != 10*time.Minute {
		t.Fatal("cooldown accessors mismatch")
	}
	if cfg.HealthCooldownFloor() != 5*time.Minute || cfg.NodeSuspicionWindow() != 30*time.Minute {
		t.Fatal("health accessors mismatch")
	}
	if cfg.EmergencyPendingAge() != 10*time.Minute || cfg.PendingUrgencyTau() != 30*time.Minute {
		t.Fatal("urgency accessors mismatch")
	}
	if !*cfg.DefaultMovable || *cfg.RawDeploymentMigrationEnabled || !*cfg.OMENativeMigrationEnabled {
		t.Fatal("Raw migration must default off while supported execution surfaces default on")
	}
	if cfg.RecommendationsConfigMapName != "alfred-recommendations" {
		t.Fatalf("recommendations configmap name: %q", cfg.RecommendationsConfigMapName)
	}
	if !*cfg.SpotPolicy.AvoidAsTarget || len(cfg.SpotPolicy.PreemptibleLabels) != 2 {
		t.Fatalf("spot defaults: %+v", cfg.SpotPolicy)
	}
}

func TestDefaultCapabilityNamespaceIsStable(t *testing.T) {
	original := constants.OMENamespace
	t.Cleanup(func() { constants.OMENamespace = original })
	constants.OMENamespace = "runtime-control-plane"

	if got := Default().OMENativeCapabilityLeaseNamespace; got != "ome" {
		t.Fatalf("capability namespace default = %q, want fixed %q", got, "ome")
	}
}

func TestLoadPreservesReservedRawDeploymentMigrationValues(t *testing.T) {
	for _, want := range []bool{false, true} {
		t.Run(fmt.Sprint(want), func(t *testing.T) {
			cfg, err := Load([]byte(fmt.Sprintf(
				"schemaVersion: 1\nrawDeploymentMigrationEnabled: %t\n", want)))
			if err != nil {
				t.Fatal(err)
			}
			if got := *cfg.RawDeploymentMigrationEnabled; got != want {
				t.Fatalf("rawDeploymentMigrationEnabled = %t, want explicit %t", got, want)
			}
		})
	}
}

func TestLoadParsesTheOEPSchema(t *testing.T) {
	raw := []byte(`
schemaVersion: 1
mode: execute
decisionLoopInterval: 2m
observationLoopInterval: 15s
omenativeCapabilityLeaseName: custom-executor
omenativeCapabilityLeaseNamespace: runtime-system
omenativeCapabilityMaxStaleness: 45s
earlyTickOn: [NodeConditionChange]
policies:
  defragmentation:
    enabled: true
    fragmentationThreshold: 0.4
    aggressiveness: conservative
    scoring:
      sizeLadder: [1, 2, 4]
      demandBlendLambda: 0.5
      sizePrior:
        "1": 0.2
        "2": 0.3
        "4": 0.5
      pendingUrgencyTauMinutes: 15
  nodeHealth:
    enabled: false
    triggerConditions: [GpuUnhealthy, NodeNotReady]
    healthCooldownFloorMinutes: 3
defaultMovable: false
recentPlacementCooldownMinutes: 20
maxInFlightMigrations: 5
maintenanceWindows:
  - days: [Mon, Tue, Wed, Thu, Fri]
    start: "09:00"
    end: "17:00"
spotPolicy:
  avoidAsTarget: false
  preemptibleLabels: [custom.io/spot]
logLevel: debug
`)
	cfg, err := Load(raw)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Mode != ModeExecute || cfg.DecisionLoopInterval.Duration != 2*time.Minute {
		t.Fatalf("top-level parse: %+v", cfg)
	}
	if cfg.OMENativeCapabilityLeaseName != "custom-executor" ||
		cfg.OMENativeCapabilityLeaseNamespace != "runtime-system" ||
		cfg.OMENativeCapabilityMaxStaleness.Duration != 45*time.Second {
		t.Fatalf("OMENative capability parse: %+v", cfg)
	}
	if *cfg.Policies.Defragmentation.FragmentationThreshold != 0.4 ||
		cfg.Policies.Defragmentation.Aggressiveness != AggressivenessConservative {
		t.Fatalf("defrag parse: %+v", cfg.Policies.Defragmentation)
	}
	if cfg.Policies.Defragmentation.Scoring.PendingUrgencyTauMinutes != 15 {
		t.Fatalf("tau parse: %+v", cfg.Policies.Defragmentation.Scoring)
	}
	if *cfg.Policies.NodeHealth.Enabled || len(cfg.Policies.NodeHealth.TriggerConditions) != 2 {
		t.Fatalf("nodeHealth parse: %+v", cfg.Policies.NodeHealth)
	}
	if *cfg.DefaultMovable || cfg.RecentPlacementCooldownMinutes != 20 || cfg.MaxInFlightMigrations != 5 {
		t.Fatalf("workload defaults parse: %+v", cfg)
	}
	if len(cfg.MaintenanceWindows) != 1 || cfg.MaintenanceWindows[0].Start != "09:00" {
		t.Fatalf("windows parse: %+v", cfg.MaintenanceWindows)
	}
	if *cfg.SpotPolicy.AvoidAsTarget || cfg.SpotPolicy.PreemptibleLabels[0] != "custom.io/spot" {
		t.Fatalf("spot parse: %+v", cfg.SpotPolicy)
	}
	// Unset sections still default.
	if cfg.MaxMigrationsPerHour != 10 || cfg.PerWorkloadCooldownMinutes != 30 {
		t.Fatalf("defaults on unset keys: %+v", cfg)
	}
}

func TestLoadRejectsInvalidConfigs(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"unknown schema version", "schemaVersion: 2", "unsupported schemaVersion"},
		{"bad mode", "schemaVersion: 1\nmode: dry-run", "mode must be"},
		{"threshold out of range", "schemaVersion: 1\npolicies:\n  defragmentation:\n    fragmentationThreshold: 1.5", "fragmentationThreshold"},
		{"lambda out of range", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      demandBlendLambda: -0.1", "demandBlendLambda"},
		{"unsorted ladder", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      sizeLadder: [4, 2, 8]", "sizeLadder must be ascending"},
		{"duplicate ladder size", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      sizeLadder: [1, 2, 2, 4]", "duplicate size"},
		{"prior does not sum to 1", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      sizePrior:\n        \"8\": 0.5", "must sum to 1"},
		{"prior bad key", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      sizePrior:\n        big: 1.0", "not a canonical positive size"},
		{"prior key with trailing garbage", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      sizePrior:\n        \"8x\": 1.0", "not a canonical positive size"},
		{"prior key with leading zero", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      sizePrior:\n        \"08\": 1.0", "not a canonical positive size"},
		{"prior key with plus sign", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      sizePrior:\n        \"+8\": 1.0", "not a canonical positive size"},
		{"prior key outside ladder", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      sizePrior:\n        \"16\": 1.0", "not a sizeLadder size"},
		{"unknown early tick", "schemaVersion: 1\nearlyTickOn: [PodDeleted]", "unknown earlyTickOn"},
		{"bad aggressiveness", "schemaVersion: 1\npolicies:\n  nodeHealth:\n    aggressiveness: yolo", "aggressiveness"},
		{"bad window day", "schemaVersion: 1\nmaintenanceWindows:\n  - days: [Monday]\n    start: \"09:00\"\n    end: \"17:00\"", "unknown day"},
		{"bad window time", "schemaVersion: 1\nmaintenanceWindows:\n  - days: [Mon]\n    start: \"9am\"\n    end: \"17:00\"", "must be HH:MM"},
		{"inverted window", "schemaVersion: 1\nmaintenanceWindows:\n  - days: [Mon]\n    start: \"17:00\"\n    end: \"09:00\"", "must be before end"},
		{"unknown field", "schemaVersion: 1\nmodee: execute", "parse config.yaml"},
		{"interval too small", "schemaVersion: 1\ndecisionLoopInterval: 100ms", "decisionLoopInterval"},
		{"invalid capability duration", "schemaVersion: 1\nomenativeCapabilityMaxStaleness: soon", "parse config.yaml"},
		{"zero capability staleness", "schemaVersion: 1\nomenativeCapabilityMaxStaleness: 0s", "omenativeCapabilityMaxStaleness"},
		{"negative capability staleness", "schemaVersion: 1\nomenativeCapabilityMaxStaleness: -1s", "omenativeCapabilityMaxStaleness"},
		{"empty capability lease name", "schemaVersion: 1\nomenativeCapabilityLeaseName: \"\"", "omenativeCapabilityLeaseName"},
		{"invalid capability lease name", "schemaVersion: 1\nomenativeCapabilityLeaseName: Bad_Name", "omenativeCapabilityLeaseName"},
		{"empty capability namespace", "schemaVersion: 1\nomenativeCapabilityLeaseNamespace: \"\"", "omenativeCapabilityLeaseNamespace"},
		{"invalid capability namespace", "schemaVersion: 1\nomenativeCapabilityLeaseNamespace: bad.namespace", "omenativeCapabilityLeaseNamespace"},
		{"negative cooldown", "schemaVersion: 1\nrecentPlacementCooldownMinutes: -5", "must be positive"},
		{"negative cap", "schemaVersion: 1\nmaxInFlightMigrations: -1", "must be positive"},
		{"negative tau", "schemaVersion: 1\npolicies:\n  defragmentation:\n    scoring:\n      pendingUrgencyTauMinutes: -30", "must be positive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load([]byte(tc.yaml))
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestStoreLastKnownGood(t *testing.T) {
	store := NewStore()
	if store.Get().Mode != ModeRecommendOnly {
		t.Fatal("store must boot with safe defaults")
	}

	outcome, err := store.Update([]byte("schemaVersion: 1\nmode: execute"))
	if err != nil || outcome != OutcomeSuccess {
		t.Fatalf("valid update: %v/%v", outcome, err)
	}
	if store.Get().Mode != ModeExecute {
		t.Fatal("update should activate the new config")
	}

	outcome, err = store.Update([]byte("schemaVersion: 99"))
	if err == nil || outcome != OutcomeFailure {
		t.Fatalf("invalid update should fail: %v/%v", outcome, err)
	}
	if store.Get().Mode != ModeExecute {
		t.Fatal("failed update must keep last-known-good")
	}
}
