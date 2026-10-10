package config

import "testing"

func TestTPUSliceMigrationDefaults(t *testing.T) {
	cfg := Default()
	if cfg.TPUSliceMigrationEnabled == nil || *cfg.TPUSliceMigrationEnabled {
		t.Fatalf("TPU slice migration must default off: %v", cfg.TPUSliceMigrationEnabled)
	}
	got := cfg.TPUSlicePartitions
	if got.TopologyLabel != "cloud.google.com/gke-tpu-topology" ||
		got.Label(got.IDLabel, "2x2x2") != "cloud.google.com/gke-tpu-partition-2x2x2-id" ||
		got.Label(got.StateLabel, "2x2x2") != "cloud.google.com/gke-tpu-partition-2x2x2-state" ||
		got.HealthyState != "HEALTHY" {
		t.Fatalf("unexpected label defaults: %+v", got)
	}
}

func TestTPUSliceMigrationValidation(t *testing.T) {
	tests := []struct {
		name, yaml string
		valid      bool
	}{
		{"enabled with defaults", "tpuSliceMigrationEnabled: true", true},
		{"custom labels", "tpuSliceMigrationEnabled: true\ntpuSlicePartitions: {topologyLabel: example.com/topo, idLabel: \"example.com/part-%s\", stateLabel: \"example.com/part-%s-st\", healthyState: OK}", true},
		{"id format without placeholder", "tpuSlicePartitions: {idLabel: example.com/part}", false},
		{"id format with two placeholders", "tpuSlicePartitions: {idLabel: \"example.com/%s-%s\"}", false},
		{"state format with another verb", "tpuSlicePartitions: {stateLabel: \"example.com/%d-%s\"}", false},
		{"format that is not a label key", "tpuSlicePartitions: {idLabel: \"bad key %s\"}", false},
		{"bad topology label", "tpuSlicePartitions: {topologyLabel: \"bad key\"}", false},
		{"bad healthy state", "tpuSlicePartitions: {healthyState: \"not ok\"}", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Load([]byte("schemaVersion: 1\n" + tt.yaml + "\n"))
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%t, err=%v", tt.valid, err)
			}
			if tt.valid && (cfg.TPUSliceMigrationEnabled == nil || !*cfg.TPUSliceMigrationEnabled) {
				t.Fatalf("enabled flag lost: %v", cfg.TPUSliceMigrationEnabled)
			}
		})
	}
}
