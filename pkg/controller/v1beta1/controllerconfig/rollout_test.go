package controllerconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
)

// TestParseRolloutConfig verifies the rollout block has NO in-code defaults:
// an absent block yields zero values (uncapped plan size, no configured ready
// timeout), and an empty, malformed, or non-positive duration degrades to
// zero rather than failing the load, mirroring the canaryAnalysis duration
// handling.
func TestParseRolloutConfig(t *testing.T) {
	tests := []struct {
		name        string
		data        map[string]string
		wantBytes   int
		wantTimeout time.Duration
	}{
		{
			name:        "both fields parse",
			data:        map[string]string{RolloutConfigName: `{"maxPinnedPlanBytes": 16384, "defaultReadyTimeout": "15m"}`},
			wantBytes:   16384,
			wantTimeout: 15 * time.Minute,
		},
		{
			name:        "absent block yields zero values",
			data:        map[string]string{},
			wantBytes:   0,
			wantTimeout: 0,
		},
		{
			name:        "empty block yields zero values",
			data:        map[string]string{RolloutConfigName: `{}`},
			wantBytes:   0,
			wantTimeout: 0,
		},
		{
			name:        "malformed duration degrades to no configured default",
			data:        map[string]string{RolloutConfigName: `{"maxPinnedPlanBytes": 1024, "defaultReadyTimeout": "nope"}`},
			wantBytes:   1024,
			wantTimeout: 0,
		},
		{
			name:        "zero duration degrades to no configured default",
			data:        map[string]string{RolloutConfigName: `{"defaultReadyTimeout": "0s"}`},
			wantBytes:   0,
			wantTimeout: 0,
		},
		{
			name:        "negative duration degrades to no configured default",
			data:        map[string]string{RolloutConfigName: `{"defaultReadyTimeout": "-5m"}`},
			wantBytes:   0,
			wantTimeout: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseRolloutConfig(&v1.ConfigMap{Data: tt.data})
			require.NoError(t, err)
			assert.Equal(t, tt.wantBytes, cfg.MaxPinnedPlanBytes)
			assert.Equal(t, tt.wantTimeout, cfg.DefaultReadyTimeout)
		})
	}
}

// The canary cadences follow the same rule as the ready timeout: configured
// in the chart, parsed here, zero when absent or unusable.
func TestParseRolloutConfigCanaryCadences(t *testing.T) {
	tests := []struct {
		name       string
		data       string
		wantStep   time.Duration
		wantParked time.Duration
	}{
		{"both cadences parse", `{"canaryRequeue": "10s", "canaryParkedRequeue": "5m"}`, 10 * time.Second, 5 * time.Minute},
		{"absent cadences read as unconfigured", `{"defaultReadyTimeout": "15m"}`, 0, 0},
		{"malformed cadence reads as unconfigured", `{"canaryRequeue": "soon", "canaryParkedRequeue": "-1m"}`, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseRolloutConfig(&v1.ConfigMap{Data: map[string]string{RolloutConfigName: tt.data}})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStep, cfg.CanaryRequeue)
			assert.Equal(t, tt.wantParked, cfg.CanaryParkedRequeue)
		})
	}
}

// TestParseRolloutConfigErrors verifies malformed JSON and a nonsensical
// negative plan-size cap are load errors.
func TestParseRolloutConfigErrors(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string]string
		errPart string
	}{
		{
			name:    "malformed json",
			data:    map[string]string{RolloutConfigName: `{not-json`},
			errPart: "unable to parse rollout config json",
		},
		{
			name:    "negative maxPinnedPlanBytes",
			data:    map[string]string{RolloutConfigName: `{"maxPinnedPlanBytes": -1}`},
			errPart: "maxPinnedPlanBytes must be >= 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseRolloutConfig(&v1.ConfigMap{Data: tt.data})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errPart)
		})
	}
}

// TestNewRolloutConfigLoaders verifies the clientset-backed loaders resolve
// through the shared ConfigMap fetch (uncached and TTL-cached paths).
func TestNewRolloutConfigLoaders(t *testing.T) {
	clientset, _ := countingClientset(t, map[string]string{
		RolloutConfigName: `{"maxPinnedPlanBytes": 2048, "defaultReadyTimeout": "90s"}`,
	})

	cfg, err := NewRolloutConfig(clientset)
	require.NoError(t, err)
	assert.Equal(t, 2048, cfg.MaxPinnedPlanBytes)
	assert.Equal(t, 90*time.Second, cfg.DefaultReadyTimeout)

	cfg, err = NewRolloutConfigCached(NewConfigCache(0), clientset)
	require.NoError(t, err)
	assert.Equal(t, 2048, cfg.MaxPinnedPlanBytes)
}
