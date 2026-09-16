package modelagent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The platform shows "staging needs X, node has Y free" before creating models,
// and there is no eviction to fall back on, so the free-space gauge has to be
// readable at any time — not only right after a staging run.
func TestNewMetricsPublishesModelsRootFreeBytes(t *testing.T) {
	reg := prometheus.NewRegistry()

	_ = NewMetrics(reg, t.TempDir())

	assert.Greater(t, gaugeValue(t, reg, "model_agent_models_root_free_bytes"), float64(0))
}

func TestNewMetricsToleratesUnreadableModelsRoot(t *testing.T) {
	reg := prometheus.NewRegistry()
	// A regular file can never become a models root; an absent path would just
	// be created by EnsureDir when the agent runs as root.
	notADir := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o644))

	metrics := NewMetrics(reg, notADir)

	require.NotNil(t, metrics)
	assert.Equal(t, float64(0), gaugeValue(t, reg, "model_agent_models_root_free_bytes"))
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		require.Len(t, family.GetMetric(), 1)
		return family.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf("metric %s not registered", name)
	return 0
}
