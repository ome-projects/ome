package modelagent

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewMetrics_RegistersMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg, t.TempDir())

	// Test that custom counters are registered and can be incremented
	metrics.modelDownloadsSuccessTotal.WithLabelValues("testtype", "testns", "testmodel").Inc()
	if got := testutil.ToFloat64(metrics.modelDownloadsSuccessTotal.WithLabelValues("testtype", "testns", "testmodel")); got != 1 {
		t.Errorf("modelDownloadsSuccessTotal did not increment, got = %v, want = 1", got)
	}

	metrics.modelDownloadsFailedTotal.WithLabelValues("testtype", "testns", "testmodel").Add(2)
	if got := testutil.ToFloat64(metrics.modelDownloadsFailedTotal.WithLabelValues("testtype", "testns", "testmodel")); got != 2 {
		t.Errorf("modelDownloadsFailedTotal did not increment, got = %v, want = 2", got)
	}

	// Test that histograms record observations
	metrics.modelDownloadDuration.WithLabelValues("testtype", "testns", "testmodel").Observe(1.23)
	count := testutil.CollectAndCount(metrics.modelDownloadDuration)
	if count == 0 {
		t.Error("modelDownloadDuration did not record observation")
	}
}

func TestGoRuntimeMetrics_AreSet(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewMetrics(reg, t.TempDir())

	// Wait for the goroutine to update runtime metrics
	time.Sleep(200 * time.Millisecond)

	metricNames := []string{
		"go_goroutines_current",
		"go_memory_alloc_bytes",
		"go_memory_heap_alloc_bytes",
		"go_memory_heap_sys_bytes",
		"go_memory_stack_sys_bytes",
	}

	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	for _, name := range metricNames {
		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() == name {
				found = true
				if len(mf.Metric) == 0 || mf.Metric[0].GetGauge().GetValue() < 0 {
					t.Errorf("metric %s has invalid value", name)
				}
				break
			}
		}
		if !found {
			t.Errorf("metric %s not found", name)
		}
	}
}
