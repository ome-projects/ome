package podmonitor

import (
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// twoEndpointPM mirrors an OME-generated PodMonitor with two bare endpoints
// (e.g. the default /metrics plus an extra /engine_metrics endpoint).
func twoEndpointPM() *monitoringv1.PodMonitor {
	return &monitoringv1.PodMonitor{
		Spec: monitoringv1.PodMonitorSpec{
			PodMetricsEndpoints: []monitoringv1.PodMetricsEndpoint{{}, {}},
		},
	}
}

func vllmRename() controllerconfig.RelabelConfig {
	return controllerconfig.RelabelConfig{
		SourceLabels: []string{"__name__"},
		Regex:        "vllm_(.+)",
		TargetLabel:  "__name__",
		Replacement:  "vllm:$1",
		Action:       "replace",
	}
}

// TestApplyManagedScrapeConfig_MetricRelabelingsOnly pins that a config with
// ONLY metricRelabelings (no relabelings) is still applied to every
// endpoint; returning early on empty relabelings would drop
// metricRelabelings entirely.
func TestApplyManagedScrapeConfig_MetricRelabelingsOnly(t *testing.T) {
	pm := twoEndpointPM()
	cfg := controllerconfig.PodMonitorConfig{
		MetricRelabelings: []controllerconfig.RelabelConfig{vllmRename()},
	}

	ApplyManagedScrapeConfig(pm, cfg)

	for i, ep := range pm.Spec.PodMetricsEndpoints {
		if len(ep.RelabelConfigs) != 0 {
			t.Errorf("endpoint %d: metricRelabelings-only config must not add relabelings, got %d", i, len(ep.RelabelConfigs))
		}
		if len(ep.MetricRelabelConfigs) != 1 {
			t.Fatalf("endpoint %d: want 1 metricRelabeling, got %d", i, len(ep.MetricRelabelConfigs))
		}
		mr := ep.MetricRelabelConfigs[0]
		if mr.TargetLabel != "__name__" || mr.Regex != "vllm_(.+)" || mr.Action != "replace" {
			t.Errorf("endpoint %d: unexpected metricRelabeling: %+v", i, mr)
		}
		if mr.Replacement == nil || *mr.Replacement != "vllm:$1" {
			t.Errorf("endpoint %d: want replacement %q, got %v", i, "vllm:$1", mr.Replacement)
		}
		if len(mr.SourceLabels) != 1 || mr.SourceLabels[0] != monitoringv1.LabelName("__name__") {
			t.Errorf("endpoint %d: want sourceLabels [__name__], got %v", i, mr.SourceLabels)
		}
	}
}

// TestApplyManagedScrapeConfig_RelabelingsAndMetricRelabelings verifies both
// kinds are applied to every endpoint and appended AFTER any existing entries.
func TestApplyManagedScrapeConfig_RelabelingsAndMetricRelabelings(t *testing.T) {
	pm := twoEndpointPM()
	// Pre-existing entries on the first endpoint must be preserved.
	existing := monitoringv1.RelabelConfig{TargetLabel: "keep"}
	pm.Spec.PodMetricsEndpoints[0].RelabelConfigs = []monitoringv1.RelabelConfig{existing}
	pm.Spec.PodMetricsEndpoints[0].MetricRelabelConfigs = []monitoringv1.RelabelConfig{existing}

	cfg := controllerconfig.PodMonitorConfig{
		Labels: map[string]string{"scrape.example.com/tier": "cluster"},
		Relabelings: []controllerconfig.RelabelConfig{{
			SourceLabels: []string{"__meta_kubernetes_pod_label_ome_io_inferenceservice"},
			TargetLabel:  "inferenceservice",
		}},
		MetricRelabelings: []controllerconfig.RelabelConfig{vllmRename()},
	}

	ApplyManagedScrapeConfig(pm, cfg)

	if got := pm.Labels["scrape.example.com/tier"]; got != "cluster" {
		t.Errorf("want metadata label scrape-tier=cluster, got %q", got)
	}

	// Endpoint 0: existing preserved (first) + OME appended.
	ep0 := pm.Spec.PodMetricsEndpoints[0]
	if len(ep0.RelabelConfigs) != 2 || ep0.RelabelConfigs[0].TargetLabel != "keep" {
		t.Errorf("endpoint 0: want existing+ome relabelings, got %+v", ep0.RelabelConfigs)
	}
	if len(ep0.MetricRelabelConfigs) != 2 || ep0.MetricRelabelConfigs[0].TargetLabel != "keep" {
		t.Errorf("endpoint 0: want existing+ome metricRelabelings, got %+v", ep0.MetricRelabelConfigs)
	}

	// Endpoint 1: just the OME-appended ones.
	ep1 := pm.Spec.PodMetricsEndpoints[1]
	if len(ep1.RelabelConfigs) != 1 {
		t.Errorf("endpoint 1: want 1 relabeling, got %d", len(ep1.RelabelConfigs))
	}
	if len(ep1.MetricRelabelConfigs) != 1 {
		t.Errorf("endpoint 1: want 1 metricRelabeling, got %d", len(ep1.MetricRelabelConfigs))
	}
}
