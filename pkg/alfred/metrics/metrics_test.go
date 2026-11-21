package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewRegistersEverySeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := New(registry)

	// Touch one child of every vector so Gather sees each family, then
	// count families: every declared series must be registered exactly
	// once (promauto would have panicked on a duplicate registration).
	m.ClusterFragmentationScore.Set(0.5)
	m.FragmentationObserved.WithLabelValues("h100", "8").Set(1)
	m.FragmentationReclaimable.WithLabelValues("h100").Set(0.2)
	m.PendingPressure.WithLabelValues("h100").Set(0.1)
	m.GPUCapacity.WithLabelValues("node1", "free").Set(3)
	m.PendingPodCount.Set(1)
	m.PendingPodGPURequirements.WithLabelValues("8").Set(1)
	m.SurgeHeadroomGPUs.WithLabelValues("h100").Set(5)
	m.TPUFreePartitions.WithLabelValues("pod-a", "4x4x4").Set(2)
	m.TPUPackingBound.WithLabelValues("pod-a", "4x4x4").Set(5)
	m.TPUDrains.WithLabelValues("completed").Inc()
	m.TPUFencedHosts.Set(16)
	m.RecommendationsProduced.WithLabelValues("defragmentation", "prod/svc", "engine", "fragmentation", "true").Inc()
	m.RecommendationsAccepted.WithLabelValues("defragmentation", "prod/svc", "engine").Inc()
	m.RecommendationsRejected.WithLabelValues("defragmentation", "prod/svc", "engine", "Cooldown").Inc()
	m.MigrationCalls.WithLabelValues("defragmentation", "prod/svc", "RollingUpdate", "rollingrestart").Inc()
	m.MigrationOutcome.WithLabelValues("defragmentation", "prod/svc", "RollingUpdate", "completed").Inc()
	m.LWSRecommendations.WithLabelValues("prod/svc", "manual").Inc()
	m.NodeHealthEvacuations.WithLabelValues("node1", "prod/svc", "omenative", "completed").Inc()
	m.NodeHealthSignals.WithLabelValues("node1", "RemediationSignal").Inc()
	m.CooldownOverrides.WithLabelValues("nodehealth").Inc()
	m.ObservationLoopDuration.Observe(0.1)
	m.DecisionLoopDuration.Observe(0.2)
	m.LeaderStatus.WithLabelValues("alfred-0").Set(1)
	m.PolicyReload.WithLabelValues("success").Inc()
	m.CircuitBreakerState.Set(0)
	m.OMENativeUnavailable.Set(1)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	const wantFamilies = 27
	if len(families) != wantFamilies {
		t.Fatalf("registered %d metric families, want %d", len(families), wantFamilies)
	}
	for _, family := range families {
		if family.GetHelp() == "" {
			t.Fatalf("metric %s has no help text", family.GetName())
		}
	}
}

// TestPoolGaugeLabelKeys pins the exported label contract of the pool-keyed
// gauges: the label is the node-derived hardware pool, named "pool".
func TestPoolGaugeLabelKeys(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := New(registry)
	m.FragmentationObserved.WithLabelValues("h100", "8").Set(1)
	m.FragmentationReclaimable.WithLabelValues("h100").Set(1)
	m.PendingPressure.WithLabelValues("h100").Set(1)
	m.SurgeHeadroomGPUs.WithLabelValues("h100").Set(1)

	want := map[string][]string{
		"alfred_fragmentation_observed":    {"pool", "size"},
		"alfred_fragmentation_reclaimable": {"pool"},
		"alfred_pending_pressure":          {"pool"},
		"alfred_surge_headroom_gpus":       {"pool"},
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, family := range families {
		wantLabels, ok := want[family.GetName()]
		if !ok {
			continue
		}
		seen++
		labels := family.GetMetric()[0].GetLabel()
		if len(labels) != len(wantLabels) {
			t.Fatalf("%s has %d labels, want %v", family.GetName(), len(labels), wantLabels)
		}
		for i, label := range labels {
			if label.GetName() != wantLabels[i] {
				t.Fatalf("%s label %d = %q, want %q", family.GetName(), i, label.GetName(), wantLabels[i])
			}
		}
	}
	if seen != len(want) {
		t.Fatalf("found %d of %d pool-keyed families", seen, len(want))
	}
}

func TestObserveConfigReload(t *testing.T) {
	m := New(prometheus.NewRegistry())
	m.ObserveConfigReload("success")
	m.ObserveConfigReload("failure")
	m.ObserveConfigReload("failure")
	if got := promtestutil.ToFloat64(m.PolicyReload.WithLabelValues("success")); got != 1 {
		t.Fatalf("success reloads = %v, want 1", got)
	}
	if got := promtestutil.ToFloat64(m.PolicyReload.WithLabelValues("failure")); got != 2 {
		t.Fatalf("failure reloads = %v, want 2", got)
	}
}

func TestResetSnapshotGauges(t *testing.T) {
	m := New(prometheus.NewRegistry())
	m.GPUCapacity.WithLabelValues("node-departed", "free").Set(8)
	m.SurgeHeadroomGPUs.WithLabelValues("h100").Set(5)
	m.FragmentationObserved.WithLabelValues("h100", "8").Set(1)

	m.ResetSnapshotGauges()

	for name, count := range map[string]int{
		"gpu capacity":  promtestutil.CollectAndCount(m.GPUCapacity),
		"surge":         promtestutil.CollectAndCount(m.SurgeHeadroomGPUs),
		"fragmentation": promtestutil.CollectAndCount(m.FragmentationObserved),
	} {
		if count != 0 {
			t.Fatalf("%s series survived reset: %d", name, count)
		}
	}
}
