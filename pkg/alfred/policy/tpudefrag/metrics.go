package tpudefrag

import (
	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// PublishPartitions sets the whole free partition count and the packing bound
// of every TPU shape in every TPU pod, so an operator can see the gap the
// policy acts on before enabling it. It resets both gauges first, so a pod or
// shape that disappeared leaves no stale series.
func PublishPartitions(snap *snapshot.ClusterSnapshot, cfg *config.Config, m *metrics.Metrics) {
	if m == nil {
		return
	}
	m.TPUFreePartitions.Reset()
	m.TPUPackingBound.Reset()
	if snap == nil || cfg == nil {
		return
	}
	l := buildLayout(snap, cfg)
	for pod, st := range l.stats() {
		for _, s := range l.shapes {
			m.TPUFreePartitions.WithLabelValues(pod, s.name).Set(float64(st.wholeFree[s.name]))
			m.TPUPackingBound.WithLabelValues(pod, s.name).Set(float64(l.packingBound(st, s)))
		}
	}
}
