// Package observer runs Alfred's observation loop: rebuild the
// ClusterSnapshot on a fixed cadence and publish the snapshot-derived
// Prometheus gauges. The loop runs on every replica (not only the leader) so
// an operator can scrape any pod, and it only ever reads — actuation lives
// exclusively in the decision loop's dispatcher (OEP-0008).
package observer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// Loop is the observation-loop Runnable.
type Loop struct {
	Reader  client.Reader
	Store   *config.Store
	Metrics *metrics.Metrics
	Log     logr.Logger

	// Scorer publishes the fragmentation gauges
	// (alfred_cluster_fragmentation_score, alfred_fragmentation_*,
	// alfred_pending_pressure) from a fresh snapshot. The defrag policy
	// package wires it; while nil those gauges stay unset.
	Scorer func(*snapshot.ClusterSnapshot, *config.Config, *metrics.Metrics)

	// OMENativeExecutor reports the checked executor capability state for
	// this cluster. Nil is a bounded unavailable state.
	OMENativeExecutor func(ctx context.Context) snapshot.OMENativeExecutorState

	// Now overrides the clock in tests.
	Now func() time.Time

	latest atomic.Pointer[snapshot.ClusterSnapshot]
}

var _ manager.Runnable = &Loop{}
var _ manager.LeaderElectionRunnable = &Loop{}

// NeedLeaderElection returns false: every replica observes, so gauges keep
// flowing even while a replica is not leading.
func (l *Loop) NeedLeaderElection() bool { return false }

// Latest returns the most recent snapshot, or nil before the first pass. The
// decision loop consumes this — at most one observation interval stale.
func (l *Loop) Latest() *snapshot.ClusterSnapshot {
	return l.latest.Load()
}

// Start runs an immediate first pass, then ticks at the configured
// observation interval, re-reading the interval every pass so a config
// reload takes effect without a restart.
func (l *Loop) Start(ctx context.Context) error {
	if err := l.RunOnce(ctx); err != nil {
		// The first pass may race informer sync; log and keep ticking.
		l.Log.Error(err, "initial observation pass failed")
	}
	for {
		interval := l.Store.Get().ObservationLoopInterval.Duration
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
			if err := l.RunOnce(ctx); err != nil {
				l.Log.Error(err, "observation pass failed")
			}
		}
	}
}

// RunOnce builds one snapshot and publishes gauges. Exported so tests (and a
// forced pre-decision refresh) can drive passes directly.
func (l *Loop) RunOnce(ctx context.Context) error {
	started := l.now()
	cfg := l.Store.Get()

	opts := snapshot.Options{
		TriggerConditions: cfg.Policies.NodeHealth.TriggerConditions,
		PreemptibleLabels: cfg.SpotPolicy.PreemptibleLabels,
		DefaultMovable:    cfg.DefaultMovable,
		Now:               l.Now,
	}
	if l.OMENativeExecutor != nil {
		opts.OMENativeExecutor = l.OMENativeExecutor(ctx)
	}
	if opts.OMENativeExecutor.Available {
		l.Metrics.OMENativeUnavailable.Set(0)
	} else {
		l.Metrics.OMENativeUnavailable.Set(1)
	}

	snap, err := snapshot.Build(ctx, l.Reader, opts)
	if err != nil {
		return fmt.Errorf("build snapshot: %w", err)
	}
	l.latest.Store(snap)
	l.publish(snap, cfg)
	l.Metrics.ObservationLoopDuration.Observe(l.now().Sub(started).Seconds())
	return nil
}

func (l *Loop) publish(snap *snapshot.ClusterSnapshot, cfg *config.Config) {
	m := l.Metrics
	m.ResetSnapshotGauges()

	for name, node := range snap.Nodes {
		if node.TotalGPUs == 0 {
			continue
		}
		m.GPUCapacity.WithLabelValues(name, "total").Set(float64(node.TotalGPUs))
		m.GPUCapacity.WithLabelValues(name, "allocated").Set(float64(node.AllocatedGPUs))
		m.GPUCapacity.WithLabelValues(name, "free").Set(float64(node.FreeGPUs))
		// v1 treats intra-node GPUs as fungible; the topology hook
		// (Q-040) would refine this to true contiguous capacity.
		m.GPUCapacity.WithLabelValues(name, "contiguous_max").Set(float64(node.FreeGPUs))
	}

	m.PendingPodCount.Set(float64(len(snap.PendingPods)))
	pendingBySize := map[int64]int{}
	for _, p := range snap.PendingPods {
		pendingBySize[p.GPUsNeeded]++
	}
	for size, count := range pendingBySize {
		m.PendingPodGPURequirements.WithLabelValues(fmt.Sprintf("%d", size)).Set(float64(count))
	}

	// Surge headroom per hardware pool: the largest single-node free
	// block — the biggest replacement footprint a surge-shaped migration
	// could place while its source still holds GPUs. 0 means surge-shaped
	// candidates degrade to advisory (NoSurgeHeadroom).
	for _, pool := range snap.GPUPools() {
		var headroom int64
		for _, node := range snap.PoolNodes(pool) {
			if node.Cordoned || node.Unhealthy || node.ScaleDownMarked || node.Suspect {
				continue
			}
			if node.FreeGPUs > headroom {
				headroom = node.FreeGPUs
			}
		}
		m.SurgeHeadroomGPUs.WithLabelValues(pool).Set(float64(headroom))
	}

	if l.Scorer != nil {
		l.Scorer(snap, cfg, m)
	}
}

func (l *Loop) now() time.Time {
	if l.Now == nil {
		return time.Now()
	}
	return l.Now()
}
