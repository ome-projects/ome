package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

type earlyRefreshSource struct {
	stubSource
	refreshes atomic.Int64
}

func (s *earlyRefreshSource) Refresh(context.Context) error {
	s.refreshes.Add(1)
	return nil
}

type refreshCheckingPolicy struct {
	source *earlyRefreshSource
	passes chan int64
}

func (*refreshCheckingPolicy) Name() string { return "refresh-check" }
func (p *refreshCheckingPolicy) Evaluate(*snapshot.ClusterSnapshot, *config.Config) []policy.Candidate {
	p.passes <- p.source.refreshes.Load()
	return nil
}

// Omitting the refresh before an early pass must leave this test red even
// when the observation loop already has a cached snapshot.
func TestEarlyDecisionRefreshesBeforePolicy(t *testing.T) {
	source := &earlyRefreshSource{stubSource: stubSource{snap: scenario().Build()}}
	p := &refreshCheckingPolicy{source: source, passes: make(chan int64, 2)}
	loop, _, early := newTestLoop(t, source.snap, p)
	loop.Snapshots = source
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Start(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	for pass := 0; pass < 2; pass++ {
		select {
		case got := <-p.passes:
			if got != int64(pass) {
				t.Fatalf("pass %d ran after %d refreshes, want %d", pass, got, pass)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("decision pass did not run")
		}
		if pass == 0 {
			early <- struct{}{}
		}
	}
}
