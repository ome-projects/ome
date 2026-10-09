package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// sourceReadyManager tracks the controller-runtime Warmup contract. A webhook
// listener alone does not prove that the manager can start its controllers.
// Warmup uses each controller's existing cache-sync timeout and watch set; no
// second dependency list, API polling, or second informer cache is introduced.
type sourceReadyManager struct {
	manager.Manager
	mu      sync.RWMutex
	sources []*readySource
}

type warmingRunnable interface {
	manager.Runnable
	manager.LeaderElectionRunnable
	Warmup(context.Context) error
}

type readySource struct {
	warmingRunnable
	ready atomic.Bool
}

func (m *sourceReadyManager) Add(r manager.Runnable) error {
	warming, ok := r.(warmingRunnable)
	if !ok {
		return m.Manager.Add(r)
	}
	tracked := &readySource{warmingRunnable: warming}
	if err := m.Manager.Add(tracked); err != nil {
		return err
	}
	m.mu.Lock()
	m.sources = append(m.sources, tracked)
	m.mu.Unlock()
	return nil
}

func (r *readySource) Warmup(ctx context.Context) error {
	if err := r.warmingRunnable.Warmup(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.ready.Store(true)
	context.AfterFunc(ctx, func() { r.ready.Store(false) })
	return nil
}

func (r *readySource) Start(ctx context.Context) error {
	defer r.ready.Store(false)
	return r.warmingRunnable.Start(ctx)
}

// sourcesReady is local and non-blocking. Liveness remains independent of
// Kubernetes availability. Standbys become ready after their watches sync;
// they do not need to acquire the leader lease or run reconcile workers.
func (m *sourceReadyManager) sourcesReady(_ *http.Request) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.sources) == 0 {
		return fmt.Errorf("controller sources have not been registered")
	}
	for i, source := range m.sources {
		if !source.ready.Load() {
			return fmt.Errorf("controller source %d has not completed cache sync", i)
		}
	}
	return nil
}
