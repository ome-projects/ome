package config

import (
	"sync"
	"sync/atomic"
)

// Reload outcomes, used as the alfred_policy_reload_total outcome label.
const (
	OutcomeSuccess = "success"
	OutcomeFailure = "failure"
)

// Store holds the active configuration with last-known-good semantics: a
// failed Update leaves the previous config serving. Reads are lock-free and
// safe from any goroutine; the returned *Config is shared and must be treated
// as immutable. Changed lets a loop that arms timers from the config react to
// an update instead of waiting out a deadline derived from stale values.
type Store struct {
	current atomic.Pointer[Config]

	// changed is closed and replaced on every successful Update so any
	// number of listeners observe the change; mu guards the swap.
	mu      sync.Mutex
	changed chan struct{}
}

// NewStore returns a store serving the built-in defaults until the first
// successful Update — Alfred starts safe (recommend-only) even if the
// ConfigMap is missing or broken at boot.
func NewStore() *Store {
	s := &Store{changed: make(chan struct{})}
	s.current.Store(Default())
	return s
}

// Get returns the active configuration.
func (s *Store) Get() *Config {
	return s.current.Load()
}

// Changed returns a channel that is closed when the active configuration next
// changes. Get already serves the announced configuration by then; callers
// re-subscribe after each wake.
func (s *Store) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

// Update parses and validates raw config.yaml content. On success the new
// config becomes active, Changed listeners are woken, and OutcomeSuccess is
// returned; on failure the previous config stays active and OutcomeFailure is
// returned with the validation error.
func (s *Store) Update(raw []byte) (string, error) {
	cfg, err := Load(raw)
	if err != nil {
		return OutcomeFailure, err
	}
	s.current.Store(cfg)
	s.mu.Lock()
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
	return OutcomeSuccess, nil
}
