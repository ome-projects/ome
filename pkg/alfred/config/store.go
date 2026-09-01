package config

import (
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
// as immutable.
type Store struct {
	current                  atomic.Pointer[Config]
	capabilityLeaseNamespace string
}

// NewStore returns a store serving the built-in defaults until the first
// successful Update — Alfred starts safe (recommend-only) even if the
// ConfigMap is missing or broken at boot.
func NewStore() *Store {
	return newStore(defaultCapabilityLeaseNamespace)
}

// NewStoreForNamespace returns a store whose omitted capability Lease
// namespace follows the Alfred runtime namespace. An explicit config value
// still wins on every reload.
func NewStoreForNamespace(namespace string) (*Store, error) {
	if err := validateCapabilityLeaseNamespace(namespace); err != nil {
		return nil, err
	}
	return newStore(namespace), nil
}

func newStore(capabilityLeaseNamespace string) *Store {
	s := &Store{capabilityLeaseNamespace: capabilityLeaseNamespace}
	s.current.Store(defaultConfig(capabilityLeaseNamespace))
	return s
}

// Get returns the active configuration.
func (s *Store) Get() *Config {
	return s.current.Load()
}

// Update parses and validates raw config.yaml content. On success the new
// config becomes active and OutcomeSuccess is returned; on failure the
// previous config stays active and OutcomeFailure is returned with the
// validation error.
func (s *Store) Update(raw []byte) (string, error) {
	cfg, err := loadWithCapabilityNamespace(raw, s.capabilityLeaseNamespace)
	if err != nil {
		return OutcomeFailure, err
	}
	s.current.Store(cfg)
	return OutcomeSuccess, nil
}
