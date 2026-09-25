package routing

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	placementendpoint "sigs.k8s.io/ome/pkg/controller/v1beta1/placement/endpoint"
)

// TrafficMapPublisher carries a selected backend and its validated,
// installation-wide reconciliation inputs.
type TrafficMapPublisher struct {
	Publisher      placementendpoint.TrafficMapPublisher
	GlobalOptions  map[string]string
	ResyncInterval time.Duration
}

// TrafficMapPublisherFactory validates the selected publisher's opaque options
// and constructs its TrafficMap publisher implementation.
type TrafficMapPublisherFactory func(
	config PublisherConfig,
	kubeClient client.Client,
	apiReader client.Reader,
) (*TrafficMapPublisher, error)

type trafficMapPublisherRegistry struct {
	mu        sync.RWMutex
	factories map[string]TrafficMapPublisherFactory
}

var publishers = &trafficMapPublisherRegistry{factories: map[string]TrafficMapPublisherFactory{}}

// RegisterTrafficMapPublisher adds an optional publisher to this binary.
func RegisterTrafficMapPublisher(name string, factory TrafficMapPublisherFactory) error {
	if name == "" {
		return fmt.Errorf("TrafficMap publisher name must not be empty")
	}
	if factory == nil {
		return fmt.Errorf("TrafficMap publisher %q must provide a factory", name)
	}
	publishers.mu.Lock()
	defer publishers.mu.Unlock()
	if _, duplicate := publishers.factories[name]; duplicate {
		return fmt.Errorf("TrafficMap publisher %q is already registered", name)
	}
	publishers.factories[name] = factory
	return nil
}

// NewTrafficMapPublisher resolves the selected publisher. An empty name keeps
// the existing Gateway API publisher path.
func NewTrafficMapPublisher(
	cfg PublisherConfig,
	kubeClient client.Client,
	apiReader client.Reader,
) (*TrafficMapPublisher, error) {
	if cfg.ResyncInterval < 0 {
		return nil, fmt.Errorf("TrafficMap publisher resync interval must not be negative")
	}
	if !cfg.IsEnabled() {
		if len(cfg.Options) != 0 {
			return nil, fmt.Errorf("TrafficMap publisher options require a publisher name")
		}
		return nil, nil
	}
	publishers.mu.RLock()
	factory, ok := publishers.factories[cfg.Name]
	publishers.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("TrafficMap publisher %q is not compiled into this manager (available: %v)",
			cfg.Name, RegisteredTrafficMapPublishers())
	}
	cfg.Options = cloneStringMap(cfg.Options)
	publisher, err := factory(cfg, kubeClient, apiReader)
	if err != nil {
		return nil, fmt.Errorf("configure TrafficMap publisher %q: %w", cfg.Name, err)
	}
	if publisher == nil || publisher.Publisher == nil {
		return nil, fmt.Errorf("TrafficMap publisher %q factory returned nil", cfg.Name)
	}
	if publisher.Publisher.Name() != cfg.Name {
		return nil, fmt.Errorf("TrafficMap publisher %q factory returned publisher named %q",
			cfg.Name, publisher.Publisher.Name())
	}
	publisher.ResyncInterval = cfg.ResyncInterval
	publisher.GlobalOptions = cloneStringMap(publisher.GlobalOptions)
	return publisher, nil
}

// RegisteredTrafficMapPublishers lists compiled-in publishers in stable order.
func RegisteredTrafficMapPublishers() []string {
	publishers.mu.RLock()
	defer publishers.mu.RUnlock()
	names := make([]string, 0, len(publishers.factories))
	for name := range publishers.factories {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
