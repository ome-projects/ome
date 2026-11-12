package acceleratorclassselector

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Selector is the main interface for accelerator class selection.
// It provides methods to select, validate, and list compatible accelerator classes.
type Selector interface {
	// GetAcceleratorClass selects the best accelerator class for a given inference service, runtime, and component.
	// Returns the accelerator class, the accelerator class name, and an error if the fetch fails.
	GetAcceleratorClass(ctx context.Context, isvc *v1beta1.InferenceService, runtime *v1beta1.ServingRuntimeSpec, component v1beta1.ComponentType) (*v1beta1.AcceleratorClass, string, error)
}

// AcceleratorFetcher abstracts the fetching of accelerator class resources.
type AcceleratorFetcher interface {
	// GetAcceleratorClass fetches an accelerator class by name. A missing
	// class is reported as found=false with a nil error; a non-nil error
	// means the read itself failed.
	GetAcceleratorClass(ctx context.Context, name string) (*v1beta1.AcceleratorClass, bool, error)
}

// Config holds configuration for the accelerator class selector.
type Config struct {
	// Client is the Kubernetes client (uses controller-runtime cache)
	Client client.Client
}

// NewConfig creates a new Config with default values.
func NewConfig(client client.Client) *Config {
	return &Config{
		Client: client,
	}
}
