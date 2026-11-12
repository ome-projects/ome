package acceleratorclassselector

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// DefaultAcceleratorFetcher is the default implementation of AcceleratorFetcher.
type DefaultAcceleratorFetcher struct {
	client client.Client
}

// NewDefaultAcceleratorFetcher creates a new DefaultAcceleratorFetcher.
func NewDefaultAcceleratorFetcher(client client.Client) AcceleratorFetcher {
	return &DefaultAcceleratorFetcher{
		client: client,
	}
}

// GetAcceleratorClass fetches an accelerator class by name. A missing class is
// reported via found=false with a nil error so callers can distinguish "does
// not exist" from transient read failures (throttling, timeouts), which are
// returned as real errors.
func (f *DefaultAcceleratorFetcher) GetAcceleratorClass(ctx context.Context, name string) (*v1beta1.AcceleratorClass, bool, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Getting accelerator class", "name", name)

	var acceleratorClass v1beta1.AcceleratorClass
	if err := f.client.Get(ctx, client.ObjectKey{Name: name}, &acceleratorClass); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to get accelerator class %s: %w", name, err)
	}

	return &acceleratorClass, true, nil
}
