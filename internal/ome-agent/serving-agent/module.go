package serving_agent

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

type servingSidecarParams struct {
	fx.In

	AnotherLogger           logging.Interface `name:"another_log"`
	ObjectStorageDataStores *ociobjectstore.OCIOSDataStore
}

var Module = fx.Provide(
	func(v *viper.Viper, params servingSidecarParams) (*ServingSidecar, error) {
		config, err := NewServingSidecarConfig(
			WithViper(v),
			WithAnotherLog(params.AnotherLogger),
			WithAppParams(params),
		)
		if err != nil {
			return nil, fmt.Errorf("error creating serving sidecar config: %+v", err)
		}
		if err := config.Validate(); err != nil {
			return nil, fmt.Errorf("error validating serving sidecar config: %w", err)
		}
		return NewServingSidecar(config)
	})
