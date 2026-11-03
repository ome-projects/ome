package fine_tuned_adapter

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

type fineTunedAdapterParams struct {
	fx.In

	AnotherLogger           logging.Interface `name:"another_log"`
	ObjectStorageDataStores *ociobjectstore.OCIOSDataStore
}

var Module = fx.Provide(
	func(v *viper.Viper, params fineTunedAdapterParams) (*FineTunedAdapter, error) {
		config, err := NewFineTunedAdapterConfig(
			WithViper(v),
			WithAnotherLog(params.AnotherLogger),
			WithAppParams(params),
		)
		if err != nil {
			return nil, fmt.Errorf("error creating fine-tuned adapter config: %+v", err)
		}
		if err := config.Validate(); err != nil {
			return nil, fmt.Errorf("error validating fine-tuned adapter config: %w", err)
		}
		return NewFineTunedAdapter(config)
	})
