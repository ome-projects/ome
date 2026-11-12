package modelmetadata

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/logging"
)

type metadataParams struct {
	fx.In

	Logger logging.Interface
	Zap    *zap.Logger
	Client client.Client
	Viper  *viper.Viper
}

// Module provides the model metadata extractor via fx
var Module = fx.Provide(provideMetadataExtractor)

func provideMetadataExtractor(params metadataParams) (*MetadataExtractor, error) {
	config, err := newConfig(params)
	if err != nil {
		return nil, fmt.Errorf("error creating model metadata config: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid model metadata config: %w", err)
	}

	return NewMetadataExtractor(config, params.Client, params.Zap)
}
