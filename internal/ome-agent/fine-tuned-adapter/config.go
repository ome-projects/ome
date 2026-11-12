package fine_tuned_adapter

import (
	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"

	"sigs.k8s.io/ome/pkg/agentconfig"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

type Config struct {
	AnotherLogger logging.Interface

	FineTunedWeightURI               *ociobjectstore.ObjectURI      `mapstructure:"model" validate:"required"`
	UnzippedFineTunedWeightDirectory string                         `mapstructure:"unzipped_fine_tuned_weight_directory" validate:"required"`
	ZippedFineTunedWeightDirectory   string                         `mapstructure:"zipped_fine_tuned_weight_directory" validate:"required"`
	ObjectStorageDataStore           *ociobjectstore.OCIOSDataStore `validate:"required"`
}

// newConfig reads the adapter's settings from v and takes its logger and
// object store from params.
func newConfig(v *viper.Viper, params fineTunedAdapterParams) (*Config, error) {
	c, err := agentconfig.Load(v, Config{})
	if err != nil {
		return nil, err
	}
	c.AnotherLogger = params.AnotherLogger
	c.ObjectStorageDataStore = params.ObjectStorageDataStores
	return c, nil
}

func (c *Config) Validate() error {
	validate := validator.New()
	if err := validate.Struct(c); err != nil {
		return err
	}
	return nil
}
