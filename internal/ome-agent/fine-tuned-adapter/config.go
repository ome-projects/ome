package fine_tuned_adapter

import (
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"

	"sigs.k8s.io/ome/pkg/agentconfig"
)

type Config struct {
	AnotherLogger logging.Interface

	FineTunedWeightURI               *ociobjectstore.ObjectURI      `mapstructure:"model" validate:"required"`
	UnzippedFineTunedWeightDirectory string                         `mapstructure:"unzipped_fine_tuned_weight_directory" validate:"required"`
	ZippedFineTunedWeightDirectory   string                         `mapstructure:"zipped_fine_tuned_weight_directory" validate:"required"`
	ObjectStorageDataStore           *ociobjectstore.OCIOSDataStore `validate:"required"`
}

type Option func(*Config) error

// Apply applies the given options to the configuration.
func (c *Config) Apply(opts ...Option) error {
	for _, o := range opts {
		if o == nil {
			continue
		}

		if err := o(c); err != nil {
			return err
		}
	}
	return nil
}

// defaultConfig returns a new configuration with default values.
func defaultConfig() *Config {
	return &Config{}
}

// NewFineTunedAdapterConfig builds and returns a new configuration from the given options.
func NewFineTunedAdapterConfig(opts ...Option) (*Config, error) {
	c := &Config{}
	if err := c.Apply(opts...); err != nil {
		return nil, err
	}

	return c, nil
}

// WithAppParams attempts to resolve the required client objects using injected named parameters
func WithAppParams(params fineTunedAdapterParams) Option {
	return func(c *Config) error {
		c.ObjectStorageDataStore = params.ObjectStorageDataStores
		return nil
	}
}

// WithAnotherLog sets the logger for the configuration.
func WithAnotherLog(logger logging.Interface) Option {
	return func(c *Config) error {
		c.AnotherLogger = logger
		return nil
	}
}

// WithViper sets the viper for the configuration.
func WithViper(v *viper.Viper) Option {
	return func(c *Config) error {

		*c = *defaultConfig()
		if err := agentconfig.BindEnvs(v, c); err != nil {
			return fmt.Errorf("error occurred when binding environment variables: %+v", err)
		}

		// Unmarshal the viper configuration into Config struct
		if err := v.Unmarshal(c); err != nil {
			return fmt.Errorf("error occurred when unmarshalling config: %+v", err)
		}

		return nil
	}
}

func (c *Config) Validate() error {
	validate := validator.New()
	if err := validate.Struct(c); err != nil {
		return err
	}
	return nil
}
