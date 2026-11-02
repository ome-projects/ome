package modelmetadata

import (
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"sigs.k8s.io/ome/pkg/logging"

	"sigs.k8s.io/ome/pkg/agentconfig"
)

// Config defines the configuration for the model metadata extractor
type Config struct {
	Logger logging.Interface

	ModelPath          string `mapstructure:"model_path" validate:"required"`
	BaseModelName      string `mapstructure:"basemodel_name" validate:"required"`
	BaseModelNamespace string `mapstructure:"basemodel_namespace"`
	ClusterScoped      bool   `mapstructure:"cluster_scoped"`
}

// Option defines a function that applies configuration options
type Option func(*Config) error

// Apply applies the given options to the configuration
func (c *Config) Apply(opts ...Option) error {
	for _, o := range opts {
		if o != nil {
			if err := o(c); err != nil {
				return err
			}
		}
	}
	return nil
}

// defaultConfig returns a new configuration with default values
func defaultConfig() *Config {
	return &Config{
		ClusterScoped: false,
	}
}

// NewConfig builds and returns a new configuration from the given options
func NewConfig(opts ...Option) (*Config, error) {
	c := defaultConfig()
	if err := c.Apply(opts...); err != nil {
		return nil, fmt.Errorf("failed to apply config options: %w", err)
	}
	return c, nil
}

// WithLogger sets the logger for the configuration
func WithLogger(logger logging.Interface) Option {
	return func(c *Config) error {
		if logger == nil {
			return errors.New("logger cannot be nil")
		}
		c.Logger = logger
		return nil
	}
}

// WithViper loads configuration using Viper
func WithViper(v *viper.Viper) Option {
	return func(c *Config) error {
		*c = *defaultConfig()

		// Bind environment variables
		if err := agentconfig.BindEnvs(v, c); err != nil {
			return fmt.Errorf("error binding envs: %w", err)
		}

		// Unmarshal configuration
		if err := v.Unmarshal(c); err != nil {
			return fmt.Errorf("error unmarshalling config: %w", err)
		}

		return nil
	}
}

// WithAppParams applies configuration parameters from model-metadata-specific params
func WithAppParams(params metadataParams) Option {
	return func(c *Config) error {
		// Currently no app params needed, but kept for consistency
		return nil
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Use struct validation
	validate := validator.New()
	if err := validate.Struct(c); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	// Custom validation
	if !c.ClusterScoped && c.BaseModelNamespace == "" {
		return errors.New("basemodel_namespace is required for namespace-scoped BaseModel")
	}

	return nil
}
