package modelmetadata

import (
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"

	"sigs.k8s.io/ome/pkg/agentconfig"
	"sigs.k8s.io/ome/pkg/logging"
)

// Config defines the configuration for the model metadata extractor
type Config struct {
	Logger logging.Interface

	ModelPath          string `mapstructure:"model_path" validate:"required"`
	BaseModelName      string `mapstructure:"basemodel_name" validate:"required"`
	BaseModelNamespace string `mapstructure:"basemodel_namespace"`
	ClusterScoped      bool   `mapstructure:"cluster_scoped"`
}

// newConfig reads the extractor's settings from params.Viper and takes its
// logger from params.
func newConfig(params metadataParams) (*Config, error) {
	c, err := agentconfig.Load(params.Viper, Config{})
	if err != nil {
		return nil, err
	}
	if params.Logger == nil {
		return nil, errors.New("logger cannot be nil")
	}
	c.Logger = params.Logger
	return c, nil
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
