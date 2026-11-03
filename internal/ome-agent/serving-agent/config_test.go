package serving_agent

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
)

func TestNewServingSidecarConfig(t *testing.T) {
	tests := []struct {
		name        string
		options     []Option
		expectError bool
		errorMsg    string
	}{
		{
			name:        "empty config",
			options:     []Option{},
			expectError: false,
		},
		{
			name: "config with logger",
			options: []Option{
				WithAnotherLog(testingPkg.SetupMockLogger()),
			},
			expectError: false,
		},
		{
			name: "config with viper",
			options: []Option{
				WithViper(viper.New()),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewServingSidecarConfig(tt.options...)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, config)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)
			}
		})
	}
}

func TestConfig_Apply(t *testing.T) {
	tests := []struct {
		name        string
		options     []Option
		expectError bool
	}{
		{
			name:        "apply empty options",
			options:     []Option{},
			expectError: false,
		},
		{
			name: "apply valid options",
			options: []Option{
				WithAnotherLog(testingPkg.SetupMockLogger()),
			},
			expectError: false,
		},
		{
			name: "apply nil option",
			options: []Option{
				nil,
				WithAnotherLog(testingPkg.SetupMockLogger()),
			},
			expectError: false,
		},
		{
			name: "apply option that returns error",
			options: []Option{
				func(c *Config) error {
					return assert.AnError
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{}
			err := config.Apply(tt.options...)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestWithAnotherLog(t *testing.T) {
	mockLogger := testingPkg.SetupMockLogger()
	config := &Config{}
	option := WithAnotherLog(mockLogger)

	err := option(config)
	assert.NoError(t, err)
	assert.Equal(t, mockLogger, config.AnotherLogger)
}

func TestWithViper(t *testing.T) {
	tests := []struct {
		name         string
		setupViper   func() *viper.Viper
		expectError  bool
		validateFunc func(*testing.T, *Config)
	}{
		{
			name: "valid viper config",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("fine_tuned_weight_info_file_path", "/test/path/weights.json")
				v.Set("unzipped_fine_tuned_weight_directory", "/test/path/unzipped")
				v.Set("zipped_fine_tuned_weight_directory", "/test/path/zipped")
				return v
			},
			expectError: false,
			validateFunc: func(t *testing.T, c *Config) {
				assert.Equal(t, "/test/path/weights.json", c.FineTunedWeightInfoFilePath)
				assert.Equal(t, "/test/path/unzipped", c.UnzippedFineTunedWeightDirectory)
				assert.Equal(t, "/test/path/zipped", c.ZippedFineTunedWeightDirectory)
			},
		},
		{
			name: "empty viper config",
			setupViper: func() *viper.Viper {
				return viper.New()
			},
			expectError: false,
			validateFunc: func(t *testing.T, c *Config) {
				// Should set defaults (which are empty in this case)
				assert.Equal(t, "", c.FineTunedWeightInfoFilePath)
				assert.Equal(t, "", c.UnzippedFineTunedWeightDirectory)
				assert.Equal(t, "", c.ZippedFineTunedWeightDirectory)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := tt.setupViper()
			config := &Config{}

			option := WithViper(v)
			err := option(config)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.validateFunc != nil {
					tt.validateFunc(t, config)
				}
			}
		})
	}
}

func TestWithAppParams(t *testing.T) {
	mockDataStore := &ociobjectstore.OCIOSDataStore{}
	params := servingSidecarParams{
		AnotherLogger:           testingPkg.SetupMockLogger(),
		ObjectStorageDataStores: mockDataStore,
	}

	config := &Config{}
	err := WithAppParams(params)(config)

	assert.NoError(t, err)
	assert.Equal(t, mockDataStore, config.ObjectStorageDataStore)
}

func TestConfig_Validate(t *testing.T) {
	// Validation only checks that the data store and its client are set; it
	// never calls the client, so no OCI configuration is needed.
	dataStore := &ociobjectstore.OCIOSDataStore{Client: &objectstorage.ObjectStorageClient{}}

	tests := []struct {
		name        string
		setupConfig func() *Config
		expectError bool
	}{
		{
			name: "valid config",
			setupConfig: func() *Config {
				return &Config{
					FineTunedWeightInfoFilePath:      "/test/path/weights.json",
					UnzippedFineTunedWeightDirectory: "/test/path/unzipped",
					ZippedFineTunedWeightDirectory:   "/test/path/zipped",
					ObjectStorageDataStore:           dataStore,
				}
			},
			expectError: false,
		},
		{
			name: "missing FineTunedWeightInfoFilePath",
			setupConfig: func() *Config {
				return &Config{
					UnzippedFineTunedWeightDirectory: "/test/path/unzipped",
					ZippedFineTunedWeightDirectory:   "/test/path/zipped",
					ObjectStorageDataStore:           dataStore,
				}
			},
			expectError: true,
		},
		{
			name: "missing UnzippedFineTunedWeightDirectory",
			setupConfig: func() *Config {
				return &Config{
					FineTunedWeightInfoFilePath:    "/test/path/weights.json",
					ZippedFineTunedWeightDirectory: "/test/path/zipped",
					ObjectStorageDataStore:         dataStore,
				}
			},
			expectError: true,
		},
		{
			name: "missing ZippedFineTunedWeightDirectory",
			setupConfig: func() *Config {
				return &Config{
					FineTunedWeightInfoFilePath:      "/test/path/weights.json",
					UnzippedFineTunedWeightDirectory: "/test/path/unzipped",
					ObjectStorageDataStore:           dataStore,
				}
			},
			expectError: true,
		},
		{
			name: "missing SourceOCIOSDataStore",
			setupConfig: func() *Config {
				return &Config{
					FineTunedWeightInfoFilePath:      "/test/path/weights.json",
					UnzippedFineTunedWeightDirectory: "/test/path/unzipped",
					ZippedFineTunedWeightDirectory:   "/test/path/zipped",
				}
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.setupConfig()
			err := config.Validate()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	config := defaultConfig()

	assert.NotNil(t, config)
	// Default config should be empty, no default values set
	assert.Equal(t, "", config.FineTunedWeightInfoFilePath)
	assert.Equal(t, "", config.UnzippedFineTunedWeightDirectory)
	assert.Equal(t, "", config.ZippedFineTunedWeightDirectory)
	assert.Nil(t, config.ObjectStorageDataStore)
}
