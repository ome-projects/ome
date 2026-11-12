package serving_agent

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
)

func TestNewConfig(t *testing.T) {
	mockLogger := testingPkg.SetupMockLogger()
	mockDataStore := &ociobjectstore.OCIOSDataStore{}
	params := servingSidecarParams{
		AnotherLogger:           mockLogger,
		ObjectStorageDataStores: mockDataStore,
	}

	tests := []struct {
		name       string
		setupViper func() *viper.Viper
		check      func(*testing.T, *Config)
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
			check: func(t *testing.T, c *Config) {
				assert.Equal(t, "/test/path/weights.json", c.FineTunedWeightInfoFilePath)
				assert.Equal(t, "/test/path/unzipped", c.UnzippedFineTunedWeightDirectory)
				assert.Equal(t, "/test/path/zipped", c.ZippedFineTunedWeightDirectory)
			},
		},
		{
			name:       "empty viper config",
			setupViper: viper.New,
			check: func(t *testing.T, c *Config) {
				// The sidecar has no defaults, so the fields stay empty
				assert.Equal(t, "", c.FineTunedWeightInfoFilePath)
				assert.Equal(t, "", c.UnzippedFineTunedWeightDirectory)
				assert.Equal(t, "", c.ZippedFineTunedWeightDirectory)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := newConfig(tt.setupViper(), params)

			require.NoError(t, err)
			assert.Equal(t, mockLogger, config.AnotherLogger)
			assert.Equal(t, mockDataStore, config.ObjectStorageDataStore)
			tt.check(t, config)
		})
	}
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
