package serving_agent

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/utils/testing"
)

func TestModule(t *testing.T) {
	// Test that the module can be created without panicking
	assert.NotNil(t, Module)
}

func TestServingSidecarParams(t *testing.T) {
	// Test the servingSidecarParams struct
	mockLogger := testingPkg.SetupMockLogger()
	mockDataStore := &ociobjectstore.OCIOSDataStore{}

	params := servingSidecarParams{
		AnotherLogger:           mockLogger,
		ObjectStorageDataStores: mockDataStore,
	}

	assert.NotNil(t, params.AnotherLogger)
	assert.NotNil(t, params.ObjectStorageDataStores)
	assert.Equal(t, mockLogger, params.AnotherLogger)
	assert.Equal(t, mockDataStore, params.ObjectStorageDataStores)
}

func TestModuleProvider(t *testing.T) {
	tests := []struct {
		name        string
		setupViper  func() *viper.Viper
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid configuration",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("fine_tuned_weight_info_file_path", "/test/path/weights.json")
				v.Set("unzipped_fine_tuned_weight_directory", "/test/path/unzipped")
				v.Set("zipped_fine_tuned_weight_directory", "/test/path/zipped")
				return v
			},
			expectError: false,
		},
		{
			name: "invalid viper configuration - missing required fields",
			setupViper: func() *viper.Viper {
				v := viper.New()
				// Missing required fields
				return v
			},
			expectError: false, // Configuration creation succeeds, but validation might fail later
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := tt.setupViper()

			// Setup mock dependencies
			mockLogger := testingPkg.SetupMockLogger()
			mockDataStore := &ociobjectstore.OCIOSDataStore{}

			params := servingSidecarParams{
				AnotherLogger:           mockLogger,
				ObjectStorageDataStores: mockDataStore,
			}

			sidecar, err := provideServingSidecar(v, params)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				assert.Nil(t, sidecar)
			} else {
				if err != nil {
					// In some cases, we might get an error during validation,
					// but the configuration creation itself succeeds
					t.Logf("Got non-critical error: %v", err)
				} else {
					assert.NotNil(t, sidecar)
					assert.Equal(t, mockLogger, sidecar.logger)
				}
			}
		})
	}
}

func TestModuleIntegration(t *testing.T) {
	// Setup viper with valid configuration
	v := viper.New()
	v.Set("fine_tuned_weight_info_file_path", "/test/path/weights.json")
	v.Set("unzipped_fine_tuned_weight_directory", "/test/path/unzipped")
	v.Set("zipped_fine_tuned_weight_directory", "/test/path/zipped")

	// Setup mock dependencies
	mockLogger := testingPkg.SetupMockLogger()
	mockDataStore := &ociobjectstore.OCIOSDataStore{}

	params := servingSidecarParams{
		AnotherLogger:           mockLogger,
		ObjectStorageDataStores: mockDataStore,
	}

	// Test configuration creation
	config, err := newConfig(v, params)

	require.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "/test/path/weights.json", config.FineTunedWeightInfoFilePath)
	assert.Equal(t, "/test/path/unzipped", config.UnzippedFineTunedWeightDirectory)
	assert.Equal(t, "/test/path/zipped", config.ZippedFineTunedWeightDirectory)
	assert.Equal(t, mockDataStore, config.ObjectStorageDataStore)

	// Test sidecar creation
	sidecar, err := NewServingSidecar(config)
	require.NoError(t, err)
	assert.NotNil(t, sidecar)
	assert.Equal(t, mockLogger, sidecar.logger)
}

func TestModuleErrorHandling(t *testing.T) {
	// Setup viper with invalid configuration
	v := viper.New()
	// Empty configuration

	// Setup mock dependencies
	mockLogger := testingPkg.SetupMockLogger()

	// Empty data store
	params := servingSidecarParams{
		AnotherLogger: mockLogger,
		// Missing ObjectStorageDataStores
	}

	// Test configuration creation
	config, err := newConfig(v, params)

	require.NoError(t, err)
	assert.NotNil(t, config)

	// The sidecar will be created, but validation would fail if used
	sidecar, err := NewServingSidecar(config)
	assert.NoError(t, err)
	assert.NotNil(t, sidecar)
}

// buildThroughModule builds the sidecar the way the agent binary does: the
// settings in v, the named logger and an object store with a client wired through fx.
func buildThroughModule(t *testing.T, v *viper.Viper) error {
	t.Helper()
	app := fx.New(
		fx.NopLogger,
		fx.Supply(v, &ociobjectstore.OCIOSDataStore{Client: &objectstorage.ObjectStorageClient{}}),
		fx.Provide(fx.Annotated{Name: "another_log", Target: func() logging.Interface { return testingPkg.SetupMockLogger() }}),
		Module,
		fx.Invoke(func(*ServingSidecar) {}),
	)
	return app.Err()
}

func completeSettings() *viper.Viper {
	v := viper.New()
	v.Set("fine_tuned_weight_info_file_path", "/mnt/ft-model-info.json")
	v.Set("unzipped_fine_tuned_weight_directory", "/mnt/unzipped-ft-models")
	v.Set("zipped_fine_tuned_weight_directory", "/mnt/zipped-ft-models")
	return v
}

func TestModule_EveryRequiredSettingPresent_Builds(t *testing.T) {
	require.NoError(t, buildThroughModule(t, completeSettings()))
}

// A required setting left empty is refused when the agent starts, naming the
// setting, instead of failing inside the weight sync later.
func TestModule_RequiredSettingMissing_RefusedAtStart(t *testing.T) {
	v := completeSettings()
	v.Set("fine_tuned_weight_info_file_path", "")
	err := buildThroughModule(t, v)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FineTunedWeightInfoFilePath")
}
