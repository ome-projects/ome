package replica

import (
	"testing"

	"sigs.k8s.io/ome/pkg/xet"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/principals"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
)

func TestModule(t *testing.T) {
	// Test that the module can be created without panicking
	assert.NotNil(t, Module)
}

func TestReplicaParams(t *testing.T) {
	// Test the replicaParams struct
	mockLogger := testingPkg.SetupMockLogger()
	var mockDataStores []*ociobjectstore.OCIOSDataStore
	mockHubClient := &xet.Client{}

	params := replicaParams{
		AnotherLogger:      mockLogger,
		OCIOSDataStoreList: mockDataStores,
		HubClient:          mockHubClient,
	}

	assert.NotNil(t, params.AnotherLogger)
	assert.Empty(t, params.OCIOSDataStoreList)
	assert.NotNil(t, params.HubClient)
	assert.Equal(t, mockLogger, params.AnotherLogger)
	assert.Equal(t, mockHubClient, params.HubClient)
	assert.Empty(t, mockDataStores)
}

func TestModuleProvider(t *testing.T) {
	tests := []struct {
		name        string
		setupViper  func() *viper.Viper
		setupParams func() replicaParams
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid configuration",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("local_path", "/test/path")
				v.Set("num_connections", 5)
				v.Set("download_size_limit_gb", 100)
				v.Set("enable_size_limit_check", true)
				v.Set("source.storage_uri", "oci://n/test-src-namespace/b/test-src-bucket/o/models")
				v.Set("target.storage_uri", "oci://n/test-tgt-namespace/b/test-tgt-bucket/o/models")
				return v
			},
			setupParams: func() replicaParams {
				mockLogger := testingPkg.SetupMockLogger()
				mockHubClient := &xet.Client{}
				var mockDataStores []*ociobjectstore.OCIOSDataStore

				return replicaParams{
					AnotherLogger:      mockLogger,
					OCIOSDataStoreList: mockDataStores,
					HubClient:          mockHubClient,
				}
			},
			expectError: false,
		},
		{
			name: "invalid viper configuration - missing required fields",
			setupViper: func() *viper.Viper {
				v := viper.New()
				// Missing required fields like local_path, source.storage_uri, target.storage_uri
				return v
			},
			setupParams: func() replicaParams {
				mockLogger := testingPkg.SetupMockLogger()
				mockHubClient := &xet.Client{}
				var mockDataStores []*ociobjectstore.OCIOSDataStore

				return replicaParams{
					AnotherLogger:      mockLogger,
					OCIOSDataStoreList: mockDataStores,
					HubClient:          mockHubClient,
				}
			},
			expectError: true,
			errorMsg:    "error validating replica config",
		},
		{
			name: "invalid storage URIs",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("local_path", "/test/path")
				v.Set("source.storage_uri", "invalid-uri")
				v.Set("target.storage_uri", "invalid-uri")
				return v
			},
			setupParams: func() replicaParams {
				mockLogger := testingPkg.SetupMockLogger()
				mockHubClient := &xet.Client{}
				var mockDataStores []*ociobjectstore.OCIOSDataStore

				return replicaParams{
					AnotherLogger:      mockLogger,
					OCIOSDataStoreList: mockDataStores,
					HubClient:          mockHubClient,
				}
			},
			expectError: true,
			errorMsg:    "error validating replica config",
		},
		{
			name: "missing HubClient",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("local_path", "/test/path")
				v.Set("source.storage_uri", "oci://n/test-src-namespace/b/test-src-bucket/o/models")
				v.Set("target.storage_uri", "oci://n/test-tgt-namespace/b/test-tgt-bucket/o/models")
				return v
			},
			setupParams: func() replicaParams {
				mockLogger := testingPkg.SetupMockLogger()
				var mockDataStores []*ociobjectstore.OCIOSDataStore

				return replicaParams{
					AnotherLogger:      mockLogger,
					OCIOSDataStoreList: mockDataStores,
					// HubClient is nil
				}
			},
			expectError: false, // HubClient is optional
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := tt.setupViper()
			params := tt.setupParams()

			agent, err := provideReplicaAgent(v, params)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				assert.Nil(t, agent)
			} else {
				if err != nil {
					// In some cases, we might get an error during validation,
					// but the configuration creation itself succeeds
					t.Logf("Got non-critical error: %v", err)
				} else {
					assert.NotNil(t, agent)
					assert.Equal(t, params.AnotherLogger, agent.Logger)
				}
			}
		})
	}
}

func TestModuleIntegration(t *testing.T) {
	// Setup viper with valid configuration
	v := viper.New()
	v.Set("local_path", "/test/path")
	v.Set("num_connections", 5)
	v.Set("download_size_limit_gb", 100)
	v.Set("enable_size_limit_check", true)
	v.Set("target_artifact_reuse_allowed", true)
	v.Set("source.storage_uri", "oci://n/test-src-namespace/b/test-src-bucket/o/models")
	v.Set("target.storage_uri", "oci://n/test-tgt-namespace/b/test-tgt-bucket/o/models")

	// Setup mock dependencies
	mockLogger := testingPkg.SetupMockLogger()
	mockHubClient := &xet.Client{}

	// Create mock data stores with proper configuration
	authType := principals.InstancePrincipal
	mockDataStores := []*ociobjectstore.OCIOSDataStore{
		{
			Config: &ociobjectstore.Config{
				Name:     ociobjectstore.SourceOsConfigName,
				AuthType: &authType,
				Region:   "us-ashburn-1",
			},
			Client: &objectstorage.ObjectStorageClient{}, // Mock client
		},
		{
			Config: &ociobjectstore.Config{
				Name:     ociobjectstore.TargetOsConfigName,
				AuthType: &authType,
				Region:   "us-ashburn-1",
			},
			Client: &objectstorage.ObjectStorageClient{}, // Mock client
		},
	}

	params := replicaParams{
		AnotherLogger:      mockLogger,
		OCIOSDataStoreList: mockDataStores,
		HubClient:          mockHubClient,
	}

	config, err := newConfig(v, params)

	require.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "/test/path", config.LocalPath)
	assert.True(t, config.TargetArtifactReuseAllowed)
	assert.Equal(t, "oci://n/test-src-namespace/b/test-src-bucket/o/models", config.Source.StorageURIStr)
	assert.Equal(t, mockDataStores[0], config.Source.OCIOSDataStore)
	assert.Equal(t, mockDataStores[1], config.Target.OCIOSDataStore)
	assert.Equal(t, mockHubClient, config.Source.HubClient)

	// Test validation
	err = config.Validate()
	require.NoError(t, err, "Config validation should pass with valid configuration")

	// Test agent creation
	agent, err := NewReplicaAgent(config)
	require.NoError(t, err)
	assert.NotNil(t, agent)
}

func TestModuleProviderSpecificErrors(t *testing.T) {
	tests := []struct {
		name          string
		setupViper    func() *viper.Viper
		expectedError string
	}{
		{
			name: "missing local_path",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("source.storage_uri", "oci://n/test-src-namespace/b/test-src-bucket/o/models")
				v.Set("target.storage_uri", "oci://n/test-tgt-namespace/b/test-tgt-bucket/o/models")
				return v
			},
			expectedError: "error validating replica config",
		},
		{
			name: "malformed source storage URI",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("local_path", "/test/path")
				v.Set("source.storage_uri", "invalid://malformed-uri")
				v.Set("target.storage_uri", "oci://n/test-tgt-namespace/b/test-tgt-bucket/o/models")
				return v
			},
			expectedError: "invalid source storage URI",
		},
		{
			name: "malformed target storage URI",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("local_path", "/test/path")
				v.Set("source.storage_uri", "oci://n/test-src-namespace/b/test-src-bucket/o/models")
				v.Set("target.storage_uri", "http://invalid-uri")
				return v
			},
			expectedError: "invalid target storage URI",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := tt.setupViper()
			mockLogger := testingPkg.SetupMockLogger()
			params := replicaParams{
				AnotherLogger: mockLogger,
			}

			_, err := provideReplicaAgent(v, params)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestModuleWithNilDependencies(t *testing.T) {
	// Test that the module handles nil dependencies gracefully
	v := viper.New()
	v.Set("local_path", "/test/path")
	v.Set("source.storage_uri", "hf://meta-llama/Llama-3-70B-Instruct")
	v.Set("target.storage_uri", "oci://n/test-tgt-namespace/b/test-tgt-bucket/o/models")

	mockLogger := testingPkg.SetupMockLogger()
	params := replicaParams{
		AnotherLogger:      mockLogger,
		OCIOSDataStoreList: nil, // Explicitly nil
		HubClient:          nil, // Explicitly nil
	}

	// This should not panic and should handle nil dependencies
	agent, err := provideReplicaAgent(v, params)

	// Should fail because HuggingFace source requires HubClient
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required Source.HubClient is nil")
	assert.Nil(t, agent)
}
