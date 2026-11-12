package replica

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/afero"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
	"sigs.k8s.io/ome/pkg/utils/storage"
	"sigs.k8s.io/ome/pkg/xet"
)

// Define reusable struct types to avoid repetition
type SourceStruct struct {
	StorageURIStr  string `mapstructure:"storage_uri" validate:"required"`
	OCIOSDataStore *ociobjectstore.OCIOSDataStore
	HubClient      *xet.Client
	PVCFileSystem  *afero.OsFs
}

type TargetStruct struct {
	StorageURIStr  string `mapstructure:"storage_uri" validate:"required"`
	OCIOSDataStore *ociobjectstore.OCIOSDataStore
	PVCFileSystem  *afero.OsFs
	ChecksumConfig *common.ChecksumConfig `mapstructure:"checksum"`
}

func TestNewConfig(t *testing.T) {
	sourceDataStore := &ociobjectstore.OCIOSDataStore{
		Config: &ociobjectstore.Config{Name: ociobjectstore.SourceOsConfigName},
	}
	targetDataStore := &ociobjectstore.OCIOSDataStore{
		Config: &ociobjectstore.Config{Name: ociobjectstore.TargetOsConfigName},
	}
	mockHubClient := &xet.Client{}
	sourcePVCFileSystem := afero.NewOsFs().(*afero.OsFs)
	targetPVCFileSystem := afero.NewOsFs().(*afero.OsFs)
	mockLogger := testingPkg.SetupMockLogger()

	tests := []struct {
		name        string
		setupViper  func() *viper.Viper
		params      replicaParams
		expectError bool
		check       func(*testing.T, *Config)
	}{
		{
			name: "valid viper config",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.Set("local_path", "/test/path")
				v.Set("num_connections", 5)
				v.Set("download_size_limit_gb", 100)
				v.Set("enable_size_limit_check", true)
				v.Set("hf_download_timeout", "2h")
				v.Set("hf_download_stale_progress_timeout", "15m")
				v.Set("artifact_upload_lock_timeout", "3h")
				v.Set("source.storage_uri", "oci://n/test-src-namespace/b/test-src-bucket/o/models")
				v.Set("target.storage_uri", "oci://n/test-tgt-namespace/b/test-tgt-bucket/o/models")
				v.Set("target.checksum.upload_enabled", true)
				v.Set("target.checksum.algorithm", "md5")
				v.Set("target.checksum.concurrency", 3)
				return v
			},
			check: func(t *testing.T, c *Config) {
				assert.Equal(t, "/test/path", c.LocalPath)
				assert.Equal(t, 5, c.NumConnections)
				assert.Equal(t, 100, c.DownloadSizeLimitGB)
				assert.Equal(t, true, c.EnableSizeLimitCheck)
				assert.Equal(t, 2*time.Hour, c.HFDownloadTimeout)
				assert.Equal(t, 15*time.Minute, c.HFDownloadStaleProgressTimeout)
				assert.Equal(t, 3*time.Hour, c.ArtifactUploadLockTimeout)
				assert.Equal(t, "oci://n/test-src-namespace/b/test-src-bucket/o/models", c.Source.StorageURIStr)
				assert.Equal(t, "oci://n/test-tgt-namespace/b/test-tgt-bucket/o/models", c.Target.StorageURIStr)
				assert.Equal(t, &common.ChecksumConfig{
					UploadEnabled:     true,
					ChecksumAlgorithm: "md5",
					Concurrency:       3,
				}, c.Target.ChecksumConfig)
			},
		},
		{
			name: "checksum concurrency from environment",
			setupViper: func() *viper.Viper {
				v := viper.New()
				v.SetEnvPrefix("OME_AGENT")
				v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
				v.AutomaticEnv()
				v.SetDefault("target.checksum.concurrency", 8)
				t.Setenv("OME_AGENT_TARGET_CHECKSUM_CONCURRENCY", "6")
				v.Set("target.checksum.concurrency", v.Get("target.checksum.concurrency"))
				return v
			},
			check: func(t *testing.T, c *Config) {
				assert.Equal(t, 6, c.Target.ChecksumConfig.Concurrency)
			},
		},
		{
			name: "empty viper config",
			check: func(t *testing.T, c *Config) {
				// Should set defaults
				assert.Equal(t, 10, c.NumConnections)
				assert.Equal(t, 650, c.DownloadSizeLimitGB)
				assert.Equal(t, true, c.EnableSizeLimitCheck)
				assert.Equal(t, 72*time.Hour, c.HFDownloadTimeout)
				assert.Equal(t, 30*time.Minute, c.HFDownloadStaleProgressTimeout)
				assert.Equal(t, 120*time.Hour, c.ArtifactUploadLockTimeout)
			},
		},
		{
			name: "invalid configuration causing unmarshal error",
			setupViper: func() *viper.Viper {
				v := viper.New()
				// Set invalid type for num_connections (string instead of int)
				v.Set("num_connections", "invalid_int_value")
				return v
			},
			expectError: true,
		},
		{
			name: "both source OCI data store and target OCI data store present",
			params: replicaParams{
				OCIOSDataStoreList: []*ociobjectstore.OCIOSDataStore{sourceDataStore, targetDataStore},
			},
			check: func(t *testing.T, c *Config) {
				assert.Equal(t, sourceDataStore, c.Source.OCIOSDataStore)
				assert.Equal(t, targetDataStore, c.Target.OCIOSDataStore)
				assert.Nil(t, c.Source.HubClient)
			},
		},
		{
			name: "hub client and target OCI data store present",
			params: replicaParams{
				OCIOSDataStoreList: []*ociobjectstore.OCIOSDataStore{targetDataStore},
				HubClient:          mockHubClient,
			},
			check: func(t *testing.T, c *Config) {
				assert.Nil(t, c.Source.OCIOSDataStore)
				assert.Equal(t, targetDataStore, c.Target.OCIOSDataStore)
				assert.Equal(t, mockHubClient, c.Source.HubClient)
			},
		},
		{
			name: "source and target PVC file systems present",
			params: replicaParams{
				SourcePVCFileSystem: sourcePVCFileSystem,
				TargetPVCFileSystem: targetPVCFileSystem,
			},
			check: func(t *testing.T, c *Config) {
				assert.Equal(t, sourcePVCFileSystem, c.Source.PVCFileSystem)
				assert.Equal(t, targetPVCFileSystem, c.Target.PVCFileSystem)
				assert.Nil(t, c.Source.OCIOSDataStore)
				assert.Nil(t, c.Target.OCIOSDataStore)
			},
		},
		{
			name: "mixed OCI and PVC storage",
			params: replicaParams{
				OCIOSDataStoreList:  []*ociobjectstore.OCIOSDataStore{sourceDataStore},
				TargetPVCFileSystem: targetPVCFileSystem,
			},
			check: func(t *testing.T, c *Config) {
				assert.Equal(t, sourceDataStore, c.Source.OCIOSDataStore)
				assert.Nil(t, c.Source.PVCFileSystem)
				assert.Nil(t, c.Target.OCIOSDataStore)
				assert.Equal(t, targetPVCFileSystem, c.Target.PVCFileSystem)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			if tt.setupViper != nil {
				v = tt.setupViper()
			}
			params := tt.params
			params.AnotherLogger = mockLogger

			config, err := newConfig(v, params)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, config)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, mockLogger, config.AnotherLogger)
			tt.check(t, config)
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	// Define common test values
	validSourceURI := "oci://n/src-ns/b/src-bucket/o/models"
	validTargetURI := "oci://n/tgt-ns/b/tgt-bucket/o/models"
	invalidURI := "invalid-uri"

	tests := []struct {
		name        string
		setupConfig func() *Config
		expectError bool
	}{
		{
			name: "valid config",
			setupConfig: func() *Config {
				return &Config{
					LocalPath:            "/test/path",
					DownloadSizeLimitGB:  100,
					EnableSizeLimitCheck: true,
					NumConnections:       5,
					Source: SourceStruct{
						StorageURIStr: validSourceURI,
					},
					Target: TargetStruct{
						StorageURIStr: validTargetURI,
					},
				}
			},
			expectError: false,
		},
		{
			name: "source URI names no bucket",
			setupConfig: func() *Config {
				return &Config{
					LocalPath:            "/test/path",
					DownloadSizeLimitGB:  100,
					EnableSizeLimitCheck: true,
					NumConnections:       5,
					Source: SourceStruct{
						StorageURIStr: "oci://n/src-ns/b//o/models",
					},
					Target: TargetStruct{
						StorageURIStr: validTargetURI,
					},
				}
			},
			expectError: true,
		},
		{
			name: "missing LocalPath",
			setupConfig: func() *Config {
				return &Config{
					DownloadSizeLimitGB:  100,
					EnableSizeLimitCheck: true,
					NumConnections:       5,
					Source: SourceStruct{
						StorageURIStr: validSourceURI,
					},
					Target: TargetStruct{
						StorageURIStr: validTargetURI,
					},
				}
			},
			expectError: true,
		},
		{
			name: "missing Source StorageURIStr",
			setupConfig: func() *Config {
				return &Config{
					LocalPath:            "/test/path",
					DownloadSizeLimitGB:  100,
					EnableSizeLimitCheck: true,
					NumConnections:       5,
					Source: SourceStruct{
						StorageURIStr: "",
					},
					Target: TargetStruct{
						StorageURIStr: validTargetURI,
					},
				}
			},
			expectError: true,
		},
		{
			name: "missing Target StorageURIStr",
			setupConfig: func() *Config {
				return &Config{
					LocalPath:            "/test/path",
					DownloadSizeLimitGB:  100,
					EnableSizeLimitCheck: true,
					NumConnections:       5,
					Source: SourceStruct{
						StorageURIStr: validSourceURI,
					},
					Target: TargetStruct{
						StorageURIStr: "",
					},
				}
			},
			expectError: true,
		},
		{
			name: "invalid NumConnections",
			setupConfig: func() *Config {
				return &Config{
					LocalPath:            "/test/path",
					DownloadSizeLimitGB:  100,
					EnableSizeLimitCheck: true,
					NumConnections:       0,
					Source: SourceStruct{
						StorageURIStr: validSourceURI,
					},
					Target: TargetStruct{
						StorageURIStr: validTargetURI,
					},
				}
			},
			expectError: true,
		},
		{
			name: "invalid source storage URI",
			setupConfig: func() *Config {
				return &Config{
					LocalPath:            "/test/path",
					DownloadSizeLimitGB:  100,
					EnableSizeLimitCheck: true,
					NumConnections:       5,
					Source: SourceStruct{
						StorageURIStr: invalidURI,
					},
					Target: TargetStruct{
						StorageURIStr: validTargetURI,
					},
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
	assert.Equal(t, 10, config.NumConnections)
	assert.Equal(t, 650, config.DownloadSizeLimitGB)
	assert.Equal(t, true, config.EnableSizeLimitCheck)
	assert.False(t, config.TargetArtifactReuseAllowed)
	assert.Equal(t, 120*time.Hour, config.ArtifactUploadLockTimeout)
}

func TestConfig_ValidateRequiredDependencies(t *testing.T) {
	// Create mock objects for testing
	mockOCIOSDataStore := &ociobjectstore.OCIOSDataStore{}
	mockHubClient := &xet.Client{}

	tests := []struct {
		name              string
		setupConfig       func() *Config
		sourceStorageType storage.StorageType
		targetStorageType storage.StorageType
		expectError       bool
		expectedErrorMsg  string
	}{
		{
			name: "valid OCI source and OCI target with all dependencies",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						OCIOSDataStore: mockOCIOSDataStore,
					},
					Target: TargetStruct{
						OCIOSDataStore: mockOCIOSDataStore,
					},
				}
			},
			sourceStorageType: storage.StorageTypeOCI,
			targetStorageType: storage.StorageTypeOCI,
			expectError:       false,
		},
		{
			name: "valid HuggingFace source and OCI target with all dependencies",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						HubClient: mockHubClient,
					},
					Target: TargetStruct{
						OCIOSDataStore: mockOCIOSDataStore,
					},
				}
			},
			sourceStorageType: storage.StorageTypeHuggingFace,
			targetStorageType: storage.StorageTypeOCI,
			expectError:       false,
		},
		{
			name: "missing Source.OCIOSDataStore for OCI source",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						OCIOSDataStore: nil,
					},
					Target: TargetStruct{
						OCIOSDataStore: mockOCIOSDataStore,
					},
				}
			},
			sourceStorageType: storage.StorageTypeOCI,
			targetStorageType: storage.StorageTypeOCI,
			expectError:       true,
			expectedErrorMsg:  "required Source.OCIOSDataStore is nil",
		},
		{
			name: "missing Source.HubClient for HuggingFace source",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						HubClient: nil,
					},
					Target: TargetStruct{
						OCIOSDataStore: mockOCIOSDataStore,
					},
				}
			},
			sourceStorageType: storage.StorageTypeHuggingFace,
			targetStorageType: storage.StorageTypeOCI,
			expectError:       true,
			expectedErrorMsg:  "required Source.HubClient is nil",
		},
		{
			name: "missing Target.OCIOSDataStore for OCI target",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						OCIOSDataStore: mockOCIOSDataStore,
					},
					Target: TargetStruct{
						OCIOSDataStore: nil,
					},
				}
			},
			sourceStorageType: storage.StorageTypeOCI,
			targetStorageType: storage.StorageTypeOCI,
			expectError:       true,
			expectedErrorMsg:  "required Target.OCIOSDataStore is nil",
		},
		{
			name: "valid PVC source and PVC target with all dependencies",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
					},
					Target: TargetStruct{
						PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
					},
				}
			},
			sourceStorageType: storage.StorageTypePVC,
			targetStorageType: storage.StorageTypePVC,
			expectError:       false,
		},
		{
			name: "missing Source.PVCFileSystem for PVC source",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						PVCFileSystem: nil,
					},
					Target: TargetStruct{
						PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
					},
				}
			},
			sourceStorageType: storage.StorageTypePVC,
			targetStorageType: storage.StorageTypePVC,
			expectError:       true,
			expectedErrorMsg:  "required Source.PVCFileSystem is nil",
		},
		{
			name: "valid HuggingFace source and PVC target with all dependencies",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						HubClient: mockHubClient,
					},
					Target: TargetStruct{
						PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
					},
				}
			},
			sourceStorageType: storage.StorageTypeHuggingFace,
			targetStorageType: storage.StorageTypePVC,
			expectError:       false,
		},
		{
			name: "valid OCI source and PVC target with all dependencies",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						OCIOSDataStore: mockOCIOSDataStore,
					},
					Target: TargetStruct{
						PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
					},
				}
			},
			sourceStorageType: storage.StorageTypeOCI,
			targetStorageType: storage.StorageTypePVC,
			expectError:       false,
		},
		{
			name: "missing Target.PVCFileSystem for OCI source and PVC target",
			setupConfig: func() *Config {
				return &Config{
					Source: SourceStruct{
						OCIOSDataStore: mockOCIOSDataStore,
					},
					Target: TargetStruct{
						PVCFileSystem: nil,
					},
				}
			},
			sourceStorageType: storage.StorageTypeOCI,
			targetStorageType: storage.StorageTypePVC,
			expectError:       true,
			expectedErrorMsg:  "required Target.PVCFileSystem is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.setupConfig()
			err := config.ValidateRequiredDependencies(tt.sourceStorageType, tt.targetStorageType)

			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedErrorMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
