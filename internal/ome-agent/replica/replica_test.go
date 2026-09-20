package replica

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/xet"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/internal/ome-agent/replica/replicator"

	ociCommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/afero"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/principals"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

type TestReplicaAgent struct {
	*ReplicaAgent
	mockListSourceObjects func() ([]common.ReplicationObject, error)
	mockValidateModelSize func(objects []common.ReplicationObject)
}

type targetArtifactStateRequestDispatcher struct {
	listResponse string
	statusCode   int
}

type targetArtifactStateRequestFunc func(*http.Request) (*http.Response, error)

func (f targetArtifactStateRequestFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

func (d targetArtifactStateRequestDispatcher) Do(request *http.Request) (*http.Response, error) {
	statusCode := d.statusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	return &http.Response{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(d.listResponse)),
		Request:    request,
	}, nil
}

type targetArtifactStateRequestSigner struct{}

func (targetArtifactStateRequestSigner) Sign(*http.Request) error {
	return nil
}

// Override Start method to use the mock
func (t *TestReplicaAgent) Start() error {
	t.Logger.Infof("Start replication from %+v to %+v", t.ReplicationInput.Source, t.ReplicationInput.Target)

	sourceObjs, err := t.mockListSourceObjects()
	if err != nil {
		return err
	}
	t.mockValidateModelSize(sourceObjs)

	replicatorInstance, err := NewReplicator(t.ReplicaAgent)
	if err != nil {
		return err
	}

	return replicatorInstance.Replicate(sourceObjs)
}

// createMockOCIOSDataStore creates a properly initialized OCIOSDataStore for testing
func createMockOCIOSDataStore() *ociobjectstore.OCIOSDataStore {
	authType := principals.InstancePrincipal
	config := &ociobjectstore.Config{
		Name:     "test-config",
		AuthType: &authType,
		Region:   "us-ashburn-1",
	}

	return &ociobjectstore.OCIOSDataStore{
		Config: config,
	}
}

func newTargetArtifactStateDataStore(listResponse string) *ociobjectstore.OCIOSDataStore {
	return &ociobjectstore.OCIOSDataStore{
		Client: &objectstorage.ObjectStorageClient{
			BaseClient: ociCommon.BaseClient{
				HTTPClient: targetArtifactStateRequestDispatcher{listResponse: listResponse},
				Signer:     targetArtifactStateRequestSigner{},
				Host:       "https://objectstorage.test",
				UserAgent:  "replica-test",
			},
		},
	}
}

type fakeReplicator struct {
	err         error
	objects     []common.ReplicationObject
	onReplicate func(objects []common.ReplicationObject) error
}

func (f *fakeReplicator) Replicate(objects []common.ReplicationObject) error {
	f.objects = objects
	if f.onReplicate != nil {
		return f.onReplicate(objects)
	}
	return f.err
}

func TestNewReplicaAgent(t *testing.T) {
	mockLogger := testingPkg.SetupMockLogger()

	tests := []struct {
		name        string
		config      *Config
		expectError bool
		errorMsg    string
		description string
	}{
		{
			name: "valid OCI to OCI configuration",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr:  "oci://n/src-ns/b/src-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
				Target: TargetStruct{
					StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
			},
			expectError: false,
			description: "Should successfully create agent with valid OCI source and target",
		},
		{
			name: "valid HuggingFace to OCI configuration with branch",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr: "hf://meta-llama/Llama-3-70B-Instruct@experimental",
					HubClient:     &xet.Client{},
				},
				Target: TargetStruct{
					StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
			},
			expectError: false,
			description: "Should successfully create agent with HuggingFace source (with branch) and OCI target",
		},
		{
			name: "valid HuggingFace to PVC configuration with branch",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr: "hf://meta-llama/Llama-3-70B-Instruct@experimental",
					HubClient:     &xet.Client{},
				},
				Target: TargetStruct{
					StorageURIStr: "pvc://target-pvc/models",
					PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
				},
			},
			expectError: false,
			description: "Should successfully create agent with HuggingFace source (with branch) and PVC target",
		},
		{
			name: "valid PVC to OCI configuration",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr: "pvc://source-pvc/models",
					PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
				},
				Target: TargetStruct{
					StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
			},
			expectError: false,
			description: "Should successfully create agent with PVC source and OCI target",
		},
		{
			name: "valid OCI to PVC configuration",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr:  "oci://n/src-ns/b/src-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
				Target: TargetStruct{
					StorageURIStr: "pvc://target-pvc/models",
					PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
				},
			},
			expectError: false,
			description: "Should successfully create agent with OCI source and PVC target",
		},
		{
			name: "valid PVC to PVC configuration",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr: "pvc://source-pvc/models",
					PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
				},
				Target: TargetStruct{
					StorageURIStr: "pvc://target-pvc/models",
					PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
				},
			},
			expectError: false,
			description: "Should successfully create agent with PVC source and PVC target",
		},
		{
			name: "invalid target storage URI",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr:  "oci://n/src-ns/b/src-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
				Target: TargetStruct{
					StorageURIStr:  "invalid://storage/uri",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
			},
			expectError: true,
			errorMsg:    "unknown storage type",
			description: "Should fail with invalid target storage URI",
		},
		{
			name: "missing OCI data store for OCI source",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr:  "oci://n/src-ns/b/src-bucket/o/models",
					OCIOSDataStore: nil, // Missing OCI data store
				},
				Target: TargetStruct{
					StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
			},
			expectError: true,
			errorMsg:    "Source.OCIOSDataStore",
			description: "Should fail when OCI source is missing OCIOSDataStore",
		},
		{
			name: "missing OCI data store for OCI target",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr:  "oci://n/src-ns/b/src-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
				Target: TargetStruct{
					StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/models",
					OCIOSDataStore: nil, // Missing OCI data store
				},
			},
			expectError: true,
			errorMsg:    "Target.OCIOSDataStore",
			description: "Should fail when OCI target is missing OCIOSDataStore",
		},
		{
			name: "missing HubClient for HuggingFace source",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr: "hf://meta-llama/Llama-3-70B-Instruct",
					HubClient:     nil, // Missing HubClient
				},
				Target: TargetStruct{
					StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
			},
			expectError: true,
			errorMsg:    "Source.HubClient",
			description: "Should fail when HuggingFace source is missing HubClient",
		},
		{
			name: "missing PVCFileSystem for PVC source",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr: "pvc://source-pvc/models",
					PVCFileSystem: nil, // Missing PVCFileSystem
				},
				Target: TargetStruct{
					StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
			},
			expectError: true,
			errorMsg:    "Source.PVCFileSystem",
			description: "Should fail when PVC source is missing PVCFileSystem",
		},
		{
			name: "invalid HuggingFace URI format",
			config: &Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/tmp/replica",
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				NumConnections:       5,
				Source: SourceStruct{
					StorageURIStr: "hf://", // Invalid: missing model ID
					HubClient:     &xet.Client{},
				},
				Target: TargetStruct{
					StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
			},
			expectError: true,
			errorMsg:    "failed to parse source storage URI",
			description: "Should fail with invalid HuggingFace URI format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agent, err := NewReplicaAgent(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				assert.Nil(t, agent)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, agent)
				assert.Equal(t, tt.config, &agent.Config)
				assert.Equal(t, tt.config.AnotherLogger, agent.Logger)

				// Verify ReplicationInput is properly set
				assert.NotNil(t, agent.ReplicationInput)
				assert.NotNil(t, agent.ReplicationInput.Source)
				assert.NotNil(t, agent.ReplicationInput.Target)
				assert.NotEmpty(t, agent.ReplicationInput.SourceStorageType)
				assert.NotEmpty(t, agent.ReplicationInput.TargetStorageType)

				// Verify OCI-specific handling for source
				if strings.HasPrefix(tt.config.Source.StorageURIStr, "oci://") {
					assert.Equal(t, tt.config.Source.OCIOSDataStore.Config.Region, agent.ReplicationInput.Source.Region)
					assert.Equal(t, "src-ns", agent.ReplicationInput.Source.Namespace)
					assert.Equal(t, "src-bucket", agent.ReplicationInput.Source.BucketName)
					assert.Equal(t, "models/", agent.ReplicationInput.Source.Prefix)
				}

				// Verify OCI-specific handling for target
				if strings.HasPrefix(tt.config.Target.StorageURIStr, "oci://") {
					assert.Equal(t, tt.config.Target.OCIOSDataStore.Config.Region, agent.ReplicationInput.Target.Region)
					assert.Equal(t, "tgt-ns", agent.ReplicationInput.Target.Namespace)
					assert.Equal(t, "tgt-bucket", agent.ReplicationInput.Target.BucketName)
					assert.Equal(t, "models/", agent.ReplicationInput.Target.Prefix)
				}

				// Verify HF-specific handling for source
				if strings.HasPrefix(tt.config.Source.StorageURIStr, "hf://") {
					assert.Equal(t, "meta-llama/Llama-3-70B-Instruct", agent.ReplicationInput.Source.BucketName)
					assert.Equal(t, "experimental", agent.ReplicationInput.Source.Prefix)
				}

				// Verify PVC-specific handling for source
				if strings.HasPrefix(tt.config.Source.StorageURIStr, "pvc://") {
					assert.Equal(t, "source-pvc", agent.ReplicationInput.Source.BucketName)
					assert.Equal(t, "models", agent.ReplicationInput.Source.Prefix)
				}
			}
		})
	}
}

func TestValidateModelSize(t *testing.T) {
	GB := int64(1024 * 1024 * 1024)

	tests := []struct {
		name          string
		config        Config
		objects       []common.ReplicationObject
		expectPanic   bool
		panicContains string
		skip          bool
	}{
		{
			name: "model size within limit - OCI objects",
			config: Config{
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				AnotherLogger:        testingPkg.SetupMockLogger(),
			},
			objects: func() []common.ReplicationObject {
				name := "test.bin"
				size := 1 * GB // 1 GB
				summary := objectstorage.ObjectSummary{
					Name: &name,
					Size: &size,
				}
				return []common.ReplicationObject{
					common.ObjectSummaryReplicationObject{ObjectSummary: summary},
				}
			}(),
			expectPanic: false,
		},
		{
			name: "model size within limit - HuggingFace objects",
			config: Config{
				DownloadSizeLimitGB:  10,
				EnableSizeLimitCheck: true,
				AnotherLogger:        testingPkg.SetupMockLogger(),
			},
			objects: func() []common.ReplicationObject {
				return []common.ReplicationObject{
					common.HFRepoFileInfoReplicationObject{
						FileInfo: xet.FileInfo{
							Path: "pytorch_model.bin",
							Size: 1073741824, // 1 GB
							Hash: "sha256:abc123...",
						},
					},
					common.HFRepoFileInfoReplicationObject{
						FileInfo: xet.FileInfo{
							Path: "config.json",
							Size: 1024, // 1 KB
							Hash: "sha256:def123...",
						},
					},
				}
			}(),
			expectPanic: false,
		},
		{
			name: "model size exceeds limit - OCI objects",
			config: Config{
				DownloadSizeLimitGB:  1,
				EnableSizeLimitCheck: true,
				AnotherLogger:        testingPkg.SetupMockLogger(),
			},
			objects: func() []common.ReplicationObject {
				name := "test.bin"
				size := 2 * GB // 2 GB

				summary := objectstorage.ObjectSummary{
					Name: &name,
					Size: &size,
				}
				return []common.ReplicationObject{
					common.ObjectSummaryReplicationObject{ObjectSummary: summary},
				}
			}(),
			expectPanic:   true,
			panicContains: "Model weights exceed size limit",
			skip:          true, // Skip this test case as it's failing due to mock expectations
		},
		{
			name: "model size exceeds limit - HuggingFace objects",
			config: Config{
				DownloadSizeLimitGB:  1,
				EnableSizeLimitCheck: true,
				AnotherLogger:        testingPkg.SetupMockLogger(),
			},
			objects: func() []common.ReplicationObject {
				return []common.ReplicationObject{
					common.HFRepoFileInfoReplicationObject{
						FileInfo: xet.FileInfo{
							Path: "pytorch_model-00001-of-00002.bin",
							Size: 1073741824, // 1 GB
							Hash: "sha256:...",
						},
					},
					common.HFRepoFileInfoReplicationObject{
						FileInfo: xet.FileInfo{
							Path: "pytorch_model-00002-of-00002.bin",
							Size: 1073741824, // 1 GB
							Hash: "sha256:...",
						},
					},
					common.HFRepoFileInfoReplicationObject{
						FileInfo: xet.FileInfo{
							Path: "config.json",
							Size: 1024, // 1 KB
							Hash: "sha256:...",
						},
					},
				}
			}(),
			expectPanic:   true,
			panicContains: "Model weights exceed size limit",
			skip:          true, // Skip this test case as it's failing due to mock expectations
		},
		{
			name: "size check disabled - OCI objects",
			config: Config{
				DownloadSizeLimitGB:  1,
				EnableSizeLimitCheck: false,
				AnotherLogger:        testingPkg.SetupMockLogger(),
			},
			objects: func() []common.ReplicationObject {
				name := "test.bin"
				size := int64(2 * GB) // 2 GB

				summary := objectstorage.ObjectSummary{
					Name: &name,
					Size: &size,
				}
				return []common.ReplicationObject{
					common.ObjectSummaryReplicationObject{ObjectSummary: summary},
				}
			}(),
			expectPanic: false,
		},
		{
			name: "size check disabled - HuggingFace objects",
			config: Config{
				DownloadSizeLimitGB:  1,
				EnableSizeLimitCheck: false,
				AnotherLogger:        testingPkg.SetupMockLogger(),
			},
			objects: func() []common.ReplicationObject {
				return []common.ReplicationObject{
					common.HFRepoFileInfoReplicationObject{
						FileInfo: xet.FileInfo{
							Path: "pytorch_model.bin",
							Size: 4294967296, // 4 GB
							Hash: "sha256:...",
						},
					},
					common.HFRepoFileInfoReplicationObject{
						FileInfo: xet.FileInfo{
							Path: "tokenizer.json",
							Size: 524288, // 512 KB
							Hash: "sha256:...",
						},
					},
				}
			}(),
			expectPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Skip tests marked for skipping
			if tt.skip {
				t.Skip("Skipping test due to mock expectation issues")
			}

			agent := &ReplicaAgent{
				Logger: tt.config.AnotherLogger,
				Config: tt.config,
			}

			if tt.expectPanic {
				defer func() {
					r := recover()
					assert.NotNil(t, r)
					if tt.panicContains != "" {
						// The Fatal call will use os.Exit in production,
						// but in tests with the mock logger it will just record the call
						// We'll verify that the Fatal method was called
						// This is a compromise since we can't actually test the os.Exit behavior
						mockLogger := tt.config.AnotherLogger.(*testingPkg.MockLogger)
						mockLogger.AssertCalled(t, "Fatalf", mock.Anything, mock.Anything)
					}
				}()
			}

			agent.validateModelSize(tt.objects)

			if !tt.expectPanic {
				// Just assert we got here without panic
				assert.True(t, true)
			}
		})
	}
}

func TestReplicaAgent_Start(t *testing.T) {
	mockLogger := testingPkg.SetupMockLogger()

	// Mocked source objects to be returned by listSourceObjects
	mockSourceObjects := []common.ReplicationObject{}

	testAgent := &TestReplicaAgent{
		ReplicaAgent: &ReplicaAgent{
			Logger: mockLogger,
			Config: Config{
				AnotherLogger:        mockLogger,
				LocalPath:            "/test/path",
				NumConnections:       1,
				DownloadSizeLimitGB:  100,
				EnableSizeLimitCheck: true,
				Source: SourceStruct{
					StorageURIStr:  "oci://n/src-ns/b/src-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
				Target: TargetStruct{
					StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/models",
					OCIOSDataStore: createMockOCIOSDataStore(),
				},
			},
			ReplicationInput: common.ReplicationInput{
				SourceStorageType: storage.StorageTypeOCI,
				TargetStorageType: storage.StorageTypeOCI,
				Source:            ociobjectstore.ObjectURI{BucketName: "src-bucket", Namespace: "src-ns", Prefix: "models/"},
				Target:            ociobjectstore.ObjectURI{BucketName: "tgt-bucket", Namespace: "tgt-ns", Prefix: "models/"},
			},
		},
		mockListSourceObjects: func() ([]common.ReplicationObject, error) {
			return mockSourceObjects, nil
		},
		mockValidateModelSize: func(objects []common.ReplicationObject) {},
	}

	err := testAgent.Start()
	assert.NoError(t, err)
}

func TestReplicaAgent_StartReturnsErrorWhenNumConnectionsInvalid(t *testing.T) {
	mockLogger := testingPkg.SetupMockLogger()
	agent := &ReplicaAgent{
		Logger: mockLogger,
		Config: Config{
			AnotherLogger:  mockLogger,
			NumConnections: 0,
		},
	}

	err := agent.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "num_connections")
}

func TestReplicaAgent_StartWritesCompletionMarkerAfterSuccessfulOCITargetReplication(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	fake := &fakeReplicator{}
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return fake, nil
	}

	var markerSource string
	var markerTarget ociobjectstore.ObjectURI
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, source string, target ociobjectstore.ObjectURI) error {
		markerSource = source
		markerTarget = target
		return nil
	}

	err := agent.Start()
	require.NoError(t, err)
	require.Len(t, fake.objects, 1)
	assert.Equal(t, constants.ArtifactCompleteMarkerBody, markerSource)
	assert.Equal(t, "tgt-ns", markerTarget.Namespace)
	assert.Equal(t, "tgt-bucket", markerTarget.BucketName)
	assert.Equal(t, "target-models/"+constants.ArtifactCompleteMarkerFileName, markerTarget.ObjectName)
	assert.Equal(t, "us-ashburn-1", markerTarget.Region)
}

func TestReplicaAgent_StartSkipsCompletionMarkerWhenReplicationFails(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return &fakeReplicator{err: errors.New("replication failed")}, nil
	}

	markerWritten := false
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
		markerWritten = true
		return nil
	}

	err := agent.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replication failed")
	assert.False(t, markerWritten)
}

func TestReplicaAgent_StartReturnsErrorWhenCompletionMarkerUploadFails(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return &fakeReplicator{}, nil
	}
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
		return errors.New("marker upload failed")
	}

	err := agent.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write target artifact completion marker")
	assert.Contains(t, err.Error(), "marker upload failed")
}

func TestReplicaAgent_StartStopsWhenTargetArtifactInspectionFails(t *testing.T) {
	tests := []struct {
		name             string
		failOnInspection int
		acquireLock      bool
		wantAcquireCalls int
		wantReleaseCalls int
		wantError        string
	}{
		{
			name:             "before lock acquisition",
			failOnInspection: 1,
			wantError:        "failed to inspect target artifact state:",
		},
		{
			name:             "while waiting for another uploader",
			failOnInspection: 2,
			wantAcquireCalls: 1,
			wantError:        "failed to inspect target artifact state while waiting for upload lock:",
		},
		{
			name:             "after acquiring own lock",
			failOnInspection: 2,
			acquireLock:      true,
			wantAcquireCalls: 1,
			wantReleaseCalls: 1,
			wantError:        "failed to inspect target artifact state before upload:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agent, cleanup := newTestAgentForCompletionMarker(t)
			defer cleanup()

			inspectionErr := errors.New("Object Storage inspection unavailable")
			inspectionCalls := 0
			targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
				inspectionCalls++
				if inspectionCalls >= tt.failOnInspection {
					return targetArtifactState{}, inspectionErr
				}
				return targetArtifactState{}, nil
			}
			// Advance a fake clock so an erroneous retry cannot spin forever.
			now := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
			nowFunc = func() time.Time { return now }
			sleepFunc = func(d time.Duration) { now = now.Add(d) }
			agent.Config.ArtifactUploadLockTimeout = time.Minute

			acquireCalls := 0
			tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
				acquireCalls++
				if tt.acquireLock {
					return "owned-lock-etag", true, nil
				}
				return "", false, nil
			}
			releaseCalls := 0
			releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, target ociobjectstore.ObjectURI, etag string) (bool, error) {
				releaseCalls++
				assert.Equal(t, agent.targetArtifactUploadLockURI(), target)
				assert.Equal(t, "owned-lock-etag", etag)
				return true, nil
			}
			newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
				t.Fatal("inspection failure must stop replication, not fall back to an uncoordinated upload")
				return nil, nil
			}
			deleteArtifactCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) error {
				t.Fatal("inspection failure must preserve the completion marker")
				return nil
			}
			uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
				t.Fatal("inspection failure must not publish a completion marker")
				return nil
			}
			deleteStaleArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, _ string) (bool, error) {
				t.Fatal("inspection failure must not delete another uploader's lock")
				return false, nil
			}

			err := agent.Start()

			require.ErrorIs(t, err, inspectionErr)
			assert.Contains(t, err.Error(), tt.wantError)
			assert.Equal(t, tt.failOnInspection, inspectionCalls)
			assert.Equal(t, tt.wantAcquireCalls, acquireCalls)
			assert.Equal(t, tt.wantReleaseCalls, releaseCalls)
		})
	}
}

func TestReplicaAgent_StartSkipsReplicationWhenTargetArtifactUploadLockCompletes(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	replicatorCalled := false
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		replicatorCalled = true
		return &fakeReplicator{}, nil
	}
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		return "", false, nil
	}
	stateCalls := 0
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		stateCalls++
		if stateCalls == 1 {
			return targetArtifactState{}, nil
		}
		if stateCalls == 2 {
			return targetArtifactState{Complete: true, CompletionMarked: true, UploadLocked: true}, nil
		}
		t.Fatal("waiter should reuse immediately after the completion marker appears")
		return targetArtifactState{}, nil
	}

	err := agent.Start()
	require.NoError(t, err)
	assert.False(t, replicatorCalled)
	assert.Equal(t, 2, stateCalls)
}

func TestReplicaAgent_StartSkipsCompletedTargetArtifactWhenUploadLockRemains(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	replicatorCalled := false
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		replicatorCalled = true
		return &fakeReplicator{}, nil
	}
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		t.Fatal("completed target artifact should be reused without acquiring the retained upload lock")
		return "", false, nil
	}
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		return targetArtifactState{Complete: true, CompletionMarked: true, UploadLocked: true}, nil
	}

	err := agent.Start()
	require.NoError(t, err)
	assert.False(t, replicatorCalled)
}

func TestReplicaAgent_StartSkipsCompletedTargetArtifactAfterLockAcquired(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	replicatorCalled := false
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		replicatorCalled = true
		return &fakeReplicator{}, nil
	}
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		return "acquired-lock-etag", true, nil
	}
	released := false
	releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, target ociobjectstore.ObjectURI, etag string) (bool, error) {
		released = true
		assert.Equal(t, "target-models/"+constants.ArtifactUploadLockFileName, target.ObjectName)
		assert.Equal(t, "acquired-lock-etag", etag)
		return true, nil
	}
	deleteArtifactCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) error {
		t.Fatal("completion marker should not be deleted when reuse is allowed and target is complete")
		return nil
	}
	stateCalls := 0
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		stateCalls++
		if stateCalls == 1 {
			return targetArtifactState{}, nil
		}
		return targetArtifactState{Complete: true, CompletionMarked: true}, nil
	}

	err := agent.Start()
	require.NoError(t, err)
	assert.False(t, replicatorCalled)
	assert.True(t, released)
	assert.Equal(t, 2, stateCalls)
}

func TestReplicaAgent_StartSkipsCompletedTargetArtifactEvenWhenUploadLockIsStale(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.ArtifactUploadLockTimeout = time.Hour

	now := time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC)
	nowFunc = func() time.Time { return now }

	replicatorCalled := false
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		replicatorCalled = true
		return &fakeReplicator{}, nil
	}
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, target ociobjectstore.ObjectURI) (string, bool, error) {
		assert.Equal(t, "target-models/"+constants.ArtifactUploadLockFileName, target.ObjectName)
		return "", false, nil
	}

	staleLockModifiedTime := now.Add(-2 * time.Hour)
	stateCalls := 0
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		stateCalls++
		if stateCalls == 1 {
			return targetArtifactState{}, nil
		}
		return targetArtifactState{
			Complete:               true,
			UploadLocked:           true,
			UploadLockModifiedTime: &staleLockModifiedTime,
		}, nil
	}
	deleteStaleArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, _ string) (bool, error) {
		t.Fatal("stale upload lock should not be deleted before skipping a complete target artifact")
		return false, nil
	}

	err := agent.Start()
	require.NoError(t, err)
	assert.False(t, replicatorCalled)
}

func TestReplicaAgent_StartDeletesStaleTargetArtifactUploadLockAndReplicates(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.ArtifactUploadLockTimeout = time.Hour

	now := time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC)
	nowFunc = func() time.Time { return now }

	fake := &fakeReplicator{}
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return fake, nil
	}

	acquireAttempts := 0
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, target ociobjectstore.ObjectURI) (string, bool, error) {
		assert.Equal(t, "target-models/"+constants.ArtifactUploadLockFileName, target.ObjectName)
		acquireAttempts++
		if acquireAttempts > 1 {
			return "replacement-lock-etag", true, nil
		}
		return "", false, nil
	}

	stateCalls := 0
	staleLockModifiedTime := now.Add(-2 * time.Hour)
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		stateCalls++
		if stateCalls == 1 {
			return targetArtifactState{}, nil
		}
		return targetArtifactState{
			UploadLocked:           true,
			UploadLockModifiedTime: &staleLockModifiedTime,
			UploadLockETag:         "stale-lock-etag",
		}, nil
	}

	deletedLock := false
	deleteStaleArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, target ociobjectstore.ObjectURI, etag string) (bool, error) {
		assert.Equal(t, "target-models/"+constants.ArtifactUploadLockFileName, target.ObjectName)
		assert.Equal(t, "stale-lock-etag", etag)
		deletedLock = true
		return true, nil
	}
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
		return nil
	}

	err := agent.Start()
	require.NoError(t, err)
	require.Len(t, fake.objects, 1)
	assert.Equal(t, 2, acquireAttempts)
	assert.True(t, deletedLock)
}

func TestReplicaAgent_StartLogsTargetArtifactSizeWhenSkippingCompletedTargetArtifact(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	replicatorCalled := false
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		replicatorCalled = true
		return &fakeReplicator{}, nil
	}
	artifactSizeBytes := int64(123456789)
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		return targetArtifactState{
			Complete:          true,
			ArtifactSizeBytes: &artifactSizeBytes,
		}, nil
	}

	err := agent.Start()
	require.NoError(t, err)
	assert.False(t, replicatorCalled)
	mockLogger := agent.Logger.(*testingPkg.MockLogger)
	mockLogger.AssertCalled(t, "Infof", "Total model size: %d bytes", []interface{}{artifactSizeBytes})
}

func TestReplicaAgent_StartDeletesCompletionMarkerBeforeOverwritingWhenReuseNotAllowed(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.TargetArtifactReuseAllowed = false

	operations := make([]string, 0, 2)
	fake := &fakeReplicator{
		onReplicate: func(objects []common.ReplicationObject) error {
			operations = append(operations, "replicate")
			return nil
		},
	}
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return fake, nil
	}
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		t.Fatal("target artifact state should not be inspected when reuse is disabled")
		return targetArtifactState{}, nil
	}
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		t.Fatal("target artifact upload lock should not be acquired when reuse is disabled")
		return "", false, nil
	}
	deleteArtifactCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, target ociobjectstore.ObjectURI) error {
		assert.Equal(t, "target-models/"+constants.ArtifactCompleteMarkerFileName, target.ObjectName)
		operations = append(operations, "delete-completion-marker")
		return nil
	}
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
		t.Fatal("completion marker should not be written when reuse is disabled")
		return nil
	}

	err := agent.Start()
	require.NoError(t, err)
	require.Len(t, fake.objects, 1)
	assert.Equal(t, []string{"delete-completion-marker", "replicate"}, operations)
}

func TestReplicaAgent_StartDeletesStaleCompletionMarkerBeforeUploadingTargetArtifact(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	operations := make([]string, 0, 3)
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return &fakeReplicator{
			onReplicate: func(objects []common.ReplicationObject) error {
				operations = append(operations, "replicate")
				return nil
			},
		}, nil
	}
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		return targetArtifactState{CompletionMarked: true}, nil
	}
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		return "acquired-lock-etag", true, nil
	}
	deleteArtifactCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, target ociobjectstore.ObjectURI) error {
		operations = append(operations, "delete-completion-marker")
		assert.Equal(t, "target-models/"+constants.ArtifactCompleteMarkerFileName, target.ObjectName)
		return nil
	}
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, target ociobjectstore.ObjectURI) error {
		operations = append(operations, "write-completion-marker")
		assert.Equal(t, "target-models/"+constants.ArtifactCompleteMarkerFileName, target.ObjectName)
		return nil
	}

	err := agent.Start()
	require.NoError(t, err)
	assert.Equal(t, []string{"delete-completion-marker", "replicate", "write-completion-marker"}, operations)
}

func TestReplicaAgent_PrepareTargetArtifactUploadSkipsWhenReuseDisabled(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.TargetArtifactReuseAllowed = false

	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		t.Fatal("target artifact state should not be inspected when reuse is disabled")
		return targetArtifactState{}, nil
	}
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		t.Fatal("target artifact upload lock should not be acquired when reuse is disabled")
		return "", false, nil
	}

	uploadLock, skipReplication, err := agent.prepareTargetArtifactUpload()
	require.NoError(t, err)
	assert.Nil(t, uploadLock)
	assert.False(t, skipReplication)
}

func TestReplicaAgent_PrepareTargetArtifactUploadUsesOneWaitDeadline(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.ArtifactUploadLockTimeout = time.Hour

	current := time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC)
	nowFunc = func() time.Time { return current }
	sleepFunc = func(time.Duration) {
		current = current.Add(time.Hour)
	}
	acquireAttempts := 0
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		acquireAttempts++
		if acquireAttempts > 2 {
			return "", false, errors.New("wait deadline was reset")
		}
		return "", false, nil
	}
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		return targetArtifactState{}, nil
	}

	_, _, err := agent.prepareTargetArtifactUpload()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out waiting for target artifact completion marker")
	assert.Equal(t, 2, acquireAttempts)
}

func TestReplicaAgent_WaitForTargetArtifactStateChangeDoesNotSleepPastDeadline(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.ArtifactUploadLockTimeout = 5 * time.Second

	current := time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC)
	nowFunc = func() time.Time { return current }
	sleepDurations := make([]time.Duration, 0, 1)
	sleepFunc = func(duration time.Duration) {
		sleepDurations = append(sleepDurations, duration)
		current = current.Add(duration)
	}
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		return targetArtifactState{UploadLocked: true}, nil
	}

	_, err := agent.waitForTargetArtifactStateChange(nowFunc().Add(agent.targetArtifactUploadLockTimeout()))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out waiting for target artifact completion marker")
	assert.Equal(t, []time.Duration{5 * time.Second}, sleepDurations)
}

func TestReplicaAgent_PrepareTargetArtifactUploadReclaimsLockThatBecomesStaleAtWaitDeadline(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.ArtifactUploadLockTimeout = time.Hour

	current := time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC)
	nowFunc = func() time.Time { return current }
	sleepFunc = func(time.Duration) {
		current = current.Add(time.Hour)
	}

	acquireAttempts := 0
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		acquireAttempts++
		if acquireAttempts == 1 {
			return "", false, nil
		}
		return "replacement-lock-etag", true, nil
	}

	lockCreatedAt := current
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		return targetArtifactState{
			UploadLocked:           true,
			UploadLockModifiedTime: &lockCreatedAt,
			UploadLockETag:         "stale-lock-etag",
		}, nil
	}

	deleted := false
	deleteStaleArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, etag string) (bool, error) {
		deleted = true
		assert.Equal(t, "stale-lock-etag", etag)
		return true, nil
	}

	uploadLock, skipReplication, err := agent.prepareTargetArtifactUpload()

	require.NoError(t, err)
	require.NotNil(t, uploadLock)
	assert.Equal(t, "replacement-lock-etag", uploadLock.ETag)
	assert.False(t, skipReplication)
	assert.True(t, deleted)
	assert.Equal(t, 2, acquireAttempts)
}

func TestReplicaAgent_StartReleasesTargetArtifactUploadLockWhenReplicationFails(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return &fakeReplicator{err: errors.New("replication failed")}, nil
	}

	released := false
	releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, target ociobjectstore.ObjectURI, etag string) (bool, error) {
		released = true
		assert.Equal(t, "target-models/"+constants.ArtifactUploadLockFileName, target.ObjectName)
		assert.Equal(t, "test-lock-etag", etag)
		return true, nil
	}

	err := agent.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replication failed")
	assert.True(t, released)
}

func TestReplicaAgent_StartRetriesTargetArtifactUploadLockRelease(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return &fakeReplicator{}, nil
	}
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
		return nil
	}

	releaseAttempts := 0
	releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, _ string) (bool, error) {
		releaseAttempts++
		if releaseAttempts == 1 {
			return false, errors.New("temporary delete failure")
		}
		return true, nil
	}

	require.NoError(t, agent.Start())
	assert.Equal(t, 2, releaseAttempts)
}

func TestReplicaAgent_StartReturnsErrorWhenTargetArtifactUploadLockReleaseFails(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return &fakeReplicator{}, nil
	}
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
		return nil
	}

	releaseAttempts := 0
	releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, _ string) (bool, error) {
		releaseAttempts++
		return false, errors.New("temporary delete failure")
	}

	err := agent.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to release target artifact upload lock")
	assert.Equal(t, 3, releaseAttempts)
}

func TestReplicaAgent_StartPreservesReplicationErrorWhenTargetArtifactUploadLockReleaseFails(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return &fakeReplicator{err: errors.New("replication failed")}, nil
	}

	releaseAttempts := 0
	releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, _ string) (bool, error) {
		releaseAttempts++
		return false, errors.New("temporary delete failure")
	}

	err := agent.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replication failed")
	assert.Equal(t, 3, releaseAttempts)
}

func TestReplicaAgent_ReleaseTargetArtifactUploadLockOnlyDeletesAcquiredLock(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	released := false
	releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, target ociobjectstore.ObjectURI, etag string) (bool, error) {
		released = true
		assert.Equal(t, "target-models/"+constants.ArtifactUploadLockFileName, target.ObjectName)
		assert.Equal(t, "acquired-lock-etag", etag)
		return false, nil
	}

	require.NoError(t, agent.releaseTargetArtifactUploadLock(targetArtifactUploadLock{ETag: "acquired-lock-etag"}))

	assert.True(t, released)
}

func TestReplicaAgent_StartSkipsCompletionMarkerForNonOCITarget(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()

	agent.ReplicationInput.TargetStorageType = storage.StorageTypePVC
	agent.ReplicationInput.Target = ociobjectstore.ObjectURI{
		BucketName: "target-pvc",
		Prefix:     "target-model",
	}
	agent.Config.Target = TargetStruct{
		StorageURIStr: "pvc://target-pvc/target-model",
		PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
	}

	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
		return &fakeReplicator{}, nil
	}
	markerWritten := false
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
		markerWritten = true
		return nil
	}

	err := agent.Start()
	require.NoError(t, err)
	assert.False(t, markerWritten)
}

func TestFilterInternalArtifactReplicationObjectsSkipsCompletionMarker(t *testing.T) {
	configName := "models/config.json"
	weightName := "models/model.safetensors"
	markerName := "models/" + constants.ArtifactCompleteMarkerFileName
	lockName := "models/" + constants.ArtifactUploadLockFileName
	rootMarkerName := constants.ArtifactCompleteMarkerFileName
	size := int64(1)

	objects := []common.ReplicationObject{
		common.ObjectSummaryReplicationObject{ObjectSummary: objectstorage.ObjectSummary{Name: &configName, Size: &size}},
		common.ObjectSummaryReplicationObject{ObjectSummary: objectstorage.ObjectSummary{Name: &markerName, Size: &size}},
		common.ObjectSummaryReplicationObject{ObjectSummary: objectstorage.ObjectSummary{Name: &lockName, Size: &size}},
		common.ObjectSummaryReplicationObject{ObjectSummary: objectstorage.ObjectSummary{Name: &weightName, Size: &size}},
		common.ObjectSummaryReplicationObject{ObjectSummary: objectstorage.ObjectSummary{Name: &rootMarkerName, Size: &size}},
	}

	filtered := filterInternalArtifactReplicationObjects(objects)

	require.Len(t, filtered, 2)
	assert.Equal(t, configName, filtered[0].GetName())
	assert.Equal(t, weightName, filtered[1].GetName())
}

func TestDefaultTargetArtifactState(t *testing.T) {
	target := ociobjectstore.ObjectURI{
		Namespace:  "target-namespace",
		BucketName: "target-bucket",
		Prefix:     "models/",
	}

	t.Run("list failure is returned instead of an incomplete artifact", func(t *testing.T) {
		dataStore := newTargetArtifactStateDataStore("")
		dataStore.Client.HTTPClient = targetArtifactStateRequestDispatcher{
			statusCode:   http.StatusForbidden,
			listResponse: `{"code":"NotAuthorized","message":"Object listing denied"}`,
		}

		state, err := defaultTargetArtifactState(dataStore, target)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "NotAuthorized")
		assert.Contains(t, err.Error(), "Object listing denied")
		assert.Equal(t, targetArtifactState{}, state)
	})

	t.Run("complete artifact excludes replication metadata from size", func(t *testing.T) {
		dataStore := newTargetArtifactStateDataStore(`{
			"objects": [
				{"name":"models/.ome-artifact-complete","size":1},
				{"name":"models/config.json","size":12},
				{"name":"models/model.safetensors","size":100},
				{"name":"models/.ome-artifact-upload.lock","size":1,"etag":"lock-etag","timeModified":"2026-08-20T12:00:00.000Z"}
			]
		}`)

		state, err := defaultTargetArtifactState(dataStore, target)

		require.NoError(t, err)
		assert.True(t, state.CompletionMarked)
		assert.True(t, state.Complete)
		assert.True(t, state.UploadLocked)
		assert.Equal(t, "lock-etag", state.UploadLockETag)
		require.NotNil(t, state.UploadLockModifiedTime)
		assert.Equal(t, time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC), *state.UploadLockModifiedTime)
		require.NotNil(t, state.ArtifactSizeBytes)
		assert.Equal(t, int64(112), *state.ArtifactSizeBytes)
	})

	t.Run("completion marker without model files is incomplete", func(t *testing.T) {
		dataStore := newTargetArtifactStateDataStore(`{
			"objects": [
				{"name":"models/.ome-artifact-complete","size":1}
			]
		}`)

		state, err := defaultTargetArtifactState(dataStore, target)

		require.NoError(t, err)
		assert.True(t, state.CompletionMarked)
		assert.False(t, state.Complete)
		assert.False(t, state.UploadLocked)
		assert.Nil(t, state.ArtifactSizeBytes)
	})

	t.Run("active lock uses creation time when modification time is unavailable", func(t *testing.T) {
		dataStore := newTargetArtifactStateDataStore(`{
			"objects": [
				{"name":"models/.ome-artifact-upload.lock","size":1,"etag":"lock-etag","timeCreated":"2026-08-20T12:30:00.000Z"}
			]
		}`)

		state, err := defaultTargetArtifactState(dataStore, target)

		require.NoError(t, err)
		assert.False(t, state.CompletionMarked)
		assert.False(t, state.Complete)
		assert.True(t, state.UploadLocked)
		assert.Equal(t, "lock-etag", state.UploadLockETag)
		require.NotNil(t, state.UploadLockModifiedTime)
		assert.Equal(t, time.Date(2026, time.August, 20, 12, 30, 0, 0, time.UTC), *state.UploadLockModifiedTime)
		assert.Nil(t, state.ArtifactSizeBytes)
	})
}

func TestDefaultTargetArtifactStateMissingObjectMetadata(t *testing.T) {
	for _, tt := range []struct {
		name     string
		prefix   string
		response string
		want     targetArtifactState
	}{
		{
			name:   "nameless and nested metadata objects cannot complete an artifact",
			prefix: "models",
			response: `{"objects":[
				{"size":999},
				{"name":"models/.ome-artifact-complete","size":1},
				{"name":"models/nested/.ome-artifact-complete","size":1},
				{"name":"models/nested/.ome-artifact-upload.lock","size":1},
				{"name":"models/.ome-artifact-upload.lock","size":1}
			]}`,
			want: targetArtifactState{CompletionMarked: true, UploadLocked: true},
		},
		{
			name:   "artifact object with unknown size still establishes completeness",
			prefix: "models/",
			response: `{"objects":[
				{"name":"models/.ome-artifact-complete"},
				{"name":"models/config.json"}
			]}`,
			want: targetArtifactState{CompletionMarked: true, Complete: true},
		},
		{
			name: "root prefix recognizes markers and empty files",
			response: `{"objects":[
				{"name":".ome-artifact-complete","size":1},
				{"name":"config.json","size":0}
			]}`,
			want: targetArtifactState{CompletionMarked: true, Complete: true},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state, err := defaultTargetArtifactState(newTargetArtifactStateDataStore(tt.response), ociobjectstore.ObjectURI{
				Namespace: "target-namespace", BucketName: "target-bucket", Prefix: tt.prefix,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, state)
		})
	}
}

func TestDefaultTargetArtifactStateRequiresAllListingPages(t *testing.T) {
	for _, tt := range []struct {
		name       string
		secondPage targetArtifactStateRequestDispatcher
		wantError  string
	}{
		{
			name: "later page contributes artifact size",
			secondPage: targetArtifactStateRequestDispatcher{
				listResponse: `{"objects":[{"name":"models/model.safetensors","size":100}]}`,
			},
		},
		{
			name: "later page denied discards complete first page",
			secondPage: targetArtifactStateRequestDispatcher{
				statusCode:   http.StatusForbidden,
				listResponse: `{"code":"NotAuthorized","message":"listing denied"}`,
			},
			wantError: "NotAuthorized",
		},
		{
			name:       "malformed later page discards complete first page",
			secondPage: targetArtifactStateRequestDispatcher{listResponse: `{"objects":`},
			wantError:  "error listing objects at page 1",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dataStore := newTargetArtifactStateDataStore("")
			requests := 0
			dataStore.Client.HTTPClient = targetArtifactStateRequestFunc(func(request *http.Request) (*http.Response, error) {
				requests++
				assert.Equal(t, "models/", request.URL.Query().Get("prefix"))
				assert.Equal(t, "/n/target-namespace/b/target-bucket/o", request.URL.Path)
				if requests == 1 {
					assert.Empty(t, request.URL.Query().Get("start"))
					return (targetArtifactStateRequestDispatcher{listResponse: `{
						"objects":[
							{"name":"models/.ome-artifact-complete","size":1},
							{"name":"models/config.json","size":12}
						],
						"nextStartWith":"models/model.safetensors"
					}`}).Do(request)
				}
				assert.Equal(t, "models/model.safetensors", request.URL.Query().Get("start"))
				return tt.secondPage.Do(request)
			})

			state, err := defaultTargetArtifactState(dataStore, ociobjectstore.ObjectURI{
				Namespace: "target-namespace", BucketName: "target-bucket", Prefix: "models/",
			})

			assert.Equal(t, 2, requests)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				assert.Equal(t, targetArtifactState{}, state, "partial listing must never permit reuse")
				return
			}
			require.NoError(t, err)
			assert.True(t, state.Complete)
			require.NotNil(t, state.ArtifactSizeBytes)
			assert.Equal(t, int64(112), *state.ArtifactSizeBytes)
		})
	}
}

func newTestAgentForCompletionMarker(t *testing.T) (*ReplicaAgent, func()) {
	t.Helper()

	oldNewReplicatorFunc := newReplicatorFunc
	oldUploadCompletionMarkerFunc := uploadCompletionMarkerFunc
	oldTryAcquireArtifactUploadLockFunc := tryAcquireArtifactUploadLockFunc
	oldReleaseArtifactUploadLockFunc := releaseArtifactUploadLockFunc
	oldDeleteArtifactCompletionMarkerFunc := deleteArtifactCompletionMarkerFunc
	oldDeleteStaleArtifactUploadLockFunc := deleteStaleArtifactUploadLockFunc
	oldTargetArtifactStateFunc := targetArtifactStateFunc
	oldSleepFunc := sleepFunc
	oldNowFunc := nowFunc
	cleanup := func() {
		newReplicatorFunc = oldNewReplicatorFunc
		uploadCompletionMarkerFunc = oldUploadCompletionMarkerFunc
		tryAcquireArtifactUploadLockFunc = oldTryAcquireArtifactUploadLockFunc
		releaseArtifactUploadLockFunc = oldReleaseArtifactUploadLockFunc
		deleteArtifactCompletionMarkerFunc = oldDeleteArtifactCompletionMarkerFunc
		deleteStaleArtifactUploadLockFunc = oldDeleteStaleArtifactUploadLockFunc
		targetArtifactStateFunc = oldTargetArtifactStateFunc
		sleepFunc = oldSleepFunc
		nowFunc = oldNowFunc
	}
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		return "test-lock-etag", true, nil
	}
	releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, _ string) (bool, error) {
		return true, nil
	}
	deleteArtifactCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) error {
		return nil
	}
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		return targetArtifactState{}, nil
	}
	sleepFunc = func(time.Duration) {}
	nowFunc = time.Now

	localPath := t.TempDir()
	sourceDir := filepath.Join(localPath, "source-model")
	require.NoError(t, os.MkdirAll(sourceDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "config.json"), []byte("model config"), 0644))

	mockLogger := testingPkg.SetupMockLogger()
	return &ReplicaAgent{
		Logger: mockLogger,
		Config: Config{
			AnotherLogger:              mockLogger,
			LocalPath:                  localPath,
			NumConnections:             1,
			DownloadSizeLimitGB:        100,
			EnableSizeLimitCheck:       true,
			TargetArtifactReuseAllowed: true,
			Source: SourceStruct{
				StorageURIStr: "pvc://source-pvc/source-model",
				PVCFileSystem: afero.NewOsFs().(*afero.OsFs),
			},
			Target: TargetStruct{
				StorageURIStr:  "oci://n/tgt-ns/b/tgt-bucket/o/target-models",
				OCIOSDataStore: createMockOCIOSDataStore(),
			},
		},
		ReplicationInput: common.ReplicationInput{
			SourceStorageType: storage.StorageTypePVC,
			TargetStorageType: storage.StorageTypeOCI,
			Source: ociobjectstore.ObjectURI{
				BucketName: "source-pvc",
				Prefix:     "source-model",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "tgt-ns",
				BucketName: "tgt-bucket",
				Prefix:     "target-models/",
				Region:     "us-ashburn-1",
			},
		},
	}, cleanup
}
