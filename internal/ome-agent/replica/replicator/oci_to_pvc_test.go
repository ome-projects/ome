package replicator

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// TestOCIToPVCReplicator_Replicate_Success tests successful replication
func TestOCIToPVCReplicator_Replicate_Success(t *testing.T) {
	// Save original function
	origDownloadFunc := downloadObjectsFromOCIOSDataStoreFunc
	defer func() {
		downloadObjectsFromOCIOSDataStoreFunc = origDownloadFunc
	}()

	// Create mock objects
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("file1", "file1", 123),
		NewCustomMockReplicationObject("file2", "file2", 456),
	}

	// Create mock logger
	mockLogger := testingPkg.SetupMockLogger()

	// Create replicator
	replicator := &OCIToPVCReplicator{
		Logger: mockLogger,
		Config: OCIToPVCReplicatorConfig{
			LocalPath:      "/tmp/test",
			NumConnections: 2,
			OCIOSDataStore: &ociobjectstore.OCIOSDataStore{
				Config: &ociobjectstore.Config{
					AnotherLogger: mockLogger,
				},
			},
		},
		ReplicationInput: common.ReplicationInput{
			SourceStorageType: storage.StorageTypeOCI,
			TargetStorageType: storage.StorageTypePVC,
			Source: ociobjectstore.ObjectURI{
				Namespace:  "test-ns",
				BucketName: "source-bucket",
				Prefix:     "models/",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "amaaaaaax7756raaolxvbyk7toite23tbfkarxhiipv6jdy3tgwjjq4l6zma",
				BucketName: "pvc-name",
				Prefix:     "pvc-path",
			},
		},
	}

	// Mock the download function to simulate successful downloads
	var downloadCalled atomic.Bool
	downloadObjectsFromOCIOSDataStoreFunc = func(
		objects <-chan common.ReplicationObject,
		ociOSDataStore *ociobjectstore.OCIOSDataStore,
		replicationInput common.ReplicationInput,
		localDirectoryPath string,
		results chan<- *ReplicationResult) {
		downloadCalled.Store(true)

		// Simulate successful downloads
		for obj := range objects {
			if strings.HasSuffix(obj.GetName(), "/") {
				continue
			}

			srcObj := ociobjectstore.ObjectURI{
				Namespace:  replicationInput.Source.Namespace,
				BucketName: replicationInput.Source.BucketName,
				ObjectName: obj.GetName(),
			}
			result := &ReplicationResult{
				source: srcObj,
				target: ociobjectstore.ObjectURI{ObjectName: "target-" + obj.GetName()},
				error:  nil,
			}
			results <- result
		}
	}

	// Execute replication
	err := replicator.Replicate(objects)

	// Assertions
	assert.NoError(t, err)
	assert.True(t, downloadCalled.Load(), "downloadObjectsFromOCIOSDataStoreFunc should be called")
	mockLogger.AssertExpectations(t)
}

// TestOCIToPVCReplicator_Replicate_PartialFailure tests replication with some failures
func TestOCIToPVCReplicator_Replicate_PartialFailure(t *testing.T) {
	// Save original function
	origDownloadFunc := downloadObjectsFromOCIOSDataStoreFunc
	defer func() {
		downloadObjectsFromOCIOSDataStoreFunc = origDownloadFunc
	}()

	// Create mock objects
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("file1", "file1", 123),
		NewCustomMockReplicationObject("file2", "file2", 456),
		NewCustomMockReplicationObject("file3", "file3", 789),
	}

	// Create mock logger
	mockLogger := testingPkg.SetupMockLogger()

	// Create replicator
	replicator := &OCIToPVCReplicator{
		Logger: mockLogger,
		Config: OCIToPVCReplicatorConfig{
			LocalPath:      "/tmp/test",
			NumConnections: 2,
			OCIOSDataStore: &ociobjectstore.OCIOSDataStore{
				Config: &ociobjectstore.Config{
					AnotherLogger: mockLogger,
				},
			},
		},
		ReplicationInput: common.ReplicationInput{
			SourceStorageType: storage.StorageTypeOCI,
			TargetStorageType: storage.StorageTypePVC,
			Source: ociobjectstore.ObjectURI{
				Namespace:  "test-ns",
				BucketName: "source-bucket",
				Prefix:     "models/",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "amaaaaaax7756raaolxvbyk7toite23tbfkarxhiipv6jdy3tgwjjq4l6zma",
				BucketName: "pvc-name",
				Prefix:     "pvc-path",
			},
		},
	}

	// Mock the download function to simulate mixed results
	var downloadCalled atomic.Bool
	downloadObjectsFromOCIOSDataStoreFunc = func(
		objects <-chan common.ReplicationObject,
		ociOSDataStore *ociobjectstore.OCIOSDataStore,
		replicationInput common.ReplicationInput,
		localDirectoryPath string,
		results chan<- *ReplicationResult) {
		downloadCalled.Store(true)

		objectCount := 0
		for obj := range objects {
			if strings.HasSuffix(obj.GetName(), "/") {
				continue
			}

			srcObj := ociobjectstore.ObjectURI{
				Namespace:  replicationInput.Source.Namespace,
				BucketName: replicationInput.Source.BucketName,
				ObjectName: obj.GetName(),
			}

			// Simulate failure for the second object
			var resultErr error
			if objectCount == 1 {
				resultErr = errors.New("download failed")
			}

			result := &ReplicationResult{
				source: srcObj,
				target: ociobjectstore.ObjectURI{ObjectName: "target-" + obj.GetName()},
				error:  resultErr,
			}
			results <- result
			objectCount++
		}
	}

	// Execute replication
	err := replicator.Replicate(objects)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "1/3 replications failed")
	assert.True(t, downloadCalled.Load(), "downloadObjectsFromOCIOSDataStoreFunc should be called")
	mockLogger.AssertExpectations(t)
}

// TestOCIToPVCReplicator_Replicate_AllFailures tests replication with all failures
func TestOCIToPVCReplicator_Replicate_AllFailures(t *testing.T) {
	// Save original function
	origDownloadFunc := downloadObjectsFromOCIOSDataStoreFunc
	defer func() {
		downloadObjectsFromOCIOSDataStoreFunc = origDownloadFunc
	}()

	// Create mock objects
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("file1", "file1", 123),
		NewCustomMockReplicationObject("file2", "file2", 456),
	}

	// Create mock logger
	mockLogger := testingPkg.SetupMockLogger()

	// Create replicator
	replicator := &OCIToPVCReplicator{
		Logger: mockLogger,
		Config: OCIToPVCReplicatorConfig{
			LocalPath:      "/tmp/test",
			NumConnections: 2,
			OCIOSDataStore: &ociobjectstore.OCIOSDataStore{
				Config: &ociobjectstore.Config{
					AnotherLogger: mockLogger,
				},
			},
		},
		ReplicationInput: common.ReplicationInput{
			SourceStorageType: storage.StorageTypeOCI,
			TargetStorageType: storage.StorageTypePVC,
			Source: ociobjectstore.ObjectURI{
				Namespace:  "test-ns",
				BucketName: "source-bucket",
				Prefix:     "models/",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "amaaaaaax7756raaolxvbyk7toite23tbfkarxhiipv6jdy3tgwjjq4l6zma",
				BucketName: "pvc-name",
				Prefix:     "pvc-path",
			},
		},
	}

	// Mock the download function to simulate all failures
	var downloadCalled atomic.Bool
	downloadObjectsFromOCIOSDataStoreFunc = func(
		objects <-chan common.ReplicationObject,
		ociOSDataStore *ociobjectstore.OCIOSDataStore,
		replicationInput common.ReplicationInput,
		localDirectoryPath string,
		results chan<- *ReplicationResult) {
		downloadCalled.Store(true)

		for obj := range objects {
			if strings.HasSuffix(obj.GetName(), "/") {
				continue
			}

			srcObj := ociobjectstore.ObjectURI{
				Namespace:  replicationInput.Source.Namespace,
				BucketName: replicationInput.Source.BucketName,
				ObjectName: obj.GetName(),
			}

			result := &ReplicationResult{
				source: srcObj,
				target: ociobjectstore.ObjectURI{ObjectName: "target-" + obj.GetName()},
				error:  errors.New("download failed"),
			}
			results <- result
		}
	}

	// Execute replication
	err := replicator.Replicate(objects)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "2/2 replications failed")
	assert.True(t, downloadCalled.Load(), "downloadObjectsFromOCIOSDataStoreFunc should be called")
	mockLogger.AssertExpectations(t)
}

// TestOCIToPVCReplicator_Replicate_SkipPrefixObject tests that objects with prefix name are skipped
func TestOCIToPVCReplicator_Replicate_SkipPrefixObject(t *testing.T) {
	// Save original function
	origDownloadFunc := downloadObjectsFromOCIOSDataStoreFunc
	defer func() {
		downloadObjectsFromOCIOSDataStoreFunc = origDownloadFunc
	}()

	// Create mock objects including one with prefix name
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("models/", "models/", 123), // This should be skipped
		NewCustomMockReplicationObject("file1", "file1", 456),
		NewCustomMockReplicationObject("file2", "file2", 789),
	}

	// Create mock logger
	mockLogger := testingPkg.SetupMockLogger()

	// Create replicator
	replicator := &OCIToPVCReplicator{
		Logger: mockLogger,
		Config: OCIToPVCReplicatorConfig{
			LocalPath:      "/tmp/test",
			NumConnections: 2,
			OCIOSDataStore: &ociobjectstore.OCIOSDataStore{
				Config: &ociobjectstore.Config{
					AnotherLogger: mockLogger,
				},
			},
		},
		ReplicationInput: common.ReplicationInput{
			SourceStorageType: storage.StorageTypeOCI,
			TargetStorageType: storage.StorageTypePVC,
			Source: ociobjectstore.ObjectURI{
				Namespace:  "test-ns",
				BucketName: "source-bucket",
				Prefix:     "models/",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "amaaaaaax7756raaolxvbyk7toite23tbfkarxhiipv6jdy3tgwjjq4l6zma",
				BucketName: "pvc-name",
				Prefix:     "pvc-path",
			},
		},
	}

	// Track processed objects with proper synchronization
	var mu sync.Mutex
	processedObjects := make(map[string]bool)

	// Mock the download function
	downloadObjectsFromOCIOSDataStoreFunc = func(
		objects <-chan common.ReplicationObject,
		ociOSDataStore *ociobjectstore.OCIOSDataStore,
		replicationInput common.ReplicationInput,
		localDirectoryPath string,
		results chan<- *ReplicationResult) {

		for obj := range objects {
			if strings.HasSuffix(obj.GetName(), "/") {
				continue // Skip directories
			}

			// Thread-safe access to processedObjects
			mu.Lock()
			processedObjects[obj.GetName()] = true
			mu.Unlock()

			srcObj := ociobjectstore.ObjectURI{
				Namespace:  replicationInput.Source.Namespace,
				BucketName: replicationInput.Source.BucketName,
				ObjectName: obj.GetName(),
			}

			result := &ReplicationResult{
				source: srcObj,
				target: ociobjectstore.ObjectURI{ObjectName: "target-" + obj.GetName()},
				error:  nil,
			}
			results <- result
		}
	}

	// Execute replication
	err := replicator.Replicate(objects)

	// Assertions
	assert.NoError(t, err)
	assert.True(t, processedObjects["file1"], "file1 should be processed")
	assert.True(t, processedObjects["file2"], "file2 should be processed")
	assert.False(t, processedObjects["models/"], "models/ should be skipped")
	mockLogger.AssertExpectations(t)
}
