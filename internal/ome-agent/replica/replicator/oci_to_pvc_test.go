package replicator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

func newOCIToPVCReplicator(t *testing.T, logger logging.Interface, transfer ObjectTransfer) *OCIToPVCReplicator {
	return &OCIToPVCReplicator{
		Logger: logger,
		Dependencies: Dependencies{
			LocalPath:   t.TempDir(),
			Connections: 2,
			SourceStore: storeWithLogger(logger),
			Transfer:    transfer,
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
}

// TestOCIToPVCReplicator_Replicate_Success tests successful replication
func TestOCIToPVCReplicator_Replicate_Success(t *testing.T) {
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("file1", "file1", 123),
		NewCustomMockReplicationObject("file2", "file2", 456),
	}
	mockLogger := testingPkg.SetupMockLogger()
	transfer := &fakeTransfer{}

	err := newOCIToPVCReplicator(t, mockLogger, transfer).Replicate(objects)

	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"file1", "file2"}, transfer.downloaded(), "every object should be downloaded")
	mockLogger.AssertExpectations(t)
}

// TestOCIToPVCReplicator_Replicate_PartialFailure tests replication with some failures
func TestOCIToPVCReplicator_Replicate_PartialFailure(t *testing.T) {
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("file1", "file1", 123),
		NewCustomMockReplicationObject("file2", "file2", 456),
		NewCustomMockReplicationObject("file3", "file3", 789),
	}
	mockLogger := testingPkg.SetupMockLogger()
	transfer := &fakeTransfer{downloadErr: failObject("file2", errors.New("download failed"))}

	err := newOCIToPVCReplicator(t, mockLogger, transfer).Replicate(objects)

	assert.EqualError(t, err, "1/3 replications failed")
	assert.ElementsMatch(t, []string{"file1", "file2", "file3"}, transfer.downloaded(), "every object should be attempted")
	mockLogger.AssertExpectations(t)
}

// TestOCIToPVCReplicator_Replicate_AllFailures tests replication with all failures
func TestOCIToPVCReplicator_Replicate_AllFailures(t *testing.T) {
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("file1", "file1", 123),
		NewCustomMockReplicationObject("file2", "file2", 456),
	}
	mockLogger := testingPkg.SetupMockLogger()
	transfer := &fakeTransfer{downloadErr: failEvery(errors.New("download failed"))}

	err := newOCIToPVCReplicator(t, mockLogger, transfer).Replicate(objects)

	assert.EqualError(t, err, "2/2 replications failed")
	assert.Len(t, transfer.downloaded(), 2, "every object should be attempted")
	mockLogger.AssertExpectations(t)
}

// TestOCIToPVCReplicator_Replicate_SkipPrefixObject tests that objects with prefix name are skipped
func TestOCIToPVCReplicator_Replicate_SkipPrefixObject(t *testing.T) {
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("models/", "models/", 123), // This should be skipped
		NewCustomMockReplicationObject("file1", "file1", 456),
		NewCustomMockReplicationObject("file2", "file2", 789),
	}
	mockLogger := testingPkg.SetupMockLogger()
	transfer := &fakeTransfer{}

	err := newOCIToPVCReplicator(t, mockLogger, transfer).Replicate(objects)

	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"file1", "file2"}, transfer.downloaded(), "file1 and file2 should be processed and models/ skipped")
	mockLogger.AssertExpectations(t)
}
