package replicator

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

func TestReplicate(t *testing.T) {
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("src-prefix/file1", "src-prefix/file1", 123),
		NewCustomMockReplicationObject("src-prefix/file2", "src-prefix/file2", 456),
		NewCustomMockReplicationObject("src-prefix/", "src-prefix/", 0),
	}
	newReplicator := func(transfer *fakeTransfer) *OCIToOCIReplicator {
		localPath := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(localPath, ReplicaWorkspacePath), 0o755))
		return &OCIToOCIReplicator{
			Logger: testingPkg.SetupMockLogger(),
			Dependencies: Dependencies{
				LocalPath:   localPath,
				Connections: 2,
				Transfer:    transfer,
			},
			ReplicationInput: common.ReplicationInput{
				SourceStorageType: storage.StorageTypeOCI,
				TargetStorageType: storage.StorageTypeOCI,
				Source: ociobjectstore.ObjectURI{
					Namespace:  "src-ns",
					BucketName: "src-bucket",
					Prefix:     "src-prefix/",
				},
				Target: ociobjectstore.ObjectURI{
					Namespace:  "tgt-ns",
					BucketName: "tgt-bucket",
					Prefix:     "tgt-prefix/",
				},
			},
		}
	}

	// Every file is downloaded and uploaded under the target prefix, the directory entry is
	// skipped, and the workspace is removed afterwards.
	transfer := &fakeTransfer{}
	replicator := newReplicator(transfer)
	assert.NoError(t, replicator.Replicate(objects))
	assert.ElementsMatch(t, []string{"src-prefix/file1", "src-prefix/file2"}, transfer.downloaded())
	assert.ElementsMatch(t, []string{"tgt-prefix/file1", "tgt-prefix/file2"}, transfer.uploaded())
	_, err := os.Stat(filepath.Join(replicator.Dependencies.LocalPath, ReplicaWorkspacePath))
	assert.True(t, os.IsNotExist(err), "the workspace should be removed after a successful run")

	// A file whose download fails is not uploaded, and the run fails with the count over every
	// listed object.
	transfer = &fakeTransfer{downloadErr: failObject("src-prefix/file2", errors.New("download failed"))}
	err = newReplicator(transfer).Replicate(objects)
	assert.EqualError(t, err, "1/3 replications failed")
	assert.ElementsMatch(t, []string{"tgt-prefix/file1"}, transfer.uploaded())
}

func TestOCIToOCIReplicatorLimitsChecksumConcurrency(t *testing.T) {
	originalChecksum := getObjectMetadataWithChecksumFunc
	t.Cleanup(func() {
		getObjectMetadataWithChecksumFunc = originalChecksum
	})

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseChecksums := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseChecksums)

	var checksumCount atomic.Int32
	getObjectMetadataWithChecksumFunc = func(_ *common.ChecksumConfig, _ string, _ logging.Interface) map[string]string {
		checksumCount.Add(1)
		started <- struct{}{}
		<-release
		return map[string]string{OCIObjectMD5MetadataKey: "checksum"}
	}
	transfer := &fakeTransfer{uploadErr: func(object ociobjectstore.ObjectURI) error {
		if object.Metadata[OCIObjectMD5MetadataKey] != "checksum" {
			return errors.New("checksum metadata missing")
		}
		return nil
	}}

	replicator := &OCIToOCIReplicator{
		Logger: testingPkg.SetupMockLogger(),
		Dependencies: Dependencies{
			LocalPath:   t.TempDir(),
			Connections: 2,
			Checksum:    &common.ChecksumConfig{UploadEnabled: true, ChecksumAlgorithm: MD5ChecksumAlgorithm, Concurrency: 1},
			SourceStore: &ociobjectstore.OCIOSDataStore{},
			TargetStore: &ociobjectstore.OCIOSDataStore{},
			Transfer:    transfer,
		},
		ReplicationInput: common.ReplicationInput{
			Source: ociobjectstore.ObjectURI{Namespace: "source-namespace", BucketName: "source-bucket"},
			Target: ociobjectstore.ObjectURI{Namespace: "target-namespace", BucketName: "target-bucket"},
		},
	}
	objects := []common.ReplicationObject{
		NewCustomMockReplicationObject("file-1", "file-1", 4),
		NewCustomMockReplicationObject("file-2", "file-2", 4),
	}

	result := make(chan error, 1)
	go func() { result <- replicator.Replicate(objects) }()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for checksum worker")
	}
	select {
	case <-started:
		t.Fatal("checksum concurrency exceeded configured limit")
	case <-time.After(50 * time.Millisecond):
	}

	releaseChecksums()
	assert.NoError(t, <-result)
	assert.Equal(t, int32(2), checksumCount.Load())
}

func TestGetTargetObjectURI(t *testing.T) {
	tests := []struct {
		name             string
		replicationInput common.ReplicationInput
		objName          string
		expectedURI      ociobjectstore.ObjectURI
	}{
		{
			name: "replace source prefix with target prefix",
			replicationInput: common.ReplicationInput{
				SourceStorageType: storage.StorageTypeOCI,
				TargetStorageType: storage.StorageTypeOCI,
				Source: ociobjectstore.ObjectURI{
					Namespace:  "src-ns",
					BucketName: "src-bucket",
					Prefix:     "src-prefix/",
				},
				Target: ociobjectstore.ObjectURI{
					Namespace:  "tgt-ns",
					BucketName: "tgt-bucket",
					Prefix:     "tgt-prefix/",
				},
			},
			objName: "src-prefix/model.bin",
			expectedURI: ociobjectstore.ObjectURI{
				Namespace:  "tgt-ns",
				BucketName: "tgt-bucket",
				ObjectName: "tgt-prefix/model.bin",
			},
		},
		{
			name: "source and target with same prefix",
			replicationInput: common.ReplicationInput{
				Source: ociobjectstore.ObjectURI{
					Namespace:  "src-ns",
					BucketName: "src-bucket",
					Prefix:     "models/",
				},
				Target: ociobjectstore.ObjectURI{
					Namespace:  "tgt-ns",
					BucketName: "tgt-bucket",
					Prefix:     "models/",
				},
			},
			objName: "models/model.bin",
			expectedURI: ociobjectstore.ObjectURI{
				Namespace:  "tgt-ns",
				BucketName: "tgt-bucket",
				ObjectName: "models/model.bin",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicator := &OCIToOCIReplicator{
				ReplicationInput: tt.replicationInput,
			}

			result := replicator.getTargetObjectURI(tt.objName)

			assert.Equal(t, tt.expectedURI.Namespace, result.Namespace)
			assert.Equal(t, tt.expectedURI.BucketName, result.BucketName)
			assert.Equal(t, tt.expectedURI.ObjectName, result.ObjectName)
		})
	}
}
