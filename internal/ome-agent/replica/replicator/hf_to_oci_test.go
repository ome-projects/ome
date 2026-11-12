package replicator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/xet"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

func newHFToOCIReplicator(localPath string, hub *fakeHubClient, transfer *fakeTransfer) *HFToOCIReplicator {
	logger := testingPkg.SetupMockLogger()
	return &HFToOCIReplicator{
		Logger: logger,
		Dependencies: Dependencies{
			LocalPath:   localPath,
			Connections: 1,
			HubClient:   hub,
			TargetStore: storeWithLogger(logger),
			Transfer:    transfer,
		},
		ReplicationInput: common.ReplicationInput{
			SourceStorageType: storage.StorageTypeHuggingFace,
			TargetStorageType: storage.StorageTypeOCI,
			Source:            ociobjectstore.ObjectURI{BucketName: "meta-llama/llama-3-70b-instruct"},
			Target:            ociobjectstore.ObjectURI{BucketName: "target-bucket", Namespace: "target-bucket-ns", Prefix: "target-prefix/"},
		},
	}
}

func TestHFToOCIReplicator_Replicate_Success(t *testing.T) {
	localPath := t.TempDir()
	workspace := filepath.Join(localPath, ReplicaWorkspacePath)
	writeFiles(t, workspace, "config.json")
	hub := &fakeHubClient{download: func(context.Context, *xet.SnapshotRequest) (string, error) {
		return workspace, nil
	}}
	transfer := &fakeTransfer{}

	err := newHFToOCIReplicator(localPath, hub, transfer).Replicate(CreateCommonMockReplicationObjects(1))

	assert.NoError(t, err)
	assert.Equal(t, 1, hub.downloads, "the snapshot should be downloaded")
	assert.Equal(t, []string{"target-prefix/config.json"}, transfer.uploaded(), "the downloaded file should be uploaded under the target prefix")
	_, err = os.Stat(workspace)
	assert.True(t, os.IsNotExist(err), "the workspace should be removed after the upload")
}

func TestHFToOCIReplicator_Replicate_Failure(t *testing.T) {
	localPath := t.TempDir()
	workspace := filepath.Join(localPath, ReplicaWorkspacePath)
	writeFiles(t, workspace, "config.json")
	hub := &fakeHubClient{}
	transfer := &fakeTransfer{}
	replicator := newHFToOCIReplicator(localPath, hub, transfer)
	objs := CreateCommonMockReplicationObjects(1)

	// Test download error
	hub.download = func(context.Context, *xet.SnapshotRequest) (string, error) {
		return "", errors.New("download error")
	}
	err := replicator.Replicate(objs)
	assert.Error(t, err)
	assert.Empty(t, transfer.uploaded(), "nothing should be uploaded if the download fails")
	assert.ErrorContains(t, err, "download error")

	// Test upload error
	hub.download = func(context.Context, *xet.SnapshotRequest) (string, error) {
		return workspace, nil
	}
	transfer.uploadErr = failEvery(errors.New("upload error"))
	err = replicator.Replicate(objs)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "some files failed to upload")
	assert.Equal(t, []string{"target-prefix/config.json"}, transfer.uploaded(), "the upload should be attempted")
}

func TestEffectiveChecksumConcurrency(t *testing.T) {
	assert.Equal(t, 1, effectiveChecksumConcurrency(nil))
	assert.Equal(t, 1, effectiveChecksumConcurrency(&common.ChecksumConfig{}))
	assert.Equal(t, 1, effectiveChecksumConcurrency(&common.ChecksumConfig{Concurrency: -1}))
	assert.Equal(t, 3, effectiveChecksumConcurrency(&common.ChecksumConfig{Concurrency: 3}))
}

func TestUploadDirectoryLimitsChecksumConcurrency(t *testing.T) {
	originalChecksum := getObjectMetadataWithChecksumFunc
	t.Cleanup(func() {
		getObjectMetadataWithChecksumFunc = originalChecksum
	})

	directory := t.TempDir()
	for i := 0; i < 4; i++ {
		err := os.WriteFile(filepath.Join(directory, fmt.Sprintf("file-%d", i)), []byte("test"), 0600)
		assert.NoError(t, err)
	}

	var active atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseChecksums := func() {
		releaseOnce.Do(func() { close(release) })
	}
	t.Cleanup(releaseChecksums)

	getObjectMetadataWithChecksumFunc = func(_ *common.ChecksumConfig, _ string, _ logging.Interface) map[string]string {
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return map[string]string{OCIObjectMD5MetadataKey: "checksum"}
	}
	transfer := &fakeTransfer{uploadErr: func(object ociobjectstore.ObjectURI) error {
		if object.Metadata[OCIObjectMD5MetadataKey] != "checksum" {
			return errors.New("checksum metadata missing")
		}
		return nil
	}}

	logger := testingPkg.SetupMockLogger()
	result := make(chan error, 1)
	go func() {
		result <- uploadDirectory(
			Dependencies{
				Connections: 4,
				Checksum:    &common.ChecksumConfig{UploadEnabled: true, ChecksumAlgorithm: MD5ChecksumAlgorithm, Concurrency: 2},
				TargetStore: storeWithLogger(logger),
				Transfer:    transfer,
			},
			ociobjectstore.ObjectURI{BucketName: "bucket", Namespace: "namespace"},
			directory,
			4,
		)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for checksum worker")
		}
	}
	select {
	case <-started:
		t.Fatal("checksum concurrency exceeded configured limit")
	case <-time.After(50 * time.Millisecond):
	}

	releaseChecksums()
	assert.NoError(t, <-result)
	assert.Equal(t, int32(2), maximum.Load())
	assert.Len(t, transfer.uploaded(), 4)
}
