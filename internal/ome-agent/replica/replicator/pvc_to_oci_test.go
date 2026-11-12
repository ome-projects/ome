package replicator

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

func newPVCToOCIReplicator(localPath string, connections int, logger logging.Interface, targetStore *ociobjectstore.OCIOSDataStore, transfer *fakeTransfer) *PVCToOCIReplicator {
	return &PVCToOCIReplicator{
		Logger: logger,
		Dependencies: Dependencies{
			LocalPath:   localPath,
			Connections: connections,
			TargetStore: targetStore,
			Transfer:    transfer,
		},
		ReplicationInput: common.ReplicationInput{
			SourceStorageType: storage.StorageTypePVC,
			TargetStorageType: storage.StorageTypeOCI,
			Source: ociobjectstore.ObjectURI{
				Namespace:  "default",
				BucketName: "model-pvc",
				Prefix:     "models",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "target-namespace",
				BucketName: "model-storage",
				Prefix:     "models",
			},
		},
	}
}

func TestPVCToOCIReplicator_Replicate_Success(t *testing.T) {
	localPath := t.TempDir()
	writeFiles(t, filepath.Join(localPath, "models"), "a.bin", "b.bin")
	logger := testingPkg.SetupMockLogger()
	transfer := &fakeTransfer{}

	err := newPVCToOCIReplicator(localPath, 5, logger, storeWithLogger(logger), transfer).Replicate(CreateCommonMockReplicationObjects(2))

	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"models/a.bin", "models/b.bin"}, transfer.uploaded(), "every file under the source path should be uploaded under the target prefix")
}

func TestPVCToOCIReplicator_Replicate_Failure(t *testing.T) {
	localPath := t.TempDir()
	writeFiles(t, filepath.Join(localPath, "models"), "a.bin")
	logger := testingPkg.SetupMockLogger()
	transfer := &fakeTransfer{uploadErr: failEvery(fmt.Errorf("mock upload error"))}

	objects := []common.ReplicationObject{NewMockReplicationObject()}
	err := newPVCToOCIReplicator(localPath, 3, logger, storeWithLogger(logger), transfer).Replicate(objects)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "some files failed to upload")
	assert.Equal(t, []string{"models/a.bin"}, transfer.uploaded(), "the upload should be attempted")
}

func TestPVCToOCIReplicator_Replicate_WithNilOCIOSDataStore(t *testing.T) {
	transfer := &fakeTransfer{}

	objects := []common.ReplicationObject{NewMockReplicationObject()}
	err := newPVCToOCIReplicator(t.TempDir(), 5, testingPkg.SetupMockLogger(), nil, transfer).Replicate(objects)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "target ociOSDataStore is nil")
	assert.Empty(t, transfer.uploaded(), "nothing should be uploaded without a target store")
}
