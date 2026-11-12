package replicator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/ome/pkg/xet"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

type HFToOCIReplicator struct {
	Logger           logging.Interface
	Dependencies     Dependencies
	ReplicationInput common.ReplicationInput
}

func (r *HFToOCIReplicator) Replicate(objects []common.ReplicationObject) error {
	r.Logger.Info("Starting replication to target")
	// Download
	tempDirPath := filepath.Join(r.Dependencies.LocalPath, ReplicaWorkspacePath)
	downloadPath, err := downloadFromHF(r.ReplicationInput, r.Dependencies, tempDirPath, r.Logger)
	if err != nil {
		r.Logger.Errorf("Failed to download model %s from HuggingFace: %v", r.ReplicationInput.Source.BucketName, err)
		return err
	}
	r.Logger.Infof("Successfully downloaded model %s from HF to %s ", r.ReplicationInput.Source.BucketName, downloadPath)

	// Upload
	if err = uploadDirectory(r.Dependencies, r.ReplicationInput.Target, tempDirPath, len(objects)); err != nil {
		r.Logger.Errorf("Failed to upload files under %s to OCI Object Storage %v: %v", tempDirPath, r.ReplicationInput.Target, err)
		return err
	}
	r.Logger.Infof("All files under %s uploaded successfully", tempDirPath)
	r.Logger.Infof("Replication completed from HuggingFace to OCI Object Storage for model %s", r.ReplicationInput.Source.BucketName)

	// Clean up temporary directory
	if err := os.RemoveAll(tempDirPath); err != nil {
		r.Logger.Warnf("Failed to clean up the temp local directory %s: %v", tempDirPath, err)
	}
	return nil
}

// downloadFromHF downloads the snapshot of the source repository at the source revision into
// downloadDir, under the dependencies' timeouts.
func downloadFromHF(input common.ReplicationInput, dependencies Dependencies, downloadDir string, logger logging.Interface) (string, error) {
	req := &xet.SnapshotRequest{
		RepoID:   input.Source.BucketName,
		RepoType: xet.RepoTypeModel,
		Revision: input.Source.Prefix,
		LocalDir: downloadDir,
	}

	path, err := downloadSnapshotWithTimeouts(dependencies.HubClient, req, hfDownloadOptions{
		DownloadTimeout:      dependencies.DownloadTimeout,
		StaleProgressTimeout: dependencies.StaleProgressTimeout,
	}, logger)
	if err != nil {
		logger.Errorf("Failed to download snapshot: %v", err)
	}
	return path, err
}

type uploadTask struct {
	targetObj ociobjectstore.ObjectURI
	filePath  string
}

// uploadDirectory uploads every file under localDirectoryPath to the target store, under the
// object's prefix and with its checksum as metadata, on as many workers as there are
// connections. numberOfObjects is the source's object count; nothing is uploaded when it is zero.
func uploadDirectory(dependencies Dependencies, object ociobjectstore.ObjectURI, localDirectoryPath string, numberOfObjects int) error {
	ociOSDataStore := dependencies.TargetStore
	if ociOSDataStore == nil {
		return fmt.Errorf("target ociOSDataStore is nil")
	}

	// Early return if no objects to upload
	if numberOfObjects <= 0 {
		ociOSDataStore.Config.AnotherLogger.Infof("No objects to upload (numberOfObjects: %d), skipping upload", numberOfObjects)
		return nil
	}

	tasks := make(chan uploadTask, numberOfObjects)
	errCh := make(chan error, numberOfObjects)
	checksumLimiter := newChecksumLimiter(dependencies.Checksum)
	runWorkers(tasks, dependencies.Connections, func(task uploadTask) {
		task.targetObj.Metadata = getObjectMetadataWithChecksumLimit(
			dependencies.Checksum,
			task.filePath,
			ociOSDataStore.Config.AnotherLogger,
			checksumLimiter,
		)

		if err := dependencies.Transfer.Upload(ociOSDataStore, task.targetObj, task.filePath); err != nil {
			errCh <- fmt.Errorf("upload failed for %s: %w", task.filePath, err)
		}
	}, func() { close(errCh) })

	err := filepath.WalkDir(localDirectoryPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Get object name relative to the root dir
		relPath, err := filepath.Rel(localDirectoryPath, path)
		if err != nil {
			return err
		}

		// Normalize path to use "/" for OCI Object Storage
		relPath = filepath.ToSlash(relPath)
		filePath := filepath.Join(localDirectoryPath, relPath)

		// Create the OCI ObjectURI with target prefix
		objectName := strings.TrimSuffix(object.Prefix, "/") + "/" + relPath
		// Handle the case when directly uploading to root directory in OCI bucket
		if object.Prefix == "" {
			objectName = relPath
		}

		targetObj := ociobjectstore.ObjectURI{
			BucketName: object.BucketName,
			Namespace:  object.Namespace,
			ObjectName: objectName,
		}

		tasks <- uploadTask{targetObj: targetObj, filePath: filePath}
		return nil
	})
	close(tasks) // signal no more work

	// errCh closes once every worker has finished; draining it waits for them.
	var uploadErrors []error
	for uploadErr := range errCh {
		uploadErrors = append(uploadErrors, uploadErr)
	}

	if err != nil {
		ociOSDataStore.Config.AnotherLogger.Errorf("Failed to upload files: %+v", err)
		return err
	}

	if len(uploadErrors) > 0 {
		for _, uploadErr := range uploadErrors {
			ociOSDataStore.Config.AnotherLogger.Errorf("error when uploading a file: %+v", uploadErr)
		}
		return fmt.Errorf("some files failed to upload")
	}
	return nil
}
