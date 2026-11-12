package replicator

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

type OCIToOCIReplicator struct {
	Logger           logging.Interface
	Dependencies     Dependencies
	ReplicationInput common.ReplicationInput

	// checksumLimiter caps how many checksums are computed at the same time; Replicate
	// sizes it from the checksum config before the workers start.
	checksumLimiter chan struct{}
}

func (r *OCIToOCIReplicator) Replicate(objects []common.ReplicationObject) error {
	r.Logger.Info("Starting replication to target")
	r.checksumLimiter = newChecksumLimiter(r.Dependencies.Checksum)
	if err := replicateInParallel(objects, r.Dependencies.Connections, r, r.Logger); err != nil {
		return err
	}

	// Cleanup
	if err := os.RemoveAll(filepath.Join(r.Dependencies.LocalPath, ReplicaWorkspacePath)); err != nil {
		r.Logger.Warnf("Failed to clean up the temp local directory %s: %v", filepath.Join(r.Dependencies.LocalPath, ReplicaWorkspacePath), err)
	}
	return nil
}

// copyObject downloads one object into the workspace and uploads it to the target, with its
// checksum as metadata.
func (r *OCIToOCIReplicator) copyObject(object common.ReplicationObject) ReplicationResult {
	srcObj := ociobjectstore.ObjectURI{
		Namespace:  r.ReplicationInput.Source.Namespace,
		BucketName: r.ReplicationInput.Source.BucketName,
		ObjectName: object.GetName(),
	}
	targetObj := r.getTargetObjectURI(object.GetName())
	result := ReplicationResult{
		source: srcObj,
		target: targetObj,
	}

	// Download
	downloadStart := time.Now()
	tempDirPath := filepath.Join(r.Dependencies.LocalPath, ReplicaWorkspacePath)
	err := r.Dependencies.Transfer.Download(r.Dependencies.SourceStore, srcObj, tempDirPath)
	downloadDuration := time.Since(downloadStart)
	if err != nil {
		result.error = err
		return result
	}
	r.Logger.Infof("Downloaded object %s in %v", srcObj.ObjectName, downloadDuration)

	// Set up checksum as metadata
	uploadedFilePath := filepath.Join(tempDirPath, object.GetName())
	targetObj.Metadata = getObjectMetadataWithChecksumLimit(
		r.Dependencies.Checksum,
		uploadedFilePath,
		r.Logger,
		r.checksumLimiter,
	)

	// Upload
	uploadStart := time.Now()
	err = r.Dependencies.Transfer.Upload(r.Dependencies.TargetStore, targetObj, uploadedFilePath)
	uploadDuration := time.Since(uploadStart)
	if err != nil {
		result.error = err
	} else {
		r.Logger.Infof("Uploaded object to %s in %v", targetObj.ObjectName, uploadDuration)
	}
	return result
}

func (r *OCIToOCIReplicator) logOutcome(result ReplicationResult) {
	if result.error != nil {
		r.Logger.Errorf("Replication failed for %+v to %+v: %v", result.source, result.target, result.error)
		return
	}
	r.Logger.Infof("Replication succeeded for %+v to %+v", result.source, result.target)
}

func (r *OCIToOCIReplicator) getTargetObjectURI(objName string) ociobjectstore.ObjectURI {
	targetObjName := strings.Replace(objName, r.ReplicationInput.Source.Prefix, r.ReplicationInput.Target.Prefix, 1)
	return ociobjectstore.ObjectURI{
		Namespace:  r.ReplicationInput.Target.Namespace,
		BucketName: r.ReplicationInput.Target.BucketName,
		ObjectName: targetObjName,
	}
}
