package replicator

import (
	"path/filepath"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/logging"
)

type PVCToOCIReplicator struct {
	Logger           logging.Interface
	Dependencies     Dependencies
	ReplicationInput common.ReplicationInput
}

func (r *PVCToOCIReplicator) Replicate(objects []common.ReplicationObject) error {
	r.Logger.Info("Starting replication to target")

	sourceDirPath := filepath.Join(r.Dependencies.LocalPath, r.ReplicationInput.Source.Prefix)
	if err := uploadDirectory(r.Dependencies, r.ReplicationInput.Target, sourceDirPath, len(objects)); err != nil {
		r.Logger.Errorf("Failed to upload files under %s to OCI Object Storage %v: %v", sourceDirPath, r.ReplicationInput.Target, err)
		return err
	}
	r.Logger.Infof("All files under %s uploaded successfully", sourceDirPath)
	r.Logger.Infof("Replication completed from PVC %s to OCI Object Storage", r.ReplicationInput.Source.BucketName)
	return nil
}
