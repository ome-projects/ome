package replicator

import (
	"path/filepath"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/logging"
)

type HFToPVCReplicator struct {
	Logger           logging.Interface
	Dependencies     Dependencies
	ReplicationInput common.ReplicationInput
}

func (r *HFToPVCReplicator) Replicate(objects []common.ReplicationObject) error {
	r.Logger.Info("Starting replication to target")

	targetDirPath := filepath.Join(r.Dependencies.LocalPath, r.ReplicationInput.Target.Prefix)
	downloadPath, err := downloadFromHF(r.ReplicationInput, r.Dependencies, targetDirPath, r.Logger)
	if err != nil {
		r.Logger.Errorf("Failed to download model %s from HuggingFace: %v", r.ReplicationInput.Source.BucketName, err)
		return err
	}
	r.Logger.Infof("Successfully downloaded model %s from HF to %s ", r.ReplicationInput.Source.BucketName, downloadPath)

	r.Logger.Infof("Replication completed from HuggingFace to PVC %s for model %s", r.ReplicationInput.Target.BucketName, r.ReplicationInput.Source.BucketName)
	return nil
}
