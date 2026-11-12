package replicator

import (
	"fmt"
	"os"
	"path/filepath"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/afero"
	"sigs.k8s.io/ome/pkg/logging"
)

type PVCToPVCReplicator struct {
	Logger           logging.Interface
	Dependencies     Dependencies
	ReplicationInput common.ReplicationInput
}

func (r *PVCToPVCReplicator) Replicate(objects []common.ReplicationObject) error {
	r.Logger.Info("Starting replication to target")
	if r.ReplicationInput.Source.Namespace != r.ReplicationInput.Target.Namespace {
		return fmt.Errorf("source PVC and target PVC namespaces do not match: %s != %s",
			r.ReplicationInput.Source.Namespace, r.ReplicationInput.Target.Namespace)
	}

	if r.ReplicationInput.Source.Namespace == r.ReplicationInput.Target.Namespace &&
		r.ReplicationInput.Source.BucketName == r.ReplicationInput.Target.BucketName &&
		r.ReplicationInput.Source.Prefix == r.ReplicationInput.Target.Prefix {
		r.Logger.Info("Source PVC and target PVC are the same, no replication needed")
		return nil
	}

	sourceDirPath := filepath.Join(r.Dependencies.LocalPath, r.ReplicationInput.Source.Prefix)
	targetDirPath := filepath.Join(r.Dependencies.LocalPath, r.ReplicationInput.Target.BucketName, r.ReplicationInput.Target.Prefix)

	err := afero.Walk(r.Dependencies.SourceFiles, sourceDirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing %q: %w", path, err)
		}

		relPath, err := filepath.Rel(sourceDirPath, path)
		if err != nil {
			return fmt.Errorf("error getting relative path: %w", err)
		}

		destPath := filepath.Join(targetDirPath, relPath)

		if info.IsDir() {
			return r.Dependencies.TargetFiles.MkdirAll(destPath, info.Mode())
		}

		return afero.CopyFileBetweenFS(r.Dependencies.SourceFiles, r.Dependencies.TargetFiles, path, destPath, info.Mode())
	})

	if err != nil {
		return fmt.Errorf("replication failed: %w", err)
	}

	r.Logger.Infof("Replication completed successfully for PVC %s under path '%s' to PVC %s under path '%s'",
		r.ReplicationInput.Source.BucketName,
		r.ReplicationInput.Source.Prefix,
		r.ReplicationInput.Target.BucketName,
		r.ReplicationInput.Target.Prefix)
	return nil
}
