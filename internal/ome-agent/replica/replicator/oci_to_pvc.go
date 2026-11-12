package replicator

import (
	"path/filepath"
	"time"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

type OCIToPVCReplicator struct {
	Logger           logging.Interface
	Dependencies     Dependencies
	ReplicationInput common.ReplicationInput
}

func (r *OCIToPVCReplicator) Replicate(objects []common.ReplicationObject) error {
	r.Logger.Info("Starting replication to target")
	return replicateInParallel(objects, r.Dependencies.Connections, r, r.Logger)
}

// copyObject downloads one object into the target path on the PVC.
func (r *OCIToPVCReplicator) copyObject(object common.ReplicationObject) ReplicationResult {
	srcObj := ociobjectstore.ObjectURI{
		Namespace:  r.ReplicationInput.Source.Namespace,
		BucketName: r.ReplicationInput.Source.BucketName,
		ObjectName: object.GetName(),
	}
	result := ReplicationResult{source: srcObj}

	downloadStart := time.Now()
	err := r.Dependencies.Transfer.Download(r.Dependencies.SourceStore, srcObj, filepath.Join(r.Dependencies.LocalPath, r.ReplicationInput.Target.Prefix))
	downloadDuration := time.Since(downloadStart)
	if err != nil {
		result.error = err
		return result
	}

	r.Dependencies.SourceStore.Config.AnotherLogger.Infof("Downloaded object %s in %v", srcObj.ObjectName, downloadDuration)
	return result
}

func (r *OCIToPVCReplicator) logOutcome(result ReplicationResult) {
	if result.error != nil {
		r.Logger.Errorf("Replication failed for %+v to PVC %s under path '%s': %v", result.source, r.ReplicationInput.Target.BucketName, r.ReplicationInput.Target.Prefix, result.error)
		return
	}
	r.Logger.Infof("Replication succeeded for %+v to PVC %s under path %+v", result.source, r.ReplicationInput.Target.Prefix, result.target)
}
