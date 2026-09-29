package modelagent

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/xet"
)

// The source and dispatcher retain the compatible destination lock through
// byte validation, metadata, and request publication. Healthy reuse is read-only.
func (source directHfSource) restoreDirect(ctx context.Context, s *Gopher, task *GopherTask, config *xet.DownloadConfig, manifest hfSnapshotManifest) error {
	if !isDirectHfRestoreEligible(task, taskModelSpec(task).Storage) {
		return fmt.Errorf("Direct restoration requires an eligible HF source")
	}
	path, err := s.directEvictionPath(config.LocalDir)
	if err != nil {
		return err
	}
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), types.UID(getModelUID(task))
	checkEntry := func(_ *corev1.ConfigMap, entry ModelEntry) error {
		return validateDirectRestoreEntry(uid, entry)
	}
	if err := s.prepareDirectRestore(ctx, task, path, false); err != nil {
		return err
	}
	valid, err := manifest.validate(ctx, path)
	if err != nil {
		return err
	}
	if !valid {
		if err := s.prepareDirectRestore(ctx, task, path, true); err != nil {
			return err
		}
		if err := manifest.removeInvalidFiles(ctx, path); err != nil {
			return err
		}
		if err := validateArtifactRestore(ctx); err != nil {
			return err
		}
		if err := source.download(ctx, task, config); err != nil {
			return err
		}
		valid, err = manifest.validate(ctx, path)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("downloaded Direct HF snapshot failed content validation")
		}
	}
	guard := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard)
	guard.validated = true
	if err := validateArtifactRestore(ctx); err != nil {
		return err
	}
	artifact, err := s.readDirectHfLegacyArtifact(ctx, task)
	if err != nil {
		return err
	}
	artifact.Sha, artifact.ParentPath = config.Revision, map[string]string{key: path}
	if err := s.parseDirectHfConfig(ctx, task, path, &artifact); err != nil {
		return err
	}
	return reportArtifactRestore(ctx, key, uid, checkEntry)
}
