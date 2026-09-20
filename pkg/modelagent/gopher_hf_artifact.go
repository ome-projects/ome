package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils"
)

var errHuggingFaceArtifactParentUpdating = errors.New("hugging face artifact parent is updating")

func (s *Gopher) buildArtifactAttributeFromIdentity(identity ArtifactIdentity, matchedParentName string, parentPath string, childrenPaths []string) *Artifact {
	return &Artifact{
		Sha:           identity.HFCommitSHA,
		Origin:        identity.toOrigin(),
		ParentPath:    map[string]string{matchedParentName: parentPath},
		ChildrenPaths: childrenPaths,
	}
}

func (s *Gopher) buildSelfParentArtifactFromIdentity(ctx context.Context, task *GopherTask, identity ArtifactIdentity, destPath string) *Artifact {
	currentModelKey := s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel)
	childrenPaths, _, _, _ := s.parseModelConfigDataEntry(ctx, currentModelKey)
	return s.buildArtifactAttributeFromIdentity(identity, currentModelKey, destPath, childrenPaths)
}

func (s *Gopher) reuseHuggingFaceOriginArtifactIfPossible(ctx context.Context, task *GopherTask, baseModelSpec v1beta1.BaseModelSpec,
	modelType string, namespace string, name string, destPath string, identity ArtifactIdentity) (*Artifact, bool, error) {
	if !shouldUseHuggingFaceOriginObjectStorageReuse(task, baseModelSpec) {
		return nil, false, nil
	}
	if !identity.isValid() {
		return nil, false, nil
	}

	currentModelKey := s.configMapReconciler.getModelConfigMapKey(task.BaseModel, task.ClusterBaseModel)
	matchedModelKey, parentPath := s.handleHuggingFaceOriginReuseIfNecessary(ctx, modelType, name, namespace, identity, currentModelKey)
	if matchedModelKey == "" || parentPath == "" {
		return nil, false, nil
	}
	if _, err := os.Stat(parentPath); err != nil {
		s.logger.Warnf("Cannot reuse Hugging Face origin artifact for %s from %s because parent path %s is not available locally: %v", name, matchedModelKey, parentPath, err)
		return nil, false, nil
	}

	artifact, err := s.linkHuggingFaceOriginArtifact(ctx, task, name, destPath, matchedModelKey, parentPath, identity)
	if err != nil {
		if errors.Is(err, errHuggingFaceArtifactParentUpdating) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return artifact, true, nil
}

func (s *Gopher) repairHuggingFaceOriginArtifactParent(ctx context.Context, task *GopherTask, name string, destPath string,
	parentKey string, parentPath string, identity ArtifactIdentity, downloadObjectStorageModel func(string) error) (*Artifact, bool, error) {
	if !identity.isValid() || strings.TrimSpace(parentKey) == "" || strings.TrimSpace(parentPath) == "" {
		return nil, false, nil
	}
	if wait, waitErr := s.requeueIfHuggingFaceArtifactParentUpdating(ctx, task, identity); wait || waitErr != nil {
		return nil, false, waitErr
	}

	repairPath := parentPath
	if _, observedParentPath, _, ok := s.getHuggingFaceArtifactParent(ctx, identity); ok {
		repairPath = observedParentPath
	}
	repairPath, acquired, err := s.configMapReconciler.markHuggingFaceArtifactParentUpdating(ctx, parentKey, identity, repairPath)
	if err != nil {
		return nil, false, err
	}
	if !acquired {
		if err := s.waitForHuggingFaceArtifactParent(task, parentKey); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if markerErr := removeHuggingFaceArtifactReadyMarker(repairPath); markerErr != nil {
		if failErr := s.configMapReconciler.markHuggingFaceArtifactParentFailed(ctx, parentKey, identity, repairPath); failErr != nil {
			s.logger.Warnf("failed to mark Hugging Face artifact parent %s at %s as Failed after ready marker removal error: %v", parentKey, repairPath, failErr)
		}
		return nil, false, fmt.Errorf("failed to remove Hugging Face artifact ready marker for parent %s at %s before repair: %w", parentKey, repairPath, markerErr)
	}
	s.logger.Infof("Repairing Hugging Face artifact parent %s at %s for OCI model %s using origin %s@%s",
		parentKey, repairPath, name, identity.HFModelID, identity.HFCommitSHA)

	if err := downloadObjectStorageModel(repairPath); err != nil {
		s.logger.Errorf("failed to repair Hugging Face artifact parent %s at %s: %v", parentKey, repairPath, err)
		if failErr := s.configMapReconciler.markHuggingFaceArtifactParentFailed(ctx, parentKey, identity, repairPath); failErr != nil {
			s.logger.Warnf("failed to mark Hugging Face artifact parent %s at %s as Failed after repair error: %v", parentKey, repairPath, failErr)
		}
		return nil, false, err
	}
	if markerErr := writeHuggingFaceArtifactReadyMarker(repairPath); markerErr != nil {
		s.logger.Warnf("failed to write Hugging Face artifact ready marker for repaired parent %s at %s: %v", parentKey, repairPath, markerErr)
	}
	if err := s.markHuggingFaceArtifactParentReady(ctx, parentKey, repairPath, identity); err != nil {
		s.logger.Errorf("repaired Hugging Face artifact parent %s at %s but failed to mark it Ready: %v", parentKey, repairPath, err)
		return nil, false, err
	}
	artifact, err := s.linkHuggingFaceOriginArtifact(ctx, task, name, destPath, parentKey, repairPath, identity)
	if err != nil {
		return nil, false, err
	}
	return artifact, true, nil
}

func (s *Gopher) tryAcquireHuggingFaceArtifactParentRebuild(ctx context.Context, parentKey string, parentPath string, identity ArtifactIdentity) (string, bool, error) {
	rebuildPath, acquired, err := s.configMapReconciler.markHuggingFaceArtifactParentUpdating(ctx, parentKey, identity, parentPath)
	if err != nil {
		return "", false, err
	}
	if acquired {
		if markerErr := removeHuggingFaceArtifactReadyMarker(rebuildPath); markerErr != nil {
			if failErr := s.configMapReconciler.markHuggingFaceArtifactParentFailed(ctx, parentKey, identity, rebuildPath); failErr != nil {
				s.logger.Warnf("failed to mark Hugging Face artifact parent %s at %s as Failed after ready marker removal error: %v", parentKey, rebuildPath, failErr)
			}
			return "", false, fmt.Errorf("failed to remove Hugging Face artifact ready marker for parent %s at %s before rebuild: %w", parentKey, rebuildPath, markerErr)
		}
		return rebuildPath, true, nil
	}
	return rebuildPath, false, nil
}

func (s *Gopher) waitForHuggingFaceArtifactParent(task *GopherTask, parentKey string) error {
	if s.requeueSamePathInFlightReuseWait(task, parentKey) {
		return nil
	}
	return fmt.Errorf("timed out waiting for Hugging Face artifact parent %s to become available", parentKey)
}

func (s *Gopher) linkHuggingFaceOriginArtifact(ctx context.Context, task *GopherTask, name string, destPath string, parentKey string, parentPath string, identity ArtifactIdentity) (*Artifact, error) {
	if filepath.Clean(destPath) != filepath.Clean(parentPath) {
		if err := utils.CreateSymbolicLink(destPath, parentPath); err != nil {
			s.logger.Errorf("failed to create symbolic link from %s to %s for OCI model %s with Hugging Face origin %s@%s: %s",
				destPath, parentPath, name, identity.HFModelID, identity.HFCommitSHA, err)
			return nil, err
		}
		s.logger.Infof("Successfully created symbolic link from %s to %s for OCI model %s using Hugging Face origin %s@%s",
			destPath, parentPath, name, identity.HFModelID, identity.HFCommitSHA)
	} else {
		s.logger.Infof("OCI model %s already uses canonical Hugging Face artifact path %s", name, parentPath)
	}

	if err := s.recordHuggingFaceOriginChildPath(ctx, parentKey, parentPath, destPath, identity); err != nil {
		if errors.Is(err, errHuggingFaceArtifactParentUpdating) {
			s.logger.Infof("Cannot add OCI model path %s to parent %s because the parent is Updating", destPath, parentKey)
			return nil, err
		}
		s.logger.Errorf("fail to update configmap to add OCI model path %s to parent %s childrenPaths: %s", destPath, parentKey, err)
		return nil, err
	}
	s.logger.Infof("Successfully added OCI model path %s to parent %s childrenPaths", destPath, parentKey)

	// A linked OCI model is a child of the canonical Hugging Face artifact entry.
	// The child entry may only have Updating status at this point, so do not parse
	// it for artifact children; the parent entry owns the childrenPaths list.
	return s.buildArtifactAttributeFromIdentity(identity, parentKey, parentPath, []string{}), nil
}

func (s *Gopher) recordHuggingFaceOriginChildPath(ctx context.Context, parentKey string, parentPath string, childPath string, identity ArtifactIdentity) error {
	if isHuggingFaceArtifactConfigMapKey(parentKey) {
		return s.configMapReconciler.upsertHuggingFaceArtifactParentEntry(ctx, parentKey, identity, parentPath, childPath)
	}
	return s.configMapReconciler.updateConfigMapWithUpdatedChildrenPaths(ctx, parentKey, childPath)
}

func (s *Gopher) markHuggingFaceArtifactParentReady(ctx context.Context, parentKey string, parentPath string, identity ArtifactIdentity) error {
	return s.configMapReconciler.upsertHuggingFaceArtifactParentEntry(ctx, parentKey, identity, parentPath, "")
}

func (s *Gopher) getHuggingFaceArtifactParent(ctx context.Context, identity ArtifactIdentity) (string, string, ModelStatus, bool) {
	parentKey, parentPath, status, ok, _ := s.getHuggingFaceArtifactParentWithError(ctx, identity)
	return parentKey, parentPath, status, ok
}

func (s *Gopher) getHuggingFaceArtifactParentWithError(ctx context.Context, identity ArtifactIdentity) (string, string, ModelStatus, bool, error) {
	parentKey := huggingFaceArtifactConfigMapKey(identity)
	exists, dataEntry, err := s.configMapReconciler.getDataEntryBasedOnModelKey(ctx, parentKey)
	if err != nil {
		s.logger.Warnf("cannot inspect Hugging Face artifact parent %s: %v", parentKey, err)
		return "", "", "", false, err
	}
	if !exists {
		return "", "", "", false, nil
	}

	var entry ModelEntry
	if err := json.Unmarshal([]byte(dataEntry), &entry); err != nil {
		s.logger.Warnf("cannot parse Hugging Face artifact parent %s: %v", parentKey, err)
		return "", "", "", false, nil
	}
	if entry.Config == nil {
		return "", "", "", false, nil
	}
	origin := entry.Config.Artifact.Origin
	if origin == nil || !strings.EqualFold(origin.Type, identity.OriginType) || origin.HFModelID != identity.HFModelID || !strings.EqualFold(origin.HFCommitSHA, identity.HFCommitSHA) {
		return "", "", "", false, nil
	}
	parentPath := entry.Config.Artifact.ParentPath[parentKey]
	if strings.TrimSpace(parentPath) == "" {
		return "", "", "", false, nil
	}
	return parentKey, parentPath, entry.Status, true, nil
}

func (s *Gopher) getReadyHuggingFaceArtifactParent(ctx context.Context, identity ArtifactIdentity) (string, string, bool) {
	parentKey, parentPath, status, ok := s.getHuggingFaceArtifactParent(ctx, identity)
	return parentKey, parentPath, ok && status == ModelStatusReady
}

// requeueIfHuggingFaceArtifactParentUpdating prevents concurrent workers from
// downloading into the same canonical parent path.
func (s *Gopher) requeueIfHuggingFaceArtifactParentUpdating(ctx context.Context, task *GopherTask, identity ArtifactIdentity) (bool, error) {
	parentKey, parentPath, status, ok := s.getHuggingFaceArtifactParent(ctx, identity)
	if !ok || status != ModelStatusUpdating {
		return false, nil
	}
	// The ready marker is written only after object storage download and checksum
	// verification complete. It lets a later task recover if ConfigMap or symlink
	// finalization failed after the canonical parent was fully downloaded.
	if hasHuggingFaceArtifactReadyMarker(parentPath) {
		if err := s.markHuggingFaceArtifactParentReady(ctx, parentKey, parentPath, identity); err != nil {
			s.logger.Warnf("Hugging Face artifact parent %s has a ready marker at %s but cannot be marked Ready yet: %v", parentKey, parentPath, err)
		} else {
			s.logger.Infof("Recovered Hugging Face artifact parent %s from ready marker at %s", parentKey, parentPath)
			return false, nil
		}
	}
	if s.requeueSamePathInFlightReuseWait(task, parentKey) {
		return true, nil
	}
	return true, fmt.Errorf("timed out waiting for Hugging Face artifact parent %s to become Ready", parentKey)
}

// handleHuggingFaceOriginReuseIfNecessary determines whether an OCI model can
// reuse an existing Ready artifact by comparing provenance metadata. The source
// storage remains OCI; the Hugging Face identity is only used to prove that the
// OCI object prefix was imported from the same HF model revision.
func (s *Gopher) handleHuggingFaceOriginReuseIfNecessary(ctx context.Context, modelType string, modelName string, namespace string, identity ArtifactIdentity, currentModelTypeAndNodeName string) (string, string) {
	if !identity.isValid() {
		return "", ""
	}
	if parentKey, parentPath, ok := s.getReadyHuggingFaceArtifactParent(ctx, identity); ok {
		s.logger.Infof("found canonical Hugging Face artifact parent %s for model %s, parentPath is %s", parentKey, modelName, parentPath)
		return parentKey, parentPath
	}

	s.logger.Infof("no canonical Hugging Face artifact parent found for model %s with identity %s@%s", modelName, identity.HFModelID, identity.HFCommitSHA)
	return "", ""
}
