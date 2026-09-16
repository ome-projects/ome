package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/constants"
)

// RemoveModelReferenceResult describes the relationship removed from both the
// model and shared artifact entries. LastReferenceRemoved means the returned
// artifact owns the deletion lock and is ready for local-file cleanup.
type RemoveModelReferenceResult struct {
	Artifact             HfArtifactEntry
	ReferenceRemoved     bool
	LastReferenceRemoved bool
}

type HfArtifactRepository struct {
	configMaps *ConfigMapReconciler
}

func newHfArtifactRepository(configMaps *ConfigMapReconciler) *HfArtifactRepository {
	return &HfArtifactRepository{configMaps: configMaps}
}

func (r *HfArtifactRepository) Get(ctx context.Context, identity HfArtifactIdentity) (HfArtifactEntry, bool, error) {
	if !isValidHfArtifactIdentity(identity) {
		return HfArtifactEntry{}, false, fmt.Errorf("invalid Hugging Face artifact identity")
	}
	key := hfArtifactConfigMapKey(identity)
	exists, raw, err := r.configMaps.getDataEntryBasedOnModelKey(ctx, key)
	if err != nil || !exists {
		return HfArtifactEntry{}, false, err
	}
	stored, err := decodeHfArtifactEntry(key, raw)
	if err != nil {
		return HfArtifactEntry{}, false, err
	}
	if err := validateStoredHfArtifactIdentity(stored, identity); err != nil {
		return HfArtifactEntry{}, false, err
	}
	if err := validateHfArtifactIdentityAndPath(stored); err != nil {
		return HfArtifactEntry{}, false, err
	}
	if !isValidHfArtifactStatus(stored.Status) {
		return HfArtifactEntry{}, false, fmt.Errorf("Hugging Face artifact %s has invalid status %q", key, stored.Status)
	}
	return stored, true, nil
}

// GetParentForChild follows a child's persisted parent reference and verifies
// both relationship directions from one ConfigMap snapshot. With a nil error,
// found=false means the child entry or its HfArtifactKey is absent; it says
// nothing about local symlinks or a parent entry that still lists the child.
func (r *HfArtifactRepository) GetParentForChild(ctx context.Context, childModelKey string) (HfArtifactEntry, bool, error) {
	if strings.TrimSpace(childModelKey) == "" || isHfArtifactConfigMapKey(childModelKey) {
		return HfArtifactEntry{}, false, fmt.Errorf("invalid child model ConfigMap key %q", childModelKey)
	}
	configMap, err := r.configMaps.getConfigMap(ctx)
	if err != nil {
		return HfArtifactEntry{}, false, err
	}
	if configMap.Data == nil {
		return HfArtifactEntry{}, false, nil
	}
	if _, exists := configMap.Data[childModelKey]; !exists {
		return HfArtifactEntry{}, false, nil
	}
	child, err := existingModelEntry(configMap.Data, childModelKey)
	if err != nil {
		return HfArtifactEntry{}, false, err
	}
	return parentForChildEntry(configMap.Data, childModelKey, child)
}

// parentForChildEntry resolves both relationship directions from the same
// snapshot as the decoded child. An existing child may have no parent yet.
func parentForChildEntry(data map[string]string, childModelKey string, child ModelEntry) (HfArtifactEntry, bool, error) {
	if child.HfArtifactKey == "" {
		return HfArtifactEntry{}, false, nil
	}
	if !isHfArtifactConfigMapKey(child.HfArtifactKey) {
		return HfArtifactEntry{}, false, fmt.Errorf("child model %s has invalid Hugging Face parent key %s", childModelKey, child.HfArtifactKey)
	}
	raw, exists := data[child.HfArtifactKey]
	if !exists {
		return HfArtifactEntry{}, false, fmt.Errorf("child model %s references missing Hugging Face parent %s", childModelKey, child.HfArtifactKey)
	}
	parent, err := decodeHfArtifactEntry(child.HfArtifactKey, raw)
	if err != nil {
		return HfArtifactEntry{}, false, err
	}
	if parent.Key != child.HfArtifactKey {
		return HfArtifactEntry{}, false, fmt.Errorf("Hugging Face parent entry %s records key %s", child.HfArtifactKey, parent.Key)
	}
	if err := validateHfArtifactIdentityAndPath(parent); err != nil {
		return HfArtifactEntry{}, false, err
	}
	if !isValidHfArtifactStatus(parent.Status) {
		return HfArtifactEntry{}, false, fmt.Errorf("Hugging Face parent %s has invalid status %q", parent.Key, parent.Status)
	}
	if childPath, found := parent.Children[childModelKey]; !found || strings.TrimSpace(childPath) == "" {
		return HfArtifactEntry{}, false, fmt.Errorf("child model %s and Hugging Face parent %s do not contain matching references", childModelKey, parent.Key)
	}
	return parent, true, nil
}

// isChildMutationBlocked reports whether in-memory model UID checks reject
// changes from this child CR instance: deletion invalidated its UID, or the
// cached model has a different UID. A missing UID is also blocked when the key
// has invalidated UIDs. It does not inspect parent status or lock ownership,
// and false does not prove that the CR still exists.
func (r *HfArtifactRepository) isChildMutationBlocked(childModelKey string, childModelUID types.UID) bool {
	return r.configMaps.isModelMutationBlocked(childModelKey, childModelUID)
}

// TryAcquireLock atomically creates or locks an artifact that needs work.
// When acquired is true, the returned artifact is Updating and its LockID is
// owned by the caller. When acquired is false, the artifact is either Ready or
// Updating under another caller's lock; callers must inspect Status and must
// not use the returned LockID as their own.
func (r *HfArtifactRepository) TryAcquireLock(ctx context.Context, expected HfArtifactEntry) (HfArtifactEntry, bool, error) {
	return r.tryAcquireLock(ctx, expected, false)
}

// TryAcquireLockForRepair is equivalent to TryAcquireLock, except that a Ready
// artifact is also transitioned to Updating under a new caller-owned LockID.
func (r *HfArtifactRepository) TryAcquireLockForRepair(ctx context.Context, expected HfArtifactEntry) (HfArtifactEntry, bool, error) {
	return r.tryAcquireLock(ctx, expected, true)
}

// tryAcquireLock changes artifact state and LockID in one ConfigMap CAS. The
// same LockID is retained across optimistic-concurrency retries.
func (r *HfArtifactRepository) tryAcquireLock(ctx context.Context, expected HfArtifactEntry, forRepair bool) (HfArtifactEntry, bool, error) {
	if err := validateHfArtifactIdentityAndPath(expected); err != nil {
		return HfArtifactEntry{}, false, err
	}

	lockID := uuid.NewString()
	var result HfArtifactEntry
	acquired := false
	err := r.configMaps.mutateConfigMapWithRetry(ctx, func(configMap *corev1.ConfigMap) (bool, error) {
		result = HfArtifactEntry{}
		acquired = false
		if configMap.Data == nil {
			configMap.Data = make(map[string]string)
		}

		raw, exists := configMap.Data[expected.Key]
		if !exists {
			stored := expected
			stored.Status = HfArtifactStatusUpdating
			stored.LockID = lockID
			stored.LastCompletedLockID = ""
			if stored.Children == nil {
				stored.Children = make(map[string]string)
			}
			result = stored
			acquired = true
			return writeHfArtifactEntry(configMap.Data, stored)
		}

		stored, decodeErr := decodeHfArtifactEntry(expected.Key, raw)
		if decodeErr != nil {
			return false, decodeErr
		}
		if err := validateStoredHfArtifactIdentity(stored, expected.Identity); err != nil {
			return false, err
		}

		if stored.Status == HfArtifactStatusUpdating {
			if stored.LocalPath != "" && stored.LocalPath != expected.LocalPath {
				return false, fmt.Errorf("Hugging Face artifact %s is Updating and uses noncanonical path %s", stored.Key, stored.LocalPath)
			}
			stored = applyExpectedHfArtifactMetadata(stored, expected)
			result = stored
			return writeHfArtifactEntry(configMap.Data, stored)
		}

		metadataComplete := sameHfArtifactIdentity(stored.Identity, expected.Identity) && stored.LocalPath == expected.LocalPath
		stored = applyExpectedHfArtifactMetadata(stored, expected)
		if stored.Status == HfArtifactStatusReady && metadataComplete && !forRepair {
			result = stored
			return false, nil
		}
		if stored.Status != HfArtifactStatusReady && stored.Status != HfArtifactStatusFailed {
			return false, fmt.Errorf("Hugging Face artifact %s has invalid status %q", stored.Key, stored.Status)
		}

		stored.Status = HfArtifactStatusUpdating
		stored.LockID = lockID
		stored.LastCompletedLockID = ""
		result = stored
		acquired = true
		return writeHfArtifactEntry(configMap.Data, stored)
	})
	return result, acquired, err
}

// MarkReady completes work owned by expected.LockID. It atomically sets the
// artifact Ready, restores affected child statuses, and clears the lock and
// repair snapshot, rejecting stale or non-owner callers.
func (r *HfArtifactRepository) MarkReady(ctx context.Context, expected HfArtifactEntry) error {
	return r.setStatus(ctx, expected, HfArtifactStatusReady)
}

// MarkFailed ends work owned by expected.LockID after an error. It atomically
// sets the artifact Failed and clears the lock, rejecting stale callers.
func (r *HfArtifactRepository) MarkFailed(ctx context.Context, expected HfArtifactEntry) error {
	return r.setStatus(ctx, expected, HfArtifactStatusFailed)
}

// setStatus performs the lock-fenced terminal transition for an artifact
// currently in Updating state.
func (r *HfArtifactRepository) setStatus(ctx context.Context, expected HfArtifactEntry, status HfArtifactStatus) error {
	if err := validateHfArtifactIdentityAndPath(expected); err != nil {
		return err
	}
	return r.configMaps.mutateConfigMapWithRetry(ctx, func(configMap *corev1.ConfigMap) (bool, error) {
		stored, err := requireStoredHfArtifactEntry(configMap.Data, expected)
		if err != nil {
			return false, err
		}
		if stored.Status == status {
			if expected.LockID != "" && stored.LastCompletedLockID == expected.LockID {
				return false, nil
			}
			return false, fmt.Errorf("Hugging Face artifact %s lock ID does not match current owner", stored.Key)
		}
		if stored.Status != HfArtifactStatusUpdating {
			return false, fmt.Errorf("Hugging Face artifact %s cannot transition from %s to %s", stored.Key, stored.Status, status)
		}
		if expected.LockID == "" || stored.LockID != expected.LockID {
			return false, fmt.Errorf("Hugging Face artifact %s lock ID does not match current owner", stored.Key)
		}
		if status == HfArtifactStatusReady {
			statuses, err := hfArtifactChildStatusesToRestore(configMap.Data, stored, expected)
			if err != nil {
				return false, err
			}
			for key, childStatus := range statuses {
				child, err := existingModelEntry(configMap.Data, key)
				if err != nil {
					return false, err
				}
				child.Status = childStatus
				if _, err := writeModelEntry(configMap.Data, key, child); err != nil {
					return false, err
				}
			}
			stored.ChildStatusesBeforeRepair = nil
		}
		stored.Status = status
		stored.LastCompletedLockID = expected.LockID
		stored.LockID = ""
		return writeHfArtifactEntry(configMap.Data, stored)
	})
}

// AddModelReference records both sides of the model-to-artifact relationship
// in one ConfigMap update. The model UID prevents a superseded task from
// changing a replacement model's reference. The caller creates or verifies the
// symlink first.
func (r *HfArtifactRepository) AddModelReference(ctx context.Context, expected HfArtifactEntry, modelKey string, modelUID types.UID, modelPath string) error {
	if err := validateHfArtifactIdentityAndPath(expected); err != nil {
		return err
	}
	if strings.TrimSpace(modelKey) == "" || isHfArtifactConfigMapKey(modelKey) {
		return fmt.Errorf("invalid model ConfigMap key %q", modelKey)
	}
	if strings.TrimSpace(modelPath) == "" {
		return fmt.Errorf("model path is empty")
	}

	return r.configMaps.mutateConfigMapWithRetry(ctx, func(configMap *corev1.ConfigMap) (bool, error) {
		if r.configMaps.isModelMutationBlocked(modelKey, modelUID) {
			r.configMaps.logger.Debugf("Skipping stale Hugging Face artifact reference update for model %s", modelKey)
			return false, nil
		}
		stored, err := requireStoredHfArtifactEntry(configMap.Data, expected)
		if err != nil {
			return false, err
		}
		if stored.Status != HfArtifactStatusReady {
			return false, fmt.Errorf("Hugging Face artifact %s is %s", stored.Key, stored.Status)
		}

		model, err := existingModelEntry(configMap.Data, modelKey)
		if err != nil {
			return false, err
		}
		if model.HfArtifactKey != "" && model.HfArtifactKey != stored.Key {
			return false, fmt.Errorf("model %s already references Hugging Face artifact %s", modelKey, model.HfArtifactKey)
		}

		if stored.Children == nil {
			stored.Children = make(map[string]string)
		}
		// A retry may confirm the same path, but must not silently replace the
		// recorded path and leave the old child symlink untracked.
		if storedChildPath, exists := stored.Children[modelKey]; exists &&
			filepath.Clean(storedChildPath) != filepath.Clean(modelPath) {
			return false, fmt.Errorf(
				"child model %s path %s does not match recorded child path %s",
				modelKey,
				modelPath,
				storedChildPath,
			)
		}
		stored.Children[modelKey] = modelPath
		model.HfArtifactKey = stored.Key

		artifactChanged, err := writeHfArtifactEntry(configMap.Data, stored)
		if err != nil {
			return false, err
		}
		modelChanged, err := writeModelEntry(configMap.Data, modelKey, model)
		return artifactChanged || modelChanged, err
	})
}

// RemoveModelReference removes both sides of a consistent model-to-artifact
// relationship. The model UID prevents a superseded task from changing a
// replacement model's reference. It refuses partial records so cleanup cannot
// delete an artifact that may still be referenced. Removing the final
// reference also moves the artifact to Updating under a new deletion LockID.
// expectedModelPath must match the stored child path; it is not a replacement path.
func (r *HfArtifactRepository) RemoveModelReference(
	ctx context.Context,
	expected HfArtifactEntry,
	modelKey string,
	modelUID types.UID,
	expectedModelPath string,
) (RemoveModelReferenceResult, error) {
	if err := validateHfArtifactIdentityAndPath(expected); err != nil {
		return RemoveModelReferenceResult{}, err
	}
	if strings.TrimSpace(expectedModelPath) == "" {
		return RemoveModelReferenceResult{}, fmt.Errorf("model path is empty")
	}
	var result RemoveModelReferenceResult
	err := r.configMaps.mutateConfigMapWithRetry(ctx, func(configMap *corev1.ConfigMap) (bool, error) {
		result = RemoveModelReferenceResult{}
		if r.configMaps.isModelMutationBlocked(modelKey, modelUID) {
			r.configMaps.logger.Debugf("Skipping stale Hugging Face artifact reference removal for model %s", modelKey)
			return false, nil
		}
		if configMap.Data == nil {
			return false, nil
		}
		if _, exists := configMap.Data[expected.Key]; !exists {
			if _, modelExists := configMap.Data[modelKey]; !modelExists {
				return false, nil
			}
			model, err := existingModelEntry(configMap.Data, modelKey)
			if err != nil {
				return false, err
			}
			if model.HfArtifactKey == expected.Key {
				return false, fmt.Errorf("model %s references missing Hugging Face artifact %s", modelKey, expected.Key)
			}
			return false, nil
		}

		stored, err := requireStoredHfArtifactEntry(configMap.Data, expected)
		if err != nil {
			return false, err
		}
		if stored.Status == HfArtifactStatusUpdating {
			return false, fmt.Errorf("Hugging Face artifact %s is Updating", stored.Key)
		}

		storedModelPath, artifactReferencesModel := stored.Children[modelKey]
		model, modelErr := existingModelEntry(configMap.Data, modelKey)
		if modelErr != nil {
			if artifactReferencesModel {
				return false, fmt.Errorf("cannot remove model %s reference from Hugging Face artifact %s: %w", modelKey, stored.Key, modelErr)
			}
			return false, nil
		}
		modelReferencesArtifact := model.HfArtifactKey == stored.Key
		if model.HfArtifactKey != "" && !modelReferencesArtifact {
			if artifactReferencesModel {
				return false, fmt.Errorf("model %s and Hugging Face artifact %s contain conflicting references", modelKey, stored.Key)
			}
			return false, nil
		}
		if artifactReferencesModel != modelReferencesArtifact {
			return false, fmt.Errorf("model %s and Hugging Face artifact %s do not contain matching references", modelKey, stored.Key)
		}
		if !artifactReferencesModel {
			return false, nil
		}
		if strings.TrimSpace(storedModelPath) == "" {
			return false, fmt.Errorf("Hugging Face artifact %s has an empty path for model %s", stored.Key, modelKey)
		}
		if filepath.Clean(storedModelPath) != filepath.Clean(expectedModelPath) {
			return false, fmt.Errorf(
				"model %s path %s does not match recorded child path %s",
				modelKey,
				expectedModelPath,
				storedModelPath,
			)
		}

		delete(stored.Children, modelKey)
		delete(stored.ChildStatusesBeforeRepair, modelKey)
		model.HfArtifactKey = ""
		result = RemoveModelReferenceResult{Artifact: stored, ReferenceRemoved: true}
		if len(stored.Children) == 0 {
			stored.Status = HfArtifactStatusUpdating
			stored.LockID = uuid.NewString()
			stored.LastCompletedLockID = ""
			result.Artifact = stored
			result.LastReferenceRemoved = true
		}

		artifactChanged, err := writeHfArtifactEntry(configMap.Data, stored)
		if err != nil {
			return false, err
		}
		modelChanged, err := writeModelEntry(configMap.Data, modelKey, model)
		return artifactChanged || modelChanged, err
	})
	return result, err
}

// DeleteIfUnreferenced removes artifact state after the lock owner deletes the
// local files. It succeeds only while the artifact remains unreferenced, Updating,
// and owned by expected.LockID.
func (r *HfArtifactRepository) DeleteIfUnreferenced(ctx context.Context, expected HfArtifactEntry) (bool, error) {
	if err := validateHfArtifactIdentityAndPath(expected); err != nil {
		return false, err
	}
	deleted := false
	err := r.configMaps.mutateConfigMapWithRetry(ctx, func(configMap *corev1.ConfigMap) (bool, error) {
		deleted = false
		if configMap.Data == nil {
			return false, nil
		}
		if _, exists := configMap.Data[expected.Key]; !exists {
			return false, nil
		}
		stored, err := requireStoredHfArtifactEntry(configMap.Data, expected)
		if err != nil {
			return false, err
		}
		if len(stored.Children) != 0 || stored.Status != HfArtifactStatusUpdating {
			return false, nil
		}
		if expected.LockID == "" || stored.LockID != expected.LockID {
			return false, fmt.Errorf("Hugging Face artifact %s lock ID does not match current owner", stored.Key)
		}
		delete(configMap.Data, expected.Key)
		deleted = true
		return true, nil
	})
	return deleted, err
}

func requireStoredHfArtifactEntry(data map[string]string, expected HfArtifactEntry) (HfArtifactEntry, error) {
	if data == nil {
		return HfArtifactEntry{}, fmt.Errorf("Hugging Face artifact %s does not exist", expected.Key)
	}
	raw, exists := data[expected.Key]
	if !exists {
		return HfArtifactEntry{}, fmt.Errorf("Hugging Face artifact %s does not exist", expected.Key)
	}
	stored, err := decodeHfArtifactEntry(expected.Key, raw)
	if err != nil {
		return HfArtifactEntry{}, err
	}
	if err := validateStoredHfArtifactIdentity(stored, expected.Identity); err != nil {
		return HfArtifactEntry{}, err
	}
	if stored.Key != expected.Key || stored.LocalPath != expected.LocalPath {
		return HfArtifactEntry{}, fmt.Errorf("Hugging Face artifact %s has incomplete canonical metadata", expected.Key)
	}
	return stored, nil
}

func existingModelEntry(data map[string]string, modelKey string) (ModelEntry, error) {
	if data == nil {
		return ModelEntry{}, fmt.Errorf("model entry %s does not exist", modelKey)
	}
	raw, exists := data[modelKey]
	if !exists {
		return ModelEntry{}, fmt.Errorf("model entry %s does not exist", modelKey)
	}
	var entry ModelEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return ModelEntry{}, fmt.Errorf("cannot decode model entry %s: %w", modelKey, err)
	}
	return entry, nil
}

func validateHfArtifactIdentityAndPath(artifact HfArtifactEntry) error {
	if !isValidHfArtifactIdentity(artifact.Identity) {
		return fmt.Errorf("invalid Hugging Face artifact identity")
	}
	expectedName := hfArtifactConfigMapKey(artifact.Identity)
	if artifact.Key != expectedName {
		return fmt.Errorf("Hugging Face artifact key %s does not match identity; expected %s", artifact.Key, expectedName)
	}
	if strings.TrimSpace(artifact.LocalPath) == "" {
		return fmt.Errorf("Hugging Face artifact path is empty")
	}
	expectedSuffix := filepath.Join(
		constants.ModelArtifactsDirectory,
		filepath.FromSlash(artifact.Identity.ModelID),
		strings.ToLower(artifact.Identity.CommitSHA),
	)
	cleanPath := filepath.Clean(artifact.LocalPath)
	if cleanPath != expectedSuffix && !strings.HasSuffix(cleanPath, string(filepath.Separator)+expectedSuffix) {
		return fmt.Errorf("Hugging Face artifact path %s is not canonical for identity %s@%s", artifact.LocalPath, artifact.Identity.ModelID, artifact.Identity.CommitSHA)
	}
	return nil
}

func validateStoredHfArtifactIdentity(stored HfArtifactEntry, expected HfArtifactIdentity) error {
	if stored.Key != "" && stored.Key != hfArtifactConfigMapKey(expected) {
		return fmt.Errorf("Hugging Face artifact %s has invalid stored key %s", hfArtifactConfigMapKey(expected), stored.Key)
	}
	if stored.Identity == (HfArtifactIdentity{}) {
		return nil
	}
	if !isValidHfArtifactIdentity(stored.Identity) {
		return fmt.Errorf("Hugging Face artifact %s has invalid identity", hfArtifactConfigMapKey(expected))
	}
	if !sameHfArtifactIdentity(stored.Identity, expected) {
		return fmt.Errorf("Hugging Face artifact %s has mismatched identity metadata", hfArtifactConfigMapKey(expected))
	}
	return nil
}

func applyExpectedHfArtifactMetadata(stored, expected HfArtifactEntry) HfArtifactEntry {
	stored.Key = expected.Key
	stored.Identity = expected.Identity
	stored.LocalPath = expected.LocalPath
	if stored.Children == nil {
		stored.Children = make(map[string]string)
	}
	return stored
}

func decodeHfArtifactEntry(key, raw string) (HfArtifactEntry, error) {
	var entry HfArtifactEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return HfArtifactEntry{}, fmt.Errorf("cannot decode Hugging Face artifact %s: %w", key, err)
	}
	return entry, nil
}

func writeHfArtifactEntry(data map[string]string, artifact HfArtifactEntry) (bool, error) {
	encoded, err := json.Marshal(artifact)
	if err != nil {
		return false, err
	}
	next := string(encoded)
	if data[artifact.Key] == next {
		return false, nil
	}
	data[artifact.Key] = next
	return true, nil
}

func writeModelEntry(data map[string]string, modelKey string, model ModelEntry) (bool, error) {
	encoded, err := json.Marshal(model)
	if err != nil {
		return false, err
	}
	next := string(encoded)
	if data[modelKey] == next {
		return false, nil
	}
	data[modelKey] = next
	return true, nil
}

func isValidHfArtifactStatus(status HfArtifactStatus) bool {
	return status == HfArtifactStatusReady || status == HfArtifactStatusUpdating || status == HfArtifactStatusFailed
}

func sameHfArtifactIdentity(left, right HfArtifactIdentity) bool {
	return left.ModelID == right.ModelID && strings.EqualFold(left.CommitSHA, right.CommitSHA)
}
