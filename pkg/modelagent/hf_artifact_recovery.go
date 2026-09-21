package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// getConfigMapForRecovery seeds reconstruction from a fresh serialized read.
// Startup uses it before child tasks have written statuses or registered UIDs.
// Never publish a caller-supplied snapshot after newer mutations have committed.
func (c *ConfigMapReconciler) getConfigMapForRecovery(ctx context.Context) (*corev1.ConfigMap, error) {
	c.configMapMutationMutex.Lock()
	defer c.configMapMutationMutex.Unlock()
	cm, err := c.getConfigMap(ctx)
	if err != nil {
		return nil, err
	}
	c.cacheCommittedConfigMapEntries(cm.Data, cm.Data, "", "")
	return cm, nil
}

// cacheCommittedConfigMapEntries runs under configMapMutationMutex, after a
// successful CAS (or a no-op read). Missing records are not deletions unless
// this mutation removed them. JSON strings avoid aliasing callers' maps.
func (c *ConfigMapReconciler) cacheCommittedConfigMapEntries(before, after map[string]string, modelID string, modelUID types.UID) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	if c.modelCache == nil {
		c.modelCache = make(map[string]*CacheEntry)
	}
	if c.hfArtifactCache == nil {
		c.hfArtifactCache = make(map[string]string)
	}
	for key := range before {
		if _, present := after[key]; !present {
			if isHfArtifactConfigMapKey(key) {
				delete(c.hfArtifactCache, key)
			} else {
				c.evictCachedModelLocked(key)
			}
		}
	}
	for key := range after {
		if !isHfArtifactConfigMapKey(key) {
			c.cacheCommittedModelEntryLocked(before, after, key, modelID, modelUID)
		}
	}
	// Children must be processed first so parent snapshots exclude evicted models.
	for key, raw := range after {
		if isHfArtifactConfigMapKey(key) {
			c.cacheCommittedHfParentLocked(key, raw)
		}
	}
}

// cacheCommittedModelEntryLocked preserves UID ownership while caching committed
// child state. The caller holds configMapMutationMutex and cacheMutex.
func (c *ConfigMapReconciler) cacheCommittedModelEntryLocked(before, after map[string]string, key, modelID string, modelUID types.UID) {
	model, err := existingModelEntry(after, key)
	if err != nil || model.Name == "" {
		return
	}
	if model.Status == ModelStatusDeleted {
		c.evictCachedModelLocked(key)
		return
	}
	entry := CacheEntry{}
	if current := c.modelCache[key]; current != nil {
		entry = *current
	}
	raw := after[key]
	if model.HfArtifactKey == "" && entry.ModelEntryJSON == "" {
		// Only explicit ordinary writes may seed typed recovery. Observing
		// unrelated records, including at startup, must not adopt them.
		if modelID == "" && before[key] != raw && c.modelCache[key] == nil {
			if _, evicted := c.evictedModels[key]; !evicted && !c.isModelUIDInvalidatedLocked(key, "") {
				c.modelCache[key] = &CacheEntry{ModelName: model.Name, ModelStatus: model.Status}
			}
		}
		return
	}
	if key == modelID && !c.isModelMutationBlockedLocked(key, modelUID) {
		entry.ModelUID = modelUID
		delete(c.evictedModels, key)
	}
	if _, evicted := c.evictedModels[key]; evicted || c.isModelUIDInvalidatedLocked(key, entry.ModelUID) {
		return
	}
	entry.ModelName = model.Name
	entry.ModelStatus = model.Status
	entry.ModelEntryJSON = raw
	c.modelCache[key] = &entry
}

// cacheCommittedHfParentLocked excludes evicted children from the recovery
// snapshot. The caller holds configMapMutationMutex and cacheMutex.
func (c *ConfigMapReconciler) cacheCommittedHfParentLocked(key, raw string) {
	parent, ok := hfArtifactRecoveryParent(key, raw)
	if !ok {
		// A corrupt foreign record must not block unrelated cache updates.
		return
	}
	for childKey := range parent.Children {
		if _, evicted := c.evictedModels[childKey]; evicted {
			delete(parent.Children, childKey)
			delete(parent.ChildStatusesBeforeRepair, childKey)
		}
	}
	encoded, err := json.Marshal(parent)
	if err == nil {
		c.hfArtifactCache[key] = string(encoded)
	}
}

// evictCachedModelLocked also removes cached reverse references so losing the
// ConfigMap during deletion cannot resurrect a child through its parent.
func (c *ConfigMapReconciler) evictCachedModelLocked(modelID string) {
	delete(c.modelCache, modelID)
	if c.evictedModels == nil {
		c.evictedModels = make(map[string]struct{})
	}
	c.evictedModels[modelID] = struct{}{}
	for key, raw := range c.hfArtifactCache {
		parent, ok := hfArtifactRecoveryParent(key, raw)
		if !ok {
			continue
		}
		delete(parent.Children, modelID)
		delete(parent.ChildStatusesBeforeRepair, modelID)
		encoded, err := json.Marshal(parent)
		if err == nil {
			c.hfArtifactCache[key] = string(encoded)
		}
	}
}

func hfArtifactRecoveryParent(key, raw string) (HfArtifactEntry, bool) {
	parent, err := decodeHfArtifactEntry(key, raw)
	if err != nil || parent.Key != key || validateHfArtifactIdentityAndPath(parent) != nil || !isValidHfArtifactStatus(parent.Status) {
		return HfArtifactEntry{}, false
	}
	for childKey, path := range parent.Children {
		if strings.TrimSpace(childKey) == "" || isHfArtifactConfigMapKey(childKey) || strings.TrimSpace(path) == "" {
			return HfArtifactEntry{}, false
		}
	}
	return parent, true
}

// cachedConfigMapEntries copies immutable records under cacheMutex. All maps
// subsequently decoded by recovery belong to that attempt, not to the cache.
func (c *ConfigMapReconciler) cachedConfigMapEntries() (map[string]string, map[string]string) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	models := make(map[string]string, len(c.modelCache))
	for key, entry := range c.modelCache {
		if entry == nil || entry.ModelStatus == ModelStatusDeleted || c.isModelMutationBlockedLocked(key, entry.ModelUID) {
			continue
		}
		if entry.ModelEntryJSON != "" {
			models[key] = entry.ModelEntryJSON
			continue
		}
		model := ordinaryCachedModelEntry(entry)
		if encoded, err := json.Marshal(model); err == nil && model.Name != "" {
			models[key] = string(encoded)
		}
	}
	return models, maps.Clone(c.hfArtifactCache)
}

// Ordinary status/progress/metadata updates may replace malformed legacy JSON,
// but must not erase a known shared relationship.
func (c *ConfigMapReconciler) validateHfArtifactChildMutation(data map[string]string, key string) error {
	child, childErr := existingModelEntry(data, key)
	for parentKey, raw := range data {
		if !isHfArtifactConfigMapKey(parentKey) {
			continue
		}
		parent, valid := hfArtifactRecoveryParent(parentKey, raw)
		if _, references := parent.Children[key]; valid && references && (childErr != nil || child.HfArtifactKey != parentKey) {
			return fmt.Errorf("shared artifact relationship for model %s requires reconciliation", key)
		}
	}
	c.cacheMutex.RLock()
	var cachedJSON string
	if cached := c.modelCache[key]; cached != nil {
		cachedJSON = cached.ModelEntryJSON
	}
	c.cacheMutex.RUnlock()
	var cached ModelEntry
	if cachedJSON == "" || json.Unmarshal([]byte(cachedJSON), &cached) != nil {
		return nil
	}
	if cached.HfArtifactKey != "" && (childErr != nil || child.HfArtifactKey == "") {
		return fmt.Errorf("shared artifact state for model %s requires reconciliation", key)
	}
	return nil
}

// restoreCachedConfigMapEntries runs inside a serialized mutation and uses only
// current trusted cache entries. A modelID limits ordinary model writes to that
// child's group; periodic recovery and whole-ConfigMap loss restore all groups.
// Ordinary records are restored only by explicit reconciliation, not as a side
// effect of status, metadata, or progress mutations.
func (c *ConfigMapReconciler) restoreCachedConfigMapEntries(data map[string]string, modelID string, includeOrdinary bool) (bool, error) {
	models, parents := c.cachedConfigMapEntries()
	if !includeOrdinary {
		c.cacheMutex.RLock()
		for key := range models {
			if entry := c.modelCache[key]; entry == nil || entry.ModelEntryJSON == "" {
				delete(models, key)
			}
		}
		c.cacheMutex.RUnlock()
	}
	changed := false
	for key, raw := range parents {
		cached, ok := hfArtifactRecoveryParent(key, raw)
		if !ok {
			continue
		}
		if modelID != "" {
			model, err := existingModelEntry(models, modelID)
			if err != nil || model.HfArtifactKey != key {
				continue
			}
		}
		restored, err := restoreHfArtifactParent(data, models, cached, modelID)
		if err != nil {
			return false, err
		}
		changed = changed || restored
	}
	for key, raw := range models {
		if _, exists := data[key]; exists || modelID != "" && modelID != key {
			continue
		}
		model, err := existingModelEntry(models, key)
		if err == nil && model.HfArtifactKey == "" && model.Status != ModelStatusDeleted {
			data[key] = raw
			changed = true
		}
	}
	if _, found := data[modelID]; modelID != "" && !found {
		model, err := existingModelEntry(models, modelID)
		if err == nil && model.HfArtifactKey != "" {
			return false, fmt.Errorf("shared Hugging Face relationship for model %s requires reconciliation", modelID)
		}
	}
	return changed, nil
}

// restoreHfArtifactParent restores one parent's relationships as needing
// validation, while preserving current records and any active lock owner.
func restoreHfArtifactParent(data, models map[string]string, cached HfArtifactEntry, modelID string) (bool, error) {
	parent := cached
	currentRaw, exists := data[cached.Key]
	if exists {
		var ok bool
		parent, ok = hfArtifactRecoveryParent(cached.Key, currentRaw)
		if !ok || parent.Status == HfArtifactStatusUpdating {
			// Do not replace corrupt records or interrupt a durable lock owner.
			return false, nil
		}
	}
	needsRecovery := !exists
	for childKey, path := range parent.Children {
		if _, present := data[childKey]; !present && (modelID == "" || modelID == childKey) {
			child, err := existingModelEntry(models, childKey)
			if err == nil && child.HfArtifactKey == parent.Key && filepath.Clean(cached.Children[childKey]) == filepath.Clean(path) {
				needsRecovery = true
			}
		}
	}
	if !needsRecovery {
		return false, nil
	}
	if parent.ChildStatusesBeforeRepair == nil {
		parent.ChildStatusesBeforeRepair = make(map[string]ModelStatus)
	}
	for childKey, path := range parent.Children {
		child, err := existingModelEntry(data, childKey)
		if _, present := data[childKey]; !present {
			child, err = existingModelEntry(models, childKey)
			if filepath.Clean(cached.Children[childKey]) != filepath.Clean(path) {
				continue
			}
		}
		_, trusted := models[childKey]
		if !trusted || err != nil || child.HfArtifactKey != parent.Key || child.Status == ModelStatusDeleted {
			if !exists {
				delete(parent.Children, childKey)
				delete(parent.ChildStatusesBeforeRepair, childKey)
			}
			continue
		}
		if _, saved := parent.ChildStatusesBeforeRepair[childKey]; !saved || child.Status != ModelStatusFailed {
			parent.ChildStatusesBeforeRepair[childKey] = child.Status
		}
		child.Status = ModelStatusFailed
		if _, err := writeModelEntry(data, childKey, child); err != nil {
			return false, err
		}
	}
	parent.Status = HfArtifactStatusFailed
	parent.LockID = ""
	parent.LastCompletedLockID = ""
	if _, err := writeHfArtifactEntry(data, parent); err != nil {
		return false, err
	}
	return true, nil
}
