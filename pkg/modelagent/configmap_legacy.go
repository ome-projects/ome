package modelagent

import "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"

// cacheOrdinaryModel keeps the legacy status/metadata cache independent of
// progress and unrelated ConfigMap observations. Shared records are published
// by the serialized mutation that committed their ownership state.
func (c *ConfigMapReconciler) cacheOrdinaryModel(base *v1beta1.BaseModel, cluster *v1beta1.ClusterBaseModel, status *ModelStatus, metadata *ModelMetadata) {
	key, uid := getModelID(base, cluster), getModelResourceUID(base, cluster)
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	if c.isModelMutationBlockedLocked(key, uid) {
		return
	}
	entry := c.modelCache[key]
	if entry != nil && entry.ModelEntryJSON != "" {
		return
	}
	if entry == nil {
		name := ""
		if base != nil {
			name = base.Name
		} else if cluster != nil {
			name = cluster.Name
		}
		entry = &CacheEntry{ModelName: name}
		if c.modelCache == nil {
			c.modelCache = make(map[string]*CacheEntry)
		}
		c.modelCache[key] = entry
	}
	entry.ModelUID = uid
	if status != nil {
		entry.ModelStatus = *status
	}
	if metadata != nil {
		entry.ModelMetadata = metadata
	}
	delete(c.evictedModels, key)
}

// ordinaryCachedModelEntry retains the legacy recovery field set. In
// particular, transient progress is not a source of recoverable model state.
func ordinaryCachedModelEntry(entry *CacheEntry) ModelEntry {
	model := ModelEntry{Name: entry.ModelName, Status: entry.ModelStatus}
	if metadata := entry.ModelMetadata; metadata != nil {
		model.Config = &ModelConfig{
			ModelType: metadata.ModelType, ModelArchitecture: metadata.ModelArchitecture,
			ModelCapabilities: metadata.ModelCapabilities, ModelParameterSize: metadata.ModelParameterSize,
			MaxTokens: metadata.MaxTokens, Quantization: string(metadata.Quantization),
			ApiCapabilities: metadata.ApiCapabilities, Artifact: metadata.Artifact,
		}
	}
	return model
}
