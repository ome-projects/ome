package modelagent

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// This is local file safety, not serving-demand authorization. Persisted claims
// always count; placement only filters unpersisted live Model references.
// data is the fresh ConfigMap snapshot from the current validation boundary.
func (s *Gopher) sharedEvictionPathReferenced(ctx context.Context, task *GopherTask, data map[string]string, path string, child bool) (bool, error) {
	if s.modelClient == nil || s.kubeClient == nil {
		return false, fmt.Errorf("shared cleanup requires live Model and node clients")
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	// The operation lock already validated the stored target. Normalize only
	// its ancestors: following the leaf would collapse a child into its parent.
	directory, err := resolveArtifactPath(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	path = filepath.Join(directory, filepath.Base(path))
	owner, err := existingModelEntry(data, key)
	if err != nil {
		return false, err
	}
	parentKey := owner.HfArtifactKey
	if owner.HfArtifactPendingDeletion != nil {
		parentKey = hfArtifactConfigMapKey(owner.HfArtifactPendingDeletion.Identity)
	}
	check := func(reference string) (bool, error) {
		if reference == "" {
			return false, nil
		}
		return artifactReferenceUsesPath(reference, path)
	}
	for otherKey, raw := range data {
		if otherKey == key {
			continue
		}
		if isHfArtifactConfigMapKey(otherKey) {
			parent, err := decodeHfArtifactEntry(otherKey, raw)
			if err != nil {
				return false, err
			}
			for owner, reference := range parent.Children {
				if owner == key {
					continue
				}
				if used, err := check(reference); used || err != nil {
					return used, err
				}
			}
			if !child && otherKey != parentKey {
				if used, err := check(parent.LocalPath); used || err != nil {
					return used, err
				}
			}
			continue
		}
		entry, err := existingModelEntry(data, otherKey)
		if err != nil {
			return false, err
		}
		for _, reference := range persistedModelArtifactReferences(entry, child) {
			if used, err := check(reference); used || err != nil {
				return used, err
			}
		}
	}
	return s.liveArtifactReferenceMatches(ctx, task, data, check)
}

// Pending cleanup retains its original paths even after CR removal or movement.
// Child-link cleanup must not count another receipt's parent as a use of every
// child beneath it. Parent/direct cleanup considers both paths.
func persistedModelArtifactReferences(entry ModelEntry, child bool) []string {
	if completedArtifactEviction(entry) {
		return nil
	}
	references := []string{entry.DirectArtifactPath}
	if receipt := entry.DirectArtifactPendingEviction; receipt != nil {
		references = append(references, receipt.Path)
	}
	if receipt := entry.HfArtifactPendingDeletion; receipt != nil {
		references = append(references, receipt.ChildPath)
		if !child {
			references = append(references, receipt.ParentPath)
		}
	}
	if entry.Config != nil {
		for _, reference := range entry.Config.Artifact.ParentPath {
			references = append(references, reference)
		}
	}
	return references
}

// Legacy source-transition admission keeps its cached-list and reserved-CR
// policy; destructive cleanup additionally uses the live/persisted guards.
func (s *Gopher) hfArtifactHasOtherPathUsers(input hfArtifactTaskInput) (bool, error) {
	namespace, name, cluster, valid := constants.ParseModelInfoFromConfigMapKey(input.ChildModelKey)
	if !valid {
		return false, fmt.Errorf("invalid shared artifact child key %q", input.ChildModelKey)
	}
	root, err := canonicalHfArtifactStoreRoot(input.ModelStoreRoot)
	if err != nil {
		return false, err
	}
	childPath, err := hfArtifactPathInRoot(input.ChildModelPath, input.ModelStoreRoot, root)
	if err != nil {
		return false, err
	}
	// CR paths need not use the canonical spelling stored in the parent index.
	// Compare under the same root without following the child symlink itself.
	matches := func(storage *v1beta1.StorageSpec) bool {
		if storage == nil || storage.Path == nil || *storage.Path == "" {
			return false
		}
		path, err := hfArtifactPathInRoot(filepath.Clean(*storage.Path), input.ModelStoreRoot, root)
		return err == nil && path == childPath
	}
	if s.baseModelLister == nil || s.clusterBaseModelLister == nil {
		return false, fmt.Errorf("model listers are unavailable for shared artifact cleanup")
	}
	models, err := s.baseModelLister.List(labels.Everything())
	if err != nil {
		return false, err
	}
	for _, model := range models {
		if !cluster && model.Namespace == namespace && model.Name == name {
			continue
		}
		// Deleting CRs are not future consumers. Persisted child references
		// still protect their paths until cleanup removes those references.
		if model.DeletionTimestamp != nil && !strings.EqualFold(model.Labels[constants.ReserveModelArtifact], "true") {
			continue
		}
		if matches(model.Spec.Storage) {
			return true, nil
		}
	}
	clusterModels, err := s.clusterBaseModelLister.List(labels.Everything())
	if err != nil {
		return false, err
	}
	for _, model := range clusterModels {
		if cluster && model.Name == name {
			continue
		}
		if model.DeletionTimestamp != nil && !strings.EqualFold(model.Labels[constants.ReserveModelArtifact], "true") {
			continue
		}
		if matches(model.Spec.Storage) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Gopher) liveArtifactReferenceMatches(ctx context.Context, task *GopherTask, data map[string]string, check func(string) (bool, error)) (bool, error) {
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	var node *corev1.Node
	checkModel := func(other *GopherTask) (bool, error) {
		if key == getModelID(other.BaseModel, other.ClusterBaseModel) && taskModelMeta(task).UID == taskModelMeta(other).UID {
			return false, nil
		}
		spec := taskModelSpec(other)
		if spec.Storage == nil {
			return false, nil
		}
		entry, err := existingModelEntry(data, getModelID(other.BaseModel, other.ClusterBaseModel))
		claim := err == nil && (entry.ModelUID == "" || entry.ModelUID == taskModelMeta(other).UID)
		if claim && completedArtifactEviction(entry) && artifactEvictionRequested(other) {
			return false, nil
		}
		if !claim {
			if node == nil {
				node, err = s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
			}
			scout := Scout{nodeInfo: node, logger: s.logger}
			if !scout.shouldDownloadModel(spec.Storage) {
				return false, nil
			}
		}
		reference, err := s.artifactDestination(other)
		if err != nil {
			return false, err
		}
		return check(reference)
	}
	models, err := s.modelClient.OmeV1beta1().BaseModels("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for i := range models.Items {
		if used, err := checkModel(&GopherTask{BaseModel: &models.Items[i]}); used || err != nil {
			return used, err
		}
	}
	clusterModels, err := s.modelClient.OmeV1beta1().ClusterBaseModels().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for i := range clusterModels.Items {
		if used, err := checkModel(&GopherTask{ClusterBaseModel: &clusterModels.Items[i]}); used || err != nil {
			return used, err
		}
	}
	return false, nil
}
