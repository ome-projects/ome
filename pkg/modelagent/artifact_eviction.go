package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/util/retry"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

const artifactResidencyAnnotation = "ome.io/artifact-residency"

// WithArtifactEviction enables live intent checks and pins the Node instance
// observed at agent startup. A replacement Node requires a new agent process.
func WithArtifactEviction(client omeclient.Interface, nodeUID types.UID) GopherOption {
	return func(s *Gopher) {
		s.omeClient, s.artifactNodeUID = client, nodeUID
		s.configMapReconciler.artifactModelClient = client
	}
}

func artifactEvictionRequested(task *GopherTask) bool {
	if task.BaseModel != nil {
		return task.BaseModel.Annotations[artifactResidencyAnnotation] == string(ModelStatusEvicted)
	}
	return task.ClusterBaseModel != nil && task.ClusterBaseModel.Annotations[artifactResidencyAnnotation] == string(ModelStatusEvicted)
}

// Source eligibility is independent of Direct or Shared task routing.
func hasEvictableArtifactSource(task *GopherTask) bool {
	spec := taskModelSpec(task)
	if spec.Storage == nil || spec.Storage.StorageUri == nil {
		return false
	}
	kind, err := storage.GetStorageType(*spec.Storage.StorageUri)
	return err == nil && (kind == storage.StorageTypeOCI || kind == storage.StorageTypeHuggingFace)
}

func liveArtifactModel(ctx context.Context, client omeclient.Interface, expected *GopherTask) (*GopherTask, error) {
	if client == nil {
		return nil, fmt.Errorf("live model client is required for artifact residency")
	}
	task := &GopherTask{}
	var err error
	if expected.ClusterBaseModel != nil {
		task.ClusterBaseModel, err = client.OmeV1beta1().ClusterBaseModels().Get(ctx, expected.ClusterBaseModel.Name, metav1.GetOptions{})
	} else if expected.BaseModel != nil {
		task.BaseModel, err = client.OmeV1beta1().BaseModels(expected.BaseModel.Namespace).Get(ctx, expected.BaseModel.Name, metav1.GetOptions{})
	} else {
		return nil, fmt.Errorf("artifact task has no model")
	}
	return task, err
}

// Check actual publication and task entry, including tasks queued before the
// annotation event. The informer snapshot alone cannot authorize Ready.
func (s *Gopher) validateArtifactDownload(ctx context.Context, task *GopherTask) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.omeClient == nil {
		if artifactRehydrationID(task) != "" {
			return fmt.Errorf("live model client is required for artifact restoration")
		}
		return nil
	}
	current, err := liveArtifactModel(ctx, s.omeClient, task)
	if err != nil {
		return err
	}
	if getModelUID(current) != getModelUID(task) || artifactEvictionRequested(current) || isModelResourceDeleting(current.BaseModel, current.ClusterBaseModel) {
		return fmt.Errorf("model is evicted, deleting, or replaced; artifact publication is blocked")
	}
	request := artifactRehydrationID(task)
	if request != artifactRehydrationID(current) {
		return fmt.Errorf("artifact restoration request changed")
	}
	if request == "" {
		return nil
	}
	if len(validation.IsValidLabelValue(request)) != 0 {
		return fmt.Errorf("artifact restoration request must be a valid Kubernetes label value")
	}
	if getModelUID(task) == "" || s.configMapReconciler.isModelMutationBlocked(getModelID(task.BaseModel, task.ClusterBaseModel), types.UID(getModelUID(task))) {
		return fmt.Errorf("artifact restoration model UID is absent or superseded")
	}
	if !reflect.DeepEqual(downloadOverrideInputsFromSpec(taskModelSpec(task)), downloadOverrideInputsFromSpec(taskModelSpec(current))) {
		return fmt.Errorf("artifact restoration source, path, policy, or placement changed")
	}
	if task.BaseModel != nil {
		if !reflect.DeepEqual(task.BaseModel.Labels, current.BaseModel.Labels) || !reflect.DeepEqual(task.BaseModel.Annotations, current.BaseModel.Annotations) {
			return fmt.Errorf("artifact restoration metadata changed")
		}
	} else if !reflect.DeepEqual(task.ClusterBaseModel.Labels, current.ClusterBaseModel.Labels) || !reflect.DeepEqual(task.ClusterBaseModel.Annotations, current.ClusterBaseModel.Annotations) {
		return fmt.Errorf("artifact restoration metadata changed")
	}
	if s.kubeClient == nil || s.artifactNodeUID == "" {
		return fmt.Errorf("startup Node identity is required for artifact restoration")
	}
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.UID != s.artifactNodeUID || node.DeletionTimestamp != nil || !(&Scout{nodeInfo: node, logger: s.logger}).shouldDownloadModel(taskModelSpec(current).Storage) {
		return fmt.Errorf("artifact restoration Node identity or eligibility changed")
	}
	return nil
}

// Shared repair/recovery and cache reconstruction also publish child Ready
// through ConfigMap CAS, independently of the dispatcher completion path.
func (c *ConfigMapReconciler) validateArtifactReadyEntries(ctx context.Context, before, after map[string]string, modelID string) error {
	if c.artifactModelClient == nil {
		return nil
	}
	var models map[string]*GopherTask
	for key, raw := range after {
		if before[key] == raw || isHfArtifactConfigMapKey(key) {
			continue
		}
		var entry ModelEntry
		if json.Unmarshal([]byte(raw), &entry) != nil || entry.Status != ModelStatusReady {
			continue
		}
		if models == nil {
			var err error
			models, err = liveArtifactModels(ctx, c.artifactModelClient)
			if err != nil {
				return err
			}
		}
		current := models[key]
		requestUnproven := false
		if current != nil && artifactRehydrationID(current) != "" {
			guard, _ := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard)
			requestUnproven = guard == nil || !guard.validated || getModelID(guard.task.BaseModel, guard.task.ClusterBaseModel) != key || entry.ModelUID != types.UID(getModelUID(current)) || entry.ArtifactRehydrationID != artifactRehydrationID(current) || entry.NodeUID != guard.gopher.artifactNodeUID
		}
		if requestUnproven || current == nil || artifactEvictionRequested(current) || isModelResourceDeleting(current.BaseModel, current.ClusterBaseModel) {
			if key == modelID && !requestUnproven {
				return fmt.Errorf("model %s cannot publish Ready while evicted, deleting, or absent", key)
			}
			// Cache restoration and sibling repair must not republish Ready or
			// prevent unrelated cleanup from making progress.
			entry.Status, entry.Progress = ModelStatusUpdating, nil
			if _, err := writeModelEntry(after, key, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// Carries this task's cleanup checks into ConfigMap updates, so each retry
// rechecks live intent and Model/Node identity.
type artifactCleanupGuard struct {
	gopher *Gopher
	task   *GopherTask
}
type artifactCleanupGuardKey struct{}

func validateArtifactCleanup(ctx context.Context) error {
	guard, _ := ctx.Value(artifactCleanupGuardKey{}).(*artifactCleanupGuard)
	if guard == nil {
		return nil
	}
	return guard.validate(ctx)
}

func (g *artifactCleanupGuard) validate(ctx context.Context) error {
	s, task := g.gopher, g.task
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := liveArtifactModel(ctx, s.omeClient, task)
	if err != nil {
		return err
	}
	if getModelUID(task) == "" || getModelUID(task) != getModelUID(current) || !artifactEvictionRequested(current) || isModelResourceDeleting(current.BaseModel, current.ClusterBaseModel) {
		return fmt.Errorf("eviction model UID or intent changed")
	}
	if !hasEvictableArtifactSource(current) {
		return fmt.Errorf("artifact eviction requires a current Hugging Face or OCI source")
	}
	if s.configMapReconciler.isModelMutationBlocked(getModelID(task.BaseModel, task.ClusterBaseModel), types.UID(getModelUID(task))) {
		return fmt.Errorf("eviction model UID is superseded in node state")
	}
	if !reflect.DeepEqual(downloadOverrideInputsFromSpec(taskModelSpec(task)), downloadOverrideInputsFromSpec(taskModelSpec(current))) {
		return fmt.Errorf("eviction source or download inputs changed")
	}
	if task.BaseModel != nil {
		if !reflect.DeepEqual(task.BaseModel.Labels, current.BaseModel.Labels) || !reflect.DeepEqual(task.BaseModel.Annotations, current.BaseModel.Annotations) {
			return fmt.Errorf("eviction model metadata changed")
		}
	} else if !reflect.DeepEqual(task.ClusterBaseModel.Labels, current.ClusterBaseModel.Labels) || !reflect.DeepEqual(task.ClusterBaseModel.Annotations, current.ClusterBaseModel.Annotations) {
		return fmt.Errorf("eviction model metadata changed")
	}
	if s.kubeClient == nil || s.artifactNodeUID == "" {
		return fmt.Errorf("startup Node identity is required for eviction")
	}
	node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.UID != s.artifactNodeUID {
		return fmt.Errorf("eviction Node identity changed")
	}
	return nil
}

func (g *artifactCleanupGuard) withdrawReady(ctx context.Context) error {
	s := g.gopher
	s.configMapMutex.Lock()
	defer s.configMapMutex.Unlock()
	key, err := getModelLabelKey(&NodeLabelOp{BaseModel: g.task.BaseModel, ClusterBaseModel: g.task.ClusterBaseModel})
	if err != nil {
		return err
	}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := g.validate(ctx); err != nil {
			return err
		}
		node, err := s.kubeClient.CoreV1().Nodes().Get(ctx, s.configMapReconciler.nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if node.UID != s.artifactNodeUID {
			return fmt.Errorf("eviction Node identity changed before readiness withdrawal")
		}
		if _, present := node.Labels[key]; !present {
			return nil
		}
		patch, err := json.Marshal([]patchStringValue{
			{Op: "test", Path: "/metadata/uid", Value: string(s.artifactNodeUID)},
			{Op: "test", Path: "/metadata/resourceVersion", Value: node.ResourceVersion},
			{Op: "remove", Path: "/metadata/labels/" + strings.ReplaceAll(key, "/", "~1")},
		})
		if err != nil {
			return err
		}
		_, err = s.kubeClient.CoreV1().Nodes().Patch(ctx, node.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
		return err // Missing Node and rejected patches are never success.
	})
	if err != nil {
		return err
	}
	return nil
}

// Returns true when a retry was queued and the caller must keep the delete barrier.
func (s *Gopher) processArtifactEviction(ctx context.Context, task *GopherTask) (bool, error) {
	guard := &artifactCleanupGuard{gopher: s, task: task}
	ctx = context.WithValue(ctx, artifactCleanupGuardKey{}, guard)
	if err := guard.validate(ctx); err != nil {
		return false, err
	}
	h := s.sharedHfArtifactHandler()
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	cm, err := s.configMapReconciler.getConfigMapForRecovery(ctx)
	if err != nil {
		return false, err
	}
	entry, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return false, err
	}
	if entry.Status == ModelStatusEvicted && entry.ModelUID == types.UID(getModelUID(task)) && entry.HfArtifactKey == "" && entry.HfArtifactPendingDeletion == nil {
		return false, guard.withdrawReady(ctx)
	}
	parent, found, err := h.repository.GetParentForChild(ctx, key)
	if err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("eviction requires persisted Shared ownership for %s", key)
	}
	input := s.hfArtifactInputForChild(task, parent)
	if pending := entry.HfArtifactPendingDeletion; pending != nil && pending.ModelUID != input.ChildModelUID {
		return false, fmt.Errorf("eviction pending cleanup belongs to another model UID")
	}
	// Shared eviction also takes the path lock used by Direct writers: a Direct
	// task may target this same parent directory. Hold it through cleanup and
	// acknowledgement so deletion cannot race that writer. This does not replace
	// the Shared artifact locks acquired below by handleDelete.
	acquired, err := s.tryLockDirectModelPath(ctx, parent.LocalPath)
	if err != nil {
		return false, err
	}
	if !acquired {
		err := s.requeueTaskOnWait(task, gopherTaskWait)
		return err == nil, err
	}
	if err := s.hfArtifactStartup.recoverParentAtPath(ctx, parent.Key, parent.LocalPath); err != nil {
		err := s.requeueHfArtifactTask(task, newHfArtifactRetryResult(parent.Key, err))
		return err == nil, err
	}
	input.beforeDelete = func(ctx context.Context) error {
		if err := guard.withdrawReady(ctx); err != nil {
			return err
		}
		return s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStatus: ModelStatusUpdating})
	}
	input.pathUsers = func(ctx context.Context, target string) (bool, error) {
		return s.sharedArtifactPathUsers(ctx, input, target, false)
	}
	input.completeDeletion = func(ctx context.Context, expected HfArtifactPendingDeletion) error {
		return h.repository.configMaps.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
			child, err := existingModelEntry(cm.Data, key)
			if err != nil {
				return false, err
			}
			if child.HfArtifactKey != "" || child.HfArtifactPendingDeletion == nil || *child.HfArtifactPendingDeletion != expected {
				return false, fmt.Errorf("eviction cleanup receipt changed")
			}
			child.HfArtifactPendingDeletion = nil
			child.Status, child.Progress, child.ModelUID = ModelStatusEvicted, nil, expected.ModelUID
			if child.Config != nil {
				child.Config.Artifact = Artifact{}
			}
			return writeModelEntry(cm.Data, key, child)
		})
	}
	result, err := h.handleDelete(ctx, input)
	if err != nil {
		return false, err
	}
	if result.Outcome == hfArtifactTaskRetry {
		err := s.requeueHfArtifactTask(task, result)
		return err == nil, err
	}
	return false, nil
}

// Combine live source paths with persisted ownership, including old paths in
// cleanup receipts. Canonicalize spelling but retain the leaf symlink itself.
func (s *Gopher) sharedArtifactPathUsers(ctx context.Context, input hfArtifactTaskInput, target string, repair bool) (bool, error) {
	return s.artifactPathUsers(ctx, input.ChildModelKey, input.ChildModelPath, input.ModelStoreRoot, target, &input.Parent, repair)
}

// The supplied Shared parent excludes its own relationship from borrower detection.
func (s *Gopher) artifactPathUsers(ctx context.Context, childKey, childPath, modelRoot, target string, sharedParent *HfArtifactEntry, repair bool) (bool, error) {
	input := hfArtifactTaskInput{ChildModelKey: childKey, ChildModelPath: childPath, ModelStoreRoot: modelRoot, Parent: *sharedParent}
	if err := validateArtifactRestore(ctx); err != nil {
		return false, err
	}
	if err := validateArtifactCleanup(ctx); err != nil {
		return false, err
	}
	root, err := canonicalHfArtifactStoreRoot(input.ModelStoreRoot)
	if err != nil {
		return false, err
	}
	target, err = hfArtifactPathInRoot(target, input.ModelStoreRoot, root)
	if err != nil {
		return false, err
	}
	protectedPaths := []string{target}
	matches := func(path string) (bool, error) {
		if path == "" {
			return false, nil
		}
		path, err := filepath.Abs(path)
		if err != nil {
			return false, err
		}
		// Resolve ancestor aliases, including Local paths via an existing
		// Shared child; also compare the un-followed leaf for child unlink.
		paths := []string{filepath.Clean(path)}
		if resolved, err := filepath.EvalSymlinks(path); err == nil {
			paths = append(paths, resolved)
		}
		if linked, err := readChildSymlinkTarget(path); err == nil {
			paths = append(paths, linked)
		}
		if canonical, err := hfArtifactPathInRoot(path, input.ModelStoreRoot, root); err == nil {
			paths = append(paths, canonical)
		}
		for _, path := range paths {
			for _, protected := range protectedPaths {
				if hfArtifactInputPathWithin(protected, path) || hfArtifactInputPathWithin(path, protected) {
					return true, nil
				}
			}
		}
		return false, nil
	}
	modelUses := func(spec v1beta1.BaseModelSpec) (bool, error) {
		if spec.Storage == nil || spec.Storage.StorageUri == nil {
			return false, nil
		}
		path := getDestPath(&spec, s.modelRootDir)
		if (spec.Storage.Path == nil || *spec.Storage.Path == "") && strings.HasPrefix(*spec.Storage.StorageUri, storage.LocalStoragePrefix) {
			local, err := storage.ParseLocalStorageURI(*spec.Storage.StorageUri)
			if err != nil {
				return false, err
			}
			path = local.Path
		}
		return matches(path)
	}
	models, err := liveArtifactModels(ctx, s.omeClient)
	if err != nil {
		return false, err
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return false, err
	}
	// Only matching managed siblings are excluded: repair withdraws their Ready
	// status before replacing files. Other path users still block repair.
	repairChildren := make(map[string]bool)
	ignoredLinks := []string{input.ChildModelPath}
	if repair {
		protectedChildren := []string{input.ChildModelPath}
		if raw, found := cm.Data[input.Parent.Key]; found {
			parent, err := decodeHfArtifactEntry(input.Parent.Key, raw)
			if err != nil {
				return false, err
			}
			if parent.LocalPath != input.Parent.LocalPath {
				return false, fmt.Errorf("Shared repair parent path changed")
			}
			for key, path := range parent.Children {
				protectedChildren = append(protectedChildren, path)
				model := models[key]
				if model == nil || key == input.ChildModelKey {
					continue
				}
				entry, err := existingModelEntry(cm.Data, key)
				if err != nil {
					return false, err
				}
				if entry.HfArtifactKey != parent.Key || entry.ModelUID == "" || entry.ModelUID != types.UID(getModelUID(model)) || entry.HfArtifactPendingDeletion != nil || isModelResourceDeleting(model.BaseModel, model.ClusterBaseModel) {
					continue
				}
				probe := *model
				probe.TaskType = Download
				candidate, eligible, err := newHfArtifactTaskInputForOCI(&probe, taskModelSpec(model).Storage, s.modelRootDir)
				if err != nil {
					return false, err
				}
				if !eligible && isDirectHfReuseEligible(&probe, taskModelSpec(model).Storage) {
					components, err := storage.ParseHuggingFaceStorageURI(*taskModelSpec(model).Storage.StorageUri)
					if err != nil {
						return false, err
					}
					guard, _ := ctx.Value(artifactRestoreGuardKey{}).(*artifactRestoreGuard)
					sameSource := guard != nil && *taskModelSpec(model).Storage.StorageUri == *taskModelSpec(guard.task).Storage.StorageUri
					if components.ModelID == parent.Identity.ModelID && (components.Branch == parent.Identity.CommitSHA || sameSource) {
						candidate, eligible, err = newHfArtifactTaskInput(&probe, taskModelSpec(model).Storage, s.modelRootDir, parent.Identity)
						if err != nil {
							return false, err
						}
					}
				}
				if !eligible || candidate.Parent.Key != parent.Key || candidate.ChildModelPath != path {
					continue
				}
				repairChildren[key] = true
				ignoredLinks = append(ignoredLinks, path)
			}
		}
		// Borrowers can name missing descendants through managed links. Full
		// symlink resolution cannot prove that use, so retain both spellings
		// of every tracked child for the live and persisted path comparisons.
		for _, path := range protectedChildren {
			canonical, err := hfArtifactPathInRoot(path, input.ModelStoreRoot, root)
			if err != nil {
				return false, err
			}
			protectedPaths = append(protectedPaths, filepath.Clean(path), canonical)
		}
	}
	for key, model := range models {
		if key == input.ChildModelKey || repairChildren[key] {
			continue
		}
		if used, err := modelUses(taskModelSpec(model)); used || err != nil {
			return used, err
		}
	}
	for key, raw := range cm.Data {
		if key == input.ChildModelKey || repairChildren[key] {
			continue
		}
		var paths []string
		if isHfArtifactConfigMapKey(key) {
			parent, err := decodeHfArtifactEntry(key, raw)
			if err != nil {
				return false, err
			}
			if key != input.Parent.Key {
				paths = append(paths, parent.LocalPath)
			}
			for child, path := range parent.Children {
				if child != input.ChildModelKey && !repairChildren[child] {
					paths = append(paths, path)
				}
			}
		} else {
			entry, err := existingModelEntry(cm.Data, key)
			if err != nil {
				return false, err
			}
			if pending := entry.HfArtifactPendingDeletion; pending != nil {
				paths = append(paths, pending.ChildPath, pending.ParentPath)
			}
			if entry.Config != nil {
				for _, path := range entry.Config.Artifact.ParentPath {
					paths = append(paths, path)
				}
				paths = append(paths, entry.Config.Artifact.ChildrenPaths...)
			}
		}
		for _, path := range paths {
			if used, err := matches(path); used || err != nil {
				return used, err
			}
		}
	}
	if repair {
		return (hfArtifactFiles{}).HasChildren(target, input.ModelStoreRoot, ignoredLinks...)
	}
	return false, nil
}

// Keys can contain truncated names and namespaces. Resolve key-only ownership
// from current objects using the forward key function, never reverse a key.
func liveArtifactModels(ctx context.Context, client omeclient.Interface) (map[string]*GopherTask, error) {
	models, err := client.OmeV1beta1().BaseModels("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	clusters, err := client.OmeV1beta1().ClusterBaseModels().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := make(map[string]*GopherTask, len(models.Items)+len(clusters.Items))
	for i := range models.Items {
		model := &models.Items[i]
		result[getModelID(model, nil)] = &GopherTask{BaseModel: model}
	}
	for i := range clusters.Items {
		model := &clusters.Items[i]
		result[getModelID(nil, model)] = &GopherTask{ClusterBaseModel: model}
	}
	return result, nil
}
