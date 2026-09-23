package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

func modelEvictionRequested(meta *metav1.ObjectMeta) bool {
	return meta != nil && meta.Annotations[constants.ModelArtifactResidencyAnnotation] == constants.ModelArtifactResidencyEvicted
}

// Intent alone must not suppress downloads when eviction fails preflight.
// The task tracker fences admitted cleanup; the persisted state additionally
// prevents late downloads from restoring a completed eviction. Unreadable state
// remains blocked until it can be checked safely.
func (s *Gopher) shouldBlockDownloadForEviction(ctx context.Context, task *GopherTask) (bool, error) {
	if s.configMapReconciler == nil {
		return true, fmt.Errorf("cannot check eviction state without a ConfigMap reconciler")
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return true, fmt.Errorf("cannot check completed eviction for %s: %w", getModelInfoForLogging(task), err)
	}
	raw := cm.Data[getModelID(task.BaseModel, task.ClusterBaseModel)]
	if raw == "" {
		return false, nil
	}
	var entry ModelEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return true, fmt.Errorf("cannot decode eviction state for %s: %w", getModelInfoForLogging(task), err)
	}
	return entry.Status == ModelStatusEvicted, nil
}

func taskModelMeta(task *GopherTask) *metav1.ObjectMeta {
	if task.BaseModel != nil {
		return &task.BaseModel.ObjectMeta
	}
	if task.ClusterBaseModel != nil {
		return &task.ClusterBaseModel.ObjectMeta
	}
	return nil
}

func downloadAnnotations(annotations map[string]string) map[string]string {
	copy := maps.Clone(annotations)
	delete(copy, constants.ModelArtifactResidencyAnnotation)
	if len(copy) == 0 {
		return nil
	}
	return copy
}

// latestEvictionTask uses the API, not the informer, before destructive work.
// A deleted/replaced CR or withdrawn request makes a queued eviction obsolete.
func (s *Gopher) latestEvictionTask(ctx context.Context, task *GopherTask) (*GopherTask, error) {
	if task == nil || taskModelMeta(task) == nil || taskModelMeta(task).UID == "" {
		return nil, fmt.Errorf("artifact eviction requires a model UID")
	}
	if s.modelClient == nil {
		return nil, fmt.Errorf("artifact eviction requires a live model client")
	}
	latest := *task
	var err error
	if task.BaseModel != nil {
		latest.BaseModel, err = s.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(ctx, task.BaseModel.Name, metav1.GetOptions{})
	} else {
		latest.ClusterBaseModel, err = s.modelClient.OmeV1beta1().ClusterBaseModels().Get(ctx, task.ClusterBaseModel.Name, metav1.GetOptions{})
	}
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	meta := taskModelMeta(&latest)
	if meta.UID != taskModelMeta(task).UID || !meta.DeletionTimestamp.IsZero() || !modelEvictionRequested(meta) {
		return nil, nil
	}
	return &latest, nil
}

// modelHasConsumers deliberately includes Pending, scale-to-zero and terminating
// services. Pods also protect consumers whose service was already deleted.
func (s *Gopher) modelHasConsumers(ctx context.Context, task *GopherTask, path string) (bool, error) {
	services, err := s.modelClient.OmeV1beta1().InferenceServices(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	meta := taskModelMeta(task)
	for _, service := range services.Items {
		if meta.Namespace != "" && service.Namespace != meta.Namespace {
			continue
		}
		// Conservatively protect same-name cluster models even when shadowed by
		// a namespaced model. Missing reference resolution must never permit eviction.
		if service.Annotations[constants.BaseModelName] == meta.Name {
			return true, nil
		}
		if service.Spec.Model == nil {
			continue
		}
		if service.Spec.Model.Name == meta.Name {
			return true, nil
		}
		for _, overlay := range service.Spec.Model.Overlays {
			if overlay.Name == meta.Name {
				return true, nil
			}
		}
	}
	key, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel})
	if err != nil {
		return false, err
	}
	return s.pathHasPodConsumers(ctx, path, key)
}

func (s *Gopher) pathHasPodConsumers(ctx context.Context, path, modelLabel string) (bool, error) {
	return s.pathHasPodConsumersOnNode(ctx, path, modelLabel, "")
}

// Repair only protects bound consumers on this node; unscheduled demand and
// Pods on other nodes must not prevent this node from restoring its own copy.
func (s *Gopher) pathHasLocalPodConsumers(ctx context.Context, path string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.configMapReconciler == nil || s.configMapReconciler.nodeName == "" {
		return false, fmt.Errorf("artifact repair requires node identity")
	}
	path, err := canonicalModelReferencePath(path)
	if err != nil {
		return false, err
	}
	used, err := s.pathHasPodConsumersOnNode(ctx, path, "", s.configMapReconciler.nodeName)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return used, err
}

func (s *Gopher) pathHasPodConsumersOnNode(ctx context.Context, path, modelLabel, nodeName string) (bool, error) {
	if s.kubeClient == nil {
		return false, fmt.Errorf("artifact cleanup requires a live pod client")
	}
	options := metav1.ListOptions{}
	if nodeName != "" {
		options.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", nodeName).String()
	}
	pods, err := s.kubeClient.CoreV1().Pods(metav1.NamespaceAll).List(ctx, options)
	if err != nil {
		return false, err
	}
	for _, pod := range pods.Items {
		if nodeName != "" && pod.Spec.NodeName != nodeName {
			continue
		}
		if pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" {
			continue
		}
		if _, uses := pod.Spec.NodeSelector[modelLabel]; modelLabel != "" && uses {
			return true, nil
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.HostPath != nil {
				// Broad root mounts (including model-agent's own volume) are not
				// serving references to every artifact in the store.
				used, err := pathReferencesArtifact(volume.HostPath.Path, path, false)
				if err != nil || used {
					return used, err
				}
				for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
					for _, mount := range container.VolumeMounts {
						if mount.Name != volume.Name {
							continue
						}
						// The final subpath may depend on runtime environment values.
						// An overlapping dynamic mount cannot be proven unrelated.
						if mount.SubPathExpr != "" {
							used, err := pathReferencesArtifact(volume.HostPath.Path, path, true)
							if err != nil || used {
								return used, err
							}
						}
						if mount.SubPath != "" {
							subPath := volume.HostPath.Path + string(filepath.Separator) + mount.SubPath
							used, err := pathReferencesArtifact(subPath, path, true)
							if err != nil || used {
								return used, err
							}
						}
					}
				}
			}
		}
	}
	return false, nil
}

func modelPathsOverlap(a, b string) bool {
	a, b = filepath.Clean(a), filepath.Clean(b)
	separator := string(filepath.Separator)
	return a == b || strings.HasPrefix(a, strings.TrimSuffix(b, separator)+separator) ||
		strings.HasPrefix(b, strings.TrimSuffix(a, separator)+separator)
}

// Resolve directory aliases for ownership comparisons without following the
// leaf: different child symlinks to one shared parent are still separate paths.
func canonicalModelReferencePath(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	for ancestor := filepath.Dir(path); ; ancestor = filepath.Dir(ancestor) {
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			relative, _ := filepath.Rel(ancestor, path)
			return filepath.Join(resolved, relative), nil
		}
		if !os.IsNotExist(err) || filepath.Dir(ancestor) == ancestor {
			return "", err
		}
	}
}

// Check every traversed symlink, not just the resolved destination: unlinking
// child breaks child/subdir and external-alias -> child even if parent survives.
// Target's ancestors are canonical, but its leaf is not resolved, so independent
// sibling links remain independent.
// allowAncestor also protects a containing directory; bare hostPath mounts omit
// that check so model-agent's own store-root mount does not block all eviction.
func pathReferencesArtifact(reference, target string, allowAncestor bool) (bool, error) {
	if !filepath.IsAbs(reference) {
		cwd, err := os.Getwd()
		if err != nil {
			return false, err
		}
		reference = cwd + string(filepath.Separator) + reference
	}
	pending := strings.Split(reference, string(filepath.Separator))
	resolved := string(filepath.Separator)
	links := 0
	for len(pending) != 0 {
		part := pending[0]
		pending = pending[1:]
		switch part {
		case "", ".":
			continue
		case "..":
			resolved = filepath.Dir(resolved)
			continue
		}
		next := filepath.Join(resolved, part)
		if next == target {
			return true, nil
		}
		info, err := os.Lstat(next)
		if os.IsNotExist(err) {
			resolved = filepath.Join(append([]string{next}, pending...)...)
			break
		}
		if err != nil {
			return false, err
		}
		if info.Mode()&os.ModeSymlink == 0 {
			resolved = next
			continue
		}
		links++
		if links > 255 {
			return false, fmt.Errorf("too many symlinks in model reference %s", reference)
		}
		link, err := os.Readlink(next)
		if err != nil {
			return false, err
		}
		if filepath.IsAbs(link) {
			resolved = string(filepath.Separator)
		}
		pending = append(strings.Split(link, string(filepath.Separator)), pending...)
	}
	if allowAncestor {
		return modelPathsOverlap(resolved, target), nil
	}
	return resolved == target || strings.HasPrefix(resolved, target+string(filepath.Separator)), nil
}

// Local models may omit storage.path and use the local:// URI directly.
// Preserve the original spelling so consumer checks can inspect symlink dependencies.
func modelPathForEvictionProtection(spec *v1beta1.BaseModelSpec, root string) (string, error) {
	if spec.Storage == nil {
		return "", nil
	}
	if spec.Storage.Path != nil && *spec.Storage.Path != "" {
		return *spec.Storage.Path, nil
	}
	if spec.Storage.StorageUri != nil && strings.HasPrefix(*spec.Storage.StorageUri, storage.LocalStoragePrefix) {
		local, err := storage.ParseLocalStorageURI(*spec.Storage.StorageUri)
		if err != nil {
			return "", err
		}
		return local.Path, nil
	}
	if spec.Storage.Path == nil || spec.Storage.StorageUri == nil {
		return "", nil
	}
	return getDestPath(spec, root), nil
}

// Reject ambiguous ordinary-directory ownership, including nested model paths.
// Shared parents are handled separately by the reference-aware deletion path.
func (s *Gopher) evictionPathHasOtherModels(ctx context.Context, task *GopherTask, path string) (bool, error) {
	if s.modelClient == nil {
		return false, fmt.Errorf("artifact cleanup requires a live model client")
	}
	models, err := s.modelClient.OmeV1beta1().BaseModels(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, model := range models.Items {
		if task.BaseModel != nil && model.UID == task.BaseModel.UID {
			continue
		}
		otherPath, err := modelPathForEvictionProtection(&model.Spec, s.modelRootDir)
		if err != nil {
			return false, err
		}
		if otherPath != "" {
			used, err := pathReferencesArtifact(otherPath, path, true)
			if err != nil || used {
				return used, err
			}
		}
	}
	clusterModels, err := s.modelClient.OmeV1beta1().ClusterBaseModels().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, model := range clusterModels.Items {
		if task.ClusterBaseModel != nil && model.UID == task.ClusterBaseModel.UID {
			continue
		}
		otherPath, err := modelPathForEvictionProtection(&model.Spec, s.modelRootDir)
		if err != nil {
			return false, err
		}
		if otherPath != "" {
			used, err := pathReferencesArtifact(otherPath, path, true)
			if err != nil || used {
				return used, err
			}
		}
	}
	return false, nil
}

// Called under the parent operation lock, after child unlink. A parent may be
// mounted directly or referenced by a CR outside the shared-child index.
func (s *Gopher) hfArtifactParentHasConsumers(ctx context.Context, parent HfArtifactEntry) (bool, error) {
	path, err := canonicalModelReferencePath(parent.LocalPath)
	if err != nil {
		return false, err
	}
	used, err := s.pathHasPodConsumers(ctx, path, "")
	if err != nil || used {
		return used, err
	}
	return s.evictionPathHasOtherModels(ctx, &GopherTask{}, path)
}

// safeArtifactEvictionPath permits only child paths under the configured root.
// Symlinked ancestor directories and direct shared-parent paths fail closed.
func safeArtifactEvictionPath(root, path string) (string, error) {
	configuredRoot := root
	root, err := canonicalHfArtifactStoreRoot(root)
	if err != nil {
		return "", err
	}
	// Validate the original spelling: cleaning symlink/../model can select a
	// different directory from the one the filesystem path actually references.
	path, err = hfArtifactPathInRoot(path, configuredRoot, root)
	if err != nil {
		return "", err
	}
	if path == root {
		return "", fmt.Errorf("refusing eviction of the model store root")
	}
	rel, _ := filepath.Rel(root, path)
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == constants.ModelArtifactsDirectory || part == hfArtifactLockDirectory || part == directArtifactStagingDirectory {
			return "", fmt.Errorf("refusing eviction of reserved artifact path %s", path)
		}
	}
	if err := validateHfArtifactPathAncestors(root, path, false); err != nil {
		return "", err
	}
	return path, nil
}

// Shared cleanup follows persisted paths, including pending deletion receipts.
// Validate that ownership before allowing eviction of the current CR path.
func (s *Gopher) validateArtifactEvictionReferences(ctx context.Context, task *GopherTask, path string) error {
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return err
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	if raw := cm.Data[key]; raw != "" {
		var entry ModelEntry
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			return fmt.Errorf("read eviction ownership: %w", err)
		}
		if entry.ModelUID != "" && entry.ModelUID != taskModelMeta(task).UID {
			return fmt.Errorf("eviction entry belongs to another model UID")
		}
		if pending := entry.ArtifactPendingEviction; pending != nil && (pending.ModelUID == "" || pending.ModelUID != taskModelMeta(task).UID || pending.Path != path) {
			return fmt.Errorf("direct eviction receipt has unknown or different UID/path")
		}
		if entry.Config != nil && len(entry.Config.Artifact.ChildrenPaths) != 0 {
			return fmt.Errorf("model path %s is a legacy parent with child references", path)
		}
		parent, found, err := parentForChildEntry(cm.Data, key, entry)
		if err != nil {
			return err
		}
		if found {
			storedPath, err := safeArtifactEvictionPath(s.modelRootDir, parent.Children[key])
			if err != nil {
				return err
			}
			if storedPath != path {
				return fmt.Errorf("stored child path %s differs from eviction path %s", storedPath, path)
			}
			if _, _, err := validateHfArtifactParentPath(parent, s.modelRootDir); err != nil {
				return err
			}
		}
	}
	// Older direct-HF parents have no synthetic entry. Preserve unrecorded links.
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || !info.IsDir() {
		return err
	}
	hasChildren, err := (hfArtifactFiles{}).HasChildren(path, s.modelRootDir)
	if err != nil {
		return err
	}
	if hasChildren {
		return fmt.Errorf("model path %s still has child symlinks", path)
	}
	return nil
}

// Check eviction eligibility before cancelling downloads, then repeat after
// acquiring the task barrier because demand or model intent may have changed.
func (s *Gopher) prepareArtifactEviction(ctx context.Context, task *GopherTask) (*GopherTask, string, error) {
	latest, err := s.latestEvictionTask(ctx, task)
	if err != nil {
		return nil, "", err
	}
	if latest == nil {
		return nil, "", nil
	}
	spec := taskModelSpec(latest)
	if spec.Distribution != nil && *spec.Distribution != v1beta1.DistributionPerNode {
		return nil, "", fmt.Errorf("artifact eviction supports only PerNode models")
	}
	if spec.Storage == nil || spec.Storage.StorageUri == nil || spec.Storage.Path == nil || strings.TrimSpace(*spec.Storage.Path) == "" {
		return nil, "", fmt.Errorf("artifact eviction requires an explicit model storage URI and child path")
	}
	kind, err := storage.GetStorageType(*spec.Storage.StorageUri)
	if err != nil || (kind != storage.StorageTypeOCI && kind != storage.StorageTypeHuggingFace) {
		return nil, "", fmt.Errorf("artifact eviction supports only OCI and Hugging Face artifacts")
	}
	path, err := safeArtifactEvictionPath(s.modelRootDir, getDestPath(&spec, s.modelRootDir))
	if err != nil {
		return nil, "", err
	}
	used, err := s.modelHasConsumers(ctx, latest, path)
	if err != nil {
		return nil, "", err
	}
	if used {
		// Endpoint orchestration owns releasing intent after recording demand;
		// the agent only observes it and protects active consumers.
		s.logger.Infof("Skipping eviction of %s: active consumer", getModelID(latest.BaseModel, latest.ClusterBaseModel))
		return nil, "", nil
	}
	if s.isReservingModelArtifact(latest) {
		return nil, "", fmt.Errorf("model artifact is reserved")
	}
	sharedPath, err := s.evictionPathHasOtherModels(ctx, latest, path)
	if err != nil {
		return nil, "", err
	}
	if sharedPath {
		return nil, "", fmt.Errorf("model path is used by another Model CR")
	}
	if err := s.validateArtifactEvictionReferences(ctx, latest, path); err != nil {
		return nil, "", err
	}
	return latest, path, nil
}

func (s *Gopher) processArtifactEviction(ctx context.Context, task *GopherTask) (bool, error) {
	latest, path, err := s.prepareArtifactEviction(ctx, task)
	if err != nil {
		return s.retryArtifactEviction(task, err)
	}
	if latest == nil {
		return false, nil
	}
	spec := taskModelSpec(latest)
	// Replays, including agent restart, repair Evicted without publishing Updating.
	// Pending shared cleanup must still run to completion.
	if _, statErr := os.Lstat(path); os.IsNotExist(statErr) {
		cm, err := s.configMapReconciler.getConfigMap(ctx)
		if err != nil && !apierrors.IsNotFound(err) {
			return s.retryArtifactEviction(task, err)
		}
		var entry ModelEntry
		if cm != nil && json.Unmarshal([]byte(cm.Data[getModelID(latest.BaseModel, latest.ClusterBaseModel)]), &entry) == nil && entry.Status == ModelStatusEvicted && entry.HfArtifactKey == "" && entry.HfArtifactPendingDeletion == nil && entry.ArtifactPendingEviction == nil {
			release, acquired, err := s.acquireDirectArtifactPath(latest)
			if err != nil {
				return s.retryArtifactEviction(task, err)
			}
			if !acquired {
				return s.retryArtifactEviction(task, fmt.Errorf("direct artifact path is busy"))
			}
			defer release()
			current, currentPath, err := s.prepareArtifactEviction(ctx, latest)
			if err != nil {
				return s.retryArtifactEviction(task, err)
			}
			if current == nil {
				return false, nil
			}
			if currentPath != path || !reflect.DeepEqual(spec, taskModelSpec(current)) {
				return s.retryArtifactEviction(task, fmt.Errorf("model changed before eviction replay"))
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				return s.retryArtifactEviction(task, fmt.Errorf("direct artifact reappeared before eviction replay"))
			}
			return false, s.reconcileDirectArtifactStatus(ctx, current, Evicted)
		}
	}
	latest, err = s.latestEvictionTask(ctx, latest)
	if err != nil {
		return s.retryArtifactEviction(task, err)
	}
	if latest == nil {
		return false, nil
	}
	// Never clean bytes selected using a different model specification.
	if !reflect.DeepEqual(spec, taskModelSpec(latest)) || s.isReservingModelArtifact(latest) {
		return s.retryArtifactEviction(task, fmt.Errorf("model changed during eviction preflight"))
	}
	used, err := s.modelHasConsumers(ctx, latest, path)
	if err != nil {
		return s.retryArtifactEviction(task, err)
	}
	if used {
		return false, nil
	}
	// Finish non-destructive preflight before withdrawing Ready. An aborted
	// request must leave intact artifacts' status unchanged. All tasks for this
	// UID are serialized, and Ready is removed before any filesystem mutation.
	op := &NodeLabelOp{BaseModel: latest.BaseModel, ClusterBaseModel: latest.ClusterBaseModel, ModelStateOnNode: Updating}
	if err := s.safeNodeLabelReconciliation(ctx, op); err != nil {
		return s.retryArtifactEviction(task, err)
	}
	handled, waiting, err := s.processSharedHfArtifactDelete(ctx, latest)
	if err != nil {
		return s.retryArtifactEviction(task, err)
	}
	if waiting {
		return true, nil
	}
	if !handled {
		// Shared handling must precede this lock: shared operations acquire the
		// parent lock before this same child lock.
		release, acquired, err := s.acquireDirectArtifactPath(latest)
		if err != nil {
			return s.retryArtifactEviction(task, err)
		}
		if !acquired {
			return s.retryArtifactEviction(task, fmt.Errorf("direct artifact path is busy"))
		}
		defer release()
		current, currentPath, err := s.prepareArtifactEviction(ctx, latest)
		if err != nil {
			return s.retryArtifactEviction(task, err)
		}
		if current == nil {
			return false, nil
		}
		if currentPath != path || !reflect.DeepEqual(spec, taskModelSpec(current)) ||
			!reflect.DeepEqual(downloadAnnotations(taskModelMeta(latest).Annotations), downloadAnnotations(taskModelMeta(current).Annotations)) {
			return s.retryArtifactEviction(task, fmt.Errorf("model changed before direct eviction lock"))
		}
		if err := s.validateDirectArtifactCleanup(ctx, current, path); err != nil {
			return s.retryArtifactEviction(task, err)
		}
		cm, err := s.configMapReconciler.getConfigMap(ctx)
		if err != nil {
			return s.retryArtifactEviction(task, err)
		}
		if _, err := directArtifactOwnedEntry(cm.Data, current, path); err != nil {
			return s.retryArtifactEviction(task, err)
		}
		if err := s.reconcileDirectArtifactStatus(ctx, current, Updating); err != nil {
			return s.retryArtifactEviction(task, err)
		}
		if err := s.mutateDirectArtifactReceipt(ctx, current, path, true); err != nil {
			return s.retryArtifactEviction(task, err)
		}
		if err := ctx.Err(); err != nil {
			return s.retryArtifactEviction(task, err)
		}
		if err := os.RemoveAll(path); err != nil {
			return s.retryArtifactEviction(task, err)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			return s.retryArtifactEviction(task, fmt.Errorf("direct artifact remains after eviction: %s (%v)", path, err))
		}
		if err := s.reconcileDirectArtifactStatus(ctx, current, Evicted); err != nil {
			return s.retryArtifactEviction(task, err)
		}
		if err := s.mutateDirectArtifactReceipt(ctx, current, path, false); err != nil {
			return s.retryArtifactEviction(task, err)
		}
		return false, nil
	}
	if err := s.completeSharedArtifactEviction(ctx, latest, path); err != nil {
		return s.retryArtifactEviction(task, err)
	}
	return false, nil
}

// Shared deletion releases its locks after clearing the reference. Reacquire
// the same child lock before reporting absence; restoration may have won meanwhile.
func (s *Gopher) completeSharedArtifactEviction(ctx context.Context, task *GopherTask, path string) error {
	unlock, acquired, err := s.acquireDirectArtifactPath(task)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("artifact child path is busy before eviction completion")
	}
	defer unlock()
	current, err := s.latestEvictionTask(ctx, task)
	if err != nil || current == nil {
		return err
	}
	if !reflect.DeepEqual(taskModelSpec(current), taskModelSpec(task)) {
		return fmt.Errorf("model changed before eviction completion")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return fmt.Errorf("model child path remains after eviction: %s (%v)", path, err)
	}
	return s.reconcileDirectArtifactStatus(ctx, current, Evicted)
}

func (s *Gopher) retryArtifactEviction(task *GopherTask, cause error) (bool, error) {
	err := s.requeueHfArtifactTask(task, newHfArtifactRetryResult(gopherTaskModelKey(task), cause))
	return err == nil, err
}
