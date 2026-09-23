package modelagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/constants"
)

const directArtifactStagingDirectory = ".artifact-staging"

// ArtifactPendingEviction keeps direct cleanup durable until the owned path is absent.
type ArtifactPendingEviction struct {
	ModelUID types.UID `json:"modelUID"`
	Path     string    `json:"path"`
}

type directArtifactDownloadKey struct{}

// A lock belongs to one execution attempt, never to a task copied into a queue.
type directArtifactDownloadOperation struct {
	path            string
	release         func()
	sharedCompleted bool
}

func withDirectArtifactDownloadOperation(ctx context.Context) (context.Context, func()) {
	op := &directArtifactDownloadOperation{}
	return context.WithValue(ctx, directArtifactDownloadKey{}, op), func() {
		if op.release != nil {
			op.release()
			op.release = nil
		}
	}
}

func directArtifactDownloadOperationFromContext(ctx context.Context) *directArtifactDownloadOperation {
	op, _ := ctx.Value(directArtifactDownloadKey{}).(*directArtifactDownloadOperation)
	return op
}

func (s *Gopher) handoffOrdinaryArtifactOwner(ctx context.Context, task *GopherTask) error {
	return s.configMapReconciler.handoffOrdinaryModelOwner(ctx,
		getModelID(task.BaseModel, task.ClusterBaseModel), taskModelMeta(task).UID, func() error {
			return s.validateDirectArtifactTask(ctx, task)
		})
}

func (s *Gopher) validateDirectArtifactDownload(ctx context.Context, task *GopherTask) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Unlike checkCurrentArtifactRequest, this also checks an empty captured R.
	skip, _, err := s.shouldSkipArtifactTask(ctx, task)
	if err != nil {
		return err
	}
	if skip {
		return fmt.Errorf("direct artifact download no longer matches the live model")
	}
	// Preserve consumer-blocked eviction behavior, but do not start a writer
	// after a destructive eviction has become eligible on another process.
	current, err := s.latestEvictionTask(ctx, task)
	if err != nil {
		return err
	}
	if current != nil && !s.isReservingModelArtifact(current) {
		eligible, _, err := s.prepareArtifactEviction(ctx, current)
		if err != nil {
			return err
		}
		if eligible != nil {
			return fmt.Errorf("direct artifact eviction is pending")
		}
	}
	return ctx.Err()
}

// acquireDirectArtifactDownload covers ordinary writers as well as restores.
// Unsupported unmanaged legacy paths cannot be evicted by the bounded cleanup.
func (s *Gopher) acquireDirectArtifactDownload(ctx context.Context, task *GopherTask) (func(), bool, error) {
	noop := func() {}
	path, err := s.directArtifactPath(task)
	if err != nil {
		if artifactRehydrationID(task) != "" || task.ResidencyManaged {
			return nil, false, err
		}
		spec := taskModelSpec(task)
		if spec.Storage != nil && spec.Storage.Path != nil && taskModelMeta(task).UID == "" {
			if _, boundedErr := safeArtifactEvictionPath(s.modelRootDir, *spec.Storage.Path); boundedErr == nil {
				return nil, false, err
			}
		}
		return noop, true, nil
	}
	unlock, acquired, err := s.acquireDirectArtifactPath(task)
	if err != nil || !acquired {
		return nil, false, err
	}
	if err := s.validateDirectArtifactDownload(ctx, task); err != nil {
		unlock()
		return nil, false, err
	}
	if err := s.handoffOrdinaryArtifactOwner(ctx, task); err != nil {
		unlock()
		return nil, false, err
	}
	if err := s.resumeDirectArtifactEviction(ctx, task); err != nil && !apierrors.IsNotFound(err) {
		unlock()
		return nil, false, err
	}
	if err := s.validateDirectArtifactDownload(ctx, task); err != nil {
		unlock()
		return nil, false, err
	}
	if op := directArtifactDownloadOperationFromContext(ctx); op != nil {
		op.path, op.release = path, unlock
		return noop, true, nil
	}
	return unlock, true, nil
}

func (s *Gopher) validateDirectArtifactPublication(ctx context.Context, task *GopherTask, path string) error {
	if err := s.validateDirectArtifactDownload(ctx, task); err != nil {
		return err
	}
	current, err := s.directArtifactPath(task)
	if err != nil || current != path {
		return fmt.Errorf("direct artifact path changed before Ready: %s (%v)", path, err)
	}
	if info, err := os.Lstat(path); err != nil || !info.IsDir() {
		return fmt.Errorf("direct artifact is absent before Ready publication: %s (%v)", path, err)
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	if cm.Data[key] == "" {
		return nil
	}
	entry, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return err
	}
	if entry.ModelUID != "" && entry.ModelUID != taskModelMeta(task).UID {
		return fmt.Errorf("direct artifact owner changed before Ready publication")
	}
	if entry.ArtifactPendingEviction != nil || entry.HfArtifactPendingDeletion != nil || entry.HfArtifactKey != "" || entry.Status == ModelStatusEvicted {
		return fmt.Errorf("direct artifact cleanup interrupted Ready publication")
	}
	return nil
}

func (s *Gopher) directArtifactPath(task *GopherTask) (string, error) {
	if task == nil || taskModelMeta(task) == nil || taskModelMeta(task).UID == "" {
		return "", fmt.Errorf("direct artifact operation requires a model UID")
	}
	spec := taskModelSpec(task)
	if spec.Storage == nil || spec.Storage.Path == nil {
		return "", fmt.Errorf("direct artifact operation requires an explicit path")
	}
	return s.validateDirectArtifactPath(*spec.Storage.Path)
}

func (s *Gopher) validateDirectArtifactPath(path string) (string, error) {
	path, err := safeArtifactEvictionPath(s.modelRootDir, path)
	if err != nil {
		return "", err
	}
	if info, err := os.Lstat(path); err == nil && !info.IsDir() {
		return "", fmt.Errorf("direct artifact path %s is not a real directory", path)
	} else if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	return path, nil
}

// acquireDirectArtifactPath uses the shared handler's exact child lock key.
// Call only after shared handling; keep the lock through cleanup and publication.
func (s *Gopher) acquireDirectArtifactPath(task *GopherTask) (release func(), acquired bool, err error) {
	path, err := s.directArtifactPath(task)
	if err != nil {
		return nil, false, err
	}
	return s.acquireDirectArtifactPathLock(path)
}

// path has been validated as a bounded, non-shared direct artifact path.
func (s *Gopher) acquireDirectArtifactPathLock(path string) (release func(), acquired bool, err error) {
	lock, acquired, err := tryHfArtifactChildFileLock(hfArtifactTaskInput{ModelStoreRoot: s.modelRootDir, ChildModelPath: path})
	if err != nil || !acquired {
		return nil, false, err
	}
	return func() {
		if err := lock.Close(); err != nil {
			s.logger.Errorf("Close direct artifact lock: %v", err)
		}
	}, true, nil
}

func (s *Gopher) validateDirectArtifactTask(ctx context.Context, task *GopherTask) error {
	if s.modelClient == nil {
		return fmt.Errorf("direct artifact cleanup requires a live model client")
	}
	latest := *task
	var err error
	if task.BaseModel != nil {
		latest.BaseModel, err = s.modelClient.OmeV1beta1().BaseModels(task.BaseModel.Namespace).Get(ctx, task.BaseModel.Name, metav1.GetOptions{})
	} else {
		latest.ClusterBaseModel, err = s.modelClient.OmeV1beta1().ClusterBaseModels().Get(ctx, task.ClusterBaseModel.Name, metav1.GetOptions{})
	}
	if err != nil {
		return err
	}
	if taskModelMeta(&latest).UID != taskModelMeta(task).UID || !taskModelMeta(&latest).DeletionTimestamp.IsZero() ||
		!reflect.DeepEqual(taskModelSpec(&latest), taskModelSpec(task)) ||
		!reflect.DeepEqual(downloadAnnotations(taskModelMeta(&latest).Annotations), downloadAnnotations(taskModelMeta(task).Annotations)) {
		return fmt.Errorf("direct artifact model changed before cleanup")
	}
	return nil
}

func directArtifactOwnedEntry(data map[string]string, task *GopherTask, path string) (ModelEntry, error) {
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	entry, err := existingModelEntry(data, key)
	if err != nil {
		return entry, err
	}
	uid := taskModelMeta(task).UID
	if uid == "" || entry.ModelUID != uid {
		return entry, fmt.Errorf("direct artifact entry has unknown or different model UID")
	}
	if entry.HfArtifactKey != "" || entry.HfArtifactPendingDeletion != nil {
		return entry, fmt.Errorf("direct artifact path has shared ownership")
	}
	if entry.Config != nil {
		artifact := entry.Config.Artifact
		if len(artifact.ChildrenPaths) != 0 || len(artifact.ParentPath) > 1 {
			return entry, fmt.Errorf("direct artifact path has shared ownership")
		}
		if len(artifact.ParentPath) == 1 {
			parentPath, self := artifact.ParentPath[key]
			spec := taskModelSpec(task)
			// Ordinary HF copies record themselves as the parent. The caller
			// validated the task's path against the bounded root; allow its
			// original OS spelling as well as the canonical form, not new aliases.
			configuredPath := spec.Storage != nil && spec.Storage.Path != nil && parentPath == *spec.Storage.Path
			if !self || parentPath == "" || (parentPath != path && !configuredPath) {
				return entry, fmt.Errorf("direct artifact parent is not the owned model path")
			}
		}
	}
	if pending := entry.ArtifactPendingEviction; pending != nil && (pending.ModelUID != uid || pending.Path != path) {
		return entry, fmt.Errorf("direct artifact receipt has unknown or different UID/path")
	}
	return entry, nil
}

// mutateDirectArtifactReceipt uses the existing CAS/cache path without replacing
// unrelated entries. A completion never drops its receipt while bytes remain.
func (s *Gopher) mutateDirectArtifactReceipt(ctx context.Context, task *GopherTask, path string, record bool) error {
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	return s.configMapReconciler.mutateConfigMapWithRetry(ctx, func(cm *corev1.ConfigMap) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if s.configMapReconciler.isModelMutationBlocked(key, taskModelMeta(task).UID) {
			return false, fmt.Errorf("direct artifact owner was replaced")
		}
		entry, err := directArtifactOwnedEntry(cm.Data, task, path)
		if err != nil {
			return false, err
		}
		if record {
			entry.ArtifactPendingEviction = &ArtifactPendingEviction{ModelUID: taskModelMeta(task).UID, Path: path}
		} else {
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				return false, fmt.Errorf("direct artifact path is not absent: %s (%v)", path, err)
			}
			entry.ArtifactPendingEviction = nil
		}
		return writeModelEntry(cm.Data, key, entry)
	})
}

func (s *Gopher) validateDirectArtifactCleanup(ctx context.Context, task *GopherTask, path string) error {
	current, err := s.directArtifactPath(task)
	if err != nil {
		return err
	}
	if current != path {
		return fmt.Errorf("direct artifact path changed")
	}
	return s.validateDirectArtifactCleanupAtPath(ctx, task, path)
}

// A committed receipt may own a previous path, but never a previous CR UID.
func (s *Gopher) validateDirectArtifactCleanupAtPath(ctx context.Context, task *GopherTask, path string) error {
	if err := s.validateDirectArtifactTask(ctx, task); err != nil {
		return err
	}
	if _, err := s.validateDirectArtifactPath(path); err != nil {
		return err
	}
	used, err := s.evictionPathHasOtherModels(ctx, task, path)
	if err != nil {
		return err
	}
	if used {
		return fmt.Errorf("direct artifact path is used by another model")
	}
	if err := s.validateArtifactEvictionReferences(ctx, task, path); err != nil {
		return err
	}
	// Removing an ancestor must not remove another child's stable lock inode or
	// a nested shared parent/staging store, even if its CR has disappeared.
	return filepath.WalkDir(path, func(current string, entry fs.DirEntry, err error) error {
		if current == path && os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Name() == hfArtifactLockDirectory || entry.Name() == constants.ModelArtifactsDirectory || entry.Name() == directArtifactStagingDirectory {
			return fmt.Errorf("direct artifact contains a reserved store path: %s", current)
		}
		return nil
	})
}

// The caller owns the child lock; do not call a callback that reacquires it.
func (s *Gopher) reconcileDirectArtifactStatus(ctx context.Context, task *GopherTask, state ModelStateOnNode) error {
	s.configMapMutex.Lock()
	defer s.configMapMutex.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.nodeLabelReconciler.ReconcileNodeLabels(&NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStateOnNode: state}); err != nil {
		return err
	}
	return s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel, ModelStatus: ModelStatus(state)})
}

// resumeDirectArtifactEviction runs UNDER acquireDirectArtifactPath's lock.
// New ISVC demand may request restoration, but a surviving consuming Pod still
// blocks deletion. Finish partial cleanup before allowing Download to validate.
func (s *Gopher) resumeDirectArtifactEviction(ctx context.Context, task *GopherTask) error {
	path, err := s.directArtifactPath(task)
	if err != nil {
		return err
	}
	if s.configMapReconciler == nil {
		return fmt.Errorf("direct artifact cleanup requires a ConfigMap reconciler")
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		return err
	}
	key := getModelID(task.BaseModel, task.ClusterBaseModel)
	if cm.Data[key] == "" {
		return nil
	}
	entry, err := existingModelEntry(cm.Data, key)
	if err != nil {
		return err
	}
	if entry.ArtifactPendingEviction == nil {
		return nil
	}
	if entry.ArtifactPendingEviction.Path != path {
		path, err = s.validateDirectArtifactPath(entry.ArtifactPendingEviction.Path)
		if err != nil {
			return err
		}
		// Nonblocking acquisition avoids deadlock when two path changes cross.
		unlock, acquired, err := s.acquireDirectArtifactPathLock(path)
		if err != nil {
			return err
		}
		if !acquired {
			return fmt.Errorf("previous direct artifact path is busy")
		}
		defer unlock()
		cm, err = s.configMapReconciler.getConfigMap(ctx)
		if err != nil {
			return err
		}
	}
	currentEntry, err := directArtifactOwnedEntry(cm.Data, task, path)
	if err != nil {
		return err
	}
	if currentEntry.ArtifactPendingEviction == nil || *currentEntry.ArtifactPendingEviction != *entry.ArtifactPendingEviction {
		return fmt.Errorf("direct artifact receipt changed before cleanup")
	}
	if err := s.validateDirectArtifactCleanupAtPath(ctx, task, path); err != nil {
		return err
	}
	label, err := getModelLabelKey(&NodeLabelOp{BaseModel: task.BaseModel, ClusterBaseModel: task.ClusterBaseModel})
	if err != nil {
		return err
	}
	if s.configMapReconciler.nodeName == "" {
		return fmt.Errorf("direct artifact cleanup requires node identity")
	}
	// New Pods may be waiting for this cleanup and restoration to finish.
	// Only a bound consumer on this node can still be using these bytes.
	used, err := s.pathHasPodConsumersOnNode(ctx, path, label, s.configMapReconciler.nodeName)
	if err != nil {
		return err
	}
	if used {
		return fmt.Errorf("direct artifact cleanup is blocked by a consuming pod")
	}
	if err := s.reconcileDirectArtifactStatus(ctx, task, Updating); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return s.mutateDirectArtifactReceipt(ctx, task, path, false)
}

// downloadRestoredArtifact runs under the caller's child lock. Existing real
// directories retain ordinary downloader validation; new copies publish only
// after validation and a fresh request/UID/source/placement check.
func (s *Gopher) downloadRestoredArtifact(ctx context.Context, task *GopherTask, path string, download func(string) error) (resultErr error) {
	target, err := s.directArtifactPath(task)
	if err != nil {
		return err
	}
	path, err = safeArtifactEvictionPath(s.modelRootDir, path)
	if err != nil {
		return err
	}
	if path != target {
		return fmt.Errorf("restored artifact destination differs from model path")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return download(path)
	} else if !os.IsNotExist(err) {
		return err
	}
	root, err := canonicalHfArtifactStoreRoot(s.modelRootDir)
	if err != nil {
		return err
	}
	identity, err := json.Marshal(struct {
		UID         types.UID
		Path        string
		Spec        interface{}
		Annotations map[string]string
	}{taskModelMeta(task).UID, path, taskModelSpec(task), downloadAnnotations(taskModelMeta(task).Annotations)})
	if err != nil {
		return err
	}
	stage := filepath.Join(root, directArtifactStagingDirectory, directArtifactDownloadStages, fmt.Sprintf("%x", sha256.Sum256(identity)))
	if err := validateHfArtifactPathAncestors(root, stage, true); err != nil {
		return err
	}
	lock, acquired, err := tryHfArtifactFileLock(root, filepath.Join(root, hfArtifactLockDirectory), "staging:"+stage)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("artifact staging cleanup is in progress")
	}
	defer func() {
		// The synchronous downloader has stopped before cleanup. Successful
		// publication moved the directory; failed or stale attempts discard it.
		resultErr = errors.Join(resultErr, removeArtifactStage(root, stage), lock.Close())
	}()
	if err := os.MkdirAll(stage, 0755); err != nil {
		return err
	}
	if err := download(stage); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	skip, deleting, err := s.shouldSkipArtifactTask(ctx, task)
	if err != nil {
		return err
	}
	if skip || deleting {
		return fmt.Errorf("restored artifact request is no longer current")
	}
	if err := validateHfArtifactPathAncestors(root, stage, true); err != nil {
		return err
	}
	if _, err := s.directArtifactPath(task); err != nil {
		return err
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return fmt.Errorf("restored artifact target appeared before publication: %s (%v)", path, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Rename(stage, path)
}
