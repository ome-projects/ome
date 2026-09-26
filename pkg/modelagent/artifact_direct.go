package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

type directArtifactDownloadKey struct{}

// A lock belongs to one execution attempt, never to a task copied into a queue.
type directArtifactDownloadOperation struct {
	path            string
	release         func()
	sharedCompleted bool
	readOnly        bool
	sharedReader    *hfArtifactTaskInput
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

func (s *Gopher) hasSharedArtifactPublication(ctx context.Context, task *GopherTask) bool {
	if op := directArtifactDownloadOperationFromContext(ctx); op != nil && op.sharedCompleted {
		return true
	}
	if taskModelMeta(task) == nil {
		return false
	}
	spec := taskModelSpec(task)
	return spec.Storage != nil && spec.Storage.Path != nil && isSharedHfArtifactSymlink(*spec.Storage.Path)
}

func (s *Gopher) handoffOrdinaryArtifactOwner(ctx context.Context, task *GopherTask) error {
	if s.configMapReconciler == nil {
		return fmt.Errorf("artifact ownership requires a ConfigMap reconciler")
	}
	return s.configMapReconciler.handoffOrdinaryModelOwner(ctx,
		getModelID(task.BaseModel, task.ClusterBaseModel), taskModelMeta(task).UID, func() error {
			return s.validateArtifactDownload(ctx, task)
		})
}

// Coordinate bounded ordinary writers through their final Ready publication.
func (s *Gopher) acquireDirectArtifactDownload(ctx context.Context, task *GopherTask) (func(), bool, error) {
	return s.acquireDirectArtifactOperation(ctx, task, false)
}

func (s *Gopher) acquireDirectArtifactOperation(ctx context.Context, task *GopherTask, readOnly bool) (func(), bool, error) {
	noop := func() {}
	if taskModelMeta(task) == nil {
		return nil, false, fmt.Errorf("artifact operation requires a model")
	}
	// Legacy destinations still need a verified persisted-UID handoff, even
	// though their files are outside canonical path ownership.
	legacy := func() (func(), bool, error) {
		if taskModelMeta(task).UID != "" && s.configMapReconciler != nil {
			if err := s.handoffOrdinaryArtifactOwner(ctx, task); err != nil {
				return nil, false, err
			}
		}
		return noop, true, nil
	}
	path, err := s.directArtifactOperationPath(task, readOnly)
	if err != nil {
		return nil, false, err
	}
	if path == "" {
		return legacy()
	}
	unlock, acquired, err := s.acquireDirectArtifactPathLock(path)
	if err != nil || !acquired {
		return nil, false, err
	}
	if !readOnly {
		if err := s.validateDirectArtifactWritePath(task, path); err != nil {
			unlock()
			return nil, false, err
		}
	}
	if err := s.validateArtifactDownload(ctx, task); err != nil {
		unlock()
		return nil, false, err
	}
	if err := s.handoffOrdinaryArtifactOwner(ctx, task); err != nil {
		unlock()
		return nil, false, err
	}
	if err := s.validateArtifactDownload(ctx, task); err != nil {
		unlock()
		return nil, false, err
	}
	if _, err := s.directArtifactPath(task); err != nil && !readOnly {
		// An external ancestor alias can become dangling after a prior eviction.
		// Create its validated managed destination while holding the family lock,
		// so the ordinary source can subsequently write through the same alias.
		// Existing leaf links are left for HF's guarded unlink/replacement.
		_, err := os.Lstat(path)
		if os.IsNotExist(err) {
			err = os.MkdirAll(path, 0o755)
		}
		if err != nil {
			unlock()
			return nil, false, err
		}
	}
	if op := directArtifactDownloadOperationFromContext(ctx); op != nil {
		op.path, op.release, op.readOnly = path, unlock, readOnly
		return noop, true, nil
	}
	return unlock, true, nil
}

func (s *Gopher) validateDirectArtifactPublication(ctx context.Context, task *GopherTask, path string) error {
	if err := s.validateArtifactDownload(ctx, task); err != nil {
		return err
	}
	held := directArtifactDownloadOperationFromContext(ctx)
	current, err := s.directArtifactOperationPath(task, held != nil && held.readOnly)
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
	if entry.HfArtifactPendingDeletion != nil || entry.HfArtifactKey != "" || entry.DirectArtifactPendingEviction != nil || entry.Status == ModelStatusEvicted {
		return fmt.Errorf("direct artifact cleanup interrupted Ready publication")
	}
	return nil
}

// Persist proof from a completed writer while its child lock is still held.
// Metadata parsing is optional, so it cannot supply mandatory cleanup ownership.
func (s *Gopher) persistDirectArtifactPath(ctx context.Context, task *GopherTask, path string) error {
	key, uid := getModelID(task.BaseModel, task.ClusterBaseModel), taskModelMeta(task).UID
	return s.configMapReconciler.mutateModelEntryWithRetry(ctx, key, uid, false, func(data map[string]string) (bool, error) {
		if err := s.validateArtifactDownload(ctx, task); err != nil {
			return false, err
		}
		entry := ModelEntry{Name: taskModelMeta(task).Name, ModelUID: uid}
		if data[key] != "" {
			var err error
			entry, err = existingModelEntry(data, key)
			if err != nil {
				return false, err
			}
		}
		if entry.HfArtifactKey != "" || entry.HfArtifactPendingDeletion != nil {
			return false, fmt.Errorf("direct artifact acquired shared ownership before publication")
		}
		entry.ModelUID = uid
		entry.DirectArtifactPath = path
		return writeModelEntry(data, key, entry)
	})
}

func (s *Gopher) directArtifactPath(task *GopherTask) (string, error) {
	if task == nil || taskModelMeta(task) == nil || taskModelMeta(task).UID == "" {
		return "", fmt.Errorf("direct artifact operation requires a model UID")
	}
	spec := taskModelSpec(task)
	if spec.Storage == nil || spec.Storage.Path == nil {
		return "", fmt.Errorf("direct artifact operation requires an explicit path")
	}
	return ownedArtifactPath(s.modelRootDir, *spec.Storage.Path)
}

// Match the source's actual path, including ordinary download and local URI
// fallbacks. This does not grant deletion authority to that spelling.
func (s *Gopher) artifactDestination(task *GopherTask) (string, error) {
	spec := taskModelSpec(task)
	if spec.Storage == nil {
		return "", nil
	}
	if spec.Storage.Path != nil && *spec.Storage.Path != "" {
		// OCI joins object keys with filepath.Join; match that effective
		// directory before resolving aliases or choosing its writer lock.
		if spec.Storage.StorageUri != nil {
			source, err := storage.GetStorageType(*spec.Storage.StorageUri)
			if err == nil && source == storage.StorageTypeOCI {
				return filepath.Clean(*spec.Storage.Path), nil
			}
		}
		return *spec.Storage.Path, nil
	}
	if spec.Storage.StorageUri == nil {
		return "", nil
	}
	source, err := storage.GetStorageType(*spec.Storage.StorageUri)
	if err != nil {
		return "", err
	}
	if source == storage.StorageTypeLocal {
		local, err := storage.ParseLocalStorageURI(*spec.Storage.StorageUri)
		if err != nil {
			return "", err
		}
		return local.Path, nil
	}
	if source == storage.StorageTypeHuggingFace || source == storage.StorageTypeOCI {
		// Match the ordinary HF source's nil-path normalization without
		// changing the task or legacy deletion's getDestPath behavior.
		storageSpec := *spec.Storage
		empty := ""
		storageSpec.Path = &empty
		spec.Storage = &storageSpec
		if source == storage.StorageTypeOCI {
			return filepath.Clean(getDestPath(&spec, s.modelRootDir)), nil
		}
		return getDestPath(&spec, s.modelRootDir), nil
	}
	return "", nil
}

// Empty means genuinely external legacy storage. Classify only after resolving
// the actual source destination; its spelling is not an ownership boundary.
func (s *Gopher) directArtifactOperationPath(task *GopherTask, readOnly bool) (string, error) {
	if s.modelRootDir == "" {
		return "", nil
	}
	raw, err := s.artifactDestination(task)
	if err != nil || raw == "" {
		return "", err
	}
	root, err := canonicalHfArtifactStoreRoot(s.modelRootDir)
	if err != nil {
		return "", err
	}
	leafLink := false
	if info, err := os.Lstat(raw); !readOnly && err == nil && info.Mode()&os.ModeSymlink != 0 {
		leafLink = true
		if isSharedHfArtifactSymlink(raw) {
			return "", fmt.Errorf("refusing direct write through shared artifact link %s", raw)
		}
		// HF replaces a leaf link instead of writing through it. Resolve only
		// its parent to coordinate that directory entry, even for relative paths.
		separator := strings.LastIndex(raw, string(filepath.Separator))
		parent, name := ".", raw
		if separator >= 0 {
			parent, name = raw[:separator+1], raw[separator+1:]
		}
		rawParent := parent
		parent, err = resolveArtifactPathWithMissingLinks(parent, true)
		if err != nil {
			return "", err
		}
		entry := filepath.Join(parent, name)
		if hfArtifactInputPathWithin(root, entry) {
			if taskModelMeta(task).UID == "" {
				return "", fmt.Errorf("direct artifact operation requires a model UID")
			}
			if err := validateManagedArtifactAliases(root, rawParent, entry, false); err != nil {
				return "", err
			}
			return validateOwnedArtifactPath(root, entry)
		}
	}
	resolved, err := resolveArtifactPathWithMissingLinks(raw, true)
	if err != nil {
		return "", err
	}
	// Even an external destination may traverse a removable managed entry.
	// Validate the complete walk before treating it as unowned legacy storage.
	if err := validateManagedArtifactAliases(root, raw, resolved, readOnly); err != nil {
		return "", err
	}
	if !hfArtifactInputPathWithin(root, resolved) {
		if hfArtifactInputPathWithin(s.modelRootDir, raw) || hfArtifactInputPathWithin(root, raw) {
			return "", fmt.Errorf("managed artifact path %s resolves outside its store", raw)
		}
		return "", nil
	}
	if taskModelMeta(task).UID == "" {
		return "", fmt.Errorf("direct artifact operation requires a model UID")
	}
	if leafLink {
		// HF replaces a leaf link, unlike an ancestor alias. It would then write
		// outside the managed destination whose ownership we are protecting.
		return "", fmt.Errorf("external artifact leaf link %s targets the managed store", raw)
	}
	if readOnly {
		return validatePhysicalArtifactPath(root, resolved)
	}
	return validateOwnedArtifactPath(root, resolved)
}

// An alias inside a removable managed family needs protection of its own path,
// not just its target. Keep writers' existing ancestor-link rejection. Readers
// may follow an in-store alias only when the same family lock protects both.
func validateManagedArtifactAliases(root, raw, target string, readOnly bool) error {
	_, err := walkArtifactPath(raw, true, func(entry string, link bool) error {
		if entry == root || !hfArtifactInputPathWithin(root, entry) {
			return nil
		}
		if !hfArtifactInputPathWithin(root, target) || hfArtifactPathFamilyLockKey(root, entry) != hfArtifactPathFamilyLockKey(root, target) || (link && !readOnly) {
			return fmt.Errorf("managed artifact entry %s is not protected by the destination family lock", entry)
		}
		return nil
	})
	return err
}

// HF can unlink a legacy child link while holding the child lock. OCI writes
// only real directories; neither source may write through a shared child link.
func (s *Gopher) validateDirectArtifactWritePath(task *GopherTask, path string) error {
	if current, err := s.directArtifactOperationPath(task, false); err != nil || current != path {
		return fmt.Errorf("direct artifact destination changed before write: %s (%v)", path, err)
	}
	if info, err := os.Lstat(path); err == nil && !info.IsDir() {
		spec := taskModelSpec(task)
		if spec.Storage.StorageUri != nil && info.Mode()&os.ModeSymlink != 0 {
			source, err := storage.GetStorageType(*spec.Storage.StorageUri)
			if err == nil && source == storage.StorageTypeHuggingFace && !isSharedHfArtifactSymlink(path) {
				return nil
			}
		}
		return fmt.Errorf("direct artifact path %s is not a real directory", path)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *Gopher) acquireDirectArtifactPathLock(path string) (release func(), acquired bool, err error) {
	root, err := canonicalHfArtifactStoreRoot(s.modelRootDir)
	if err != nil {
		return nil, false, err
	}
	if _, err := validatePhysicalArtifactPath(root, path); err != nil {
		return nil, false, err
	}
	lock, acquired, err := tryHfArtifactFileLock(root, filepath.Join(root, hfArtifactLockDirectory), hfArtifactPathFamilyLockKey(root, path))
	if err != nil || !acquired {
		return nil, false, err
	}
	return func() {
		if err := lock.Close(); err != nil {
			s.logger.Errorf("Close direct artifact lock: %v", err)
		}
	}, true, nil
}

// Bound writer coordination to child paths, excluding shared stores and lock files.
func ownedArtifactPath(root, path string) (string, error) {
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
	return validateOwnedArtifactPath(root, path)
}

// The physical writer path may contain literal spaces. Deletion callers still
// validate the stricter original spelling through ownedArtifactPath first.
func validateOwnedArtifactPath(root, path string) (string, error) {
	if _, err := validatePhysicalArtifactPath(root, path); err != nil {
		return "", err
	}
	rel, _ := filepath.Rel(root, path)
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == constants.ModelArtifactsDirectory || part == hfArtifactLockDirectory {
			return "", fmt.Errorf("refusing artifact operation on reserved artifact path %s", path)
		}
	}
	return path, nil
}

func validatePhysicalArtifactPath(root, path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, '\x00') || !hfArtifactInputPathWithin(root, path) {
		return "", fmt.Errorf("invalid physical artifact path %q", path)
	}
	if path == root {
		return "", fmt.Errorf("refusing artifact operation on the model store root")
	}
	if err := validateHfArtifactPathAncestors(root, path, false); err != nil {
		return "", err
	}
	return path, nil
}
