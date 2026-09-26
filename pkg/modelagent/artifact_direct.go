package modelagent

import (
	"fmt"
	"os"
	"path/filepath"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/storage"
	"strings"
)

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
