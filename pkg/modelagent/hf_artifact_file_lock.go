package modelagent

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
	"golang.org/x/sys/unix"
)

const hfArtifactLockDirectory = ".hf-artifact-locks"

// tryHfArtifactParentFileLock is also usable by startup recovery without a
// child. The stable lock inode is outside _artifacts, so deleting a parent
// cannot split its lock. Never unlink these files, including after Close.
func tryHfArtifactParentFileLock(parent HfArtifactEntry, modelStoreRoot string) (*flock.Flock, bool, error) {
	root, parentPath, err := validateHfArtifactParentPath(parent, modelStoreRoot)
	if err != nil {
		return nil, false, err
	}
	artifactRoot := hfArtifactParentStoreRoot(parent)
	artifactRoot, err = hfArtifactPathInRoot(artifactRoot, modelStoreRoot, root)
	if err != nil {
		return nil, false, err
	}
	return tryHfArtifactFileLock(root, filepath.Join(artifactRoot, hfArtifactLockDirectory), "parent:"+parentPath)
}

func tryHfArtifactChildFileLock(input hfArtifactTaskInput) (*flock.Flock, bool, error) {
	root, err := canonicalHfArtifactStoreRoot(input.ModelStoreRoot)
	if err != nil {
		return nil, false, err
	}
	childPath, err := hfArtifactPathInRoot(input.ChildModelPath, input.ModelStoreRoot, root)
	if err != nil {
		return nil, false, err
	}
	if err := validateHfArtifactPathAncestors(root, childPath, false); err != nil {
		return nil, false, err
	}
	return tryHfArtifactFileLock(root, filepath.Join(filepath.Dir(childPath), hfArtifactLockDirectory), "child:"+childPath)
}

func tryHfArtifactFileLock(root, directory, key string) (*flock.Flock, bool, error) {
	if err := validateHfArtifactPathAncestors(root, directory, true); err != nil {
		return nil, false, err
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, false, err
	}
	path := filepath.Join(directory, fmt.Sprintf("%x.lock", sha256.Sum256([]byte(key))))
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("artifact lock %s is not a regular file", path)
	} else if err != nil && !os.IsNotExist(err) {
		return nil, false, err
	}
	lock := flock.New(path, flock.SetFlag(os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW))
	acquired, err := lock.TryLock()
	if err != nil || !acquired {
		_ = lock.Close()
		return nil, false, err
	}
	return lock, true, nil
}

// tryParentFileOperation retains the local guard used by label callbacks and
// adds cross-process exclusion. Contention never blocks a queue worker.
func (h *hfArtifactTaskHandler) tryParentFileOperation(parent HfArtifactEntry, root string) (func(), bool, error) {
	unlock, acquired := h.tryParentOperation(parent.Key)
	if !acquired {
		return nil, false, nil
	}
	lock, acquired, err := tryHfArtifactParentFileLock(parent, root)
	if err != nil || !acquired {
		unlock()
		return nil, false, err
	}
	return func() {
		if err := lock.Close(); err != nil {
			h.repository.configMaps.logger.Errorf("Close shared artifact lock: %v", err)
		}
		unlock()
	}, true, nil
}

// All child-changing operations take parent then child, including replacements
// that use a different parent identity but the same child path.
func (h *hfArtifactTaskHandler) tryArtifactOperation(input hfArtifactTaskInput) (func(), bool, error) {
	if err := input.validateFilesystemPaths(input.Parent); err != nil {
		return nil, false, err
	}
	unlock, acquired, err := h.tryParentFileOperation(input.Parent, input.ModelStoreRoot)
	if err != nil || !acquired {
		return nil, false, err
	}
	child, acquired, err := tryHfArtifactChildFileLock(input)
	if err != nil || !acquired {
		unlock()
		return nil, false, err
	}
	return func() {
		if err := child.Close(); err != nil {
			h.repository.configMaps.logger.Errorf("Close shared artifact child lock: %v", err)
		}
		unlock()
	}, true, nil
}
