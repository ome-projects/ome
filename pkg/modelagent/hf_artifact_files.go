package modelagent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"sigs.k8s.io/ome/pkg/constants"
)

var errHfArtifactChildPathConflict = errors.New("child model path conflicts with shared Hugging Face parent")

// hfArtifactFiles operates on the parent directory, local ready marker, and
// child symlinks. It stores no state and does not read or write the ConfigMap.
type hfArtifactFiles struct{}

// ParentReadyMarkerExists checks for a parent directory and a local ready
// marker. It does not validate model files or their checksums.
func (hfArtifactFiles) ParentReadyMarkerExists(parent HfArtifactEntry) bool {
	info, err := os.Stat(parent.LocalPath)
	if err != nil || !info.IsDir() {
		return false
	}
	_, err = os.Stat(filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName))
	return err == nil
}

// ParentReadyMarkerMatchesLock checks whether the active parent lock owner
// published local completion. An older download's marker cannot finish its work.
func (files hfArtifactFiles) ParentReadyMarkerMatchesLock(parent HfArtifactEntry) bool {
	if parent.LockID == "" || !files.ParentReadyMarkerExists(parent) {
		return false
	}
	content, err := os.ReadFile(filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName))
	return err == nil && string(content) == parent.LockID
}

// ResetParentDirectory removes existing contents and creates an empty directory.
// The caller must own the parent lock and have checked for child symlinks.
func (hfArtifactFiles) ResetParentDirectory(parentPath string) error {
	if err := os.RemoveAll(parentPath); err != nil {
		return err
	}
	return os.MkdirAll(parentPath, 0o755)
}

// WriteParentReadyMarker records the LockID after a successful download.
func (hfArtifactFiles) WriteParentReadyMarker(parent HfArtifactEntry) error {
	return os.WriteFile(
		filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName),
		[]byte(parent.LockID),
		0o644,
	)
}

// ChildPathExists includes dangling symlinks. An unreadable path is treated as
// occupied so the shared-artifact handler does not overwrite it.
func (hfArtifactFiles) ChildPathExists(childModelPath string) bool {
	_, err := os.Lstat(childModelPath)
	return !os.IsNotExist(err)
}

// IsChildLinkedToParent compares a symlink's target with the expected parent.
// Relative links and dangling links are included; the parent need not exist.
func (hfArtifactFiles) IsChildLinkedToParent(childModelPath, parentPath string) bool {
	target, err := readChildSymlinkTarget(childModelPath)
	return err == nil && target == filepath.Clean(parentPath)
}

func readChildSymlinkTarget(childModelPath string) (string, error) {
	target, err := os.Readlink(childModelPath)
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(childModelPath), target)
	}
	return filepath.Clean(target), nil
}

// CreateChildSymlink creates a relative link or accepts the same link on retry.
// An existing directory, file, or link to another parent is a conflict.
func (files hfArtifactFiles) CreateChildSymlink(childModelPath, parentPath string) error {
	if err := os.MkdirAll(filepath.Dir(childModelPath), 0o755); err != nil {
		return err
	}
	if _, err := os.Lstat(childModelPath); err == nil {
		if files.IsChildLinkedToParent(childModelPath, parentPath) {
			return nil
		}
		return fmt.Errorf("%w: %s", errHfArtifactChildPathConflict, childModelPath)
	} else if !os.IsNotExist(err) {
		return err
	}
	target, err := filepath.Rel(filepath.Dir(childModelPath), parentPath)
	if err != nil {
		return err
	}
	return os.Symlink(target, childModelPath)
}

// RemoveChildSymlink removes only a link to this parent. A missing path is
// already removed; any other existing path is preserved and reported as a conflict.
func (files hfArtifactFiles) RemoveChildSymlink(childModelPath, parentPath string) error {
	if _, err := os.Lstat(childModelPath); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if !files.IsChildLinkedToParent(childModelPath, parentPath) {
		return fmt.Errorf("%w: %s", errHfArtifactChildPathConflict, childModelPath)
	}
	return os.Remove(childModelPath)
}

// HasChildren scans modelStoreRoot for any child symlink to this parent,
// including links missing from the ConfigMap. It skips the parent contents and
// stops at the first matching child. An error prevents destructive parent work.
// WalkDir inspects symlinks without following them. This is a point-in-time
// filesystem check, not a lock against concurrent child creation.
func (hfArtifactFiles) HasChildren(parentPath, modelStoreRoot string) (bool, error) {
	cleanParentPath := filepath.Clean(parentPath)
	foundChild := false
	err := filepath.WalkDir(modelStoreRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// A missing root has no children. An error below an existing root
			// means the scan is incomplete, even if a directory disappeared.
			if path == modelStoreRoot && os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if path == modelStoreRoot && !entry.IsDir() {
			return fmt.Errorf("model store root %s is not a directory", modelStoreRoot)
		}
		if filepath.Clean(path) == cleanParentPath && entry.IsDir() {
			return filepath.SkipDir
		}
		if entry.Type()&os.ModeSymlink == 0 {
			return nil
		}
		target, err := readChildSymlinkTarget(path)
		if err != nil {
			return err
		}
		if target == cleanParentPath {
			foundChild = true
			return filepath.SkipAll
		}
		return nil
	})
	return foundChild, err
}

// RemoveParentDirectory deletes the parent contents; the caller owns the parent
// deletion lock and must have checked both ConfigMap references and local links.
func (hfArtifactFiles) RemoveParentDirectory(parentPath string) error {
	return os.RemoveAll(parentPath)
}
