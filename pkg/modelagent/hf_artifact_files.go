package modelagent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/ome/pkg/constants"
)

var errHfArtifactChildPathConflict = errors.New("child model path conflicts with shared Hugging Face parent")

// hfArtifactFiles operates on the parent directory, local ready marker, and
// child symlinks. It stores no state and does not read or write the ConfigMap.
type hfArtifactFiles struct{}

func canonicalHfArtifactStoreRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", errors.New("model store root is required for shared Hugging Face artifacts")
	}
	if err := validateHfArtifactCleanPath(root); err != nil {
		return "", err
	}
	if root == string(filepath.Separator) {
		return "", errors.New("filesystem root is not a model store")
	}
	if info, err := os.Lstat(root); err == nil && !info.IsDir() {
		return "", fmt.Errorf("model store root %s is not a directory", root)
	} else if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	// Resolve OS aliases above the store (e.g. /var -> /private/var), including
	// when the store has not been created yet. Links below it are not allowed.
	base := root
	for {
		resolved, err := filepath.EvalSymlinks(base)
		if err == nil {
			relative, _ := filepath.Rel(base, root)
			return filepath.Join(resolved, relative), nil
		}
		if !os.IsNotExist(err) || filepath.Dir(base) == base {
			return "", err
		}
		base = filepath.Dir(base)
	}
}

func validateHfArtifactCleanPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.TrimSpace(path) != path || strings.ContainsRune(path, '\x00') {
		return fmt.Errorf("expected a canonical absolute artifact path, got %q", path)
	}
	return nil
}

// hfArtifactPathInRoot normalizes only the store prefix, never a symlink within
// the store. Both spellings of an OS-aliased root refer to the same lock inode.
func hfArtifactPathInRoot(path, configuredRoot, root string) (string, error) {
	if err := validateHfArtifactCleanPath(path); err != nil {
		return "", err
	}
	for _, prefix := range []string{configuredRoot, root} {
		if hfArtifactInputPathWithin(prefix, path) {
			relative, _ := filepath.Rel(prefix, path)
			return filepath.Join(root, relative), nil
		}
	}
	for prefix := path; filepath.Dir(prefix) != prefix; prefix = filepath.Dir(prefix) {
		if resolved, err := filepath.EvalSymlinks(prefix); err == nil && resolved == root {
			relative, _ := filepath.Rel(prefix, path)
			return filepath.Join(root, relative), nil
		}
	}
	return "", fmt.Errorf("artifact path %s is outside model store root %s", path, root)
}

func validateHfArtifactPathAncestors(root, path string, includeLeaf bool) error {
	if !hfArtifactInputPathWithin(root, path) {
		return fmt.Errorf("artifact path %s is outside model store root %s", path, root)
	}
	if !includeLeaf {
		path = filepath.Dir(path)
	}
	for current := path; hfArtifactInputPathWithin(root, current); current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err == nil && !info.IsDir() {
			return fmt.Errorf("artifact path ancestor %s is not a real directory", current)
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if current == root {
			break
		}
	}
	return nil
}

func hfArtifactParentStoreRoot(parent HfArtifactEntry) string {
	root := parent.LocalPath
	for i := 0; i < len(strings.Split(parent.Identity.ModelID, "/"))+2; i++ {
		root = filepath.Dir(root)
	}
	return root
}

func validateHfArtifactParentPath(parent HfArtifactEntry, modelStoreRoot string) (string, string, error) {
	if err := validateHfArtifactIdentityAndPath(parent); err != nil {
		return "", "", err
	}
	root, err := canonicalHfArtifactStoreRoot(modelStoreRoot)
	if err != nil {
		return "", "", err
	}
	path, err := hfArtifactPathInRoot(parent.LocalPath, modelStoreRoot, root)
	if err != nil {
		return "", "", err
	}
	if err := validateHfArtifactPathAncestors(root, path, true); err != nil {
		return "", "", err
	}
	return root, path, nil
}

func (input hfArtifactTaskInput) validateFilesystemPaths(parent HfArtifactEntry) error {
	root, parentPath, err := validateHfArtifactParentPath(parent, input.ModelStoreRoot)
	if err != nil {
		return err
	}
	expectedPath, err := hfArtifactPathInRoot(input.Parent.LocalPath, input.ModelStoreRoot, root)
	if err != nil || parentPath != expectedPath || parent.Key != input.Parent.Key {
		return fmt.Errorf("shared artifact parent path changed: %s", parent.LocalPath)
	}
	childPath, err := hfArtifactPathInRoot(input.ChildModelPath, input.ModelStoreRoot, root)
	if err != nil {
		return err
	}
	if childPath == root || hfArtifactInputPathWithin(childPath, parentPath) || hfArtifactInputPathWithin(parentPath, childPath) {
		return fmt.Errorf("child path %s overlaps the shared artifact or store", childPath)
	}
	relative, _ := filepath.Rel(root, childPath)
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == constants.ModelArtifactsDirectory || part == hfArtifactLockDirectory {
			return fmt.Errorf("child path %s uses a reserved artifact directory", childPath)
		}
	}
	return validateHfArtifactPathAncestors(root, childPath, false)
}

// ParentReadyMarkerExists checks for a parent directory and a local ready
// marker. It does not validate model files or their checksums.
func (hfArtifactFiles) ParentReadyMarkerExists(parent HfArtifactEntry) bool {
	info, err := os.Lstat(parent.LocalPath)
	if err != nil || !info.IsDir() {
		return false
	}
	info, err = os.Lstat(filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName))
	return err == nil && info.Mode().IsRegular()
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
	marker := filepath.Join(parent.LocalPath, constants.HfArtifactReadyMarkerFileName)
	if info, err := os.Lstat(marker); err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("parent ready marker %s is not a regular file", marker)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(
		marker,
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
	_, err := files.createChildSymlink(childModelPath, parentPath)
	return err
}

// createChildSymlink reports ownership of a newly created link for rollback.
// The caller holds the child path lock through reference publication/cleanup.
func (files hfArtifactFiles) createChildSymlink(childModelPath, parentPath string) (bool, error) {
	if err := os.MkdirAll(filepath.Dir(childModelPath), 0o755); err != nil {
		return false, err
	}
	if _, err := os.Lstat(childModelPath); err == nil {
		if files.IsChildLinkedToParent(childModelPath, parentPath) {
			return false, nil
		}
		return false, fmt.Errorf("%w: %s", errHfArtifactChildPathConflict, childModelPath)
	} else if !os.IsNotExist(err) {
		return false, err
	}
	target, err := filepath.Rel(filepath.Dir(childModelPath), parentPath)
	if err != nil {
		return false, err
	}
	err = os.Symlink(target, childModelPath)
	return err == nil, err
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
	root, err := canonicalHfArtifactStoreRoot(modelStoreRoot)
	if err != nil {
		return false, err
	}
	cleanParentPath, err := hfArtifactPathInRoot(parentPath, modelStoreRoot, root)
	if err != nil {
		return false, err
	}
	foundChild := false
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// A missing root has no children. An error below an existing root
			// means the scan is incomplete, even if a directory disappeared.
			if path == root && os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if path == root && !entry.IsDir() {
			return fmt.Errorf("model store root %s is not a directory", modelStoreRoot)
		}
		if path != root && entry.IsDir() && (path == cleanParentPath || entry.Name() == hfArtifactLockDirectory) {
			return filepath.SkipDir
		}
		if entry.Type()&os.ModeSymlink == 0 {
			return nil
		}
		target, err := readChildSymlinkTarget(path)
		if err != nil {
			return err
		}
		// Absolute targets may use a different OS spelling of the store root.
		canonicalTarget, err := hfArtifactPathInRoot(target, modelStoreRoot, root)
		if err == nil && canonicalTarget == cleanParentPath {
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
