package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/ome/pkg/constants"
)

// Local models borrow bytes, not deletion ownership. The persisted shared
// layout selects the existing parent lock and (when used) its child link lock.
func (s *Gopher) sharedArtifactReaderInput(ctx context.Context, task *GopherTask) (hfArtifactTaskInput, bool, error) {
	var input hfArtifactTaskInput
	if s.configMapReconciler == nil {
		return input, false, nil
	}
	raw, err := s.artifactDestination(task)
	if err != nil || raw == "" {
		return input, false, err
	}
	path, err := resolveArtifactPath(raw)
	if err != nil {
		return input, false, err
	}
	var configuredRoot string
	if s.modelRootDir != "" {
		configuredRoot, err = canonicalHfArtifactStoreRoot(s.modelRootDir)
		if err != nil {
			return input, false, err
		}
	}
	cm, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil && !apierrors.IsNotFound(err) {
		return input, false, err
	}
	var data map[string]string
	if err == nil {
		data = cm.Data
	}
	for key, rawParent := range data {
		if !isHfArtifactConfigMapKey(key) {
			continue
		}
		parent, err := decodeHfArtifactEntry(key, rawParent)
		if err != nil {
			continue // An unrelated corrupt identity must not block ordinary readers.
		}
		candidate, err := resolveArtifactPath(parent.LocalPath)
		if err != nil || !hfArtifactInputPathWithin(candidate, path) {
			continue
		}
		// BaseModels may use a persisted custom shared store outside the
		// configured default. Match deletion/startup's operation root before
		// classifying any outside-default path as an ordinary legacy reader.
		storeRoot := hfArtifactParentStoreRoot(parent)
		if configuredRoot != "" && hfArtifactInputPathWithin(configuredRoot, candidate) {
			storeRoot = s.modelRootDir
		}
		root, parentPath, err := validateHfArtifactParentPath(parent, storeRoot)
		if err != nil {
			return input, false, err
		}
		if !hfArtifactInputPathWithin(parentPath, path) {
			continue
		}
		input = hfArtifactTaskInput{Parent: parent, ModelStoreRoot: storeRoot}
		for childKey, childPath := range parent.Children {
			childPath, err = hfArtifactPathInRoot(childPath, storeRoot, root)
			if err != nil {
				return input, false, err
			}
			usesChild, err := artifactReferenceUsesPath(raw, childPath)
			if err != nil {
				return input, false, err
			}
			if usesChild {
				if input.ChildModelPath != "" {
					return input, false, fmt.Errorf("local reader crosses multiple shared child links")
				}
				input.ChildModelKey, input.ChildModelPath = childKey, childPath
			}
		}
		// Root-level parents have independent locks, not one _artifacts family
		// lock. Inspect real entries too: visiting another removable directory
		// before '..' depends on that directory just as following its alias does.
		_, err = walkArtifactPath(raw, false, func(entry string, link bool) error {
			if entry == input.ChildModelPath {
				return nil
			}
			if !hfArtifactInputPathWithin(root, entry) && (configuredRoot == "" || !hfArtifactInputPathWithin(configuredRoot, entry)) && !isSharedArtifactLayoutPath(entry) {
				return nil
			}
			if !link {
				if entry == root || entry == configuredRoot || hfArtifactInputPathWithin(entry, parentPath) || hfArtifactInputPathWithin(parentPath, entry) ||
					input.ChildModelPath != "" && hfArtifactInputPathWithin(entry, input.ChildModelPath) {
					return nil
				}
				if hfArtifactInputPathWithin(root, entry) {
					family := hfArtifactPathFamilyLockKey(root, entry)
					if family == hfArtifactParentLockKey(root, parentPath) ||
						input.ChildModelPath != "" && family == hfArtifactPathFamilyLockKey(root, input.ChildModelPath) {
						return nil
					}
				}
			}
			return fmt.Errorf("shared reader entry %s is not protected by its operation locks", entry)
		})
		if err != nil {
			return input, false, err
		}
		return input, true, nil
	}
	// An outward link can end at ordinary external storage while its entry
	// remains owned by a shared parent. Without a matching operation above,
	// every traversed shared-layout entry must fail closed, including on loss
	// of the ConfigMap. Only a genuinely unmanaged walk may use legacy access.
	_, err = walkArtifactPath(raw, false, func(entry string, _ bool) error {
		if isSharedArtifactLayoutPath(entry) {
			return fmt.Errorf("shared reader requires protected parent identity for %s", entry)
		}
		return nil
	})
	return input, false, err
}

func isSharedArtifactLayoutPath(path string) bool {
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == constants.ModelArtifactsDirectory {
			return true
		}
	}
	return false
}

func (s *Gopher) acquireLocalArtifactReader(ctx context.Context, task *GopherTask) (func(), bool, error) {
	input, shared, err := s.sharedArtifactReaderInput(ctx, task)
	if err != nil {
		return nil, false, err
	}
	if !shared {
		return s.acquireDirectArtifactOperation(ctx, task, true)
	}
	h := s.sharedHfArtifactHandler()
	var unlock func()
	var acquired bool
	if input.ChildModelPath == "" {
		unlock, acquired, err = h.tryParentFileOperation(input.Parent, input.ModelStoreRoot)
	} else {
		unlock, acquired, err = h.tryArtifactOperation(input)
	}
	if err != nil || !acquired {
		return nil, false, err
	}
	if err := s.validateSharedArtifactReader(ctx, task, input); err != nil {
		unlock()
		return nil, false, err
	}
	if err := s.handoffOrdinaryArtifactOwner(ctx, task); err != nil {
		unlock()
		return nil, false, err
	}
	if op := directArtifactDownloadOperationFromContext(ctx); op != nil {
		op.release, op.readOnly, op.sharedReader = unlock, true, &input
		return func() {}, true, nil
	}
	return unlock, true, nil
}

func (s *Gopher) validateSharedArtifactReader(ctx context.Context, task *GopherTask, expected hfArtifactTaskInput) error {
	if err := s.validateArtifactDownload(ctx, task); err != nil {
		return err
	}
	current, found, err := s.sharedArtifactReaderInput(ctx, task)
	if err != nil {
		return err
	}
	if !found || current.Parent.Key != expected.Parent.Key || current.Parent.LocalPath != expected.Parent.LocalPath ||
		current.ChildModelPath != expected.ChildModelPath || current.ChildModelKey != expected.ChildModelKey {
		return fmt.Errorf("shared reader ownership changed before Ready")
	}
	if current.Parent.Status != HfArtifactStatusReady || !s.sharedHfArtifactHandler().files.ParentReadyMarkerExists(current.Parent) {
		return fmt.Errorf("shared reader parent is not ready")
	}
	path, err := s.artifactDestination(task)
	if err != nil {
		return err
	}
	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		return fmt.Errorf("shared reader path is absent: %v", err)
	}
	return nil
}
