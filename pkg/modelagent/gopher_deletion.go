package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// canonicalModelPath resolves directory aliases but leaves the final component
// unresolved: deleting a model symlink must remove the link, not its target.
func canonicalModelPath(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("model path is empty")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	for ancestor := filepath.Dir(path); ; ancestor = filepath.Dir(ancestor) {
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			relative, err := filepath.Rel(ancestor, path)
			if err != nil {
				return "", err
			}
			return filepath.Join(resolved, relative), nil
		}
		if !os.IsNotExist(err) || ancestor == filepath.Dir(ancestor) {
			return "", err
		}
	}
}

func modelPathWithin(parent, path string) bool {
	relative, err := filepath.Rel(parent, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

// collectReferencedPaths snapshots exact and descendant references before any
// file removal. Symlink consumers also protect their real directory in target.
func (s *Gopher) collectReferencedPaths(target string, excludeBaseModel *v1beta1.BaseModel, excludeClusterBaseModel *v1beta1.ClusterBaseModel) ([]string, error) {
	lexicalTarget, err := filepath.Abs(target)
	if err != nil {
		return nil, err
	}
	target, err = canonicalModelPath(target)
	if err != nil {
		return nil, err
	}
	if s.baseModelLister == nil || s.clusterBaseModelLister == nil {
		return nil, fmt.Errorf("model listers are unavailable for artifact deletion")
	}
	baseModels, err := s.baseModelLister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("failed to list BaseModels: %w", err)
	}
	clusterBaseModels, err := s.clusterBaseModelLister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("failed to list ClusterBaseModels: %w", err)
	}

	references := make(map[string]struct{})
	add := func(path string) {
		if modelPathWithin(target, path) {
			references[path] = struct{}{}
		}
	}
	collect := func(spec v1beta1.BaseModelSpec) error {
		if spec.Storage == nil {
			return nil
		}
		var path string
		if spec.Storage.Path != nil && *spec.Storage.Path != "" {
			path = *spec.Storage.Path
		} else if spec.Storage.StorageUri != nil {
			path = getDestPath(&spec, s.modelRootDir)
		} else {
			return nil
		}
		lexicalPath, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		// Preserve a symlink container when a consumer names a path through it.
		if modelPathWithin(lexicalTarget, lexicalPath) {
			relative, err := filepath.Rel(lexicalTarget, lexicalPath)
			if err != nil {
				return err
			}
			add(filepath.Join(target, relative))
		}
		canonical, err := canonicalModelPath(path)
		if err != nil {
			return err
		}
		add(canonical)
		resolved, err := filepath.EvalSymlinks(lexicalPath)
		if err == nil {
			add(resolved)
		} else if !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	for _, model := range baseModels {
		if excludeBaseModel != nil && model.Namespace == excludeBaseModel.Namespace && model.Name == excludeBaseModel.Name {
			continue
		}
		if err := collect(model.Spec); err != nil {
			return nil, fmt.Errorf("check BaseModel %s/%s path: %w", model.Namespace, model.Name, err)
		}
	}
	for _, model := range clusterBaseModels {
		if excludeClusterBaseModel != nil && model.Name == excludeClusterBaseModel.Name {
			continue
		}
		if err := collect(model.Spec); err != nil {
			return nil, fmt.Errorf("check ClusterBaseModel %s path: %w", model.Name, err)
		}
	}
	paths := make([]string, 0, len(references))
	for path := range references {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

// removeUnreferencedFiles returns whether path was retained. It never follows
// symlinks or removes internal artifact/lock directories owned by other flows.
func removeUnreferencedFiles(ctx context.Context, path string, protectedPaths []string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if filepath.Base(path) == hfArtifactLockDirectory || filepath.Base(path) == constants.ModelArtifactsDirectory {
		return true, nil
	}
	containsReference := false
	for _, protected := range protectedPaths {
		if modelPathWithin(protected, path) {
			return true, nil
		}
		containsReference = containsReference || modelPathWithin(path, protected)
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return containsReference, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		if containsReference {
			return true, nil
		}
		err := os.Remove(path)
		if os.IsNotExist(err) {
			err = nil
		}
		return false, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	retained := containsReference
	for _, entry := range entries {
		kept, err := removeUnreferencedFiles(ctx, filepath.Join(path, entry.Name()), protectedPaths)
		if err != nil {
			return false, err
		}
		retained = retained || kept
	}
	if retained {
		return true, nil
	}
	err = os.Remove(path)
	if os.IsNotExist(err) {
		err = nil
	}
	return false, err
}
