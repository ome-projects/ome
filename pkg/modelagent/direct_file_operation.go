package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"

	"sigs.k8s.io/ome/pkg/utils/storage"
)

type directFileOperationKey struct{}

// Locks belong to the executing attempt, never to a queued task. Source
// adapters can acquire after shared-reference cleanup, while the dispatcher
// retains protection through metadata and terminal status publication.
func directFileOperationContext(ctx context.Context) (context.Context, func()) {
	if ctx.Value(directFileOperationKey{}) != nil {
		return ctx, func() {}
	}
	locks := make(map[string]*flock.Flock)
	return context.WithValue(ctx, directFileOperationKey{}, locks), func() {
		for _, lock := range locks {
			_ = lock.Close()
		}
	}
}

// isDirectFileTask identifies tasks routed directly to HF or OCI file handling.
// A Shared task can still fall back to Direct handling after routing.
func isDirectFileTask(task *GopherTask) bool {
	if task.SharedArtifact {
		return false
	}
	if task.BaseModel == nil && task.ClusterBaseModel == nil {
		return false
	}
	spec := taskModelSpec(task)
	if spec.Storage == nil || spec.Storage.StorageUri == nil {
		return false
	}
	kind, err := storage.GetStorageType(*spec.Storage.StorageUri)
	return err == nil && (kind == storage.StorageTypeOCI || kind == storage.StorageTypeHuggingFace)
}

// Shared tasks and Direct file tasks use the same per-model task coordinator.
func usesArtifactTaskCoordinator(task *GopherTask) bool {
	return task.SharedArtifact || isDirectFileTask(task)
}

func (s *Gopher) tryLockDirectModelPath(ctx context.Context, path string) (bool, error) {
	locks := ctx.Value(directFileOperationKey{}).(map[string]*flock.Flock)
	path, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	if path == string(filepath.Separator) {
		return false, fmt.Errorf("direct model destination cannot be the filesystem root")
	}
	// Resolve directory aliases, including an ordinary symlinked model root.
	// Keep the leaf unresolved: deleting a legacy child removes its link.
	for ancestor := filepath.Dir(path); ; ancestor = filepath.Dir(ancestor) {
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			relative, _ := filepath.Rel(ancestor, path)
			path = filepath.Join(resolved, relative)
			break
		}
		if !os.IsNotExist(err) || ancestor == filepath.Dir(ancestor) {
			return false, err
		}
	}
	if locks[path] != nil {
		return true, nil
	}
	// Use the existing child lock key and directory, without imposing shared
	// layout input restrictions (ordinary destinations can contain whitespace).
	root := filepath.Dir(path)
	lock, acquired, err := tryHfArtifactFileLock(root, filepath.Join(root, hfArtifactLockDirectory), "child:"+path)
	if acquired {
		locks[path] = lock
	}
	return acquired, err
}
