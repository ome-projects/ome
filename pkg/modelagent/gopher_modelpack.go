package modelagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/llmman"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// processModelPackModel acquires a CNCF ModelPack OCI artifact through a
// running `llmman serve` daemon.
//
// The daemon owns the registry work (ModelPack media types, registry auth,
// resumable blob download and a content-addressed store), so it is not
// reimplemented here. It does the pull (POST /api/pull, streamed as NDJSON so
// a multi-gigabyte fetch is not silent) but deliberately exposes no local
// path, so `llmman resolve --no-pull` reports where the bytes landed.
func (s *Gopher) processModelPackModel(ctx context.Context, task *GopherTask,
	baseModelSpec v1beta1.BaseModelSpec, modelInfo, modelType, namespace, name string) error {

	components, err := storage.ParseModelPackStorageURI(*baseModelSpec.Storage.StorageUri)
	if err != nil {
		s.logger.Errorf("Failed to parse ModelPack URI for model %s: %v", modelInfo, err)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "invalid_modelpack_uri")
		s.markModelOnNodeFailed(ctx, task)
		return err
	}

	destPath := getDestPath(&baseModelSpec, s.modelRootDir)

	progress := func(status string, completed, total int64) {
		if total > 0 {
			s.logger.Infof("llmman: %s (%d/%d bytes) for model %s", status, completed, total, modelInfo)
		} else {
			s.logger.Infof("llmman: %s for model %s", status, modelInfo)
		}
	}

	s.logger.Infof("Pulling ModelPack artifact %s for model %s", components.Reference, modelInfo)
	resolved, err := llmman.PullAndResolve(ctx, components.Reference, progress)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.logger.Errorf("Failed to pull ModelPack artifact for model %s: %v", modelInfo, err)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "llmman_pull_failed")
		s.markModelOnNodeFailed(ctx, task)
		return err
	}

	if err := materializeModelPack(ctx, resolved, destPath); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.logger.Errorf("Failed to place ModelPack artifact for model %s: %v", modelInfo, err)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "llmman_place_failed")
		s.markModelOnNodeFailed(ctx, task)
		return err
	}

	s.logger.Infof("Placed ModelPack artifact %s at %s", components.Reference, destPath)

	// Register the model config in the ConfigMap before it is marked Ready.
	if err := s.safeParseAndUpdateModelConfig(ctx, destPath, task.BaseModel, task.ClusterBaseModel, nil); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.logger.Errorf("Failed to parse and update model config: %v", err)
	}
	return nil
}

// materializeModelPack places llmman's extracted artifact at dest.
//
// The artifact is built in a staging directory next to dest and swapped in
// only after it is complete, so dest never mixes files from two artifacts and
// a failed copy leaves any existing dest untouched.
//
// Files are hard-linked where possible so a model shared with llmman's store
// costs its bytes once, falling back to a copy across filesystems.
//
// ctx is checked while staging and again before the swap, so a canceled task
// (for example a deletion) never installs files or keeps waiting on a copy.
func materializeModelPack(ctx context.Context, src, dest string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("resolved path %q: %w", src, err)
	}

	dest = filepath.Clean(dest)
	// Replacing dest would delete src if src lives inside it.
	if rel, err := filepath.Rel(dest, src); err == nil && rel != ".." && !strings.HasPrefix(rel, "../") {
		return fmt.Errorf("resolved path %q is inside destination %q", src, dest)
	}

	parent := filepath.Dir(dest)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("creating %q: %w", parent, err)
	}
	// A sibling of dest keeps the final swap a same-filesystem rename.
	work, err := os.MkdirTemp(parent, "."+filepath.Base(dest)+".staging-")
	if err != nil {
		return fmt.Errorf("creating staging directory for %q: %w", dest, err)
	}
	defer os.RemoveAll(work)

	staged := filepath.Join(work, "new")
	if err := stageModelPack(ctx, src, staged, info.IsDir()); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return swapDir(staged, dest, filepath.Join(work, "old"))
}

// stageModelPack copies or links src into the new directory staged.
func stageModelPack(ctx context.Context, src, staged string, srcIsDir bool) error {
	if err := os.MkdirAll(staged, 0o755); err != nil {
		return fmt.Errorf("creating %q: %w", staged, err)
	}

	if !srcIsDir {
		// A single-file payload, e.g. GGUF.
		return linkOrCopyFile(src, filepath.Join(staged, filepath.Base(src)))
	}

	return filepath.Walk(src, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(staged, rel)
		if fi.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !fi.Mode().IsRegular() {
			// Symlinks and devices carry no model payload.
			return nil
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return linkOrCopyFile(path, target)
	})
}

// swapDir renames staged to dest. Any existing dest is first moved to backup
// (same filesystem, must not exist) and put back if the swap fails.
func swapDir(staged, dest, backup string) error {
	hadDest := true
	if err := os.Rename(dest, backup); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("moving aside %q: %w", dest, err)
		}
		hadDest = false
	}
	if err := os.Rename(staged, dest); err != nil {
		if hadDest {
			if rerr := os.Rename(backup, dest); rerr != nil {
				return fmt.Errorf("placing %q: %w (restoring previous: %v)", dest, err, rerr)
			}
		}
		return fmt.Errorf("placing %q: %w", dest, err)
	}
	return nil
}

func linkOrCopyFile(src, dest string) error {
	if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Link(src, dest); err == nil {
		return nil
	}

	// Stream across filesystems so a large model is never held in memory.
	return ociobjectstore.CopyByFilePath(src, dest)
}
