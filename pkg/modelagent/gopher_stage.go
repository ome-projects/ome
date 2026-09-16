package modelagent

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/hfutil/hub"
	"sigs.k8s.io/ome/pkg/modelagent/stage"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// StageConfig configures how stage:// models are copied onto this node.
type StageConfig struct {
	// SourceRoots limits which paths a stage:// URI may name. Empty disables
	// staging entirely — this is a security boundary, not a convenience,
	// because whatever is staged ends up mounted into inference pods.
	SourceRoots []string
	// Concurrency caps simultaneous staging operations. Staging is bound by the
	// shared storage's egress rather than by this node, so it gets its own
	// limit instead of reusing the download concurrency.
	Concurrency int
}

const defaultStageConcurrency = 2

// stageDestPath returns the node-local directory a stage:// model is copied to.
//
// Unlike the object-storage protocols, stage:// cannot fall back to
// getDestPath's modelRootDir + storageUri form: that would build a directory
// literally named after the URI.
func (s *Gopher) stageDestPath(baseModelSpec v1beta1.BaseModelSpec) (string, error) {
	if baseModelSpec.Storage == nil || baseModelSpec.Storage.Path == nil || *baseModelSpec.Storage.Path == "" {
		return "", fmt.Errorf("stage:// requires spec.storage.path to name the node-local destination")
	}

	destPath := *baseModelSpec.Storage.Path
	if !strings.HasPrefix(destPath, "/") {
		return "", fmt.Errorf("stage:// destination %q must be an absolute path", destPath)
	}

	return filepath.Clean(destPath), nil
}

// processStageStorageModel copies a model from an already-mounted source
// directory onto this node's local disk.
//
// For stage storage:
//   - Download: copies source -> spec.storage.path, atomically, then parses the
//     model configuration from the copy
//   - Delete: removes the node-local copy only; the source is left untouched
func (s *Gopher) processStageStorageModel(ctx context.Context, task *GopherTask, baseModelSpec v1beta1.BaseModelSpec,
	modelInfo, modelType, namespace, name string) error {

	components, err := storage.ParseStageStorageURI(*baseModelSpec.Storage.StorageUri)
	if err != nil {
		s.logger.Errorf("Failed to parse stage storage URI for model %s: %v", modelInfo, err)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "invalid_stage_uri")
		s.markModelOnNodeFailed(task)
		return err
	}

	destPath, err := s.stageDestPath(baseModelSpec)
	if err != nil {
		s.logger.Errorf("Invalid stage destination for model %s: %v", modelInfo, err)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "invalid_stage_destination")
		s.markModelOnNodeFailed(task)
		return err
	}

	release, err := s.acquireStageSlot(ctx)
	if err != nil {
		return err
	}
	defer release()

	alwaysCopy := baseModelSpec.Storage.DownloadPolicy != nil &&
		*baseModelSpec.Storage.DownloadPolicy == v1beta1.AlwaysDownload

	if err := s.checkStageDiskSpace(components.SourcePath, destPath, alwaysCopy); err != nil {
		s.logger.Errorf("Cannot stage model %s: %v", modelInfo, err)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "insufficient_disk_space")
		s.markModelOnNodeFailed(task)
		return err
	}

	s.logger.Infof("Staging model %s from %s to %s", modelInfo, components.SourcePath, destPath)
	result, err := stage.Run(ctx, components.SourcePath, destPath, stage.Options{
		SourceRoots: s.stageConfig.SourceRoots,
		AlwaysCopy:  alwaysCopy,
	})
	if err != nil {
		s.logger.Errorf("Failed to stage model %s: %v", modelInfo, err)
		s.metrics.RecordFailedDownload(modelType, namespace, name, "stage_failed")
		s.markModelOnNodeFailed(task)
		return err
	}

	if result.Copied {
		s.metrics.RecordBytesTransferred(modelType, namespace, name, result.BytesCopied)
		s.logger.Infof("Staged model %s to %s (%d bytes)", modelInfo, destPath, result.BytesCopied)
	} else {
		s.logger.Infof("Reusing existing stage of model %s at %s", modelInfo, destPath)
	}

	var baseModel *v1beta1.BaseModel
	var clusterBaseModel *v1beta1.ClusterBaseModel
	if task.BaseModel != nil {
		baseModel = task.BaseModel
	} else if task.ClusterBaseModel != nil {
		clusterBaseModel = task.ClusterBaseModel
	}

	if err := s.safeParseAndUpdateModelConfig(destPath, baseModel, clusterBaseModel, nil); err != nil {
		// Consistent with the other protocols: a model whose config we cannot
		// parse may still be servable.
		s.logger.Errorf("Failed to parse and update model config for staged model %s: %v", modelInfo, err)
	}

	return nil
}

// checkStageDiskSpace fails early when the node cannot hold the copy. Without
// it a full disk surfaces as a partial copy far into the transfer.
func (s *Gopher) checkStageDiskSpace(sourcePath, destPath string, alwaysCopy bool) error {
	resolvedSource, err := stage.ResolveSource(sourcePath, s.stageConfig.SourceRoots)
	if err != nil {
		return err
	}

	if !alwaysCopy {
		staged, err := stage.IsStaged(destPath, resolvedSource)
		if err != nil {
			return err
		}
		if staged {
			// Nothing will be written, so sizing the source is wasted walking.
			return nil
		}
	}

	size, err := stage.DirSize(resolvedSource)
	if err != nil {
		return err
	}

	return hub.CheckDiskSpace(size, filepath.Dir(destPath))
}

// acquireStageSlot blocks until a staging slot is free, and returns the
// function that gives it back.
func (s *Gopher) acquireStageSlot(ctx context.Context) (func(), error) {
	if s.stageSlots == nil {
		return func() {}, nil
	}

	select {
	case s.stageSlots <- struct{}{}:
		return func() { <-s.stageSlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
